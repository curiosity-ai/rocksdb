using System.Globalization;
using System.Text;
using RocksDb.Managed.Env;

namespace RocksDb.Managed.Utilities.Backup;

/// <summary>
/// A file referenced by one or more backups (BackupEngineImpl::FileInfo). Shared files are reference counted across
/// backups.
/// </summary>
internal sealed class BackupFileEntry
{
    public BackupFileEntry(string filename, ulong size, uint? crc32c, string dbSessionId)
    {
        Filename = filename;
        Size = size;
        Crc32c = crc32c;
        DbSessionId = dbSessionId;
    }

    /// <summary>Path relative to the backup directory.</summary>
    public string Filename { get; }
    public ulong Size { get; }
    /// <summary>crc32c (unmasked) of the contents; null when unknown.</summary>
    public uint? Crc32c { get; set; }
    public string DbSessionId { get; }
    public int Refs { get; set; }

    /// <summary>
    /// FileInfo::GetDbFileName: the file name in the DB (shared_checksum names drop the <c>_&lt;checksum/session/size&gt;</c>
    /// part).
    /// </summary>
    public string GetDbFileName()
    {
        int slash = Filename.LastIndexOf('/');
        string rv = Filename[(slash + 1)..];
        if (slash >= 0 && Filename.AsSpan(0, slash).SequenceEqual(BackupEngineReadOnly.SharedChecksumDirName)) rv = BackupEngineReadOnly.GetFileFromChecksumFile(rv);
        return rv;
    }
}

/// <summary>
/// Port of BackupEngineImpl::BackupMeta (utilities/backup/backup_engine.cc): one backup's meta file
/// (<c>meta/&lt;id&gt;</c>) with its timestamp, sequence number, app metadata and file list, in the schema version 1 / 2
/// text format of LoadFromFile / StoreToFile.
/// </summary>
internal sealed class BackupMeta
{
    // Constants for backup meta file schema (see LoadFromFile).
    private const string SchemaVersionPrefix = "schema_version ";
    private const string FooterMarker = "// FOOTER";
    private const string AppMetaDataFieldName = "metadata";
    // WART: The checksums are crc32c but named "crc32".
    private const string FileCrc32cFieldName = "crc32";
    private const string FileSizeFieldName = "size";
    private const string TemperatureFieldName = "temp";
    private const string ExcludedFieldName = "ni::excluded";
    private const string NonIgnorableFieldPrefix = "ni::";

    /// <summary>Schema minor version strings written for each major version (minor_version_strings).</summary>
    private static readonly string[] MinorVersionStrings = ["", "", "2.1"];

    private readonly Dictionary<string, BackupFileEntry> _fileInfos; // shared by all backups of the engine
    private readonly List<BackupFileEntry> _files = new();
    private readonly List<string> _excludedFiles = new();

    public BackupMeta(string metaFilename, string metaTmpFilename, Dictionary<string, BackupFileEntry> fileInfos)
    {
        MetaFilename = metaFilename;
        MetaTmpFilename = metaTmpFilename;
        _fileInfos = fileInfos;
    }

    public string MetaFilename { get; }
    public string MetaTmpFilename { get; }
    public long Timestamp { get; set; }
    public ulong SequenceNumber { get; set; }
    public byte[] AppMetadata { get; set; } = [];
    public ulong Size { get; private set; }
    public IReadOnlyList<BackupFileEntry> Files => _files;
    public IReadOnlyList<string> ExcludedFiles => _excludedFiles;
    public bool Empty => _files.Count == 0 && _excludedFiles.Count == 0;
    public uint NumberFiles => (uint)_files.Count;

    public void RecordTimestamp() => Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

    /// <summary>Port of BackupMeta::AddFile: coalesces with the engine-wide entry (size and checksum must agree).</summary>
    /// <summary>BackupMeta::AddExcludedFile.</summary>
    public void AddExcludedFile(string relativeFile) => _excludedFiles.Add(relativeFile);

    public void AddFile(BackupFileEntry info)
    {
        if (!_fileInfos.TryGetValue(info.Filename, out BackupFileEntry? existing))
        {
            existing = info;
            existing.Refs = 1;
            _fileInfos.Add(info.Filename, existing);
        }
        else
        {
            // Compare sizes, because we scanned that off the filesystem on both ends.
            if (existing.Size != info.Size)
            {
                ThrowHelper.Corruption("Size mismatch for existing backup file: " + info.Filename + " Size in backup is " +
                    existing.Size.ToString(CultureInfo.InvariantCulture) + " while size in DB is " + info.Size.ToString(CultureInfo.InvariantCulture) +
                    " If this DB file checks as not corrupt, try deleting old backups or backing up to a different backup directory.");
            }
            if (info.Crc32c is null)
            {
                // No checksum available to check.
            }
            else if (existing.Crc32c is null)
            {
                existing.Crc32c = info.Crc32c; // remember checksum if newly acquired
            }
            else if (existing.Crc32c != info.Crc32c)
            {
                ThrowHelper.Corruption("Checksum mismatch for existing backup file: " + info.Filename + " Expected checksum is " +
                    BackupEngineReadOnly.ChecksumToHex(existing.Crc32c.Value) + " while computed checksum is " + BackupEngineReadOnly.ChecksumToHex(info.Crc32c.Value) +
                    " If this DB file checks as not corrupt, try deleting old backups or backing up to a different backup directory.");
            }
            existing.Refs++;
        }
        Size += info.Size;
        _files.Add(existing);
    }

    /// <summary>Port of BackupMeta::Delete: drops the file references and deletes the meta file.</summary>
    public void Delete(IFileSystem fs, bool deleteMeta)
    {
        foreach (BackupFileEntry f in _files) f.Refs--;
        _files.Clear();
        _excludedFiles.Clear();
        if (deleteMeta && fs.FileExists(MetaFilename)) fs.DeleteFile(MetaFilename);
        Timestamp = 0;
    }

    /// <summary>
    /// Port of BackupMeta::LoadFromFile. Each file listed must exist in <paramref name="absPathToSize"/> (with the
    /// recorded size, if any). Throws Corruption / NotSupported for malformed meta files.
    /// </summary>
    public void LoadFromFile(IFileSystem fs, string backupDir, Dictionary<string, ulong> absPathToSize, HashSet<string> reportedIgnoredFields,
        Action<string>? logWarn)
    {
        List<string> lines = ReadLines(fs, MetaFilename);
        int li = 0;
        int schemaMajorVersion = 1;
        string? line = null;
        if (li < lines.Count)
        {
            line = lines[li++];
            if (line.StartsWith(SchemaVersionPrefix, StringComparison.Ordinal))
            {
                string ver = line[SchemaVersionPrefix.Length..];
                if (ver == "2" || ver.StartsWith("2.", StringComparison.Ordinal)) schemaMajorVersion = 2;
                else ThrowHelper.NotSupported("Unsupported/unrecognized schema version: " + ver);
                line = null;
            }
            else if (line.Length == 0)
            {
                ThrowHelper.Corruption("Unexpected empty line");
            }
        }
        if (line is not null) Timestamp = ParseLong(line);
        else if (li < lines.Count) Timestamp = ParseLong(lines[li++]);
        if (li < lines.Count) SequenceNumber = ParseULong(lines[li++]);
        uint numFiles = uint.MaxValue;
        while (li < lines.Count)
        {
            line = lines[li++];
            if (line.Length == 0) ThrowHelper.Corruption("Unexpected empty line");
            // Number -> number of files -> exit loop reading optional meta fields.
            if (line[0] >= '0' && line[0] <= '9')
            {
                numFiles = (uint)ParseULong(line);
                break;
            }
            int space = line.IndexOf(' ');
            if (space < 0) ThrowHelper.Corruption("Expected number of files or meta field");
            string fieldName = line[..space];
            string fieldData = line[(space + 1)..];
            if (fieldName == AppMetaDataFieldName)
            {
                if (!TryDecodeHex(fieldData, out byte[] meta)) ThrowHelper.Corruption("Failed to decode stored hex encoded app metadata");
                AppMetadata = meta;
            }
            else if (schemaMajorVersion < 2)
            {
                ThrowHelper.Corruption("Expected number of files or \"" + AppMetaDataFieldName + "\" field");
            }
            else if (fieldName.StartsWith(NonIgnorableFieldPrefix, StringComparison.Ordinal))
            {
                ThrowHelper.NotSupported("Unrecognized non-ignorable meta field " + fieldName + " (from future version?)");
            }
            else if (reportedIgnoredFields.Add("meta:" + fieldName))
            {
                logWarn?.Invoke("Ignoring unrecognized backup meta field " + fieldName);
            }
        }
        var files = new List<BackupFileEntry>();
        bool footerPresent = false;
        while (li < lines.Count)
        {
            line = lines[li++];
            string[] components = line.Split(' ');
            if (schemaMajorVersion >= 2 && components.Length == 2 && line == FooterMarker)
            {
                footerPresent = true;
                break;
            }
            string filename = components[0];
            if (schemaMajorVersion >= 2)
            {
                if (components.Length % 2 != 1) ThrowHelper.Corruption("Bad number of line components for file entry.");
            }
            else
            {
                // Check restricted original schema.
                if (components.Length < 3) ThrowHelper.Corruption("File checksum is missing for " + filename + " in " + MetaFilename);
                if (components[1] != FileCrc32cFieldName) ThrowHelper.Corruption("Unknown checksum type for " + filename + " in " + MetaFilename);
                if (components.Length > 3) ThrowHelper.Corruption("Extra data for entry " + filename + " in " + MetaFilename);
            }
            ulong? expectedSize = null;
            uint? crc = null;
            bool excluded = false;
            for (int i = 1; i < components.Length; i += 2)
            {
                string fieldName = components[i];
                string fieldData = components[i + 1];
                if (fieldName == FileCrc32cFieldName)
                {
                    // strtoul + round-trip check: the stored value must be a canonical unsigned decimal.
                    if (!uint.TryParse(fieldData, NumberStyles.None, CultureInfo.InvariantCulture, out uint v) ||
                        v.ToString(CultureInfo.InvariantCulture) != fieldData)
                    {
                        ThrowHelper.Corruption("Invalid checksum value for " + filename + " in " + MetaFilename);
                    }
                    crc = v;
                }
                else if (fieldName == FileSizeFieldName)
                {
                    expectedSize = ParseULong(fieldData);
                }
                else if (fieldName == TemperatureFieldName)
                {
                    // Temperatures are not modelled; unknown values map to kUnknown upstream too.
                }
                else if (fieldName == ExcludedFieldName)
                {
                    if (fieldData == "true") excluded = true;
                    else if (fieldData == "false") excluded = false;
                    else ThrowHelper.NotSupported("Unrecognized value \"" + fieldData + "\" for field " + fieldName);
                }
                else if (fieldName.StartsWith(NonIgnorableFieldPrefix, StringComparison.Ordinal))
                {
                    ThrowHelper.NotSupported("Unrecognized non-ignorable file field " + fieldName + " (from future version?)");
                }
                else if (reportedIgnoredFields.Add("file:" + fieldName))
                {
                    logWarn?.Invoke("Ignoring unrecognized backup file field " + fieldName);
                }
            }
            if (excluded)
            {
                _excludedFiles.Add(filename);
            }
            else
            {
                // Verify file exists, with expected size.
                string absPath = backupDir + "/" + filename;
                if (!absPathToSize.TryGetValue(absPath, out ulong actualSize)) ThrowHelper.Corruption("Pathname in meta file not found on disk: " + absPath);
                if (expectedSize is ulong es && es != actualSize)
                {
                    ThrowHelper.Corruption("For file " + filename + " expected size " + es.ToString(CultureInfo.InvariantCulture) + " but found size" +
                        actualSize.ToString(CultureInfo.InvariantCulture));
                }
                files.Add(new BackupFileEntry(filename, actualSize, crc, SessionIdFromName(filename)));
            }
        }
        if (footerPresent)
        {
            while (li < lines.Count)
            {
                line = lines[li++];
                if (line.Length == 0) ThrowHelper.Corruption("Unexpected empty line");
                int space = line.IndexOf(' ');
                if (space < 0) ThrowHelper.Corruption("Expected footer field");
                string fieldName = line[..space];
                if (fieldName.StartsWith(NonIgnorableFieldPrefix, StringComparison.Ordinal))
                {
                    ThrowHelper.NotSupported("Unrecognized non-ignorable field " + fieldName + " (from future version?)");
                }
                else if (reportedIgnoredFields.Add("footer:" + fieldName))
                {
                    logWarn?.Invoke("Ignoring unrecognized backup meta footer field " + fieldName);
                }
            }
        }
        if (numFiles != (uint)files.Count) ThrowHelper.Corruption("Inconsistent number of files or missing/incomplete header in " + MetaFilename);
        foreach (BackupFileEntry f in files) AddFile(f);
    }

    /// <summary>The DB session id embedded in a <c>shared_checksum/&lt;number&gt;_s&lt;session&gt;[_&lt;size&gt;].sst</c> name, or "".</summary>
    internal static string SessionIdFromName(string filename)
    {
        if (!filename.StartsWith(BackupEngineReadOnly.SharedChecksumDirName + "/", StringComparison.Ordinal)) return "";
        int us = filename.IndexOf("_s", StringComparison.Ordinal);
        if (us < 0) return "";
        int end = filename.IndexOfAny(['_', '.'], us + 2);
        return end < 0 ? "" : filename[(us + 2)..end];
    }

    /// <summary>Port of BackupMeta::StoreToFile: writes <c>meta/.&lt;id&gt;.tmp</c>, syncs, renames to <c>meta/&lt;id&gt;</c>.</summary>
    public async ValueTask StoreToFileAsync(IFileSystem fs, bool sync, int schemaVersion, bool synchronous, CancellationToken ct)
    {
        if (schemaVersion < 1) ThrowHelper.InvalidArgument("BackupEngineOptions::schema_version must be >= 1");
        if (schemaVersion > MinorVersionStrings.Length - 1)
        {
            ThrowHelper.NotSupported("Only BackupEngineOptions::schema_version <= " + (MinorVersionStrings.Length - 1).ToString(CultureInfo.InvariantCulture) +
                " is supported");
        }
        string ver = MinorVersionStrings[schemaVersion];
        var buf = new StringBuilder();
        if (ver.Length > 0) buf.Append(SchemaVersionPrefix).Append(ver).Append('\n');
        buf.Append(Timestamp.ToString(CultureInfo.InvariantCulture)).Append('\n');
        buf.Append(SequenceNumber.ToString(CultureInfo.InvariantCulture)).Append('\n');
        if (AppMetadata.Length > 0) buf.Append(AppMetaDataFieldName).Append(' ').Append(Convert.ToHexString(AppMetadata)).Append('\n');
        buf.Append(_files.Count.ToString(CultureInfo.InvariantCulture)).Append('\n');
        foreach (BackupFileEntry f in _files)
        {
            // use crc32c for now, switch to something else if needed
            buf.Append(f.Filename).Append(' ').Append(FileCrc32cFieldName).Append(' ').Append((f.Crc32c ?? 0).ToString(CultureInfo.InvariantCulture)).Append('\n');
        }
        foreach (string f in _excludedFiles) buf.Append(f).Append(' ').Append(ExcludedFieldName).Append(" true\n");
        byte[] bytes = Encoding.UTF8.GetBytes(buf.ToString());
        IWritableFile file = fs.CreateWritable(MetaTmpFilename, truncate: true);
        try
        {
            if (synchronous)
            {
                file.Append(bytes);
                if (sync) file.Sync();
            }
            else
            {
                await file.AppendAsync(bytes, ct).ConfigureAwait(false);
                if (sync) await file.SyncAsync(ct).ConfigureAwait(false);
            }
        }
        finally
        {
            if (synchronous) file.Dispose();
            else await file.DisposeAsync().ConfigureAwait(false);
        }
        fs.RenameFile(MetaTmpFilename, MetaFilename);
    }

    /// <summary>LineFileReader: lines split on '\n' (a final line without newline is returned too).</summary>
    private static List<string> ReadLines(IFileSystem fs, string path)
    {
        long size = fs.GetFileSize(path);
        var data = new byte[size];
        using (ISequentialFile f = fs.OpenSequential(path))
        {
            int read = 0;
            while (read < data.Length)
            {
                int n = f.Read(data.AsSpan(read));
                if (n <= 0) break;
                read += n;
            }
            if (read < data.Length) Array.Resize(ref data, read);
        }
        string text = Encoding.UTF8.GetString(data);
        var lines = new List<string>(text.Split('\n'));
        if (lines.Count > 0 && lines[^1].Length == 0) lines.RemoveAt(lines.Count - 1);
        return lines;
    }

    /// <summary>strtoull semantics: leading digits, 0 if none.</summary>
    private static ulong ParseULong(string s)
    {
        ulong v = 0;
        foreach (char c in s)
        {
            if (c < '0' || c > '9') break;
            v = unchecked(v * 10 + (ulong)(c - '0'));
        }
        return v;
    }

    private static long ParseLong(string s) => unchecked((long)ParseULong(s));

    /// <summary>Slice::DecodeHex (upper or lower case digits, even length).</summary>
    private static bool TryDecodeHex(string s, out byte[] result)
    {
        result = [];
        if (s.Length % 2 != 0) return false;
        try
        {
            result = Convert.FromHexString(s);
            return true;
        }
        catch (FormatException)
        {
            return false;
        }
    }
}
