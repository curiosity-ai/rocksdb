using System.Globalization;
using System.Text;
using RocksDb.Managed.Env;

namespace RocksDb.Managed.Db;

/// <summary>Mirrors rocksdb::FileType (include/rocksdb/types.h).</summary>
public enum FileType
{
    WalFile,
    DbLockFile,
    TableFile,
    DescriptorFile,
    CurrentFile,
    TempFile,
    /// <summary>Either the current info log or an old one.</summary>
    InfoLogFile,
    MetaDatabase,
    IdentityFile,
    OptionsFile,
    BlobFile,
    CompactionProgressFile,
}

/// <summary>Mirrors rocksdb::WalFileType (include/rocksdb/transaction_log.h).</summary>
public enum WalFileType
{
    /// <summary>Archived WAL file under the "archive" directory.</summary>
    ArchivedLogFile = 0,
    /// <summary>Live WAL file in the DB (or wal_dir) directory.</summary>
    AliveLogFile = 1,
}

/// <summary>
/// Port of file/filename.{h,cc}: builders and parser for every file name RocksDB places in a DB
/// directory, plus the CURRENT / IDENTITY update protocols (<c>SetCurrentFile</c>,
/// <c>SetIdentityFile</c>) and CURRENT parsing (<c>GetCurrentManifestPath</c> from db/manifest_ops.cc).
/// Paths are joined with '/' exactly like the C++ code (which also works on Windows).
/// </summary>
public static class FileNames
{
    public const string CurrentFileName = "CURRENT";
    public const string LockFileNameConst = "LOCK";
    public const string IdentityFileNameConst = "IDENTITY";
    public const string OptionsFileNamePrefix = "OPTIONS-";
    public const string CompactionProgressFileNamePrefix = "COMPACTION_PROGRESS-";
    public const string TempFileNameSuffix = "dbtmp";
    public const string ArchivalDirName = "archive";
    public const string SessionTmpDirName = "session_tmp";
    public const string DescriptorFilePrefix = "MANIFEST-";
    public const string MetaDatabasePrefix = "METADB-";
    private const string RocksDbTableFileExt = "sst";
    private const string LevelDbTableFileExt = "ldb";
    private const string BlobFileExt = "blob";
    private const string WalFileExt = "log";

    // ---- builders ---------------------------------------------------------------------------

    /// <summary>"%06llu.suffix" — numbers are zero padded to at least six digits.</summary>
    private static string MakeFileName(ulong number, string suffix) =>
        number.ToString("D6", CultureInfo.InvariantCulture) + "." + suffix;

    private static string MakeFileName(string dir, ulong number, string suffix) => dir + "/" + MakeFileName(number, suffix);

    /// <summary>LogFileName: "dbname/NNNNNN.log".</summary>
    public static string LogFileName(string dbname, ulong number) => MakeFileName(dbname, number, WalFileExt);
    public static string LogFileName(ulong number) => MakeFileName(number, WalFileExt);

    public static string BlobFileName(ulong number) => MakeFileName(number, BlobFileExt);
    public static string BlobFileName(string blobDirName, ulong number) => MakeFileName(blobDirName, number, BlobFileExt);
    public static string BlobFileName(string dbname, string blobDir, ulong number) => MakeFileName(dbname + "/" + blobDir, number, BlobFileExt);

    public static string ArchivalDirectory(string dir) => dir + "/" + ArchivalDirName;
    public static string ArchivedLogFileName(string dbname, ulong number) => MakeFileName(dbname + "/" + ArchivalDirName, number, WalFileExt);

    public static string MakeTableFileName(string path, ulong number) => MakeFileName(path, number, RocksDbTableFileExt);
    public static string MakeTableFileName(ulong number) => MakeFileName(number, RocksDbTableFileExt);

    /// <summary>Rocks2LevelTableFileName: replaces the ".sst" extension with ".ldb".</summary>
    public static string Rocks2LevelTableFileName(string fullname)
    {
        if (fullname.Length <= RocksDbTableFileExt.Length + 1) return "";
        return fullname[..^RocksDbTableFileExt.Length] + LevelDbTableFileExt;
    }

    /// <summary>TableFileNameToNumber: parses the digits immediately preceding the last '.'.</summary>
    public static ulong TableFileNameToNumber(string name)
    {
        ulong number = 0, @base = 1;
        int pos = name.LastIndexOf('.');
        while (--pos >= 0 && name[pos] >= '0' && name[pos] <= '9')
        {
            number += (ulong)(name[pos] - '0') * @base;
            @base *= 10;
        }
        return number;
    }

    /// <summary>TableFileName: picks the db path for <paramref name="pathId"/> (last one if out of range).</summary>
    public static string TableFileName(IReadOnlyList<string> dbPaths, ulong number, uint pathId)
    {
        string path = pathId >= dbPaths.Count ? dbPaths[^1] : dbPaths[(int)pathId];
        return MakeTableFileName(path, number);
    }

    /// <summary>FormatFileNumber: "N" or "N(path P)".</summary>
    public static string FormatFileNumber(ulong number, uint pathId) =>
        pathId == 0
            ? number.ToString(CultureInfo.InvariantCulture)
            : number.ToString(CultureInfo.InvariantCulture) + "(path " + pathId.ToString(CultureInfo.InvariantCulture) + ")";

    /// <summary>DescriptorFileName: "MANIFEST-%06llu".</summary>
    public static string DescriptorFileName(ulong number) => DescriptorFilePrefix + number.ToString("D6", CultureInfo.InvariantCulture);
    public static string DescriptorFileName(string dbname, ulong number) => dbname + "/" + DescriptorFileName(number);

    public static string CurrentFilePath(string dbname) => dbname + "/" + CurrentFileName;
    public static string LockFileName(string dbname) => dbname + "/" + LockFileNameConst;
    public static string IdentityFileName(string dbname) => dbname + "/" + IdentityFileNameConst;

    /// <summary>TempFileName: "dbname/NNNNNN.dbtmp".</summary>
    public static string TempFileName(string dbname, ulong number) => MakeFileName(dbname, number, TempFileNameSuffix);

    /// <summary>InfoLogFileName: "dbname/LOG", or "log_dir/&lt;escaped db path&gt;_LOG" when a log dir is configured.</summary>
    public static string InfoLogFileName(string dbname, string dbAbsolutePath, string logDir)
    {
        if (logDir.Length == 0) return dbname + "/LOG";
        return logDir + "/" + InfoLogPrefix(hasLogDir: true, dbAbsolutePath);
    }

    /// <summary>OldInfoLogFileName: "dbname/LOG.old.&lt;ts&gt;" (or the log-dir variant).</summary>
    public static string OldInfoLogFileName(string dbname, ulong ts, string dbAbsolutePath, string logDir)
    {
        string t = ts.ToString(CultureInfo.InvariantCulture);
        if (logDir.Length == 0) return dbname + "/LOG.old." + t;
        return logDir + "/" + InfoLogPrefix(hasLogDir: true, dbAbsolutePath) + ".old." + t;
    }

    /// <summary>OptionsFileName: "OPTIONS-%06llu".</summary>
    public static string OptionsFileName(ulong fileNumber) => OptionsFileNamePrefix + fileNumber.ToString("D6", CultureInfo.InvariantCulture);
    public static string OptionsFileName(string dbname, ulong fileNumber) => dbname + "/" + OptionsFileName(fileNumber);

    /// <summary>TempOptionsFileName: "dbname/OPTIONS-%06llu.dbtmp".</summary>
    public static string TempOptionsFileName(string dbname, ulong fileNumber) => dbname + "/" + OptionsFileName(fileNumber) + "." + TempFileNameSuffix;

    public static string CompactionProgressFileName(string dbname, ulong timestamp) =>
        dbname + "/" + CompactionProgressFileNamePrefix + timestamp.ToString(CultureInfo.InvariantCulture);

    public static string TempCompactionProgressFileName(string dbname, ulong timestamp) =>
        CompactionProgressFileName(dbname, timestamp) + "." + TempFileNameSuffix;

    public static string SessionTmpDir(string dbname) => dbname + "/" + SessionTmpDirName;

    /// <summary>MetaDatabaseName: "dbname/METADB-N" (no zero padding).</summary>
    public static string MetaDatabaseName(string dbname, ulong number) => dbname + "/" + MetaDatabasePrefix + number.ToString(CultureInfo.InvariantCulture);

    /// <summary>
    /// Port of InfoLogPrefix: "LOG" when there is no separate log dir; otherwise the DB absolute path with
    /// every character outside [A-Za-z0-9._-] replaced by '_' (a leading one dropped), suffixed with "_LOG",
    /// truncated to the 260-byte C++ buffer.
    /// </summary>
    public static string InfoLogPrefix(bool hasLogDir, string dbAbsolutePath)
    {
        if (!hasLogDir) return "LOG";
        string path = NormalizePath(dbAbsolutePath);
        const int len = 260;
        const int suffixSize = 5; // sizeof("_LOG") including NUL
        var sb = new StringBuilder(len);
        for (int i = 0; i < path.Length && sb.Length < len - suffixSize; i++)
        {
            char c = path[i];
            if (c is (>= 'a' and <= 'z') or (>= '0' and <= '9') or (>= 'A' and <= 'Z') or '-' or '.' or '_')
            {
                sb.Append(c);
            }
            else if (i > 0)
            {
                sb.Append('_');
            }
        }
        sb.Append("_LOG");
        return sb.ToString();
    }

    /// <summary>Port of NormalizePath: collapses runs of separators (keeping a leading UNC "//").</summary>
    public static string NormalizePath(string path)
    {
        char sep = Path.DirectorySeparatorChar;
        var dst = new StringBuilder(path.Length);
        if (path.Length > 2 && path[0] == sep && path[1] == sep)
        {
            dst.Append(sep, 2);
        }
        foreach (char c in path)
        {
            if (dst.Length > 0 && (c == sep || c == '/') && (dst[^1] == sep || dst[^1] == '/')) continue;
            dst.Append(c);
        }
        return dst.ToString();
    }

    // ---- parsing ----------------------------------------------------------------------------

    /// <summary>ParseFileName without an info-log prefix (info log files are then not recognised).</summary>
    public static bool TryParseFileName(ReadOnlySpan<char> fname, out ulong number, out FileType type) =>
        TryParseFileName(fname, [], out number, out type, out _);

    /// <summary>
    /// Port of ParseFileName. Recognises IDENTITY, CURRENT, LOCK, info logs (when
    /// <paramref name="infoLogNamePrefix"/> is non-empty), MANIFEST-N, METADB-N, OPTIONS-N[.dbtmp],
    /// COMPACTION_PROGRESS-N[.dbtmp], [archive/]N.log, N.sst, N.ldb, N.blob, N.dbtmp. A single leading '/' is ignored.
    /// </summary>
    /// <remarks>
    /// Deviation: for a name that starts with the info-log prefix but is not followed by "", ".old" or
    /// ".old.N", C++ returns true without setting the type; this port returns false.
    /// </remarks>
    public static bool TryParseFileName(ReadOnlySpan<char> fname, ReadOnlySpan<char> infoLogNamePrefix, out ulong number, out FileType type, out WalFileType walType)
    {
        number = 0;
        type = default;
        walType = WalFileType.AliveLogFile;
        ReadOnlySpan<char> rest = fname;
        if (fname.Length > 1 && fname[0] == '/') rest = rest[1..];

        if (rest.SequenceEqual(IdentityFileNameConst)) { type = FileType.IdentityFile; return true; }
        if (rest.SequenceEqual(CurrentFileName)) { type = FileType.CurrentFile; return true; }
        if (rest.SequenceEqual(LockFileNameConst)) { type = FileType.DbLockFile; return true; }

        if (infoLogNamePrefix.Length > 0 && rest.StartsWith(infoLogNamePrefix))
        {
            rest = rest[infoLogNamePrefix.Length..];
            if (rest.Length == 0 || rest.SequenceEqual(".old"))
            {
                type = FileType.InfoLogFile;
                return true;
            }
            if (rest.StartsWith(".old."))
            {
                rest = rest[5..];
                if (!ConsumeDecimalNumber(ref rest, out ulong ts)) return false;
                number = ts;
                type = FileType.InfoLogFile;
                return true;
            }
            return false;
        }

        if (rest.StartsWith(DescriptorFilePrefix))
        {
            rest = rest[DescriptorFilePrefix.Length..];
            if (!ConsumeDecimalNumber(ref rest, out ulong num) || rest.Length != 0) return false;
            type = FileType.DescriptorFile;
            number = num;
            return true;
        }
        if (rest.StartsWith(MetaDatabasePrefix))
        {
            rest = rest[MetaDatabasePrefix.Length..];
            if (!ConsumeDecimalNumber(ref rest, out ulong num) || rest.Length != 0) return false;
            type = FileType.MetaDatabase;
            number = num;
            return true;
        }
        if (rest.StartsWith(OptionsFileNamePrefix))
        {
            // Note: like C++, trailing garbage after the number is not rejected here.
            rest = rest[OptionsFileNamePrefix.Length..];
            bool isTemp = false;
            if (rest.EndsWith("." + TempFileNameSuffix))
            {
                rest = rest[..^(TempFileNameSuffix.Length + 1)];
                isTemp = true;
            }
            if (!ConsumeDecimalNumber(ref rest, out ulong ts)) return false;
            number = ts;
            type = isTemp ? FileType.TempFile : FileType.OptionsFile;
            return true;
        }
        if (rest.StartsWith(CompactionProgressFileNamePrefix))
        {
            rest = rest[CompactionProgressFileNamePrefix.Length..];
            bool isTemp = false;
            if (rest.EndsWith("." + TempFileNameSuffix))
            {
                rest = rest[..^(TempFileNameSuffix.Length + 1)];
                isTemp = true;
            }
            if (!ConsumeDecimalNumber(ref rest, out ulong ts) || rest.Length != 0) return false;
            number = ts;
            type = isTemp ? FileType.TempFile : FileType.CompactionProgressFile;
            return true;
        }

        bool archiveDirFound = false;
        if (rest.StartsWith(ArchivalDirName))
        {
            if (rest.Length <= ArchivalDirName.Length) return false;
            rest = rest[(ArchivalDirName.Length + 1)..]; // also drop the '/'
            walType = WalFileType.ArchivedLogFile;
            archiveDirFound = true;
        }
        if (!ConsumeDecimalNumber(ref rest, out ulong n)) return false;
        if (rest.Length <= 1 || rest[0] != '.') return false;
        ReadOnlySpan<char> suffix = rest[1..];
        if (suffix.SequenceEqual(WalFileExt))
        {
            type = FileType.WalFile;
        }
        else if (archiveDirFound)
        {
            return false; // archive dir can contain only log files
        }
        else if (suffix.SequenceEqual(RocksDbTableFileExt) || suffix.SequenceEqual(LevelDbTableFileExt))
        {
            type = FileType.TableFile;
        }
        else if (suffix.SequenceEqual(BlobFileExt))
        {
            type = FileType.BlobFile;
        }
        else if (suffix.SequenceEqual(TempFileNameSuffix))
        {
            type = FileType.TempFile;
        }
        else
        {
            return false;
        }
        number = n;
        return true;
    }

    /// <summary>Port of ConsumeDecimalNumber (util/string_util.cc): at least one digit, fails on uint64 overflow.</summary>
    internal static bool ConsumeDecimalNumber(ref ReadOnlySpan<char> input, out ulong value)
    {
        ulong v = 0;
        int digits = 0;
        while (input.Length > 0)
        {
            char c = input[0];
            if (c < '0' || c > '9') break;
            uint delta = (uint)(c - '0');
            const ulong max = ulong.MaxValue;
            if (v > max / 10 || (v == max / 10 && delta > max % 10))
            {
                value = 0;
                return false;
            }
            v = v * 10 + delta;
            digits++;
            input = input[1..];
        }
        value = v;
        return digits > 0;
    }

    // ---- CURRENT / IDENTITY -----------------------------------------------------------------

    /// <summary>
    /// Port of SetCurrentFile: writes "MANIFEST-NNNNNN\n" to "dbname/NNNNNN.dbtmp" (synced), renames it over
    /// CURRENT and fsyncs the directory. On failure the temp file is removed (best effort) and the error rethrown.
    /// </summary>
    public static async ValueTask SetCurrentFileAsync(IFileSystem fs, string dbname, ulong descriptorNumber, bool syncDirectory, CancellationToken cancellationToken)
    {
        string contents = DescriptorFileName(descriptorNumber) + "\n";
        string tmp = TempFileName(dbname, descriptorNumber);
        try
        {
            await WriteStringToFileAsync(fs, contents, tmp, cancellationToken).ConfigureAwait(false);
            fs.RenameFile(tmp, CurrentFilePath(dbname));
        }
        catch
        {
            TryDelete(fs, tmp);
            throw;
        }
        if (syncDirectory) fs.SyncDirectory(dbname);
    }

    /// <summary>
    /// Port of SetIdentityFile: writes the DB id (a generated UUID string when <paramref name="dbId"/> is empty)
    /// to "dbname/000000.dbtmp", renames it to IDENTITY and fsyncs the directory. Returns the id written.
    /// </summary>
    public static async ValueTask<string> SetIdentityFileAsync(IFileSystem fs, string dbname, string? dbId, CancellationToken cancellationToken)
    {
        string id = string.IsNullOrEmpty(dbId) ? GenerateUniqueId() : dbId;
        string tmp = TempFileName(dbname, 0);
        try
        {
            await WriteStringToFileAsync(fs, id, tmp, cancellationToken).ConfigureAwait(false);
            fs.RenameFile(tmp, IdentityFileName(dbname));
            fs.SyncDirectory(dbname);
        }
        catch
        {
            TryDelete(fs, tmp);
            throw;
        }
        return id;
    }

    /// <summary>Env::GenerateUniqueId: a lower-case 36 character UUID string.</summary>
    public static string GenerateUniqueId() => Guid.NewGuid().ToString("D");

    /// <summary>Reads the IDENTITY file contents (the DB id), trimming a trailing newline like GetDbIdentityFromIdentityFile.</summary>
    public static string ReadIdentityFile(IFileSystem fs, string dbname)
    {
        string id = Encoding.UTF8.GetString(ReadAllBytes(fs, IdentityFileName(dbname)));
        if (id.Length > 0 && id[^1] == '\n') id = id[..^1];
        return id;
    }

    /// <summary>
    /// Port of GetCurrentManifestPath (db/manifest_ops.cc): reads CURRENT, which must end in '\n' and name a
    /// MANIFEST file. Throws <see cref="RocksDbException"/> (Corruption) otherwise.
    /// </summary>
    public static string GetCurrentManifestPath(IFileSystem fs, string dbname, out ulong manifestFileNumber)
    {
        byte[] data = ReadAllBytes(fs, CurrentFilePath(dbname));
        if (data.Length == 0 || data[^1] != (byte)'\n') ThrowHelper.Corruption("CURRENT file does not end with newline");
        string fname = Encoding.UTF8.GetString(data, 0, data.Length - 1);
        if (!TryParseFileName(fname, out manifestFileNumber, out FileType type) || type != FileType.DescriptorFile)
        {
            ThrowHelper.Corruption("CURRENT file corrupted");
        }
        return dbname.EndsWith('/') ? dbname + fname : dbname + "/" + fname;
    }

    private static async ValueTask WriteStringToFileAsync(IFileSystem fs, string contents, string path, CancellationToken cancellationToken)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(contents);
        IWritableFile file = fs.CreateWritable(path, truncate: true);
        await using (file.ConfigureAwait(false))
        {
            await file.AppendAsync(bytes, cancellationToken).ConfigureAwait(false);
            await file.SyncAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    internal static byte[] ReadAllBytes(IFileSystem fs, string path)
    {
        using ISequentialFile f = fs.OpenSequential(path);
        var w = new System.Buffers.ArrayBufferWriter<byte>(256);
        while (true)
        {
            int n = f.Read(w.GetSpan(4096));
            if (n <= 0) break;
            w.Advance(n);
        }
        return w.WrittenSpan.ToArray();
    }

    private static void TryDelete(IFileSystem fs, string path)
    {
        try { fs.DeleteFile(path); }
        catch (IOException) { }
        catch (UnauthorizedAccessException) { }
    }
}
