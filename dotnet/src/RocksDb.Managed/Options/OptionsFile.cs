using System.Globalization;
using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Options;

namespace RocksDb.Managed;

/// <summary>A column family name with its options (rocksdb::ColumnFamilyDescriptor).</summary>
public sealed record ColumnFamilyDescriptor(string Name, ColumnFamilyOptions Options)
{
    public const string DefaultColumnFamilyName = "default";
}

/// <summary>
/// An OPTIONS-NNNNNN file: [Version], [DBOptions], and per column family [CFOptions "name"] +
/// [TableOptions/&lt;factory&gt; "name"] sections. Port of RocksDBOptionsParser / PersistRocksDBOptions
/// (options/options_parser.cc). The written layout matches native byte-for-byte except for option order (native
/// iterates an unordered_map) and options not modelled by the managed classes, which are carried in
/// <see cref="PersistedOptions{TSelf}.UnknownOptions"/>.
/// </summary>
public sealed class OptionsFile
{
    /// <summary>ROCKSDB_MAJOR.MINOR.PATCH of the C++ tree this port tracks (include/rocksdb/version.h).</summary>
    public static readonly Version CurrentRocksDbVersion = new(11, 14, 0);
    /// <summary>ROCKSDB_OPTION_FILE_MAJOR.MINOR.</summary>
    public static readonly Version CurrentOptionsFileVersion = new(1, 1);

    private const string Header =
        "# This is a RocksDB option file.\n" +
        "#\n" +
        "# For detailed file format spec, please refer to the example file\n" +
        "# in examples/rocksdb_option_file_example.ini\n" +
        "#\n" +
        "\n";

    private const string SectionVersion = "Version";
    private const string SectionDbOptions = "DBOptions";
    private const string SectionCfOptions = "CFOptions";
    private const string SectionTableOptions = "TableOptions/";

    /// <summary>rocksdb_version of the writer (3 components).</summary>
    public Version RocksDbVersion { get; set; } = CurrentRocksDbVersion;
    public Version OptionsFileVersion { get; set; } = CurrentOptionsFileVersion;
    public DbOptions DbOptions { get; set; } = new();
    /// <summary>Column families in file order; the first must be "default".</summary>
    public List<ColumnFamilyDescriptor> ColumnFamilies { get; } = new();

    public ColumnFamilyOptions? FindColumnFamily(string name)
    {
        foreach (ColumnFamilyDescriptor cf in ColumnFamilies)
        {
            if (cf.Name == name) return cf.Options;
        }
        return null;
    }

    // ---- writing ----------------------------------------------------------------------------

    /// <summary>Serializes to the OPTIONS file text (PersistRocksDBOptions layout, "\n  " delimiter).</summary>
    public string Serialize()
    {
        var sb = new StringBuilder(16 * 1024);
        sb.Append(Header);
        sb.Append('[').Append(SectionVersion).Append("]\n");
        sb.Append("  rocksdb_version=").Append(FormatVersion(RocksDbVersion, 3)).Append('\n');
        sb.Append("  options_file_version=").Append(FormatVersion(OptionsFileVersion, 2)).Append('\n');
        sb.Append("\n[").Append(SectionDbOptions).Append("]\n  ");
        AppendOptions(sb, DbOptions.ToOptionsList());
        sb.Append('\n');
        foreach (ColumnFamilyDescriptor cf in ColumnFamilies)
        {
            string escapedName = OptionStrings.Escape(cf.Name);
            sb.Append("\n[").Append(SectionCfOptions).Append(" \"").Append(escapedName).Append("\"]\n  ");
            AppendOptions(sb, cf.Options.ToOptionsList());
            sb.Append('\n');
            string factory = OptionEnumMaps.TableFactory.ToName(cf.Options.TableFactory);
            if (cf.Options.TableFactory == TableFactoryType.BlockBasedTable)
            {
                sb.Append('[').Append(SectionTableOptions).Append(factory).Append(" \"").Append(escapedName).Append("\"]\n  ");
                AppendOptions(sb, cf.Options.TableOptions.ToOptionsList());
                sb.Append('\n');
            }
            else if (cf.Options.OtherTableFactoryOptions is { } other)
            {
                sb.Append('[').Append(SectionTableOptions).Append(factory).Append(" \"").Append(escapedName).Append("\"]\n  ");
                AppendOptions(sb, other);
                sb.Append('\n');
            }
        }
        return sb.ToString();
    }

    private static void AppendOptions(StringBuilder sb, IEnumerable<KeyValuePair<string, string>> options)
    {
        foreach (KeyValuePair<string, string> kv in options)
        {
            if (kv.Value.Length == 0) continue; // like ConfigurableHelper::SerializeOptions
            sb.Append(kv.Key).Append('=').Append(kv.Value).Append("\n  ");
        }
    }

    private static string FormatVersion(Version v, int components)
    {
        string s = v.Major.ToString(CultureInfo.InvariantCulture) + "." + Math.Max(v.Minor, 0).ToString(CultureInfo.InvariantCulture);
        if (components == 3) s += "." + Math.Max(v.Build, 0).ToString(CultureInfo.InvariantCulture);
        return s;
    }

    /// <summary>Writes the file (synced). The DB writes "OPTIONS-N.dbtmp" first and renames it into place.</summary>
    public async ValueTask WriteAsync(IFileSystem fs, string path, CancellationToken cancellationToken)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(Serialize());
        IWritableFile file = fs.CreateWritable(path, truncate: true);
        await using (file.ConfigureAwait(false))
        {
            await file.AppendAsync(bytes, cancellationToken).ConfigureAwait(false);
            await file.SyncAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Writes OPTIONS-<paramref name="fileNumber"/> into <paramref name="dbname"/> via a temp file + rename, like
    /// DBImpl::WriteOptionsFile / RenameTempFileToOptionsFile.
    /// </summary>
    public async ValueTask WriteToDbDirectoryAsync(IFileSystem fs, string dbname, ulong fileNumber, CancellationToken cancellationToken)
    {
        string tmp = FileNames.TempOptionsFileName(dbname, fileNumber);
        await WriteAsync(fs, tmp, cancellationToken).ConfigureAwait(false);
        fs.RenameFile(tmp, FileNames.OptionsFileName(dbname, fileNumber));
        fs.SyncDirectory(dbname);
    }

    // ---- reading ----------------------------------------------------------------------------

    /// <summary>Reads and parses an OPTIONS file.</summary>
    public static OptionsFile Load(IFileSystem fs, string path, bool preserveUnknownOptions) =>
        Parse(Encoding.UTF8.GetString(FileNames.ReadAllBytes(fs, path)), preserveUnknownOptions);

    /// <summary>Port of GetLatestOptionsFileName: the OPTIONS-N file with the largest N in the directory, or null.</summary>
    public static string? FindLatestOptionsFile(IFileSystem fs, string dbname)
    {
        string? best = null;
        ulong bestNumber = 0;
        foreach (string child in fs.GetChildren(dbname))
        {
            if (FileNames.TryParseFileName(child, out ulong number, out FileType type) && type == FileType.OptionsFile && (best is null || number > bestNumber))
            {
                best = child;
                bestNumber = number;
            }
        }
        return best is null ? null : dbname + "/" + best;
    }

    /// <summary>
    /// Port of RocksDBOptionsParser::Parse. Option names without a typed property are kept in
    /// <c>UnknownOptions</c> when <paramref name="preserveUnknownOptions"/> is true; otherwise they fail with
    /// InvalidArgument (native's ignore_unknown_options = false behaviour). Throws <see cref="RocksDbException"/>
    /// (InvalidArgument / Corruption) on malformed files.
    /// </summary>
    public static OptionsFile Parse(string text, bool preserveUnknownOptions)
    {
        var file = new OptionsFile();
        var parser = new Parser(file, preserveUnknownOptions);
        int lineNum = 0;
        int pos = 0;
        while (pos <= text.Length)
        {
            int nl = text.IndexOf('\n', pos);
            string rawLine = nl < 0 ? text[pos..] : text[pos..nl];
            pos = nl < 0 ? text.Length + 1 : nl + 1;
            lineNum++;
            if (nl < 0 && rawLine.Length == 0) break;
            string line = TrimAndRemoveComment(rawLine, trimOnly: false);
            if (line.Length == 0) continue;
            if (IsSection(line))
            {
                parser.EndSection();
                parser.BeginSection(line, lineNum);
            }
            else
            {
                int eq = line.IndexOf('=');
                if (eq < 0) throw InvalidArgument(lineNum, "A valid statement must have a '='.");
                string name = TrimAndRemoveComment(line[..eq], trimOnly: true);
                string value = TrimAndRemoveComment(line[(eq + 1)..], trimOnly: false);
                if (name.Length == 0) throw InvalidArgument(lineNum, "A valid statement must have a variable name.");
                parser.AddStatement(name, value, lineNum);
            }
        }
        parser.EndSection();
        if (!parser.HasDbOptions) ThrowHelper.Corruption("A RocksDB Option file must have a single DBOptions section");
        if (!parser.HasDefaultCf) ThrowHelper.Corruption("A RocksDB Option file must have a single CFOptions:default section");
        return file;
    }

    private static bool IsSection(string line) => line.Length >= 2 && line[0] == '[' && line[^1] == ']';

    /// <summary>Port of RocksDBOptionsParser::TrimAndRemoveComment ('#' comments unless escaped as "\#").</summary>
    internal static string TrimAndRemoveComment(string line, bool trimOnly)
    {
        int start = 0, end = line.Length;
        if (!trimOnly)
        {
            int search = 0;
            while (search < line.Length)
            {
                int c = line.IndexOf('#', search);
                if (c < 0) break;
                if (c == 0 || line[c - 1] != '\\')
                {
                    end = c;
                    break;
                }
                search = c + 1;
            }
        }
        while (start < end && IsSpace(line[start])) start++;
        while (start < end && IsSpace(line[end - 1])) end--;
        return start < end ? line[start..end] : "";
    }

    private static bool IsSpace(char c) => c is ' ' or '\t' or '\n' or '\v' or '\f' or '\r';

    private static RocksDbException InvalidArgument(int lineNum, string message) =>
        RocksDbException.InvalidArgument("[RocksDBOptionsParser Error] " + message + " (at line " + lineNum.ToString(CultureInfo.InvariantCulture) + ")");

    private enum Section { None, Version, DbOptions, CfOptions, TableOptions }

    private sealed class Parser
    {
        private readonly OptionsFile _file;
        private readonly bool _preserveUnknown;
        private Section _section = Section.None;
        private string _title = "";
        private string _argument = "";
        private readonly List<KeyValuePair<string, string>> _statements = new();
        private bool _hasVersion;

        public Parser(OptionsFile file, bool preserveUnknown)
        {
            _file = file;
            _preserveUnknown = preserveUnknown;
        }

        public bool HasDbOptions { get; private set; }
        public bool HasDefaultCf { get; private set; }

        public void AddStatement(string name, string value, int lineNum)
        {
            // Native collects statements into a map; a duplicate name keeps the first one (map insert semantics).
            foreach (KeyValuePair<string, string> kv in _statements)
            {
                if (kv.Key == name) return;
            }
            _ = lineNum; // statements before the first section are ignored, like native (EndSection of an unknown section)
            _statements.Add(new(name, value));
        }

        public void BeginSection(string line, int lineNum)
        {
            int argStart = line.IndexOf('"');
            int argEnd = line.LastIndexOf('"');
            if (argStart >= 0 && argStart != argEnd)
            {
                _title = TrimAndRemoveComment(line[1..argStart], trimOnly: true);
                _argument = OptionStrings.Unescape(line[(argStart + 1)..argEnd]);
            }
            else
            {
                _title = TrimAndRemoveComment(line[1..^1], trimOnly: true);
                _argument = "";
            }
            if (_title == SectionVersion) _section = Section.Version;
            else if (_title == SectionDbOptions) _section = Section.DbOptions;
            else if (_title == SectionCfOptions) _section = Section.CfOptions;
            else if (_title.StartsWith(SectionTableOptions, StringComparison.Ordinal) && _title.Length > SectionTableOptions.Length) _section = Section.TableOptions;
            else throw RocksDbException.InvalidArgument("Unknown section " + line);
            CheckSection(lineNum);
        }

        private void CheckSection(int lineNum)
        {
            switch (_section)
            {
                case Section.DbOptions:
                    if (HasDbOptions) throw InvalidArgument(lineNum, "More than one DBOption section found in the option config file");
                    HasDbOptions = true;
                    break;
                case Section.CfOptions:
                {
                    bool isDefault = _argument == ColumnFamilyDescriptor.DefaultColumnFamilyName;
                    if (_file.ColumnFamilies.Count == 0 && !isDefault)
                    {
                        throw InvalidArgument(lineNum, "Default column family must be the first CFOptions section in the option config file");
                    }
                    if (_file.ColumnFamilies.Count != 0 && isDefault)
                    {
                        throw InvalidArgument(lineNum, "Default column family must be the first CFOptions section in the optio/n config file");
                    }
                    if (_file.FindColumnFamily(_argument) is not null)
                    {
                        throw InvalidArgument(lineNum, "Two identical column families found in option config file");
                    }
                    HasDefaultCf |= isDefault;
                    break;
                }
                case Section.TableOptions:
                    if (_file.FindColumnFamily(_argument) is null)
                    {
                        throw InvalidArgument(lineNum, "Does not find a matched column family name in TableOptions section.  Column Family Name:" + _argument);
                    }
                    break;
                case Section.Version:
                    if (_hasVersion) throw InvalidArgument(lineNum, "More than one Version section found in the option config file.");
                    _hasVersion = true;
                    break;
            }
        }

        public void EndSection()
        {
            switch (_section)
            {
                case Section.DbOptions:
                    _file.DbOptions.SetOptions(_statements, _preserveUnknown);
                    break;
                case Section.CfOptions:
                {
                    var cf = new ColumnFamilyOptions();
                    cf.SetOptions(_statements, _preserveUnknown);
                    _file.ColumnFamilies.Add(new ColumnFamilyDescriptor(_argument, cf));
                    break;
                }
                case Section.TableOptions:
                {
                    ColumnFamilyOptions cf = _file.FindColumnFamily(_argument)!;
                    string factory = _title[SectionTableOptions.Length..];
                    if (OptionEnumMaps.TableFactory.TryParse(factory, out TableFactoryType type) && type == TableFactoryType.BlockBasedTable)
                    {
                        var t = new BlockBasedTableOptions();
                        t.SetOptions(_statements, _preserveUnknown);
                        cf.TableOptions = t;
                    }
                    else
                    {
                        var raw = new Dictionary<string, string>(StringComparer.Ordinal);
                        foreach (KeyValuePair<string, string> kv in _statements) raw[kv.Key] = kv.Value;
                        cf.OtherTableFactoryOptions = raw;
                    }
                    break;
                }
                case Section.Version:
                    foreach (KeyValuePair<string, string> kv in _statements)
                    {
                        if (kv.Key == "rocksdb_version")
                        {
                            _file.RocksDbVersion = ParseVersionNumber(kv.Key, kv.Value, 3);
                        }
                        else if (kv.Key == "options_file_version")
                        {
                            _file.OptionsFileVersion = ParseVersionNumber(kv.Key, kv.Value, 2);
                            if (_file.OptionsFileVersion.Major < 1)
                            {
                                ThrowHelper.InvalidArgument("A valid options_file_version must be at least 1.");
                            }
                        }
                    }
                    break;
            }
            _statements.Clear();
            _section = Section.None;
        }

        /// <summary>Port of RocksDBOptionsParser::ParseVersionNumber.</summary>
        private static Version ParseVersionNumber(string verName, string verString, int maxCount)
        {
            Span<int> version = stackalloc int[3];
            int index = 0, current = 0, digits = 0;
            bool hasDot = false;
            foreach (char c in verString)
            {
                if (c == '.')
                {
                    if (index >= maxCount - 1) ThrowHelper.InvalidArgument("A valid " + verName + " can only contains at most " + (maxCount - 1) + " dots.");
                    if (digits == 0) ThrowHelper.InvalidArgument("A valid " + verName + " must have at least one digit before each dot.");
                    version[index++] = current;
                    current = 0;
                    digits = 0;
                    hasDot = true;
                }
                else if (c is >= '0' and <= '9')
                {
                    current = current * 10 + (c - '0');
                    digits++;
                }
                else
                {
                    ThrowHelper.InvalidArgument("A valid " + verName + " can only contains dots and numbers.");
                }
            }
            version[index] = current;
            if (hasDot && digits == 0) ThrowHelper.InvalidArgument("A valid " + verName + " must have at least one digit after each dot.");
            return maxCount == 3 ? new Version(version[0], version[1], version[2]) : new Version(version[0], version[1]);
        }
    }
}
