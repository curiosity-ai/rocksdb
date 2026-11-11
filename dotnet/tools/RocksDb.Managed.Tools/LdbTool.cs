using System.Globalization;
using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Db.Iterators;
using RocksDb.Managed.Env;
using RocksDb.Managed.Wal;

namespace RocksDb.Managed.Tools;

/// <summary>
/// Port of the ldb tool (tools/ldb_tool.cc, tools/ldb_cmd.cc): <c>ldb --db=&lt;path&gt; [global options] &lt;command&gt; [params]</c>.
/// Options are <c>--name=value</c>, flags <c>--name</c>, everything else is the command and its parameters (as
/// LDBCommand::InitFromCmdLineArgs). Output lines and error messages follow the native tool; failures print
/// <c>Failed: message</c> on stderr and exit with 1.
/// </summary>
internal sealed class LdbTool
{
    private const string Delim = " ==> ";

    private readonly Dictionary<string, string> _options = new(StringComparer.Ordinal);
    private readonly HashSet<string> _flags = new(StringComparer.Ordinal);
    private readonly List<string> _params = new();
    private readonly List<string> _flagOrder = new();
    private readonly IFileSystem _fs;
    private readonly TextWriter _out;
    private readonly TextWriter _err;
    private string _command = "";
    private string _dbPath = "";
    private string _cfName = ColumnFamilyDescriptor.DefaultColumnFamilyName;
    private bool _keyHex, _valueHex;
    private RocksDatabase? _db;

    private LdbTool(IFileSystem fs, TextWriter stdout, TextWriter stderr)
    {
        _fs = fs;
        _out = stdout;
        _err = stderr;
    }

    public static int Run(IReadOnlyList<string> args, IFileSystem fs, TextWriter stdout, TextWriter stderr)
    {
        if (args.Count == 0)
        {
            PrintHelp(stderr);
            return 1;
        }
        if (args.Count == 1 && args[0] is not ("--version" or "--help"))
        {
            // LDBCommandRunner::RunCommand: "argc <= 2" with anything but --version / --help prints the help, exit 1.
            PrintHelp(stderr);
            return 1;
        }
        if (args.Count == 1 && args[0] == "--version")
        {
            Version v = OptionsFile.CurrentRocksDbVersion;
            stdout.Write("ldb from RocksDB " + v.Major + "." + v.Minor + "." + v.Build + " (managed)\n");
            return 0;
        }
        if (args.Count == 1 && args[0] == "--help")
        {
            PrintHelp(stdout);
            return 0;
        }
        var tool = new LdbTool(fs, stdout, stderr);
        foreach (string a in args)
        {
            if (a.StartsWith("--", StringComparison.Ordinal))
            {
                // LDBCommand::ParseSingleParam: StringSplit(param, '=') drops a trailing empty piece, so "--x=" is the
                // flag x and "--x=y=" is x -> y; with three or more pieces the value is everything after the first '='.
                string[] splits = a.Split('=');
                int pieces = splits[^1].Length == 0 ? splits.Length - 1 : splits.Length;
                if (pieces <= 1)
                {
                    tool._flags.Add(splits[0][2..]);
                    tool._flagOrder.Add(splits[0][2..]);
                }
                else if (pieces == 2)
                {
                    tool._options[splits[0][2..]] = splits[1];
                }
                else
                {
                    tool._options[splits[0][2..]] = a[(splits[0].Length + 1)..];
                }
            }
            else if (tool._command.Length == 0)
            {
                tool._command = a;
            }
            else
            {
                tool._params.Add(a);
            }
        }
        if (tool._command.Length == 0)
        {
            stderr.Write("Command not specified!");
            PrintHelp(stderr);
            return 1;
        }
        try
        {
            tool.Init();
            if (ValidOptions(tool._command, out bool noDbOpen) is { } valid)
            {
                // LDBCommand::ValidateCmdLineOptions: options in map (sorted) order, then flags in command-line order.
                var names = new List<string>(tool._options.Keys);
                names.Sort(StringComparer.Ordinal);
                foreach (string name in names)
                {
                    if (!IsValidOption(valid, name))
                    {
                        stderr.Write("Invalid command-line option " + name + "\n");
                        return 1;
                    }
                }
                foreach (string flag in tool._flagOrder)
                {
                    if (!IsValidOption(valid, flag))
                    {
                        stderr.Write("Invalid command-line flag " + flag + "\n");
                        return 1;
                    }
                }
                if (tool._command == "dump_wal" && tool._options.ContainsKey("db")) noDbOpen = false;
                if (!noDbOpen && !tool._options.ContainsKey("db") && !tool._options.ContainsKey("path"))
                {
                    stderr.Write("Either --db or --path must be specified.\n");
                    return 1;
                }
            }
            if (!tool.Execute())
            {
                stderr.Write("Unknown command\n");
                PrintHelp(stderr);
                return 1;
            }
            return 0;
        }
        catch (ToolException e)
        {
            stderr.Write("Failed: " + e.Message + "\n");
            return 1;
        }
        catch (RocksDbException e)
        {
            stderr.Write("Failed: " + e.Message + "\n");
            return 1;
        }
        catch (IOException e)
        {
            stderr.Write("Failed: IO error: " + e.Message + "\n");
            return 1;
        }
        finally
        {
            tool._db?.Dispose();
        }
    }

    /// <summary>LDBCommand::BuildCmdLineOptions: the options every command built with it accepts.</summary>
    private static readonly string[] CommonOptions =
    [
        "env_uri", "fs_uri", "db", "secondary_path", "leader_path", "bloom_bits", "block_size", "uniform_cv_threshold", "auto_compaction",
        "compression_type", "compression_max_dict_bytes", "write_buffer_size", "file_size", "fix_prefix_len", "try_load_options",
        "disable_consistency_checks", "enable_blob_files", "min_blob_size", "blob_file_size", "blob_compression_type",
        "enable_blob_garbage_collection", "blob_garbage_collection_age_cutoff", "blob_garbage_collection_force_threshold",
        "blob_compaction_readahead_size", "blob_file_starting_level", "prepopulate_blob_cache", "ignore_unknown_options", "column_family",
        "use_txn", "txn_write_policy",
    ];

    private const string NoCommonOptions = "-";

    private static bool IsValidOption(string[] valid, string name)
    {
        if (Array.IndexOf(valid, name, 1) >= 0) return true;
        return valid[0] != NoCommonOptions && Array.IndexOf(CommonOptions, name) >= 0;
    }

    /// <summary>
    /// The valid_cmd_line_options each command's constructor passes to LDBCommand (tools/ldb_cmd.cc) and its NoDBOpen();
    /// null for an unknown command. The first element says whether the list was built with BuildCmdLineOptions ("") or
    /// stands alone (<see cref="NoCommonOptions"/>: create_column_family / drop_column_family pass {ARG_DB}).
    /// </summary>
    private static string[]? ValidOptions(string command, out bool noDbOpen)
    {
        noDbOpen = false;
        switch (command)
        {
            case "get": return ["", "ttl", "hex", "key_hex", "value_hex", "read_timestamp"];
            case "multi_get": return ["", "hex", "key_hex", "value_hex", "read_timestamp"];
            case "put":
            case "batchput": return ["", "ttl", "hex", "key_hex", "value_hex", "create_if_missing"];
            case "delete":
            case "singledelete":
            case "deleterange": return ["", "hex", "key_hex", "value_hex"];
            case "scan":
                return ["", "ttl", "no_value", "hex", "key_hex", "to", "value_hex", "from", "timestamp", "max_keys", "start_time", "end_time",
                    "read_timestamp", "get_write_unix_time"];
            case "dump":
                return ["", "ttl", "hex", "key_hex", "value_hex", "from", "to", "max_keys", "count_only", "count_delim", "stats", "start_time",
                    "end_time", "bucket", "timestamp", "path", "decode_blob_index", "dump_uncompressed_blobs"];
            case "idump":
                return ["", "hex", "key_hex", "value_hex", "from", "to", "max_keys", "count_only", "count_delim", "stats", "input_key_hex",
                    "decode_blob_index"];
            case "manifest_dump": noDbOpen = true; return ["", "verbose", "path", "hex", "json"];
            case "dump_wal": noDbOpen = true; return ["", "walfile", "db", "write_committed", "header", "print_value", "only_print_seqno_gaps"];
            case "list_column_families": noDbOpen = true; return [""];
            case "create_column_family":
            case "drop_column_family": return [NoCommonOptions, "db"];
            case "compact": return ["", "from", "to", "hex", "key_hex", "value_hex", "ttl"];
            case "checkconsistency": noDbOpen = true; return [""];
            case "repair": noDbOpen = true; return ["", "verbose"];
            case "approxsize": return ["", "hex", "key_hex", "value_hex", "from", "to"];
            case "reduce_levels": noDbOpen = true; return ["", "new_levels", "print_old_levels"];
            case "file_checksum_dump": noDbOpen = true; return ["", "path", "hex"];
            case "list_live_files_metadata": return ["", "sort_by_filename"];
            case "get_property": return [""];
            case "checkpoint": return ["", "checkpoint_dir"];
            default: return null;
        }
    }

    private void Init()
    {
        _dbPath = _options.GetValueOrDefault("db", "");
        if (_dbPath.Length > 1) _dbPath = _dbPath.TrimEnd('/');
        _cfName = _options.GetValueOrDefault("column_family", ColumnFamilyDescriptor.DefaultColumnFamilyName);
        bool hex = _flags.Contains("hex");
        _keyHex = hex || _flags.Contains("key_hex");
        _valueHex = hex || _flags.Contains("value_hex");
    }

    private bool Flag(string name) => _flags.Contains(name);

    private string? Opt(string name) => _options.TryGetValue(name, out string? v) ? v : null;

    private byte[] KeyArg(string s) => _keyHex ? ToolText.HexToBytes(s) : ToolText.ArgBytes(s);

    private byte[] ValueArg(string s) => _valueHex ? ToolText.HexToBytes(s) : ToolText.ArgBytes(s);

    private string Key(ReadOnlySpan<byte> k) => _keyHex ? ToolText.StringToHex(k) : ToolText.Raw(k);

    private string Val(ReadOnlySpan<byte> v) => _valueHex ? ToolText.StringToHex(v) : ToolText.Raw(v);

    private void W(string s) => _out.Write(s);

    private void RequireDb()
    {
        if (_dbPath.Length == 0) throw new ToolException("--db must be specified");
    }

    /// <summary>
    /// std::stoi of an option: leading white space, a sign and the longest run of digits (trailing text is ignored); no digits is
    /// "has an invalid value", overflow "has a value out-of-range". LDBCommand::ParseIntOption ends both messages with a dot
    /// (<paramref name="dot"/>), the scan / dump constructors do not.
    /// </summary>
    private int IntOpt(string name, int def, bool dot)
    {
        string? v = Opt(name);
        if (v is null) return def;
        int i = 0;
        while (i < v.Length && (v[i] == ' ' || (v[i] >= '\t' && v[i] <= '\r'))) i++;
        bool neg = false;
        if (i < v.Length && (v[i] == '+' || v[i] == '-')) neg = v[i++] == '-';
        int digits = i;
        long r = 0;
        bool overflow = false;
        for (; i < v.Length && v[i] >= '0' && v[i] <= '9'; i++)
        {
            if (!overflow) r = r * 10 + (v[i] - '0');
            if (r > (long)int.MaxValue + 1) overflow = true;
        }
        if (i == digits) throw new ToolException(name + " has an invalid value" + (dot ? "." : ""));
        if (neg) r = -r;
        if (overflow || r > int.MaxValue || r < int.MinValue) throw new ToolException(name + " has a value out-of-range" + (dot ? "." : ""));
        return (int)r;
    }

    // ---- DB open (LDBCommand::OpenDB / PrepareOptions) ------------------------------------------------------------

    private ColumnFamilyOptions CfOptions(OptionsFile? file, string name)
    {
        ColumnFamilyOptions o = file?.FindColumnFamily(name) ?? new ColumnFamilyOptions();
        // "If merge operator is not set, set a string append operator."
        o.MergeOperator ??= MergeOperator.StringAppend(":");
        return o;
    }

    private RocksDatabase OpenDb(bool readOnly, bool createIfMissing = false)
    {
        RequireDb();
        OptionsFile? file = null;
        try
        {
            if (OptionsFile.FindLatestOptionsFile(_fs, _dbPath) is { } latest) file = OptionsFile.Load(_fs, latest, preserveUnknownOptions: true);
        }
        catch (Exception e) when (e is RocksDbException or IOException or FormatException)
        {
            file = null;
        }
        var descriptors = new List<ColumnFamilyDescriptor>();
        try
        {
            foreach (string name in RocksDatabase.ListColumnFamilies(_dbPath, _fs)) descriptors.Add(new ColumnFamilyDescriptor(name, CfOptions(file, name)));
        }
        catch (Exception e) when (e is RocksDbException or IOException)
        {
            // The DB may not exist yet ("create if not existing" case); Open reports the real problem.
        }
        if (descriptors.Count == 0) descriptors.Add(new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, CfOptions(file, "default")));
        var o = new DbOptions { FileSystem = _fs, CreateIfMissing = createIfMissing };
        _db = readOnly ? RocksDatabase.OpenReadOnly(_dbPath, o, descriptors, errorIfWalFileExists: false) : RocksDatabase.Open(_dbPath, o, descriptors);
        if (!_db.TryGetColumnFamily(_cfName, out _)) throw new ToolException("Non-existing column family " + _cfName);
        return _db;
    }

    private ColumnFamily Cf(RocksDatabase db) => db.GetColumnFamily(_cfName);

    // ---- commands ----------------------------------------------------------------------------------------------

    private bool Execute()
    {
        switch (_command)
        {
            case "get": Get(); return true;
            case "multi_get": MultiGet(); return true;
            case "put": Put(); return true;
            case "batchput": BatchPut(); return true;
            case "delete": Delete(single: false); return true;
            case "singledelete": Delete(single: true); return true;
            case "deleterange": DeleteRange(); return true;
            case "scan": Scan(); return true;
            case "dump": Dump(); return true;
            case "idump": InternalDump(); return true;
            case "manifest_dump": ManifestDump(); return true;
            case "dump_wal": DumpWal(); return true;
            case "list_column_families": ListColumnFamilies(); return true;
            case "create_column_family": CreateColumnFamily(); return true;
            case "drop_column_family": DropColumnFamily(); return true;
            case "compact": Compact(); return true;
            case "checkconsistency": CheckConsistency(); return true;
            case "repair": Repair(); return true;
            case "approxsize": ApproxSize(); return true;
            case "reduce_levels": ReduceLevels(); return true;
            case "file_checksum_dump": FileChecksumDump(); return true;
            case "list_live_files_metadata": ListLiveFilesMetadata(); return true;
            case "get_property": GetPropertyCmd(); return true;
            case "checkpoint": Checkpoint(); return true;
            default: return false;
        }
    }

    private void Get()
    {
        if (_params.Count != 1) throw new ToolException("<key> must be specified for the get command");
        byte[] key = KeyArg(_params[0]);
        RocksDatabase db = OpenDb(readOnly: true);
        byte[]? v;
        try
        {
            v = Cf(db).Get(key);
        }
        catch (RocksDbException e)
        {
            throw new ToolException("Get failed: " + e.Message);
        }
        W(v is null ? "Key not found\n" : Val(v) + "\n");
    }

    private void MultiGet()
    {
        if (_params.Count < 1) throw new ToolException("At least one <key> must be specified for multi_get.");
        var keys = new List<byte[]>();
        foreach (string p in _params) keys.Add(KeyArg(p));
        RocksDatabase db = OpenDb(readOnly: true);
        byte[]?[] values = Cf(db).MultiGet(keys);
        for (int i = 0; i < keys.Count; i++)
        {
            if (values[i] is { } v) W(Key(keys[i]) + Delim + Val(v) + "\n");
            else W("Key not found: " + Key(keys[i]) + "\n");
        }
    }

    private void Put()
    {
        if (_params.Count != 2) throw new ToolException("<key> and <value> must be specified for the put command");
        byte[] key = KeyArg(_params[0]), value = ValueArg(_params[1]);
        RocksDatabase db = OpenDb(readOnly: false, createIfMissing: Flag("create_if_missing"));
        Cf(db).Put(key, value);
        W("OK\n");
    }

    private void BatchPut()
    {
        if (_params.Count < 2) throw new ToolException("At least one <key> <value> pair must be specified batchput.");
        if (_params.Count % 2 != 0) throw new ToolException("Equal number of <key>s and <value>s must be specified for batchput.");
        RocksDatabase db = OpenDb(readOnly: false, createIfMissing: Flag("create_if_missing"));
        ColumnFamily cf = Cf(db);
        using WriteBatch batch = db.CreateWriteBatch();
        for (int i = 0; i < _params.Count; i += 2) batch.Put(cf, KeyArg(_params[i]), ValueArg(_params[i + 1]));
        db.Write(batch);
        W("OK\n");
    }

    private void Delete(bool single)
    {
        if (_params.Count != 1) throw new ToolException(single ? "KEY must be specified for the single delete command" : "KEY must be specified for the delete command");
        byte[] key = KeyArg(_params[0]);
        RocksDatabase db = OpenDb(readOnly: false);
        if (single) Cf(db).SingleDelete(key);
        else Cf(db).Delete(key);
        W("OK\n");
    }

    private void DeleteRange()
    {
        if (_params.Count != 2) throw new ToolException("begin and end keys must be specified for the delete command");
        byte[] begin = KeyArg(_params[0]), end = KeyArg(_params[1]);
        RocksDatabase db = OpenDb(readOnly: false);
        Cf(db).DeleteRange(begin, end);
        W("OK\n");
    }

    private string KeyValueLine(Iterator it)
    {
        // PrintKeyValueOrWideColumns: entities other than a plain default column print their columns
        // (WideColumnsHelper::DumpWideColumns honors value_hex itself, so the dump is not hexed again).
        if (it.IsEntity)
        {
            WideColumns columns = it.Columns;
            if (!(columns.Count == 1 && columns.HasDefaultColumn))
            {
                return Key(it.Key) + Delim + SstFileDumper.WideColumnsToString(columns.Serialized, _valueHex);
            }
        }
        return Key(it.Key) + Delim + Val(it.Value);
    }

    private void Scan()
    {
        byte[]? from = Opt("from") is { } f ? KeyArg(f) : null;
        byte[]? to = Opt("to") is { } t ? KeyArg(t) : null;
        int maxKeys = IntOpt("max_keys", -1, dot: false);
        bool noValue = Flag("no_value");
        RocksDatabase db = OpenDb(readOnly: true);
        using Iterator it = Cf(db).NewIterator(new ReadOptions { TotalOrderSeek = true });
        if (from is not null) it.Seek(from);
        else it.SeekToFirst();
        int scanned = 0;
        for (; it.Valid && (to is null || it.Key.SequenceCompareTo(to) < 0); it.Next())
        {
            W(noValue ? Key(it.Key) + "\n" : KeyValueLine(it) + "\n");
            scanned++;
            if (maxKeys >= 0 && scanned >= maxKeys) break;
        }
    }

    private void Dump()
    {
        bool countOnly = Flag("count_only");
        bool countDelim = Opt("count_delim") is not null || Flag("count_delim");
        string delim = Opt("count_delim") ?? ".";
        if (Opt("path") is { } path)
        {
            // DBDumperCommand on a single file: dispatch on its type.
            string name = Path.GetFileName(path);
            if (!FileNames.TryParseFileName(name, out _, out FileType type)) throw new ToolException("Can't parse file type: " + path);
            switch (type)
            {
                case FileType.WalFile: DumpWalFiles(path, printHeader: true, printValues: true, onlySeqnoGaps: false); return;
                case FileType.TableFile: DumpSstFile(path, _keyHex, showProperties: true, Opt("from") is { } sf ? KeyArg(sf) : [], Opt("to") is { } st ? KeyArg(st) : []); return;
                case FileType.DescriptorFile: DumpManifestFile(path, verbose: false, hex: _keyHex); return;
                default: throw new ToolException("File type not supported: " + path);
            }
        }
        byte[]? from = Opt("from") is { } f ? KeyArg(f) : null;
        byte[]? to = Opt("to") is { } t ? KeyArg(t) : null;
        int maxKeys = IntOpt("max_keys", -1, dot: false);
        RocksDatabase db = OpenDb(readOnly: true);
        ColumnFamily cf = Cf(db);
        if (Flag("stats") && db.GetProperty("rocksdb.stats") is { } stats) W(stats + "\n");
        using Iterator it = cf.NewIterator(new ReadOptions { TotalOrderSeek = true });
        Comparator dumpUcmp = cf.Data.UserComparator;
        if (from is not null) it.Seek(from);
        else it.SeekToFirst();
        ulong count = 0, c = 0, s2 = 0;
        string rtype2 = "";
        var sizes = new List<ulong>();
        for (; it.Valid; it.Next())
        {
            // DoDumpCommand: "ucmp->Compare(iter->key(), to_) >= 0" (the scan command compares the raw strings).
            if (to is not null && dumpUcmp.Compare(it.Key, to) >= 0) break;
            if (maxKeys == 0) break;
            if (maxKeys > 0) --maxKeys;
            ++count;
            if (countDelim)
            {
                string row = ToolText.Raw(it.Key);
                ulong s1 = (ulong)(it.Key.Length + it.Value.Length);
                int end = row.IndexOf(delim[0], StringComparison.Ordinal);
                string rtype1 = end < 0 ? row : row[..end];
                int nul = rtype1.IndexOf('\0', StringComparison.Ordinal);
                if (nul >= 0) rtype1 = rtype1[..nul];
                if (rtype2.Length != 0 && rtype2 != rtype1)
                {
                    W(rtype2 + " => count:" + c.ToString(CultureInfo.InvariantCulture) + "\tsize:" + s2.ToString(CultureInfo.InvariantCulture) + "\n");
                    c = 1;
                    s2 = s1;
                }
                else
                {
                    c++;
                    s2 += s1;
                }
                rtype2 = rtype1;
            }
            if (countOnly) sizes.Add((ulong)it.Value.Length);
            if (!countOnly && !countDelim) W(KeyValueLine(it) + "\n");
        }
        if (countDelim) W(rtype2 + " => count:" + c.ToString(CultureInfo.InvariantCulture) + "\tsize:" + s2.ToString(CultureInfo.InvariantCulture) + "\n");
        else W("Keys in range: " + count.ToString(CultureInfo.InvariantCulture) + "\n");
        if (countOnly)
        {
            // Deviation: the value size histogram is summarized (count / min / max / average) rather than HistogramImpl::ToString.
            ulong min = sizes.Count == 0 ? 0 : ulong.MaxValue, max = 0, sum = 0;
            foreach (ulong s in sizes) { min = Math.Min(min, s); max = Math.Max(max, s); sum += s; }
            W("Value size distribution: \n");
            W("Count: " + sizes.Count.ToString(CultureInfo.InvariantCulture) + " Average: " + (sizes.Count == 0 ? 0 : (double)sum / sizes.Count).ToString("F4", CultureInfo.InvariantCulture)
                + "  StdDev: 0.00\nMin: " + min.ToString(CultureInfo.InvariantCulture) + "  Median: 0.0000  Max: " + max.ToString(CultureInfo.InvariantCulture) + "\n");
        }
    }

    /// <summary>InternalDumpCommand (GetAllKeyVersions over the internal iterator, utilities/debug.cc).</summary>
    private void InternalDump()
    {
        bool inputKeyHex = Flag("input_key_hex");
        byte[]? from = Opt("from") is { } f ? (inputKeyHex ? ToolText.HexToBytes(f) : ToolText.ArgBytes(f)) : null;
        byte[]? to = Opt("to") is { } t ? (inputKeyHex ? ToolText.HexToBytes(t) : ToolText.ArgBytes(t)) : null;
        int maxKeys = IntOpt("max_keys", -1, dot: true);
        bool countOnly = Flag("count_only");
        bool countDelim = Opt("count_delim") is not null || Flag("count_delim");
        string delim = Opt("count_delim") ?? ".";
        RocksDatabase db = OpenDb(readOnly: true);
        ColumnFamily cf = Cf(db);
        if (Flag("stats") && cf.GetProperty("rocksdb.stats") is { } stats) W(stats + "\n");
        Comparator ucmp = cf.Data.UserComparator;
        DbIteratorSources sources = cf.Db.NewIteratorSources(cf.Data, null);
        long count = 0;
        ulong c = 0, s2 = 0;
        string rtype2 = "";
        try
        {
            using InternalIterator it = DbIteratorFactory.NewInternalIterator(sources, null);
            if (from is not null)
            {
                byte[] seek = new byte[from.Length + 8];
                from.CopyTo(seek, 0);
                Util.Coding.EncodeFixed64(seek.AsSpan(from.Length), DbFormat.PackSequenceAndType(DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek));
                it.Seek(seek);
            }
            else
            {
                it.SeekToFirst();
            }
            for (; it.Valid; it.Next())
            {
                ReadOnlySpan<byte> ikey = it.Key;
                if (!DbFormat.TryParseInternalKey(ikey, out ParsedInternalKey p)) throw new ToolException("Corruption: corrupted internal key");
                ReadOnlySpan<byte> uk = ikey[..p.UserKeyLength];
                // GetAllKeyVersions stops past the end key; the command stops at a key equal to it.
                if (to is not null && ucmp.Compare(uk, to) > 0) break;
                if (to is not null && ucmp.Compare(uk, to) == 0) break;
                ++count;
                if (countDelim)
                {
                    // InternalDumpCommand::DoCommand reads the encoded internal key and the value as C strings that also end
                    // at 0x01: the size is the two prefixes' lengths, the group the key up to the delimiter.
                    ReadOnlySpan<byte> val = it.Value;
                    ulong s1 = 0;
                    for (int k = 0; k < ikey.Length && ikey[k] != 1 && ikey[k] != 0; k++) s1++;
                    for (int k = 0; k < val.Length && val[k] != 1 && val[k] != 0; k++) s1++;
                    int j = 0;
                    byte d = delim.Length == 0 ? (byte)0 : (byte)delim[0];
                    while (j < ikey.Length && ikey[j] != d && ikey[j] != 0 && ikey[j] != 1) j++;
                    string rtype1 = ToolText.Raw(ikey[..j]);
                    if (rtype2.Length != 0 && rtype2 != rtype1)
                    {
                        W(rtype2 + " => count:" + c.ToString(CultureInfo.InvariantCulture) + "\tsize:" + s2.ToString(CultureInfo.InvariantCulture) + "\n");
                        c = 1;
                        s2 = s1;
                    }
                    else
                    {
                        c++;
                        s2 += s1;
                    }
                    rtype2 = rtype1;
                }
                if (!countOnly && !countDelim)
                {
                    string keyText = ToolText.InternalKeyDebugString(ikey, _keyHex);
                    if (p.Type == EntryType.WideColumnEntity)
                    {
                        if (SstFileDumper.TryWideColumnsToString(it.Value, _valueHex, out string wide)) W(keyText + " => " + wide + "\n");
                        else _err.Write(keyText + " => error deserializing wide columns\n");
                    }
                    else
                    {
                        W(keyText + " => " + ToolText.ToString(it.Value, _valueHex) + "\n");
                    }
                }
                // GetAllKeyVersions stops at "++num_keys >= max_num_ikeys" after adding a key: --max_keys=0 yields one key.
                if (maxKeys >= 0 && count >= Math.Max(maxKeys, 1)) break;
            }
        }
        finally
        {
            sources.Cleanup?.Dispose();
        }
        if (countDelim) W(rtype2 + " => count:" + c.ToString(CultureInfo.InvariantCulture) + "\tsize:" + s2.ToString(CultureInfo.InvariantCulture) + "\n");
        else W("Internal keys in range: " + count.ToString(CultureInfo.InvariantCulture) + "\n");
    }

    private void ListColumnFamilies()
    {
        RequireDb();
        List<string> cfs;
        try
        {
            cfs = RocksDatabase.ListColumnFamilies(_dbPath, _fs);
        }
        catch (Exception e) when (e is RocksDbException or IOException)
        {
            _err.Write("Error in processing db " + _dbPath + " " + e.Message + "\n");
            return;
        }
        W("Column families in " + _dbPath + ": \n{" + string.Join(", ", cfs) + "}\n");
    }

    private void CreateColumnFamily()
    {
        if (_params.Count != 1) throw new ToolException("new column family name must be specified");
        RocksDatabase db = OpenDb(readOnly: false);
        try
        {
            db.CreateColumnFamily(_params[0], new ColumnFamilyOptions());
        }
        catch (RocksDbException e)
        {
            throw new ToolException("Fail to create new column family: " + e.Message);
        }
        W("OK\n");
    }

    private void DropColumnFamily()
    {
        if (_params.Count != 1) throw new ToolException("The name of column family to drop must be specified");
        RocksDatabase db = OpenDb(readOnly: false);
        if (!db.TryGetColumnFamily(_params[0], out ColumnFamily? cf)) throw new ToolException("Column family: " + _params[0] + " doesn't exist in db.");
        try
        {
            db.DropColumnFamily(cf!);
        }
        catch (RocksDbException e)
        {
            throw new ToolException("Fail to drop column family: " + e.Message);
        }
        W("OK\n");
    }

    private void Compact()
    {
        byte[]? from = Opt("from") is { } f ? KeyArg(f) : null;
        byte[]? to = Opt("to") is { } t ? KeyArg(t) : null;
        RocksDatabase db = OpenDb(readOnly: false);
        try
        {
            Cf(db).CompactRange(new CompactRangeOptions { BottommostLevelCompaction = BottommostLevelCompaction.ForceOptimized }, from, to);
        }
        catch (RocksDbException e)
        {
            throw new ToolException("Compaction failed: " + e.Message);
        }
    }

    private void CheckConsistency()
    {
        // paranoid_checks: the DB opens (MANIFEST replay) and every live table file has the size the MANIFEST records
        // (DBImpl::CheckConsistency).
        RocksDatabase db = OpenDb(readOnly: true);
        foreach (LiveFileMetadata f in db.GetLiveFilesMetadata())
        {
            string path = NormalizePath(f.Directory + "/" + f.Name);
            long size;
            try
            {
                size = _fs.GetFileSize(path);
            }
            catch (Exception e) when (e is RocksDbException or IOException)
            {
                throw new ToolException("Corruption: Can't access " + f.Name + ": " + e.Message);
            }
            if ((ulong)size != f.Size)
            {
                throw new ToolException("Corruption: Sst file size mismatch: " + path + ". Size recorded in manifest " + f.Size.ToString(CultureInfo.InvariantCulture)
                    + ", actual size " + size.ToString(CultureInfo.InvariantCulture) + "\n");
            }
        }
        W("OK\n");
    }

    private void Repair()
    {
        RequireDb();
        RocksDatabase.Repair(_dbPath, new DbOptions { FileSystem = _fs });
        W("OK\n");
    }

    private void ApproxSize()
    {
        string from = Opt("from") ?? throw new ToolException("from must be specified for approxsize command");
        string to = Opt("to") ?? throw new ToolException("to must be specified for approxsize command");
        byte[] start = KeyArg(from), end = KeyArg(to);
        RocksDatabase db = OpenDb(readOnly: true);
        ulong[] sizes;
        try
        {
            sizes = Cf(db).GetApproximateSizes([new KeyRange(start, end)]);
        }
        catch (RocksDbException e)
        {
            throw new ToolException("ApproximateSize failed: " + e.Message);
        }
        W(sizes[0].ToString(CultureInfo.InvariantCulture) + "\n");
    }

    /// <summary>
    /// ReduceDBLevelsCommand: compacts the DB, then VersionSet::ReduceNumberOfLevels rewrites the MANIFEST with the files
    /// of the only non-empty level at or below new_levels-1 moved to level new_levels-1 (default column family, as upstream).
    /// The DB must then be opened with num_levels = new_levels (or more).
    /// </summary>
    private void ReduceLevels()
    {
        int newLevels = IntOpt("new_levels", -1, dot: true);
        // The constructor rejects new_levels <= 0 (also when the option is missing) before DoCommand's "<= 1" check.
        if (newLevels <= 0) throw new ToolException(" Use --new_levels to specify a new level number\n");
        if (newLevels <= 1) throw new ToolException("Invalid number of levels.\n");
        RequireDb();
        RecoveredVersionState state = ManifestReader.RecoverFromDirectory(_fs, _dbPath, null, out _);
        int oldLevels = UsedLevels(state);
        if (Flag("print_old_levels")) W("The old number of levels in use is " + oldLevels.ToString(CultureInfo.InvariantCulture) + "\n");
        if (oldLevels <= newLevels) return;
        RocksDatabase db = OpenDb(readOnly: false);
        W("Compacting the db...\n");
        Cf(db).CompactRange(new CompactRangeOptions(), null, null);
        db.Dispose();
        _db = null;

        state = ManifestReader.RecoverFromDirectory(_fs, _dbPath, null, out _);
        ColumnFamilyVersion def = state.FindColumnFamily(ColumnFamilyDescriptor.DefaultColumnFamilyName)!;
        int first = -1;
        for (int l = newLevels - 1; l < def.NumLevels; l++)
        {
            if (def.Levels[l].Count == 0) continue;
            if (first > 0)
            {
                throw new ToolException("Invalid argument: Found at least two levels containing files: [" + first.ToString(CultureInfo.InvariantCulture) + ":"
                    + l.ToString(CultureInfo.InvariantCulture) + "].\n");
            }
            first = l;
        }
        if (first > newLevels - 1)
        {
            List<FileMetaData> from = def.MutableLevel(first);
            List<FileMetaData> to = def.MutableLevel(newLevels - 1);
            foreach (FileMetaData f in from) def.FileLevels[f.FileNumber] = newLevels - 1;
            to.AddRange(from);
            from.Clear();
        }
        ulong manifestNumber = state.NextFileNumber;
        state.NextFileNumber = manifestNumber + 1;
        using (ManifestLogWriter log = ManifestLogWriter.Create(_fs, FileNames.DescriptorFileName(_dbPath, manifestNumber)))
        {
            new ManifestWriter(log).WriteSnapshotAsync(state, static _ => 0, CancellationToken.None).AsTask().GetAwaiter().GetResult();
        }
        FileNames.SetCurrentFileAsync(_fs, _dbPath, manifestNumber, syncDirectory: true, CancellationToken.None).AsTask().GetAwaiter().GetResult();
    }

    private static int UsedLevels(RecoveredVersionState state)
    {
        int max = -1;
        if (state.FindColumnFamily(ColumnFamilyDescriptor.DefaultColumnFamilyName) is { } def)
        {
            for (int l = 0; l < def.NumLevels; l++)
            {
                if (def.Levels[l].Count > 0) max = l;
            }
        }
        return max + 1;
    }

    private void FileChecksumDump()
    {
        RequireDb();
        bool hex = Flag("hex");
        RecoveredVersionState state;
        if (Opt("path") is { } manifest)
        {
            using ManifestLogReader reader = ManifestLogReader.Open(_fs, manifest);
            state = ManifestReader.Recover(reader, null);
        }
        else
        {
            state = ManifestReader.RecoverFromDirectory(_fs, _dbPath, null, out _);
        }
        var files = new List<FileMetaData>();
        foreach (ColumnFamilyVersion cf in state.ColumnFamilies.Values)
        {
            foreach ((_, FileMetaData f) in cf.AllFiles()) files.Add(f);
        }
        files.Sort(static (a, b) => a.FileNumber.CompareTo(b.FileNumber));
        foreach (FileMetaData f in files)
        {
            string checksum = hex ? ToolText.StringToHex(f.FileChecksum) : ToolText.Raw(f.FileChecksum);
            W(f.FileNumber.ToString(CultureInfo.InvariantCulture) + ", " + f.FileChecksumFuncName + ", " + checksum + "\n");
        }
        W("Print SST file checksum information finished \n");
    }

    private void ListLiveFilesMetadata()
    {
        RocksDatabase db = OpenDb(readOnly: true);
        List<LiveFileMetadata> files = db.GetLiveFilesMetadata();
        if (Flag("sort_by_filename"))
        {
            W("Live SST and Blob Files:\n");
            var all = new List<(string Name, int Level, string Cf)>();
            foreach (LiveFileMetadata f in files) all.Add((NormalizePath(f.Directory + "/" + f.Name), f.Level, f.ColumnFamilyName));
            all.Sort(static (a, b) =>
            {
                int c = string.CompareOrdinal(a.Name, b.Name);
                if (c == 0) c = a.Level.CompareTo(b.Level);
                return c != 0 ? c : string.CompareOrdinal(a.Cf, b.Cf);
            });
            foreach ((string name, int level, string cf) in all) W(name + " : level " + level.ToString(CultureInfo.InvariantCulture) + ", column family '" + cf + "'\n");
        }
        else
        {
            foreach (ColumnFamily cf in db.ColumnFamilies)
            {
                W("===== Column Family: " + cf.Name + " =====\n");
                W("Live SST Files:\n");
                for (int level = 0; level < cf.Options.NumLevels; level++)
                {
                    W("---------- level " + level.ToString(CultureInfo.InvariantCulture) + " ----------\n");
                    foreach (LiveFileMetadata f in files)
                    {
                        if (f.ColumnFamilyName == cf.Name && f.Level == level) W(NormalizePath(f.Directory + "/" + f.Name) + "\n");
                    }
                }
                W("Live Blob Files:\n");
            }
        }
        W("------------------------------\n");
    }

    private static string NormalizePath(string p)
    {
        var sb = new StringBuilder(p.Length);
        foreach (char ch in p)
        {
            if (ch == '/' && sb.Length > 0 && sb[^1] == '/') continue;
            sb.Append(ch);
        }
        return sb.ToString();
    }

    private void GetPropertyCmd()
    {
        if (_params.Count != 1) throw new ToolException("property name must be specified");
        RocksDatabase db = OpenDb(readOnly: true);
        // GetPropertyCommand::DoCommand tries the map property first.
        if (Cf(db).GetMapProperty(_params[0]) is { } map)
        {
            if (map.Count == 0)
            {
                W(_params[0] + ": <empty map>\n");
                return;
            }
            var names = new List<string>(map.Keys);
            names.Sort(StringComparer.Ordinal);
            foreach (string name in names) W(_params[0] + "." + name + ": " + map[name] + "\n");
            return;
        }
        string? v = Cf(db).GetProperty(_params[0]);
        if (v is null) throw new ToolException("failed to get property: " + _params[0]);
        W(_params[0] + ": " + v + "\n");
    }

    private void Checkpoint()
    {
        string dir = Opt("checkpoint_dir") ?? throw new ToolException("--checkpoint_dir must be specified");
        RocksDatabase db = OpenDb(readOnly: false);
        db.CreateCheckpointAsync(dir, 0).AsTask().GetAwaiter().GetResult();
        W("OK\n");
    }

    // ---- file dumps --------------------------------------------------------------------------------------------

    private void ManifestDump()
    {
        RequireDbOrPath();
        bool verbose = Flag("verbose");
        string manifest;
        if (Opt("path") is { } path)
        {
            manifest = path;
        }
        else
        {
            string? matched = null;
            foreach (string f in _fs.GetChildren(_dbPath))
            {
                if (FileNames.TryParseFileName(f, out _, out FileType t) && t == FileType.DescriptorFile)
                {
                    if (matched is not null) throw new ToolException("Multiple MANIFEST files found; use --path to select one");
                    matched = f;
                }
            }
            if (matched is null) throw new ToolException("No MANIFEST found in " + _dbPath);
            manifest = _dbPath + "/" + matched;
        }
        if (verbose) W("Processing Manifest file " + manifest + "\n");
        DumpManifestFile(manifest, verbose, _keyHex);
        if (verbose) W("Processing Manifest file " + manifest + " done\n");
    }

    private void RequireDbOrPath()
    {
        if (_dbPath.Length == 0 && Opt("path") is null) throw new ToolException("Either --db or --path must be specified.");
    }

    /// <summary>VersionSet::DumpManifest + DumpManifestHandler (db/version_set.cc, db/version_edit_handler.cc).</summary>
    private void DumpManifestFile(string manifest, bool verbose, bool hex)
    {
        var replayer = new ManifestReplayer(null);
        RecoveredVersionState state;
        try
        {
            using ManifestLogReader reader = ManifestLogReader.Open(_fs, manifest);
            while (reader.TryReadRecord(out ReadOnlySpan<byte> record))
            {
                VersionEdit edit = VersionEdit.DecodeFrom(record);
                if (verbose) W(EditDebugString(edit, hex));
                replayer.Apply(edit);
            }
            state = replayer.Finish();
        }
        catch (Exception e) when (e is RocksDbException or IOException)
        {
            // DumpManifestFile: the status goes to stderr and the command still succeeds.
            _err.Write("Error in processing file " + manifest + " " + (e is IOException ? "IO error: " : "") + e.Message + "\n");
            return;
        }
        int cfCount = state.ColumnFamilies.Count;
        int index = 0;
        foreach (ColumnFamilyVersion cf in state.ColumnFamilies.Values)
        {
            W("--------------- Column family \"" + cf.Name + "\"  (ID " + cf.Id.ToString(CultureInfo.InvariantCulture) + ") --------------\n");
            W("log number: " + cf.LogNumber.ToString(CultureInfo.InvariantCulture) + "\n");
            W("comparator: " + (cf.ComparatorName ?? "leveldb.BytewiseComparator") + "\n");
            // Version numbers: one per column family creation, then one per installed version (best effort).
            ulong versionNumber = (ulong)(cfCount + index);
            int levels = Math.Max(7, cf.NumLevels);
            var sb = new StringBuilder();
            for (int level = 0; level < levels; level++)
            {
                sb.Append("--- level ").Append(level).Append(" --- version# ").Append(versionNumber);
                if (cf.CompactCursors.TryGetValue(level, out byte[]? cursor)) sb.Append(" --- compact_cursor: ").Append(ToolText.InternalKeyDebugString(cursor, hex));
                sb.Append(" ---\n");
                if (level >= cf.NumLevels) continue;
                foreach (FileMetaData f in cf.Levels[level])
                {
                    sb.Append(' ').Append(f.FileNumber).Append(':').Append(f.FileSize).Append('[').Append(f.SmallestSeqno).Append(" .. ").Append(f.LargestSeqno)
                        .Append("][").Append(ToolText.InternalKeyDebugString(f.Smallest, hex)).Append(" .. ").Append(ToolText.InternalKeyDebugString(f.Largest, hex)).Append(']');
                    if (f.OldestBlobFileNumber != FileMetaData.InvalidBlobFileNumber) sb.Append(" blob_file:").Append(f.OldestBlobFileNumber);
                    sb.Append('\n');
                }
            }
            if (cf.BlobFiles.Count > 0)
            {
                sb.Append("--- blob files --- version# ").Append(versionNumber).Append(" ---\n");
                foreach ((ulong number, BlobFileState b) in cf.BlobFiles) sb.Append("blob_file_number: ").Append(number).Append(' ').Append(b).Append('\n');
            }
            W(sb.ToString());
            W("By default, manifest file dump prints LSM trees as if 7 levels were configured, which is not necessarily true for the column family (CF) "
                + "this manifest is associated with. Please consult other DB files, such as the OPTIONS file, to confirm.\n");
            index++;
        }
        W("next_file_number " + (state.NextFileNumber + 1).ToString(CultureInfo.InvariantCulture) + " last_sequence " + state.LastSequence.ToString(CultureInfo.InvariantCulture)
            + "  prev_log_number " + state.PrevLogNumber.ToString(CultureInfo.InvariantCulture) + " max_column_family " + state.MaxColumnFamily.ToString(CultureInfo.InvariantCulture)
            + " min_log_number_to_keep " + state.MinLogNumberToKeep.ToString(CultureInfo.InvariantCulture) + "\n");
    }

    /// <summary>VersionEdit::DebugString(hex_key) (db/version_edit.cc).</summary>
    internal static string EditDebugString(VersionEdit e, bool hex)
    {
        var r = new StringBuilder("VersionEdit {");
        if (e.DbId is not null) r.Append("\n  DB ID: ").Append(e.DbId);
        if (e.ComparatorName is not null) r.Append("\n  Comparator: ").Append(e.ComparatorName);
        if (e.PersistUserDefinedTimestamps is bool p) r.Append("\n  PersistUserDefinedTimestamps: ").Append(p ? "true" : "false");
        if (e.LogNumber is ulong ln) r.Append("\n  LogNumber: ").Append(ln);
        if (e.PrevLogNumber is ulong pln) r.Append("\n  PrevLogNumber: ").Append(pln);
        if (e.NextFileNumber is ulong nf) r.Append("\n  NextFileNumber: ").Append(nf);
        if (e.MaxColumnFamily is uint mcf) r.Append("\n  MaxColumnFamily: ").Append(mcf);
        if (e.MinLogNumberToKeep is ulong ml) r.Append("\n  MinLogNumberToKeep: ").Append(ml);
        if (e.LastSequence is ulong ls) r.Append("\n  LastSeq: ").Append(ls);
        foreach (CompactCursor c in e.CompactCursors) r.Append("\n  CompactCursor: ").Append(c.Level).Append(' ').Append(ToolText.InternalKeyDebugString(c.InternalKey, hex));
        foreach ((int level, ulong number) in e.DeletedFiles) r.Append("\n  DeleteFile: ").Append(level).Append(' ').Append(number);
        foreach (NewFileEntry nfe in e.NewFiles)
        {
            FileMetaData f = nfe.Meta;
            r.Append("\n  AddFile: ").Append(nfe.Level).Append(' ').Append(f.FileNumber).Append(' ').Append(f.FileSize).Append(' ')
                .Append(ToolText.InternalKeyDebugString(f.Smallest, hex)).Append(" .. ").Append(ToolText.InternalKeyDebugString(f.Largest, hex));
            if (f.OldestBlobFileNumber != FileMetaData.InvalidBlobFileNumber) r.Append(" blob_file:").Append(f.OldestBlobFileNumber);
            r.Append(" oldest_ancester_time:").Append(f.OldestAncestorTime);
            r.Append(" file_creation_time:").Append(f.FileCreationTime);
            r.Append(" epoch_number:").Append(f.EpochNumber);
            r.Append(" file_checksum:").Append(Convert.ToHexString(f.FileChecksum));
            r.Append(" file_checksum_func_name: ").Append(f.FileChecksumFuncName);
            if (f.Temperature != Temperature.Unknown) r.Append(" temperature: ").Append((int)f.Temperature);
            if (!f.UniqueId.IsNull)
            {
                r.Append(" unique_id(internal): {").Append(f.UniqueId.Lo).Append(',').Append(f.UniqueId.Hi).Append('}');
                const ulong HiOffsetForZero = 17391078804906429400UL;
                const ulong LoOffsetForZero = 6417269962128484497UL;
                Util.Hash.BijectiveHash2x64(unchecked(f.UniqueId.Hi + HiOffsetForZero), unchecked(f.UniqueId.Lo + LoOffsetForZero), out ulong hi, out ulong lo);
                byte[] b = new byte[16];
                Util.Coding.EncodeFixed64(b, lo);
                Util.Coding.EncodeFixed64(b[8..], hi);
                string hx = Convert.ToHexString(b);
                r.Append(" public_unique_id: ").Append(hx[..16]).Append('-').Append(hx[16..]);
            }
            r.Append(" tail size: ").Append(f.TailSize);
            r.Append(" User-defined timestamps persisted: ").Append(f.UserDefinedTimestampsPersisted ? "true" : "false");
            if (f.FileOpenMetadata.Length > 0) r.Append(" file_open_metadata_size: ").Append(f.FileOpenMetadata.Length);
        }
        foreach (BlobFileAddition a in e.BlobFileAdditions) r.Append("\n  BlobFileAddition: ").Append(a);
        foreach (BlobFileGarbage g in e.BlobFileGarbages) r.Append("\n  BlobFileGarbage: ").Append(g);
        foreach (WalAddition w in e.WalAdditions) r.Append("\n  WalAddition: ").Append(w);
        if (e.IsWalDeletion) r.Append("\n  WalDeletion: log_number: ").Append(e.WalDeletionBefore);
        r.Append("\n  ColumnFamily: ").Append(e.ColumnFamily);
        if (e.ColumnFamilyAddName is not null) r.Append("\n  ColumnFamilyAdd: ").Append(e.ColumnFamilyAddName);
        if (e.IsColumnFamilyDrop) r.Append("\n  ColumnFamilyDrop");
        if (e.IsInAtomicGroup) r.Append("\n  AtomicGroup: ").Append(e.RemainingEntries).Append(" entries remains");
        if (e.FullHistoryTsLow is { Length: > 0 } ts) r.Append("\n FullHistoryTsLow: ").Append(ToolText.ToString(ts, hex));
        r.Append("\n}\n");
        return r.ToString();
    }

    private void DumpWal()
    {
        string wal = Opt("walfile") ?? throw new ToolException("Argument walfile must be specified.");
        DumpWalFiles(wal, Flag("header"), Flag("print_value"), Flag("only_print_seqno_gaps"));
    }

    /// <summary>DumpWalFiles (tools/ldb_cmd.cc): a WAL file, or every WAL of a directory in number order.</summary>
    private void DumpWalFiles(string dirOrFile, bool printHeader, bool printValues, bool onlySeqnoGaps)
    {
        ulong? prevSeq = null;
        uint? prevCount = null;
        bool isDir = false;
        try { isDir = _fs.DirectoryExists(dirOrFile); }
        catch (RocksDbException) { isDir = Directory.Exists(dirOrFile); }
        // DumpWalFiles: "!st.ok() || filenames.empty()" -- an empty directory is handled like a file name.
        bool emptyDir = isDir && !HasChildren(dirOrFile);
        if (!isDir || emptyDir)
        {
            if (!emptyDir && !_fs.FileExists(dirOrFile)) throw new ToolException(dirOrFile + ": No such file or directory");
            if (dirOrFile.Length <= 4 || !dirOrFile.EndsWith(".log", StringComparison.Ordinal)) throw new ToolException(dirOrFile + ": Invalid log file name");
            DumpWalFile(dirOrFile, printHeader, printValues, onlySeqnoGaps, ref prevSeq, ref prevCount);
            return;
        }
        var wals = new List<(ulong Number, string Name)>();
        foreach (string f in _fs.GetChildren(dirOrFile))
        {
            if (FileNames.TryParseFileName(f, out ulong n, out FileType t) && t == FileType.WalFile) wals.Add((n, f));
        }
        if (wals.Count == 0) throw new ToolException(dirOrFile + ": No valid wal logs found");
        wals.Sort();
        foreach ((_, string name) in wals)
        {
            string path = dirOrFile.TrimEnd('/') + "/" + name;
            W("Checking wal file: " + path + "\n");
            DumpWalFile(path, printHeader, printValues, onlySeqnoGaps, ref prevSeq, ref prevCount);
        }
    }

    private bool HasChildren(string dir)
    {
        foreach (string _ in _fs.GetChildren(dir)) return true;
        return false;
    }

    private sealed class StdErrReporter(TextWriter err) : LogReporter
    {
        public override void Corruption(long bytes, StatusCode code, string reason, ulong logNumber) =>
            err.Write("Corruption detected in log file " + (code == StatusCode.IOError ? "IO error: " : "Corruption: ") + reason + "\n");
    }

    private void DumpWalFile(string walFile, bool printHeader, bool printValues, bool onlySeqnoGaps, ref ulong? prevSeq, ref uint? prevCount)
    {
        ISequentialFile file;
        try
        {
            file = _fs.OpenSequential(walFile);
        }
        catch (Exception e) when (e is RocksDbException or IOException)
        {
            throw new ToolException("Failed to open WAL file " + e.Message);
        }
        string name = Path.GetFileName(walFile);
        if (!FileNames.TryParseFileName(name, out ulong logNumber, out _)) logNumber = 0;
        using var reader = new LogReader(file, new StdErrReporter(_err), checksum: true, logNumber);
        var encountered = new SortedSet<uint>();
        if (printHeader) W("Sequence,Count,ByteSize,Physical Offset,Key(s)" + (printValues ? " : value " : "") + "\n");
        while (reader.TryReadRecord(out ReadOnlySpan<byte> record))
        {
            if (record.Length < WriteBatch.HeaderSize)
            {
                _err.Write("Corruption detected in log file Corruption: log record too small\n");
                continue;
            }
            var row = new StringBuilder();
            ulong seq = Util.Coding.DecodeFixed64(record);
            uint count = Util.Coding.DecodeFixed32(record[8..]);
            if (onlySeqnoGaps)
            {
                if (prevSeq is null || prevCount is null || prevSeq.Value + prevCount.Value == seq)
                {
                    prevSeq = seq;
                    prevCount = count;
                    continue;
                }
                row.Append("Prev batch sequence number: ").Append(prevSeq.Value).Append(", prev batch count: ").Append(prevCount.Value).Append(", ");
            }
            prevSeq = seq;
            prevCount = count;
            row.Append(seq).Append(',').Append(count).Append(',').Append(record.Length).Append(',').Append(reader.LastRecordOffset).Append(',');
            var handler = new WalRowHandler(row, printValues, encountered);
            try
            {
                WriteBatch.Iterate(record, ref handler);
            }
            catch (RocksDbException e)
            {
                // DumpWalFile breaks out of the record loop before the row is printed.
                throw new ToolException("Print write batch error: " + e.Message);
            }
            row.Append('\n');
            W(row.ToString());
        }
        if (_dbPath.Length > 0 && encountered.Count > 0)
        {
            // --db: the DB's column families are known (their comparators would format timestamped keys).
            try
            {
                foreach (uint id in ManifestReader.RecoverFromDirectory(_fs, _dbPath, null, out _).ColumnFamilies.Keys) encountered.Remove(id);
            }
            catch (Exception e) when (e is RocksDbException or IOException)
            {
                // The DB cannot be read: keep the default formatting note.
            }
        }
        if (encountered.Count > 0)
        {
            // No DB is opened to resolve the column families' comparators (always the case here: keys are printed in hex).
            W("(Column family id: [" + string.Join(",", encountered) + "] contained in WAL are not opened in DB. Applied default hex formatting for user key. "
                + "Specify --db=<db_path> to open DB for better user key formatting if it contains timestamp.)\n");
        }
    }

    /// <summary>InMemoryHandler of the WAL dumper (tools/ldb_cmd.cc).</summary>
    private struct WalRowHandler(StringBuilder row, bool printValues, SortedSet<uint> cfs) : IWriteBatchHandler
    {
        public readonly bool Continue() => true;

        private readonly void PutMerge(string op, uint cf, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            cfs.Add(cf);
            row.Append(op).Append('(').Append(cf).Append(") : ").Append(ToolText.StringToHex(key));
            if (printValues) row.Append(" : ").Append(ToolText.StringToHex(value)).Append(' ');
            else row.Append(' ');
        }

        public readonly void Put(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => PutMerge("PUT", columnFamilyId, key, value);
        public readonly void TimedPut(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ulong writeUnixTime)
        {
            // InMemoryHandler does not override TimedPutCF: WriteBatch::Handler's default fails the dump.
            throw RocksDbException.InvalidArgument("TimedPutCF not implemented");
        }

        public readonly void PutEntity(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> entity)
        {
            cfs.Add(columnFamilyId);
            row.Append("PUT_ENTITY(").Append(columnFamilyId).Append(") : ").Append(ToolText.StringToHex(key));
            if (printValues)
            {
                row.Append(" : ");
                if (!SstFileDumper.TryWideColumnsToString(entity, hex: true, out string wide)) throw RocksDbException.Corruption("Error while decoding wide column");
                row.Append(wide);
            }
            row.Append(' ');
        }

        public readonly void Delete(uint columnFamilyId, ReadOnlySpan<byte> key)
        {
            cfs.Add(columnFamilyId);
            row.Append("DELETE(").Append(columnFamilyId).Append(") : ").Append(ToolText.StringToHex(key)).Append(' ');
        }

        public readonly void SingleDelete(uint columnFamilyId, ReadOnlySpan<byte> key)
        {
            cfs.Add(columnFamilyId);
            row.Append("SINGLE_DELETE(").Append(columnFamilyId).Append(") : ").Append(ToolText.StringToHex(key)).Append(' ');
        }

        public readonly void DeleteRange(uint columnFamilyId, ReadOnlySpan<byte> beginKey, ReadOnlySpan<byte> endKey)
        {
            cfs.Add(columnFamilyId);
            row.Append("DELETE_RANGE(").Append(columnFamilyId).Append(") : ").Append(ToolText.StringToHex(beginKey)).Append(' ').Append(ToolText.StringToHex(endKey)).Append(' ');
        }

        public readonly void Merge(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => PutMerge("MERGE", columnFamilyId, key, value);

        public readonly void PutBlobIndex(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> blobIndex)
        {
            // WriteBatch::Handler::PutBlobIndexCF default: InvalidArgument("PutBlobIndexCF not implemented").
            throw RocksDbException.InvalidArgument("PutBlobIndexCF not implemented");
        }

        public readonly void LogData(ReadOnlySpan<byte> blob) { }
        public readonly void MarkBeginPrepare(bool unprepare) => row.Append("BEGIN_PREPARE(").Append(unprepare ? "true" : "false").Append(") ");
        public readonly void MarkEndPrepare(ReadOnlySpan<byte> xid) => row.Append("END_PREPARE(").Append(ToolText.StringToHex(xid)).Append(") ");
        public readonly void MarkNoop(bool emptyBatch) => row.Append("NOOP ");
        public readonly void MarkCommit(ReadOnlySpan<byte> xid) => row.Append("COMMIT(").Append(ToolText.StringToHex(xid)).Append(") ");
        public readonly void MarkCommitWithTimestamp(ReadOnlySpan<byte> xid, ReadOnlySpan<byte> commitTimestamp) =>
            row.Append("COMMIT_WITH_TIMESTAMP(").Append(ToolText.StringToHex(xid)).Append(", ").Append(ToolText.StringToHex(commitTimestamp)).Append(") ");
        public readonly void MarkRollback(ReadOnlySpan<byte> xid) => row.Append("ROLLBACK(").Append(ToolText.StringToHex(xid)).Append(") ");
    }

    /// <summary>DumpSstFile (tools/ldb_cmd.cc): a table file with its properties.</summary>
    private void DumpSstFile(string path, bool hex, bool showProperties, byte[] from, byte[] to)
    {
        if (path.Length <= 4 || !path.EndsWith(".sst", StringComparison.Ordinal))
        {
            W("Invalid sst file name.\n");
            return;
        }
        using var dumper = new SstFileDumper(_fs, path, verifyChecksum: false, hex, silent: false, _out);
        if (dumper.InitError is { } e)
        {
            _err.Write("Error in reading SST file " + path + e.Message + "\n");
            return;
        }
        string? err = dumper.ReadSequential(true, ulong.MaxValue, from.Length > 0, from, to.Length > 0, to, false);
        if (err is not null)
        {
            _err.Write("Error in reading SST file " + path + err + "\n");
            return;
        }
        if (showProperties) W("\nTable Properties:\n" + ToolText.TablePropertiesToString(dumper.Properties!, "\n", ": ") + "\n");
    }

    private static void PrintHelp(TextWriter o) => o.Write(
        "ldb - RocksDB Tool (managed port)\n\n" +
        "commands MUST specify --db=<full_path_to_db_directory> when necessary\n\n" +
        "The following optional parameters control if keys/values are input/output as hex or as plain strings:\n" +
        "  --key_hex : Keys are input/output as hex\n" +
        "  --value_hex : Values are input/output as hex\n" +
        "  --hex : Both keys and values are input/output as hex\n\n" +
        "The following optional parameters control the database internals:\n" +
        "  --column_family=<string> : name of the column family to operate on. default: default column family\n" +
        "  --create_if_missing : (put / batchput) create the DB if it does not exist\n\n" +
        "Data Access Commands:\n" +
        "  put <key> <value> [--create_if_missing]\n" +
        "  get <key>\n" +
        "  multi_get <key_1> <key_2> <key_3> ...\n" +
        "  batchput <key> <value> [<key> <value>] [..] [--create_if_missing]\n" +
        "  scan [--from] [--to] [--max_keys=<N>] [--no_value]\n" +
        "  delete <key>\n" +
        "  singledelete <key>\n" +
        "  deleterange <begin key> <end key>\n" +
        "  approxsize [--from] [--to]\n" +
        "  checkconsistency\n" +
        "  list_file_range_deletes (not ported)\n\n" +
        "Admin Commands:\n" +
        "  dump_wal --walfile=<write_ahead_log_file_path_or_directory> [--header] [--print_value] [--only_print_seqno_gaps]\n" +
        "  compact [--from] [--to]\n" +
        "  reduce_levels --new_levels=<New number of levels> [--print_old_levels]\n" +
        "  dump [--from] [--to] [--max_keys=<N>] [--count_only] [--count_delim=<char>] [--stats] [--path=<path_to_a_file>]\n" +
        "  manifest_dump [--verbose] [--path=<path_to_manifest_file>]\n" +
        "  file_checksum_dump [--path=<path_to_manifest_file>] [--hex]\n" +
        "  list_column_families\n" +
        "  create_column_family --db=<db_path> <new_column_family_name>\n" +
        "  drop_column_family --db=<db_path> <column_family_name_to_drop>\n" +
        "  idump [--from] [--to] [--input_key_hex] [--max_keys=<N>] [--count_only] [--stats]\n" +
        "  repair [--verbose]\n" +
        "  checkpoint [--checkpoint_dir]\n" +
        "  get_property <property_name>\n" +
        "  list_live_files_metadata [--sort_by_filename]\n");
}
