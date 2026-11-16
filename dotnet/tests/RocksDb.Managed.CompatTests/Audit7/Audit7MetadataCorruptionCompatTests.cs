using System.Runtime.InteropServices;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;

namespace RocksDb.Managed.CompatTests.Audit7;

/// <summary>
/// Audit #7, area "MetadataCorruption": native RocksDB (NuGet, in-process) and the managed port on the same damaged
/// metadata (OPTIONS files, CURRENT, MANIFEST, WAL).
/// </summary>
[TestClass]
[DoNotParallelize]
public sealed unsafe class Audit7MetadataCorruptionCompatTests
{
    private static readonly Lazy<IntPtr> s_library = new(LoadLibrary);

    private static IntPtr LoadLibrary()
    {
        _ = RocksDbSharp.Native.Instance;
        foreach (string line in File.ReadLines("/proc/self/maps"))
        {
            int slash = line.IndexOf('/', StringComparison.Ordinal);
            if (slash < 0) continue;
            string path = line[slash..];
            if (Path.GetFileName(path).StartsWith("librocksdb", StringComparison.Ordinal)) return NativeLibrary.Load(path);
        }
        throw new InvalidOperationException("librocksdb is not loaded");
    }

    private static IntPtr Export(string name) => NativeLibrary.GetExport(s_library.Value, name);

    /// <summary>
    /// rocksdb_load_latest_options (LoadLatestOptions -> RocksDBOptionsParser::Parse) on <paramref name="dir"/>: the
    /// error message, or null when native parsed the file.
    /// </summary>
    private static string? NativeLoadLatestOptions(string dir, bool ignoreUnknownOptions)
    {
        IntPtr env = RocksDbSharp.Native.Instance.rocksdb_create_default_env();
        IntPtr dbOptions = IntPtr.Zero, names = IntPtr.Zero, cfOptions = IntPtr.Zero, err = IntPtr.Zero;
        nuint numCfs = 0;
        byte[] path = Encoding.UTF8.GetBytes(dir + "\0");
        fixed (byte* p = path)
        {
            ((delegate* unmanaged<byte*, IntPtr, byte, IntPtr, IntPtr*, nuint*, IntPtr*, IntPtr*, IntPtr*, void>)Export("rocksdb_load_latest_options"))(
                p, env, ignoreUnknownOptions ? (byte)1 : (byte)0, IntPtr.Zero, &dbOptions, &numCfs, &names, &cfOptions, &err);
        }
        RocksDbSharp.Native.Instance.rocksdb_env_destroy(env);
        if (err != IntPtr.Zero)
        {
            string msg = Marshal.PtrToStringUTF8(err)!;
            RocksDbSharp.Native.Instance.rocksdb_free(err);
            return msg;
        }
        ((delegate* unmanaged<IntPtr, IntPtr, IntPtr, nuint, void>)Export("rocksdb_load_latest_options_destroy"))(dbOptions, names, cfOptions, numCfs);
        return null;
    }

    /// <summary>A minimal valid OPTIONS file with the given Version section lines.</summary>
    private static string OptionsText(string versionLines) =>
        "[Version]\n" + versionLines + "\n[DBOptions]\n  create_if_missing=false\n[CFOptions \"default\"]\n  num_levels=7\n";

    /// <summary>Managed RocksDBOptionsParser::Parse result: null, or "Code: message".</summary>
    private static string? ManagedParse(string text, bool preserveUnknown)
    {
        try
        {
            OptionsFile.Parse(text, preserveUnknown);
            return null;
        }
        catch (RocksDbException e)
        {
            return e.Code + ": " + e.Message;
        }
    }

    /// <summary>
    /// RocksDBOptionsParser::ParseVersionNumber (options/options_parser.cc) with a component that overflows an int:
    /// native accumulates "current_number * 10 + digit" in an int. The managed parser must give the same outcome and
    /// never throw a non-RocksDb exception.
    /// </summary>
    [TestMethod]
    [DataRow("  rocksdb_version=99999999999.0.0\n  options_file_version=1.1")]
    [DataRow("  rocksdb_version=11.14.0\n  options_file_version=99999999999.1")]
    [DataRow("  rocksdb_version=11.14.0\n  options_file_version=2147483648.1")]
    [DataRow("  rocksdb_version=11.14.0\n  options_file_version=4294967297.0")]
    [DataRow("  rocksdb_version=11.99999999999.0\n  options_file_version=1.1")]
    public void OptionsVersionNumberOverflowMatchesNative(string versionLines)
    {
        using var dir = new TempDir("a7opts");
        string text = OptionsText(versionLines);
        File.WriteAllText(Path.Combine(dir.Path, "OPTIONS-000005"), text);
        string? native = NativeLoadLatestOptions(dir.Path, ignoreUnknownOptions: false);
        string? managed = ManagedParse(text, preserveUnknown: false);
        Console.WriteLine("native:  " + (native ?? "OK"));
        Console.WriteLine("managed: " + (managed ?? "OK"));
        AssertSameOutcome(native, managed);
    }

    /// <summary>Both accept, or both reject with the same status code (native "Invalid argument: ..." / "Corruption: ...").</summary>
    private static void AssertSameOutcome(string? native, string? managed)
    {
        Assert.AreEqual(native is null, managed is null, "native: " + native + " managed: " + managed);
        if (native is null) return;
        string code = native.StartsWith("Invalid argument", StringComparison.Ordinal) ? "InvalidArgument"
            : native.StartsWith("Corruption", StringComparison.Ordinal) ? "Corruption"
            : native.StartsWith("Not implemented", StringComparison.Ordinal) ? "NotSupported"
            : native.StartsWith("IO error", StringComparison.Ordinal) ? "IOError" : native;
        StringAssert.StartsWith(managed!, code + ":", "native: " + native);
    }

    private const string Head = "[Version]\n  rocksdb_version=11.12.0\n  options_file_version=1.1\n[DBOptions]\n  max_open_files=-1\n";

    /// <summary>Writes <paramref name="text"/> as the only OPTIONS file of a directory and returns native's and managed's outcomes.</summary>
    private static (string? Native, string? Managed) BothParse(string text)
    {
        using var dir = new TempDir("a7opts");
        File.WriteAllText(Path.Combine(dir.Path, "OPTIONS-000005"), text);
        string? native = NativeLoadLatestOptions(dir.Path, ignoreUnknownOptions: false);
        string? managed = ManagedParse(text, preserveUnknown: false);
        Console.WriteLine("native:  " + (native ?? "OK"));
        Console.WriteLine("managed: " + (managed ?? "OK"));
        return (native, managed);
    }

    /// <summary>
    /// A statement or section the parser rejects (unknown option, bad value): both reject it with InvalidArgument.
    /// Pins RocksDBOptionsParser::EndSection -> GetDBOptionsFromMap / GetColumnFamilyOptionsFromMap /
    /// TableFactory::ConfigureFromMap ("Extra option not recognized", "Error parsing ...").
    /// </summary>
    [TestMethod]
    [DataRow("[CFOptions \"default\"]\n  foo_bar=1\n")]
    [DataRow("[CFOptions \"default\"]\n[TableOptions/BlockBasedTable \"default\"]\n  foo_bar=1\n")]
    [DataRow("[CFOptions \"default\"]\n[TableOptions/BlockBasedTable \"default\"]\n  block_size=abc\n")]
    [DataRow("[CFOptions \"default\"]\n[TableOptions/BlockBasedTable \"default\"]\n  checksum=kNoSuch\n")]
    [DataRow("[CFOptions \"default\"]\n  compression=kNoSuch\n")]
    [DataRow("[CFOptions \"default\"]\n  num_levels=99999999999\n")]
    [DataRow("[CFOptions \"default\"]\n[TableOptions/BlockBasedTable \"nocf\"]\n")]
    [DataRow("[CFOptions \"cf1\"]\n")]
    [DataRow("[CFOptions \"default\"]\n[CFOptions \"default\"]\n")]
    [DataRow("[CFOptions \"default\"]\n[NoSuchSection]\n")]
    [DataRow("[CFOptions \"default\"]\n  no equals sign\n")]
    [DataRow("[CFOptions \"default\"]\n  block_protection_bytes_per_key=256\n")]
    public void MalformedOptionsStatementsRejectedLikeNative(string tail)
    {
        (string? native, string? managed) = BothParse(Head + tail);
        Assert.IsNotNull(native);
        AssertSameOutcome(native, managed);
    }

    /// <summary>
    /// A Customizable factory id that is not registered (memtable_factory, table_factory, flush_block_policy_factory):
    /// creating it is NotSupported, which ConfigOptions::ignore_unsupported_options (default true, also in
    /// RocksDBOptionsParser::Parse) turns into OK with the option left unchanged (options/configurable.cc
    /// ConfigureOptions). The managed parser rejected these files with InvalidArgument.
    /// </summary>
    [TestMethod]
    [DataRow("[CFOptions \"default\"]\n  memtable_factory=NoSuchFactory\n")]
    [DataRow("[CFOptions \"default\"]\n  table_factory=NoSuchTable\n[TableOptions/BlockBasedTable \"default\"]\n  block_size=8192\n")]
    [DataRow("[CFOptions \"default\"]\n[TableOptions/BlockBasedTable \"default\"]\n  flush_block_policy_factory=NoSuchPolicyFactory\n")]
    [DataRow("[CFOptions \"default\"]\n  memtable_factory={\n")]
    [DataRow("[CFOptions \"default\"]\n  table_factory={\n[TableOptions/BlockBasedTable \"default\"]\n  block_size=8192\n")]
    [DataRow("[CFOptions \"default\"]\n[TableOptions/BlockBasedTable \"default\"]\n  flush_block_policy_factory={\n")]
    public void UnregisteredFactoryIdsIgnoredLikeNative(string tail)
    {
        (string? native, string? managed) = BothParse(Head + tail);
        Assert.IsNull(native);
        AssertSameOutcome(native, managed);
        OptionsFile f = OptionsFile.Parse(Head + tail, preserveUnknownOptions: false);
        Assert.AreEqual(new ColumnFamilyOptions().MemtableFactory, f.ColumnFamilies[0].Options.MemtableFactory);
    }

    /// <summary>
    /// LineFileReader::ReadLine (file/line_file_reader.cc) returns false at EOF for a last line without '\n', so an
    /// OPTIONS file torn in its last line is parsed without it. The managed parser read the partial line (a torn
    /// statement could fail the file or set a truncated value).
    /// </summary>
    [TestMethod]
    [DataRow("[CFOptions \"default\"]\n  num_levels=ab")]
    [DataRow("[CFOptions \"default\"]\n  foo_ba")]
    [DataRow("[CFOptions \"default\"]\n[TableOptions/BlockBasedTable \"nocf")]
    [DataRow("[CFOptions \"default\"]\n  num_levels=4")]
    public void TornLastLineIgnoredLikeNative(string tail)
    {
        (string? native, string? managed) = BothParse(Head + tail);
        Assert.IsNull(native);
        AssertSameOutcome(native, managed);
        OptionsFile f = OptionsFile.Parse(Head + tail, preserveUnknownOptions: false);
        Assert.AreEqual(new ColumnFamilyOptions().NumLevels, f.ColumnFamilies[0].Options.NumLevels);
    }

    /// <summary>
    /// read_amp_bytes_per_bit has a custom parse function upstream (block_based_table_factory.cc: ParseUint64, then a
    /// cast to uint32_t, kept for OPTIONS files of 6.10 - 6.14), so values beyond 32 bits and negative ones are accepted
    /// and truncated; the managed parser rejected them as out of range.
    /// </summary>
    [TestMethod]
    [DataRow("-1", 0xffffffffu)]
    [DataRow("4294967297", 1u)]
    [DataRow("9223372036854775808", 0u)]
    public void ReadAmpBytesPerBitTruncatedLikeNative(string value, uint expected)
    {
        string text = Head + "[CFOptions \"default\"]\n[TableOptions/BlockBasedTable \"default\"]\n  read_amp_bytes_per_bit=" + value + "\n";
        (string? native, string? managed) = BothParse(text);
        Assert.IsNull(native);
        AssertSameOutcome(native, managed);
        Assert.AreEqual(expected, OptionsFile.Parse(text, preserveUnknownOptions: false).ColumnFamilies[0].Options.TableOptions!.ReadAmpBytesPerBit);
    }

    /// <summary>
    /// A malformed file_checksum_gen_factory struct cannot create the Customizable (NotSupported, ignored by
    /// ignore_unsupported_options): the factory set before stays. The managed setter cleared it.
    /// </summary>
    [TestMethod]
    public void MalformedFileChecksumGenFactoryIgnoredLikeNative()
    {
        string text = "[Version]\n  rocksdb_version=11.12.0\n  options_file_version=1.1\n[DBOptions]\n  file_checksum_gen_factory=FileChecksumGenCrc32cFactory\n" +
            "  file_checksum_gen_factory={\n[CFOptions \"default\"]\n";
        (string? native, string? managed) = BothParse(text);
        Assert.IsNull(native);
        AssertSameOutcome(native, managed);
        Assert.IsNotNull(OptionsFile.Parse(text, preserveUnknownOptions: false).DbOptions.FileChecksumGenFactory);
    }

    /// <summary>
    /// Option values native rejects that the managed parser accepted: "comparator=" leaves no comparator and
    /// OptionTypeInfo::Prepare / Validate fail with "Missing configurable object: comparator" (options/options_helper.cc);
    /// an empty blob_compression_opts goes to the legacy ParseCompressionOptions (options/cf_options.cc), which fails with
    /// "unable to parse the specified CF option blob_compression_opts".
    /// </summary>
    [TestMethod]
    [DataRow("[CFOptions \"default\"]\n  comparator=\n")]
    [DataRow("[CFOptions \"default\"]\n  blob_compression_opts=\n")]
    [DataRow("[CFOptions \"default\"]\n  blob_compression_opts=1:2\n")]
    public void EmptyRequiredOptionValuesRejectedLikeNative(string tail)
    {
        (string? native, string? managed) = BothParse(Head + tail);
        Assert.IsNotNull(native);
        AssertSameOutcome(native, managed);
    }

    /// <summary>The legacy ':' form of blob_compression_opts is accepted by both, with the same fields.</summary>
    [TestMethod]
    public void BlobCompressionOptsLegacyFormLikeNative()
    {
        string text = Head + "[CFOptions \"default\"]\n  blob_compression_opts=-13:4:1:16\n";
        (string? native, string? managed) = BothParse(text);
        Assert.IsNull(native);
        AssertSameOutcome(native, managed);
        CompressionOptions c = OptionsFile.Parse(text, preserveUnknownOptions: false).ColumnFamilies[0].Options.BlobCompressionOpts;
        Assert.AreEqual(-13, c.WindowBits);
        Assert.AreEqual(4, c.Level);
        Assert.AreEqual(1, c.Strategy);
        Assert.AreEqual(16u, c.MaxDictBytes);
    }

    private static int Seed()
    {
        string? env = Environment.GetEnvironmentVariable("AUDIT7_SEED");
        return env is not null ? int.Parse(env, System.Globalization.CultureInfo.InvariantCulture) : Environment.TickCount & 0x7fffffff;
    }

    private static string NativeOptionsFileText(string dir)
    {
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions().SetCreateIfMissing(true), dir))
        {
        }
        string file = Directory.GetFiles(dir, "OPTIONS-*").OrderBy(f => f, StringComparer.Ordinal).Last();
        return File.ReadAllText(file);
    }

    /// <summary>
    /// Differential fuzz of RocksDBOptionsParser::Parse (options/options_parser.cc: ParseSection, ParseStatement,
    /// CheckSection, EndSection, ValidityCheck) and the option value parsers: native's own OPTIONS file, mutated line by
    /// line, must be accepted or rejected (with the same status code) by both. Seed: AUDIT7_SEED.
    /// </summary>
    [TestMethod]
    public void MutatedOptionsFileAcceptedOrRejectedLikeNative()
    {
        // AUDIT7_OPTS_SEEDS=n runs n consecutive seeds from AUDIT7_SEED (3000 mutations each, no time limit).
        int first = Seed();
        string? many = Environment.GetEnvironmentVariable("AUDIT7_OPTS_SEEDS");
        int seeds = many is not null ? int.Parse(many, System.Globalization.CultureInfo.InvariantCulture) : 1;
        using var dir = new TempDir("a7optsfuzz");
        string baseText = NativeOptionsFileText(Path.Combine(dir.Path, "db"));
        string probe = Path.Combine(dir.Path, "probe");
        Directory.CreateDirectory(probe);
        var lessStrict = new List<string>();
        var stricter = new SortedDictionary<string, int>(StringComparer.Ordinal);
        int clean = 0;
        for (int seed = first; seed < first + seeds; seed++)
        {
            int before = lessStrict.Count;
            FuzzOptionsFile(seed, baseText, probe, seeds == 1 ? TimeSpan.FromSeconds(15) : TimeSpan.MaxValue, lessStrict, stricter);
            if (lessStrict.Count == before) clean++;
        }
        Console.WriteLine("seeds " + first + ".." + (first + seeds - 1) + ": " + clean + " without a less-strict difference");
        Console.WriteLine("allowed stricter-than-native classes (managed rejects, native accepts; or another rejection code):");
        foreach (KeyValuePair<string, int> kv in stricter) Console.WriteLine("  " + kv.Value + "x " + kv.Key);
        foreach (string d in lessStrict.Distinct().Take(200)) Console.WriteLine("LESS STRICT " + d);
        Assert.AreEqual(0, lessStrict.Count, lessStrict.Count + " inputs native rejects and managed accepts, first: " +
            (lessStrict.Count > 0 ? lessStrict[0] : ""));
    }

    /// <summary>A short class name for a stricter-than-native difference: the option name and the managed error.</summary>
    private static string StricterClass(string line, string managed)
    {
        int eq = line.IndexOf('=', StringComparison.Ordinal);
        string name = eq > 0 ? line[..eq].Trim() : line.Trim();
        if (name.Length > 40) name = name[..40];
        string msg = System.Text.RegularExpressions.Regex.Replace(managed, "'[^']*'|\\(at line \\d+\\)", "");
        if (msg.Length > 110) msg = msg[..110];
        return name + " -> " + msg;
    }

    /// <summary>
    /// One seed of the differential OPTIONS fuzz. The managed parser may be stricter than native (reject what native
    /// accepts, or reject with another code) but never less strict: input native rejects must be rejected.
    /// </summary>
    private static void FuzzOptionsFile(int seed, string baseText, string probe, TimeSpan budget, List<string> lessStrict,
        SortedDictionary<string, int> stricter)
    {
        var rnd = new Random(seed);
        string[] lines = baseText.Split('\n');
        string[] hostile = ["99999999999999999999999", "-1", "1e400", "kNoSuchEnum", "", "{", "}", "0x10", "true;", "9223372036854775808",
            "18446744073709551616", "1.2.3", " 5 ", "0", "4294967296", "2147483648", "1.5", "nan", "kZSTD", "false", "1", "yes", ":"];
        var sw = System.Diagnostics.Stopwatch.StartNew();
        for (int iter = 0; iter < 3000 && sw.Elapsed < budget; iter++)
        {
            string[] m = (string[])lines.Clone();
            int li = rnd.Next(m.Length);
            string line = m[li];
            int eq = line.IndexOf('=', StringComparison.Ordinal);
            switch (rnd.Next(6))
            {
                case 0 or 1 when eq > 0: m[li] = line[..(eq + 1)] + hostile[rnd.Next(hostile.Length)]; break;
                case 2: m[li] = line.Length > 0 ? line.Remove(rnd.Next(line.Length), 1) : line; break;
                case 3: m[li] = line.Length > 0 ? line.Insert(rnd.Next(line.Length), ((char)(32 + rnd.Next(95))).ToString()) : line; break;
                case 4: m[li] = ""; break;
                default: m[li] = m[rnd.Next(m.Length)]; break;
            }
            string text = string.Join('\n', m);
            File.WriteAllText(Path.Combine(probe, "OPTIONS-000007"), text);
            string? native = NativeLoadLatestOptions(probe, ignoreUnknownOptions: false);
            string? managed;
            try
            {
                OptionsFile.Parse(text, preserveUnknownOptions: false);
                managed = null;
            }
            catch (RocksDbException e)
            {
                managed = e.Code + ": " + e.Message;
            }
            catch (Exception e)
            {
                Assert.Fail("seed=" + seed + " iter=" + iter + " line '" + m[li] + "': unexpected " + e);
                return;
            }
            if (native is not null && managed is null)
            {
                lessStrict.Add("seed=" + seed + " line '" + m[li].Trim() + "' (was '" + line.Trim() + "'): native=" + native);
                continue;
            }
            if (managed is null) continue;
            string code = native is null ? "OK"
                : native.StartsWith("Invalid argument", StringComparison.Ordinal) ? "InvalidArgument"
                : native.StartsWith("Corruption", StringComparison.Ordinal) ? "Corruption" : "?";
            if (native is null || !managed.StartsWith(code + ":", StringComparison.Ordinal))
            {
                string key = (native is null ? "native OK: " : "native " + code + ": ") + StricterClass(m[li], managed);
                stricter[key] = stricter.TryGetValue(key, out int n) ? n + 1 : 1;
            }
        }
    }

    // ---- DB open: native and managed on the same damaged directory ------------------------------------------------

    /// <summary>Native DB::Open of <paramref name="dir"/>: null, or the status message.</summary>
    private static string? NativeOpen(string dir, RocksDbSharp.Recovery mode)
    {
        try
        {
            using RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions().SetWalRecoveryMode(mode), dir);
            return null;
        }
        catch (RocksDbSharp.RocksDbException e)
        {
            return e.Message;
        }
    }

    private static string? ManagedOpen(string dir, WalRecoveryMode mode)
    {
        try
        {
            using RocksDatabase db = RocksDatabase.Open(dir, new DbOptions { WalRecoveryMode = mode });
            return null;
        }
        catch (RocksDbException e)
        {
            return e.Code + ": " + e.Message;
        }
    }

    /// <summary>Copies a directory (the damaged DB is opened by both engines from identical copies).</summary>
    private static void CopyDir(string from, string to)
    {
        Directory.CreateDirectory(to);
        foreach (string f in Directory.GetFiles(from)) File.Copy(f, Path.Combine(to, Path.GetFileName(f)));
    }

    private static string NativeDbWithData(TempDir dir)
    {
        string path = Path.Combine(dir.Path, "src");
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions().SetCreateIfMissing(true), path))
        {
            db.Put("a", "1");
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            db.Put("b", "2");
        }
        return path;
    }

    private static (string? Native, string? Managed) OpenBoth(TempDir dir, string src, WalRecoveryMode mode)
    {
        string n = Path.Combine(dir.Path, "native-" + Guid.NewGuid().ToString("N"));
        string m = Path.Combine(dir.Path, "managed-" + Guid.NewGuid().ToString("N"));
        CopyDir(src, n);
        CopyDir(src, m);
        string? native = NativeOpen(n, (RocksDbSharp.Recovery)(int)mode);
        string? managed = ManagedOpen(m, mode);
        Console.WriteLine("native:  " + (native ?? "OK"));
        Console.WriteLine("managed: " + (managed ?? "OK"));
        return (native, managed);
    }

    /// <summary>Appends one MANIFEST record (a VersionEdit) to the current MANIFEST of <paramref name="db"/>.</summary>
    private static void AppendManifestRecord(string db, byte[] record)
    {
        string current = File.ReadAllText(Path.Combine(db, "CURRENT")).TrimEnd('\n');
        string manifest = Path.Combine(db, current);
        long len = new FileInfo(manifest).Length;
        var mem = new RocksDb.Managed.Env.MemoryFileSystem();
        mem.CreateDirectoryIfMissing("/m");
        using (var w = new RocksDb.Managed.Wal.LogWriter(mem.CreateWritable("/m/x"), 0, recycleLogFiles: false, manualFlush: false, RocksDb.Managed.Compression.CompressionType.None,
                   (int)(len % RocksDb.Managed.Wal.LogFormat.BlockSize)))
        {
            w.AddRecord(record);
        }
        using FileStream fs = new(manifest, FileMode.Append);
        fs.Write(mem.ReadAllBytes("/m/x"));
    }

    private static byte[] IKey(string userKey, ulong seq)
    {
        byte[] u = Encoding.UTF8.GetBytes(userKey);
        var k = new byte[u.Length + 8];
        Db.DbFormat.AppendInternalKey(k, u, seq, Db.EntryType.Value);
        return k;
    }

    /// <summary>
    /// A MANIFEST edit adding a table file on a level &gt;= num_levels: VersionBuilder::Rep::ApplyFileAddition counts it in
    /// invalid_level_sizes_ and VersionEditHandler::CheckIterationResult fails with InvalidArgument "db has more levels
    /// than options.num_levels". The managed replay allocated one list per level up to the decoded level, so a damaged
    /// level number (up to 2^31 - 1) exhausted memory or ran for minutes.
    /// </summary>
    [TestMethod]
    [DataRow(7)]
    [DataRow(4096)]
    [DataRow(1_000_000_000)]
    [DataRow(int.MaxValue)]
    public void ManifestFileOnHugeLevelIsInvalidArgumentLikeNative(int level)
    {
        using var dir = new TempDir("a7level");
        string src = NativeDbWithData(dir);
        var edit = new Db.VersionEdit { ColumnFamily = 0 };
        edit.AddFile(level, new Db.FileMetaData
        {
            FileNumber = 900, FileSize = 1000, Smallest = IKey("x", 1), Largest = IKey("y", 2), SmallestSeqno = 1, LargestSeqno = 2, EpochNumber = 9,
        });
        AppendManifestRecord(src, edit.Encode(0));
        (string? native, string? managed) = OpenBoth(dir, src, WalRecoveryMode.PointInTimeRecovery);
        Assert.IsNotNull(native);
        AssertSameOutcome(native, managed);
    }

    /// <summary>
    /// GetCurrentManifestPath (db/manifest_ops.cc) and VersionSet::Recover over a damaged CURRENT: no trailing newline,
    /// empty, garbage, a name that is not a MANIFEST, a MANIFEST that does not exist. Both engines fail the open with
    /// the same status code.
    /// </summary>
    [TestMethod]
    [DataRow("MANIFEST-000001")]
    [DataRow("")]
    [DataRow("\n")]
    [DataRow("garbage\n")]
    [DataRow("000005.log\n")]
    [DataRow("MANIFEST-99999999999999999999999\n")]
    [DataRow("MANIFEST-000777\n")]
    [DataRow("MANIFEST-\n")]
    public void DamagedCurrentFailsOpenLikeNative(string current)
    {
        using var dir = new TempDir("a7current");
        string src = NativeDbWithData(dir);
        File.WriteAllText(Path.Combine(src, "CURRENT"), current);
        (string? native, string? managed) = OpenBoth(dir, src, WalRecoveryMode.PointInTimeRecovery);
        Assert.IsNotNull(native);
        AssertSameOutcome(native, managed);
    }

    // ---- differential fuzz: damaged WAL / MANIFEST, every wal_recovery_mode ------------------------------------------

    private const int FuzzKeys = 24;

    private static string FK(int i) => "key" + i.ToString("D3", System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>
    /// Native DB: keys 0..7 flushed, 8..15 in a WAL kept alive by avoid_flush_during_recovery, 16..23 in a second WAL;
    /// one put per batch, in key order.
    /// </summary>
    private static string NativeDbWithTwoWals(TempDir dir) => NativeDbWithTwoWals(dir, walZstd: false);

    private static string NativeDbWithTwoWals(TempDir dir, bool walZstd)
    {
        string path = Path.Combine(dir.Path, "src");
        RocksDbSharp.DbOptions Options()
        {
            var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
            RocksDbSharp.Native.Instance.rocksdb_options_set_avoid_flush_during_recovery(o.Handle, 1);
            if (walZstd) RocksDbSharp.Native.Instance.rocksdb_options_set_wal_compression(o.Handle, 0x7); // kZSTD
            return o;
        }
        int k = 0;
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(Options(), path))
        {
            for (int i = 0; i < 8; i++, k++) db.Put(FK(k), "v" + k);
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            for (int i = 0; i < 8; i++, k++) db.Put(FK(k), "v" + k);
        }
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(Options(), path))
        {
            for (int i = 0; i < 8; i++, k++) db.Put(FK(k), "v" + k);
        }
        return path;
    }

    /// <summary>Opens with native and reads every key: ("OK", bitmap of present keys) or (status, "").</summary>
    private static (string Status, string Keys) NativeOpenAndRead(string dir, WalRecoveryMode mode)
    {
        try
        {
            var o = new RocksDbSharp.DbOptions().SetWalRecoveryMode((RocksDbSharp.Recovery)(int)mode);
            RocksDbSharp.Native.Instance.rocksdb_options_set_avoid_flush_during_recovery(o.Handle, 1);
            using RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(o, dir);
            var sb = new StringBuilder();
            for (int i = 0; i < FuzzKeys; i++) sb.Append(db.Get(FK(i)) == "v" + i ? '1' : db.Get(FK(i)) is null ? '0' : 'X');
            if (mode == WalRecoveryMode.SkipAnyCorruptedRecords) sb.Append(" updates:").Append(NativeUpdatesSummary(db));
            return ("OK", sb.ToString());
        }
        catch (RocksDbSharp.RocksDbException e)
        {
            string code = e.Message.StartsWith("Invalid argument", StringComparison.Ordinal) ? "InvalidArgument"
                : e.Message.StartsWith("Corruption", StringComparison.Ordinal) ? "Corruption"
                : e.Message.StartsWith("IO error", StringComparison.Ordinal) ? "IOError"
                : e.Message.StartsWith("Not implemented", StringComparison.Ordinal) ? "NotSupported" : e.Message;
            return (code, "");
        }
    }

    private static (string Status, string Keys, string Message) ManagedOpenAndRead(string dir, WalRecoveryMode mode)
    {
        try
        {
            using RocksDatabase db = RocksDatabase.Open(dir, new DbOptions { WalRecoveryMode = mode, AvoidFlushDuringRecovery = true });
            var sb = new StringBuilder();
            for (int i = 0; i < FuzzKeys; i++)
            {
                byte[]? v = db.Get(Encoding.UTF8.GetBytes(FK(i)));
                sb.Append(v is null ? '0' : Encoding.UTF8.GetString(v) == "v" + i ? '1' : 'X');
            }
            if (mode == WalRecoveryMode.SkipAnyCorruptedRecords) sb.Append(" updates:").Append(ManagedUpdatesSummary(db));
            return ("OK", sb.ToString(), "");
        }
        catch (RocksDbException e)
        {
            return (e.Code.ToString(), "", e.Message);
        }
    }

    private static string StatusKind(string message) =>
        message.StartsWith("Corruption", StringComparison.Ordinal) ? "Corruption"
        : message.StartsWith("NotFound", StringComparison.Ordinal) || message.StartsWith("Not found", StringComparison.Ordinal) ? "NotFound"
        : message.StartsWith("Invalid argument", StringComparison.Ordinal) ? "InvalidArgument"
        : message.StartsWith("Not implemented", StringComparison.Ordinal) ? "NotSupported"
        : message.StartsWith("IO error", StringComparison.Ordinal) ? "IOError" : message;

    /// <summary>DB::GetUpdatesSince(1) (db/transaction_log_impl.cc): the batch sequences read, then the final status.</summary>
    private static string NativeUpdatesSummary(RocksDbSharp.RocksDb db)
    {
        var sb = new StringBuilder();
        RocksDbSharp.TransactionLogIterator it;
        try
        {
            it = db.GetUpdatesSince(1);
        }
        catch (RocksDbSharp.RocksDbException e)
        {
            return StatusKind(e.Message);
        }
        using (it)
        {
            for (; it.Valid(); it.Next())
            {
                using RocksDbSharp.WriteBatch b = it.GetBatch(out ulong seq);
                sb.Append(seq).Append(',');
            }
            try
            {
                it.Status();
            }
            catch (RocksDbSharp.RocksDbException e)
            {
                return sb + StatusKind(e.Message);
            }
        }
        return sb + "OK";
    }

    private static string ManagedUpdatesSummary(RocksDatabase db)
    {
        var sb = new StringBuilder();
        TransactionLogIterator it;
        try
        {
            it = db.GetUpdatesSince(1);
        }
        catch (RocksDbException e)
        {
            return e.Code.ToString();
        }
        using (it)
        {
            for (; it.Valid; it.Next()) sb.Append(it.Current.Sequence).Append(',');
            return sb + (it.Error is { } err ? err.Code.ToString() : "OK");
        }
    }

    /// <summary>Offsets of the type bytes of the physical records in a WAL / MANIFEST (header byte 6).</summary>
    private static HashSet<int> TypeByteOffsets(byte[] log)
    {
        var set = new HashSet<int>();
        foreach (int off in RecordOffsets(log)) set.Add(off + 6);
        return set;
    }

    private static bool IsRecyclableType(byte t) => t is >= 5 and <= 8 or 11 or 131;

    /// <summary>
    /// DBImpl::RecoverLogFiles / ProcessLogFile / HandleNonOkStatusOrOldLogRecord and log::Reader (every
    /// wal_recovery_mode), VersionSet::Recover over a damaged MANIFEST: native and managed open identical copies of a
    /// native DB whose WAL or MANIFEST bytes were flipped, overwritten, zeroed, truncated or extended with zeros; the
    /// status code and the recovered keys must match. Mutations that turn a record type byte into a recyclable type
    /// are skipped (native hangs on them, see LogReader). Seed: AUDIT7_SEED.
    /// </summary>
    [TestMethod]
    [DataRow(false, false)]
    [DataRow(true, false)]
    [DataRow(false, true)]
    public void DamagedWalOrManifestRecoversLikeNative(bool recycle, bool walZstd)
    {
        int seed = Seed();
        Console.WriteLine("seed=" + seed);
        var rnd = new Random(seed);
        using var dir = new TempDir("a7walfuzz");
        string src = recycle ? NativeRecycledDb(dir) : NativeDbWithTwoWals(dir, walZstd);
        string[] targets = Directory.GetFiles(src).Select(Path.GetFileName).Where(n => n!.EndsWith(".log", StringComparison.Ordinal) ||
            n.StartsWith("MANIFEST-", StringComparison.Ordinal)).Select(n => n!).OrderBy(n => n, StringComparer.Ordinal).ToArray();
        Console.WriteLine("targets: " + string.Join(", ", targets));
        var diffs = new List<string>();
        var sw = System.Diagnostics.Stopwatch.StartNew();
        int iter = 0, compared = 0;
        for (; iter < 2000 && sw.Elapsed < TimeSpan.FromSeconds(20); iter++)
        {
            string target = targets[rnd.Next(targets.Length)];
            byte[] orig = File.ReadAllBytes(Path.Combine(src, target));
            byte[] b = (byte[])orig.Clone();
            string how;
            switch (rnd.Next(5))
            {
                case 0:
                {
                    int at = rnd.Next(b.Length);
                    b[at] ^= (byte)(1 << rnd.Next(8));
                    how = "flip@" + at;
                    break;
                }
                case 1:
                {
                    int at = rnd.Next(b.Length);
                    b[at] = (byte)rnd.Next(256);
                    how = "byte@" + at + "=" + b[at];
                    break;
                }
                case 2:
                {
                    int n = rnd.Next(b.Length);
                    b = b[..n];
                    how = "truncate@" + n;
                    break;
                }
                case 3:
                {
                    int at = rnd.Next(b.Length), n = Math.Min(b.Length - at, 1 + rnd.Next(40));
                    b.AsSpan(at, n).Clear();
                    how = "zero@" + at + "+" + n;
                    break;
                }
                default:
                {
                    int n = 1 + rnd.Next(64);
                    b = [.. b, .. new byte[n]];
                    how = "zerotail+" + n;
                    break;
                }
            }
            bool hangs = false;
            foreach (int t in TypeByteOffsets(orig))
            {
                // Any change of a record type byte between the legacy and the recyclable families can make native loop
                // forever in ReadPhysicalRecord ("A recycled log should have started with a recycled record").
                if (t < b.Length && b[t] != orig[t] && (IsRecyclableType(b[t]) || IsRecyclableType(orig[t]))) hangs = true;
            }
            if (hangs) continue;
            var mode = (WalRecoveryMode)rnd.Next(4);
            string n1 = Path.Combine(dir.Path, "n" + iter), m1 = Path.Combine(dir.Path, "m" + iter);
            CopyDir(src, n1);
            CopyDir(src, m1);
            File.WriteAllBytes(Path.Combine(n1, target), b);
            File.WriteAllBytes(Path.Combine(m1, target), b);
            (string ns, string nk) = NativeOpenAndRead(n1, mode);
            (string ms, string mk, string mmsg) = ManagedOpenAndRead(m1, mode);
            nk = nk.Replace(NativeTryAgain, "TryAgain", StringComparison.Ordinal);
            if (nk.EndsWith(",TryAgain", StringComparison.Ordinal) && mk == nk[..^"TryAgain".Length] + "OK") mk = nk; // see GetUpdatesSinceStoppedBelowLastSequenceIsTryAgainLikeNative
            compared++;
            if (ns != ms || nk != mk)
            {
                diffs.Add("iter=" + iter + " " + target + " " + how + " mode=" + mode + ": native=" + ns + " " + nk + " | managed=" + ms + " " + mk + " " + mmsg);
            }
            Directory.Delete(n1, true);
            Directory.Delete(m1, true);
        }
        Console.WriteLine("compared " + compared + " of " + iter);
        foreach (string d in diffs.Take(40)) Console.WriteLine(d);
        Assert.AreEqual(0, diffs.Count, "seed=" + seed + ": " + diffs.Count + " differences, first: " + (diffs.Count > 0 ? diffs[0] : ""));
    }

    /// <summary>Start offsets of the physical records of a log (legacy or recyclable headers).</summary>
    private static List<int> RecordOffsets(byte[] log)
    {
        var list = new List<int>();
        int off = 0;
        while (off + 7 <= log.Length)
        {
            int inBlock = off % 32768;
            int type = log[off + 6];
            int header = type is >= 5 and <= 8 or 11 or 131 ? 11 : 7;
            if (32768 - inBlock < header)
            {
                off += 32768 - inBlock;
                continue;
            }
            list.Add(off);
            off += header + (log[off + 4] | (log[off + 5] << 8));
        }
        return list;
    }

    private static void Damage(string db, string scenario)
    {
        string[] wals = Directory.GetFiles(db, "*.log").OrderBy(f => f, StringComparer.Ordinal).ToArray();
        string manifest = Path.Combine(db, File.ReadAllText(Path.Combine(db, "CURRENT")).TrimEnd('\n'));
        byte[] first = File.ReadAllBytes(wals[0]), last = File.ReadAllBytes(wals[^1]);
        switch (scenario)
        {
            case "wal-torn-tail": File.WriteAllBytes(wals[^1], last[..^3]); break;
            case "wal-torn-header": File.WriteAllBytes(wals[^1], last[..(RecordOffsets(last)[^1] + 4)]); break;
            case "wal-middle-crc": first[RecordOffsets(first)[3] + 9] ^= 1; File.WriteAllBytes(wals[0], first); break;
            case "wal-first-record-crc": first[RecordOffsets(first)[0] + 8] ^= 0x10; File.WriteAllBytes(wals[0], first); break;
            case "wal-last-wal-middle-crc": last[RecordOffsets(last)[2] + 9] ^= 1; File.WriteAllBytes(wals[^1], last); break;
            case "wal-first-empty": File.WriteAllBytes(wals[0], []); break;
            case "wal-first-missing": File.Delete(wals[0]); break;
            case "wal-zero-tail": File.WriteAllBytes(wals[^1], [.. last, .. new byte[4096]]); break;
            case "wal-zero-tail-first": File.WriteAllBytes(wals[0], [.. first, .. new byte[100]]); break;
            case "wal-garbage-tail": File.WriteAllBytes(wals[^1], [.. last, 1, 2, 3, 4, 5, 6, 1, 9, 9]); break;
            case "wal-zeroed-record": first.AsSpan(RecordOffsets(first)[4], 7).Clear(); File.WriteAllBytes(wals[0], first); break;
            case "wal-sequence-gap":
            {
                // A whole record (one batch) removed from the middle of the first WAL: a sequence gap between valid records.
                List<int> offs = RecordOffsets(first);
                File.WriteAllBytes(wals[0], [.. first[..offs[3]], .. first[offs[4]..]]);
                break;
            }
            case "wal-recycled-wrong-lognumber":
            {
                // Recyclable header (11 bytes: crc, length, type, log number): another log number = a previous user's record.
                List<int> offs = RecordOffsets(last);
                last[offs[3] + 7] ^= 0x01;
                File.WriteAllBytes(wals[^1], last);
                break;
            }
            case "wal-zstd-bad-frame":
            case "wal-zstd-truncated-frame":
            {
                // A compressed WAL record whose payload is not a valid zstd frame (or only its first half), with a valid CRC:
                // ReadPhysicalRecord's Uncompress fails -> kBadRecord (buffer dropped, nothing reported).
                List<int> offs = RecordOffsets(last);
                int at = offs[3], len = last[at + 4] | (last[at + 5] << 8);
                byte[] payload = last.AsSpan(at + 7, len).ToArray();
                if (scenario == "wal-zstd-bad-frame") payload.AsSpan(payload.Length / 2).Fill(0x55);
                else payload = payload[..(payload.Length / 2)];
                byte[] rec = new byte[7 + payload.Length];
                rec[4] = (byte)payload.Length;
                rec[5] = (byte)(payload.Length >> 8);
                rec[6] = last[at + 6];
                payload.CopyTo(rec, 7);
                Util.Coding.EncodeFixed32(rec, Util.Crc32C.Mask(Util.Crc32C.Value(rec.AsSpan(6))));
                File.WriteAllBytes(wals[^1], [.. last[..at], .. rec, .. last[(at + 7 + len)..]]);
                break;
            }
            case "manifest-truncated-mid-record":
            {
                byte[] m = File.ReadAllBytes(manifest);
                File.WriteAllBytes(manifest, m[..(RecordOffsets(m)[^1] + 10)]);
                break;
            }
            case "manifest-crc":
            {
                byte[] m = File.ReadAllBytes(manifest);
                m[RecordOffsets(m)[^1] + 8] ^= 1;
                File.WriteAllBytes(manifest, m);
                break;
            }
            case "manifest-unknown-required-tag":
                AppendManifestRecord(db, [99, 1, 2]);
                break;
            case "manifest-unknown-ignorable-tag":
                // varint(kTagSafeIgnoreMask + 99) = 0xe3 0x40, then a length-prefixed payload.
                AppendManifestRecord(db, [0xe3, 0x40, 3, 7, 7, 7]);
                break;
            case "manifest-incomplete-atomic-group":
            {
                var e1 = new Db.VersionEdit { ColumnFamily = 0, LastSequence = 1 };
                e1.MarkAtomicGroup(2);
                var e2 = new Db.VersionEdit { ColumnFamily = 0, LastSequence = 1 };
                e2.MarkAtomicGroup(1);
                AppendManifestRecord(db, e1.Encode(0));
                AppendManifestRecord(db, e2.Encode(0));
                break;
            }
            case "manifest-bad-atomic-group":
            {
                var e1 = new Db.VersionEdit { ColumnFamily = 0, LastSequence = 1 };
                e1.MarkAtomicGroup(2);
                AppendManifestRecord(db, e1.Encode(0));
                AppendManifestRecord(db, new Db.VersionEdit { ColumnFamily = 0, LastSequence = 1 }.Encode(0));
                break;
            }
            case "manifest-unknown-cf":
                AppendManifestRecord(db, new Db.VersionEdit { ColumnFamily = 77, LogNumber = 1 }.Encode(0));
                break;
            case "manifest-delete-missing-file":
            {
                var e = new Db.VersionEdit { ColumnFamily = 0 };
                e.DeleteFile(1, 4242);
                AppendManifestRecord(db, e.Encode(0));
                break;
            }
            case "manifest-comparator-mismatch":
                AppendManifestRecord(db, new Db.VersionEdit { ColumnFamily = 0, ComparatorName = "rocksdb.ReverseBytewiseComparator" }.Encode(0));
                break;
            case "manifest-missing":
                File.Delete(manifest);
                break;
            case "identity-garbage":
                File.WriteAllBytes(Path.Combine(db, "IDENTITY"), [0xff, 0xfe, 0, 1, 2]);
                break;
            case "identity-missing":
                File.Delete(Path.Combine(db, "IDENTITY"));
                break;
            default:
                throw new ArgumentException(scenario);
        }
    }

    /// <summary>
    /// The damage scenarios of the audit brief, each opened by native and managed in every wal_recovery_mode (log::Reader
    /// ReadRecord / ReadPhysicalRecord, DBImpl::RecoverLogFiles / MaybeReviseStopReplayForCorruption, VersionSet::Recover /
    /// VersionEditHandler, VersionEdit::DecodeFrom unknown tags, AtomicGroupReadBuffer): same status code and same
    /// recovered keys.
    /// </summary>
    [TestMethod]
    [DataRow("wal-torn-tail", false)]
    [DataRow("wal-torn-header", false)]
    [DataRow("wal-middle-crc", false)]
    [DataRow("wal-first-record-crc", false)]
    [DataRow("wal-last-wal-middle-crc", false)]
    [DataRow("wal-first-empty", false)]
    [DataRow("wal-first-missing", false)]
    [DataRow("wal-zero-tail", false)]
    [DataRow("wal-zero-tail-first", false)]
    [DataRow("wal-garbage-tail", false)]
    [DataRow("wal-zeroed-record", false)]
    [DataRow("wal-sequence-gap", false)]
    [DataRow("wal-torn-tail", true)]
    [DataRow("wal-middle-crc", true)]
    [DataRow("wal-recycled-wrong-lognumber", true)]
    [DataRow("manifest-truncated-mid-record", false)]
    [DataRow("manifest-crc", false)]
    [DataRow("manifest-unknown-required-tag", false)]
    [DataRow("manifest-unknown-ignorable-tag", false)]
    [DataRow("manifest-incomplete-atomic-group", false)]
    [DataRow("manifest-bad-atomic-group", false)]
    [DataRow("manifest-unknown-cf", false)]
    [DataRow("manifest-delete-missing-file", false)]
    [DataRow("manifest-comparator-mismatch", false)]
    [DataRow("manifest-missing", false)]
    [DataRow("identity-garbage", false)]
    [DataRow("identity-missing", false)]
    public void DamageScenarioRecoversLikeNative(string scenario, bool recycle) => CompareScenario(scenario, recycle, strictIteratorEnd: false);

    /// <summary>
    /// WAL compression (kSetCompressionType record, ZSTDStreamingUncompress, log::Reader::ReadPhysicalRecord's
    /// uncompress loop): a CRC-valid record whose compressed payload is damaged or cut, opened by both engines.
    /// </summary>
    [TestMethod]
    [DataRow("wal-zstd-bad-frame")]
    [DataRow("wal-zstd-truncated-frame")]
    [DataRow("wal-torn-tail")]
    [DataRow("wal-middle-crc")]
    public void DamagedCompressedWalRecoversLikeNative(string scenario) => CompareScenario(scenario, recycle: false, strictIteratorEnd: false, walZstd: true);

    /// <summary>
    /// Audit #7 open: GetUpdatesSince over a recycled WAL that holds a record of its previous use (another log number)
    /// before the end of the recovered data. WalIteratorImpl::NextImpl (db/wal_iterator_impl.cc) ends with
    /// TryAgain("Create a new iterator to fetch the new tail.") because the iterator stopped below
    /// VersionSet::LastSequence; the managed iterator (deliberately, for tailing: it re-lists the WAL files instead of
    /// returning TryAgain) ends with an OK status, which reads as "caught up" although later batches exist.
    /// </summary>
    [TestMethod]
    [Ignore("audit #7 open: managed TransactionLogIterator ends with OK where native returns TryAgain (tailing design decision)")]
    public void GetUpdatesSinceStoppedBelowLastSequenceIsTryAgainLikeNative() =>
        CompareScenario("wal-recycled-wrong-lognumber", recycle: true, strictIteratorEnd: true);

    private const string NativeTryAgain = "Operation failed. Try again.: Create a new iterator to fetch the new tail.";

    private static void CompareScenario(string scenario, bool recycle, bool strictIteratorEnd, bool walZstd = false)
    {
        using var dir = new TempDir("a7scen");
        string src = recycle ? NativeRecycledDb(dir) : NativeDbWithTwoWals(dir, walZstd);
        Damage(src, scenario);
        var lines = new List<string>();
        bool same = true;
        foreach (WalRecoveryMode mode in new[] { WalRecoveryMode.TolerateCorruptedTailRecords, WalRecoveryMode.AbsoluteConsistency,
                     WalRecoveryMode.PointInTimeRecovery, WalRecoveryMode.SkipAnyCorruptedRecords })
        {
            // Native refuses recycle_log_file_num with kTolerateCorruptedTailRecords / kAbsoluteConsistency by turning
            // recycling off; the damaged WAL is read the same way.
            string n1 = Path.Combine(dir.Path, "n" + mode), m1 = Path.Combine(dir.Path, "m" + mode);
            CopyDir(src, n1);
            CopyDir(src, m1);
            (string ns, string nk) = NativeOpenAndRead(n1, mode);
            (string ms, string mk, string mmsg) = ManagedOpenAndRead(m1, mode);
            nk = nk.Replace(NativeTryAgain, "TryAgain", StringComparison.Ordinal);
            if (!strictIteratorEnd && nk.EndsWith(",TryAgain", StringComparison.Ordinal) && mk == nk[..^"TryAgain".Length] + "OK")
            {
                // Known deviation, see GetUpdatesSinceStoppedBelowLastSequenceIsTryAgainLikeNative.
                mk = nk;
            }
            lines.Add(mode + ": native=" + ns + " " + nk + " | managed=" + ms + " " + mk + " " + mmsg);
            same &= ns == ms && nk == mk;
            // DB::OpenForReadOnly replays the WALs too (DBImplReadOnly, nothing written).
            string ro = ReadOnlyOpenBoth(src, mode, out bool roSame);
            lines.Add(mode + " read-only: " + ro);
            same &= roSame;
        }
        foreach (string l in lines) Console.WriteLine(l);
        Assert.IsTrue(same, scenario + ":\n" + string.Join("\n", lines));
    }

    private static string ReadKeysNative(RocksDbSharp.RocksDb db)
    {
        var sb = new StringBuilder();
        for (int i = 0; i < FuzzKeys; i++) sb.Append(db.Get(FK(i)) == "v" + i ? '1' : db.Get(FK(i)) is null ? '0' : 'X');
        return sb.ToString();
    }

    private static string ReadKeysManaged(RocksDatabase db)
    {
        var sb = new StringBuilder();
        for (int i = 0; i < FuzzKeys; i++)
        {
            byte[]? v = db.Get(Encoding.UTF8.GetBytes(FK(i)));
            sb.Append(v is null ? '0' : Encoding.UTF8.GetString(v) == "v" + i ? '1' : 'X');
        }
        return sb.ToString();
    }

    /// <summary>Opens <paramref name="src"/> read-only with both engines (the directory is not modified).</summary>
    private static string ReadOnlyOpenBoth(string src, WalRecoveryMode mode, out bool same)
    {
        string native, managed;
        try
        {
            using RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.OpenReadOnly(new RocksDbSharp.DbOptions().SetWalRecoveryMode((RocksDbSharp.Recovery)(int)mode),
                src, false);
            native = "OK " + ReadKeysNative(db);
        }
        catch (RocksDbSharp.RocksDbException e)
        {
            native = StatusKind(e.Message);
        }
        try
        {
            using RocksDatabase db = RocksDatabase.OpenReadOnly(src, new DbOptions { WalRecoveryMode = mode });
            managed = "OK " + ReadKeysManaged(db);
        }
        catch (RocksDbException e)
        {
            managed = e.Code.ToString();
        }
        same = native == managed;
        return "native=" + native + " | managed=" + managed;
    }

    /// <summary>Like <see cref="NativeDbWithTwoWals"/>, written with recycle_log_file_num = 2 (recyclable WAL records).</summary>
    private static string NativeRecycledDb(TempDir dir)
    {
        string path = Path.Combine(dir.Path, "src");
        RocksDbSharp.DbOptions Options()
        {
            var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetWalRecoveryMode(RocksDbSharp.Recovery.PointInTime);
            RocksDbSharp.Native.Instance.rocksdb_options_set_avoid_flush_during_recovery(o.Handle, 1);
            RocksDbSharp.Native.Instance.rocksdb_options_set_recycle_log_file_num(o.Handle, (UIntPtr)2);
            return o;
        }
        int k = 0;
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(Options(), path))
        {
            for (int i = 0; i < 8; i++, k++) db.Put(FK(k), "v" + k);
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            for (int i = 0; i < 8; i++, k++) db.Put(FK(k), "v" + k);
        }
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(Options(), path))
        {
            for (int i = 0; i < 8; i++, k++) db.Put(FK(k), "v" + k);
        }
        return path;
    }

    // ---- WriteBatch from untrusted bytes ---------------------------------------------------------------------------

    private static byte[] BaseBatchRep()
    {
        using var b = new Db.WriteBatch();
        b.Put(Encoding.UTF8.GetBytes("k0"), Encoding.UTF8.GetBytes("v0"));
        b.Delete(Encoding.UTF8.GetBytes("k1"));
        b.Put(Encoding.UTF8.GetBytes("k2"), Encoding.UTF8.GetBytes("v2"));
        b.SingleDelete(Encoding.UTF8.GetBytes("k3"));
        b.DeleteRange(Encoding.UTF8.GetBytes("r0"), Encoding.UTF8.GetBytes("r9"));
        b.PutLogData(Encoding.UTF8.GetBytes("blob"));
        b.Put(Encoding.UTF8.GetBytes("k4"), Encoding.UTF8.GetBytes("v4"));
        b.Put(Encoding.UTF8.GetBytes("k5"), Encoding.UTF8.GetBytes("v5"));
        return b.Data.ToArray();
    }

    /// <summary>Whether a record of <paramref name="rep"/>, up to the first malformed one, is a (column family) TimedPut tag.</summary>
    private static bool HasTimedPutRecord(byte[] rep)
    {
        int pos = 12;
        while (pos < rep.Length)
        {
            var tag = (Db.EntryType)rep[pos];
            if (tag is Db.EntryType.ValuePreferredSeqno or Db.EntryType.ColumnFamilyValuePreferredSeqno) return true;
            int at = pos;
            try
            {
                var r = new Db.WriteBatchReader(rep.AsSpan(at), wholeBatch: false, count: 0);
                if (!r.TryReadNext(out _)) return false;
                pos = at + (rep.Length - at - r.RemainingBytes);
            }
            catch (RocksDbException)
            {
                return false;
            }
        }
        return false;
    }

    private static readonly string[] s_batchKeys = ["k0", "k2", "k4", "k5", "z"];

    private static string NativeApply(string dir, byte[] rep)
    {
        using RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions().SetCreateIfMissing(true), dir);
        string status;
        try
        {
            using var b = new RocksDbSharp.WriteBatch(rep);
            db.Write(b);
            status = "OK";
        }
        catch (RocksDbSharp.RocksDbException e)
        {
            status = StatusKind(e.Message);
        }
        string after;
        try
        {
            db.Put("z", "1");
            after = "OK";
        }
        catch (RocksDbSharp.RocksDbException e)
        {
            after = StatusKind(e.Message);
        }
        var sb = new StringBuilder(status + " then put " + after + " keys ");
        foreach (string k in s_batchKeys)
        {
            try
            {
                sb.Append(db.Get(k) is null ? '0' : '1');
            }
            catch (RocksDbSharp.RocksDbException e)
            {
                sb.Append('[').Append(StatusKind(e.Message)).Append(']');
            }
        }
        return sb.ToString();
    }

    private static string ManagedApply(string dir, byte[] rep)
    {
        using RocksDatabase db = RocksDatabase.Open(dir, new DbOptions { CreateIfMissing = true });
        string status;
        try
        {
            using var b = new Db.WriteBatch(rep);
            db.Write(b);
            status = "OK";
        }
        catch (RocksDbException e)
        {
            status = e.Code.ToString();
        }
        string after;
        try
        {
            db.Put(Encoding.UTF8.GetBytes("z"), Encoding.UTF8.GetBytes("1"));
            after = "OK";
        }
        catch (RocksDbException e)
        {
            after = e.Code.ToString();
        }
        var sb = new StringBuilder(status + " then put " + after + " keys ");
        foreach (string k in s_batchKeys)
        {
            try
            {
                sb.Append(db.Get(Encoding.UTF8.GetBytes(k)) is null ? '0' : '1');
            }
            catch (RocksDbException e)
            {
                sb.Append('[').Append(e.Code).Append(']');
            }
        }
        return sb.ToString();
    }

    /// <summary>
    /// DBImpl::Write with a WriteBatch built from mutated bytes (WriteBatch(rep), WriteBatchInternal::InsertInto ->
    /// Iterate / ReadRecordFromWriteBatch, "WriteBatch has wrong count", unknown column family ids): native and managed
    /// return the same status, leave the same keys visible, and accept (or refuse, after a background error) the next
    /// write the same way. Reps shorter than the 12-byte header are not given to native (it reads the header unchecked).
    /// Seed: AUDIT7_SEED.
    /// </summary>
    [TestMethod]
    public void MutatedWriteBatchAppliedLikeNative()
    {
        int seed = Seed();
        Console.WriteLine("seed=" + seed);
        var rnd = new Random(seed);
        byte[] baseRep = BaseBatchRep();
        using var dir = new TempDir("a7wb");
        var diffs = new List<string>();
        var sw = System.Diagnostics.Stopwatch.StartNew();
        int iter = 0, skipped = 0;
        for (; iter < 400 && sw.Elapsed < TimeSpan.FromSeconds(15); iter++)
        {
            byte[] rep = (byte[])baseRep.Clone();
            string how;
            switch (rnd.Next(5))
            {
                case 0:
                {
                    int at = 12 + rnd.Next(rep.Length - 12);
                    rep[at] ^= (byte)(1 << rnd.Next(8));
                    how = "flip@" + at;
                    break;
                }
                case 1:
                {
                    int at = 12 + rnd.Next(rep.Length - 12);
                    rep[at] = (byte)rnd.Next(256);
                    how = "byte@" + at + "=" + rep[at];
                    break;
                }
                case 2:
                {
                    int n = 12 + rnd.Next(rep.Length - 12);
                    rep = rep[..n];
                    how = "truncate@" + n;
                    break;
                }
                case 3:
                {
                    uint count = (uint)rnd.Next(12);
                    Util.Coding.EncodeFixed32(rep.AsSpan(8), count);
                    how = "count=" + count;
                    break;
                }
                default:
                {
                    // A column family variant of a Put with a random (mostly unknown) column family id.
                    byte cf = (byte)rnd.Next(4);
                    rep = [.. rep, (byte)Db.EntryType.ColumnFamilyValue, cf, 1, (byte)'c', 1, (byte)'v'];
                    Util.Coding.EncodeFixed32(rep.AsSpan(8), Util.Coding.DecodeFixed32(rep.AsSpan(8)) + 1);
                    how = "cfput cf=" + cf;
                    break;
                }
            }
            Util.Coding.EncodeFixed64(rep, 0);
            string n1 = Path.Combine(dir.Path, "n" + iter), m1 = Path.Combine(dir.Path, "m" + iter);
            if (HasTimedPutRecord(rep))
            {
                // Native aborts the process (std::length_error) on a TimedPut whose value is shorter than its 8-byte write
                // time: ReadRecordFromWriteBatch -> ParsePackedValueWithWriteTime (db/seqno_to_time_mapping.cc) builds
                // Slice(value.data(), value.size() - 8). The managed reader returns Corruption "bad WriteBatch TimedPut"
                // (see WriteBatchMutationFuzzOnlyCorruptionEscapes); such reps are not given to native.
                skipped++;
                continue;
            }
            string native = NativeApply(n1, rep);
            string managed = ManagedApply(m1, rep);
            // Known deviation (stricter than upstream): the managed write path iterates the whole batch before the WAL
            // write (ValidateColumnFamilies), so a malformed batch is refused without reaching the WAL. Native writes it
            // to the WAL, fails the memtable insert and stops the DB with that background error
            // (DBImpl::HandleMemTableInsertFailure), so its next write fails too and the batch's keys stay invisible in
            // both. Compare everything but the next write in that case. The validation also reads the whole batch before
            // anything is applied, so with two problems in one batch (a refused entry, e.g. an inverted DeleteRange, then
            // a malformed record) the managed write reports the malformed record's Corruption where native reports the
            // first entry's status: both refuse the write; only the code can differ.
            string[] np = native.Split(' '), mp = managed.Split(' ');
            if (np[0] != "OK" && mp[0] != "OK" && np[3] == np[0] && mp[3] == "OK" && np[5] == "00000" && mp[5] == "00001")
            {
                managed = native;
            }
            // A Put tag flipped to kTypeBlobIndex leaves a (bogus) blob index in the memtable. Native 11.12's MemTable
            // SaveValue answers NotSupported "Encountered unexpected blob index"; main (11.14, which the port follows)
            // resolves memtable blob indexes for blob direct write (DBImpl::MaybeResolveMemtableBlobValue), so the bogus
            // index fails to decode: Corruption. Not comparable with the 11.12 package.
            native = native.Replace("[NotSupported]", "[Corruption]", StringComparison.Ordinal);
            if (native != managed) diffs.Add("iter=" + iter + " " + how + ": native=" + native + " | managed=" + managed);
            Directory.Delete(n1, true);
            Directory.Delete(m1, true);
        }
        Console.WriteLine("iterations " + iter + ", not given to native: " + skipped);
        foreach (string d in diffs.Take(40)) Console.WriteLine(d);
        Assert.AreEqual(0, diffs.Count, "seed=" + seed + ": " + diffs.Count + " differences, first: " + (diffs.Count > 0 ? diffs[0] : ""));
    }
}
