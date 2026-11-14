using System.Collections.Concurrent;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>
/// Best-efforts (point-in-time) recovery opens its tables through the table cache, with max_file_opening_threads
/// (VersionEditHandlerPointInTime::VerifyFile / MaybeCreateVersionBeforeApplyEdit, db/version_edit_handler.cc;
/// VersionSet::VerifyFileMetadata, db/version_set.cc; VersionBuilder::Rep::LoadTableHandlers, db/version_builder.cc).
/// Ports of DBBasicTest.RecoverWithMissingFiles, DBBasicTest.BestEffortRecoveryFailureWithTableCacheUseAfterFree
/// (db/db_basic_test.cc) and DBTest2.BestEffortsRecoveryWithSstUniqueIdVerification (db/db_etc2_test.cc), plus the
/// thread count, the recovered point in time with 1 and 8 threads and the reuse of the readers after the open.
/// </summary>
[TestClass]
public sealed class BestEffortsRecoveryTableCacheTests
{
    private const int KeysPerTable = 10;

    private static FaultInjectionFileSystem NewFaultFs() => new(NewFs());

    /// <summary>One L0 table per round: f{round}-{i} = v{round} and common = r{round}. Returns the tables oldest first.</summary>
    private static List<string> BuildTables(IFileSystem fs, int rounds)
    {
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { DisableAutoCompactions = true }))
        {
            for (int r = 0; r < rounds; r++)
            {
                for (int i = 0; i < KeysPerTable; i++) db.Put(B("f" + r + "-" + i), B("v" + r));
                db.Put(B("common"), B("r" + r));
                db.Flush();
            }
        }
        return TableFiles(fs, DbPath);
    }

    private static List<string> TableFiles(IFileSystem fs, string dir)
    {
        var tables = new List<(ulong, string)>();
        foreach (string child in fs.GetChildren(dir))
        {
            if (RocksDb.Managed.Db.FileNames.TryParseFileName(child, out ulong n, out RocksDb.Managed.Db.FileType t) && t == RocksDb.Managed.Db.FileType.TableFile)
                tables.Add((n, dir + "/" + child));
        }
        tables.Sort();
        return tables.ConvertAll(static x => x.Item2);
    }

    private static void Rewrite(IFileSystem fs, string path, byte[] data)
    {
        fs.DeleteFile(path);
        using IWritableFile w = fs.CreateWritable(path);
        w.Append(data);
        w.Sync();
    }

    private static byte[] ReadAll(IFileSystem fs, string path)
    {
        using IRandomAccessFile f = fs.OpenRandomAccess(path);
        var data = new byte[f.Length];
        f.Read(0, data);
        return data;
    }

    /// <summary>The size the MANIFEST records no longer matches (VerifyFileMetadata: "File size mismatch").</summary>
    private static void Truncate(IFileSystem fs, string path, int bytes)
    {
        byte[] data = ReadAll(fs, path);
        Rewrite(fs, path, data.AsSpan(0, data.Length - bytes).ToArray());
    }

    /// <summary>The footer's magic number is zeroed: same size, the table does not open (Corruption).</summary>
    private static void CorruptFooter(IFileSystem fs, string path)
    {
        byte[] data = ReadAll(fs, path);
        Array.Clear(data, data.Length - 8, 8);
        Rewrite(fs, path, data);
    }

    private static SortedDictionary<string, string> Dump(RocksDatabase db, ColumnFamily? cf = null)
    {
        var r = new SortedDictionary<string, string>(StringComparer.Ordinal);
        using Iterator it = cf is null ? db.NewIterator(null) : cf.NewIterator(null);
        for (it.SeekToFirst(); it.Valid; it.Next()) r[S(it.Key.ToArray())!] = S(it.Value.ToArray())!;
        return r;
    }

    /// <summary>The contents of the tables of <paramref name="rounds"/> (the WAL is not replayed by best-efforts recovery).</summary>
    private static SortedDictionary<string, string> Expected(IEnumerable<int> rounds)
    {
        var r = new SortedDictionary<string, string>(StringComparer.Ordinal);
        int newest = -1;
        foreach (int round in rounds)
        {
            for (int i = 0; i < KeysPerTable; i++) r["f" + round + "-" + i] = "v" + round;
            newest = Math.Max(newest, round);
        }
        if (newest >= 0) r["common"] = "r" + newest;
        return r;
    }

    private static DbOptions RecoveryOptions(IFileSystem fs, int threads, bool verifyUniqueId)
    {
        DbOptions o = NewOptions(fs);
        o.BestEffortsRecovery = true;
        o.MaxFileOpeningThreads = threads;
        o.VerifySstUniqueIdInManifest = verifyUniqueId;
        return o;
    }

    /// <summary>Opens the default column family without automatic compactions (no table is written or opened after the open).</summary>
    private static RocksDatabase OpenRecovered(DbOptions options) =>
        RocksDatabase.Open(DbPath, options, new ColumnFamilyOptions { DisableAutoCompactions = true });

    /// <summary>The rounds (table indexes) the recovered default column family holds.</summary>
    private static List<int> RecoveredRounds(RocksDatabase db, List<string> tables)
    {
        var rounds = new List<int>();
        foreach (LiveFileMetadata f in db.GetLiveFilesMetadata())
        {
            int round = tables.FindIndex(t => t == RocksDb.Managed.Db.FileNames.TableFileName([DbPath], f.FileNumber, 0));
            Assert.IsTrue(round >= 0, "unexpected live file " + f.Name);
            rounds.Add(round);
        }
        rounds.Sort();
        return rounds;
    }

    public enum Damage { DeleteNewest, DeleteMiddle, TruncateMiddle, CorruptFooterAndDeleteLater, CorruptNewestFooter }

    /// <summary>
    /// Missing, truncated and corrupted tables in different versions: the recovered point in time and its contents are the
    /// same with 1 and 8 file opening threads. With verify_sst_unique_id_in_manifest the files are opened when the edit
    /// adding them is replayed (VerifyFile), so a corrupt table counts as missing; without it only the versions about to
    /// be created open their tables (LoadTableHandlers), and a corrupt table skips that point in time.
    /// </summary>
    [TestMethod]
    [DataRow(Damage.DeleteNewest, true, new[] { 0, 1, 2, 3, 4 })]
    [DataRow(Damage.DeleteNewest, false, new[] { 0, 1, 2, 3, 4 })]
    [DataRow(Damage.DeleteMiddle, true, new[] { 0, 1 })]
    [DataRow(Damage.DeleteMiddle, false, new[] { 0, 1 })]
    [DataRow(Damage.TruncateMiddle, true, new[] { 0, 1, 2 })]
    [DataRow(Damage.TruncateMiddle, false, new[] { 0, 1, 2 })]
    // 2 corrupt, 4 missing. Verified: 2 is missing, so adding 3 is the negative edge and {0, 1} is kept. Not verified: adding
    // 5 is the negative edge, {0..3} cannot be loaded (2), and the final state is invalid: nothing is recovered.
    [DataRow(Damage.CorruptFooterAndDeleteLater, true, new[] { 0, 1 })]
    [DataRow(Damage.CorruptFooterAndDeleteLater, false, new int[0])]
    // Verified: the corrupt newest table is a missing L0 suffix. Not verified: the only version (the final one) cannot be
    // loaded and is dropped.
    [DataRow(Damage.CorruptNewestFooter, true, new[] { 0, 1, 2, 3, 4 })]
    [DataRow(Damage.CorruptNewestFooter, false, new int[0])]
    public void SamePointInTimeWithOneAndEightThreads(Damage damage, bool verifyUniqueId, int[] expectedRounds)
    {
        foreach (int threads in new[] { 1, 8 })
        {
            var fs = NewFs();
            List<string> tables = BuildTables(fs, 6);
            switch (damage)
            {
                case Damage.DeleteNewest: fs.DeleteFile(tables[5]); break;
                case Damage.DeleteMiddle: fs.DeleteFile(tables[2]); break;
                case Damage.TruncateMiddle: Truncate(fs, tables[3], 2); break;
                case Damage.CorruptFooterAndDeleteLater:
                    CorruptFooter(fs, tables[2]);
                    fs.DeleteFile(tables[4]);
                    break;
                default: CorruptFooter(fs, tables[5]); break;
            }
            using RocksDatabase db = OpenRecovered(RecoveryOptions(fs, threads, verifyUniqueId));
            CollectionAssert.AreEqual(expectedRounds, RecoveredRounds(db, tables).ToArray(), "threads: " + threads);
            CollectionAssert.AreEqual(Expected(expectedRounds).ToList(), Dump(db).ToList(), "threads: " + threads);
        }
    }

    /// <summary>
    /// Runs <paramref name="open"/> with a BeforeFindTable hook that records the thread of every LoadTableHandlers lookup;
    /// the first ones wait (up to 10 s) until <paramref name="expectedThreads"/> distinct threads are inside the loader.
    /// </summary>
    private static HashSet<int> ThreadsLoadingTables(int expectedThreads, Action open)
    {
        var threads = new HashSet<int>();
        using var allArrived = new ManualResetEventSlim(expectedThreads <= 1);
        RocksDb.Managed.Db.TestHooks.Set("VersionBuilder::Rep::LoadTableHandlers::BeforeFindTable", () =>
        {
            lock (threads)
            {
                threads.Add(Environment.CurrentManagedThreadId);
                if (threads.Count >= expectedThreads) allArrived.Set();
            }
            allArrived.Wait(TimeSpan.FromSeconds(10));
        });
        try
        {
            open();
        }
        finally
        {
            RocksDb.Managed.Db.TestHooks.Set("VersionBuilder::Rep::LoadTableHandlers::BeforeFindTable", null);
        }
        return threads;
    }

    /// <summary>
    /// The version a point in time creates loads its tables with max_file_opening_threads threads (the recovering thread
    /// and n - 1 more), each table once, into the table cache the DB then reads from.
    /// </summary>
    [TestMethod]
    [DataRow(1)]
    [DataRow(4)]
    [DoNotParallelize]
    public void RecoveryLoadsTablesWithMaxFileOpeningThreads(int threads)
    {
        var fs = NewFaultFs();
        List<string> tables = BuildTables(fs, 24);
        fs.DeleteFile(tables[23]);
        var opens = new ConcurrentDictionary<string, int>(StringComparer.Ordinal);
        fs.BeforeOpenRandomAccess = p => { if (p.EndsWith(".sst", StringComparison.Ordinal)) opens.AddOrUpdate(p, 1, static (_, n) => n + 1); };
        DbOptions o = RecoveryOptions(fs, threads, verifyUniqueId: false);
        o.SkipStatsUpdateOnDbOpen = true;
        RocksDatabase? db = null;
        HashSet<int> used = ThreadsLoadingTables(threads, () => db = OpenRecovered(o));
        using (db)
        {
            Assert.AreEqual(threads, used.Count, "threads that loaded tables");
            Assert.AreEqual(23, db!.DefaultColumnFamily.Data.TableCache.Count, "every table of the recovered version is open");
            Assert.AreEqual(23, opens.Count);
            foreach (KeyValuePair<string, int> kv in opens) Assert.AreEqual(1, kv.Value, kv.Key);
            CollectionAssert.AreEqual(Expected(Enumerable.Range(0, 23)).ToList(), Dump(db).ToList());
        }
    }

    /// <summary>
    /// The readers opened during the recovery (by VerifyFile with verify_sst_unique_id_in_manifest, by LoadTableHandlers
    /// otherwise) stay in the table cache: neither the rest of the open (version stats) nor the reads open a table again.
    /// </summary>
    [TestMethod]
    [DataRow(true, 1)]
    [DataRow(true, 8)]
    [DataRow(false, 1)]
    [DataRow(false, 8)]
    public void TablesOpenedDuringRecoveryAreReusedAfterwards(bool verifyUniqueId, int threads)
    {
        var fs = NewFaultFs();
        List<string> tables = BuildTables(fs, 6);
        fs.DeleteFile(tables[2]);
        var opens = new ConcurrentDictionary<string, int>(StringComparer.Ordinal);
        fs.BeforeOpenRandomAccess = p => { if (p.EndsWith(".sst", StringComparison.Ordinal)) opens.AddOrUpdate(p, 1, static (_, n) => n + 1); };
        using RocksDatabase db = OpenRecovered(RecoveryOptions(fs, threads, verifyUniqueId));
        CollectionAssert.AreEqual(new[] { 0, 1 }, RecoveredRounds(db, tables).ToArray());
        Assert.AreEqual(1, opens[tables[0]]);
        Assert.AreEqual(1, opens[tables[1]]);
        foreach (KeyValuePair<string, int> kv in opens) Assert.AreEqual(1, kv.Value, kv.Key);
        // VerifyFile opened the tables of the versions that were not kept as well; LoadTableHandlers only the kept ones.
        Assert.AreEqual(verifyUniqueId ? 5 : 2, opens.Count);
        CollectionAssert.AreEqual(Expected([0, 1]).ToList(), Dump(db).ToList());
        for (int i = 0; i < KeysPerTable; i++) Assert.AreEqual("v1", GetString(db, "f1-" + i));
        Assert.AreEqual(verifyUniqueId ? 5 : 2, opens.Count, "no table opened by the reads");
        foreach (KeyValuePair<string, int> kv in opens) Assert.AreEqual(1, kv.Value, "reopened by the reads: " + kv.Key);
    }

    /// <summary>
    /// An error that is neither a missing file nor a corruption (an I/O error) fails the recovery instead of counting the
    /// file as missing (VersionBuilder::Rep::ApplyFileAddition returns it; MaybeCreateVersionBeforeApplyEdit returns a
    /// LoadTableHandlers error other than Corruption / PathNotFound). A table that disappears between the replay of its
    /// edit and the load of the version (PathNotFound) skips that point in time.
    /// </summary>
    [TestMethod]
    [DataRow(true)]
    [DataRow(false)]
    public void TableOpenErrorsHaveTheirUpstreamStatus(bool verifyUniqueId)
    {
        var fs = NewFaultFs();
        List<string> tables = BuildTables(fs, 4);
        fs.BeforeOpenRandomAccess = p => { if (p == tables[1]) throw new IOException("injected read error"); };
        var ex = Assert.ThrowsExactly<RocksDbException>(() => OpenRecovered(RecoveryOptions(fs, 8, verifyUniqueId)).Dispose());
        Assert.AreEqual(StatusCode.IOError, ex.Code, ex.Message);
        StringAssert.Contains(ex.Message, "injected read error");

        // Not found when opened: with VerifyFile the table is missing (the newest one: an L0 suffix); without it the final
        // version cannot be loaded and nothing is recovered.
        fs.BeforeOpenRandomAccess = p => { if (p == tables[3]) throw new FileNotFoundException("gone", p); };
        using RocksDatabase db = OpenRecovered(RecoveryOptions(fs, 8, verifyUniqueId));
        int[] expected = verifyUniqueId ? [0, 1, 2] : [];
        CollectionAssert.AreEqual(expected, RecoveredRounds(db, tables).ToArray());
        CollectionAssert.AreEqual(Expected(expected).ToList(), Dump(db).ToList());
    }

    /// <summary>
    /// With a bounded table cache LoadTableHandlers opens at most min(16, capacity / 4) tables of a version on this initial
    /// load (kInitialLoadLimit), like upstream: a corrupt table beyond them does not skip the point in time. With an
    /// unbounded cache every table is loaded and the only version is dropped.
    /// </summary>
    [TestMethod]
    public void LoadLimitOfABoundedTableCache()
    {
        foreach (int maxOpenFiles in new[] { 20, -1 })
        {
            var fs = NewFs();
            List<string> tables = BuildTables(fs, 10);
            CorruptFooter(fs, tables[5]);
            DbOptions o = RecoveryOptions(fs, 4, verifyUniqueId: false);
            o.MaxOpenFiles = maxOpenFiles;
            o.SkipStatsUpdateOnDbOpen = true;
            using RocksDatabase db = OpenRecovered(o);
            if (maxOpenFiles < 0)
            {
                Assert.AreEqual(0, RecoveredRounds(db, tables).Count);
                continue;
            }
            // table_cache capacity 10: at most min(16, 10 / 4) = 2 tables are loaded; the version keeps all ten.
            CollectionAssert.AreEqual(Enumerable.Range(0, 10).ToArray(), RecoveredRounds(db, tables).ToArray());
            // Point lookups walk L0 newest first: f7-0 is found before the corrupt table, f4-0 is not.
            Assert.AreEqual("v7", GetString(db, "f7-0"));
            var ex = Assert.ThrowsExactly<RocksDbException>(() => db.Get(B("f4-0")));
            Assert.AreEqual(StatusCode.Corruption, ex.Code, ex.Message);
        }
    }

    /// <summary>
    /// Port of DBBasicTest.RecoverWithMissingFiles: three column families of three L0 tables (a, b, c); in each the newest
    /// table is truncated, and in column family i the tables from i to the second newest are deleted. Every column family
    /// keeps the tables before its first damaged one.
    /// </summary>
    [TestMethod]
    [DataRow(1, true)]
    [DataRow(8, true)]
    [DataRow(1, false)]
    [DataRow(8, false)]
    public void RecoverWithMissingFiles(int threads, bool verifyUniqueId)
    {
        var fs = NewFs();
        string[] names = ["default", "pikachu", "eevee"];
        var cfOptions = new ColumnFamilyOptions { DisableAutoCompactions = true };
        var descriptors = Array.ConvertAll(names, n => new ColumnFamilyDescriptor(n, cfOptions));
        DbOptions create = NewOptions(fs);
        create.CreateMissingColumnFamilies = true;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, create, descriptors))
        {
            foreach (string name in names)
            {
                ColumnFamily cf = db.GetColumnFamily(name);
                foreach (string k in new[] { "a", "b", "c" })
                {
                    cf.Put(B(k), B("0_value"));
                    cf.Flush();
                }
            }
        }
        var files = new Dictionary<string, List<string>>();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), descriptors))
        {
            foreach (LiveFileMetadata f in db.GetLiveFilesMetadata())
            {
                if (!files.TryGetValue(f.ColumnFamilyName, out List<string>? list)) files[f.ColumnFamilyName] = list = new List<string>();
                list.Add(RocksDb.Managed.Db.FileNames.TableFileName([DbPath], f.FileNumber, 0));
            }
        }
        for (int i = 0; i < names.Length; i++)
        {
            List<string> list = files[names[i]];
            list.Sort(StringComparer.Ordinal);
            Assert.AreEqual(3, list.Count);
            Truncate(fs, list[^1], 2);
            for (int j = list.Count - 2; j >= i; j--) fs.DeleteFile(list[j]);
        }
        DbOptions o = RecoveryOptions(fs, threads, verifyUniqueId);
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o, descriptors))
        {
            Assert.AreEqual(0, Dump(db, db.GetColumnFamily("default")).Count);
            CollectionAssert.AreEqual(new[] { "a" }, Dump(db, db.GetColumnFamily("pikachu")).Keys.ToArray());
            CollectionAssert.AreEqual(new[] { "a", "b" }, Dump(db, db.GetColumnFamily("eevee")).Keys.ToArray());
        }
    }

    /// <summary>
    /// Port of DBTest2.BestEffortsRecoveryWithSstUniqueIdVerification: table k of seven carries another DB's unique id
    /// (copied from a twin DB written the same way, so its size and contents match the MANIFEST). A regular open fails with
    /// Corruption; best-efforts recovery opens it through the table cache (VerifyFileMetadata with
    /// verify_sst_unique_id_in_manifest), counts it as missing and recovers the tables before it, which then reopen normally.
    /// </summary>
    [TestMethod]
    [DataRow(0)]
    [DataRow(3)]
    [DataRow(6)]
    public void BestEffortsRecoveryWithSstUniqueIdVerification(int k)
    {
        const string twin = "/twin";
        var fs = NewFs();
        fs.CreateDirectoryIfMissing(twin);
        foreach (string path in new[] { DbPath, twin })
        {
            using RocksDatabase db = RocksDatabase.Open(path, NewOptions(fs), new ColumnFamilyOptions { DisableAutoCompactions = true });
            for (int i = 0; i < 7; i++)
            {
                for (int j = 0; j < KeysPerTable; j++) db.Put(B(j.ToString(System.Globalization.CultureInfo.InvariantCulture)), B("v" + i));
                db.Flush();
            }
        }
        List<string> tables = TableFiles(fs, DbPath);
        List<string> twinTables = TableFiles(fs, twin);
        Assert.AreEqual(tables[k][(tables[k].LastIndexOf('/') + 1)..], twinTables[k][(twinTables[k].LastIndexOf('/') + 1)..]);
        byte[] foreign = ReadAll(fs, twinTables[k]);
        Assert.AreEqual(fs.GetFileSize(tables[k]), foreign.Length, "the twin table has the size the MANIFEST records");
        Rewrite(fs, tables[k], foreign);

        var ex = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, NewOptions(fs)).Dispose());
        Assert.AreEqual(StatusCode.Corruption, ex.Code, ex.Message);
        StringAssert.Contains(ex.Message, "Mismatch in unique ID");

        void AssertDb(RocksDatabase db)
        {
            SortedDictionary<string, string> contents = Dump(db);
            Assert.AreEqual(k == 0 ? 0 : KeysPerTable, contents.Count);
            foreach (string v in contents.Values) Assert.AreEqual("v" + (k - 1), v);
        }
        using (RocksDatabase db = OpenRecovered(RecoveryOptions(fs, 8, verifyUniqueId: true))) AssertDb(db);
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs))) AssertDb(db);
    }

    /// <summary>
    /// Port of DBBasicTest.BestEffortRecoveryFailureWithTableCacheUseAfterFree: the newest MANIFEST fails after its tables
    /// were opened (a record naming an unknown column family), and the recovery moves on to the older one
    /// (VersionSet::TryRecover). The readers the failed attempt opened stay usable and cached: no table is opened twice,
    /// and every key reads.
    /// </summary>
    [TestMethod]
    [DataRow(true)]
    [DataRow(false)]
    public void FailedManifestAttemptKeepsTheTableCacheUsable(bool verifyUniqueId)
    {
        var fs = NewFaultFs();
        List<string> tables = BuildTables(fs, 6);
        string? manifest = null;
        foreach (string child in fs.GetChildren(DbPath))
        {
            if (RocksDb.Managed.Db.FileNames.TryParseFileName(child, out _, out RocksDb.Managed.Db.FileType t) && t == RocksDb.Managed.Db.FileType.DescriptorFile)
            {
                Assert.IsNull(manifest, "one MANIFEST");
                manifest = DbPath + "/" + child;
            }
        }
        using (var reader = RocksDb.Managed.Db.ManifestLogReader.Open(fs, manifest!))
        using (var writer = RocksDb.Managed.Db.ManifestLogWriter.Create(fs, RocksDb.Managed.Db.FileNames.DescriptorFileName(DbPath, 1000)))
        {
            while (reader.TryReadRecord(out ReadOnlySpan<byte> record)) writer.AddRecordAsync(record.ToArray(), default).AsTask().GetAwaiter().GetResult();
            byte[] unknownCf = new RocksDb.Managed.Db.VersionEdit { ColumnFamily = 77, LogNumber = 1 }.Encode(0);
            writer.AddRecordAsync(unknownCf, default).AsTask().GetAwaiter().GetResult();
            writer.SyncAsync(default).AsTask().GetAwaiter().GetResult();
        }
        var opens = new ConcurrentDictionary<string, int>(StringComparer.Ordinal);
        fs.BeforeOpenRandomAccess = p => { if (p.EndsWith(".sst", StringComparison.Ordinal)) opens.AddOrUpdate(p, 1, static (_, n) => n + 1); };
        using RocksDatabase db = OpenRecovered(RecoveryOptions(fs, 8, verifyUniqueId));
        CollectionAssert.AreEqual(Enumerable.Range(0, 6).ToArray(), RecoveredRounds(db, tables).ToArray());
        CollectionAssert.AreEqual(Expected(Enumerable.Range(0, 6)).ToList(), Dump(db).ToList());
        Assert.AreEqual(6, opens.Count, string.Join(",", opens.Keys));
        foreach (KeyValuePair<string, int> kv in opens) Assert.AreEqual(1, kv.Value, kv.Key);
    }
}
