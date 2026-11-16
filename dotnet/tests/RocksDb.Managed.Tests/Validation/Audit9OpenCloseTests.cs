using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Tests.Db;
using RocksDb.Managed.Utilities.Transactions;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #9: opening and closing a DB (DB::Open / DBImpl::Open / DBImpl::Recover in db/db_impl/db_impl_open.cc,
/// DBImpl::Close / CloseHelper in db/db_impl/db_impl.cc, the read-only and secondary opens, DestroyDB): crash
/// consistency of the open and close sequences, what a failed open leaves behind, and the close semantics.
/// </summary>
[TestClass]
public sealed class Audit9OpenCloseTests
{
    private const string Path = "/audit9";
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);

    private static MemoryFileSystem NewMemFs()
    {
        var fs = new MemoryFileSystem();
        fs.CreateDirectoryIfMissing(Path);
        fs.SyncDirectory("/");
        return fs;
    }

    private static DbOptions Options(IFileSystem fs) => new() { CreateIfMissing = true, FileSystem = fs };

    private sealed class BlockingShutdownListener : EventListener
    {
        public readonly ManualResetEventSlim Entered = new(false);
        public readonly ManualResetEventSlim Release = new(false);

        public override void OnDBShutdownBegin(RocksDatabase? db)
        {
            Entered.Set();
            Release.Wait(Timeout);
        }
    }

    // ---- close semantics ---------------------------------------------------------------------------------------

    /// <summary>
    /// DBImpl::Close / ~DBImpl (db/db_impl/db_impl.cc): both take closing_mutex_, so a second close made while the first
    /// one runs waits until the first one has finished (and with it UnlockFile of the LOCK) and returns its status. The
    /// port returned at once from the second Dispose, so a caller reopening right after its own Dispose found the LOCK
    /// still held.
    /// </summary>
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task ConcurrentSecondCloseReturnsOnlyOnceTheFirstReleasedTheLock(bool async)
    {
        var fs = new FaultInjectionFileSystem(NewMemFs());
        var listener = new BlockingShutdownListener();
        DbOptions o = Options(fs);
        o.Listeners.Add(listener);
        RocksDatabase db = RocksDatabase.Open(Path, o);
        db.Put(B("k"), B("v"));
        Task first = Task.Run(() => db.Dispose());
        Assert.IsTrue(listener.Entered.Wait(Timeout), "first close did not start");
        Task<Exception?> second = Task.Run<Exception?>(async () =>
        {
            if (async) await db.DisposeAsync().ConfigureAwait(false);
            else db.Dispose();
            try
            {
                using RocksDatabase again = RocksDatabase.Open(Path, Options(fs));
                return null;
            }
            catch (RocksDbException e)
            {
                return e;
            }
        });
        // The second close must not complete while the first is still closing.
        bool completedEarly = second.Wait(TimeSpan.FromMilliseconds(300));
        listener.Release.Set();
        await first.WaitAsync(Timeout).ConfigureAwait(false);
        Exception? reopenError = await second.WaitAsync(Timeout).ConfigureAwait(false);
        Assert.IsFalse(completedEarly && reopenError is not null, "second close returned before the LOCK was released: " + reopenError);
        Assert.IsNull(reopenError, "reopen after the second close failed: " + reopenError);
        Assert.AreEqual(0, fs.OpenLocks);
        Assert.AreEqual(0, fs.OpenFiles);
    }

    /// <summary>
    /// ErrorHandler::RecoverFromRetryableBGIOError waits bgerror_resume_retry_interval between resumes (a uint64 of
    /// microseconds, any value accepted), and CloseHelper's CancelErrorRecoveryForShutDown / EndAutoRecovery never fails
    /// the close because of the recovery thread. The port converted the interval with TimeSpan.FromMicroseconds, which
    /// throws OverflowException for values above TimeSpan.MaxValue: the recovery task faulted, and Close rethrew the
    /// OverflowException (EndAutoRecoveryAsync filtered only RocksDbException / IOException / ObjectDisposedException /
    /// OperationCanceledException) before closing anything: the LOCK and every file stayed open, and every later Dispose
    /// threw again.
    /// </summary>
    [TestMethod]
    public void CloseCompletesWhenTheAutoRecoveryTaskFailedWithAnUnexpectedException()
    {
        var fs = new FaultInjectionFileSystem(NewMemFs());
        DbOptions o = Options(fs);
        o.BgErrorResumeRetryInterval = ulong.MaxValue;
        RocksDatabase db = RocksDatabase.Open(Path, o);
        db.Put(B("a"), B("1"));
        fs.FailMatching(".sst", FaultOps.Append, 1, InjectedFaultKind.Retryable);
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
        Assert.AreEqual(StatusCode.IOError, ex.Code);
        db.Dispose();
        Assert.AreEqual(0, fs.OpenLocks, "LOCK not released");
        Assert.AreEqual(0, fs.OpenFiles, "file handles leaked");
        using RocksDatabase again = RocksDatabase.Open(Path, Options(fs));
        Assert.AreEqual("1", S(again.Get(B("a"))));
    }

    // ---- crash consistency of DB::Open ---------------------------------------------------------------------------

    /// <summary>What the DB looks like before the open that crashes, and how that open is configured.</summary>
    public enum OpenScenario
    {
        /// <summary>WALs with unflushed data of two column families; the open replays and flushes them.</summary>
        ReplayAndFlush,
        /// <summary>avoid_flush_during_recovery: the replayed data stays in the memtables, the WALs are kept.</summary>
        AvoidFlushDuringRecovery,
        /// <summary>Opened with small memtables: the replay flushes in the middle (MaybeWriteLevel0TableForRecovery).</summary>
        FlushesDuringReplay,
        /// <summary>cf1 was flushed before the close: only the default column family has data in the WALs.</summary>
        OneColumnFamilyFlushed,
        /// <summary>The open adds a column family (create_missing_column_families).</summary>
        CreateMissingColumnFamily,
        /// <summary>recycle_log_file_num = 2.</summary>
        WalRecycling,
        /// <summary>A wal_dir separate from the DB directory.</summary>
        SeparateWalDir,
        /// <summary>track_and_verify_wals_in_manifest.</summary>
        WalsTrackedInManifest,
        /// <summary>The previous process was killed (no close) after its last sync write.</summary>
        AfterProcessKill,
        /// <summary>best_efforts_recovery (everything flushed before the close).</summary>
        BestEffortsRecovery,
        /// <summary>The first open of a new DB (create_if_missing).</summary>
        NewDb,
        /// <summary>A MANIFEST over max_manifest_file_size: the open rolls it and deletes the old one.</summary>
        ManifestRoll,
    }

    private sealed class NullLogger() : InfoLogger(InfoLogLevel.Info)
    {
        protected override void LogCore(InfoLogLevel level, string message)
        {
        }
    }

    private const int OpenNumKeys = 40;
    private const string WalDirPath = "/audit9wal";

    private readonly record struct Op(int Cf, int Kind, int Key, int KeyEnd, int Value);

    private static List<Op> MakeOps(int seed, int count)
    {
        var rnd = new Random(seed);
        var ops = new List<Op>(count);
        for (int i = 0; i < count; i++)
        {
            int r = rnd.Next(100);
            int key = rnd.Next(OpenNumKeys);
            ops.Add(new Op(rnd.Next(3) == 0 ? 1 : 0, r < 80 ? 0 : r < 94 ? 1 : 2, key, Math.Min(OpenNumKeys, key + 1 + rnd.Next(4)), i));
        }
        return ops;
    }

    private static byte[] Key(int k) => B("k" + k.ToString("D3", System.Globalization.CultureInfo.InvariantCulture));

    private static byte[] Value(int key, int value) =>
        B("v" + key.ToString(System.Globalization.CultureInfo.InvariantCulture) + "-" + value.ToString(System.Globalization.CultureInfo.InvariantCulture) + "-" +
          new string('x', 600 + value % 400));

    private static void Apply(Dictionary<int, int>[] model, Op op)
    {
        Dictionary<int, int> m = model[op.Cf];
        if (op.Kind == 0) m[op.Key] = op.Value;
        else if (op.Kind == 1) m.Remove(op.Key);
        else
        {
            for (int k = op.Key; k < op.KeyEnd; k++) m.Remove(k);
        }
    }

    private static void Write(RocksDatabase db, ColumnFamily cf1, Op op, WriteOptions wo)
    {
        ColumnFamily cf = op.Cf == 0 ? db.DefaultColumnFamily : cf1;
        if (op.Kind == 0) cf.Put(wo, Key(op.Key), Value(op.Key, op.Value));
        else if (op.Kind == 1) cf.Delete(wo, Key(op.Key));
        else cf.DeleteRange(wo, Key(op.Key), Key(op.KeyEnd));
    }

    private static ColumnFamilyOptions OpenCfOptions(ulong writeBufferSize) => new()
    {
        WriteBufferSize = writeBufferSize,
        Level0FileNumCompactionTrigger = 3,
        TargetFileSizeBase = 64 << 10,
        MaxBytesForLevelBase = 256 << 10,
        Compression = RocksDb.Managed.Compression.CompressionType.None,
    };

    private static DbOptions ScenarioOptions(IFileSystem fs, OpenScenario s)
    {
        DbOptions o = Options(fs);
        o.InfoLog = new NullLogger(); // the info LOG's appends are not crash points of interest
        o.CreateMissingColumnFamilies = true;
        o.AvoidFlushDuringRecovery = s == OpenScenario.AvoidFlushDuringRecovery;
        o.RecycleLogFileNum = s == OpenScenario.WalRecycling ? 2UL : 0;
        o.WalDir = s == OpenScenario.SeparateWalDir ? WalDirPath : "";
        o.TrackAndVerifyWalsInManifest = s == OpenScenario.WalsTrackedInManifest;
        o.BestEffortsRecovery = s == OpenScenario.BestEffortsRecovery;
        if (s == OpenScenario.ManifestRoll)
        {
            o.MaxManifestFileSize = 1024;
            o.MaxManifestSpaceAmpPct = 0;
        }
        return o;
    }

    private static List<ColumnFamilyDescriptor> ScenarioColumnFamilies(OpenScenario s, bool sweptOpen)
    {
        ulong wbs = sweptOpen && s == OpenScenario.FlushesDuringReplay ? 64UL << 10 : 8UL << 20;
        var list = new List<ColumnFamilyDescriptor>
        {
            new("default", OpenCfOptions(wbs)),
            new("cf1", OpenCfOptions(wbs)),
        };
        if (sweptOpen && s == OpenScenario.CreateMissingColumnFamily) list.Add(new ColumnFamilyDescriptor("cf2", OpenCfOptions(wbs)));
        return list;
    }

    /// <summary>
    /// Builds the DB the swept open starts from: <paramref name="ops"/> written as sync writes over two column families
    /// (with a flush of the default column family in the middle, so there are several WALs), then closed (or the process
    /// killed). Every write is durable. Returns the DB id.
    /// </summary>
    private static string BuildDbBeforeOpen(FaultInjectionFileSystem fs, OpenScenario s, List<Op> ops)
    {
        if (s == OpenScenario.NewDb) return "";
        RocksDatabase db = RocksDatabase.Open(Path, ScenarioOptions(fs, s), ScenarioColumnFamilies(s, sweptOpen: false));
        ColumnFamily cf1 = db.GetColumnFamily("cf1");
        var wo = new WriteOptions { Sync = true };
        for (int i = 0; i < ops.Count; i++)
        {
            Write(db, cf1, ops[i], wo);
            if (i == ops.Count / 2) db.Flush();
            if (s == OpenScenario.ManifestRoll && i % 20 == 0) cf1.Flush();
        }
        if (s == OpenScenario.OneColumnFamilyFlushed) cf1.Flush();
        if (s == OpenScenario.BestEffortsRecovery)
        {
            db.Flush();
            cf1.Flush();
        }
        string id = db.DbId;
        if (s == OpenScenario.AfterProcessKill)
        {
            fs.CrashAtOperation = fs.Operations + 1;
            try { db.Dispose(); }
            catch (Exception e) when (e is RocksDbException or IOException) { }
            FaultInjectionFileSystem.ProcessKill();
            fs.Restart();
        }
        else
        {
            db.Dispose();
        }
        return id;
    }

    private static Dictionary<int, int>[] ReadAll(RocksDatabase db, ColumnFamily cf1, string context)
    {
        var result = new Dictionary<int, int>[] { new(), new() };
        for (int c = 0; c < 2; c++)
        {
            ColumnFamily cf = c == 0 ? db.DefaultColumnFamily : cf1;
            for (int k = 0; k < OpenNumKeys; k++)
            {
                string? v = S(cf.Get(Key(k)));
                if (v is null) continue;
                string[] parts = v.Split('-');
                Assert.AreEqual("v" + k.ToString(System.Globalization.CultureInfo.InvariantCulture), parts[0], "value under the wrong key: " + context);
                result[c][k] = int.Parse(parts[1], System.Globalization.CultureInfo.InvariantCulture);
            }
            using Iterator it = cf.NewIterator();
            int n = 0;
            for (it.SeekToFirst(); it.Valid; it.Next())
            {
                if (it.Key[0] == (byte)'k') n++;
            }
            Assert.AreEqual(result[c].Count, n, "iterator / Get mismatch in cf" + c + ": " + context);
        }
        return result;
    }

    private static void AssertModel(Dictionary<int, int>[] expected, Dictionary<int, int>[] actual, string context)
    {
        for (int c = 0; c < 2; c++)
        {
            foreach ((int k, int v) in expected[c])
            {
                Assert.IsTrue(actual[c].TryGetValue(k, out int a), "lost cf" + c + " key " + k + ": " + context);
                Assert.AreEqual(v, a, "wrong version of cf" + c + " key " + k + ": " + context);
            }
            foreach (int k in actual[c].Keys)
                Assert.IsTrue(expected[c].ContainsKey(k), "resurrected or invented cf" + c + " key " + k + ": " + context);
        }
    }

    /// <summary>
    /// One crash point of an open sweep: the open (and, if it succeeds, the close) crashes at mutating operation
    /// <paramref name="crashAt"/>; returns whether a crash happened. The failed open releases the LOCK and every file
    /// handle; after the power loss / process kill the DB opens twice with exactly the durable writes, keeps its DB id
    /// and accepts a sync write and a flush.
    /// </summary>
    private static bool CrashDuringOpen(OpenScenario s, int seed, long crashAt, bool powerLoss, bool transient)
    {
        string context = s + " seed=" + seed + " crashAt=" + crashAt + " powerLoss=" + powerLoss + " transient=" + transient;
        List<Op> ops = MakeOps(seed, 160);
        var fs = new FaultInjectionFileSystem(NewMemFs());
        string dbId = BuildDbBeforeOpen(fs, s, ops);
        var model = new Dictionary<int, int>[] { new(), new() };
        if (s != OpenScenario.NewDb)
        {
            foreach (Op op in ops) Apply(model, op);
        }
        fs.TornWrites = !powerLoss && !transient;
        fs.TransientFailure = transient;
        fs.CrashAtOperation = fs.Operations + crashAt;
        try
        {
            RocksDatabase db = RocksDatabase.Open(Path, ScenarioOptions(fs, s), ScenarioColumnFamilies(s, sweptOpen: true));
            try { db.Dispose(); }
            catch (Exception e) when (e is RocksDbException or IOException) { }
        }
        catch (Exception e) when (e is RocksDbException or IOException)
        {
            Assert.IsTrue(fs.Crashed, "open failed without a crash: " + context + ": " + e);
        }
        bool crashed = fs.Crashed;
        context += " at=" + fs.CrashedAt;
        Assert.AreEqual(0, fs.OpenLocks, "LOCK not released: " + context);
        Assert.AreEqual(0, fs.OpenFiles, "file handles leaked: " + context);
        if (powerLoss) fs.PowerLoss();
        else FaultInjectionFileSystem.ProcessKill();
        fs.Restart();
        fs.TornWrites = false;
        fs.TransientFailure = false;
        for (int round = 0; round < 2; round++)
        {
            RocksDatabase db2;
            try
            {
                db2 = RocksDatabase.Open(Path, ScenarioOptions(fs, s), ScenarioColumnFamilies(s, sweptOpen: true));
            }
            catch (Exception e)
            {
                Assert.Fail("reopen " + round + " failed: " + context + ": " + e);
                throw;
            }
            using (db2)
            {
                ColumnFamily cf1 = db2.GetColumnFamily("cf1");
                AssertModel(model, ReadAll(db2, cf1, context), context + " round=" + round);
                if (dbId.Length > 0) Assert.AreEqual(dbId, db2.DbId, "DB id changed: " + context);
                if (round == 0)
                {
                    db2.Put(new WriteOptions { Sync = true }, B("after"), B("crash"));
                    db2.Flush();
                }
                else
                {
                    Assert.AreEqual("crash", S(db2.Get(B("after"))), context);
                }
            }
        }
        return crashed;
    }

    private static void RunOpenSweep(OpenScenario s, int seed)
    {
        // Power loss, process kill (torn last write), and a single failed operation in a process that goes on (the
        // failed open's cleanup runs on a working file system, then the DB is opened again).
        foreach ((bool powerLoss, bool transient) in new[] { (true, false), (false, false), (false, true) })
        {
            long crashAt = 1;
            for (; crashAt < 600; crashAt++)
            {
                if (!CrashDuringOpen(s, seed, crashAt, powerLoss, transient)) break;
            }
            Assert.IsTrue(crashAt > 3 && crashAt < 600, s + ": unexpected number of crash points " + crashAt);
        }
    }

    /// <summary>
    /// DB::Open (db_impl_open.cc: DBImpl::Open, Recover, RecoverLogFiles, WriteLevel0TableForRecovery,
    /// LogAndApplyForRecovery, NewDB, SetIdentityFile / SetCurrentFile, WriteOptionsFile, DeleteObsoleteFiles): a power
    /// loss or a process kill (torn last write) at every mutating file operation of an open, or of the close right after
    /// it, leaves a DB that opens with every write that was durable before, nothing resurrected, the same DB id, and no
    /// handle or LOCK held.
    /// </summary>
    [TestMethod]
    [DataRow(OpenScenario.ReplayAndFlush)]
    [DataRow(OpenScenario.AvoidFlushDuringRecovery)]
    [DataRow(OpenScenario.FlushesDuringReplay)]
    [DataRow(OpenScenario.OneColumnFamilyFlushed)]
    [DataRow(OpenScenario.CreateMissingColumnFamily)]
    [DataRow(OpenScenario.WalRecycling)]
    [DataRow(OpenScenario.SeparateWalDir)]
    [DataRow(OpenScenario.WalsTrackedInManifest)]
    [DataRow(OpenScenario.AfterProcessKill)]
    [DataRow(OpenScenario.BestEffortsRecovery)]
    [DataRow(OpenScenario.NewDb)]
    [DataRow(OpenScenario.ManifestRoll)]
    public void CrashAtEveryStepOfOpenKeepsDurableData(OpenScenario s) => RunOpenSweep(s, seed: 9 + (int)s);

    // ---- crash consistency of DB::Close --------------------------------------------------------------------------

    /// <summary>The state of the DB when it is closed.</summary>
    public enum CloseScenario
    {
        /// <summary>Unflushed memtables of two column families, every fourth write synced.</summary>
        UnflushedMemtables,
        /// <summary>manual_wal_flush: writes since the last sync are only in the WAL buffer, which close writes out.</summary>
        ManualWalFlush,
        /// <summary>cf1 holds disableWAL writes: the shutdown flush (avoid_flush_during_shutdown = false) persists them.</summary>
        ShutdownFlushOfUnloggedWrites,
        /// <summary>avoid_flush_during_shutdown with disableWAL writes in cf1: they are lost, and nothing else.</summary>
        AvoidFlushDuringShutdown,
        /// <summary>A compaction is picked right before the close.</summary>
        PendingCompaction,
        /// <summary>A dropped column family whose files close deletes.</summary>
        DroppedColumnFamily,
    }

    private static DbOptions CloseOptions(IFileSystem fs, CloseScenario s)
    {
        DbOptions o = Options(fs);
        o.InfoLog = new NullLogger();
        o.CreateMissingColumnFamilies = true;
        o.ManualWalFlush = s == CloseScenario.ManualWalFlush;
        o.AvoidFlushDuringShutdown = s == CloseScenario.AvoidFlushDuringShutdown;
        return o;
    }

    private static List<ColumnFamilyDescriptor> CloseColumnFamilies() =>
        [new("default", OpenCfOptions(8UL << 20)), new("cf1", OpenCfOptions(8UL << 20))];

    /// <summary>
    /// One crash point of a close sweep. Returns whether the close crashed. Model: the WAL-logged writes in the default
    /// column family (and cf1 unless the scenario writes cf1 without the WAL) form a prefix of the issued writes that
    /// includes every write up to the last sync write (or flush); cf1's unlogged writes are all there or none.
    /// </summary>
    private static bool CrashDuringClose(CloseScenario s, int seed, long crashAt, bool powerLoss)
    {
        string context = s + " seed=" + seed + " crashAt=" + crashAt + " powerLoss=" + powerLoss;
        bool unloggedCf1 = s is CloseScenario.ShutdownFlushOfUnloggedWrites or CloseScenario.AvoidFlushDuringShutdown;
        List<Op> ops = MakeOps(seed, 120);
        var fs = new FaultInjectionFileSystem(NewMemFs());
        RocksDatabase db = RocksDatabase.Open(Path, CloseOptions(fs, s), CloseColumnFamilies());
        ColumnFamily cf1 = db.GetColumnFamily("cf1");
        int durable = 0;
        if (s == CloseScenario.DroppedColumnFamily)
        {
            ColumnFamily cf2 = db.CreateColumnFamily("cf2", OpenCfOptions(8UL << 20));
            for (int i = 0; i < 20; i++) cf2.Put(Key(i), Value(i, i));
            cf2.Flush();
            db.DropColumnFamily(cf2);
        }
        for (int i = 0; i < ops.Count; i++)
        {
            Op op = ops[i];
            bool unlogged = unloggedCf1 && op.Cf == 1;
            bool sync = !unlogged && i % 4 == 3;
            Write(db, cf1, op, new WriteOptions { Sync = sync, DisableWal = unlogged });
            if (sync) durable = i + 1;
            if (s == CloseScenario.PendingCompaction && i % 30 == 29)
            {
                db.Flush();
                cf1.Flush();
                durable = i + 1;
            }
        }
        var expectedUnlogged = new Dictionary<int, int>();
        foreach (Op op in ops)
        {
            if (unloggedCf1 && op.Cf == 1)
            {
                var m = new Dictionary<int, int>[] { expectedUnlogged, expectedUnlogged };
                Apply(m, op);
            }
        }
        fs.TornWrites = !powerLoss;
        fs.CrashAtOperation = fs.Operations + crashAt;
        try { db.Dispose(); }
        catch (Exception e) when (e is RocksDbException or IOException) { }
        bool crashed = fs.Crashed;
        context += " at=" + fs.CrashedAt;
        Assert.AreEqual(0, fs.OpenLocks, "LOCK not released: " + context);
        Assert.AreEqual(0, fs.OpenFiles, "file handles leaked: " + context);
        if (powerLoss) fs.PowerLoss();
        else FaultInjectionFileSystem.ProcessKill();
        fs.Restart();
        fs.TornWrites = false;
        for (int round = 0; round < 2; round++)
        {
            RocksDatabase db2;
            try
            {
                db2 = RocksDatabase.Open(Path, CloseOptions(fs, s), CloseColumnFamilies());
            }
            catch (Exception e)
            {
                Assert.Fail("reopen " + round + " failed: " + context + ": " + e);
                throw;
            }
            using (db2)
            {
                ColumnFamily c1 = db2.GetColumnFamily("cf1");
                Dictionary<int, int>[] actual = ReadAll(db2, c1, context);
                if (unloggedCf1)
                {
                    // cf1: the unlogged writes are all there (the shutdown flush committed) or none.
                    bool all = actual[1].Count == expectedUnlogged.Count && actual[1].All(kv => expectedUnlogged.TryGetValue(kv.Key, out int v) && v == kv.Value);
                    Assert.IsTrue(all || actual[1].Count == 0, "cf1 holds part of its unlogged writes: " + context);
                    if (s == CloseScenario.AvoidFlushDuringShutdown) Assert.AreEqual(0, actual[1].Count, "avoid_flush_during_shutdown flushed: " + context);
                    if (s == CloseScenario.ShutdownFlushOfUnloggedWrites && !crashed) Assert.IsTrue(all, "the shutdown flush lost writes: " + context);
                }
                // The logged writes: a prefix in [durable, all].
                var model = new Dictionary<int, int>[] { new(), new() };
                int matched = -1;
                for (int p = 0; p <= ops.Count; p++)
                {
                    if (p > 0 && !(unloggedCf1 && ops[p - 1].Cf == 1)) Apply(model, ops[p - 1]);
                    if (p < durable) continue;
                    bool equal = model[0].Count == actual[0].Count && model[0].All(kv => actual[0].TryGetValue(kv.Key, out int v) && v == kv.Value);
                    if (!unloggedCf1)
                        equal &= model[1].Count == actual[1].Count && model[1].All(kv => actual[1].TryGetValue(kv.Key, out int v) && v == kv.Value);
                    if (equal)
                    {
                        matched = p;
                        break;
                    }
                }
                Assert.AreNotEqual(-1, matched, "recovered state is not a prefix in [" + durable + ", " + ops.Count + "]: " + context);
                if (!crashed && !powerLoss) Assert.AreEqual(ops.Count, matched, "a clean close lost writes: " + context);
                if (s == CloseScenario.DroppedColumnFamily)
                {
                    Assert.IsFalse(RocksDatabase.ListColumnFamilies(Path, fs).Contains("cf2"), "dropped column family came back: " + context);
                }
            }
        }
        return crashed;
    }

    /// <summary>
    /// DBImpl::CloseHelper (db/db_impl/db_impl.cc: CancelAllBackgroundWork with the shutdown flush, the wait for
    /// background work, FindObsoleteFiles / PurgeObsoleteFiles, logs_ ClearWriter, VersionSet::Close, UnlockFile): a power
    /// loss or a process kill at every mutating file operation of a close leaves a DB that opens with every write that
    /// was durable (synced or flushed) before the close and a consistent prefix of the rest; the close releases the LOCK
    /// and every handle even when it fails.
    /// </summary>
    [TestMethod]
    [DataRow(CloseScenario.UnflushedMemtables)]
    [DataRow(CloseScenario.ManualWalFlush)]
    [DataRow(CloseScenario.ShutdownFlushOfUnloggedWrites)]
    [DataRow(CloseScenario.AvoidFlushDuringShutdown)]
    [DataRow(CloseScenario.PendingCompaction)]
    [DataRow(CloseScenario.DroppedColumnFamily)]
    public void CrashAtEveryStepOfCloseKeepsDurableData(CloseScenario s)
    {
        foreach (bool powerLoss in new[] { true, false })
        {
            long crashAt = 1;
            for (; crashAt < 400; crashAt++)
            {
                if (!CrashDuringClose(s, seed: 31 + (int)s, crashAt, powerLoss)) break;
            }
            Assert.IsTrue(crashAt < 400, s + ": unexpected number of crash points " + crashAt);
        }
    }

    // ---- read-only and secondary ---------------------------------------------------------------------------------

    /// <summary>
    /// DBImplReadOnly (db/db_impl/db_impl_readonly.cc): a read-only open replays the WALs into memtables and never
    /// creates, writes, renames or deletes a file -- also not on close (CloseHelper skips the purge with read_only_), and
    /// not with an unflushed WAL holding several column families' data.
    /// </summary>
    [TestMethod]
    public void ReadOnlyOpenAndCloseMutateNothing()
    {
        var mem = NewMemFs();
        var fs = new FaultInjectionFileSystem(mem);
        using (RocksDatabase db = RocksDatabase.Open(Path, Options(fs)))
        {
            for (int i = 0; i < 50; i++) db.Put(Key(i), Value(i, i));
            db.Flush();
            for (int i = 50; i < 80; i++) db.Put(Key(i), Value(i, i));
        }
        fs.OpLog = new();
        using (RocksDatabase ro = RocksDatabase.OpenReadOnly(Path, Options(fs)))
        {
            for (int i = 0; i < 80; i++) Assert.IsNotNull(ro.Get(Key(i)));
            using Iterator it = ro.NewIterator();
            int n = 0;
            for (it.SeekToFirst(); it.Valid; it.Next()) n++;
            Assert.AreEqual(80, n);
        }
        Assert.AreEqual("", string.Join("; ", fs.OpLog), "a read-only open mutated files");
        Assert.AreEqual(0, fs.OpenFiles);
    }

    /// <summary>
    /// DBImplSecondary (db/db_impl/db_impl_secondary.cc): a secondary follows an in-process primary through MANIFEST
    /// rotations (max_manifest_file_size) and WAL switches with TryCatchUpWithPrimary, writes only into its own
    /// directory, and its close deletes none of the primary's files (no purge of a directory it does not own).
    /// </summary>
    [TestMethod]
    public void SecondaryFollowsManifestRotationAndTouchesOnlyItsOwnDirectory()
    {
        var mem = NewMemFs();
        var primaryFs = new FaultInjectionFileSystem(mem);
        var secondaryFs = new FaultInjectionFileSystem(mem) { OpLog = new() };
        DbOptions po = Options(primaryFs);
        po.MaxManifestFileSize = 512;
        po.MaxManifestSpaceAmpPct = 0;
        using RocksDatabase primary = RocksDatabase.Open(Path, po);
        primary.Put(Key(0), Value(0, 0));
        primary.Flush();
        const string secPath = "/audit9sec";
        RocksDatabase secondary = RocksDatabase.OpenAsSecondary(Path, secPath, new DbOptions { FileSystem = secondaryFs },
            [new ColumnFamilyDescriptor("default", new ColumnFamilyOptions())]);
        try
        {
            var manifests = new HashSet<string>();
            for (int round = 1; round <= 6; round++)
            {
                for (int i = 0; i < 10; i++) primary.Put(Key(round * 10 + i), Value(round * 10 + i, round));
                primary.Flush(); // a MANIFEST write; above 512 bytes the next one starts a new MANIFEST
                foreach (string f in mem.GetChildren(Path))
                {
                    if (f.StartsWith("MANIFEST-", StringComparison.Ordinal)) manifests.Add(f);
                }
                secondary.TryCatchUpWithPrimary();
                for (int i = 0; i < 10; i++)
                    Assert.AreEqual(S(Value(round * 10 + i, round)), S(secondary.Get(Key(round * 10 + i))), "round " + round);
            }
            Assert.IsTrue(manifests.Count > 1, "the primary never rotated its MANIFEST");
        }
        finally
        {
            secondary.Dispose();
        }
        foreach (string op in secondaryFs.OpLog)
            Assert.IsTrue(op.Contains(secPath, StringComparison.Ordinal), "the secondary mutated a file outside its directory: " + op);
        for (int i = 10; i < 70; i++) Assert.IsNotNull(primary.Get(Key(i)), "the primary lost key " + i);
    }

    // ---- TransactionDB open ----------------------------------------------------------------------------------------

    private static TransactionDatabase OpenTxnDb(IFileSystem fs, TransactionWritePolicy policy)
    {
        DbOptions o = Options(fs);
        o.InfoLog = new NullLogger();
        return TransactionDatabase.Open(Path, o, new TransactionDbOptions { WritePolicy = policy },
            [new ColumnFamilyDescriptor("default", OpenCfOptions(8UL << 20))]);
    }

    /// <summary>
    /// One crash point of a TransactionDB open (TransactionDB::Open: DB::Open with allow_2pc, then
    /// PessimisticTransactionDB::Initialize rebuilding the recovered prepared transactions): committed transactions
    /// survive, the transaction that was only prepared is recovered as prepared (and commits), nothing else appears.
    /// </summary>
    private static bool CrashDuringTxnDbOpen(TransactionWritePolicy policy, long crashAt, bool powerLoss)
    {
        string context = policy + " crashAt=" + crashAt + " powerLoss=" + powerLoss;
        var fs = new FaultInjectionFileSystem(NewMemFs());
        var sync = new WriteOptions { Sync = true };
        using (TransactionDatabase db = OpenTxnDb(fs, policy))
        {
            for (int i = 0; i < 6; i++)
            {
                using Transaction t = db.BeginTransaction(sync);
                t.SetName("c" + i);
                t.Put(B("c" + i), B("committed"));
                t.Prepare();
                t.Commit();
                if (i == 2) db.Database.Flush(); // the prepared transaction below spans a flush
            }
            Transaction p = db.BeginTransaction(sync);
            p.SetName("prepared");
            p.Put(B("p"), B("prepared"));
            p.Prepare();
            db.Database.Put(sync, B("direct"), B("1"));
            p.Dispose(); // stays prepared (the WAL keeps it)
        }
        fs.TornWrites = !powerLoss;
        fs.CrashAtOperation = fs.Operations + crashAt;
        try
        {
            using TransactionDatabase db = OpenTxnDb(fs, policy);
        }
        catch (Exception e) when (e is RocksDbException or IOException)
        {
            Assert.IsTrue(fs.Crashed, "open failed without a crash: " + context + ": " + e);
        }
        bool crashed = fs.Crashed;
        context += " at=" + fs.CrashedAt;
        Assert.AreEqual(0, fs.OpenLocks, "LOCK not released: " + context);
        Assert.AreEqual(0, fs.OpenFiles, "file handles leaked: " + context);
        if (powerLoss) fs.PowerLoss();
        fs.Restart();
        fs.TornWrites = false;
        using (TransactionDatabase db = OpenTxnDb(fs, policy))
        {
            for (int i = 0; i < 6; i++) Assert.AreEqual("committed", S(db.Database.Get(B("c" + i))), "c" + i + ": " + context);
            Assert.AreEqual("1", S(db.Database.Get(B("direct"))), context);
            Assert.IsNull(db.Database.Get(B("p")), "a prepared transaction is visible: " + context);
            Transaction? recovered = db.GetTransactionByName("prepared");
            Assert.IsNotNull(recovered, "the prepared transaction was not recovered: " + context);
            Assert.AreEqual(1, db.GetAllPreparedTransactions().Count, context);
            recovered.Commit();
            recovered.Dispose();
        }
        using (TransactionDatabase db = OpenTxnDb(fs, policy))
        {
            Assert.AreEqual("prepared", S(db.Database.Get(B("p"))), context);
            Assert.AreEqual(0, db.GetAllPreparedTransactions().Count, context);
        }
        return crashed;
    }

    /// <summary>
    /// TransactionDB::Open (utilities/transactions/pessimistic_transaction_db.cc) with a crash at every mutating file
    /// operation of the open: a power loss or a process kill never loses a committed transaction, never drops or
    /// commits the prepared one (its WAL is kept by the 2PC min-log rule, PrecomputeMinLogNumberToKeep2PC), and leaves no
    /// handle or LOCK held.
    /// </summary>
    [TestMethod]
    [DataRow(TransactionWritePolicy.WriteCommitted)]
    [DataRow(TransactionWritePolicy.WritePrepared)]
    public void CrashAtEveryStepOfTransactionDbOpenKeepsPreparedTransactions(TransactionWritePolicy policy)
    {
        foreach (bool powerLoss in new[] { true, false })
        {
            long crashAt = 1;
            for (; crashAt < 400; crashAt++)
            {
                if (!CrashDuringTxnDbOpen(policy, crashAt, powerLoss)) break;
            }
            Assert.IsTrue(crashAt > 3 && crashAt < 400, policy + ": unexpected number of crash points " + crashAt);
        }
    }
}
