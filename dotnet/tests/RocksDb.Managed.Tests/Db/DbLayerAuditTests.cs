using System.Collections.Concurrent;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>
/// Regression tests for the DB-layer audit (docs/validation.md, "Db: database layer"): durability ordering, recovery,
/// column family drop races, and randomized concurrent stress tests with invariants.
/// </summary>
[TestClass]
public sealed class DbLayerAuditTests
{
    [TestCleanup]
    public void Cleanup() => TestHooks.ClearAll();

    private static ColumnFamilyOptions SmallCf() => new()
    {
        WriteBufferSize = 64 * 1024,
        TargetFileSizeBase = 64 * 1024,
        MaxBytesForLevelBase = 256 * 1024,
        Level0FileNumCompactionTrigger = 2,
        Compression = RocksDb.Managed.Compression.CompressionType.None,
        MaxWriteBufferNumber = 4,
    };

    // ---- MANIFEST write failure ---------------------------------------------------------------------------

    [TestMethod]
    public void FailedManifestAppendForcesNewManifestForLaterEdits()
    {
        // VersionSet::ProcessManifestWrites: after a failed (possibly torn) MANIFEST append the next edit must go to a
        // new MANIFEST; appending after the torn record made the column family creation below unrecoverable.
        var fs = new FaultInjectionFileSystem(NewFs()) { TornWrites = true };
        var options = new DbOptions { CreateIfMissing = true, FileSystem = fs };
        using (RocksDatabase db = RocksDatabase.Open(DbPath, options))
        {
            db.Put(new WriteOptions { Sync = true }, B("a"), B("1"));
            fs.FailNextAppendTo = "MANIFEST";
            var ex = Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
            Assert.AreEqual(StatusCode.IOError, ex.Code);
            // Column family creation still writes MANIFEST edits.
            db.CreateColumnFamily("x", new ColumnFamilyOptions());
        }
        using RocksDatabase db2 = RocksDatabase.Open(DbPath, new DbOptions { FileSystem = fs },
            [new ColumnFamilyDescriptor("default", new ColumnFamilyOptions()), new ColumnFamilyDescriptor("x", new ColumnFamilyOptions())]);
        Assert.AreEqual("1", GetString(db2, "a"));
        db2.GetColumnFamily("x").Put(B("b"), B("2"));
        Assert.AreEqual("2", GetString(db2.GetColumnFamily("x"), "b"));
    }

    // ---- column family drop races -------------------------------------------------------------------------

    [TestMethod]
    public void DropColumnFamilyWhileItsFlushRuns()
    {
        // The flush reads the CF's immutable memtables without the mutex and then writes a MANIFEST edit. A concurrent
        // drop must neither free the memtables under it (arena reuse / bg error) nor let its edit reach the MANIFEST
        // after the drop record ("MANIFEST record referencing unknown column family" on the next open).
        foreach (string hook in new[] { "DBImpl::FlushMemTables:Start", "DBImpl::FlushMemTables:BeforeLogAndApply" })
        {
            MemoryFileSystem fs = NewFs();
            using var reached = new ManualResetEventSlim(false);
            using var release = new ManualResetEventSlim(false);
            using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
            {
                ColumnFamily cf = db.CreateColumnFamily("victim", new ColumnFamilyOptions());
                for (int i = 0; i < 2000; i++) cf.Put(Key(i), Value(i, 0));
                db.Put(B("keep"), B("me"));
                TestHooks.Set(hook, () =>
                {
                    reached.Set();
                    release.Wait();
                });
                Task flush = Task.Run(() => cf.Flush());
                Assert.IsTrue(reached.Wait(10000), hook);
                db.DropColumnFamily(cf);
                release.Set();
                try { flush.Wait(10000); }
                catch (AggregateException e) when (e.InnerException is RocksDbException { Code: StatusCode.ColumnFamilyDropped }) { }
                TestHooks.ClearAll();
                db.WaitForCompact();
                Assert.IsNull(db.DefaultColumnFamily.Db.BackgroundError, hook);
                db.Put(new WriteOptions { Sync = true }, B("after"), B("drop"));
                db.Flush();
            }
            using RocksDatabase db2 = RocksDatabase.Open(DbPath, NewOptions(fs));
            Assert.AreEqual("me", GetString(db2, "keep"), hook);
            Assert.AreEqual("drop", GetString(db2, "after"), hook);
            CollectionAssert.AreEqual(new[] { "default" }, RocksDatabase.ListColumnFamilies(DbPath, fs).ToArray(), hook);
        }
    }

    [TestMethod]
    public void DroppedColumnFamilyIteratorKeepsReadingTableFiles()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        ColumnFamily cf = db.CreateColumnFamily("victim", new ColumnFamilyOptions());
        for (int i = 0; i < 500; i++) cf.Put(Key(i), Value(i, 0));
        cf.Flush();
        using Iterator it = cf.NewIterator(null);
        it.SeekToFirst();
        db.DropColumnFamily(cf);
        // The iterator holds a super version: its files and table readers stay usable (upstream semantics).
        int n = 0;
        for (; it.Valid; it.Next()) n++;
        Assert.AreEqual(500, n);
    }

    // ---- read path ----------------------------------------------------------------------------------------

    [TestMethod]
    public void GetTakesSuperVersionBeforeReadSequence()
    {
        // DBImpl::GetImpl references the super version before reading the last sequence. In the other order an
        // overwrite + flush + compaction between the two steps drops the version visible at the read sequence (no
        // snapshot protects it) and the new super version only holds the newer value: the key reads as missing.
        // v0 sits in L2 so that the L0->L1 compaction below is not bottommost: v2 keeps its sequence number and v1
        // (hidden by v2, no snapshot) is dropped. A read at v1's sequence then finds only v0 in the new super version.
        MemoryFileSystem fs = NewFs();
        var cfo = new ColumnFamilyOptions { LevelCompactionDynamicLevelBytes = false, Level0FileNumCompactionTrigger = 2 };
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        db.Put(B("k"), B("v0"));
        db.Flush();
        db.CompactRange(new CompactRangeOptions { ChangeLevel = true, TargetLevel = 2 }, null, null);
        Assert.AreEqual("1", db.GetProperty("rocksdb.num-files-at-level2"));
        db.Put(B("k"), B("v1"));
        db.Flush();
        int calls = 0;
        TestHooks.Set("DBImpl::GetImpl:AfterSuperVersion", () =>
        {
            if (Interlocked.Increment(ref calls) != 1) return;
            db.Put(B("k"), B("v2"));
            db.Flush();
            db.WaitForCompact();
        });
        string? v = GetString(db, "k");
        TestHooks.ClearAll();
        Assert.IsTrue(v is "v1" or "v2", "got " + (v ?? "<missing>"));
        Assert.AreEqual("v2", GetString(db, "k"));
        Assert.AreEqual("0", db.GetProperty("rocksdb.num-files-at-level0"));
    }

    // ---- recovery -----------------------------------------------------------------------------------------

    [TestMethod]
    public void PointInTimeRecoveryResumesWhenSequenceContinuesAfterCorruption()
    {
        // MaybeReviseStopReplayForCorruption: after a PIT recovery stopped at a corrupted record (and kept the data in
        // memtables, avoid_flush_during_recovery), writes go to a new WAL starting at the next sequence. The next
        // recovery must replay them instead of stopping at the old corruption again (acknowledged synced writes lost).
        MemoryFileSystem fs = NewFs();
        var options = new DbOptions { CreateIfMissing = true, FileSystem = fs, AvoidFlushDuringRecovery = true };
        using (RocksDatabase db = RocksDatabase.Open(DbPath, options))
        {
            for (int i = 0; i < 10; i++) db.Put(new WriteOptions { Sync = true }, Key(i), new byte[200]);
        }
        string wal = LargestWal(fs);
        byte[] bytes = fs.ReadAllBytes(wal);
        bytes[bytes.Length * 6 / 10] ^= 0xff; // inside the 7th record's payload
        using (IWritableFile f = fs.CreateWritable(wal, truncate: true))
        {
            f.Append(bytes);
            f.Sync();
        }
        int recovered;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, options))
        {
            recovered = 0;
            while (recovered < 10 && db.Get(Key(recovered)) is not null) recovered++;
            Assert.IsTrue(recovered is > 0 and < 10, "recovered " + recovered);
            for (int i = recovered; i < 10; i++) Assert.IsNull(db.Get(Key(i)));
            db.Put(new WriteOptions { Sync = true }, B("new"), B("write"));
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, options))
        {
            Assert.AreEqual("write", GetString(db, "new"));
            for (int i = 0; i < recovered; i++) Assert.IsNotNull(db.Get(Key(i)));
            db.Flush();
        }
        using RocksDatabase final = RocksDatabase.Open(DbPath, NewOptions(fs));
        Assert.AreEqual("write", GetString(final, "new"));
    }

    private static string LargestWal(MemoryFileSystem fs)
    {
        string? best = null;
        long size = -1;
        foreach (string c in fs.GetChildren(DbPath))
        {
            if (!c.EndsWith(".log", StringComparison.Ordinal)) continue;
            long s = fs.GetFileSize(DbPath + "/" + c);
            if (s > size)
            {
                size = s;
                best = DbPath + "/" + c;
            }
        }
        Assert.IsNotNull(best);
        return best;
    }

    [TestMethod]
    public void IngestionPublishesSequenceOnlyAfterInstall()
    {
        // Snapshot stability: a snapshot taken while an ingestion writes its MANIFEST edit must not observe the files
        // later. The last sequence is published after the files are installed, and the MANIFEST still records the
        // ingested seqno so that it survives a crash.
        MemoryFileSystem fs = NewFs();
        string sst = "/ingest.sst";
        using (var w = new SstFileWriter(new ColumnFamilyOptions(), fs))
        {
            w.Open(sst);
            w.Put(B("a"), B("ingested"));
            w.Finish();
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            db.Put(B("a"), B("old"));
            db.Flush();
            ulong before = db.GetLatestSequenceNumber();
            ulong seenDuringManifestWrite = 0;
            Snapshot? snap = null;
            TestHooks.Set("VersionSet::LogAndApply:BeforeManifestWrite", () =>
            {
                seenDuringManifestWrite = db.GetLatestSequenceNumber();
                snap ??= db.GetSnapshot();
            });
            db.IngestExternalFiles([sst], new IngestExternalFileOptions());
            TestHooks.ClearAll();
            Assert.AreEqual(before, seenDuringManifestWrite);
            Assert.AreEqual(before + 1, db.GetLatestSequenceNumber());
            Assert.IsNotNull(snap);
            using (snap)
            {
                Assert.AreEqual("old", GetString(db, "a", snap));
            }
            Assert.AreEqual("ingested", GetString(db, "a"));
        }
        using RocksDatabase db2 = RocksDatabase.Open(DbPath, NewOptions(fs));
        Assert.AreEqual("ingested", GetString(db2, "a"));
        db2.Put(B("a"), B("newer"));
        Assert.AreEqual("newer", GetString(db2, "a"));
        db2.Flush();
        db2.CompactRange();
        Assert.AreEqual("newer", GetString(db2, "a"));
    }

    [TestMethod]
    public void CloseWaitsForTheFlushObsoleteFilePurge()
    {
        // The purge at the end of a background flush is part of the job: close must not return while it runs, or it
        // could delete files of a new DB created in the same directory after close (e.g. its MANIFEST-000001).
        using var entered = new ManualResetEventSlim(false);
        using var gate = new ManualResetEventSlim(false);
        int armed = 0;
        RocksDatabase db = RocksDatabase.Open(DbPath, new DbOptions { CreateIfMissing = true, FileSystem = NewFs() });
        TestHooks.Set("DBImpl::PurgeObsoleteFiles:Start", () =>
        {
            if (Interlocked.Exchange(ref armed, 0) == 0) return;
            entered.Set();
            gate.Wait();
        });
        try
        {
            db.Put(Key(1), Value(1, 0));
            Volatile.Write(ref armed, 1);
            Task flush = Task.Run(() => db.Flush());
            Assert.IsTrue(entered.Wait(10_000), "flush purge did not start");
            Task close = Task.Run(db.Dispose);
            Assert.IsFalse(close.Wait(200), "close returned while the flush job was still deleting files");
            gate.Set();
            Assert.IsTrue(Task.WaitAll([flush, close], 10_000));
        }
        finally
        {
            gate.Set();
            TestHooks.ClearAll();
            db.Dispose();
        }
    }

    [TestMethod]
    public void CloseRacingWritersAndFlushes()
    {
        // Writers and flushes queued behind close get leadership after close released it: they must fail cleanly
        // (ObjectDisposedException) instead of writing through a closed WAL or creating a new WAL after close, and every
        // acknowledged write survives.
        for (int round = 0; round < 20; round++)
        {
            var fs = new FaultInjectionFileSystem(NewFs());
            var acked = new ConcurrentDictionary<int, bool>();
            var unexpected = new ConcurrentQueue<Exception>();
            RocksDatabase db = RocksDatabase.Open(DbPath, new DbOptions { CreateIfMissing = true, FileSystem = fs });
            using var start = new ManualResetEventSlim(false);
            var tasks = new List<Task>();
            for (int t = 0; t < 4; t++)
            {
                int tt = t;
                tasks.Add(Task.Run(async () =>
                {
                    start.Wait();
                    for (int i = 0; i < 200; i++)
                    {
                        int k = tt * 1000 + i;
                        try
                        {
                            if (i % 50 == 49) await db.FlushAsync();
                            else await db.PutAsync(Key(k), Value(k, 0));
                            acked[k] = true;
                        }
                        catch (ObjectDisposedException)
                        {
                            return;
                        }
                        catch (Exception e)
                        {
                            unexpected.Enqueue(e);
                            return;
                        }
                    }
                }));
            }
            start.Set();
            Thread.Sleep(round % 5);
            db.Dispose();
            Task.WaitAll(tasks.ToArray());
            Assert.IsTrue(unexpected.IsEmpty, string.Join("\n", unexpected));
            Assert.AreEqual(0, fs.OpenFiles, "round " + round);
            Assert.AreEqual(0, fs.OpenLocks, "round " + round);
            using RocksDatabase db2 = RocksDatabase.Open(DbPath, new DbOptions { FileSystem = fs });
            foreach (int k in acked.Keys)
            {
                if (k % 1000 % 50 == 49) continue;
                Assert.IsNotNull(db2.Get(Key(k)), "acknowledged write " + k + " lost, round " + round);
            }
        }
    }

    // ---- stress -------------------------------------------------------------------------------------------

    /// <summary>
    /// Randomized concurrent stress: overwriting writers, readers (Get, iterators, snapshots), manual flushes and
    /// compactions, and column family create/drop cycles, with invariants checked continuously:
    /// a key that always exists is never reported missing (sequence acquired after the super version), a snapshot's
    /// view never changes, iterators see every key exactly once, and the DB reopens with all data.
    /// </summary>
    [TestMethod]
    public void ConcurrentStressWithInvariants()
    {
        for (int round = 0; round < 2; round++) RunStress(seed: Environment.TickCount + round, TimeSpan.FromSeconds(2.5));
    }

    private static byte[] StressValue(int key, long version) =>
        B(key.ToString(System.Globalization.CultureInfo.InvariantCulture) + ":" + version.ToString(System.Globalization.CultureInfo.InvariantCulture) + new string('v', (int)(version % 97)));

    private static void CheckValue(int key, byte[]? v, ConcurrentQueue<string> errors, string what)
    {
        if (v is null)
        {
            errors.Enqueue(what + ": key " + key + " missing");
            return;
        }
        string s = S(v)!;
        if (!s.StartsWith(key.ToString(System.Globalization.CultureInfo.InvariantCulture) + ":", StringComparison.Ordinal))
            errors.Enqueue(what + ": key " + key + " has value " + s);
    }

    private static void RunStress(int seed, TimeSpan duration)
    {
        const int numKeys = 300;
        MemoryFileSystem fs = NewFs();
        var errors = new ConcurrentQueue<string>();
        var options = new DbOptions { CreateIfMissing = true, FileSystem = fs, MaxBackgroundJobs = 4 };
        using (RocksDatabase db = RocksDatabase.Open(DbPath, options, SmallCf()))
        {
            for (int k = 0; k < numKeys; k++) db.Put(Key(k), StressValue(k, 0));
            using var stop = new CancellationTokenSource(duration);
            long version = 1;
            var tasks = new List<Task>();
            for (int t = 0; t < 3; t++)
            {
                int tseed = seed * 7 + t;
                tasks.Add(Task.Run(async () =>
                {
                    var rnd = new Random(tseed);
                    while (!stop.IsCancellationRequested)
                    {
                        int k = rnd.Next(numKeys);
                        long v = Interlocked.Increment(ref version);
                        if (rnd.Next(4) == 0) await db.PutAsync(Key(k), StressValue(k, v));
                        else
                        {
                            using WriteBatch b = db.CreateWriteBatch();
                            b.Put(Key(k), StressValue(k, v));
                            int k2 = rnd.Next(numKeys);
                            b.Put(Key(k2), StressValue(k2, v));
                            db.Write(b);
                        }
                    }
                }));
            }
            for (int t = 0; t < 2; t++)
            {
                int tseed = seed * 13 + t;
                tasks.Add(Task.Run(() =>
                {
                    var rnd = new Random(tseed);
                    while (!stop.IsCancellationRequested)
                    {
                        int k = rnd.Next(numKeys);
                        CheckValue(k, db.Get(Key(k)), errors, "Get");
                        if (rnd.Next(50) == 0)
                        {
                            using Snapshot snap = db.GetSnapshot();
                            var ro = new ReadOptions { Snapshot = snap };
                            byte[]? first = db.Get(ro, Key(k));
                            CheckValue(k, first, errors, "snapshot Get");
                            Thread.Yield();
                            byte[]? second = db.Get(ro, Key(k));
                            if (first is not null && (second is null || !first.AsSpan().SequenceEqual(second)))
                                errors.Enqueue("snapshot view of key " + k + " changed");
                        }
                        if (rnd.Next(200) == 0)
                        {
                            using Iterator it = db.NewIterator();
                            int n = 0;
                            for (it.SeekToFirst(); it.Valid; it.Next())
                            {
                                if (it.Key[0] != (byte)'k') continue;
                                n++;
                            }
                            if (n != numKeys) errors.Enqueue("iterator saw " + n + " keys");
                        }
                    }
                }));
            }
            tasks.Add(Task.Run(() =>
            {
                var rnd = new Random(seed * 17);
                while (!stop.IsCancellationRequested)
                {
                    try
                    {
                        if (rnd.Next(3) == 0) db.CompactRange();
                        else db.Flush();
                    }
                    catch (RocksDbException e) when (e.Code == StatusCode.ShutdownInProgress)
                    {
                    }
                    Thread.Sleep(rnd.Next(5));
                }
            }));
            tasks.Add(Task.Run(() =>
            {
                int i = 0;
                while (!stop.IsCancellationRequested)
                {
                    string name = "tmp" + (i++ % 3).ToString(System.Globalization.CultureInfo.InvariantCulture);
                    ColumnFamily cf = db.CreateColumnFamily(name, SmallCf());
                    for (int j = 0; j < 300; j++) cf.Put(Key(j), new byte[300]);
                    Task f = Task.Run(() =>
                    {
                        try { cf.Flush(); }
                        catch (RocksDbException e) when (e.Code == StatusCode.ColumnFamilyDropped) { }
                    });
                    using (Iterator it = cf.NewIterator(null))
                    {
                        it.SeekToFirst();
                        db.DropColumnFamily(cf);
                        int n = 0;
                        for (; it.Valid; it.Next()) n++;
                        if (n != 300) errors.Enqueue("dropped CF iterator saw " + n);
                    }
                    f.Wait();
                }
            }));
            Task.WaitAll(tasks.ToArray());
            db.WaitForCompact();
            Assert.IsNull(db.DefaultColumnFamily.Db.BackgroundError, "seed " + seed);
            for (int k = 0; k < numKeys; k++) CheckValue(k, db.Get(Key(k)), errors, "final");
        }
        Assert.IsTrue(errors.IsEmpty, "seed " + seed + ": " + string.Join("; ", errors.Take(10)));
        using RocksDatabase reopened = RocksDatabase.Open(DbPath, new DbOptions { FileSystem = fs }, SmallCf());
        for (int k = 0; k < numKeys; k++) CheckValue(k, reopened.Get(Key(k)), errors, "reopen");
        Assert.IsTrue(errors.IsEmpty, "seed " + seed + ": " + string.Join("; ", errors.Take(10)));
        CollectionAssert.AreEqual(new[] { "default" }, RocksDatabase.ListColumnFamilies(DbPath, fs).ToArray());
    }
}
