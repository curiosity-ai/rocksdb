using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>
/// Crash-consistency tests in the spirit of db_stress' crash test / FaultInjectionTestFS: a deterministic workload
/// (puts, deletes, range deletes, sync writes, manual flushes, background flushes and compactions over two column
/// families) runs against a <see cref="FaultInjectionFileSystem"/> that "crashes" at the N-th mutating file-system
/// operation, for many N. After a simulated power loss (unsynced data and directory entries dropped) or process kill
/// (torn last write, everything else kept) the DB is reopened and must
/// <list type="bullet">
/// <item>open without corruption,</item>
/// <item>contain every write acknowledged before the last successful sync write / flush (power loss) or every
/// acknowledged write (process kill),</item>
/// <item>equal the state after some prefix of the issued writes (no holes, nothing from the future), across both
/// column families,</item>
/// <item>keep working (write, flush, compact, reopen) afterwards.</item>
/// </list>
/// </summary>
[TestClass]
public sealed class DbCrashConsistencyTests
{
    private const int NumKeys = 48;
    private const int NumOps = 360;

    private readonly record struct Op(int Cf, int Kind, int Key, int KeyEnd, int Value, bool Sync, bool FlushAfter);

    // Kind: 0 = put, 1 = delete, 2 = delete range [Key, KeyEnd)
    private static List<Op> MakeOps(int seed)
    {
        var rnd = new Random(seed);
        var ops = new List<Op>(NumOps);
        for (int i = 0; i < NumOps; i++)
        {
            int cf = rnd.Next(3) == 0 ? 1 : 0;
            int r = rnd.Next(100);
            int kind = r < 80 ? 0 : r < 95 ? 1 : 2;
            int key = rnd.Next(NumKeys);
            int keyEnd = Math.Min(NumKeys, key + 1 + rnd.Next(5));
            ops.Add(new Op(cf, kind, key, keyEnd, i, Sync: rnd.Next(5) == 0, FlushAfter: rnd.Next(60) == 0));
        }
        return ops;
    }

    private static byte[] K(int k) => B("k" + k.ToString("D3", System.Globalization.CultureInfo.InvariantCulture));

    private static byte[] V(int key, int value) =>
        B("v" + key.ToString(System.Globalization.CultureInfo.InvariantCulture) + "-" + value.ToString(System.Globalization.CultureInfo.InvariantCulture) +
          new string('x', 700 + (value % 300)));

    private static ColumnFamilyOptions CfOptions() => new()
    {
        WriteBufferSize = 64 * 1024,
        TargetFileSizeBase = 64 * 1024,
        MaxBytesForLevelBase = 256 * 1024,
        Level0FileNumCompactionTrigger = 2,
        Compression = RocksDb.Managed.Compression.CompressionType.None,
        MaxWriteBufferNumber = 3,
    };

    /// <summary>The write path under test.</summary>
    public enum WriteMode { Ordered, Pipelined, Unordered }

    private static RocksDatabase Open(IFileSystem fs, out ColumnFamily cf1) => Open(fs, out cf1, WriteMode.Ordered);

    private static RocksDatabase Open(IFileSystem fs, out ColumnFamily cf1, WriteMode mode)
    {
        var options = new DbOptions { CreateIfMissing = true, CreateMissingColumnFamilies = true, FileSystem = fs,
            EnablePipelinedWrite = mode == WriteMode.Pipelined, UnorderedWrite = mode == WriteMode.Unordered };
        RocksDatabase db = RocksDatabase.Open(DbPath, options,
            [new ColumnFamilyDescriptor("default", CfOptions()), new ColumnFamilyDescriptor("cf1", CfOptions())]);
        cf1 = db.GetColumnFamily("cf1");
        return db;
    }

    private static void Apply(Dictionary<int, int>[] model, Op op)
    {
        Dictionary<int, int> m = model[op.Cf];
        switch (op.Kind)
        {
            case 0: m[op.Key] = op.Value; break;
            case 1: m.Remove(op.Key); break;
            default:
                for (int k = op.Key; k < op.KeyEnd; k++) m.Remove(k);
                break;
        }
    }

    /// <summary>Runs the workload until it finishes or the file system crashes. Returns (durable, acked, attempted).</summary>
    private static (int Durable, int Acked, int Attempted) RunWorkload(RocksDatabase db, ColumnFamily cf1, List<Op> ops)
    {
        int durable = 0, acked = 0, attempted = 0;
        try
        {
            for (int i = 0; i < ops.Count; i++)
            {
                Op op = ops[i];
                ColumnFamily cf = op.Cf == 0 ? db.DefaultColumnFamily : cf1;
                var wo = new WriteOptions { Sync = op.Sync };
                attempted = i + 1;
                switch (op.Kind)
                {
                    case 0: cf.Put(wo, K(op.Key), V(op.Key, op.Value)); break;
                    case 1: cf.Delete(wo, K(op.Key)); break;
                    default: cf.DeleteRange(wo, K(op.Key), K(op.KeyEnd)); break;
                }
                acked = i + 1;
                if (op.Sync) durable = acked;
                if (op.FlushAfter)
                {
                    cf.Flush();
                    // The memtable switch syncs the previous WAL, so every earlier write is durable now.
                    durable = acked;
                }
            }
        }
        catch (RocksDbException)
        {
        }
        catch (IOException)
        {
        }
        return (durable, acked, attempted);
    }

    private static Dictionary<int, int>[] ReadAll(RocksDatabase db, ColumnFamily cf1)
    {
        var result = new Dictionary<int, int>[] { new(), new() };
        for (int c = 0; c < 2; c++)
        {
            ColumnFamily cf = c == 0 ? db.DefaultColumnFamily : cf1;
            for (int k = 0; k < NumKeys; k++)
            {
                byte[]? v = cf.Get(K(k));
                if (v is null) continue;
                string s = S(v)!;
                int dash = s.IndexOf('-', StringComparison.Ordinal);
                int end = s.IndexOf('x', StringComparison.Ordinal);
                Assert.AreEqual("v" + k.ToString(System.Globalization.CultureInfo.InvariantCulture), s[..dash], "value stored under the wrong key");
                result[c][k] = int.Parse(s.AsSpan(dash + 1, end - dash - 1), System.Globalization.CultureInfo.InvariantCulture);
            }
            // The iterator must agree with point lookups.
            using Iterator it = cf.NewIterator(null);
            int n = 0;
            for (it.SeekToFirst(); it.Valid; it.Next())
            {
                if (it.Key[0] == (byte)'k') n++;
            }
            Assert.AreEqual(result[c].Count, n, "iterator / Get mismatch in cf " + c);
        }
        return result;
    }

    private static bool Equal(Dictionary<int, int> a, Dictionary<int, int> b)
    {
        if (a.Count != b.Count) return false;
        foreach ((int k, int v) in a)
        {
            if (!b.TryGetValue(k, out int o) || o != v) return false;
        }
        return true;
    }

    /// <summary>Returns the prefix length P in [min, max] whose model equals <paramref name="actual"/>, or -1.</summary>
    private static int FindMatchingPrefix(List<Op> ops, Dictionary<int, int>[] actual, int min, int max)
    {
        var model = new Dictionary<int, int>[] { new(), new() };
        for (int i = 0; i < min; i++) Apply(model, ops[i]);
        for (int p = min; p <= max; p++)
        {
            if (p > min) Apply(model, ops[p - 1]);
            if (Equal(model[0], actual[0]) && Equal(model[1], actual[1])) return p;
        }
        return -1;
    }

    private static long CountOperations(int seed, WriteMode mode)
    {
        var fs = new FaultInjectionFileSystem(NewFs());
        RocksDatabase db = Open(fs, out ColumnFamily cf1, mode);
        long start = fs.Operations;
        (_, int acked, _) = RunWorkload(db, cf1, MakeOps(seed));
        Assert.AreEqual(NumOps, acked);
        long n = fs.Operations - start;
        db.Dispose();
        return n;
    }

    private static void CrashAndRecover(int seed, long crashAt, bool powerLoss, WriteMode mode)
    {
        string context = "seed=" + seed + " crashAt=" + crashAt + " powerLoss=" + powerLoss + " mode=" + mode;
        List<Op> ops = MakeOps(seed);
        var fs = new FaultInjectionFileSystem(NewFs()) { TornWrites = !powerLoss };
        RocksDatabase db = Open(fs, out ColumnFamily cf1, mode);
        fs.CrashAtOperation = fs.Operations + crashAt;
        (int durable, int acked, int attempted) = RunWorkload(db, cf1, ops);
        try { db.Dispose(); }
        catch (Exception e) when (e is RocksDbException or IOException) { }
        Assert.AreEqual(0, fs.OpenFiles, "file handles leaked after a failed close: " + context);
        Assert.AreEqual(0, fs.OpenLocks, "LOCK not released: " + context);
        if (powerLoss) fs.PowerLoss();
        else FaultInjectionFileSystem.ProcessKill();
        fs.Restart();

        RocksDatabase db2;
        try
        {
            db2 = Open(fs, out cf1, mode);
        }
        catch (Exception e)
        {
            Assert.Fail("reopen failed: " + context + ": " + e);
            throw;
        }
        using (db2)
        {
            Dictionary<int, int>[] actual = ReadAll(db2, cf1);
            int min = powerLoss ? durable : acked;
            int p = FindMatchingPrefix(ops, actual, min, attempted);
            Assert.AreNotEqual(-1, p,
                "recovered state is not a prefix of the writes in [" + min + ", " + attempted + "] (acked " + acked + "): " + context);

            // The recovered DB keeps working.
            db2.Put(new WriteOptions { Sync = true }, B("after"), B("crash"));
            db2.Flush();
            cf1.Flush();
            db2.CompactRange();
        }
        using RocksDatabase db3 = Open(fs, out ColumnFamily cf3, mode);
        Assert.AreEqual("crash", S(db3.Get(B("after"))), context);
        Dictionary<int, int>[] again = ReadAll(db3, cf3);
        Assert.AreNotEqual(-1, FindMatchingPrefix(ops, again, powerLoss ? durable : acked, attempted), "state changed after reopen: " + context);
    }

    private static void RunCrashSweep(int seed, bool powerLoss, int points) => RunCrashSweep(seed, powerLoss, points, WriteMode.Ordered);

    private static void RunCrashSweep(int seed, bool powerLoss, int points, WriteMode mode)
    {
        long total = CountOperations(seed, mode);
        Assert.IsTrue(total > 100, "workload too small: " + total);
        var rnd = new Random(seed * 31 + (powerLoss ? 1 : 0));
        long step = Math.Max(1, total / points);
        for (long at = 1; at <= total; at += step)
        {
            long crashAt = Math.Min(total, at + rnd.Next((int)step));
            CrashAndRecover(seed, crashAt, powerLoss, mode);
        }
    }

    [TestMethod]
    public void PowerLossAtManyPointsKeepsSyncedWritesAndPrefixConsistency() => RunCrashSweep(seed: 7, powerLoss: true, points: 120);

    [TestMethod]
    public void ProcessKillWithTornWritesKeepsAckedWritesAndPrefixConsistency() => RunCrashSweep(seed: 11, powerLoss: false, points: 120);

    [TestMethod]
    public void PowerLossSweepSecondSeed() => RunCrashSweep(seed: 1234, powerLoss: true, points: 60);

    [TestMethod]
    public void PipelinedWritesPowerLossSweep() => RunCrashSweep(seed: 21, powerLoss: true, points: 60, WriteMode.Pipelined);

    [TestMethod]
    public void PipelinedWritesProcessKillSweep() => RunCrashSweep(seed: 23, powerLoss: false, points: 60, WriteMode.Pipelined);

    [TestMethod]
    public void UnorderedWritesPowerLossSweep() => RunCrashSweep(seed: 25, powerLoss: true, points: 60, WriteMode.Unordered);

    [TestMethod]
    public void UnorderedWritesProcessKillSweep() => RunCrashSweep(seed: 27, powerLoss: false, points: 60, WriteMode.Unordered);

    /// <summary>
    /// Concurrent writers (sync and async API) with pipelined writes and parallel memtable inserts, or with unordered writes
    /// (memtable inserts after the WAL group, in any order), crash at many points.
    /// Every writer writes its own keys in order and waits for each acknowledgement, so WAL order follows each writer's
    /// order: after recovery each writer's surviving keys must be a prefix of its writes (no holes), containing at least
    /// its writes up to its last acknowledged sync write (power loss) or all its acknowledged writes (process kill).
    /// </summary>
    [TestMethod]
    [Timeout(120_000, CooperativeCancellation = true)]
    [DataRow(true, WriteMode.Pipelined)]
    [DataRow(false, WriteMode.Pipelined)]
    [DataRow(true, WriteMode.Unordered)]
    [DataRow(false, WriteMode.Unordered)]
    public void ConcurrentWritersCrashSweep(bool powerLoss, WriteMode mode)
    {
        const int points = 24;
        long total = RunConcurrentWriters(new FaultInjectionFileSystem(NewFs()), crashAt: 0, mode, out _, out _);
        long step = Math.Max(1, total / points);
        var rnd = new Random(powerLoss ? 5 : 6);
        for (long at = 1; at <= total; at += step)
        {
            long crashAt = Math.Min(total, at + rnd.Next((int)step));
            string context = "crashAt=" + crashAt + " powerLoss=" + powerLoss + " mode=" + mode;
            var fs = new FaultInjectionFileSystem(NewFs()) { TornWrites = !powerLoss };
            RunConcurrentWriters(fs, crashAt, mode, out int[] durable, out int[] acked);
            if (powerLoss) fs.PowerLoss();
            else FaultInjectionFileSystem.ProcessKill();
            fs.Restart();
            using RocksDatabase db = Open(fs, out ColumnFamily cf1, mode);
            for (int t = 0; t < ConcurrentWriters; t++)
            {
                ColumnFamily cf = t % 2 == 0 ? db.DefaultColumnFamily : cf1;
                int present = 0;
                while (present < ConcurrentWritesPerWriter && cf.Get(WriterKey(t, present)) is not null) present++;
                for (int i = present; i < ConcurrentWritesPerWriter; i++)
                {
                    Assert.IsNull(cf.Get(WriterKey(t, i)), "hole in writer " + t + " at " + present + " (found " + i + "): " + context);
                }
                int min = powerLoss ? durable[t] : acked[t];
                Assert.IsTrue(present >= min, "writer " + t + " lost acknowledged writes: " + present + " < " + min + ": " + context);
            }
            db.Put(new WriteOptions { Sync = true }, B("after"), B("crash"));
        }
    }

    private const int ConcurrentWriters = 6;
    private const int ConcurrentWritesPerWriter = 120;

    private static byte[] WriterKey(int t, int i) =>
        B("w" + t.ToString(System.Globalization.CultureInfo.InvariantCulture) + "-" + i.ToString("D4", System.Globalization.CultureInfo.InvariantCulture));

    /// <summary>Runs the concurrent workload (crashing at <paramref name="crashAt"/> if not 0); returns the operation count.</summary>
    private static long RunConcurrentWriters(FaultInjectionFileSystem fs, long crashAt, WriteMode mode, out int[] durable, out int[] acked)
    {
        var durableOut = new int[ConcurrentWriters];
        var ackedOut = new int[ConcurrentWriters];
        RocksDatabase db = Open(fs, out ColumnFamily cf1, mode);
        long start = fs.Operations;
        if (crashAt != 0) fs.CrashAtOperation = start + crashAt;
        var threads = new Thread[ConcurrentWriters];
        for (int t = 0; t < ConcurrentWriters; t++)
        {
            int id = t;
            threads[t] = new Thread(() =>
            {
                ColumnFamily cf = id % 2 == 0 ? db.DefaultColumnFamily : cf1;
                try
                {
                    for (int i = 0; i < ConcurrentWritesPerWriter; i++)
                    {
                        var wo = new WriteOptions { Sync = (i + id) % 10 == 0 };
                        byte[] value = V(id, i);
                        if (id % 3 == 0) cf.PutAsync(wo, WriterKey(id, i), value).AsTask().GetAwaiter().GetResult();
                        else cf.Put(wo, WriterKey(id, i), value);
                        ackedOut[id] = i + 1;
                        if (wo.Sync) durableOut[id] = i + 1;
                    }
                }
                catch (Exception e) when (e is RocksDbException or IOException or ObjectDisposedException)
                {
                }
            });
            threads[t].Start();
        }
        foreach (Thread th in threads) th.Join();
        long ops = fs.Operations - start;
        try { db.Dispose(); }
        catch (Exception e) when (e is RocksDbException or IOException) { }
        Assert.AreEqual(0, fs.OpenFiles, "file handles leaked after a failed close: crashAt=" + crashAt);
        Assert.AreEqual(0, fs.OpenLocks, "LOCK not released: crashAt=" + crashAt);
        if (crashAt == 0)
        {
            for (int t = 0; t < ConcurrentWriters; t++) Assert.AreEqual(ConcurrentWritesPerWriter, ackedOut[t]);
        }
        durable = durableOut;
        acked = ackedOut;
        return ops;
    }

    [TestMethod]
    public void CleanCloseReleasesAllFileHandles()
    {
        var fs = new FaultInjectionFileSystem(NewFs());
        RocksDatabase db = Open(fs, out ColumnFamily cf1);
        RunWorkload(db, cf1, MakeOps(3));
        using (Iterator it = db.NewIterator())
        {
            for (it.SeekToFirst(); it.Valid; it.Next()) { }
        }
        db.WaitForCompact();
        db.Dispose();
        Assert.AreEqual(0, fs.OpenFiles);
        Assert.AreEqual(0, fs.OpenLocks);
    }

    [TestMethod]
    public void FailedOpenReleasesAllFileHandles()
    {
        var fs = new FaultInjectionFileSystem(NewFs());
        using (RocksDatabase db = Open(fs, out ColumnFamily cf1)) RunWorkload(db, cf1, MakeOps(5));
        // Fail somewhere in the middle of the next open (recovery, MANIFEST roll, OPTIONS file, ...).
        for (long at = 1; at < 40; at++)
        {
            fs.CrashAtOperation = fs.Operations + at;
            try
            {
                using RocksDatabase db = Open(fs, out _);
            }
            catch (Exception e) when (e is RocksDbException or IOException)
            {
            }
            Assert.AreEqual(0, fs.OpenFiles, "leak after failed open at op " + at);
            Assert.AreEqual(0, fs.OpenLocks, "LOCK leaked after failed open at op " + at);
            fs.Restart();
        }
        using RocksDatabase final = Open(fs, out _);
    }
}
