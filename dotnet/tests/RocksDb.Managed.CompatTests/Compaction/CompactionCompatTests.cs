using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using static RocksDb.Managed.CompatTests.Wal.NativeDbHelper;

namespace RocksDb.Managed.CompatTests.Compaction;

/// <summary>
/// Compaction compatibility with native RocksDB: LSM trees produced by managed compactions are read and further
/// compacted by native RocksDB and vice versa, and a randomized differential test runs the same operation stream
/// (with compactions and snapshots) against both engines.
/// </summary>
[TestClass]
public sealed class CompactionCompatTests
{
    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);
    private static string? S(byte[]? b) => b is null ? null : Encoding.UTF8.GetString(b);
    private static string K(int i) => "key" + i.ToString("D6", System.Globalization.CultureInfo.InvariantCulture);

    private static ColumnFamilyOptions ManagedCf() => new()
    {
        WriteBufferSize = 64 * 1024,
        ArenaBlockSize = 16 * 1024,
        TargetFileSizeBase = 64 * 1024,
        MaxBytesForLevelBase = 256 * 1024,
        Level0FileNumCompactionTrigger = 4,
        MergeOperator = new StringAppendOperator(),
    };

    private static RocksDbSharp.ColumnFamilyOptions NativeCf(RocksDbSharp.MergeOperator merge)
    {
        RocksDbSharp.ColumnFamilyOptions o = CfOptions(merge);
        o.SetWriteBufferSize(64 * 1024).SetTargetFileSizeBase(64 * 1024).SetMaxBytesForLevelBase(256 * 1024).SetLevel0FileNumCompactionTrigger(4);
        return o;
    }

    private static RocksDbSharp.RocksDb OpenNative(string path, RocksDbSharp.MergeOperator merge)
    {
        var options = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        KeepAlive(options);
        var cfs = new RocksDbSharp.ColumnFamilies(NativeCf(merge));
        return RocksDbSharp.RocksDb.Open(options, path, cfs);
    }

    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();
    private static void KeepAlive(object o) => s_keepAlive.Add(o);

    private static unsafe void NativeSingleDelete(RocksDbSharp.RocksDb db, byte[] key)
    {
        var wo = new RocksDbSharp.WriteOptions();
        fixed (byte* k = key) RocksDbSharp.Native.Instance.rocksdb_singledelete(db.Handle, wo.Handle, k, (UIntPtr)key.Length);
        GC.KeepAlive(wo);
    }

    private static SortedDictionary<string, string> ScanNative(RocksDbSharp.RocksDb db, RocksDbSharp.ReadOptions? ro = null)
    {
        var d = new SortedDictionary<string, string>(StringComparer.Ordinal);
        using RocksDbSharp.Iterator it = db.NewIterator(null, ro);
        for (it.SeekToFirst(); it.Valid(); it.Next()) d[S(it.Key())!] = S(it.Value())!;
        return d;
    }

    private static SortedDictionary<string, string> ScanManaged(RocksDatabase db, ReadOptions? ro = null)
    {
        var d = new SortedDictionary<string, string>(StringComparer.Ordinal);
        using Iterator it = db.NewIterator(ro);
        for (it.SeekToFirst(); it.Valid; it.Next()) d[S(it.Key.ToArray())!] = S(it.Value.ToArray())!;
        return d;
    }

    private static void AssertSame(SortedDictionary<string, string> expected, SortedDictionary<string, string> actual, string context)
    {
        Assert.AreEqual(expected.Count, actual.Count, context + ": count");
        foreach ((string k, string v) in expected)
        {
            Assert.IsTrue(actual.TryGetValue(k, out string? a), context + ": missing " + k);
            Assert.AreEqual(v, a, context + ": value of " + k);
        }
    }

    /// <summary>Native view of the LSM: files of L1+ must be non-overlapping within a level.</summary>
    private static int[] AssertNativeShapeValid(RocksDbSharp.RocksDb db)
    {
        var levels = new Dictionary<int, List<(string Smallest, string Largest)>>();
        var counts = new int[7];
        foreach (RocksDbSharp.LiveFileMetadata f in db.GetLiveFilesMetadata())
        {
            int level = f.FileMetadata.FileLevel;
            counts[Math.Min(level, 6)]++;
            if (level == 0) continue;
            if (!levels.TryGetValue(level, out List<(string, string)>? l)) levels[level] = l = new();
            l.Add((f.FileDataMetadata.SmallestKeyInFile, f.FileDataMetadata.LargestKeyInFile));
        }
        foreach ((int level, List<(string Smallest, string Largest)> files) in levels)
        {
            files.Sort((a, b) => string.CompareOrdinal(a.Smallest, b.Smallest));
            for (int i = 1; i < files.Count; i++)
            {
                Assert.IsTrue(string.CompareOrdinal(files[i - 1].Largest, files[i].Smallest) <= 0, "overlapping files at L" + level);
            }
        }
        return counts;
    }

    /// <summary>(a) Managed compactions under heavy writes produce an LSM tree native RocksDB reads and compacts further.</summary>
    [TestMethod]
    public void ManagedCompactedDbOpensInNativeAndBack()
    {
        using var dir = new TempDir("cc-a");
        var expected = new SortedDictionary<string, string>(StringComparer.Ordinal);
        var rnd = new Random(42);
        using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions { CreateIfMissing = true }, ManagedCf()))
        {
            for (int i = 0; i < 30000; i++)
            {
                int k = rnd.Next(8000);
                switch (rnd.Next(10))
                {
                    case 0:
                        db.Delete(U(K(k)));
                        expected.Remove(K(k));
                        break;
                    case 1:
                        db.Merge(U(K(k)), U("m" + i));
                        expected[K(k)] = expected.TryGetValue(K(k), out string? old) ? old + ",m" + i : "m" + i;
                        break;
                    default:
                        string v = "v" + i + new string('x', rnd.Next(100));
                        db.Put(U(K(k)), U(v));
                        expected[K(k)] = v;
                        break;
                }
                if (i == 20000)
                {
                    db.DeleteRange(U(K(1000)), U(K(1200)));
                    foreach (string key in new List<string>(expected.Keys))
                    {
                        if (string.CompareOrdinal(key, K(1000)) >= 0 && string.CompareOrdinal(key, K(1200)) < 0) expected.Remove(key);
                    }
                }
            }
            db.WaitForCompact();
            AssertSame(expected, ScanManaged(db), "managed before close");
        }

        RocksDbSharp.MergeOperator merge = CreateConcatMergeOperator();
        using (RocksDbSharp.RocksDb native = OpenNative(dir.Path, merge))
        {
            int[] counts = AssertNativeShapeValid(native);
            int deeper = 0;
            for (int l = 1; l < 7; l++) deeper += counts[l];
            Assert.IsTrue(deeper > 0, "expected compacted levels: " + string.Join(",", counts));
            AssertSame(expected, ScanNative(native), "native read of managed LSM");
            for (int k = 0; k < 8000; k += 97) Assert.AreEqual(expected.GetValueOrDefault(K(k)), S(native.Get(U(K(k)))), "native get " + K(k));
            // Native writes more and compacts everything.
            for (int k = 0; k < 8000; k += 13)
            {
                native.Put(U(K(k)), U("native" + k));
                expected[K(k)] = "native" + k;
            }
            native.CompactRange((byte[]?)null, null);
            AssertNativeShapeValid(native);
        }

        using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions { CreateIfMissing = true }, ManagedCf()))
        {
            AssertSame(expected, ScanManaged(db), "managed reopen after native compaction");
        }
    }

    /// <summary>(b) A native multi-level DB is opened by managed, extended and compacted; native verifies the result.</summary>
    [TestMethod]
    public void NativeMultiLevelDbCompactedByManaged()
    {
        using var dir = new TempDir("cc-b");
        var expected = new SortedDictionary<string, string>(StringComparer.Ordinal);
        RocksDbSharp.MergeOperator merge = CreateConcatMergeOperator();
        using (RocksDbSharp.RocksDb native = OpenNative(dir.Path, merge))
        {
            var rnd = new Random(7);
            for (int i = 0; i < 20000; i++)
            {
                int k = rnd.Next(6000);
                string v = "n" + i + new string('y', rnd.Next(80));
                native.Put(U(K(k)), U(v));
                expected[K(k)] = v;
                if (i % 5000 == 4999) native.CompactRange(U(K(0)), U(K(3000)));
            }
            for (int k = 0; k < 6000; k += 11)
            {
                native.Remove(U(K(k)));
                expected.Remove(K(k));
            }
            native.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            int[] counts = AssertNativeShapeValid(native);
            int levelsUsed = 0;
            foreach (int c in counts) levelsUsed += c > 0 ? 1 : 0;
            Assert.IsTrue(levelsUsed >= 2, "native multi-level: " + string.Join(",", counts));
        }

        using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions { CreateIfMissing = true }, ManagedCf()))
        {
            AssertSame(expected, ScanManaged(db), "managed read of native LSM");
            for (int k = 0; k < 6000; k += 7)
            {
                if (k % 3 == 0)
                {
                    db.Merge(U(K(k)), U("mm"));
                    expected[K(k)] = expected.TryGetValue(K(k), out string? old) ? old + ",mm" : "mm";
                }
                else
                {
                    db.Put(U(K(k)), U("managed" + k));
                    expected[K(k)] = "managed" + k;
                }
            }
            db.DeleteRange(U(K(5000)), U(K(5100)));
            foreach (string key in new List<string>(expected.Keys))
            {
                if (string.CompareOrdinal(key, K(5000)) >= 0 && string.CompareOrdinal(key, K(5100)) < 0) expected.Remove(key);
            }
            db.CompactRange(new CompactRangeOptions { BottommostLevelCompaction = BottommostLevelCompaction.Force }, null, null);
            AssertSame(expected, ScanManaged(db), "managed after CompactRange");
        }

        using (RocksDbSharp.RocksDb native = OpenNative(dir.Path, merge))
        {
            AssertNativeShapeValid(native);
            AssertSame(expected, ScanNative(native), "native read after managed compaction");
            for (int k = 0; k < 6000; k += 31) Assert.AreEqual(expected.GetValueOrDefault(K(k)), S(native.Get(U(K(k)))), "native get " + K(k));
        }
    }

    /// <summary>
    /// (c) Differential test: the same seeded operation stream (put / delete / merge / delete range / single delete /
    /// compact range / snapshot reads) against a native and a managed DB; Gets and scans must agree throughout.
    /// </summary>
    [TestMethod]
    public void RandomizedDifferentialAgainstNative()
    {
        int seed = Environment.TickCount & 0x7fffffff;
        Console.WriteLine("seed=" + seed);
        RunDifferential(seed);
        RunDifferential(2024);
    }

    private static void RunDifferential(int seed)
    {
        using var nativeDir = new TempDir("cc-cn");
        using var managedDir = new TempDir("cc-cm");
        RocksDbSharp.MergeOperator merge = CreateConcatMergeOperator();
        using RocksDbSharp.RocksDb native = OpenNative(nativeDir.Path, merge);
        using RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true }, ManagedCf());
        var rnd = new Random(seed);
        var sdState = new Dictionary<int, bool>();
        var snaps = new List<(RocksDbSharp.Snapshot N, Snapshot M)>();
        string ctx = "seed " + seed;
        for (int op = 0; op < 15000; op++)
        {
            int r = rnd.Next(100);
            int k = rnd.Next(1500);
            byte[] key = U(K(k));
            if (r < 45)
            {
                byte[] v = U("v" + op + new string('z', rnd.Next(64)));
                native.Put(key, v);
                db.Put(key, v);
            }
            else if (r < 55)
            {
                native.Remove(key);
                db.Delete(key);
            }
            else if (r < 70)
            {
                byte[] v = U("m" + op);
                native.Merge(key, v);
                db.Merge(key, v);
            }
            else if (r < 72)
            {
                byte[] b = U(K(k)), e = U(K(k + rnd.Next(1, 40)));
                using (var wb = new RocksDbSharp.WriteBatch())
                {
                    wb.DeleteRange(b, (ulong)b.Length, e, (ulong)e.Length);
                    native.Write(wb);
                }
                db.DeleteRange(b, e);
            }
            else if (r < 82)
            {
                int s = rnd.Next(300);
                byte[] sk = U("sd" + s.ToString("D4", System.Globalization.CultureInfo.InvariantCulture));
                if (sdState.TryGetValue(s, out bool present) && present)
                {
                    NativeSingleDelete(native, sk);
                    db.SingleDelete(sk);
                    sdState[s] = false;
                }
                else
                {
                    byte[] v = U("s" + op);
                    native.Put(sk, v);
                    db.Put(sk, v);
                    sdState[s] = true;
                }
            }
            else if (r < 84)
            {
                if (snaps.Count < 3)
                {
                    snaps.Add((native.CreateSnapshot(), db.GetSnapshot()));
                }
                else
                {
                    int i = rnd.Next(snaps.Count);
                    CompareSnapshot(native, db, snaps[i], ctx + " op " + op);
                    snaps[i].N.Dispose();
                    snaps[i].M.Dispose();
                    snaps.RemoveAt(i);
                }
            }
            else if (r < 85 && rnd.Next(8) == 0)
            {
                int a = rnd.Next(1500);
                byte[] b = U(K(a)), e = U(K(a + 300));
                native.CompactRange(b, e);
                db.CompactRange(b, e);
            }
            else if (r < 95)
            {
                Assert.AreEqual(S(native.Get(key)), S(db.Get(key)), ctx + " op " + op + " get " + K(k));
            }
            else
            {
                byte[] v = U("p" + op);
                native.Put(key, v);
                db.Put(key, v);
            }
            if (op % 2500 == 2499)
            {
                AssertSame(ScanNative(native), ScanManaged(db), ctx + " op " + op);
                foreach ((RocksDbSharp.Snapshot N, Snapshot M) s in snaps) CompareSnapshot(native, db, s, ctx + " op " + op);
            }
        }
        native.CompactRange((byte[]?)null, null);
        db.CompactRange();
        AssertSame(ScanNative(native), ScanManaged(db), ctx + " after full compaction");
        foreach ((RocksDbSharp.Snapshot N, Snapshot M) s in snaps)
        {
            CompareSnapshot(native, db, s, ctx + " end");
            s.N.Dispose();
            s.M.Dispose();
        }
    }

    private static void CompareSnapshot(RocksDbSharp.RocksDb native, RocksDatabase db, (RocksDbSharp.Snapshot N, Snapshot M) s, string ctx)
    {
        var nro = new RocksDbSharp.ReadOptions().SetSnapshot(s.N);
        AssertSame(ScanNative(native, nro), ScanManaged(db, new ReadOptions { Snapshot = s.M }), ctx + " snapshot");
    }
}
