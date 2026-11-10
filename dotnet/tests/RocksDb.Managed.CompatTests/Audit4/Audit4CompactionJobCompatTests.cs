using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using static RocksDb.Managed.CompatTests.Wal.NativeDbHelper;

namespace RocksDb.Managed.CompatTests.Audit4;

/// <summary>
/// Audit #4, compaction job slice: a snapshot-heavy differential test against native RocksDB that stresses the paths
/// the older differential test does not reach: tiny output files (cuts between versions of one user key, range
/// tombstones spanning many outputs), subcompactions, forced bottommost compactions and many live snapshots.
/// </summary>
[TestClass]
public sealed class Audit4CompactionJobCompatTests
{
    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);
    private static string? S(byte[]? b) => b is null ? null : Encoding.UTF8.GetString(b);
    private static string K(int i) => "k" + i.ToString("D4", System.Globalization.CultureInfo.InvariantCulture);

    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    private static RocksDbSharp.RocksDb OpenNative(string path)
    {
        RocksDbSharp.ColumnFamilyOptions cf = CfOptions(CreateConcatMergeOperator());
        cf.SetWriteBufferSize(32 * 1024).SetTargetFileSizeBase(8 * 1024).SetMaxBytesForLevelBase(64 * 1024).SetLevel0FileNumCompactionTrigger(2);
        var options = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        s_keepAlive.Add(options);
        return RocksDbSharp.RocksDb.Open(options, path, new RocksDbSharp.ColumnFamilies(cf));
    }

    private static RocksDatabase OpenManaged(string path, uint maxSubcompactions) => RocksDatabase.Open(path,
        new DbOptions { CreateIfMissing = true, MaxSubcompactions = maxSubcompactions },
        new ColumnFamilyOptions
        {
            WriteBufferSize = 32 * 1024,
            ArenaBlockSize = 8 * 1024,
            TargetFileSizeBase = 8 * 1024,
            MaxBytesForLevelBase = 64 * 1024,
            Level0FileNumCompactionTrigger = 2,
            MergeOperator = new StringAppendOperator(),
        });

    private static unsafe void NativeSingleDelete(RocksDbSharp.RocksDb db, byte[] key)
    {
        var wo = new RocksDbSharp.WriteOptions();
        fixed (byte* k = key) RocksDbSharp.Native.Instance.rocksdb_singledelete(db.Handle, wo.Handle, k, (UIntPtr)key.Length);
        GC.KeepAlive(wo);
    }

    private static List<KeyValuePair<string, string>> ScanNative(RocksDbSharp.RocksDb db, RocksDbSharp.Snapshot? snapshot)
    {
        var d = new List<KeyValuePair<string, string>>();
        RocksDbSharp.ReadOptions? ro = snapshot is null ? null : new RocksDbSharp.ReadOptions().SetSnapshot(snapshot);
        using RocksDbSharp.Iterator it = db.NewIterator(null, ro);
        for (it.SeekToFirst(); it.Valid(); it.Next()) d.Add(new(S(it.Key())!, S(it.Value())!));
        return d;
    }

    private static List<KeyValuePair<string, string>> ScanManaged(RocksDatabase db, Snapshot? snapshot)
    {
        var d = new List<KeyValuePair<string, string>>();
        using Iterator it = db.NewIterator(snapshot is null ? null : new ReadOptions { Snapshot = snapshot });
        for (it.SeekToFirst(); it.Valid; it.Next()) d.Add(new(S(it.Key.ToArray())!, S(it.Value.ToArray())!));
        return d;
    }

    private static void AssertSame(List<KeyValuePair<string, string>> expected, List<KeyValuePair<string, string>> actual, string context)
    {
        int n = Math.Min(expected.Count, actual.Count);
        for (int i = 0; i < n; i++)
        {
            Assert.AreEqual(expected[i].Key, actual[i].Key, context + ": key #" + i);
            Assert.AreEqual(expected[i].Value, actual[i].Value, context + ": value of " + expected[i].Key);
        }
        Assert.AreEqual(expected.Count, actual.Count, context + ": count");
    }

    private static void CompareAll(RocksDbSharp.RocksDb native, RocksDatabase db, List<(RocksDbSharp.Snapshot N, Snapshot M)> snaps, string ctx)
    {
        AssertSame(ScanNative(native, null), ScanManaged(db, null), ctx + " tip");
        for (int i = 0; i < snaps.Count; i++) AssertSame(ScanNative(native, snaps[i].N), ScanManaged(db, snaps[i].M), ctx + " snapshot " + i);
    }

    private static void ReleaseAll(List<(RocksDbSharp.Snapshot N, Snapshot M)> snaps)
    {
        foreach ((RocksDbSharp.Snapshot N, Snapshot M) s in snaps)
        {
            s.N.Dispose();
            s.M.Dispose();
        }
        snaps.Clear();
    }

    /// <summary>
    /// Pins CompactionIterator::NextFromInput (rule A per snapshot stripe, SingleDelete / Delete / Merge handling),
    /// MergeHelper::MergeUntil (stop_before, range tombstone coverage), CompactionOutputs::AddRangeDels (tombstones
    /// clipped to output and subcompaction boundaries) and PrepareOutput (seqno zeroing) end to end: every snapshot and
    /// the tip must read the same as native RocksDB after each full compaction with the snapshots held.
    /// </summary>
    [TestMethod]
    [DataRow(1, 1u)]
    [DataRow(7, 4u)]
    [DataRow(20261110, 4u)]
    public void SnapshotsHeldAcrossFullCompactionsReadLikeNative(int seed, uint maxSubcompactions)
    {
        using var nativeDir = new TempDir("a4cj-n");
        using var managedDir = new TempDir("a4cj-m");
        using RocksDbSharp.RocksDb native = OpenNative(nativeDir.Path);
        using RocksDatabase db = OpenManaged(managedDir.Path, maxSubcompactions);
        var rnd = new Random(seed);
        var sdPresent = new Dictionary<int, bool>();
        var snaps = new List<(RocksDbSharp.Snapshot N, Snapshot M)>();
        string ctx = "seed " + seed;
        const int Keys = 400;
        var force = new CompactRangeOptions { BottommostLevelCompaction = BottommostLevelCompaction.Force };
        try
        {
            for (int op = 0; op < 6000; op++)
            {
                int r = rnd.Next(100);
                int k = rnd.Next(Keys);
                byte[] key = U(K(k));
                if (r < 40)
                {
                    byte[] v = U("v" + op + new string('y', rnd.Next(120)));
                    native.Put(key, v);
                    db.Put(key, v);
                }
                else if (r < 52)
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
                else if (r < 75)
                {
                    byte[] b = U(K(k)), e = U(K(k + rnd.Next(1, 60)));
                    using (var wb = new RocksDbSharp.WriteBatch())
                    {
                        wb.DeleteRange(b, (ulong)b.Length, e, (ulong)e.Length);
                        native.Write(wb);
                    }
                    db.DeleteRange(b, e);
                }
                else if (r < 87)
                {
                    // SingleDelete keys: exactly one Put between SingleDeletes (the documented contract).
                    int s = rnd.Next(120);
                    byte[] sk = U("s" + s.ToString("D4", System.Globalization.CultureInfo.InvariantCulture));
                    if (sdPresent.TryGetValue(s, out bool present) && present)
                    {
                        NativeSingleDelete(native, sk);
                        db.SingleDelete(sk);
                        sdPresent[s] = false;
                    }
                    else
                    {
                        byte[] v = U("s" + op);
                        native.Put(sk, v);
                        db.Put(sk, v);
                        sdPresent[s] = true;
                    }
                }
                else if (r < 93)
                {
                    if (snaps.Count < 6)
                    {
                        snaps.Add((native.CreateSnapshot(), db.GetSnapshot()));
                    }
                    else
                    {
                        int i = rnd.Next(snaps.Count);
                        snaps[i].N.Dispose();
                        snaps[i].M.Dispose();
                        snaps.RemoveAt(i);
                    }
                }
                else if (r < 96)
                {
                    native.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
                    db.Flush();
                }
                else
                {
                    Assert.AreEqual(S(native.Get(key)), S(db.Get(key)), ctx + " op " + op + " get " + K(k));
                }
                if (op % 1500 == 1499)
                {
                    native.CompactRange((byte[]?)null, null);
                    db.CompactRange(force, null, null);
                    CompareAll(native, db, snaps, ctx + " after full compaction at op " + op);
                    foreach ((RocksDbSharp.Snapshot N, Snapshot M) s in snaps)
                    {
                        var nro = new RocksDbSharp.ReadOptions().SetSnapshot(s.N);
                        var mro = new ReadOptions { Snapshot = s.M };
                        for (int g = 0; g < Keys; g += 7)
                        {
                            Assert.AreEqual(S(native.Get(U(K(g)), null, nro)), S(db.Get(mro, U(K(g)))), ctx + " snapshot get " + K(g) + " at op " + op);
                        }
                    }
                }
            }
            db.WaitForCompact();
            CompareAll(native, db, snaps, ctx + " end");
            // Releasing the snapshots lets the next full compaction drop what they protected.
            ReleaseAll(snaps);
            native.CompactRange((byte[]?)null, null);
            db.CompactRange(force, null, null);
            CompareAll(native, db, snaps, ctx + " after the snapshots were released");
        }
        finally
        {
            ReleaseAll(snaps);
        }
    }
}
