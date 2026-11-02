using System.Runtime.InteropServices;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Db;
using ValueType = RocksDb.Managed.Db.ValueType;

namespace RocksDb.Managed.CompatTests.Memtable;

/// <summary>
/// Differential tests of comparator ordering and memtable read semantics against native RocksDB (memtable-only
/// workloads: WAL disabled, big write buffer, so the native reads are served by its memtable).
/// </summary>
[TestClass]
public sealed class MemTableCompatTests
{
    private static RocksDbSharp.DbOptions NativeOptions(string? optionsString)
    {
        var baseOptions = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetWriteBufferSize(256UL << 20);
        if (optionsString is null) return baseOptions;
        var result = new RocksDbSharp.DbOptions();
        IntPtr s = Marshal.StringToHGlobalAnsi(optionsString);
        try
        {
            RocksDbSharp.Native.Instance.rocksdb_get_options_from_string(baseOptions.Handle, s, result.Handle);
        }
        finally
        {
            Marshal.FreeHGlobal(s);
        }
        return result;
    }

    private static List<byte[]> RandomKeys(Random rnd, int n)
    {
        var keys = new List<byte[]>();
        var seen = new HashSet<string>();
        while (keys.Count < n)
        {
            byte[] k = new byte[rnd.Next(1, 12)];
            rnd.NextBytes(k);
            // Bias some bytes to boundary values.
            for (int i = 0; i < k.Length; i++)
            {
                if (rnd.Next(4) == 0) k[i] = (byte)(rnd.Next(2) == 0 ? 0 : 0xff);
            }
            if (seen.Add(Convert.ToHexString(k))) keys.Add(k);
        }
        return keys;
    }

    [TestMethod]
    [DataRow(null, "leveldb.BytewiseComparator")]
    [DataRow("comparator=rocksdb.ReverseBytewiseComparator", "rocksdb.ReverseBytewiseComparator")]
    public void ComparatorOrderingMatchesNative(string? optionsString, string comparatorName)
    {
        Assert.IsTrue(Comparator.TryGetBuiltin(comparatorName, out Comparator? cmp));
        var rnd = new Random(7);
        List<byte[]> keys = RandomKeys(rnd, 3000);

        using var dir = new TempDir("cmp");
        var writeOptions = new RocksDbSharp.WriteOptions().DisableWal(1);
        var expected = new List<byte[]>(keys);
        expected.Sort((a, b) => cmp.Compare(a, b));

        // Managed memtable with the same comparator must iterate in the same order.
        using var mem = new MemTable(new InternalKeyComparator(cmp), new MemTableOptions(), 0);
        ulong seq = 1;
        foreach (byte[] k in keys) Assert.IsTrue(mem.Add(seq++, ValueType.Value, k, k));

        var native = new List<byte[]>();
        using (var db = RocksDbSharp.RocksDb.Open(NativeOptions(optionsString), dir.Path))
        {
            foreach (byte[] k in keys) db.Put(k, k, null, writeOptions);
            using RocksDbSharp.Iterator it = db.NewIterator();
            for (it.SeekToFirst(); it.Valid(); it.Next()) native.Add(it.Key());
        }

        Assert.AreEqual(expected.Count, native.Count);
        MemTableIterator mit = mem.NewIterator();
        mit.SeekToFirst();
        for (int i = 0; i < native.Count; i++)
        {
            CollectionAssert.AreEqual(native[i], expected[i], "comparator order differs at " + i);
            Assert.IsTrue(mit.Valid);
            CollectionAssert.AreEqual(native[i], mit.UserKey.ToArray(), "memtable order differs at " + i);
            mit.Next();
        }
        Assert.IsFalse(mit.Valid);
    }

    /// <summary>
    /// Random Put/Delete/DeleteRange stream applied to native RocksDB and to a managed MemTable (sequence numbers
    /// assigned like the native write path: one per operation). Point reads at several snapshots must agree.
    /// </summary>
    [TestMethod]
    public void RandomOpsWithSnapshotsMatchNative()
    {
        int seed = Environment.TickCount;
        string msg = "seed=" + seed;
        var rnd = new Random(seed);
        using var dir = new TempDir("memdiff");
        var writeOptions = new RocksDbSharp.WriteOptions().DisableWal(1);
        using var db = RocksDbSharp.RocksDb.Open(NativeOptions(null), dir.Path);
        using var mem = new MemTable(InternalKeyComparator.Bytewise, new MemTableOptions(), 0);

        static byte[] Key(int i) => System.Text.Encoding.ASCII.GetBytes("key" + i.ToString("D4"));
        const int KeySpace = 300;
        var snapshots = new List<(RocksDbSharp.Snapshot snap, ulong seq)>();
        try
        {
            for (int op = 0; op < 5000; op++)
            {
                int r = rnd.Next(100);
                byte[] k = Key(rnd.Next(KeySpace));
                if (r < 70)
                {
                    byte[] v = new byte[rnd.Next(0, 200)];
                    rnd.NextBytes(v);
                    db.Put(k, v, null, writeOptions);
                    ulong seq = db.GetLatestSequenceNumber();
                    Assert.IsTrue(mem.Add(seq, ValueType.Value, k, v));
                }
                else if (r < 90)
                {
                    db.Remove(k, null, writeOptions);
                    Assert.IsTrue(mem.Add(db.GetLatestSequenceNumber(), ValueType.Deletion, k, default));
                }
                else if (r < 93)
                {
                    int a = rnd.Next(KeySpace);
                    byte[] start = Key(a), end = Key(Math.Min(KeySpace, a + rnd.Next(1, 20)));
                    using var batch = new RocksDbSharp.WriteBatch();
                    batch.DeleteRange(start, (ulong)start.Length, end, (ulong)end.Length);
                    db.Write(batch, writeOptions);
                    Assert.IsTrue(mem.Add(db.GetLatestSequenceNumber(), ValueType.RangeDeletion, start, end));
                }
                if (op % 500 == 0) snapshots.Add((db.CreateSnapshot(), db.GetLatestSequenceNumber()));
            }
            snapshots.Add((db.CreateSnapshot(), db.GetLatestSequenceNumber()));

            Span<byte> scratch = stackalloc byte[LookupKey.StackBufferSize];
            foreach ((RocksDbSharp.Snapshot snap, ulong seq) in snapshots)
            {
                var ro = new RocksDbSharp.ReadOptions().SetSnapshot(snap);
                for (int i = 0; i < KeySpace; i++)
                {
                    byte[] k = Key(i);
                    byte[]? nativeValue = db.Get(k, null, ro);
                    using var lk = new LookupKey(k, seq, scratch);
                    ulong maxCovering = 0;
                    MemTableGetResult res = mem.Get(in lk, out ReadOnlySpan<byte> v, out _, out _, null, ref maxCovering);
                    if (nativeValue is null)
                    {
                        Assert.IsTrue(res is MemTableGetResult.Deleted or MemTableGetResult.NotFound, msg + " key " + i + " seq " + seq + " got " + res);
                    }
                    else
                    {
                        Assert.AreEqual(MemTableGetResult.Found, res, msg + " key " + i + " seq " + seq);
                        Assert.IsTrue(v.SequenceEqual(nativeValue), msg);
                    }
                }
            }
        }
        finally
        {
            foreach ((RocksDbSharp.Snapshot snap, _) in snapshots) snap.Dispose();
        }
    }
}
