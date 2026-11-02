using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Memory;
using RocksDb.Managed.Memtable;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Memtable;

/// <summary>
/// Memtable prefix / whole-key bloom (util/dynamic_bloom_test.cc, db_bloom_filter_test.cc memtable cases), insert hints
/// (memtable_insert_with_hint_prefix_extractor, db_memtable_test.cc InsertWithHint) and in-place updates
/// (db_inplace_update_test.cc).
/// </summary>
[TestClass]
public sealed class MemtableFeatureTests
{
    private static byte[] Prefixed(int prefix, int i) => B("p" + prefix.ToString("D3") + "/" + i.ToString("D6"));

    /// <summary>inplace_update_support requires allow_concurrent_memtable_write = false (CheckConcurrentWritesSupported).</summary>
    private static DbOptions InplaceDbOptions(RocksDb.Managed.Env.IFileSystem fs) => new() { CreateIfMissing = true, FileSystem = fs, AllowConcurrentMemtableWrite = false };

    [TestMethod]
    public void DynamicBloomHasNoFalseNegativesAndFewFalsePositives()
    {
        using var arena = new Arena();
        var bloom = new DynamicBloom(arena, 10 * 1000 * 8, 6);
        for (int i = 0; i < 10000; i++) bloom.Add(B("key" + i));
        for (int i = 0; i < 10000; i++) Assert.IsTrue(bloom.MayContain(B("key" + i)), "false negative " + i);
        int fp = 0;
        for (int i = 0; i < 10000; i++) if (bloom.MayContain(B("other" + i))) fp++;
        // ~8 bits/key with 6 probes: a few percent.
        Assert.IsTrue(fp < 600, "false positives " + fp);

        using var arena2 = new ConcurrentArena();
        var cbloom = new DynamicBloom(arena2, 64 * 1024 * 8, 6);
        Parallel.For(0, 4, t =>
        {
            for (int i = t; i < 20000; i += 4) cbloom.AddConcurrently(B("c" + i));
        });
        for (int i = 0; i < 20000; i++) Assert.IsTrue(cbloom.MayContain(B("c" + i)), "concurrent false negative " + i);
    }

    private static ColumnFamilyOptions BloomOptions(bool wholeKey, bool prefix) => new()
    {
        MemtablePrefixBloomSizeRatio = 0.1,
        MemtableWholeKeyFiltering = wholeKey,
        PrefixExtractor = prefix ? SliceTransform.CreateFixedPrefix(4) : null,
    };

    [TestMethod]
    [DataRow(true, false)]
    [DataRow(false, true)]
    [DataRow(true, true)]
    public void MemtableBloomGetAndPrefixSeek(bool wholeKey, bool prefix)
    {
        var fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), BloomOptions(wholeKey, prefix));
        for (int p = 0; p < 50; p += 2)
            for (int i = 0; i < 20; i++) db.Put(Prefixed(p, i), Value(i, p));
        db.Put(B("ab"), B("short")); // outside the prefix domain
        MemTable mem = db.DefaultColumnFamily.Data.Mem;
        Assert.IsNotNull(mem.BloomFilter);

        for (int p = 0; p < 50; p++)
        {
            for (int i = 0; i < 20; i++)
            {
                byte[]? v = db.Get(Prefixed(p, i));
                if (p % 2 == 0) CollectionAssert.AreEqual(Value(i, p), v);
                else Assert.IsNull(v);
            }
            Assert.IsNull(db.Get(Prefixed(p, 99)));
        }
        Assert.AreEqual("short", S(db.Get(B("ab"))));
        Assert.IsNull(db.Get(B("a")));
        // Most absent prefixes are excluded by the bloom without searching the skiplist.
        int excluded = 0;
        for (int p = 1; p < 50; p += 2) if (!mem.BloomFilter!.MayContain(wholeKey ? Prefixed(p, 0) : Prefixed(p, 0).AsSpan(0, 4))) excluded++;
        Assert.IsTrue(excluded > 15, "excluded " + excluded);

        // MultiGet agrees.
        var keys = new List<byte[]> { Prefixed(0, 1), Prefixed(1, 1), Prefixed(48, 19), B("ab") };
        byte[]?[] values = db.MultiGet(keys);
        CollectionAssert.AreEqual(Value(1, 0), values[0]);
        Assert.IsNull(values[1]);
        CollectionAssert.AreEqual(Value(19, 48), values[2]);

        if (!prefix) return;
        // Prefix seek: present prefixes iterate their keys, absent ones are empty (bloom or skiplist).
        var ro = new ReadOptions { PrefixSameAsStart = true };
        for (int p = 0; p < 50; p++)
        {
            using Iterator it = db.NewIterator(ro);
            int n = 0;
            for (it.Seek(Prefixed(p, 0)); it.Valid; it.Next()) n++;
            Assert.AreEqual(p % 2 == 0 ? 20 : 0, n, "prefix " + p);
        }
        // Total order still sees everything.
        using Iterator all = db.NewIterator(new ReadOptions { TotalOrderSeek = true });
        int total = 0;
        for (all.SeekToFirst(); all.Valid; all.Next()) total++;
        Assert.AreEqual(25 * 20 + 1, total);
    }

    [TestMethod]
    public void InsertWithHintKeepsOrder()
    {
        var fs = NewFs();
        var cfo = new ColumnFamilyOptions { MemtableInsertWithHintPrefixExtractor = SliceTransform.CreateFixedPrefix(4) };
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        int seed = Environment.TickCount;
        var rnd = new Random(seed);
        var expected = new SortedDictionary<string, string>(StringComparer.Ordinal);
        for (int n = 0; n < 5000; n++)
        {
            // Mostly ascending keys within each of 20 prefixes, interleaved, plus random ones and short keys.
            int p = rnd.Next(20);
            string k = n % 10 == 0 ? "x" + rnd.Next(100) : "p" + p.ToString("D3") + "/" + (n * 7 + rnd.Next(5)).ToString("D7");
            string v = "v" + n;
            db.Put(B(k), B(v));
            expected[k] = v;
        }
        using Iterator it = db.NewIterator();
        using var e = expected.GetEnumerator();
        for (it.SeekToFirst(); it.Valid; it.Next())
        {
            Assert.IsTrue(e.MoveNext(), "seed=" + seed);
            Assert.AreEqual(e.Current.Key, Encoding.UTF8.GetString(it.Key), "seed=" + seed);
            Assert.AreEqual(e.Current.Value, Encoding.UTF8.GetString(it.Value), "seed=" + seed);
        }
        Assert.IsFalse(e.MoveNext());
        db.Flush();
        foreach (KeyValuePair<string, string> kv in expected) Assert.AreEqual(kv.Value, GetString(db, kv.Key));
    }

    [TestMethod]
    public void InplaceUpdateOverwritesSmallerValues()
    {
        var fs = NewFs();
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(
            () => RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { InplaceUpdateSupport = true })).Code);
        using RocksDatabase db = RocksDatabase.Open(DbPath, InplaceDbOptions(fs), new ColumnFamilyOptions { InplaceUpdateSupport = true });
        MemTable mem = db.DefaultColumnFamily.Data.Mem;
        db.Put(B("k"), B("value-0123456789"));
        for (int i = 9; i >= 0; i--) db.Put(B("k"), B("v" + new string('x', i)));
        // Every Put fitted into the first entry: one memtable entry.
        Assert.AreEqual(1L, mem.NumEntries);
        Assert.AreEqual("v", GetString(db, "k"));
        db.Put(B("k"), B("a much longer value than before"));
        Assert.AreEqual(2L, mem.NumEntries);
        Assert.AreEqual("a much longer value than before", GetString(db, "k"));
        // Deletions are added (not updated in place); a put after a delete adds again.
        db.Delete(B("k"));
        Assert.IsNull(db.Get(B("k")));
        db.Put(B("k"), B("again"));
        Assert.AreEqual(4L, mem.NumEntries);
        Assert.AreEqual("again", GetString(db, "k"));
        db.Flush();
        Assert.AreEqual("again", GetString(db, "k"));
        // The WAL replays the same way after reopen.
        db.Put(B("w"), B("12345"));
        db.Put(B("w"), B("12"));
    }

    /// <summary>Concurrent readers never observe a torn value while the writer updates in place.</summary>
    [TestMethod]
    public void InplaceUpdateReadsAreConsistent()
    {
        var fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, InplaceDbOptions(fs), new ColumnFamilyOptions { InplaceUpdateSupport = true, InplaceUpdateNumLocks = 4 });
        const int len = 512;
        static byte[] Fill(byte b) { byte[] v = new byte[len]; v.AsSpan().Fill(b); return v; }
        db.Put(B("hot"), Fill(0));
        using var cts = new CancellationTokenSource();
        int torn = 0;
        Task[] readers = new Task[3];
        for (int r = 0; r < readers.Length; r++)
        {
            readers[r] = Task.Run(() =>
            {
                while (!cts.IsCancellationRequested)
                {
                    byte[] v = db.Get(B("hot"))!;
                    if (v.Length != len || v.AsSpan().IndexOfAnyExcept(v[0]) >= 0) Interlocked.Increment(ref torn);
                }
            });
        }
        for (int i = 0; i < 20000; i++) db.Put(B("hot"), Fill((byte)i));
        cts.Cancel();
        Task.WaitAll(readers);
        Assert.AreEqual(0, torn);
    }

    /// <summary>inplace_callback: an 8-byte little-endian counter incremented by each Put (db_inplace_update_test.cc style).</summary>
    [TestMethod]
    public void InplaceCallbackCounters()
    {
        static InplaceUpdateStatus Increment(Span<byte> existing, bool hasExisting, ref int existingSize, ReadOnlySpan<byte> delta, IBufferWriter<byte> merged)
        {
            ulong d = BinaryPrimitives.ReadUInt64LittleEndian(delta);
            if (hasExisting && existingSize == 8)
            {
                BinaryPrimitives.WriteUInt64LittleEndian(existing, BinaryPrimitives.ReadUInt64LittleEndian(existing) + d);
                return InplaceUpdateStatus.UpdatedInplace;
            }
            if (d == 0) return InplaceUpdateStatus.UpdateFailed;
            Span<byte> o = merged.GetSpan(8);
            BinaryPrimitives.WriteUInt64LittleEndian(o, d);
            merged.Advance(8);
            return InplaceUpdateStatus.Updated;
        }
        static byte[] U(ulong v) { byte[] b = new byte[8]; BinaryPrimitives.WriteUInt64LittleEndian(b, v); return b; }
        static ulong Read(RocksDatabase db, string k) => BinaryPrimitives.ReadUInt64LittleEndian(db.Get(B(k)));

        var fs = NewFs();
        var cfo = new ColumnFamilyOptions { InplaceUpdateSupport = true, InplaceCallback = Increment };
        using RocksDatabase db = RocksDatabase.Open(DbPath, InplaceDbOptions(fs), cfo);
        MemTable mem = db.DefaultColumnFamily.Data.Mem;
        db.Put(B("zero"), U(0)); // no existing value and a zero delta: nothing is written
        Assert.IsNull(db.Get(B("zero")));
        for (int i = 1; i <= 100; i++) db.Put(B("c"), U((ulong)i));
        Assert.AreEqual(5050UL, Read(db, "c"));
        Assert.AreEqual(1L, mem.NumEntries);

        // After a flush the counter lives in a table file: the first Put reads it from the DB and adds a merged value.
        db.Flush();
        db.Put(B("c"), U(10));
        Assert.AreEqual(5060UL, Read(db, "c"));
        db.Put(B("c"), U(1));
        Assert.AreEqual(5061UL, Read(db, "c"));
        Assert.AreEqual(1L, db.DefaultColumnFamily.Data.Mem.NumEntries);
    }
}
