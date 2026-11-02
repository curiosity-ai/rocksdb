using RocksDb.Managed.Db;
using RocksDb.Managed.Table;
using RocksDb.Managed.Util;
using static RocksDb.Managed.Tests.Table.TableTestUtil;

namespace RocksDb.Managed.Tests.Table;

/// <summary>Ports of meaningful cases from table/block_based/block_test.cc and data_block_hash_index_test.cc.</summary>
[TestClass]
public sealed class BlockTests
{
    private static List<byte[]> SortedInternalKeys(int count, Random rnd, int maxLen = 24)
    {
        var set = new SortedSet<string>(StringComparer.Ordinal);
        while (set.Count < count)
        {
            int len = rnd.Next(1, maxLen);
            char[] c = new char[len];
            for (int i = 0; i < len; i++) c[i] = (char)('a' + rnd.Next(0, 6));
            set.Add(new string(c));
        }
        var keys = new List<byte[]>();
        foreach (string s in set) keys.Add(IKey(s, 7));
        return keys;
    }

    private static Block BuildBlock(List<byte[]> keys, List<byte[]> values, int restartInterval, DataBlockIndexType indexType = DataBlockIndexType.BinarySearch)
    {
        using var b = new BlockBuilder(restartInterval, true, false, indexType, 0.75);
        for (int i = 0; i < keys.Count; i++) b.Add(keys[i], values[i]);
        return Block.CreateCopy(b.Finish());
    }

    [TestMethod]
    [DataRow(1)]
    [DataRow(2)]
    [DataRow(16)]
    [DataRow(1000)]
    public void DataBlockRoundTripSeekAndPrev(int restartInterval)
    {
        int seed = Environment.TickCount;
        string trace = "seed=" + seed;
        var rnd = new Random(seed);
        List<byte[]> keys = SortedInternalKeys(500, rnd);
        var values = new List<byte[]>();
        foreach (byte[] k in keys) { byte[] v = new byte[rnd.Next(0, 40)]; rnd.NextBytes(v); values.Add(v); }
        Block block = BuildBlock(keys, values, restartInterval);
        using var it = new BlockIter();
        it.InitializeData(block, Comparator.Bytewise, BlockIter.DisableGlobalSequenceNumber);

        int idx = 0;
        for (it.SeekToFirst(); it.Valid; it.Next(), idx++)
        {
            CollectionAssert.AreEqual(keys[idx], it.Key.ToArray(), trace);
            CollectionAssert.AreEqual(values[idx], it.Value.ToArray(), trace);
        }
        Assert.AreEqual(keys.Count, idx, trace);

        idx = keys.Count - 1;
        for (it.SeekToLast(); it.Valid; it.Prev(), idx--) CollectionAssert.AreEqual(keys[idx], it.Key.ToArray(), trace);
        Assert.AreEqual(-1, idx, trace);

        // Seek to every existing key, and to keys between (seq larger => sorts before).
        for (int i = 0; i < keys.Count; i++)
        {
            it.Seek(keys[i]);
            Assert.IsTrue(it.Valid, trace);
            CollectionAssert.AreEqual(keys[i], it.Key.ToArray(), trace);
            CollectionAssert.AreEqual(values[i], it.Value.ToArray(), trace);

            byte[] after = IKey(DbFormat.ExtractUserKey(keys[i]), 3); // smaller seq sorts after
            it.Seek(after);
            if (i + 1 < keys.Count) CollectionAssert.AreEqual(keys[i + 1], it.Key.ToArray(), trace);
            else Assert.IsFalse(it.Valid, trace);

            it.SeekForPrev(after);
            CollectionAssert.AreEqual(keys[i], it.Key.ToArray(), trace);
            byte[] before = IKey(DbFormat.ExtractUserKey(keys[i]), 100);
            it.SeekForPrev(before);
            if (i > 0) CollectionAssert.AreEqual(keys[i - 1], it.Key.ToArray(), trace);
            else Assert.IsFalse(it.Valid, trace);
        }
        // Random walk mixing Next/Prev.
        it.Seek(keys[keys.Count / 2]);
        int pos = keys.Count / 2;
        for (int step = 0; step < 2000 && it.Valid; step++)
        {
            if (rnd.Next(2) == 0) { it.Next(); pos++; }
            else { it.Prev(); pos--; }
            if (pos < 0 || pos >= keys.Count) { Assert.IsFalse(it.Valid, trace); break; }
            CollectionAssert.AreEqual(keys[pos], it.Key.ToArray(), trace);
        }
        block.Release();
    }

    [TestMethod]
    public void EmptyBlock()
    {
        using var b = new BlockBuilder(16, true, false, DataBlockIndexType.BinarySearch, 0.75);
        Assert.IsTrue(b.IsEmpty);
        ReadOnlySpan<byte> contents = b.Finish();
        Assert.AreEqual(8, contents.Length); // one restart (0) + count (1)
        Block block = Block.CreateCopy(contents);
        using var it = new BlockIter();
        it.InitializeData(block, Comparator.Bytewise, BlockIter.DisableGlobalSequenceNumber);
        it.SeekToFirst();
        Assert.IsFalse(it.Valid);
        it.SeekToLast();
        Assert.IsFalse(it.Valid);
        it.Seek(IKey("a", 1));
        Assert.IsFalse(it.Valid);
        it.SeekForPrev(IKey("a", 1));
        Assert.IsFalse(it.Valid);
        block.Release();
    }

    [TestMethod]
    public void KnownEncoding()
    {
        // Two meta entries with restart interval 16: "abc"->"1", "abd"->"22".
        using var b = BlockBuilder.ForMetaBlock(16);
        b.Add("abc"u8, "1"u8);
        b.Add("abd"u8, "22"u8);
        byte[] expected = [0, 3, 1, (byte)'a', (byte)'b', (byte)'c', (byte)'1', 2, 1, 2, (byte)'d', (byte)'2', (byte)'2', 0, 0, 0, 0, 1, 0, 0, 0];
        CollectionAssert.AreEqual(expected, b.Finish().ToArray());
    }

    [TestMethod]
    public void HashIndexDataBlock()
    {
        var rnd = new Random(42);
        List<byte[]> keys = SortedInternalKeys(300, rnd, 12);
        var values = new List<byte[]>();
        foreach (byte[] k in keys) values.Add(k);
        Block block = BuildBlock(keys, values, 4, DataBlockIndexType.BinaryAndHash);
        Assert.IsTrue(block.HasHashIndex);
        Assert.AreEqual(1, block.HashNumBuckets % 2);
        using var it = new BlockIter();
        it.InitializeData(block, Comparator.Bytewise, BlockIter.DisableGlobalSequenceNumber);
        for (int i = 0; i < keys.Count; i++)
        {
            byte[] lookup = IKey(DbFormat.ExtractUserKey(keys[i]), DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
            Assert.IsTrue(it.SeekForGet(lookup));
            Assert.IsTrue(it.Valid);
            CollectionAssert.AreEqual(keys[i], it.Key.ToArray());
        }
        // Absent keys either report "not here" or position at a different user key.
        for (int i = 0; i < 200; i++)
        {
            byte[] uk = System.Text.Encoding.ASCII.GetBytes("zz" + i);
            byte[] lookup = IKey(uk, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
            if (it.SeekForGet(lookup) && it.Valid) Assert.IsFalse(DbFormat.ExtractUserKey(it.Key).SequenceEqual(uk));
        }
        block.Release();
    }

    [TestMethod]
    public void LegacyHashMatchesUpstream()
    {
        // Values from util/hash_test.cc (Hash with seed 0xbc9f1d34), incl. sign-extended tail bytes.
        const uint seed = 0xbc9f1d34;
        Assert.AreEqual(3164544308u, DataBlockHashIndex.LegacyHash([], seed));
        Assert.AreEqual(422599524u, DataBlockHashIndex.LegacyHash([0x08], seed));
        Assert.AreEqual(3195034349u, DataBlockHashIndex.LegacyHash([0x9a], seed));
        Assert.AreEqual(31066776u, DataBlockHashIndex.LegacyHash([0x91, 0xf7], seed));
        Assert.AreEqual(2469105222u, DataBlockHashIndex.LegacyHash([0x6a, 0xb5, 0xf4], seed));
        Assert.AreEqual(447346355u, DataBlockHashIndex.LegacyHash([0x46, 0xdc, 0x54, 0xbe], seed));
        Assert.AreEqual(2177978500u, DataBlockHashIndex.LegacyHash([0x6c, 0xa8, 0xbc, 0xe5, 0x99], seed));
        Assert.AreEqual(2030937252u, DataBlockHashIndex.LegacyHash([0xd3, 0xa5, 0x7c, 0x0e, 0xc0, 0x02, 0x07], seed));
        Assert.AreEqual(2706087434u, DataBlockHashIndex.LegacyHash([0x1a, 0xb5, 0xd0, 0xfe, 0xab, 0xc3, 0x61, 0xb2, 0x99], seed));
    }

    [TestMethod]
    [DataRow(1, true)]
    [DataRow(4, true)]
    [DataRow(1, false)]
    [DataRow(3, false)]
    public void IndexBlockValueDeltaEncoding(int restartInterval, bool deltaEncoded)
    {
        var rnd = new Random(7);
        List<byte[]> keys = SortedInternalKeys(200, rnd);
        var handles = new List<BlockHandle>();
        using var b = new BlockBuilder(restartInterval, true, deltaEncoded, DataBlockIndexType.BinarySearch, 0.75);
        using var full = new PooledBufferWriter();
        using var delta = new PooledBufferWriter();
        ulong offset = 0;
        BlockHandle prev = default;
        for (int i = 0; i < keys.Count; i++)
        {
            var h = new BlockHandle(offset, (ulong)rnd.Next(100, 5000));
            handles.Add(h);
            full.Clear();
            delta.Clear();
            IndexValue.EncodeFull(full, h, false, default);
            if (i > 0) IndexValue.EncodeDelta(delta, h, prev, false, default);
            b.Add(keys[i], full.WrittenSpan, delta.WrittenSpan, false);
            prev = h;
            offset += h.Size + BlockBasedTableConstants.BlockTrailerSize;
        }
        Block block = Block.CreateCopy(b.Finish());
        using var it = new BlockIter();
        it.InitializeIndex(block, Comparator.Bytewise, keyIncludesSeq: true, valueIsFull: !deltaEncoded, haveFirstKey: false, BlockIter.DisableGlobalSequenceNumber);
        int n = 0;
        for (it.SeekToFirst(); it.Valid; it.Next(), n++)
        {
            CollectionAssert.AreEqual(keys[n], it.Key.ToArray());
            Assert.AreEqual(handles[n], it.BlockHandle);
        }
        Assert.AreEqual(keys.Count, n);
        for (int i = keys.Count - 1; i >= 0; i -= 7)
        {
            it.Seek(keys[i]);
            Assert.AreEqual(handles[i], it.BlockHandle);
            it.Prev();
            if (i > 0) Assert.AreEqual(handles[i - 1], it.BlockHandle);
            else Assert.IsFalse(it.Valid);
        }
        it.SeekToLast();
        Assert.AreEqual(handles[^1], it.BlockHandle);
        block.Release();
    }

    [TestMethod]
    public void CorruptBlockIsDetected()
    {
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => Block.CreateCopy([1, 2, 3])).Code);
        // Restart count larger than the block.
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => Block.CreateCopy([0, 0, 0, 0, 9, 0, 0, 0])).Code);
    }

    [TestMethod]
    public void GlobalSeqnoIsApplied()
    {
        var keys = new List<byte[]> { IKey("a", 0), IKey("b", 0), IKey("c", 0, RocksDb.Managed.Db.EntryType.Deletion) };
        var values = new List<byte[]> { new byte[] { 1 }, new byte[] { 2 }, Array.Empty<byte>() };
        Block block = BuildBlock(keys, values, 2);
        using var it = new BlockIter();
        it.InitializeData(block, Comparator.Bytewise, 42);
        int n = 0;
        for (it.SeekToFirst(); it.Valid; it.Next(), n++)
        {
            DbFormat.UnpackSequenceAndType(DbFormat.ExtractFooter(it.Key), out ulong seq, out RocksDb.Managed.Db.EntryType t);
            Assert.AreEqual(42UL, seq);
            Assert.AreEqual(n == 2 ? RocksDb.Managed.Db.EntryType.Deletion : RocksDb.Managed.Db.EntryType.Value, t);
        }
        it.Seek(IKey("b", 42));
        Assert.AreEqual("b", UserKeyOf(it.Key));
        it.Seek(IKey("b", 41));
        Assert.AreEqual("c", UserKeyOf(it.Key));
        block.Release();
    }
}
