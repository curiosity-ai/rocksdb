using RocksDb.Managed.Db;
using RocksDb.Managed.Table;
using RocksDb.Managed.Util;
using static RocksDb.Managed.Tests.Table.TableTestUtil;

namespace RocksDb.Managed.Tests.Table;

/// <summary>Ports of meaningful cases from table/block_based/block_test.cc and data_block_hash_index_test.cc.</summary>
[TestClass]
public sealed class BlockTests
{
    private static List<byte[]> SortedInternalKeys(int count, Random rnd, int maxLen = 24, string prefix = "")
    {
        var set = new SortedSet<string>(StringComparer.Ordinal);
        while (set.Count < count)
        {
            int len = rnd.Next(1, maxLen);
            char[] c = new char[len];
            for (int i = 0; i < len; i++) c[i] = (char)('a' + rnd.Next(0, 6));
            set.Add(prefix + new string(c));
        }
        var keys = new List<byte[]>();
        foreach (string s in set) keys.Add(IKey(s, 7));
        return keys;
    }

    private static Block BuildBlock(List<byte[]> keys, List<byte[]> values, int restartInterval, DataBlockIndexType indexType = DataBlockIndexType.BinarySearch,
        bool commonPrefix = false, bool separatedKv = false)
    {
        using var b = new BlockBuilder(restartInterval, true, false, indexType, 0.75, commonPrefix, isUserKey: false, stripTimestampSize: 0,
            useSeparatedKvStorage: separatedKv, uniformCvThreshold: -1);
        for (int i = 0; i < keys.Count; i++) b.Add(keys[i], values[i]);
        return Block.CreateCopy(b.Finish());
    }

    [TestMethod]
    [DataRow(1, "", false)]
    [DataRow(2, "", false)]
    [DataRow(16, "", false)]
    [DataRow(1000, "", false)]
    [DataRow(1, "common/prefix/", true)]
    [DataRow(16, "common/prefix/", true)]
    [DataRow(1000, "p", true)]
    [DataRow(16, "", true)] // common-prefix builder whose keys happen to share little or nothing
    [DataRow(1, "", false, true)]
    [DataRow(3, "", false, true)]
    [DataRow(16, "", false, true)]
    [DataRow(16, "common/prefix/", true, true)]
    [DataRow(1000, "p", true, true)]
    public void DataBlockRoundTripSeekAndPrev(int restartInterval, string prefix, bool commonPrefix, bool separatedKv = false)
    {
        int seed = Environment.TickCount;
        string trace = "seed=" + seed;
        var rnd = new Random(seed);
        List<byte[]> keys = SortedInternalKeys(500, rnd, 24, prefix);
        var values = new List<byte[]>();
        foreach (byte[] k in keys) { byte[] v = new byte[rnd.Next(0, 40)]; rnd.NextBytes(v); values.Add(v); }
        Block block = BuildBlock(keys, values, restartInterval, DataBlockIndexType.BinarySearch, commonPrefix, separatedKv);
        Assert.AreEqual(separatedKv, block.ValuesSectionOffset >= 0, trace);
        if (prefix.Length > 0) Assert.IsTrue(block.CommonPrefixLength >= prefix.Length, trace);
        if (!commonPrefix) Assert.AreEqual(0, block.CommonPrefixLength, trace);
        using var it = new BlockIter();
        it.InitializeData(block, InternalKeyComparator.Bytewise, BlockIter.DisableGlobalSequenceNumber);

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
        it.InitializeData(block, InternalKeyComparator.Bytewise, BlockIter.DisableGlobalSequenceNumber);
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

    /// <summary>separate_key_value_in_data_block layout (DataBlockFooter with values_section_offset and bit 28).</summary>
    [TestMethod]
    public void KnownEncodingSeparatedKv()
    {
        using var b = new BlockBuilder(1, true, false, DataBlockIndexType.BinarySearch, 0.75, false, false, 0, useSeparatedKvStorage: true, uniformCvThreshold: -1);
        b.Add("abc"u8, "1"u8);
        b.Add("abd"u8, "22"u8);
        // keys: <shared><non_shared><value_size><value_offset><key> per restart entry; then values "1" "22"; restarts
        // [0, 7]; values_section_offset 14; packed = 2 | 1 << 28.
        byte[] expected =
        [
            0, 3, 1, 0, (byte)'a', (byte)'b', (byte)'c',
            0, 3, 2, 1, (byte)'a', (byte)'b', (byte)'d',
            (byte)'1', (byte)'2', (byte)'2',
            0, 0, 0, 0, 7, 0, 0, 0,
            14, 0, 0, 0,
            2, 0, 0, 0x10,
        ];
        CollectionAssert.AreEqual(expected, b.Finish().ToArray());
        Block block = Block.CreateCopy(expected);
        Assert.AreEqual(14, block.ValuesSectionOffset);
        using var it = new BlockIter();
        it.InitializeMeta(block);
        it.SeekToLast();
        CollectionAssert.AreEqual("abd"u8.ToArray(), it.Key.ToArray());
        CollectionAssert.AreEqual("22"u8.ToArray(), it.Value.ToArray());
        it.Prev();
        CollectionAssert.AreEqual("1"u8.ToArray(), it.Value.ToArray());
        block.Release();

        // A values section offset beyond the restart array is corruption.
        byte[] bad = (byte[])expected.Clone();
        bad[^8] = 40;
        var ex = Assert.ThrowsExactly<RocksDbException>(() => Block.CreateCopy(bad));
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
    }

    [TestMethod]
    public void EmptySeparatedKvBlock()
    {
        using var b = new BlockBuilder(16, true, false, DataBlockIndexType.BinarySearch, 0.75, false, false, 0, useSeparatedKvStorage: true, uniformCvThreshold: -1);
        Assert.AreEqual(12, b.CurrentSizeEstimate);
        ReadOnlySpan<byte> contents = b.Finish();
        Assert.AreEqual(12, contents.Length);
        Block block = Block.CreateCopy(contents);
        using var it = new BlockIter();
        it.InitializeData(block, InternalKeyComparator.Bytewise, BlockIter.DisableGlobalSequenceNumber);
        it.SeekToFirst();
        Assert.IsFalse(it.Valid);
        it.Seek(IKey("a", 1));
        Assert.IsFalse(it.Valid);
        block.Release();
    }

    /// <summary>Data block hash index over a separated key/value block (point lookups through SeekForGet).</summary>
    [TestMethod]
    public void HashIndexWithSeparatedKv()
    {
        var rnd = new Random(42);
        List<byte[]> keys = SortedInternalKeys(300, rnd);
        var values = new List<byte[]>();
        foreach (byte[] k in keys) values.Add([.. k, .. "#v"u8]);
        Block block = BuildBlock(keys, values, 4, DataBlockIndexType.BinaryAndHash, separatedKv: true);
        Assert.IsTrue(block.HasHashIndex);
        Assert.IsTrue(block.ValuesSectionOffset > 0);
        using var it = new BlockIter();
        it.InitializeData(block, InternalKeyComparator.Bytewise, BlockIter.DisableGlobalSequenceNumber);
        for (int i = 0; i < keys.Count; i++)
        {
            Assert.IsTrue(it.SeekForGet(keys[i]));
            CollectionAssert.AreEqual(keys[i], it.Key.ToArray());
            CollectionAssert.AreEqual(values[i], it.Value.ToArray());
        }
        block.Release();
    }

    /// <summary>uniform_cv_threshold: evenly spaced index keys get the "is_uniform" bit (bit 29), skewed keys do not.</summary>
    [TestMethod]
    public void UniformHint()
    {
        static Block Build(Func<int, ulong> key, double threshold)
        {
            using var b = new BlockBuilder(1, true, true, DataBlockIndexType.BinarySearch, 0.75, false, isUserKey: true, 0, false, threshold);
            byte[] handle = [1, 2];
            for (int i = 0; i < 100; i++)
            {
                byte[] k = new byte[8];
                System.Buffers.Binary.BinaryPrimitives.WriteUInt64BigEndian(k, key(i));
                b.Add(k, handle, handle, false);
            }
            ReadOnlySpan<byte> contents = b.Finish();
            Assert.AreEqual(b.IsUniform, (Coding.DecodeFixed32(contents[^4..]) & (1u << 29)) != 0);
            return Block.CreateCopy(contents);
        }
        Block uniform = Build(static i => 1000 + (ulong)i * 977, 0.1);
        Assert.IsTrue(uniform.IsUniform);
        Assert.AreEqual(100, uniform.NumRestarts);
        uniform.Release();
        Block skewed = Build(static i => (ulong)i * (ulong)i * (ulong)i * 1_000_003, 0.1);
        Assert.IsFalse(skewed.IsUniform);
        skewed.Release();
        Block disabled = Build(static i => 1000 + (ulong)i * 977, -1);
        Assert.IsFalse(disabled.IsUniform);
        disabled.Release();

        Assert.AreEqual(0x0102000000000000UL, BlockBuilder.ReadBe64FromKey([0xff, 1, 2], isUserKey: true, offset: 1));
        Assert.AreEqual(0x0102030405060708UL, BlockBuilder.ReadBe64FromKey([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 0, 0, 0, 0, 0, 0, 0], isUserKey: false, offset: 1));
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
        it.InitializeData(block, InternalKeyComparator.Bytewise, BlockIter.DisableGlobalSequenceNumber);
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
    [DataRow(1, true, false, false)]
    [DataRow(4, true, false, false)]
    [DataRow(1, false, false, false)]
    [DataRow(3, false, false, false)]
    [DataRow(1, true, true, false)]
    [DataRow(4, true, true, true)]
    [DataRow(1, false, true, true)]
    public void IndexBlockValueDeltaEncoding(int restartInterval, bool deltaEncoded, bool valueDeltaEscape, bool commonPrefix)
    {
        var rnd = new Random(7);
        List<byte[]> keys = SortedInternalKeys(200, rnd, 24, commonPrefix ? "idx/" : "");
        var handles = new List<BlockHandle>();
        using var b = new BlockBuilder(restartInterval, true, deltaEncoded, DataBlockIndexType.BinarySearch, 0.75, commonPrefix, isUserKey: false);
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
            if (i > 0) IndexValue.EncodeDelta(delta, h, prev, valueDeltaEscape, false, default);
            b.Add(keys[i], full.WrittenSpan, delta.WrittenSpan, false);
            prev = h;
            offset += h.Size + BlockBasedTableConstants.BlockTrailerSize;
            // With the format_version 8 escape, non-contiguous handles (e.g. padding) remain delta entries.
            if (valueDeltaEscape && i % 5 == 0) offset += (ulong)rnd.Next(1, 4096);
        }
        Block block = Block.CreateCopy(b.Finish());
        using var it = new BlockIter();
        it.InitializeIndex(block, InternalKeyComparator.Bytewise, keyIncludesSeq: true, valueIsFull: !deltaEncoded, haveFirstKey: false, valueDeltaEscape: valueDeltaEscape, BlockIter.DisableGlobalSequenceNumber);
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
        it.InitializeData(block, InternalKeyComparator.Bytewise, 42);
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
