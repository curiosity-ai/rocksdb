using RocksDb.Managed.Db;
using RocksDb.Managed.Table;
using RocksDb.Managed.Table.Filters;
using RocksDb.Managed.Util;
using static RocksDb.Managed.Tests.Table.TableTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4, slice "TableBlocks": blocks, block iterators, footer, index/filter builders and the built-in filters
/// against table/block_based/block.{h,cc}, table/format.cc and table/block_based/filter_policy.cc.
/// </summary>
[TestClass]
public sealed class Audit4TableBlocksTests
{
    private const int FuzzRounds = 4000;

    private static byte[] Bytes(params object[] parts)
    {
        var ms = new MemoryStream();
        foreach (object p in parts)
        {
            if (p is string s) ms.Write(System.Text.Encoding.ASCII.GetBytes(s));
            else if (p is byte[] b) ms.Write(b);
            else ms.WriteByte(Convert.ToByte(p, System.Globalization.CultureInfo.InvariantCulture));
        }
        return ms.ToArray();
    }

    private static byte[] BuildDataBlock(List<byte[]> keys, int restartInterval, DataBlockIndexType indexType = DataBlockIndexType.BinarySearch,
        bool commonPrefix = false, bool separatedKv = false)
    {
        using var b = new BlockBuilder(restartInterval, true, false, indexType, 0.75, commonPrefix, isUserKey: false, stripTimestampSize: 0,
            useSeparatedKvStorage: separatedKv, uniformCvThreshold: -1);
        for (int i = 0; i < keys.Count; i++) b.Add(keys[i], Bytes("v", i.ToString(System.Globalization.CultureInfo.InvariantCulture)));
        return b.Finish().ToArray();
    }

    private static List<byte[]> SortedKeys(int count, string prefix, ulong seq)
    {
        var keys = new List<byte[]>();
        for (int i = 0; i < count; i++) keys.Add(IKey(prefix + (i * 7).ToString("D5", System.Globalization.CultureInfo.InvariantCulture), seq));
        return keys;
    }

    // ---------------------------------------------------------------------------------------------------------------
    // F1: global seqno override must not leak into the delta base

    /// <summary>
    /// BlockIter::ParseNextKey delta-decodes against raw_key_ (the stored key) while DataBlockIter::UpdateKey exposes the
    /// global seqno through a separate key_buf_ (table/block_based/block.h, UpdateKey; block.cc:697 TrimAppend). A user
    /// key that extends the previous user key by the previous key's footer bytes shares more than the previous user key,
    /// so the shared bytes include stored footer bytes (type, seqno 0) and must not be read back as the global seqno.
    /// </summary>
    [TestMethod]
    [DataRow(1)]
    [DataRow(4)]
    [DataRow(16)]
    public void GlobalSeqnoDoesNotCorruptKeysSharingFooterBytes(int restartInterval)
    {
        const ulong GlobalSeqno = 0x0102030405;
        // "a" | footer(seq 0, Value) is a prefix of the following internal keys.
        byte[] footer = [(byte)EntryType.Value, 0, 0, 0, 0, 0, 0, 0];
        var userKeys = new List<byte[]>
        {
            Bytes("a"),
            Bytes("a", footer),
            Bytes("a", footer, "b"),
            Bytes("a", footer, "b", footer, "c"),
            Bytes("b"),
            Bytes("b", footer, footer),
        };
        var keys = new List<byte[]>();
        foreach (byte[] uk in userKeys) keys.Add(IKey(uk, 0));
        Block block = Block.CreateCopy(BuildDataBlock(keys, restartInterval));
        using var it = new BlockIter();
        it.InitializeData(block, InternalKeyComparator.Bytewise, GlobalSeqno);

        var expected = new List<byte[]>();
        foreach (byte[] uk in userKeys) expected.Add(IKey(uk, GlobalSeqno));

        int n = 0;
        for (it.SeekToFirst(); it.Valid; it.Next(), n++) CollectionAssert.AreEqual(expected[n], it.Key.ToArray(), "forward " + n);
        Assert.AreEqual(expected.Count, n);

        // Backward (DataBlockIter::PrevImpl, with and without the prev-entries cache), then forward again from each
        // position: a Next after a cached Prev delta-decodes against the key restored from the cache.
        n = expected.Count - 1;
        for (it.SeekToLast(); it.Valid; it.Prev(), n--) CollectionAssert.AreEqual(expected[n], it.Key.ToArray(), "backward " + n);
        Assert.AreEqual(-1, n);
        for (int start = expected.Count - 1; start >= 1; start--)
        {
            it.SeekToLast();
            for (int i = expected.Count - 1; i > start - 1; i--) it.Prev();
            CollectionAssert.AreEqual(expected[start - 1], it.Key.ToArray(), "prev to " + (start - 1));
            it.Next();
            CollectionAssert.AreEqual(expected[start], it.Key.ToArray(), "next after prev to " + (start - 1));
        }

        // Seek / SeekForPrev land on the exact entries.
        for (int i = 0; i < expected.Count; i++)
        {
            it.Seek(expected[i]);
            Assert.IsTrue(it.Valid);
            CollectionAssert.AreEqual(expected[i], it.Key.ToArray(), "seek " + i);
            it.SeekForPrev(expected[i]);
            Assert.IsTrue(it.Valid);
            CollectionAssert.AreEqual(expected[i], it.Key.ToArray(), "seekforprev " + i);
        }
        block.Release();
    }

    /// <summary>
    /// End to end: an ingested file that is assigned a global sequence number (it overlaps the memtable) and whose user
    /// keys extend each other by footer bytes. Before the fix the second key came back with the global seqno spliced into
    /// its user key, so Get missed it and the iterator returned a key that was never written.
    /// </summary>
    [TestMethod]
    public void IngestedFileWithGlobalSeqnoReturnsKeysSharingFooterBytes()
    {
        RocksDb.Managed.Env.MemoryFileSystem fs = RocksDb.Managed.Tests.Db.DbTestUtil.NewFs();
        fs.CreateDirectoryIfMissing("/ext");
        byte[] footer = [(byte)EntryType.Value, 0, 0, 0, 0, 0, 0, 0];
        byte[] k1 = Bytes("a");
        byte[] k2 = Bytes("a", footer, "b");
        byte[] k3 = Bytes("a", footer, "b", footer);
        using (var w = new SstFileWriter(new ColumnFamilyOptions(), fs))
        {
            w.Open("/ext/1.sst");
            w.Put(k1, "v1"u8);
            w.Put(k2, "v2"u8);
            w.Put(k3, "v3"u8);
            w.Finish();
        }
        using RocksDatabase db = RocksDatabase.Open("/audit4blocks", RocksDb.Managed.Tests.Db.DbTestUtil.NewOptions(fs));
        for (int i = 0; i < 300; i++) db.Put(Bytes("filler", i.ToString(System.Globalization.CultureInfo.InvariantCulture)), "x"u8);
        db.Put(k1, "old"u8); // overlap: the file gets a global sequence number above every existing one
        db.IngestExternalFiles(["/ext/1.sst"], new IngestExternalFileOptions());
        CollectionAssert.AreEqual("v1"u8.ToArray(), db.Get(k1));
        CollectionAssert.AreEqual("v2"u8.ToArray(), db.Get(k2));
        CollectionAssert.AreEqual("v3"u8.ToArray(), db.Get(k3));
        using Iterator it = db.NewIterator();
        it.Seek(k1);
        foreach (byte[] k in new[] { k1, k2, k3 })
        {
            Assert.IsTrue(it.Valid);
            CollectionAssert.AreEqual(k, it.Key.ToArray());
            it.Next();
        }
    }

    /// <summary>Same as above for an index block with internal-key separators (IndexBlockIter shares BlockIter::ParseNextKey / UpdateKey).</summary>
    [TestMethod]
    public void GlobalSeqnoDoesNotCorruptIndexKeysSharingFooterBytes()
    {
        const ulong GlobalSeqno = 77;
        byte[] footer = [(byte)EntryType.Value, 0, 0, 0, 0, 0, 0, 0];
        var userKeys = new List<byte[]> { Bytes("k"), Bytes("k", footer, "x"), Bytes("k", footer, "y") };
        using var b = new BlockBuilder(16, true, true, DataBlockIndexType.BinarySearch, 0.75);
        using var full = new PooledBufferWriter(32);
        using var delta = new PooledBufferWriter(32);
        var handles = new List<BlockHandle>();
        BlockHandle prev = default;
        for (int i = 0; i < userKeys.Count; i++)
        {
            var h = new BlockHandle(i == 0 ? 0 : prev.Offset + prev.Size + BlockBasedTableConstants.BlockTrailerSize, (ulong)(100 + i));
            full.Clear();
            IndexValue.EncodeFull(full, h, false, default);
            delta.Clear();
            if (i > 0) IndexValue.EncodeDelta(delta, h, prev, false, false, default);
            b.Add(IKey(userKeys[i], 0), full.WrittenSpan, delta.WrittenSpan, false);
            handles.Add(h);
            prev = h;
        }
        Block block = Block.CreateCopy(b.Finish());
        using var it = new BlockIter();
        it.InitializeIndex(block, InternalKeyComparator.Bytewise, keyIncludesSeq: true, valueIsFull: false, haveFirstKey: false, valueDeltaEscape: false, GlobalSeqno);
        int n = 0;
        for (it.SeekToFirst(); it.Valid; it.Next(), n++)
        {
            CollectionAssert.AreEqual(IKey(userKeys[n], GlobalSeqno), it.Key.ToArray(), "index key " + n);
            Assert.AreEqual(handles[n], it.BlockHandle);
        }
        Assert.AreEqual(userKeys.Count, n);
        block.Release();
    }

    // ---------------------------------------------------------------------------------------------------------------
    // F2: malformed blocks surface as Corruption

    /// <summary>
    /// A data block entry whose internal key is shorter than the 8-byte footer (upstream: ExtractUserKey asserts,
    /// undefined behaviour in release builds) must be a Corruption, not an ArgumentOutOfRangeException out of the
    /// comparator. Block bytes: one entry shared=0, non_shared=3, value_length=1, then one restart at 0.
    /// </summary>
    [TestMethod]
    public void InternalKeyShorterThanFooterIsCorruption()
    {
        byte[] raw = [0, 3, 1, (byte)'a', (byte)'b', (byte)'c', (byte)'v', 0, 0, 0, 0, 1, 0, 0, 0];
        Block block = Block.CreateCopy(raw);
        using var it = new BlockIter();
        it.InitializeData(block, InternalKeyComparator.Bytewise, BlockIter.DisableGlobalSequenceNumber);
        byte[] target = IKey("abc", 5);
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => it.SeekToFirst()).Code);
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => it.Seek(target)).Code);
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => it.SeekForPrev(target)).Code);
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => it.SeekToLast()).Code);
        // The same bytes are a valid meta block (raw keys).
        it.InitializeMeta(block);
        it.SeekToFirst();
        Assert.IsTrue(it.Valid);
        CollectionAssert.AreEqual(Bytes("abc"), it.Key.ToArray());
        block.Release();
    }

    public enum FuzzKind { Data, DataHash, DataCommonPrefix, DataSeparated, IndexDelta, IndexInterpolation, Meta }

    private static byte[] BuildFuzzBlock(FuzzKind kind, out List<byte[]> keys)
    {
        switch (kind)
        {
            case FuzzKind.Data:
                keys = SortedKeys(60, "key", 9);
                return BuildDataBlock(keys, 4);
            case FuzzKind.DataHash:
                keys = SortedKeys(60, "key", 9);
                return BuildDataBlock(keys, 3, DataBlockIndexType.BinaryAndHash);
            case FuzzKind.DataCommonPrefix:
                keys = SortedKeys(60, "common/prefix/", 9);
                return BuildDataBlock(keys, 4, DataBlockIndexType.BinarySearch, commonPrefix: true);
            case FuzzKind.DataSeparated:
                keys = SortedKeys(60, "key", 9);
                return BuildDataBlock(keys, 4, DataBlockIndexType.BinarySearch, false, separatedKv: true);
            case FuzzKind.Meta:
            {
                keys = new List<byte[]>();
                using var b = BlockBuilder.ForMetaBlock(3);
                for (int i = 0; i < 30; i++)
                {
                    byte[] k = Bytes("rocksdb.meta." + i.ToString("D3", System.Globalization.CultureInfo.InvariantCulture));
                    keys.Add(k);
                    b.Add(k, Bytes("value", i));
                }
                return b.Finish().ToArray();
            }
            default:
            {
                // Index block: user-key separators, delta-encoded values (format_version 4..7), first keys.
                keys = new List<byte[]>();
                int count = kind == FuzzKind.IndexInterpolation ? 200 : 40;
                using var b = new BlockBuilder(kind == FuzzKind.IndexInterpolation ? 1 : 4, true, true, DataBlockIndexType.BinarySearch, 0.75,
                    useCommonPrefix: false, isUserKey: true);
                using var full = new PooledBufferWriter(64);
                using var delta = new PooledBufferWriter(64);
                BlockHandle prev = default;
                for (int i = 0; i < count; i++)
                {
                    byte[] uk = Bytes("sep" + (i * 13).ToString("D6", System.Globalization.CultureInfo.InvariantCulture));
                    byte[] firstKey = IKey(uk, 3);
                    var h = new BlockHandle(i == 0 ? 0 : prev.Offset + prev.Size + BlockBasedTableConstants.BlockTrailerSize, (ulong)(4000 + (i % 7)));
                    full.Clear();
                    IndexValue.EncodeFull(full, h, true, firstKey);
                    delta.Clear();
                    if (i > 0) IndexValue.EncodeDelta(delta, h, prev, false, true, firstKey);
                    b.Add(uk, full.WrittenSpan, delta.WrittenSpan, false);
                    keys.Add(IKey(uk, 3)); // seek targets are internal keys
                    prev = h;
                }
                return b.Finish().ToArray();
            }
        }
    }

    private static void InitFuzzIter(BlockIter it, Block block, FuzzKind kind)
    {
        switch (kind)
        {
            case FuzzKind.Meta:
                it.InitializeMeta(block);
                break;
            case FuzzKind.IndexDelta:
            case FuzzKind.IndexInterpolation:
                it.InitializeIndex(block, InternalKeyComparator.Bytewise, keyIncludesSeq: false, valueIsFull: false, haveFirstKey: true,
                    valueDeltaEscape: false, BlockIter.DisableGlobalSequenceNumber);
                if (kind == FuzzKind.IndexInterpolation) it.UseInterpolationSearch();
                break;
            default:
                it.InitializeData(block, InternalKeyComparator.Bytewise, BlockIter.DisableGlobalSequenceNumber);
                break;
        }
    }

    /// <summary>Runs every positioning call; returns the number of steps taken. Throws whatever the iterator throws.</summary>
    private static void ExerciseIterator(BlockIter it, FuzzKind kind, List<byte[]> keys, Random rnd, int maxSteps)
    {
        int steps = 0;
        for (it.SeekToFirst(); it.Valid; it.Next())
        {
            _ = it.Key.Length + it.Value.Length;
            if (++steps > maxSteps) Assert.Fail("forward scan does not terminate");
        }
        bool index = kind is FuzzKind.IndexDelta or FuzzKind.IndexInterpolation;
        steps = 0;
        for (it.SeekToLast(); it.Valid; it.Prev())
        {
            _ = it.Key.Length + it.Value.Length;
            if (++steps > maxSteps) Assert.Fail("backward scan does not terminate");
        }
        for (int i = 0; i < 6; i++)
        {
            byte[] target = keys[rnd.Next(keys.Count)];
            if (kind == FuzzKind.Meta) target = target.AsSpan(0, rnd.Next(1, target.Length + 1)).ToArray();
            it.Seek(target);
            steps = 0;
            while (it.Valid && steps++ < 3) it.Next();
            if (!index)
            {
                it.SeekForPrev(target);
                steps = 0;
                while (it.Valid && steps++ < 3) it.Prev();
            }
            if (kind is FuzzKind.Data or FuzzKind.DataHash or FuzzKind.DataCommonPrefix or FuzzKind.DataSeparated)
            {
                if (it.SeekForGet(target) && it.Valid) _ = it.Key.Length + it.Value.Length;
            }
        }
    }

    /// <summary>
    /// Random byte flips in valid blocks of every flavour: Block::Block (footer / restart validation), DecodeEntry,
    /// BlockIter::ParseNextKey, BinarySeekRestartPointIndex, InterpolationSeekRestartPointIndex, DataBlockIter::
    /// SeekForGetImpl and IndexValue::DecodeFrom either work or report Corruption (upstream: CorruptionError()); no other
    /// exception type and no endless scan.
    /// </summary>
    [TestMethod]
    [DataRow(FuzzKind.Data)]
    [DataRow(FuzzKind.DataHash)]
    [DataRow(FuzzKind.DataCommonPrefix)]
    [DataRow(FuzzKind.DataSeparated)]
    [DataRow(FuzzKind.IndexDelta)]
    [DataRow(FuzzKind.IndexInterpolation)]
    [DataRow(FuzzKind.Meta)]
    public void CorruptedBlocksNeverThrowAnythingButCorruption(FuzzKind kind)
    {
        byte[] valid = BuildFuzzBlock(kind, out List<byte[]> keys);
        var rnd = new Random(20261110 + (int)kind);
        using var it = new BlockIter();
        // The unmodified block is fully readable.
        {
            Block b = Block.CreateCopy(valid);
            InitFuzzIter(it, b, kind);
            ExerciseIterator(it, kind, keys, rnd, valid.Length + 1);
            it.Clear();
            b.Release();
        }
        for (int round = 0; round < FuzzRounds; round++)
        {
            byte[] mutated = (byte[])valid.Clone();
            int flips = rnd.Next(1, 4);
            string what = "round " + round + ":";
            for (int f = 0; f < flips; f++)
            {
                // Bias towards the trailer (restart array, hash index, footer) every other round.
                int pos = (round & 1) == 0 ? rnd.Next(mutated.Length) : mutated.Length - 1 - rnd.Next(Math.Min(mutated.Length, 48));
                byte v = (byte)rnd.Next(256);
                what += " [" + pos + "]=" + v;
                mutated[pos] = v;
            }
            int length = rnd.Next(8) == 0 ? rnd.Next(mutated.Length + 1) : mutated.Length; // sometimes truncated
            what += " len=" + length;
            Block? block = null;
            try
            {
                block = Block.CreateCopy(mutated.AsSpan(0, length));
                InitFuzzIter(it, block, kind);
                ExerciseIterator(it, kind, keys, rnd, mutated.Length + 1);
            }
            catch (RocksDbException e)
            {
                Assert.AreEqual(StatusCode.Corruption, e.Code, what);
            }
            catch (Exception e) when (e is not AssertFailedException)
            {
                Assert.Fail(what + ": " + e.GetType().Name + ": " + e.Message);
            }
            finally
            {
                it.Clear();
                block?.Release();
            }
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Data block hash index (DataBlockIter::SeekForGetImpl)

    /// <summary>
    /// SeekForGetImpl contract (block.cc:273): "false" only when no entry of the user key is at or after the target in
    /// this block and none can be in the next one; when the search runs off the end of the block the result is "true"
    /// with an invalid iterator
    /// (the caller continues with the next block), in particular for a target whose user key is the block's last user
    /// key but whose sequence number is lower than every entry here (case 1 of the upstream comment).
    /// </summary>
    [TestMethod]
    [DataRow(1)]
    [DataRow(3)]
    [DataRow(16)]
    public void SeekForGetFallBackRules(int restartInterval)
    {
        var keys = new List<byte[]>();
        for (int i = 0; i < 40; i++)
        {
            string uk = "user" + (i * 3).ToString("D4", System.Globalization.CultureInfo.InvariantCulture);
            keys.Add(IKey(uk, 20));
            if (i % 5 == 0) keys.Add(IKey(uk, 10, EntryType.Deletion));
            if (i % 10 == 0) keys.Add(IKey(uk, 5, EntryType.Merge));
        }
        byte[] lastUserKey = DbFormat.ExtractUserKey(keys[^1]).ToArray();
        Block block = Block.CreateCopy(BuildDataBlock(keys, restartInterval, DataBlockIndexType.BinaryAndHash));
        Assert.IsTrue(block.HasHashIndex);
        using var it = new BlockIter();
        using var reference = new BlockIter();
        it.InitializeData(block, InternalKeyComparator.Bytewise, BlockIter.DisableGlobalSequenceNumber);
        reference.InitializeData(block, InternalKeyComparator.Bytewise, BlockIter.DisableGlobalSequenceNumber);

        string lastUk = UserKeyOf(keys[^1]);
        var present = new HashSet<string>();
        foreach (byte[] k in keys) present.Add(UserKeyOf(k));

        // Every (user key, snapshot) combination, present or not.
        for (int i = -1; i <= 120; i++)
        {
            string uk = "user" + i.ToString("D4", System.Globalization.CultureInfo.InvariantCulture);
            foreach (ulong seq in new ulong[] { DbFormat.MaxSequenceNumber, 20, 15, 10, 7, 5, 4, 0 })
            {
                byte[] target = IKey(uk, seq, DbFormat.ValueTypeForSeek);
                string trace = uk + "@" + seq;
                bool mayExist = it.SeekForGet(target);
                reference.Seek(target);
                if (!mayExist)
                {
                    // "not in this block and cannot be in the next block either": no entry of this user key is visible
                    // at the target's sequence number here, and the block does not end with this user key.
                    Assert.IsFalse(reference.Valid && UserKeyOf(reference.Key) == uk, trace);
                    Assert.AreNotEqual(lastUk, uk, trace);
                    continue;
                }
                if (!it.Valid)
                {
                    // Ran off the end of the block: a total-order seek must agree that nothing here is >= target.
                    Assert.IsFalse(reference.Valid, trace);
                    continue;
                }
                if (present.Contains(uk))
                {
                    // Positioned exactly like Seek.
                    Assert.IsTrue(reference.Valid, trace);
                    CollectionAssert.AreEqual(reference.Key.ToArray(), it.Key.ToArray(), trace);
                    CollectionAssert.AreEqual(reference.Value.ToArray(), it.Value.ToArray(), trace);
                }
                else
                {
                    // Absent key with a hash collision: wherever the iterator stands, it is not on the target user key.
                    Assert.AreNotEqual(uk, UserKeyOf(it.Key), trace);
                }
            }
        }

        // Case 1: the last user key with a sequence number below all of its entries -> continue with the next block.
        ulong lowest = DbFormat.ExtractFooter(keys[^1]) >> 8;
        Assert.IsTrue(lowest > 0);
        Assert.IsTrue(it.SeekForGet(IKey(lastUserKey, lowest - 1, DbFormat.ValueTypeForSeek)));
        Assert.IsFalse(it.Valid);
        block.Release();
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Filters

    private static byte[] BuildFilter(IFilterBitsBuilder builder, int keys)
    {
        using var w = new PooledBufferWriter(1024);
        for (int i = 0; i < keys; i++) builder.AddKey(Bytes("filterkey", i.ToString(System.Globalization.CultureInfo.InvariantCulture)));
        builder.Finish(w);
        builder.Dispose();
        return w.WrittenSpan.ToArray();
    }

    /// <summary>
    /// F3. BuiltinFilterPolicy::GetBloomBitsReader does not look at the filter length: a FastLocalBloom filter whose data
    /// is shorter than one 64-byte cache line (only possible with a damaged filter block) has no line to probe. The
    /// reader must be safe (always "may match"), not throw out of MayMatch.
    /// </summary>
    [TestMethod]
    public void FastLocalBloomShorterThanACacheLineIsAlwaysTrue()
    {
        foreach (int len in new[] { 1, 5, 63 })
        {
            byte[] contents = new byte[len + BuiltinFilterFormat.MetadataLen];
            contents[len] = unchecked((byte)BuiltinFilterFormat.NewBloomMarker);
            contents[len + 1] = 0; // FastLocalBloom
            contents[len + 2] = 6; // 64-byte blocks, 6 probes
            BuiltinFilterBitsReader reader = BuiltinFilterBitsReader.Create(contents);
            Assert.IsTrue(reader.MayMatch("anything"u8), "len=" + len);
            Assert.IsTrue(reader.HashMayMatch(0x123456789abcdefUL), "len=" + len);
        }
        // One full line is a real filter: all-zero bits match nothing.
        byte[] oneLine = new byte[64 + BuiltinFilterFormat.MetadataLen];
        oneLine[64] = unchecked((byte)BuiltinFilterFormat.NewBloomMarker);
        oneLine[66] = 6;
        Assert.IsInstanceOfType<FastLocalBloomBitsReader>(BuiltinFilterBitsReader.Create(oneLine));
        Assert.IsFalse(BuiltinFilterBitsReader.Create(oneLine).MayMatch("anything"u8));
    }

    /// <summary>
    /// Random damage (byte flips, truncation) to FastLocalBloom, Standard128 Ribbon and legacy Bloom filters:
    /// GetBuiltinFilterBitsReader and the readers' MayMatch return a boolean or report Corruption, nothing else.
    /// </summary>
    [TestMethod]
    [DataRow(0)]
    [DataRow(1)]
    [DataRow(2)]
    public void CorruptedFiltersNeverThrowAnythingButCorruption(int flavour)
    {
        byte[] valid = flavour switch
        {
            0 => BuildFilter(new FastLocalBloomBitsBuilder(10000), 300),
            1 => BuildFilter(new Standard128RibbonBitsBuilder(new RibbonFilterPolicy(10, -1).DesiredOneInFpRate, 10000), 3000),
            _ => BuildFilter(new LegacyBloomBitsBuilder(10), 300),
        };
        if (flavour == 1) Assert.AreEqual(unchecked((byte)BuiltinFilterFormat.RibbonMarker), valid[^5]);
        var rnd = new Random(77 + flavour);
        for (int round = 0; round < FuzzRounds; round++)
        {
            byte[] mutated = (byte[])valid.Clone();
            string what = "round " + round + ":";
            int flips = rnd.Next(0, 3);
            for (int f = 0; f < flips; f++)
            {
                // Mostly the 5 metadata bytes, which select the implementation and its geometry.
                int pos = rnd.Next(4) != 0 ? mutated.Length - 1 - rnd.Next(5) : rnd.Next(mutated.Length);
                byte v = (byte)rnd.Next(256);
                what += " [" + pos + "]=" + v;
                mutated[pos] = v;
            }
            int cut = rnd.Next(3) == 0 ? rnd.Next(mutated.Length + 1) : 0;
            what += " cut=" + cut;
            // Truncate from the front so the metadata trailer stays in place.
            ReadOnlyMemory<byte> contents = mutated.AsMemory(cut);
            try
            {
                BuiltinFilterBitsReader reader = BuiltinFilterBitsReader.Create(contents);
                for (int i = 0; i < 40; i++)
                {
                    _ = reader.MayMatch(Bytes("filterkey", (i * 31).ToString(System.Globalization.CultureInfo.InvariantCulture)));
                    _ = reader.HashMayMatch((ulong)rnd.NextInt64());
                }
            }
            catch (RocksDbException e)
            {
                Assert.AreEqual(StatusCode.Corruption, e.Code, what);
            }
            catch (Exception e)
            {
                Assert.Fail(what + ": " + e.GetType().Name + ": " + e.Message);
            }
        }
    }

    /// <summary>
    /// F4. RibbonFilterPolicy::GetBuilderWithContext (filter_policy.cc) only looks at the flush reason and the level
    /// under level and universal compaction; with FIFO or "none" the file is treated as bottommost (INT_MAX - 1), so a
    /// flush gets a Ribbon filter unless bloom_before_level is INT_MAX.
    /// </summary>
    [TestMethod]
    public void RibbonPolicyTreatsFifoAndNoneAsBottommost()
    {
        var ribbon = new RibbonFilterPolicy(10, 1);
        foreach (CompactionStyle style in new[] { CompactionStyle.Fifo, CompactionStyle.None })
        {
            using IFilterBitsBuilder flush = ribbon.CreateBuilder(new FilterBuildingContext(5, 0, true) { CompactionStyle = style });
            Assert.IsInstanceOfType<Standard128RibbonBitsBuilder>(flush, style.ToString());
            using IFilterBitsBuilder l0 = ribbon.CreateBuilder(new FilterBuildingContext(5, 0, false) { CompactionStyle = style });
            Assert.IsInstanceOfType<Standard128RibbonBitsBuilder>(l0, style.ToString());
        }
        foreach (CompactionStyle style in new[] { CompactionStyle.Level, CompactionStyle.Universal })
        {
            using IFilterBitsBuilder flush = ribbon.CreateBuilder(new FilterBuildingContext(5, 0, true) { CompactionStyle = style });
            Assert.IsInstanceOfType<FastLocalBloomBitsBuilder>(flush, style.ToString());
            using IFilterBitsBuilder l1 = ribbon.CreateBuilder(new FilterBuildingContext(5, 1, false) { CompactionStyle = style });
            Assert.IsInstanceOfType<Standard128RibbonBitsBuilder>(l1, style.ToString());
        }
        // INT_MAX == always Bloom, whatever the style.
        ribbon.BloomBeforeLevel = int.MaxValue;
        using IFilterBitsBuilder always = ribbon.CreateBuilder(new FilterBuildingContext(5, 0, true) { CompactionStyle = CompactionStyle.Fifo });
        Assert.IsInstanceOfType<FastLocalBloomBitsBuilder>(always);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Footer, properties, prefix index

    /// <summary>
    /// Footer::DecodeFrom on damaged footers of every format_version: success, Corruption or NotSupported (legacy
    /// block-based magic, unknown incompatible features), never another exception. format_version &gt;= 6 footers are
    /// checksummed, so any single flipped byte in them must be rejected.
    /// </summary>
    [TestMethod]
    public void CorruptedFootersAreRejectedCleanly()
    {
        var rnd = new Random(4242);
        for (int fv = 2; fv <= BlockBasedTableConstants.LatestSupportedFormatVersion; fv++)
        {
            byte[] valid = new byte[Footer.EncodedLength];
            const ulong FooterOffset = 100000;
            var meta = new BlockHandle(FooterOffset - 300 - BlockBasedTableConstants.BlockTrailerSize, 300);
            Footer.Build(valid, fv, FooterOffset, ChecksumType.Xxh3, meta, fv >= 6 ? BlockHandle.Null : new BlockHandle(5000, 700), fv >= 6 ? 0x1234567u : 0u);
            Footer ok = Footer.Decode(valid, FooterOffset);
            Assert.AreEqual(fv, ok.FormatVersion);
            Assert.AreEqual(meta, ok.MetaindexHandle);
            for (int round = 0; round < 1500; round++)
            {
                byte[] mutated = (byte[])valid.Clone();
                int pos = rnd.Next(mutated.Length);
                byte v = (byte)rnd.Next(256);
                bool changed = mutated[pos] != v;
                mutated[pos] = v;
                string what = "fv " + fv + " [" + pos + "]=" + v;
                try
                {
                    Footer f = Footer.Decode(mutated, FooterOffset);
                    if (fv >= 6) Assert.IsFalse(changed, what + ": damaged checksummed footer accepted");
                }
                catch (RocksDbException e)
                {
                    Assert.IsTrue(e.Code is StatusCode.Corruption or StatusCode.NotSupported, what + ": " + e.Code);
                }
                catch (Exception e) when (e is not AssertFailedException)
                {
                    Assert.Fail(what + ": " + e.GetType().Name + ": " + e.Message);
                }
            }
            // A footer read at another offset fails its context checksum (format_version >= 6).
            if (fv >= 6)
                Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => Footer.Decode(valid, FooterOffset + 1)).Code);
        }
    }

    /// <summary>
    /// ParsePropertiesBlock (table/meta_blocks.cc) and BlockPrefixIndex::Create (block_prefix_index.cc) on damaged
    /// input: a result or Corruption.
    /// </summary>
    [TestMethod]
    public void CorruptedPropertiesAndPrefixMetaAreRejectedCleanly()
    {
        var props = new TableProperties { NumEntries = 123456, DataSize = 99, ComparatorName = "leveldb.BytewiseComparator", ColumnFamilyName = "default" };
        byte[] valid;
        using (var pb = new PropertyBlockBuilder())
        {
            props.AddTo(pb);
            pb.Add("user.collected", [1, 2, 3]);
            valid = pb.Finish().ToArray();
        }
        {
            Block b = Block.CreateCopy(valid);
            TableProperties parsed = TableProperties.Parse(b, 1000);
            Assert.AreEqual(123456UL, parsed.NumEntries);
            Assert.AreEqual("default", parsed.ColumnFamilyName);
            CollectionAssert.AreEqual(new byte[] { 1, 2, 3 }, parsed.UserCollectedProperties["user.collected"]);
            b.Release();
        }
        var rnd = new Random(99);
        for (int round = 0; round < FuzzRounds; round++)
        {
            byte[] mutated = (byte[])valid.Clone();
            int flips = rnd.Next(1, 4);
            string what = "round " + round;
            for (int f = 0; f < flips; f++) mutated[rnd.Next(mutated.Length)] = (byte)rnd.Next(256);
            Block? block = null;
            try
            {
                block = Block.CreateCopy(mutated);
                _ = TableProperties.Parse(block, 1000);
            }
            catch (RocksDbException e)
            {
                Assert.AreEqual(StatusCode.Corruption, e.Code, what);
            }
            catch (Exception e)
            {
                Assert.Fail(what + ": " + e.GetType().Name + ": " + e.Message);
            }
            finally
            {
                block?.Release();
            }
        }

        // Prefix index meta blocks: 3 prefixes "aa", "bb", "cc" over data blocks 0..1, 1..3, 4.
        byte[] prefixes = Bytes("aabbcc");
        byte[] metaBlock = [2, 0, 2, 2, 1, 3, 2, 4, 1];
        BlockPrefixIndex index = BlockPrefixIndex.Create(prefixes, metaBlock);
        CollectionAssert.AreEqual(new uint[] { 0, 1 }, index.GetBlocks("aa"u8).ToArray()[..2]);
        for (int round = 0; round < FuzzRounds; round++)
        {
            byte[] m = (byte[])metaBlock.Clone();
            for (int f = rnd.Next(1, 3); f > 0; f--) m[rnd.Next(m.Length)] = (byte)rnd.Next(256);
            int len = rnd.Next(m.Length + 1);
            try
            {
                BlockPrefixIndex idx = BlockPrefixIndex.Create(prefixes, m.AsSpan(0, len));
                foreach (string p in new[] { "aa", "bb", "cc", "zz" }) _ = idx.GetBlocks(System.Text.Encoding.ASCII.GetBytes(p)).Length;
            }
            catch (RocksDbException e)
            {
                Assert.AreEqual(StatusCode.Corruption, e.Code);
            }
            catch (Exception e)
            {
                Assert.Fail("round " + round + ": " + e.GetType().Name + ": " + e.Message + " meta=" + Convert.ToHexString(m.AsSpan(0, len)));
            }
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // IndexValue (table/format.cc)

    /// <summary>
    /// IndexValue::EncodeTo / DecodeFrom delta forms at the boundaries: size deltas of either sign, the format_version
    /// &gt;= 8 escape for a non-contiguous handle (varint 0 + full handle) and the "+1" shift that keeps 0 free, plus
    /// truncated input (DecodeFrom returns Corruption: "bad delta-encoded index value").
    /// </summary>
    [TestMethod]
    public void IndexValueDeltaForms()
    {
        var prev = new BlockHandle(1000, 500);
        ulong contiguous = prev.Offset + prev.Size + BlockBasedTableConstants.BlockTrailerSize;
        using var w = new PooledBufferWriter(64);
        foreach (bool escape in new[] { false, true })
        {
            foreach (ulong size in new ulong[] { 0, 1, 499, 500, 501, 1UL << 40 })
            {
                var h = new BlockHandle(contiguous, size);
                w.Clear();
                IndexValue.EncodeDelta(w, h, prev, escape, true, "first"u8);
                ReadOnlySpan<byte> input = w.WrittenSpan;
                Assert.IsTrue(IndexValue.TryDecode(ref input, true, true, prev, escape, out BlockHandle decoded, out int fko, out int fkl));
                Assert.AreEqual(h, decoded, "escape=" + escape + " size=" + size);
                Assert.IsTrue(input.IsEmpty);
                Assert.IsTrue(w.WrittenSpan.Slice(fko, fkl).SequenceEqual("first"u8));
                // Equal sizes: zigzag(0) = 0 without the escape, 1 with it.
                if (size == prev.Size) Assert.AreEqual(escape ? 1 : 0, w.WrittenSpan[0]);
                // Every strict prefix is malformed.
                for (int cut = 0; cut < w.WrittenCount; cut++)
                {
                    ReadOnlySpan<byte> truncated = w.WrittenSpan[..cut];
                    Assert.IsFalse(IndexValue.TryDecode(ref truncated, true, true, prev, escape, out _, out _, out _), "cut=" + cut);
                }
            }
        }
        // Non-contiguous handle: only expressible with the escape.
        var gap = new BlockHandle(contiguous + 4096, 123);
        w.Clear();
        IndexValue.EncodeDelta(w, gap, prev, true, false, default);
        Assert.AreEqual(0, w.WrittenSpan[0]);
        ReadOnlySpan<byte> g = w.WrittenSpan;
        Assert.IsTrue(IndexValue.TryDecode(ref g, false, true, prev, true, out BlockHandle gd, out _, out _));
        Assert.AreEqual(gap, gd);
    }
}
