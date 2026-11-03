using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Table;
using RocksDb.Managed.Util;
using static RocksDb.Managed.Tests.Table.TableTestUtil;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.Tests.Table;

/// <summary>Builder → reader round trips. Ports of meaningful cases from table/table_test.cc.</summary>
[TestClass]
public sealed class BlockBasedTableTests
{
    private static void VerifyAll(BlockBasedTableReader reader, List<(byte[] Key, byte[] Value)> entries, string trace)
    {
        using BlockBasedTableIterator it = reader.NewIterator();
        int i = 0;
        for (it.SeekToFirst(); it.Valid; it.Next(), i++)
        {
            CollectionAssert.AreEqual(entries[i].Key, it.Key.ToArray(), trace);
            CollectionAssert.AreEqual(entries[i].Value, it.Value.ToArray(), trace);
        }
        Assert.AreEqual(entries.Count, i, trace);
        i = entries.Count - 1;
        for (it.SeekToLast(); it.Valid; it.Prev(), i--) CollectionAssert.AreEqual(entries[i].Key, it.Key.ToArray(), trace);
        Assert.AreEqual(-1, i, trace);

        using var value = new PooledBufferWriter();
        for (int j = 0; j < entries.Count; j += Math.Max(1, entries.Count / 300))
        {
            byte[] uk = DbFormat.ExtractUserKey(entries[j].Key).ToArray();
            value.Clear();
            DbFormat.UnpackSequenceAndType(DbFormat.ExtractFooter(entries[j].Key), out ulong entrySeq, out _);
            TableGetResult r = reader.Get(IKey(uk, entrySeq, DbFormat.ValueTypeForSeek), value);
            Assert.IsTrue(r.Found, trace);
            CollectionAssert.AreEqual(entries[j].Value, value.WrittenSpan.ToArray(), trace);

            it.Seek(entries[j].Key);
            CollectionAssert.AreEqual(entries[j].Key, it.Key.ToArray(), trace);
            it.SeekForPrev(entries[j].Key);
            CollectionAssert.AreEqual(entries[j].Key, it.Key.ToArray(), trace);
            if (j + 1 < entries.Count && DbFormat.ExtractUserKey(entries[j + 1].Key).SequenceEqual(uk)) continue;
            // Between j and j+1: user key + "\0" sorts after key j.
            byte[] between = IKey([.. uk, 0], 5);
            it.Seek(between);
            if (j + 1 < entries.Count) CollectionAssert.AreEqual(entries[j + 1].Key, it.Key.ToArray(), trace);
            else Assert.IsFalse(it.Valid, trace);
            it.SeekForPrev(between);
            CollectionAssert.AreEqual(entries[j].Key, it.Key.ToArray(), trace);
            Assert.IsFalse(reader.Get(between, null).Found, trace);
        }
    }

    public static IEnumerable<object[]> FormatMatrix()
    {
        for (int fv = 2; fv <= BlockBasedTableConstants.LatestSupportedFormatVersion; fv++)
            foreach (ChecksumType ct in new[] { ChecksumType.Crc32c, ChecksumType.XxHash, ChecksumType.XxHash64, ChecksumType.Xxh3, ChecksumType.NoChecksum })
                yield return [fv, ct];
    }

    [TestMethod]
    [DynamicData(nameof(FormatMatrix))]
    public void RoundTripFormatVersionsAndChecksums(int formatVersion, ChecksumType checksum)
    {
        int seed = Environment.TickCount;
        string trace = "seed=" + seed + " fv=" + formatVersion + " cs=" + checksum;
        var rnd = new Random(seed);
        var options = new TableFormatOptions
        {
            FormatVersion = formatVersion,
            Checksum = checksum,
            BlockSize = rnd.Next(64, 4096),
            BlockRestartInterval = rnd.Next(1, 20),
            IndexBlockRestartInterval = rnd.Next(1, 5),
            DataBlockIndexType = rnd.Next(2) == 0 ? DataBlockIndexType.BinarySearch : DataBlockIndexType.BinaryAndHash,
            IndexType = rnd.Next(2) == 0 ? IndexType.BinarySearch : IndexType.BinarySearchWithFirstKey,
        };
        List<(byte[] Key, byte[] Value)> entries = MakeEntries(rnd.Next(1, 3000), seed);
        byte[] data = BuildTable(entries, options);
        using BlockBasedTableReader reader = Open(data);
        Assert.AreEqual(formatVersion, reader.Footer.FormatVersion, trace);
        Assert.AreEqual(checksum, reader.Footer.ChecksumType, trace);
        Assert.AreEqual((ulong)entries.Count, reader.Properties.NumEntries, trace);
        Assert.AreEqual((ulong)formatVersion, reader.Properties.FormatVersion, trace);
        Assert.AreEqual(options.IndexType, reader.IndexType, trace);
        Assert.AreEqual(formatVersion >= 4 ? 1UL : 0UL, reader.Properties.IndexValueIsDeltaEncoded, trace);
        Assert.AreEqual(formatVersion <= 2 ? 0UL : 1UL, reader.Properties.IndexKeyIsUserKey, trace);
        VerifyAll(reader, entries, trace);
    }

    [TestMethod]
    public void EmptyTable()
    {
        byte[] data = BuildTable([], TableFormatOptions.Default);
        using BlockBasedTableReader reader = Open(data);
        Assert.AreEqual(0UL, reader.Properties.NumEntries);
        Assert.AreEqual(0UL, reader.Properties.NumDataBlocks);
        using BlockBasedTableIterator it = reader.NewIterator();
        it.SeekToFirst();
        Assert.IsFalse(it.Valid);
        it.SeekToLast();
        Assert.IsFalse(it.Valid);
        it.Seek(IKey("a", 1));
        Assert.IsFalse(it.Valid);
        Assert.IsFalse(reader.Get(LookupKey("a"), null).Found);
    }

    [TestMethod]
    public void LargeKeysAndValues()
    {
        var rnd = new Random(3);
        var entries = new List<(byte[], byte[])>();
        for (int i = 0; i < 50; i++)
        {
            byte[] uk = new byte[rnd.Next(1, 20000)];
            rnd.NextBytes(uk);
            uk[0] = (byte)i; // keep sorted by first byte
            byte[] v = new byte[rnd.Next(0, 200000)];
            rnd.NextBytes(v);
            entries.Add((IKey(uk, 1), v));
        }
        byte[] data = BuildTable(entries, new TableFormatOptions { BlockSize = 1024 });
        using BlockBasedTableReader reader = Open(data);
        VerifyAll(reader, entries, "large");
    }

    [TestMethod]
    public void MultipleVersionsOfSameUserKeyAcrossBlocks()
    {
        // Same user key with many sequence numbers forces separators with seq (index_key_is_user_key = 0).
        var entries = new List<(byte[], byte[])>();
        for (ulong s = 500; s > 0; s--) entries.Add((IKey("dup", s), Encoding.ASCII.GetBytes("v" + s)));
        entries.Add((IKey("zzz", 1), [1]));
        byte[] data = BuildTable(entries, new TableFormatOptions { BlockSize = 128 });
        using BlockBasedTableReader reader = Open(data);
        Assert.AreEqual(0UL, reader.Properties.IndexKeyIsUserKey);
        VerifyAll(reader, entries, "dup");
        using var value = new PooledBufferWriter();
        foreach (ulong snap in new ulong[] { 1000, 500, 250, 1 })
        {
            value.Clear();
            TableGetResult r = reader.Get(IKey("dup", snap, DbFormat.ValueTypeForSeek), value);
            Assert.IsTrue(r.Found);
            ulong expected = Math.Min(snap, 500);
            Assert.AreEqual(expected, r.Sequence);
            Assert.AreEqual("v" + expected, Encoding.ASCII.GetString(value.WrittenSpan));
        }
        Assert.IsFalse(reader.Get(IKey("dup", 0, DbFormat.ValueTypeForSeek), null).Found);
        // Visitor sees all versions <= snapshot in order.
        int count = 0;
        reader.Get(IKey("dup", 300, DbFormat.ValueTypeForSeek), ref count, static (ref int c, ReadOnlySpan<byte> k, ReadOnlySpan<byte> v) => { c++; return true; });
        Assert.AreEqual(300, count);
    }

    [TestMethod]
    public void PropertiesAreRecorded()
    {
        var entries = new List<(byte[], byte[])>
        {
            (IKey("a", 10, EntryType.Value), [1, 2, 3]),
            (IKey("b", 11, EntryType.Deletion), []),
            (IKey("c", 12, EntryType.Merge), [4]),
            (IKey("d", 5, EntryType.RangeDeletion), Encoding.ASCII.GetBytes("f")),
        };
        var tbo = new TableBuilderOptions
        {
            ColumnFamilyId = 3,
            ColumnFamilyName = "cf3",
            DbId = "db-id",
            DbSessionId = "SESSION",
            FileNumber = 77,
            FileCreationTime = 1234,
            ExternalSstFile = true,
        };
        byte[] data = BuildTable(entries, TableFormatOptions.Default, tbo);
        using BlockBasedTableReader reader = Open(data);
        TableProperties p = reader.Properties;
        Assert.AreEqual(4UL, p.NumEntries);
        Assert.AreEqual(2UL, p.NumDeletions);
        Assert.AreEqual(1UL, p.NumMergeOperands);
        Assert.AreEqual(1UL, p.NumRangeDeletions);
        Assert.AreEqual(1UL, p.NumDataBlocks);
        Assert.AreEqual(3UL, p.ColumnFamilyId);
        Assert.AreEqual("cf3", p.ColumnFamilyName);
        Assert.AreEqual("db-id", p.DbId);
        Assert.AreEqual("SESSION", p.DbSessionId);
        Assert.AreEqual(77UL, p.OriginalFileNumber);
        Assert.AreEqual(1234UL, p.FileCreationTime);
        Assert.AreEqual("leveldb.BytewiseComparator", p.ComparatorName);
        Assert.AreEqual("nullptr", p.MergeOperatorName);
        Assert.AreEqual(";;", p.CompressionName);
        Assert.AreEqual(12UL, p.KeyLargestSeqno);
        Assert.AreEqual(5UL, p.KeySmallestSeqno);
        Assert.AreEqual(16UL, p.DataBlockRestartInterval);
        Assert.AreEqual(4 * 9UL, p.RawKeySize);
        Assert.AreEqual(5UL, p.RawValueSize);
        Assert.IsTrue(p.TailStartOffset > 0);
        Assert.IsTrue(p.ExternalSstFileGlobalSeqnoOffset > 0);
        Assert.AreEqual(2u, Coding.DecodeFixed32(p.UserCollectedProperties[TablePropertiesNames.ExternalSstFileVersion]));
        Assert.AreEqual("1", p.GetUserPropertyString(TablePropertiesNames.BlockBasedWholeKeyFiltering));
        // External file v2 with global seqno 0 → no rewriting.
        Assert.AreEqual(BlockIter.DisableGlobalSequenceNumber, reader.GlobalSequenceNumber);

        Assert.IsTrue(reader.HasRangeTombstones);
        using BlockIter? rt = reader.NewRangeTombstoneIterator();
        rt!.SeekToFirst();
        Assert.IsTrue(rt.Valid);
        Assert.AreEqual("d", UserKeyOf(rt.Key));
        Assert.AreEqual("f", Encoding.ASCII.GetString(rt.Value));
        rt.Next();
        Assert.IsFalse(rt.Valid);
        // Range tombstones are not point entries.
        using BlockBasedTableIterator it = reader.NewIterator();
        int n = 0;
        for (it.SeekToFirst(); it.Valid; it.Next()) n++;
        Assert.AreEqual(3, n);
    }

    [TestMethod]
    public void GlobalSeqnoFromLargestSeqno()
    {
        var entries = new List<(byte[], byte[])> { (IKey("a", 0), [1]), (IKey("b", 0), [2]) };
        byte[] data = BuildTable(entries, TableFormatOptions.Default, new TableBuilderOptions { ExternalSstFile = true });
        using BlockBasedTableReader reader = Open(data, new TableReaderOptions { LargestSeqno = 99 });
        Assert.AreEqual(99UL, reader.GlobalSequenceNumber);
        TableGetResult r = reader.Get(LookupKey("b"), null);
        Assert.IsTrue(r.Found);
        Assert.AreEqual(99UL, r.Sequence);
        Assert.IsFalse(reader.Get(LookupKey("b", 98), null).Found);
    }

    [TestMethod]
    public void FilterIsWrittenAndConsulted()
    {
        var policy = new ExactSetFilterPolicy();
        List<(byte[] Key, byte[] Value)> entries = MakeEntries(500, 1);
        byte[] data = BuildTable(entries, new TableFormatOptions { FilterPolicy = policy });
        var file = new MemRandomAccessFile(data);
        using var reader = BlockBasedTableReader.Open(file, new TableReaderOptions { FilterPolicy = policy });
        Assert.IsTrue(reader.HasFilter);
        Assert.AreEqual(500UL, reader.Properties.NumFilterEntries);
        Assert.AreEqual("test.ExactSetFilter", reader.Properties.FilterPolicyName);
        Assert.IsTrue(reader.Properties.FilterSize > 0);
        int readsBefore = file.Reads;
        for (int i = 0; i < 100; i++) Assert.IsFalse(reader.Get(LookupKey("absent" + i), null).Found);
        Assert.AreEqual(readsBefore, file.Reads, "filter should prevent data block reads");
        Assert.IsTrue(reader.Get(LookupKey("key00000003"), null).Found);
        Assert.IsTrue(file.Reads > readsBefore);
    }

    [TestMethod]
    public void CorruptionIsDetected()
    {
        List<(byte[] Key, byte[] Value)> entries = MakeEntries(200, 2);
        foreach (ChecksumType ct in new[] { ChecksumType.Crc32c, ChecksumType.XxHash, ChecksumType.XxHash64, ChecksumType.Xxh3 })
        {
            byte[] data = BuildTable(entries, new TableFormatOptions { Checksum = ct, BlockSize = 256 });
            data[10] ^= 0x40; // inside the first data block
            using BlockBasedTableReader reader = Open(data);
            using BlockBasedTableIterator it = reader.NewIterator();
            var ex = Assert.ThrowsExactly<RocksDbException>(() => it.SeekToFirst());
            Assert.AreEqual(StatusCode.Corruption, ex.Code, ct.ToString());
        }
        // Truncated file.
        byte[] full = BuildTable(entries, TableFormatOptions.Default);
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => Open(full.AsSpan(0, full.Length - 1).ToArray())).Code);
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => Open(new byte[10])).Code);
    }

    [TestMethod]
    public void OutOfOrderKeysRejected()
    {
        var file = new MemWritableFile();
        using var b = new BlockBasedTableBuilder(file, TableFormatOptions.Default, new TableBuilderOptions());
        b.Add(IKey("b", 1), []);
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => b.Add(IKey("a", 1), [])).Code);
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => b.Add(IKey("b", 1), [])).Code);
    }

    [TestMethod]
    public async Task AsyncBuildOpenGetAndScan()
    {
        List<(byte[] Key, byte[] Value)> entries = MakeEntries(2000, 9, 2000);
        var wf = new MemWritableFile();
        using (var b = new BlockBasedTableBuilder(wf, new TableFormatOptions { BlockSize = 1024 }, new TableBuilderOptions()))
        {
            foreach ((byte[] k, byte[] v) in entries) await b.AddAsync(k, v);
            await b.FinishAsync();
        }
        byte[] data = wf.ToArray();
        // Same layout as the sync path (contents differ only by the random context checksum).
        Assert.AreEqual(BuildTable(entries, new TableFormatOptions { BlockSize = 1024 }).Length, data.Length);
        using BlockBasedTableReader reader = await BlockBasedTableReader.OpenAsync(new MemRandomAccessFile(data), TableReaderOptions.Default);
        using var value = new PooledBufferWriter();
        for (int i = 0; i < entries.Count; i += 37)
        {
            value.Clear();
            TableGetResult r = await reader.GetAsync(LookupKey(UserKeyOf(entries[i].Key)), value);
            Assert.IsTrue(r.Found);
            CollectionAssert.AreEqual(entries[i].Value, value.WrittenSpan.ToArray());
        }
        Assert.IsFalse((await reader.GetAsync(LookupKey("nope"), null)).Found);
        using BlockBasedTableIterator it = reader.NewIterator();
        int n = 0;
        await it.SeekToFirstAsync();
        while (it.Valid)
        {
            CollectionAssert.AreEqual(entries[n++].Key, it.Key.ToArray());
            await it.NextAsync();
        }
        Assert.AreEqual(entries.Count, n);
        await it.SeekAsync(entries[1500].Key);
        CollectionAssert.AreEqual(entries[1500].Key, it.Key.ToArray());
    }

    /// <summary>Counts synchronous and asynchronous reads separately (async reads yield first).</summary>
    private sealed class SyncAsyncCountingFile(byte[] data) : RocksDb.Managed.Env.IRandomAccessFile
    {
        public int SyncReads, AsyncReads;
        public long Length => data.Length;
        public int Read(long offset, Span<byte> destination)
        {
            SyncReads++;
            return Copy(offset, destination);
        }
        public async ValueTask<int> ReadAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            AsyncReads++;
            return Copy(offset, destination.Span);
        }
        private int Copy(long offset, Span<byte> destination)
        {
            if (offset >= data.Length) return 0;
            int n = (int)Math.Min(destination.Length, data.Length - offset);
            data.AsSpan((int)offset, n).CopyTo(destination);
            return n;
        }
        public void Dispose() { }
    }

    /// <summary>
    /// Partitioned index: the async iterator moves and async point lookups read index partitions (not cached: no block
    /// cache) with async I/O like the data blocks: no synchronous read after the table is open.
    /// </summary>
    [TestMethod]
    public async Task AsyncPathsReadIndexPartitionsAsynchronously()
    {
        List<(byte[] Key, byte[] Value)> entries = MakeEntries(3000, 11, 200);
        byte[] data = BuildTable(entries, new TableFormatOptions { BlockSize = 512, IndexType = IndexType.TwoLevelIndexSearch, MetadataBlockSize = 256 });
        var file = new SyncAsyncCountingFile(data);
        using BlockBasedTableReader reader = BlockBasedTableReader.Open(file, TableReaderOptions.Default);
        Assert.IsTrue(reader.IsPartitionedIndex);
        Assert.IsTrue(reader.Properties.IndexPartitions > 4);
        int syncAtOpen = file.SyncReads;

        using (BlockBasedTableIterator it = reader.NewIterator())
        {
            int n = 0;
            await it.SeekToFirstAsync();
            while (it.Valid)
            {
                CollectionAssert.AreEqual(entries[n++].Key, it.Key.ToArray());
                await it.NextAsync();
            }
            Assert.AreEqual(entries.Count, n);
            await it.SeekToLastAsync();
            while (it.Valid)
            {
                n--;
                await it.PrevAsync();
            }
            Assert.AreEqual(0, n);
            await it.SeekAsync(entries[2222].Key);
            CollectionAssert.AreEqual(entries[2222].Key, it.Key.ToArray());
            await it.SeekForPrevAsync(entries[1111].Key);
            CollectionAssert.AreEqual(entries[1111].Key, it.Key.ToArray());
        }
        using var value = new PooledBufferWriter();
        for (int i = 0; i < entries.Count; i += 97)
        {
            value.Clear();
            Assert.IsTrue((await reader.GetAsync(LookupKey(UserKeyOf(entries[i].Key)), value)).Found);
            CollectionAssert.AreEqual(entries[i].Value, value.WrittenSpan.ToArray());
        }
        Assert.AreEqual(syncAtOpen, file.SyncReads, "no synchronous reads in the async paths");
        Assert.IsTrue((ulong)file.AsyncReads > reader.Properties.IndexPartitions);
    }

    [TestMethod]
    public void BlockCacheSeamIsUsed()
    {
        List<(byte[] Key, byte[] Value)> entries = MakeEntries(1000, 4);
        byte[] data = BuildTable(entries, new TableFormatOptions { BlockSize = 512 });
        var cache = new DictionaryBlockCache();
        var file = new MemRandomAccessFile(data);
        using var reader = BlockBasedTableReader.Open(file, new TableReaderOptions { BlockCache = cache });
        for (int i = 0; i < 1000; i += 10) Assert.IsTrue(reader.Get(LookupKey(UserKeyOf(entries[i].Key)), null).Found);
        int reads = file.Reads;
        Assert.IsTrue(cache.Count > 0);
        for (int i = 0; i < 1000; i += 10) Assert.IsTrue(reader.Get(LookupKey(UserKeyOf(entries[i].Key)), null).Found);
        Assert.AreEqual(reads, file.Reads, "second pass must be served from the cache");
        cache.Clear();
    }
}

/// <summary>Trivial thread-safe block cache for tests and benchmarks.</summary>
internal sealed class DictionaryBlockCache : IBlockCache
{
    private readonly Dictionary<BlockCacheKey, Block> _map = new();
    private readonly Lock _lock = new();
    public int Count { get { lock (_lock) return _map.Count; } }

    public bool TryGet(in BlockCacheKey key, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out Block? block)
    {
        lock (_lock)
        {
            if (_map.TryGetValue(key, out block))
            {
                block.AddRef();
                return true;
            }
        }
        return false;
    }

    public void Insert(in BlockCacheKey key, Block block)
    {
        lock (_lock)
        {
            if (_map.TryAdd(key, block)) block.AddRef();
        }
    }

    public void Clear()
    {
        lock (_lock)
        {
            foreach (Block b in _map.Values) b.Release();
            _map.Clear();
        }
    }
}
