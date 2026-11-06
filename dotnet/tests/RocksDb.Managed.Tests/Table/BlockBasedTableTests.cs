using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Table;
using RocksDb.Managed.Table.Filters;
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

    /// <summary>
    /// The index block and full filter a reader pins come from the pinned table buffer pool (separate from the pool table
    /// builders rent from, so live readers never take the builders' arrays) and go back there when the reader is closed,
    /// also after iterators over the reader were used; with
    /// cache_index_and_filter_blocks they stay in the block buffer pool / ArrayPool.Shared the cache returns them to.
    /// </summary>
    [TestMethod]
    public void PinnedIndexAndFilterUseTableBufferPool()
    {
        List<(byte[] Key, byte[] Value)> entries = MakeEntries(20_000, 7);
        var policy = new BloomFilterPolicy(10);
        byte[] data = BuildTable(entries, new TableFormatOptions { FilterPolicy = policy });
        byte[] index, filter;
        using (BlockBasedTableReader reader = BlockBasedTableReader.Open(new MemRandomAccessFile(data), new TableReaderOptions { FilterPolicy = policy }))
        {
            index = reader.IndexBlock.Buffer;
            filter = reader.PinnedFilterBuffer!;
            Assert.IsNotNull(filter);
            // Table buffer pool arrays are power-of-two sized (block buffer pool classes above 4 KiB are not).
            Assert.IsTrue(index.Length > 4096 && System.Numerics.BitOperations.IsPow2(index.Length), "index buffer " + index.Length);
            Assert.IsTrue(System.Numerics.BitOperations.IsPow2(filter.Length), "filter buffer " + filter.Length);
            using (BlockBasedTableIterator it = reader.NewIterator())
            {
                int n = 0;
                for (it.SeekToFirst(); it.Valid; it.Next()) n++;
                Assert.AreEqual(entries.Count, n);
            }
            Assert.IsTrue(reader.Get(entries[123].Key, null).Found);
        }
        // Closed: both buffers are back in the table buffer pool (a LIFO per size class), not dropped or returned to
        // another pool. Rent both before returning either, so they are told apart when they share a size class.
        byte[] a = TableBuilderBuffers.Pinned.Rent(filter.Length);
        byte[] b = TableBuilderBuffers.Pinned.Rent(index.Length);
        try
        {
            Assert.IsTrue(ReferenceEquals(a, filter) || ReferenceEquals(a, index), "filter buffer not returned to the pinned table buffer pool");
            Assert.IsTrue(ReferenceEquals(b, index) || ReferenceEquals(b, filter), "index buffer not returned to the pinned table buffer pool");
        }
        finally
        {
            TableBuilderBuffers.Pinned.Return(b);
            TableBuilderBuffers.Pinned.Return(a);
        }

        // cache_index_and_filter_blocks: the cache owns them, charged at the block buffer pool's finer size classes.
        var cache = RocksDb.Managed.Cache.BlockCache.CreateLru(new RocksDb.Managed.Cache.LruCacheOptions { Capacity = 8 << 20 });
        using (BlockBasedTableReader reader = BlockBasedTableReader.Open(new MemRandomAccessFile(data),
            new TableReaderOptions { FilterPolicy = policy, BlockCache = cache, CacheIndexAndFilterBlocks = true }))
        {
            Assert.IsNull(reader.PinnedFilterBuffer);
            Assert.IsTrue(reader.Get(entries[5].Key, null).Found);
        }
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

    /// <summary>
    /// BlockPrefetcher implicit auto readahead: after num_file_reads_for_auto_readahead sequential data block reads the
    /// iterator reads through a readahead buffer starting at initial_auto_readahead_size and doubling up to
    /// max_auto_readahead_size (far fewer file reads); a non-sequential block resets it; max_auto_readahead_size = 0
    /// disables it; ReadOptions::readahead_size (SetUserReadahead) uses a fixed size.
    /// </summary>
    [TestMethod]
    public void IteratorAutoReadahead()
    {
        List<(byte[] Key, byte[] Value)> entries = MakeEntries(4000, 12, 200);
        byte[] data = BuildTable(entries, new TableFormatOptions { BlockSize = 1024 });
        int Scan(TableReaderOptions options, out BlockBasedTableIterator? kept, bool keep, ulong userReadahead)
        {
            var file = new MemRandomAccessFile(data);
            BlockBasedTableReader reader = BlockBasedTableReader.Open(file, options);
            int before = file.Reads;
            BlockBasedTableIterator it = reader.NewIterator();
            it.SetUserReadahead(userReadahead);
            int n = 0;
            for (it.SeekToFirst(); it.Valid; it.Next()) CollectionAssert.AreEqual(entries[n++].Key, it.Key.ToArray());
            Assert.AreEqual(entries.Count, n);
            int reads = file.Reads - before;
            kept = keep ? it : null;
            if (!keep)
            {
                it.Dispose();
                reader.Dispose();
            }
            return reads;
        }
        int none = Scan(new TableReaderOptions { MaxAutoReadaheadSize = 0 }, out _, keep: false, 0);
        Assert.IsTrue(none > 100, "one read per data block without readahead: " + none);
        int auto = Scan(TableReaderOptions.Default, out BlockBasedTableIterator? autoIt, keep: true, 0);
        Assert.IsTrue(auto * 5 < none, "auto readahead: " + auto + " reads vs " + none);
        Assert.IsNotNull(autoIt!.Readahead);
        Assert.AreEqual(256 * 1024, autoIt.Readahead.ReadaheadSize, "grown to max_auto_readahead_size");
        // A random seek resets the readahead to its initial size.
        autoIt.Seek(entries[100].Key);
        autoIt.Seek(entries[3900].Key);
        autoIt.Seek(entries[2000].Key);
        Assert.AreEqual(8 * 1024, autoIt.Readahead.ReadaheadSize);
        autoIt.Dispose();
        int fixedReads = Scan(new TableReaderOptions { MaxAutoReadaheadSize = 0 }, out BlockBasedTableIterator? fixedIt, keep: true, 64 * 1024);
        Assert.IsTrue(fixedReads * 5 < none, "readahead_size: " + fixedReads);
        Assert.AreEqual(64 * 1024, fixedIt!.Readahead!.ReadaheadSize);
        fixedIt.Dispose();
    }

    /// <summary>
    /// BlockPrefetcher implicit auto readahead on a file that supports Prefetch (readahead(2)): no readahead buffer; after
    /// num_file_reads_for_auto_readahead sequential blocks the iterator asks the OS for the block plus readahead_size_
    /// bytes, again only for a block beyond the prefetched range (readahead_limit_), doubling the size up to
    /// max_auto_readahead_size; data blocks are still read one by one. A seek to a non-sequential block resets the size.
    /// </summary>
    [TestMethod]
    public void IteratorAutoReadaheadUsesFilePrefetch()
    {
        List<(byte[] Key, byte[] Value)> entries = MakeEntries(4000, 12, 200);
        byte[] data = BuildTable(entries, new TableFormatOptions { BlockSize = 1024 });
        var file = new MemRandomAccessFile(data) { SupportsPrefetch = true };
        using BlockBasedTableReader reader = BlockBasedTableReader.Open(file, TableReaderOptions.Default);
        int before = file.Reads;
        using BlockBasedTableIterator it = reader.NewIterator();
        int n = 0;
        for (it.SeekToFirst(); it.Valid; it.Next()) CollectionAssert.AreEqual(entries[n++].Key, it.Key.ToArray());
        Assert.AreEqual(entries.Count, n);
        Assert.IsNull(it.Readahead, "no prefetch buffer when the file supports Prefetch");
        int blockReads = file.Reads - before;
        Assert.IsTrue(blockReads > 100, "data blocks are read individually: " + blockReads);
        List<(long Offset, long Length)> calls = file.PrefetchCalls;
        Assert.IsTrue(calls.Count >= 6, "prefetches: " + calls.Count);
        // readahead_size_ = 8, 16, 32, 64, 128, 256, 256 KiB ... on top of the block that triggered the prefetch.
        long expected = 8 * 1024;
        for (int i = 0; i < calls.Count; i++)
        {
            long blockLength = calls[i].Length - expected;
            Assert.IsTrue(blockLength > 0 && blockLength < 2048, "prefetch #" + i + ": " + calls[i].Length + " bytes, readahead " + expected);
            if (i > 0)
            {
                // Issued for the first block that ends past the previous prefetched range.
                long previousEnd = calls[i - 1].Offset + calls[i - 1].Length;
                Assert.IsTrue(calls[i].Offset <= previousEnd && calls[i].Offset + blockLength > previousEnd, "prefetch #" + i);
            }
            expected = Math.Min(256 * 1024, expected * 2);
        }
        // Seek + 10 Next (~4 entries per block): the first two blocks after a non-sequential seek never prefetch, the
        // third asks for initial_auto_readahead_size again (ResetValues), the fourth is inside that range.
        calls.Clear();
        it.Seek(entries[100].Key);
        for (int i = 0; i < 10 && it.Valid; i++) it.Next();
        Assert.IsTrue(calls.Count <= 1, "Seek + 10 Next: " + calls.Count + " prefetches");
        foreach ((long _, long length) in calls) Assert.IsTrue(length > 8 * 1024 && length < 8 * 1024 + 2048, "reset to 8 KiB: " + length);
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
