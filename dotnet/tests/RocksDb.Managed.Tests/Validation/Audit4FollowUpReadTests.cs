using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Db;
using RocksDb.Managed.Db.Iterators;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;
using RocksDb.Managed.Table.Filters;
using RocksDb.Managed.Tests.Db;
using RocksDb.Managed.Tests.Iterators;
using RocksDb.Managed.Tests.Table;
using RocksDb.Managed.Util;
using static RocksDb.Managed.Tests.Db.DbTestUtil;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4 follow-up (read path): table reader, level iterator, GetContext, memtable lookups and the suspicions the
/// first pass left unproven. Each test names the C++ function it pins down.
/// </summary>
[TestClass]
public sealed class Audit4FollowUpReadTests
{
    private const string DbPath = "/audit4-fur";

    [TestCleanup]
    public void Cleanup() => TestHooks.ClearAll();

    private static byte[] Ts(ulong t)
    {
        byte[] b = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(b, t);
        return b;
    }

    private static byte[] WithTs(string key, ulong ts) => [.. B(key), .. Ts(ts)];

    // ------------------------------------------------------------------------------------------ table reader

    /// <summary>
    /// FullFilterBlockReader::Create (table/block_based/full_filter_block.cc:114-121) and FullFilterBlockReader::MayMatch
    /// (:145-150): a full filter block that cannot be read is ignored (IGNORE_STATUS_IF_ERROR): the table opens and its
    /// keys are found without the filter. The port failed the table open (pinned filter) or the lookup (cached filter)
    /// with Corruption.
    /// </summary>
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void UnreadableFullFilterBlockIsIgnored(bool cacheIndexAndFilterBlocks)
    {
        var policy = new BloomFilterPolicy(10);
        var entries = new List<(byte[] Key, byte[] Value)>();
        for (int i = 0; i < 200; i++) entries.Add((TableTestUtil.IKey("key" + i.ToString("D4"), 7), B("v" + i)));
        byte[] data = TableTestUtil.BuildTable(entries, new TableFormatOptions { FilterPolicy = policy });
        BlockHandle filter;
        using (BlockBasedTableReader intact = TableTestUtil.Open(data, new TableReaderOptions { FilterPolicy = policy }))
        {
            Assert.IsTrue(intact.HasFilter);
            filter = intact.MetaBlocks[BlockBasedTableConstants.FullFilterBlockPrefix + policy.CompatibilityName];
            Assert.IsFalse(intact.KeyMayMatch(B("absent-key")), "the intact filter excludes an absent key");
        }
        // Damage the filter payload: its block checksum no longer matches.
        data[(int)filter.Offset + 3] ^= 0x40;

        var options = new TableReaderOptions { FilterPolicy = policy };
        if (cacheIndexAndFilterBlocks)
        {
            options = options with { BlockCache = RocksDb.Managed.Cache.BlockCache.CreateLru(1 << 20), CacheIndexAndFilterBlocks = true };
        }
        using BlockBasedTableReader reader = TableTestUtil.Open(data, options);
        var value = new ArrayBufferWriter<byte>();
        TableGetResult r = reader.Get(TableTestUtil.LookupKey("key0042"), value);
        Assert.IsTrue(r.Found);
        Assert.AreEqual("v42", Encoding.UTF8.GetString(value.WrittenSpan));
        Assert.IsFalse(reader.Get(TableTestUtil.LookupKey("absent-key"), null).Found);
        Assert.IsTrue(reader.KeyMayMatch(B("absent-key")), "without a readable filter every key may match");
    }

    // ------------------------------------------------------------------------------------------ level iterator

    /// <summary>
    /// LevelIterator::Seek (db/version_set.cc:1617-1626): the prefix check after the file initially positioned to has
    /// no key for the target uses ExtractUserKeyAndStripTimestamp. With user-defined timestamps and a capped prefix
    /// extractor, the port transformed the keys with their timestamps, so a key shorter than the cap got a "prefix"
    /// that included timestamp bytes: the seek target (maximum timestamp) and the first key of the next file (its
    /// write timestamp) looked like different prefixes and the iterator was invalidated although the key exists.
    /// </summary>
    [TestMethod]
    public void LevelIteratorPrefixCheckIgnoresTheUserDefinedTimestamp()
    {
        Comparator ucmp = Comparator.BytewiseWithU64Ts;
        var icmp = new InternalKeyComparator(ucmp);
        SliceTransform capped = SliceTransform.CreateCappedPrefix(4);
        var format = new TableFormatOptions();
        var tbo = new TableBuilderOptions { Comparator = ucmp };
        byte[] first = TableTestUtil.BuildTable([(TableTestUtil.IKey(WithTs("aa", 5), 1), B("1"))], format, tbo);
        byte[] second = TableTestUtil.BuildTable([(TableTestUtil.IKey(WithTs("ab", 5), 2), B("2"))], format, tbo);
        using var cache = new TestTableCache(new TableReaderOptions { Comparator = ucmp, PrefixExtractor = capped });
        cache.AddFile(1, first);
        cache.AddFile(2, second);
        var files = new List<FileMetaData>
        {
            // The first file's boundary was extended by a range tombstone ending at "ab" (sentinel largest key), so a seek
            // to "ab" lands in it and finds no point key.
            new()
            {
                FileNumber = 1, FileSize = (ulong)first.Length, Smallest = TableTestUtil.IKey(WithTs("aa", 5), 1),
                Largest = TableTestUtil.IKey(WithTs("ab", ulong.MaxValue), DbFormat.MaxSequenceNumber, EntryType.RangeDeletion),
                SmallestSeqno = 1, LargestSeqno = 1,
            },
            new()
            {
                FileNumber = 2, FileSize = (ulong)second.Length, Smallest = TableTestUtil.IKey(WithTs("ab", 5), 2),
                Largest = TableTestUtil.IKey(WithTs("ab", 5), 2), SmallestSeqno = 2, LargestSeqno = 2,
            },
        };
        using var it = new LevelIterator(cache, icmp, files, null, capped);
        byte[] target = TableTestUtil.IKey(WithTs("ab", ulong.MaxValue), DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
        it.Seek(target);
        Assert.IsTrue(it.Valid, "the key with the target's prefix in the next file must be found");
        Assert.IsTrue(DbFormat.ExtractUserKey(it.Key).SequenceEqual(WithTs("ab", 5)));
        Assert.AreEqual(1, it.CurrentFileIndex);

        it.SeekAsync(target).AsTask().GetAwaiter().GetResult();
        Assert.IsTrue(it.Valid, "async seek");
        Assert.IsTrue(DbFormat.ExtractUserKey(it.Key).SequenceEqual(WithTs("ab", 5)));
    }

    // ------------------------------------------------------------------------------------------ GetContext / memtable

    /// <summary>
    /// S8. GetContext::SaveValue (table/get_context.cc:423-427) and MemTable's SaveValue (db/memtable.cc:1417-1427): the
    /// timestamp reported for a key is the one of the first entry processed, except that a timestamp equal to kMaxTs
    /// ("timestamp->empty() || CompareTimestamp(*timestamp, kMaxTs) == 0") is replaced by the next entry's. A merge
    /// operand written at the maximum timestamp over a base value at timestamp 5 therefore reads with timestamp 5.
    /// </summary>
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void MaxTimestampOfTheNewestEntryIsReplacedByTheNextEntrys(bool flush)
    {
        MemoryFileSystem fs = NewFs();
        var cfo = new ColumnFamilyOptions
        {
            Comparator = Comparator.BytewiseWithU64Ts, DisableAutoCompactions = true, MergeOperator = new StringAppendMergeOperator(),
        };
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        db.Put(B("k"), Ts(5), B("a"));
        if (flush) db.Flush();
        db.Merge(B("k"), Ts(ulong.MaxValue), B("b"));
        if (flush) db.Flush();
        // Control: an ordinary newest timestamp is the one reported.
        db.Put(B("c"), Ts(5), B("a"));
        if (flush) db.Flush();
        db.Merge(B("c"), Ts(9), B("b"));
        if (flush) db.Flush();

        var read = new ReadOptions { Timestamp = Ts(ulong.MaxValue) };
        var v = new ArrayBufferWriter<byte>();
        var t = new ArrayBufferWriter<byte>();
        Assert.IsTrue(db.TryGet(read, B("k"), v, t));
        Assert.AreEqual("a,b", Encoding.UTF8.GetString(v.WrittenSpan));
        Assert.AreEqual(5UL, BinaryPrimitives.ReadUInt64LittleEndian(t.WrittenSpan));

        v.Clear();
        t.Clear();
        Assert.IsTrue(db.TryGet(read, B("c"), v, t));
        Assert.AreEqual("a,b", Encoding.UTF8.GetString(v.WrittenSpan));
        Assert.AreEqual(9UL, BinaryPrimitives.ReadUInt64LittleEndian(t.WrittenSpan));
    }

    /// <summary>
    /// S49. SaveValue (db/memtable.cc:1375-1399) verifies the entry's per key-value checksum BEFORE CheckCallback: a
    /// corrupted entry of the key fails the lookup even when the read does not see it. The port verified after the
    /// visibility checks, so a corrupted entry newer than the read snapshot was silently skipped.
    /// </summary>
    [TestMethod]
    [DoNotParallelize]
    public void MemtableEntryChecksumIsVerifiedBeforeTheVisibilityCheck()
    {
        MemoryFileSystem fs = NewFs();
        var cfo = new ColumnFamilyOptions { Comparator = Comparator.BytewiseWithU64Ts, MemtableProtectionBytesPerKey = 8 };
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        db.Put(B("k"), Ts(5), B("old"));
        using Snapshot snapshot = db.GetSnapshot();
        db.Put(B("k"), Ts(9), B("NEWVALUE"));

        var read = new ReadOptions { Snapshot = snapshot, Timestamp = Ts(ulong.MaxValue) };
        var v = new ArrayBufferWriter<byte>();
        var t = new ArrayBufferWriter<byte>();
        Assert.IsTrue(db.TryGet(read, B("k"), v, t));
        Assert.AreEqual("old", Encoding.UTF8.GetString(v.WrittenSpan));

        // Corrupt the entry written after the snapshot (once: the hook runs before every verification).
        bool corrupted = false;
        TestHooks.SetSpan("Memtable::SaveValue:Found:entry", data =>
        {
            int i = data.IndexOf("NEWVALUE"u8);
            if (i < 0 || corrupted) return;
            data[i] ^= 0x01;
            corrupted = true;
        });
        var ex = Assert.ThrowsExactly<RocksDbException>(() => db.TryGet(read, B("k"), new ArrayBufferWriter<byte>(), new ArrayBufferWriter<byte>()));
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
        Assert.IsTrue(corrupted);
    }

    // ------------------------------------------------------------------------------------------ KeyMayExist

    /// <summary>
    /// S9. DBImpl::KeyMayExist (db/db_impl/db_impl.cc:5404-5432) returns s.ok() || s.IsIncomplete() of a GetImpl: the
    /// InvalidArgument of its timestamp checks (FailIfCfHasTs / FailIfTsMismatchCf) answers false, also for a key that
    /// exists.
    /// </summary>
    [TestMethod]
    public void KeyMayExistIsFalseWhenTheReadTimestampDoesNotFitTheColumnFamily()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { Comparator = Comparator.BytewiseWithU64Ts }))
        {
            db.Put(B("k"), Ts(5), B("v"));
            Assert.IsFalse(db.KeyMayExist(B("k")), "no read timestamp on a column family with timestamps");
            Assert.IsFalse(db.KeyMayExist(new ReadOptions { Timestamp = new byte[4] }, B("k"), null, out _), "timestamp of the wrong size");
            Assert.IsTrue(db.KeyMayExist(new ReadOptions { Timestamp = Ts(9) }, B("k"), null, out _));
        }
        fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            db.Put(B("k"), B("v"));
            Assert.IsTrue(db.KeyMayExist(B("k")));
            Assert.IsFalse(db.KeyMayExist(new ReadOptions { Timestamp = Ts(9) }, B("k"), null, out _), "read timestamp without timestamps");
        }
    }

    // ------------------------------------------------------------------------------------------ iterator statistics

    /// <summary>
    /// S6. DBIter::Seek / SeekToFirst / Next / Prev (db/db_iter.cc) record NUMBER_DB_SEEK{,_FOUND}, NUMBER_DB_NEXT{,_FOUND},
    /// NUMBER_DB_PREV{,_FOUND} and ITER_BYTES_READ for every move; the port's async moves recorded none of them.
    /// </summary>
    [TestMethod]
    public async Task AsyncIteratorMovesRecordTheSameTickersAsSyncMoves()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions options = NewOptions(fs);
        var stats = new Statistics();
        options.Statistics = stats;
        using RocksDatabase db = RocksDatabase.Open(DbPath, options);
        db.Put(B("a"), B("1"));
        db.Put(B("b"), B("22"));
        db.Put(B("c"), B("333"));
        using Iterator it = db.NewIterator();
        await it.SeekToFirstAsync();
        await it.NextAsync();
        await it.NextAsync();
        await it.NextAsync(); // past the end: counted, not found
        await it.SeekAsync(B("b"));
        await it.PrevAsync();
        await it.SeekForPrevAsync(B("zz"));
        await it.SeekToLastAsync();
        Assert.AreEqual(4UL, stats.GetTickerCount(Ticker.NumberDbSeek));
        Assert.AreEqual(4UL, stats.GetTickerCount(Ticker.NumberDbSeekFound));
        Assert.AreEqual(3UL, stats.GetTickerCount(Ticker.NumberDbNext));
        Assert.AreEqual(2UL, stats.GetTickerCount(Ticker.NumberDbNextFound));
        Assert.AreEqual(1UL, stats.GetTickerCount(Ticker.NumberDbPrev));
        Assert.AreEqual(1UL, stats.GetTickerCount(Ticker.NumberDbPrevFound));
        // a(2) + b(3) + c(4) for SeekToFirst / Next / Next, b(3) for Seek, a(2) for Prev, c(4) twice.
        Assert.AreEqual(2UL + 3 + 4 + 3 + 2 + 4 + 4, stats.GetTickerCount(Ticker.IterBytesRead));
    }

    // ------------------------------------------------------------------------------------------ SstFileWriter

    /// <summary>
    /// S23. SstFileWriter::Finish (table/sst_file_writer.cc): "if (!s.ok()) { env->DeleteFile(file_path) ... }" when the
    /// builder's Finish, the sync or the close fails. The port left the partial file behind.
    /// </summary>
    [TestMethod]
    public void SstFileWriterDeletesTheFileWhenFinishFails()
    {
        var fs = new FaultInjectionFileSystem(NewFs());
        const string path = "/ext.sst";
        using var w = new SstFileWriter(new ColumnFamilyOptions(), fs);
        w.Open(path);
        w.Put(B("a"), B("1"));
        Assert.IsTrue(fs.FileExists(path));
        fs.FailMatching("ext.sst", FaultOps.Sync, 1, InjectedFaultKind.IOError);
        var ex = Assert.ThrowsExactly<RocksDbException>(() => w.Finish());
        Assert.AreEqual(StatusCode.IOError, ex.Code);
        Assert.IsFalse(fs.FileExists(path), "the file of a failed Finish is deleted");
        // The writer is closed: "File is not opened".
        var again = Assert.ThrowsExactly<RocksDbException>(() => w.Finish());
        Assert.AreEqual(StatusCode.InvalidArgument, again.Code);
    }

    // ------------------------------------------------------------------------------------------ options / filters

    /// <summary>
    /// S24. BlockBasedTableFactory::InitializeOptions (table/block_based/block_based_table_factory.cc:668-678) raises a
    /// format_version below kMinSupportedBbtFormatVersionForWrite (2) in the factory's options, so the OPTIONS file
    /// records the sanitized value, for the column families given to DB::Open and for those created later.
    /// </summary>
    [TestMethod]
    public void OptionsFileRecordsTheSanitizedFormatVersion()
    {
        MemoryFileSystem fs = NewFs();
        var cfo = new ColumnFamilyOptions();
        cfo.TableOptions.FormatVersion = 1;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo))
        {
            var later = new ColumnFamilyOptions();
            later.TableOptions.FormatVersion = 0;
            db.CreateColumnFamily("later", later);
            db.Put(B("a"), B("1"));
            db.Flush();
            foreach (TableProperties p in db.GetPropertiesOfAllTables().Values) Assert.AreEqual(2UL, p.FormatVersion);
        }
        string? newest = null;
        foreach (string child in fs.GetChildren(DbPath))
        {
            if (child.StartsWith("OPTIONS-", StringComparison.Ordinal) && (newest is null || string.CompareOrdinal(child, newest) > 0)) newest = child;
        }
        Assert.IsNotNull(newest);
        string text = Encoding.UTF8.GetString(fs.ReadAllBytes(DbPath + "/" + newest));
        int count = 0;
        foreach (string line in text.Split('\n'))
        {
            string trimmed = line.Trim();
            if (!trimmed.StartsWith("format_version=", StringComparison.Ordinal)) continue;
            Assert.AreEqual("format_version=2", trimmed);
            count++;
        }
        Assert.AreEqual(2, count, "one table options section per column family");
    }

    /// <summary>
    /// S32. RibbonFilterPolicy::GetBuilderWithContext (table/block_based/filter_policy.cc): with level or universal
    /// compaction a flush is "level -1" and gets a Bloom filter when bloom_before_level is 0; with FIFO compaction the
    /// file counts as bottommost (levelish = INT_MAX - 1) and gets a Ribbon filter. Checks the wiring of the column
    /// family's compaction style through the flush job into the filter builder (the last 5 bytes of a built-in filter
    /// start with -1 for FastLocalBloom and -2 for Ribbon).
    /// </summary>
    [TestMethod]
    [DataRow(CompactionStyle.Level, (byte)0xFF)]
    [DataRow(CompactionStyle.Universal, (byte)0xFF)]
    [DataRow(CompactionStyle.Fifo, (byte)0xFE)]
    public void FlushChoosesBloomOrRibbonByCompactionStyle(CompactionStyle style, byte expectedMarker)
    {
        MemoryFileSystem fs = NewFs();
        var policy = new RibbonFilterPolicy(10, 0);
        var cfo = new ColumnFamilyOptions { CompactionStyle = style, DisableAutoCompactions = true };
        cfo.TableOptions.FilterPolicy = policy;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo))
        {
            // Enough keys for a Ribbon filter: below 1024 slots the Ribbon builder itself falls back to Bloom.
            for (int i = 0; i < 5000; i++) db.Put(Key(i), Value(i, 0));
            db.Flush();
        }
        int tables = 0;
        foreach (string child in fs.GetChildren(DbPath))
        {
            if (!child.EndsWith(".sst", StringComparison.Ordinal)) continue;
            tables++;
            byte[] data = fs.ReadAllBytes(DbPath + "/" + child);
            using BlockBasedTableReader reader = TableTestUtil.Open(data, new TableReaderOptions { FilterPolicy = policy });
            BlockHandle h = reader.MetaBlocks[BlockBasedTableConstants.FullFilterBlockPrefix + policy.CompatibilityName];
            Assert.IsTrue(h.Size > 5);
            Assert.AreEqual(expectedMarker, data[(int)(h.Offset + h.Size) - 5], style.ToString());
            Assert.IsTrue(reader.KeyMayMatch(Key(42)));
        }
        Assert.AreEqual(1, tables);
    }

    // ------------------------------------------------------------------------------------------ compression

    /// <summary>
    /// S51. LZ4_decompress_safe (used by the LZ4 decompressor of util/compression.cc) fails for an empty input and, for
    /// an empty output, accepts only the one-byte stream { 0 }: "Error decompressing LZ4 data". The port accepted a
    /// block that declares size 0 and has no payload.
    /// </summary>
    [TestMethod]
    [DataRow(CompressionType.LZ4)]
    [DataRow(CompressionType.LZ4HC)]
    public void Lz4BlockWithoutPayloadIsCorruption(CompressionType type)
    {
        foreach (byte[] bad in (byte[][])[[0x00], [0x00, 0x01], [0x00, 0x00, 0x00], [0x03]])
        {
            using var d = new PooledBufferWriter();
            var ex = Assert.ThrowsExactly<RocksDbException>(() => BlockCompression.Decompress(type, bad, 5, d), Convert.ToHexString(bad));
            Assert.AreEqual(StatusCode.Corruption, ex.Code);
        }
        // The valid encoding of an empty block: declared size 0 and the single token byte 0.
        using var ok = new PooledBufferWriter();
        BlockCompression.Decompress(type, [0x00, 0x00], 5, ok);
        Assert.AreEqual(0, ok.WrittenCount);
        // What the port's own compressor writes for an empty input decompresses.
        using var c = new PooledBufferWriter();
        if (BlockCompression.TryCompress(type, [], 5, BlockCompression.DefaultCompressionLevel, c))
        {
            using var back = new PooledBufferWriter();
            BlockCompression.Decompress(type, c.WrittenSpan, 5, back);
            Assert.AreEqual(0, back.WrittenCount);
        }
    }

    /// <summary>
    /// OPEN-3. Zlib_DecompressBlock (util/compression.cc:1340-1345): "if (st != Z_STREAM_END) return Corruption": a
    /// deflate stream that does not end within the block is corruption even when it inflates to every expected byte.
    /// Every prefix of a valid stream must be rejected, the complete stream accepted, also for inputs above the 16 KiB
    /// chunking of the port's own compressor and for an empty block.
    /// </summary>
    [TestMethod]
    [DataRow(0)]
    [DataRow(1)]
    [DataRow(3000)]
    [DataRow(40000)]
    public void ZlibBlockMustContainTheEndOfItsDeflateStream(int length)
    {
        byte[] raw = new byte[length];
        for (int i = 0; i < raw.Length; i++) raw[i] = (byte)("abcabcabd"[i % 9] + (i / 500));
        using var c = new PooledBufferWriter();
        if (!BlockCompression.TryCompress(CompressionType.Zlib, raw, 5, BlockCompression.DefaultCompressionLevel, c))
        {
            Assert.IsTrue(length <= 1, "only tiny inputs may be rejected by the compressor");
            return;
        }
        byte[] good = c.WrittenSpan.ToArray();
        using (var d = new PooledBufferWriter())
        {
            BlockCompression.Decompress(CompressionType.Zlib, good, 5, d);
            Assert.IsTrue(d.WrittenSpan.SequenceEqual(raw));
        }
        int header = length < 128 ? 1 : length < 16384 ? 2 : 3; // varint32(length)
        for (int cut = 1; cut <= Math.Min(8, good.Length - header); cut++)
        {
            using var d = new PooledBufferWriter();
            var ex = Assert.ThrowsExactly<RocksDbException>(() => BlockCompression.Decompress(CompressionType.Zlib, good.AsSpan(0, good.Length - cut), 5, d),
                "cut by " + cut);
            Assert.AreEqual(StatusCode.Corruption, ex.Code);
        }
        // Trailing bytes after the end of the stream are ignored by inflate (st == Z_STREAM_END with avail_in > 0).
        using (var d = new PooledBufferWriter())
        {
            BlockCompression.Decompress(CompressionType.Zlib, [.. good, 0x00, 0xFF], 5, d);
            Assert.IsTrue(d.WrittenSpan.SequenceEqual(raw));
        }
    }
}
