using System.Buffers;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;
using RocksDb.Managed.Table.Filters;
using RocksDb.Managed.Tests.Db;
using RocksDb.Managed.Tests.Table;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4, slice TableReader: block-based table reader / iterator / builder, SstFileWriter, SstFileReader, table
/// cache. Each test names the C++ function it pins down.
/// </summary>
[TestClass]
public sealed class Audit4TableReaderTests
{
    private sealed record Entry(byte[] Key, byte[] Value);

    private sealed class Batch(List<byte[]> lookupKeys) : ITableMultiGetBatch
    {
        public readonly List<List<Entry>> Seen = lookupKeys.ConvertAll(_ => new List<Entry>());
        public readonly HashSet<int> Incomplete = new();
        public ReadOnlySpan<byte> InternalKey(int key) => lookupKeys[key];
        public ReadOnlyMemory<byte> InternalKeyMemory(int key) => lookupKeys[key];
        public bool Visit(int key, ReadOnlySpan<byte> internalKey, ReadOnlySpan<byte> value)
        {
            Seen[key].Add(new Entry(internalKey.ToArray(), value.ToArray()));
            return true;
        }
        public void SetIncomplete(int key) => Incomplete.Add(key);
    }

    private static readonly TableEntryVisitor<List<Entry>> s_collect =
        static (ref List<Entry> l, ReadOnlySpan<byte> k, ReadOnlySpan<byte> v) =>
        {
            l.Add(new Entry(k.ToArray(), v.ToArray()));
            return true;
        };

    private static byte[] RandomUserKey(Random rnd)
    {
        // A small alphabet and short keys give many shared prefixes, prefix-of-each-other keys and the empty key.
        int len = rnd.Next(0, 7);
        byte[] k = new byte[len];
        for (int i = 0; i < len; i++) k[i] = (byte)("ab\0\xff"[rnd.Next(4)]);
        return k;
    }

    private static List<Entry> MakeModel(Random rnd, InternalKeyComparator icmp, int userKeys)
    {
        var seen = new HashSet<string>();
        var list = new List<Entry>();
        ulong seq = 1;
        for (int i = 0; i < userKeys; i++)
        {
            byte[] uk = RandomUserKey(rnd);
            if (!seen.Add(Convert.ToHexString(uk))) continue;
            int versions = rnd.Next(4) == 0 ? rnd.Next(1, 40) : rnd.Next(1, 4);
            for (int v = 0; v < versions; v++)
            {
                EntryType t = rnd.Next(5) switch { 0 => EntryType.Deletion, 1 => EntryType.Merge, _ => EntryType.Value };
                byte[] value = new byte[t == EntryType.Deletion ? 0 : rnd.Next(0, 80)];
                if (rnd.Next(2) == 0) rnd.NextBytes(value); else value.AsSpan().Fill((byte)'v'); // compressible
                seq += (ulong)rnd.Next(1, 4);
                list.Add(new Entry(TableTestUtil.IKey(uk, seq, t), value));
            }
        }
        list.Sort((a, b) => icmp.Compare(a.Key, b.Key));
        return list;
    }

    private static int LowerBound(List<Entry> model, InternalKeyComparator icmp, byte[] target)
    {
        int lo = 0, hi = model.Count;
        while (lo < hi)
        {
            int mid = (lo + hi) / 2;
            if (icmp.Compare(model[mid].Key, target) < 0) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    private static void AssertAt(List<Entry> model, int pos, BlockBasedTableIterator it, string what)
    {
        if (pos < 0 || pos >= model.Count)
        {
            Assert.IsFalse(it.Valid, what + ": expected invalid");
            return;
        }
        Assert.IsTrue(it.Valid, what + ": expected valid at " + pos);
        Assert.AreEqual(Convert.ToHexString(model[pos].Key), Convert.ToHexString(it.Key), what + ": key");
        Assert.AreEqual(Convert.ToHexString(model[pos].Value), Convert.ToHexString(it.Value), what + ": value");
    }

    /// <summary>
    /// BlockBasedTable::Get / MultiGet (continuing into the following data blocks while the user key matches,
    /// DataBlockIter::SeekForGet with the data block hash index) and BlockBasedTableIterator Seek / SeekForPrev /
    /// Next / Prev / SeekToFirst / SeekToLast (sync and async) against a sorted model, over index types, data block
    /// index types, comparators, restart intervals and format versions.
    /// </summary>
    [TestMethod]
    public async Task ReaderAndIteratorMatchModelAcrossFormats()
    {
        const int seed = 20261110; // fixed: deterministic runs (change to explore)
        for (int round = 0; round < 60; round++)
        {
            int roundSeed = seed + round;
            var rnd = new Random(roundSeed);
            Comparator ucmp = rnd.Next(3) == 0 ? Comparator.ReverseBytewise : Comparator.Bytewise;
            var icmp = new InternalKeyComparator(ucmp);
            IndexType indexType = (IndexType)rnd.Next(4);
            SliceTransform? pe = indexType == IndexType.HashSearch || rnd.Next(3) == 0 ? SliceTransform.CreateCappedPrefix(2) : null;
            FilterPolicy? policy = rnd.Next(2) == 0 ? new BloomFilterPolicy(10) : null;
            int fv = rnd.Next(2, 9);
            var compression = new CompressionOptions
            {
                ParallelThreads = rnd.Next(3) == 0 ? 4u : 1u,
                MaxDictBytes = rnd.Next(3) == 0 ? 512u : 0u,
                MaxDictBufferBytes = rnd.Next(2) == 0 ? 2048UL : 0UL,
            };
            var ctype = (new[] { RocksDb.Managed.Compression.CompressionType.None, RocksDb.Managed.Compression.CompressionType.Snappy, RocksDb.Managed.Compression.CompressionType.LZ4, RocksDb.Managed.Compression.CompressionType.Zstd })[rnd.Next(4)];
            var format = new TableFormatOptions
            {
                BlockSize = rnd.Next(1, 300),
                BlockSizeDeviation = rnd.Next(0, 30),
                BlockRestartInterval = rnd.Next(1, 17),
                IndexBlockRestartInterval = indexType == IndexType.HashSearch ? 1 : rnd.Next(1, 5),
                MetadataBlockSize = rnd.Next(1, 200),
                FormatVersion = fv,
                SeparateKeyValueInDataBlock = fv >= 8 && rnd.Next(2) == 0,
                OptimizeKeyCommonPrefix = (OptimizeKeyCommonPrefix)rnd.Next(3),
                IndexType = indexType,
                IndexShortening = (IndexShorteningMode)rnd.Next(3),
                DataBlockIndexType = (DataBlockIndexType)rnd.Next(2),
                UseDeltaEncoding = rnd.Next(4) != 0,
                FilterPolicy = policy,
                WholeKeyFiltering = pe is null || rnd.Next(2) == 0,
                PartitionFilters = indexType == IndexType.TwoLevelIndexSearch && rnd.Next(2) == 0,
            };
            string trace = $"seed={roundSeed} cmp={ucmp.Name} index={indexType} fv={format.FormatVersion} bs={format.BlockSize} "
                + $"comp={ctype} par={compression.ParallelThreads} dict={compression.MaxDictBytes} sep={format.SeparateKeyValueInDataBlock} "
                + $"hash={format.DataBlockIndexType} wkf={format.WholeKeyFiltering} pf={format.PartitionFilters} pe={pe is not null} filter={policy is not null}";
            List<Entry> model = MakeModel(rnd, icmp, rnd.Next(1, 120));
            byte[] data = TableTestUtil.BuildTable(model.ConvertAll(e => (e.Key, e.Value)), format,
                new TableBuilderOptions { Comparator = ucmp, PrefixExtractor = pe, CompressionType = ctype, CompressionOptions = compression });
            using BlockBasedTableReader reader = TableTestUtil.Open(data,
                new TableReaderOptions { Comparator = ucmp, FilterPolicy = policy, PrefixExtractor = pe });

            // Lookup keys: existing user keys and random ones, at random snapshots.
            var lookups = new List<byte[]>();
            for (int i = 0; i < 80; i++)
            {
                byte[] uk = rnd.Next(3) == 0 ? RandomUserKey(rnd) : DbFormat.ExtractUserKey(model[rnd.Next(model.Count)].Key).ToArray();
                ulong snap = rnd.Next(4) == 0 ? DbFormat.MaxSequenceNumber : (ulong)rnd.Next(0, 400);
                lookups.Add(TableTestUtil.IKey(uk, snap, DbFormat.ValueTypeForSeek));
            }
            lookups.Sort((a, b) => icmp.Compare(a, b));

            var expected = new List<List<Entry>>();
            foreach (byte[] lk in lookups)
            {
                var e = new List<Entry>();
                for (int p = LowerBound(model, icmp, lk); p < model.Count
                     && ucmp.Compare(DbFormat.ExtractUserKey(model[p].Key), DbFormat.ExtractUserKey(lk)) == 0; p++) e.Add(model[p]);
                expected.Add(e);
            }

            for (int i = 0; i < lookups.Count; i++)
            {
                var got = new List<Entry>();
                reader.Get(lookups[i], ref got, s_collect, TableReadFlags.None);
                AssertSame(expected[i], got, trace + " Get#" + i);

                var gotAsync = new List<Entry>();
                await reader.GetAsync(lookups[i], gotAsync, s_collect, TableReadFlags.None, CancellationToken.None);
                AssertSame(expected[i], gotAsync, trace + " GetAsync#" + i);

                var w = new ArrayBufferWriter<byte>();
                TableGetResult r = reader.Get(lookups[i], w, TableReadFlags.None);
                Assert.AreEqual(expected[i].Count > 0, r.Found, trace + " Get(first)#" + i);
                if (r.Found)
                {
                    DbFormat.UnpackSequenceAndType(DbFormat.ExtractFooter(expected[i][0].Key), out ulong seq, out EntryType t);
                    Assert.AreEqual(seq, r.Sequence, trace);
                    Assert.AreEqual(t, r.Type, trace);
                    Assert.AreEqual(Convert.ToHexString(expected[i][0].Value), Convert.ToHexString(w.WrittenSpan), trace);
                }
            }

            foreach (bool synchronous in new[] { true, false })
            {
                var batch = new Batch(lookups);
                int[] keys = Enumerable.Range(0, lookups.Count).ToArray();
                await reader.MultiGetAsync(batch, keys, keys.Length, new bool[keys.Length], TableReadFlags.None, synchronous, CancellationToken.None);
                for (int i = 0; i < lookups.Count; i++) AssertSame(expected[i], batch.Seen[i], trace + " MultiGet#" + i + " sync=" + synchronous);
            }

            using BlockBasedTableIterator it = reader.NewIterator();
            int pos = -1;
            for (int op = 0; op < 400; op++)
            {
                bool async = rnd.Next(2) == 0;
                int kind = rnd.Next(8);
                bool valid = pos >= 0 && pos < model.Count;
                string what = trace + " op#" + op + " kind=" + kind + " async=" + async;
                if (kind == 0)
                {
                    if (async) await it.SeekToFirstAsync(); else it.SeekToFirst();
                    pos = 0;
                }
                else if (kind == 1)
                {
                    if (async) await it.SeekToLastAsync(); else it.SeekToLast();
                    pos = model.Count - 1;
                }
                else if (kind <= 3 || !valid)
                {
                    byte[] target = rnd.Next(2) == 0
                        ? model[rnd.Next(model.Count)].Key
                        : TableTestUtil.IKey(RandomUserKey(rnd), (ulong)rnd.Next(0, 400), rnd.Next(2) == 0 ? EntryType.Value : DbFormat.ValueTypeForSeek);
                    int lb = LowerBound(model, icmp, target);
                    if (rnd.Next(2) == 0)
                    {
                        if (async) await it.SeekAsync(target); else it.Seek(target);
                        pos = lb;
                    }
                    else
                    {
                        if (async) await it.SeekForPrevAsync(target); else it.SeekForPrev(target);
                        pos = lb < model.Count && icmp.Compare(model[lb].Key, target) == 0 ? lb : lb - 1;
                    }
                }
                else if (kind <= 5)
                {
                    if (async) await it.NextAsync(); else it.Next();
                    pos++;
                }
                else
                {
                    if (async) await it.PrevAsync(); else it.Prev();
                    pos--;
                }
                AssertAt(model, pos, it, what);
            }
        }
    }

    private static void AssertSame(List<Entry> expected, List<Entry> actual, string what)
    {
        Assert.AreEqual(expected.Count, actual.Count, what + ": entry count");
        for (int i = 0; i < expected.Count; i++)
        {
            Assert.AreEqual(Convert.ToHexString(expected[i].Key), Convert.ToHexString(actual[i].Key), what + ": key " + i);
            Assert.AreEqual(Convert.ToHexString(expected[i].Value), Convert.ToHexString(actual[i].Value), what + ": value " + i);
        }
    }

    /// <summary>
    /// ReadFooterFromFile / Footer::DecodeFrom / BlockBasedTable::Open (ReadMetaIndexBlock, ReadPropertiesBlock, index
    /// handle) / BlockFetcher: any single corrupted byte in the footer of a format_version 5 file (no footer checksum)
    /// and any truncation must surface as a RocksDbException (Corruption / NotSupported / InvalidArgument), never as an
    /// IndexOutOfRange, overflow or out-of-memory failure.
    /// </summary>
    [TestMethod]
    public void CorruptFooterOrTruncatedFileFailsWithStatus()
    {
        List<(byte[] Key, byte[] Value)> entries = TableTestUtil.MakeEntries(300, 7);
        foreach (int fv in new[] { 5, 6, 7 })
        {
            byte[] good = TableTestUtil.BuildTable(entries, new TableFormatOptions { FormatVersion = fv, BlockSize = 256 });
            int footerStart = good.Length - Footer.EncodedLength;
            for (int i = footerStart; i < good.Length; i++)
            {
                foreach (byte flip in new byte[] { 0x01, 0x80, 0xff })
                {
                    byte[] bad = (byte[])good.Clone();
                    bad[i] ^= flip;
                    ExpectStatusOrSuccess(bad, $"fv={fv} footer byte {i - footerStart} ^ {flip:X2}");
                }
            }
            for (int len = 0; len < good.Length; len += len < good.Length - 200 ? 37 : 1)
            {
                byte[] bad = good.AsSpan(0, len).ToArray();
                try
                {
                    using BlockBasedTableReader r = TableTestUtil.Open(bad);
                    Assert.Fail($"fv={fv}: a file truncated to {len} of {good.Length} bytes opened");
                }
                catch (RocksDbException)
                {
                }
            }
        }
    }

    private static void ExpectStatusOrSuccess(byte[] file, string what)
    {
        try
        {
            using BlockBasedTableReader r = TableTestUtil.Open(file);
            using BlockBasedTableIterator it = r.NewIterator();
            int n = 0;
            for (it.SeekToFirst(); it.Valid && n < 1000; it.Next()) n++;
        }
        catch (RocksDbException)
        {
        }
        catch (Exception e)
        {
            Assert.Fail(what + ": expected a RocksDbException (Status), got " + e.GetType().Name + ": " + e.Message);
        }
    }

    /// <summary>
    /// BlockBasedTable::Get with read_tier = kBlockCacheTier (no_io): a data block that is not in the block cache ends
    /// the lookup with Incomplete; once cached, the same lookup succeeds. MultiGet reports Incomplete per key.
    /// </summary>
    [TestMethod]
    public async Task BlockCacheTierGetIsIncompleteUntilBlockIsCached()
    {
        List<(byte[] Key, byte[] Value)> entries = TableTestUtil.MakeEntries(200, 3);
        byte[] data = TableTestUtil.BuildTable(entries, new TableFormatOptions { BlockSize = 256 });
        var cache = RocksDb.Managed.Cache.BlockCache.CreateLru(1 << 20);
        using BlockBasedTableReader reader = TableTestUtil.Open(data, new TableReaderOptions { BlockCache = cache });
        byte[] lk = TableTestUtil.IKey(DbFormat.ExtractUserKey(entries[150].Key), DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);

        var ex = Assert.ThrowsExactly<RocksDbException>(() => reader.Get(lk, null, TableReadFlags.BlockCacheTier));
        Assert.AreEqual(StatusCode.Incomplete, ex.Code);

        var lookups = new List<byte[]> { TableTestUtil.IKey(DbFormat.ExtractUserKey(entries[10].Key), DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek), lk };
        Assert.IsTrue(reader.Get(lookups[0], null, TableReadFlags.None).Found); // caches the block of key 10
        var batch = new Batch(lookups);
        await reader.MultiGetAsync(batch, new[] { 0, 1 }, 2, new bool[2], TableReadFlags.BlockCacheTier, synchronous: true, CancellationToken.None);
        Assert.AreEqual(1, batch.Seen[0].Count);
        Assert.IsFalse(batch.Incomplete.Contains(0));
        Assert.IsTrue(batch.Incomplete.Contains(1));

        Assert.IsTrue(reader.Get(lk, null, TableReadFlags.None).Found);
        Assert.IsTrue(reader.Get(lk, null, TableReadFlags.BlockCacheTier).Found);
    }

    /// <summary>
    /// BlockBasedTable::FullFilterKeyMayMatch with whole_key_filtering = false: only keys in the prefix extractor's
    /// domain are filtered; a key outside the domain must be looked up in the table.
    /// </summary>
    [TestMethod]
    public void PrefixFilterDoesNotExcludeOutOfDomainKeys()
    {
        SliceTransform pe = SliceTransform.CreateFixedPrefix(3);
        var policy = new BloomFilterPolicy(10);
        var entries = new List<(byte[], byte[])>
        {
            (TableTestUtil.IKey("a", 5), DbTestUtil.B("short")),          // out of domain (shorter than the prefix)
            (TableTestUtil.IKey("abcd", 6), DbTestUtil.B("in-domain")),
        };
        byte[] data = TableTestUtil.BuildTable(entries, new TableFormatOptions { FilterPolicy = policy, WholeKeyFiltering = false },
            new TableBuilderOptions { PrefixExtractor = pe });
        using BlockBasedTableReader reader = TableTestUtil.Open(data, new TableReaderOptions { FilterPolicy = policy, PrefixExtractor = pe });
        Assert.IsFalse(reader.WholeKeyFiltering);
        Assert.IsTrue(reader.Get(TableTestUtil.LookupKey("a"), null).Found);
        Assert.IsTrue(reader.Get(TableTestUtil.LookupKey("abcd"), null).Found);
        Assert.IsFalse(reader.Get(TableTestUtil.LookupKey("abce"), null).Found);
        // A prefix that was never added is ruled out by the filter alone.
        Assert.IsFalse(reader.InternalKeyMayMatch(TableTestUtil.LookupKey("zzzz"), TableReadFlags.None));
        Assert.IsTrue(reader.InternalKeyMayMatch(TableTestUtil.LookupKey("zz"), TableReadFlags.None));
    }

    // ---------------------------------------------------------------------------------------------
    // SstFileWriter

    private static SstFileWriter NewWriter(MemoryFileSystem fs, ColumnFamilyOptions? options = null) => new(options ?? new ColumnFamilyOptions(), fs);

    private static StatusCode CodeOf(Action a) => Assert.ThrowsExactly<RocksDbException>(a).Code;

    /// <summary>SstFileWriter::Rep::AddImpl / DeleteRangeImpl / Finish argument validation (table/sst_file_writer.cc).</summary>
    [TestMethod]
    public void SstFileWriterValidation()
    {
        MemoryFileSystem fs = DbTestUtil.NewFs();
        using SstFileWriter w = NewWriter(fs);
        // "File is not opened"
        Assert.AreEqual(StatusCode.InvalidArgument, CodeOf(() => w.Put("a"u8, "1"u8)));
        Assert.AreEqual(StatusCode.InvalidArgument, CodeOf(() => w.DeleteRange("a"u8, "b"u8)));
        Assert.AreEqual(StatusCode.InvalidArgument, CodeOf(() => w.Finish()));

        w.Open("/db/a.sst");
        // "File is already opened"
        Assert.AreEqual(StatusCode.InvalidArgument, CodeOf(() => w.Open("/db/b.sst")));
        // "Cannot create sst file with no entries": the writer stays open.
        Assert.AreEqual(StatusCode.InvalidArgument, CodeOf(() => w.Finish()));
        // An empty range is accepted and records nothing; a reversed range is an error.
        w.DeleteRange("k"u8, "k"u8);
        Assert.AreEqual(StatusCode.InvalidArgument, CodeOf(() => w.Finish()));
        Assert.AreEqual(StatusCode.InvalidArgument, CodeOf(() => w.DeleteRange("b"u8, "a"u8)));

        w.Put("b"u8, "1"u8);
        // "Keys must be added in strict ascending order." for every entry kind, equal keys included.
        Assert.AreEqual(StatusCode.InvalidArgument, CodeOf(() => w.Put("b"u8, "2"u8)));
        Assert.AreEqual(StatusCode.InvalidArgument, CodeOf(() => w.Merge("b"u8, "2"u8)));
        Assert.AreEqual(StatusCode.InvalidArgument, CodeOf(() => w.Delete("a"u8)));
        // Timestamp overloads without a timestamp comparator: "Timestamp size mismatch".
        Assert.AreEqual(StatusCode.InvalidArgument, CodeOf(() => w.Put("c"u8, new byte[8], "2"u8)));
        Assert.AreEqual(StatusCode.InvalidArgument, CodeOf(() => w.Delete("c"u8, new byte[8])));
        Assert.AreEqual(StatusCode.InvalidArgument, CodeOf(() => w.DeleteRange("c"u8, "d"u8, new byte[8])));
        w.Merge("c"u8, "m"u8);
        w.Delete("d"u8);
        // Range deletions may come in any order and overlap the point keys.
        w.DeleteRange("x"u8, "z"u8);
        w.DeleteRange("a"u8, "c"u8);
        ExternalSstFileInfo info = w.Finish();
        Assert.AreEqual("b", DbTestUtil.S(info.SmallestKey));
        Assert.AreEqual("d", DbTestUtil.S(info.LargestKey));
        Assert.AreEqual("a", DbTestUtil.S(info.SmallestRangeDelKey));
        Assert.AreEqual("z", DbTestUtil.S(info.LargestRangeDelKey));
        Assert.AreEqual(3UL, info.NumEntries);
        Assert.AreEqual(2UL, info.NumRangeDelEntries);
        Assert.AreEqual(0UL, info.SequenceNumber);
        Assert.AreEqual(2, info.Version);
        Assert.AreEqual((ulong)fs.ReadAllBytes("/db/a.sst").Length, info.FileSize);
        // Finished: closed again.
        Assert.AreEqual(StatusCode.InvalidArgument, CodeOf(() => w.Put("e"u8, "1"u8)));
    }

    /// <summary>
    /// SstFileWriter::Open's TableBuilderOptions and SstFileWriterPropertiesCollector: version 2, global seqno 0,
    /// db_id "SST Writer", creation_time / oldest_key_time / file_creation_time 0 (so "rocksdb.file.creation.time" is
    /// not written), every key at sequence number 0; BlockBasedTableBuilder::Add's entry counters.
    /// </summary>
    [TestMethod]
    public void SstFileWriterProperties()
    {
        MemoryFileSystem fs = DbTestUtil.NewFs();
        using (SstFileWriter w = NewWriter(fs))
        {
            w.Open("/db/p.sst");
            w.Put("a"u8, "value-a"u8);
            w.Merge("b"u8, "m"u8);
            w.Delete("c"u8);
            w.DeleteRange("d"u8, "f"u8);
            w.Finish();
        }
        using BlockBasedTableReader r = BlockBasedTableReader.Open(fs.OpenRandomAccess("/db/p.sst"), TableReaderOptions.Default);
        TableProperties p = r.Properties;
        Assert.AreEqual(4UL, p.NumEntries);
        Assert.AreEqual(2UL, p.NumDeletions);       // the point deletion and the range deletion
        Assert.AreEqual(1UL, p.NumRangeDeletions);
        Assert.AreEqual(1UL, p.NumMergeOperands);
        Assert.AreEqual((ulong)(4 * (1 + 8)), p.RawKeySize);
        Assert.AreEqual((ulong)("value-a".Length + 1 + 0 + 1), p.RawValueSize); // the range tombstone's value is its end key
        Assert.AreEqual(0UL, p.KeySmallestSeqno);
        Assert.AreEqual(0UL, p.KeyLargestSeqno);
        Assert.AreEqual("SST Writer", p.DbId);
        Assert.AreEqual(0UL, p.CreationTime);
        Assert.AreEqual(0UL, p.OldestKeyTime);
        Assert.AreEqual(0UL, p.FileCreationTime, "SstFileWriter::Open passes file_creation_time = 0");
        // Rep::next_file_number starts at 1 and the session id lives as long as the writer.
        Assert.AreEqual(1UL, p.OriginalFileNumber);
        using (SstFileWriter w = NewWriter(fs))
        {
            string[] sessions = new string[2];
            for (int i = 0; i < 2; i++)
            {
                w.Open("/db/q" + i + ".sst");
                w.Put("a"u8, "1"u8);
                w.Finish();
                using BlockBasedTableReader q = BlockBasedTableReader.Open(fs.OpenRandomAccess("/db/q" + i + ".sst"), TableReaderOptions.Default);
                Assert.AreEqual((ulong)(i + 1), q.Properties.OriginalFileNumber);
                sessions[i] = q.Properties.DbSessionId;
            }
            Assert.AreEqual(sessions[0], sessions[1]);
            Assert.AreNotEqual(p.DbSessionId, sessions[0]);
        }
        Assert.AreEqual(2u, RocksDb.Managed.Util.Coding.DecodeFixed32(p.UserCollectedProperties[TablePropertiesNames.ExternalSstFileVersion]));
        Assert.AreEqual(0UL, RocksDb.Managed.Util.Coding.DecodeFixed64(p.UserCollectedProperties[TablePropertiesNames.ExternalSstFileGlobalSeqno]));
        // Opened with no known largest seqno (kMaxSequenceNumber): a zero global seqno, keys read back with seqno 0.
        using BlockBasedTableIterator it = r.NewIterator();
        int n = 0;
        for (it.SeekToFirst(); it.Valid; it.Next(), n++)
        {
            DbFormat.UnpackSequenceAndType(DbFormat.ExtractFooter(it.Key), out ulong seq, out _);
            Assert.AreEqual(0UL, seq);
        }
        Assert.AreEqual(3, n);
    }

    /// <summary>
    /// GetGlobalSequenceNumber (table/meta_blocks.cc) + BlockIter global seqno: an external file opened with the
    /// largest seqno the MANIFEST assigned exposes every point key and range tombstone at that sequence number, in Get,
    /// in the iterator and in the range tombstone iterator; a file that is not external must not carry a global seqno.
    /// </summary>
    [TestMethod]
    public void GlobalSeqnoAppliesToGetIteratorAndRangeTombstones()
    {
        MemoryFileSystem fs = DbTestUtil.NewFs();
        using (SstFileWriter w = NewWriter(fs))
        {
            w.Open("/db/g.sst");
            w.Put("a"u8, "1"u8);
            w.Delete("b"u8);
            w.DeleteRange("c"u8, "e"u8);
            w.Finish();
        }
        using BlockBasedTableReader r = BlockBasedTableReader.Open(fs.OpenRandomAccess("/db/g.sst"), new TableReaderOptions { LargestSeqno = 42 });
        Assert.AreEqual(42UL, r.GlobalSequenceNumber);
        // A snapshot below the global seqno does not see the file's entries; at or above it does.
        Assert.IsFalse(r.Get(TableTestUtil.LookupKey("a", 41), null).Found);
        TableGetResult g = r.Get(TableTestUtil.LookupKey("a", 42), null);
        Assert.IsTrue(g.Found);
        Assert.AreEqual(42UL, g.Sequence);
        Assert.AreEqual(EntryType.Value, g.Type);
        g = r.Get(TableTestUtil.LookupKey("b"), null);
        Assert.AreEqual(EntryType.Deletion, g.Type);
        Assert.AreEqual(42UL, g.Sequence);
        using (BlockBasedTableIterator it = r.NewIterator())
        {
            it.Seek(TableTestUtil.IKey("a", 42, DbFormat.ValueTypeForSeek));
            Assert.IsTrue(it.Valid);
            Assert.AreEqual(Convert.ToHexString(TableTestUtil.IKey("a", 42)), Convert.ToHexString(it.Key));
            it.Seek(TableTestUtil.IKey("a", 41, DbFormat.ValueTypeForSeek));
            Assert.AreEqual(Convert.ToHexString(TableTestUtil.IKey("b", 42, EntryType.Deletion)), Convert.ToHexString(it.Key));
            it.SeekForPrev(TableTestUtil.IKey("b", 41, DbFormat.ValueTypeForSeek));
            Assert.AreEqual(Convert.ToHexString(TableTestUtil.IKey("b", 42, EntryType.Deletion)), Convert.ToHexString(it.Key));
        }
        using (BlockIter rd = r.NewRangeTombstoneIterator()!)
        {
            rd.SeekToFirst();
            Assert.IsTrue(rd.Valid);
            Assert.AreEqual(Convert.ToHexString(TableTestUtil.IKey("c", 42, EntryType.RangeDeletion)), Convert.ToHexString(rd.Key));
            Assert.AreEqual("e", DbTestUtil.S(rd.Value.ToArray()));
        }
    }

    /// <summary>
    /// BlockBasedTableFactory::InitializeOptions: a format_version below kMinSupportedBbtFormatVersionForWrite (2) is
    /// sanitized to it instead of failing the table build.
    /// </summary>
    [TestMethod]
    public void FormatVersionBelowMinimumIsSanitized()
    {
        foreach (uint fv in new uint[] { 0, 1 })
        {
            Assert.AreEqual(2, TableFactory.CreateFormatOptions(new BlockBasedTableOptions { FormatVersion = fv }).FormatVersion);
            MemoryFileSystem fs = DbTestUtil.NewFs();
            var cf = new ColumnFamilyOptions();
            cf.TableOptions.FormatVersion = fv;
            using (SstFileWriter w = NewWriter(fs, cf))
            {
                w.Open("/db/v.sst");
                w.Put("a"u8, "1"u8);
                w.Finish();
            }
            using BlockBasedTableReader r = BlockBasedTableReader.Open(fs.OpenRandomAccess("/db/v.sst"), TableReaderOptions.Default);
            Assert.AreEqual(2, r.Footer.FormatVersion);
            Assert.IsTrue(r.Get(TableTestUtil.LookupKey("a"), null).Found);
        }
        Assert.AreEqual(5, TableFactory.CreateFormatOptions(new BlockBasedTableOptions { FormatVersion = 5 }).FormatVersion);
    }

    /// <summary>
    /// SstFileReader::Rep::MultiGet (table/sst_file_reader_sync_and_async.h) reads as of ReadOptions::snapshot, while
    /// Rep::Get always uses LookupKey(key, kMaxSequenceNumber); NewIterator honours the snapshot too.
    /// </summary>
    [TestMethod]
    public void SstFileReaderMultiGetReadsAtSnapshot()
    {
        MemoryFileSystem fs = DbTestUtil.NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, DbTestUtil.NewOptions(fs));
        db.Put("k"u8, "old"u8);
        db.Put("gone"u8, "x"u8);
        using Snapshot snap = db.GetSnapshot();
        db.Put("k"u8, "new"u8);
        db.Delete("gone"u8);
        db.Put("later"u8, "y"u8);
        db.Flush();
        string sst = fs.GetChildren(DbTestUtil.DbPath).Single(f => f.EndsWith(".sst", StringComparison.Ordinal));
        using var reader = new SstFileReader(new ColumnFamilyOptions(), fs);
        reader.Open(sst.StartsWith('/') ? sst : DbTestUtil.DbPath + "/" + sst);
        var atSnapshot = new ReadOptions { Snapshot = snap };

        using var batch = new MultiGetBatch();
        batch.Add("k"u8).Add("gone"u8).Add("later"u8);
        reader.MultiGet(atSnapshot, batch);
        Assert.AreEqual("old", DbTestUtil.S(batch.GetValue(0).ToArray()));
        Assert.AreEqual("x", DbTestUtil.S(batch.GetValue(1).ToArray()));
        Assert.IsFalse(batch.TryGetValue(2, out _));

        batch.Clear();
        batch.Add("k"u8).Add("gone"u8).Add("later"u8);
        reader.MultiGet(null, batch);
        Assert.AreEqual("new", DbTestUtil.S(batch.GetValue(0).ToArray()));
        Assert.IsFalse(batch.TryGetValue(1, out _));
        Assert.AreEqual("y", DbTestUtil.S(batch.GetValue(2).ToArray()));

        // Get ignores the snapshot.
        Assert.AreEqual("new", DbTestUtil.S(reader.Get(atSnapshot, "k"u8)));
        Assert.IsNull(reader.Get(atSnapshot, "gone"u8));

        using Iterator it = reader.NewIterator(atSnapshot);
        var seen = new List<string>();
        for (it.SeekToFirst(); it.Valid; it.Next()) seen.Add(DbTestUtil.S(it.Key.ToArray()) + "=" + DbTestUtil.S(it.Value.ToArray()));
        CollectionAssert.AreEqual(new[] { "gone=x", "k=old" }, seen);
    }

    /// <summary>
    /// BlockBasedTable::ApproximateKeyAnchors (iiter->user_key()) / ApproximateOffsetOf / ApproximateSize: anchors are
    /// user keys in order whose range sizes add up to the data size, whether the index stores user keys or internal keys.
    /// </summary>
    [TestMethod]
    public void ApproximateKeyAnchorsAreUserKeys()
    {
        List<(byte[] Key, byte[] Value)> entries = TableTestUtil.MakeEntries(2000, 11);
        foreach (IndexShorteningMode mode in new[] { IndexShorteningMode.NoShortening, IndexShorteningMode.ShortenSeparators })
        {
            foreach (IndexType type in new[] { IndexType.BinarySearch, IndexType.TwoLevelIndexSearch })
            {
                byte[] data = TableTestUtil.BuildTable(entries, new TableFormatOptions { BlockSize = 128, IndexShortening = mode, IndexType = type, MetadataBlockSize = 256 });
                using BlockBasedTableReader r = TableTestUtil.Open(data);
                List<(byte[] UserKey, ulong RangeSize)> anchors = r.ApproximateKeyAnchors();
                Assert.IsTrue(anchors.Count is > 100 and <= 257, anchors.Count.ToString());
                ulong total = 0;
                byte[]? prev = null;
                foreach ((byte[] k, ulong size) in anchors)
                {
                    Assert.IsTrue(k.Length <= 11 && k.AsSpan().StartsWith("key"u8), Convert.ToHexString(k));
                    if (prev is not null) Assert.IsTrue(prev.AsSpan().SequenceCompareTo(k) < 0);
                    prev = k;
                    total += size;
                }
                // Every data block and its trailer except the last trailer.
                Assert.AreEqual(r.Properties.DataSize - BlockBasedTableConstants.BlockTrailerSize, total);

                Assert.AreEqual(0UL, r.ApproximateOffsetOf(entries[0].Key));
                ulong mid = r.ApproximateOffsetOf(entries[1000].Key);
                Assert.IsTrue(mid > (ulong)data.Length / 3 && mid < (ulong)data.Length * 2 / 3, mid.ToString());
                Assert.AreEqual((ulong)data.Length, r.ApproximateOffsetOf(TableTestUtil.IKey("zzz", 1)));
                Assert.AreEqual(0UL, r.ApproximateSize(entries[5].Key, entries[5].Key));
                Assert.AreEqual((ulong)data.Length, r.ApproximateSize(entries[0].Key, TableTestUtil.IKey("zzz", 1)));
            }
        }
    }

    /// <summary>
    /// BlockBasedTable::Open with checksum verification off (ReadMetaIndexBlock, ReadPropertiesBlock, index reader
    /// creation, Block / BlockIter decoding): a corrupted byte anywhere in the tail (filter, index, properties,
    /// metaindex) must fail with a Status (RocksDbException) or be harmless, never with another exception type.
    /// </summary>
    [TestMethod]
    public void CorruptTailWithoutChecksumVerificationFailsWithStatus()
    {
        List<(byte[] Key, byte[] Value)> entries = TableTestUtil.MakeEntries(120, 5);
        foreach (IndexType type in new[] { IndexType.BinarySearch, IndexType.TwoLevelIndexSearch, IndexType.BinarySearchWithFirstKey })
        {
            byte[] good = TableTestUtil.BuildTable(entries, new TableFormatOptions { FormatVersion = 5, BlockSize = 256, IndexType = type, MetadataBlockSize = 128 });
            int tailStart;
            using (BlockBasedTableReader r = TableTestUtil.Open(good)) tailStart = (int)r.Properties.DataSize;
            var options = new TableReaderOptions { VerifyChecksums = false };
            for (int i = tailStart; i < good.Length - Footer.EncodedLength; i++)
            {
                foreach (byte flip in new byte[] { 0x01, 0x80, 0xff })
                {
                    byte[] bad = (byte[])good.Clone();
                    bad[i] ^= flip;
                    try
                    {
                        using BlockBasedTableReader r = TableTestUtil.Open(bad, options);
                        using BlockBasedTableIterator it = r.NewIterator();
                        int n = 0;
                        for (it.SeekToFirst(); it.Valid && n < 1000; it.Next()) n++;
                        r.Get(entries[60].Key, null);
                    }
                    catch (RocksDbException)
                    {
                    }
                    catch (Exception e)
                    {
                        Assert.Fail($"index={type} byte {i} (tail starts at {tailStart}, file {good.Length}) ^ {flip:X2}: {e.GetType().Name}: {e.Message}");
                    }
                }
            }
        }
    }

    /// <summary>
    /// SstFileWriter::Rep::Add(user_key, timestamp, ...) / AddImpl with a timestamp comparator: entries are ordered by
    /// the user comparator over key + timestamp (same key: newer timestamp first), "Timestamp size mismatch" for the
    /// overloads without or with a wrong-sized timestamp, and DeleteRange compares the bounds without the timestamp.
    /// </summary>
    [TestMethod]
    public void SstFileWriterWithTimestampsValidatesOrder()
    {
        static byte[] Ts(ulong t)
        {
            byte[] b = new byte[8];
            ComparatorWithU64Ts.EncodeU64Ts(t, b);
            return b;
        }
        MemoryFileSystem fs = DbTestUtil.NewFs();
        using SstFileWriter w = NewWriter(fs, new ColumnFamilyOptions { Comparator = Comparator.BytewiseWithU64Ts });
        w.Open("/db/t.sst");
        Assert.AreEqual(StatusCode.InvalidArgument, CodeOf(() => w.Put("a"u8, "v"u8)));
        Assert.AreEqual(StatusCode.InvalidArgument, CodeOf(() => w.Delete("a"u8)));
        Assert.AreEqual(StatusCode.InvalidArgument, CodeOf(() => w.DeleteRange("a"u8, "b"u8)));
        Assert.AreEqual(StatusCode.InvalidArgument, CodeOf(() => w.Put("a"u8, new byte[4], "v"u8)));
        w.Put("a"u8, Ts(10), "a10"u8);
        Assert.AreEqual(StatusCode.InvalidArgument, CodeOf(() => w.Put("a"u8, Ts(10), "dup"u8)));
        Assert.AreEqual(StatusCode.InvalidArgument, CodeOf(() => w.Put("a"u8, Ts(11), "newer"u8)));
        w.Put("a"u8, Ts(9), "a9"u8);
        w.Delete("a"u8, Ts(3));
        w.Put("b"u8, Ts(100), "b100"u8);
        Assert.AreEqual(StatusCode.InvalidArgument, CodeOf(() => w.DeleteRange("d"u8, "c"u8, Ts(5))));
        w.DeleteRange("c"u8, "c"u8, Ts(5)); // empty: ignored
        w.DeleteRange("c"u8, "d"u8, Ts(5));
        ExternalSstFileInfo info = w.Finish();
        Assert.AreEqual(4UL, info.NumEntries);
        Assert.AreEqual(1UL, info.NumRangeDelEntries);
        Assert.AreEqual(Convert.ToHexString([.. "a"u8, .. Ts(10)]), Convert.ToHexString(info.SmallestKey));
        Assert.AreEqual(Convert.ToHexString([.. "b"u8, .. Ts(100)]), Convert.ToHexString(info.LargestKey));

        using BlockBasedTableReader r = BlockBasedTableReader.Open(fs.OpenRandomAccess("/db/t.sst"),
            new TableReaderOptions { Comparator = Comparator.BytewiseWithU64Ts });
        // BlockBasedTable::Get with timestamps: a lookup at timestamp 9 starts at the version written at 9 and the
        // saver sees the older versions of the same key (EqualWithoutTimestamp), not the newer one.
        var got = new List<Entry>();
        GetAll(r, [.. "a"u8, .. Ts(9)], got);
        Assert.AreEqual(2, got.Count);
        Assert.AreEqual("a9", DbTestUtil.S(got[0].Value));
        Assert.AreEqual((byte)EntryType.DeletionWithTimestamp, got[1].Key[^8]);
        Assert.AreEqual(Convert.ToHexString(Ts(100)), Convert.ToHexString(r.Properties.UserCollectedProperties[TablePropertiesNames.TimestampMax]));
        Assert.AreEqual(Convert.ToHexString(Ts(3)), Convert.ToHexString(r.Properties.UserCollectedProperties[TablePropertiesNames.TimestampMin]));
    }

    private static void GetAll(BlockBasedTableReader r, byte[] userKeyWithTs, List<Entry> got) =>
        r.Get(TableTestUtil.IKey(userKeyWithTs, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek), ref got, s_collect, TableReadFlags.None);

    /// <summary>
    /// BlockBasedTable::NewIterator (check_filter, need_upper_bound_check) + BlockBasedTableIterator::CheckPrefixMayMatch
    /// + BlockBasedTable::PrefixRangeMayMatch / FilterBlockReaderCommon::RangeMayExist / IsFilterCompatible: when the
    /// prefix filter may end a Seek / SeekForPrev early and when it must not.
    /// </summary>
    [TestMethod]
    public void IteratorPrefixFilterChecks()
    {
        SliceTransform fixed3 = SliceTransform.CreateFixedPrefix(3);
        var policy = new ExactSetFilterPolicy();
        var entries = new List<(byte[], byte[])>
        {
            (TableTestUtil.IKey("aa", 9), DbTestUtil.B("short")), // out of the extractor's domain
            (TableTestUtil.IKey("abc1", 9), DbTestUtil.B("1")),
            (TableTestUtil.IKey("abe1", 9), DbTestUtil.B("2")),
        };
        byte[] data = TableTestUtil.BuildTable(entries, new TableFormatOptions { FilterPolicy = policy, WholeKeyFiltering = false },
            new TableBuilderOptions { PrefixExtractor = fixed3 });
        byte[] absent = TableTestUtil.IKey("abd0", DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
        string abe1 = Convert.ToHexString(TableTestUtil.IKey("abe1", 9));
        string abc1 = Convert.ToHexString(TableTestUtil.IKey("abc1", 9));

        // Same extractor as the table.
        using (BlockBasedTableReader r = TableTestUtil.Open(data, new TableReaderOptions { FilterPolicy = policy, PrefixExtractor = fixed3 }))
        {
            // Prefix seek: the filter excludes "abd", the iterator is invalid; backward as well.
            using (BlockBasedTableIterator it = r.NewIterator(TableReadFlags.None, r.GetPrefixOptions(new ReadOptions(), fixed3)))
            {
                it.Seek(absent);
                Assert.IsFalse(it.Valid);
                it.SeekForPrev(absent);
                Assert.IsFalse(it.Valid);
                // A seek key outside the domain is never filtered.
                it.Seek(TableTestUtil.IKey("ab", DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek));
                Assert.AreEqual(abc1, Convert.ToHexString(it.Key));
                it.Seek(TableTestUtil.IKey("abc0", DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek));
                Assert.AreEqual(abc1, Convert.ToHexString(it.Key));
            }
            // total_order_seek: check_filter is off.
            using (BlockBasedTableIterator it = r.NewIterator(TableReadFlags.None, r.GetPrefixOptions(new ReadOptions { TotalOrderSeek = true }, fixed3)))
            {
                it.Seek(absent);
                Assert.AreEqual(abe1, Convert.ToHexString(it.Key));
                it.SeekForPrev(absent);
                Assert.AreEqual(abc1, Convert.ToHexString(it.Key));
            }
            // auto_prefix_mode: need_upper_bound_check. Without an upper bound inside the prefix the filter is not
            // used; with one it is (forward only).
            using (BlockBasedTableIterator it = r.NewIterator(TableReadFlags.None, r.GetPrefixOptions(new ReadOptions { AutoPrefixMode = true }, fixed3)))
            {
                it.Seek(absent);
                Assert.AreEqual(abe1, Convert.ToHexString(it.Key));
            }
            var bounded = new ReadOptions { AutoPrefixMode = true, IterateUpperBound = DbTestUtil.B("abd9") };
            using (BlockBasedTableIterator it = r.NewIterator(TableReadFlags.None, r.GetPrefixOptions(bounded, fixed3)))
            {
                it.Seek(absent);
                Assert.IsFalse(it.Valid);
                it.SeekForPrev(absent); // kBackward with need_upper_bound_check: no prefix check
                Assert.AreEqual(abc1, Convert.ToHexString(it.Key));
            }
            // The upper bound "abe" is the immediate successor of the full-length prefix "abd": still compatible.
            var successor = new ReadOptions { AutoPrefixMode = true, IterateUpperBound = DbTestUtil.B("abe") };
            using (BlockBasedTableIterator it = r.NewIterator(TableReadFlags.None, r.GetPrefixOptions(successor, fixed3)))
            {
                it.Seek(absent);
                Assert.IsFalse(it.Valid);
            }
            // An upper bound in another prefix that is not the immediate successor: not compatible, no filtering.
            var far = new ReadOptions { AutoPrefixMode = true, IterateUpperBound = DbTestUtil.B("abf") };
            using (BlockBasedTableIterator it = r.NewIterator(TableReadFlags.None, r.GetPrefixOptions(far, fixed3)))
            {
                it.Seek(absent);
                Assert.AreEqual(abe1, Convert.ToHexString(it.Key));
            }
        }

        // The column family's extractor changed (PrefixExtractorChanged): the table's own extractor is used, and only
        // under an upper bound that keeps the scan inside one table prefix.
        SliceTransform fixed2 = SliceTransform.CreateFixedPrefix(2);
        using (BlockBasedTableReader r = TableTestUtil.Open(data, new TableReaderOptions { FilterPolicy = policy, PrefixExtractor = fixed2 }))
        {
            Assert.IsTrue(r.PrefixExtractorChanged(fixed2));
            using (BlockBasedTableIterator it = r.NewIterator(TableReadFlags.None, r.GetPrefixOptions(new ReadOptions(), fixed2)))
            {
                it.Seek(absent);
                Assert.AreEqual(abe1, Convert.ToHexString(it.Key));
            }
            var bounded = new ReadOptions { IterateUpperBound = DbTestUtil.B("abd9") };
            using (BlockBasedTableIterator it = r.NewIterator(TableReadFlags.None, r.GetPrefixOptions(bounded, fixed2)))
            {
                it.Seek(absent);
                Assert.IsFalse(it.Valid);
            }
            // Point lookups do not use a prefix filter built with another extractor (FullFilterKeyMayMatch).
            Assert.IsTrue(r.InternalKeyMayMatch(absent, TableReadFlags.None));
        }
    }

    /// <summary>
    /// GetGlobalSequenceNumber (table/meta_blocks.cc): only external file versions below 2 are special (version 1 has
    /// no global seqno, anything else is corrupt); every version &gt;= 2 resolves the global seqno like version 2.
    /// </summary>
    [TestMethod]
    public void ExternalFileVersionAboveTwoResolvesGlobalSeqno()
    {
        MemoryFileSystem fs = DbTestUtil.NewFs();
        using (SstFileWriter w = NewWriter(fs))
        {
            w.Open("/db/e.sst");
            w.Put("a"u8, "1"u8);
            w.Finish();
        }
        byte[] file = fs.ReadAllBytes("/db/e.sst");
        // The properties block entry "rocksdb.external_sst_file.version" (key delta "version") = fixed32 2.
        int at = file.AsSpan().IndexOf("version\x02\0\0\0"u8);
        Assert.IsTrue(at > 0);
        var noVerify = new TableReaderOptions { VerifyChecksums = false, LargestSeqno = 7 };

        byte[] v3 = (byte[])file.Clone();
        v3[at + 7] = 3;
        using (BlockBasedTableReader r = TableTestUtil.Open(v3, noVerify))
        {
            Assert.AreEqual(7UL, r.GlobalSequenceNumber);
            Assert.AreEqual(7UL, r.Get(TableTestUtil.LookupKey("a"), null).Sequence);
        }

        // Version 1 with a global seqno property and version 0 are corrupt.
        foreach (byte bad in new byte[] { 0, 1 })
        {
            byte[] v = (byte[])file.Clone();
            v[at + 7] = bad;
            Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => TableTestUtil.Open(v, noVerify)).Code);
        }
    }
}
