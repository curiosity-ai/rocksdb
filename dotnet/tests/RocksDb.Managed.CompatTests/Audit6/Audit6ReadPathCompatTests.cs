using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using RocksDb.Managed.Cache;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Table.Filters;

namespace RocksDb.Managed.CompatTests.Audit6;

/// <summary>
/// Audit #6 (read path, Mosaik's table / filter / memtable configuration): managed vs native on the exact options of
/// RocksDbBase.CreateTableOptions / CreatePointLookupColumnFamilyOptions and the prefix families (partitioned index and
/// filters, pinned top levels, optimize_filters_for_hits, a data block hash index). Covers the iterate_upper_bound check
/// of BlockBasedTableIterator (table/block_based/block_based_table_iterator.cc FindBlockForward /
/// CheckDataBlockWithinUpperBound) and the skip_filters choice of iterators (db/version_set.cc AddIteratorsForLevel).
/// Seeds: AUDIT6_SEED.
/// </summary>
[TestClass]
public sealed class Audit6ReadPathCompatTests
{
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    public enum Family { Prefix8Bloom, Prefix10Bloom, Prefix8NoFilter, PointLookup }

    private sealed class N : IDisposable
    {
        public RocksDbSharp.RocksDb Db = null!;
        public RocksDbSharp.Cache Cache = null!;
        public RocksDbSharp.DbOptions Options = null!;
        public void Dispose() => Db.Dispose();
    }

    private sealed class M : IDisposable
    {
        public RocksDatabase Db = null!;
        public LruCache Cache = null!;
        public Statistics Stats = null!;
        public void Dispose() => Db.Dispose();
    }

    private static N OpenNative(string path, Family family, ulong cacheBytes, int blockSize = 0, int metadataBlockSize = 0)
    {
        RocksDbSharp.Native n = RocksDbSharp.Native.Instance;
        RocksDbSharp.Cache cache = RocksDbSharp.Cache.CreateLru(cacheBytes);
        var t = new RocksDbSharp.BlockBasedTableOptions().SetBlockCache(cache);
        n.rocksdb_block_based_options_set_cache_index_and_filter_blocks(t.Handle, RocksDbSharp.Native.MarshalBool(true));
        n.rocksdb_block_based_options_set_cache_index_and_filter_blocks_with_high_priority(t.Handle, RocksDbSharp.Native.MarshalBool(true));
        n.rocksdb_block_based_options_set_pin_l0_filter_and_index_blocks_in_cache(t.Handle, RocksDbSharp.Native.MarshalBool(true));
        n.rocksdb_block_based_options_set_index_type(t.Handle, RocksDbSharp.BlockBasedTableIndexType.TwoLevelIndex);
        n.rocksdb_block_based_options_set_partition_filters(t.Handle, RocksDbSharp.Native.MarshalBool(true));
        n.rocksdb_block_based_options_set_pin_top_level_index_and_filter(t.Handle, RocksDbSharp.Native.MarshalBool(true));
        if (blockSize != 0) t.SetBlockSize((ulong)blockSize);
        if (metadataBlockSize != 0) n.rocksdb_block_based_options_set_metadata_block_size(t.Handle, (ulong)metadataBlockSize);
        if (family != Family.Prefix8NoFilter) t.SetFilterPolicy(RocksDbSharp.BloomFilterPolicy.Create(10, false));
        if (family == Family.PointLookup)
        {
            n.rocksdb_block_based_options_set_data_block_index_type(t.Handle, (int)RocksDbSharp.BlockBasedTableDataBlockIndexType.BinarySearchAndHash);
            n.rocksdb_block_based_options_set_data_block_hash_table_util_ratio(t.Handle, 0.75);
        }
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetDisableAutoCompactions(1);
        o.EnableStatistics();
        o.SetBlockBasedTableFactory(t);
        switch (family)
        {
            case Family.Prefix8Bloom: o.SetPrefixExtractor(RocksDbSharp.SliceTransform.CreateFixedPrefix(8)).SetOptimizeFiltersForHits(1); break;
            case Family.Prefix10Bloom: o.SetPrefixExtractor(RocksDbSharp.SliceTransform.CreateFixedPrefix(10)).SetOptimizeFiltersForHits(1); break;
            case Family.Prefix8NoFilter: o.SetPrefixExtractor(RocksDbSharp.SliceTransform.CreateFixedPrefix(8)); break;
            case Family.PointLookup:
                o.SetMemtablePrefixBloomSizeRatio(0.02);
                n.rocksdb_options_set_memtable_whole_key_filtering(o.Handle, RocksDbSharp.Native.MarshalBool(true));
                o.SetCompression(RocksDbSharp.Compression.Lz4);
                break;
        }
        s_keepAlive.Add(cache);
        s_keepAlive.Add(t);
        s_keepAlive.Add(o);
        return new N { Db = RocksDbSharp.RocksDb.Open(o, path), Cache = cache, Options = o };
    }

    private static M OpenManaged(string path, Family family, long cacheBytes, int blockSize = 0, int metadataBlockSize = 0)
    {
        var cfo = new ColumnFamilyOptions();
        BlockBasedTableOptions t = cfo.TableOptions;
        var cache = new LruCache(new LruCacheOptions { Capacity = cacheBytes });
        t.BlockCache = cache;
        t.CacheIndexAndFilterBlocks = true;
        t.CacheIndexAndFilterBlocksWithHighPriority = true;
        t.PinL0FilterAndIndexBlocksInCache = true;
        t.IndexType = IndexType.TwoLevelIndexSearch;
        t.PartitionFilters = true;
        t.PinTopLevelIndexAndFilter = true;
        if (blockSize != 0) t.BlockSize = (ulong)blockSize;
        if (metadataBlockSize != 0) t.MetadataBlockSize = (ulong)metadataBlockSize;
        if (family != Family.Prefix8NoFilter) t.FilterPolicy = new BloomFilterPolicy(10);
        if (family == Family.PointLookup)
        {
            t.DataBlockIndexType = DataBlockIndexType.BinaryAndHash;
            t.DataBlockHashTableUtilRatio = 0.75;
        }
        cfo.DisableAutoCompactions = true;
        switch (family)
        {
            case Family.Prefix8Bloom: cfo.PrefixExtractor = SliceTransform.CreateFixedPrefix(8); cfo.OptimizeFiltersForHits = true; break;
            case Family.Prefix10Bloom: cfo.PrefixExtractor = SliceTransform.CreateFixedPrefix(10); cfo.OptimizeFiltersForHits = true; break;
            case Family.Prefix8NoFilter: cfo.PrefixExtractor = SliceTransform.CreateFixedPrefix(8); break;
            case Family.PointLookup:
                cfo.MemtablePrefixBloomSizeRatio = 0.02;
                cfo.MemtableWholeKeyFiltering = true;
                cfo.Compression = Managed.Compression.CompressionType.LZ4;
                break;
        }
        var stats = new Statistics();
        var dbo = new DbOptions { CreateIfMissing = true, Statistics = stats };
        return new M { Db = RocksDatabase.Open(path, dbo, cfo), Cache = cache, Stats = stats };
    }

    private static string Hex(ReadOnlySpan<byte> b) => Convert.ToHexString(b);

    private static ulong TickerValue(string statistics, string name)
    {
        Match m = Regex.Match(statistics, "^" + Regex.Escape(name) + " COUNT : (\\d+)$", RegexOptions.Multiline);
        Assert.IsTrue(m.Success, name);
        return ulong.Parse(m.Groups[1].Value, CultureInfo.InvariantCulture);
    }

    private static int Seed(string env)
    {
        return int.TryParse(Environment.GetEnvironmentVariable(env), NumberStyles.Integer, CultureInfo.InvariantCulture, out int s) ? s : Environment.TickCount;
    }

    // ------------------------------------------------------------------------------------------------------------
    // 1. Point lookups at snapshots through the data block hash index (content / nodeversion / docmetadata / data-*).

    [TestMethod]
    public void SnapshotGetsThroughHashIndexMatchNative()
    {
        int seed = Seed("AUDIT6_SEED");
        var rnd = new Random(seed);
        string ctx = "seed=" + seed;
        using var nd = new TempDir("a6-snap-n");
        using var md = new TempDir("a6-snap-m");
        using N n = OpenNative(nd.Path, Family.PointLookup, 1UL << 20, blockSize: 1024);
        using M m = OpenManaged(md.Path, Family.PointLookup, 1L << 20, blockSize: 1024);
        var nSnaps = new List<RocksDbSharp.Snapshot>();
        var mSnaps = new List<Snapshot>();
        var keys = new List<byte[]>();
        for (int i = 0; i < 300; i++)
        {
            // Mosaik-like keys: 16-byte UID128s, some sharing long prefixes, plus a few short ones.
            byte[] k = new byte[rnd.Next(10) == 0 ? 1 + rnd.Next(4) : 16];
            rnd.NextBytes(k);
            if (k.Length == 16 && i > 0 && rnd.Next(4) == 0 && keys[rnd.Next(keys.Count)] is { Length: 16 } src) src.AsSpan(0, 15).CopyTo(k);
            keys.Add(k);
        }
        // A few hot keys get many versions so that one user key spans restart intervals and data blocks.
        for (int round = 0; round < 12; round++)
        {
            for (int op = 0; op < 400; op++)
            {
                byte[] k = rnd.Next(3) == 0 ? keys[rnd.Next(5)] : keys[rnd.Next(keys.Count)];
                int r = rnd.Next(100);
                if (r < 85)
                {
                    byte[] v = new byte[rnd.Next(1, 120)];
                    rnd.NextBytes(v);
                    n.Db.Put(k, v);
                    m.Db.Put(k, v);
                }
                else
                {
                    n.Db.Remove(k);
                    m.Db.Delete(k);
                }
                if (rnd.Next(40) == 0)
                {
                    nSnaps.Add(n.Db.CreateSnapshot());
                    mSnaps.Add(m.Db.GetSnapshot());
                }
            }
            if (round % 3 != 2)
            {
                n.Db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
                m.Db.Flush();
            }
            if (round == 7)
            {
                unsafe { RocksDbSharp.Native.Instance.rocksdb_compact_range(n.Db.Handle, (byte*)null, UIntPtr.Zero, (byte*)null, UIntPtr.Zero); }
                m.Db.CompactRange(null, null);
            }
        }
        var probes = new List<byte[]>(keys);
        for (int i = 0; i < 200; i++)
        {
            byte[] k = new byte[16];
            rnd.NextBytes(k);
            probes.Add(k);
        }
        for (int s = -1; s < nSnaps.Count; s++)
        {
            var nro = new RocksDbSharp.ReadOptions();
            if (s >= 0) nro.SetSnapshot(nSnaps[s]);
            var mro = new ReadOptions { Snapshot = s >= 0 ? mSnaps[s] : null };
            foreach (byte[] k in probes)
            {
                byte[]? nv = n.Db.Get(k, null, nro);
                byte[]? mv = m.Db.DefaultColumnFamily.Get(mro, k);
                if (!(nv is null ? mv is null : mv is not null && nv.AsSpan().SequenceEqual(mv)))
                    Assert.Fail(ctx + " snapshot " + s + " key " + Hex(k) + ": native " + (nv is null ? "<null>" : Hex(nv)) + " managed " + (mv is null ? "<null>" : Hex(mv)));
                // RocksDbManagedCompat.Get(key, buffer, offset, length): TryGet into a span.
                byte[] buf = new byte[32];
                bool found = m.Db.DefaultColumnFamily.TryGet(mro, k, buf, out int len);
                Assert.AreEqual(nv is not null, found, ctx + " TryGet span");
                if (found) Assert.AreEqual(nv!.Length, len, ctx + " TryGet length");
            }
        }
        foreach (RocksDbSharp.Snapshot s in nSnaps) s.Dispose();
        foreach (Snapshot s in mSnaps) s.Dispose();
    }

    // ------------------------------------------------------------------------------------------------------------
    // 2. Random iterator programs with prefix_same_as_start / bounds, in both directions, over memtable + L0 + levels,
    //    including a SetCapacity shrink of the shared block cache in the middle (RocksDbBase.ShrinkSharedCacheCapacity).

    [TestMethod]
    [DataRow(Family.Prefix8Bloom)]
    [DataRow(Family.Prefix10Bloom)]
    [DataRow(Family.Prefix8NoFilter)]
    [DataRow(Family.PointLookup)]
    [DataRow(Family.Prefix8Bloom, 128)]
    [DataRow(Family.Prefix10Bloom, 128)]
    [DataRow(Family.PointLookup, 128)]
    public void RandomIteratorProgramsMatchNative(Family family, int metadataBlockSize = 0)
    {
        int seed = Seed("AUDIT6_SEED") ^ (int)family;
        var rnd = new Random(seed);
        string ctx = "family=" + family + " seed=" + seed;
        using var nd = new TempDir("a6-it-n");
        using var md = new TempDir("a6-it-m");
        using N n = OpenNative(nd.Path, family, 4UL << 20, blockSize: 512, metadataBlockSize: metadataBlockSize);
        using M m = OpenManaged(md.Path, family, 4L << 20, blockSize: 512, metadataBlockSize: metadataBlockSize);
        int plen = family == Family.Prefix10Bloom ? 10 : 8;
        var prefixes = new List<byte[]>();
        for (int i = 0; i < 40; i++)
        {
            byte[] p = new byte[plen];
            rnd.NextBytes(p);
            if (i > 0 && rnd.Next(3) == 0) prefixes[rnd.Next(prefixes.Count)].AsSpan(0, plen - 1).CopyTo(p); // neighbours
            if (i > 0 && rnd.Next(5) == 0)
            {
                // the same-length immediate successor of another prefix (IsFilterCompatible's second case)
                byte[] q = (byte[])prefixes[rnd.Next(prefixes.Count)].Clone();
                for (int j = q.Length - 1; j >= 0; j--) { if (++q[j] != 0) break; }
                p = q;
            }
            prefixes.Add(p);
        }
        byte[] Key()
        {
            byte[] p = prefixes[rnd.Next(prefixes.Count)];
            byte[] k = new byte[plen + rnd.Next(0, 3) * 4 + (rnd.Next(2) == 0 ? 0 : 4)];
            p.CopyTo(k, 0);
            rnd.NextBytes(k.AsSpan(plen));
            if (rnd.Next(4) == 0) k.AsSpan(plen).Clear();
            return k;
        }
        var written = new List<byte[]>();
        void Round(int ops)
        {
            for (int i = 0; i < ops; i++)
            {
                if (rnd.Next(100) < 80 || written.Count == 0)
                {
                    byte[] k = Key(), v = Encoding.ASCII.GetBytes("v" + rnd.Next());
                    written.Add(k);
                    n.Db.Put(k, v);
                    m.Db.Put(k, v);
                }
                else
                {
                    byte[] k = written[rnd.Next(written.Count)];
                    n.Db.Remove(k);
                    m.Db.Delete(k);
                }
            }
        }
        void Flush()
        {
            n.Db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            m.Db.Flush();
        }
        unsafe void Compact()
        {
            RocksDbSharp.Native.Instance.rocksdb_compact_range(n.Db.Handle, (byte*)null, UIntPtr.Zero, (byte*)null, UIntPtr.Zero);
            m.Db.CompactRange(null, null);
        }
        void Check(string where)
        {
            for (int g = 0; g < 600; g++)
            {
                byte[] k = g % 2 == 0 && written.Count > 0 ? written[rnd.Next(written.Count)] : Key();
                byte[]? nv = n.Db.Get(k);
                byte[]? mv = m.Db.Get(k);
                if (!(nv is null ? mv is null : mv is not null && nv.AsSpan().SequenceEqual(mv)))
                    Assert.Fail(ctx + " " + where + " Get " + Hex(k) + ": native " + (nv is null ? "<null>" : Hex(nv)) + " managed " + (mv is null ? "<null>" : Hex(mv)));
                Assert.AreEqual(nv is not null, m.Db.DefaultColumnFamily.TryGet(null, k, 0, static (_, _) => true, out _), ctx + " " + where + " HasKey");
            }
            for (int prog = 0; prog < 300; prog++)
            {
                byte[]? lower = null, upper = null;
                int b = rnd.Next(6);
                byte[] p = prefixes[rnd.Next(prefixes.Count)];
                if (b == 1 || b == 3) lower = p;
                if (b == 2 || b == 3)
                {
                    upper = (byte[])p.Clone();
                    for (int j = upper.Length - 1; j >= 0; j--) { if (++upper[j] != 0) break; }
                }
                if (b == 4) { upper = Key(); }
                bool psas = family != Family.PointLookup && rnd.Next(3) != 0;
                bool fill = rnd.Next(4) != 0;
                ulong readahead = rnd.Next(5) == 0 ? 1UL << 20 : 0;
                var nro = new RocksDbSharp.ReadOptions();
                if (lower is not null) nro.SetIterateLowerBound(lower);
                if (upper is not null) nro.SetIterateUpperBound(upper);
                if (psas) nro.SetPrefixSameAsStart(true);
                if (!fill) nro.SetFillCache(false);
                if (readahead != 0) nro.SetReadaheadSize(readahead);
                var mro = new ReadOptions
                {
                    IterateLowerBound = lower is null ? (ReadOnlyMemory<byte>?)null : lower,
                    IterateUpperBound = upper is null ? (ReadOnlyMemory<byte>?)null : upper,
                    PrefixSameAsStart = psas,
                    FillCache = fill,
                    ReadaheadSize = readahead,
                };
                using RocksDbSharp.Iterator ni = n.Db.NewIterator(null, nro);
                using Iterator mi = m.Db.NewIterator(mro);
                var trace = new StringBuilder();
                byte[]? seekPrefix = null;
                bool prefixMode = family != Family.PointLookup;
                for (int step = 0; step < 25; step++)
                {
                    int op = rnd.Next(8);
                    string opName;
                    if ((!ni.Valid() || !mi.Valid) && op >= 4) op = rnd.Next(4);
                    byte[] target = rnd.Next(3) == 0 && written.Count > 0 ? written[rnd.Next(written.Count)] : Key();
                    if (rnd.Next(6) == 0) target = target.AsSpan(0, plen).ToArray();
                    switch (op)
                    {
                        case 0: ni.Seek(target); mi.Seek(target); opName = "Seek " + Hex(target); seekPrefix = target; break;
                        case 1: ni.SeekForPrev(target); mi.SeekForPrev(target); opName = "SeekForPrev " + Hex(target); seekPrefix = target; break;
                        case 2: ni.SeekToFirst(); mi.SeekToFirst(); opName = "SeekToFirst"; seekPrefix = lower; break;
                        case 3: ni.SeekToLast(); mi.SeekToLast(); opName = "SeekToLast"; seekPrefix = upper; break;
                        case 4: case 5: ni.Next(); mi.Next(); opName = "Next"; break;
                        default: ni.Prev(); mi.Prev(); opName = "Prev"; break;
                    }
                    // Prefix seek mode without prefix_same_as_start: only keys with the seek key's prefix are defined.
                    bool project = prefixMode && !psas && seekPrefix is not null && seekPrefix.Length >= plen;
                    string R(bool valid, ReadOnlySpan<byte> k, ReadOnlySpan<byte> v) =>
                        !valid ? "<invalid>" : project && !k.StartsWith(seekPrefix.AsSpan(0, plen)) ? "<out of prefix>" : Hex(k) + "=" + Hex(v);
                    string nr = R(ni.Valid(), ni.Valid() ? ni.GetKeySpan() : default, ni.Valid() ? ni.GetValueSpan() : default);
                    string mr = R(mi.Valid, mi.Valid ? mi.Key : default, mi.Valid ? mi.Value : default);
                    if (project && (nr == "<invalid>" || mr == "<invalid>") && (nr == "<out of prefix>" || mr == "<out of prefix>")) mr = nr;
                    trace.Append(opName).Append(" -> ").Append(nr).Append('\n');
                    if (nr != mr)
                    {
                        Assert.Fail(ctx + " " + where + " lower=" + (lower is null ? "-" : Hex(lower)) + " upper=" + (upper is null ? "-" : Hex(upper))
                            + " psas=" + psas + " fill=" + fill + " readahead=" + readahead + "\n" + trace + "managed: " + mr);
                    }
                }
            }
        }
        Round(3000);
        Check("memtable");
        Flush();
        Check("one L0");
        for (int i = 0; i < 6; i++)
        {
            Round(800);
            Flush();
        }
        Check("seven L0");
        Compact();
        Check("compacted");
        Round(1500);
        Flush();
        Round(500);
        Check("memtable + L0 + bottom");
        // ShrinkSharedCacheCapacity / RestoreSharedCacheCapacity with pinned top levels and L0 partitions.
        n.Cache.SetCapacity(16 << 10);
        m.Cache.SetCapacity(16 << 10);
        Check("cache shrunk");
        n.Cache.SetCapacity(4UL << 20);
        m.Cache.SetCapacity(4L << 20);
        Check("cache restored");
    }

    // ------------------------------------------------------------------------------------------------------------
    // 3. A bounded scan (Mosaik's prefix scans between iterate_lower_bound / iterate_upper_bound) does not read the
    //    data block after the upper bound (BlockBasedTableIterator::FindBlockForward next_block_is_out_of_bound /
    //    CheckDataBlockWithinUpperBound): same data block reads as native, over an L0 file and over a compacted level
    //    (LevelIterator), with the sync and the async iterator paths.

    [TestMethod]
    [DataRow(false, false)]
    [DataRow(true, false)]
    [DataRow(false, true)]
    [DataRow(true, true)]
    public async Task BoundedScanDataBlockReadsMatchNative(bool compact, bool async)
    {
        using var nd = new TempDir("a6-ub-n");
        using var md = new TempDir("a6-ub-m");
        using N n = OpenNative(nd.Path, Family.Prefix8Bloom, 64UL << 20, blockSize: 256);
        using M m = OpenManaged(md.Path, Family.Prefix8Bloom, 64L << 20, blockSize: 256);
        // 64 prefixes x 50 keys, values of 40 bytes: many 256-byte data blocks per prefix.
        for (ulong p = 0; p < 64; p++)
        {
            for (int i = 0; i < 50; i++)
            {
                byte[] k = new byte[12];
                System.Buffers.Binary.BinaryPrimitives.WriteUInt64BigEndian(k, p * 1000);
                System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(k.AsSpan(8), i);
                byte[] v = Encoding.ASCII.GetBytes(new string('x', 40));
                n.Db.Put(k, v);
                m.Db.Put(k, v);
            }
        }
        n.Db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
        m.Db.Flush();
        if (compact)
        {
            unsafe { RocksDbSharp.Native.Instance.rocksdb_compact_range(n.Db.Handle, (byte*)null, UIntPtr.Zero, (byte*)null, UIntPtr.Zero); }
            m.Db.CompactRange(null, null);
        }
        ulong nBefore = TickerValue(n.Options.GetStatisticsString(), "rocksdb.block.cache.data.miss");
        ulong mBefore = m.Stats.GetTickerCount(Ticker.BlockCacheDataMiss);
        int nCount = 0, mCount = 0;
        for (ulong p = 0; p < 64; p += 2)
        {
            byte[] lo = new byte[8], hi = new byte[8];
            System.Buffers.Binary.BinaryPrimitives.WriteUInt64BigEndian(lo, p * 1000);
            System.Buffers.Binary.BinaryPrimitives.WriteUInt64BigEndian(hi, p * 1000 + 1);
            var nro = new RocksDbSharp.ReadOptions().SetIterateLowerBound(lo).SetIterateUpperBound(hi);
            using (RocksDbSharp.Iterator it = n.Db.NewIterator(null, nro))
            {
                for (it.SeekToFirst(); it.Valid(); it.Next()) nCount++;
            }
            using (Iterator it = m.Db.NewIterator(new ReadOptions { IterateLowerBound = lo, IterateUpperBound = hi }))
            {
                if (async)
                {
                    await it.SeekToFirstAsync();
                    while (it.Valid)
                    {
                        mCount++;
                        await it.NextAsync();
                    }
                }
                else
                {
                    for (it.SeekToFirst(); it.Valid; it.Next()) mCount++;
                }
            }
        }
        Assert.AreEqual(nCount, mCount, "rows");
        ulong nMiss = TickerValue(n.Options.GetStatisticsString(), "rocksdb.block.cache.data.miss") - nBefore;
        ulong mMiss = m.Stats.GetTickerCount(Ticker.BlockCacheDataMiss) - mBefore;
        Assert.AreEqual(nMiss, mMiss, "data blocks read by 32 bounded prefix scans (native vs managed)");
    }

    // ------------------------------------------------------------------------------------------------------------
    // 4. SeekToLast / SeekForPrev with iterate_upper_bound = the next prefix (Mosaik's prefix scans in reverse) on a
    //    prefix-extractor family, prefix_same_as_start off, total_order_seek off. With one L0 file and
    //    optimize_filters_for_hits, native still consults the prefix filter (L0 table iterators get skip_filters =
    //    false; Version::IsFilterSkipped with is_file_last_in_level is for Get / MultiGet only), finds no key with the
    //    next prefix and leaves the iterator invalid.

    [TestMethod]
    [DataRow(false, false)]
    [DataRow(true, false)]
    [DataRow(false, true)]
    [DataRow(true, true)]
    public void SeekToLastWithNextPrefixUpperBoundMatchesNative(bool withLower, bool flush)
    {
        using var nd = new TempDir("a6-stl-n");
        using var md = new TempDir("a6-stl-m");
        using N n = OpenNative(nd.Path, Family.Prefix8Bloom, 4UL << 20);
        using M m = OpenManaged(md.Path, Family.Prefix8Bloom, 4L << 20);
        byte[] p = [0, 0, 0, 0, 0, 0, 0, 5];
        byte[] next = [0, 0, 0, 0, 0, 0, 0, 6];
        foreach (byte b in new byte[] { 1, 2, 3 })
        {
            byte[] k = [.. p, b];
            n.Db.Put(k, k);
            m.Db.Put(k, k);
        }
        byte[] other = [0, 0, 0, 0, 0, 0, 0, 9, 1];
        n.Db.Put(other, other);
        m.Db.Put(other, other);
        if (flush)
        {
            n.Db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            m.Db.Flush();
        }
        var nro = new RocksDbSharp.ReadOptions().SetIterateUpperBound(next);
        if (withLower) nro.SetIterateLowerBound(p);
        using RocksDbSharp.Iterator ni = n.Db.NewIterator(null, nro);
        using Iterator mi = m.Db.NewIterator(new ReadOptions { IterateUpperBound = next, IterateLowerBound = withLower ? p : (ReadOnlyMemory<byte>?)null });
        ni.SeekToLast();
        mi.SeekToLast();
        string nr = ni.Valid() ? Hex(ni.GetKeySpan()) : "<invalid>";
        string mr = mi.Valid ? Hex(mi.Key) : "<invalid>";
        ni.SeekForPrev(next);
        mi.SeekForPrev(next);
        nr += " / " + (ni.Valid() ? Hex(ni.GetKeySpan()) : "<invalid>");
        mr += " / " + (mi.Valid ? Hex(mi.Key) : "<invalid>");
        Assert.AreEqual(nr, mr, "SeekToLast / SeekForPrev(next prefix): native vs managed");
    }
}
