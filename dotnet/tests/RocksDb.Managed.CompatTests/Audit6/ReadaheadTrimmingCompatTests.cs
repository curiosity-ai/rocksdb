using System.Buffers.Binary;
using System.Globalization;
using System.Text.RegularExpressions;
using RocksDb.Managed.Cache;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Table.Filters;

namespace RocksDb.Managed.CompatTests.Audit6;

/// <summary>
/// ReadOptions::auto_readahead_size (include/rocksdb/options.h): with a block cache and iterate_upper_bound or
/// prefix_same_as_start, a forward scan trims its readahead to the bound / the seek key's prefix and skips blocks that
/// are already cached (BlockBasedTableIterator::BlockCacheLookupForReadAheadSize, FilePrefetchBuffer::ReadAheadSizeTuning).
/// Managed vs native on Mosaik's table options (shared LRU block cache, partitioned index and filters, fixed 8-byte
/// prefix extractor) and its scans (RocksDbDataStorage: iterate bounds + readahead_size = 16 MiB): the file bytes read
/// (rocksdb.{non.,}last.level.read.bytes) and the trimmed-readahead count must match native closely.
/// </summary>
[TestClass]
public sealed class ReadaheadTrimmingCompatTests
{
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    public enum Scan
    {
        /// <summary>[prefix, next prefix) bounds and an explicit 16 MiB readahead (Mosaik's RocksDbDataStorage scans).</summary>
        BoundedReadahead,
        /// <summary>prefix_same_as_start, no upper bound, 16 MiB readahead.</summary>
        PrefixSameAsStartReadahead,
        /// <summary>Bounds, 16 MiB readahead and async_io (two buffers, the second prefetched).</summary>
        BoundedReadaheadAsyncIo,
        /// <summary>Bounds with auto_readahead_size = false: nothing is trimmed.</summary>
        BoundedReadaheadNoAutoSize,
        /// <summary>Bounds, implicit auto readahead (OS prefetch, no buffer): nothing to trim on either side.</summary>
        BoundedImplicit,
    }

    private const int Prefixes = 96;
    private const int KeysPerPrefix = 300;
    private const ulong Readahead = 16UL << 20;

    private static ulong TickerValue(string statistics, string name)
    {
        Match m = Regex.Match(statistics, "^" + Regex.Escape(name) + " COUNT : (\\d+)$", RegexOptions.Multiline);
        Assert.IsTrue(m.Success, name);
        return ulong.Parse(m.Groups[1].Value, CultureInfo.InvariantCulture);
    }

    private readonly record struct Counters(ulong Bytes, ulong Trimmed, ulong DataMiss, ulong DataHit);

    private static Counters Native(RocksDbSharp.DbOptions o)
    {
        string s = o.GetStatisticsString();
        return new Counters(
            TickerValue(s, "rocksdb.non.last.level.read.bytes") + TickerValue(s, "rocksdb.last.level.read.bytes"),
            TickerValue(s, "rocksdb.readahead.trimmed"),
            TickerValue(s, "rocksdb.block.cache.data.miss"),
            TickerValue(s, "rocksdb.block.cache.data.hit"));
    }

    private static Counters Managed(Statistics st) => new(
        st.GetTickerCount(Ticker.NonLastLevelReadBytes) + st.GetTickerCount(Ticker.LastLevelReadBytes),
        st.GetTickerCount(Ticker.ReadaheadTrimmed),
        st.GetTickerCount(Ticker.BlockCacheDataMiss),
        st.GetTickerCount(Ticker.BlockCacheDataHit));

    private static Counters Delta(Counters a, Counters b) => new(b.Bytes - a.Bytes, b.Trimmed - a.Trimmed, b.DataMiss - a.DataMiss, b.DataHit - a.DataHit);

    private static byte[] Prefix(int p)
    {
        byte[] k = new byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(k, (ulong)p * 1000);
        return k;
    }

    [TestMethod]
    [DataRow(Scan.BoundedReadahead, false)]
    [DataRow(Scan.BoundedReadahead, true)]
    [DataRow(Scan.PrefixSameAsStartReadahead, false)]
    [DataRow(Scan.PrefixSameAsStartReadahead, true)]
    [DataRow(Scan.BoundedReadaheadAsyncIo, true)]
    [DataRow(Scan.BoundedReadaheadNoAutoSize, true)]
    [DataRow(Scan.BoundedImplicit, true)]
    public void LongScansReadTheFileBytesNativeReads(Scan scan, bool compact)
    {
        using var nd = new TempDir("ra-trim-n");
        using var md = new TempDir("ra-trim-m");

        // Native: RocksDbBase.CreateTableOptions shape.
        RocksDbSharp.Native n = RocksDbSharp.Native.Instance;
        RocksDbSharp.Cache ncache = RocksDbSharp.Cache.CreateLru(256UL << 20);
        var t = new RocksDbSharp.BlockBasedTableOptions().SetBlockCache(ncache);
        n.rocksdb_block_based_options_set_cache_index_and_filter_blocks(t.Handle, RocksDbSharp.Native.MarshalBool(true));
        n.rocksdb_block_based_options_set_cache_index_and_filter_blocks_with_high_priority(t.Handle, RocksDbSharp.Native.MarshalBool(true));
        n.rocksdb_block_based_options_set_pin_l0_filter_and_index_blocks_in_cache(t.Handle, RocksDbSharp.Native.MarshalBool(true));
        n.rocksdb_block_based_options_set_index_type(t.Handle, RocksDbSharp.BlockBasedTableIndexType.TwoLevelIndex);
        n.rocksdb_block_based_options_set_partition_filters(t.Handle, RocksDbSharp.Native.MarshalBool(true));
        n.rocksdb_block_based_options_set_pin_top_level_index_and_filter(t.Handle, RocksDbSharp.Native.MarshalBool(true));
        t.SetFilterPolicy(RocksDbSharp.BloomFilterPolicy.Create(10, false));
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetDisableAutoCompactions(1);
        o.EnableStatistics();
        o.SetBlockBasedTableFactory(t);
        o.SetPrefixExtractor(RocksDbSharp.SliceTransform.CreateFixedPrefix(8)).SetOptimizeFiltersForHits(1);
        s_keepAlive.Add(ncache);
        s_keepAlive.Add(t);
        s_keepAlive.Add(o);
        using RocksDbSharp.RocksDb ndb = RocksDbSharp.RocksDb.Open(o, nd.Path);

        var cfo = new ColumnFamilyOptions();
        BlockBasedTableOptions mt = cfo.TableOptions;
        mt.BlockCache = new LruCache(new LruCacheOptions { Capacity = 256L << 20 });
        mt.CacheIndexAndFilterBlocks = true;
        mt.CacheIndexAndFilterBlocksWithHighPriority = true;
        mt.PinL0FilterAndIndexBlocksInCache = true;
        mt.IndexType = IndexType.TwoLevelIndexSearch;
        mt.PartitionFilters = true;
        mt.PinTopLevelIndexAndFilter = true;
        mt.FilterPolicy = new BloomFilterPolicy(10);
        cfo.DisableAutoCompactions = true;
        cfo.PrefixExtractor = SliceTransform.CreateFixedPrefix(8);
        cfo.OptimizeFiltersForHits = true;
        var stats = new Statistics();
        using RocksDatabase mdb = RocksDatabase.Open(md.Path, new DbOptions { CreateIfMissing = true, Statistics = stats }, cfo);

        // 96 prefixes x 300 keys of 12 bytes, 100-byte values (incompressible): ~3.4 MB, ~25 data blocks per prefix.
        var rnd = new Random(42);
        byte[] v = new byte[100];
        for (int p = 0; p < Prefixes; p++)
        {
            for (int i = 0; i < KeysPerPrefix; i++)
            {
                byte[] k = new byte[12];
                Prefix(p).CopyTo(k, 0);
                BinaryPrimitives.WriteInt32BigEndian(k.AsSpan(8), i);
                rnd.NextBytes(v);
                ndb.Put(k, v);
                mdb.Put(k, v);
            }
        }
        ndb.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
        mdb.Flush();
        if (compact)
        {
            unsafe { RocksDbSharp.Native.Instance.rocksdb_compact_range(ndb.Handle, (byte*)null, UIntPtr.Zero, (byte*)null, UIntPtr.Zero); }
            mdb.CompactRange(null, null);
        }

        // Pass 1 scans every other prefix from a cold data block cache; pass 2 scans every prefix, half of them cached.
        for (int pass = 1; pass <= 2; pass++)
        {
            Counters n0 = Native(o), m0 = Managed(stats);
            long nRows = 0, mRows = 0;
            int scans = 0;
            for (int p = 0; p < Prefixes; p += pass == 1 ? 2 : 1)
            {
                scans++;
                byte[] lo = Prefix(p), hi = Prefix(p + 1);
                bool bounded = scan != Scan.PrefixSameAsStartReadahead;
                var nro = new RocksDbSharp.ReadOptions();
                var mro = new ReadOptions();
                if (bounded)
                {
                    nro.SetIterateLowerBound(lo).SetIterateUpperBound(hi);
                    mro.IterateLowerBound = lo;
                    mro.IterateUpperBound = hi;
                }
                else
                {
                    nro.SetPrefixSameAsStart(true);
                    mro.PrefixSameAsStart = true;
                }
                if (scan != Scan.BoundedImplicit)
                {
                    nro.SetReadaheadSize(Readahead);
                    mro.ReadaheadSize = Readahead;
                }
                if (scan == Scan.BoundedReadaheadAsyncIo)
                {
                    nro.SetAsyncIO(true);
                    mro.AsyncIo = true;
                }
                if (scan == Scan.BoundedReadaheadNoAutoSize)
                {
                    nro.SetAutoReadaheadSize(false);
                    mro.AutoReadaheadSize = false;
                }
                using (RocksDbSharp.Iterator it = ndb.NewIterator(null, nro))
                {
                    for (it.Seek(lo); it.Valid(); it.Next()) nRows++;
                }
                using (Iterator it = mdb.NewIterator(mro))
                {
                    for (it.Seek(lo); it.Valid; it.Next()) mRows++;
                }
            }
            Counters nd1 = Delta(n0, Native(o)), md1 = Delta(m0, Managed(stats));
            string ctx = scan + " compact=" + compact + " pass " + pass + ": native bytes=" + nd1.Bytes + " trimmed=" + nd1.Trimmed + " miss=" + nd1.DataMiss
                + " hit=" + nd1.DataHit + " | managed bytes=" + md1.Bytes + " trimmed=" + md1.Trimmed + " miss=" + md1.DataMiss + " hit=" + md1.DataHit;
            Console.WriteLine(ctx);
            Assert.AreEqual(nRows, mRows, ctx + " (rows)");
            Assert.AreEqual(nd1.DataMiss, md1.DataMiss, ctx + " (data block misses)");
            Assert.AreEqual(nd1.DataHit, md1.DataHit, ctx + " (data block hits)");
            // Native turns async_io off when its file system does not support async reads (ArenaWrappedDBIter::Init,
            // kAsyncIO): its numbers are then the synchronous buffer's, whose second-buffer prefetch is never trimmed.
            if (scan != Scan.BoundedReadaheadAsyncIo) Assert.AreEqual(nd1.Trimmed, md1.Trimmed, ctx + " (readahead trimmed)");
            // Native aligns each readahead window to the file's GetRequiredBufferAlignment() (4 KiB for a POSIX file,
            // also without direct I/O); the managed buffer reads the window as is: at most two pages less per scan.
            Assert.IsTrue(md1.Bytes <= nd1.Bytes && nd1.Bytes - md1.Bytes <= 8192UL * (ulong)scans, ctx + " (file bytes read)");
        }
    }
}
