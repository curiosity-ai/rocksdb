using System.Buffers.Binary;
using System.Runtime.InteropServices;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;
using RocksDb.Managed.Table.Filters;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>
/// A database shaped like the one Mosaik (the main downstream user) opens through rocksdb-sharp
/// (Mosaik/Graph/src/Graph/Storage: DBOptionsExt.SetRockDbDefaultOptions, RocksDbBase.CreateColumnFamilyOptions /
/// CreatePointLookupColumnFamilyOptions / CreateTableOptions(WithBloom), RocksDbStorage's family map and
/// SafeRocksDb's change-tracker family with the custom ulong comparator), configured with the exact option values
/// on each engine, written by one and read, extended, flushed and compacted by the other. Covers the custom
/// comparator (name persisted and checked, order honoured), fixed prefix extractors with bloom filters (whole-key
/// and prefix-only, partitioned, optimize_filters_for_hits), two-level indexes, the data block hash index, LZ4,
/// the table properties each engine writes for these options, the OPTIONS file read by the other engine, and
/// reopening with Mosaik's family set.
/// </summary>
[TestClass]
public sealed class MosaikShapedDbCompatTests
{
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    private const string TrackerFamily = "replica-change-tracker";
    private const string ComparatorName = "Mosaik.GraphDB.ulong_Comparator";

    private enum Shape
    {
        /// <summary>CreateColumnFamilyOptions: shared cache, partitioned two-level index, no filter.</summary>
        Plain,
        /// <summary>CreatePointLookupColumnFamilyOptions: + bloom 10, data block hash index, memtable whole-key bloom.</summary>
        PointLookup,
        /// <summary>"content": PointLookup + LZ4 + arena block size.</summary>
        Content,
        /// <summary>CreateColumnFamilyOptions + fixed 8-byte prefix (data-vectors), no filter.</summary>
        Prefix8NoFilter,
        /// <summary>Prefix + bloom (whole-key filtering on) + optimize_filters_for_hits (the index families).</summary>
        PrefixBloom,
        /// <summary>SafeRocksDb change tracker: ulong comparator, 16-byte prefix, prefix-only bloom, optimize_filters_for_hits.</summary>
        Tracker,
    }

    private sealed record Family(string Name, Shape Shape, int PrefixLength);

    // RocksDbStorage's family map (the "default" family is opened with default options, then the tracker is appended).
    private static readonly Family[] s_families =
    [
        new("nodeschemas", Shape.Plain, 0),
        new("edgeschemas", Shape.Plain, 0),
        new("types", Shape.Plain, 0),
        new("nodes", Shape.Plain, 0),
        new("edges", Shape.Plain, 0),
        new("content", Shape.Content, 0),
        new("changelog", Shape.Plain, 0),
        new("changeduidslog", Shape.Plain, 0),
        new("nodeversion", Shape.PointLookup, 0),
        new("graph-config", Shape.Plain, 0),
        new("docmetadata", Shape.PointLookup, 0),
        new("blob-length", Shape.Plain, 0),
        new("blob-pages", Shape.Plain, 0),
        new("data-paths", Shape.PointLookup, 0),
        new("data-length", Shape.PointLookup, 0),
        new("data-pages", Shape.PointLookup, 0),
        new("data-vectors", Shape.Prefix8NoFilter, 8),
        new("data-vectors-hashes", Shape.Prefix8NoFilter, 8),
        new("lucene-file-length", Shape.Plain, 0),
        new("lucene-file-size", Shape.Plain, 0),
        new("lucene-file-pages-count", Shape.Plain, 0),
        new("lucene-file-pages", Shape.Plain, 0),
        new("simple-text-index-keys", Shape.PrefixBloom, 8),
        new("simple-text-index-entries", Shape.PrefixBloom, 8),
        new("file-extraction-queue", Shape.Plain, 0),
        new("file-extraction-queue-stats", Shape.PrefixBloom, 10),
        new("command-score-index-entries", Shape.PrefixBloom, 8),
        new("to-index-queue", Shape.PrefixBloom, 8),
        new("deletion-queue", Shape.Plain, 0),
        new(TrackerFamily, Shape.Tracker, 16),
    ];

    // ---- the ulong comparator, on both engines ----------------------------------------------------------------

    /// <summary>Mosaik's ulong_Comparator: the first 8 bytes as a little-endian ulong, the rest ignored.</summary>
    private sealed class ManagedULongComparator : Comparator
    {
        public static readonly ManagedULongComparator Instance = new();
        public override string Name => ComparatorName;
        public override int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) =>
            BinaryPrimitives.ReadUInt64LittleEndian(a).CompareTo(BinaryPrimitives.ReadUInt64LittleEndian(b));
    }

    private sealed unsafe class NativeULongComparator : RocksDbSharp.Comparator
    {
        public static readonly NativeULongComparator Instance = new();
        public string Name => ComparatorName;
        public int Compare(IntPtr a, UIntPtr alen, IntPtr b, UIntPtr blen) =>
            BinaryPrimitives.ReadUInt64LittleEndian(new ReadOnlySpan<byte>((void*)a, (int)alen))
                .CompareTo(BinaryPrimitives.ReadUInt64LittleEndian(new ReadOnlySpan<byte>((void*)b, (int)blen)));
    }

    // ---- Mosaik's option values -------------------------------------------------------------------------------

    private const ulong CacheSize = 64UL << 20;
    private const ulong WriteBufferManagerSize = 32UL << 20;
    private const int Parallelism = 4; // procCount in SetRockDbDefaultOptions (not the low-resource branch)

    /// <summary>RocksDbBase.WAL_ARCHIVE_TTL_NEVER_SECONDS stand-in: a ttl no file reaches.</summary>
    private const ulong WalTtl = 100UL * 365 * 24 * 3600;

    /// <summary>The native configuration, call for call as Mosaik makes it (DBOptionsExt + RocksDbBase + SafeRocksDb).</summary>
    private sealed class NativeConfig
    {
        public readonly RocksDbSharp.DbOptions Db;
        public readonly RocksDbSharp.ColumnFamilies Families = new();
        public readonly IntPtr Wbm;
        private readonly RocksDbSharp.Cache _cache = RocksDbSharp.Cache.CreateLru(CacheSize);

        public NativeConfig(string walDir, IEnumerable<Family> families, ulong writeBufferManagerSize)
        {
            RocksDbSharp.Native n = RocksDbSharp.Native.Instance;
            if (writeBufferManagerSize > 0) Wbm = n.rocksdb_write_buffer_manager_create((UIntPtr)writeBufferManagerSize, 0);
            var o = new RocksDbSharp.DbOptions();
            o.SetCreateIfMissing(true);
            o.SetCreateMissingColumnFamilies(true);
            o.SetWalDir(walDir);
            o.EnableStatistics();
            if (writeBufferManagerSize > 0) n.rocksdb_options_set_write_buffer_manager(o.Handle, Wbm);
            o.IncreaseParallelism(Math.Min(8, Math.Max(2, Parallelism / 2)));
            o.SetDeleteObsoleteFilesPeriodMicros(1_000_000ul * 3600);
            o.SetInfoLogLevel(RocksDbSharp.InfoLogLevel.Info);
            n.rocksdb_options_set_max_subcompactions(o.Handle, (uint)Math.Max(2, Parallelism / 4));
            n.rocksdb_options_set_max_background_jobs(o.Handle, Math.Max(2, Parallelism / 2));
            n.rocksdb_options_set_dump_malloc_stats(o.Handle, 1);
            o.SetBytesPerSync(1_048_576);
            n.rocksdb_options_set_wal_bytes_per_sync(o.Handle, 1048576);
            o.SetMaxFileOpeningThreads(Math.Max(2, Parallelism / 2));
            o.SetWalTtlSeconds(WalTtl);
            o.SetWalSizeLimitMB(0);
            o.SetMaxLogFileSize(64UL * 1024 * 1024);
            o.SetKeepLogFileNum(10);
            o.SetMaxTotalWalSize(1024UL * 1024 * 1024);
            o.SkipStatsUpdateOnOpen(true);
            n.rocksdb_options_set_max_file_opening_threads(o.Handle, 16);
            o.SetWalRecoveryMode(RocksDbSharp.Recovery.SkipAnyCorruptedRecords);
            Db = o;
            foreach (Family f in families) Families.Add(f.Name, Create(f));
            s_keepAlive.Add(this);
        }

        private RocksDbSharp.BlockBasedTableOptions Table(bool bloom)
        {
            RocksDbSharp.Native n = RocksDbSharp.Native.Instance;
            var t = new RocksDbSharp.BlockBasedTableOptions().SetBlockCache(_cache.Handle);
            n.rocksdb_block_based_options_set_cache_index_and_filter_blocks(t.Handle, 1);
            n.rocksdb_block_based_options_set_cache_index_and_filter_blocks_with_high_priority(t.Handle, 1);
            n.rocksdb_block_based_options_set_pin_l0_filter_and_index_blocks_in_cache(t.Handle, 1);
            n.rocksdb_block_based_options_set_index_type(t.Handle, RocksDbSharp.BlockBasedTableIndexType.TwoLevelIndex);
            n.rocksdb_block_based_options_set_partition_filters(t.Handle, 1);
            n.rocksdb_block_based_options_set_pin_top_level_index_and_filter(t.Handle, 1);
            if (bloom) t.SetFilterPolicy(RocksDbSharp.BloomFilterPolicy.Create(10, false));
            s_keepAlive.Add(t);
            return t;
        }

        private RocksDbSharp.ColumnFamilyOptions Create(Family f)
        {
            RocksDbSharp.Native n = RocksDbSharp.Native.Instance;
            var cf = new RocksDbSharp.ColumnFamilyOptions();
            switch (f.Shape)
            {
                case Shape.Plain:
                    cf.SetBlockBasedTableFactory(Table(false));
                    break;
                case Shape.PointLookup:
                case Shape.Content:
                {
                    RocksDbSharp.BlockBasedTableOptions t = Table(true);
                    n.rocksdb_block_based_options_set_data_block_index_type(t.Handle, (int)RocksDbSharp.BlockBasedTableDataBlockIndexType.BinarySearchAndHash);
                    n.rocksdb_block_based_options_set_data_block_hash_table_util_ratio(t.Handle, 0.75);
                    cf.SetBlockBasedTableFactory(t).SetMemtablePrefixBloomSizeRatio(0.02);
                    n.rocksdb_options_set_memtable_whole_key_filtering(cf.Handle, 1);
                    if (f.Shape == Shape.Content) cf.SetCompression(RocksDbSharp.Compression.Lz4).SetArenaBlockSize(128 * 1024 * 1024);
                    break;
                }
                case Shape.Prefix8NoFilter:
                    cf.SetBlockBasedTableFactory(Table(false)).SetPrefixExtractor(RocksDbSharp.SliceTransform.CreateFixedPrefix((ulong)f.PrefixLength));
                    break;
                case Shape.PrefixBloom:
                    cf.SetBlockBasedTableFactory(Table(false)).SetPrefixExtractor(RocksDbSharp.SliceTransform.CreateFixedPrefix((ulong)f.PrefixLength))
                        .SetBlockBasedTableFactory(Table(true)).SetOptimizeFiltersForHits(1);
                    break;
                case Shape.Tracker:
                    cf.SetBlockBasedTableFactory(Table(false));
                    cf.SetPrefixExtractor(RocksDbSharp.SliceTransform.CreateFixedPrefix((ulong)f.PrefixLength))
                        .SetBlockBasedTableFactory(Table(true).SetWholeKeyFiltering(false)).SetOptimizeFiltersForHits(1);
                    cf.SetComparator(NativeULongComparator.Instance);
                    break;
            }
            s_keepAlive.Add(cf);
            return cf;
        }
    }

    private sealed class ManagedConfig
    {
        public readonly DbOptions Db;
        public readonly List<ColumnFamilyDescriptor> Families = [];
        private readonly Cache.LruCache _cache = new(new Cache.LruCacheOptions { Capacity = (long)CacheSize });

        public ManagedConfig(string walDir, IEnumerable<Family> families, ulong writeBufferManagerSize)
        {
            Db = new DbOptions
            {
                CreateIfMissing = true,
                CreateMissingColumnFamilies = true,
                WalDir = walDir,
                Statistics = new Statistics(),
                WriteBufferManager = writeBufferManagerSize > 0 ? new WriteBufferManager((long)writeBufferManagerSize, null, allowStall: false) : null,
                MaxBackgroundJobs = Math.Max(2, Parallelism / 2),
                DeleteObsoleteFilesPeriodMicros = 1_000_000ul * 3600,
                InfoLogLevel = InfoLogLevel.Info,
                MaxSubcompactions = (uint)Math.Max(2, Parallelism / 4),
                DumpMallocStats = true,
                BytesPerSync = 1_048_576,
                WalBytesPerSync = 1048576,
                MaxFileOpeningThreads = 16,
                WalTtlSeconds = WalTtl,
                WalSizeLimitMb = 0,
                MaxLogFileSize = 64UL * 1024 * 1024,
                KeepLogFileNum = 10,
                MaxTotalWalSize = 1024UL * 1024 * 1024,
                SkipStatsUpdateOnDbOpen = true,
                WalRecoveryMode = WalRecoveryMode.SkipAnyCorruptedRecords,
            };
            Families.Add(new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions()));
            foreach (Family f in families) Families.Add(new ColumnFamilyDescriptor(f.Name, Create(f)));
        }

        private BlockBasedTableOptions Table(bool bloom) => new()
        {
            BlockCache = _cache,
            CacheIndexAndFilterBlocks = true,
            CacheIndexAndFilterBlocksWithHighPriority = true,
            PinL0FilterAndIndexBlocksInCache = true,
            IndexType = IndexType.TwoLevelIndexSearch,
            PartitionFilters = true,
            PinTopLevelIndexAndFilter = true,
            FilterPolicy = bloom ? new BloomFilterPolicy(10) : null,
        };

        private ColumnFamilyOptions Create(Family f)
        {
            var cf = new ColumnFamilyOptions();
            switch (f.Shape)
            {
                case Shape.Plain:
                    cf.TableOptions = Table(false);
                    break;
                case Shape.PointLookup:
                case Shape.Content:
                {
                    BlockBasedTableOptions t = Table(true);
                    t.DataBlockIndexType = DataBlockIndexType.BinaryAndHash;
                    t.DataBlockHashTableUtilRatio = 0.75;
                    cf.TableOptions = t;
                    cf.MemtablePrefixBloomSizeRatio = 0.02;
                    cf.MemtableWholeKeyFiltering = true;
                    if (f.Shape == Shape.Content)
                    {
                        cf.Compression = CompressionType.LZ4;
                        cf.ArenaBlockSize = 128 * 1024 * 1024;
                    }
                    break;
                }
                case Shape.Prefix8NoFilter:
                    cf.TableOptions = Table(false);
                    cf.PrefixExtractor = SliceTransform.CreateFixedPrefix(f.PrefixLength);
                    break;
                case Shape.PrefixBloom:
                    cf.TableOptions = Table(true);
                    cf.PrefixExtractor = SliceTransform.CreateFixedPrefix(f.PrefixLength);
                    cf.OptimizeFiltersForHits = true;
                    break;
                case Shape.Tracker:
                {
                    BlockBasedTableOptions t = Table(true);
                    t.WholeKeyFiltering = false;
                    cf.TableOptions = t;
                    cf.PrefixExtractor = SliceTransform.CreateFixedPrefix(f.PrefixLength);
                    cf.OptimizeFiltersForHits = true;
                    cf.Comparator = ManagedULongComparator.Instance;
                    break;
                }
            }
            return cf;
        }
    }

    // ---- data -------------------------------------------------------------------------------------------------

    /// <summary>
    /// The keys of one round for a family: 16-byte UIDs for plain/point families, prefix + suffix for prefix families
    /// (several keys per prefix), and 8-byte little-endian versions for the tracker (crossing byte boundaries so that
    /// bytewise and numeric order differ).
    /// </summary>
    private static List<byte[]> Keys(Family f, int round)
    {
        var keys = new List<byte[]>();
        var rnd = new Random(f.Name.Length * 7919 + round);
        switch (f.Shape)
        {
            case Shape.Tracker:
                for (ulong v = (ulong)round * 400 + 1; v <= (ulong)round * 400 + 400; v++)
                {
                    var k = new byte[8];
                    BinaryPrimitives.WriteUInt64LittleEndian(k, v);
                    keys.Add(k);
                }
                break;
            case Shape.Prefix8NoFilter:
            case Shape.PrefixBloom:
                for (int p = 0; p < 40; p++)
                {
                    var prefix = new byte[f.PrefixLength];
                    rnd.NextBytes(prefix);
                    prefix[0] = (byte)(round * 40 + p); // distinct prefixes across rounds
                    for (int s = 0; s < 1 + p % 7; s++)
                    {
                        var k = new byte[f.PrefixLength + 4 + s % 3];
                        prefix.CopyTo(k, 0);
                        rnd.NextBytes(k.AsSpan(f.PrefixLength));
                        keys.Add(k);
                    }
                }
                break;
            default:
                for (int i = 0; i < 300; i++)
                {
                    var k = new byte[16];
                    rnd.NextBytes(k);
                    keys.Add(k);
                }
                break;
        }
        return keys;
    }

    private static byte[] Value(byte[] key, int round)
    {
        var v = new byte[24 + key[^1] % 40];
        for (int i = 0; i < v.Length; i++) v[i] = (byte)(key[i % key.Length] ^ (i * 31) ^ round);
        return v;
    }

    /// <summary>Absent keys: the tracker's next versions, prefix families' unknown prefixes and known prefixes with new suffixes.</summary>
    private static List<byte[]> AbsentKeys(Family f, List<KeyValuePair<byte[], byte[]>> present)
    {
        var result = new List<byte[]>();
        var rnd = new Random(f.Name.Length);
        if (f.Shape == Shape.Tracker)
        {
            for (ulong v = 5000; v < 5050; v++)
            {
                var k = new byte[8];
                BinaryPrimitives.WriteUInt64LittleEndian(k, v);
                result.Add(k);
            }
            return result;
        }
        for (int i = 0; i < 50; i++)
        {
            byte[] k = (byte[])present[i * 7 % present.Count].Key.Clone();
            if (f.PrefixLength > 0 && i % 2 == 0) k[0] = (byte)(0xF0 + i % 16); // unknown prefix
            rnd.NextBytes(k.AsSpan(Math.Max(f.PrefixLength, k.Length - 4)));
            if (!present.Exists(p => p.Key.AsSpan().SequenceEqual(k))) result.Add(k);
        }
        return result;
    }

    /// <summary>The expected contents of a family, sorted in the family's order.</summary>
    private static List<KeyValuePair<byte[], byte[]>> Expected(Family f, int rounds)
    {
        var all = new List<KeyValuePair<byte[], byte[]>>();
        for (int r = 0; r < rounds; r++)
        {
            foreach (byte[] k in Keys(f, r)) all.Add(new(k, Value(k, r)));
        }
        if (f.Shape == Shape.Tracker) all.Sort(static (a, b) => ManagedULongComparator.Instance.Compare(a.Key, b.Key));
        else all.Sort(static (a, b) => a.Key.AsSpan().SequenceCompareTo(b.Key));
        return all;
    }

    // ---- engine drivers ---------------------------------------------------------------------------------------

    private static RocksDbSharp.RocksDb OpenNative(string path, string walDir, IEnumerable<Family> families)
    {
        var c = new NativeConfig(walDir, families, WriteBufferManagerSize);
        return RocksDbSharp.RocksDb.Open(c.Db, path, c.Families);
    }

    private static RocksDatabase OpenManaged(string path, string walDir, IEnumerable<Family> families)
    {
        var c = new ManagedConfig(walDir, families, WriteBufferManagerSize);
        return RocksDatabase.Open(path, c.Db, c.Families);
    }

    private static void NativeWrite(RocksDbSharp.RocksDb db, Family f, int round)
    {
        RocksDbSharp.ColumnFamilyHandle cf = db.GetColumnFamily(f.Name);
        foreach (byte[] k in Keys(f, round)) db.Put(k, Value(k, round), cf);
    }

    private static void NativeFlush(RocksDbSharp.RocksDb db, Family f)
    {
        RocksDbSharp.Native n = RocksDbSharp.Native.Instance;
        IntPtr fo = n.rocksdb_flushoptions_create();
        n.rocksdb_flushoptions_set_wait(fo, 1);
        n.rocksdb_flush_cf(db.Handle, fo, db.GetColumnFamily(f.Name).Handle);
        n.rocksdb_flushoptions_destroy(fo);
    }

    private static void NativeVerify(RocksDbSharp.RocksDb db, Family f, int rounds)
    {
        RocksDbSharp.ColumnFamilyHandle cf = db.GetColumnFamily(f.Name);
        List<KeyValuePair<byte[], byte[]>> expected = Expected(f, rounds);
        foreach (KeyValuePair<byte[], byte[]> kv in expected)
        {
            CollectionAssert.AreEqual(kv.Value, db.Get(kv.Key, cf), f.Name + ": native Get");
        }
        foreach (byte[] k in AbsentKeys(f, expected)) Assert.IsNull(db.Get(k, cf), f.Name + ": native Get absent");
        var got = new List<byte[]>();
        using (RocksDbSharp.Iterator it = db.NewIterator(cf, new RocksDbSharp.ReadOptions().SetTotalOrderSeek(true)))
        {
            for (it.SeekToFirst(); it.Valid(); it.Next()) got.Add(it.Key());
        }
        AssertKeys(expected, got, f.Name + ": native scan");
        if (f.Shape == Shape.Tracker)
        {
            // RocksDbBase.HandleChangesFromPrimary: SeekToFirst with an 8-byte lower bound, default read options.
            byte[] lower = new byte[8];
            BinaryPrimitives.WriteUInt64LittleEndian(lower, 255);
            var bounded = new List<byte[]>();
            using RocksDbSharp.Iterator it = db.NewIterator(cf, new RocksDbSharp.ReadOptions().SetIterateLowerBound(lower));
            for (it.SeekToFirst(); it.Valid(); it.Next()) bounded.Add(it.Key());
            AssertKeys(expected.FindAll(kv => BinaryPrimitives.ReadUInt64LittleEndian(kv.Key) >= 255), bounded, f.Name + ": native tracker scan");
        }
        else if (f.PrefixLength > 0)
        {
            // RocksDbIndexStorage: prefix_same_as_start seeks.
            foreach (IGrouping<string, KeyValuePair<byte[], byte[]>> g in expected.GroupBy(kv => Convert.ToHexString(kv.Key, 0, f.PrefixLength)))
            {
                var inPrefix = new List<byte[]>();
                using RocksDbSharp.Iterator it = db.NewIterator(cf, new RocksDbSharp.ReadOptions().SetPrefixSameAsStart(true));
                for (it.Seek(Convert.FromHexString(g.Key)); it.Valid(); it.Next()) inPrefix.Add(it.Key());
                AssertKeys([.. g], inPrefix, f.Name + ": native prefix scan " + g.Key);
            }
        }
    }

    private static void ManagedWrite(RocksDatabase db, Family f, int round)
    {
        ColumnFamily cf = db.GetColumnFamily(f.Name);
        foreach (byte[] k in Keys(f, round)) cf.Put(k, Value(k, round));
    }

    private static void ManagedVerify(RocksDatabase db, Family f, int rounds)
    {
        ColumnFamily cf = db.GetColumnFamily(f.Name);
        List<KeyValuePair<byte[], byte[]>> expected = Expected(f, rounds);
        foreach (KeyValuePair<byte[], byte[]> kv in expected)
        {
            CollectionAssert.AreEqual(kv.Value, cf.Get(kv.Key), f.Name + ": managed Get");
        }
        foreach (byte[] k in AbsentKeys(f, expected)) Assert.IsNull(cf.Get(k), f.Name + ": managed Get absent");
        var got = new List<byte[]>();
        using (Iterator it = cf.NewIterator(new ReadOptions { TotalOrderSeek = true }))
        {
            for (it.SeekToFirst(); it.Valid; it.Next()) got.Add(it.Key.ToArray());
        }
        AssertKeys(expected, got, f.Name + ": managed scan");
        if (f.Shape == Shape.Tracker)
        {
            byte[] lower = new byte[8];
            BinaryPrimitives.WriteUInt64LittleEndian(lower, 255);
            var bounded = new List<byte[]>();
            using Iterator it = cf.NewIterator(new ReadOptions { IterateLowerBound = lower });
            for (it.SeekToFirst(); it.Valid; it.Next()) bounded.Add(it.Key.ToArray());
            AssertKeys(expected.FindAll(kv => BinaryPrimitives.ReadUInt64LittleEndian(kv.Key) >= 255), bounded, f.Name + ": managed tracker scan");
        }
        else if (f.PrefixLength > 0)
        {
            foreach (IGrouping<string, KeyValuePair<byte[], byte[]>> g in expected.GroupBy(kv => Convert.ToHexString(kv.Key, 0, f.PrefixLength)))
            {
                var inPrefix = new List<byte[]>();
                using Iterator it = cf.NewIterator(new ReadOptions { PrefixSameAsStart = true });
                for (it.Seek(Convert.FromHexString(g.Key)); it.Valid; it.Next()) inPrefix.Add(it.Key.ToArray());
                AssertKeys([.. g], inPrefix, f.Name + ": managed prefix scan " + g.Key);
            }
        }
    }

    private static void AssertKeys(IReadOnlyList<KeyValuePair<byte[], byte[]>> expected, List<byte[]> actual, string message)
    {
        Assert.AreEqual(expected.Count, actual.Count, message + ": count");
        for (int i = 0; i < expected.Count; i++) CollectionAssert.AreEqual(expected[i].Key, actual[i], message + " #" + i);
    }

    private static bool HasFilter(Family f) => f.Shape is Shape.PointLookup or Shape.Content or Shape.PrefixBloom or Shape.Tracker;

    /// <summary>
    /// The table properties two SST files of family <paramref name="f"/> share when written with the same options;
    /// with <paramref name="sameData"/> (the same entries, in the same order, with the same sequence numbers) also the
    /// block layout and sizes.
    /// </summary>
    private static void AssertSameTableShape(Family f, TableProperties native, TableProperties managed, string where, bool sameData)
    {
        string m = f.Name + " " + where + ": ";
        if (sameData)
        {
            // Which compression types the blocks ended up using depends on the data (the ratio check).
            Assert.AreEqual(native.CompressionName, managed.CompressionName, m + "compression");
            Assert.AreEqual(native.NumEntries, managed.NumEntries, m + "entries");
            Assert.AreEqual(native.RawKeySize, managed.RawKeySize, m + "raw key size");
            Assert.AreEqual(native.RawValueSize, managed.RawValueSize, m + "raw value size");
            Assert.AreEqual(native.NumDataBlocks, managed.NumDataBlocks, m + "data blocks");
            Assert.AreEqual(native.DataSize, managed.DataSize, m + "data size");
            Assert.AreEqual(native.IndexPartitions, managed.IndexPartitions, m + "index partitions");
            Assert.AreEqual(native.IndexSize, managed.IndexSize, m + "index size");
            Assert.AreEqual(native.TopLevelIndexSize, managed.TopLevelIndexSize, m + "top-level index size");
            Assert.AreEqual(native.FilterSize, managed.FilterSize, m + "filter size");
            Assert.AreEqual(native.NumFilterEntries, managed.NumFilterEntries, m + "filter entries count");
        }
        Assert.AreEqual(f.Shape == Shape.Tracker ? ComparatorName : "leveldb.BytewiseComparator", managed.ComparatorName, m + "comparator name");
        Assert.AreEqual(native.ComparatorName, managed.ComparatorName, m + "comparator");
        Assert.AreEqual(native.PrefixExtractorName, managed.PrefixExtractorName, m + "prefix extractor");
        Assert.AreEqual(native.FilterPolicyName, managed.FilterPolicyName, m + "filter policy");
        Assert.AreEqual(native.FormatVersion, managed.FormatVersion, m + "format_version");
        Assert.AreEqual(native.IndexPartitions > 0, managed.IndexPartitions > 0, m + "partitioned index");
        Assert.AreEqual(native.FilterSize > 0, managed.FilterSize > 0, m + "filter");
        Assert.AreEqual(native.NumFilterEntries > 0, managed.NumFilterEntries > 0, m + "filter entries");
        foreach (string key in new[]
                 {
                     "rocksdb.block.based.table.index.type", "rocksdb.block.based.table.whole.key.filtering",
                     "rocksdb.block.based.table.prefix.filtering",
                 })
        {
            Assert.AreEqual(native.GetUserPropertyString(key), managed.GetUserPropertyString(key), m + key);
        }
    }

    /// <summary>Properties of the live SST files of each family (by file name).</summary>
    private static Dictionary<string, Dictionary<string, TableProperties>> Props(RocksDatabase m) =>
        s_families.ToDictionary(f => f.Name, f => m.GetColumnFamily(f.Name).GetPropertiesOfAllTables());

    // ---- tests ------------------------------------------------------------------------------------------------

    /// <summary>
    /// Native creates the Mosaik-shaped DB (compacted bottommost files, flushed L0 files and WAL-only data), managed
    /// opens it with the same options, reads everything, extends, flushes and compacts every family (the L0 files it
    /// writes carry the same table properties as native's), and native reads the result.
    /// </summary>
    [TestMethod]
    public void NativeMosaikDbRoundTripsThroughManaged()
    {
        using var dir = new TempDir("mosaik");
        string db = Path.Combine(dir.Path, "db"), wal = Path.Combine(dir.Path, "wal");
        using (RocksDbSharp.RocksDb n = OpenNative(db, wal, s_families))
        {
            foreach (Family f in s_families)
            {
                NativeWrite(n, f, 0);
                NativeFlush(n, f);
                n.CompactRange((byte[]?)null, null, n.GetColumnFamily(f.Name)); // bottommost (optimize_filters_for_hits)
                NativeWrite(n, f, 1);
                NativeFlush(n, f);    // L0
                NativeWrite(n, f, 2); // WAL only
            }
        }

        using (RocksDatabase m = OpenManaged(db, wal, s_families))
        {
            Dictionary<string, Dictionary<string, TableProperties>> nativeProps = Props(m);
            foreach (Family f in s_families)
            {
                ManagedVerify(m, f, 3);
                ColumnFamily cf = m.GetColumnFamily(f.Name);
                // Native's L0 file of this family (the one written by a flush: the bottommost one may have no filter).
                TableProperties nativeL0 = nativeProps[f.Name].Values.OrderBy(static p => p.KeySmallestSeqno).Last();
                ManagedWrite(m, f, 3);
                // Only files at L0 are compared: a compaction that started at open may install bottommost files (no filter
                // under optimize_filters_for_hits) at any time. Auto compactions are held off so none picks up the flushed
                // file before it is listed; one already running chose its inputs before the flush.
                cf.SetOptions(new Dictionary<string, string> { ["disable_auto_compactions"] = "true" });
                cf.Flush();
                HashSet<ulong> level0 = m.GetLiveFilesMetadata()
                    .Where(x => x.ColumnFamilyName == f.Name && x.Level == 0).Select(static x => x.FileNumber).ToHashSet();
                int compared = 0;
                foreach ((string file, TableProperties p) in cf.GetPropertiesOfAllTables())
                {
                    if (nativeProps[f.Name].ContainsKey(file) || !level0.Contains(ulong.Parse(Path.GetFileNameWithoutExtension(file)))) continue;
                    AssertSameTableShape(f, nativeL0, p, "L0", sameData: false);
                    compared++;
                }
                Assert.IsTrue(compared > 0, f.Name + ": the flushed L0 file was compared");
                cf.SetOptions(new Dictionary<string, string> { ["disable_auto_compactions"] = "false" });
                // Forced, so every bottommost file is rewritten (a key range disjoint from the others would be a trivial move,
                // keeping its filter).
                cf.CompactRange(new CompactRangeOptions { BottommostLevelCompaction = BottommostLevelCompaction.Force }, null, null);
                foreach (TableProperties p in cf.GetPropertiesOfAllTables().Values)
                {
                    // Bottommost: optimize_filters_for_hits drops the filter where it is set.
                    bool expectFilter = HasFilter(f) && f.Shape is not (Shape.PrefixBloom or Shape.Tracker);
                    Assert.AreEqual(expectFilter, p.FilterSize > 0, f.Name + ": bottommost filter");
                }
                ManagedVerify(m, f, 4);
                ManagedWrite(m, f, 4); // WAL only
            }
        }

        using (RocksDbSharp.RocksDb n = OpenNative(db, wal, s_families))
        {
            foreach (Family f in s_families) NativeVerify(n, f, 5);
        }
    }

    /// <summary>
    /// The same entries written to a fresh Mosaik-shaped DB by each engine (same order, so the same sequence numbers)
    /// produce L0 files with the same table properties: block layout, partitioned index and filter sizes, compression.
    /// </summary>
    [TestMethod]
    public void SameDataProducesSameTablesOnBothEngines()
    {
        using var dir = new TempDir("mosaik-sst");
        string ndb = Path.Combine(dir.Path, "n"), mdb = Path.Combine(dir.Path, "m");
        // Without the write buffer manager, so that only the explicit flushes cut files.
        var nc = new NativeConfig(Path.Combine(dir.Path, "nwal"), s_families, writeBufferManagerSize: 0);
        using (RocksDbSharp.RocksDb n = RocksDbSharp.RocksDb.Open(nc.Db, ndb, nc.Families))
        {
            foreach (Family f in s_families)
            {
                NativeWrite(n, f, 0);
                NativeFlush(n, f);
            }
        }
        var mc = new ManagedConfig(Path.Combine(dir.Path, "mwal"), s_families, writeBufferManagerSize: 0);
        using (RocksDatabase m = RocksDatabase.Open(mdb, mc.Db, mc.Families))
        {
            foreach (Family f in s_families)
            {
                ManagedWrite(m, f, 0);
                m.GetColumnFamily(f.Name).Flush();
            }
        }
        using (RocksDatabase n = OpenManaged(ndb, Path.Combine(dir.Path, "nwal"), s_families))
        using (RocksDatabase m = OpenManaged(mdb, Path.Combine(dir.Path, "mwal"), s_families))
        {
            foreach (Family f in s_families)
            {
                Dictionary<string, TableProperties> na = n.GetColumnFamily(f.Name).GetPropertiesOfAllTables();
                Dictionary<string, TableProperties> ma = m.GetColumnFamily(f.Name).GetPropertiesOfAllTables();
                Assert.AreEqual(1, na.Count, f.Name + ": native files");
                Assert.AreEqual(1, ma.Count, f.Name + ": managed files");
                AssertSameTableShape(f, na.Values.Single(), ma.Values.Single(), "L0", sameData: true);
            }
        }
    }

    /// <summary>
    /// The write buffer manager Mosaik shares between its families is charged like native's: an empty memtable costs
    /// the arena's 2 KB inline block, not a whole arena block (with 31 families of 1 MB blocks, the old accounting kept
    /// a 32 MB manager permanently over its flush threshold), and both engines report about the same usage once open
    /// and after writes.
    /// </summary>
    [TestMethod]
    public void WriteBufferManagerUsageMatchesNative()
    {
        using var dir = new TempDir("mosaik-wbm");
        var nc = new NativeConfig(Path.Combine(dir.Path, "nwal"), s_families, 1UL << 30); // large enough that the writes below flush nothing
        var mc = new ManagedConfig(Path.Combine(dir.Path, "mwal"), s_families, 1UL << 30);
        RocksDbSharp.Native native = RocksDbSharp.Native.Instance;
        using RocksDbSharp.RocksDb n = RocksDbSharp.RocksDb.Open(nc.Db, Path.Combine(dir.Path, "n"), nc.Families);
        using RocksDatabase m = RocksDatabase.Open(Path.Combine(dir.Path, "m"), mc.Db, mc.Families);
        WriteBufferManager wbm = mc.Db.WriteBufferManager!;
        long nativeOpen = (long)(ulong)native.rocksdb_write_buffer_manager_memory_usage(nc.Wbm);
        // The point-lookup families' memtable blooms (and content's 128 MB arena block they come from) dominate.
        Assert.AreEqual(nativeOpen, wbm.MemoryUsage, 64 * 1024, "after open");
        Family plain = s_families[0];
        NativeWrite(n, plain, 0);
        ManagedWrite(m, plain, 0);
        long nativeWritten = (long)(ulong)native.rocksdb_write_buffer_manager_memory_usage(nc.Wbm);
        Assert.AreEqual(nativeWritten - nativeOpen, wbm.MemoryUsage - nativeOpen, 64 * 1024, "after writes");
    }

    /// <summary>The reverse: managed creates the DB, native reads, extends and compacts it, managed reads the result.</summary>
    [TestMethod]
    public void ManagedMosaikDbRoundTripsThroughNative()
    {
        using var dir = new TempDir("mosaik");
        string db = Path.Combine(dir.Path, "db"), wal = Path.Combine(dir.Path, "wal");
        using (RocksDatabase m = OpenManaged(db, wal, s_families))
        {
            foreach (Family f in s_families)
            {
                ColumnFamily cf = m.GetColumnFamily(f.Name);
                ManagedWrite(m, f, 0);
                cf.Flush();
                cf.CompactRange();
                ManagedWrite(m, f, 1);
                cf.Flush();
                ManagedWrite(m, f, 2);
            }
        }
        using (RocksDbSharp.RocksDb n = OpenNative(db, wal, s_families))
        {
            foreach (Family f in s_families) NativeVerify(n, f, 3);
            foreach (Family f in s_families)
            {
                NativeWrite(n, f, 3);
                NativeFlush(n, f);
                n.CompactRange((byte[]?)null, null, n.GetColumnFamily(f.Name));
                NativeWrite(n, f, 4);
            }
        }
        using (RocksDatabase m = OpenManaged(db, wal, s_families))
        {
            foreach (Family f in s_families) ManagedVerify(m, f, 5);
        }
    }

    /// <summary>
    /// Reopening a Mosaik DB written by an older version (fewer families) with today's family set and
    /// create_missing_column_families; opening without every existing family; the read-only subset open and the
    /// default-options open of RocksDbStorage.MigrateDatabases (the tracker's comparator mismatches): each behaves the
    /// same on both engines.
    /// </summary>
    [TestMethod]
    public void ReopenWithMosaikFamilySetMatchesNative()
    {
        using var dir = new TempDir("mosaik-cf");
        string db = Path.Combine(dir.Path, "db"), wal = Path.Combine(dir.Path, "wal");
        Family[] older = [.. s_families.Where(static f => f.Name is not ("deletion-queue" or "data-vectors-hashes"))];
        using (RocksDbSharp.RocksDb n = OpenNative(db, wal, older))
        {
            foreach (Family f in older) NativeWrite(n, f, 0);
        }
        // Today's family set: the missing families are created.
        using (RocksDatabase m = OpenManaged(db, wal, s_families))
        {
            foreach (Family f in older) ManagedVerify(m, f, 1);
            Assert.IsTrue(m.TryGetColumnFamily("deletion-queue", out ColumnFamily? dq));
            dq!.Put("k"u8, "v"u8);
            Assert.IsFalse(m.TryGetColumnFamily("no-such-family", out _));
        }
        List<string> names = RocksDatabase.ListColumnFamilies(db);
        CollectionAssert.AreEquivalent(RocksDbSharp.RocksDb.ListColumnFamilies(new RocksDbSharp.DbOptions(), db).ToList(), names);
        Assert.AreEqual(s_families.Length + 1, names.Count);

        // Opening without one of the existing families fails on both engines.
        Family[] missingContent = [.. s_families.Where(static f => f.Name != "content")];
        var me = Assert.ThrowsExactly<RocksDbException>(() => OpenManaged(db, wal, missingContent).Dispose());
        Assert.AreEqual(StatusCode.InvalidArgument, me.Code);
        var ne = Assert.ThrowsExactly<RocksDbSharp.RocksDbException>(() => OpenNative(db, wal, missingContent).Dispose());
        StringAssert.Contains(ne.Message, "Invalid argument");

        // MigrateDatabases: a read-only open of the default family only, then every family with default options. The
        // tracker's comparator name does not match the default comparator.
        using (RocksDatabase ro = RocksDatabase.OpenReadOnly(db, new DbOptions { WalDir = wal })) { }
        using (RocksDbSharp.RocksDb ro = RocksDbSharp.RocksDb.OpenReadOnly(new RocksDbSharp.DbOptions().SetWalDir(wal), db, new RocksDbSharp.ColumnFamilies(), false)) { }
        var plain = names.ConvertAll(static n => new ColumnFamilyDescriptor(n, new ColumnFamilyOptions()));
        var me2 = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(db, new DbOptions { WalDir = wal }, plain).Dispose());
        Assert.AreEqual(StatusCode.InvalidArgument, me2.Code);
        StringAssert.Contains(me2.Message, "does not match existing comparator " + ComparatorName);
        var nativePlain = new RocksDbSharp.ColumnFamilies();
        foreach (string n in names) if (n != "default") nativePlain.Add(n, new RocksDbSharp.ColumnFamilyOptions());
        var ne2 = Assert.ThrowsExactly<RocksDbSharp.RocksDbException>(
            () => RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions().SetWalDir(wal), db, nativePlain).Dispose());
        StringAssert.Contains(ne2.Message, "does not match existing comparator " + ComparatorName);

        // Native reads what managed wrote to the created family.
        using (RocksDbSharp.RocksDb n = OpenNative(db, wal, s_families))
        {
            Assert.AreEqual("v", n.Get("k", n.GetColumnFamily("deletion-queue")));
        }
    }

    /// <summary>
    /// The OPTIONS file each engine writes for Mosaik's configuration: every option of every section the same (except
    /// the version section and the C API's filter policy id), and each file is loaded strictly by the other engine
    /// with the values Mosaik set.
    /// </summary>
    [TestMethod]
    public void OptionsFilesMatchAndLoadOnBothEngines()
    {
        using var dir = new TempDir("mosaik-opt");
        string ndb = Path.Combine(dir.Path, "n"), mdb = Path.Combine(dir.Path, "m");
        using (OpenNative(ndb, Path.Combine(dir.Path, "nwal"), s_families)) { }
        using (OpenManaged(mdb, Path.Combine(dir.Path, "mwal"), s_families)) { }

        string nativeFile = OptionsFile.FindLatestOptionsFile(LocalFileSystem.Instance, ndb)!;
        string managedFile = OptionsFile.FindLatestOptionsFile(LocalFileSystem.Instance, mdb)!;
        Dictionary<string, Dictionary<string, string>> nat = Sections(File.ReadAllText(nativeFile)), man = Sections(File.ReadAllText(managedFile));
        CollectionAssert.AreEquivalent(nat.Keys.ToList(), man.Keys.ToList(), "sections");
        var diffs = new List<string>();
        foreach ((string section, Dictionary<string, string> n) in nat)
        {
            if (section == "[Version]") continue;
            Dictionary<string, string> m = man[section];
            foreach (string key in n.Keys.Union(m.Keys))
            {
                n.TryGetValue(key, out string? nv);
                m.TryGetValue(key, out string? mv);
                if (nv == mv) continue;
                if (key == "wal_dir")
                {
                    Assert.AreEqual(Path.Combine(dir.Path, "nwal"), nv);
                    Assert.AreEqual(Path.Combine(dir.Path, "mwal"), mv);
                    continue;
                }
                // A filter policy made through the C API (rocksdb_filterpolicy_create_bloom_full) is persisted by its
                // Name() only: the C API wrapper does not forward GetId(). Managed writes the policy's id.
                if (key == "filter_policy" && nv == "bloomfilter" && mv == "bloomfilter:10:false") continue;
                // Native persists the sanitized arena_block_size (min(1 MB, write_buffer_size / 8) for 0); managed keeps
                // 0 and derives the same block size when it creates a memtable (and again after a SetOptions of
                // write_buffer_size, where native keeps the open-time value). Either value loads to the same block size.
                if (key == "arena_block_size" && nv == "1048576" && mv == "0") continue;
                diffs.Add(key + ": native=" + nv + " managed=" + mv);
            }
        }
        Assert.AreEqual(0, diffs.Count, string.Join("\n", diffs.Distinct()));

        // Managed loads native's file strictly; the custom comparator is kept by name.
        OptionsFile parsed = OptionsFile.Load(LocalFileSystem.Instance, nativeFile, preserveUnknownOptions: false);
        Assert.AreEqual(s_families.Length + 1, parsed.ColumnFamilies.Count);
        ColumnFamilyOptions tracker = parsed.FindColumnFamily(TrackerFamily)!;
        Assert.AreEqual("rocksdb.FixedPrefix.16", tracker.PrefixExtractor!.AsString());
        Assert.IsFalse(tracker.TableOptions.WholeKeyFiltering);
        Assert.IsTrue(tracker.OptimizeFiltersForHits);
        Assert.AreEqual(IndexType.TwoLevelIndexSearch, tracker.TableOptions.IndexType);
        Assert.IsTrue(tracker.TableOptions.PartitionFilters);
        Assert.AreEqual(CompressionType.LZ4, parsed.FindColumnFamily("content")!.Compression);
        Assert.AreEqual(128UL * 1024 * 1024, parsed.FindColumnFamily("content")!.ArenaBlockSize);
        Assert.AreEqual(DataBlockIndexType.BinaryAndHash, parsed.FindColumnFamily("content")!.TableOptions.DataBlockIndexType);
        Assert.AreEqual(Math.Max(2, Parallelism / 2), parsed.DbOptions.MaxBackgroundJobs);
        // Native loads managed's file strictly.
        NativeLoadLatestOptions(mdb, s_families.Length + 1);
    }

    /// <summary>
    /// wal_dir is persisted sanitized like native: a trailing '/' removed, and cleared when it names the DB directory.
    /// </summary>
    [TestMethod]
    [DataRow("sep/", "sep")]
    [DataRow("same", null)]
    [DataRow("same/", null)]
    public void WalDirPersistedLikeNative(string walDir, string? expected)
    {
        using var dir = new TempDir("mosaik-waldir");
        string Persisted(string db)
        {
            Sections(File.ReadAllText(OptionsFile.FindLatestOptionsFile(LocalFileSystem.Instance, db)!))["[DBOptions]"].TryGetValue("wal_dir", out string? v);
            return v ?? "";
        }
        string Resolve(string db) => walDir.StartsWith("same", StringComparison.Ordinal) ? db + walDir[4..] : Path.Combine(dir.Path, walDir);
        string ndb = Path.Combine(dir.Path, "n"), mdb = Path.Combine(dir.Path, "m");
        var no = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetWalDir(Resolve(ndb));
        s_keepAlive.Add(no);
        using (RocksDbSharp.RocksDb.Open(no, ndb)) { }
        string nativeValue = Persisted(ndb);
        string managedWal = expected is null ? Resolve(mdb) : Path.Combine(dir.Path, "msep/");
        using (RocksDatabase.Open(mdb, new DbOptions { CreateIfMissing = true, WalDir = managedWal })) { }
        string managedValue = Persisted(mdb);
        Assert.AreEqual(expected is null ? "" : Path.Combine(dir.Path, expected), nativeValue);
        Assert.AreEqual(expected is null ? "" : Path.Combine(dir.Path, "m" + expected), managedValue);
        // Both files load back.
        Assert.AreEqual(managedValue, OptionsFile.Load(LocalFileSystem.Instance, OptionsFile.FindLatestOptionsFile(LocalFileSystem.Instance, mdb)!, false).DbOptions.WalDir);
    }

    private static Dictionary<string, Dictionary<string, string>> Sections(string text)
    {
        var result = new Dictionary<string, Dictionary<string, string>>(StringComparer.Ordinal);
        Dictionary<string, string>? current = null;
        foreach (string raw in text.Split('\n'))
        {
            string line = raw.Trim();
            if (line.Length == 0 || line[0] == '#') continue;
            if (line[0] == '[')
            {
                current = new Dictionary<string, string>(StringComparer.Ordinal);
                result[line] = current;
                continue;
            }
            int eq = line.IndexOf('=');
            current![line[..eq]] = line[(eq + 1)..];
        }
        return result;
    }

    /// <summary>rocksdb_load_latest_options without ignore_unknown_options, checking values Mosaik set.</summary>
    private static void NativeLoadLatestOptions(string dbPath, int expectedFamilies)
    {
        RocksDbSharp.Native native = RocksDbSharp.Native.Instance;
        IntPtr path = Marshal.StringToHGlobalAnsi(dbPath);
        IntPtr env = native.rocksdb_create_default_env();
        IntPtr cache = native.rocksdb_cache_create_lru((UIntPtr)(1 << 20));
        IntPtr outs = Marshal.AllocHGlobal(IntPtr.Size * 4);
        try
        {
            for (int i = 0; i < 4; i++) Marshal.WriteIntPtr(outs, i * IntPtr.Size, IntPtr.Zero);
            IntPtr dbOptsOut = outs, numOut = outs + IntPtr.Size, namesOut = outs + 2 * IntPtr.Size, cfOptsOut = outs + 3 * IntPtr.Size;
            native.rocksdb_load_latest_options(path, env, 0, cache, dbOptsOut, numOut, namesOut, cfOptsOut);
            int num = (int)Marshal.ReadIntPtr(numOut);
            IntPtr dbOpts = Marshal.ReadIntPtr(dbOptsOut);
            Assert.AreNotEqual(IntPtr.Zero, dbOpts);
            Assert.AreEqual(expectedFamilies, num);
            IntPtr names = Marshal.ReadIntPtr(namesOut), cfOpts = Marshal.ReadIntPtr(cfOptsOut);
            Assert.AreEqual(Math.Max(2, Parallelism / 2), native.rocksdb_options_get_max_background_jobs(dbOpts));
            Assert.AreEqual(1048576UL, native.rocksdb_options_get_wal_bytes_per_sync(dbOpts));
            bool sawContent = false, sawTracker = false;
            for (int i = 0; i < num; i++)
            {
                string name = Marshal.PtrToStringAnsi(Marshal.ReadIntPtr(names, i * IntPtr.Size))!;
                IntPtr cf = Marshal.ReadIntPtr(cfOpts, i * IntPtr.Size);
                if (name == "content")
                {
                    sawContent = true;
                    Assert.AreEqual(4, native.rocksdb_options_get_compression(cf)); // kLZ4Compression
                    Assert.AreEqual(128UL * 1024 * 1024, (ulong)native.rocksdb_options_get_arena_block_size(cf));
                    Assert.AreEqual(0.02, native.rocksdb_options_get_memtable_prefix_bloom_size_ratio(cf));
                    Assert.AreNotEqual(0, native.rocksdb_options_get_memtable_whole_key_filtering(cf));
                }
                if (name == TrackerFamily)
                {
                    sawTracker = true;
                    Assert.AreNotEqual(0, native.rocksdb_options_get_optimize_filters_for_hits(cf));
                }
            }
            Assert.IsTrue(sawContent && sawTracker);
            native.rocksdb_load_latest_options_destroy(dbOpts, names, cfOpts, (UIntPtr)num);
        }
        finally
        {
            Marshal.FreeHGlobal(outs);
            Marshal.FreeHGlobal(path);
            native.rocksdb_cache_destroy(cache);
            native.rocksdb_env_destroy(env);
        }
    }
}
