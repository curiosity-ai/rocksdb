using System.Buffers.Binary;
using RocksDb.Managed.TestShared;

namespace RocksDb.Managed.CompatTests.Infrastructure;

/// <summary>
/// The native twin of <see cref="MosaikProfile"/>: the database Mosaik's default (rocksdb-sharp) build opens, call for
/// call as the <c>#else</c> branches of <c>DBOptionsExt.SetRockDbDefaultOptions</c>, <c>RocksDbBase.CreateTableOptions</c>
/// / <c>CreatePointLookupColumnFamilyOptions</c>, <c>RocksDbStorage</c>'s family map and
/// <c>SafeRocksDb.ConfigureForChangeTracker</c> make them (<c>Mosaik/Graph/src/Graph/Storage/</c>). Same parameters
/// and the same deliberate differences as the managed profile; one shared default Env per process, as Mosaik's
/// <c>RocksDbBase.DefaultEnv</c>.
/// </summary>
internal sealed class NativeMosaikProfile
{
    private static readonly RocksDbSharp.Native N = RocksDbSharp.Native.Instance;

    // Native handles referenced by options must outlive every database opened with them.
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    /// <summary>RocksDbBase.DefaultEnv: the process's, shared by every database (its thread pools are sized by each open).</summary>
    public static readonly RocksDbSharp.Env DefaultEnv = RocksDbSharp.Env.CreateDefaultEnv();

    /// <summary>Mosaik's ulong_Comparator, rocksdb-sharp flavour.</summary>
    public sealed unsafe class UlongComparator : RocksDbSharp.Comparator
    {
        public static readonly UlongComparator Instance = new();
        public string Name => MosaikProfile.ComparatorName;
        public int Compare(IntPtr a, UIntPtr alen, IntPtr b, UIntPtr blen) =>
            BinaryPrimitives.ReadUInt64LittleEndian(new ReadOnlySpan<byte>((void*)a, (int)alen)[..sizeof(ulong)])
                .CompareTo(BinaryPrimitives.ReadUInt64LittleEndian(new ReadOnlySpan<byte>((void*)b, (int)blen)[..sizeof(ulong)]));
    }

    public bool LowMemoryMode { get; }
    public int ProcessorCount { get; }
    public bool LowResources => LowMemoryMode || ProcessorCount < 4;

    public RocksDbSharp.Cache Cache { get; }
    public IntPtr WriteBufferManager { get; }
    public RocksDbSharp.DbOptions DbOptions { get; }

    /// <summary>rocksdb-sharp's ColumnFamilies: "default" with default options first, then Mosaik's families.</summary>
    public RocksDbSharp.ColumnFamilies Families { get; }

    public NativeMosaikProfile(string walDir, bool lowMemoryMode, int processorCount, long cacheBytes = 0, long writeBufferBytes = 0)
    {
        LowMemoryMode = lowMemoryMode;
        ProcessorCount = processorCount;
        if (cacheBytes == 0) cacheBytes = lowMemoryMode ? 8L * 1024 * 1024 : 256L * 1024 * 1024 * processorCount;
        if (writeBufferBytes == 0) writeBufferBytes = lowMemoryMode ? 32L * 1024 * 1024 : 64L * 1024 * 1024;
        Cache = RocksDbSharp.Cache.CreateLru((ulong)cacheBytes);
        WriteBufferManager = N.rocksdb_write_buffer_manager_create((UIntPtr)(ulong)writeBufferBytes, RocksDbSharp.Native.MarshalBool(false));
        DbOptions = CreateDbOptions(walDir);
        Families = new RocksDbSharp.ColumnFamilies();
        foreach (MosaikProfile.Family f in MosaikProfile.Families) Families.Add(f.Name, CreateFamilyOptions(f));
        s_keepAlive.Add(this);
    }

    public RocksDbSharp.RocksDb Open(string path) => RocksDbSharp.RocksDb.Open(DbOptions, path, Families);

    public RocksDbSharp.ColumnFamilyOptions CreateFamilyOptions(MosaikProfile.Family f)
    {
        RocksDbSharp.ColumnFamilyOptions o;
        switch (f.Shape)
        {
            case MosaikProfile.Shape.Plain:
                o = ColumnFamilyOptions();
                break;
            case MosaikProfile.Shape.PointLookup:
                o = PointLookupColumnFamilyOptions();
                break;
            case MosaikProfile.Shape.Content:
                o = PointLookupColumnFamilyOptions().SetCompression(RocksDbSharp.Compression.Lz4)
                    .SetArenaBlockSize(LowMemoryMode ? 1UL * 1024 * 1024 : 128UL * 1024 * 1024);
                break;
            case MosaikProfile.Shape.Prefix8NoFilter:
                o = ColumnFamilyOptions().SetPrefixExtractor(RocksDbSharp.SliceTransform.CreateFixedPrefix((ulong)f.PrefixLength));
                break;
            case MosaikProfile.Shape.PrefixBloom:
                o = ColumnFamilyOptions().SetPrefixExtractor(RocksDbSharp.SliceTransform.CreateFixedPrefix((ulong)f.PrefixLength))
                    .SetBlockBasedTableFactory(TableOptionsWithBloom()).SetOptimizeFiltersForHits(1);
                break;
            case MosaikProfile.Shape.Tracker:
                o = ColumnFamilyOptions().SetPrefixExtractor(RocksDbSharp.SliceTransform.CreateFixedPrefix((ulong)f.PrefixLength))
                    .SetBlockBasedTableFactory(TableOptionsWithBloom().SetWholeKeyFiltering(false)).SetOptimizeFiltersForHits(1);
                o.SetComparator(UlongComparator.Instance);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(f));
        }
        s_keepAlive.Add(o);
        return o;
    }

    /// <summary>DBOptionsExt.SetRockDbDefaultOptions, native branch, line for line.</summary>
    private RocksDbSharp.DbOptions CreateDbOptions(string walDir)
    {
        int procCount = ProcessorCount;
        var o = new RocksDbSharp.DbOptions();
        o.SetCreateIfMissing(true);
        o.SetCreateMissingColumnFamilies(true);
        o.SetWalDir(walDir);
        o.EnableStatistics();
        N.rocksdb_options_set_write_buffer_manager(o.Handle, WriteBufferManager);

        if (LowResources)
        {
            o.SetInfoLogLevel(RocksDbSharp.InfoLogLevel.Error);
            o.IncreaseParallelism(2);
            o.SetMaxOpenFiles(8);
            o.SetDeleteObsoleteFilesPeriodMicros(1_000_000ul * 900);
            int backgroundJobs = Math.Max(2, procCount / 4);
            int backgroundFlushes = 2;
            int backgroundCompactions = Math.Max(1, backgroundJobs - 1);
            N.rocksdb_options_set_max_background_jobs(o.Handle, backgroundJobs);
            o.SetMaxBackgroundFlushes(backgroundFlushes);
            o.SetMaxBackgroundCompactions(backgroundCompactions);
            DefaultEnv.SetBackgroundThreads(backgroundCompactions);
            DefaultEnv.SetHighPriorityBackgroundThreads(backgroundFlushes);
            o.SetEnv(DefaultEnv.Handle);
        }
        else
        {
            o.IncreaseParallelism(Math.Min(8, Math.Max(2, procCount / 2)));
            o.SetDeleteObsoleteFilesPeriodMicros(1_000_000ul * 3600);
            o.SetInfoLogLevel(RocksDbSharp.InfoLogLevel.Info);
            N.rocksdb_options_set_max_subcompactions(o.Handle, (uint)Math.Max(2, procCount / 4));
            N.rocksdb_options_set_max_background_jobs(o.Handle, Math.Max(2, procCount / 2));
            DefaultEnv.SetBackgroundThreads(Math.Max(2, procCount / 2));
            DefaultEnv.SetHighPriorityBackgroundThreads(Math.Max(2, procCount / 2));
            o.SetEnv(DefaultEnv.Handle);
            N.rocksdb_options_set_dump_malloc_stats(o.Handle, RocksDbSharp.Native.MarshalBool(true));
        }

        o.SetBytesPerSync(1_048_576);
        N.rocksdb_options_set_wal_bytes_per_sync(o.Handle, 1048576);
        o.SetMaxFileOpeningThreads(LowResources ? 2 : Math.Max(2, procCount / 2));
        o.SetWalTtlSeconds(MosaikProfile.WalArchiveTtlNeverSeconds);
        o.SetWalSizeLimitMB(0);
        o.SetMaxLogFileSize(LowResources ? 1UL * 1024 * 1024 : 64UL * 1024 * 1024);
        o.SetKeepLogFileNum(10);
        o.SetMaxTotalWalSize(LowResources ? 1UL * 1024 * 1024 : 1024UL * 1024 * 1024);
        o.SkipStatsUpdateOnOpen(true);
        N.rocksdb_options_set_max_file_opening_threads(o.Handle, Math.Max(procCount, 16));
        o.SetWalRecoveryMode(RocksDbSharp.Recovery.SkipAnyCorruptedRecords);
        s_keepAlive.Add(o);
        return o;
    }

    /// <summary>RocksDbBase.CreateTableOptions.</summary>
    private RocksDbSharp.BlockBasedTableOptions TableOptions()
    {
        var t = new RocksDbSharp.BlockBasedTableOptions().SetBlockCache(Cache.Handle);
        N.rocksdb_block_based_options_set_cache_index_and_filter_blocks(t.Handle, RocksDbSharp.Native.MarshalBool(true));
        N.rocksdb_block_based_options_set_cache_index_and_filter_blocks_with_high_priority(t.Handle, RocksDbSharp.Native.MarshalBool(true));
        N.rocksdb_block_based_options_set_pin_l0_filter_and_index_blocks_in_cache(t.Handle, RocksDbSharp.Native.MarshalBool(true));
        N.rocksdb_block_based_options_set_index_type(t.Handle, RocksDbSharp.BlockBasedTableIndexType.TwoLevelIndex);
        N.rocksdb_block_based_options_set_partition_filters(t.Handle, RocksDbSharp.Native.MarshalBool(true));
        N.rocksdb_block_based_options_set_pin_top_level_index_and_filter(t.Handle, RocksDbSharp.Native.MarshalBool(true));
        s_keepAlive.Add(t);
        return t;
    }

    private RocksDbSharp.BlockBasedTableOptions TableOptionsWithBloom() => TableOptions().SetFilterPolicy(RocksDbSharp.BloomFilterPolicy.Create(10, false));

    private RocksDbSharp.ColumnFamilyOptions ColumnFamilyOptions() => new RocksDbSharp.ColumnFamilyOptions().SetBlockBasedTableFactory(TableOptions());

    private RocksDbSharp.ColumnFamilyOptions PointLookupColumnFamilyOptions()
    {
        RocksDbSharp.BlockBasedTableOptions t = TableOptionsWithBloom();
        N.rocksdb_block_based_options_set_data_block_index_type(t.Handle, (int)RocksDbSharp.BlockBasedTableDataBlockIndexType.BinarySearchAndHash);
        N.rocksdb_block_based_options_set_data_block_hash_table_util_ratio(t.Handle, 0.75);
        var o = new RocksDbSharp.ColumnFamilyOptions().SetBlockBasedTableFactory(t).SetMemtablePrefixBloomSizeRatio(0.02);
        N.rocksdb_options_set_memtable_whole_key_filtering(o.Handle, RocksDbSharp.Native.MarshalBool(true));
        return o;
    }
}
