using System.Buffers.Binary;
using RocksDb.Managed.Cache;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table.Filters;

namespace RocksDb.Managed.TestShared;

/// <summary>
/// The database Mosaik (the main downstream user) opens with its <c>ROCKSDB_MANAGED</c> build, option for option:
/// <c>DBOptionsExt.SetRockDbDefaultOptions</c> (both the low-resource and the normal branch),
/// <c>RocksDbBase.CreateTableOptions</c> / <c>CreateTableOptionsWithBloom</c> / <c>CreateColumnFamilyOptions</c> /
/// <c>CreatePointLookupColumnFamilyOptions</c>, the family map of <c>RocksDbStorage</c>'s constructor and the replica
/// change tracker family of <c>SafeRocksDb.ConfigureForChangeTracker</c> (all under
/// <c>Mosaik/Graph/src/Graph/Storage/</c>). The values Mosaik derives from the machine (processor count, available
/// memory) are parameters, so a test can pick either branch on any host.
/// </summary>
/// <remarks>
/// Deliberate differences, so tests stay small: the shared block cache and write buffer manager sizes are parameters
/// (Mosaik: 256 MiB per processor / 8 MiB, and available memory / 20 clamped to 64 MiB..4 GiB / 32 MiB), and the
/// caller supplies the file system. <c>ROCKSDB_PARALLELISM</c>, <c>ROCKSDB_MAXFILES</c> and the cache / write buffer
/// size environment variables are not read. Keep this in step with Mosaik when its storage options change; the native
/// twin is <c>RocksDb.Managed.CompatTests.Infrastructure.NativeMosaikProfile</c>.
/// </remarks>
internal sealed class MosaikProfile
{
    /// <summary>RocksDbBase.WAL_ARCHIVE_TTL_NEVER_SECONDS: a ttl no file reaches, which keeps archiving on.</summary>
    public const ulong WalArchiveTtlNeverSeconds = 100UL * 365 * 24 * 3600;

    /// <summary>ulong_Comparator.Name, persisted with the family (never changes).</summary>
    public const string ComparatorName = "Mosaik.GraphDB.ulong_Comparator";

    public const string GraphConfigFamily = "graphsettings";
    public const string ReplicaChangeTrackerFamily = "replica-change-tracker";

    /// <summary>The shape of a family's options in Mosaik.</summary>
    public enum Shape
    {
        /// <summary>CreateColumnFamilyOptions: shared cache, partitioned two-level index and filters cached, no filter policy.</summary>
        Plain,
        /// <summary>CreatePointLookupColumnFamilyOptions: + Bloom 10, data block hash index 0.75, memtable whole-key bloom 0.02.</summary>
        PointLookup,
        /// <summary>"content": PointLookup + LZ4, with the engine's default arena block size (see <see cref="ContentArenaBlockSize"/>).</summary>
        Content,
        /// <summary>CreateColumnFamilyOptions + fixed 8-byte prefix extractor, no filter policy (data-vectors*).</summary>
        Prefix8NoFilter,
        /// <summary>Fixed prefix + Bloom 10 (whole-key filtering on) + optimize_filters_for_hits (the index / queue families).</summary>
        PrefixBloom,
        /// <summary>SafeRocksDb change tracker: ulong comparator, 16-byte prefix, prefix-only Bloom, optimize_filters_for_hits.</summary>
        Tracker,
    }

    public sealed record Family(string Name, Shape Shape, int PrefixLength);

    /// <summary>
    /// RocksDbStorage's family map in Mosaik's order, then the change tracker SafeRocksDb.Open appends. The default
    /// family is not listed (Mosaik's managed build does not list it; the engine adds it with default options).
    /// </summary>
    public static readonly IReadOnlyList<Family> Families =
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
        new(GraphConfigFamily, Shape.Plain, 0),
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
        new(ReplicaChangeTrackerFamily, Shape.Tracker, 16),
    ];

    /// <summary>Mosaik's ulong_Comparator: the first 8 bytes as a little-endian ulong, the rest ignored (BitConverter on x86/ARM).</summary>
    public sealed class UlongComparator : Comparator
    {
        public static readonly UlongComparator Instance = new();
        public override string Name => ComparatorName;
        public override int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) =>
            BinaryPrimitives.ReadUInt64LittleEndian(a[..sizeof(ulong)]).CompareTo(BinaryPrimitives.ReadUInt64LittleEndian(b[..sizeof(ulong)]));
    }

    public bool LowMemoryMode { get; }

    /// <summary>
    /// The content family's arena_block_size, or 0 for the engine's default (min(1 MiB, write_buffer_size / 8)), which is
    /// what Mosaik uses since curiosity-ai/mosaik#2776. The tests of the configuration before it (audit #15's write buffer
    /// manager findings) pass <see cref="ContentArenaBlockSizeBefore2776"/> explicitly.
    /// </summary>
    public ulong ContentArenaBlockSize { get; }

    /// <summary>Mosaik's content arena block before curiosity-ai/mosaik#2776: 128 MiB, 1 MiB in low-memory mode.</summary>
    public static ulong ContentArenaBlockSizeBefore2776(bool lowMemoryMode) => lowMemoryMode ? 1UL * 1024 * 1024 : 128UL * 1024 * 1024;

    /// <summary>Environment.ProcessorCount as SetRockDbDefaultOptions sees it (fewer than 4 forces the low-resource branch).</summary>
    public int ProcessorCount { get; }

    /// <summary>True when SetRockDbDefaultOptions takes its low-resource branch (low-memory mode or fewer than 4 processors).</summary>
    public bool LowResources => LowMemoryMode || ProcessorCount < 4;

    public BlockCache Cache { get; }
    public WriteBufferManager WriteBufferManager { get; }
    public DbOptions DbOptions { get; }

    /// <summary>The descriptors Mosaik passes to RocksDatabase.Open (no default family).</summary>
    public List<ColumnFamilyDescriptor> Descriptors { get; }

    /// <summary>What RocksDbBase.WalArchiveSizeLimitBytes is set to (the size cap of Mosaik's own archive cleanup).</summary>
    public long WalArchiveSizeLimitBytes => LowResources ? 1L * 1024 * 1024 : 256L * 1024 * 1024;

    /// <param name="walDir">The journal path (Mosaik keeps the WALs outside the database directory).</param>
    /// <param name="lowMemoryMode">Mosaik's low-memory mode.</param>
    /// <param name="processorCount">Environment.ProcessorCount (or ROCKSDB_PARALLELISM).</param>
    /// <param name="fileSystem">Null for the local file system.</param>
    /// <param name="cacheBytes">Shared LRU cache capacity; 0 = Mosaik's value for the mode (8 MiB, or 256 MiB per processor).</param>
    /// <param name="writeBufferBytes">Shared write buffer manager size; 0 = Mosaik's low-memory value (32 MiB) or its floor (64 MiB).</param>
    /// <param name="contentArenaBlockSize">See <see cref="ContentArenaBlockSize"/>; 0 = the engine default, as Mosaik.</param>
    public MosaikProfile(string walDir, bool lowMemoryMode, int processorCount, IFileSystem? fileSystem = null, long cacheBytes = 0, long writeBufferBytes = 0,
        ulong contentArenaBlockSize = 0)
    {
        LowMemoryMode = lowMemoryMode;
        ContentArenaBlockSize = contentArenaBlockSize;
        ProcessorCount = processorCount;
        if (cacheBytes == 0) cacheBytes = lowMemoryMode ? 8L * 1024 * 1024 : 256L * 1024 * 1024 * processorCount;
        if (writeBufferBytes == 0) writeBufferBytes = lowMemoryMode ? 32L * 1024 * 1024 : 64L * 1024 * 1024;
        Cache = BlockCache.CreateLru(cacheBytes);
        WriteBufferManager = new WriteBufferManager(writeBufferBytes, cache: null, allowStall: false);
        DbOptions = CreateDbOptions(walDir, fileSystem);
        Descriptors = [];
        foreach (Family f in Families) Descriptors.Add(new ColumnFamilyDescriptor(f.Name, CreateFamilyOptions(f)));
    }

    /// <summary>The options of one family, created afresh (Mosaik builds every family's options separately).</summary>
    public ColumnFamilyOptions CreateFamilyOptions(Family f)
    {
        switch (f.Shape)
        {
            case Shape.Plain:
                return ColumnFamilyOptions();
            case Shape.PointLookup:
                return PointLookupColumnFamilyOptions();
            case Shape.Content:
            {
                ColumnFamilyOptions o = PointLookupColumnFamilyOptions();
                o.Compression = CompressionType.LZ4;
                if (ContentArenaBlockSize != 0) o.ArenaBlockSize = ContentArenaBlockSize;
                return o;
            }
            case Shape.Prefix8NoFilter:
            {
                ColumnFamilyOptions o = ColumnFamilyOptions();
                o.PrefixExtractor = SliceTransform.CreateFixedPrefix(f.PrefixLength);
                return o;
            }
            case Shape.PrefixBloom:
            {
                ColumnFamilyOptions o = ColumnFamilyOptions();
                o.PrefixExtractor = SliceTransform.CreateFixedPrefix(f.PrefixLength);
                o.TableOptions = TableOptionsWithBloom();
                o.OptimizeFiltersForHits = true;
                return o;
            }
            case Shape.Tracker:
            {
                ColumnFamilyOptions o = ColumnFamilyOptions();
                BlockBasedTableOptions t = TableOptionsWithBloom();
                t.WholeKeyFiltering = false;
                o.PrefixExtractor = SliceTransform.CreateFixedPrefix(f.PrefixLength);
                o.TableOptions = t;
                o.OptimizeFiltersForHits = true;
                o.Comparator = UlongComparator.Instance;
                return o;
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(f));
        }
    }

    /// <summary>Opens (or creates) the database the way SafeRocksDb.Open does, before its tracker cleanup.</summary>
    public RocksDatabase Open(string path) => RocksDatabase.Open(path, DbOptions, Descriptors);

    /// <summary>The handles Mosaik keeps, by family name (RocksDbStorage's *Family fields).</summary>
    public static Dictionary<string, ColumnFamily> Handles(RocksDatabase db)
    {
        var handles = new Dictionary<string, ColumnFamily>(StringComparer.Ordinal);
        foreach (Family f in Families) handles[f.Name] = db.GetColumnFamily(f.Name);
        return handles;
    }

    /// <summary>DBOptionsExt.SetRockDbDefaultOptions, managed branch, line for line.</summary>
    private DbOptions CreateDbOptions(string walDir, IFileSystem? fileSystem)
    {
        int procCount = ProcessorCount;
        var o = new DbOptions
        {
            CreateIfMissing = true,
            CreateMissingColumnFamilies = true,
            WalDir = walDir,
            Statistics = new Statistics(),
            WriteBufferManager = WriteBufferManager,
        };
        if (fileSystem is not null) o.FileSystem = fileSystem;

        if (LowResources)
        {
            o.InfoLogLevel = InfoLogLevel.Error;
            o.MaxBackgroundJobs = 2; // IncreaseParallelism(2)
            o.MaxOpenFiles = 8;
            o.DeleteObsoleteFilesPeriodMicros = 1_000_000ul * 900;
            int backgroundJobs = Math.Max(2, procCount / 4);
            o.MaxBackgroundJobs = backgroundJobs;
            o.MaxBackgroundFlushes = 2;
            o.MaxBackgroundCompactions = Math.Max(1, backgroundJobs - 1);
        }
        else
        {
            o.MaxBackgroundJobs = Math.Min(8, Math.Max(2, procCount / 2)); // IncreaseParallelism
            o.DeleteObsoleteFilesPeriodMicros = 1_000_000ul * 3600;
            o.InfoLogLevel = InfoLogLevel.Info;
            o.MaxSubcompactions = (uint)Math.Max(2, procCount / 4);
            o.MaxBackgroundJobs = Math.Max(2, procCount / 2);
            o.DumpMallocStats = true;
        }

        o.BytesPerSync = 1_048_576;
        o.WalBytesPerSync = 1048576;
        o.MaxFileOpeningThreads = LowResources ? 2 : Math.Max(2, procCount / 2);
        o.WalTtlSeconds = WalArchiveTtlNeverSeconds;
        o.WalSizeLimitMb = 0;
        o.MaxLogFileSize = LowResources ? 1UL * 1024 * 1024 : 64UL * 1024 * 1024;
        o.KeepLogFileNum = 10;
        o.MaxTotalWalSize = LowResources ? 1UL * 1024 * 1024 : 1024UL * 1024 * 1024;
        o.SkipStatsUpdateOnDbOpen = true;
        o.MaxFileOpeningThreads = Math.Max(procCount, 16); // Mosaik uses Environment.ProcessorCount here
        o.WalRecoveryMode = WalRecoveryMode.SkipAnyCorruptedRecords;
        return o;
    }

    /// <summary>RocksDbBase.CreateTableOptions.</summary>
    private BlockBasedTableOptions TableOptions() => new()
    {
        BlockCache = Cache,
        CacheIndexAndFilterBlocks = true,
        CacheIndexAndFilterBlocksWithHighPriority = true,
        PinL0FilterAndIndexBlocksInCache = true,
        IndexType = IndexType.TwoLevelIndexSearch,
        PartitionFilters = true,
        PinTopLevelIndexAndFilter = true,
    };

    /// <summary>RocksDbBase.CreateTableOptionsWithBloom: BloomFilterPolicy.Create(10, false).</summary>
    private BlockBasedTableOptions TableOptionsWithBloom()
    {
        BlockBasedTableOptions t = TableOptions();
        t.FilterPolicy = new BloomFilterPolicy(10);
        return t;
    }

    /// <summary>RocksDbBase.CreateColumnFamilyOptions.</summary>
    private ColumnFamilyOptions ColumnFamilyOptions() => new() { TableOptions = TableOptions() };

    /// <summary>RocksDbBase.CreatePointLookupColumnFamilyOptions.</summary>
    private ColumnFamilyOptions PointLookupColumnFamilyOptions()
    {
        BlockBasedTableOptions t = TableOptionsWithBloom();
        t.DataBlockIndexType = DataBlockIndexType.BinaryAndHash;
        t.DataBlockHashTableUtilRatio = 0.75;
        return new ColumnFamilyOptions
        {
            TableOptions = t,
            MemtablePrefixBloomSizeRatio = 0.02,
            MemtableWholeKeyFiltering = true,
        };
    }
}
