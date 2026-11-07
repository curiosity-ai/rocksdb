using System.Text.Json;
using System.Text.Json.Serialization;
using RocksDb.Managed.Cache;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Table;
using RocksDb.Managed.Table.Filters;

namespace RocksDb.Managed.Soak;

/// <summary>How a RUN phase ends (the nemesis of the cycle).</summary>
internal enum FaultKind
{
    /// <summary>SIGKILL at a random moment.</summary>
    Kill,
    /// <summary>Clean shutdown (Dispose) on a supervisor command.</summary>
    Graceful,
    /// <summary>In-process LazyFS-style clear-cache: all unsynced data and directory entries are dropped, then _exit.</summary>
    PowerLoss,
    /// <summary>One unsynced write is split in parts and only some parts persist (LazyFS torn-op).</summary>
    TornOp,
    /// <summary>A non-prefix subset of consecutive unsynced writes to one file persists (LazyFS torn-seq).</summary>
    TornSeq,
    /// <summary>clear-cache right after the N-th fsync of a file type (WAL, MANIFEST, SST).</summary>
    FsyncTrigger,
    /// <summary>SIGKILL, then the next open is killed during recovery (a third process verifies).</summary>
    RecoveryKill,
    /// <summary>SIGSTOP / SIGCONT pauses during the run, then SIGKILL.</summary>
    Pause,
    /// <summary>Random write, sync and read errors from the in-process fault file system, then Resume, then a clean or killed end.</summary>
    IoError,
    /// <summary>Real LazyFS: SIGKILL, then lazyfs::clear-cache through the FIFO (only with --lazyfs-fifo / --lazyfs-bin).</summary>
    LazyFsClear,
}

/// <summary>What VERIFY may assume survived.</summary>
internal enum Durability
{
    /// <summary>The OS kept everything: every acknowledged write survives.</summary>
    AllAcked,
    /// <summary>Power loss: every write acknowledged before a successful sync (or as a sync write) survives.</summary>
    Synced,
    /// <summary>No durability claim (used for damaged copies).</summary>
    None,
}

/// <summary>Per-database choices, fixed for the whole run (derived from the seed).</summary>
internal sealed class DbChoices
{
    public CompressionType Compression { get; set; }
    public uint FormatVersion { get; set; }
    public string Filter { get; set; } = "bloom";
    public ChecksumType Checksum { get; set; }
    public ulong BlockSize { get; set; }
    public bool BlobFiles { get; set; }

    public static DbChoices FromSeed(ulong seed)
    {
        var r = new Random((int)(Keys.Mix(seed ^ 0xD8) & 0x7FFFFFFF));
        CompressionType[] comps = [CompressionType.None, CompressionType.Snappy, CompressionType.LZ4, CompressionType.Zstd];
        ChecksumType[] sums = [ChecksumType.Crc32c, ChecksumType.XxHash, ChecksumType.XxHash64, ChecksumType.Xxh3];
        string[] filters = ["none", "bloom", "ribbon"];
        ulong[] blocks = [1024, 4096, 16384];
        return new DbChoices
        {
            Compression = comps[r.Next(comps.Length)],
            FormatVersion = (uint)r.Next(2, 8),
            Filter = filters[r.Next(filters.Length)],
            Checksum = sums[r.Next(sums.Length)],
            BlockSize = blocks[r.Next(blocks.Length)],
            BlobFiles = r.Next(2) == 0,
        };
    }

    public override string ToString() =>
        $"compression={Compression} format_version={FormatVersion} filter={Filter} checksum={Checksum} block_size={BlockSize} blob_files(reg1)={BlobFiles}";
}

/// <summary>Options varied per cycle (all safe to change across a reopen).</summary>
internal sealed class CycleOptions
{
    public ulong WriteBufferSize { get; set; }
    public int MaxBackgroundJobs { get; set; }
    public ulong CompactionReadaheadSize { get; set; }
    public long BlockCacheSize { get; set; }
    public ulong RecycleLogFileNum { get; set; }
    public ulong TargetFileSizeBase { get; set; }
    public ulong MaxBytesForLevelBase { get; set; }
    public int Level0Trigger { get; set; }
    public uint MaxSubcompactions { get; set; }

    public static CycleOptions FromSeed(ulong cycleSeed)
    {
        var r = new Random((int)(Keys.Mix(cycleSeed ^ 0x0C) & 0x7FFFFFFF));
        ulong[] wbs = [64 << 10, 128 << 10, 256 << 10, 512 << 10, 1 << 20];
        ulong[] ra = [0, 256 << 10, 2 << 20];
        long[] cache = [1 << 20, 8 << 20, 32 << 20];
        ulong[] tfs = [256 << 10, 512 << 10, 1 << 20, 2 << 20];
        return new CycleOptions
        {
            WriteBufferSize = wbs[r.Next(wbs.Length)],
            MaxBackgroundJobs = r.Next(1, 5),
            CompactionReadaheadSize = ra[r.Next(ra.Length)],
            BlockCacheSize = cache[r.Next(cache.Length)],
            RecycleLogFileNum = r.Next(10) < 3 ? 1UL : 0UL,
            TargetFileSizeBase = tfs[r.Next(tfs.Length)],
            MaxBytesForLevelBase = (ulong)r.Next(1, 5) << 20,
            Level0Trigger = r.Next(2, 7),
            MaxSubcompactions = (uint)r.Next(1, 3),
        };
    }

    public override string ToString() =>
        $"write_buffer_size={WriteBufferSize} max_background_jobs={MaxBackgroundJobs} compaction_readahead_size={CompactionReadaheadSize} " +
        $"block_cache={BlockCacheSize} recycle_log_file_num={RecycleLogFileNum} target_file_size_base={TargetFileSizeBase} " +
        $"max_bytes_for_level_base={MaxBytesForLevelBase} level0_file_num_compaction_trigger={Level0Trigger} max_subcompactions={MaxSubcompactions}";
}

/// <summary>The run configuration (run.json in the run directory).</summary>
internal sealed class RunConfig
{
    public ulong Seed { get; set; }
    public Layout Layout { get; set; } = new();
    public DbChoices Db { get; set; } = new();
    public Dictionary<string, int> FaultWeights { get; set; } = DefaultWeights();
    /// <summary>Mean RUN phase length (exponential, clamped to [MinRunSeconds, MaxRunSeconds]).</summary>
    public double MeanRunSeconds { get; set; } = 40;
    public double MinRunSeconds { get; set; } = 3;
    public double MaxRunSeconds { get; set; } = 600;
    /// <summary>Native cross-check every N cycles (0 = never; always at the end).</summary>
    public int NativeEvery { get; set; } = 10;
    /// <summary>File-corruption trial on a copy every N cycles (0 = never).</summary>
    public int CorruptEvery { get; set; } = 3;
    /// <summary>Histories of the last N successful cycles are kept (for export-history).</summary>
    public int KeepHistory { get; set; } = 20;
    /// <summary>A FIFO of a user-mounted LazyFS (the DB directory must be on that mount), or null.</summary>
    public string? LazyFsFifo { get; set; }
    /// <summary>The completion FIFO of that LazyFS (fifo_path_completed), or null.</summary>
    public string? LazyFsFifoCompleted { get; set; }
    /// <summary>A lazyfs binary: the supervisor mounts LazyFS itself (Jepsen's jepsen.lazyfs layout), or null.</summary>
    public string? LazyFsBinary { get; set; }
    /// <summary>
    /// Test-only (<c>--unsafe-skip-fsync</c>, LazyFS mode): the worker drops every fsync, so the run must FAIL at the
    /// first durability check after a clear-cache. Proves the LazyFS mode catches a missing fsync.
    /// </summary>
    public bool UnsafeSkipFsync { get; set; }
    public string StartedUtc { get; set; } = "";

    public static Dictionary<string, int> DefaultWeights() => new()
    {
        [nameof(FaultKind.Kill)] = 30,
        [nameof(FaultKind.Graceful)] = 8,
        [nameof(FaultKind.PowerLoss)] = 12,
        [nameof(FaultKind.TornOp)] = 7,
        [nameof(FaultKind.TornSeq)] = 7,
        [nameof(FaultKind.FsyncTrigger)] = 8,
        [nameof(FaultKind.RecoveryKill)] = 8,
        [nameof(FaultKind.Pause)] = 6,
        [nameof(FaultKind.IoError)] = 8,
        [nameof(FaultKind.LazyFsClear)] = 0,
    };

    public bool UsesLazyFs => LazyFsFifo is not null || LazyFsBinary is not null;

    private static readonly JsonSerializerOptions s_json = new()
    {
        WriteIndented = true,
        Converters = { new JsonStringEnumConverter() },
    };

    public static JsonSerializerOptions Json => s_json;

    public void Save(string path) => File.WriteAllText(path, JsonSerializer.Serialize(this, s_json));

    public static RunConfig Load(string path) =>
        JsonSerializer.Deserialize<RunConfig>(File.ReadAllText(path), s_json) ?? throw new InvalidDataException(path);
}

/// <summary>The plan of one cycle (cycle.json, read by the worker and the verifier).</summary>
internal sealed class CyclePlan
{
    public int Cycle { get; set; }
    public ulong CycleSeed { get; set; }
    public FaultKind Fault { get; set; }
    public double RunSeconds { get; set; }
    /// <summary>The worker runs on the in-process fault file system.</summary>
    public bool FaultFs { get; set; }
    /// <summary>Test-only: the fault file system ignores fsync (data never becomes durable).</summary>
    public bool SkipSync { get; set; }
    /// <summary>Test-only: fsyncs are not passed to the real file system either (see <see cref="RunConfig.UnsafeSkipFsync"/>).</summary>
    public bool SkipRealSync { get; set; }
    public CycleOptions Options { get; set; } = new();
    public bool NativeCheck { get; set; }
    public bool CorruptTrial { get; set; }

    public Durability Durability => Fault switch
    {
        FaultKind.PowerLoss or FaultKind.TornOp or FaultKind.TornSeq or FaultKind.FsyncTrigger or FaultKind.LazyFsClear => Durability.Synced,
        _ => Durability.AllAcked,
    };

    public void Save(string path) => File.WriteAllText(path, JsonSerializer.Serialize(this, RunConfig.Json));

    public static CyclePlan Load(string path) =>
        JsonSerializer.Deserialize<CyclePlan>(File.ReadAllText(path), RunConfig.Json) ?? throw new InvalidDataException(path);
}

/// <summary>Paths inside a run directory.</summary>
internal sealed class RunPaths(string root)
{
    public string Root { get; } = Path.GetFullPath(root);
    public string Db => Path.Combine(Root, "db");
    public string RunJson => Path.Combine(Root, "run.json");
    public string ModelFile => Path.Combine(Root, "model.bin");
    public string NextModelFile => Path.Combine(Root, "model-next.bin");
    public string CycleJson => Path.Combine(Root, "cycle.json");
    public string Status => Path.Combine(Root, "status.json");
    public string Log => Path.Combine(Root, "soak.log");
    public string Logs => Path.Combine(Root, "logs");
    public string Histories => Path.Combine(Root, "history");
    public string Verify => Path.Combine(Root, "verify");
    public string Failures => Path.Combine(Root, "failures");
    public string Tmp => Path.Combine(Root, "tmp");
    public string Corrupt => Path.Combine(Root, "corrupt");

    /// <summary>History of one worker process of a cycle (a cycle with a crash during recovery has two).</summary>
    public string History(int cycle, int part) => Path.Combine(Histories, $"cycle-{cycle:D6}-p{part}.bin");

    /// <summary>All history parts of a cycle, in order.</summary>
    public List<string> HistoryParts(int cycle)
    {
        var parts = new List<string>();
        for (int p = 0; File.Exists(History(cycle, p)); p++) parts.Add(History(cycle, p));
        return parts;
    }
    public string VerifyJson(int cycle) => Path.Combine(Verify, $"cycle-{cycle:D6}.json");
    public string LogFile(int cycle, string what) => Path.Combine(Logs, $"cycle-{cycle:D6}-{what}.log");

    public void CreateDirectories()
    {
        foreach (string d in new[] { Root, Logs, Histories, Verify, Tmp }) Directory.CreateDirectory(d);
    }
}

/// <summary>Builds the DB options of a run / cycle.</summary>
internal static class DbSetup
{
    public static DbOptions DbOptions(CycleOptions c, Env.IFileSystem fs)
    {
        return new DbOptions
        {
            CreateIfMissing = true,
            CreateMissingColumnFamilies = true,
            FileSystem = fs,
            MaxBackgroundJobs = c.MaxBackgroundJobs,
            CompactionReadaheadSize = c.CompactionReadaheadSize,
            RecycleLogFileNum = c.RecycleLogFileNum,
            MaxSubcompactions = c.MaxSubcompactions,
            FileChecksumGenFactory = FileChecksumGenCrc32cFactory.Instance,
            ParanoidChecks = true,
        };
    }

    public static ColumnFamilyOptions CfOptions(string name, DbChoices d, CycleOptions c, BlockCache cache)
    {
        var o = new ColumnFamilyOptions
        {
            Compression = d.Compression,
            WriteBufferSize = c.WriteBufferSize,
            TargetFileSizeBase = c.TargetFileSizeBase,
            MaxBytesForLevelBase = c.MaxBytesForLevelBase,
            Level0FileNumCompactionTrigger = c.Level0Trigger,
            TableOptions = new BlockBasedTableOptions
            {
                FormatVersion = d.FormatVersion,
                Checksum = d.Checksum,
                BlockSize = d.BlockSize,
                BlockCache = cache,
                FilterPolicy = d.Filter switch
                {
                    "bloom" => new BloomFilterPolicy(10),
                    "ribbon" => new RibbonFilterPolicy(10, 1),
                    _ => null,
                },
            },
        };
        if (name is Keys.Bank or Keys.Ctr) o.MergeOperator = MergeOperator.UInt64Add;
        if (name == Keys.Reg1 && d.BlobFiles)
        {
            o.EnableBlobFiles = true;
            o.MinBlobSize = 1024;
            o.EnableBlobGarbageCollection = true;
        }
        return o;
    }

    /// <summary>Column family descriptors for every column family on disk (plus the fixed ones).</summary>
    public static List<ColumnFamilyDescriptor> Descriptors(string path, Env.IFileSystem fs, DbChoices d, CycleOptions c, BlockCache cache)
    {
        var names = new List<string>(Keys.FixedColumnFamilies);
        if (fs.FileExists(Path.Combine(path, "CURRENT")))
        {
            foreach (string n in RocksDatabase.ListColumnFamilies(path, fs))
                if (!names.Contains(n)) names.Add(n);
        }
        var list = new List<ColumnFamilyDescriptor>();
        foreach (string n in names) list.Add(new ColumnFamilyDescriptor(n, CfOptions(n, d, c, cache)));
        return list;
    }
}
