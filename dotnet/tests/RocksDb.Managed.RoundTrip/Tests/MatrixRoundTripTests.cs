using System.Globalization;
using RocksDb.Managed.Compression;
using RocksDb.Managed.RoundTrip.Engines;
using RocksDb.Managed.RoundTrip.Harness;
using RocksDb.Managed.RoundTrip.Protocol;
using RocksDb.Managed.Table;

namespace RocksDb.Managed.RoundTrip.Tests;

/// <summary>
/// Combinatorial managed &lt;-&gt; native alternation. 32 factors that matter for on-disk compatibility and recovery
/// (table format, index and filter layout, checksums, compression, LSM shape and compaction style, memtable
/// representation, WAL and write-path options, merge operators, blob files, ingestion, manual compaction, data
/// pattern, how the writer ends) are crossed by a seeded pairwise covering array (<see cref="Pairwise"/>): every pair of
/// levels of every two factors occurs in at least one case. Each case alternates native, managed, native, managed on
/// one DB (the managed runs optionally with different but compatible options), every run first verifies what the other
/// engine left, and both engines verify the final state. Combinations upstream rejects are excluded
/// (<see cref="Excluded"/>). <c>ROUNDTRIP_LONG=1</c> adds <see cref="LongRandomRows"/> random rows.
/// </summary>
[TestClass]
public sealed class MatrixRoundTripTests : RoundTripTestBase
{
    /// <summary>Seed of the covering array (part of the case identity: changing it changes every case).</summary>
    public const int ArraySeed = 20261106;

    public const int LongRandomRows = 200;

    private enum F
    {
        Pattern, FormatVersion, Index, Filter, Checksum, Block, Compression, Compaction, Pri, Memtable, WriteBuffers,
        RecoveryMode, Recycle, ManualWalFlush, WalCompression, TrackWals, MaxTotalWal, AvoidFlush, WriteMode, Flushes, NoWal,
        SyncRate, ManyCfs, ReopenDifferent, Subcompactions, WalDir, FilterTuning, Merge, Blob, Ingest, ManualCompaction, EndMode,
    }

    private static readonly string[][] s_levelNames =
    [
        ["Random", "Sequential", "Reverse", "HotKeys", "LongPrefix", "TombstoneHeavy", "MergeHeavy", "ValueSizes"],
        ["fv2", "fv3", "fv4", "fv5", "fv6", "fv7"],
        ["idx-binary", "idx-hash", "idx-2level", "idx-2level+partfilters", "idx-binary-firstkey"],
        ["bloom", "ribbon", "bloom+fixed4", "bloom+fixed3-nowkf+membloom", "ribbon+capped5+membloom+memwkf", "nofilter"],
        ["crc32c", "xxhash", "xxhash64", "xxh3"],
        ["bs4K", "bs1K-restart1-datahash", "bs4K-restart4-idxrestart8", "bs16K-idxrestart4-datahash"],
        ["none", "snappy", "lz4+bottom-zstd", "zstd-level5", "perlevel+bottom-zstd-dict", "zstd-dict+pthreads3"],
        ["level-dyn-small", "level-static-L4-small", "level-default", "universal-r1-amp50-trivial", "universal-r10-amp200", "fifo", "fifo-compact-ttl"],
        ["pri-compensated", "pri-oldest-largest", "pri-oldest-smallest", "pri-min-overlap", "pri-round-robin"],
        ["skiplist", "skiplist-inplace", "vector", "hash-skiplist"],
        ["wb64K", "wb128K-max4-merge2", "wb64M", "wb32K-max3"],
        ["tolerate-tail", "absolute", "point-in-time", "skip-any"],
        ["norecycle", "recycle2"],
        ["auto-wal-flush", "manual-wal-flush"],
        ["wal-plain", "wal-zstd"],
        ["notrack", "track-wals", "track-wals-in-manifest"],
        ["maxwal-default", "maxwal64K"],
        ["flush-on-recovery+shutdown", "avoid-flush-recovery", "avoid-flush-shutdown", "avoid-flush-both"],
        ["write-default", "pipelined", "unordered", "two-queues", "no-concurrent-memtable"],
        ["flush-random", "flush-none", "flush-every-n", "flush-all-atomic"],
        ["wal-only", "nowal-mix"],
        ["sync-rare", "sync-often"],
        ["4cfs", "10cfs"],
        ["same-options", "reopen-different-options"],
        ["subcomp1", "subcomp4"],
        ["waldir-db", "waldir-separate"],
        ["filters-plain", "optimize-filters-for-hits", "cache-index-filter", "ofh+cache-index-filter"],
        ["merge-stringappend", "merge-stringappend-pipe", "merge-max", "merge-bytesxor"],
        ["noblob", "blob"],
        ["noingest", "ingest-global-seqno", "ingest-behind", "ingest-no-global-seqno"],
        ["compact-plain", "compact-bottommost-exclusive", "compact-change-level", "delete-files-in-range"],
        ["end-close", "end-reopen-mid-run", "end-abandon"],
    ];

    /// <summary>Levels per factor for the covering array. The data pattern is not crossed pairwise (its 8 levels would
    /// dominate the array size); row i uses pattern i mod 8, so every pattern runs in several configurations.</summary>
    private static readonly int[] s_levels = Levels();

    private static int[] Levels()
    {
        int[] l = new int[s_levelNames.Length];
        for (int i = 0; i < l.Length; i++) l[i] = s_levelNames[i].Length;
        l[(int)F.Pattern] = 1;
        return l;
    }

    /// <summary>Filter levels whose prefix extractor has every key in its domain (the capped prefix). Upstream's hash index
    /// (BlockPrefixIndex, HashIndexBuilder) and hash skip list call Transform without checking InDomain, so a fixed prefix
    /// longer than a key reads past the key (undefined behaviour; native 11.12 then misses such keys on Get).</summary>
    private static bool AllKeysInDomain(int filter) => filter == 4;

    /// <summary>
    /// Combinations upstream rejects (InvalidArgument / NotSupported), that native 11.12 cannot run, or that are outside
    /// the model: a hash index or hash skip list memtable without an all-keys-in-domain prefix extractor
    /// (<see cref="AllKeysInDomain"/>); unordered_write without concurrent memtable writes (in-place updates, vector and
    /// hash memtables cannot be written concurrently); unordered_write with manual_wal_flush (native self-deadlock:
    /// ConcurrentWriteGroupToWAL holds wal_write_mutex_ and WriteToWAL locks it again when manual_wal_flush_ and not
    /// two_write_queues_, db/db_impl/db_impl_write.cc, also on main); atomic_flush (the flush-all-atomic level) with
    /// pipelined writes; disableWAL writes with recycle_log_file_num &gt; 0; FIFO with blob files, ingest_behind or
    /// allow_global_seqno=false (FIFO ingests into L0 with a global seqno) or change_level.
    /// </summary>
    private static bool Excluded(int a, int x, int b, int y)
    {
        if (a > b) return Excluded(b, y, a, x);
        var fa = (F)a;
        var fb = (F)b;
        if (fa == F.Index && fb == F.Filter) return x == 1 && !AllKeysInDomain(y);
        if (fa == F.Filter && fb == F.Memtable) return y == 3 && !AllKeysInDomain(x);
        if (fa == F.Memtable && fb == F.WriteMode) return x != 0 && y == 2;
        if (fa == F.WriteMode && fb == F.Flushes) return x == 1 && y == 3;
        if (fa == F.ManualWalFlush && fb == F.WriteMode) return x == 1 && y == 2;
        if (fa == F.Recycle && fb == F.NoWal) return x == 1 && y == 1;
        if (fa == F.Compaction && fb == F.Blob) return x >= 5 && y == 1;
        if (fa == F.Compaction && fb == F.Ingest) return x >= 5 && y >= 2;
        if (fa == F.Compaction && fb == F.ManualCompaction) return x >= 5 && y == 2;
        // change_level moves the ingested-behind (sequence number 0) files of the last level up to L1; a later
        // ingest_behind then fails ("files with 0 seqno ... at upper levels") unless a background compaction has
        // already moved them back, which is timing-dependent in both engines.
        if (fa == F.Ingest && fb == F.ManualCompaction) return x == 2 && y == 2;
        return false;
    }

    private static void Repair(int[] r)
    {
        if (Excluded((int)F.Index, r[(int)F.Index], (int)F.Filter, r[(int)F.Filter])) r[(int)F.Filter] = 4;
        if (Excluded((int)F.Filter, r[(int)F.Filter], (int)F.Memtable, r[(int)F.Memtable])) r[(int)F.Memtable] = 0;
        if (Excluded((int)F.Memtable, r[(int)F.Memtable], (int)F.WriteMode, r[(int)F.WriteMode])) r[(int)F.WriteMode] = 0;
        if (Excluded((int)F.WriteMode, r[(int)F.WriteMode], (int)F.Flushes, r[(int)F.Flushes])) r[(int)F.WriteMode] = 0;
        if (Excluded((int)F.ManualWalFlush, r[(int)F.ManualWalFlush], (int)F.WriteMode, r[(int)F.WriteMode])) r[(int)F.WriteMode] = 0;
        if (Excluded((int)F.Recycle, r[(int)F.Recycle], (int)F.NoWal, r[(int)F.NoWal])) r[(int)F.NoWal] = 0;
        if (Excluded((int)F.Compaction, r[(int)F.Compaction], (int)F.Blob, r[(int)F.Blob])) r[(int)F.Blob] = 0;
        if (Excluded((int)F.Compaction, r[(int)F.Compaction], (int)F.Ingest, r[(int)F.Ingest])) r[(int)F.Ingest] = 1;
        if (Excluded((int)F.Compaction, r[(int)F.Compaction], (int)F.ManualCompaction, r[(int)F.ManualCompaction])) r[(int)F.ManualCompaction] = 0;
        if (Excluded((int)F.Ingest, r[(int)F.Ingest], (int)F.ManualCompaction, r[(int)F.ManualCompaction])) r[(int)F.ManualCompaction] = 0;
    }

    private static List<int[]>? s_rows;

    /// <summary>The pairwise rows, then (ROUNDTRIP_LONG=1) random valid rows.</summary>
    internal static List<int[]> Rows()
    {
        if (s_rows is not null) return s_rows;
        var rows = Pairwise.Generate(s_levels, ArraySeed, Excluded, Repair);
        if (LongRuns)
        {
            var rnd = new Random(ArraySeed + 1);
            for (int i = 0; i < LongRandomRows; i++)
            {
                var row = new int[s_levels.Length];
                for (int j = 0; j < row.Length; j++) row[j] = rnd.Next(s_levels[j]);
                Repair(row);
                rows.Add(row);
            }
        }
        int patterns = s_levelNames[(int)F.Pattern].Length;
        for (int i = 0; i < rows.Count; i++) rows[i][(int)F.Pattern] = i % patterns;
        return s_rows = rows;
    }

    public static string Describe(int[] row)
    {
        var parts = new string[row.Length];
        for (int i = 0; i < row.Length; i++) parts[i] = s_levelNames[i][row[i]];
        return string.Join(",", parts);
    }

    public static IEnumerable<object[]> Cases
    {
        get
        {
            List<int[]> rows = Rows();
            for (int i = 0; i < rows.Count; i++) yield return [i, Describe(rows[i])];
        }
    }

    public static string CaseName(System.Reflection.MethodInfo method, object?[] data) =>
        $"{method.Name}({((int)data[0]!).ToString("D3", CultureInfo.InvariantCulture)}: {data[1]})";

    // ---------------------------------------------------------------------------------------------------------
    // row -> specs

    private sealed record Case(DbSpec Spec, DbSpec ManagedSpec, GeneratorOptions Gen, int EndMode);

    private static readonly MergeKind[] s_merges = [MergeKind.StringAppend, MergeKind.StringAppend, MergeKind.Max, MergeKind.BytesXor];

    /// <summary>Applies the row's CF-level factors to <paramref name="c"/> (merge level <paramref name="merge"/>).</summary>
    private static void ApplyCf(CfSpec c, int[] r, int merge)
    {
        c.FormatVersion = 2 + r[(int)F.FormatVersion];
        switch (r[(int)F.Index])
        {
            case 0: c.Index = IndexType.BinarySearch; break;
            case 1: c.Index = IndexType.HashSearch; break;
            case 2: c.Index = IndexType.TwoLevelIndexSearch; break;
            case 3: c.Index = IndexType.TwoLevelIndexSearch; c.PartitionFilters = true; break;
            case 4: c.Index = IndexType.BinarySearchWithFirstKey; break;
        }
        switch (r[(int)F.Filter])
        {
            case 0: c.Filter = FilterKind.Bloom; break;
            case 1: c.Filter = FilterKind.Ribbon; break;
            case 2: c.Filter = FilterKind.Bloom; c.Prefix = PrefixKind.Fixed; c.PrefixLength = 4; break;
            case 3: c.Filter = FilterKind.Bloom; c.Prefix = PrefixKind.Fixed; c.PrefixLength = 3; c.WholeKeyFiltering = false; c.MemtablePrefixBloomRatio = 0.1; break;
            case 4:
                c.Filter = FilterKind.Ribbon; c.Prefix = PrefixKind.Capped; c.PrefixLength = 5; c.MemtablePrefixBloomRatio = 0.05;
                c.MemtableWholeKeyFiltering = true;
                break;
            case 5: c.Filter = FilterKind.None; break;
        }
        c.Checksum = (ChecksumType)(1 + r[(int)F.Checksum]);
        switch (r[(int)F.Block])
        {
            case 0: c.BlockSize = 4096; break;
            case 1: c.BlockSize = 1024; c.BlockRestartInterval = 1; c.DataBlockHashIndex = true; break;
            case 2: c.BlockSize = 4096; c.BlockRestartInterval = 4; c.IndexBlockRestartInterval = 8; break;
            case 3: c.BlockSize = 16 * 1024; c.IndexBlockRestartInterval = 4; c.DataBlockHashIndex = true; break;
        }
        switch (r[(int)F.Compression])
        {
            case 0: c.Compression = CompressionType.None; break;
            case 1: c.Compression = CompressionType.Snappy; break;
            case 2: c.Compression = CompressionType.LZ4; c.BottommostCompression = CompressionType.Zstd; break;
            case 3: c.Compression = CompressionType.Zstd; c.CompressionLevel = 5; break;
            case 4:
                c.Compression = CompressionType.LZ4;
                c.CompressionPerLevel = [CompressionType.None, CompressionType.LZ4, CompressionType.Snappy, CompressionType.LZ4, CompressionType.Zstd, CompressionType.Zstd, CompressionType.Zstd];
                c.BottommostCompression = CompressionType.Zstd;
                c.MaxDictBytes = 8 * 1024;
                c.ZstdMaxTrainBytes = 64 * 1024;
                c.BottommostDictionary = true;
                break;
            case 5: c.Compression = CompressionType.Zstd; c.MaxDictBytes = 16 * 1024; c.ZstdMaxTrainBytes = 128 * 1024; c.ParallelThreads = 3; break;
        }
        switch (r[(int)F.Compaction])
        {
            case 0: c.TargetFileSizeBase = 64 << 10; c.MaxBytesForLevelBase = 256 << 10; c.Level0FileNumCompactionTrigger = 2; break;
            case 1: c.DynamicLevelBytes = false; c.NumLevels = 4; c.TargetFileSizeBase = 32 << 10; c.MaxBytesForLevelBase = 128 << 10; c.Level0FileNumCompactionTrigger = 3; break;
            case 2: break;
            case 3:
                c.Style = CompactionStyle.Universal; c.UniversalSizeRatio = 1; c.UniversalMaxSizeAmplificationPercent = 50; c.UniversalAllowTrivialMove = true;
                c.Level0FileNumCompactionTrigger = 2;
                break;
            case 4: c.Style = CompactionStyle.Universal; c.UniversalSizeRatio = 10; c.NumLevels = 5; break;
            case 5: c.Style = CompactionStyle.Fifo; c.NumLevels = 1; break;
            case 6: c.Style = CompactionStyle.Fifo; c.NumLevels = 1; c.FifoAllowCompaction = true; c.Ttl = 86400; c.Level0FileNumCompactionTrigger = 3; break;
        }
        c.Pri = (CompactionPri)r[(int)F.Pri];
        switch (r[(int)F.Memtable])
        {
            case 1: c.InplaceUpdateSupport = true; break;
            case 2: c.Memtable = MemtableKind.Vector; break;
            case 3: c.Memtable = MemtableKind.HashSkipList; break;
        }
        switch (r[(int)F.WriteBuffers])
        {
            case 0: c.WriteBufferSize = 64 << 10; break;
            case 1: c.WriteBufferSize = 128 << 10; c.MaxWriteBufferNumber = 4; c.MinWriteBufferNumberToMerge = 2; break;
            case 2: c.WriteBufferSize = 64UL << 20; break;
            case 3: c.WriteBufferSize = 32 << 10; c.MaxWriteBufferNumber = 3; break;
        }
        switch (r[(int)F.FilterTuning])
        {
            case 1: c.OptimizeFiltersForHits = true; break;
            case 2: c.CacheIndexAndFilterBlocks = true; break;
            case 3: c.OptimizeFiltersForHits = true; c.CacheIndexAndFilterBlocks = true; break;
        }
        c.Merge = s_merges[merge % s_merges.Length];
        c.MergeDelimiter = merge % s_merges.Length == 1 ? "|" : ",";
        // Not with the max operator: upstream DBIter::MergeWithBlobBaseValue frees the blob value (blob_state_.Reset())
        // after a merge whose result is the base value itself (MaxOperator returns existing_operand, which
        // MergeHelper::TimedFullMerge hands back as pinned_value_), so native iterators return freed memory for such
        // keys (db/db_iter.cc, 11.12 and main). The managed iterator copies the value.
        if (r[(int)F.Blob] == 1 && c.Merge != MergeKind.Max)
        {
            c.EnableBlobFiles = true;
            c.MinBlobSize = 128;
            c.BlobFileSize = 256 << 10;
            c.BlobCompression = r[(int)F.Compression] == 0 ? CompressionType.None : CompressionType.LZ4;
            c.EnableBlobGc = merge % 2 == 0;
            c.BlobGcAgeCutoff = 0.5;
        }
    }

    private static Case Build(int[] r)
    {
        DbSpec spec = DbSpec.Standard(CompressionType.LZ4, 6, FilterKind.Bloom, 64 << 10, false);
        int mergeLevel = r[(int)F.Merge];
        ApplyCf(spec.Cf("default"), r, mergeLevel);
        ApplyCf(spec.Cf("snappy"), r, mergeLevel + 1);
        ApplyCf(spec.Cf("late"), r, mergeLevel + 2);
        // "counters" (uint64add) and "blobs" keep their fixed settings but share the compaction style (one style per
        // DB keeps the case readable) and the write buffers.
        foreach (string fixedCf in new[] { "counters", "blobs" })
        {
            CfSpec c = spec.Cf(fixedCf);
            var tmp = new CfSpec();
            ApplyCf(tmp, r, 0);
            c.Style = tmp.Style;
            c.NumLevels = tmp.NumLevels;
            c.FifoAllowCompaction = tmp.FifoAllowCompaction;
            c.Ttl = tmp.Ttl;
            c.WriteBufferSize = Math.Max(c.WriteBufferSize, tmp.WriteBufferSize);
        }
        if (r[(int)F.ManyCfs] == 1)
        {
            for (int i = 0; i < 6; i++)
            {
                var c = new CfSpec { Name = "cf" + (5 + i).ToString("D2", CultureInfo.InvariantCulture) };
                ApplyCf(c, r, mergeLevel + 3 + i);
                // Vary the table settings per CF around the row's values.
                c.FormatVersion = 2 + (c.FormatVersion - 2 + i + 1) % 6;
                c.Checksum = (ChecksumType)(1 + ((int)c.Checksum - 1 + i) % 4);
                c.Compression = (CompressionType)new[] { CompressionType.None, CompressionType.Snappy, CompressionType.LZ4, CompressionType.Zstd }[i % 4];
                if (i == 2) c.Merge = MergeKind.UInt64Add;
                if (i == 4) c.Merge = MergeKind.None;
                spec.ColumnFamilies.Add(c);
            }
        }

        DbWideSpec d = spec.Db;
        d.WalRecoveryMode = (WalRecoveryMode)r[(int)F.RecoveryMode];
        d.RecycleLogFileNum = r[(int)F.Recycle] == 1 ? 2UL : 0;
        d.ManualWalFlush = r[(int)F.ManualWalFlush] == 1;
        d.WalCompression = r[(int)F.WalCompression] == 1 ? CompressionType.Zstd : CompressionType.None;
        d.TrackAndVerifyWals = r[(int)F.TrackWals] == 1;
        d.TrackAndVerifyWalsInManifest = r[(int)F.TrackWals] == 2;
        d.MaxTotalWalSize = r[(int)F.MaxTotalWal] == 1 ? 64UL << 10 : 0;
        d.AvoidFlushDuringRecovery = r[(int)F.AvoidFlush] is 1 or 3;
        d.AvoidFlushDuringShutdown = r[(int)F.AvoidFlush] is 2 or 3;
        switch (r[(int)F.WriteMode])
        {
            case 1: d.EnablePipelinedWrite = true; break;
            case 2: d.UnorderedWrite = true; break;
            case 3: d.TwoWriteQueues = true; break;
            case 4: d.AllowConcurrentMemtableWrite = false; break;
        }
        // In-place updates, vector and hash skip list memtables cannot be written concurrently (upstream
        // CheckConcurrentWritesSupported).
        if (r[(int)F.Memtable] != 0) d.AllowConcurrentMemtableWrite = false;
        d.AtomicFlush = r[(int)F.Flushes] == 3;
        d.MaxSubcompactions = r[(int)F.Subcompactions] == 1 ? 4u : 1u;
        d.SeparateWalDir = r[(int)F.WalDir] == 1;
        d.AllowIngestBehind = r[(int)F.Ingest] == 2;

        int end = r[(int)F.EndMode];
        var gen = new GeneratorOptions
        {
            Ops = LongRuns ? 900 : 300,
            KeySpace = 600,
            Pattern = (DataPattern)r[(int)F.Pattern],
            Flushes = (FlushPattern)r[(int)F.Flushes],
            FlushEvery = 120,
            NoWalWrites = r[(int)F.NoWal] == 1,
            FlushAfterNoWal = d.AvoidFlushDuringShutdown || end == 2,
            FlushWalOps = d.ManualWalFlush,
            SyncRate = r[(int)F.SyncRate] == 1 ? 0.6 : 0.05,
            Ingest = (IngestMode)r[(int)F.Ingest],
            Compaction = r[(int)F.Memtable] == 1 && r[(int)F.ManualCompaction] == 3 ? ManualCompaction.Plain : (ManualCompaction)r[(int)F.ManualCompaction],
            // In-place updates overwrite memtable entries a snapshot could see (upstream documents the conflict).
            AllowSnapshots = r[(int)F.Memtable] != 1,
            // An in-place update keeps the sequence number of the entry it overwrites (MemTable::Update: "existing_seq
            // prevails"), so a range tombstone written between the two puts deletes the new value: no range deletes.
            AllowRangeDelete = r[(int)F.Memtable] != 1,
            LargeValueRate = 0.003,
        };
        gen.AllowFlush = gen.Flushes != FlushPattern.None || gen.NoWalWrites;

        DbSpec managed = spec;
        if (r[(int)F.ReopenDifferent] == 1)
        {
            // Different but compatible options for the managed runs: table format, block size, filter type, index
            // type (not the hash index), checksum, compression and write buffer (merge operators, prefix extractor,
            // compaction style and num_levels stay).
            managed = spec.Clone();
            foreach (CfSpec c in managed.ColumnFamilies)
            {
                c.FormatVersion = c.FormatVersion == 7 ? 5 : c.FormatVersion + 1;
                c.BlockSize *= 2;
                c.Filter = c.Filter == FilterKind.Bloom ? FilterKind.Ribbon : FilterKind.Bloom;
                if (c.Index != IndexType.HashSearch) c.Index = c.Index == IndexType.BinarySearch ? IndexType.TwoLevelIndexSearch : IndexType.BinarySearch;
                c.PartitionFilters = c.Index == IndexType.TwoLevelIndexSearch;
                c.Checksum = c.Checksum == ChecksumType.Xxh3 ? ChecksumType.Crc32c : (ChecksumType)((int)c.Checksum + 1);
                c.Compression = c.Compression == CompressionType.Zstd ? CompressionType.LZ4 : CompressionType.Zstd;
                c.CompressionPerLevel = [];
                c.WriteBufferSize = c.WriteBufferSize >= 1 << 20 ? 96 << 10 : c.WriteBufferSize * 2;
            }
            managed.Db.WalCompression = managed.Db.WalCompression == CompressionType.None ? CompressionType.Zstd : CompressionType.None;
        }
        return new Case(spec, managed, gen, end);
    }

    // ---------------------------------------------------------------------------------------------------------

    [TestMethod]
    public void CoveringArrayCoversEveryAllowedPair()
    {
        List<int[]> rows = Pairwise.Generate(s_levels, ArraySeed, Excluded, Repair);
        List<string> missing = Pairwise.Uncovered(s_levels, rows, Excluded);
        Assert.AreEqual(0, missing.Count, string.Join("; ", missing.Take(20)));
        foreach (int[] row in rows)
        {
            int[] copy = (int[])row.Clone();
            Repair(copy);
            CollectionAssert.AreEqual(row, copy, "row violates a constraint: " + Describe(row));
        }
        TestContext.WriteLine($"{rows.Count} pairwise rows over {s_levels.Length} factors");
    }

    [TestMethod]
    [DynamicData(nameof(Cases), DynamicDataDisplayName = nameof(CaseName))]
    public async Task Matrix(int index, string description)
    {
        int[] row = Rows()[index];
        Assert.AreEqual(Describe(row), description, "covering array changed under the test");
        Case c = Build(row);
        // Fixed seed per case (the op streams are reproducible); ROUNDTRIP_SEED overrides it.
        RoundTripHarness h = CreateHarness(c.Spec, "m" + index.ToString("D3", CultureInfo.InvariantCulture), ArraySeed + 7919 * (index + 1));
        h.Log("case: " + description);
        h.Log("managed spec: " + c.ManagedSpec);
        EngineKind engine = EngineKind.Native;
        int runs = LongRuns ? 6 : 4;
        for (int i = 0; i < runs; i++)
        {
            DbSpec spec = engine == EngineKind.Managed ? c.ManagedSpec : c.Spec;
            ScriptBuilder b = h.NewScript($"{engine}-{i}", h.DbPath, spec, h.Model);
            if (c.EndMode == 1)
            {
                int half = c.Gen.Ops / 2;
                GeneratorOptions g1 = Clone(c.Gen, half);
                OpGenerator.Generate(b, g1);
                b.Add(Op.Reopen()).Verify();
                OpGenerator.Generate(b, Clone(c.Gen, c.Gen.Ops - half));
            }
            else
            {
                OpGenerator.Generate(b, c.Gen);
            }
            b.Verify();
            if (c.EndMode == 2)
            {
                // Process crash after everything reached the OS (FlushWAL(true) also covers manual_wal_flush).
                b.Add(Op.FlushWal(true)).Add(Op.Abandon());
            }
            else
            {
                b.Dump();
            }
            await h.RunAsync(engine, b);
            engine = engine == EngineKind.Native ? EngineKind.Managed : EngineKind.Native;
        }
        // Both engines verify the final state (and compact it fully, then verify again).
        foreach (EngineKind e in new[] { engine, engine == EngineKind.Native ? EngineKind.Managed : EngineKind.Native })
        {
            DbSpec spec = e == EngineKind.Managed ? c.ManagedSpec : c.Spec;
            ScriptBuilder f = h.NewScript($"{e}-final", h.DbPath, spec, h.Model);
            foreach (string cf in h.Model.ColumnFamilyNames) f.Add(Op.Compact(cf));
            f.Verify().Dump();
            await h.RunAsync(e, f);
        }
    }

    private static GeneratorOptions Clone(GeneratorOptions g, int ops) => new()
    {
        Ops = ops, KeySpace = g.KeySpace, AllowFlush = g.AllowFlush, AllowCompact = g.AllowCompact, AllowSnapshots = g.AllowSnapshots,
        AllowColumnFamilyChanges = g.AllowColumnFamilyChanges, AllowSingleDelete = g.AllowSingleDelete, AllowRangeDelete = g.AllowRangeDelete,
        AllowMerge = g.AllowMerge, LargeValueRate = g.LargeValueRate, Pattern = g.Pattern, Flushes = g.Flushes, FlushEvery = g.FlushEvery,
        Compaction = g.Compaction, Ingest = g.Ingest, NoWalWrites = g.NoWalWrites, FlushAfterNoWal = g.FlushAfterNoWal,
        FlushWalOps = g.FlushWalOps, SyncRate = g.SyncRate,
    };
}
