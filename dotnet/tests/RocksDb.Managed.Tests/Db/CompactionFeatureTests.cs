using RocksDb.Managed.Cache;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Options;
using RocksDb.Managed.Table;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>
/// DB-level tests of the compaction features closing audit #3's remaining deviations: SST partitioner and
/// target_file_size_is_upper_bound output cutting (db_compaction_test.cc, compaction_job_test.cc), universal periodic
/// compaction (db_universal_compaction_test.cc), memtable first-insert time as oldest key / ancestor time
/// (flush_job.cc), periodic compaction defaults (SanitizeCfOptions), and the HyperClock eviction effort cap and table
/// sizing (clock_cache_test.cc).
/// </summary>
[TestClass]
public sealed class CompactionFeatureTests
{
    private const string DbPath = "/features";

    private static ColumnFamilyOptions Leveled() => new()
    {
        WriteBufferSize = 4 << 20,
        DisableAutoCompactions = true,
        Compression = RocksDb.Managed.Compression.CompressionType.None,
    };

    private static List<LiveFileMetadata> FilesAt(RocksDatabase db, int level)
    {
        var l = new List<LiveFileMetadata>();
        foreach (LiveFileMetadata f in db.GetLiveFilesMetadata())
        {
            if (f.Level == level) l.Add(f);
        }
        l.Sort((a, b) => a.SmallestKey.AsSpan().SequenceCompareTo(b.SmallestKey));
        return l;
    }

    /// <summary>
    /// Writes "{prefix}{i:D4}" for each prefix, twice into two overlapping L0 files (so that CompactRange rewrites them
    /// instead of moving a single file).
    /// </summary>
    private static void WritePrefixes(RocksDatabase db, string prefixes, int perPrefix)
    {
        for (int round = 0; round < 2; round++)
        {
            foreach (char p in prefixes)
            {
                for (int i = 0; i < perPrefix; i++) db.Put(B(p + i.ToString("D4", System.Globalization.CultureInfo.InvariantCulture)), B("v" + i));
            }
            db.Flush();
        }
    }

    [TestMethod]
    public void FixedPrefixPartitionerCutsOutputsAtPrefixBoundaries()
    {
        // SstPartitionerFixedPrefix (db_compaction_test.cc PartitionerTest): outputs never span two prefixes, and the
        // single L0 file spanning all prefixes is not trivially moved (CanDoTrivialMove).
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cf = Leveled();
        cf.SstPartitionerFactory = new SstPartitionerFixedPrefixFactory(1);
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cf))
        {
            WritePrefixes(db, "abc", 300);
            db.CompactRange();
            List<LiveFileMetadata> files = FilesAt(db, db.DefaultColumnFamily.Options.NumLevels - 1);
            Assert.AreEqual(3, files.Count);
            for (int i = 0; i < 3; i++)
            {
                Assert.AreEqual((byte)"abc"[i], files[i].SmallestKey[0]);
                Assert.AreEqual((byte)"abc"[i], files[i].LargestKey[0]);
            }
            Assert.AreEqual("v7", GetString(db, "b0007"));
        }
        // Persisted like native ("{id=SstPartitionerFixedPrefixFactory;length=1;}") and recreated when loaded.
        OptionsFile f = OptionsFile.Load(fs, OptionsFile.FindLatestOptionsFile(fs, DbPath)!, preserveUnknownOptions: true);
        ColumnFamilyOptions persisted = f.FindColumnFamily("default")!;
        Assert.AreEqual("{id=SstPartitionerFixedPrefixFactory;length=1;}", persisted.GetOption("sst_partitioner_factory"));
        Assert.AreEqual(1, Assert.IsInstanceOfType<SstPartitionerFixedPrefixFactory>(persisted.SstPartitionerFactory).Length);
        // The bare id (native accepts it too) creates a factory with length 0 (no partitions); unknown ids are kept.
        persisted.SetOption("sst_partitioner_factory", "SstPartitionerFixedPrefixFactory", preserveUnknown: false);
        Assert.AreEqual(0, Assert.IsInstanceOfType<SstPartitionerFixedPrefixFactory>(persisted.SstPartitionerFactory).Length);
        persisted.SetOption("sst_partitioner_factory", "{id=MyPartitioner;width=3;}", preserveUnknown: false);
        Assert.IsNull(persisted.SstPartitionerFactory);
        Assert.AreEqual("MyPartitioner", persisted.SstPartitionerFactoryName);
        Assert.AreEqual("{id=MyPartitioner;width=3;}", persisted.GetOption("sst_partitioner_factory"));
    }

    /// <summary>A partitioner without the prefix fast path: cuts where the key's second byte changes.</summary>
    private sealed class SecondBytePartitioner : SstPartitioner
    {
        public int Calls;
        public override string Name => "SecondByte";

        public override PartitionerResult ShouldPartition(ReadOnlySpan<byte> prevUserKey, ReadOnlySpan<byte> currentUserKey, ulong currentOutputFileSize)
        {
            Calls++;
            return prevUserKey[1] != currentUserKey[1] ? PartitionerResult.Required : PartitionerResult.NotRequired;
        }

        public override bool CanDoTrivialMove(ReadOnlySpan<byte> smallestUserKey, ReadOnlySpan<byte> largestUserKey) =>
            smallestUserKey[1] == largestUserKey[1];
    }

    private sealed class SecondByteFactory : SstPartitionerFactory
    {
        public readonly List<SecondBytePartitioner> Created = new();
        public readonly List<SstPartitionerContext> Contexts = new();
        public override string Name => "SecondByteFactory";

        public override SstPartitioner CreatePartitioner(in SstPartitionerContext context)
        {
            var p = new SecondBytePartitioner();
            lock (Created)
            {
                Created.Add(p);
                Contexts.Add(context);
            }
            return p;
        }
    }

    [TestMethod]
    public void CustomPartitionerIsConsultedPerKey()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cf = Leveled();
        var factory = new SecondByteFactory();
        cf.SstPartitionerFactory = factory;
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cf);
        // Keys "a0000".."a0299": the second byte takes the values '0'..'2'.
        WritePrefixes(db, "ab", 300);
        db.CompactRange();
        List<LiveFileMetadata> files = FilesAt(db, db.DefaultColumnFamily.Options.NumLevels - 1);
        Assert.AreEqual(1, files.Count, "keys a0xxx..b0xxx all have second byte '0'");
        Assert.IsTrue(factory.Created.Count > 0);
        Assert.IsTrue(factory.Contexts[^1].IsManualCompaction);
        int calls = 0;
        foreach (SecondBytePartitioner p in factory.Created) calls += p.Calls;
        Assert.IsTrue(calls >= 599, "ShouldPartition is called for every key after the first: " + calls);
    }

    [TestMethod]
    public void TargetFileSizeIsUpperBoundKeepsOutputsBelowTheTarget()
    {
        // target_file_size_is_upper_bound: the estimated tail (index, filter, properties, metaindex, footer) is added to
        // the data size when deciding the cut, so outputs stay below the target; without it they exceed it by the tail.
        const ulong Target = 64 * 1024;
        foreach (bool upperBound in new[] { false, true })
        {
            MemoryFileSystem fs = NewFs();
            ColumnFamilyOptions cf = Leveled();
            cf.TargetFileSizeBase = Target;
            cf.TargetFileSizeIsUpperBound = upperBound;
            cf.TableOptions.FilterPolicy = new RocksDb.Managed.Table.Filters.BloomFilterPolicy(10);
            using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cf);
            for (int round = 0; round < 2; round++)
            {
                for (int i = 0; i < 20000; i++) db.Put(Key(i), Value(i, round));
                db.Flush();
            }
            db.CompactRange();
            List<LiveFileMetadata> files = FilesAt(db, db.DefaultColumnFamily.Options.NumLevels - 1);
            Assert.IsTrue(files.Count > 5);
            ulong max = 0;
            for (int i = 0; i < files.Count - 1; i++) max = Math.Max(max, files[i].Size);
            if (upperBound)
            {
                Assert.IsTrue(max <= Target, "largest output " + max);
                Assert.IsTrue(max > Target * 8 / 10, "outputs still come close to the target: " + max);
            }
            else
            {
                Assert.IsTrue(max > Target, "without the option the tail is not counted: " + max);
            }
        }
    }

    [TestMethod]
    public void FlushRecordsMemtableFirstInsertTime()
    {
        // FlushJob::WriteLevel0Table: oldest_key_time is the memtable's first-insert time (MemTable::
        // UpdateOldestKeyTime) and oldest_ancestor_time = min(now, oldest_key_time); rocksdb.creation.time is the oldest
        // ancestor time. The port used the flush time for all of them.
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), Leveled());
        ulong before = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        db.Put(B("a"), B("1"));
        WaitUntilUnixSecondPasses(before);
        db.Put(B("b"), B("2"));
        db.Flush();
        LiveFileMetadata file = db.GetLiveFilesMetadata()[0];
        TableProperties props = Single(db.GetPropertiesOfAllTables());
        Assert.AreEqual(before, props.OldestKeyTime, "first insert");
        Assert.IsTrue(props.FileCreationTime > before, "flush time");
        Assert.AreEqual(before, file.OldestAncestorTime);
        Assert.AreEqual(before, props.CreationTime);
        Assert.AreEqual(props.FileCreationTime, file.FileCreationTime);
        Assert.AreEqual(props.FileCreationTime, props.NewestKeyTime);
    }

    private static TableProperties Single(Dictionary<string, TableProperties> all)
    {
        Assert.AreEqual(1, all.Count);
        foreach (TableProperties p in all.Values) return p;
        throw new InvalidOperationException();
    }

    private static void WaitUntilUnixSecondPasses(ulong second)
    {
        // The feature is wall-clock based (seconds): poll the clock rather than sleeping a fixed time.
        var sw = System.Diagnostics.Stopwatch.StartNew();
        while ((ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds() <= second)
        {
            if (sw.Elapsed > TimeSpan.FromSeconds(10)) Assert.Fail("clock did not advance");
            Thread.Sleep(20);
        }
    }

    [TestMethod]
    public void UniversalPeriodicCompactionRewritesOldFiles()
    {
        // db_universal_compaction_test.cc PeriodicCompaction: files older than periodic_compaction_seconds trigger a full
        // compaction to the last level even below the sorted-run trigger.
        MemoryFileSystem fs = NewFs();
        var cf = new ColumnFamilyOptions
        {
            CompactionStyle = CompactionStyle.Universal,
            Level0FileNumCompactionTrigger = 10,
            PeriodicCompactionSeconds = 1,
        };
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cf);
        db.Put(B("a"), B("1"));
        db.Flush();
        db.WaitForCompact();
        LiveFileMetadata first = db.GetLiveFilesMetadata()[0];
        // Young file: nothing to do yet.
        Assert.AreEqual(0, first.Level);
        WaitUntilUnixSecondPasses(first.FileCreationTime + 1);
        db.Put(B("b"), B("2"));
        // The next pick (or already the periodic compaction trigger, whose period is 1 s here) sees the old file marked
        // for periodic compaction.
        db.Flush();
        db.WaitForCompact();
        List<LiveFileMetadata> files = db.GetLiveFilesMetadata();
        Assert.IsFalse(files.Exists(f => f.FileNumber == first.FileNumber), "the old file was rewritten");
        Assert.IsTrue(files.Exists(f => f.Level == cf.NumLevels - 1 && f.SmallestKey[0] == (byte)'a'), "into the last level");
        Assert.AreEqual("1", GetString(db, "a"));
        Assert.AreEqual("2", GetString(db, "b"));
    }

    [TestMethod]
    public void PeriodicCompactionSecondsDefaultsLikeSanitizeCfOptions()
    {
        // kDefaultPeriodicCompSecs resolves to 30 days for universal compaction and for leveled compaction with a
        // compaction filter, else 0; universal takes the stricter of ttl and periodic_compaction_seconds.
        const ulong ThirtyDays = 30UL * 24 * 60 * 60;
        ColumnFamilyOptions Sanitized(ColumnFamilyOptions o)
        {
            RocksDb.Managed.Db.DbImpl.SanitizeCfOptions(o, allowIngestBehind: false, numDbPaths: 1);
            return o;
        }
        Assert.AreEqual(0UL, Sanitized(new ColumnFamilyOptions()).PeriodicCompactionSeconds);
        Assert.AreEqual(ThirtyDays, Sanitized(new ColumnFamilyOptions { CompactionStyle = CompactionStyle.Universal }).PeriodicCompactionSeconds);
        Assert.AreEqual(500UL, Sanitized(new ColumnFamilyOptions { CompactionStyle = CompactionStyle.Universal, Ttl = 500 }).PeriodicCompactionSeconds);
        Assert.AreEqual(500UL, Sanitized(new ColumnFamilyOptions { CompactionStyle = CompactionStyle.Universal, Ttl = 0, PeriodicCompactionSeconds = 500 })
            .PeriodicCompactionSeconds);
        Assert.AreEqual(ThirtyDays, Sanitized(new ColumnFamilyOptions { CompactionFilter = new DropAllFilter() }).PeriodicCompactionSeconds);
        Assert.AreEqual(0UL, Sanitized(new ColumnFamilyOptions { CompactionStyle = CompactionStyle.Fifo }).PeriodicCompactionSeconds);
        Assert.AreEqual(Temperature.Unknown,
            Sanitized(new ColumnFamilyOptions { CompactionStyle = CompactionStyle.Fifo, LastLevelTemperature = Temperature.Cold }).LastLevelTemperature);
    }

    private sealed class DropAllFilter : CompactionFilter
    {
        public override string Name => "DropAll";
        public override CompactionFilterDecision Filter(int level, ReadOnlySpan<byte> key, CompactionFilterValueType valueType,
            ReadOnlySpan<byte> existingValue, System.Buffers.IBufferWriter<byte> newValue, System.Buffers.IBufferWriter<byte> skipUntil) =>
            CompactionFilterDecision.Remove;
    }

    // ---- HyperClock (clock_cache_test.cc) ----

    [TestMethod]
    public void HyperClockEvictionEffortCapLetsInsertsExceedCapacityWhenAllPinned()
    {
        // BaseClockTable::IsEvictionEffortExceeded: with (almost) everything pinned, an eviction gives up after seeing
        // eviction_effort_cap pinned entries per freed one, and a non-strict insert goes over capacity instead.
        var c = new HyperClockCache(new HyperClockCacheOptions
        {
            Capacity = 64 * 1024,
            EstimatedEntryCharge = 1024,
            NumShardBits = 0,
            MetadataChargePolicy = CacheMetadataChargePolicy.DontCharge,
            EvictionEffortCap = 1,
        });
        var held = new List<CachedBlock>();
        for (int i = 0; i < 64; i++)
        {
            Assert.IsTrue(c.TryInsert(new CacheKey(1, (ulong)i), CacheValue.FromObject(new object()), 1024, CachePriority.Low, out CachedBlock h));
            held.Add(h);
        }
        Assert.AreEqual(64 * 1024, c.Usage);
        Assert.IsTrue(c.Insert(new CacheKey(2, 1), CacheValue.FromObject(new object()), 1024, CachePriority.Low));
        Assert.IsTrue(c.EvictionEffortExceededCount > 0);
        Assert.IsTrue(c.Usage > c.Capacity);
        foreach (CachedBlock h in held) h.Dispose();
    }

    [TestMethod]
    public void HyperClockTableIsClampedForTinyCapacities()
    {
        // FixedHyperClockTable::CalcHashBits: with full metadata charging the table's own metadata (64 bytes per slot,
        // charged up front) must fit the capacity.
        var c = new HyperClockCache(new HyperClockCacheOptions { Capacity = 1000, EstimatedEntryCharge = 1, NumShardBits = 0 });
        Assert.IsTrue(c.TableSize * HyperClockCache.SlotMetadataCharge <= 1000, "table " + c.TableSize);
        Assert.AreEqual(c.TableSize * HyperClockCache.SlotMetadataCharge, c.Usage);
        var big = new HyperClockCache(new HyperClockCacheOptions { Capacity = 1 << 20, EstimatedEntryCharge = 4096, NumShardBits = 0 });
        // capacity / (4096 * 0.7 + 64) = 357 slots -> 512
        Assert.AreEqual(512, big.TableSize);
    }
    // ---- read-triggered compaction (db_compaction_test.cc ReadTriggeredCompaction, monitoring/file_read_sample.h) -----

    /// <summary>
    /// num_levels 3: keys 0..9 at L2, a tombstone file for keys 0..4 in L0 (not the last non-empty level).
    /// </summary>
    private static RocksDatabase OpenWithTombstoneFile(MemoryFileSystem fs, ColumnFamilyOptions cf, DbOptions? options = null)
    {
        RocksDatabase db = RocksDatabase.Open(DbPath, options ?? NewOptions(fs), cf);
        for (int i = 0; i < 10; i++) db.Put(Key(i), B(new string('v', 1024)));
        db.Flush();
        db.CompactRange();
        Assert.AreEqual(1, FilesAt(db, 2).Count);
        for (int i = 0; i < 5; i++) db.Delete(Key(i));
        db.Flush();
        Assert.AreEqual(1, FilesAt(db, 0).Count);
        return db;
    }

    /// <summary>Point lookups of the deleted keys with every read sampled (sync point should_sample_file_read:override).</summary>
    private static void ReadDeletedKeysSampled(RocksDatabase db, int rounds)
    {
        int old = Interlocked.Exchange(ref FileReadSample.TestOverride, 1);
        try
        {
            for (int r = 0; r < rounds; r++)
            {
                for (int i = 0; i < 5; i++) Assert.IsNull(db.Get(Key(i)));
            }
        }
        finally
        {
            Volatile.Write(ref FileReadSample.TestOverride, old);
        }
    }

    private static ColumnFamilyOptions ReadTriggered(double threshold) => new()
    {
        NumLevels = 3,
        Level0FileNumCompactionTrigger = 10,
        ReadTriggeredCompactionThreshold = threshold,
        DisableAutoCompactions = true,
        Compression = RocksDb.Managed.Compression.CompressionType.None,
    };

    [TestMethod]
    public void SampledReadsOfTombstonesTriggerCompaction()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = OpenWithTombstoneFile(fs, ReadTriggered(0.001));
        Assert.AreEqual(0UL, FilesAt(db, 0)[0].NumReadsSampled);
        ReadDeletedKeysSampled(db, 1);
        // Every lookup of a deleted key read the L0 file and found a tombstone there (kDeleted).
        Assert.AreEqual(5UL * FileReadSample.SampleRate, FilesAt(db, 0)[0].NumReadsSampled);
        // Enabling auto compactions recomputes the score: the L0 file is marked and compacted to the next level.
        db.DefaultColumnFamily.SetOptions(new Dictionary<string, string> { ["disable_auto_compactions"] = "false" });
        db.WaitForCompact();
        Assert.AreEqual(0, FilesAt(db, 0).Count);
        Assert.AreEqual(5, CountKeys(db));
        for (int i = 5; i < 10; i++) Assert.IsNotNull(db.Get(Key(i)));
    }

    [TestMethod]
    public void ReadTriggeredCompactionNeedsSampledCollapsibleReads()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cf = ReadTriggered(0.001);
        cf.DisableAutoCompactions = false;
        using RocksDatabase db = OpenWithTombstoneFile(fs, cf);
        // Reads of live keys find values in L2 (the L0 lookup is a miss there: collapsible for L0 only).
        int old = Interlocked.Exchange(ref FileReadSample.TestOverride, -1);
        try
        {
            for (int r = 0; r < 100; r++) Assert.IsNull(db.Get(Key(0)));
        }
        finally
        {
            Volatile.Write(ref FileReadSample.TestOverride, old);
        }
        Assert.AreEqual(0UL, FilesAt(db, 0)[0].NumReadsSampled, "no read sampled");
        db.Flush();
        db.WaitForCompact();
        Assert.AreEqual(1, FilesAt(db, 0).Count, "nothing marked without sampled reads");
    }

    [TestMethod]
    public void PeriodicTriggerPicksReadTriggeredFilesWithoutWrites()
    {
        // TriggerPeriodicCompaction (max_compaction_trigger_wakeup_seconds): a DB that only serves reads re-evaluates
        // its files and compacts the hot tombstone file.
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cf = ReadTriggered(0.001);
        cf.DisableAutoCompactions = false;
        DbOptions options = NewOptions(fs);
        options.MaxCompactionTriggerWakeupSeconds = 0; // polls every second
        using RocksDatabase db = OpenWithTombstoneFile(fs, cf, options);
        ReadDeletedKeysSampled(db, 1);
        var sw = System.Diagnostics.Stopwatch.StartNew();
        while (FilesAt(db, 0).Count != 0 && sw.Elapsed < TimeSpan.FromSeconds(20)) Thread.Sleep(50);
        db.WaitForCompact();
        Assert.AreEqual(0, FilesAt(db, 0).Count, "the periodic trigger compacted the read-hot file");
        Assert.AreEqual(5, CountKeys(db));
    }

    [TestMethod]
    public void IteratorSamplesFileReads()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = OpenWithTombstoneFile(fs, ReadTriggered(0));
        int old = Interlocked.Exchange(ref FileReadSample.TestOverride, 1);
        try
        {
            using Iterator it = db.NewIterator();
            it.Seek(Key(0));
            Assert.IsTrue(it.Valid);
        }
        finally
        {
            Volatile.Write(ref FileReadSample.TestOverride, old);
        }
        // L0: once per iterator creation; L2 (LevelIterator): the seek.
        Assert.IsTrue(FilesAt(db, 0)[0].NumReadsSampled >= FileReadSample.SampleRate);
        Assert.IsTrue(FilesAt(db, 2)[0].NumReadsSampled >= FileReadSample.SampleRate);
    }

    [TestMethod]
    public void ReadTriggeredCompactionThresholdIsValidatedAndPersisted()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions bad = Leveled();
        bad.ReadTriggeredCompactionThreshold = -1;
        var ex = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, NewOptions(fs), bad).Dispose());
        Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
        bad.ReadTriggeredCompactionThreshold = double.PositiveInfinity;
        ex = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, NewOptions(fs), bad).Dispose());
        Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);

        ColumnFamilyOptions cf = Leveled();
        cf.ReadTriggeredCompactionThreshold = 0.01;
        DbOptions options = NewOptions(fs);
        options.MaxCompactionTriggerWakeupSeconds = 600;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, options, cf))
        {
            ex = Assert.ThrowsExactly<RocksDbException>(() =>
                db.DefaultColumnFamily.SetOptions(new Dictionary<string, string> { ["read_triggered_compaction_threshold"] = "-0.5" }));
            Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
            db.DefaultColumnFamily.SetOptions(new Dictionary<string, string> { ["read_triggered_compaction_threshold"] = "0.02" });
        }
        OptionsFile f = OptionsFile.Load(fs, OptionsFile.FindLatestOptionsFile(fs, DbPath)!, preserveUnknownOptions: false);
        Assert.AreEqual(0.02, f.FindColumnFamily("default")!.ReadTriggeredCompactionThreshold);
        Assert.AreEqual(600UL, f.DbOptions.MaxCompactionTriggerWakeupSeconds);
    }

    [TestMethod]
    public void TriggerCompactionPeriodFollowsUpstream()
    {
        // ComputeTriggerCompactionPeriod: min(max wakeup, stats periods, min time-based interval / 5), at least 1.
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cf = Leveled();
        cf.Ttl = 0;
        cf.PeriodicCompactionSeconds = 0;
        DbOptions options = NewOptions(fs);
        options.StatsDumpPeriodSec = 0;
        options.StatsPersistPeriodSec = 0;
        options.MaxCompactionTriggerWakeupSeconds = 43200;
        using RocksDatabase db = RocksDatabase.Open(DbPath, options, cf);
        RocksDb.Managed.Db.DbImpl impl = db.DefaultColumnFamily.Db;
        Assert.AreEqual(43200UL, impl.ComputeTriggerCompactionPeriod());
        db.DefaultColumnFamily.SetOptions(new Dictionary<string, string> { ["periodic_compaction_seconds"] = "1000" });
        Assert.AreEqual(200UL, impl.ComputeTriggerCompactionPeriod());
        db.SetDBOptions(new Dictionary<string, string> { ["max_compaction_trigger_wakeup_seconds"] = "0" });
        Assert.AreEqual(1UL, impl.ComputeTriggerCompactionPeriod());
    }

    private static int CountKeys(RocksDatabase db)
    {
        int n = 0;
        using Iterator it = db.NewIterator();
        for (it.SeekToFirst(); it.Valid; it.Next()) n++;
        return n;
    }
    // ---- CompactRange first-level selection (DBImpl::CompactRangeInternal, Version::OverlapWithLevelIterator) --------

    /// <summary>One L0 file with the keys "001" and "111"; CompactRange("011", "101") with an optional partitioner.</summary>
    private static List<LiveFileMetadata> CompactRangeInsideFileGap(SstPartitionerFactory? partitioner, string begin, string end)
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cf = Leveled();
        cf.SstPartitionerFactory = partitioner;
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cf);
        db.Put(B("001"), B("a"));
        db.Put(B("111"), B("b"));
        db.Flush();
        db.CompactRange(B(begin), B(end));
        Assert.AreEqual("a", GetString(db, "001"));
        Assert.AreEqual("b", GetString(db, "111"));
        return db.GetLiveFilesMetadata();
    }

    [TestMethod]
    public void CompactRangeChecksKeysWithinFiles()
    {
        // No key of the file is inside ["011", "101"]: nothing is compacted (the file range alone overlaps).
        List<LiveFileMetadata> files = CompactRangeInsideFileGap(null, "011", "101");
        Assert.AreEqual(1, files.Count);
        Assert.AreEqual(0, files[0].Level);
        // A range holding a key compacts the file.
        files = CompactRangeInsideFileGap(null, "100", "200");
        Assert.AreEqual(1, files.Count);
        Assert.AreNotEqual(0, files[0].Level);
    }

    [TestMethod]
    public void CompactRangeWithPartitionBoundaryInRangeUsesFileOverlap()
    {
        // The range contains the partition boundary between prefixes "0" and "1": the file straddling it is compacted
        // although none of its keys is inside the range (moved to the last level: Compaction::IsTrivialMove does not
        // consult the partitioner for the last level).
        List<LiveFileMetadata> files = CompactRangeInsideFileGap(new SstPartitionerFixedPrefixFactory(1), "011", "101");
        Assert.AreEqual(1, files.Count);
        foreach (LiveFileMetadata f in files) Assert.AreNotEqual(0, f.Level);
        // No boundary inside ["011", "055"]: keys within the file are checked, nothing to compact.
        files = CompactRangeInsideFileGap(new SstPartitionerFixedPrefixFactory(1), "011", "055");
        Assert.AreEqual(1, files.Count);
        Assert.AreEqual(0, files[0].Level);
    }

    [TestMethod]
    public void CompactRangeSeesRangeTombstonesWithinFiles()
    {
        // A range tombstone inside the range counts as overlap (RangeDelAggregator::IsRangeOverlapped).
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), Leveled());
        db.Put(B("001"), B("a"));
        db.Put(B("111"), B("b"));
        db.DeleteRange(B("050"), B("060"));
        db.Flush();
        db.CompactRange(B("011"), B("101"));
        List<LiveFileMetadata> files = db.GetLiveFilesMetadata();
        Assert.AreEqual(1, files.Count);
        Assert.AreNotEqual(0, files[0].Level);
    }
    // ---- bottom-priority pool and reduce_file_locking (db_universal_compaction_test.cc ConcurrentBottomPriLowPri...) ---

    private static int CountBeingCompacted(RocksDatabase db, int level)
    {
        lock (db.DefaultColumnFamily.Db.MutexForTests)
        {
            int n = 0;
            foreach (FileMetaData f in db.DefaultColumnFamily.Data.Current.Storage.LevelFiles(level)) n += f.BeingCompacted ? 1 : 0;
            return n;
        }
    }

    /// <summary>
    /// A full universal compaction (to the last level) is forwarded to the bottom-priority pool. While it waits for the
    /// bottom thread, reduce_file_locking = true reserves only the oldest L0 file (the intended compaction of
    /// CreateIntendedCompactionForwardedToBottomPriorityPool) and the bottom thread repicks; with false every input
    /// file stays reserved. Both end with one sorted run in the last level.
    /// </summary>
    [TestMethod]
    public void BottomPriorityPoolReservesFilesPerReduceFileLocking()
    {
        foreach (bool reduceFileLocking in new[] { true, false })
        {
            using var entered = new ManualResetEventSlim(false);
            using var gate = new ManualResetEventSlim(false);
            TestHooks.Set("DBImpl::BGWorkBottomCompaction", () =>
            {
                entered.Set();
                gate.Wait(TimeSpan.FromSeconds(30));
            });
            try
            {
                MemoryFileSystem fs = NewFs();
                DbOptions options = NewOptions(fs);
                options.BottomPriorityBackgroundThreads = 1;
                var cf = new ColumnFamilyOptions
                {
                    CompactionStyle = CompactionStyle.Universal,
                    NumLevels = 3,
                    Level0FileNumCompactionTrigger = 3,
                    Compression = RocksDb.Managed.Compression.CompressionType.None,
                };
                cf.CompactionOptionsUniversal.ReduceFileLocking = reduceFileLocking;
                using RocksDatabase db = RocksDatabase.Open(DbPath, options, cf);
                for (int f = 0; f < 3; f++)
                {
                    for (int i = 0; i < 100; i++) db.Put(Key(i), Value(i, f));
                    db.Flush();
                }
                Assert.IsTrue(entered.Wait(TimeSpan.FromSeconds(20)), "forwarded to the bottom-priority pool");
                Assert.AreEqual(3, FilesAt(db, 0).Count);
                Assert.AreEqual(reduceFileLocking ? 1 : 3, CountBeingCompacted(db, 0), "reduce_file_locking=" + reduceFileLocking);
                gate.Set();
                db.WaitForCompact();
                Assert.AreEqual(0, FilesAt(db, 0).Count);
                Assert.AreNotEqual(0, FilesAt(db, 2).Count);
                Assert.AreEqual(0, CountBeingCompacted(db, 2));
                for (int i = 0; i < 100; i++) Assert.AreEqual(S(Value(i, 2)), GetString(db, S(Key(i))!));
            }
            finally
            {
                gate.Set();
                TestHooks.Set("DBImpl::BGWorkBottomCompaction", null);
            }
        }
    }

    [TestMethod]
    public void UniversalRequireMaxOutputLevelPicksOnlyLastLevelCompactions()
    {
        // MeetsOutputLevelRequirements: the bottom-priority repick ignores compactions that do not output to the
        // non-zero max output level.
        var o = new ColumnFamilyOptions { CompactionStyle = CompactionStyle.Universal, NumLevels = 3, Level0FileNumCompactionTrigger = 2 };
        o.CompactionOptionsUniversal.MaxSizeAmplificationPercent = 1000000;
        var picker = new UniversalCompactionPicker(o, InternalKeyComparator.Bytewise);
        static FileMetaData F(ulong n, ulong epoch, ulong seq) => new()
        {
            FileNumber = n,
            FileSize = 1000,
            Smallest = MakeIKey("a", seq),
            Largest = MakeIKey("z", seq),
            SmallestSeqno = seq,
            LargestSeqno = seq,
            EpochNumber = epoch,
        };
        // Two small L0 runs above a large last level: size ratio picks the two L0 files into L0... L1 (not the last).
        FileMetaData big = F(1, 0, 1);
        big.FileSize = 1000000;
        FileMetaData[][] files = [[F(3, 3, 30), F(2, 2, 20)], [], [big]];
        var vs = new VersionStorageInfo(InternalKeyComparator.Bytewise, 3, o, files);
        Compaction? c = picker.PickCompaction(vs, [], requireMaxOutputLevel: false);
        Assert.IsNotNull(c);
        Assert.AreEqual(1, c.OutputLevel);
        picker.ReleaseCompactionFiles(c, succeeded: false);
        vs.ComputeCompactionScore();
        Assert.IsNull(picker.PickCompaction(vs, [], requireMaxOutputLevel: true));
    }

    private static byte[] MakeIKey(string user, ulong seq)
    {
        byte[] u = B(user);
        var k = new byte[u.Length + 8];
        DbFormat.AppendInternalKey(k, u, seq, EntryType.Value);
        return k;
    }
}
