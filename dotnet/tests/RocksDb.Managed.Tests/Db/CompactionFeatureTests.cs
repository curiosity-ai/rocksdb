using RocksDb.Managed.Cache;
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
        db.Flush(); // the next pick sees the old file marked for periodic compaction
        db.WaitForCompact();
        List<LiveFileMetadata> files = db.GetLiveFilesMetadata();
        Assert.AreEqual(1, files.Count);
        Assert.AreEqual(cf.NumLevels - 1, files[0].Level);
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
}
