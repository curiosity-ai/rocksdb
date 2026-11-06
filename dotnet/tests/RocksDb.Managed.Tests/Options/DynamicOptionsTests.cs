using RocksDb.Managed.Env;
using RocksDb.Managed.Options;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Options;

/// <summary>
/// Regression tests of the second correctness review for the options layer: SetOptions validates the complete new option
/// set before installing it (ColumnFamilyData::SetOptions), struct options are updated field by field
/// (OptionTypeInfo::ParseStruct), SetDBOptions (no-op detection, per-CF validation, SwitchWAL), the 11.x options typed
/// by the port, and the DBOptions sanitization of max_open_files / delayed_write_rate (SanitizeOptions).
/// </summary>
[TestClass]
public sealed class DynamicOptionsTests
{
    private static KeyValuePair<string, string> P(string k, string v) => new(k, v);

    private static OptionsFile LatestOptions(IFileSystem fs) =>
        OptionsFile.Load(fs, OptionsFile.FindLatestOptionsFile(fs, DbPath)!, preserveUnknownOptions: false);

    private static RocksDbException Rejects(Action a) => Assert.ThrowsExactly<RocksDbException>(a);

    /// <summary>Asserts that a rejected SetOptions left the live options and the OPTIONS file untouched.</summary>
    private static void AssertRejectedCfChange(RocksDatabase db, IFileSystem fs, StatusCode code, params KeyValuePair<string, string>[] change)
    {
        string optionsBefore = db.DefaultColumnFamily.Options.ToOptionsString();
        string fileBefore = OptionsFile.FindLatestOptionsFile(fs, DbPath)!;
        RocksDbException e = Rejects(() => db.SetOptions(change));
        Assert.AreEqual(code, e.Code, e.Message);
        Assert.AreEqual(optionsBefore, db.DefaultColumnFamily.Options.ToOptionsString(), "live options changed by a rejected SetOptions");
        Assert.AreEqual(fileBefore, OptionsFile.FindLatestOptionsFile(fs, DbPath), "OPTIONS file written by a rejected SetOptions");
    }

    [TestMethod]
    public void RejectedSetOptionsChangesNothingAndLaterCallsWork()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        // A valid change together with an invalid protection length: nothing is applied (the memtable options used to
        // reject the value only after the live options had been overwritten, poisoning every later SetOptions / Flush).
        AssertRejectedCfChange(db, fs, StatusCode.NotSupported, P("write_buffer_size", "1048576"), P("memtable_protection_bytes_per_key", "3"));
        AssertRejectedCfChange(db, fs, StatusCode.NotSupported, P("block_protection_bytes_per_key", "5"));
        Assert.AreEqual(0u, db.DefaultColumnFamily.Options.MemtableProtectionBytesPerKey);

        db.SetOptions([P("write_buffer_size", "1048576"), P("memtable_protection_bytes_per_key", "8")]);
        db.Put("k"u8, "v"u8);
        db.Flush();
        Assert.AreEqual("v", GetString(db, "k"));
        ColumnFamilyOptions persisted = LatestOptions(fs).FindColumnFamily("default")!;
        Assert.AreEqual(8u, persisted.MemtableProtectionBytesPerKey);
        Assert.AreEqual(1048576UL, persisted.WriteBufferSize);
    }

    [TestMethod]
    public void SetOptionsRunsColumnFamilyValidateOptions()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.MaxOpenFiles = 100;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o, new ColumnFamilyOptions { CompactionStyle = CompactionStyle.Fifo, Ttl = 0, NumLevels = 1 }))
        {
            // FIFO + ttl requires max_open_files = -1.
            AssertRejectedCfChange(db, fs, StatusCode.NotSupported, P("ttl", "100"));
            // file_temperature_age_thresholds must be sorted by age.
            AssertRejectedCfChange(db, fs, StatusCode.NotSupported,
                P("compaction_options_fifo", "{file_temperature_age_thresholds={{temperature=kCold;age=20;}:{temperature=kWarm;age=10;}}}"));
            db.SetOptions([P("compaction_options_fifo", "{file_temperature_age_thresholds={{temperature=kWarm;age=10;}:{temperature=kCold;age=20;}}}")]);
            Assert.AreEqual(2, db.DefaultColumnFamily.Options.CompactionOptionsFifo.FileTemperatureAgeThresholds.Count);
        }

        MemoryFileSystem fs2 = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs2), new ColumnFamilyOptions { CompactionStyle = CompactionStyle.Universal }))
        {
            // max_read_amp below the compaction trigger (4).
            AssertRejectedCfChange(db, fs2, StatusCode.NotSupported, P("compaction_options_universal.max_read_amp", "2"));
            AssertRejectedCfChange(db, fs2, StatusCode.NotSupported, P("compaction_options_universal", "{max_read_amp=-2}"));
            db.SetOptions([P("compaction_options_universal.max_read_amp", "6")]);
            Assert.AreEqual(6, db.DefaultColumnFamily.Options.CompactionOptionsUniversal.MaxReadAmp);
        }

        // DB::Open runs the same checks.
        DbOptions o3 = NewOptions(NewFs());
        o3.MaxOpenFiles = 100;
        Assert.AreEqual(StatusCode.NotSupported, Rejects(() => RocksDatabase.Open(DbPath, o3, new ColumnFamilyOptions
        {
            CompactionStyle = CompactionStyle.Fifo, Ttl = 3600,
        }).Dispose()).Code);
    }

    [TestMethod]
    public void StructOptionsUpdateOnlyTheNamedFields()
    {
        MemoryFileSystem fs = NewFs();
        var cf = new ColumnFamilyOptions { CompactionStyle = CompactionStyle.Universal };
        cf.CompactionOptionsUniversal.SizeRatio = 7;
        cf.CompressionOpts.Level = 5;
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cf);

        db.SetOptions([P("compaction_options_universal", "{max_merge_width=9}")]);
        CompactionOptionsUniversal u = db.DefaultColumnFamily.Options.CompactionOptionsUniversal;
        Assert.AreEqual(9u, u.MaxMergeWidth);
        Assert.AreEqual(7u, u.SizeRatio, "a partial struct update must keep the other fields");

        db.SetOptions([P("compaction_options_universal.max_merge_width", "7"), P("compression_opts.max_dict_bytes", "4096")]);
        Assert.AreEqual(7u, db.DefaultColumnFamily.Options.CompactionOptionsUniversal.MaxMergeWidth);
        Assert.AreEqual(7u, db.DefaultColumnFamily.Options.CompactionOptionsUniversal.SizeRatio);
        Assert.AreEqual(4096u, db.DefaultColumnFamily.Options.CompressionOpts.MaxDictBytes);
        Assert.AreEqual(5, db.DefaultColumnFamily.Options.CompressionOpts.Level);

        RocksDbException e = Rejects(() => db.SetOptions([P("compaction_options_universal", "{bogus=1}")]));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        StringAssert.Contains(e.Message, "Unrecognized option: compaction_options_universal.bogus");
        e = Rejects(() => db.SetOptions([P("compaction_options_universal.bogus", "1")]));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        StringAssert.Contains(e.Message, "Unrecognized option: compaction_options_universal.bogus");
        Assert.AreEqual(0, db.DefaultColumnFamily.Options.CompactionOptionsUniversal.UnknownOptions.Count);

        ColumnFamilyOptions persisted = LatestOptions(fs).FindColumnFamily("default")!;
        Assert.AreEqual(7u, persisted.CompactionOptionsUniversal.MaxMergeWidth);
        Assert.AreEqual(7u, persisted.CompactionOptionsUniversal.SizeRatio);
        Assert.AreEqual(5, persisted.CompressionOpts.Level);
        StringAssert.DoesNotMatch(persisted.GetOption("compaction_options_universal")!, new System.Text.RegularExpressions.Regex("bogus"));
    }

    [TestMethod]
    public void OptionsFileStructParsing()
    {
        const string head = "[Version]\n  rocksdb_version=11.12.0\n  options_file_version=1.1\n[DBOptions]\n  create_if_missing=true\n[CFOptions \"default\"]\n";
        // Strict parsing rejects an unknown struct field, lenient parsing keeps it and writes it back.
        RocksDbException e = Rejects(() => OptionsFile.Parse(head + "  compaction_options_universal={size_ratio=3;bogus=1;}\n", preserveUnknownOptions: false));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        StringAssert.Contains(e.Message, "compaction_options_universal.bogus");
        OptionsFile f = OptionsFile.Parse(head + "  compaction_options_universal={size_ratio=3;bogus=1;}\n", preserveUnknownOptions: true);
        Assert.AreEqual(3u, f.ColumnFamilies[0].Options.CompactionOptionsUniversal.SizeRatio);
        StringAssert.Contains(f.ColumnFamilies[0].Options.GetOption("compaction_options_universal")!, "bogus=1;");

        // Dotted names and the legacy scalar forms.
        f = OptionsFile.Parse(head + "  compaction_options_universal.max_merge_width=7\n  compaction_options_fifo=23\n  compression_opts=-13:4:1:16:32:true\n"
            + "  bottommost_compression_opts=-14:6:0:0:0:2:false:64:false\n  memtable_veirfy_per_key_checksum_on_seek=true\n  soft_rate_limit=0.5\n",
            preserveUnknownOptions: false);
        ColumnFamilyOptions c = f.ColumnFamilies[0].Options;
        Assert.AreEqual(7u, c.CompactionOptionsUniversal.MaxMergeWidth);
        Assert.AreEqual(1u, c.CompactionOptionsUniversal.SizeRatio);
        Assert.AreEqual(23UL, c.CompactionOptionsFifo.MaxTableFilesSize);
        Assert.AreEqual(-13, c.CompressionOpts.WindowBits);
        Assert.AreEqual(4, c.CompressionOpts.Level);
        Assert.AreEqual(1, c.CompressionOpts.Strategy);
        Assert.AreEqual(16u, c.CompressionOpts.MaxDictBytes);
        Assert.AreEqual(32u, c.CompressionOpts.ZstdMaxTrainBytes);
        Assert.IsTrue(c.CompressionOpts.Enabled, "6 fields: the last one is 'enabled'");
        Assert.AreEqual(1u, c.CompressionOpts.ParallelThreads);
        Assert.AreEqual(2u, c.BottommostCompressionOpts.ParallelThreads);
        Assert.AreEqual(64UL, c.BottommostCompressionOpts.MaxDictBufferBytes);
        Assert.IsFalse(c.BottommostCompressionOpts.UseZstdDictTrainer);
        Assert.IsTrue(c.MemtableVerifyPerKeyChecksumOnSeek);
        Assert.IsFalse(c.ToOptionsString().Contains("soft_rate_limit", StringComparison.Ordinal), "deprecated options are not written");
        Assert.IsFalse(c.ToOptionsString().Contains("veirfy", StringComparison.Ordinal));
    }

    [TestMethod]
    public void NewTypedOptionsRoundTripAndAreMutable()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        db.SetOptions([P("memtable_max_range_deletions", "2"), P("paranoid_memory_checks", "true"), P("uncache_aggressiveness", "300"),
            P("strict_max_successive_merges", "true"), P("verify_output_flags", "1025"), P("blob_compression_opts.level", "3"),
            P("compression_opts.auto_skip", "true"), P("memtable_op_scan_flush_trigger", "100"), P("min_tombstones_for_range_conversion", "4")]);
        db.SetDBOptions([P("max_manifest_space_amp_pct", "200"), P("verify_manifest_content_on_close", "true"), P("daily_offpeak_time_utc", "04:30-07:30"),
            P("fast_sst_open", "true")]);
        ColumnFamilyOptions o = db.DefaultColumnFamily.Options;
        Assert.AreEqual(2u, o.MemtableMaxRangeDeletions);
        Assert.AreEqual(VerifyOutputFlags.VerifyBlockChecksum | VerifyOutputFlags.EnableForLocalCompaction, o.VerifyOutputFlags);
        Assert.AreEqual(3, o.BlobCompressionOpts.Level);
        Assert.IsTrue(o.CompressionOpts.AutoSkip);
        OptionsFile f = LatestOptions(fs);
        ColumnFamilyOptions p = f.FindColumnFamily("default")!;
        Assert.AreEqual(300u, p.UncacheAggressiveness);
        Assert.IsTrue(p.StrictMaxSuccessiveMerges);
        Assert.AreEqual(4u, p.MinTombstonesForRangeConversion);
        Assert.AreEqual(200, f.DbOptions.MaxManifestSpaceAmpPct);
        Assert.AreEqual("04:30-07:30", f.DbOptions.DailyOffpeakTimeUtc);
        Assert.AreEqual(StatusCode.InvalidArgument, Rejects(() => db.SetOptions([P("memtable_batch_lookup_optimization", "true")])).Code);
        Assert.AreEqual(StatusCode.InvalidArgument, Rejects(() => db.SetDBOptions([P("prefix_seek_opt_in_only", "true")])).Code);

        // memtable_max_range_deletions marks the memtable (created after the change) for flush.
        db.Put("a"u8, "1"u8);
        db.Flush();
        db.DeleteRange("b"u8, "c"u8);
        db.DeleteRange("d"u8, "e"u8);
        db.Put("f"u8, "1"u8); // the write after the limit was reached switches the memtable
        db.WaitForCompact();
        Assert.AreEqual("2", db.GetProperty("rocksdb.num-files-at-level0"), "the memtable with 2 range deletions was flushed");
    }

    [TestMethod]
    public void SetDbOptionsSkipsNoOpsAndValidatesColumnFamilies()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { CompactionStyle = CompactionStyle.Fifo, Ttl = 3600 });
        // Like upstream, SetDBOptions turns bytes_per_sync = 0 (the default) into 1 MiB before comparing, so the first
        // call is never a no-op.
        db.SetDBOptions([P("bytes_per_sync", "1048576")]);
        string before = OptionsFile.FindLatestOptionsFile(fs, DbPath)!;
        db.SetDBOptions([P("max_background_jobs", db.GetDBOptions().MaxBackgroundJobs.ToString(System.Globalization.CultureInfo.InvariantCulture))]);
        Assert.AreEqual(before, OptionsFile.FindLatestOptionsFile(fs, DbPath), "a no-op SetDBOptions must not write an OPTIONS file");

        // The FIFO column family with ttl rejects max_open_files != -1 (ColumnFamilyData::ValidateOptions).
        Assert.AreEqual(StatusCode.NotSupported, Rejects(() => db.SetDBOptions([P("max_open_files", "100")])).Code);
        Assert.AreEqual(-1, db.GetDBOptions().MaxOpenFiles);
        // DBImpl::ValidateOptions: daily_offpeak_time_utc.
        Assert.AreEqual(StatusCode.InvalidArgument, Rejects(() => db.SetDBOptions([P("daily_offpeak_time_utc", "25:00-01:00")])).Code);
        Assert.AreEqual(StatusCode.InvalidArgument, Rejects(() => db.SetDBOptions([P("daily_offpeak_time_utc", "04:00-04:00")])).Code);
        Assert.AreEqual(before, OptionsFile.FindLatestOptionsFile(fs, DbPath));
        db.SetDBOptions([P("daily_offpeak_time_utc", "23:30-04:30")]);
        Assert.AreEqual("23:30-04:30", db.DefaultColumnFamily.Db.OffpeakTime.DailyOffpeakTimeUtc);
        Assert.AreSame(db.DefaultColumnFamily.Db.OffpeakTime, db.DefaultColumnFamily.Options.DbOffpeakTime);
    }

    [TestMethod]
    public void LoweringMaxTotalWalSizeFlushesTheOldestWal()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        for (int i = 0; i < 100; i++) db.Put(Key(i), Value(i, 0));
        Assert.AreEqual("0", db.GetProperty("rocksdb.num-files-at-level0"));
        db.SetDBOptions([P("max_total_wal_size", "1")]);
        db.WaitForCompact();
        Assert.AreEqual("1", db.GetProperty("rocksdb.num-files-at-level0"), "SetDBOptions switches the WAL when it exceeds the new limit");
        Assert.AreEqual(GetString(db, "key000005"), System.Text.Encoding.UTF8.GetString(Value(5, 0)));

        // wal_bytes_per_sync changes switch the WAL too.
        for (int i = 0; i < 10; i++) db.Put(Key(i), Value(i, 1));
        db.SetDBOptions([P("max_total_wal_size", "0"), P("wal_bytes_per_sync", "65536")]);
        db.WaitForCompact();
        Assert.AreEqual("0", db.GetProperty("rocksdb.num-entries-active-mem-table"));
    }

    [TestMethod]
    public void MaxOpenFilesAndDelayedWriteRateAreSanitizedLikeNative()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.MaxOpenFiles = 5;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o))
        {
            Assert.AreEqual(20, db.GetDBOptions().MaxOpenFiles);
            Assert.AreEqual(10, db.DefaultColumnFamily.Db.SharedTableCache.Capacity);
            Assert.AreEqual(20, LatestOptions(fs).DbOptions.MaxOpenFiles);
        }
        Assert.AreEqual(5, o.MaxOpenFiles, "the caller's options are not modified");

        o.MaxOpenFiles = 25;
        using var limiter = new GenericRateLimiter(5 << 20);
        o.RateLimiter = limiter;
        o.DelayedWriteRate = 0;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o))
        {
            Assert.AreEqual(15, db.DefaultColumnFamily.Db.SharedTableCache.Capacity);
            DbOptions eff = db.GetDBOptions();
            Assert.AreEqual(25, eff.MaxOpenFiles);
            Assert.AreEqual(5UL << 20, eff.DelayedWriteRate);
            Assert.AreEqual(1UL << 20, eff.BytesPerSync, "a rate limiter turns bytes_per_sync = 0 into 1 MiB");
            Assert.AreEqual(5UL << 20, LatestOptions(fs).DbOptions.DelayedWriteRate);
        }
        o.RateLimiter = null;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o)) Assert.AreEqual(16UL << 20, db.GetDBOptions().DelayedWriteRate);
    }

    [TestMethod]
    public void PrefixSeekOptInOnlyForcesTotalOrderSeek()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.PrefixSeekOptInOnly = true;
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, new ColumnFamilyOptions { PrefixExtractor = SliceTransform.CreateFixedPrefix(1) });
        RocksDb.Managed.Db.DbImpl impl = db.DefaultColumnFamily.Db;
        RocksDb.Managed.Db.ColumnFamilyData cfd = impl.GetColumnFamilyData("default");
        var ro = new ReadOptions();
        RocksDb.Managed.Db.Iterators.DbIteratorSources s = impl.NewIteratorSources(cfd, ro);
        Assert.IsTrue(s.ReadOptions!.TotalOrderSeek);
        Assert.IsFalse(ro.TotalOrderSeek, "the caller's read options are not modified");
        s.Cleanup!.Dispose();
        s = impl.NewIteratorSources(cfd, new ReadOptions { PrefixSameAsStart = true });
        Assert.IsTrue(s.ReadOptions!.TotalOrderSeek && s.ReadOptions.PrefixSameAsStart);
        s.Cleanup!.Dispose();
        // Iteration crosses prefixes as with total_order_seek.
        db.Put("a1"u8, "x"u8);
        db.Put("b1"u8, "y"u8);
        using Iterator it = db.NewIterator();
        it.Seek("a"u8);
        Assert.IsTrue(it.Valid);
        it.Next();
        Assert.IsTrue(it.Valid);
        CollectionAssert.AreEqual("b1"u8.ToArray(), it.Key.ToArray());
    }

    /// <summary>OffpeakTimeOption (port of the offpeak cases of options/options_test.cc OffpeakTimeInfoTest).</summary>
    [TestMethod]
    public void OffpeakTimeInfo()
    {
        static long T(int h, int m, int s = 0) => 3 * 86400L + h * 3600 + m * 60 + s;
        var o = new OffpeakTimeOption("06:30-11:30");
        Assert.IsFalse(o.GetOffpeakTimeInfo(T(6, 29)).IsNowOffpeak);
        Assert.IsTrue(o.GetOffpeakTimeInfo(T(6, 30)).IsNowOffpeak);
        Assert.IsTrue(o.GetOffpeakTimeInfo(T(11, 30, 59)).IsNowOffpeak, "compared to the nearest minute");
        Assert.IsFalse(o.GetOffpeakTimeInfo(T(11, 31)).IsNowOffpeak);
        Assert.AreEqual(60, o.GetOffpeakTimeInfo(T(6, 29)).SecondsTillNextOffpeakStart);
        Assert.AreEqual(86400, o.GetOffpeakTimeInfo(T(6, 30)).SecondsTillNextOffpeakStart);
        o.SetFromOffpeakTimeString("23:30-04:30");
        Assert.IsTrue(o.GetOffpeakTimeInfo(T(0, 0)).IsNowOffpeak);
        Assert.IsTrue(o.GetOffpeakTimeInfo(T(23, 45)).IsNowOffpeak);
        Assert.IsFalse(o.GetOffpeakTimeInfo(T(12, 0)).IsNowOffpeak);
        o.SetFromOffpeakTimeString("bad");
        Assert.AreEqual("23:30-04:30", o.DailyOffpeakTimeUtc, "an invalid string keeps the previous window");
        Assert.IsFalse(new OffpeakTimeOption("").GetOffpeakTimeInfo(T(1, 0)).IsNowOffpeak);
        foreach (string bad in new[] { "1:2:3-4:5", "24:00-01:00", "01:60-02:00", "0100-0200", "01:00", "01:00-02:00-03:00", "01:00 -02:00" })
        {
            Assert.IsFalse(OffpeakTimeOption.TryParseTimeRange(bad, out _, out _), bad);
        }
        Assert.IsTrue(OffpeakTimeOption.TryParseTimeRange("1:5-23:59", out int start, out int end));
        Assert.AreEqual(3900, start);
        Assert.AreEqual(86340, end);
    }
}
