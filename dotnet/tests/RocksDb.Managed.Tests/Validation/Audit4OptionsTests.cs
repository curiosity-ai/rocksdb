using RocksDb.Managed.Compression;
using RocksDb.Managed.Env;
using RocksDb.Managed.Options;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4, slice Options (options model, OPTIONS file, SetOptions / SetDBOptions, sanitization and validation):
/// regression tests for the divergences found against options/*.cc, util/string_util.cc, db/column_family.cc,
/// db/db_impl/db_impl_open.cc and table/block_based/block_based_table_factory.cc, plus coverage of verified branches
/// that had no test.
/// </summary>
[TestClass]
public sealed class Audit4OptionsTests
{
    private static KeyValuePair<string, string> P(string k, string v) => new(k, v);

    private static OptionsFile LatestOptions(MemoryFileSystem fs) =>
        OptionsFile.Load(fs, OptionsFile.FindLatestOptionsFile(fs, DbPath)!, preserveUnknownOptions: false);

    private static RocksDbException OpenFails(DbOptions db, ColumnFamilyOptions cf) =>
        Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, db, cf).Dispose());

    // ---- SanitizeCfOptions ------------------------------------------------------------------------------------

    /// <summary>
    /// SanitizeCfOptions (db/column_family.cc): the ttl and periodic_compaction_seconds defaults become 30 days only
    /// when the table factory is block-based (is_block_based_table); otherwise ttl becomes 0 and
    /// periodic_compaction_seconds stays at its default and ends as 0. The port adjusted periodic_compaction_seconds
    /// for every table factory, and ColumnFamilyData::ValidateOptions then rejected the open of a universal
    /// compaction column family with a plain / cuckoo table ("Periodic Compaction is only supported in Block-Based
    /// Table format").
    /// </summary>
    [TestMethod]
    [DataRow(TableFactoryType.PlainTable)]
    [DataRow(TableFactoryType.CuckooTable)]
    public void UniversalCompactionWithNonBlockBasedTableOpensWithoutTimeBasedCompaction(TableFactoryType factory)
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.AllowMmapReads = true;
        var cf = new ColumnFamilyOptions { CompactionStyle = CompactionStyle.Universal, TableFactory = factory };
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o, cf))
        {
            Assert.AreEqual(0UL, db.DefaultColumnFamily.Options.PeriodicCompactionSeconds);
            Assert.AreEqual(0UL, db.DefaultColumnFamily.Options.Ttl);
        }
        ColumnFamilyOptions persisted = LatestOptions(fs).FindColumnFamily("default")!;
        Assert.AreEqual("0", persisted.GetOption("periodic_compaction_seconds"));
        Assert.AreEqual("0", persisted.GetOption("ttl"));
    }

    /// <summary>The block-based defaults of the same code are unchanged: 30 days for both with universal compaction.</summary>
    [TestMethod]
    public void UniversalCompactionWithBlockBasedTableKeepsThirtyDayDefaults()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { CompactionStyle = CompactionStyle.Universal });
        Assert.AreEqual(30UL * 24 * 60 * 60, db.DefaultColumnFamily.Options.PeriodicCompactionSeconds);
        Assert.AreEqual(30UL * 24 * 60 * 60, db.DefaultColumnFamily.Options.Ttl);
    }

    /// <summary>
    /// SanitizeCfOptions (db/column_family.cc): "if (db_options.atomic_flush &amp;&amp;
    /// result.min_write_buffer_number_to_merge &gt; 1) result.min_write_buffer_number_to_merge = 1" (after the clamp to
    /// max_write_buffer_number - 1). The OPTIONS file records the sanitized value.
    /// </summary>
    [TestMethod]
    public void AtomicFlushSanitizesMinWriteBufferNumberToMerge()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.AtomicFlush = true;
        var cf = new ColumnFamilyOptions { MaxWriteBufferNumber = 6, MinWriteBufferNumberToMerge = 3 };
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o, cf))
        {
            Assert.AreEqual(1, db.DefaultColumnFamily.Options.MinWriteBufferNumberToMerge);
        }
        Assert.AreEqual("1", LatestOptions(fs).FindColumnFamily("default")!.GetOption("min_write_buffer_number_to_merge"));
        // Without atomic_flush the value is only clamped to max_write_buffer_number - 1.
        MemoryFileSystem fs2 = NewFs();
        using RocksDatabase db2 = RocksDatabase.Open(DbPath, NewOptions(fs2), new ColumnFamilyOptions { MaxWriteBufferNumber = 3, MinWriteBufferNumberToMerge = 9 });
        Assert.AreEqual(2, db2.DefaultColumnFamily.Options.MinWriteBufferNumberToMerge);
    }

    /// <summary>
    /// BlockBasedTableFactory::InitializeOptions (table/block_based/block_based_table_factory.cc) sanitizes the table
    /// options held by the factory, so the DB's options and the OPTIONS file show the sanitized values:
    /// block_size_deviation outside [0, 100] -> 0, restart intervals &lt; 1 -> 1, index_block_restart_interval = 1 with
    /// kHashSearch, partition_filters = false without kTwoLevelIndexSearch, format_version &lt; 2 -> 2.
    /// </summary>
    [TestMethod]
    public void BlockBasedTableOptionsAreSanitizedLikeInitializeOptions()
    {
        MemoryFileSystem fs = NewFs();
        var cf = new ColumnFamilyOptions { PrefixExtractor = SliceTransform.CreateFixedPrefix(2) };
        cf.TableOptions.BlockSizeDeviation = 101;
        cf.TableOptions.BlockRestartInterval = 0;
        cf.TableOptions.IndexBlockRestartInterval = 8;
        cf.TableOptions.IndexType = IndexType.HashSearch;
        cf.TableOptions.PartitionFilters = true;
        cf.TableOptions.FormatVersion = 1;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cf))
        {
            BlockBasedTableOptions t = db.DefaultColumnFamily.Options.TableOptions;
            Assert.AreEqual(0, t.BlockSizeDeviation);
            Assert.AreEqual(1, t.BlockRestartInterval);
            Assert.AreEqual(1, t.IndexBlockRestartInterval);
            Assert.IsFalse(t.PartitionFilters);
            Assert.AreEqual(2u, t.FormatVersion);
            db.Put(B("aa1"), B("v"));
            db.Flush();
            Assert.AreEqual("v", GetString(db, "aa1"));
        }
        BlockBasedTableOptions persisted = LatestOptions(fs).FindColumnFamily("default")!.TableOptions;
        Assert.AreEqual("0", persisted.GetOption("block_size_deviation"));
        Assert.AreEqual("1", persisted.GetOption("block_restart_interval"));
        Assert.AreEqual("1", persisted.GetOption("index_block_restart_interval"));
        Assert.AreEqual("false", persisted.GetOption("partition_filters"));
        Assert.AreEqual("2", persisted.GetOption("format_version"));
        // The caller's object is not modified (the DB sanitizes a copy).
        Assert.AreEqual(1u, cf.TableOptions.FormatVersion);
    }

    // ---- DBImpl::ValidateOptions(const DBOptions&) --------------------------------------------------------------

    /// <summary>
    /// DBImpl::ValidateOptions(const DBOptions&amp;) (db/db_impl/db_impl_open.cc): each of these combinations fails
    /// DB::Open with the given status; the port only had the unordered_write / atomic_flush / offpeak checks.
    /// </summary>
    [TestMethod]
    public void OpenRejectsInvalidDbOptionCombinations()
    {
        (Action<DbOptions> Mutate, StatusCode Code, string Message)[] cases =
        [
            (static o => o.ReadIoExecutorThreads = 0, StatusCode.InvalidArgument, "read_io_executor_threads must be greater than zero"),
            (static o => { for (int i = 0; i < 5; i++) o.DbPaths.Add(("/p" + i, 1UL << 30)); }, StatusCode.NotSupported, "More than four DB paths"),
            (static o => { o.AllowMmapReads = true; o.UseDirectReads = true; }, StatusCode.NotSupported, "allow_mmap_reads"),
            (static o => { o.AllowMmapReads = true; o.UseDirectIoForCompactionReads = true; }, StatusCode.NotSupported, "use_direct_io_for_compaction_reads"),
            (static o => { o.AllowMmapWrites = true; o.UseDirectIoForFlushAndCompaction = true; }, StatusCode.NotSupported, "allow_mmap_writes"),
            (static o => o.KeepLogFileNum = 0, StatusCode.InvalidArgument, "keep_log_file_num must be greater than 0"),
            (static o => { o.UseDirectIoForFlushAndCompaction = true; o.WritableFileMaxBufferSize = 0; }, StatusCode.InvalidArgument,
                "writes in direct IO require writable_file_max_buffer_size > 0"),
            (static o => { o.WriteDbIdToManifest = false; o.WriteIdentityFile = false; }, StatusCode.InvalidArgument,
                "write_dbid_to_manifest and write_identity_file cannot both be false"),
        ];
        foreach ((Action<DbOptions> mutate, StatusCode code, string message) in cases)
        {
            MemoryFileSystem fs = NewFs();
            DbOptions o = NewOptions(fs);
            mutate(o);
            RocksDbException e = OpenFails(o, new ColumnFamilyOptions());
            Assert.AreEqual(code, e.Code, message);
            StringAssert.Contains(e.Message, message);
            Assert.IsNull(OptionsFile.FindLatestOptionsFile(fs, DbPath), message); // nothing was created
        }
    }

    // ---- BlockBasedTableFactory::ValidateOptions ----------------------------------------------------------------

    /// <summary>
    /// BlockBasedTableFactory::ValidateOptions (table/block_based/block_based_table_factory.cc), run by DB::Open
    /// (ValidateOptionsByTable): invalid table option combinations fail the open with InvalidArgument. The port
    /// reported some of them only when the first table was built (a background error after the DB accepted writes) and
    /// did not have the others.
    /// </summary>
    [TestMethod]
    public void OpenRejectsInvalidBlockBasedTableOptions()
    {
        (Action<ColumnFamilyOptions> Mutate, string Message)[] cases =
        [
            (static c => c.TableOptions.IndexType = IndexType.HashSearch, "Hash index is specified for block-based table, but prefix_extractor is not given"),
            (static c => { c.TableOptions.CacheIndexAndFilterBlocks = true; c.TableOptions.NoBlockCache = true; },
                "Enable cache_index_and_filter_blocks, , but block cache is disabled"),
            (static c => { c.TableOptions.PinL0FilterAndIndexBlocksInCache = true; c.TableOptions.NoBlockCache = true; },
                "Enable pin_l0_filter_and_index_blocks_in_cache, , but block cache is disabled"),
            (static c => c.TableOptions.FormatVersion = 99, "Unsupported BlockBasedTable format_version"),
            (static c => c.Compression = CompressionType.DisableCompressionOption, "kDisableCompressionOption not permitted for option: compression"),
            (static c => { c.TableOptions.BlockAlign = true; c.Compression = CompressionType.Snappy; }, "Enable block_align, but compression enabled"),
            (static c =>
            {
                c.TableOptions.BlockAlign = true;
                c.Compression = CompressionType.None;
                c.BottommostCompression = CompressionType.Zstd;
            }, "Enable block_align, but bottommost_compression enabled"),
            (static c =>
            {
                c.TableOptions.BlockAlign = true;
                c.Compression = CompressionType.None;
                c.CompressionPerLevel = [CompressionType.None, CompressionType.LZ4];
            }, "Enable block_align, but compression_per_level enabled"),
            (static c => { c.TableOptions.BlockAlign = true; c.Compression = CompressionType.None; c.TableOptions.BlockSize = 5000; },
                "Block alignment requested but block size is not a power of 2"),
            (static c => c.TableOptions.SuperBlockAlignmentSize = 3000, "Super Block alignment requested but super block alignment size is not a power of 2"),
            (static c => c.TableOptions.SuperBlockAlignmentSpaceOverheadRatio = 3, "Super block alignment space overhead is too high"),
            (static c => { c.TableOptions.DataBlockIndexType = DataBlockIndexType.BinaryAndHash; c.TableOptions.DataBlockHashTableUtilRatio = 0; },
                "data_block_hash_table_util_ratio should be greater than 0 when data_block_index_type is set to kDataBlockBinaryAndHash"),
            (static c => c.TableOptions.IndexMode = BlockBasedIndexMode.CustomOnly, "index_mode kCustomDefault/kCustomOnly requires user_defined_index_factory"),
        ];
        foreach ((Action<ColumnFamilyOptions> mutate, string message) in cases)
        {
            MemoryFileSystem fs = NewFs();
            var cf = new ColumnFamilyOptions();
            mutate(cf);
            RocksDbException e = OpenFails(NewOptions(fs), cf);
            Assert.AreEqual(StatusCode.InvalidArgument, e.Code, message);
            StringAssert.Contains(e.Message, message);
        }
        // kDisableCompressionOption is fine for bottommost_compression (the default) and compression_per_level, and
        // block_align without compression opens.
        MemoryFileSystem ok = NewFs();
        var good = new ColumnFamilyOptions { Compression = CompressionType.None };
        good.TableOptions.BlockAlign = true;
        good.CompressionPerLevel = [CompressionType.DisableCompressionOption, CompressionType.None];
        RocksDatabase.Open(DbPath, NewOptions(ok), good).Dispose();
    }

    // ---- OPTIONS text ---------------------------------------------------------------------------------------------

    /// <summary>
    /// block_base_table_prepopulate_block_cache_string_map (table/block_based/block_based_table_factory.cc) has
    /// kDisable, kFlushOnly and kFlushAndCompaction; an OPTIONS file with the third value did not parse.
    /// </summary>
    [TestMethod]
    public void PrepopulateBlockCacheFlushAndCompactionRoundTrips()
    {
        var t = new BlockBasedTableOptions();
        t.SetOption("prepopulate_block_cache", "kFlushAndCompaction", preserveUnknown: false);
        Assert.AreEqual(PrepopulateBlockCache.FlushAndCompaction, t.PrepopulateBlockCache);
        Assert.AreEqual("kFlushAndCompaction", t.GetOption("prepopulate_block_cache"));

        var file = new OptionsFile();
        var cf = new ColumnFamilyOptions();
        cf.TableOptions.PrepopulateBlockCache = PrepopulateBlockCache.FlushAndCompaction;
        file.ColumnFamilies.Add(new ColumnFamilyDescriptor("default", cf));
        OptionsFile parsed = OptionsFile.Parse(file.Serialize(), preserveUnknownOptions: false);
        Assert.AreEqual(PrepopulateBlockCache.FlushAndCompaction, parsed.FindColumnFamily("default")!.TableOptions.PrepopulateBlockCache);
    }

    /// <summary>
    /// ParseUint64 / ParseInt64 / ParseInt (util/string_util.cc): std::stoull / stoll / stoi read the leading number
    /// (optional whitespace and sign), then only the character right after it is checked for a k/m/g/t suffix and the
    /// rest is ignored ("64MB" is 64 MiB); the suffix shifts without an overflow check; stoull negates a "-" value
    /// modulo 2^64. A value with no digits or out of range is an error, which OptionTypeInfo::Parse
    /// (options/options_helper.cc) turns into InvalidArgument.
    /// </summary>
    [TestMethod]
    public void NumericOptionValuesParseLikeStringUtil()
    {
        var cf = new ColumnFamilyOptions();
        (string Text, ulong Expected)[] unsignedCases =
        [
            ("64MB", 64UL << 20), ("10kb", 10UL << 10), ("2G", 2UL << 30), ("1t", 1UL << 40), ("7", 7), ("12abc", 12), ("+5", 5),
            ("-1", ulong.MaxValue), ("18446744073709551615", ulong.MaxValue), ("18446744073709551615k", unchecked(ulong.MaxValue << 10)),
            ("1.5m", 1),
        ];
        foreach ((string text, ulong expected) in unsignedCases)
        {
            cf.SetOption("write_buffer_size", text, preserveUnknown: false);
            Assert.AreEqual(expected, cf.WriteBufferSize, text);
        }
        (string Text, int Expected)[] intCases = [("5k", 5 << 10), ("-3", -3), ("2m", 2 << 20), ("8x", 8), ("-2k", -2048)];
        foreach ((string text, int expected) in intCases)
        {
            cf.SetOption("level0_file_num_compaction_trigger", text, preserveUnknown: false);
            Assert.AreEqual(expected, cf.Level0FileNumCompactionTrigger, text);
        }
        cf.SetOption("max_write_buffer_size_to_maintain", "-4k", preserveUnknown: false);
        Assert.AreEqual(-4096L, cf.MaxWriteBufferSizeToMaintain);

        // Errors are InvalidArgument (never an OverflowException / FormatException escaping the parser).
        (string Name, string Text)[] bad =
        [
            ("write_buffer_size", "k"), ("write_buffer_size", ""), ("write_buffer_size", "abc"), ("write_buffer_size", "18446744073709551616"),
            ("level0_file_num_compaction_trigger", "99999999999"), ("level0_file_num_compaction_trigger", "-99999999999"),
            ("max_write_buffer_size_to_maintain", "9223372036854775808"), ("bloom_locality", "4294967296"), ("bloom_locality", "4g"),
        ];
        foreach ((string name, string text) in bad)
        {
            RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => cf.SetOption(name, text, preserveUnknown: false), name + "=" + text);
            Assert.AreEqual(StatusCode.InvalidArgument, e.Code, name + "=" + text);
        }
        // The same through an OPTIONS file.
        string textFile = new OptionsFile { ColumnFamilies = { new ColumnFamilyDescriptor("default", new ColumnFamilyOptions()) } }.Serialize()
            .Replace("max_open_files=-1", "max_open_files=99999999999", StringComparison.Ordinal);
        Assert.AreEqual(StatusCode.InvalidArgument,
            Assert.ThrowsExactly<RocksDbException>(() => OptionsFile.Parse(textFile, preserveUnknownOptions: false)).Code);
    }

    // ---- SetOptions / SetDBOptions --------------------------------------------------------------------------------

    /// <summary>
    /// cf_mutable_options_type_info / db_mutable_options_type_info (options/cf_options.cc, options/db_options.cc): the
    /// deprecated entries flagged kMutable are accepted and ignored by SetOptions / SetDBOptions
    /// (ConfigurableHelper::ConfigureSingleOption takes the mutable branch, OptionTypeInfo::Parse returns OK for
    /// kDeprecated); deprecated immutable ones are "Option not changeable".
    /// </summary>
    [TestMethod]
    public void DeprecatedMutableOptionsAreAcceptedBySetOptions()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        foreach (string name in new[]
                 {
                     "filter_deletes", "check_flush_compaction_key_order", "verify_checksums_in_compaction", "hard_rate_limit", "soft_rate_limit",
                     "ignore_max_compaction_bytes_for_input", "expanded_compaction_factor", "max_grandparent_overlap_factor",
                     "source_compaction_factor", "memtable_prefix_bloom_huge_page_tlb_size", "memtable_prefix_bloom_bits",
                     "memtable_prefix_bloom_probes", "min_partial_merge_operands", "snap_refresh_nanos", "bottommost_temperature",
                 })
        {
            db.SetOptions([P(name, "1"), P("level0_file_num_compaction_trigger", "5")]);
        }
        Assert.AreEqual(5, db.DefaultColumnFamily.Options.Level0FileNumCompactionTrigger);
        foreach (string name in new[] { "purge_redundant_kvs_while_flush", "max_mem_compaction_level" })
        {
            RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => db.SetOptions([P(name, "1")]), name);
            Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
            StringAssert.Contains(e.Message, "Option not changeable");
        }
        db.SetDBOptions([P("base_background_compactions", "2"), P("allow_os_buffer", "true"), P("max_background_jobs", "3")]);
        Assert.AreEqual(3, db.GetDBOptions().MaxBackgroundJobs);
        RocksDbException d = Assert.ThrowsExactly<RocksDbException>(() => db.SetDBOptions([P("skip_log_error_on_recovery", "true")]));
        Assert.AreEqual(StatusCode.InvalidArgument, d.Code);
        StringAssert.Contains(d.Message, "Option not changeable");
    }

    // ---- DbOptions -------------------------------------------------------------------------------------------------

    private sealed class CustomChecksumFactory : FileChecksumGenFactory
    {
        public override string Name => "Audit4CustomChecksumFactory";
        public override FileChecksumGenerator? CreateFileChecksumGenerator(FileChecksumGenContext context) => null;
    }

    /// <summary>
    /// DBOptions is copied by value by DB::Open / SetDBOptions (BuildDBOptions): a custom file_checksum_gen_factory
    /// object stays attached. The managed Clone re-parsed the option by name after assigning the object, which replaced
    /// every factory but the built-in crc32c one by null (so the DB silently computed no file checksums).
    /// </summary>
    [TestMethod]
    public void DbOptionsCloneKeepsCustomFileChecksumGenFactory()
    {
        var custom = new CustomChecksumFactory();
        var o = new DbOptions { FileChecksumGenFactory = custom };
        Assert.AreSame(custom, o.Clone().FileChecksumGenFactory);
        Assert.AreEqual("Audit4CustomChecksumFactory", o.Clone().GetOption("file_checksum_gen_factory"));
        var builtin = new DbOptions { FileChecksumGenFactory = FileChecksumGenCrc32cFactory.Instance };
        Assert.AreSame(FileChecksumGenCrc32cFactory.Instance, builtin.Clone().FileChecksumGenFactory);
        Assert.IsNull(new DbOptions().Clone().FileChecksumGenFactory);

        MemoryFileSystem fs = NewFs();
        DbOptions open = NewOptions(fs);
        open.FileChecksumGenFactory = custom;
        using RocksDatabase db = RocksDatabase.Open(DbPath, open);
        Assert.AreSame(custom, db.GetDBOptions().FileChecksumGenFactory);
    }
}
