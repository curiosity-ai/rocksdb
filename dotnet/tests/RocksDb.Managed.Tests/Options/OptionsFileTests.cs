using RocksDb.Managed.Compression;
using RocksDb.Managed.Table;
using RocksDb.Managed.Table.Filters;

namespace RocksDb.Managed.Tests.Options;

/// <summary>OPTIONS file parse/serialize tests (options/options_parser.cc semantics).</summary>
[TestClass]
public sealed class OptionsFileTests
{
    private const string Sample = """
        # This is a RocksDB option file.
        #
        [Version]
          rocksdb_version=11.12.0
          options_file_version=1.1

        [DBOptions]
          max_open_files=5000
          create_if_missing=true
          wal_recovery_mode=kAbsoluteConsistency
          info_log_level=WARN_LEVEL
          db_host_id=host\:1   # a comment
          some_future_db_option=42

        [CFOptions "default"]
          compression=kZSTD
          bottommost_compression=kDisableCompressionOption
          compression_opts={checksum=false;max_dict_buffer_bytes=0;max_dict_bytes=0;auto_skip_min_sample_every=0;enabled=false;auto_skip=false;max_compressed_bytes_per_kb=896;parallel_threads=1;zstd_max_train_bytes=0;level=3;use_zstd_dict_trainer=true;strategy=0;window_bits=-14;}
          max_bytes_for_level_multiplier_additional=1:2:3
          max_bytes_for_level_multiplier=8.500000
          write_buffer_size=128M
          comparator=leveldb.BytewiseComparator
          merge_operator=nullptr
          prefix_extractor=rocksdb.FixedPrefix.4
          compaction_style=kCompactionStyleUniversal
          compaction_options_universal={reduce_file_locking=true;size_ratio=1;}
          table_factory=BlockBasedTable

        [TableOptions/BlockBasedTable "default"]
          block_size=16384
          checksum=kCRC32c
          filter_policy=bloomfilter:10:false
          index_type=kTwoLevelIndexSearch
          metadata_cache_options={unpartitioned_pinning=kFallback;partition_pinning=kFallback;top_level_index_pinning=kFallback;}

        [CFOptions "my cf\#1"]
          num_levels=4
          compression_per_level=kNoCompression:kSnappyCompression:kLZ4Compression

        [TableOptions/BlockBasedTable "my cf\#1"]
          format_version=6
        """;

    [TestMethod]
    public void ParsesTypedAndUnknownOptions()
    {
        OptionsFile f = OptionsFile.Parse(Sample, preserveUnknownOptions: true);
        Assert.AreEqual(new Version(11, 12, 0), f.RocksDbVersion);
        Assert.AreEqual(new Version(1, 1), f.OptionsFileVersion);
        Assert.AreEqual(5000, f.DbOptions.MaxOpenFiles);
        Assert.IsTrue(f.DbOptions.CreateIfMissing);
        Assert.AreEqual(WalRecoveryMode.AbsoluteConsistency, f.DbOptions.WalRecoveryMode);
        Assert.AreEqual(InfoLogLevel.Warn, f.DbOptions.InfoLogLevel);
        Assert.AreEqual("host:1", f.DbOptions.DbHostId);
        Assert.AreEqual("42", f.DbOptions.UnknownOptions["some_future_db_option"]);

        Assert.AreEqual(2, f.ColumnFamilies.Count);
        ColumnFamilyOptions d = f.ColumnFamilies[0].Options;
        Assert.AreEqual("default", f.ColumnFamilies[0].Name);
        Assert.AreEqual(CompressionType.Zstd, d.Compression);
        Assert.AreEqual(CompressionType.DisableCompressionOption, d.BottommostCompression);
        Assert.AreEqual(3, d.CompressionOpts.Level);
        Assert.AreEqual("false", d.CompressionOpts.UnknownOptions["auto_skip"]);
        CollectionAssert.AreEqual(new List<int> { 1, 2, 3 }, d.MaxBytesForLevelMultiplierAdditional);
        Assert.AreEqual(8.5, d.MaxBytesForLevelMultiplier);
        Assert.AreEqual(128UL << 20, d.WriteBufferSize);
        Assert.IsNull(d.MergeOperatorName);
        Assert.AreEqual("rocksdb.FixedPrefix.4", d.PrefixExtractor?.AsString());
        Assert.AreEqual(CompactionStyle.Universal, d.CompactionStyle);
        // compaction_options_universal is typed (audit #3); members the port does not model are preserved.
        Assert.AreEqual(1u, d.CompactionOptionsUniversal.SizeRatio);
        Assert.IsTrue(d.CompactionOptionsUniversal.ReduceFileLocking);
        Assert.IsFalse(d.UnknownOptions.ContainsKey("compaction_options_universal"));
        Assert.AreEqual(16384UL, d.TableOptions.BlockSize);
        Assert.AreEqual(ChecksumType.Crc32c, d.TableOptions.Checksum);
        Assert.AreEqual(10, Assert.IsInstanceOfType<BloomFilterPolicy>(d.TableOptions.FilterPolicy).WholeBitsPerKey);
        Assert.AreEqual("bloomfilter:10:false", d.TableOptions.GetOption("filter_policy"));
        Assert.AreEqual(IndexType.TwoLevelIndexSearch, d.TableOptions.IndexType);

        Assert.AreEqual("my cf#1", f.ColumnFamilies[1].Name);
        ColumnFamilyOptions c = f.ColumnFamilies[1].Options;
        Assert.AreEqual(4, c.NumLevels);
        CollectionAssert.AreEqual(new List<CompressionType> { CompressionType.None, CompressionType.Snappy, CompressionType.LZ4 }, c.CompressionPerLevel);
        Assert.AreEqual(6u, c.TableOptions.FormatVersion);
    }

    [TestMethod]
    public void RoundTripPreservesEverything()
    {
        OptionsFile f = OptionsFile.Parse(Sample, preserveUnknownOptions: true);
        string text = f.Serialize();
        OptionsFile g = OptionsFile.Parse(text, preserveUnknownOptions: true);
        Assert.AreEqual(text, g.Serialize());
        AssertSameOptions(f.DbOptions.ToOptionsList(), g.DbOptions.ToOptionsList());
        for (int i = 0; i < f.ColumnFamilies.Count; i++)
        {
            Assert.AreEqual(f.ColumnFamilies[i].Name, g.ColumnFamilies[i].Name);
            AssertSameOptions(f.ColumnFamilies[i].Options.ToOptionsList(), g.ColumnFamilies[i].Options.ToOptionsList());
            AssertSameOptions(f.ColumnFamilies[i].Options.TableOptions.ToOptionsList(), g.ColumnFamilies[i].Options.TableOptions.ToOptionsList());
        }
        StringAssert.Contains(text, "[CFOptions \"my cf\\#1\"]\n  ");
        StringAssert.Contains(text, "[TableOptions/BlockBasedTable \"default\"]\n  ");
        StringAssert.Contains(text, "db_host_id=host\\:1\n");
        StringAssert.Contains(text, "  some_future_db_option=42\n");
    }

    private static void AssertSameOptions(List<KeyValuePair<string, string>> a, List<KeyValuePair<string, string>> b)
    {
        Assert.AreEqual(a.Count, b.Count);
        for (int i = 0; i < a.Count; i++) Assert.AreEqual(a[i], b[i]);
    }

    [TestMethod]
    public void DefaultsSerializeInNativeLayout()
    {
        var f = new OptionsFile();
        f.ColumnFamilies.Add(new ColumnFamilyDescriptor("default", new ColumnFamilyOptions()));
        string text = f.Serialize();
        StringAssert.StartsWith(text, "# This is a RocksDB option file.\n#\n# For detailed file format spec, please refer to the example file\n" +
                                      "# in examples/rocksdb_option_file_example.ini\n#\n\n[Version]\n  rocksdb_version=11.14.0\n  options_file_version=1.1\n\n[DBOptions]\n  ");
        StringAssert.Contains(text, "\n  \n\n[CFOptions \"default\"]\n  ");
        StringAssert.Contains(text, "  compression=kLZ4Compression\n");
        StringAssert.Contains(text, "  max_bytes_for_level_multiplier=10.000000\n");
        StringAssert.Contains(text, "  compression_opts={window_bits=-14;level=32767;strategy=0;");
        StringAssert.Contains(text, "  merge_operator=nullptr\n");
        StringAssert.Contains(text, "  checksum=kXXH3\n");
        Assert.IsFalse(text.Contains("compression_per_level", StringComparison.Ordinal)); // empty values are skipped
        Assert.IsTrue(text.EndsWith("\n  \n", StringComparison.Ordinal));
        OptionsFile g = OptionsFile.Parse(text, preserveUnknownOptions: false);
        Assert.AreEqual(text, g.Serialize());
    }

    [TestMethod]
    public void StrictModeRejectsUnknownOptions()
    {
        var ex = Assert.ThrowsExactly<RocksDbException>(() => OptionsFile.Parse(Sample, preserveUnknownOptions: false));
        Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
    }

    [TestMethod]
    public void InvalidFiles()
    {
        static void Expect(StatusCode code, string text)
        {
            var ex = Assert.ThrowsExactly<RocksDbException>(() => OptionsFile.Parse(text, preserveUnknownOptions: true));
            Assert.AreEqual(code, ex.Code, text);
        }
        Expect(StatusCode.Corruption, "[Version]\n  rocksdb_version=1.2.3\n");
        Expect(StatusCode.Corruption, "[DBOptions]\n  a=1\n");
        Expect(StatusCode.InvalidArgument, "[DBOptions]\n[CFOptions \"x\"]\n");
        Expect(StatusCode.InvalidArgument, "[DBOptions]\n[DBOptions]\n[CFOptions \"default\"]\n");
        Expect(StatusCode.InvalidArgument, "[DBOptions]\n[CFOptions \"default\"]\n[CFOptions \"default\"]\n");
        Expect(StatusCode.InvalidArgument, "[DBOptions]\n[CFOptions \"default\"]\n[TableOptions/BlockBasedTable \"nope\"]\n");
        Expect(StatusCode.InvalidArgument, "[DBOptions]\n[CFOptions \"default\"]\n[Bogus]\n");
        Expect(StatusCode.InvalidArgument, "[DBOptions]\n  novalue\n[CFOptions \"default\"]\n");
        Expect(StatusCode.InvalidArgument, "[Version]\n  rocksdb_version=1.2.3.4\n[DBOptions]\n[CFOptions \"default\"]\n");
        Expect(StatusCode.InvalidArgument, "[Version]\n  options_file_version=0.1\n[DBOptions]\n[CFOptions \"default\"]\n");
        Expect(StatusCode.InvalidArgument, "[DBOptions]\n  max_open_files=abc\n[CFOptions \"default\"]\n");
        Expect(StatusCode.InvalidArgument, "[DBOptions]\n  wal_recovery_mode=kNope\n[CFOptions \"default\"]\n");
        // Minimal valid file.
        OptionsFile ok = OptionsFile.Parse("[DBOptions]\n[CFOptions \"default\"]\n", preserveUnknownOptions: false);
        Assert.AreEqual(1, ok.ColumnFamilies.Count);
    }

    [TestMethod]
    public void IntegerSuffixesAndCustomCompression()
    {
        var o = new ColumnFamilyOptions();
        o.SetOption("write_buffer_size", "2k", preserveUnknown: false);
        Assert.AreEqual(2048UL, o.WriteBufferSize);
        o.SetOption("max_write_buffer_size_to_maintain", "-1", preserveUnknown: false);
        Assert.AreEqual(-1L, o.MaxWriteBufferSizeToMaintain);
        o.SetOption("compression", "kCustomCompression8A", preserveUnknown: false);
        Assert.AreEqual((CompressionType)0x8A, o.Compression);
        Assert.AreEqual("kCustomCompression8A", o.GetOption("compression"));
        o.SetOption("prefix_extractor", "nullptr", preserveUnknown: false);
        Assert.IsNull(o.PrefixExtractor);
    }

    [TestMethod]
    public void CloneIsDeep()
    {
        var o = new ColumnFamilyOptions();
        o.TableOptions.BlockSize = 1;
        o.CompressionOpts.Level = 5;
        o.UnknownOptions["x"] = "y";
        ColumnFamilyOptions c = o.Clone();
        c.TableOptions.BlockSize = 2;
        c.CompressionOpts.Level = 6;
        Assert.AreEqual(1UL, o.TableOptions.BlockSize);
        Assert.AreEqual(5, o.CompressionOpts.Level);
        Assert.AreEqual("y", c.UnknownOptions["x"]);
    }
}
