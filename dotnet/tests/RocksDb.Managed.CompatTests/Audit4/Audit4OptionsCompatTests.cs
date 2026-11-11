using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Env;

namespace RocksDb.Managed.CompatTests.Audit4;

/// <summary>
/// Audit #4, slice Options: differential tests against native librocksdb for option sanitization (what the OPTIONS
/// file records for out-of-range inputs), the option validation of DB::Open and the value syntax accepted by
/// SetOptions.
/// </summary>
[TestClass]
public sealed class Audit4OptionsCompatTests
{
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    private static KeyValuePair<string, string> P(string k, string v) => new(k, v);

    private static OptionsFile Latest(string dir) =>
        OptionsFile.Load(LocalFileSystem.Instance, OptionsFile.FindLatestOptionsFile(LocalFileSystem.Instance, dir)!, preserveUnknownOptions: true);

    private static RocksDbSharp.DbOptions NativeOptions()
    {
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        s_keepAlive.Add(o);
        return o;
    }

    private static RocksDbSharp.Native Native => RocksDbSharp.Native.Instance;

    /// <summary>
    /// SanitizeCfOptions (db/column_family.cc) and BlockBasedTableFactory::InitializeOptions
    /// (table/block_based/block_based_table_factory.cc): for the same out-of-range inputs both engines record the same
    /// sanitized values in their OPTIONS file (atomic_flush forces min_write_buffer_number_to_merge = 1; restart
    /// intervals, block_size_deviation, partition_filters and format_version are sanitized by the table factory).
    /// </summary>
    [TestMethod]
    public void SanitizedColumnFamilyAndTableOptionsMatchNative()
    {
        using var nativeDir = new TempDir("a4opt-san-n");
        using var managedDir = new TempDir("a4opt-san-m");

        RocksDbSharp.DbOptions ndb = NativeOptions();
        Native.rocksdb_options_set_atomic_flush(ndb.Handle, 1);
        var ncf = new RocksDbSharp.ColumnFamilyOptions();
        Native.rocksdb_options_set_max_write_buffer_number(ncf.Handle, 6);
        Native.rocksdb_options_set_min_write_buffer_number_to_merge(ncf.Handle, 3);
        var nt = new RocksDbSharp.BlockBasedTableOptions();
        Native.rocksdb_block_based_options_set_format_version(nt.Handle, 1);
        Native.rocksdb_block_based_options_set_block_restart_interval(nt.Handle, 0);
        Native.rocksdb_block_based_options_set_block_size_deviation(nt.Handle, 101);
        Native.rocksdb_block_based_options_set_partition_filters(nt.Handle, 1);
        ncf.SetBlockBasedTableFactory(nt);
        s_keepAlive.Add(ncf);
        s_keepAlive.Add(nt);
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(ndb, nativeDir.Path, new RocksDbSharp.ColumnFamilies(ncf))) db.Put("k", "v");

        var mcf = new ColumnFamilyOptions { MaxWriteBufferNumber = 6, MinWriteBufferNumberToMerge = 3 };
        mcf.TableOptions.FormatVersion = 1;
        mcf.TableOptions.BlockRestartInterval = 0;
        mcf.TableOptions.BlockSizeDeviation = 101;
        mcf.TableOptions.PartitionFilters = true;
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true, AtomicFlush = true }, mcf)) db.Put("k"u8, "v"u8);

        ColumnFamilyOptions n = Latest(nativeDir.Path).FindColumnFamily("default")!;
        ColumnFamilyOptions m = Latest(managedDir.Path).FindColumnFamily("default")!;
        Assert.AreEqual("1", n.GetOption("min_write_buffer_number_to_merge"));
        foreach (string name in new[] { "min_write_buffer_number_to_merge", "max_write_buffer_number" })
        {
            Assert.AreEqual(n.GetOption(name), m.GetOption(name), name);
        }
        foreach (string name in new[] { "format_version", "block_restart_interval", "index_block_restart_interval", "block_size_deviation", "partition_filters" })
        {
            Assert.AreEqual(n.TableOptions.GetOption(name), m.TableOptions.GetOption(name), name);
        }
    }

    /// <summary>
    /// SanitizeCfOptions (db/column_family.cc), is_block_based_table: with a plain table the ttl default becomes 0 and
    /// periodic_compaction_seconds is not raised to 30 days for universal compaction; both engines open such a DB and
    /// record the same values.
    /// </summary>
    [TestMethod]
    public void UniversalCompactionWithPlainTableMatchesNative()
    {
        using var nativeDir = new TempDir("a4opt-plain-n");
        using var managedDir = new TempDir("a4opt-plain-m");
        RocksDbSharp.DbOptions ndb = NativeOptions();
        Native.rocksdb_options_set_allow_mmap_reads(ndb.Handle, 1);
        var ncf = new RocksDbSharp.ColumnFamilyOptions().SetCompactionStyle(RocksDbSharp.Compaction.Universal);
        ncf.SetPlainTableFactory(0, 10, 0.75, 16, 0, (char)0, false, false);
        s_keepAlive.Add(ncf);
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(ndb, nativeDir.Path, new RocksDbSharp.ColumnFamilies(ncf)))
        {
        }
        var mcf = new ColumnFamilyOptions { CompactionStyle = CompactionStyle.Universal, TableFactory = TableFactoryType.PlainTable };
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true, AllowMmapReads = true }, mcf))
        {
        }
        ColumnFamilyOptions n = Latest(nativeDir.Path).FindColumnFamily("default")!;
        ColumnFamilyOptions m = Latest(managedDir.Path).FindColumnFamily("default")!;
        Assert.AreEqual("PlainTable", n.GetOption("table_factory"));
        foreach (string name in new[] { "ttl", "periodic_compaction_seconds", "table_factory", "compaction_style" })
        {
            Assert.AreEqual(n.GetOption(name), m.GetOption(name), name);
        }
        Assert.AreEqual("0", m.GetOption("periodic_compaction_seconds"));
    }

    /// <summary>
    /// DBImpl::ValidateOptions (db/db_impl/db_impl_open.cc) and BlockBasedTableFactory::ValidateOptions
    /// (table/block_based/block_based_table_factory.cc): option combinations native DB::Open rejects are rejected by the
    /// managed open as well (InvalidArgument / NotSupported), before anything is created.
    /// </summary>
    [TestMethod]
    public void InvalidOptionCombinationsFailOpenLikeNative()
    {
        (string Name, Action<RocksDbSharp.DbOptions, RocksDbSharp.ColumnFamilyOptions, RocksDbSharp.BlockBasedTableOptions> Native,
            Action<DbOptions, ColumnFamilyOptions> Managed, StatusCode Code)[] cases =
        [
            ("keep_log_file_num=0", static (d, _, _) => Native.rocksdb_options_set_keep_log_file_num(d.Handle, 0),
                static (d, _) => d.KeepLogFileNum = 0, StatusCode.InvalidArgument),
            ("no identity", static (d, _, _) =>
                {
                    Native.rocksdb_options_set_write_dbid_to_manifest(d.Handle, 0);
                    Native.rocksdb_options_set_write_identity_file(d.Handle, 0);
                },
                static (d, _) => { d.WriteDbIdToManifest = false; d.WriteIdentityFile = false; }, StatusCode.InvalidArgument),
            ("mmap + direct reads", static (d, _, _) =>
                {
                    Native.rocksdb_options_set_allow_mmap_reads(d.Handle, 1);
                    Native.rocksdb_options_set_use_direct_reads(d.Handle, 1);
                },
                static (d, _) => { d.AllowMmapReads = true; d.UseDirectReads = true; }, StatusCode.NotSupported),
            ("cache_index_and_filter_blocks without block cache", static (_, _, t) =>
                {
                    Native.rocksdb_block_based_options_set_cache_index_and_filter_blocks(t.Handle, 1);
                    Native.rocksdb_block_based_options_set_no_block_cache(t.Handle, 1);
                },
                static (_, c) => { c.TableOptions.CacheIndexAndFilterBlocks = true; c.TableOptions.NoBlockCache = true; }, StatusCode.InvalidArgument),
            ("pin_l0 without block cache", static (_, _, t) =>
                {
                    Native.rocksdb_block_based_options_set_pin_l0_filter_and_index_blocks_in_cache(t.Handle, 1);
                    Native.rocksdb_block_based_options_set_no_block_cache(t.Handle, 1);
                },
                static (_, c) => { c.TableOptions.PinL0FilterAndIndexBlocksInCache = true; c.TableOptions.NoBlockCache = true; }, StatusCode.InvalidArgument),
            ("hash index without prefix extractor", static (_, _, t) => Native.rocksdb_block_based_options_set_index_type(t.Handle, 1),
                static (_, c) => c.TableOptions.IndexType = IndexType.HashSearch, StatusCode.InvalidArgument),
            ("block_align with compression", static (_, c, t) =>
                {
                    c.SetCompression(RocksDbSharp.Compression.Lz4);
                    Native.rocksdb_block_based_options_set_block_align(t.Handle, 1);
                },
                static (_, c) => { c.Compression = CompressionType.LZ4; c.TableOptions.BlockAlign = true; }, StatusCode.InvalidArgument),
        ];
        foreach (var c in cases)
        {
            using var nativeDir = new TempDir("a4opt-val-n");
            using var managedDir = new TempDir("a4opt-val-m");
            RocksDbSharp.DbOptions ndb = NativeOptions();
            var ncf = new RocksDbSharp.ColumnFamilyOptions();
            var nt = new RocksDbSharp.BlockBasedTableOptions();
            c.Native(ndb, ncf, nt);
            ncf.SetBlockBasedTableFactory(nt);
            s_keepAlive.Add(ncf);
            s_keepAlive.Add(nt);
            Assert.ThrowsExactly<RocksDbSharp.RocksDbException>(
                () => RocksDbSharp.RocksDb.Open(ndb, nativeDir.Path, new RocksDbSharp.ColumnFamilies(ncf)).Dispose(), c.Name);

            var mdb = new DbOptions { CreateIfMissing = true };
            var mcf = new ColumnFamilyOptions();
            c.Managed(mdb, mcf);
            RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(managedDir.Path, mdb, mcf).Dispose(), c.Name);
            Assert.AreEqual(c.Code, e.Code, c.Name);
            Assert.IsEmpty(Directory.GetFiles(managedDir.Path), c.Name);
        }
    }

    /// <summary>
    /// ParseUint64 / ParseInt (util/string_util.cc) and the kDeprecated | kMutable entries of
    /// cf_mutable_options_type_info (options/cf_options.cc) through DB::SetOptions: a size suffix followed by other
    /// characters ("8MB", "10kb"), a negative value for an unsigned option and a deprecated mutable option are accepted
    /// by both engines and end as the same values in the OPTIONS file; a value without digits fails in both.
    /// </summary>
    [TestMethod]
    public void SetOptionsValueSyntaxMatchesNative()
    {
        using var nativeDir = new TempDir("a4opt-set-n");
        using var managedDir = new TempDir("a4opt-set-m");
        KeyValuePair<string, string>[] changes =
        [
            P("write_buffer_size", "8MB"), P("max_bytes_for_level_base", "10kb"), P("target_file_size_base", "2M"),
            P("level0_file_num_compaction_trigger", "1k"), P("max_compaction_bytes", "-1"), P("filter_deletes", "true"),
            P("max_successive_merges", "+7"), P("soft_rate_limit", "0.5"),
        ];
        string[] names = ["write_buffer_size", "max_bytes_for_level_base", "target_file_size_base", "level0_file_num_compaction_trigger",
            "max_compaction_bytes", "max_successive_merges"];
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(NativeOptions(), nativeDir.Path))
        {
            db.SetOptions(changes);
            Assert.ThrowsExactly<RocksDbSharp.RocksDbException>(() => db.SetOptions([P("write_buffer_size", "MB")]));
            Assert.ThrowsExactly<RocksDbSharp.RocksDbException>(() => db.SetOptions([P("level0_file_num_compaction_trigger", "99999999999")]));
        }
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true }))
        {
            db.SetOptions(changes);
            Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => db.SetOptions([P("write_buffer_size", "MB")])).Code);
            Assert.AreEqual(StatusCode.InvalidArgument,
                Assert.ThrowsExactly<RocksDbException>(() => db.SetOptions([P("level0_file_num_compaction_trigger", "99999999999")])).Code);
        }
        ColumnFamilyOptions n = Latest(nativeDir.Path).FindColumnFamily("default")!;
        ColumnFamilyOptions m = Latest(managedDir.Path).FindColumnFamily("default")!;
        Assert.AreEqual((8UL << 20).ToString(System.Globalization.CultureInfo.InvariantCulture), n.GetOption("write_buffer_size"));
        foreach (string name in names) Assert.AreEqual(n.GetOption(name), m.GetOption(name), name);
    }
}
