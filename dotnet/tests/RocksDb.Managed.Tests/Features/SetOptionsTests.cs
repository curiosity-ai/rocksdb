using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Features;

/// <summary>DB::SetOptions / SetDBOptions (port of the spirit of db/db_options_test.cc).</summary>
[TestClass]
public sealed class SetOptionsTests
{
    private static KeyValuePair<string, string> P(string k, string v) => new(k, v);

    private static OptionsFile LatestOptions(MemoryFileSystem fs) =>
        OptionsFile.Load(fs, OptionsFile.FindLatestOptionsFile(fs, DbPath)!, preserveUnknownOptions: true);

    private static int L0Files(RocksDatabase db) => int.Parse(db.GetProperty("rocksdb.num-files-at-level0")!, System.Globalization.CultureInfo.InvariantCulture);

    [TestMethod]
    public void MutableCfOptionsTakeEffectAndArePersisted()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { Level0FileNumCompactionTrigger = 2 });
        db.SetOptions([P("disable_auto_compactions", "true"), P("level0_file_num_compaction_trigger", "3"), P("write_buffer_size", "131072")]);
        Assert.IsTrue(db.DefaultColumnFamily.Options.DisableAutoCompactions);
        Assert.AreEqual(131072UL, db.DefaultColumnFamily.Options.WriteBufferSize);
        OptionsFile f = LatestOptions(fs);
        Assert.IsTrue(f.FindColumnFamily("default")!.DisableAutoCompactions);
        Assert.AreEqual(3, f.FindColumnFamily("default")!.Level0FileNumCompactionTrigger);
        Assert.AreEqual(131072UL, f.FindColumnFamily("default")!.WriteBufferSize);

        // Auto compactions are off: L0 files accumulate beyond the trigger.
        for (int r = 0; r < 5; r++)
        {
            for (int i = 0; i < 50; i++) db.Put(Key(i), Value(i, r));
            db.Flush();
        }
        db.WaitForCompact();
        int accumulated = L0Files(db);
        Assert.IsTrue(accumulated >= 5, accumulated.ToString()); // (a small memtable may also fill up by itself)
        // The small write buffer makes the memtable fill and flush by itself.
        for (int i = 0; i < 2000; i++) db.Put(Key(10000 + i), B(new string('v', 200)));
        db.WaitForCompact();
        Assert.IsTrue(L0Files(db) > accumulated, L0Files(db).ToString());

        // Typed form: re-enable compactions.
        db.DefaultColumnFamily.SetOptions(o => o.DisableAutoCompactions = false);
        db.WaitForCompact();
        Assert.IsTrue(L0Files(db) < 3, L0Files(db).ToString());
        Assert.IsFalse(LatestOptions(fs).FindColumnFamily("default")!.DisableAutoCompactions);
        for (int i = 0; i < 50; i++) CollectionAssert.AreEqual(Value(i, 4), db.Get(Key(i)));
    }

    [TestMethod]
    public void InvalidSetOptionsChangesNothing()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        string before = OptionsFile.FindLatestOptionsFile(fs, DbPath)!;
        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => db.SetOptions([]));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        StringAssert.Contains(e.Message, "empty input");
        e = Assert.ThrowsExactly<RocksDbException>(() => db.SetOptions([P("write_buffer_size", "1048576"), P("num_levels", "3")]));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        StringAssert.Contains(e.Message, "Option not changeable: num_levels");
        e = Assert.ThrowsExactly<RocksDbException>(() => db.SetOptions([P("no_such_option", "1")]));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        e = Assert.ThrowsExactly<RocksDbException>(() => db.SetOptions([P("write_buffer_size", "abc")]));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        e = Assert.ThrowsExactly<RocksDbException>(() => db.DefaultColumnFamily.SetOptions(o => o.NumLevels = 4));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        Assert.AreEqual(64UL << 20, db.DefaultColumnFamily.Options.WriteBufferSize);
        Assert.AreEqual(before, OptionsFile.FindLatestOptionsFile(fs, DbPath));
        e = Assert.ThrowsExactly<RocksDbException>(() => db.SetDBOptions([P("create_if_missing", "false")]));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        StringAssert.Contains(e.Message, "Option not changeable: create_if_missing");
        e = Assert.ThrowsExactly<RocksDbException>(() => db.SetDBOptions([]));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
    }

    [TestMethod]
    public void MutableDbOptions()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.CreateMissingColumnFamilies = true;
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, [new("default", new ColumnFamilyOptions()), new("other", new ColumnFamilyOptions())]);
        db.SetDBOptions([P("max_background_jobs", "6"), P("delayed_write_rate", "1048576"), P("bytes_per_sync", "0"), P("stats_dump_period_sec", "0")]);
        DbOptions current = db.GetDBOptions();
        Assert.AreEqual(6, current.MaxBackgroundJobs);
        Assert.AreEqual(1048576UL, current.DelayedWriteRate);
        Assert.AreEqual(1048576UL, current.BytesPerSync); // 0 means 1 MiB, like upstream
        DbOptions persisted = LatestOptions(fs).DbOptions;
        Assert.AreEqual(6, persisted.MaxBackgroundJobs);
        Assert.AreEqual(1048576UL, persisted.DelayedWriteRate);
        db.SetDBOptions(x => x.MaxBackgroundJobs = 3);
        Assert.AreEqual(3, LatestOptions(fs).DbOptions.MaxBackgroundJobs);
        // Column family options of another CF.
        ColumnFamily other = db.GetColumnFamily("other");
        db.SetOptions(other, [P("compression", "kZSTD"), P("target_file_size_base", "1048576")]);
        Assert.AreEqual(RocksDb.Managed.Compression.CompressionType.Zstd, other.Options.Compression);
        Assert.AreEqual(RocksDb.Managed.Compression.CompressionType.Zstd, LatestOptions(fs).FindColumnFamily("other")!.Compression);
        Assert.AreNotEqual(RocksDb.Managed.Compression.CompressionType.Zstd, LatestOptions(fs).FindColumnFamily("default")!.Compression);
        other.Put(B("k"), B("v"));
        other.Flush();
        Assert.AreEqual("v", GetString(other, "k"));
    }

    [TestMethod]
    public void SetOptionsSurvivesReopenThroughOptionsFile()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            db.SetOptions([P("max_write_buffer_number", "5"), P("ttl", "3600")]);
        }
        ColumnFamilyOptions cf = LatestOptions(fs).FindColumnFamily("default")!;
        Assert.AreEqual(5, cf.MaxWriteBufferNumber);
        Assert.AreEqual(3600UL, cf.Ttl);
        using RocksDatabase again = RocksDatabase.Open(DbPath, NewOptions(fs), cf);
        Assert.AreEqual(5, again.DefaultColumnFamily.Options.MaxWriteBufferNumber);
    }

    /// <summary>Each super version carries an immutable options snapshot; SetOptions installs a new one.</summary>
    [TestMethod]
    public void SuperVersionsCarryImmutableOptionsSnapshots()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        RocksDb.Managed.Db.ColumnFamilyData cfd = db.DefaultColumnFamily.Data;
        RocksDb.Managed.Db.SuperVersion before = cfd.Super;
        ulong oldSkip = before.MutableOptions.MaxSequentialSkipInIterations;
        db.SetOptions([P("max_sequential_skip_in_iterations", "77")]);
        RocksDb.Managed.Db.SuperVersion after = cfd.Super;
        Assert.AreNotSame(before, after);
        Assert.AreEqual(oldSkip, before.MutableOptions.MaxSequentialSkipInIterations, "old snapshot unchanged");
        Assert.AreEqual(77UL, after.MutableOptions.MaxSequentialSkipInIterations);
        Assert.AreNotSame(cfd.Options, after.MutableOptions);
    }

    /// <summary>Mutable block-based table options (db_options_test.cc SetMutableTableOptions).</summary>
    [TestMethod]
    public void MutableTableOptionsApplyToNewTablesAndArePersisted()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        RocksDb.Managed.Table.TableFormatOptions oldFormat = db.DefaultColumnFamily.Data.Format;
        db.SetOptions([P("table_factory.block_size", "16384"), P("table_factory.block_restart_interval", "11")]);
        Assert.AreEqual(16384UL, db.DefaultColumnFamily.Options.TableOptions.BlockSize);
        Assert.AreNotSame(oldFormat, db.DefaultColumnFamily.Data.Format);
        db.SetOptions([P("block_based_table_factory", "{block_size=8192;block_restart_interval=7}")]);
        BlockBasedTableOptions persisted = LatestOptions(fs).FindColumnFamily("default")!.TableOptions;
        Assert.AreEqual("8192", persisted.GetOption("block_size"));
        Assert.AreEqual("7", persisted.GetOption("block_restart_interval"));
        // Unknown table options, invalid values and a different factory are refused; nothing changes.
        foreach (KeyValuePair<string, string> bad in new[] { P("table_factory.does_not_exist", "true"), P("table_factory.no_block_cache", "maybe"),
                     P("table_factory.block_cache", "lots"), P("table_factory", "PlainTable") })
        {
            var ex = Assert.ThrowsExactly<RocksDbException>(() => db.SetOptions([P("table_factory.block_size", "1024"), bad]));
            Assert.AreEqual(StatusCode.InvalidArgument, ex.Code, bad.Key);
        }
        Assert.AreEqual("8192", db.DefaultColumnFamily.Options.TableOptions.GetOption("block_size"));
    }

    /// <summary>
    /// Reader-affecting block-based table options (TableFactoryParseFn clones the factory for any field): the block cache,
    /// filter policy, cache/pin settings and index search type apply to table readers opened afterwards; readers already
    /// open keep theirs.
    /// </summary>
    [TestMethod]
    public void ReaderTableOptionsApplyToNewlyOpenedTables()
    {
        MemoryFileSystem fs = NewFs();
        var cf = new ColumnFamilyOptions { DisableAutoCompactions = true };
        cf.TableOptions.NoBlockCache = true;
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cf);
        RocksDb.Managed.Db.ColumnFamilyData cfd = db.DefaultColumnFamily.Data;
        for (int i = 0; i < 300; i++) db.Put(Key(i), Value(i, 0));
        db.Flush();
        Assert.AreEqual("value-5-0" + new string('x', 5), GetString(db, "key000005"));
        Assert.IsNull(cfd.ReaderOptions.FilterPolicy);

        db.SetOptions([P("table_factory.no_block_cache", "false"), P("table_factory.block_cache", "4M"),
            P("table_factory.filter_policy", "bloomfilter:10:false"), P("table_factory.cache_index_and_filter_blocks", "true"),
            P("table_factory.index_block_search_type", "kAuto"), P("table_factory.pin_top_level_index_and_filter", "false")]);
        RocksDb.Managed.Table.TableReaderOptions ro = cfd.ReaderOptions;
        var cache = (RocksDb.Managed.Cache.BlockCache)ro.BlockCache;
        Assert.AreEqual(4L << 20, cache.Capacity);
        Assert.IsInstanceOfType<RocksDb.Managed.Table.Filters.BloomFilterPolicy>(ro.FilterPolicy);
        Assert.IsTrue(ro.CacheIndexAndFilterBlocks);
        Assert.IsFalse(ro.PinTopLevelIndexAndFilter);
        Assert.AreEqual(IndexBlockSearchType.Auto, ro.IndexBlockSearchType);
        Assert.AreSame(ro, cfd.TableCache.ReaderOptions);
        // The table opened before keeps reading without the cache.
        Assert.AreEqual(0L, cache.Usage);
        Assert.AreEqual("value-5-0" + new string('x', 5), GetString(db, "key000005"));
        Assert.AreEqual(0L, cache.Usage);

        // A new table is opened with the new options: its blocks go to the new cache and it has a bloom filter.
        for (int i = 300; i < 600; i++) db.Put(Key(i), Value(i, 0));
        db.Flush();
        Assert.AreEqual("value-400-0" + new string('x', 400 % 50), GetString(db, "key000400"));
        Assert.IsTrue(cache.Usage > 0, "new reader uses the new block cache");
        LiveFileMetadata newest = db.GetLiveFilesMetadata().MaxBy(f => f.FileNumber)!;
        Assert.AreEqual("bloomfilter", db.GetPropertiesOfAllTables().Single(p => p.Key.EndsWith(newest.Name, StringComparison.Ordinal)).Value.FilterPolicyName);
        BlockBasedTableOptions persisted = LatestOptions(fs).FindColumnFamily("default")!.TableOptions;
        Assert.AreEqual("bloomfilter:10:false", persisted.GetOption("filter_policy"));
        Assert.AreEqual("kAuto", persisted.GetOption("index_block_search_type"));
        Assert.AreEqual("false", persisted.GetOption("no_block_cache"));

        // Typed SetOptions with a block cache object.
        RocksDb.Managed.Cache.BlockCache other = RocksDb.Managed.Cache.BlockCache.CreateLru(1 << 20);
        db.DefaultColumnFamily.SetOptions(o => o.TableOptions.BlockCache = other);
        Assert.AreSame(other, cfd.ReaderOptions.BlockCache);
    }
}
