using RocksDb.Managed.Env;
using RocksDb.Managed.Tools;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Regression tests of the audit #4 follow-up on the tools (ldb / sst_dump), the trie index, listeners, option defaults,
/// caches and statistics.
/// </summary>
[TestClass]
public sealed partial class Audit4FollowUpMiscTests
{
    private static (int Rc, string Out, string Err) Tool(IFileSystem fs, params string[] args)
    {
        var o = new StringWriter { NewLine = "\n" };
        var e = new StringWriter { NewLine = "\n" };
        int rc = ToolMain.Run(args, fs, o, e);
        return (rc, o.ToString(), e.ToString());
    }

    private static string Ldb(IFileSystem fs, params string[] args)
    {
        (int rc, string o, string e) = Tool(fs, ["ldb", "--db=" + DbPath, .. args]);
        Assert.AreEqual(0, rc, e);
        return o;
    }

    // ---- ldb ---------------------------------------------------------------------------------------------------

    [TestMethod]
    public void LdbScanAndDumpPrintTheColumnsOfAnEntity()
    {
        // LDBCommand::PrintKeyValueOrWideColumns (tools/ldb_cmd.cc): an entity that is not just a default column prints
        // "name:value name:value" (WideColumnsHelper::DumpWideColumns, the default column name is empty; with value_hex
        // names and values are 0x-prefixed hex and the dump is not hexed a second time). The port printed only the
        // default column's value.
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            db.Put(B("a"), B("1"));
            db.DefaultColumnFamily.PutEntity(B("e"), [new WideColumn(ReadOnlyMemory<byte>.Empty, B("dv")), new WideColumn(B("c1"), B("v1"))]);
            db.DefaultColumnFamily.PutEntity(B("f"), [new WideColumn(B("c1"), B("v1")), new WideColumn(B("c2"), B("v2"))]);
            // An entity with only the default column prints like a plain value (HasDefaultColumnOnly).
            db.DefaultColumnFamily.PutEntity(B("g"), [new WideColumn(ReadOnlyMemory<byte>.Empty, B("only"))]);
        }
        Assert.AreEqual("a ==> 1\ne ==> :dv c1:v1\nf ==> c1:v1 c2:v2\ng ==> only\n", Ldb(fs, "scan"));
        Assert.AreEqual("a ==> 1\ne ==> :dv c1:v1\nf ==> c1:v1 c2:v2\ng ==> only\nKeys in range: 4\n", Ldb(fs, "dump"));
        Assert.AreEqual("0x65 ==> :0x6476 0x6331:0x7631\n", Ldb(fs, "--hex", "scan", "--from=0x65", "--to=0x66"));
    }

    [TestMethod]
    public void LdbDumpStopsAtTheEndKeyInComparatorOrder()
    {
        // DBDumperCommand::DoDumpCommand: "if (!null_to_ && ucmp->Compare(iter->key(), to_) >= 0) break" -- the column
        // family's comparator, unlike ScanCommand ("it->key().ToString() < end_key_"). With the reverse bytewise
        // comparator the port's bytewise test stopped at the first key.
        MemoryFileSystem fs = NewFs();
        var cfo = new ColumnFamilyOptions { Comparator = Comparator.ReverseBytewise };
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), [new ColumnFamilyDescriptor("default", cfo)]))
        {
            db.Put(B("a"), B("1"));
            db.Put(B("b"), B("2"));
            db.Put(B("c"), B("3"));
        }
        Assert.AreEqual("c ==> 3\nb ==> 2\nKeys in range: 2\n", Ldb(fs, "dump", "--to=a"));
    }

    [TestMethod]
    public void LdbIdumpWithMaxKeysZeroPrintsOneKey()
    {
        // InternalDumpCommand passes max_keys to GetAllKeyVersions (utilities/debug.cc), which stops at
        // "++num_keys >= max_num_ikeys" after adding a key: --max_keys=0 returns one key version (the command's own
        // "max_keys_ > 0" test never fires). The port printed every key.
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            db.Put(B("a"), B("1"));
            db.Put(B("b"), B("2"));
        }
        Assert.AreEqual("'a' seq:1, type:1 => 1\nInternal keys in range: 1\n", Ldb(fs, "idump", "--max_keys=0"));
        Assert.AreEqual("'a' seq:1, type:1 => 1\n'b' seq:2, type:1 => 2\nInternal keys in range: 2\n", Ldb(fs, "idump", "--max_keys=2"));
    }

    [TestMethod]
    public void LdbGetPropertyPrintsMapPropertiesAndTheNativeFailure()
    {
        // GetPropertyCommand::DoCommand: GetMapProperty first ("<property>.<key>: <value>" per entry), then GetProperty
        // ("<property>: <value>"), else Failed("failed to get property: <property>").
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            db.Put(B("a"), B("1"));
            db.Flush();
        }
        string map = Ldb(fs, "get_property", "rocksdb.aggregated-table-properties");
        StringAssert.Contains(map, "rocksdb.aggregated-table-properties.num_entries: 1\n");
        Assert.IsFalse(map.Contains("rocksdb.aggregated-table-properties: ", StringComparison.Ordinal), map);
        Assert.AreEqual("rocksdb.num-files-at-level0: 1\n", Ldb(fs, "get_property", "rocksdb.num-files-at-level0"));
        (int rc, _, string err) = Tool(fs, "ldb", "--db=" + DbPath, "get_property", "rocksdb.nope");
        Assert.AreEqual(1, rc);
        Assert.AreEqual("Failed: failed to get property: rocksdb.nope\n", err);
    }

    // ---- trie index: S80 ---------------------------------------------------------------------------------------

    [TestMethod]
    [DataRow(0)]
    [DataRow(2)]
    [DataRow(200)]
    public void TrieIndexHandlesADataBlockEndingWithTheEmptyUserKey(int otherKeys)
    {
        // S80. A data block whose last key is the empty user key gets the empty separator (TrieIndexBuilder::
        // AddIndexEntry: FindShortestSeparator("", next) = "", and the last block keeps its last key). Upstream's
        // LoudsTrieBuilder::Finish (louds_trie.cc) adds no label for a zero-length key, so its handle is dropped while
        // the header still counts it (an assert in debug builds, a malformed trie in release builds). The port threw
        // InvalidArgument, which failed the flush for good. The managed builder now stores the empty key as the root's
        // prefix key, which the trie format and both iterators already support (Next from a prefix key with an empty
        // path, DescendToLeftmostLeaf's prefix check at the root).
        MemoryFileSystem fs = NewFs();
        var table = new BlockBasedTableOptions
        {
            UserDefinedIndexFactory = TrieIndexFactory.Instance, IndexMode = BlockBasedIndexMode.CustomOnly, BlockSize = 64,
        };
        var cfo = new ColumnFamilyOptions { TableOptions = table };
        byte[] big = new byte[200];
        Array.Fill(big, (byte)'v');
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), [new ColumnFamilyDescriptor("default", cfo)]);
        db.Put(ReadOnlySpan<byte>.Empty, big);
        // 200 one-byte keys make the root a LOUDS-Dense node; 2 keep it sparse; 0 leaves a label-less root.
        var expected = new List<string> { "" };
        for (int i = 0; i < otherKeys; i++)
        {
            byte[] k = [(byte)(i + 0x20)];
            db.Put(k, big);
            expected.Add(System.Text.Encoding.Latin1.GetString(k));
        }
        db.Flush();
        CollectionAssert.AreEqual(big, db.Get(ReadOnlySpan<byte>.Empty));
        Assert.IsNull(db.Get([0x01]));
        var forward = new List<string>();
        using (Iterator it = db.NewIterator())
        {
            for (it.SeekToFirst(); it.Valid; it.Next()) forward.Add(System.Text.Encoding.Latin1.GetString(it.Key));
            var backward = new List<string>();
            for (it.SeekToLast(); it.Valid; it.Prev()) backward.Add(System.Text.Encoding.Latin1.GetString(it.Key));
            backward.Reverse();
            CollectionAssert.AreEqual(forward, backward);
            it.Seek(ReadOnlySpan<byte>.Empty);
            Assert.IsTrue(it.Valid);
            Assert.AreEqual(0, it.Key.Length);
            it.SeekForPrev([0x01]);
            Assert.IsTrue(it.Valid);
            Assert.AreEqual(0, it.Key.Length);
        }
        CollectionAssert.AreEqual(expected, forward);
    }

    // ---- listeners (GAP-13) ------------------------------------------------------------------------------------

    private sealed class FlushListener : EventListener
    {
        public readonly List<FlushJobInfo> Completed = new();

        public override void OnFlushCompleted(RocksDatabase? db, FlushJobInfo info)
        {
            lock (Completed) Completed.Add(info);
        }
    }

    [TestMethod]
    public void FlushJobInfoStallFlagsCompareTheL0FileCountWithTheTriggers()
    {
        // DBImpl::NotifyOnFlushCompleted (db/db_impl/db_impl_compaction_flush.cc): triggered_writes_slowdown =
        // NumLevelFiles(0) >= level0_slowdown_writes_trigger, triggered_writes_stop = NumLevelFiles(0) >=
        // level0_stop_writes_trigger, whatever the write controller says. The port reported the column family's write
        // stall condition, which ignores L0 while auto compactions are disabled (and is Stopped for other causes).
        MemoryFileSystem fs = NewFs();
        var listener = new FlushListener();
        DbOptions o = NewOptions(fs);
        o.Listeners.Add(listener);
        var cfo = new ColumnFamilyOptions { DisableAutoCompactions = true, Level0FileNumCompactionTrigger = 2, Level0SlowdownWritesTrigger = 2, Level0StopWritesTrigger = 3 };
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, [new ColumnFamilyDescriptor("default", cfo)]);
        for (int i = 0; i < 3; i++)
        {
            db.Put(Key(i), B("v"));
            db.Flush();
        }
        lock (listener.Completed)
        {
            Assert.AreEqual(3, listener.Completed.Count);
            Assert.IsFalse(listener.Completed[0].TriggeredWritesSlowdown);
            Assert.IsFalse(listener.Completed[0].TriggeredWritesStop);
            Assert.IsTrue(listener.Completed[1].TriggeredWritesSlowdown, "2 L0 files >= level0_slowdown_writes_trigger");
            Assert.IsFalse(listener.Completed[1].TriggeredWritesStop);
            Assert.IsTrue(listener.Completed[2].TriggeredWritesSlowdown);
            Assert.IsTrue(listener.Completed[2].TriggeredWritesStop, "3 L0 files >= level0_stop_writes_trigger");
        }
    }

    // ---- option defaults (GAP-13) ------------------------------------------------------------------------------

    [TestMethod]
    public void MaxCompactionBytesDefaultFollowsTargetFileSizeBase()
    {
        // advanced_options.h: max_compaction_bytes = 0; SanitizeCfOptions (db/column_family.cc): 0 becomes
        // target_file_size_base * 25, except for FIFO with use_kv_ratio_compaction where 0 is kept ("derive the target
        // from the capacity"). The port defaulted to the fixed 64 MiB * 25.
        Assert.AreEqual(64UL * 1048576 * 25, new ColumnFamilyOptions().MaxCompactionBytes);
        Assert.AreEqual(8UL * 1048576 * 25, new ColumnFamilyOptions { TargetFileSizeBase = 8UL << 20 }.MaxCompactionBytes);
        Assert.AreEqual(123UL, new ColumnFamilyOptions { TargetFileSizeBase = 8UL << 20, MaxCompactionBytes = 123 }.MaxCompactionBytes);
        var fifo = new ColumnFamilyOptions { CompactionStyle = CompactionStyle.Fifo };
        fifo.CompactionOptionsFifo.UseKvRatioCompaction = true;
        Assert.AreEqual(0UL, fifo.MaxCompactionBytes);

        MemoryFileSystem fs = NewFs();
        var cfo = new ColumnFamilyOptions { TargetFileSizeBase = 8UL << 20 };
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), [new ColumnFamilyDescriptor("default", cfo)]);
        Assert.AreEqual(8UL * 1048576 * 25, db.DefaultColumnFamily.Options.MaxCompactionBytes);
        // Like upstream, the value is fixed at open: a later target_file_size_base change does not move it.
        db.DefaultColumnFamily.SetOptions([new KeyValuePair<string, string>("target_file_size_base", "1048576")]);
        Assert.AreEqual(8UL * 1048576 * 25, db.DefaultColumnFamily.Options.MaxCompactionBytes);
    }

    // ---- option parsing (S58, S60) -----------------------------------------------------------------------------

    [TestMethod]
    public void ParseDoubleReadsTheLongestNumericPrefixLikeStod()
    {
        // ParseDouble (util/string_util.cc) is std::stod: leading whitespace is skipped, the longest numeric prefix is
        // converted and the rest ignored; "inf" / "nan" are numbers; no digits (std::invalid_argument) and overflow
        // (std::out_of_range) fail. The port rejected trailing characters and "inf" / "nan".
        Assert.AreEqual(0.5, RocksDb.Managed.Options.OptionStrings.ParseDouble("x", "0.5abc"));
        Assert.AreEqual(10.0, RocksDb.Managed.Options.OptionStrings.ParseDouble("x", " 10 "));
        Assert.AreEqual(-2.5e3, RocksDb.Managed.Options.OptionStrings.ParseDouble("x", "-2.5e3x"));
        Assert.AreEqual(1.0, RocksDb.Managed.Options.OptionStrings.ParseDouble("x", "1e"));
        Assert.AreEqual(0.5, RocksDb.Managed.Options.OptionStrings.ParseDouble("x", ".5"));
        Assert.AreEqual(double.PositiveInfinity, RocksDb.Managed.Options.OptionStrings.ParseDouble("x", "inf"));
        Assert.AreEqual(double.NegativeInfinity, RocksDb.Managed.Options.OptionStrings.ParseDouble("x", "-Infinity"));
        Assert.IsTrue(double.IsNaN(RocksDb.Managed.Options.OptionStrings.ParseDouble("x", "nan")));
        foreach (string bad in new[] { "", "abc", ".", "-", "e5", "1e999" })
        {
            var ex = Assert.ThrowsExactly<RocksDbException>(() => RocksDb.Managed.Options.OptionStrings.ParseDouble("x", bad), bad);
            Assert.AreEqual(StatusCode.InvalidArgument, ex.Code, bad);
        }
    }

    [TestMethod]
    public void SetOptionsBlockCacheCapacityAboveInt64MaxIsAccepted()
    {
        // Cache::CreateFromString with a plain capacity: a size_t, so 2^64-1 is a valid (unbounded) LRU cache. The
        // port's checked cast to long threw OverflowException out of SetOptions.
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        db.DefaultColumnFamily.SetOptions([new KeyValuePair<string, string>("table_factory.block_cache", "18446744073709551615")]);
        db.Put(B("a"), B("1"));
        db.Flush();
        CollectionAssert.AreEqual(B("1"), db.Get(B("a")));
    }
}
