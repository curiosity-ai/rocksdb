using System.Buffers;
using System.Buffers.Binary;
using RocksDb.Managed.Db;
using RocksDb.Managed.Db.Iterators;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;
using RocksDb.Managed.Table.Filters;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>
/// User-defined timestamps end to end. Ports of key cases of db/db_with_timestamp_basic_test.cc,
/// db/db_with_timestamp_compaction_test.cc and the timestamp cases of db/write_batch_test.cc.
/// </summary>
[TestClass]
public sealed class DbTimestampTests
{
    internal static byte[] Ts(ulong t)
    {
        byte[] b = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(b, t);
        return b;
    }

    internal static ulong TsValue(ReadOnlySpan<byte> ts) => BinaryPrimitives.ReadUInt64LittleEndian(ts);

    private static ColumnFamilyOptions UdtOptions(bool bloom = false, bool partitionedFilters = false)
    {
        var cfo = new ColumnFamilyOptions { Comparator = Comparator.BytewiseWithU64Ts, DisableAutoCompactions = true };
        if (bloom) cfo.TableOptions.FilterPolicy = new BloomFilterPolicy(10);
        if (partitionedFilters)
        {
            cfo.TableOptions.IndexType = IndexType.TwoLevelIndexSearch;
            cfo.TableOptions.PartitionFilters = true;
            cfo.TableOptions.MetadataBlockSize = 64;
        }
        return cfo;
    }

    private static RocksDatabase OpenUdt(IFileSystem fs, ColumnFamilyOptions cfo) => RocksDatabase.Open(DbPath, NewOptions(fs), cfo);

    private static ReadOptions At(ulong ts) => new() { Timestamp = Ts(ts) };

    /// <summary>Value of <paramref name="key"/> read at <paramref name="readTs"/> plus the timestamp found, or null.</summary>
    private static (string? Value, ulong Ts) GetAt(ColumnFamily cf, string key, ulong readTs)
    {
        var v = new ArrayBufferWriter<byte>();
        var t = new ArrayBufferWriter<byte>();
        bool found = cf.TryGet(At(readTs), B(key), v, t);
        ulong ts = t.WrittenCount == 8 ? TsValue(t.WrittenSpan) : ulong.MaxValue;
        return (found ? System.Text.Encoding.UTF8.GetString(v.WrittenSpan) : null, ts);
    }

    private static string? ValueAt(RocksDatabase db, string key, ulong readTs) => S(db.Get(At(readTs), B(key)));

    /// <summary>Writes versions of 3 keys: k1@10=a,k1@20=b,k1@30 deleted; k2@15=x; k3@25=y,k3@35=z.</summary>
    private static void WriteHistory(RocksDatabase db)
    {
        db.Put(B("k1"), Ts(10), B("a"));
        db.Put(B("k1"), Ts(20), B("b"));
        db.Delete(B("k1"), Ts(30));
        db.Put(B("k2"), Ts(15), B("x"));
        db.Put(B("k3"), Ts(25), B("y"));
        db.Put(B("k3"), Ts(35), B("z"));
    }

    private static void AssertHistory(RocksDatabase db)
    {
        Assert.IsNull(ValueAt(db, "k1", 5));
        Assert.AreEqual("a", ValueAt(db, "k1", 10));
        Assert.AreEqual("a", ValueAt(db, "k1", 19));
        Assert.AreEqual("b", ValueAt(db, "k1", 20));
        Assert.AreEqual("b", ValueAt(db, "k1", 29));
        Assert.IsNull(ValueAt(db, "k1", 30));
        Assert.IsNull(ValueAt(db, "k1", ulong.MaxValue));
        Assert.IsNull(ValueAt(db, "k2", 14));
        Assert.AreEqual("x", ValueAt(db, "k2", 15));
        Assert.AreEqual("x", ValueAt(db, "k2", 100));
        Assert.AreEqual("y", ValueAt(db, "k3", 30));
        Assert.AreEqual("z", ValueAt(db, "k3", 35));
        Assert.IsNull(ValueAt(db, "k4", 100));

        ColumnFamily cf = db.DefaultColumnFamily;
        Assert.AreEqual(("b", 20UL), GetAt(cf, "k1", 25));
        Assert.AreEqual(("a", 10UL), GetAt(cf, "k1", 15));
        // The timestamp of the deletion hiding the key is reported (GetContext::SaveValue).
        Assert.AreEqual(((string?)null, 30UL), GetAt(cf, "k1", 40));
        Assert.AreEqual(("z", 35UL), GetAt(cf, "k3", 40));

        // Scans at several timestamps.
        CollectionAssert.AreEqual(new[] { "k1=a@10", "k2=x@15" }, Scan(db, At(16)));
        CollectionAssert.AreEqual(new[] { "k1=b@20", "k2=x@15", "k3=y@25" }, Scan(db, At(25)));
        CollectionAssert.AreEqual(new[] { "k2=x@15", "k3=z@35" }, Scan(db, At(100)));
        CollectionAssert.AreEqual(new[] { "k3=z@35", "k2=x@15" }, ScanReverse(db, At(100)));
        CollectionAssert.AreEqual(new[] { "k3=y@25", "k2=x@15", "k1=b@20" }, ScanReverse(db, At(25)));
    }

    internal static List<string> Scan(RocksDatabase db, ReadOptions ro)
    {
        var list = new List<string>();
        using Iterator it = db.NewIterator(ro);
        for (it.SeekToFirst(); it.Valid; it.Next()) list.Add(Entry(it));
        return list;
    }

    internal static List<string> ScanReverse(RocksDatabase db, ReadOptions ro)
    {
        var list = new List<string>();
        using Iterator it = db.NewIterator(ro);
        for (it.SeekToLast(); it.Valid; it.Prev()) list.Add(Entry(it));
        return list;
    }

    private static string Entry(Iterator it)
    {
        string s = S(it.Key.ToArray()) + "=" + S(it.Value.ToArray()) + "@" + TsValue(it.Timestamp);
        if (!it.InternalKey.IsEmpty)
        {
            DbFormat.UnpackSequenceAndType(DbFormat.ExtractFooter(it.InternalKey), out _, out EntryType t);
            if (t is EntryType.Deletion or EntryType.DeletionWithTimestamp or EntryType.SingleDeletion) s += "(del)";
        }
        return s;
    }

    [TestMethod]
    [DataRow(0)] // memtable
    [DataRow(1)] // after flush
    [DataRow(2)] // after compaction
    [DataRow(3)] // after reopen (WAL recovery)
    public void GetAndIterateAtTimestamps(int stage)
    {
        MemoryFileSystem fs = NewFs();
        RocksDatabase db = OpenUdt(fs, UdtOptions(bloom: true));
        try
        {
            WriteHistory(db);
            if (stage == 1) db.Flush();
            if (stage == 2) db.CompactRange();
            if (stage == 3)
            {
                db.Dispose();
                db = OpenUdt(fs, UdtOptions(bloom: true));
            }
            AssertHistory(db);
        }
        finally
        {
            db.Dispose();
        }
    }

    [TestMethod]
    public void VersionsSpreadOverMemtableAndFiles()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = OpenUdt(fs, UdtOptions(bloom: true, partitionedFilters: true));
        db.Put(B("k1"), Ts(10), B("a"));
        db.Put(B("k2"), Ts(15), B("x"));
        db.Flush();
        db.Put(B("k1"), Ts(20), B("b"));
        db.Flush();
        db.Delete(B("k1"), Ts(30));
        db.Put(B("k3"), Ts(25), B("y"));
        db.Put(B("k3"), Ts(35), B("z"));
        AssertHistory(db);
        db.CompactRange();
        AssertHistory(db);
    }

    [TestMethod]
    public void IterStartTsReturnsAllVersionsIncludingDeletions()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = OpenUdt(fs, UdtOptions());
        WriteHistory(db);
        var ro = new ReadOptions { Timestamp = Ts(40), IterStartTs = Ts(12) };
        string[] expected = ["k1=@30(del)", "k1=b@20", "k2=x@15", "k3=z@35", "k3=y@25"];
        CollectionAssert.AreEqual(expected, Scan(db, ro));
        db.Flush();
        CollectionAssert.AreEqual(expected, Scan(db, ro));
        // Bounded by the read timestamp too.
        CollectionAssert.AreEqual(new[] { "k1=b@20", "k1=a@10", "k2=x@15", "k3=y@25" },
            Scan(db, new ReadOptions { Timestamp = Ts(26), IterStartTs = Ts(0) }));
    }

    [TestMethod]
    public void SeekAndBoundsIgnoreTimestamps()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = OpenUdt(fs, UdtOptions());
        for (int i = 0; i < 20; i++)
        {
            for (ulong t = 1; t <= 5; t++) db.Put(Key(i), Ts(t * 10), B("v" + i + "-" + t));
        }
        db.Flush();
        var ro = new ReadOptions { Timestamp = Ts(35), IterateLowerBound = Key(5), IterateUpperBound = Key(8) };
        using Iterator it = db.NewIterator(ro);
        var keys = new List<string>();
        for (it.SeekToFirst(); it.Valid; it.Next())
        {
            keys.Add(S(it.Key.ToArray())!);
            Assert.AreEqual(30UL, TsValue(it.Timestamp));
            Assert.AreEqual("v" + int.Parse(S(it.Key.ToArray())![3..], System.Globalization.CultureInfo.InvariantCulture) + "-3", S(it.Value.ToArray()));
        }
        CollectionAssert.AreEqual(new[] { "key000005", "key000006", "key000007" }, keys);
        it.Seek(Key(6));
        Assert.IsTrue(it.Valid);
        CollectionAssert.AreEqual(Key(6), it.Key.ToArray());
        it.SeekForPrev(Key(6));
        Assert.IsTrue(it.Valid);
        CollectionAssert.AreEqual(Key(6), it.Key.ToArray());
        Assert.AreEqual(30UL, TsValue(it.Timestamp));
        it.Prev();
        CollectionAssert.AreEqual(Key(5), it.Key.ToArray());
        it.Next();
        it.Next();
        CollectionAssert.AreEqual(Key(7), it.Key.ToArray());
        it.SeekToLast();
        CollectionAssert.AreEqual(Key(7), it.Key.ToArray());
        // Many versions per key exercise the reseek paths (max_sequential_skip_in_iterations = 8).
        for (int i = 0; i < 30; i++) db.Put(Key(6), Ts(100 + (ulong)i), B("hot" + i));
        using Iterator it2 = db.NewIterator(new ReadOptions { Timestamp = Ts(110) });
        it2.Seek(Key(6));
        Assert.AreEqual("hot10", S(it2.Value.ToArray()));
        it2.Next();
        CollectionAssert.AreEqual(Key(7), it2.Key.ToArray());
        it2.Prev();
        Assert.AreEqual("hot10", S(it2.Value.ToArray()));
        Assert.AreEqual(110UL, TsValue(it2.Timestamp));
    }

    [TestMethod]
    public void TimestampArgumentValidation()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = OpenUdt(fs, UdtOptions());
        ColumnFamily plain = db.CreateColumnFamily("plain", new ColumnFamilyOptions());

        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => db.Put(B("a"), B("v"))).Code);
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => db.Delete(B("a"))).Code);
        Assert.AreEqual(StatusCode.InvalidArgument,
            Assert.ThrowsExactly<RocksDbException>(() => db.PutEntity(B("a"), [new WideColumn(B("c"), B("v"))])).Code);
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => db.Put(B("a"), new byte[4], B("v"))).Code);
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => db.Get(B("a"))).Code);
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => db.Get(new ReadOptions { Timestamp = new byte[3] }, B("a"))).Code);
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => db.NewIterator()).Code);
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => plain.Put(B("a"), Ts(1), B("v"))).Code);
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => plain.Get(At(1), B("a"))).Code);
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => plain.NewIterator(At(1))).Code);
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => plain.GetFullHistoryTsLow()).Code);
        Assert.AreEqual(StatusCode.NotSupported, Assert.ThrowsExactly<RocksDbException>(() => db.GetNewestUserDefinedTimestamp()).Code);
        Assert.AreEqual(StatusCode.NotSupported,
            Assert.ThrowsExactly<RocksDbException>(() => db.CreateColumnFamily("memonly", new ColumnFamilyOptions
            {
                Comparator = Comparator.BytewiseWithU64Ts, PersistUserDefinedTimestamps = false,
            })).Code);
        plain.Put(B("a"), B("v"));
        Assert.AreEqual("v", S(plain.Get(B("a"))));
        Assert.AreEqual(8, db.DefaultColumnFamily.TimestampSize);
        Assert.AreEqual(0, plain.TimestampSize);
    }

    [TestMethod]
    public void MergeAndSingleDeleteWithTimestamps()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cfo = UdtOptions();
        cfo.MergeOperator = new StringAppendMergeOperator();
        using RocksDatabase db = OpenUdt(fs, cfo);
        db.Put(B("m"), Ts(1), B("base"));
        db.Merge(B("m"), Ts(2), B("op1"));
        db.Merge(B("m"), Ts(3), B("op2"));
        db.Put(B("s"), Ts(1), B("single"));
        db.SingleDelete(B("s"), Ts(5));
        for (int pass = 0; pass < 2; pass++)
        {
            Assert.AreEqual("base", ValueAt(db, "m", 1));
            Assert.AreEqual("base,op1", ValueAt(db, "m", 2));
            Assert.AreEqual("base,op1,op2", ValueAt(db, "m", 10));
            Assert.AreEqual("single", ValueAt(db, "s", 4));
            Assert.IsNull(ValueAt(db, "s", 5));
            CollectionAssert.AreEqual(new[] { "m=base,op1@2" }, Scan(db, new ReadOptions { Timestamp = Ts(2), IterateUpperBound = B("n") }));
            CollectionAssert.AreEqual(new[] { "m=base,op1,op2@3" }, ScanReverse(db, new ReadOptions { Timestamp = Ts(9), IterateUpperBound = B("n") }));
            db.CompactRange();
        }
    }

    [TestMethod]
    public void MultiGetWithTimestamp()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = OpenUdt(fs, UdtOptions(bloom: true));
        WriteHistory(db);
        db.Flush();
        byte[]?[] r = db.MultiGet(At(25), [B("k1"), B("k2"), B("k3"), B("k4")]);
        Assert.AreEqual("b", S(r[0]));
        Assert.AreEqual("x", S(r[1]));
        Assert.AreEqual("y", S(r[2]));
        Assert.IsNull(r[3]);
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => db.MultiGet([B("k1")])).Code);
    }

    [TestMethod]
    public async Task AsyncReadsAndWrites()
    {
        MemoryFileSystem fs = NewFs();
        await using RocksDatabase db = OpenUdt(fs, UdtOptions());
        await db.PutAsync(B("a"), Ts(5), B("five"));
        await db.PutAsync(B("a"), Ts(7), B("seven"));
        db.Flush();
        Assert.AreEqual("five", S(await db.DefaultColumnFamily.GetAsync(At(6), B("a"))));
        Assert.AreEqual("seven", S(await db.DefaultColumnFamily.GetAsync(At(7), B("a"))));
        using Iterator it = await db.DefaultColumnFamily.NewIteratorAsync(At(6));
        await it.SeekToFirstAsync();
        Assert.IsTrue(it.Valid);
        Assert.AreEqual(5UL, TsValue(it.Timestamp));
    }

    [TestMethod]
    public void FullHistoryTsLowValidationAndPersistence()
    {
        MemoryFileSystem fs = NewFs();
        RocksDatabase db = OpenUdt(fs, UdtOptions());
        try
        {
            Assert.IsNull(db.GetFullHistoryTsLow());
            WriteHistory(db);
            db.IncreaseFullHistoryTsLow(Ts(18));
            Assert.AreEqual(18UL, TsValue(db.GetFullHistoryTsLow()));
            Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => db.IncreaseFullHistoryTsLow(Ts(17))).Code);
            Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => db.IncreaseFullHistoryTsLow(new byte[4])).Code);
            // Reads below full_history_ts_low are rejected (FailIfReadCollapsedHistory).
            Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => db.Get(At(17), B("k1"))).Code);
            Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => db.NewIterator(At(17))).Code);
            Assert.AreEqual("a", ValueAt(db, "k1", 18));
            db.IncreaseFullHistoryTsLow(Ts(18)); // same value is fine
            db.Dispose();
            db = OpenUdt(fs, UdtOptions());
            Assert.AreEqual(18UL, TsValue(db.GetFullHistoryTsLow()));
            // Survives a MANIFEST rewrite (new MANIFEST snapshot on reopen) and a second reopen.
            db.Dispose();
            db = OpenUdt(fs, UdtOptions());
            Assert.AreEqual(18UL, TsValue(db.GetFullHistoryTsLow()));
            AssertHistoryAtOrAbove(db);
        }
        finally
        {
            db.Dispose();
        }
    }

    private static void AssertHistoryAtOrAbove(RocksDatabase db)
    {
        Assert.AreEqual("a", ValueAt(db, "k1", 19));
        Assert.AreEqual("b", ValueAt(db, "k1", 20));
        Assert.IsNull(ValueAt(db, "k1", 30));
        Assert.AreEqual("x", ValueAt(db, "k2", 100));
        Assert.AreEqual("z", ValueAt(db, "k3", 35));
    }

    /// <summary>
    /// Port of the spirit of DBBasicTestWithTimestamp.GCPreserveRangeTombstoneWhenNoOrSmallFullHistoryLow /
    /// TimestampCompactionTest: without full_history_ts_low compaction keeps every version; with it, versions older
    /// than the newest one below full_history_ts_low are collapsed and its seqno/timestamp zeroed at the bottommost level.
    /// </summary>
    [TestMethod]
    public void CompactionGarbageCollectsBelowFullHistoryTsLow()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = OpenUdt(fs, UdtOptions());
        for (ulong t = 1; t <= 6; t++) db.Put(B("a"), Ts(t * 10), B("a" + t));
        db.Put(B("b"), Ts(10), B("b1"));
        db.Delete(B("b"), Ts(20));
        db.Put(B("c"), Ts(50), B("c1"));
        var all = new ReadOptions { Timestamp = Ts(ulong.MaxValue), IterStartTs = Ts(0) };
        db.CompactRange();
        // No full_history_ts_low: every version is kept.
        Assert.AreEqual(9, Scan(db, all).Count);

        // full_history_ts_low = 35 via CompactRangeOptions: a@10, a@20 are hidden by a@30 (the newest below 35);
        // b's deletion (below 35) drops b entirely at the bottommost level.
        db.CompactRange(new CompactRangeOptions { FullHistoryTsLow = Ts(35), BottommostLevelCompaction = BottommostLevelCompaction.Force }, null, null);
        Assert.AreEqual(35UL, TsValue(db.GetFullHistoryTsLow()));
        List<string> versions = Scan(db, all);
        CollectionAssert.AreEqual(new[] { "a=a6@60", "a=a5@50", "a=a4@40", "a=a3@0", "c=c1@50" }, versions);
        Assert.AreEqual("a3", ValueAt(db, "a", 35));
        Assert.AreEqual("a4", ValueAt(db, "a", 45));
        Assert.IsNull(ValueAt(db, "b", 100));
        Assert.AreEqual(StatusCode.InvalidArgument,
            Assert.ThrowsExactly<RocksDbException>(() => db.CompactRange(new CompactRangeOptions { FullHistoryTsLow = Ts(40) }, B("a"), B("z"))).Code);
    }

    [TestMethod]
    public void SstFileCarriesTimestampPropertiesAndFileMetadata()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = OpenUdt(fs, UdtOptions(bloom: true)))
        {
            WriteHistory(db);
            db.Flush();
        }
        string sst = fs.GetChildren(DbPath).Single(static n => n.EndsWith(".sst", StringComparison.Ordinal));
        using var reader = BlockBasedTableReader.Open(fs.OpenRandomAccess(DbPath + "/" + sst),
            new TableReaderOptions { Comparator = Comparator.BytewiseWithU64Ts, FilterPolicy = new BloomFilterPolicy(10) });
        Assert.AreEqual(10UL, TsValue(reader.Properties.UserCollectedProperties[TablePropertiesNames.TimestampMin]));
        Assert.AreEqual(35UL, TsValue(reader.Properties.UserCollectedProperties[TablePropertiesNames.TimestampMax]));
        Assert.AreEqual(Comparator.BytewiseWithU64Ts.Name, reader.Properties.ComparatorName);
        // The filter holds user keys without timestamps.
        Assert.IsTrue(reader.InternalKeyMayMatch(InternalKeyAt("k1", 99), TableReadFlags.None));
        Assert.IsTrue(reader.InternalKeyMayMatch(InternalKeyAt("k3", 1), TableReadFlags.None));

        // MANIFEST NewFile4 records the min/max timestamps (kMinTimestamp / kMaxTimestamp custom tags).
        string manifest = fs.GetChildren(DbPath).Where(static n => n.StartsWith("MANIFEST-", StringComparison.Ordinal)).Order(StringComparer.Ordinal).Last();
        FileMetaData? f = null;
        using (ManifestLogReader r = ManifestLogReader.Open(fs, DbPath + "/" + manifest))
        {
            while (r.TryReadRecord(out ReadOnlySpan<byte> rec))
            {
                VersionEdit e = VersionEdit.DecodeFrom(rec);
                foreach (NewFileEntry n in e.NewFiles) f = n.Meta;
            }
        }
        Assert.IsNotNull(f);
        Assert.AreEqual(10UL, TsValue(f.MinTimestamp));
        Assert.AreEqual(35UL, TsValue(f.MaxTimestamp));
    }

    private static byte[] InternalKeyAt(string key, ulong ts)
    {
        byte[] uk = B(key);
        byte[] ikey = new byte[uk.Length + 8 + DbFormat.InternalKeyFooterSize];
        DbFormat.AppendInternalKey(ikey, uk, Ts(ts), DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
        return ikey;
    }

    // ---- WriteBatch (write_batch_test.cc timestamp cases) --------------------------------------------------------

    [TestMethod]
    public void WriteBatchRecordsAppendTimestampToKeys()
    {
        using var b = new WriteBatch();
        b.Put(0, B("k"), Ts(1), B("v"));
        b.Delete(2, B("k"), Ts(2));
        b.SingleDelete(0, B("k"), Ts(3));
        b.Merge(0, B("k"), Ts(4), B("m"));
        b.DeleteRange(0, B("a"), B("b"), Ts(5));
        Assert.AreEqual(5, b.Count);
        using var plain = new WriteBatch();
        plain.Put(0, [.. B("k"), .. Ts(1)], B("v"));
        plain.Delete(2, [.. B("k"), .. Ts(2)]);
        plain.SingleDelete(0, [.. B("k"), .. Ts(3)]);
        plain.Merge(0, [.. B("k"), .. Ts(4)], B("m"));
        plain.DeleteRange(0, [.. B("a"), .. Ts(5)], [.. B("b"), .. Ts(5)]);
        CollectionAssert.AreEqual(plain.Data.ToArray(), b.Data.ToArray());
    }

    [TestMethod]
    public void WriteBatchUpdateTimestamps()
    {
        using var b = new WriteBatch();
        byte[] dummy = new byte[8];
        b.Put(0, B("k1"), dummy, B("v"));
        b.Put(1, B("other"), B("v")); // CF 1 has no timestamps: untouched
        b.Delete(0, B("k2"), dummy);
        b.DeleteRange(0, B("a"), B("b"), dummy);
        b.PutLogData(B("blob"));
        b.UpdateTimestamps(Ts(42), static cf => cf == 0 ? 8 : 0);
        using var expected = new WriteBatch();
        expected.Put(0, B("k1"), Ts(42), B("v"));
        expected.Put(1, B("other"), B("v"));
        expected.Delete(0, B("k2"), Ts(42));
        expected.DeleteRange(0, B("a"), B("b"), Ts(42));
        expected.PutLogData(B("blob"));
        CollectionAssert.AreEqual(expected.Data.ToArray(), b.Data.ToArray());
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => b.UpdateTimestamps(new byte[4], static _ => 8)).Code);
    }

    [TestMethod]
    public void WriteBatchWithTimestampsAppliesToDb()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = OpenUdt(fs, UdtOptions());
        using WriteBatch b = db.CreateWriteBatch();
        b.Put(0, B("x"), Ts(3), B("three"));
        b.Delete(0, B("y"), Ts(3));
        b.Put(0, B("y"), Ts(2), B("two"));
        db.Write(b);
        Assert.AreEqual("three", ValueAt(db, "x", 3));
        Assert.AreEqual("two", ValueAt(db, "y", 2));
        Assert.IsNull(ValueAt(db, "y", 3));
    }

    [TestMethod]
    public void WalRecordsTimestampSizes()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = OpenUdt(fs, UdtOptions()))
        {
            db.Put(B("k"), Ts(1), B("v"));
        }
        string wal = fs.GetChildren(DbPath).Where(static n => n.EndsWith(".log", StringComparison.Ordinal)).Order(StringComparer.Ordinal).First();
        using var reader = new RocksDb.Managed.Wal.LogReader(fs.OpenSequential(DbPath + "/" + wal), null, true, 0);
        while (reader.TryReadRecord(out _)) { }
        Assert.AreEqual(8, reader.RecordedTimestampSizes[0]);
    }
}
