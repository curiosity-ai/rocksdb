using System.Buffers;
using System.Buffers.Binary;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>
/// User-defined timestamps in memtable only (persist_user_defined_timestamps = false). Ports of
/// ColumnFamilyRetainUDTTest / AutoFlushRetainUDTTest / ManualFlushSkipRetainUDTTest (db/column_family_test.cc),
/// DBWALTestWithTimestamp.EnableDisableUDT (db/db_wal_test.cc) and the UDT toggling cases of
/// util/udt_util_test.cc (ValidateUserDefinedTimestampsOptions).
/// </summary>
[TestClass]
public sealed class DbTimestampMemtableOnlyTests
{
    private static byte[] Ts(ulong t)
    {
        byte[] b = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(b, t);
        return b;
    }

    private static ulong TsValue(ReadOnlySpan<byte> ts) => BinaryPrimitives.ReadUInt64LittleEndian(ts);

    private static DbOptions MemOnlyDbOptions(IFileSystem fs) => new() { CreateIfMissing = true, FileSystem = fs, AllowConcurrentMemtableWrite = false };

    private static ColumnFamilyOptions MemOnlyOptions() => new()
    {
        Comparator = Comparator.BytewiseWithU64Ts,
        PersistUserDefinedTimestamps = false,
        DisableAutoCompactions = true,
    };

    private static RocksDatabase Open(IFileSystem fs, ColumnFamilyOptions cfo) => RocksDatabase.Open(DbPath, MemOnlyDbOptions(fs), cfo);

    private static ReadOptions At(ulong ts) => new() { Timestamp = Ts(ts) };

    /// <summary>(value, timestamp) of <paramref name="key"/> read at <paramref name="readTs"/>; value null when not found.</summary>
    private static (string? Value, ulong Ts) GetAt(RocksDatabase db, string key, ulong readTs)
    {
        var v = new ArrayBufferWriter<byte>();
        var t = new ArrayBufferWriter<byte>();
        bool found = db.TryGet(At(readTs), B(key), v, t);
        return (found ? System.Text.Encoding.UTF8.GetString(v.WrittenSpan) : null, t.WrittenCount == 8 ? TsValue(t.WrittenSpan) : ulong.MaxValue);
    }

    private static ulong? FullHistoryTsLow(RocksDatabase db) => db.DefaultColumnFamily.GetFullHistoryTsLow() is { } low ? TsValue(low) : null;

    private static TableProperties SingleTable(RocksDatabase db)
    {
        Dictionary<string, TableProperties> all = db.GetPropertiesOfAllTables();
        Assert.AreEqual(1, all.Count);
        foreach (TableProperties p in all.Values) return p;
        throw new InvalidOperationException();
    }

    private static List<(string Key, string Value, ulong Ts)> Scan(RocksDatabase db, ulong readTs)
    {
        var r = new List<(string, string, ulong)>();
        using Iterator it = db.NewIterator(At(readTs));
        for (it.SeekToFirst(); it.Valid; it.Next())
        {
            r.Add((System.Text.Encoding.UTF8.GetString(it.Key), System.Text.Encoding.UTF8.GetString(it.Value), TsValue(it.Timestamp)));
        }
        return r;
    }

    /// <summary>Waits (bounded) until <paramref name="condition"/> holds; postponed flushes are retried every 100 ms.</summary>
    private static void WaitUntil(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(20);
        while (!condition())
        {
            if (DateTime.UtcNow > deadline) Assert.Fail("condition not reached in time");
            Thread.Yield();
            Thread.Sleep(10);
        }
    }

    private sealed class TestTsComparator : Comparator
    {
        public override string Name => "TestTs";
        public override int TimestampSize => 8;
        public override int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => 0;
    }

    [TestMethod]
    public void SanityCheck()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = Open(fs, MemOnlyOptions()))
        {
            // Only uint64_t timestamps (".u64ts" comparators) are supported.
            var e = Assert.ThrowsExactly<RocksDbException>(() =>
                db.CreateColumnFamily("pikachu", new ColumnFamilyOptions { Comparator = new TestTsComparator(), PersistUserDefinedTimestamps = false }));
            Assert.AreEqual(StatusCode.NotSupported, e.Code);
        }
        MemoryFileSystem fs2 = NewFs();
        DbOptions atomic = MemOnlyDbOptions(fs2);
        atomic.AtomicFlush = true;
        Assert.AreEqual(StatusCode.NotSupported, Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, atomic, MemOnlyOptions())).Code);
        DbOptions concurrent = MemOnlyDbOptions(fs2);
        concurrent.AllowConcurrentMemtableWrite = true;
        Assert.AreEqual(StatusCode.NotSupported, Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, concurrent, MemOnlyOptions())).Code);
    }

    [TestMethod]
    public void FlushStripsTimestampsAndReadsPadMinimumTimestamp()
    {
        MemoryFileSystem fs = NewFs();
        foreach (bool partitioned in new[] { false, true })
        {
            ColumnFamilyOptions cfo = MemOnlyOptions();
            cfo.TableOptions.BlockSize = 64;
            if (partitioned)
            {
                cfo.TableOptions.IndexType = IndexType.TwoLevelIndexSearch;
                cfo.TableOptions.PartitionFilters = true;
                cfo.TableOptions.MetadataBlockSize = 64;
                cfo.TableOptions.FilterPolicy = new RocksDb.Managed.Table.Filters.BloomFilterPolicy(10);
            }
            string path = DbPath + (partitioned ? "-p" : "");
            using (RocksDatabase db = RocksDatabase.Open(path, MemOnlyDbOptions(fs), cfo))
            {
                for (int i = 0; i < 50; i++) db.Put(B("key" + i.ToString("D3", System.Globalization.CultureInfo.InvariantCulture)), Ts((ulong)(i + 1)), B("v" + i));
                db.Delete(B("key007"), Ts(60));
                Assert.AreEqual(60UL, TsValue(db.GetNewestUserDefinedTimestamp()));
                // Before the flush the memtable keeps every timestamp.
                Assert.AreEqual(("v3", 4UL), GetAt(db, "key003", 100));
                Assert.AreEqual((null, ulong.MaxValue), GetAt(db, "key003", 3));
                db.Flush();

                // MaybeIncreaseFullHistoryTsLowToAboveCutoffUDT: newest flushed timestamp + 1.
                Assert.AreEqual(61UL, FullHistoryTsLow(db));
                Assert.AreEqual(60UL, TsValue(db.GetNewestUserDefinedTimestamp()));
                TableProperties p = SingleTable(db);
                Assert.AreEqual(0UL, p.UserDefinedTimestampsPersisted);
                Assert.IsFalse(p.UserCollectedProperties.ContainsKey(TablePropertiesNames.TimestampMin));
                // raw_key_size excludes the stripped timestamps: 50 keys (key007@8 is hidden by its deletion) of 6 bytes + 8 byte footers.
                Assert.AreEqual(50UL * (6 + 8), p.RawKeySize);
                // Reads below full_history_ts_low are rejected; the flushed keys come back with the minimum timestamp.
                Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => GetAt(db, "key003", 60)).Code);
                Assert.AreEqual(("v3", 0UL), GetAt(db, "key003", 61));
                // A deletion reports its own (padded) timestamp.
                Assert.AreEqual((null, 0UL), GetAt(db, "key007", 61));
                Assert.AreEqual((null, ulong.MaxValue), GetAt(db, "nokey", 61));
                List<(string Key, string Value, ulong Ts)> scan = Scan(db, 100);
                Assert.AreEqual(49, scan.Count);
                foreach ((string _, string _, ulong ts) in scan) Assert.AreEqual(0UL, ts);
                using (Iterator it = db.NewIterator(At(100)))
                {
                    it.Seek(B("key020"));
                    Assert.AreEqual("key020", S(it.Key.ToArray()));
                    it.SeekForPrev(B("key007"));
                    Assert.AreEqual("key006", S(it.Key.ToArray()));
                    it.SeekToLast();
                    Assert.AreEqual("key049", S(it.Key.ToArray()));
                    it.Prev();
                    Assert.AreEqual("key048", S(it.Key.ToArray()));
                }
                // Writes after the flush keep their timestamps in the memtable.
                db.Put(B("key003"), Ts(70), B("new"));
                Assert.AreEqual(("new", 70UL), GetAt(db, "key003", 70));
                Assert.AreEqual(("v3", 0UL), GetAt(db, "key003", 69));
            }
            // The MANIFEST stores the boundaries without timestamps; the WAL keeps the timestamp of the unflushed write.
            DbOptions noRecoveryFlush = MemOnlyDbOptions(fs);
            noRecoveryFlush.AvoidFlushDuringRecovery = true;
            using (RocksDatabase db = RocksDatabase.Open(path, noRecoveryFlush, cfo))
            {
                Assert.AreEqual(("new", 70UL), GetAt(db, "key003", 70));
                Assert.AreEqual(("v3", 0UL), GetAt(db, "key003", 69));
                Assert.AreEqual(("v10", 0UL), GetAt(db, "key010", 100));
                db.Flush();
                db.CompactRange();
                Assert.AreEqual(71UL, FullHistoryTsLow(db));
                TableProperties p = SingleTable(db);
                Assert.AreEqual(0UL, p.UserDefinedTimestampsPersisted);
                Assert.AreEqual(("new", 0UL), GetAt(db, "key003", 71));
                Assert.AreEqual(("v10", 0UL), GetAt(db, "key010", 100));
                Assert.AreEqual(49, Scan(db, 100).Count);
            }
            using (RocksDatabase db = RocksDatabase.Open(path, MemOnlyDbOptions(fs), cfo))
            {
                Assert.AreEqual(("new", 0UL), GetAt(db, "key003", 71));
                Assert.AreEqual(49, Scan(db, 100).Count);
                Assert.AreEqual(70UL, TsValue(db.GetNewestUserDefinedTimestamp()));
            }
        }
    }

    [TestMethod]
    public void FlushRemovesStaleEntries()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cfo = MemOnlyOptions();
        cfo.MaxWriteBufferNumber = 4;
        using RocksDatabase db = Open(fs, cfo);
        db.IncreaseFullHistoryTsLow(Ts(0));
        using (Snapshot snap = db.GetSnapshot())
        {
            for (int version = 0; version < 100; version++) db.Put(B("foo"), Ts((ulong)version), B("v" + version));
        }
        db.Put(B("bar"), Ts(1), B("b1"));
        using (Snapshot snap = db.GetSnapshot())
        {
            db.Put(B("bar"), Ts(2), B("b2"));
            db.Flush();
            // The version visible to the snapshot is kept.
            Assert.AreEqual(3UL, SingleTable(db).NumEntries);
            Assert.AreEqual("b1", S(db.Get(new ReadOptions { Timestamp = Ts(200), Snapshot = snap }, B("bar"))));
        }
        Assert.AreEqual(100UL, FullHistoryTsLow(db));
        Assert.AreEqual(("v99", 0UL), GetAt(db, "foo", 100));
        Assert.AreEqual(("b2", 0UL), GetAt(db, "bar", 100));
    }

    [TestMethod]
    public void RangeDeletionFlushRemovesStaleEntries()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cfo = MemOnlyOptions();
        cfo.MaxWriteBufferNumber = 4;
        using RocksDatabase db = Open(fs, cfo);
        db.IncreaseFullHistoryTsLow(Ts(9));
        for (int i = 10; i < 100; i++)
        {
            db.Put(B("foo" + i), Ts((ulong)i), B("val" + i));
            if (i % 2 == 1) db.DeleteRange(B("foo" + (i - 1)), B("foo" + i), Ts((ulong)i));
        }
        db.Flush();
        Assert.AreEqual(100UL, FullHistoryTsLow(db));
        int expect = 11;
        foreach ((string key, string value, ulong ts) in Scan(db, 100))
        {
            Assert.AreEqual("foo" + expect, key);
            Assert.AreEqual("val" + expect, value);
            Assert.AreEqual(0UL, ts);
            expect += 2;
        }
        Assert.AreEqual(101, expect);
        TableProperties p = SingleTable(db);
        // 45 point entries + 45 range deletions; the 45 covered point entries are garbage collected.
        Assert.AreEqual(90UL, p.NumEntries);
        Assert.AreEqual(45UL, p.NumDeletions);
        Assert.AreEqual(45UL, p.NumRangeDeletions);
        // Range tombstones read back from the table (stored without timestamps) still delete.
        for (int i = 10; i < 100; i += 2) Assert.IsNull(GetAt(db, "foo" + i, 100).Value);
    }

    [TestMethod]
    public void AutomaticFlushIsPostponedUntilTimestampsExpire()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cfo = MemOnlyOptions();
        cfo.WriteBufferSize = 64 << 10;
        cfo.MaxWriteBufferNumber = 4;
        int reschedules = 0;
        TestHooks.Set("DBImpl::AfterRetainUDTReschedule:cb", () => Interlocked.Increment(ref reschedules));
        try
        {
            using RocksDatabase db = Open(fs, cfo);
            db.IncreaseFullHistoryTsLow(Ts(1));
            byte[] value = new byte[1000];
            int n = 0;
            // Fill one memtable: the switch schedules an automatic flush, which must wait for full_history_ts_low.
            while (db.GetIntProperty(DbProperties.NumImmutableMemTable) == 0)
            {
                db.Put(B("key" + n), Ts((ulong)(n + 1)), value);
                n++;
            }
            WaitUntil(() => Volatile.Read(ref reschedules) >= 2);
            Assert.AreEqual(0, db.GetLiveFilesMetadata().Count);
            Assert.AreEqual(1UL, FullHistoryTsLow(db));
            // Every flushed timestamp expires: the next retry flushes.
            db.IncreaseFullHistoryTsLow(Ts((ulong)n + 1));
            WaitUntil(() => db.GetLiveFilesMetadata().Count == 1);
            Assert.AreEqual((ulong)n + 1, FullHistoryTsLow(db));
            Assert.AreEqual(0UL, GetAt(db, "key0", (ulong)n + 1).Ts);
        }
        finally
        {
            TestHooks.Set("DBImpl::AfterRetainUDTReschedule:cb", null);
        }
    }

    [TestMethod]
    public void AutomaticFlushProceedsWithoutFullHistoryTsLowOrUnderWriteStall()
    {
        foreach (bool setLow in new[] { false, true })
        {
            MemoryFileSystem fs = NewFs();
            ColumnFamilyOptions cfo = MemOnlyOptions();
            cfo.WriteBufferSize = 64 << 10;
            // With max_write_buffer_number = 2 (default), the switched memtable plus a non-empty one would stall writes.
            using RocksDatabase db = Open(fs, cfo);
            if (setLow) db.IncreaseFullHistoryTsLow(Ts(1));
            byte[] value = new byte[1000];
            int n = 0;
            while (db.GetLiveFilesMetadata().Count == 0 && n < 100_000)
            {
                db.Put(B("key" + n), Ts((ulong)(n + 1)), value);
                n++;
                if (n % 64 == 0) db.WaitForCompact();
            }
            WaitUntil(() => db.GetLiveFilesMetadata().Count >= 1);
            Assert.IsTrue(FullHistoryTsLow(db) > 1UL);
        }
    }

    [TestMethod]
    public void ManualFlushIsNotPostponed()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = Open(fs, MemOnlyOptions());
        db.IncreaseFullHistoryTsLow(Ts(0));
        db.Put(B("foo"), Ts(1), B("v1"));
        db.Flush();
        Assert.AreEqual(2UL, FullHistoryTsLow(db));
        Assert.AreEqual(("v1", 0UL), GetAt(db, "foo", 2));
        // Manual compaction proceeds as well.
        db.Put(B("foo"), Ts(5), B("v2"));
        db.Flush();
        db.CompactRange();
        Assert.AreEqual(6UL, FullHistoryTsLow(db));
        Assert.AreEqual(("v2", 0UL), GetAt(db, "foo", 6));
    }

    [TestMethod]
    public void GetNewestUserDefinedTimestamp()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = Open(fs, MemOnlyOptions());
        Assert.AreEqual(0, db.GetNewestUserDefinedTimestamp().Length);
        db.Put(B("a"), Ts(5), B("x"));
        db.Put(B("b"), Ts(3), B("y"));
        Assert.AreEqual(5UL, TsValue(db.GetNewestUserDefinedTimestamp()));
        db.Flush();
        // Only table files left: full_history_ts_low - 1.
        Assert.AreEqual(5UL, TsValue(db.GetNewestUserDefinedTimestamp()));
        db.Put(B("c"), Ts(9), B("z"));
        Assert.AreEqual(9UL, TsValue(db.GetNewestUserDefinedTimestamp()));
    }

    [TestMethod]
    public void BulkLoadingWithMinimumTimestamps()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cfo = MemOnlyOptions();
        using RocksDatabase db = Open(fs, cfo);
        db.IncreaseFullHistoryTsLow(Ts(0));
        db.Put(B("foo"), Ts(1), B("v1"));
        fs.CreateDirectoryIfMissing("/sst");
        using (var w = new SstFileWriter(cfo, fs))
        {
            w.Open("/sst/file1.sst");
            Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => w.Put(B("a"), Ts(1), B("x"))).Code);
            w.Put(B("foo"), Ts(0), B("v2"));
            ExternalSstFileInfo info = w.Finish();
            Assert.AreEqual("foo", S(info.SmallestKey));
        }
        // Overlap with the memtable (keys compared without timestamps): InvalidArgument.
        Assert.AreEqual(StatusCode.InvalidArgument,
            Assert.ThrowsExactly<RocksDbException>(() => db.IngestExternalFiles(["/sst/file1.sst"], new IngestExternalFileOptions())).Code);
        using (var w = new SstFileWriter(cfo, fs))
        {
            w.Open("/sst/file2.sst");
            w.Put(B("bar"), Ts(0), B("val"));
            w.DeleteRange(B("c"), B("d"), Ts(0));
            w.Finish();
        }
        db.IngestExternalFiles(["/sst/file2.sst"], new IngestExternalFileOptions());
        Assert.AreEqual(("v1", 1UL), GetAt(db, "foo", 1));
        Assert.AreEqual(("val", 0UL), GetAt(db, "bar", 0));
        Assert.AreEqual(0UL, FullHistoryTsLow(db));
        // A file written without timestamps (UDT enabled in memtable only on top of it) is ingested and read padded.
        var plain = new ColumnFamilyOptions();
        using (var w = new SstFileWriter(plain, fs))
        {
            w.Open("/sst/file3.sst");
            w.Put(B("x1"), B("p1"));
            w.Finish();
        }
        db.IngestExternalFiles(["/sst/file3.sst"], new IngestExternalFileOptions());
        Assert.AreEqual(("p1", 0UL), GetAt(db, "x1", 5));
        // A column family persisting timestamps rejects files that do not.
        MemoryFileSystem fs2 = NewFs();
        using RocksDatabase persisted = RocksDatabase.Open(DbPath, NewOptions(fs2),
            new ColumnFamilyOptions { Comparator = Comparator.BytewiseWithU64Ts });
        fs2.CreateDirectoryIfMissing("/sst");
        using (var w = new SstFileWriter(cfo, fs2))
        {
            w.Open("/sst/file2.sst");
            w.Put(B("bar"), Ts(0), B("val"));
            w.Finish();
        }
        Assert.AreEqual(StatusCode.InvalidArgument,
            Assert.ThrowsExactly<RocksDbException>(() => persisted.IngestExternalFiles(["/sst/file2.sst"], new IngestExternalFileOptions())).Code);
    }

    /// <summary>Port of DBWALTestWithTimestamp.EnableDisableUDT plus flushed data: toggling UDT in memtable only.</summary>
    [TestMethod]
    public void EnableAndDisableTimestampsOnExistingColumnFamily()
    {
        MemoryFileSystem fs = NewFs();
        var plain = new ColumnFamilyOptions { DisableAutoCompactions = true };
        using (RocksDatabase db = RocksDatabase.Open(DbPath, MemOnlyDbOptions(fs), plain))
        {
            db.Put(B("flushed"), B("f"));
            db.DeleteRange(B("r1"), B("r3"));
            db.Flush();
            db.Put(B("r2"), B("in-wal")); // written after the tombstone: visible
            db.Put(B("wal"), B("w"));
        }
        // Persisting timestamps cannot be enabled on an existing column family.
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() =>
            RocksDatabase.Open(DbPath, MemOnlyDbOptions(fs), new ColumnFamilyOptions { Comparator = Comparator.BytewiseWithU64Ts })).Code);
        // Enable in memtable only: table keys and WAL keys get the minimum timestamp.
        using (RocksDatabase db = Open(fs, MemOnlyOptions()))
        {
            // The recovery flush of the padded WAL entries moved full_history_ts_low to 0 + 1.
            Assert.AreEqual(1UL, FullHistoryTsLow(db));
            Assert.AreEqual(("f", 0UL), GetAt(db, "flushed", 1));
            Assert.AreEqual(("w", 0UL), GetAt(db, "wal", 1));
            Assert.AreEqual(("in-wal", 0UL), GetAt(db, "r2", 1));
            db.Put(B("new"), Ts(7), B("n"));
            Assert.AreEqual(("n", 7UL), GetAt(db, "new", 7));
            Assert.AreEqual(4, Scan(db, 10).Count);
        }
        // Toggling the persist flag of a column family with timestamps is rejected.
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() =>
            RocksDatabase.Open(DbPath, MemOnlyDbOptions(fs), new ColumnFamilyOptions { Comparator = Comparator.BytewiseWithU64Ts })).Code);
        DbOptions noRecoveryFlush = MemOnlyDbOptions(fs);
        noRecoveryFlush.AvoidFlushDuringRecovery = true;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, noRecoveryFlush, MemOnlyOptions()))
        {
            Assert.AreEqual(("n", 7UL), GetAt(db, "new", 7)); // replayed from the WAL with its timestamp
            db.Flush();
            db.Put(B("late"), Ts(9), B("l"));
        }
        // Disable again: the WAL entries lose their timestamps, the table files keep being read without them.
        using (RocksDatabase db = RocksDatabase.Open(DbPath, MemOnlyDbOptions(fs), plain))
        {
            Assert.AreEqual("f", GetString(db, "flushed"));
            Assert.AreEqual("w", GetString(db, "wal"));
            Assert.AreEqual("n", GetString(db, "new"));
            Assert.AreEqual("l", GetString(db, "late"));
            Assert.AreEqual("in-wal", GetString(db, "r2"));
            Assert.IsNull(GetString(db, "r1"));
            db.Put(B("after"), B("a"));
            db.Flush();
            db.CompactRange();
            Assert.AreEqual("l", GetString(db, "late"));
            Assert.AreEqual("a", GetString(db, "after"));
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, MemOnlyDbOptions(fs), plain))
        {
            Assert.AreEqual("n", GetString(db, "new"));
            Assert.AreEqual("f", GetString(db, "flushed"));
        }
    }

    /// <summary>Port of DBWALTestWithTimestamp.RecoverAndNoFlush / RecoverAndFlush (kStripUserDefinedTimestamp).</summary>
    [TestMethod]
    public void RecoverWithAndWithoutFlush()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions noFlush = MemOnlyDbOptions(fs);
        noFlush.AvoidFlushDuringRecovery = true;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, noFlush, MemOnlyOptions()))
        {
            db.Put(B("foo"), Ts(1), B("v1"));
            db.Put(B("baz"), Ts(1), B("v5"));
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, noFlush, MemOnlyOptions()))
        {
            Assert.AreEqual(0, db.GetLiveFilesMetadata().Count);
            Assert.AreEqual(("v1", 1UL), GetAt(db, "foo", 1));
            db.Put(B("foo"), Ts(2), B("v3"));
            Assert.IsNull(FullHistoryTsLow(db));
        }
        using (RocksDatabase db = Open(fs, MemOnlyOptions()))
        {
            List<LiveFileMetadata> files = db.GetLiveFilesMetadata();
            Assert.AreEqual(1, files.Count);
            // WriteLevel0TableForRecovery moves the cutoff up to the newest flushed timestamp + 1.
            Assert.AreEqual(3UL, FullHistoryTsLow(db));
            Assert.AreEqual(("v3", 0UL), GetAt(db, "foo", 3));
            Assert.AreEqual(("v5", 0UL), GetAt(db, "baz", 3));
        }
    }

    [TestMethod]
    public void PersistedTimestampsCannotBeDisabled()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { Comparator = Comparator.BytewiseWithU64Ts }))
        {
            db.Put(B("a"), Ts(1), B("x"));
        }
        var e = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions()));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        StringAssert.Contains(e.Message, "disable user-defined timestamps");
    }
}
