using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>
/// WriteBatch column family overloads, <see cref="WriteOptions.SyncWrites"/>, WriteBatch::MarkWalTerminationPoint,
/// reads through a dropped column family and the TTL iterator.
/// </summary>
[TestClass]
public sealed class ApiErgonomicsTests
{
    [TestMethod]
    public void WriteBatchColumnFamilyOverloadsMatchIdOverloads()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        ColumnFamily cf = db.CreateColumnFamily("users", new ColumnFamilyOptions());
        using WriteBatch byHandle = db.CreateWriteBatch();
        using WriteBatch byId = db.CreateWriteBatch();
        byHandle.Put(cf, B("a"), B("1")).Put(cf, B("b"), B("2")).Merge(cf, B("m"), B("x")).Delete(cf, B("b")).SingleDelete(cf, B("s"))
            .DeleteRange(cf, B("x"), B("y")).Put(db.DefaultColumnFamily, B("d"), B("default"));
        byId.Put(cf.Id, B("a"), B("1")).Put(cf.Id, B("b"), B("2")).Merge(cf.Id, B("m"), B("x")).Delete(cf.Id, B("b")).SingleDelete(cf.Id, B("s"))
            .DeleteRange(cf.Id, B("x"), B("y")).Put(0, B("d"), B("default"));
        CollectionAssert.AreEqual(byId.Data.ToArray(), byHandle.Data.ToArray());
        db.Write(WriteOptions.SyncWrites, byHandle);
        Assert.AreEqual("1", GetString(cf, "a"));
        Assert.IsNull(GetString(cf, "b"));
        Assert.AreEqual("default", GetString(db, "d"));
        Assert.IsNull(GetString(db, "a"));
        Assert.ThrowsExactly<ArgumentNullException>(() => byHandle.Put((ColumnFamily)null!, B("a"), B("1")));
    }

    [TestMethod]
    public void SyncWritesIsSharedAndReadOnly()
    {
        Assert.IsTrue(WriteOptions.SyncWrites.Sync);
        Assert.IsFalse(WriteOptions.SyncWrites.DisableWal);
        Assert.IsTrue(WriteOptions.SyncWrites.IsReadOnly);
        WriteOptions shared = WriteOptions.SyncWrites;
        Assert.AreSame(shared, WriteOptions.SyncWrites);
        Assert.ThrowsExactly<InvalidOperationException>(() => WriteOptions.SyncWrites.Sync = false);
        Assert.ThrowsExactly<InvalidOperationException>(() => WriteOptions.SyncWrites.DisableWal = true);
        Assert.IsTrue(WriteOptions.SyncWrites.Sync);
        var mine = new WriteOptions { Sync = true, DisableWal = false };
        Assert.IsFalse(mine.IsReadOnly);

        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        db.Put(WriteOptions.SyncWrites, B("k"), B("v"));
        Assert.AreEqual("v", GetString(db, "k"));
    }

    [TestMethod]
    public void WalTerminationPointLimitsWhatIsLogged()
    {
        MemoryFileSystem fs = NewFs();
        ulong seq;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            using WriteBatch b = db.CreateWriteBatch();
            b.Put(B("logged"), B("1"));
            Assert.IsFalse(b.HasWalTerminationPoint);
            b.MarkWalTerminationPoint();
            Assert.IsTrue(b.HasWalTerminationPoint);
            b.Put(B("memtable-only"), B("2")).Delete(B("logged2"));
            db.Write(b);
            seq = db.GetLatestSequenceNumber();
            Assert.AreEqual(3UL, seq, "all records consume sequence numbers");
            // Both are in the memtable.
            Assert.AreEqual("1", GetString(db, "logged"));
            Assert.AreEqual("2", GetString(db, "memtable-only"));
            // The WAL record holds only the records before the termination point (WriteBatchInternal::Append(wal_only)).
            using TransactionLogIterator it = db.GetUpdatesSince(1);
            Assert.IsTrue(it.Valid);
            WalBatch wb = it.Current;
            Assert.AreEqual(1UL, wb.Sequence);
            Assert.AreEqual(1, wb.Count);
            using var logged = new WriteBatch(wb.Span);
            var kinds = new List<string>();
            foreach (WriteBatchEntry e in logged) kinds.Add(S(e.Key.ToArray())!);
            CollectionAssert.AreEqual(new[] { "logged" }, kinds);

            // Clear removes the point; the next write logs everything.
            b.Clear();
            Assert.IsFalse(b.HasWalTerminationPoint);
            b.Put(B("after"), B("3"));
            db.Write(b);
        }
        // No flush at close: the WAL is replayed and the unlogged records are gone (like upstream).
        using RocksDatabase re = RocksDatabase.Open(DbPath, NewOptions(fs));
        Assert.AreEqual("1", GetString(re, "logged"));
        Assert.IsNull(GetString(re, "memtable-only"));
        Assert.AreEqual("3", GetString(re, "after"));
    }

    [TestMethod]
    public void WalOnlyAppendRespectsTerminationPoint()
    {
        using var a = new WriteBatch();
        a.Put(B("a1"), B("1"));
        a.MarkWalTerminationPoint();
        a.Put(B("a2"), B("2"));
        using var c = new WriteBatch();
        c.Put(B("c1"), B("1"));
        using var merged = new WriteBatch();
        merged.SetContentsWalOnly(a);
        WriteBatch.Append(merged, c, walOnly: true);
        Assert.AreEqual(2, merged.Count);
        var keys = new List<string>();
        foreach (WriteBatchEntry e in merged) keys.Add(S(e.Key.ToArray())!);
        CollectionAssert.AreEqual(new[] { "a1", "c1" }, keys);
        // Memtable path (wal_only = false) keeps everything.
        using var all = new WriteBatch();
        WriteBatch.Append(all, a, walOnly: false);
        Assert.AreEqual(2, all.Count);
    }

    [TestMethod]
    public async Task GroupCommitLogsTheWalPartOfEveryBatch()
    {
        MemoryFileSystem fs = NewFs();
        await using (RocksDatabase db = await RocksDatabase.OpenAsync(DbPath, NewOptions(fs)))
        {
            var tasks = new List<Task>();
            for (int t = 0; t < 16; t++)
            {
                int id = t;
                tasks.Add(Task.Run(async () =>
                {
                    for (int i = 0; i < 50; i++)
                    {
                        using WriteBatch b = db.CreateWriteBatch();
                        b.Put(B("w" + id + "-" + i), B("1"));
                        b.MarkWalTerminationPoint();
                        b.Put(B("m" + id + "-" + i), B("2"));
                        await db.WriteAsync(b);
                    }
                }));
            }
            await Task.WhenAll(tasks);
            Assert.AreEqual("2", GetString(db, "m3-7"));
        }
        using RocksDatabase re = RocksDatabase.Open(DbPath, NewOptions(fs));
        for (int t = 0; t < 16; t++)
        {
            for (int i = 0; i < 50; i++)
            {
                Assert.AreEqual("1", GetString(re, "w" + t + "-" + i));
                Assert.IsNull(GetString(re, "m" + t + "-" + i));
            }
        }
    }

    [TestMethod]
    public void ReadsThroughDroppedColumnFamilyKeepWorking()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            ColumnFamily cf = db.CreateColumnFamily("two", new ColumnFamilyOptions());
            for (int i = 0; i < 100; i++) cf.Put(Key(i), Value(i, 0));
            cf.Flush();
            for (int i = 100; i < 150; i++) cf.Put(Key(i), Value(i, 0)); // memtable
            using Iterator live = cf.NewIterator();
            live.SeekToFirst();
            db.DropColumnFamily(cf);
            Assert.IsTrue(cf.IsDropped);
            // An iterator created before the drop can still be used (ColumnFamilyTest.ReadDroppedColumnFamily).
            int n = 0;
            for (; live.Valid; live.Next()) n++;
            Assert.AreEqual(150, n);
            // New reads and iterators through the handle still see the data.
            Assert.AreEqual(S(Value(5, 0)), GetString(cf, S(Key(5))!));
            Assert.AreEqual(S(Value(120, 0)), GetString(cf, S(Key(120))!));
            using (Iterator it = cf.NewIterator())
            {
                n = 0;
                for (it.SeekToFirst(); it.Valid; it.Next()) n++;
            }
            Assert.AreEqual(150, n);
            // Writes are rejected; other column families are unaffected.
            RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => cf.Put(B("x"), B("y")));
            Assert.IsTrue(e.Code is StatusCode.InvalidArgument or StatusCode.ColumnFamilyDropped, e.Code.ToString());
            db.Put(B("d"), B("1"));
            db.Flush();
            db.CompactRange();
            Assert.AreEqual(S(Value(7, 0)), GetString(cf, S(Key(7))!));
        }
        using RocksDatabase re = RocksDatabase.Open(DbPath, NewOptions(fs));
        Assert.AreEqual("1", GetString(re, "d"));
        // The dropped CF's files are obsolete after reopen.
        Assert.AreEqual(1, fs.GetChildren(DbPath).Count(c => c.EndsWith(".sst", StringComparison.Ordinal)));
    }

    [TestMethod]
    public void TtlIteratorStripsTimestampsAndKeepsExpiredEntries()
    {
        MemoryFileSystem fs = NewFs();
        long now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        using TtlDatabase ttl = TtlDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions(), 1, () => now);
        ttl.Put(B("a"), B("1"));
        ttl.Put(B("b"), B("22"));
        // An entry written long ago: expired, but iterators do not check the TTL (TtlIterator).
        using (WriteBatch raw = WriteBatch.Rent())
        {
            var w = new System.Buffers.ArrayBufferWriter<byte>();
            TtlValue.AppendTimestamp(B("old"), TtlValue.MinTimestamp + 10, w);
            raw.Put(B("c"), w.WrittenSpan);
            ttl.Database.Write(raw);
        }
        var seen = new List<(string, string)>();
        using (Iterator it = ttl.NewIterator())
        {
            for (it.SeekToFirst(); it.Valid; it.Next()) seen.Add((S(it.Key.ToArray())!, S(it.Value.ToArray())!));
            it.SeekForPrev(B("bz"));
            Assert.AreEqual("22", S(it.Value.ToArray()));
        }
        CollectionAssert.AreEqual(new[] { ("a", "1"), ("b", "22"), ("c", "old") }, seen);
        using (Iterator it = ttl.NewIterator(new ReadOptions()))
        {
            var kvs = new List<string>();
            foreach (KeyValueRef kv in it.Range()) kvs.Add(S(kv.Value.ToArray())!);
            CollectionAssert.AreEqual(new[] { "1", "22", "old" }, kvs);
        }
        // Raw iterator of the underlying DB still has the suffix.
        using (Iterator raw = ttl.Database.NewIterator())
        {
            raw.SeekToFirst();
            Assert.AreEqual(1 + TtlValue.TimestampLength, raw.Value.Length);
        }
        // Compaction drops the expired entry.
        ttl.Flush();
        ttl.CompactRange();
        using Iterator after = ttl.NewIterator();
        int n = 0;
        for (after.SeekToFirst(); after.Valid; after.Next()) n++;
        Assert.AreEqual(2, n);
    }
}
