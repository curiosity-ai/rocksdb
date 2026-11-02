using System.Buffers;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Util;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>End-to-end DB tests on <see cref="MemoryFileSystem"/> (port of the spirit of db/db_basic_test.cc).</summary>
[TestClass]
public sealed class DbBasicTests
{
    [TestMethod]
    public void PutGetDeleteOverwrite()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        Assert.IsNull(GetString(db, "a"));
        db.Put(B("a"), B("1"));
        db.Put(B("b"), B("2"));
        Assert.AreEqual("1", GetString(db, "a"));
        db.Put(B("a"), B("3"));
        Assert.AreEqual("3", GetString(db, "a"));
        db.Delete(B("a"));
        Assert.IsNull(GetString(db, "a"));
        Assert.AreEqual("2", GetString(db, "b"));
        db.SingleDelete(B("b"));
        Assert.IsNull(GetString(db, "b"));
        Assert.AreEqual(5UL, db.GetLatestSequenceNumber());
    }

    [TestMethod]
    public void GetOverloads()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        db.Put(B("k"), B("hello"));
        var w = new ArrayBufferWriter<byte>();
        Assert.IsTrue(db.TryGet(B("k"), w));
        Assert.AreEqual("hello", S(w.WrittenSpan.ToArray()));
        Assert.IsFalse(db.TryGet(B("missing"), w));

        Span<byte> buf = stackalloc byte[3];
        Assert.IsTrue(db.TryGet(B("k"), buf, out int len));
        Assert.AreEqual(5, len);
        Assert.AreEqual("hel", S(buf.ToArray()));

        Assert.AreEqual(5, db.Get(B("k"), 0, static (v, _) => v.Length));
        Assert.AreEqual(0, db.Get(B("nope"), 0, static (v, _) => v.Length));

        using (PooledValue? pv = db.GetPooled(B("k")))
        {
            Assert.IsNotNull(pv);
            Assert.AreEqual("hello", S(pv.ToArray()));
        }
        Assert.IsNull(db.GetPooled(B("nope")));
        Assert.IsTrue(db.ContainsKey(B("k")));
        byte[]?[] multi = db.MultiGet([B("k"), B("nope")]);
        Assert.AreEqual("hello", S(multi[0]));
        Assert.IsNull(multi[1]);
    }

    [TestMethod]
    public async Task AsyncApis()
    {
        MemoryFileSystem fs = NewFs();
        await using RocksDatabase db = await RocksDatabase.OpenAsync(DbPath, NewOptions(fs));
        await db.PutAsync(B("a"), B("1"));
        Assert.AreEqual("1", S(await db.GetAsync(B("a"))));
        var w = new ArrayBufferWriter<byte>();
        Assert.IsTrue(await db.TryGetAsync(B("a"), w));
        using (PooledValue? pv = await db.GetPooledAsync(B("a"))) Assert.AreEqual("1", S(pv!.ToArray()));
        await db.DeleteAsync(B("a"));
        Assert.IsNull(await db.GetAsync(B("a")));
        await db.FlushAsync();
        await db.PutAsync(B("b"), B("2"));
        await db.FlushAsync();
        Assert.AreEqual("2", S(await db.GetAsync(B("b"))));
        Assert.IsNull(await db.GetAsync(B("a")));
    }

    [TestMethod]
    public void ReopenRecoversFromWal()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            for (int i = 0; i < 100; i++) db.Put(Key(i), Value(i, 0));
            db.Delete(Key(5));
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            for (int i = 0; i < 100; i++) CollectionAssert.AreEqual(i == 5 ? null : Value(i, 0), db.Get(Key(i)), "key " + i);
            Assert.AreEqual(101UL, db.GetLatestSequenceNumber());
            // Recovery flushed the WAL data to L0.
            Assert.AreEqual("1", db.GetProperty("rocksdb.num-files-at-level0"));
            db.Put(Key(5), Value(5, 1));
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            CollectionAssert.AreEqual(Value(5, 1), db.Get(Key(5)));
            Assert.AreEqual(102UL, db.GetLatestSequenceNumber());
        }
    }

    [TestMethod]
    public void AvoidFlushDuringRecoveryKeepsWal()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.AvoidFlushDuringRecovery = true;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o)) db.Put(B("a"), B("1"));
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o))
        {
            Assert.AreEqual("1", GetString(db, "a"));
            Assert.AreEqual("0", db.GetProperty("rocksdb.num-files-at-level0"));
            db.Put(B("b"), B("2"));
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            Assert.AreEqual("1", GetString(db, "a"));
            Assert.AreEqual("2", GetString(db, "b"));
        }
    }

    [TestMethod]
    public void CrashDropsOnlyUnsyncedWrites()
    {
        MemoryFileSystem fs = NewFs();
        RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        var sync = new WriteOptions { Sync = true };
        db.Put(sync, B("a"), B("1"));
        db.Put(sync, B("b"), B("2"));
        db.Put(B("c"), B("3")); // not synced
        fs.SimulateCrash();     // the crashed instance is abandoned
        using (RocksDatabase db2 = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            Assert.AreEqual("1", GetString(db2, "a"));
            Assert.AreEqual("2", GetString(db2, "b"));
            Assert.IsNull(GetString(db2, "c"));
            db2.Put(B("d"), B("4"));
            db2.Flush();
        }
        fs.SimulateCrash();
        using (RocksDatabase db3 = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            Assert.AreEqual("1", GetString(db3, "a"));
            Assert.AreEqual("4", GetString(db3, "d"));
        }
        GC.KeepAlive(db);
    }

    [TestMethod]
    public void FlushThenReadFromTable()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        var cfo = new ColumnFamilyOptions();
        cfo.TableOptions.FilterPolicy = "bloomfilter:10:false";
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o, cfo))
        {
            for (int i = 0; i < 1000; i++) db.Put(Key(i), Value(i, 0));
            db.Flush();
            Assert.AreEqual("1", db.GetProperty("rocksdb.num-files-at-level0"));
            Assert.AreEqual("0", db.GetProperty("rocksdb.num-entries-active-mem-table"));
            for (int i = 0; i < 1000; i += 7) CollectionAssert.AreEqual(Value(i, 0), db.Get(Key(i)));
            Assert.IsNull(db.Get(B("nope")));
            // Newer values in the memtable shadow the table; deletions too.
            db.Put(Key(1), Value(1, 1));
            db.Delete(Key(2));
            CollectionAssert.AreEqual(Value(1, 1), db.Get(Key(1)));
            Assert.IsNull(db.Get(Key(2)));
            db.Flush();
            Assert.AreEqual("2", db.GetProperty("rocksdb.num-files-at-level0"));
            CollectionAssert.AreEqual(Value(1, 1), db.Get(Key(1)));
            Assert.IsNull(db.Get(Key(2)));
            Assert.AreEqual(1000UL, ulong.Parse(db.GetProperty("rocksdb.estimate-num-keys")!) + 0); // 1000 + 2 - 2*1
            List<LiveFileMetadata> files = db.GetLiveFilesMetadata();
            Assert.AreEqual(2, files.Count);
            Assert.AreEqual("default", files[0].ColumnFamilyName);
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o, cfo))
        {
            for (int i = 3; i < 1000; i += 13) CollectionAssert.AreEqual(Value(i, 0), db.Get(Key(i)));
            CollectionAssert.AreEqual(Value(1, 1), db.Get(Key(1)));
            Assert.IsNull(db.Get(Key(2)));
        }
    }

    [TestMethod]
    public void AutomaticFlushOnFullMemtable()
    {
        MemoryFileSystem fs = NewFs();
        var cfo = new ColumnFamilyOptions { WriteBufferSize = 64 * 1024, ArenaBlockSize = 8 * 1024, MaxWriteBufferNumber = 3 };
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo))
        {
            for (int i = 0; i < 5000; i++) db.Put(Key(i), Value(i, 0));
            db.Flush();
            int files = int.Parse(db.GetProperty("rocksdb.num-files-at-level0")!);
            Assert.IsTrue(files > 2, "files=" + files);
            for (int i = 0; i < 5000; i += 3) CollectionAssert.AreEqual(Value(i, 0), db.Get(Key(i)), "key " + i);
        }
        // Obsolete WALs were deleted: only the current one remains.
        int wals = 0;
        foreach (string f in fs.GetChildren(DbPath)) if (f.EndsWith(".log", StringComparison.Ordinal)) wals++;
        Assert.AreEqual(1, wals);
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo))
        {
            for (int i = 0; i < 5000; i += 7) CollectionAssert.AreEqual(Value(i, 0), db.Get(Key(i)), "key " + i);
        }
    }

    [TestMethod]
    public void Snapshots()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        db.Put(B("a"), B("1"));
        using Snapshot s1 = db.GetSnapshot();
        db.Put(B("a"), B("2"));
        db.Put(B("b"), B("x"));
        Snapshot s2 = db.GetSnapshot();
        db.Delete(B("a"));
        Assert.AreEqual("1", GetString(db, "a", s1));
        Assert.IsNull(GetString(db, "b", s1));
        Assert.AreEqual("2", GetString(db, "a", s2));
        Assert.IsNull(GetString(db, "a"));
        db.Flush(); // flush keeps every version
        Assert.AreEqual("1", GetString(db, "a", s1));
        Assert.AreEqual("2", GetString(db, "a", s2));
        Assert.IsNull(GetString(db, "a"));
        Assert.AreEqual("2", db.GetProperty("rocksdb.num-snapshots"));
        s2.Dispose();
        Assert.AreEqual("1", db.GetProperty("rocksdb.num-snapshots"));
        Assert.ThrowsExactly<RocksDbException>(() => db.Get(new ReadOptions { Snapshot = s2 }, B("a")));
    }

    [TestMethod]
    public void MergeOperatorAcrossMemtableAndTables()
    {
        MemoryFileSystem fs = NewFs();
        var cfo = new ColumnFamilyOptions { MergeOperator = new StringAppendMergeOperator() };
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo))
        {
            db.Merge(B("m"), B("a"));
            db.Merge(B("m"), B("b"));
            Assert.AreEqual("a,b", GetString(db, "m"));
            db.Flush();
            db.Merge(B("m"), B("c"));
            Assert.AreEqual("a,b,c", GetString(db, "m"));
            db.Put(B("p"), B("base"));
            db.Flush();
            db.Merge(B("p"), B("1"));
            db.Flush();
            db.Merge(B("p"), B("2"));
            Assert.AreEqual("base,1,2", GetString(db, "p"));
            db.Delete(B("p"));
            db.Merge(B("p"), B("3"));
            Assert.AreEqual("3", GetString(db, "p"));
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo))
        {
            Assert.AreEqual("a,b,c", GetString(db, "m"));
            Assert.AreEqual("3", GetString(db, "p"));
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => db.Get(B("m")));
            Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        }
    }

    [TestMethod]
    public void DeleteRangeInMemtableAndTables()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            for (int i = 0; i < 100; i++) db.Put(Key(i), Value(i, 0));
            db.Flush();
            using Snapshot before = db.GetSnapshot();
            db.DeleteRange(Key(10), Key(20));
            Assert.IsNull(db.Get(Key(10)));
            Assert.IsNull(db.Get(Key(19)));
            CollectionAssert.AreEqual(Value(20, 0), db.Get(Key(20)));
            CollectionAssert.AreEqual(Value(9, 0), db.Get(Key(9)));
            CollectionAssert.AreEqual(Value(15, 0), db.Get(new ReadOptions { Snapshot = before }, Key(15)));
            db.Put(Key(15), Value(15, 1)); // newer than the tombstone
            db.Flush();
            Assert.IsNull(db.Get(Key(10)));
            CollectionAssert.AreEqual(Value(15, 1), db.Get(Key(15)));
            CollectionAssert.AreEqual(Value(15, 0), db.Get(new ReadOptions { Snapshot = before }, Key(15)));
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            Assert.IsNull(db.Get(Key(11)));
            CollectionAssert.AreEqual(Value(15, 1), db.Get(Key(15)));
            CollectionAssert.AreEqual(Value(25, 0), db.Get(Key(25)));
        }
    }

    [TestMethod]
    public void WriteBatchAtomicityAndValidation()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        using (WriteBatch b = db.CreateWriteBatch())
        {
            b.Put(B("x"), B("1")).Put(B("y"), B("2")).Delete(B("x"));
            db.Write(b);
        }
        Assert.IsNull(GetString(db, "x"));
        Assert.AreEqual("2", GetString(db, "y"));
        using (WriteBatch b = db.CreateWriteBatch())
        {
            b.Put(B("z"), B("1")).Put(42, B("w"), B("bad cf"));
            RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => db.Write(b));
            Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        }
        Assert.IsNull(GetString(db, "z"));
        using (WriteBatch b = db.CreateWriteBatch())
        {
            b.Put(B("z"), B("1")).Put(42, B("w"), B("ignored"));
            db.Write(new WriteOptions { IgnoreMissingColumnFamilies = true }, b);
        }
        Assert.AreEqual("1", GetString(db, "z"));
        RocksDbException e2 = Assert.ThrowsExactly<RocksDbException>(() => db.Put(new WriteOptions { Sync = true, DisableWal = true }, B("a"), B("b")));
        Assert.AreEqual(StatusCode.InvalidArgument, e2.Code);
    }

    [TestMethod]
    public void DisableWalDataIsFlushedOnClose()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs))) db.Put(new WriteOptions { DisableWal = true }, B("a"), B("1"));
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs))) Assert.AreEqual("1", GetString(db, "a"));
    }

    [TestMethod]
    public void OpenErrors()
    {
        MemoryFileSystem fs = NewFs();
        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, new DbOptions { FileSystem = fs }));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            // LOCK is held.
            e = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, NewOptions(fs)));
            Assert.AreEqual(StatusCode.IOError, e.Code);
        }
        e = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, new DbOptions { CreateIfMissing = true, ErrorIfExists = true, FileSystem = fs }));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        // The failed opens released the lock.
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs))) db.Put(B("a"), B("b"));
    }

    [TestMethod]
    public void NewDbFilesAndOptions()
    {
        MemoryFileSystem fs = NewFs();
        string dbId;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            dbId = db.DbId;
            Assert.AreEqual(36, dbId.Length);
        }
        var names = new List<string>(fs.GetChildren(DbPath));
        CollectionAssert.Contains(names, "CURRENT");
        CollectionAssert.Contains(names, "IDENTITY");
        CollectionAssert.Contains(names, "LOCK");
        Assert.AreEqual(1, names.FindAll(n => n.StartsWith("MANIFEST-", StringComparison.Ordinal)).Count, string.Join(",", names));
        Assert.IsTrue(names.Exists(n => n.StartsWith("OPTIONS-", StringComparison.Ordinal)));
        Assert.AreEqual(dbId, FileNames.ReadIdentityFile(fs, DbPath));
        string? optionsPath = OptionsFile.FindLatestOptionsFile(fs, DbPath);
        Assert.IsNotNull(optionsPath);
        OptionsFile opts = OptionsFile.Load(fs, optionsPath, preserveUnknownOptions: true);
        Assert.AreEqual("default", opts.ColumnFamilies[0].Name);
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs))) Assert.AreEqual(dbId, db.DbId);
        names = new List<string>(fs.GetChildren(DbPath));
        Assert.AreEqual(1, names.FindAll(n => n.StartsWith("MANIFEST-", StringComparison.Ordinal)).Count, string.Join(",", names));
        Assert.IsTrue(names.FindAll(n => n.StartsWith("OPTIONS-", StringComparison.Ordinal)).Count <= 2, string.Join(",", names));
    }

    [TestMethod]
    public void ManifestRollsOverWhenTooLarge()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.MaxManifestFileSize = 1; // every LogAndApply writes a new MANIFEST
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o))
        {
            for (int r = 0; r < 4; r++)
            {
                db.Put(Key(r), Value(r, r));
                db.Flush();
            }
        }
        var names = new List<string>(fs.GetChildren(DbPath));
        Assert.AreEqual(1, names.FindAll(n => n.StartsWith("MANIFEST-", StringComparison.Ordinal)).Count, string.Join(",", names));
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            for (int r = 0; r < 4; r++) CollectionAssert.AreEqual(Value(r, r), db.Get(Key(r)));
        }
    }

    [TestMethod]
    public void CorruptedWalTailPointInTime()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            db.Put(B("a"), B("1"));
            db.Put(B("b"), B("2"));
        }
        // Truncate the newest WAL in the middle of the last record.
        string? wal = null;
        foreach (string f in fs.GetChildren(DbPath)) if (f.EndsWith(".log", StringComparison.Ordinal)) wal = f;
        Assert.IsNotNull(wal);
        byte[] data = fs.ReadAllBytes(DbPath + "/" + wal);
        using (IWritableFile w = fs.CreateWritable(DbPath + "/" + wal, truncate: true))
        {
            w.Append(data.AsSpan(0, data.Length - 3));
            w.Sync();
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            Assert.AreEqual("1", GetString(db, "a"));
            Assert.IsNull(GetString(db, "b"));
        }
    }

    [TestMethod]
    public void WalReplayHonorsColumnFamilyLogNumbers()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.CreateMissingColumnFamilies = true;
        ColumnFamilyDescriptor[] cfs = [new("default", new ColumnFamilyOptions()), new("one", new ColumnFamilyOptions())];
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o, cfs))
        {
            ColumnFamily one = db.GetColumnFamily("one");
            db.Put(B("a"), B("d1"));
            one.Put(B("a"), B("o1"));
            one.Flush(); // "one" flushed; default still only in the WAL
            db.Put(B("b"), B("d2"));
            one.Put(B("b"), B("o2"));
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o, cfs))
        {
            ColumnFamily one = db.GetColumnFamily("one");
            Assert.AreEqual("d1", GetString(db, "a"));
            Assert.AreEqual("d2", GetString(db, "b"));
            Assert.AreEqual("o1", GetString(one, "a"));
            Assert.AreEqual("o2", GetString(one, "b"));
        }
    }

    [TestMethod]
    public void SteadyStatePutGetDoNotAllocate()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        byte[] key = Key(1), value = Value(1, 0);
        byte[] buffer = new byte[256];
        for (int i = 0; i < 2000; i++)
        {
            key[^1] = (byte)('0' + i % 10);
            db.Put(key, value);
            db.TryGet(key, buffer, out _);
        }
        db.Flush();
        byte[] missing = B("missing");
        for (int i = 0; i < 100; i++) db.TryGet(missing, buffer, out _); // opens the table reader
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 2000; i++)
        {
            key[^1] = (byte)('0' + i % 10);
            Assert.IsTrue(db.TryGet(key, buffer, out int len));
            Assert.AreEqual(value.Length, len);
            db.TryGet(missing, buffer, out _);
        }
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        // TODO(block cache): BlockBasedTableReader.Get currently allocates ~32 bytes per data block read without a
        // block cache (Table/, perf stream); tighten to 0 once the block cache is wired. The DB layer adds nothing.
        Assert.IsTrue(allocated <= 4000 * 40, "bytes allocated by 4000 TryGet (table): " + allocated);
        // MemoryFileSystem grows its in-memory files by reallocation, so the WAL is disabled here; with
        // LocalFileSystem the WAL path is allocation-free too (see the dbbench "alloc B/op" column).
        var noWal = new WriteOptions { DisableWal = true };
        db.Put(noWal, key, value); // first insert into the new memtable allocates its arena block
        before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 2000; i++)
        {
            key[^1] = (byte)('0' + i % 10);
            db.Put(noWal, key, value);
        }
        allocated = GC.GetAllocatedBytesForCurrentThread() - before;
#if !DEBUG
        Assert.AreEqual(0L, allocated, "bytes allocated by 2000 Put");
#else
        // Debug builds compile async state machines as classes, so the shared sync/async write path allocates there.
        _ = allocated;
#endif
        before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 2000; i++)
        {
            key[^1] = (byte)('0' + i % 10);
            Assert.IsTrue(db.TryGet(key, buffer, out _));
        }
        allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.AreEqual(0L, allocated, "bytes allocated by 2000 TryGet (memtable)");
    }

    private static List<string> Scan(Iterator it)
    {
        var list = new List<string>();
        for (it.SeekToFirst(); it.Valid; it.Next()) list.Add(S(it.Key.ToArray()) + "=" + S(it.Value.ToArray()));
        return list;
    }

    [TestMethod]
    public async Task IteratorsOverMemtablesAndTables()
    {
        MemoryFileSystem fs = NewFs();
        var cfo = new ColumnFamilyOptions { MergeOperator = new StringAppendMergeOperator() };
        await using RocksDatabase db = await RocksDatabase.OpenAsync(DbPath, NewOptions(fs), cfo);
        db.Put(B("a"), B("1"));
        db.Put(B("b"), B("2"));
        db.Put(B("c"), B("3"));
        db.Merge(B("m"), B("x"));
        db.Flush();
        db.Delete(B("b"));
        db.Merge(B("m"), B("y"));
        db.Put(B("d"), B("4"));
        using Snapshot snap = db.GetSnapshot();
        db.DeleteRange(B("c"), B("d"));
        db.Put(B("e"), B("5"));
        using (Iterator it = db.NewIterator())
        {
            CollectionAssert.AreEqual(new[] { "a=1", "d=4", "e=5", "m=x,y" }, Scan(it));
            it.Seek(B("b"));
            Assert.IsTrue(it.Valid);
            Assert.AreEqual("d", S(it.Key.ToArray()));
            it.Prev();
            Assert.AreEqual("a", S(it.Key.ToArray()));
        }
        // The iterator keeps reading its own view across later writes and flushes.
        Iterator pinned = db.NewIterator(new ReadOptions { Snapshot = snap });
        db.Put(B("f"), B("6"));
        db.Flush();
        CollectionAssert.AreEqual(new[] { "a=1", "c=3", "d=4", "m=x,y" }, Scan(pinned));
        pinned.Dispose();
        using (Iterator it = db.NewIterator(new ReadOptions { IterateLowerBound = B("b"), IterateUpperBound = B("f") }))
        {
            CollectionAssert.AreEqual(new[] { "d=4", "e=5" }, Scan(it));
        }
        using (Iterator it = await db.NewIteratorAsync(null))
        {
            var keys = new List<string>();
            await foreach (KeyValue kv in it.ScanAsync()) keys.Add(S(kv.Key.ToArray())!);
            CollectionAssert.AreEqual(new[] { "a", "d", "e", "f", "m" }, keys);
        }
    }
}
