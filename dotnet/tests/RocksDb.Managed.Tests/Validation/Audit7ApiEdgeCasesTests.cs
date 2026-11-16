using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Tests.Db;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #7, area "ApiEdgeCases": the public API surface under misuse and boundary inputs (lifecycle / disposal,
/// empty and boundary keys, iterator bounds, MultiGet shapes, column family drops, ingestion, checkpoints, read-only and
/// secondary opens), compared with db/db_impl/*.cc, db/db_iter.cc, db/column_family.cc,
/// db/external_sst_file_ingestion_job.cc, table/sst_file_writer.cc and utilities/checkpoint/checkpoint_impl.cc.
/// </summary>
[TestClass]
public sealed class Audit7ApiEdgeCasesTests
{
    private static RocksDatabase OpenWithFiles(MemoryFileSystem fs, int files, int keysPerFile)
    {
        RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        int k = 0;
        for (int f = 0; f < files; f++)
        {
            for (int i = 0; i < keysPerFile; i++, k++) db.Put(Key(k), Value(k, 0));
            db.Flush();
        }
        return db;
    }

    // ---------------------------------------------------------------------------------------------------------
    // Lifecycle
    // ---------------------------------------------------------------------------------------------------------

    /// <summary>
    /// ~DBImpl with live iterators (upstream asserts in debug builds; the port must stay safe): an iterator created
    /// before the DB was disposed either keeps returning the data of its view or throws ObjectDisposedException; it never
    /// returns wrong data, and disposing it afterwards does not throw.
    /// </summary>
    [TestMethod]
    public void IteratorOpenWhenDbIsDisposedStaysSafe()
    {
        MemoryFileSystem fs = NewFs();
        RocksDatabase db = OpenWithFiles(fs, 4, 50);
        for (int i = 200; i < 230; i++) db.Put(Key(i), Value(i, 0)); // memtable part
        Iterator it = db.NewIterator();
        it.SeekToFirst();
        Assert.IsTrue(it.Valid);
        db.Dispose();
        int n = 0;
        try
        {
            for (; it.Valid; it.Next())
            {
                CollectionAssert.AreEqual(Key(n), it.Key.ToArray());
                CollectionAssert.AreEqual(Value(n, 0), it.Value.ToArray());
                n++;
            }
            Assert.AreEqual(230, n);
        }
        catch (ObjectDisposedException)
        {
        }
        it.Dispose();
        it.Dispose();
    }

    /// <summary>Snapshots released after the DB was disposed (DB::ReleaseSnapshot after Close) are harmless.</summary>
    [TestMethod]
    public void SnapshotReleasedAfterDbDisposeIsHarmless()
    {
        MemoryFileSystem fs = NewFs();
        RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        db.Put(B("a"), B("1"));
        Snapshot s = db.GetSnapshot();
        db.Dispose();
        s.Dispose();
        s.Dispose();
        using RocksDatabase db2 = RocksDatabase.Open(DbPath, NewOptions(fs));
        Assert.AreEqual("1", GetString(db2, "a"));
    }

    /// <summary>Every DB entry point throws ObjectDisposedException after Dispose (upstream: use after delete is UB).</summary>
    [TestMethod]
    public void DbMethodsAfterDisposeThrowObjectDisposed()
    {
        MemoryFileSystem fs = NewFs();
        RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        ColumnFamily cf = db.CreateColumnFamily("cf", new ColumnFamilyOptions());
        db.Dispose();
        db.Dispose();
        void Expect(Action a, string what)
        {
            try
            {
                a();
                Assert.Fail(what + " did not throw");
            }
            catch (ObjectDisposedException)
            {
            }
        }
        Expect(() => db.Get(B("a")), "Get");
        Expect(() => db.Put(B("a"), B("1")), "Put");
        Expect(() => cf.Put(B("a"), B("1")), "cf.Put");
        Expect(() => cf.Get(B("a")), "cf.Get");
        Expect(() => db.Delete(B("a")), "Delete");
        Expect(() => db.DeleteRange(B("a"), B("b")), "DeleteRange");
        Expect(() => db.NewIterator(), "NewIterator");
        Expect(() => db.GetSnapshot(), "GetSnapshot");
        Expect(() => db.GetProperty("rocksdb.stats"), "GetProperty");
        Expect(() => db.MultiGet([B("a")]), "MultiGet");
        Expect(() => db.Flush(), "Flush");
        Expect(() => db.CompactRange(), "CompactRange");
        Expect(() => db.CreateColumnFamily("x", new ColumnFamilyOptions()), "CreateColumnFamily");
        Expect(() => db.DropColumnFamily(cf), "DropColumnFamily");
        Expect(() => db.CreateCheckpoint("/ckpt"), "CreateCheckpoint");
        Expect(() => db.GetPooled(B("a")), "GetPooled");
        using WriteBatch wb = db.CreateWriteBatch();
        wb.Put(B("a"), B("1"));
        Expect(() => db.Write(wb), "Write");
    }

    /// <summary>A pooled WriteBatch disposed twice must not empty or re-pool a batch that is in the pool.</summary>
    [TestMethod]
    public void WriteBatchDoubleDisposeIsHarmless()
    {
        for (int round = 0; round < 4; round++)
        {
            WriteBatch b = WriteBatch.Rent();
            b.Put(B("k"), B("v"));
            b.Dispose();
            b.Dispose();
            // Whatever the pool hands out next must be a valid empty batch.
            WriteBatch c = WriteBatch.Rent();
            WriteBatch d = WriteBatch.Rent();
            Assert.AreNotSame(c, d);
            Assert.AreEqual(0, c.Count);
            Assert.AreEqual(0, d.Count);
            c.Put(B("x"), B("y"));
            Assert.AreEqual(1, c.Count);
            Assert.AreEqual(0, d.Count);
            c.Dispose();
            d.Dispose();
        }
    }

    /// <summary>A non-pooled WriteBatch used after Dispose throws ObjectDisposedException rather than misbehaving.</summary>
    [TestMethod]
    public void WriteBatchUseAfterDisposeThrows()
    {
        var b = new WriteBatch();
        b.Put(B("k"), B("v"));
        b.Dispose();
        b.Dispose();
        Assert.ThrowsExactly<ObjectDisposedException>(() => b.Put(B("k"), B("v")));
        WriteBatch r = WriteBatch.Rent();
        r.Dispose();
        Assert.ThrowsExactly<ObjectDisposedException>(() => r.Put(B("k"), B("v")));
    }

    /// <summary>MultiGetBatch after Dispose: ObjectDisposedException, double Dispose harmless.</summary>
    [TestMethod]
    public void MultiGetBatchAfterDispose()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        db.Put(B("a"), B("1"));
        var batch = new MultiGetBatch();
        batch.Add(B("a"));
        db.MultiGet(batch);
        Assert.AreEqual(MultiGetStatus.Found, batch.GetStatus(0));
        batch.Dispose();
        batch.Dispose();
        Assert.ThrowsExactly<ObjectDisposedException>(() => batch.Add(B("a")));
        Assert.ThrowsExactly<ObjectDisposedException>(() => db.MultiGet(batch));
        Assert.ThrowsExactly<ObjectDisposedException>(() => batch.GetStatus(0));
        batch.Clear();
    }

    /// <summary>PooledValue: double Dispose harmless (without an intervening rent).</summary>
    [TestMethod]
    public void PooledValueDoubleDispose()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        db.Put(B("a"), B("1"));
        db.Put(B("b"), B("2"));
        PooledValue? v = db.GetPooled(B("a"));
        Assert.IsNotNull(v);
        v.Dispose();
        v.Dispose();
        using PooledValue? w = db.GetPooled(B("b"));
        using PooledValue? x = db.GetPooled(B("a"));
        Assert.AreEqual("2", S(w!.ToArray()));
        Assert.AreEqual("1", S(x!.ToArray()));
    }

    /// <summary>A snapshot of another DB, or a released one, is rejected (upstream: undefined behaviour).</summary>
    [TestMethod]
    public void SnapshotOfAnotherDbIsRejected()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        fs.CreateDirectoryIfMissing("/other");
        using RocksDatabase other = RocksDatabase.Open("/other", NewOptions(fs));
        using Snapshot s = other.GetSnapshot();
        var ro = new ReadOptions { Snapshot = s };
        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => db.Get(ro, B("a")));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        e = Assert.ThrowsExactly<RocksDbException>(() => db.NewIterator(ro));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        e = Assert.ThrowsExactly<RocksDbException>(() => db.MultiGet(ro, [B("a")]));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        using var batch = new MultiGetBatch();
        batch.Add(B("a"));
        e = Assert.ThrowsExactly<RocksDbException>(() => db.MultiGet(ro, batch));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        e = Assert.ThrowsExactly<RocksDbException>(() => db.GetPooled(ro, B("a")));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        Snapshot released = db.GetSnapshot();
        released.Dispose();
        e = Assert.ThrowsExactly<RocksDbException>(() => db.Get(new ReadOptions { Snapshot = released }, B("a")));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
    }

    // ---------------------------------------------------------------------------------------------------------
    // Keys and values at the boundaries
    // ---------------------------------------------------------------------------------------------------------

    /// <summary>Empty key and empty value round trip through memtable, SST, Get, MultiGet and the iterator.</summary>
    [TestMethod]
    public void EmptyKeyAndEmptyValue()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        db.Put([], B("empty-key"));
        db.Put(B("e"), []);
        for (int pass = 0; pass < 2; pass++)
        {
            Assert.AreEqual("empty-key", S(db.Get([])));
            byte[]? v = db.Get(B("e"));
            Assert.IsNotNull(v);
            Assert.AreEqual(0, v.Length);
            byte[]?[] mg = db.MultiGet([[], B("e"), B("missing")]);
            Assert.AreEqual("empty-key", S(mg[0]));
            Assert.IsNotNull(mg[1]);
            Assert.AreEqual(0, mg[1]!.Length);
            Assert.IsNull(mg[2]);
            using Iterator it = db.NewIterator();
            it.SeekToFirst();
            Assert.IsTrue(it.Valid);
            Assert.AreEqual(0, it.Key.Length);
            it.Seek([]);
            Assert.AreEqual(0, it.Key.Length);
            it.SeekForPrev([]);
            Assert.IsTrue(it.Valid);
            Assert.AreEqual(0, it.Key.Length);
            it.Prev();
            Assert.IsFalse(it.Valid);
            it.SeekToLast();
            Assert.AreEqual("e", S(it.Key.ToArray()));
            Assert.AreEqual(0, it.Value.Length);
            db.Flush();
        }
        db.Delete([]);
        Assert.IsNull(db.Get([]));
    }

    /// <summary>Keys made of 0xFF bytes next to an upper bound (DBIter::Next / iterate_upper_bound check).</summary>
    [TestMethod]
    public void KeysOfFfBytesNearUpperBound()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        byte[][] keys = [[0xFE], [0xFE, 0xFF], [0xFF], [0xFF, 0x00], [0xFF, 0xFF], [0xFF, 0xFF, 0xFF]];
        foreach (byte[] k in keys) db.Put(k, k);
        for (int pass = 0; pass < 2; pass++)
        {
            using (Iterator it = db.NewIterator(new ReadOptions { IterateUpperBound = new byte[] { 0xFF, 0xFF } }))
            {
                int n = 0;
                for (it.SeekToFirst(); it.Valid; it.Next()) n++;
                Assert.AreEqual(4, n);
                it.SeekToLast();
                CollectionAssert.AreEqual(new byte[] { 0xFF, 0x00 }, it.Key.ToArray());
                it.SeekForPrev(new byte[] { 0xFF, 0xFF, 0xFF });
                CollectionAssert.AreEqual(new byte[] { 0xFF, 0x00 }, it.Key.ToArray());
                it.Seek(new byte[] { 0xFF, 0xFF });
                Assert.IsFalse(it.Valid);
            }
            using (Iterator it = db.NewIterator(new ReadOptions { IterateLowerBound = new byte[] { 0xFF }, IterateUpperBound = new byte[] { 0xFF, 0xFF, 0xFF } }))
            {
                int n = 0;
                for (it.SeekToLast(); it.Valid; it.Prev()) n++;
                Assert.AreEqual(3, n);
                it.Seek(new byte[] { 0x00 });
                CollectionAssert.AreEqual(new byte[] { 0xFF }, it.Key.ToArray());
                it.SeekForPrev(new byte[] { 0xFE, 0xFF });
                Assert.IsFalse(it.Valid);
            }
            db.Flush();
        }
    }

    /// <summary>
    /// iterate_lower_bound &gt;= iterate_upper_bound (DBIter: every seek finds nothing): no entry, no exception, in both
    /// directions.
    /// </summary>
    [TestMethod]
    public void LowerBoundAboveUpperBoundYieldsNothing()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        for (int i = 0; i < 20; i++) db.Put(Key(i), Value(i, 0));
        for (int pass = 0; pass < 2; pass++)
        {
            foreach ((byte[] lo, byte[] hi) in new[] { (Key(10), Key(5)), (Key(7), Key(7)) })
            {
                using Iterator it = db.NewIterator(new ReadOptions { IterateLowerBound = lo, IterateUpperBound = hi });
                it.SeekToFirst();
                Assert.IsFalse(it.Valid);
                it.SeekToLast();
                Assert.IsFalse(it.Valid);
                it.Seek(Key(8));
                Assert.IsFalse(it.Valid);
                it.SeekForPrev(Key(8));
                Assert.IsFalse(it.Valid);
                it.Seek(Key(0));
                Assert.IsFalse(it.Valid);
                it.SeekForPrev(Key(19));
                Assert.IsFalse(it.Valid);
            }
            db.Flush();
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // MultiGet shapes
    // ---------------------------------------------------------------------------------------------------------

    /// <summary>DBImpl::MultiGet with an empty key list and with duplicate keys (each duplicate gets the value).</summary>
    [TestMethod]
    public void MultiGetEmptyAndDuplicateKeys()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { MergeOperator = new StringAppendMergeOperator() });
        db.Put(B("a"), B("1"));
        db.Merge(B("m"), B("x"));
        db.Flush();
        db.Merge(B("m"), B("y"));
        Assert.AreEqual(0, db.MultiGet(Array.Empty<byte[]>()).Length);
        using (var empty = new MultiGetBatch())
        {
            db.MultiGet(empty);
            Assert.AreEqual(0, empty.FoundCount);
        }
        byte[]?[] r = db.MultiGet([B("a"), B("m"), B("a"), B("zz"), B("m"), B("a")]);
        Assert.AreEqual("1", S(r[0]));
        Assert.AreEqual("x,y", S(r[1]));
        Assert.AreEqual("1", S(r[2]));
        Assert.IsNull(r[3]);
        Assert.AreEqual("x,y", S(r[4]));
        Assert.AreEqual("1", S(r[5]));
    }

    // ---------------------------------------------------------------------------------------------------------
    // Probes (batch 2)
    // ---------------------------------------------------------------------------------------------------------

    /// <summary>DropColumnFamily with a handle of another DB must not touch either DB.</summary>
    [TestMethod]
    public void DropColumnFamilyOfAnotherDbIsRejected()
    {
        MemoryFileSystem fs = NewFs();
        fs.CreateDirectoryIfMissing("/other");
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        using (RocksDatabase other = RocksDatabase.Open("/other", NewOptions(fs)))
        {
            ColumnFamily mine = db.CreateColumnFamily("cf", new ColumnFamilyOptions());
            ColumnFamily theirs = other.CreateColumnFamily("cf", new ColumnFamilyOptions());
            mine.Put(B("k"), B("mine"));
            theirs.Put(B("k"), B("theirs"));
            RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => db.DropColumnFamily(theirs));
            Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
            Assert.IsFalse(theirs.IsDropped);
            Assert.IsFalse(mine.IsDropped);
            mine.Put(B("k2"), B("x"));
            theirs.Put(B("k2"), B("y"));
        }
        DbOptions o = NewOptions(fs);
        var cfs = new[] { new ColumnFamilyDescriptor("default", new ColumnFamilyOptions()), new ColumnFamilyDescriptor("cf", new ColumnFamilyOptions()) };
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o, cfs))
        {
            Assert.AreEqual("mine", GetString(db.GetColumnFamily("cf"), "k"));
        }
        using (RocksDatabase other = RocksDatabase.Open("/other", o, cfs))
        {
            Assert.AreEqual("theirs", GetString(other.GetColumnFamily("cf"), "k"));
        }
    }

    /// <summary>IngestExternalFiles with a handle of another DB must not ingest into either DB.</summary>
    [TestMethod]
    public void IngestIntoColumnFamilyOfAnotherDbIsRejected()
    {
        MemoryFileSystem fs = NewFs();
        fs.CreateDirectoryIfMissing("/other");
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        using RocksDatabase other = RocksDatabase.Open("/other", NewOptions(fs));
        ColumnFamily theirs = other.CreateColumnFamily("cf", new ColumnFamilyOptions());
        using (var w = new SstFileWriter(new ColumnFamilyOptions(), fs))
        {
            w.Open("/ext.sst");
            w.Put(B("a"), B("1"));
            w.Finish();
        }
        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => db.IngestExternalFiles(theirs, ["/ext.sst"], new IngestExternalFileOptions()));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        Assert.IsNull(theirs.Get(B("a")));
    }

    /// <summary>Async scan cancelled mid-way, then abandoned by an early break: the iterator and DB stay usable and close.</summary>
    [TestMethod]
    public async Task ScanAsyncCancelledAndBrokenEarly()
    {
        MemoryFileSystem fs = NewFs();
        RocksDatabase db = OpenWithFiles(fs, 3, 100);
        using var cts = new CancellationTokenSource();
        Iterator it = db.NewIterator();
        int n = 0;
        await Assert.ThrowsExactlyAsync<OperationCanceledException>(async () =>
        {
            await foreach (KeyValue kv in it.ScanAsync().WithCancellation(cts.Token))
            {
                CollectionAssert.AreEqual(Key(n), kv.Key.ToArray());
                if (++n == 10) cts.Cancel();
            }
        });
        Assert.AreEqual(10, n);
        n = 0;
        await foreach (KeyValue kv in it.ScanAsync(Key(50), null))
        {
            CollectionAssert.AreEqual(Key(50 + n), kv.Key.ToArray());
            if (++n == 5) break;
        }
        n = 0;
        await foreach (KeyValue kv in it.ScanAsyncAndDispose(null, Key(20))) n++;
        Assert.AreEqual(20, n);
        Assert.ThrowsExactly<ObjectDisposedException>(() => it.SeekToFirst());
        Snapshot s = db.GetSnapshot();
        s.Dispose();
        await db.DisposeAsync();
    }

    /// <summary>DBImplReadOnly: every write path returns NotSupported("Not supported operation in read only mode.").</summary>
    [TestMethod]
    public void ReadOnlyDbRejectsWrites()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            db.CreateColumnFamily("cf", new ColumnFamilyOptions());
            db.Put(B("a"), B("1"));
        }
        var cfs = new[] { new ColumnFamilyDescriptor("default", new ColumnFamilyOptions()), new ColumnFamilyDescriptor("cf", new ColumnFamilyOptions()) };
        using RocksDatabase ro = RocksDatabase.OpenReadOnly(DbPath, NewOptions(fs), cfs, errorIfWalFileExists: false);
        Assert.AreEqual("1", GetString(ro, "a"));
        void Ns(Action a, string what)
        {
            try
            {
                a();
                Assert.Fail(what + " did not throw");
            }
            catch (RocksDbException e)
            {
                Assert.AreEqual(StatusCode.NotSupported, e.Code, what + ": " + e.Message);
            }
        }
        ColumnFamily cf = ro.GetColumnFamily("cf");
        Ns(() => ro.Put(B("a"), B("2")), "Put");
        Ns(() => ro.Delete(B("a")), "Delete");
        Ns(() => ro.DeleteRange(B("a"), B("b")), "DeleteRange");
        Ns(() => ro.Merge(B("a"), B("b")), "Merge");
        Ns(() => cf.Put(B("a"), B("2")), "cf.Put");
        Ns(() => ro.Flush(), "Flush");
        Ns(() => ro.CompactRange(), "CompactRange");
        Ns(() => ro.CreateColumnFamily("x", new ColumnFamilyOptions()), "CreateColumnFamily");
        Ns(() => ro.DropColumnFamily(cf), "DropColumnFamily");
        Ns(() => ro.IngestExternalFiles(["/nope.sst"], new IngestExternalFileOptions()), "Ingest");
        using WriteBatch wb = ro.CreateWriteBatch();
        wb.Put(B("a"), B("3"));
        Ns(() => ro.Write(wb), "Write");
        using Snapshot snap = ro.GetSnapshot();
        Assert.AreEqual("1", GetString(ro, "a", snap));
    }

    /// <summary>Iterator over many lazily opened L1 files, left open while the DB is disposed (LevelIterator opens files after close).</summary>
    [TestMethod]
    public void LevelIteratorOpeningFilesAfterDbDisposeStaysSafe()
    {
        var mem = new MemoryFileSystem();
        mem.CreateDirectoryIfMissing(DbPath);
        var fs = new FaultInjectionFileSystem(mem);
        var cfo = new ColumnFamilyOptions { TargetFileSizeBase = 4 << 10 };
        RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        for (int i = 0; i < 2000; i++) db.Put(Key(i), Value(i, 0));
        db.Flush();
        db.CompactRange();
        Iterator it = db.NewIterator(new ReadOptions { FillCache = false });
        it.SeekToFirst();
        db.Dispose();
        int n = 0;
        try
        {
            for (; it.Valid; it.Next())
            {
                CollectionAssert.AreEqual(Key(n), it.Key.ToArray());
                CollectionAssert.AreEqual(Value(n, 0), it.Value.ToArray());
                n++;
            }
            Assert.AreEqual(2000, n);
            Console.WriteLine("iterated all after close");
        }
        catch (ObjectDisposedException e)
        {
            Console.WriteLine("ODE after " + n + ": " + e.Message);
        }
        Console.WriteLine("open files before iterator dispose: " + fs.OpenFiles);
        it.Dispose();
        Console.WriteLine("open files after iterator dispose: " + fs.OpenFiles);
        Assert.AreEqual(0, fs.OpenFiles);
    }
    /// <summary>
    /// Ingestion into a column family dropped before the call: DBImpl::ReserveFileNumbersBeforeIngestion's dummy edit
    /// fails in VersionSet::LogAndApply ("if (0 == num_undropped_cfds) s = Status::ColumnFamilyDropped();",
    /// db/version_set.cc). The file is not ingested and nothing is left in the DB directory. (The 11.12 package reports
    /// InvalidArgument here, see Audit7ApiEdgeCasesCompatTests.DroppedColumnFamilyHandleMatchesNative.)
    /// </summary>
    [TestMethod]
    public void IngestIntoDroppedColumnFamilyFails()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        ColumnFamily cf = db.CreateColumnFamily("cf", new ColumnFamilyOptions());
        WriteExternalFile(fs, "/ext.sst");
        db.DropColumnFamily(cf);
        int before = CountTableFiles(fs);
        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => db.IngestExternalFiles(cf, ["/ext.sst"], new IngestExternalFileOptions()));
        Assert.AreEqual(StatusCode.ColumnFamilyDropped, e.Code);
        Assert.AreEqual(before, CountTableFiles(fs));
        Assert.IsTrue(fs.FileExists("/ext.sst"));
        Assert.IsNull(cf.Get(B("a")));
    }

    /// <summary>
    /// A column family dropped while its ingestion is being prepared (after the file numbers were reserved): DBImpl::
    /// IngestExternalFiles (db/db_impl/db_impl.cc ~8172) checks under the write thread, "if (cfd->IsDropped()) { status =
    /// Status::InvalidArgument("cannot ingest an external file into a dropped CF"); break; }", and the jobs' Cleanup removes
    /// the prepared copies. The port threw ColumnFamilyDropped. Native cannot be driven into this window through the C API
    /// (no sync points).
    /// </summary>
    [TestMethod]
    public async Task IngestIntoColumnFamilyDroppedDuringPrepareIsInvalidArgument()
    {
        var mem = new MemoryFileSystem();
        mem.CreateDirectoryIfMissing(DbPath);
        var fs = new FaultInjectionFileSystem(mem);
        await using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        ColumnFamily cf = db.CreateColumnFamily("cf", new ColumnFamilyOptions());
        WriteExternalFile(mem, "/ext.sst");
        int before = CountTableFiles(mem);
        bool dropped = false;
        fs.OnAsyncWrite = (op, path) =>
        {
            // The first asynchronous append to the copy inside the DB directory: the numbers are reserved by now.
            if (dropped || op != "AppendAsync" || !path.EndsWith(".sst", StringComparison.Ordinal)) return;
            dropped = true;
            db.DropColumnFamily(cf);
        };
        RocksDbException e = await Assert.ThrowsExactlyAsync<RocksDbException>(async () =>
            await db.IngestExternalFilesAsync(cf, ["/ext.sst"], new IngestExternalFileOptions()));
        fs.OnAsyncWrite = null;
        Assert.IsTrue(dropped, "the copy did not use an asynchronous append");
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code, e.Message);
        StringAssert.Contains(e.Message, "cannot ingest an external file into a dropped CF");
        Assert.AreEqual(before, CountTableFiles(mem));
        Assert.IsTrue(mem.FileExists("/ext.sst"));
        Assert.IsNull(cf.Get(B("a")));
    }

    private static void WriteExternalFile(IFileSystem fs, string path)
    {
        using var w = new SstFileWriter(new ColumnFamilyOptions(), fs);
        w.Open(path);
        w.Put(B("a"), B("1"));
        w.Finish();
    }

    private static int CountTableFiles(IFileSystem fs)
    {
        int n = 0;
        foreach (string c in fs.GetChildren(DbPath))
        {
            if (c.EndsWith(".sst", StringComparison.Ordinal)) n++;
        }
        return n;
    }
}
