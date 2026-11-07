using System.Reflection;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Utilities.Transactions;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Iterators;

/// <summary>
/// Iterator reuse: <see cref="Iterator.Refresh()"/> / <see cref="Iterator.Refresh(Snapshot?)"/> (ports of
/// DBIteratorTest.Refresh / RefreshWithSnapshot, db/db_iterator_test.cc, and DBRangeDelTest.RefreshMemtableIter-style
/// cases), <see cref="Iterator.SetBounds"/>, the per-column-family iterator pool behind NewIterator, the cached
/// ScanAsync enumerator, and the steady-state allocation bounds of these patterns.
/// </summary>
[TestClass]
public sealed class IteratorReuseTests
{
    // An Iterator handle (the only per-NewIterator allocation once the pool is warm) plus slack for the runtime.
    private const long HandleBytes = 64;

    // A Debug build of the library compiles async state machines as classes (allocated per call even when they complete
    // synchronously), so the allocation bounds of async paths are only asserted for optimized builds (as in
    // DbMultiGetTests); the profiler's Release numbers cover them.
    private static readonly bool s_optimized =
        typeof(RocksDatabase).Assembly.GetCustomAttribute<System.Diagnostics.DebuggableAttribute>() is not { IsJITOptimizerDisabled: true };

    private static RocksDatabase Open(MemoryFileSystem fs, ColumnFamilyOptions? cfo = null)
    {
        cfo ??= new ColumnFamilyOptions();
        cfo.DisableAutoCompactions = true;
        return RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
    }

    /// <summary>Keys key000000..key(n-1) in two L0 files + an L1 run + the memtable, so every iterator kind is in the stack.</summary>
    private static void FillLayered(RocksDatabase db, int n)
    {
        for (int i = 0; i < n; i += 3) db.Put(Key(i), Value(i, 0));
        db.Flush();
        db.CompactRange();
        for (int i = 1; i < n; i += 3) db.Put(Key(i), Value(i, 0));
        db.Flush();
        for (int i = 2; i < n; i += 6) db.Put(Key(i), Value(i, 0));
        db.Flush();
        for (int i = 5; i < n; i += 6) db.Put(Key(i), Value(i, 0));
    }

    private static List<string> Scan(Iterator it)
    {
        var keys = new List<string>();
        for (it.SeekToFirst(); it.Valid; it.Next()) keys.Add(S(it.Key.ToArray())!);
        return keys;
    }

    private static List<string> Expected(int from, int to)
    {
        var keys = new List<string>();
        for (int i = from; i < to; i++) keys.Add(S(Key(i))!);
        return keys;
    }

    // ------------------------------------------------------------------ Refresh

    /// <summary>DBIteratorTest.Refresh: writes, flushes and compactions after creation become visible after Refresh.</summary>
    [TestMethod]
    public async Task RefreshSeesWritesFlushesAndCompactions()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = Open(fs);
        db.Put(B("x"), B("x"));
        using Iterator it = db.NewIterator();
        it.Seek(B("x"));
        Assert.IsTrue(it.Valid);
        Assert.AreEqual("x", S(it.Key.ToArray()));
        db.Put(B("y"), B("y"));
        // Not yet visible.
        it.Seek(B("y"));
        Assert.IsFalse(it.Valid);

        it.Refresh(); // same super version: only the sequence moves
        Assert.IsFalse(it.Valid, "unpositioned after Refresh");
        CollectionAssert.AreEqual(new[] { "x", "y" }, Scan(it));

        db.Flush(); // new super version: the stack is rebuilt
        db.Put(B("z"), B("z"));
        CollectionAssert.AreEqual(new[] { "x", "y" }, Scan(it));
        it.Refresh();
        CollectionAssert.AreEqual(new[] { "x", "y", "z" }, Scan(it));

        db.Delete(B("y"));
        db.Flush();
        db.CompactRange();
        await it.RefreshAsync();
        CollectionAssert.AreEqual(new[] { "x", "z" }, Scan(it));
        await it.SeekAsync(B("z"));
        Assert.AreEqual("z", S(it.Value.ToArray()));
    }

    /// <summary>DBIteratorTest.RefreshWithSnapshot: Refresh(snapshot) reads at the snapshot; Refresh() goes back to the latest.</summary>
    [TestMethod]
    public void RefreshWithSnapshot()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = Open(fs);
        db.Put(B("a"), B("1"));
        using Snapshot s1 = db.GetSnapshot();
        db.Put(B("b"), B("2"));
        db.Flush();
        db.Put(B("a"), B("3"));
        using Snapshot s2 = db.GetSnapshot();
        db.Delete(B("b"));

        using Iterator it = db.NewIterator(new ReadOptions { Snapshot = s2 });
        CollectionAssert.AreEqual(new[] { "a", "b" }, Scan(it));
        it.Refresh(s1);
        CollectionAssert.AreEqual(new[] { "a" }, Scan(it));
        it.SeekToFirst();
        Assert.AreEqual("1", S(it.Value.ToArray()));
        Assert.AreEqual(s1.SequenceNumber, it.Sequence);
        it.Refresh(); // latest state, although the iterator was created with a snapshot
        CollectionAssert.AreEqual(new[] { "a" }, Scan(it));
        it.SeekToFirst();
        Assert.AreEqual("3", S(it.Value.ToArray()));
        it.Refresh(s2);
        CollectionAssert.AreEqual(new[] { "a", "b" }, Scan(it));
    }

    [TestMethod]
    public void RefreshSeesRangeDeletionsAddedLater()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = Open(fs);
        for (int i = 0; i < 20; i++) db.Put(Key(i), Value(i, 0));
        db.Flush();
        for (int i = 20; i < 30; i++) db.Put(Key(i), Value(i, 0));
        using Iterator it = db.NewIterator();
        CollectionAssert.AreEqual(Expected(0, 30), Scan(it));

        db.DeleteRange(Key(5), Key(25)); // into the mutable memtable of the same super version
        CollectionAssert.AreEqual(Expected(0, 30), Scan(it));
        it.Refresh();
        var expected = Expected(0, 5);
        expected.AddRange(Expected(25, 30));
        CollectionAssert.AreEqual(expected, Scan(it));

        db.Flush(); // the tombstone moves to a file
        db.Put(Key(10), Value(10, 1));
        it.Refresh();
        expected = Expected(0, 5);
        expected.Add(S(Key(10))!);
        expected.AddRange(Expected(25, 30));
        CollectionAssert.AreEqual(expected, Scan(it));
    }

    [TestMethod]
    public void RefreshIsNotSupportedForTailingAndNonDbIterators()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = Open(fs);
        db.Put(B("a"), B("1"));
        using (Iterator tailing = db.NewIterator(new ReadOptions { Tailing = true }))
        {
            var ex = Assert.ThrowsExactly<RocksDbException>(() => tailing.Refresh());
            Assert.AreEqual(StatusCode.NotSupported, ex.Code);
        }
        db.Flush();
        string sst = null!;
        foreach (string f in fs.GetChildren(DbPath)) if (f.EndsWith(".sst", StringComparison.Ordinal)) sst = DbPath + "/" + f;
        using var reader = new SstFileReader(new ColumnFamilyOptions(), fs);
        reader.Open(sst);
        using Iterator fileIt = reader.NewIterator(new ReadOptions());
        var ex2 = Assert.ThrowsExactly<RocksDbException>(() => fileIt.Refresh());
        Assert.AreEqual(StatusCode.NotSupported, ex2.Code);
    }

    [TestMethod]
    public void RefreshIsNotSupportedForWritePreparedIterators()
    {
        MemoryFileSystem fs = NewFs();
        using TransactionDatabase t = TransactionDatabase.Open(DbPath, NewOptions(fs),
            new TransactionDbOptions { WritePolicy = TransactionWritePolicy.WritePrepared, WpCommitCacheBits = 10 });
        t.Database.Put(B("a"), B("1"));
        using Iterator it = t.Database.NewIterator();
        CollectionAssert.AreEqual(new[] { "a" }, Scan(it));
        var ex = Assert.ThrowsExactly<RocksDbException>(() => it.Refresh());
        Assert.AreEqual(StatusCode.NotSupported, ex.Code);
    }

    [TestMethod]
    public void RefreshRejectsAForeignSnapshot()
    {
        using RocksDatabase db = Open(NewFs());
        using RocksDatabase other = RocksDatabase.Open(DbPath, NewOptions(NewFs()));
        using Snapshot foreign = other.GetSnapshot();
        using Iterator it = db.NewIterator();
        var ex = Assert.ThrowsExactly<RocksDbException>(() => it.Refresh(foreign));
        Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
        // Still usable.
        it.Refresh();
        Assert.AreEqual(0, Scan(it).Count);
    }

    // ------------------------------------------------------------------ SetBounds

    [TestMethod]
    public void SetBoundsScansManyRangesWithOneIterator()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = Open(fs);
        FillLayered(db, 300);
        var ro = new ReadOptions { IterateUpperBound = Key(10) };
        using Iterator it = db.NewIterator(ro);
        CollectionAssert.AreEqual(Expected(0, 10), Scan(it));
        var rng = new Random(5);
        for (int round = 0; round < 50; round++)
        {
            int lo = rng.Next(300), hi = lo + rng.Next(40);
            it.SetBounds(Key(lo), Key(hi));
            Assert.IsFalse(it.Valid);
            List<string> expected = Expected(lo, Math.Min(hi, 300));
            CollectionAssert.AreEqual(expected, Scan(it), "range " + lo + ".." + hi);
            // Reverse from the upper bound.
            var reverse = new List<string>();
            for (it.SeekToLast(); it.Valid; it.Prev()) reverse.Add(S(it.Key.ToArray())!);
            reverse.Reverse();
            CollectionAssert.AreEqual(expected, reverse);
            // Seeks below the lower bound are clamped.
            it.Seek(Key(0));
            Assert.AreEqual(expected.Count > 0, it.Valid);
        }
        Assert.AreEqual(S(Key(10)), S(ro.IterateUpperBound!.Value.ToArray()), "the caller's options are unchanged");
        it.SetBounds(null, null);
        CollectionAssert.AreEqual(Expected(0, 300), Scan(it));
        it.SetBounds(null, Key(3));
        db.Put(Key(1), B("new"));
        it.Refresh(); // bounds survive a refresh
        CollectionAssert.AreEqual(Expected(0, 3), Scan(it));
        it.Seek(Key(1));
        Assert.AreEqual("new", S(it.Value.ToArray()));
    }

    [TestMethod]
    public void SetBoundsWithPrefixExtractor()
    {
        MemoryFileSystem fs = NewFs();
        var cfo = new ColumnFamilyOptions { PrefixExtractor = SliceTransform.CreateFixedPrefix(5) };
        cfo.TableOptions.FilterPolicy = new Managed.Table.Filters.BloomFilterPolicy(10);
        using RocksDatabase db = Open(fs, cfo);
        FillLayered(db, 300);
        using Iterator it = db.NewIterator(new ReadOptions { AutoPrefixMode = true });
        for (int lo = 0; lo < 300; lo += 37)
        {
            it.SetBounds(Key(lo), Key(lo + 15));
            CollectionAssert.AreEqual(Expected(lo, Math.Min(lo + 15, 300)), Scan(it));
            it.Seek(Key(lo + 3));
            Assert.AreEqual(S(Key(lo + 3)), S(it.Key.ToArray()));
        }
        db.Put(Key(301), B("v"));
        it.SetBounds(Key(299), null);
        it.Refresh();
        CollectionAssert.AreEqual(new[] { S(Key(299)), S(Key(301)) }, Scan(it));
    }

    // ------------------------------------------------------------------ pool

    [TestMethod]
    public void DisposedIteratorThrowsAndDoesNotSeeTheNextIteratorsState()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = Open(fs);
        FillLayered(db, 50);
        Iterator first = db.NewIterator();
        first.Seek(Key(7));
        first.Dispose();
        first.Dispose(); // idempotent
        using Iterator second = db.NewIterator(); // reuses the stack of the first one
        second.Seek(Key(20));
        Assert.ThrowsExactly<ObjectDisposedException>(() => _ = first.Valid);
        Assert.ThrowsExactly<ObjectDisposedException>(() => first.Next());
        Assert.ThrowsExactly<ObjectDisposedException>(() => first.Seek(Key(1)));
        Assert.ThrowsExactly<ObjectDisposedException>(() => first.Refresh());
        Assert.ThrowsExactly<ObjectDisposedException>(() => first.SetBounds(null, null));
        Assert.ThrowsExactly<ObjectDisposedException>(() => first.ScanAsync());
        Assert.ThrowsExactly<ObjectDisposedException>(() => _ = first.Key.Length);
        Assert.AreEqual(S(Key(20)), S(second.Key.ToArray()));
    }

    [TestMethod]
    public void PooledIteratorsAreIndependentAndSeeTheirOwnView()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = Open(fs);
        FillLayered(db, 100);
        // Warm the pool with several stacks, then interleave live iterators with different views and options.
        for (int i = 0; i < 4; i++) db.NewIterator().Dispose();
        using Snapshot snap = db.GetSnapshot();
        db.Put(Key(100), B("later"));
        db.Flush();
        using Iterator latest = db.NewIterator();
        using Iterator old = db.NewIterator(new ReadOptions { Snapshot = snap });
        using Iterator bounded = db.NewIterator(new ReadOptions { IterateLowerBound = Key(40), IterateUpperBound = Key(45) });
        latest.SeekToFirst();
        old.SeekToFirst();
        bounded.SeekToFirst();
        var a = new List<string>();
        var b = new List<string>();
        var c = new List<string>();
        while (latest.Valid || old.Valid || bounded.Valid)
        {
            if (latest.Valid) { a.Add(S(latest.Key.ToArray())!); latest.Next(); }
            if (old.Valid) { b.Add(S(old.Key.ToArray())!); old.Next(); }
            if (bounded.Valid) { c.Add(S(bounded.Key.ToArray())!); bounded.Next(); }
        }
        CollectionAssert.AreEqual(Expected(0, 101), a);
        CollectionAssert.AreEqual(Expected(0, 100), b);
        CollectionAssert.AreEqual(Expected(40, 45), c);
        // A new iterator from the pool sees later writes (no stale view leaks through the pool).
        db.Delete(Key(0));
        using Iterator fresh = db.NewIterator();
        fresh.SeekToFirst();
        Assert.AreEqual(S(Key(1)), S(fresh.Key.ToArray()));
    }

    [TestMethod]
    public void PooledIteratorsDoNotPinObsoleteFiles()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = Open(fs);
        FillLayered(db, 200);
        for (int i = 0; i < 6; i++)
        {
            using Iterator it = db.NewIterator();
            Assert.AreEqual(200, Scan(it).Count);
        }
        Assert.IsTrue(db.DefaultColumnFamily.Data.IteratorPool.Count > 0);
        db.CompactRange(); // every input file becomes obsolete
        int ssts = 0;
        foreach (string f in fs.GetChildren(DbPath)) if (f.EndsWith(".sst", StringComparison.Ordinal)) ssts++;
        Assert.AreEqual(db.GetLiveFilesMetadata().Count, ssts, "pooled iterator stacks must not keep compacted files alive");
    }

    [TestMethod]
    public async Task ConcurrentScansThroughThePoolWhileFlushing()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = Open(fs);
        const int n = 400;
        for (int i = 0; i < n; i++) db.Put(Key(i), Value(i, 0));
        db.Flush();
        using var stop = new CancellationTokenSource();
        Task writer = Task.Run(() =>
        {
            int round = 1;
            while (!stop.IsCancellationRequested)
            {
                for (int i = 0; i < n; i += 7) db.Put(Key(i), Value(i, round));
                db.Flush();
                round++;
            }
        });
        var readers = new Task[4];
        for (int r = 0; r < readers.Length; r++)
        {
            int seed = r;
            readers[r] = Task.Run(() =>
            {
                var rng = new Random(seed);
                for (int iter = 0; iter < 300; iter++)
                {
                    int lo = rng.Next(n - 20);
                    using Iterator it = db.NewIterator(new ReadOptions { IterateUpperBound = Key(lo + 20) });
                    int count = 0;
                    for (it.Seek(Key(lo)); it.Valid; it.Next())
                    {
                        Assert.AreEqual(S(Key(lo + count)), S(it.Key.ToArray()));
                        count++;
                    }
                    Assert.AreEqual(20, count);
                    if ((iter & 7) == 0)
                    {
                        it.Refresh();
                        it.Seek(Key(lo));
                        Assert.IsTrue(it.Valid);
                    }
                }
            });
        }
        try
        {
            await Task.WhenAll(readers);
        }
        finally
        {
            stop.Cancel();
            await writer;
        }
        Assert.IsTrue(db.DefaultColumnFamily.Data.IteratorPool.Count <= Managed.Db.Iterators.IteratorCorePool.Capacity);
    }

    // ------------------------------------------------------------------ ScanAsync reuse

    [TestMethod]
    public async Task ScanAsyncReusesOneEnumeratorPerIterator()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = Open(fs);
        FillLayered(db, 60);
        using Iterator it = db.NewIterator();
        for (int lo = 0; lo < 50; lo += 10)
        {
            var keys = new List<string>();
            await foreach (KeyValue kv in it.ScanAsync(Key(lo), Key(lo + 10))) keys.Add(S(kv.KeyToArray())!);
            CollectionAssert.AreEqual(Expected(lo, lo + 10), keys);
        }
        IAsyncEnumerable<KeyValue> first = it.ScanAsync(null, null);
        await using (IAsyncEnumerator<KeyValue> e = first.GetAsyncEnumerator())
        {
            Assert.IsTrue(await e.MoveNextAsync());
            Assert.ThrowsExactly<InvalidOperationException>(() => it.ScanAsync(null, null));
            Assert.ThrowsExactly<InvalidOperationException>(() => first.GetAsyncEnumerator());
        }
        int all = 0;
        await foreach (KeyValue kv in it.ScanAsync()) all++;
        Assert.AreEqual(60, all);

        Iterator owned = db.NewIterator();
        int n = 0;
        await foreach (KeyValue kv in owned.ScanAsyncAndDispose(Key(5), null)) n++;
        Assert.AreEqual(55, n);
        Assert.ThrowsExactly<ObjectDisposedException>(() => owned.SeekToFirst());
    }

    [TestMethod]
    public async Task ScanAsyncOfADisposedIteratorThrows()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = Open(fs);
        FillLayered(db, 20);
        Iterator it = db.NewIterator();
        IAsyncEnumerator<KeyValue> e = it.ScanAsync().GetAsyncEnumerator();
        Assert.IsTrue(await e.MoveNextAsync());
        it.Dispose();
        await Assert.ThrowsExactlyAsync<ObjectDisposedException>(async () => await e.MoveNextAsync());
        await e.DisposeAsync();
    }

    // ------------------------------------------------------------------ derived iterators

    [TestMethod]
    public void BaseDeltaAndTransactionIteratorsReuseTheirState()
    {
        MemoryFileSystem fs = NewFs();
        using TransactionDatabase t = TransactionDatabase.Open(DbPath, NewOptions(fs));
        RocksDatabase db = t.Database;
        for (int i = 0; i < 30; i += 2) db.Put(Key(i), Value(i, 0));
        db.Flush();
        using var wbwi = new WriteBatchWithIndex();
        for (int i = 1; i < 30; i += 2) wbwi.Put(Key(i), Value(i, 1));
        wbwi.Delete(Key(4));
        for (int round = 0; round < 3; round++)
        {
            BaseDeltaIterator bd = wbwi.NewIteratorWithBase(db.NewIterator());
            var keys = new List<string>();
            for (bd.SeekToFirst(); bd.Valid; bd.Next()) keys.Add(S(bd.Key.ToArray())!);
            List<string> expected = Expected(0, 30);
            expected.Remove(S(Key(4))!);
            CollectionAssert.AreEqual(expected, keys);
            bd.Dispose();
            Assert.ThrowsExactly<ObjectDisposedException>(() => bd.Seek(Key(1)));
        }
        // Two at once: each has its own delta iterator.
        using (BaseDeltaIterator x = wbwi.NewIteratorWithBase(db.NewIterator()))
        using (BaseDeltaIterator y = wbwi.NewIteratorWithBase(db.NewIterator()))
        {
            x.Seek(Key(1));
            y.Seek(Key(20));
            x.Next();
            Assert.AreEqual(S(Key(2)), S(x.Key.ToArray()));
            Assert.AreEqual(S(Key(20)), S(y.Key.ToArray()));
        }

        using Transaction txn = t.BeginTransaction(new WriteOptions());
        txn.Put(Key(3), B("txn"));
        for (int round = 0; round < 3; round++)
        {
            using TransactionIterator ti = txn.NewIterator(null);
            ti.Seek(Key(2));
            Assert.AreEqual(S(Key(2)), S(ti.Key.ToArray()));
            ti.Next();
            Assert.AreEqual("txn", S(ti.Value.ToArray()));
        }
    }

    // ------------------------------------------------------------------ steady-state allocations

    private static long MeasureCallerThread(Action body, int warmup, int measured)
    {
        for (int i = 0; i < warmup; i++) body();
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < measured; i++) body();
        return (GC.GetAllocatedBytesForCurrentThread() - before) / measured;
    }

    [TestMethod]
    public void NewIteratorPerScanAllocatesOnlyTheHandle()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = Open(fs);
        FillLayered(db, 2000);
        byte[][] keys = new byte[512][];
        var rng = new Random(3);
        for (int i = 0; i < keys.Length; i++) keys[i] = Key(rng.Next(1990));
        var ro = new ReadOptions();
        int next = 0;
        long sum = 0;
        long perScan = MeasureCallerThread(() =>
        {
            using Iterator it = db.NewIterator(ro);
            it.Seek(keys[next++ & 511]);
            for (int j = 0; j < 10 && it.Valid; j++, it.Next()) sum += it.Key.Length + it.Value.Length;
        }, warmup: 2000, measured: 2000);
        Assert.IsTrue(sum > 0);
        Assert.IsTrue(perScan <= HandleBytes, "bytes per NewIterator + Seek + 10 Next + Dispose: " + perScan);

        // With a snapshot per scan (the snapshot itself is a small handle object too).
        long perSnapshotScan = MeasureCallerThread(() =>
        {
            using Snapshot s = db.GetSnapshot();
            ro.Snapshot = s;
            using Iterator it = db.NewIterator(ro);
            it.Seek(keys[next++ & 511]);
            for (int j = 0; j < 10 && it.Valid; j++, it.Next()) sum += it.Key.Length;
            ro.Snapshot = null;
        }, warmup: 2000, measured: 2000);
        Assert.IsTrue(perSnapshotScan <= HandleBytes + 128, "bytes per snapshot + iterator scan: " + perSnapshotScan);
    }

    /// <summary>The result of a ValueTask that must complete synchronously (cached data; no async state machine in the loop).</summary>
    private static T Sync<T>(ValueTask<T> t)
    {
        if (!t.IsCompletedSuccessfully) throw new AssertFailedException("expected a synchronous completion");
        return t.Result;
    }

    private static void Sync(ValueTask t)
    {
        if (!t.IsCompletedSuccessfully) throw new AssertFailedException("expected a synchronous completion");
        t.GetAwaiter().GetResult();
    }

    [TestMethod]
    public void NewIteratorAsyncPerScanAllocatesOnlyTheHandle()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = Open(fs);
        FillLayered(db, 1000);
        byte[] key = Key(500);
        long sum = 0;
        // The async API is driven without async methods of the test (Debug builds allocate their state machines): every
        // call completes synchronously on cached data.
        long perScan = MeasureCallerThread(() =>
        {
            using Iterator it = Sync(db.NewIteratorAsync(null));
            Sync(it.SeekAsync(key));
            for (int j = 0; j < 10 && it.Valid; j++)
            {
                sum += it.Key.Length;
                Sync(it.NextAsync());
            }
        }, warmup: 2000, measured: 2000);
        Assert.IsTrue(sum > 0);
        if (s_optimized) Assert.IsTrue(perScan <= HandleBytes, "bytes per NewIteratorAsync + SeekAsync + 10 NextAsync + Dispose: " + perScan);
    }

    [TestMethod]
    public void RefreshAndSetBoundsPerScanDoNotAllocate()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = Open(fs);
        FillLayered(db, 2000);
        byte[][] keys = new byte[2001][];
        for (int i = 0; i < keys.Length; i++) keys[i] = Key(i);
        using Iterator it = db.NewIterator();
        int next = 0;
        long sum = 0;
        long perScan = MeasureCallerThread(() =>
        {
            int lo = (next++ * 37) % 1990;
            it.Refresh();
            it.SetBounds(keys[lo], keys[lo + 10]);
            for (it.SeekToFirst(); it.Valid; it.Next()) sum += it.Key.Length;
        }, warmup: 2000, measured: 2000);
        Assert.IsTrue(sum > 0);
        Assert.AreEqual(0L, perScan, "bytes per Refresh + SetBounds + bounded scan");

        // A refresh that rebuilds the stack (new super version) recycles it too.
        db.Put(Key(5), B("v"));
        long before = GC.GetAllocatedBytesForCurrentThread();
        it.Refresh();
        long rebuilt = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.AreEqual(0L, rebuilt, "same super version");
    }

    [TestMethod]
    public void ScanAsyncOnAReusedIteratorDoesNotAllocate()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = Open(fs);
        FillLayered(db, 1000);
        ReadOnlyMemory<byte>[] keys = new ReadOnlyMemory<byte>[1001];
        for (int i = 0; i < keys.Length; i++) keys[i] = Key(i);
        using Iterator it = db.NewIterator();
        int next = 0;
        long sum = 0;
        long perScan = MeasureCallerThread(() =>
        {
            int lo = (next++ * 13) % 990;
            IAsyncEnumerator<KeyValue> e = it.ScanAsync(keys[lo], keys[lo + 10]).GetAsyncEnumerator();
            while (Sync(e.MoveNextAsync())) sum += e.Current.Value.Length;
            Sync(e.DisposeAsync());
        }, warmup: 2000, measured: 2000);
        Assert.IsTrue(sum > 0);
        if (s_optimized) Assert.AreEqual(0L, perScan, "bytes per ScanAsync enumeration of 10 keys");
    }

    [TestMethod]
    public void BaseDeltaAndTransactionIteratorsPerScanAllocateOnlyHandles()
    {
        MemoryFileSystem fs = NewFs();
        using TransactionDatabase t = TransactionDatabase.Open(DbPath, NewOptions(fs));
        RocksDatabase db = t.Database;
        FillLayered(db, 1000);
        using var wbwi = new WriteBatchWithIndex();
        for (int i = 0; i < 100; i++) wbwi.Put(Key(i * 7), Value(i, 9));
        byte[][] keys = new byte[512][];
        var rng = new Random(9);
        for (int i = 0; i < keys.Length; i++) keys[i] = Key(rng.Next(990));
        int next = 0;
        long sum = 0;
        long wbwiScan = MeasureCallerThread(() =>
        {
            using BaseDeltaIterator it = wbwi.NewIteratorWithBase(db.NewIterator());
            it.Seek(keys[next++ & 511]);
            for (int j = 0; j < 10 && it.Valid; j++, it.Next()) sum += it.Key.Length;
        }, warmup: 2000, measured: 2000);
        Assert.IsTrue(wbwiScan <= 2 * HandleBytes + 64, "bytes per NewIteratorWithBase scan: " + wbwiScan);

        using Transaction txn = t.BeginTransaction(new WriteOptions());
        for (int j = 0; j < 10; j++) txn.Put(Key(j * 13), Value(j, 3));
        long txnScan = MeasureCallerThread(() =>
        {
            using TransactionIterator it = txn.NewIterator(null);
            it.Seek(keys[next++ & 511]);
            for (int j = 0; j < 10 && it.Valid; j++, it.Next()) sum += it.Key.Length;
        }, warmup: 2000, measured: 2000);
        Assert.IsTrue(txnScan <= 3 * HandleBytes + 64, "bytes per transaction iterator scan: " + txnScan);
        Assert.IsTrue(sum > 0);
    }
}
