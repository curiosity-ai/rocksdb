using System.Runtime.CompilerServices;
using System.Reflection;
using RocksDb.Managed.Env;
using RocksDb.Managed.Util;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>
/// The allocation-free waits of WAL tailing (<see cref="AsyncBroadcastSignal"/> behind
/// <see cref="RocksDatabase.WaitForSequenceAsync"/>, <see cref="TransactionLogIterator.MoveNextTailingAsync"/> and
/// <see cref="RocksDatabase.TailUpdatesAsync"/>), <see cref="RocksDatabase.CurrentWalNumber"/>, the iterator's rotation
/// check and the secondary's idle catch-up: functional coverage plus steady-state allocation bounds.
/// </summary>
[TestClass]
public sealed class ReplicationAllocationTests
{
    private static readonly bool Optimized =
        typeof(RocksDatabase).Assembly.GetCustomAttribute<System.Diagnostics.DebuggableAttribute>() is not { IsJITOptimizerDisabled: true };

    private static readonly byte[] Value100 = new byte[100];

    /// <summary>A DB on a memory file system whose memtable never fills during a test (no background flush allocations).</summary>
    private static RocksDatabase OpenQuiet(MemoryFileSystem fs) =>
        RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { WriteBufferSize = 256UL << 20 });

    /// <summary>
    /// A DB in a new temporary directory on the local file system: the memory file system grows its file buffers on
    /// writes, which would drown the all-thread allocation numbers.
    /// </summary>
    private static RocksDatabase OpenQuietLocal(out string dir)
    {
        dir = Path.Combine(Path.GetTempPath(), "rdbm-replalloc-" + Guid.NewGuid().ToString("N"));
        return RocksDatabase.Open(dir, new DbOptions { CreateIfMissing = true }, new ColumnFamilyOptions { WriteBufferSize = 256UL << 20 });
    }

    /// <summary>
    /// Average bytes per op allocated by all threads over <paramref name="ops"/> runs of <paramref name="op"/>, after
    /// <paramref name="warmup"/> runs (waits complete on thread-pool threads, so the caller-thread counter does not see them).
    /// </summary>
    private static async Task<double> MeasureAllThreadsAsync(Func<int, ValueTask> op, int warmup, int ops)
    {
        for (int i = 0; i < warmup; i++) await op(i);
        long before = GC.GetTotalAllocatedBytes(precise: true);
        for (int i = 0; i < ops; i++) await op(warmup + i);
        return (GC.GetTotalAllocatedBytes(precise: true) - before) / (double)ops;
    }

    // =====================================================================================================
    // AsyncBroadcastSignal
    // =====================================================================================================

    [TestMethod]
    public async Task SignalWakesEveryRegisteredWaiterAndRecyclesNodes()
    {
        var signal = new AsyncBroadcastSignal();
        for (int round = 0; round < 3; round++)
        {
            var waiters = new AsyncBroadcastSignal.Waiter[8];
            var tasks = new Task[8];
            for (int i = 0; i < waiters.Length; i++) waiters[i] = signal.Register();
            Assert.IsTrue(signal.HasWaiters);
            for (int i = 0; i < waiters.Length; i++) tasks[i] = signal.WaitAsync(waiters[i], CancellationToken.None).AsTask();
            Assert.IsFalse(tasks[0].IsCompleted);
            signal.SignalAll();
            await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(10));
            Assert.IsFalse(signal.HasWaiters);
        }
        // Signalling with nobody registered is a no-op; an unregistered waiter is not woken later.
        signal.SignalAll();
        AsyncBroadcastSignal.Waiter w = signal.Register();
        signal.Unregister(w);
        Assert.IsFalse(signal.HasWaiters);
        AsyncBroadcastSignal.Waiter w2 = signal.Register();
        Task t = signal.WaitAsync(w2, CancellationToken.None).AsTask();
        signal.SignalAll();
        await t.WaitAsync(TimeSpan.FromSeconds(10));
    }

    [TestMethod]
    public async Task SignalWaitCancellationThrowsWithTheTokenAndLeavesOthersWaiting()
    {
        var signal = new AsyncBroadcastSignal();
        using var cts = new CancellationTokenSource();
        Task cancelled = signal.WaitAsync(signal.Register(), cts.Token).AsTask();
        Task other = signal.WaitAsync(signal.Register(), CancellationToken.None).AsTask();
        await cts.CancelAsync();
        OperationCanceledException e = await Assert.ThrowsExactlyAsync<OperationCanceledException>(() => cancelled);
        Assert.AreEqual(cts.Token, e.CancellationToken);
        Assert.IsFalse(other.IsCompleted, "cancelling one waiter must not wake the others");
        Assert.IsTrue(signal.HasWaiters);
        signal.SignalAll();
        await other.WaitAsync(TimeSpan.FromSeconds(10));

        // An already cancelled token completes the wait at once; the node is recycled and the signal keeps working.
        Task pre = signal.WaitAsync(signal.Register(), cts.Token).AsTask();
        await Assert.ThrowsExactlyAsync<OperationCanceledException>(() => pre);
        Assert.IsFalse(signal.HasWaiters);
        Task after = signal.WaitAsync(signal.Register(), CancellationToken.None).AsTask();
        signal.SignalAll();
        await after.WaitAsync(TimeSpan.FromSeconds(10));
    }

    /// <summary>Waiters register / unregister / cancel while a producer signals: no lost wake-up, no corrupted list.</summary>
    [TestMethod]
    public async Task SignalSurvivesConcurrentRegisterUnregisterCancelAndSignal()
    {
        var signal = new AsyncBroadcastSignal();
        long published = 0;
        using var stop = new CancellationTokenSource();
        Task producer = Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
            {
                Interlocked.Increment(ref published);
                signal.SignalAll();
                await Task.Yield();
            }
        });
        var waiters = new Task[4];
        for (int w = 0; w < waiters.Length; w++)
        {
            int worker = w;
            waiters[w] = Task.Run(async () =>
            {
                for (int i = 0; i < 2000; i++)
                {
                    long target = Volatile.Read(ref published) + 1;
                    while (Volatile.Read(ref published) < target)
                    {
                        AsyncBroadcastSignal.Waiter n = signal.Register();
                        if (Volatile.Read(ref published) >= target || (i + worker) % 7 == 0)
                        {
                            signal.Unregister(n);
                            continue;
                        }
                        if ((i + worker) % 11 == 0)
                        {
                            using var cts = new CancellationTokenSource();
                            ValueTask wait = signal.WaitAsync(n, cts.Token);
                            cts.Cancel();
                            try
                            {
                                await wait;
                            }
                            catch (OperationCanceledException)
                            {
                            }
                            continue;
                        }
                        await signal.WaitAsync(n, CancellationToken.None);
                    }
                }
            });
        }
        await Task.WhenAll(waiters).WaitAsync(TimeSpan.FromSeconds(60));
        await stop.CancelAsync();
        await producer;
    }

    // =====================================================================================================
    // WaitForSequenceAsync / tailing
    // =====================================================================================================

    [TestMethod]
    public async Task WaitForSequenceCompletesOnWriteCancelsAndEndsOnClose()
    {
        MemoryFileSystem fs = NewFs();
        RocksDatabase db = OpenQuiet(fs);
        db.Put(Key(0), Value100);
        Assert.IsTrue(db.WaitForSequenceAsync(1).IsCompletedSuccessfully, "already published: synchronous");

        // Several waiters for different sequence numbers: each completes once its sequence is published.
        var waits = new Task<bool>[4];
        for (int i = 0; i < waits.Length; i++) waits[i] = db.WaitForSequenceAsync(2 + (ulong)i).AsTask();
        Assert.IsFalse(waits[0].IsCompleted);
        db.Put(Key(1), Value100);
        Assert.IsTrue(await waits[0].WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.IsFalse(waits[3].IsCompleted);
        for (int i = 2; i <= 4; i++) db.Put(Key(i), Value100);
        foreach (Task<bool> t in waits) Assert.IsTrue(await t.WaitAsync(TimeSpan.FromSeconds(10)));

        using (var cts = new CancellationTokenSource())
        {
            Task<bool> cancelled = db.WaitForSequenceAsync(100, cts.Token).AsTask();
            await cts.CancelAsync();
            OperationCanceledException e = await Assert.ThrowsExactlyAsync<OperationCanceledException>(() => cancelled);
            Assert.AreEqual(cts.Token, e.CancellationToken);
        }

        Task<bool> pending = db.WaitForSequenceAsync(100).AsTask();
        await db.DisposeAsync();
        Assert.IsFalse(await pending.WaitAsync(TimeSpan.FromSeconds(10)), "closing the DB ends the wait with false");
    }

    [TestMethod]
    public async Task TailingWaitsAllocateNothingInSteadyState()
    {
        RocksDatabase db = OpenQuietLocal(out string dir);
        try
        {
            await MeasureTailingWaitsAsync(db);
        }
        finally
        {
            await db.DisposeAsync();
            Directory.Delete(dir, recursive: true);
        }
    }

    private static async Task MeasureTailingWaitsAsync(RocksDatabase db)
    {
        db.Put(Key(0), Value100);
        using var cts = new CancellationTokenSource();

        // The same loop with only the write: the baseline the tailing variants are compared with (in a Debug build the
        // write path's async state machines are classes allocated per call).
        int n = 1;
        double putBytes = await MeasureAllThreadsAsync([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int i) => await db.PutAsync(Key(n++), Value100), 500, 2000);

        // TransactionLogIterator.MoveNextTailingAsync: the wait is pending when the write arrives.
        using TransactionLogIterator it = db.GetUpdatesSince(db.GetLatestSequenceNumber());
        double iteratorBytes = await MeasureAllThreadsAsync([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int i) =>
        {
            ValueTask<bool> next = it.MoveNextTailingAsync(cts.Token);
            await db.PutAsync(Key(n++), Value100);
            Assert.IsTrue(await next);
        }, 500, 2000);
        Assert.AreEqual(db.GetLatestSequenceNumber(), it.Current.Sequence);

        // TailUpdatesAsync and a bare WaitForSequenceAsync.
        await using IAsyncEnumerator<WalBatch> e = db.TailUpdatesAsync(db.GetLatestSequenceNumber(), cts.Token).GetAsyncEnumerator(cts.Token);
        Assert.IsTrue(await e.MoveNextAsync());
        double tailBytes = await MeasureAllThreadsAsync([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int i) =>
        {
            ValueTask<bool> next = e.MoveNextAsync();
            await db.PutAsync(Key(n++), Value100);
            Assert.IsTrue(await next);
        }, 500, 2000);
        Assert.AreEqual(db.GetLatestSequenceNumber(), e.Current.Sequence);
        double waitBytes = await MeasureAllThreadsAsync([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int i) =>
        {
            ValueTask<bool> wait = db.WaitForSequenceAsync(db.GetLatestSequenceNumber() + 1, cts.Token);
            await db.PutAsync(Key(n++), Value100);
            Assert.IsTrue(await wait);
        }, 500, 2000);

        // Before: a TaskCompletionSource, a Task.WaitAsync promise and two state machine boxes per wait (~500 B over the
        // write). The bound leaves room for thread-pool bookkeeping; a Debug build of the library allocates its async
        // state machines (classes) per call.
        double bound = Optimized ? 32 : 1024;
        string what = " B/op over PutAsync alone (" + putBytes + " B/op)";
        iteratorBytes -= putBytes;
        tailBytes -= putBytes;
        waitBytes -= putBytes;
        Assert.IsTrue(iteratorBytes < bound, "MoveNextTailingAsync: " + iteratorBytes + what);
        Assert.IsTrue(tailBytes < bound, "TailUpdatesAsync: " + tailBytes + what);
        Assert.IsTrue(waitBytes < bound, "WaitForSequenceAsync: " + waitBytes + what);
    }

    [TestMethod]
    public void CurrentWalNumberTracksRotationsWithoutListing()
    {
        MemoryFileSystem fs = NewFs();
        RocksDatabase db = OpenQuiet(fs);
        db.Put(Key(0), Value100);
        ulong first = db.CurrentWalNumber;
        Assert.AreNotEqual(0UL, first);
        Assert.AreEqual(db.GetCurrentWalFile().LogNumber, first);
        db.Flush();
        db.Put(Key(1), Value100);
        Assert.IsTrue(db.CurrentWalNumber > first, "a flush switches to a new WAL");
        Assert.AreEqual(db.GetCurrentWalFile().LogNumber, db.CurrentWalNumber);
        db.Dispose();
        Assert.ThrowsExactly<ObjectDisposedException>(() => _ = db.CurrentWalNumber);
        using RocksDatabase ro = RocksDatabase.OpenReadOnly(DbPath, NewOptions(fs));
        Assert.AreEqual(0UL, ro.CurrentWalNumber, "a read-only instance writes no WAL");
    }

    /// <summary>A held iterator keeps following the WAL across rotations (re-listing only after a rotation).</summary>
    [TestMethod]
    public void HeldIteratorFollowsRotationsAndEndsQuietlyAtTheTail()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = OpenQuiet(fs);
        db.Put(Key(0), Value100);
        using TransactionLogIterator it = db.GetUpdatesSince(1);
        Assert.IsTrue(it.Valid);
        int files = it.Files.Count;
        for (int i = 1; i <= 20; i++)
        {
            it.Next();
            Assert.IsFalse(it.Valid, "at the tail");
            Assert.IsNull(it.Error);
            Assert.AreEqual(files, it.Files.Count, "no rotation: the file list is unchanged");
            if (i % 5 == 0)
            {
                db.Flush();
                files++;
            }
            db.Put(Key(i), Value100);
            it.Next();
            Assert.IsTrue(it.Valid, "batch " + i);
            Assert.AreEqual((ulong)i + 1, it.Current.Sequence);
            Assert.AreEqual(files, it.Files.Count, "followed the rotation");
        }
    }

    // =====================================================================================================
    // Secondary catch-up
    // =====================================================================================================

    [TestMethod]
    public void IdleSecondaryCatchUpIsCheapAndStillSeesNewWritesAndManifests()
    {
        MemoryFileSystem fs = NewFs();
        RocksDatabase primary = OpenQuiet(fs);
        for (int i = 0; i < 100; i++) primary.Put(Key(i), Value100);
        primary.Flush();
        primary.Put(Key(100), Value100);
        using RocksDatabase secondary = RocksDatabase.OpenAsSecondary(DbPath, "/secondary", NewOptions(fs),
            [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions())]);
        secondary.TryCatchUpWithPrimary();
        Assert.AreEqual(primary.GetLatestSequenceNumber(), secondary.GetLatestSequenceNumber());

        for (int i = 0; i < 50; i++) secondary.TryCatchUpWithPrimary();
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 200; i++) secondary.TryCatchUpWithPrimary();
        double perPoll = (GC.GetAllocatedBytesForCurrentThread() - before) / 200.0;
        // Before: ~7 KB per idle poll (CURRENT read through a 4 KiB growing buffer, a super version per column family,
        // per-poll lists and reporter). What remains is the WAL directory listing and opening CURRENT, as upstream does.
        Assert.IsTrue(perPoll < 3000, "idle TryCatchUpWithPrimary: " + perPoll + " B/poll");
        Assert.IsTrue(secondary.TryGet(Key(100), new byte[200], out int written));
        Assert.AreEqual(100, written);

        // New WAL data and a new MANIFEST (primary reopened) are still picked up after idle polls.
        primary.Put(Key(101), Value100);
        secondary.TryCatchUpWithPrimary();
        Assert.AreEqual(primary.GetLatestSequenceNumber(), secondary.GetLatestSequenceNumber());
        (int opens, _) = secondary.SecondaryManifestTailStats;
        primary.Dispose();
        primary = OpenQuiet(fs);
        primary.Put(Key(102), Value100);
        primary.Flush();
        secondary.TryCatchUpWithPrimary();
        Assert.IsTrue(secondary.SecondaryManifestTailStats.ManifestOpens > opens, "switched to the reopened primary's MANIFEST");
        Assert.IsTrue(secondary.TryGet(Key(102), new byte[200], out _));
        Assert.IsTrue(secondary.TryGet(Key(5), new byte[200], out _));
        primary.Dispose();
    }
}
