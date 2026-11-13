using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #5, the write path under concurrency (db/db_impl/db_impl_write.cc, db/write_thread.cc, DBImpl::FlushWAL /
/// LockWAL / UnlockWAL in db/db_impl/db_impl.cc). The interleavings are pinned with <see cref="TestHooks"/> sync points
/// and a <see cref="UserWriteCallback"/> that holds a write leader after its WAL write, so that the writers behind it
/// form the next write group in a known order.
/// <list type="bullet">
/// <item>The cancellation of an asynchronous write that leads a write group and waits for a write stall cancels that
/// write only: the other writers of its group did not ask for it and are written once the stall ends.</item>
/// <item>The WAL is only ever used by one thread at a time, also by FlushWAL / SyncWAL while LockWAL is held (they do
/// not enter the write queue then, since a stalled write leader may hold it until UnlockWAL).</item>
/// </list>
/// </summary>
[TestClass]
public sealed class Audit5WritePathTests
{
    private const string DbPath = "/audit5writepath";
    private const string JoinWaitHook = "WriteThread::JoinBatchGroup:Wait";
    private const string DelayWriteWaitHook = "DBImpl::DelayWrite:Wait";
    private const string WbmStallWaitHook = "DBImpl::WriteBufferManagerStallWrites:Wait";
    private const string WalIoWaitHook = "DBImpl::WalIoLock:Wait";
    private const long Mib = 1024 * 1024;

    [TestCleanup]
    public void Cleanup() => TestHooks.ClearAll();

    /// <summary>The write modes of DBImpl::WriteImpl.</summary>
    public enum WriteMode
    {
        Default,
        Pipelined,
        Unordered,
    }

    private static DbOptions ModeOptions(IFileSystem fs, WriteMode mode)
    {
        DbOptions o = NewOptions(fs);
        o.EnablePipelinedWrite = mode == WriteMode.Pipelined;
        o.UnorderedWrite = mode == WriteMode.Unordered;
        return o;
    }

    /// <summary>Holds its write, as the leader of its write group, right after the group's WAL record is written.</summary>
    private sealed class HoldAfterWal : UserWriteCallback, IDisposable
    {
        public readonly ManualResetEventSlim InWal = new();
        public readonly ManualResetEventSlim Release = new();

        public override void OnWriteEnqueued() { }

        public override void OnWalWriteFinish()
        {
            InWal.Set();
            Release.Wait();
        }

        public void Dispose()
        {
            InWal.Dispose();
            Release.Dispose();
        }
    }

    /// <summary>
    /// A write held as write leader (after its WAL write) on its own thread; the writes issued meanwhile queue up behind
    /// it and form the next write group, in the order in which they were issued.
    /// </summary>
    private sealed class HeldLeader : IDisposable
    {
        private readonly HoldAfterWal _hold = new();
        private readonly Thread _thread;
        private Exception? _error;

        public HeldLeader(RocksDatabase db, string key)
        {
            _thread = new Thread(() =>
            {
                try
                {
                    using var batch = new WriteBatch();
                    batch.Put(B(key), B("held"));
                    db.WriteWithCallback(null, batch, _hold);
                }
                catch (Exception e)
                {
                    _error = e;
                }
            });
            _thread.Start();
            _hold.InWal.Wait();
        }

        public void ReleaseAndJoin()
        {
            _hold.Release.Set();
            _thread.Join();
            Assert.IsNull(_error, _error?.ToString());
        }

        public void Dispose()
        {
            _hold.Release.Set();
            _thread.Join();
            _hold.Dispose();
        }
    }

    // ---- cancellation of a stalled write leader -------------------------------------------------------------

    /// <summary>
    /// An asynchronous write whose CancellationToken fires while it leads a write group and waits for stopped writes
    /// (DBImpl::DelayWrite; here the stop of LockWAL) is canceled: it throws OperationCanceledException and is not
    /// written. The follower of its group did not ask for the cancellation: it stays queued, takes the leadership, waits
    /// for the stall itself and is written once the stall ends. The port failed the whole group with the leader's
    /// cancellation (as an IOError).
    /// </summary>
    [TestMethod]
    [DataRow(WriteMode.Default)]
    [DataRow(WriteMode.Pipelined)]
    [DataRow(WriteMode.Unordered)]
    [Timeout(60_000, CooperativeCancellation = true)]
    public async Task CanceledStalledLeaderDoesNotFailTheFollowersOfItsGroup(WriteMode mode)
    {
        MemoryFileSystem fs = NewFs();
        using var queued = new SemaphoreSlim(0);
        using var stalls = new SemaphoreSlim(0);
        using var cts = new CancellationTokenSource();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, ModeOptions(fs, mode)))
        {
            db.Put(B("before"), B("1"));
            TestHooks.Set(JoinWaitHook, () => queued.Release());
            TestHooks.Set(DelayWriteWaitHook, () => stalls.Release());
            WalLock? walLock = null;
            Task leader, follower;
            using (var held = new HeldLeader(db, "held"))
            {
                // Queue: LockWAL (alone in its group), then the leader and the follower of the next group.
                var locker = new Thread(() => walLock = db.LockWal());
                locker.Start();
                queued.Wait();
                leader = db.PutAsync(B("leader"), B("l"), cts.Token).AsTask();
                queued.Wait();
                follower = db.PutAsync(B("follower"), B("f")).AsTask();
                queued.Wait();
                held.ReleaseAndJoin();
                locker.Join();
            }
            Assert.IsNotNull(walLock);
            try
            {
                // The leader took the follower into its group and waits for the stopped writes.
                stalls.Wait();
                Assert.IsFalse(leader.IsCompleted);
                Assert.IsFalse(follower.IsCompleted);
                cts.Cancel();
                // The follower either fails with the leader's cancellation (the bug) or leads its own group into the stall.
                Task secondStall = stalls.WaitAsync();
                await Task.WhenAny(follower, secondStall);
                Assert.IsFalse(follower.IsCompleted, follower.Exception?.ToString() ?? "the follower was acknowledged during the stall");
                await Assert.ThrowsAsync<OperationCanceledException>(() => leader);
            }
            finally
            {
                walLock.Dispose();
            }
            await follower;
            Assert.AreEqual("f", GetString(db, "follower"));
            Assert.IsNull(db.Get(B("leader")), "a canceled write must not be written");
            Assert.AreEqual("held", GetString(db, "held"));
            Assert.IsNull(db.BackgroundError);
            db.Put(B("after"), B("2"));
        }
        TestHooks.ClearAll();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, ModeOptions(fs, mode)))
        {
            Assert.AreEqual("f", GetString(db, "follower"));
            Assert.IsNull(db.Get(B("leader")));
            Assert.AreEqual("2", GetString(db, "after"));
        }
    }

    /// <summary>
    /// The same for a write stalled by the write buffer manager (DBImpl::WriteBufferManagerStallWrites): the canceled
    /// leader throws OperationCanceledException promptly, and its follower is written once memory is released. The port
    /// did not notice the cancellation at all: the wait loop's Task.WhenAny swallowed it and then spun (a canceled
    /// Task.Delay completes at once) until the stall ended.
    /// </summary>
    [TestMethod]
    [DataRow(WriteBufferFlushPolicy.FlushOldest)]
    [DataRow(WriteBufferFlushPolicy.FlushLargestAcrossDbs)]
    [Timeout(60_000, CooperativeCancellation = true)]
    public async Task CanceledLeaderStalledByWriteBufferManagerIsCanceledAlone(WriteBufferFlushPolicy policy)
    {
        MemoryFileSystem fs = NewFs();
        using var queued = new SemaphoreSlim(0);
        using var stalls = new SemaphoreSlim(0);
        using var cts = new CancellationTokenSource();
        var wbm = new WriteBufferManager(64 * Mib, null, allowStall: true, policy);
        DbOptions o = NewOptions(fs);
        o.WriteBufferManager = wbm;
        using RocksDatabase db = RocksDatabase.Open(DbPath, o);
        db.Put(B("before"), B("1"));
        TestHooks.Set(JoinWaitHook, () => queued.Release());
        TestHooks.Set(WbmStallWaitHook, () => stalls.Release());
        Task leader, follower;
        bool reserved = false;
        try
        {
            using (var held = new HeldLeader(db, "held"))
            {
                leader = db.PutAsync(B("leader"), B("l"), cts.Token).AsTask();
                queued.Wait();
                follower = db.PutAsync(B("follower"), B("f")).AsTask();
                queued.Wait();
                wbm.ReserveMem(128 * Mib); // memory held elsewhere: the next write group stalls
                reserved = true;
                Assert.IsTrue(wbm.ShouldStall());
                held.ReleaseAndJoin();
            }
            stalls.Wait();
            Assert.IsFalse(leader.IsCompleted);
            Assert.IsFalse(follower.IsCompleted);
            cts.Cancel();
            Task done = await Task.WhenAny(leader, Task.Delay(TimeSpan.FromSeconds(20)));
            Assert.AreSame(leader, done, "the canceled write is still waiting for the stall");
            await Assert.ThrowsAsync<OperationCanceledException>(() => leader);
            Task secondStall = stalls.WaitAsync();
            await Task.WhenAny(follower, secondStall);
            Assert.IsFalse(follower.IsCompleted, follower.Exception?.ToString() ?? "the follower was acknowledged during the stall");
        }
        finally
        {
            if (reserved) wbm.FreeMem(128 * Mib);
        }
        await follower;
        Assert.AreEqual("f", GetString(db, "follower"));
        Assert.IsNull(db.Get(B("leader")), "a canceled write must not be written");
        Assert.IsNull(db.BackgroundError);
    }

    // ---- close while a write is stalled ---------------------------------------------------------------------

    /// <summary>
    /// DBImpl::DelayWrite waits "while writes are stopped and not shutting down" and then returns ShutdownInProgress:
    /// closing the DB (CloseHelper -> CancelAllBackgroundWork sets shutting_down_ and signals bg_cv_) releases a writer
    /// that waits for stopped writes. The port set its shutdown flag only once close had become the write leader, which
    /// it cannot become while the stalled leader holds the write queue: close waited for the stall to end, for as long
    /// as LockWAL was held (or background work could not relieve the stall). Now close announces itself first: the
    /// stalled write and the write queued behind it fail with ObjectDisposedException (the port's error for a write
    /// that races the close), neither is written, and close returns.
    /// </summary>
    [TestMethod]
    [DataRow(WriteMode.Default)]
    [DataRow(WriteMode.Pipelined)]
    [DataRow(WriteMode.Unordered)]
    [Timeout(60_000, CooperativeCancellation = true)]
    public void CloseReleasesAWriteStalledByStoppedWrites(WriteMode mode)
    {
        MemoryFileSystem fs = NewFs();
        using var queued = new SemaphoreSlim(0);
        using var stalls = new SemaphoreSlim(0);
        RocksDatabase db = RocksDatabase.Open(DbPath, ModeOptions(fs, mode));
        Thread? closer = null;
        db.Put(B("a"), B("1"));
        WalLock walLock = db.LockWal();
        try
        {
            TestHooks.Set(JoinWaitHook, () => queued.Release());
            TestHooks.Set(DelayWriteWaitHook, () => stalls.Release());
            Task stalled = db.PutAsync(B("stalled"), B("s")).AsTask();
            stalls.Wait();
            Task behind = db.PutAsync(B("behind"), B("b")).AsTask();
            queued.Wait();
            closer = new Thread(db.Dispose);
            closer.Start();
            Assert.IsTrue(closer.Join(TimeSpan.FromSeconds(20)), "close waits for the write stall to end");
            foreach (Task t in new[] { stalled, behind }) Assert.ThrowsExactly<ObjectDisposedException>(() => t.GetAwaiter().GetResult());
        }
        finally
        {
            walLock.Dispose();
            closer?.Join();
            db.Dispose();
        }
        TestHooks.ClearAll();
        using RocksDatabase reopened = RocksDatabase.Open(DbPath, ModeOptions(fs, mode));
        Assert.AreEqual("1", GetString(reopened, "a"));
        Assert.IsNull(reopened.Get(B("stalled")));
        Assert.IsNull(reopened.Get(B("behind")));
    }

    // ---- FlushWAL / SyncWAL while LockWAL is held -------------------------------------------------------------

    /// <summary>
    /// Forwards to a <see cref="MemoryFileSystem"/>; the WAL files it creates report every use by two threads at once
    /// (a writable file has a single writer) and let a test hold one thread inside Flush.
    /// </summary>
    private sealed class WalWatchFileSystem(MemoryFileSystem inner) : IFileSystem
    {
        /// <summary>Set once: the next WAL Flush on this thread signals <see cref="InFlush"/> and waits for <see cref="ReleaseFlush"/>.</summary>
        public volatile Thread? HoldFlushOf;
        public readonly ManualResetEventSlim InFlush = new();
        public readonly ManualResetEventSlim ReleaseFlush = new();
        public readonly ManualResetEventSlim Overlapped = new();
        public volatile string? OverlapDetail;

        private sealed class WatchedFile(WalWatchFileSystem owner, IWritableFile file) : IWritableFile
        {
            private readonly WalWatchFileSystem fs = owner;
            private int _inside;

            public long Size => file.Size;

            private void Enter(string op)
            {
                if (Interlocked.Increment(ref _inside) > 1)
                {
                    fs.OverlapDetail ??= op + " on thread " + Environment.CurrentManagedThreadId + " while another thread uses the same WAL file";
                    fs.Overlapped.Set();
                }
            }

            private void Leave() => Interlocked.Decrement(ref _inside);

            public void Append(ReadOnlySpan<byte> data)
            {
                Enter("Append");
                try { file.Append(data); }
                finally { Leave(); }
            }

            public async ValueTask AppendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
            {
                Enter("AppendAsync");
                try { await file.AppendAsync(data, cancellationToken).ConfigureAwait(false); }
                finally { Leave(); }
            }

            public void Flush()
            {
                Enter("Flush");
                try
                {
                    if (ReferenceEquals(fs.HoldFlushOf, Thread.CurrentThread))
                    {
                        fs.HoldFlushOf = null;
                        fs.InFlush.Set();
                        fs.ReleaseFlush.Wait();
                    }
                    file.Flush();
                }
                finally { Leave(); }
            }

            public async ValueTask FlushAsync(CancellationToken cancellationToken = default)
            {
                Enter("FlushAsync");
                try { await file.FlushAsync(cancellationToken).ConfigureAwait(false); }
                finally { Leave(); }
            }

            public void Sync()
            {
                Enter("Sync");
                try { file.Sync(); }
                finally { Leave(); }
            }

            public async ValueTask SyncAsync(CancellationToken cancellationToken = default)
            {
                Enter("SyncAsync");
                try { await file.SyncAsync(cancellationToken).ConfigureAwait(false); }
                finally { Leave(); }
            }

            public void Dispose()
            {
                Enter("Dispose");
                try { file.Dispose(); }
                finally { Leave(); }
            }

            public async ValueTask DisposeAsync()
            {
                Enter("DisposeAsync");
                try { await file.DisposeAsync().ConfigureAwait(false); }
                finally { Leave(); }
            }
        }

        private IWritableFile Watch(string path, IWritableFile file) => path.EndsWith(".log", StringComparison.Ordinal) ? new WatchedFile(this, file) : file;

        public IWritableFile CreateWritable(string path, bool truncate = true) => Watch(path, inner.CreateWritable(path, truncate));
        public IWritableFile CreateWritable(string path, bool truncate, WritableFileOptions options) => Watch(path, inner.CreateWritable(path, truncate, options));
        public IRandomAccessFile OpenRandomAccess(string path) => inner.OpenRandomAccess(path);
        public ISequentialFile OpenSequential(string path) => inner.OpenSequential(path);
        public bool FileExists(string path) => inner.FileExists(path);
        public long GetFileSize(string path) => inner.GetFileSize(path);
        public void DeleteFile(string path) => inner.DeleteFile(path);
        public void RenameFile(string source, string target) => inner.RenameFile(source, target);
        public void CreateDirectoryIfMissing(string path) => inner.CreateDirectoryIfMissing(path);
        public IEnumerable<string> GetChildren(string directory) => inner.GetChildren(directory);
        public IDisposable LockFile(string path) => inner.LockFile(path);
        public void SyncDirectory(string path) => inner.SyncDirectory(path);
        public bool DirectoryExists(string path) => inner.DirectoryExists(path);
        public void DeleteDirectory(string path) => inner.DeleteDirectory(path);
    }

    /// <summary>What runs against a FlushWAL that is inside the WAL file while LockWAL is held.</summary>
    public enum WalRacer
    {
        /// <summary>UnlockWAL, then a write: the write's WAL record.</summary>
        UnlockThenWrite,
        /// <summary>A manual flush: its memtable switch writes out and replaces the WAL.</summary>
        MemtableSwitch,
        /// <summary>Another FlushWAL (with sync).</summary>
        SyncWal,
    }

    /// <summary>
    /// DBImpl::FlushWAL / SyncWAL take wal_write_mutex_, which every WAL write and WAL switch takes too. The port
    /// serializes WAL users through the write queue, except for a FlushWAL called while LockWAL is held: it must not
    /// enter the queue (a stalled write leader may hold it until UnlockWAL) and used the WAL unprotected, relying on
    /// the write stop. That does not exclude: a write issued right after UnlockWAL, the memtable switch of a flush (an
    /// unbatched leader is not stalled), or a second FlushWAL / SyncWAL. Each could append to, flush, sync or replace
    /// the WAL writer (not thread-safe: a buffered file) while the first FlushWAL was inside it.
    /// <para>
    /// Here a FlushWAL under LockWAL is held inside the WAL file's Flush while the racer runs. The racer must wait for
    /// it (it reaches the "WalIoLock:Wait" sync point); without the exclusion it uses the WAL file concurrently, which the
    /// file system reports.
    /// </para>
    /// </summary>
    [TestMethod]
    [DataRow(WalRacer.UnlockThenWrite)]
    [DataRow(WalRacer.MemtableSwitch)]
    [DataRow(WalRacer.SyncWal)]
    [Timeout(60_000, CooperativeCancellation = true)]
    public void FlushWalUnderLockWalExcludesOtherWalUsers(WalRacer racer)
    {
        MemoryFileSystem mem = NewFs();
        var fs = new WalWatchFileSystem(mem);
        using var waiting = new ManualResetEventSlim();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            db.Put(B("a"), B("1"));
            WalLock walLock = db.LockWal();
            bool unlocked = false;
            Exception? flushError = null, racerError = null;
            try
            {
                TestHooks.Set(WalIoWaitHook, waiting.Set);
                var flusher = new Thread(() =>
                {
                    try { db.FlushWal(sync: false); }
                    catch (Exception e) { flushError = e; }
                });
                fs.HoldFlushOf = flusher;
                flusher.Start();
                fs.InFlush.Wait();
                var other = new Thread(() =>
                {
                    try
                    {
                        switch (racer)
                        {
                            case WalRacer.UnlockThenWrite:
                                unlocked = true;
                                walLock.Dispose();
                                db.Put(B("b"), B("2"));
                                break;
                            case WalRacer.MemtableSwitch:
                                db.Flush();
                                break;
                            default:
                                db.SyncWal();
                                break;
                        }
                    }
                    catch (Exception e)
                    {
                        racerError = e;
                    }
                });
                other.Start();
                // Either the racer waits for the FlushWAL in flight, or it used the WAL file while that was inside it.
                WaitHandle.WaitAny([waiting.WaitHandle, fs.Overlapped.WaitHandle]);
                fs.ReleaseFlush.Set();
                flusher.Join();
                other.Join();
            }
            finally
            {
                fs.ReleaseFlush.Set();
                if (!unlocked) walLock.Dispose();
            }
            Assert.IsFalse(fs.Overlapped.IsSet, fs.OverlapDetail);
            Assert.IsNull(flushError, flushError?.ToString());
            Assert.IsNull(racerError, racerError?.ToString());
            db.Put(B("c"), B("3"));
            Assert.IsFalse(fs.Overlapped.IsSet, fs.OverlapDetail);
        }
        TestHooks.ClearAll();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(mem)))
        {
            Assert.AreEqual("1", GetString(db, "a"));
            if (racer == WalRacer.UnlockThenWrite) Assert.AreEqual("2", GetString(db, "b"));
            Assert.AreEqual("3", GetString(db, "c"));
        }
    }
}
