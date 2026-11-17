using System.Collections.Concurrent;
using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Utilities.Transactions;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #12: synchronous and asynchronous writers on one DB (the thread-pool starvation of the write path, audit #8
/// open item). A synchronous writer (Put / Write, a synchronous Flush...) blocks its thread until the writers ahead of it
/// are done; nothing it waits for may need a .NET thread-pool thread, because its own thread may be the pool's last one.
/// The waiting points: an asynchronous group leader's WAL I/O and stall waits (DelayWrite, the write buffer manager), the
/// hand-off of the leadership to an asynchronous writer, the pipelined memtable stage, parallel and unordered memtable
/// inserts, the WAL-only queue of two_write_queues, asynchronous unbatched operations (FlushAsync, FlushWalAsync,
/// CreateColumnFamilyAsync...), the WAL I/O lock and the MANIFEST writer slot. Upstream has no asynchronous writers
/// (db/write_thread.cc AwaitState blocks every waiter); the reference is the synchronous managed path: the same program
/// run all-sync gives the same DB.
/// <para>
/// Most tests block every .NET thread-pool worker (<see cref="DeadThreadPool"/>) while synchronous writers on dedicated
/// threads and asynchronous writers started from a dedicated thread write concurrently: the synchronous writers must
/// finish while the pool is dead (the asynchronous ones finish once it is released, their callers' continuations need
/// it). Without the fix each of them hangs until the deadline. The DB gets its first synchronous write before the
/// asynchronous writers start (see WriteThread.SyncWriterJoined: an asynchronous WAL write started before the first
/// synchronous writer ever joined is finished on the pool).
/// </para>
/// </summary>
[TestClass]
[DoNotParallelize]
public sealed class Audit12SyncAsyncWriteTests
{
    // AUDIT12_DEADLINE_SECONDS shortens it (e.g. to check that the tests fail without the fix).
    private static readonly TimeSpan Deadline = TimeSpan.FromSeconds(
        Environment.GetEnvironmentVariable("AUDIT12_DEADLINE_SECONDS") is { } d ? int.Parse(d, System.Globalization.CultureInfo.InvariantCulture) : 30);

    [TestCleanup]
    public void Cleanup() => TestHooks.ClearAll();

    /// <summary>The write modes of DBImpl::WriteImpl.</summary>
    public enum WriteMode
    {
        Default,
        SerialInserts,
        Pipelined,
        Unordered,
        TwoWriteQueues,
    }

    private static DbOptions ModeOptions(IFileSystem fs, WriteMode mode)
    {
        DbOptions o = NewOptions(fs);
        o.EnablePipelinedWrite = mode == WriteMode.Pipelined;
        o.UnorderedWrite = mode == WriteMode.Unordered;
        o.TwoWriteQueues = mode == WriteMode.TwoWriteQueues;
        o.AllowConcurrentMemtableWrite = mode != WriteMode.SerialInserts;
        return o;
    }

    // ---------------------------------------------------------------------------------------------------------
    // Helpers

    /// <summary>
    /// Forwards to a <see cref="MemoryFileSystem"/>; every asynchronous member completes on the .NET thread pool
    /// (Task.Yield) before it does its work, so an asynchronous WAL write always needs a pool thread to finish.
    /// </summary>
    private sealed class PoolCompletingFileSystem(MemoryFileSystem inner) : IFileSystem
    {
        private long _asyncCalls;

        public MemoryFileSystem Inner { get; } = inner;

        /// <summary>Calls of asynchronous file members so far.</summary>
        public long AsyncCalls => Interlocked.Read(ref _asyncCalls);

        private async ValueTask BeforeAsync(CancellationToken ct)
        {
            Interlocked.Increment(ref _asyncCalls);
            await Task.Yield();
            ct.ThrowIfCancellationRequested();
        }

        public IWritableFile CreateWritable(string path, bool truncate = true) => new W(this, Inner.CreateWritable(path, truncate));
        public IWritableFile CreateWritable(string path, bool truncate, WritableFileOptions options) => new W(this, Inner.CreateWritable(path, truncate, options));
        public IWritableFile ReopenWritable(string path, WritableFileOptions options) => new W(this, Inner.ReopenWritable(path, options));
        public IRandomAccessFile OpenRandomAccess(string path) => new R(this, Inner.OpenRandomAccess(path));
        public IRandomAccessFile OpenRandomAccess(string path, RandomAccessFileOptions options) => new R(this, Inner.OpenRandomAccess(path, options));
        public ISequentialFile OpenSequential(string path) => new S(this, Inner.OpenSequential(path));
        public ISequentialFile OpenSequential(string path, SequentialFileOptions options) => new S(this, Inner.OpenSequential(path, options));
        public bool FileExists(string path) => Inner.FileExists(path);
        public bool DirectoryExists(string path) => Inner.DirectoryExists(path);
        public long GetFileSize(string path) => Inner.GetFileSize(path);
        public void DeleteFile(string path) => Inner.DeleteFile(path);
        public void RenameFile(string source, string target) => Inner.RenameFile(source, target);
        public void CreateDirectoryIfMissing(string path) => Inner.CreateDirectoryIfMissing(path);
        public void DeleteDirectory(string path) => Inner.DeleteDirectory(path);
        public IEnumerable<string> GetChildren(string directory) => Inner.GetChildren(directory);
        public IDisposable LockFile(string path) => Inner.LockFile(path);
        public void SyncDirectory(string path) => Inner.SyncDirectory(path);
        public void LinkFile(string source, string target) => Inner.LinkFile(source, target);
        public DateTimeOffset GetFileModificationTime(string path) => Inner.GetFileModificationTime(path);
        public long GetFreeSpace(string path) => Inner.GetFreeSpace(path);

        private sealed class W(PoolCompletingFileSystem fs, IWritableFile inner) : IWritableFile
        {
            public long Size => inner.Size;
            public void Append(ReadOnlySpan<byte> data) => inner.Append(data);
            public void Flush() => inner.Flush();
            public void Sync() => inner.Sync();
            public void Dispose() => inner.Dispose();

            public async ValueTask AppendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
            {
                await fs.BeforeAsync(cancellationToken);
                inner.Append(data.Span);
            }

            public async ValueTask FlushAsync(CancellationToken cancellationToken = default)
            {
                await fs.BeforeAsync(cancellationToken);
                inner.Flush();
            }

            public async ValueTask SyncAsync(CancellationToken cancellationToken = default)
            {
                await fs.BeforeAsync(cancellationToken);
                inner.Sync();
            }

            public async ValueTask DisposeAsync()
            {
                await fs.BeforeAsync(CancellationToken.None);
                inner.Dispose();
            }
        }

        private sealed class R(PoolCompletingFileSystem fs, IRandomAccessFile inner) : IRandomAccessFile
        {
            public long Length => inner.Length;
            public int Read(long offset, Span<byte> destination) => inner.Read(offset, destination);

            public async ValueTask<int> ReadAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken = default)
            {
                await fs.BeforeAsync(cancellationToken);
                return inner.Read(offset, destination.Span);
            }

            public void Dispose() => inner.Dispose();
        }

        private sealed class S(PoolCompletingFileSystem fs, ISequentialFile inner) : ISequentialFile
        {
            public int Read(Span<byte> destination) => inner.Read(destination);
            public void Skip(long bytes) => inner.Skip(bytes);

            public async ValueTask<int> ReadAsync(Memory<byte> destination, CancellationToken cancellationToken = default)
            {
                await fs.BeforeAsync(cancellationToken);
                return inner.Read(destination.Span);
            }

            public void Dispose() => inner.Dispose();
        }
    }

    /// <summary>
    /// Caps the .NET thread pool at the processor count and, with <c>block</c>, blocks every worker until disposed: no
    /// thread-pool work item runs meanwhile (the pool cannot inject threads above the cap).
    /// </summary>
    private sealed class DeadThreadPool : IDisposable
    {
        private readonly ManualResetEventSlim _gate = new(false);
        private readonly int _maxWorkers, _maxIo, _minWorkers, _minIo;

        public DeadThreadPool(bool block)
        {
            ThreadPool.GetMaxThreads(out _maxWorkers, out _maxIo);
            ThreadPool.GetMinThreads(out _minWorkers, out _minIo);
            int cap = Environment.ProcessorCount;
            Assert.IsTrue(ThreadPool.SetMinThreads(Math.Min(_minWorkers, cap), _minIo));
            Assert.IsTrue(ThreadPool.SetMaxThreads(cap, _maxIo), "SetMaxThreads");
            if (!block) return;
            for (int i = 0; i < cap; i++) ThreadPool.UnsafeQueueUserWorkItem(static g => ((ManualResetEventSlim)g!).Wait(), _gate);
            // Saturated once a probe work item no longer runs (other work of the process may hold some workers).
            var probe = new ManualResetEventSlim(false); // not disposed: the probe runs once the pool is released
            ThreadPool.UnsafeQueueUserWorkItem(static p => ((ManualResetEventSlim)p!).Set(), probe);
            Assert.IsFalse(probe.Wait(500), "the thread pool still runs work items");
        }

        public void Dispose()
        {
            _gate.Set();
            ThreadPool.SetMaxThreads(_maxWorkers, _maxIo);
            ThreadPool.SetMinThreads(_minWorkers, _minIo);
        }
    }

    /// <summary>Dedicated (non-pool) threads whose failures are collected.</summary>
    private sealed class Threads
    {
        private readonly List<Thread> _threads = new();
        public readonly ConcurrentQueue<Exception> Errors = new();

        public void Start(string name, Action body)
        {
            var t = new Thread(() =>
            {
                try { body(); }
                catch (Exception e) { Errors.Enqueue(e); }
            }) { IsBackground = true, Name = name };
            t.Start();
            _threads.Add(t);
        }

        /// <summary>Fails the test if a thread did not finish by the deadline (a hang) or failed.</summary>
        public void JoinOrFail(string what, Func<string>? diagnostics = null)
        {
            DateTime end = DateTime.UtcNow + Deadline;
            foreach (Thread t in _threads)
            {
                TimeSpan left = end - DateTime.UtcNow;
                if (left < TimeSpan.Zero || !t.Join(left))
                    Assert.Fail(what + ": thread " + t.Name + " made no progress within " + Deadline + (diagnostics is null ? "" : " (" + diagnostics() + ")"));
            }
            if (Errors.TryPeek(out Exception? e)) throw new AssertFailedException(what + ": " + e, e);
        }
    }

    private static void WaitOrFail(Task t, string what)
    {
        if (!((IAsyncResult)t).AsyncWaitHandle.WaitOne(Deadline)) Assert.Fail(what + " made no progress within " + Deadline);
        t.GetAwaiter().GetResult();
    }

    private static string Dump(RocksDatabase db)
    {
        var sb = new StringBuilder();
        using Iterator it = db.NewIterator();
        for (it.SeekToFirst(); it.Valid; it.Next()) sb.Append(Encoding.UTF8.GetString(it.Key)).Append('=').Append(Encoding.UTF8.GetString(it.Value)).Append(';');
        return sb.ToString();
    }

    /// <summary>The write buffer manager of the running test, for <see cref="Diagnostics"/>.</summary>
    private static WriteBufferManager? s_diagnosticWbm;

    private static object? Field(object o, string name) =>
        o.GetType().GetField(name, System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public)?.GetValue(o);

    /// <summary>The main write queue (head writers and their wait state), for a test that made no progress.</summary>
    private static string WriteQueueState(RocksDatabase db)
    {
        var sb = new StringBuilder();
        object impl = Field(db, "_db")!;
        var wt = (WriteThread)Field(impl, "_writeThread")!;
        sb.Append("leaderActive ").Append(Field(wt, "_leaderActive")).Append(" memLeaderActive ").Append(Field(wt, "_memLeaderActive"));
        var w = (WriteThread.Writer?)Field(wt, "_head");
        for (int i = 0; w is not null && i < 4; i++, w = w.Next)
        {
            object? c = Field(w, "_continuation");
            sb.Append(" | q").Append(i).Append(w.IsSyncWaiter ? " sync " + w.SyncState : " async cont=" + (c is null ? "none" : c.GetType().Name + ((Delegate)c).Method.Name))
              .Append(w.IsUnbatched ? " unbatched" : "");
        }
        var group = (List<WriteThread.Writer>)Field(impl, "_group")!;
        sb.Append(" | group ").Append(group.Count);
        if (group.Count > 0)
        {
            WriteThread.Writer l = group[0];
            object? c = Field(l, "_continuation");
            sb.Append(" leader ").Append(l.IsSyncWaiter ? "sync " + l.SyncState : "async cont=" + (c is null ? "none" : ((Delegate)c).Method.Name) + " work=" + (Field(l, "_work") is null ? "none" : "queued"))
              .Append(l.IsUnbatched ? " unbatched" : "").Append(" error=").Append(l.Error?.GetType().Name);
        }
        // By reflection: the tests also build (and must fail, not break the build) against the sources before audit #12.
        if (typeof(BackgroundThreadPool).GetField("WriteWakeup")?.GetValue(null) is { } pool)
        {
            sb.Append(" | wake-up threads ").Append(Field(pool, "_threads")).Append(" queue ").Append(((System.Collections.ICollection)Field(pool, "_queue")!).Count)
              .Append(" idle ").Append(Field(pool, "_idle"));
        }
        var m = (WriteThread.Writer?)Field(wt, "_memHead");
        if (m is not null) sb.Append(" | memhead ").Append(m.IsSyncWaiter ? "sync" : "async").Append(m.IsUnbatched ? " marker" : "");
        return sb.ToString();
    }

    private static string Diagnostics(RocksDatabase db)
    {
        string? P(string name)
        {
            try { return db.GetProperty(name); }
            catch (Exception e) { return e.GetType().Name; }
        }
        return WriteQueueState(db) + "; seq " + db.GetLatestSequenceNumber() + ", L0 " + P(DbProperties.NumFilesAtLevelPrefix + "0") + ", compactions " + P(DbProperties.NumRunningCompactions) +
               "/" + P(DbProperties.CompactionPending) + ", stopped " + P(DbProperties.IsWriteStopped) + ", delayed " + P(DbProperties.ActualDelayedWriteRate) +
               ", bg errors " + P(DbProperties.BackgroundErrors) + ", " +
               "imm " + P(DbProperties.NumImmutableMemTable) + ", flush pending " + P(DbProperties.MemTableFlushPending) + ", running flushes " +
               P(DbProperties.NumRunningFlushes) + ", memtables " + P(DbProperties.CurSizeAllMemTables) + ", wbm " +
               (s_diagnosticWbm is { } m ? m.MemoryUsage + "/" + m.BufferSize + " mutable " + m.MutableMemtableMemoryUsage : "-");
    }

    private static string V(string key, int size) => key + ":" + new string((char)('a' + (key.Length + size) % 26), size);

    /// <summary>
    /// The core scenario: with every thread-pool worker blocked, <paramref name="syncThreads"/> dedicated threads run
    /// <paramref name="syncOps"/> synchronous writes each, while one dedicated thread starts <paramref name="asyncOps"/>
    /// asynchronous writes (without waiting for them: their completions need the pool). The synchronous writers must
    /// finish while the pool is dead; then the pool is released and the asynchronous writes complete.
    /// </summary>
    private static void RunWithDeadPool(RocksDatabase db, int syncThreads, int syncOps, Action<int, int> syncWrite, int asyncOps,
        Func<int, ValueTask> asyncWrite, string what)
    {
        var pending = new ConcurrentQueue<Task>();
        var threads = new Threads();
        using (new DeadThreadPool(block: true))
        {
            threads.Start("async-writes", () =>
            {
                for (int i = 0; i < asyncOps; i++)
                {
                    ValueTask t = asyncWrite(i);
                    pending.Enqueue(t.IsCompletedSuccessfully ? Task.CompletedTask : t.AsTask());
                }
            });
            for (int t = 0; t < syncThreads; t++)
            {
                int id = t;
                threads.Start("sync-writes-" + id, () =>
                {
                    for (int i = 0; i < syncOps; i++) syncWrite(id, i);
                });
            }
            threads.JoinOrFail(what + " with the thread pool dead", () => Diagnostics(db));
        }
        WaitOrFail(Task.WhenAll(pending), what + ": the asynchronous writes once the thread pool was released");
    }

    // ---------------------------------------------------------------------------------------------------------
    // 1. Every write mode: WAL I/O of asynchronous leaders, hand-offs, memtable stage / parallel / unordered inserts.

    /// <summary>
    /// Pins the write path's waiting points (1) and (2) in every write mode: asynchronous group leaders whose WAL writes
    /// complete on the thread pool (and synced writes, large batches, memtable and WAL switches from a small write buffer
    /// and max_total_wal_size), leadership handed to asynchronous writers, the pipelined memtable stage, parallel and
    /// unordered memtable inserts and the main queue of two_write_queues. Synchronous writers progress with the thread
    /// pool dead; every write is in the DB, also after a reopen.
    /// </summary>
    [TestMethod]
    [DataRow(WriteMode.Default)]
    [DataRow(WriteMode.SerialInserts)]
    [DataRow(WriteMode.Pipelined)]
    [DataRow(WriteMode.Unordered)]
    [DataRow(WriteMode.TwoWriteQueues)]
    [Timeout(120_000, CooperativeCancellation = true)]
    public void SyncWritersProgressBehindAsyncWritersWithThePoolDead(WriteMode mode)
    {
        var fs = new PoolCompletingFileSystem(NewFs());
        DbOptions o = ModeOptions(fs, mode);
        o.MaxTotalWalSize = 1 << 20;
        var cfo = new ColumnFamilyOptions { WriteBufferSize = 256 << 10 };
        var expected = new ConcurrentDictionary<string, string>();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o, cfo))
        {
            db.Put(B("first"), B("sync"));
            expected["first"] = "sync";
            RunWithDeadPool(db, syncThreads: 4, syncOps: 300, (t, i) =>
            {
                string k = "s" + t + "-" + i;
                string v = V(k, i % 40 == 0 ? 150_000 : 1 + i % 200);
                db.Put(new WriteOptions { Sync = i % 7 == 0 }, B(k), B(v));
                expected[k] = v;
            }, asyncOps: 1200, i =>
            {
                string k = "a-" + i;
                string v = V(k, i % 97 == 0 ? 150_000 : 1 + i % 300);
                expected[k] = v;
                return db.PutAsync(new WriteOptions { Sync = i % 5 == 0 }, B(k), B(v));
            }, mode + " writes");
            foreach ((string k, string v) in expected) Assert.AreEqual(v, GetString(db, k), k);
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, ModeOptions(fs, mode), cfo))
        {
            foreach ((string k, string v) in expected) Assert.AreEqual(v, GetString(db, k), "after reopen: " + k);
        }
    }

    /// <summary>
    /// The audit #8 repro in every write mode: synchronous writers on all the threads of a capped thread pool (no
    /// thread injection) and asynchronous writers whose WAL I/O completes on the pool. All make progress: the synchronous
    /// writers never wait for a pool thread, so they finish and free the pool for the asynchronous ones.
    /// </summary>
    [TestMethod]
    [DataRow(WriteMode.Default)]
    [DataRow(WriteMode.SerialInserts)]
    [DataRow(WriteMode.Pipelined)]
    [DataRow(WriteMode.Unordered)]
    [DataRow(WriteMode.TwoWriteQueues)]
    [Timeout(120_000, CooperativeCancellation = true)]
    public void SyncWritersOnACappedPoolBehindAsyncWritersProgress(WriteMode mode)
    {
        var fs = new PoolCompletingFileSystem(NewFs());
        using RocksDatabase db = RocksDatabase.Open(DbPath, ModeOptions(fs, mode), new ColumnFamilyOptions { WriteBufferSize = 256 << 10 });
        db.Put(B("first"), B("sync"));
        int n = Environment.ProcessorCount;
        Task all;
        using (new DeadThreadPool(block: false))
        {
            var tasks = new List<Task>();
            for (int t = 0; t < n; t++)
            {
                int id = t;
                tasks.Add(Task.Factory.StartNew(() =>
                {
                    for (int i = 0; i < 1500; i++) db.Put(new WriteOptions { Sync = i % 9 == 0 }, Key(id * 100_000 + i), B("s"));
                }, CancellationToken.None, TaskCreationOptions.None, TaskScheduler.Default));
            }
            for (int t = 0; t < 2; t++)
            {
                int id = t;
                var done = new TaskCompletionSource();
                var writer = new Thread(() =>
                {
                    try
                    {
                        // Bursts of concurrent asynchronous writes: many asynchronous writers queue and lead.
                        for (int burst = 0; burst < 60; burst++)
                        {
                            var batch = new Task[16];
                            for (int i = 0; i < batch.Length; i++) batch[i] = db.PutAsync(Key(900_000 + id * 10_000 + burst * 16 + i), B("a")).AsTask();
                            Task.WaitAll(batch);
                        }
                        done.SetResult();
                    }
                    catch (Exception e)
                    {
                        done.SetException(e);
                    }
                }) { IsBackground = true };
                writer.Start();
                tasks.Add(done.Task);
            }
            all = Task.WhenAll(tasks);
            if (!all.Wait(Deadline)) Assert.Fail(mode + ": sync writers on pool threads and async writers made no progress within " + Deadline);
        }
        all.GetAwaiter().GetResult();
        for (int t = 0; t < n; t++) Assert.AreEqual("s", GetString(db, "key" + (t * 100_000 + 1499).ToString("D6", System.Globalization.CultureInfo.InvariantCulture)));
    }

    // ---------------------------------------------------------------------------------------------------------
    // 2. Stall waits: delayed and stopped writes (DelayWrite), write buffer manager stalls.

    /// <summary>
    /// Pins waiting point (1) for stall waits: asynchronous leaders delayed by the write controller (L0 slowdown, delayed
    /// write rate) and stopped (L0 stop trigger, cleared by compactions on the LOW threads) wait synchronously once the
    /// DB has synchronous writers, so the synchronous writers behind them progress with the thread pool dead.
    /// </summary>
    [TestMethod]
    [DataRow(WriteMode.Default)]
    [DataRow(WriteMode.Pipelined)]
    [DataRow(WriteMode.Unordered)]
    [DataRow(WriteMode.TwoWriteQueues)]
    [Timeout(120_000, CooperativeCancellation = true)]
    public void SyncWritersProgressThroughDelayedAndStoppedWritesWithThePoolDead(WriteMode mode)
    {
        var fs = new PoolCompletingFileSystem(NewFs());
        DbOptions o = ModeOptions(fs, mode);
        o.DelayedWriteRate = 8 << 20;
        var cfo = new ColumnFamilyOptions
        {
            WriteBufferSize = 64 << 10,
            Level0FileNumCompactionTrigger = 2,
            Level0SlowdownWritesTrigger = 2,
            Level0StopWritesTrigger = 4,
        };
        var expected = new ConcurrentDictionary<string, string>();
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, cfo);
        db.Put(B("first"), B("sync"));
        RunWithDeadPool(db, syncThreads: 3, syncOps: 250, (t, i) =>
        {
            string k = "s" + t + "-" + i;
            string v = V(k, 300 + i % 500);
            db.Put(B(k), B(v));
            expected[k] = v;
        }, asyncOps: 800, i =>
        {
            string k = "a-" + i;
            string v = V(k, 300 + i % 700);
            expected[k] = v;
            return db.PutAsync(B(k), B(v));
        }, mode + " stalled writes");
        foreach ((string k, string v) in expected) Assert.AreEqual(v, GetString(db, k), k);
    }

    /// <summary>
    /// Pins the write buffer manager waiting points: stalls (allow_stall) of asynchronous leaders, the flush that a
    /// leader triggers for the manager, and with FlushLargestAcrossDbs the cross-DB flush coordinator (now started on a
    /// dedicated thread instead of Task.Run) whose asynchronous unbatched flush synchronous writers wait for.
    /// </summary>
    [TestMethod]
    [DataRow(WriteBufferFlushPolicy.FlushOldest, true)]
    [DataRow(WriteBufferFlushPolicy.FlushLargestAcrossDbs, true)]
    [DataRow(WriteBufferFlushPolicy.FlushOldest, false)]
    [DataRow(WriteBufferFlushPolicy.FlushLargestAcrossDbs, false)]
    [Timeout(120_000, CooperativeCancellation = true)]
    public void SyncWritersProgressThroughWriteBufferManagerStallsWithThePoolDead(WriteBufferFlushPolicy policy, bool asyncWriters)
    {
        var fs = new PoolCompletingFileSystem(NewFs());
        DbOptions o = NewOptions(fs);
        o.WriteBufferManager = s_diagnosticWbm = new WriteBufferManager(1 << 20, null, allowStall: true, policy);
        var expected = new ConcurrentDictionary<string, string>();
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, new ColumnFamilyOptions { WriteBufferSize = 4 << 20, ArenaBlockSize = 16 << 10 });
        db.Put(B("first"), B("sync"));
        RunWithDeadPool(db, syncThreads: 3, syncOps: 300, (t, i) =>
        {
            string k = "s" + t + "-" + i;
            string v = V(k, 500 + i % 700);
            db.Put(B(k), B(v));
            expected[k] = v;
        }, asyncOps: asyncWriters ? 900 : 0, i =>
        {
            string k = "a-" + i;
            string v = V(k, 500 + i % 900);
            expected[k] = v;
            return db.PutAsync(B(k), B(v));
        }, policy + " write buffer manager");
        foreach ((string k, string v) in expected) Assert.AreEqual(v, GetString(db, k), k);
    }

    // ---------------------------------------------------------------------------------------------------------
    // 3. Asynchronous unbatched operations, manual_wal_flush, FlushWAL / SyncWAL, LockWAL, the WAL-only queue.

    /// <summary>
    /// Pins waiting point (3) for asynchronous operations that lead the write queue (unbatched writers) or hold the WAL
    /// I/O lock or the MANIFEST writer slot: FlushAsync (memtable switch), FlushWalAsync / SyncWalAsync with
    /// manual_wal_flush, CreateColumnFamilyAsync / DropColumnFamilyAsync, started while synchronous writers write, call
    /// FlushWal, Flush and LockWal. The synchronous calls progress with the thread pool dead.
    /// </summary>
    [TestMethod]
    [DataRow(WriteMode.Default)]
    [DataRow(WriteMode.Pipelined)]
    [DataRow(WriteMode.Unordered)]
    [DataRow(WriteMode.TwoWriteQueues)]
    [Timeout(120_000, CooperativeCancellation = true)]
    public void SyncCallsProgressBesideAsyncControlOperationsWithThePoolDead(WriteMode mode)
    {
        var fs = new PoolCompletingFileSystem(NewFs());
        DbOptions o = ModeOptions(fs, mode);
        o.ManualWalFlush = true;
        var cfo = new ColumnFamilyOptions { WriteBufferSize = 128 << 10 };
        var expected = new ConcurrentDictionary<string, string>();
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, cfo);
        db.Put(B("first"), B("sync"));
        var created = new ConcurrentQueue<Task<ColumnFamily>>();
        RunWithDeadPool(db, syncThreads: 3, syncOps: 200, (t, i) =>
        {
            string k = "s" + t + "-" + i;
            string v = V(k, 1 + i % 300);
            db.Put(B(k), B(v));
            expected[k] = v;
            switch (i % 50)
            {
                case 10: db.FlushWal(sync: i % 20 == 0); break;
                case 20: db.Flush(); break;
                case 30:
                    using (WalLock l = db.LockWal()) db.FlushWal(sync: true);
                    break;
            }
        }, asyncOps: 600, i =>
        {
            string k = "a-" + i;
            string v = V(k, 1 + i % 300);
            expected[k] = v;
            ValueTask put = db.PutAsync(B(k), B(v));
            switch (i % 60)
            {
                case 7: _ = db.FlushAsync().AsTask(); break;
                case 17: _ = db.FlushWalAsync(sync: true).AsTask(); break;
                case 27: _ = db.SyncWalAsync().AsTask(); break;
                case 37: created.Enqueue(db.CreateColumnFamilyAsync("cf" + i, cfo).AsTask()); break;
            }
            return put;
        }, mode + " control operations");
        foreach (Task<ColumnFamily> c in created)
        {
            WaitOrFail(c, "CreateColumnFamilyAsync");
            WaitOrFail(db.DropColumnFamilyAsync(c.Result).AsTask(), "DropColumnFamilyAsync");
        }
        db.FlushWal(sync: true);
        foreach ((string k, string v) in expected) Assert.AreEqual(v, GetString(db, k), k);
    }

    /// <summary>
    /// Pins the WAL-only queue of two_write_queues (DBImpl::WriteImplWALOnly; the 2PC prepares of WriteCommitted
    /// transactions) and the WAL write lock shared by both queues: asynchronous prepares lead WAL-only groups while
    /// synchronous transactions prepare and commit; the synchronous ones progress with the thread pool dead.
    /// </summary>
    [TestMethod]
    [Timeout(120_000, CooperativeCancellation = true)]
    public void SyncTransactionsProgressBesideAsyncPreparesOnTwoWriteQueuesWithThePoolDead()
    {
        var fs = new PoolCompletingFileSystem(NewFs());
        DbOptions o = ModeOptions(fs, WriteMode.TwoWriteQueues);
        using TransactionDatabase tdb = TransactionDatabase.Open(DbPath, o, new TransactionDbOptions { WritePolicy = TransactionWritePolicy.WriteCommitted });
        tdb.Database.Put(B("first"), B("sync"));
        var prepared = new ConcurrentQueue<Transaction>();
        RunWithDeadPool(tdb.Database, syncThreads: 3, syncOps: 150, (t, i) =>
        {
            using Transaction txn = tdb.BeginTransaction(new WriteOptions { Sync = i % 10 == 0 });
            txn.SetName("s" + t + "-" + i);
            txn.Put(B("s" + t + "-" + i), B("v" + i));
            txn.Prepare();
            txn.Commit();
        }, asyncOps: 400, i =>
        {
            Transaction txn = tdb.BeginTransaction(new WriteOptions { Sync = i % 8 == 0 });
            txn.SetName("a-" + i);
            txn.Put(B("a-" + i), B("v" + i));
            prepared.Enqueue(txn);
            return txn.PrepareAsync();
        }, "two write queues");
        foreach (Transaction txn in prepared)
        {
            WaitOrFail(txn.CommitAsync().AsTask(), "CommitAsync");
            txn.Dispose();
        }
        for (int i = 0; i < 400; i++) Assert.AreEqual("v" + i, GetString(tdb.Database, "a-" + i));
        for (int t = 0; t < 3; t++)
        {
            for (int i = 0; i < 150; i++) Assert.AreEqual("v" + i, GetString(tdb.Database, "s" + t + "-" + i));
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // 4. Semantics: the same results as all-sync, cancellation, callers never resumed on a write wake-up thread.

    /// <summary>One operation of a generated program (a write of a logical writer).</summary>
    private readonly record struct Op(int Kind, string Key, string Value, bool Sync, bool AsyncCall);

    /// <summary>
    /// A seeded random program: <paramref name="writers"/> logical writers, each with its own keys, doing puts, deletes,
    /// merges, multi-entry batches, synced writes, large values, flushes and WAL flushes; every operation randomly through
    /// the synchronous or the asynchronous API.
    /// </summary>
    private static List<Op>[] GenerateProgram(int seed, int writers, int opsPerWriter)
    {
        var rnd = new Random(seed);
        var program = new List<Op>[writers];
        for (int w = 0; w < writers; w++)
        {
            var ops = new List<Op>(opsPerWriter);
            for (int i = 0; i < opsPerWriter; i++)
            {
                int kind = rnd.Next(100) switch { < 55 => 0, < 65 => 1, < 75 => 2, < 90 => 3, < 93 => 4, < 96 => 5, _ => 6 };
                string key = "w" + w + "-" + rnd.Next(60);
                int size = rnd.Next(20) == 0 ? 100_000 + rnd.Next(100_000) : 1 + rnd.Next(400);
                ops.Add(new Op(kind, key, V(key, size), Sync: rnd.Next(6) == 0, AsyncCall: rnd.Next(2) == 0));
            }
            program[w] = ops;
        }
        return program;
    }

    private static async Task RunOpAsync(RocksDatabase db, Op op, bool allSync)
    {
        bool async = op.AsyncCall && !allSync;
        var wo = new WriteOptions { Sync = op.Sync };
        switch (op.Kind)
        {
            case 0:
                if (async) await db.PutAsync(wo, B(op.Key), B(op.Value));
                else db.Put(wo, B(op.Key), B(op.Value));
                break;
            case 1:
                if (async) await db.DeleteAsync(wo, B(op.Key));
                else db.Delete(wo, B(op.Key));
                break;
            case 2:
                if (async) await db.MergeAsync(wo, B(op.Key + "-m"), B(op.Value[..Math.Min(8, op.Value.Length)]));
                else db.Merge(wo, B(op.Key + "-m"), B(op.Value[..Math.Min(8, op.Value.Length)]));
                break;
            case 3:
            {
                using var batch = new WriteBatch();
                batch.Put(B(op.Key), B(op.Value)).Put(B(op.Key + "-b"), B(op.Value[..Math.Min(30, op.Value.Length)])).Delete(B(op.Key + "-gone"));
                if (async) await db.WriteAsync(wo, batch);
                else db.Write(wo, batch);
                break;
            }
            case 4:
                if (async) await db.FlushAsync();
                else db.Flush();
                break;
            case 5:
                if (async) await db.FlushWalAsync(sync: op.Sync);
                else db.FlushWal(sync: op.Sync);
                break;
            default:
                if (async) await db.PutAsync(B(op.Key), B(op.Value));
                else db.Put(B(op.Key), B(op.Value));
                break;
        }
    }

    private static DbOptions StressOptions(IFileSystem fs, WriteMode mode, bool stalls)
    {
        DbOptions o = ModeOptions(fs, mode);
        if (stalls) o.DelayedWriteRate = 16 << 20;
        o.MaxTotalWalSize = 2 << 20;
        return o;
    }

    private static ColumnFamilyOptions StressCfOptions(bool stalls) => new()
    {
        WriteBufferSize = stalls ? 64UL << 10 : 512UL << 10,
        Level0FileNumCompactionTrigger = 2,
        Level0SlowdownWritesTrigger = stalls ? 3 : 20,
        Level0StopWritesTrigger = stalls ? 6 : 36,
        MergeOperator = new RocksDb.Managed.Tests.Db.StringAppendMergeOperator(),
    };

    /// <summary>
    /// Runs the program concurrently, one task per logical writer on the thread pool (synchronous calls block pool
    /// threads), with the pool capped at the processor count (more writers than threads); returns the DB contents and
    /// its last sequence number.
    /// </summary>
    private static (string Contents, ulong LastSequence) RunProgram(List<Op>[] program, WriteMode mode, bool stalls, bool allSync, string what)
    {
        var fs = new PoolCompletingFileSystem(NewFs());
        using RocksDatabase db = RocksDatabase.Open(DbPath, StressOptions(fs, mode, stalls), StressCfOptions(stalls));
        db.Put(B("first"), B("sync"));
        Task all;
        using (new DeadThreadPool(block: false))
        {
            var tasks = new Task[program.Length];
            for (int w = 0; w < program.Length; w++)
            {
                List<Op> ops = program[w];
                tasks[w] = Task.Run(async () =>
                {
                    foreach (Op op in ops) await RunOpAsync(db, op, allSync);
                });
            }
            all = Task.WhenAll(tasks);
            if (!all.Wait(Deadline)) Assert.Fail(what + " made no progress within " + Deadline);
        }
        all.GetAwaiter().GetResult();
        return (Dump(db), db.GetLatestSequenceNumber());
    }

    /// <summary>
    /// Seeded randomized stress (seed logged; replay with AUDIT12_SEED): concurrent logical writers doing every kind of
    /// write, synced writes, large batches, flushes and WAL flushes, each call randomly synchronous or asynchronous, in
    /// every write mode, with and without write stalls, on a capped thread pool with more writers than pool threads. The
    /// mixed run makes progress and ends with the same DB contents and the same last sequence number as the same program
    /// run all-synchronously (each writer owns its keys, so the outcome does not depend on the interleaving).
    /// </summary>
    [TestMethod]
    [DataRow(WriteMode.Default, false)]
    [DataRow(WriteMode.Default, true)]
    [DataRow(WriteMode.SerialInserts, true)]
    [DataRow(WriteMode.Pipelined, false)]
    [DataRow(WriteMode.Pipelined, true)]
    [DataRow(WriteMode.Unordered, false)]
    [DataRow(WriteMode.Unordered, true)]
    [DataRow(WriteMode.TwoWriteQueues, true)]
    [Timeout(240_000, CooperativeCancellation = true)]
    public void RandomMixedProgramGivesTheAllSyncResult(WriteMode mode, bool stalls)
    {
        int seed = Environment.GetEnvironmentVariable("AUDIT12_SEED") is { } s ? int.Parse(s, System.Globalization.CultureInfo.InvariantCulture) : Environment.TickCount;
        string what = mode + (stalls ? " with stalls" : "") + ", seed " + seed;
        Console.WriteLine(what);
        int writers = Environment.ProcessorCount + 4;
        for (int round = 0; round < 2; round++)
        {
            List<Op>[] program = GenerateProgram(seed + round, writers, 150);
            (string mixedContents, ulong mixedSeq) = RunProgram(program, mode, stalls, allSync: false, what + " round " + round + " (mixed)");
            (string syncContents, ulong syncSeq) = RunProgram(program, mode, stalls, allSync: true, what + " round " + round + " (all sync)");
            Assert.AreEqual(syncSeq, mixedSeq, what + ": last sequence");
            Assert.AreEqual(syncContents, mixedContents, what + ": contents");
        }
    }

    /// <summary>
    /// A single writer alternating synchronous and asynchronous calls (seeded) in a DB that has synchronous writers:
    /// every write gets the sequence number it gets when the same program runs all-synchronously, and the WAL holds the
    /// same records in the same order (GetUpdatesSince), so the mixed mode changes no visible state.
    /// </summary>
    [TestMethod]
    [DataRow(WriteMode.Default)]
    [DataRow(WriteMode.SerialInserts)]
    [DataRow(WriteMode.Pipelined)]
    [DataRow(WriteMode.Unordered)]
    [DataRow(WriteMode.TwoWriteQueues)]
    [Timeout(120_000, CooperativeCancellation = true)]
    public async Task SequentialMixedProgramMatchesAllSyncSequencesAndWal(WriteMode mode)
    {
        int seed = Environment.GetEnvironmentVariable("AUDIT12_SEED") is { } s ? int.Parse(s, System.Globalization.CultureInfo.InvariantCulture) : Environment.TickCount;
        Console.WriteLine("seed " + seed);
        List<Op> program = GenerateProgram(seed, 1, 400)[0];
        async Task<(List<ulong> Seqs, List<string> Wal, string Contents)> Run(bool allSync)
        {
            var fs = new PoolCompletingFileSystem(NewFs());
            // One memtable and WAL for the whole program, so GetUpdatesSince sees every record.
            ColumnFamilyOptions cfo = StressCfOptions(stalls: false);
            cfo.WriteBufferSize = 256 << 20;
            using RocksDatabase db = RocksDatabase.Open(DbPath, ModeOptions(fs, mode), cfo);
            db.Put(B("first"), B("sync"));
            var seqs = new List<ulong>();
            foreach (Op op in program)
            {
                if (op.Kind is 4 or 5) continue; // flushes switch WALs at times the WAL comparison does not need
                await RunOpAsync(db, op, allSync);
                seqs.Add(db.GetLatestSequenceNumber());
            }
            var wal = new List<string>();
            using (TransactionLogIterator it = db.GetUpdatesSince(1))
            {
                for (; it.Valid; it.Next()) wal.Add(Convert.ToHexString(it.Current.Data.Span));
            }
            return (seqs, wal, Dump(db));
        }
        var mixed = await Run(allSync: false);
        var sync = await Run(allSync: true);
        CollectionAssert.AreEqual(sync.Seqs, mixed.Seqs, "sequence numbers, seed " + seed);
        CollectionAssert.AreEqual(sync.Wal, mixed.Wal, "WAL records, seed " + seed);
        Assert.AreEqual(sync.Contents, mixed.Contents, "contents, seed " + seed);
    }

    /// <summary>
    /// Cancellation semantics are unchanged in a DB with synchronous writers: an asynchronous write that leads its group
    /// (synchronously, on the calling thread) while writes are stopped (LockWAL's write stop) fails with
    /// OperationCanceledException when its token fires, and the asynchronous write queued behind it, which did not ask to
    /// be canceled, commits once the WAL is unlocked (CancelStalledLeader).
    /// </summary>
    [TestMethod]
    [Timeout(60_000, CooperativeCancellation = true)]
    public void AsyncLeaderOfAMixedDbIsCanceledAloneWhileStopped()
    {
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(NewFs()));
        db.Put(B("first"), B("sync"));
        WalLock l = db.LockWal();
        using var cts = new CancellationTokenSource();
        ValueTask canceled = default, follower = default;
        var reached = new ManualResetEventSlim();
        TestHooks.Set("DBImpl::DelayWrite:Wait", () => reached.Set());
        var t = new Thread(() =>
        {
            canceled = db.PutAsync(B("canceled"), B("x"), cts.Token);
        }) { IsBackground = true };
        t.Start();
        Assert.IsTrue(reached.Wait(Deadline), "the asynchronous leader did not reach the write stop");
        var t2 = new Thread(() => follower = db.PutAsync(B("follower"), B("y"))) { IsBackground = true };
        t2.Start();
        Assert.IsTrue(t2.Join(Deadline), "the follower's PutAsync call did not return");
        cts.Cancel();
        Assert.IsTrue(t.Join(Deadline), "the canceled leader's PutAsync call did not return");
        try
        {
            WaitOrFail(canceled.AsTask(), "the canceled write");
            Assert.Fail("the write was not canceled");
        }
        catch (OperationCanceledException)
        {
        }
        Assert.IsFalse(follower.IsCompleted, "the follower committed while writes are stopped");
        l.Dispose();
        WaitOrFail(follower.AsTask(), "the follower");
        Assert.IsNull(GetString(db, "canceled"));
        Assert.AreEqual("y", GetString(db, "follower"));
    }

    /// <summary>
    /// Asynchronous writers of a DB with synchronous writers that are woken to lead run on a write wake-up thread
    /// ("rocksdb:write"), but their callers' continuations never do: the write returns to the thread pool first. And no
    /// write wake-up thread is used by a DB without synchronous writers, whose asynchronous WAL writes stay asynchronous.
    /// </summary>
    [TestMethod]
    [DataRow(WriteMode.Default)]
    [DataRow(WriteMode.Pipelined)]
    [DataRow(WriteMode.Unordered)]
    [DataRow(WriteMode.TwoWriteQueues)]
    [Timeout(60_000, CooperativeCancellation = true)]
    public async Task CallersNeverResumeOnWriteWakeupThreads(WriteMode mode)
    {
        var fs = new PoolCompletingFileSystem(NewFs());
        using RocksDatabase db = RocksDatabase.Open(DbPath, ModeOptions(fs, mode));
        var leadersOnWakeupThreads = 0;
        TestHooks.Set("DBImpl::WriteImpl:UnorderedWriteAfterWriteWAL", () => { if (OnWakeupThread()) Interlocked.Increment(ref leadersOnWakeupThreads); });
        TestHooks.Set("DBImpl::PipelinedWriteImpl:MemTableWriterLeader", () => { if (OnWakeupThread()) Interlocked.Increment(ref leadersOnWakeupThreads); });

        // Without synchronous writers: asynchronous WAL I/O, no wake-up thread.
        await Task.WhenAll(Enumerable.Range(0, 8).Select(w => Task.Run(async () =>
        {
            for (int i = 0; i < 100; i++)
            {
                await db.PutAsync(B("pure" + w + "-" + i), B("v"));
                Assert.IsFalse(OnWakeupThread(), "a caller resumed on a write wake-up thread");
            }
        })));
        Assert.IsTrue(fs.AsyncCalls > 0, "the asynchronous writes did not use the asynchronous file members");
        Assert.AreEqual(0, leadersOnWakeupThreads, "a DB without synchronous writers used a write wake-up thread");

        // With synchronous writers.
        using var stop = new CancellationTokenSource();
        var syncThread = new Thread(() =>
        {
            for (int i = 0; !stop.IsCancellationRequested; i++) db.Put(B("sync-" + i), B("s"));
        }) { IsBackground = true };
        syncThread.Start();
        while (db.GetLatestSequenceNumber() < 900) Thread.Yield();
        long asyncCallsBefore = fs.AsyncCalls;
        await Task.WhenAll(Enumerable.Range(0, 8).Select(w => Task.Run(async () =>
        {
            for (int i = 0; i < 300; i++)
            {
                await db.PutAsync(new WriteOptions { Sync = i % 10 == 0 }, B("mixed" + w + "-" + i), B("v"));
                Assert.IsFalse(OnWakeupThread(), "a caller resumed on a write wake-up thread");
            }
        })));
        stop.Cancel();
        Assert.IsTrue(syncThread.Join(Deadline));
        // Once the DB has synchronous writers, leaders write the WAL synchronously.
        Assert.AreEqual(asyncCallsBefore, fs.AsyncCalls, "an asynchronous WAL write in a DB with synchronous writers");
    }

    private static bool OnWakeupThread() => Thread.CurrentThread.Name?.StartsWith("rocksdb:write", StringComparison.Ordinal) == true;
}
