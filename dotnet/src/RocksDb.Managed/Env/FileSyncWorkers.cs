using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading.Tasks.Sources;
using Microsoft.Win32.SafeHandles;

namespace RocksDb.Managed.Env;

/// <summary>
/// Dedicated threads that run fdatasync / FlushFileBuffers for async callers (<see cref="LocalWritableFile.SyncAsync"/>),
/// so that a synced write awaiting its sync does not block a thread-pool thread for the device flush (upstream's
/// PosixWritableFile::Sync blocks the calling thread; the port's async API should not).
/// <list type="bullet">
/// <item>Semantics are those of the synchronous call: the request is queued after the caller's data reached the OS
/// (its write completed), so the fdatasync started by a worker covers it; the returned task completes only after that
/// fdatasync returned, with its error if it failed. Requests are independent (one fdatasync each); ordering between
/// syncs is the caller's, exactly as with blocking calls (each caller awaits its sync before it proceeds).</item>
/// <item>Workers are created on demand, up to <see cref="MaxWorkers"/>, so syncs of different files (WALs, MANIFEST,
/// table files) still run in parallel like blocking calls from different threads; idle workers stay parked.</item>
/// <item>Request objects are pooled (<see cref="IValueTaskSource"/>): a sync allocates nothing in steady state.
/// Completions run asynchronously, so a waiter's continuation (user code) never runs on, delays or blocks a sync
/// worker.</item>
/// </list>
/// The hand-off costs two thread wake-ups per sync (worker, then the waiter's continuation on the thread pool); see
/// docs/perf.md for the measured latency and <see cref="LocalFileSystem.OffloadAsyncSync"/> to turn it off.
/// </summary>
internal static class FileSyncWorkers
{
    /// <summary>Upper bound of concurrently running syncs.</summary>
    internal static readonly int MaxWorkers = Math.Clamp(Environment.ProcessorCount, 2, 16);

    private static readonly ConcurrentQueue<Request> s_queue = new();
    private static readonly ConcurrentQueue<Request> s_pool = new();
    private static readonly SemaphoreSlim s_signal = new(0, int.MaxValue);
    private static int s_workers;
    private static int s_idle;
    private static int s_pending; // requests queued and not yet taken by a worker

    /// <summary>Number of started worker threads (test observability).</summary>
    internal static int WorkerCount => Volatile.Read(ref s_workers);

    /// <summary>Queues an fdatasync of <paramref name="handle"/>; the task completes when it finished.</summary>
    public static ValueTask DataSyncAsync(SafeFileHandle handle, string path)
    {
        if (!s_pool.TryDequeue(out Request? r)) r = new Request();
        r.Handle = handle;
        r.Path = path;
        ValueTask task = new(r, r.Version);
        // Counted before it is visible: a worker that dequeues it decrements after this increment.
        int pending = Interlocked.Increment(ref s_pending);
        s_queue.Enqueue(r);
        // Start a worker when the queued requests outnumber the idle workers; otherwise an idle one takes it. (Comparing
        // idle with 0 let two requests queued back to back both count on the same idle worker, so the second sync
        // waited for the whole of the first although more workers were allowed.) The counts are read without a lock:
        // at worst one worker more than needed is started, never one too few for a request that stays queued.
        if (pending > Volatile.Read(ref s_idle)) MaybeStartWorker();
        s_signal.Release();
        return task;
    }

    private static void MaybeStartWorker()
    {
        int n = Volatile.Read(ref s_workers);
        while (n < MaxWorkers)
        {
            int o = Interlocked.CompareExchange(ref s_workers, n + 1, n);
            if (o == n)
            {
                var t = new Thread(static () => WorkerLoop()) { IsBackground = true, Name = "rocksdb-fsync" };
                t.UnsafeStart();
                return;
            }
            n = o;
        }
    }

    private static void WorkerLoop()
    {
        while (true)
        {
            Interlocked.Increment(ref s_idle);
            Request? r = SpinForRequest();
            if (r is null)
            {
                s_signal.Wait();
                Interlocked.Decrement(ref s_idle);
                if (!s_queue.TryDequeue(out r)) continue;
                Interlocked.Decrement(ref s_pending);
            }
            else
            {
                Interlocked.Decrement(ref s_idle);
            }
            Exception? error = null;
            try
            {
                UnixNative.DataSync(r.Handle!, r.Path!);
            }
            catch (Exception e)
            {
                error = e;
            }
            r.Handle = null;
            r.Path = null;
            r.Complete(error);
        }
    }

    /// <summary>
    /// How long a worker polls the queue before it blocks: a synced writer queues its next sync soon after the previous
    /// one completed, and a blocked worker costs a wake-up (measured: fillsync --async, 1 writer, 268 -> 229 us/op).
    /// </summary>
    private static readonly long s_spinTicks = Stopwatch.Frequency / 20_000; // 50 us

    /// <summary>
    /// Polls the queue for a short while before the worker blocks: a request taken this way skips the semaphore wake-up
    /// (its permit stays and later only makes a blocked worker re-check the queue).
    /// </summary>
    private static Request? SpinForRequest()
    {
        long until = Stopwatch.GetTimestamp() + s_spinTicks;
        var spin = new SpinWait();
        do
        {
            if (s_queue.TryDequeue(out Request? r))
            {
                Interlocked.Decrement(ref s_pending);
                return r;
            }
            spin.SpinOnce(sleep1Threshold: -1);
        }
        while (Stopwatch.GetTimestamp() < until);
        return null;
    }

    private sealed class Request : IValueTaskSource
    {
        private ManualResetValueTaskSourceCore<bool> _core = new() { RunContinuationsAsynchronously = true };
        public SafeFileHandle? Handle;
        public string? Path;

        public short Version => _core.Version;

        public void Complete(Exception? error)
        {
            if (error is null) _core.SetResult(true);
            else _core.SetException(error);
        }

        public void GetResult(short token)
        {
            try
            {
                _core.GetResult(token);
            }
            finally
            {
                _core.Reset();
                s_pool.Enqueue(this);
            }
        }

        public ValueTaskSourceStatus GetStatus(short token) => _core.GetStatus(token);

        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
            _core.OnCompleted(continuation, state, token, flags);
    }
}
