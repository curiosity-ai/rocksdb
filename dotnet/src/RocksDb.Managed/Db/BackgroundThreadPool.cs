using System.Runtime.CompilerServices;

namespace RocksDb.Managed.Db;

/// <summary>
/// The threads flushes and compactions run on. Port of the idea of Env::Schedule over ThreadPoolImpl
/// (util/threadpool_imp.cc): one process-wide pool per Env::Priority (<see cref="High"/> for flushes, <see cref="Low"/>
/// for compactions, <see cref="Bottom"/> for bottom-priority compactions) whose threads live across jobs, so the
/// per-thread scratch of table building and compaction (compression hash tables, decompression buffers, comparator and
/// logging scratch, all <c>[ThreadStatic]</c>) is allocated once per thread instead of once per job, and CPU-bound
/// background work stays off the .NET thread pool.
/// <para>
/// Sizing: like Env::IncBackgroundThreadsIfNeeded, every DB raises the number of threads a pool keeps alive to its
/// background job limits (<see cref="IncreaseThreadsIfNeeded"/>; max_background_flushes / max_background_compactions,
/// derived from max_background_jobs, and bottom_priority_background_threads), and pools never shrink below that. The
/// DB itself limits how many jobs of each kind it runs at once (DBImpl's bg_*_scheduled_ counters), so a pool never
/// queues a job behind a busy thread: when no thread is idle it starts another one (which stops again after
/// <see cref="IdleTimeout"/> if the pool has more threads than it keeps). That keeps the DB's scheduling semantics
/// (a scheduled job starts immediately, a manual compaction or subcompaction never waits for a pool slot, so no job
/// can deadlock waiting for a job queued behind it) while the threads are reused.
/// </para>
/// <para>
/// Work items must not throw (the job wrappers record failures as background errors); an exception escaping a work item
/// is swallowed so that the thread survives. Threads are background threads, so they never keep the process alive, and
/// they hold no reference to a work item (or the DB it belongs to) once the item has run.
/// </para>
/// </summary>
internal sealed class BackgroundThreadPool
{
    /// <summary>Env::Priority::HIGH: flushes.</summary>
    public static readonly BackgroundThreadPool High = new("rocksdb:high");

    /// <summary>Env::Priority::LOW: compactions (automatic and manual) and subcompactions.</summary>
    public static readonly BackgroundThreadPool Low = new("rocksdb:low");

    /// <summary>Env::Priority::BOTTOM: compactions forwarded to the bottom-priority pool.</summary>
    public static readonly BackgroundThreadPool Bottom = new("rocksdb:bottom");

    /// <summary>How long a thread above the kept count waits for work before it exits.</summary>
    internal static readonly TimeSpan IdleTimeout = TimeSpan.FromSeconds(30);

    /// <summary>Upper bound for <see cref="IncreaseThreadsIfNeeded"/> (threads above it are still started on demand).</summary>
    private const int MaxKeptThreads = 256;

    private readonly string _name;
    private readonly object _lock = new();
    private readonly Queue<IThreadPoolWorkItem> _queue = new();   // guarded by _lock
    private int _threads;      // live threads, guarded by _lock
    private int _idle;         // threads waiting for work, guarded by _lock
    private int _keep;         // threads kept alive while idle, guarded by _lock
    private int _nextId;

    private BackgroundThreadPool(string name) => _name = name;

    /// <summary>Live threads of the pool (tests and diagnostics).</summary>
    internal int ThreadCount
    {
        get
        {
            lock (_lock) return _threads;
        }
    }

    /// <summary>Threads the pool keeps alive while idle.</summary>
    internal int KeptThreadCount
    {
        get
        {
            lock (_lock) return _keep;
        }
    }

    /// <summary>
    /// Port of Env::IncBackgroundThreadsIfNeeded: keeps at least <paramref name="count"/> threads alive once they were
    /// started (threads are started on demand, not here).
    /// </summary>
    public void IncreaseThreadsIfNeeded(int count)
    {
        count = Math.Min(count, MaxKeptThreads);
        lock (_lock)
        {
            if (count > _keep) _keep = count;
        }
    }

    /// <summary>
    /// Port of Env::Schedule: runs <paramref name="work"/> on an idle thread of the pool, or on a new thread when none is
    /// idle.
    /// </summary>
    public void Schedule(IThreadPoolWorkItem work)
    {
        ArgumentNullException.ThrowIfNull(work);
        lock (_lock)
        {
            _queue.Enqueue(work);
            // Every idle thread takes one item when it wakes: start a thread for the items no idle thread will take.
            if (_idle >= _queue.Count)
            {
                Monitor.Pulse(_lock);
                return;
            }
            _threads++;
        }
        try
        {
            var t = new Thread(static s => ((BackgroundThreadPool)s!).WorkerLoop())
            {
                IsBackground = true,
                Name = _name + ":" + Interlocked.Increment(ref _nextId).ToString(System.Globalization.CultureInfo.InvariantCulture),
            };
            t.UnsafeStart(this);
        }
        catch
        {
            // Out of threads: withdraw the item (unless a thread that became idle meanwhile took it) and fail the caller.
            bool withdrawn = false;
            lock (_lock)
            {
                _threads--;
                int n = _queue.Count;
                for (int i = 0; i < n; i++)
                {
                    IThreadPoolWorkItem w = _queue.Dequeue();
                    if (!withdrawn && ReferenceEquals(w, work)) withdrawn = true;
                    else _queue.Enqueue(w);
                }
            }
            if (withdrawn) throw;
        }
    }

    /// <summary>
    /// Runs <paramref name="action"/>(<paramref name="state"/>) on the pool and returns a task that completes with its
    /// outcome (the exception it threw, if any). For callers that wait for the job (manual compactions, subcompactions).
    /// </summary>
    public Task Run(Action<object?> action, object? state)
    {
        var item = new TaskWorkItem(action, state);
        Schedule(item);
        return item.Task;
    }

    /// <summary>
    /// Starts <paramref name="action"/>(<paramref name="state"/>) on the pool and returns a task that completes with the
    /// outcome of the <see cref="ValueTask"/> it returns. The job runs on the pool thread up to its first incomplete
    /// await; that thread is then free, and the job decides where it continues (<see cref="SwitchTo"/> returns to the
    /// pool). For manual compactions that wait for a compaction service without holding a thread.
    /// The returned task's continuations never run inline on the thread that completes it.
    /// </summary>
    public Task RunAsync(Func<object?, ValueTask> action, object? state)
    {
        var item = new AsyncTaskWorkItem(action, state);
        Schedule(item);
        return item.Task;
    }

    /// <summary>
    /// An awaitable that continues the awaiting method on a thread of this pool (always asynchronously). Used by a job
    /// started with <see cref="RunAsync"/> to bring its CPU-bound and blocking work back onto the pool after an
    /// asynchronous wait.
    /// </summary>
    public SwitchToAwaitable SwitchTo() => new(this);

    /// <summary>The awaitable of <see cref="SwitchTo"/>.</summary>
    internal readonly struct SwitchToAwaitable(BackgroundThreadPool pool) : ICriticalNotifyCompletion
    {
        public SwitchToAwaitable GetAwaiter() => this;
        public bool IsCompleted => false;
        public void GetResult() { }
        public void OnCompleted(Action continuation) => pool.Schedule(new ContinuationWorkItem(continuation));
        public void UnsafeOnCompleted(Action continuation) => pool.Schedule(new ContinuationWorkItem(continuation));
    }

    private sealed class ContinuationWorkItem(Action continuation) : IThreadPoolWorkItem
    {
        public void Execute() => continuation();
    }

    private void WorkerLoop()
    {
        while (true)
        {
            IThreadPoolWorkItem? work;
            lock (_lock)
            {
                while (!_queue.TryDequeue(out work))
                {
                    _idle++;
                    bool signaled = Monitor.Wait(_lock, IdleTimeout);
                    _idle--;
                    if (!signaled && _queue.Count == 0 && _threads > _keep)
                    {
                        _threads--;
                        return;
                    }
                }
            }
            try
            {
                work.Execute();
            }
            catch (Exception)
            {
                // The job wrappers record failures; keep the thread alive.
            }
            // Don't keep the job (and its DB) reachable from this thread's stack while it waits.
            work = null;
        }
    }

    /// <summary>A work item whose outcome is observed through <see cref="Task"/>.</summary>
    private sealed class TaskWorkItem(Action<object?> action, object? state) : IThreadPoolWorkItem
    {
        private readonly TaskCompletionSource _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task Task => _tcs.Task;

        public void Execute()
        {
            try
            {
                action(state);
                _tcs.TrySetResult();
            }
            catch (OperationCanceledException e)
            {
                // Like Task.Factory.StartNew without a token: a cancellation from inside the job faults the task (awaiting
                // it rethrows the same exception).
                _tcs.TrySetException(e);
            }
            catch (Exception e)
            {
                _tcs.TrySetException(e);
            }
        }
    }

    /// <summary>A work item running an async job whose outcome is observed through <see cref="Task"/>.</summary>
    private sealed class AsyncTaskWorkItem(Func<object?, ValueTask> action, object? state) : IThreadPoolWorkItem
    {
        private readonly TaskCompletionSource _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task Task => _tcs.Task;

        public void Execute()
        {
            ValueTask job;
            try
            {
                job = action(state);
            }
            catch (Exception e)
            {
                _tcs.TrySetException(e);
                return;
            }
            if (job.IsCompleted)
            {
                Complete(job);
                return;
            }
            ValueTaskAwaiter awaiter = job.GetAwaiter();
            awaiter.UnsafeOnCompleted(() => Complete(job));
        }

        private void Complete(ValueTask job)
        {
            try
            {
                job.GetAwaiter().GetResult();
                _tcs.TrySetResult();
            }
            catch (Exception e)
            {
                // A cancellation from inside the job faults the task, as for TaskWorkItem.
                _tcs.TrySetException(e);
            }
        }
    }
}
