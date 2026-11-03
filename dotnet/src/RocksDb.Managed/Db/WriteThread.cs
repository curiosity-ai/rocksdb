using System.Diagnostics;
using System.Threading.Tasks.Sources;

namespace RocksDb.Managed.Db;

/// <summary>
/// Leader/follower group commit for writes. Port of rocksdb::WriteThread (db/write_thread.cc) reshaped for async:
/// <list type="bullet">
/// <item>A writer joins the queue (<see cref="JoinBatchGroup"/>). If no leader is active it becomes the leader
/// immediately (STATE_GROUP_LEADER); otherwise it waits.</item>
/// <item>The leader takes compatible queued writers into its group (<see cref="EnterAsBatchGroupLeader"/>), writes
/// them, completes the followers (STATE_COMPLETED) and hands leadership to the next queued writer
/// (<see cref="ExitAsBatchGroupLeader"/>).</item>
/// </list>
/// Waiting is either asynchronous (an <see cref="IValueTaskSource{TResult}"/> backed by
/// <see cref="ManualResetValueTaskSourceCore{TResult}"/>, continuations run asynchronously so they never execute on
/// the leader) or synchronous (<see cref="Writer.WaitSync"/>: AwaitState's busy spin, then the adaptive yield phase
/// of up to <c>write_thread_max_yield_usec</c>, then a blocking wait on a monitor, BlockingAwaitWaitImpl). A synchronous writer
/// promoted to leader performs the group's I/O on its own thread, so the sync API never blocks on thread-pool work.
/// <para>
/// With <c>allow_concurrent_memtable_write</c> the leader may wake its followers with
/// <see cref="StateParallelMemtableWriter"/> after the WAL write (LaunchParallelMemTableWriters): every writer inserts
/// its own batch, and the last one to finish (<see cref="DbImpl"/> counts them down) completes the others and hands
/// leadership on (<see cref="ExitAsParallelGroup"/>, ExitAsBatchGroupFollower).
/// </para>
/// <para>
/// Writers with <c>Batch == null</c> are "unbatched" writers (EnterUnbatched): they become leader of a group of their
/// own and are used to run exclusive operations such as memtable switches.
/// </para>
/// </summary>
internal sealed class WriteThread
{
    public const int StateCompleted = 1;
    public const int StateGroupLeader = 2;
    /// <summary>STATE_PARALLEL_MEMTABLE_WRITER: insert the writer's own batch into the memtables, then count down.</summary>
    public const int StateParallelMemtableWriter = 3;

    private readonly Lock _lock = new();
    private Writer? _head;
    private Writer? _tail;
    private bool _leaderActive;
    private readonly long _maxGroupBytes;
    private readonly long _maxYieldTicks;
    private readonly long _slowYieldTicks;
    // AdaptationContext::value (yield_credit): fixed-point running estimate of whether yielding ends before
    // max_yield_usec without involuntary context switches (>= 0: yield).
    private int _yieldCredit;
    [ThreadStatic] private static uint t_yieldSample;

    /// <param name="maxWriteBatchGroupSizeBytes">max_write_batch_group_size_bytes.</param>
    /// <param name="maxYieldMicros">write_thread_max_yield_usec when enable_write_thread_adaptive_yield, else 0.</param>
    public WriteThread(ulong maxWriteBatchGroupSizeBytes, ulong maxYieldMicros)
    {
        _maxGroupBytes = (long)Math.Min(maxWriteBatchGroupSizeBytes, (ulong)int.MaxValue);
        _maxYieldTicks = (long)Math.Min(maxYieldMicros * (ulong)Stopwatch.Frequency / 1_000_000UL, (ulong)Stopwatch.Frequency);
        // write_thread_slow_yield_usec (default 3): a yield that takes longer is assumed to be an involuntary context switch.
        _slowYieldTicks = Math.Max(1, 3 * Stopwatch.Frequency / 1_000_000);
    }

    /// <summary>
    /// The adaptive yield phase of AwaitState (db/write_thread.cc): yields while the state is unset, for at most
    /// write_thread_max_yield_usec, if the yield credit is non-negative or this wait is sampled (1 in 256) to update it.
    /// Three slow yields fall back to blocking at once. Returns the state, or 0 to block.
    /// </summary>
    private int YieldWait(Writer w)
    {
        const int kMaxSlowYieldsWhileSpinning = 3;
        bool updateCtx = (++t_yieldSample & 255) == 0;
        bool wouldSpinAgain = false;
        int state = 0;
        if (updateCtx || Volatile.Read(ref _yieldCredit) >= 0)
        {
            long spinBegin = Stopwatch.GetTimestamp();
            long iterBegin = spinBegin;
            int slowYields = 0;
            while (iterBegin - spinBegin <= _maxYieldTicks)
            {
                Thread.Yield();
                if ((state = w.SyncState) > 0)
                {
                    wouldSpinAgain = true;
                    break;
                }
                long now = Stopwatch.GetTimestamp();
                if (now == iterBegin || now - iterBegin >= _slowYieldTicks)
                {
                    // conservatively count it as a slow yield if our clock isn't accurate enough to measure the yield duration
                    if (++slowYields >= kMaxSlowYieldsWhileSpinning)
                    {
                        // Not just one ivcsw, but several. Immediately update yield_credit and fall back to blocking
                        updateCtx = true;
                        break;
                    }
                }
                iterBegin = now;
            }
        }
        if (updateCtx)
        {
            // fixed point exponential decay with decay constant 1/1024, with +1 and -1 scaled to avoid overflow
            int v = Volatile.Read(ref _yieldCredit);
            v = v - (v / 1024) + (wouldSpinAgain ? 1 : -1) * 131072;
            Volatile.Write(ref _yieldCredit, v);
        }
        return state;
    }

    /// <summary>A queued write request. Pooled; reset with <see cref="Prepare"/> before each use.</summary>
    internal sealed class Writer : IValueTaskSource<int>
    {
        private ManualResetValueTaskSourceCore<int> _core = new() { RunContinuationsAsynchronously = true };
        // STATE_LOCKED_WAITING: the waiter is blocked on the monitor of _stateLock (BlockingAwaitWaitImpl).
        private const int StateLockedWaiting = -1;
        private readonly object _stateLock = new();
        private int _syncState;

        public WriteBatch? Batch;
        public bool Sync;
        public bool DisableWal;
        public bool NoSlowdown;
        public bool IgnoreMissingColumnFamilies;
        public bool IsSyncWaiter;
        public Exception? Error;
        public Writer? Next;
        /// <summary>Transaction extras (callback, memtable batch, prepared-section WAL tracking); null for plain writes.</summary>
        public TransactionWriteContext? Txn;
        /// <summary>The write callback failed: the writer is excluded from the WAL record and the memtables.</summary>
        public bool CallbackFailed;
        /// <summary>The write thread the writer joined (its adaptive-yield context for <see cref="WaitSync"/>).</summary>
        public WriteThread? Owner;

        public void Prepare(WriteBatch? batch, WriteOptions? options, bool syncWaiter)
        {
            Batch = batch;
            Sync = options?.Sync ?? false;
            DisableWal = options?.DisableWal ?? false;
            NoSlowdown = options?.NoSlowdown ?? false;
            IgnoreMissingColumnFamilies = options?.IgnoreMissingColumnFamilies ?? false;
            IsSyncWaiter = syncWaiter;
            Error = null;
            Next = null;
            Txn = null;
            CallbackFailed = false;
            Rearm();
        }

        /// <summary>Re-arms the wait primitive so the writer can be woken once more (after it consumed its last wake-up).</summary>
        public void Rearm()
        {
            if (IsSyncWaiter)
            {
                Volatile.Write(ref _syncState, 0);
            }
            else
            {
                _core.Reset();
            }
        }

        public bool IsUnbatched => Batch is null;
        public long Size => Batch?.DataSize ?? 0;

        /// <summary>Wakes the writer with <paramref name="state"/> (SetState).</summary>
        public void Complete(int state)
        {
            if (IsSyncWaiter)
            {
                // SetState: publish with a CAS unless the waiter has registered as blocked; then set it under the
                // monitor and wake it. A waiter that already saw the state never has its monitor touched later.
                int cur = Volatile.Read(ref _syncState);
                if (cur == StateLockedWaiting || Interlocked.CompareExchange(ref _syncState, state, 0) != 0)
                {
                    lock (_stateLock)
                    {
                        Volatile.Write(ref _syncState, state);
                        Monitor.Pulse(_stateLock);
                    }
                }
            }
            else
            {
                _core.SetResult(state);
            }
        }

        /// <summary>
        /// Blocks until woken; returns the state. Port of WriteThread::AwaitState: a short busy spin (a hand-off from a
        /// leader usually takes a few microseconds), then, with adaptive yield, Thread.Yield for up to
        /// write_thread_max_yield_usec while the yield credit says that yielding tends to pay off, and only then a blocking
        /// wait (a futex wake-up costs the waker a system call and the waiter a reschedule).
        /// </summary>
        public int WaitSync()
        {
            int s;
            // kMaxSpin iterations of the pause loop.
            for (int i = 0; i < 200; i++)
            {
                if ((s = Volatile.Read(ref _syncState)) != 0) return s;
                Thread.SpinWait(1);
            }
            WriteThread? owner = Owner;
            if (owner is not null && owner._maxYieldTicks > 0)
            {
                s = owner.YieldWait(this);
                if (s != 0) return s;
            }
            // BlockingAwaitWaitImpl: register as blocked unless the state was set meanwhile.
            s = Interlocked.CompareExchange(ref _syncState, StateLockedWaiting, 0);
            if (s != 0) return s;
            lock (_stateLock)
            {
                while ((s = Volatile.Read(ref _syncState)) == StateLockedWaiting) Monitor.Wait(_stateLock);
            }
            return s;
        }

        internal int SyncState => Volatile.Read(ref _syncState);

        public ValueTask<int> WaitAsync() => new(this, _core.Version);

        int IValueTaskSource<int>.GetResult(short token) => _core.GetResult(token);
        ValueTaskSourceStatus IValueTaskSource<int>.GetStatus(short token) => _core.GetStatus(token);
        void IValueTaskSource<int>.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
            _core.OnCompleted(continuation, state, token, flags);
    }

    /// <summary>
    /// Port of JoinBatchGroup: returns true if <paramref name="w"/> became the leader right away; otherwise the caller
    /// waits (<see cref="Writer.WaitSync"/> / <see cref="Writer.WaitAsync"/>) for <see cref="StateCompleted"/> or
    /// <see cref="StateGroupLeader"/>.
    /// </summary>
    public bool JoinBatchGroup(Writer w)
    {
        w.Owner = this;
        lock (_lock)
        {
            if (!_leaderActive)
            {
                Debug.Assert(_head is null);
                _leaderActive = true;
                return true;
            }
            w.Next = null;
            if (_tail is null) _head = _tail = w;
            else
            {
                _tail.Next = w;
                _tail = w;
            }
            return false;
        }
    }

    /// <summary>
    /// Port of EnterAsBatchGroupLeader: appends to <paramref name="group"/> (which starts with the leader) the queued
    /// writers that can be committed together with it. Stops at the first incompatible writer (unbatched, different
    /// WAL setting, a sync write behind a non-sync leader, or the group size limit).
    /// </summary>
    public void EnterAsBatchGroupLeader(Writer leader, List<Writer> group)
    {
        group.Clear();
        group.Add(leader);
        if (leader.IsUnbatched) return;
        long size = leader.Size;
        // Allow the group to grow up to a maximum size, but if the original write is small, limit the growth so we do
        // not slow down the small write too much.
        long maxSize = _maxGroupBytes;
        const long minBatchSizeBytes = 128 << 10;
        if (size <= minBatchSizeBytes) maxSize = size + minBatchSizeBytes;
        lock (_lock)
        {
            while (_head is { } w)
            {
                if (w.IsUnbatched) break;
                if (w.Txn?.Callback is { AllowWriteBatching: false }) break; // dont batch writes that don't want to be batched
                if (w.Sync && !leader.Sync) break; // Do not include a sync write into a batch handled by a non-sync write.
                if (w.DisableWal != leader.DisableWal) break;
                if (w.NoSlowdown != leader.NoSlowdown) break;
                if (size + w.Size > maxSize) break;
                size += w.Size;
                _head = w.Next;
                if (_head is null) _tail = null;
                w.Next = null;
                group.Add(w);
            }
        }
    }

    /// <summary>
    /// Port of ExitAsBatchGroupLeader: completes the followers of the group (not the leader itself) and hands
    /// leadership to the next queued writer, if any.
    /// </summary>
    public void ExitAsBatchGroupLeader(List<Writer> group)
    {
        for (int i = 1; i < group.Count; i++) group[i].Complete(StateCompleted);
        group.Clear();
        ExitAsLeader();
    }

    /// <summary>
    /// ExitAsBatchGroupFollower / the parallel part of ExitAsBatchGroupLeader: called by the writer of a parallel
    /// memtable group that finished last (<paramref name="self"/>, the leader or a follower); completes every other writer
    /// of the group (including the leader) and hands leadership to the next queued writer.
    /// </summary>
    public void ExitAsParallelGroup(List<Writer> group, Writer self)
    {
        // The woken writers do not touch the group, so the next leader may reuse it once leadership is handed on.
        for (int i = 0; i < group.Count; i++)
        {
            if (group[i] != self) group[i].Complete(StateCompleted);
        }
        group.Clear();
        ExitAsLeader();
    }

    /// <summary>Releases leadership (ExitUnbatched for unbatched writers).</summary>
    public void ExitAsLeader()
    {
        Writer? next;
        lock (_lock)
        {
            next = _head;
            if (next is null)
            {
                _leaderActive = false;
                return;
            }
            _head = next.Next;
            if (_head is null) _tail = null;
            next.Next = null;
        }
        next.Complete(StateGroupLeader);
    }

    // ---- writer pool ------------------------------------------------------------------------------------

    [ThreadStatic] private static Writer? t_syncWriter;
    private static readonly Writer?[] s_pool = new Writer?[Math.Min(256, Environment.ProcessorCount * 16)];

    /// <summary>Rents a writer: a per-thread instance for synchronous callers, a pooled one for async callers.</summary>
    public static Writer Rent(bool synchronous)
    {
        if (synchronous)
        {
            Writer? t = t_syncWriter;
            if (t is not null)
            {
                t_syncWriter = null;
                return t;
            }
            return new Writer();
        }
        Writer?[] pool = s_pool;
        for (int i = 0; i < pool.Length; i++)
        {
            Writer? w = pool[i];
            if (w is not null && Interlocked.CompareExchange(ref pool[i], null, w) == w) return w;
        }
        return new Writer();
    }

    public static void Return(Writer w, bool synchronous)
    {
        w.Batch = null;
        w.Error = null;
        w.Next = null;
        w.Txn = null;
        if (synchronous)
        {
            t_syncWriter = w;
            return;
        }
        Writer?[] pool = s_pool;
        for (int i = 0; i < pool.Length; i++)
        {
            if (pool[i] is null && Interlocked.CompareExchange(ref pool[i], w, null) is null) return;
        }
    }
}
