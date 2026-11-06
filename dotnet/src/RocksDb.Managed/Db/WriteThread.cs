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
/// With <c>enable_pipelined_write</c> the WAL write and the memtable inserts are two stages with a queue each: the WAL
/// leader links its group's memtable writers to the memtable writer queue (<see cref="ExitAsPipelinedWalLeader"/>,
/// LinkGroup) before it hands WAL leadership on, so the next group's WAL write overlaps this group's memtable inserts.
/// The first writer of that queue becomes memtable writer leader (<see cref="StateMemtableWriterLeader"/>), takes the
/// following queued writers into its memtable group (<see cref="EnterAsMemTableWriter"/>), inserts them (serially or in
/// parallel) and hands the stage on (<see cref="ExitAsMemTableWriter"/>). Memtable groups run one at a time in WAL order,
/// so each publishes its last sequence in order. <see cref="EnqueueMemTableDrain"/> (WaitForMemTableWriters) lets the WAL
/// leader or an unbatched leader wait until the memtable stage is idle (memtable switches, exclusive operations).
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
    /// <summary>STATE_MEMTABLE_WRITER_LEADER (pipelined writes): lead the next memtable group.</summary>
    public const int StateMemtableWriterLeader = 4;

    private readonly Lock _lock = new();
    private Writer? _head;
    private Writer? _tail;
    private bool _leaderActive;
    private readonly long _maxGroupBytes;
    // Pipelined writes: the memtable writer queue (newest_memtable_writer_), guarded by _memLock. _memLeaderActive is
    // set while a memtable group (or a WaitForMemTableWriters caller) owns the memtable stage.
    private readonly Lock _memLock = new();
    private Writer? _memHead;
    private Writer? _memTail;
    private bool _memLeaderActive;
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
        /// <summary>protection_bytes_per_key of the batch (0 for unbatched writers).</summary>
        public int ProtectionBytesPerKey;
        /// <summary>WriteOptions.MemtableInsertParallelism (see DbImpl.TryInsertBatchInParallel).</summary>
        public int MemtableInsertParallelism;
        public Exception? Error;
        public Writer? Next;
        /// <summary>Pipelined writes: the next writer in the memtable writer queue.</summary>
        public Writer? MemNext;
        /// <summary>Transaction extras (callback, memtable batch, prepared-section WAL tracking); null for plain writes.</summary>
        public TransactionWriteContext? Txn;
        /// <summary>The write callback failed: the writer is excluded from the WAL record and the memtables.</summary>
        public bool CallbackFailed;
        /// <summary>The write thread the writer joined (its adaptive-yield context for <see cref="WaitSync"/>).</summary>
        public WriteThread? Owner;
        /// <summary>DB::WriteWithCallback's user_write_cb (null for every other write).</summary>
        public UserWriteCallback? UserCallback;

        /// <summary>CheckPostWalWriteCallback: the writer's WAL record is written.</summary>
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        public void CheckPostWalWriteCallback() => UserCallback?.OnWalWriteFinish();

        public void Prepare(WriteBatch? batch, WriteOptions? options, bool syncWaiter)
        {
            Batch = batch;
            Sync = options?.Sync ?? false;
            DisableWal = options?.DisableWal ?? false;
            NoSlowdown = options?.NoSlowdown ?? false;
            IgnoreMissingColumnFamilies = options?.IgnoreMissingColumnFamilies ?? false;
            ProtectionBytesPerKey = batch?.ProtectionBytesPerKey ?? 0;
            MemtableInsertParallelism = options?.MemtableInsertParallelism ?? 1;
            IsSyncWaiter = syncWaiter;
            Error = null;
            Next = null;
            MemNext = null;
            Txn = null;
            CallbackFailed = false;
            UserCallback = null;
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
        /// <summary>The batch inserted into the memtables (a transaction's separate memtable batch, or the batch).</summary>
        public WriteBatch MemtableBatch => Txn?.MemtableBatch ?? Batch!;
        /// <summary>ShouldWriteToMemtable: no error, no failed callback, not a WAL-only (disable_memtable) write.</summary>
        public bool ShouldWriteToMemtable => Error is null && !CallbackFailed && Txn is not { DisableMemtable: true };
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
        bool linkedAsLeader;
        lock (_lock)
        {
            if (!_leaderActive)
            {
                Debug.Assert(_head is null);
                _leaderActive = true;
                linkedAsLeader = true;
            }
            else
            {
                w.Next = null;
                if (_tail is null) _head = _tail = w;
                else
                {
                    _tail.Next = w;
                    _tail = w;
                }
                linkedAsLeader = false;
            }
        }
        // CheckWriteEnqueuedCallback (after LinkOne, before waiting).
        w.UserCallback?.OnWriteEnqueued();
        return linkedAsLeader;
    }

    /// <summary>
    /// Port of EnterAsBatchGroupLeader (db/write_thread.cc): appends to <paramref name="group"/> (which starts with the
    /// leader) the queued writers that can be committed together with it. A writer that is incompatible with the leader
    /// (a sync write behind a non-sync leader, different no_slowdown, disable_wal or protection_bytes_per_key, an
    /// unbatched writer, a callback that does not allow batching, or one that would make the group too big) stays queued
    /// in order (upstream's r_list, grafted back after the group), and the compatible writers behind it still join. The
    /// queue is walked under <c>_lock</c>, so no writer arrives meanwhile and the incompatible ones simply keep their
    /// places. WriteOptions has no rate_limiter_priority here (WAL writes are not rate limited), and WBWI ingestion is not
    /// ported, so those two criteria never apply.
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
        long minBatchSizeBytes = _maxGroupBytes / 8;
        if (size <= minBatchSizeBytes) maxSize = size + minBatchSizeBytes;
        lock (_lock)
        {
            // The writers that stay queued (the r_list) are relinked in order; the queue is rewritten once at the end.
            Writer? keptHead = null, keptTail = null;
            Writer? w = _head;
            while (w is not null)
            {
                Writer? next = w.Next;
                if ((w.Sync && !leader.Sync) ||
                    // Do not include a sync write into a batch handled by a non-sync write.
                    w.NoSlowdown != leader.NoSlowdown ||
                    // Do not mix writes that are ok with delays with the ones that request fail on delays.
                    w.DisableWal != leader.DisableWal ||
                    // Do not mix writes that enable WAL with the ones whose WAL disabled.
                    w.ProtectionBytesPerKey != leader.ProtectionBytesPerKey ||
                    // Do not mix writes with different levels of integrity protection.
                    w.IsUnbatched ||
                    // Do not include those writes with nullptr batch. Those are not writes those are something else.
                    // They want to be alone
                    w.Txn?.Callback is { AllowWriteBatching: false } ||
                    // dont batch writes that don't want to be batched
                    size + w.Size > maxSize)
                    // Do not make batch too big
                {
                    // Stays in the queue (r_list).
                    if (keptTail is null) keptHead = w;
                    else keptTail.Next = w;
                    keptTail = w;
                }
                else
                {
                    // grow up
                    size += w.Size;
                    w.Next = null;
                    group.Add(w);
                }
                w = next;
            }
            if (keptTail is not null) keptTail.Next = null;
            _head = keptHead;
            _tail = keptTail;
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

    // ---- pipelined writes (enable_pipelined_write) --------------------------------------------------------

    /// <summary>
    /// The pipelined part of ExitAsBatchGroupLeader, called by the WAL leader once the group's WAL record is written (or
    /// failed): completes the writers that do not write to the memtables, links the others in order to the memtable
    /// writer queue (LinkGroup), and only then hands WAL leadership to the next queued writer, so that memtable groups
    /// follow WAL order. Returns the leader's next state: <see cref="StateMemtableWriterLeader"/> when it leads the
    /// memtable stage right away, <see cref="StateCompleted"/> when it does not write to the memtables, or 0 when it must
    /// wait (it is re-armed before it is linked).
    /// </summary>
    public int ExitAsPipelinedWalLeader(List<Writer> group, Writer leader)
    {
        Writer? first = null, last = null;
        // Decided before linking: once linked, a memtable leader may insert the leader's batch (and set its error).
        int leaderState = StateCompleted;
        for (int i = 0; i < group.Count; i++)
        {
            Writer w = group[i];
            if (!w.ShouldWriteToMemtable)
            {
                if (w != leader) w.Complete(StateCompleted);
                continue;
            }
            if (w == leader)
            {
                w.Rearm();
                leaderState = 0;
            }
            w.MemNext = null;
            if (last is null) first = w;
            else last.MemNext = w;
            last = w;
        }
        group.Clear();
        bool firstLeads = false;
        if (first is not null)
        {
            lock (_memLock)
            {
                Writer? rest = first;
                if (!_memLeaderActive)
                {
                    _memLeaderActive = true;
                    firstLeads = true;
                    rest = first.MemNext;
                    first.MemNext = null;
                }
                if (rest is not null)
                {
                    if (_memTail is null) _memHead = rest;
                    else _memTail.MemNext = rest;
                    _memTail = last;
                }
            }
        }
        ExitAsLeader();
        if (firstLeads)
        {
            if (first == leader) leaderState = StateMemtableWriterLeader;
            else first!.Complete(StateMemtableWriterLeader);
        }
        return leaderState;
    }

    /// <summary>
    /// Port of EnterAsMemTableWriter: appends to <paramref name="group"/> (which starts with the memtable leader) the
    /// queued memtable writers inserted together with it. Without concurrent memtable writes the group is bounded by
    /// size (growth of max_write_batch_group_size_bytes / 8 for small writes); with them, a batch with merge operands is
    /// inserted alone. A WaitForMemTableWriters marker ends the group.
    /// </summary>
    public void EnterAsMemTableWriter(Writer leader, List<Writer> group, bool allowConcurrentMemtableWrite)
    {
        group.Clear();
        group.Add(leader);
        WriteBatch lb = leader.MemtableBatch;
        if (allowConcurrentMemtableWrite && lb.HasMerge) return;
        long size = lb.DataSize;
        long maxSize = _maxGroupBytes;
        long minBatchSizeBytes = _maxGroupBytes / 8;
        if (size <= minBatchSizeBytes) maxSize = size + minBatchSizeBytes;
        lock (_memLock)
        {
            while (_memHead is { } w)
            {
                if (w.IsUnbatched) break;
                WriteBatch b = w.MemtableBatch;
                if (b.HasMerge) break;
                if (!allowConcurrentMemtableWrite)
                {
                    // Do not make batch too big
                    if (size + b.DataSize > maxSize) break;
                    size += b.DataSize;
                }
                _memHead = w.MemNext;
                if (_memHead is null) _memTail = null;
                w.MemNext = null;
                group.Add(w);
            }
        }
    }

    /// <summary>
    /// Port of ExitAsMemTableWriter (also run by the last writer of a parallel memtable group): completes every writer of
    /// the group other than <paramref name="self"/> and hands the memtable stage to the next queued writer; a
    /// WaitForMemTableWriters marker takes it over idle.
    /// </summary>
    public void ExitAsMemTableWriter(List<Writer> group, Writer self)
    {
        // The woken writers do not touch the group, so the next memtable leader may reuse it once the stage is handed on.
        for (int i = 0; i < group.Count; i++)
        {
            if (group[i] != self) group[i].Complete(StateCompleted);
        }
        group.Clear();
        Writer? next;
        lock (_memLock)
        {
            next = _memHead;
            if (next is null)
            {
                _memLeaderActive = false;
                return;
            }
            _memHead = next.MemNext;
            if (_memHead is null) _memTail = null;
            next.MemNext = null;
            if (next.IsUnbatched)
            {
                // A drain marker is always last (only the WAL leader links writers, and it is the one waiting), so the
                // stage is idle from here on (WaitForMemTableWriters: newest_memtable_writer_ = nullptr).
                Debug.Assert(_memHead is null);
                _memLeaderActive = false;
            }
        }
        next.Complete(StateMemtableWriterLeader);
    }

    /// <summary>
    /// First half of WaitForMemTableWriters, called by the WAL leader or an unbatched leader (so that no memtable writer
    /// can be linked meanwhile): returns false if the memtable stage is idle; otherwise queues <paramref name="marker"/>
    /// (an unbatched writer) behind the pending memtable writers, and the caller waits for
    /// <see cref="StateMemtableWriterLeader"/>, after which the stage is idle.
    /// </summary>
    public bool EnqueueMemTableDrain(Writer marker)
    {
        Debug.Assert(marker.IsUnbatched);
        marker.Owner = this;
        lock (_memLock)
        {
            if (!_memLeaderActive) return false;
            marker.MemNext = null;
            if (_memTail is null) _memHead = _memTail = marker;
            else
            {
                _memTail.MemNext = marker;
                _memTail = marker;
            }
            return true;
        }
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
        w.MemNext = null;
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
