using System.Globalization;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using RocksDb.Managed.Env;
using RocksDb.Managed.Util;
using RocksDb.Managed.Wal;

namespace RocksDb.Managed.Db;

internal sealed partial class DbImpl
{
    private readonly WriteThread _writeThread;
    private readonly bool _allowConcurrentMemtableWrite;
    /// <summary>enable_pipelined_write: WAL writes and memtable inserts are separate stages (see <see cref="WritePipelinedAsync"/>).</summary>
    private readonly bool _pipelined;
    private readonly List<WriteThread.Writer> _memGroup = new();    // pipelined: memtable-leader-only
    private readonly WriteThread.Writer _memDrainMarker = new();    // pipelined: WaitForMemTableWriters (WAL / unbatched leader only)
    private readonly List<WriteThread.Writer> _group = new();       // leader-only
    private readonly WriteBatch _groupBatch = new(64 << 10, 0);     // leader-only: merged WAL record
    private readonly List<ColumnFamilyData> _flushScheduler = new(); // leader-only: CFs whose memtable became full
    // Column families by id for the write path: copy-on-write array, replaced by an unbatched leader.
    private ColumnFamilyData?[] _cfById = [];
    /// <summary>Some column family uses disallow_memtable_writes (set once, never reset).</summary>
    private volatile bool _anyDisallowMemtableWrites;
    /// <summary>
    /// Some column family uses disallow_memtable_writes or was dropped (set once, never reset): batches written without
    /// column family validation (writes through a <see cref="ColumnFamily"/> receiver, internal transaction batches) are
    /// checked before the WAL too, so that a write to such a column family fails without reaching the memtable insert,
    /// where the failure would be a fatal background error (HandleMemTableInsertFailure).
    /// </summary>
    private volatile bool _checkUnvalidatedBatches;

    private LogWriter? _log;          // leader-only
    private ulong _logNumber;         // written by leader under mutex
    private bool _logEmpty = true;    // leader-only
    private bool _walDirSynced;       // leader-only
    private bool _hasUnpersistedData; // set when a DisableWal write was applied

    // ---- WAL size accounting and lazy sync of closed WALs (logs_ / alive_wal_files_ / wals_total_size_) ----

    /// <summary>
    /// Closed WALs (older than <see cref="_log"/>) whose buffer was written to the OS at the switch but which were not
    /// synced yet (logs_ entries before the current one; single column family without 2PC, see SwitchMemtableAsync).
    /// Like upstream, the next sync write, FlushWAL(sync) / SyncWAL syncs them (oldest first) before the current WAL;
    /// WALs whose data was flushed are released unsynced. Leader-only.
    /// </summary>
    private readonly List<LogWriter> _closedWalsToSync = new();
    /// <summary>
    /// logs_to_free_: obsolete closed WAL writers taken off <see cref="_closedWalsToSync"/> by the write leader, closed
    /// by the next obsolete-file purge (background) or at shutdown. Closing a large WAL whose tail is still being written
    /// back can block for tens of milliseconds (ext4 releases its block reservation), which must not happen on the write
    /// path. Guarded by the mutex.
    /// </summary>
    private readonly List<LogWriter> _walsToFree = new();
    /// <summary>Bytes appended to the current WAL (alive_wal_files_.back().size). Leader-only.</summary>
    private long _currentWalSize;
    /// <summary>wals_total_size_: bytes of all alive WALs (written by the leader, decremented by obsolete-file purges).</summary>
    private long _walsTotalSize;
    /// <summary>Sizes of alive WALs other than the current one (alive_wal_files_ entries). Guarded by the mutex.</summary>
    private readonly Dictionary<ulong, long> _aliveWalSizes = new();
    /// <summary>alive_wal_files_.begin()->getting_flushed: the oldest WAL a SwitchWAL already asked to flush. Leader-only.</summary>
    private ulong _walGettingFlushed;
    // Cached IsWriteStalled() (recomputed under the mutex whenever memtable counts change) so that the common,
    // unstalled write path does not take the DB mutex.
    private volatile bool _maybeStalled;

    // ---- public entry points ----------------------------------------------------------------------------

    /// <summary>
    /// Writes a batch (DBImpl::WriteImpl). With <paramref name="synchronous"/> the returned task is always complete:
    /// the caller blocks only while waiting for its turn, and performs the group's I/O itself when it leads.
    /// </summary>
    public ValueTask WriteAsync(WriteOptions? options, WriteBatch batch, bool validateColumnFamilies, bool synchronous, CancellationToken cancellationToken) =>
        WriteAsync(options, batch, validateColumnFamilies, txn: null, synchronous, cancellationToken);

    /// <summary>
    /// <see cref="WriteAsync(WriteOptions?, WriteBatch, bool, bool, CancellationToken)"/> with the transaction extras of
    /// DBImpl::WriteImpl (write callback, disable_memtable, log_ref, log_used, WAL-only commit marker). See
    /// <see cref="TransactionWriteContext"/>.
    /// </summary>
    internal ValueTask WriteAsync(WriteOptions? options, WriteBatch batch, bool validateColumnFamilies, TransactionWriteContext? txn, bool synchronous,
        CancellationToken cancellationToken) =>
        WriteImplAsync(options, batch, validateColumnFamilies, txn, userCallback: null, synchronous, cancellationToken);

    /// <summary>
    /// DB::WriteWithCallback(options, updates, user_write_cb): <see cref="WriteAsync(WriteOptions?, WriteBatch, bool, bool, CancellationToken)"/>
    /// that notifies <paramref name="userCallback"/> when the write is enqueued and when its WAL record is written. Not
    /// supported by sequence-per-batch (WritePrepared / WriteUnprepared) DBs, whose plain writes go through the
    /// transaction layer (upstream's StackableDB does not implement WriteWithCallback either).
    /// </summary>
    internal ValueTask WriteWithCallbackAsync(WriteOptions? options, WriteBatch batch, UserWriteCallback userCallback, bool synchronous,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(userCallback);
        if (_seqPerBatch) ThrowHelper.NotSupported("WriteWithCallback not implemented for this interface.");
        return WriteImplAsync(options, batch, validateColumnFamilies: true, txn: null, userCallback, synchronous, cancellationToken);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask WriteImplAsync(WriteOptions? options, WriteBatch batch, bool validateColumnFamilies, TransactionWriteContext? txn,
        UserWriteCallback? userCallback, bool synchronous, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(batch);
        ThrowIfDisposed();
        if (_readOnly) ThrowReadOnly();
        if (_queryTracer is not null) MaybeTraceWrite(options, batch, txn);
        if (options is { Sync: true, DisableWal: true }) ThrowHelper.InvalidArgument("Sync writes has to enable WAL.");
        // Corruption detection in recycled WALs relies on sequential sequence numbers, but WritePreparedTxnDB uses
        // disableWAL internally for split writes (DBImpl::WriteImpl).
        if (options is { DisableWal: true } && Options.RecycleLogFileNum > 0 && !(_twoWriteQueues && txn is { DisableMemtable: true }))
            ThrowHelper.InvalidArgument("WriteOptions::disableWAL option is not supported if DBOptions::recycle_log_file_num > 0");
        if (batch.DataSize < WriteBatch.HeaderSize) ThrowHelper.Corruption("malformed WriteBatch (too small)");
        // Row cache entries ignore range tombstones of their own file (TableCache::Get), so DeleteRange is rejected.
        if (Options.RowCache is not null && batch.HasDeleteRange) ThrowHelper.NotSupported("DeleteRange is not compatible with row cache.");
        if (options is { ProtectionBytesPerKey: not 0 } || batch.ProtectionBytesPerKey != 0) PrepareProtectedWrite(options, batch);
        if (validateColumnFamilies)
        {
            ValidateColumnFamilies(batch, options?.IgnoreMissingColumnFamilies ?? false);
            if (txn?.MemtableBatch is { } mb) ValidateColumnFamilies(mb, options?.IgnoreMissingColumnFamilies ?? false);
        }
        else if (_checkUnvalidatedBatches)
        {
            ValidateColumnFamilies(batch, options?.IgnoreMissingColumnFamilies ?? false);
            if (txn?.MemtableBatch is { } mb) ValidateColumnFamilies(mb, options?.IgnoreMissingColumnFamilies ?? false);
        }
        if (options is { LowPri: true } && WriteControllerInstance.NeedSpeedupCompaction)
        {
            await ThrottleLowPriWritesAsync(options, batch, synchronous, cancellationToken).ConfigureAwait(false);
        }
        if (_twoWriteQueues && txn is { DisableMemtable: true })
        {
            // two_write_queues: WAL-only writes (2PC prepare of WriteCommitted, commit markers of WritePrepared /
            // WriteUnprepared) go to the second queue (DBImpl::WriteImpl -> WriteImplWALOnly).
            await WriteWalOnlyAsync(options, batch, txn, userCallback, synchronous, cancellationToken).ConfigureAwait(false);
            return;
        }
        if (_seqPerBatch && (txn is null || txn.BatchCount == 0))
        {
            // WritePreparedTxnDB::Write (skip_concurrency_control) -> WriteInternal: a write that does not come from a
            // transaction gets its sub-batch count, a trailing Noop boundary and a commit-map update.
            await WritePlainSeqPerBatchAsync(options, batch, txn, synchronous, cancellationToken).ConfigureAwait(false);
            return;
        }
        if (_unorderedWrite)
        {
            await WriteUnorderedAsync(options, batch, txn, userCallback, synchronous, cancellationToken).ConfigureAwait(false);
            return;
        }
        if (_pipelined)
        {
            await WritePipelinedAsync(options, batch, txn, userCallback, synchronous, cancellationToken).ConfigureAwait(false);
            return;
        }
        Statistics? stats = Options.Statistics;
        long writeStart = stats?.StartTimer() ?? 0;
        WriteThread.Writer w = WriteThread.Rent(synchronous);
        w.Prepare(batch, options, synchronous);
        w.Txn = txn;
        w.UserCallback = userCallback;
        try
        {
            if (!_writeThread.JoinBatchGroup(w))
            {
                int state = synchronous ? w.WaitSync() : await w.WaitAsync().ConfigureAwait(false);
                if (state == WriteThread.StateParallelMemtableWriter)
                {
                    // The leader wrote the WAL record and assigned the sequence numbers; insert our own batch.
                    ParallelMemTableInsert(w);
                    if (CompleteParallelMemTableWriter()) FinishParallelGroup(w);
                    else state = synchronous ? w.WaitSync() : await w.WaitAsync().ConfigureAwait(false);
                    Debug.Assert(state is WriteThread.StateCompleted or WriteThread.StateParallelMemtableWriter);
                    if (w.Error is { } pe) ThrowWriteError(pe);
                    return;
                }
                if (state == WriteThread.StateCompleted)
                {
                    if (w.Error is { } e) ThrowWriteError(e);
                    return;
                }
                Debug.Assert(state == WriteThread.StateGroupLeader);
            }
            ValueTask<int> lead = LeadWriteGroup(w, synchronous, cancellationToken);
            int leadState;
            if (synchronous)
            {
                Debug.Assert(lead.IsCompleted);
                leadState = lead.GetAwaiter().GetResult();
            }
            else
            {
                leadState = await lead.ConfigureAwait(false);
            }
            if (leadState == WriteThread.StateParallelMemtableWriter)
            {
                // The leader launched a parallel memtable group: insert its own batch, then wait for the others.
                ParallelMemTableInsert(w);
                if (CompleteParallelMemTableWriter()) FinishParallelGroup(w);
                else if (synchronous) w.WaitSync();
                else await w.WaitAsync().ConfigureAwait(false);
            }
            if (w.Error is { } err) ThrowWriteError(err);
        }
        finally
        {
            WriteThread.Return(w, synchronous);
            stats?.RecordElapsedMicros(Histogram.DbWrite, writeStart);
        }
    }

    /// <summary>
    /// Port of DBImpl::ThrottleLowPriWritesIfNeeded (db_impl_write.cc), called for WriteOptions.low_pri writes once the
    /// write controller needs compaction to speed up (stopped, delayed or compaction pressure; checked by the caller
    /// outside the DB mutex, like upstream, so the condition may be stale). The write is charged to the controller's
    /// low-priority rate limiter (IO_HIGH, in chunks of at most its burst size) instead of waiting for the stall to end,
    /// so heavy low-priority writes still make slow progress. With no_slowdown it fails with Incomplete
    /// "Low priority write stall". With 2PC, only prepare is rate limited, not commit or rollback.
    /// </summary>
    private async ValueTask ThrottleLowPriWritesAsync(WriteOptions options, WriteBatch batch, bool synchronous, CancellationToken cancellationToken)
    {
        if (Options.Allow2Pc && (batch.HasCommit || batch.HasRollback))
        {
            // For 2PC, we only rate limit prepare, not commit.
            return;
        }
        if (options.NoSlowdown) throw new RocksDbException(StatusCode.Incomplete, "Incomplete: Low priority write stall");
        // Rate limit those writes. The reason that we don't completely wait is that in case the write is heavy, low pri
        // writes may never have a chance to run. Now we guarantee we are still slowly making progress.
        GenericRateLimiter limiter = WriteControllerInstance.LowPriRateLimiter;
        long dataSize = batch.DataSize;
        while (dataSize > 0)
        {
            // RateLimiter::RequestToken: at most one burst per request.
            long allowed = Math.Min(dataSize, limiter.SingleBurstBytes);
            if (synchronous) limiter.Request(allowed, IoPriority.High, null, RateLimiterOpType.Write);
            else await limiter.RequestAsync(allowed, IoPriority.High, null, RateLimiterOpType.Write, cancellationToken).ConfigureAwait(false);
            dataSize -= allowed;
        }
    }

    /// <summary>
    /// Port of DBImpl::PipelinedWriteImpl. The WAL leader groups the queued writers, runs PreprocessWrite, the write
    /// callbacks and the sequence allocation, writes the group's WAL record (<see cref="LeadWriteGroupAsync"/>), links the
    /// memtable writers of the group to the memtable writer queue and hands WAL leadership on
    /// (<see cref="WriteThread.ExitAsPipelinedWalLeader"/>). The next WAL group is then written while this group's
    /// memtable inserts run: the first queued memtable writer leads a memtable group (<see cref="LeadMemTableGroup"/>),
    /// inserting serially or waking the writers for parallel inserts, and the group publishes its last sequence before
    /// the next memtable group starts. Not compatible with two write queues or seq_per_batch (NotSupported at write time,
    /// as upstream).
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask WritePipelinedAsync(WriteOptions? options, WriteBatch batch, TransactionWriteContext? txn, UserWriteCallback? userCallback,
        bool synchronous,
        CancellationToken cancellationToken)
    {
        if (_twoWriteQueues) ThrowHelper.NotSupported("pipelined_writes is not compatible with concurrent prepares");
        if (_seqPerBatch) ThrowHelper.NotSupported("pipelined_writes is not compatible with seq_per_batch");
        Statistics? stats = Options.Statistics;
        long writeStart = stats?.StartTimer() ?? 0;
        WriteThread.Writer w = WriteThread.Rent(synchronous);
        w.Prepare(batch, options, synchronous);
        w.Txn = txn;
        w.UserCallback = userCallback;
        try
        {
            int state = WriteThread.StateGroupLeader;
            if (!_writeThread.JoinBatchGroup(w)) state = synchronous ? w.WaitSync() : await w.WaitAsync().ConfigureAwait(false);
            if (state == WriteThread.StateGroupLeader)
            {
                ValueTask<int> lead = LeadWriteGroup(w, synchronous, cancellationToken);
                if (synchronous)
                {
                    Debug.Assert(lead.IsCompleted);
                    state = lead.GetAwaiter().GetResult();
                }
                else
                {
                    state = await lead.ConfigureAwait(false);
                }
                // 0: linked behind earlier memtable writers; wait for our turn.
                if (state == 0) state = synchronous ? w.WaitSync() : await w.WaitAsync().ConfigureAwait(false);
            }
            if (state == WriteThread.StateMemtableWriterLeader) state = LeadMemTableGroup(w);
            if (state == WriteThread.StateParallelMemtableWriter)
            {
                ParallelMemTableInsert(w);
                if (CompleteParallelMemTableWriter()) FinishParallelGroup(w);
                else state = synchronous ? w.WaitSync() : await w.WaitAsync().ConfigureAwait(false);
            }
            Debug.Assert(state is WriteThread.StateCompleted or WriteThread.StateParallelMemtableWriter);
            if (w.Error is { } err) ThrowWriteError(err);
        }
        finally
        {
            WriteThread.Return(w, synchronous);
            stats?.RecordElapsedMicros(Histogram.DbWrite, writeStart);
        }
    }

    /// <summary>
    /// The memtable stage of a pipelined write led by <paramref name="leader"/> (PipelinedWriteImpl,
    /// STATE_MEMTABLE_WRITER_LEADER): EnterAsMemTableWriter, then either LaunchParallelMemTableWriters (returns
    /// <see cref="WriteThread.StateParallelMemtableWriter"/>: the leader inserts its own batch next) or the serial insert
    /// of the whole group, publication of its last sequence and ExitAsMemTableWriter (returns
    /// <see cref="WriteThread.StateCompleted"/>).
    /// </summary>
    private int LeadMemTableGroup(WriteThread.Writer leader)
    {
        TestHooks.Invoke("DBImpl::PipelinedWriteImpl:MemTableWriterLeader");
        List<WriteThread.Writer> group = _memGroup;
        _writeThread.EnterAsMemTableWriter(leader, group, _allowConcurrentMemtableWrite);
        ulong total = 0;
        for (int i = 0; i < group.Count; i++) total += (uint)group[i].MemtableBatch.Count;
        WriteBatch lastBatch = group[^1].MemtableBatch;
        // write_group.last_sequence (sequences are consecutive along the memtable queue).
        ulong last = lastBatch.Sequence + (uint)lastBatch.Count - 1;
        if (CanInsertInParallel(group, group.Count, total))
        {
            LaunchParallelMemTableWriters(group, leader, total, last, walRecordBytes: 0, wal: false, needSync: false);
            return WriteThread.StateParallelMemtableWriter;
        }
        Exception? insertError = InsertWritersSerially(group);
        if (insertError is not null)
        {
            // PipelinedWriteImpl: the memtable group's status is the leader's.
            leader.Error ??= insertError;
            HandleMemTableInsertFailure(insertError);
        }
        else if (total > 0) // Don't publish a partial batch write
        {
            Versions.SetLastSequence(last);
            NotifySequenceWaiters();
        }
        _writeThread.ExitAsMemTableWriter(group, leader);
        return WriteThread.StateCompleted;
    }

    /// <summary>
    /// WriteThread::WaitForMemTableWriters (DBImpl::WaitForPendingWrites with pipelined writes): waits until no memtable
    /// insert is pending. REQUIRES: the caller is the WAL leader or an unbatched leader (no new memtable writer can be
    /// linked meanwhile).
    /// </summary>
    private async ValueTask WaitForMemTableWritersAsync(bool synchronous)
    {
        if (!_pipelined) return;
        WriteThread.Writer marker = _memDrainMarker;
        marker.Prepare(null, null, synchronous);
        if (!_writeThread.EnqueueMemTableDrain(marker)) return;
        int state = synchronous ? marker.WaitSync() : await marker.WaitAsync().ConfigureAwait(false);
        Debug.Assert(state == WriteThread.StateMemtableWriterLeader);
    }

    /// <summary>A plain write into a sequence-per-batch DB (see <see cref="SeqPerBatchHooks.WritePlainAsync"/>).</summary>
    private ValueTask WritePlainSeqPerBatchAsync(WriteOptions? options, WriteBatch batch, TransactionWriteContext? txn, bool synchronous,
        CancellationToken cancellationToken)
    {
        SeqPerBatchHooks hooks = _txnHooks ?? throw new InvalidOperationException("WritePrepared/WriteUnprepared DB written before its transaction layer was initialized");
        if (batch.Count == 0) return default; // WriteInternal: nothing to write
        if (txn is not null && (txn.Callback is not null || txn.MemtableBatch is not null || txn.DisableMemtable))
        {
            throw new InvalidOperationException("transaction writes into a sequence-per-batch DB must give their batch count");
        }
        return hooks.WritePlainAsync(options, batch, synchronous, cancellationToken);
    }

    private static void ThrowWriteError(Exception e)
    {
        // Each writer gets its own exception object (the original may be shared by the whole group).
        if (e is RocksDbException r) throw r.Rethrowable();
        if (e is ObjectDisposedException) ThrowDisposed();
        throw RocksDbException.IOError(e.Message, e);
    }

    /// <summary>
    /// Becomes the write leader without a batch (WriteThread::EnterUnbatched): on return no other write is in progress
    /// until <see cref="WriteThread.ExitAsLeader"/> is called.
    /// </summary>
    internal async ValueTask<WriteThread.Writer> EnterUnbatchedAsync(bool synchronous, CancellationToken cancellationToken)
    {
        WriteThread.Writer w = await EnterUnbatchedCoreAsync(synchronous).ConfigureAwait(false);
        if (IsDisposed)
        {
            // The DB was closed while this operation waited for its turn: the WAL and MANIFEST are gone.
            ExitUnbatched(w, synchronous);
            ThrowDisposed();
        }
        return w;
    }

    /// <summary>
    /// EnterUnbatched without the closed-DB check (used by close itself). With two write queues the second queue is
    /// entered too (after the main one), so no WAL write of either queue is in progress.
    /// </summary>
    private async ValueTask<WriteThread.Writer> EnterUnbatchedCoreAsync(bool synchronous)
    {
        WriteThread.Writer w = WriteThread.Rent(synchronous);
        w.Prepare(null, null, synchronous);
        if (!_writeThread.JoinBatchGroup(w))
        {
            int state = synchronous ? w.WaitSync() : await w.WaitAsync().ConfigureAwait(false);
            Debug.Assert(state == WriteThread.StateGroupLeader);
        }
        if (_pipelined) await WaitForMemTableWritersAsync(synchronous).ConfigureAwait(false);
        if (_twoWriteQueues) await EnterNonMemUnbatchedAsync(synchronous).ConfigureAwait(false);
        // WaitForPendingWrites (unordered_write): the writers that already wrote the WAL finish their memtable inserts.
        // Upstream waits at the callers of EnterUnbatched that switch or inspect the memtables (flush, ingestion, column
        // family drop, error recovery); waiting for every unbatched operation is at least as strict.
        ValueTask pending = WaitForPendingWritesAsync(synchronous);
        if (!pending.IsCompletedSuccessfully) await pending.ConfigureAwait(false);
        return w;
    }

    internal void ExitUnbatched(WriteThread.Writer w, bool synchronous)
    {
        ExitUnbatchedQueues();
        WriteThread.Return(w, synchronous);
    }

    // ---- leader -----------------------------------------------------------------------------------------

    /// <summary><see cref="LeadWriteGroupAsync"/> with the leader's own reusable state machine box.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ValueTask<int> LeadWriteGroup(WriteThread.Writer leader, bool synchronous, CancellationToken cancellationToken)
    {
        OwnedValueTaskMethodBuilder<int>.SetNextSlot(leader.LeadBox);
        return LeadWriteGroupAsync(leader, synchronous, cancellationToken);
    }

    /// <summary>
    /// Commits the group led by <paramref name="leader"/>: PreprocessWrite, sequence allocation, one WAL record for the
    /// whole group, memtable inserts, publication of the last sequence, then wakes the followers
    /// (DBImpl::WriteImpl, non-pipelined). With pipelined writes only the WAL stage: the group is then linked to the
    /// memtable writer queue, and the result is the leader's next state (see
    /// <see cref="WriteThread.ExitAsPipelinedWalLeader"/>); otherwise it is <see cref="WriteThread.StateCompleted"/>, or
    /// <see cref="WriteThread.StateParallelMemtableWriter"/> when the group's memtable inserts run in parallel (the caller
    /// then inserts the leader's own batch and waits for the group like a parallel follower).
    /// </summary>
    /// <remarks>
    /// Built with <see cref="OwnedValueTaskMethodBuilder{TResult}"/>: when the leader suspends (async WAL write, write
    /// stall), its state machine lives in the leader writer's <see cref="WriteThread.Writer.LeadBox"/>,
    /// so concurrent async writers do not allocate boxes. Callers go through <see cref="LeadWriteGroup"/>.
    /// </remarks>
    [AsyncMethodBuilder(typeof(OwnedValueTaskMethodBuilder<>))]
    private async ValueTask<int> LeadWriteGroupAsync(WriteThread.Writer leader, bool synchronous, CancellationToken cancellationToken)
    {
        List<WriteThread.Writer> group = _group;
        // PipelinedWriteImpl: a writer that must not be batched (its callback reads the DB) waits until every earlier
        // write is in the memtables.
        if (_pipelined && leader.Txn?.Callback is { AllowWriteBatching: false }) await WaitForMemTableWritersAsync(synchronous).ConfigureAwait(false);
        _writeThread.EnterAsBatchGroupLeader(leader, group);
        bool parallel = false;
        int leaderState = WriteThread.StateCompleted;
        try
        {
            // Writers that queued before the DB was closed get leadership after close released it.
            if (IsDisposed) ThrowDisposed();
            ValueTask pre = PreprocessWriteAsync(leader.NoSlowdown, synchronous, cancellationToken);
            if (!pre.IsCompletedSuccessfully) await pre.ConfigureAwait(false);

            int valid = CheckGroupCallbacks(group, out ulong total, out bool needSync, out WriteThread.Writer? firstValid);
            if (_queryTracer is { PreserveWriteOrder: true } tracer) TraceWriteGroup(tracer, group);
            if (_bdwTransformer is { Touched.Count: > 0 } bdw)
            {
                // SyncBlobDirectWriteManagers: the blobs must be durable before a sync WAL write references them.
                try
                {
                    if (needSync)
                    {
                        foreach (Blob.BlobDirectWriteManager m in bdw.Touched) m.SyncAllOpenFiles();
                    }
                }
                finally
                {
                    bdw.ClearTouched();
                }
            }
            long perfWal = PerfContext.StartTimer(); // PERF_TIMER_GUARD(write_wal_time)
            ValueTask<(ulong Last, int WalBytes)> commit = AllocateAndWriteGroupToWalAsync(group, leader, firstValid, valid, total, needSync, synchronous);
            (ulong last, int walRecordBytes) = commit.IsCompletedSuccessfully ? commit.Result : await commit.ConfigureAwait(false);
            PerfContext.StopTimer(PerfMetric.WriteWalTime, perfWal);
            if (_seqPerBatch && valid > 0) RunGroupPreReleaseCallbacks(group, leader);

            if (_pipelined)
            {
                // PipelinedWriteImpl records the group's stats in the WAL stage; the memtable stage follows.
                RecordWriteGroup(group, total, walRecordBytes, !leader.DisableWal, needSync);
            }
            else if (CanInsertInParallel(group, valid, total))
            {
                // LaunchParallelMemTableWriters: every writer inserts its own batch; the last one to finish publishes the
                // sequence and exits the group (FinishParallelGroup).
                parallel = true;
                LaunchParallelMemTableWriters(group, leader, total, last, walRecordBytes, !leader.DisableWal, needSync);
            }
            else
            {
                long perfMem = PerfContext.StartTimer(); // PERF_TIMER_GUARD(write_memtable_time)
                InsertGroupSerially(group, leader, total, last, walRecordBytes, needSync);
                PerfContext.StopTimer(PerfMetric.WriteMemtableTime, perfMem);
            }
        }
        catch (Exception e)
        {
            for (int i = 0; i < group.Count; i++) group[i].Error ??= e;
        }
        finally
        {
            if (_pipelined)
            {
                // Writers that failed (error, callback) are completed; the others go to the memtable stage.
                leaderState = _writeThread.ExitAsPipelinedWalLeader(group, leader);
            }
            else if (!parallel)
            {
                if (_bdwSwapped.Count > 0) RestoreBlobDirectWriteBatches();
                _writeThread.ExitAsBatchGroupLeader(group);
            }
        }
        // LaunchParallelMemTableWriters: the leader inserts its own batch like any parallel memtable writer. That part
        // (and its wait for the other writers) is run by the caller, so that the wait suspends the caller's pooled state
        // machine rather than this one (a wake-up of this method's box would be queued to the thread pool through an
        // allocated work item).
        return parallel ? WriteThread.StateParallelMemtableWriter : leaderState;
    }

    /// <summary>
    /// The WriteCallbacks of a write group (WriteThread::Writer::CheckCallback), run once PreprocessWrite is done, before
    /// sequence allocation: a failed callback excludes its writer from the WAL record and the memtables. Returns the
    /// number of valid writers; <paramref name="total"/> is the number of sequence numbers they consume.
    /// </summary>
    private int CheckGroupCallbacks(List<WriteThread.Writer> group, out ulong total, out bool needSync, out WriteThread.Writer? firstValid)
    {
        total = 0;
        needSync = false;
        firstValid = null;
        int valid = 0;
        for (int i = 0; i < group.Count; i++)
        {
            WriteThread.Writer w = group[i];
            TransactionWriteContext? t = w.Txn;
            if (t?.Callback is { } cb)
            {
                RocksDbException? err;
                try { err = cb.Check(this); }
                catch (RocksDbException e) { err = e; }
                if (err is not null)
                {
                    w.Error = err;
                    w.CallbackFailed = true;
                    continue;
                }
            }
            if (_anyBlobDirectWrite && t is null && !TryTransformForBlobDirectWrite(w)) continue;
            valid++;
            firstValid ??= w;
            // seq_per_batch_: every valid writer consumes its batch_cnt sequence numbers, also with disable_memtable.
            if (_seqPerBatch) total += (uint)t!.BatchCount;
            else if (t is null || !t.DisableMemtable) total += (uint)(t?.MemtableBatch ?? w.Batch!).Count;
            needSync |= w.Sync;
        }
        return valid;
    }

    /// <summary>
    /// Allocates the group's <paramref name="total"/> sequence numbers (<see cref="AssignGroupSequences"/>) and writes
    /// its WAL record (<see cref="WriteGroupToWalAsync"/>) unless the WAL is disabled. With two write queues the WAL is
    /// shared with the WAL-only queue, so both happen under the WAL write lock: WAL order matches sequence order
    /// (ConcurrentWriteGroupToWAL). Returns the group's last sequence and the WAL record size.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<(ulong Last, int WalBytes)> AllocateAndWriteGroupToWalAsync(List<WriteThread.Writer> group, WriteThread.Writer leader,
        WriteThread.Writer? firstValid, int valid, ulong total, bool needSync, bool synchronous)
    {
        bool walLocked = false;
        if (_twoWriteQueues && !leader.DisableWal && valid > 0)
        {
            ValueTask l = AcquireWalWriteLockAsync(synchronous);
            if (!l.IsCompletedSuccessfully) await l.ConfigureAwait(false);
            walLocked = true;
        }
        try
        {
            ulong last = AssignGroupSequences(group, total);
            int walBytes = 0;
            if (!leader.DisableWal && valid > 0)
            {
                ValueTask<int> walWrite = WriteGroupToWalAsync(group, firstValid!, valid, last, needSync, synchronous);
                walBytes = walWrite.IsCompletedSuccessfully ? walWrite.Result : await walWrite.ConfigureAwait(false);
            }
            else if (total > 0)
            {
                Volatile.Write(ref _hasUnpersistedData, true);
            }
            return (last, walBytes);
        }
        finally
        {
            if (walLocked) _walWriteLock!.Release();
        }
    }

    /// <summary>
    /// Allocates <paramref name="total"/> sequence numbers and gives each valid writer of the group its first one
    /// (batch sequence, and the transaction's sequence_used). Returns the group's last sequence.
    /// </summary>
    private ulong AssignGroupSequences(List<WriteThread.Writer> group, ulong total)
    {
        ulong seq = Versions.AllocateSequence(total);
        for (int i = 0; i < group.Count; i++)
        {
            WriteThread.Writer w = group[i];
            if (w.CallbackFailed) continue;
            TransactionWriteContext? t = w.Txn;
            WriteBatch b = w.Batch!;
            b.Sequence = seq;
            if (_seqPerBatch)
            {
                t!.SequenceUsed = seq;
                seq += (uint)t.BatchCount;
            }
            else if (t is not null)
            {
                t.SequenceUsed = seq;
                if (t.MemtableBatch is { } mb) mb.Sequence = seq;
                if (t.DisableMemtable) continue; // a prepare consumes no sequence numbers
                seq += (uint)(t.MemtableBatch ?? b).Count;
            }
            else
            {
                seq += (uint)b.Count;
            }
        }
        return seq - 1;
    }

    /// <summary>
    /// DBImpl::WriteToWAL for a write group: one record with the WAL parts of the valid writers' batches, synced (with
    /// the closed WALs, oldest first) when a writer asked for it. A failure stops writes (WAL background error). Returns
    /// the record size.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<int> WriteGroupToWalAsync(List<WriteThread.Writer> group, WriteThread.Writer firstValid, int valid, ulong last, bool needSync,
        bool synchronous)
    {
        // DBImpl::MergeBatch: a single batch without a WAL termination point is logged as is; otherwise the
        // WAL parts of the batches (WriteBatchInternal::Append(wal_only = true)) are flattened into one record.
        ReadOnlyMemory<byte> record;
        if (valid == 1 && !firstValid!.Batch!.HasWalTerminationPoint)
        {
            record = firstValid.Batch.DataMemory;
        }
        else
        {
            _groupBatch.SetContentsWalOnly(firstValid!.Batch!);
            for (int i = 0; i < group.Count; i++)
            {
                WriteThread.Writer w = group[i];
                if (w == firstValid || w.CallbackFailed) continue;
                WriteBatch.Append(_groupBatch, w.Batch!, walOnly: true);
            }
            record = _groupBatch.DataMemory;
        }
        try
        {
            // DBImpl::WriteToWAL: merged_batch.VerifyChecksum() (each protected batch of the group).
            if (Volatile.Read(ref _protectedWrites) != 0)
                VerifyWalRecordProtection(valid == 1 && !firstValid!.Batch!.HasWalTerminationPoint ? firstValid.Batch : _groupBatch);
            LogWriter log = _log!;
            // wal_context.prev_size (DBImpl::WriteImpl, one write queue only): the WAL's size before this group's record.
            if (!_twoWriteQueues)
            {
                _walPrevSize = log.File?.Size ?? -1;
                _walPrevSizeNumber = _logNumber;
            }
            // DBImpl::WriteToWAL: record the timestamp sizes of column families with timestamps not yet recorded
            // in this WAL (recovery uses them to reconcile timestamp-size changes).
            if (_cfTimestampSizes is { } tsSizes) log.MaybeAddUserDefinedTimestampSizeRecord(tsSizes);
            if (synchronous)
            {
                log.AddRecord(record.Span, Util.Coding.DecodeFixed64(record.Span));
                if (needSync)
                {
                    // WriteToWAL with need_log_sync: every WAL in logs_ is synced, oldest first.
                    if (_closedWalsToSync.Count > 0) SyncClosedWalsAsync(synchronous: true).GetAwaiter().GetResult();
                    log.Sync();
                    SyncWalDirIfNeeded();
                }
            }
            else
            {
                // No cancellation once the group is being committed: an abandoned AddRecord would leave a torn
                // record in the middle of the WAL (and fail followers that did not ask for cancellation).
                await log.AddRecordAsync(record, Util.Coding.DecodeFixed64(record.Span), CancellationToken.None).ConfigureAwait(false);
                if (needSync)
                {
                    if (_closedWalsToSync.Count > 0) await SyncClosedWalsAsync(synchronous: false).ConfigureAwait(false);
                    await log.SyncAsync(CancellationToken.None).ConfigureAwait(false);
                    SyncWalDirIfNeeded();
                }
            }
            _logEmpty = false;
            _currentWalSize += record.Length;
            Interlocked.Add(ref _walsTotalSize, record.Length);
            if (_seqPerBatch) CacheRecoverableStateOf(group);
        }
        catch (Exception e)
        {
            // A failed WAL write leaves the WAL in an unknown state: stop accepting writes (bg error). The recovery
            // replaces the WAL and never writes to or syncs the failed one again. Its record may be complete in the
            // failed WAL while the write is not in the DB: fence the range from WAL iterators (WALIOStatusCheck ->
            // RecordIndeterminateWriteSequence).
            if (WalErrorStopsWrites(e))
            {
                WalRecoverySequences.Record(_logNumber, Util.Coding.DecodeFixed64(record.Span), last);
                SetBackgroundError(e, BackgroundErrorReason.WriteCallback, walRelated: true);
            }
            throw;
        }
        for (int i = 0; i < group.Count; i++)
        {
            WriteThread.Writer w = group[i];
            if (w.CallbackFailed) continue;
            // WriteGroupToWAL: CheckPostWalWriteCallback of every writer whose callback did not fail.
            w.CheckPostWalWriteCallback();
            // PreReleaseCallback of WriteCommittedTxn::PrepareInternal (MarkLogCallback) + log_used.
            if (w.Txn is { } t)
            {
                t.LogUsed = _logNumber;
                if (t.MarkLogContainingPrepSection) MarkLogAsContainingPrepSection(_logNumber);
            }
        }
        return record.Length;
    }

    /// <summary>
    /// PreReleaseCallback of a seq_per_batch group: called after the WAL write and before the memtable write, in sequence
    /// order.
    /// </summary>
    private static void RunGroupPreReleaseCallbacks(List<WriteThread.Writer> group, WriteThread.Writer leader)
    {
        int callbacks = 0;
        for (int i = 0; i < group.Count; i++)
        {
            if (group[i] is { CallbackFailed: false, Txn.PreRelease: not null }) callbacks++;
        }
        int index = 0;
        for (int i = 0; i < group.Count && callbacks > 0; i++)
        {
            WriteThread.Writer w = group[i];
            if (w.CallbackFailed || w.Txn is not { PreRelease: { } cb } t) continue;
            cb.Callback(t.SequenceUsed, t.DisableMemtable, leader.DisableWal ? 0 : t.LogUsed, index++, callbacks);
        }
    }

    /// <summary>The memtable inserts of a group by its leader (the serial part of DBImpl::WriteImpl), then publication.</summary>
    private void InsertGroupSerially(List<WriteThread.Writer> group, WriteThread.Writer leader, ulong total, ulong last, int walRecordBytes, bool needSync)
    {
        // A lone writer may ask for its (large) batch to be inserted in parallel ranges (DbImpl.SplitInsert.cs).
        Exception? insertError;
        if (group.Count == 1 && TryInsertBatchInParallel(group[0])) insertError = group[0].Error;
        else insertError = InsertWritersSerially(group);
        if (insertError is null)
        {
            // Don't publish a partial batch write
            if (total > 0)
            {
                Versions.SetLastSequence(last);
                NotifySequenceWaiters();
            }
        }
        else
        {
            // DBImpl::WriteImpl: the leader's status is the group's insert status (w.status = InsertInto(write_group)).
            leader.Error ??= insertError;
            // DBImpl::WriteImpl: "logs_.back().SetAttemptTruncateSize(wal_context.prev_size)". The group's record is in
            // the WAL but the group is not in the DB: closing the DB cuts the WAL back to its size before the record
            // (CloseAsync), so the next open does not replay the refused write.
            if (walRecordBytes > 0 && !leader.DisableWal && !_twoWriteQueues && _walPrevSize >= 0 && _walPrevSizeNumber == _logNumber &&
                _walAttemptTruncateSize < 0)
            {
                _walAttemptTruncateSize = _walPrevSize;
                _walAttemptTruncateNumber = _logNumber;
            }
            HandleMemTableInsertFailure(insertError);
        }
        RecordWriteGroup(group, total, walRecordBytes, !leader.DisableWal, needSync);
    }

    /// <summary>
    /// Port of DBImpl::HandleMemTableInsertFailure (db_impl_write.cc): a non-OK status on memtable insert indicates that
    /// the state implied by the WAL has diverged from the in-memory state (a corrupt write batch, or an invalid column
    /// family without ignore_missing_column_families): a fatal background error with reason kMemTable stops further
    /// writes. The group's sequence numbers are not published, so readers never see its partial inserts (they stay in the
    /// memtable above the last sequence until the DB is reopened).
    /// </summary>
    private void HandleMemTableInsertFailure(Exception e) => SetBackgroundError(e, BackgroundErrorReason.MemTable, walRelated: false);

    /// <summary>
    /// Port of the condition of DBImpl::WALIOStatusCheck: a failed WAL write or sync becomes a background error only
    /// with paranoid_checks (and not for Busy / Incomplete), or when the error is IOFenced. Otherwise the write just
    /// fails and the current WAL stays writable ("Force writable file to be continue writable":
    /// logs_.back().writer->file()->reset_seen_error()); returns false then.
    /// </summary>
    private bool WalErrorStopsWrites(Exception e)
    {
        RocksDbException status = RocksDbException.FromException(e);
        if ((Options.ParanoidChecks && status.Code is not (StatusCode.Busy or StatusCode.Incomplete)) || status.SubCode == StatusSubCode.IOFenced)
        {
            return true;
        }
        _log?.ResetSeenError();
        return false;
    }

    // wal_context.prev_size / LogWriterNumber::attempt_truncate_size (db_impl.h). Written by the write leader; the
    // truncate size is read when the DB closes (no writer is active then).
    private long _walPrevSize = -1;
    private ulong _walPrevSizeNumber;
    private long _walAttemptTruncateSize = -1;
    private ulong _walAttemptTruncateNumber;

    /// <summary>
    /// Port of LogWriterNumber::ClearWriter's truncation (db_impl.h): after a failed memtable insert the current WAL is
    /// cut back to its size before the failed write's record. Best effort, like upstream: errors are ignored and a file
    /// that cannot be truncated while open is left alone.
    /// </summary>
    private void MaybeTruncateWalOnClose(LogWriter log)
    {
        long size = _walAttemptTruncateSize;
        if (size < 0 || log.LogNumber != _walAttemptTruncateNumber) return;
        try
        {
            log.WriteBuffer();
            if (log.File is Env.ITruncatableWritableFile file && size < log.File.Size) file.Truncate(size);
        }
        catch (Exception e) when (e is IOException or RocksDbException)
        {
            LogWarn("Unable to truncate WAL " + FileNames.LogFileName(WalDir, log.LogNumber) + " -- " + e.Message);
        }
    }

    /// <summary>
    /// WriteBatchInternal::InsertInto(write_group): the memtable inserts of the group's writers, one after another. Stops
    /// at the first writer whose insert fails (its Error is set) and returns that error; the writers behind it are not
    /// inserted. Returns null when every insert succeeded.
    /// </summary>
    private Exception? InsertWritersSerially(List<WriteThread.Writer> group)
    {
        var inserter = new MemTableInserter(this, logNumber: 0, ignoreMissingColumnFamilies: false);
        for (int i = 0; i < group.Count; i++)
        {
            WriteThread.Writer w = group[i];
            if (w.CallbackFailed) continue;
            TransactionWriteContext? t = w.Txn;
            if (t is { DisableMemtable: true }) continue;
            WriteBatch mb = t?.MemtableBatch ?? w.Batch!;
            inserter.Sequence = _seqPerBatch ? t!.SequenceUsed : mb.Sequence;
            if (_seqPerBatch) mb.Sequence = inserter.Sequence;
            inserter.IgnoreMissingColumnFamilies = w.IgnoreMissingColumnFamilies;
            inserter.LogRef = t?.LogRef ?? 0;
            inserter.SetProtection(mb);
            try
            {
                inserter.Run(mb.Data);
                Debug.Assert(!_seqPerBatch || inserter.Sequence - t!.SequenceUsed == (ulong)t.BatchCount, "sub-batch count mismatch");
            }
            catch (Exception e)
            {
                w.Error = e;
                return e;
            }
        }
        return null;
    }

    // ---- parallel memtable writes (allow_concurrent_memtable_write) ----------------------------------------
    // State of the current parallel group: written by its leader before it wakes the followers, read by the writer that
    // finishes last (ordered by the wake-up and by the countdown).
    private ulong _parallelLast;
    private ulong _parallelTotal;
    private int _parallelWalBytes;
    private bool _parallelWal;
    private bool _parallelSynced;
    private int _parallelRemaining;
    private List<WriteThread.Writer>? _parallelGroup;

    /// <summary>
    /// LaunchParallelMemTableWriters: records the group's state for the writer that finishes last, re-arms the leader
    /// (completed by whichever writer finishes last) and wakes the other writers for their own inserts.
    /// </summary>
    private void LaunchParallelMemTableWriters(List<WriteThread.Writer> group, WriteThread.Writer leader, ulong total, ulong last,
        int walRecordBytes, bool wal, bool needSync)
    {
        _parallelGroup = group;
        _parallelLast = last;
        _parallelTotal = total;
        _parallelWalBytes = walRecordBytes;
        _parallelWal = wal;
        _parallelSynced = needSync;
        Volatile.Write(ref _parallelRemaining, group.Count);
        leader.Rearm();
        for (int i = 0; i < group.Count; i++)
        {
            if (group[i] != leader) group[i].Complete(WriteThread.StateParallelMemtableWriter);
        }
    }

    /// <summary>
    /// Whether the group's memtable inserts run in parallel (DBImpl::WriteImpl: allow_concurrent_memtable_write, more
    /// than one writer, no merge operands; like upstream, for groups of any size). Transaction writes (callbacks, separate
    /// memtable batches, prepared sections, sequence-per-batch), excluded writers and blob direct write rewrites keep the
    /// serial path.
    /// </summary>
    private bool CanInsertInParallel(List<WriteThread.Writer> group, int valid, ulong total)
    {
        if (!_allowConcurrentMemtableWrite || group.Count < 2 || valid != group.Count || _seqPerBatch || _bdwSwapped.Count > 0) return false;
        for (int i = 0; i < group.Count; i++)
        {
            WriteThread.Writer w = group[i];
            if (w.Txn is not null || w.Batch!.HasMerge) return false;
        }
        return true;
    }

    /// <summary>The memtable insert of one writer of a parallel group (WriteBatchInternal::InsertInto with concurrent_memtable_writes).</summary>
    private void ParallelMemTableInsert(WriteThread.Writer w)
    {
        var inserter = MemTableInserter.ForConcurrentWriter(this, w.IgnoreMissingColumnFamilies);
        WriteBatch b = w.Batch!;
        inserter.Sequence = b.Sequence;
        inserter.SetProtection(b);
        try
        {
            inserter.Run(b.Data);
        }
        catch (Exception e)
        {
            w.Error = e;
        }
        finally
        {
            inserter.PostProcess();
        }
        // Re-arm before counting down: the writer finishing last may complete this one right after.
        w.Rearm();
    }

    /// <summary>WriteThread::CompleteParallelMemTableWriter: true for the writer that finished last.</summary>
    private bool CompleteParallelMemTableWriter() => Interlocked.Decrement(ref _parallelRemaining) == 0;

    /// <summary>
    /// The end of a parallel group, run by the writer that finished last (DBImpl::WriteImpl after
    /// CompleteParallelMemTableWriter + ExitAsBatchGroupFollower): publishes the last sequence, then completes the other
    /// writers and hands leadership on.
    /// </summary>
    private void FinishParallelGroup(WriteThread.Writer self)
    {
        List<WriteThread.Writer> group = _parallelGroup!;
        // Don't publish a partial batch write. Upstream checks the status of the writer that finished last only (a TODO
        // there: "propagate status to write_group"); the port checks every writer of the group, so a failed insert of any
        // of them keeps the group unpublished and stops writes.
        Exception? insertError = null;
        for (int i = 0; i < group.Count && insertError is null; i++) insertError = group[i].Error;
        if (insertError is not null) HandleMemTableInsertFailure(insertError);
        else if (_parallelTotal > 0)
        {
            Versions.SetLastSequence(_parallelLast);
            NotifySequenceWaiters();
        }
        if (_pipelined)
        {
            // The stats were recorded by the WAL stage.
            _writeThread.ExitAsMemTableWriter(group, self);
            return;
        }
        RecordWriteGroup(group, _parallelTotal, _parallelWalBytes, _parallelWal, _parallelSynced);
        _writeThread.ExitAsParallelGroup(group, self);
    }

    // ---- blob direct write (leader-only state) -------------------------------------------------------------

    /// <summary>Some column family uses blob direct write (set once, never reset).</summary>
    private volatile bool _anyBlobDirectWrite;
    private Blob.BlobWriteBatchTransformer? _bdwTransformer;
    private readonly List<(WriteThread.Writer Writer, WriteBatch Original)> _bdwSwapped = new();

    /// <summary>
    /// MaybeTransformBatchForBlobDirectWrite for one writer of the group (after PreprocessWrite, so the blobs belong to
    /// the memtable the batch is inserted into): swaps in the transformed batch. Returns false when the writer failed
    /// (excluded from the group like a failed write callback; its blobs become garbage of their files).
    /// </summary>
    private bool TryTransformForBlobDirectWrite(WriteThread.Writer w)
    {
        Blob.BlobWriteBatchTransformer tr = _bdwTransformer ??= new Blob.BlobWriteBatchTransformer();
        WriteBatch? transformed;
        try
        {
            transformed = tr.Transform(w.Batch!, _cfById);
        }
        catch (Exception e) when (e is RocksDbException or IOException)
        {
            w.Error = e;
            w.CallbackFailed = true;
            return false;
        }
        if (transformed is not null)
        {
            _bdwSwapped.Add((w, w.Batch!));
            w.Batch = transformed;
        }
        return true;
    }

    private void RestoreBlobDirectWriteBatches()
    {
        foreach ((WriteThread.Writer w, WriteBatch original) in _bdwSwapped)
        {
            WriteBatch transformed = w.Batch!;
            original.Sequence = transformed.Sequence;
            w.Batch = original;
            transformed.Dispose();
        }
        _bdwSwapped.Clear();
    }

    private void SyncWalDirIfNeeded()
    {
        if (_walDirSynced) return;
        FileSystem.SyncDirectory(WalDir);
        _walDirSynced = true;
    }

    /// <summary>
    /// Port of DBImpl::PreprocessWrite: background error check, memtable switches for full memtables
    /// (ScheduleFlushes), and write stalls (DelayWrite with the "stop" condition only).
    /// </summary>
    private async ValueTask PreprocessWriteAsync(bool noSlowdown, bool synchronous, CancellationToken cancellationToken)
    {
        ThrowIfDbStopped();
        if (Volatile.Read(ref _walsTotalSize) > (long)GetMaxTotalWalSize() && Versions.ColumnFamilies.Count > 1)
        {
            // WaitForPendingWrites: with pipelined writes the memtables are switched only once no insert is pending.
            if (_pipelined) await WaitForMemTableWritersAsync(synchronous).ConfigureAwait(false);
            await WaitForPendingWritesAsync(synchronous).ConfigureAwait(false);
            await SwitchWalAsync(synchronous, cancellationToken).ConfigureAwait(false);
        }
        if (_flushScheduler.Count > 0)
        {
            // With pipelined writes the memtable stage may still add to the flush scheduler until it is drained.
            if (_pipelined) await WaitForMemTableWritersAsync(synchronous).ConfigureAwait(false);
            // unordered_write: likewise for the memtable inserts of earlier write groups (WaitForPendingWrites).
            await WaitForPendingWritesAsync(synchronous).ConfigureAwait(false);
            // ScheduleFlushes: with atomic_flush every column family with data is switched and one request flushes them.
            try
            {
                await SwitchMemtablesForFlushAsync(_flushScheduler, FlushReason.WriteBufferFull, synchronous, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _flushScheduler.Clear();
            }
        }
        WriteBufferManager wbm = WriteBufferManager;
        if (wbm.Enabled && (wbm.ShouldFlush() || wbm.ShouldStall()))
            await PreprocessWriteBufferManagerAsync(noSlowdown, synchronous, cancellationToken).ConfigureAwait(false);
        if (_maybeStalled)
        {
            long stallStart = Stopwatch.GetTimestamp();
            await DelayWriteAsync(noSlowdown, synchronous, cancellationToken, countGroupBytes: true).ConfigureAwait(false);
            RecordWriteStall(stallStart);
        }
    }

    /// <summary>
    /// Writes are stopped or delayed (memtable count, L0 file count, pending compaction bytes; see
    /// <see cref="RecalculateWriteStall"/>). REQUIRES: mutex.
    /// </summary>
    private bool IsWriteStalled() => IsWriteStoppedOrDelayed();

    // ---- memtable switch --------------------------------------------------------------------------------

    /// <summary>
    /// Port of DBImpl::SwitchMemtable: makes the CF's mutable memtable immutable, starts a new WAL (unless the current
    /// one is empty) and schedules a flush. REQUIRES: caller is the write leader (batched or unbatched).
    /// </summary>
    internal async ValueTask SwitchMemtableAsync(ColumnFamilyData cfd, FlushReason reason, bool synchronous, CancellationToken cancellationToken)
    {
        // two_write_queues: the WAL-only queue may be appending to the current WAL; the WAL is replaced under the WAL
        // write lock (logs_ / cur_wal_number_ are guarded by wal_write_mutex_ upstream).
        bool walLocked = false;
        if (_twoWriteQueues)
        {
            ValueTask l = AcquireWalWriteLockAsync(synchronous);
            if (!l.IsCompletedSuccessfully) await l.ConfigureAwait(false);
            walLocked = true;
        }
        try
        {
            await SwitchMemtableCoreAsync(cfd, reason, synchronous).ConfigureAwait(false);
        }
        finally
        {
            if (walLocked) _walWriteLock!.Release();
        }
    }

    private async ValueTask SwitchMemtableCoreAsync(ColumnFamilyData cfd, FlushReason reason, bool synchronous)
    {
        // Recoverable state is persisted in WAL. After memtable switch, WAL might be deleted, so we write the state to
        // memtable to be persisted as well.
        WriteRecoverableState();
        LogWriter? newLog = null;
        ulong newLogNumber;
        bool walFailed;
        lock (_mutex)
        {
            newLogNumber = _logNumber;
            // A WAL whose write or sync failed is replaced and never written or synced again (WAL write error recovery).
            walFailed = _log is not null && _logNumber <= _failedWalNumber;
        }
        ulong recycleLogNumber = 0;
        if (!_logEmpty || _log is null || walFailed)
        {
            ReleaseObsoleteClosedWals();
            // SwitchMemtable: reuse the oldest recycled WAL, unless file deletions are disabled (the rename would look
            // like a deletion) or it is still open (IsWalInLogs).
            if (Options.RecycleLogFileNum > 0) recycleLogNumber = PickRecycledWal();
            newLogNumber = Versions.NewFileNumber();
            try
            {
                LogWriter? old = _log;
                // SwitchMemtable: the new WAL records its predecessor (the current WAL: number, size so far including
                // buffered bytes, and the sequence of its last record) for track_and_verify_wals.
                PredecessorWalInfo? predecessor = old?.File is { } oldFile
                    ? new PredecessorWalInfo(old.LogNumber, (ulong)oldFile.Size, old.LastSequenceRecorded)
                    : null;
                try
                {
                    newLog = CreateWal(newLogNumber, predecessor, recycleLogNumber);
                }
                finally
                {
                    // Popped whether or not the reuse worked: it was held in the list only so that a concurrent purge
                    // does not delete it while it is renamed.
                    if (recycleLogNumber != 0)
                    {
                        lock (_mutex) _walRecycleFiles.Remove(recycleLogNumber);
                    }
                }
                if (old is not null && walFailed)
                {
                    // Its data is in the memtables, which the recovery flushes.
                    try { old.Dispose(); }
                    catch (Exception de) when (de is IOException or RocksDbException) { }
                }
                else if (old is not null && Versions.ColumnFamilies.Count == 1 && !Options.Allow2Pc)
                {
                    // SwitchMemtable: always flush the buffer of the last log before switching to a new one; it is synced
                    // later with the other logs_ entries (next sync write / SyncWAL / FlushWAL(sync)), like upstream.
                    // A crash may lose its unsynced tail together with the unsynced writes of the new WAL (recovery
                    // stops at the gap), exactly as upstream with a single column family.
                    if (synchronous) old.WriteBuffer();
                    else await old.WriteBufferAsync(CancellationToken.None).ConfigureAwait(false);
                    _closedWalsToSync.Add(old);
                }
                else if (old is not null)
                {
                    // With several column families (or 2PC) upstream syncs the closed WALs before each flush
                    // (FlushMemTableToOutputFile -> SyncClosedWals) so that a flushed SST never holds data of write
                    // batches whose updates to other column families are not durable. The port syncs the closed WAL
                    // here instead, right before its memtable can be flushed (at least as durable as upstream).
                    // Earlier closed WALs still waiting for a sync go first.
                    if (_closedWalsToSync.Count > 0) await SyncClosedWalsAsync(synchronous).ConfigureAwait(false);
                    if (synchronous) old.Sync();
                    else await old.SyncAsync(CancellationToken.None).ConfigureAwait(false);
                    // SyncWalImpl (via SyncClosedWals) also fsyncs the WAL directory when it was not synced since the
                    // closed WAL was created (need_wal_dir_sync = !wal_dir_synced_): a synced WAL whose directory entry
                    // can vanish is not durable. With a separate wal_dir the flush's DB directory fsync does not cover it.
                    SyncWalDirIfNeeded();
                    NoteClosedWalSynced(old);
                    if (synchronous) old.Dispose();
                    else await old.DisposeAsync().ConfigureAwait(false);
                }
            }
            catch (Exception e)
            {
                // SwitchMemtable: a failure to create the new WAL or to make the old one durable stops writes (bg error).
                if (newLog is not null)
                {
                    try { newLog.Dispose(); }
                    catch (Exception de) when (de is IOException or RocksDbException) { }
                    TryDeleteFile(FileNames.LogFileName(WalDir, newLogNumber));
                }
                SetBackgroundError(e, BackgroundErrorReason.MemTable, walRelated: true);
                throw;
            }
        }
        lock (_mutex)
        {
            if (newLog is not null)
            {
                if (_logNumber != 0) _aliveWalSizes[_logNumber] = _currentWalSize;
                _currentWalSize = 0;
                _log = newLog;
                _logNumber = newLogNumber;
                _logEmpty = true;
                _walDirSynced = false;
                _aliveWals.Add(newLogNumber);
            }
            // SwitchMemtable: "if (cf->IsEmpty()) ... cf->mem()->SetCreationSeq(versions_->LastSequence())" for every
            // column family, with or without a new WAL (the creation sequence orders memtables by age for the write
            // buffer manager's flush pick).
            ulong lastSequence = Versions.LastSequence;
            foreach (ColumnFamilyData c in Versions.ColumnFamilies)
            {
                if (c.Mem.IsEmpty && c.NumNotFlushed == 0) c.Mem.SetCreationSequence(lastSequence);
            }
            MemTable mem = cfd.Mem;
            // RotateCurrentGeneration: the blob direct write files of this memtable are registered by its flush.
            Blob.BlobDirectWriteGeneration? generation = cfd.BlobDirectWrite?.RotateGeneration();
            // A switch outside BeginAtomicSwitch / EndAtomicSwitch is complete (flushable) by itself.
            ulong cut = _atomicSwitchCut != 0 ? _atomicSwitchCut : ++_atomicCutNext;
            if (_atomicSwitchCut == 0) _atomicCutCompleted = cut;
            cfd.Imm.Add(new ImmutableMemTable(mem, newLogNumber) { FlushReason = reason, BlobGeneration = generation, AtomicCut = cut });
            mem.MarkImmutable();
            if (generation is not null) ProtectBlobDirectWriteFiles(cfd, mem, generation);
            if (HasListeners) EnqueueMemTableSealed(cfd, mem);
            cfd.Mem = cfd.ConstructNewMemtable(Versions.LastSequence);
            if (newLog is not null)
            {
                // All this is just optimization to delete logs that are no longer needed: if a CF is empty, it doesn't
                // need the old log to stay alive, so advance its log number (in memory only, like upstream).
                foreach (ColumnFamilyData c in Versions.ColumnFamilies)
                {
                    if (c.Mem.IsEmpty && c.NumNotFlushed == 0 && c.LogNumber < newLogNumber) c.LogNumber = newLogNumber;
                }
            }
            InstallSuperVersion(cfd);
            TrimMemTableHistory(cfd);
            _maybeStalled = IsWriteStalled();
            EnqueueFlush(cfd);
            MaybeScheduleFlush();
        }
    }

    /// <summary>
    /// Syncs (oldest first) and closes the closed WALs of <see cref="_closedWalsToSync"/> (the logs_ loop of
    /// DBImpl::WriteToWAL / SyncWAL); WALs whose data is already in table files (number below the minimum log to keep)
    /// are closed without a sync (FindObsoleteFiles moves them to logs_to_free_). Leader-only. A sync failure propagates
    /// (the caller turns it into a WAL background error) and keeps the remaining WALs queued.
    /// </summary>
    private async ValueTask SyncClosedWalsAsync(bool synchronous)
    {
        ulong minLog;
        lock (_mutex) minLog = MinLogNumberToKeepLive();
        List<LogWriter> wals = _closedWalsToSync;
        int done = 0;
        try
        {
            for (; done < wals.Count; done++)
            {
                LogWriter w = wals[done];
                if (w.LogNumber >= minLog)
                {
                    if (synchronous) w.Sync();
                    else await w.SyncAsync(CancellationToken.None).ConfigureAwait(false);
                    NoteClosedWalSynced(w);
                }
                CloseQuietly(w);
            }
        }
        finally
        {
            wals.RemoveRange(0, done);
        }
    }

    /// <summary>
    /// Releases, without syncing, the closed WALs whose data is in table files (FindObsoleteFiles moving logs_ entries
    /// below min_log_number to logs_to_free_), so that WALs never synced by a write do not accumulate. Their writers are
    /// closed by the next purge (<see cref="CloseFreedWals"/>), not here on the write path. Leader-only.
    /// </summary>
    private void ReleaseObsoleteClosedWals()
    {
        List<LogWriter> wals = _closedWalsToSync;
        if (wals.Count == 0) return;
        lock (_mutex)
        {
            ulong minLog = MinLogNumberToKeepLive();
            int n = 0;
            while (n < wals.Count && wals[n].LogNumber < minLog) _walsToFree.Add(wals[n++]);
            wals.RemoveRange(0, n);
        }
    }

    /// <summary>Closes the writers released by <see cref="ReleaseObsoleteClosedWals"/> (JobContext::logs_to_free).</summary>
    private void CloseFreedWals()
    {
        LogWriter[] freed;
        lock (_mutex)
        {
            if (_walsToFree.Count == 0) return;
            freed = _walsToFree.ToArray();
            _walsToFree.Clear();
            // Until they are closed they must not be recycled (PickRecycledWal).
            foreach (LogWriter w in freed) _walsBeingClosed.Add(w.LogNumber);
        }
        foreach (LogWriter w in freed) CloseQuietly(w);
        lock (_mutex)
        {
            foreach (LogWriter w in freed) _walsBeingClosed.Remove(w.LogNumber);
        }
    }

    /// <summary>WALs whose writer <see cref="CloseFreedWals"/> is closing outside the mutex (guarded by mutex).</summary>
    private readonly HashSet<ulong> _walsBeingClosed = new();

    /// <summary>
    /// The recycled WAL a memtable switch reuses (SwitchMemtable: the front of wal_recycle_files_ when file deletions
    /// are enabled and !IsWalInLogs), or 0. A WAL still waiting in the free list is closed first: a WAL that may be
    /// recycled must be closed before it is reused (FindObsoleteFiles). One still open elsewhere is not reused this
    /// time. The number stays in the list until the reuse is done. REQUIRES: write leader (owns
    /// <see cref="_closedWalsToSync"/>).
    /// </summary>
    private ulong PickRecycledWal()
    {
        ulong n;
        LogWriter? toClose = null;
        lock (_mutex)
        {
            if (_walRecycleFiles.Count == 0 || FileDeletionsDisabled) return 0;
            n = _walRecycleFiles[0];
            if (_walsBeingClosed.Contains(n) || n == _logNumber) return 0;
            for (int i = 0; i < _walsToFree.Count; i++)
            {
                if (_walsToFree[i].LogNumber == n)
                {
                    toClose = _walsToFree[i];
                    _walsToFree.RemoveAt(i);
                    break;
                }
            }
        }
        foreach (LogWriter w in _closedWalsToSync)
        {
            if (w.LogNumber == n) return 0;
        }
        if (toClose is not null) CloseQuietly(toClose);
        return n;
    }

    private static void CloseQuietly(LogWriter w)
    {
        try { w.Dispose(); }
        catch (Exception e) when (e is IOException or RocksDbException) { }
    }

    /// <summary>
    /// Port of DBImpl::GetMaxTotalWalSize: max_total_wal_size, or 4 x max_total_in_memory_state (the sum over column
    /// families of write_buffer_size * max_write_buffer_number) when it is 0.
    /// </summary>
    internal ulong GetMaxTotalWalSize()
    {
        ulong max = Options.MaxTotalWalSize;
        if (max > 0) return max;
        return 4 * Volatile.Read(ref _maxTotalInMemoryState);
    }

    /// <summary>max_total_in_memory_state_ (kept up to date on column family add/drop and SetOptions).</summary>
    private ulong _maxTotalInMemoryState;

    private void RecomputeMaxTotalInMemoryState()
    {
        ulong inMemory = 0;
        foreach (ColumnFamilyData? cfd in Volatile.Read(ref _cfById))
        {
            if (cfd is null || cfd.Dropped) continue;
            inMemory += cfd.Options.WriteBufferSize * (ulong)Math.Max(cfd.Options.MaxWriteBufferNumber, 0);
        }
        Volatile.Write(ref _maxTotalInMemoryState, inMemory);
    }

    /// <summary>
    /// Port of DBImpl::SwitchWAL: the alive WALs exceed max_total_wal_size, so switch the memtables of every column
    /// family whose data is in the oldest alive WAL (OldestLogToKeep &lt;= that WAL) and flush them (FlushReason
    /// kWalFull), letting the WAL be released. Done once per oldest WAL (getting_flushed). Only called with more than
    /// one column family (a single CF's memtables already bound its WALs). REQUIRES: write leader.
    /// With atomic_flush every column family with data is switched (SelectColumnFamiliesForAtomicFlush).
    /// Not ported: the 2PC check for outstanding prepared transactions in the oldest WAL (unable_to_release_oldest_log_).
    /// </summary>
    private async ValueTask SwitchWalAsync(bool synchronous, CancellationToken cancellationToken)
    {
        ulong oldest;
        var cfds = new List<ColumnFamilyData>();
        lock (_mutex)
        {
            if (_aliveWals.Count == 0) return;
            oldest = ulong.MaxValue;
            foreach (ulong n in _aliveWals) oldest = Math.Min(oldest, n);
            if (_walGettingFlushed == oldest) return;
            _walGettingFlushed = oldest;
            foreach (ColumnFamilyData cfd in Versions.ColumnFamilies)
            {
                if (cfd.Dropped) continue;
                if (cfd.LogNumber <= oldest) cfds.Add(cfd);
            }
        }
        LogInfo("Flushing all column families with data in WAL number " + oldest + ". Total log size is " + Volatile.Read(ref _walsTotalSize) +
                " while max_total_wal_size is " + GetMaxTotalWalSize());
        // An empty memtable is not switched: its log number advances with the next WAL switch (SwitchMemtable).
        await SwitchMemtablesForFlushAsync(cfds, FlushReason.WalFull, synchronous, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Test helper (TEST_SwitchMemtable): switches the default column family's memtable and WAL.</summary>
    internal void SwitchMemtableForTest()
    {
        WriteThread.Writer w = EnterUnbatchedAsync(synchronous: true, CancellationToken.None).GetAwaiter().GetResult();
        try
        {
            SwitchMemtableAsync(GetColumnFamilyData(DefaultColumnFamilyName), FlushReason.Test, synchronous: true, CancellationToken.None)
                .GetAwaiter().GetResult();
        }
        finally
        {
            ExitUnbatched(w, synchronous: true);
        }
    }

    /// <summary>Writeback granularity of a WAL without wal_bytes_per_sync (see <see cref="CreateWal"/>).</summary>
    private const long WalWritebackBytes = 1 << 20;

    /// <summary>
    /// Creates WAL #<paramref name="number"/> (DBImpl::CreateWAL + StartWALFile): the compression type record, then with
    /// track_and_verify_wals the predecessor-WAL-info record when <paramref name="predecessor"/> is known
    /// (Writer::MaybeAddPredecessorWALInfo). Like upstream, the first WAL of an open has no predecessor record.
    /// </summary>
    private LogWriter CreateWal(ulong number, PredecessorWalInfo? predecessor, ulong recycleLogNumber)
    {
        string path = FileNames.LogFileName(WalDir, number);
        var fileOptions = new WritableFileOptions
        {
            // The WAL is fdatasync'ed when a memtable switch closes it (SwitchMemtableAsync), on the write path. Without
            // wal_bytes_per_sync, its writeback is still started every WalWritebackBytes (sync_file_range WRITE, which
            // does not wait) so that this sync only has to wait for the last chunk instead of the whole file.
            BytesPerSync = Options.WalBytesPerSync != 0 ? (long)Math.Min(Options.WalBytesPerSync, long.MaxValue) : WalWritebackBytes,
            MaxBufferSize = (int)Math.Clamp(Options.WritableFileMaxBufferSize, 64 * 1024, int.MaxValue),
        };
        IWritableFile file;
        if (recycleLogNumber != 0)
        {
            // CreateWALWriter: ReuseWritableFile renames the recycled WAL and overwrites it in place (no truncation);
            // its old records past the new data carry the old log number and end the log when it is read.
            LogInfo("reusing log " + recycleLogNumber.ToString(CultureInfo.InvariantCulture) + " from recycle list");
            TestHooks.Invoke("DBImpl::CreateWAL:BeforeReuseWritableFile");
            file = FileSystem.ReuseWritable(path, FileNames.LogFileName(WalDir, recycleLogNumber), fileOptions);
        }
        else
        {
            file = FileSystem.CreateWritable(path, truncate: true, fileOptions);
        }
        try
        {
            // Any non-zero recycle_log_file_num (after sanitization) writes the recyclable record format, also for WALs
            // that are not reused.
            var log = new LogWriter(file, number, recycleLogFiles: Options.RecycleLogFileNum > 0, Options.ManualWalFlush, Options.WalCompression, 0);
            log.AddCompressionTypeRecord();
            if (Options.TrackAndVerifyWals && predecessor is { } info) log.AddPredecessorWalInfo(info);
            return log;
        }
        catch
        {
            file.Dispose();
            throw;
        }
    }

    // ---- memtable inserter ------------------------------------------------------------------------------

    /// <summary>
    /// Leader-side column family resolution for inserts (ColumnFamilyMemTablesImpl::Seek). Returns null for unknown or
    /// dropped CFs and for CFs whose data in WAL <paramref name="logNumber"/> was already flushed (recovery).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ColumnFamilyData? CfForInsert(uint id, ulong logNumber, bool ignoreMissing)
    {
        ColumnFamilyData?[] map = _cfById;
        ColumnFamilyData? c = id < (uint)map.Length ? map[id] : null;
        if (c is null || c.Dropped)
        {
            // Writes to dropped column families are ignored (like upstream); unknown ids are an error unless ignored.
            if (c is null && !ignoreMissing && logNumber == 0) ThrowHelper.InvalidArgument("Invalid column family specified in write batch");
            return null;
        }
        // MemTableInserter::SeekToColumnFamily (write_batch.cc) rejects writes to a disallow_memtable_writes column
        // family, live and during recovery (the open fails).
        if (c.Options.DisallowMemtableWrites) ThrowHelper.InvalidArgument("This column family has disallow_memtable_writes=true");
        // During recovery, skip data that was already flushed for this CF.
        if (logNumber != 0 && logNumber < c.LogNumber) return null;
        return c;
    }

    private void RegisterCfForWrite(ColumnFamilyData cfd)
    {
        ColumnFamilyData?[] old = _cfById;
        var map = new ColumnFamilyData?[Math.Max(old.Length, (int)cfd.Id + 1)];
        old.CopyTo(map, 0);
        map[cfd.Id] = cfd;
        Volatile.Write(ref _cfById, map);
        RecomputeMaxTotalInMemoryState();
        RefreshTimestampSizes(map);
        if (cfd.BlobDirectWrite is not null) _anyBlobDirectWrite = true;
        if (cfd.Options.DisallowMemtableWrites) _anyDisallowMemtableWrites = _checkUnvalidatedBatches = true;
    }

    private void UnregisterCfForWrite(ColumnFamilyData cfd)
    {
        ColumnFamilyData?[] map = (ColumnFamilyData?[])_cfById.Clone();
        if (cfd.Id < (uint)map.Length) map[cfd.Id] = null;
        Volatile.Write(ref _cfById, map);
        _checkUnvalidatedBatches = true;
        RecomputeMaxTotalInMemoryState();
        RefreshTimestampSizes(map);
    }

    /// <summary>Validates the column family ids of a user batch before it is committed (avoids partially applied batches).</summary>
    /// <summary>
    /// Rejects a batch before it reaches the WAL when it names an unknown column family (unless
    /// <paramref name="ignoreMissing"/>) or a disallow_memtable_writes one. Upstream rejects the latter when the batch is
    /// built (WriteBatch::Put(handle) -> GetColumnFamilyIdAndTimestampSize: InvalidArgument "This column family has
    /// disallow_memtable_writes=true"); the managed WriteBatch records ids only, so the check happens here, and
    /// SeekToColumnFamily (<see cref="CfForInsert"/>) remains the backstop.
    /// </summary>
    private void ValidateColumnFamilies(WriteBatch batch, bool ignoreMissing)
    {
        if (ignoreMissing && !_anyDisallowMemtableWrites) return;
        var v = new CfValidator(Volatile.Read(ref _cfById), ignoreMissing);
        WriteBatch.Iterate(batch.Data, ref v);
    }

    private readonly struct CfValidator(ColumnFamilyData?[] map, bool ignoreMissing) : IWriteBatchHandler
    {
        private void Check(uint id)
        {
            ColumnFamilyData? c = id < (uint)map.Length ? map[id] : null;
            if (c is null)
            {
                if (!ignoreMissing) ThrowHelper.InvalidArgument("Invalid column family specified in write batch");
                return;
            }
            if (c.Options.DisallowMemtableWrites) ThrowHelper.InvalidArgument("This column family has disallow_memtable_writes=true");
        }

        public bool Continue() => true;
        public void Put(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => Check(columnFamilyId);
        public void TimedPut(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ulong writeUnixTime) => Check(columnFamilyId);
        public void PutEntity(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> entity) => Check(columnFamilyId);
        public void Delete(uint columnFamilyId, ReadOnlySpan<byte> key) => Check(columnFamilyId);
        public void SingleDelete(uint columnFamilyId, ReadOnlySpan<byte> key) => Check(columnFamilyId);
        public void DeleteRange(uint columnFamilyId, ReadOnlySpan<byte> beginKey, ReadOnlySpan<byte> endKey) => Check(columnFamilyId);
        public void Merge(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => Check(columnFamilyId);
        public void PutBlobIndex(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> blobIndex) => Check(columnFamilyId);
        public void LogData(ReadOnlySpan<byte> blob) { }
        public void MarkBeginPrepare(bool unprepare) { }
        public void MarkEndPrepare(ReadOnlySpan<byte> xid) { }
        public void MarkNoop(bool emptyBatch) { }
        public void MarkCommit(ReadOnlySpan<byte> xid) { }
        public void MarkCommitWithTimestamp(ReadOnlySpan<byte> xid, ReadOnlySpan<byte> commitTimestamp) { }
        public void MarkRollback(ReadOnlySpan<byte> xid) { }
    }

    /// <summary>
    /// Applies WriteBatch records to memtables. Port of MemTableInserter (db/write_batch.cc), sequential (not concurrent)
    /// insertion:
    /// <list type="bullet">
    /// <item>Without seq_per_batch (WriteCommitted / plain DB) every key gets its own sequence number and 2PC uses
    /// write-after-commit: live writes ignore the markers; recovery rebuilds each prepared section into a recovered
    /// transaction (no memtable insert, no sequence numbers consumed), applies it when its commit marker is replayed,
    /// and drops it on a rollback marker.</item>
    /// <item>With seq_per_batch (WritePrepared / WriteUnprepared) all keys of a (sub-)batch share one sequence number:
    /// the sequence advances at batch boundaries (end-prepare, commit, rollback and non-leading Noop markers) and when a
    /// key repeats within the sub-batch (the memtable refuses a duplicate key+seq, MemTable::Add's TryAgain). Prepared
    /// and unprepared sections are inserted at prepare time and, during recovery, also rebuilt into recovered
    /// transactions with their sub-batch count; commit markers then only drop the recovered transaction.</item>
    /// <item><see cref="LogRef"/> makes the memtables keep the WAL holding the prepared section alive.</item>
    /// </list>
    /// Begin-prepare markers of another write policy fail with NotSupported (WriteBatchInternal::Iterate checks).
    /// </summary>
    /// <summary>
    /// A recovered batch hit an existing key+seq without seq_per_batch (MemTable::Add's TryAgain, twice in a row in
    /// WriteBatch::Iterate). Thrown by <see cref="MemTableInserter"/> with <see cref="MemTableInserter.FailOnDuplicateKeySeq"/>.
    /// </summary>
    private sealed class DuplicateKeySeqException() : Exception("two consecutive TryAgain in WriteBatch handler; this is either a software bug or data corruption.");

    /// <summary>
    /// A non-OK status of a MemTableInserter handler for an entry the DB refuses (an inverted DeleteRange, a Merge
    /// without merge operator). Live writes fail with it (the memtable now diverges from the WAL: a background error,
    /// HandleMemTableInsertFailure); WAL recovery reports it like upstream's InsertLogRecordToMemtable failure.
    /// </summary>
    private sealed class InsertRejectedException(StatusCode code, string message)
        : RocksDbException(code, (code == StatusCode.InvalidArgument ? "Invalid argument: " : "") + message);

    private struct MemTableInserter : IWriteBatchHandler
    {
        private readonly DbImpl _db;
        private readonly ulong _logNumber;
        public ulong Sequence;
        public bool IgnoreMissingColumnFamilies;
        /// <summary>WAL recovery: an existing key+seq fails the batch (<see cref="DuplicateKeySeqException"/>) instead of being skipped.</summary>
        public bool FailOnDuplicateKeySeq;
        /// <summary>log_number_ref_: WAL with the prepared section of the data being inserted (0 = none).</summary>
        public ulong LogRef;
        // rebuilding_trx_ / rebuilding_trx_seq_ / unprepared_batch_ (recovery only).
        private WriteBatch? _rebuilding;
        private ulong _rebuildingSeq;
        private bool _unpreparedBatch;
        // seq_per_batch_ / write_after_commit_ / write_before_prepare_ (as a marker policy).
        private readonly bool _seqPerBatch;
        private readonly bool _writeAfterCommit;
        private readonly WriteBatch.TxnMarkerPolicy _policy;
        private DuplicateDetector? _dupDetector;
        // prot_info_ of the batch being inserted (WriteBatch per key-value protection) and the next entry's index.
        private WriteBatch? _protBatch;
        private int _protIdx;

        /// <summary>set_prot_info: the protection info of <paramref name="batch"/> is used for its next <see cref="Run"/>.</summary>
        public void SetProtection(WriteBatch batch)
        {
            _protBatch = batch.ProtectionBytesPerKey != 0 ? batch : null;
            _protIdx = 0;
        }
        // concurrent_memtable_writes_: inserts use MemTable.AddConcurrently, whose counters are accumulated per memtable
        // (post_info_map_; one memtable at a time here, flushed when the batch moves to another) and applied by
        // PostProcess.
        private bool _concurrent;
        private MemTable? _postMem;
        private ColumnFamilyData? _postCfd;
        private MemTablePostProcessInfo _post;

        public MemTableInserter(DbImpl db, ulong logNumber, bool ignoreMissingColumnFamilies)
        {
            _db = db;
            _logNumber = logNumber;
            IgnoreMissingColumnFamilies = ignoreMissingColumnFamilies;
            Sequence = 0;
            LogRef = 0;
            _rebuilding = null;
            _rebuildingSeq = 0;
            _unpreparedBatch = false;
            _seqPerBatch = db._seqPerBatch;
            // Write after commit currently uses one seq per key (instead of per batch). So seq_per_batch being false
            // indicates write_after_commit approach; batch_per_txn being false indicates write_before_prepare.
            _writeAfterCommit = !_seqPerBatch;
            _policy = !_seqPerBatch ? WriteBatch.TxnMarkerPolicy.WriteCommitted
                : db._batchPerTxn ? WriteBatch.TxnMarkerPolicy.WritePrepared : WriteBatch.TxnMarkerPolicy.WriteUnprepared;
            _dupDetector = null;
            _protBatch = null;
            _protIdx = 0;
            _concurrent = false;
            _postMem = null;
            _postCfd = null;
            _post = default;
        }

        /// <summary>An inserter for one writer of a parallel memtable group (concurrent_memtable_writes).</summary>
        public static MemTableInserter ForConcurrentWriter(DbImpl db, bool ignoreMissingColumnFamilies)
        {
            var m = new MemTableInserter(db, logNumber: 0, ignoreMissingColumnFamilies);
            m._concurrent = true;
            return m;
        }

        /// <summary>
        /// MemTableInserter::PostProcess: applies the counters of concurrent inserts (MemTable::BatchPostProcess) and
        /// schedules a flush of the memtable when that made it full (CheckMemtableFull).
        /// </summary>
        public void PostProcess()
        {
            if (_postMem is not { } mem) return;
            mem.BatchPostProcess(_post);
            if (mem.ShouldScheduleFlush && mem.MarkFlushScheduled())
            {
                List<ColumnFamilyData> scheduler = _db._flushScheduler;
                lock (scheduler) scheduler.Add(_postCfd!);
            }
            _postMem = null;
            _postCfd = null;
            _post = default;
        }

        /// <summary>Iterates <paramref name="rep"/> into the memtables (WriteBatch::Iterate with this handler's option states).</summary>
        public void Run(ReadOnlySpan<byte> rep) => WriteBatch.Iterate(rep, ref this, _policy);

        /// <summary>Iterates whole records of a batch (no header, no count check) into the memtables, from <see cref="Sequence"/> on.</summary>
        public void RunRange(ReadOnlySpan<byte> records) => WriteBatch.IterateRecordRange(records, ref this, _policy);

        public readonly bool Continue() => true;

        /// <summary>
        /// MaybeAdvanceSeq: within a batch the sequence advances per key (default) or, with seq_per_batch, only at batch
        /// boundaries.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void MaybeAdvanceSeq(bool batchBoundary)
        {
            if (batchBoundary == _seqPerBatch) Sequence++;
        }

        /// <summary>Recovery of a WriteCommitted prepared section: the records only go to the rebuilt batch.</summary>
        private readonly bool RebuildOnly
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _rebuilding is not null && _writeAfterCommit;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Add(uint cf, EntryType type, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            // NextProtectionInfo: the batch's ProtectionInfoKVOC64 of this entry, without its column family (KVO).
            bool hasProt = false;
            ulong kvo = 0;
            if (_protBatch is not null && _protIdx < _protBatch.ProtectionInfo.Length)
            {
                kvo = KvChecksum.StripC(_protBatch.ProtectionInfo[_protIdx++], cf);
                hasProt = true;
            }
            ColumnFamilyData? c = _db.CfForInsert(cf, _logNumber, IgnoreMissingColumnFamilies);
            if (c is null)
            {
                if (_rebuilding is not null && !_writeAfterCommit)
                {
                    // The CF is probably flushed and hence no need for insert but we still need to keep track of the keys
                    // for upcoming rollback/commit (the caller adds the key to the rebuilt batch).
                    MaybeAdvanceSeq(IsDuplicateKeySeq(cf, key));
                }
                else
                {
                    MaybeAdvanceSeq(batchBoundary: false);
                }
                return;
            }
            // MemTableInserter::DeleteCF: a deletion in a column family with timestamps is kTypeDeletionWithTimestamp.
            if (type == EntryType.Deletion && c.TimestampSize != 0)
            {
                type = EntryType.DeletionWithTimestamp;
                if (hasProt) kvo = KvChecksum.UpdateO(kvo, EntryType.Deletion, EntryType.DeletionWithTimestamp);
            }
            MemTable mem = c.Mem;
            while (!AddToMemTable(c, mem, type, key, value, hasProt, kvo))
            {
                // key+seq exists (TryAgain). Without seq_per_batch this can only come from replaying an already applied
                // batch. WAL recovery fails the batch like upstream: MaybeAdvanceSeq(kBatchBoundary) does not advance,
                // and WriteBatch::Iterate's retry of the entry hits the second TryAgain ("two consecutive TryAgain"); the
                // sequence stays at this entry. Other callers ignore it. With seq_per_batch the key starts a new
                // sub-batch: advance and retry.
                if (!_seqPerBatch)
                {
                    if (FailOnDuplicateKeySeq) throw new DuplicateKeySeqException();
                    break;
                }
                MaybeAdvanceSeq(batchBoundary: true);
            }
            if (LogRef != 0) mem.RefLogContainingPrepSection(LogRef);
            // Concurrent inserts update the flush state in PostProcess (BatchPostProcess), which also schedules the flush.
            if (!_concurrent && mem.ShouldScheduleFlush && mem.MarkFlushScheduled()) _db._flushScheduler.Add(c);
            MaybeAdvanceSeq(batchBoundary: false);
        }

        /// <summary>
        /// The memtable write of MemTableInserter::PutCFImpl: plain Add, or with inplace_update_support MemTable::Update
        /// (Put / PutEntity / PutBlobIndex / TimedPut) and, for Puts with an inplace_callback, MemTable::UpdateCallback with
        /// the DB read fallback when the memtable has no value for the key. Returns false when the key+seq exists.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool AddToMemTable(ColumnFamilyData c, MemTable mem, EntryType type, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value,
            bool hasProt, ulong kvo)
        {
            if (_concurrent) return AddConcurrently(c, mem, type, key, value, hasProt, kvo);
            if (!mem.InplaceUpdateSupport || type is not (EntryType.Value or EntryType.WideColumnEntity or EntryType.BlobIndex or EntryType.ValuePreferredSeqno))
                return hasProt ? mem.Add(Sequence, type, key, value, true, KvChecksum.ProtectS(kvo, Sequence)) : mem.Add(Sequence, type, key, value);
            return AddInplace(c, mem, type, key, value);
        }

        private bool AddConcurrently(ColumnFamilyData c, MemTable mem, EntryType type, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value,
            bool hasProt, ulong kvo)
        {
            if (!ReferenceEquals(mem, _postMem))
            {
                PostProcess();
                _postMem = mem;
                _postCfd = c;
            }
            return hasProt
                ? mem.AddConcurrently(Sequence, type, key, value, ref _post, true, KvChecksum.ProtectS(kvo, Sequence))
                : mem.AddConcurrently(Sequence, type, key, value, ref _post);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private readonly bool AddInplace(ColumnFamilyData c, MemTable mem, EntryType type, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            InplaceUpdateCallback? callback = mem.InplaceCallback;
            if (callback is null || type != EntryType.Value) return mem.Update(Sequence, type, key, value);
            switch (mem.UpdateCallback(Sequence, key, value, callback))
            {
                case MemTable.UpdateCallbackResult.Done: return true;
                case MemTable.UpdateCallbackResult.AddFailed: return false;
            }
            // key not found in memtable. Do sst get, update, add (reading at the batch's sequence number; not during
            // recovery, where upstream has no DB to read from).
            using var prev = new Util.PooledBufferWriter(0);
            bool found = false;
            if (_logNumber == 0) found = _db.GetForInplaceCallback(c, key, Sequence, prev);
            using var merged = new Util.PooledBufferWriter(0);
            int prevSize = prev.WrittenCount;
            byte[]? copy = found ? prev.WrittenSpan.ToArray() : null;
            InplaceUpdateStatus status = callback(copy is null ? default : copy.AsSpan(), found, ref prevSize, value, merged);
            if (status == InplaceUpdateStatus.UpdatedInplace)
            {
                // prev_value is updated in-place with final value.
                if (copy is null || (uint)prevSize > (uint)copy.Length) ThrowHelper.InvalidArgument("inplace_callback returned UpdatedInplace without a valid existing value");
                return mem.Add(Sequence, EntryType.Value, key, copy.AsSpan(0, prevSize));
            }
            if (status == InplaceUpdateStatus.Updated) return mem.Add(Sequence, EntryType.Value, key, merged.WrittenSpan);
            return true;
        }

        /// <summary>IsDuplicateKeySeq (util/duplicate_detector.h): recovery of a column family whose insert is skipped.</summary>
        private bool IsDuplicateKeySeq(uint cf, ReadOnlySpan<byte> key) => (_dupDetector ??= new DuplicateDetector(_db)).IsDuplicateKeySeq(cf, key, Sequence);

        public void Put(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            // optimize for non-recovery mode
            if (RebuildOnly)
            {
                _rebuilding!.Put(columnFamilyId, key, value);
                return;
            }
            Add(columnFamilyId, EntryType.Value, key, value);
            _rebuilding?.Put(columnFamilyId, key, value);
        }

        [SkipLocalsInit]
        public void TimedPut(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ulong writeUnixTime)
        {
            if (RebuildOnly)
            {
                _rebuilding!.TimedPut(columnFamilyId, key, value, writeUnixTime);
                return;
            }
            // PackValueAndWriteTime: value | fixed64(write_unix_time), stored as kTypeValuePreferredSeqno.
            byte[] buf = RocksArrayPool<byte>.Shared.Rent(value.Length + 8);
            try
            {
                value.CopyTo(buf);
                Util.Coding.EncodeFixed64(buf.AsSpan(value.Length), writeUnixTime);
                Add(columnFamilyId, EntryType.ValuePreferredSeqno, key, buf.AsSpan(0, value.Length + 8));
            }
            finally
            {
                RocksArrayPool<byte>.Shared.Return(buf);
            }
            _rebuilding?.TimedPut(columnFamilyId, key, value, writeUnixTime);
        }

        public void PutEntity(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> entity)
        {
            if (RebuildOnly)
            {
                _rebuilding!.PutEntitySerialized(columnFamilyId, key, entity);
                return;
            }
            Add(columnFamilyId, EntryType.WideColumnEntity, key, entity);
            _rebuilding?.PutEntitySerialized(columnFamilyId, key, entity);
        }

        public void Delete(uint columnFamilyId, ReadOnlySpan<byte> key)
        {
            if (RebuildOnly)
            {
                _rebuilding!.Delete(columnFamilyId, key);
                return;
            }
            Add(columnFamilyId, EntryType.Deletion, key, default);
            _rebuilding?.Delete(columnFamilyId, key);
        }

        public void SingleDelete(uint columnFamilyId, ReadOnlySpan<byte> key)
        {
            if (RebuildOnly)
            {
                _rebuilding!.SingleDelete(columnFamilyId, key);
                return;
            }
            Add(columnFamilyId, EntryType.SingleDeletion, key, default);
            _rebuilding?.SingleDelete(columnFamilyId, key);
        }

        public void DeleteRange(uint columnFamilyId, ReadOnlySpan<byte> beginKey, ReadOnlySpan<byte> endKey)
        {
            if (RebuildOnly)
            {
                _rebuilding!.DeleteRange(columnFamilyId, beginKey, endKey);
                return;
            }
            // MemTableInserter::DeleteRangeCF (write_batch.cc): once the column family is found, a range whose end
            // comes before its start is an error (the write fails, see InsertRejectedException), and an empty range is
            // not applied: no entry, and the sequence number does not advance.
            if (_db.CfForInsert(columnFamilyId, _logNumber, IgnoreMissingColumnFamilies) is { } c)
            {
                int cmp = c.UserComparator.CompareWithoutTimestamp(beginKey, true, endKey, true);
                if (cmp > 0) throw new InsertRejectedException(StatusCode.InvalidArgument, "end key comes before start key");
                if (cmp == 0)
                {
                    // NextProtectionInfo() was consumed for the entry
                    if (_protBatch is not null && _protIdx < _protBatch.ProtectionInfo.Length) _protIdx++;
                    return;
                }
            }
            Add(columnFamilyId, EntryType.RangeDeletion, beginKey, endKey);
            _rebuilding?.DeleteRange(columnFamilyId, beginKey, endKey);
        }

        public void Merge(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            if (RebuildOnly)
            {
                _rebuilding!.Merge(columnFamilyId, key, value);
                return;
            }
            // MemTableInserter::MergeCF: a merge into a column family without a merge operator fails the write.
            if (_db.CfForInsert(columnFamilyId, _logNumber, IgnoreMissingColumnFamilies) is { MergeOperator: null })
            {
                throw new InsertRejectedException(StatusCode.InvalidArgument, "Merge requires `ColumnFamilyOptions::merge_operator != nullptr`");
            }
            Add(columnFamilyId, EntryType.Merge, key, value);
            _rebuilding?.Merge(columnFamilyId, key, value);
        }

        /// <summary>MemTableInserter::PutBlobIndexCF: the blob index is stored as a kTypeBlobIndex entry.</summary>
        public void PutBlobIndex(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> blobIndex)
        {
            if (RebuildOnly)
            {
                _rebuilding!.PutBlobIndex(columnFamilyId, key, blobIndex);
                return;
            }
            Add(columnFamilyId, EntryType.BlobIndex, key, blobIndex);
            _rebuilding?.PutBlobIndex(columnFamilyId, key, blobIndex);
        }

        public readonly void LogData(ReadOnlySpan<byte> blob) { }

        /// <summary>The batch iteration calls MarkBeginPrepare with unprepare set for a kTypeBeginUnprepareXID marker.</summary>
        public void MarkBeginPrepare(bool unprepare)
        {
            if (_logNumber == 0) return;
            // during recovery we rebuild a hollow transaction from all encountered prepare sections of the wal
            if (!_db.Options.Allow2Pc) ThrowHelper.NotSupported("WAL contains prepared transactions. Open with TransactionDB::Open().");
            if (_rebuilding is not null) ThrowHelper.Corruption("WAL: nested begin-prepare marker");
            // we are now iterating through a prepared section
            _rebuilding = new WriteBatch();
            _rebuildingSeq = Sequence;
            _unpreparedBatch = unprepare;
        }

        public void MarkEndPrepare(ReadOnlySpan<byte> xid)
        {
            if (_logNumber != 0)
            {
                if (_rebuilding is null) ThrowHelper.Corruption("WAL: end-prepare marker without a begin-prepare marker");
                // 0 will disable further checks (write after commit)
                int batchCnt = _writeAfterCommit ? 0 : (int)(Sequence - _rebuildingSeq + 1);
                _db.InsertRecoveredTransaction(_logNumber, xid, _rebuilding!, _rebuildingSeq, batchCnt, _unpreparedBatch);
                _unpreparedBatch = false;
                _rebuilding = null;
            }
            MaybeAdvanceSeq(batchBoundary: true);
        }

        public void MarkNoop(bool emptyBatch)
        {
            // A hack in pessimistic transaction could result into a noop at the start of the write batch, that should be
            // ignored. In the absence of Prepare markers, a kTypeNoop tag indicates the end of a batch (a write batch
            // that commits skipping the prepare phase).
            if (!emptyBatch) MaybeAdvanceSeq(batchBoundary: true);
        }

        public void MarkCommit(ReadOnlySpan<byte> xid)
        {
            if (_logNumber != 0)
            {
                // in recovery when we encounter a commit marker we lookup this transaction in our set of rebuilt
                // transactions and commit. The log containing the prepared section may have been released in the last
                // incarnation because the data was flushed to L0.
                RecoveredTransaction? trx = _db.GetRecoveredTransaction(xid);
                if (trx is not null)
                {
                    if (_writeAfterCommit)
                    {
                        // all inserts must reference this trx log number; CF log numbers prevent duplicate re-insertion.
                        LogRef = trx.LogNumber;
                        try
                        {
                            WriteBatch.Iterate(trx.Batch.Data, ref this);
                        }
                        finally
                        {
                            LogRef = 0;
                        }
                    }
                    // else the values are already inserted before the commit
                    _db.DeleteRecoveredTransaction(trx);
                }
            }
            // Live write-after-commit writes: the prepared batch is inserted by the commit write itself.
            MaybeAdvanceSeq(batchBoundary: true);
        }

        /// <summary>
        /// MemTableInserter::MarkCommitWithTimestamp: in recovery the rebuilt prepared batch gets the commit timestamp
        /// (UpdateTimestamps with the running column families' timestamp sizes) and is inserted like
        /// <see cref="MarkCommit"/>; live writes ignore the marker.
        /// </summary>
        public void MarkCommitWithTimestamp(ReadOnlySpan<byte> xid, ReadOnlySpan<byte> commitTimestamp)
        {
            if (_logNumber != 0)
            {
                RecoveredTransaction? trx = _db.GetRecoveredTransaction(xid);
                if (trx is not null)
                {
                    if (_writeAfterCommit)
                    {
                        trx.Batch.UpdateTimestamps(commitTimestamp, _db.TimestampSizeOf);
                        LogRef = trx.LogNumber;
                        try
                        {
                            WriteBatch.Iterate(trx.Batch.Data, ref this);
                        }
                        finally
                        {
                            LogRef = 0;
                        }
                    }
                    _db.DeleteRecoveredTransaction(trx);
                }
            }
            MaybeAdvanceSeq(batchBoundary: true);
        }

        public void MarkRollback(ReadOnlySpan<byte> xid)
        {
            // In non recovery we simply ignore this tag. The log containing the transaction's prep section may have been
            // released in the previous incarnation because we knew it had been rolled back.
            if (_logNumber != 0 && _db.GetRecoveredTransaction(xid) is { } trx) _db.DeleteRecoveredTransaction(trx);
            MaybeAdvanceSeq(batchBoundary: true);
        }
    }

    /// <summary>
    /// Port of DuplicateDetector (util/duplicate_detector.h): detects a key repeated within the current sub-batch
    /// sequence number (recovery of a column family whose memtable insert is skipped). Like upstream's
    /// std::set&lt;Slice, SetComparator&gt; (InitWithComp), keys are compared with the column family's comparator: keys it
    /// considers equal are duplicates even when their bytes differ (the memtable would have refused the second one).
    /// </summary>
    private sealed class DuplicateDetector(DbImpl db)
    {
        private ulong _batchSeq;
        private readonly Dictionary<uint, ISet<byte[]>> _keys = new();

        public bool IsDuplicateKeySeq(uint cf, ReadOnlySpan<byte> key, ulong seq)
        {
            if (_batchSeq != seq)
            {
                // it is a new batch
                _keys.Clear();
            }
            _batchSeq = seq;
            if (!_keys.TryGetValue(cf, out ISet<byte[]>? set)) _keys[cf] = set = NewSet(cf);
            if (set.Add(key.ToArray())) return false;
            _keys.Clear();
            set = NewSet(cf);
            set.Add(key.ToArray());
            _keys[cf] = set;
            return true;
        }

        /// <summary>InitWithComp: the key set of a column family, ordered by its comparator.</summary>
        private ISet<byte[]> NewSet(uint cf)
        {
            Comparator cmp = db.UserComparatorOf(cf);
            return cmp.CanKeysWithDifferentByteContentsBeEqual
                ? new SortedSet<byte[]>(Comparer<byte[]>.Create((a, b) => cmp.Compare(a, b)))
                : new HashSet<byte[]>(ByteArrayContentComparer.Instance);
        }
    }

    private sealed class ByteArrayContentComparer : IEqualityComparer<byte[]>
    {
        public static readonly ByteArrayContentComparer Instance = new();

        public bool Equals(byte[]? x, byte[]? y) => x.AsSpan().SequenceEqual(y);

        public int GetHashCode(byte[] obj)
        {
            var h = new HashCode();
            h.AddBytes(obj);
            return h.ToHashCode();
        }
    }
}
