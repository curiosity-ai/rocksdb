using System.Diagnostics;
using System.Runtime.CompilerServices;
using RocksDb.Managed.Wal;

namespace RocksDb.Managed.Db;

/// <summary>
/// The second write queue of <c>two_write_queues</c>. Port of DBImpl::WriteImplWALOnly, ConcurrentWriteGroupToWAL and
/// the published-sequence bookkeeping of db/db_impl/db_impl_write.cc (nonmem_write_thread_, wal_write_mutex_,
/// last_seq_same_as_publish_seq_):
/// <list type="bullet">
/// <item>Writes that do not touch the memtables (<see cref="TransactionWriteContext.DisableMemtable"/>: 2PC prepares of
/// WriteCommitted, commit / rollback markers of WritePrepared and WriteUnprepared) are grouped on their own write thread
/// and never wait behind memtable inserts.</item>
/// <item>Both queues append to the same WAL: sequence numbers are allocated (FetchAddLastAllocatedSequence) and the
/// record is written under one WAL write lock, so WAL order is sequence order. The WAL-only queue only consumes
/// sequence numbers with seq_per_batch (kDoAssignOrder); otherwise its writes reuse the next sequence number like
/// upstream's kDontAssignOrder.</item>
/// <item>With seq_per_batch the main queue's last sequence (the memtables) no longer is what readers see: the commit
/// callbacks of the second queue publish sequence numbers (SetLastPublishedSequence) once the commit cache knows about
/// them. Snapshots and reads use <see cref="LastPublishedSequence"/>.</item>
/// <item>Exclusive operations (EnterUnbatched: memtable switch for flush, ingestion, column family changes, close) block
/// both queues; while they hold both, the last sequence is synced with the last allocated one and published.</item>
/// </list>
/// The PreprocessWrite of the second queue only waits for write stalls (DelayWrite); memtable switches stay on the main
/// queue, which takes the WAL write lock while it replaces the WAL.
/// </summary>
internal sealed partial class DbImpl
{
    /// <summary>two_write_queues_.</summary>
    private readonly bool _twoWriteQueues;
    /// <summary>last_seq_same_as_publish_seq_: false only with two write queues and seq_per_batch.</summary>
    private readonly bool _lastSeqSameAsPublishSeq;
    /// <summary>nonmem_write_thread_ (null without two write queues).</summary>
    private readonly WriteThread? _nonmemWriteThread;
    /// <summary>wal_write_mutex_ for the WAL writes of both queues (null without two write queues).</summary>
    private readonly SemaphoreSlim? _walWriteLock;
    private readonly List<WriteThread.Writer> _nonmemGroup = new();       // second-queue leader only
    private readonly WriteBatch _nonmemGroupBatch = new(4 << 10, 0);       // second-queue leader only

    internal bool TwoWriteQueues => _twoWriteQueues;

    /// <summary>
    /// GetLastPublishedSequence: the last sequence number visible to readers (snapshots, reads without a snapshot).
    /// </summary>
    internal ulong LastPublishedSequence
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => _lastSeqSameAsPublishSeq ? Versions.LastSequence : Versions.LastPublishedSequence;
    }

    /// <summary>DBImpl::SetLastPublishedSequence (the commit callbacks of a two-queue WritePrepared DB).</summary>
    internal void SetLastPublishedSequence(ulong seq) => Versions.SetLastPublishedSequence(seq);

    private ValueTask AcquireWalWriteLockAsync(bool synchronous)
    {
        SemaphoreSlim l = _walWriteLock!;
        if (synchronous)
        {
            l.Wait();
            return default;
        }
        Task t = l.WaitAsync();
        return t.IsCompletedSuccessfully ? default : new ValueTask(t);
    }

    /// <summary>
    /// EnterUnbatched of the second queue (after the main one): no WAL-only write is in progress until
    /// <see cref="ExitUnbatchedQueues"/>. Allocated sequence numbers are then all written, so the last sequence is
    /// synced with them and published.
    /// </summary>
    private async ValueTask EnterNonMemUnbatchedAsync(bool synchronous)
    {
        WriteThread.Writer nw = WriteThread.Rent(synchronous);
        nw.Prepare(null, null, synchronous);
        if (!_nonmemWriteThread!.JoinBatchGroup(nw))
        {
            int state = synchronous ? nw.WaitSync() : await nw.WaitAsync().ConfigureAwait(false);
            Debug.Assert(state == WriteThread.StateGroupLeader);
        }
        WriteThread.Return(nw, synchronous);
        SyncPublishedSequenceWhileUnbatched();
    }

    /// <summary>
    /// With both queues blocked: the last sequence includes the sequence numbers consumed by WAL-only writes and the
    /// published sequence includes everything written (SyncLastSequenceWithAllocated; ingestion's
    /// SetLastPublishedSequence). Publishing data of uncommitted WritePrepared batches is harmless: they are in the
    /// prepared heap, so readers skip them.
    /// </summary>
    private void SyncPublishedSequenceWhileUnbatched()
    {
        Versions.SyncLastSequenceWithAllocated();
        if (!_lastSeqSameAsPublishSeq && Versions.LastPublishedSequence < Versions.LastSequence) Versions.SetLastPublishedSequence(Versions.LastSequence);
    }

    /// <summary>Releases what EnterUnbatched took: the main queue and, with two write queues, the second one.</summary>
    private void ExitUnbatchedQueues()
    {
        if (_twoWriteQueues)
        {
            SyncPublishedSequenceWhileUnbatched();
            _nonmemWriteThread!.ExitAsLeader();
        }
        _writeThread.ExitAsLeader();
    }

    /// <summary>
    /// DBImpl::WriteImplWALOnly on the second queue: joins the WAL-only write group; its leader appends the group's
    /// record to the WAL and runs the pre-release callbacks.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask WriteWalOnlyAsync(WriteOptions? options, WriteBatch batch, TransactionWriteContext txn, bool synchronous, CancellationToken cancellationToken)
    {
        Statistics? stats = Options.Statistics;
        long writeStart = stats?.StartTimer() ?? 0;
        WriteThread.Writer w = WriteThread.Rent(synchronous);
        w.Prepare(batch, options, synchronous);
        w.Txn = txn;
        try
        {
            if (!_nonmemWriteThread!.JoinBatchGroup(w))
            {
                int state = synchronous ? w.WaitSync() : await w.WaitAsync().ConfigureAwait(false);
                if (state == WriteThread.StateCompleted)
                {
                    if (w.Error is { } e) ThrowWriteError(e);
                    return;
                }
                Debug.Assert(state == WriteThread.StateGroupLeader);
            }
            ValueTask lead = LeadWalOnlyGroupAsync(w, synchronous, cancellationToken);
            if (synchronous)
            {
                Debug.Assert(lead.IsCompleted);
                lead.GetAwaiter().GetResult();
            }
            else
            {
                await lead.ConfigureAwait(false);
            }
            if (w.Error is { } err) ThrowWriteError(err);
        }
        finally
        {
            WriteThread.Return(w, synchronous);
            stats?.RecordElapsedMicros(Histogram.DbWrite, writeStart);
        }
    }

    /// <summary>The leader half of WriteImplWALOnly (kDontPublishLastSeq; kDoAssignOrder iff seq_per_batch).</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask LeadWalOnlyGroupAsync(WriteThread.Writer leader, bool synchronous, CancellationToken cancellationToken)
    {
        List<WriteThread.Writer> group = _nonmemGroup;
        _nonmemWriteThread!.EnterAsBatchGroupLeader(leader, group);
        try
        {
            if (IsDisposed) ThrowDisposed();
            ThrowIfDbStopped();
            if (_maybeStalled)
            {
                // WriteImplWALOnly: DelayWrite(num_bytes = 0) only; the main queue switches memtables.
                long stallStart = Stopwatch.GetTimestamp();
                await DelayWriteAsync(leader.NoSlowdown, synchronous, cancellationToken, countGroupBytes: false).ConfigureAwait(false);
                RecordWriteStall(stallStart);
            }
            int valid = 0;
            ulong seqInc = 0;
            bool needSync = false;
            WriteThread.Writer? firstValid = null;
            int callbacks = 0;
            for (int i = 0; i < group.Count; i++)
            {
                WriteThread.Writer w = group[i];
                TransactionWriteContext t = w.Txn!;
                if (t.Callback is { } cb)
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
                valid++;
                firstValid ??= w;
                // kDoAssignOrder (seq_per_batch): every writer consumes its batch_cnt sequence numbers.
                if (_seqPerBatch) seqInc += (uint)t.BatchCount;
                needSync |= w.Sync;
                if (t.PreRelease is not null) callbacks++;
            }
            if (valid == 0) return;

            int walBytes = 0;
            if (!leader.DisableWal)
            {
                ValueTask l = AcquireWalWriteLockAsync(synchronous);
                if (!l.IsCompletedSuccessfully) await l.ConfigureAwait(false);
                try
                {
                    ulong first = AssignWalOnlySequences(group, seqInc);
                    // MergeBatch: WAL parts only (WriteBatchInternal::Append(wal_only = true), WAL termination points).
                    ReadOnlyMemory<byte> record;
                    if (valid == 1 && !firstValid!.Batch!.HasWalTerminationPoint)
                    {
                        record = firstValid.Batch.DataMemory;
                    }
                    else
                    {
                        _nonmemGroupBatch.SetContentsWalOnly(firstValid!.Batch!);
                        for (int i = 0; i < group.Count; i++)
                        {
                            WriteThread.Writer w = group[i];
                            if (w == firstValid || w.CallbackFailed) continue;
                            WriteBatch.Append(_nonmemGroupBatch, w.Batch!, walOnly: true);
                        }
                        record = _nonmemGroupBatch.DataMemory;
                    }
                    walBytes = record.Length;
                    ulong last = first + seqInc - 1;
                    try
                    {
                        LogWriter log = _log!;
                        if (_cfTimestampSizes is { } tsSizes) log.MaybeAddUserDefinedTimestampSizeRecord(tsSizes);
                        if (synchronous)
                        {
                            log.AddRecord(record.Span, last);
                            if (needSync)
                            {
                                // Requesting sync with two_write_queues is expected to be very rare (upstream calls
                                // SyncWAL); the record is synced while the WAL write lock is held, after the closed WALs
                                // (every logs_ entry, oldest first).
                                if (_closedWalsToSync.Count > 0) SyncClosedWalsAsync(synchronous: true).GetAwaiter().GetResult();
                                log.Sync();
                                SyncWalDirIfNeeded();
                            }
                        }
                        else
                        {
                            await log.AddRecordAsync(record, last, CancellationToken.None).ConfigureAwait(false);
                            if (needSync)
                            {
                                if (_closedWalsToSync.Count > 0) await SyncClosedWalsAsync(synchronous: false).ConfigureAwait(false);
                                await log.SyncAsync(CancellationToken.None).ConfigureAwait(false);
                                SyncWalDirIfNeeded();
                            }
                        }
                        _logEmpty = false;
                        // wals_total_size_ (both queues write the WAL under the WAL write lock).
                        _currentWalSize += record.Length;
                        Interlocked.Add(ref _walsTotalSize, record.Length);
                        if (_seqPerBatch) CacheRecoverableStateOf(group);
                    }
                    catch (Exception e)
                    {
                        SetBackgroundError(e, BackgroundErrorReason.WriteCallback, walRelated: true);
                        throw;
                    }
                    ulong logNumber = _logNumber;
                    for (int i = 0; i < group.Count; i++)
                    {
                        if (group[i] is { CallbackFailed: false, Txn: { } t })
                        {
                            // MarkLogCallback of WriteCommittedTxn::PrepareInternal + log_used.
                            t.LogUsed = logNumber;
                            if (t.MarkLogContainingPrepSection) MarkLogAsContainingPrepSection(logNumber);
                        }
                    }
                }
                finally
                {
                    _walWriteLock!.Release();
                }
            }
            else
            {
                // Otherwise we inc seq number to do solely the seq allocation. Taken under the WAL write lock too, so that
                // WriteRecoverableState (which reads the last allocated sequence and then consumes the following ones
                // under that lock) cannot interleave with it.
                ValueTask l = AcquireWalWriteLockAsync(synchronous);
                if (!l.IsCompletedSuccessfully) await l.ConfigureAwait(false);
                try
                {
                    AssignWalOnlySequences(group, seqInc);
                }
                finally
                {
                    _walWriteLock!.Release();
                }
            }

            // PreReleaseCallback (is_mem_disabled = true: the commit callbacks publish from this queue only, in order).
            int index = 0;
            for (int i = 0; i < group.Count && callbacks > 0; i++)
            {
                WriteThread.Writer w = group[i];
                if (w.CallbackFailed || w.Txn is not { PreRelease: { } cb } t) continue;
                cb.Callback(t.SequenceUsed, isMemDisabled: true, leader.DisableWal ? 0 : t.LogUsed, index++, callbacks);
            }
            RecordWriteGroup(group, 0, leader.DisableWal ? 0 : walBytes, !leader.DisableWal, needSync);
        }
        catch (Exception e)
        {
            for (int i = 0; i < group.Count; i++) group[i].Error ??= e;
        }
        finally
        {
            _nonmemWriteThread.ExitAsBatchGroupLeader(group);
        }
    }

    /// <summary>
    /// Allocates <paramref name="seqInc"/> sequence numbers and gives every valid writer its first one (with
    /// kDontAssignOrder all writers get the next, unconsumed, sequence number). Returns the first sequence number.
    /// </summary>
    private ulong AssignWalOnlySequences(List<WriteThread.Writer> group, ulong seqInc)
    {
        ulong first = Versions.AllocateSequence(seqInc);
        ulong curr = first;
        for (int i = 0; i < group.Count; i++)
        {
            WriteThread.Writer w = group[i];
            if (w.CallbackFailed) continue;
            TransactionWriteContext t = w.Txn!;
            w.Batch!.Sequence = curr;
            t.SequenceUsed = curr;
            if (_seqPerBatch) curr += (uint)t.BatchCount;
        }
        return first;
    }

    // ---- cached recoverable state (use_only_the_last_commit_time_batch_for_recovery) -------------------------

    // cached_recoverable_state_ / cached_recoverable_state_empty_: written by the WAL writers (main-queue leader, or under
    // the WAL write lock with two write queues), consumed by WriteRecoverableState before a memtable switch.
    private WriteBatch? _cachedRecoverableState;
    private volatile bool _cachedRecoverableStateEmpty = true;

    /// <summary>
    /// recoverable_state_pre_release_callback_: commits each sub-batch of the recoverable state written to the memtables
    /// (WritePreparedTxnDB's CommitSubBatchPreReleaseCallback).
    /// </summary>
    internal IPreReleaseCallback? RecoverableStatePreReleaseCallback { get; set; }

    internal bool HasCachedRecoverableState => !_cachedRecoverableStateEmpty;

    /// <summary>
    /// MergeBatch's to_be_cached_state: after the group's WAL record was written, remembers the last batch of the
    /// group marked <see cref="WriteBatch.IsLatestPersistentState"/>.
    /// </summary>
    private void CacheRecoverableStateOf(List<WriteThread.Writer> group)
    {
        for (int i = group.Count - 1; i >= 0; i--)
        {
            WriteThread.Writer w = group[i];
            if (w.CallbackFailed || w.Batch is not { IsLatestPersistentState: true } b) continue;
            WriteBatch c = _cachedRecoverableState ??= new WriteBatch();
            c.SetContents(b.Data);
            _cachedRecoverableStateEmpty = false;
            return;
        }
    }

    /// <summary>
    /// DBImpl::WriteRecoverableState: the recoverable state is persisted in the WAL; after a memtable switch that WAL
    /// might be deleted, so the state is written to the memtables (at new sequence numbers, published at once) to be
    /// persisted by the flush. REQUIRES: the caller is the main write leader (and holds the WAL write lock with two
    /// write queues).
    /// </summary>
    private void WriteRecoverableState()
    {
        if (_cachedRecoverableStateEmpty) return;
        Debug.Assert(_seqPerBatch, "Only for write-prepared and write-unprepared.");
        WriteBatch state = _cachedRecoverableState!;
        ulong seq = _twoWriteQueues ? Versions.LastAllocatedSequence : Versions.LastSequence;
        state.Sequence = seq + 1;
        var inserter = new MemTableInserter(this, logNumber: 0, ignoreMissingColumnFamilies: true) { Sequence = seq + 1 };
        inserter.Run(state.Data);
        ulong nextSeq = inserter.Sequence;
        ulong lastSeq = nextSeq - 1;
        if (_twoWriteQueues)
        {
            Versions.AllocateSequence(lastSeq - seq);
            Versions.SetLastPublishedSequence(lastSeq);
        }
        Versions.SetLastSequence(lastSeq);
        if (RecoverableStatePreReleaseCallback is { } cb)
        {
            for (ulong subBatchSeq = seq + 1; subBatchSeq < nextSeq; subBatchSeq++) cb.Callback(subBatchSeq, isMemDisabled: false, logNumber: 0, 0, 1);
        }
        state.Clear();
        _cachedRecoverableStateEmpty = true;
    }
}
