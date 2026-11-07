using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace RocksDb.Managed.Db;

/// <summary>
/// unordered_write. Port of the unordered path of DBImpl::WriteImpl (db/db_impl/db_impl_write.cc): WriteImplWALOnly on
/// the main write queue with kDoAssignOrder + kDoPublishLastSeq, then DBImpl::UnorderedWriteMemtable, and the
/// pending_memtable_writes_ / switch_cv_ drain of DBImpl::WaitForPendingWrites (db_impl.h).
/// <list type="bullet">
/// <item>The write group only runs PreprocessWrite, the write callbacks, the sequence allocation, the WAL write, the
/// pre-release callbacks and the publication of the group's last sequence (in group order). It counts the writers that
/// still have to insert into the memtables in <see cref="_pendingMemtableWrites"/> and hands leadership on.</item>
/// <item>Every writer then inserts its own batch concurrently (concurrent memtable inserts, hence the
/// allow_concurrent_memtable_write requirement checked at open), independently of the following write groups, and
/// counts down.</item>
/// <item>Whatever needs the memtables quiescent (a memtable switch in PreprocessWrite, EnterUnbatched: flush, ingestion,
/// column family drop, error recovery, close) first waits until no insert is pending
/// (<see cref="WaitForPendingWritesAsync"/>). Only the main queue's leader (or unbatched leader) waits, and no new
/// insert can become pending while it holds the queue.</item>
/// </list>
/// As documented upstream (DBOptions::unordered_write), snapshots are no longer immutable: a sequence number is visible
/// once its group published it, while writes with lower sequence numbers may still be landing in the memtables. Writes
/// issued after a snapshot (larger sequence numbers) stay invisible to it, and a writer reads its own write once the
/// write returned (read-your-own-write).
/// </summary>
internal sealed partial class DbImpl
{
    /// <summary>immutable_db_options_.unordered_write.</summary>
    private readonly bool _unorderedWrite;
    /// <summary>pending_memtable_writes_: writers whose WAL group finished and whose memtable insert has not.</summary>
    private int _pendingMemtableWrites;
    /// <summary>switch_mutex_: guards <see cref="_pendingDrained"/>.</summary>
    private readonly Lock _switchLock = new();
    /// <summary>switch_cv_: completed when <see cref="_pendingMemtableWrites"/> drops to 0 (null when nobody waits).</summary>
    private TaskCompletionSource? _pendingDrained;

    /// <summary>Number of unordered memtable inserts in flight (test observability).</summary>
    internal int PendingMemtableWritesForTest => Volatile.Read(ref _pendingMemtableWrites);

    /// <summary>
    /// The unordered path of DBImpl::WriteImpl: joins a WAL write group on the main queue (WriteImplWALOnly with
    /// kDoAssignOrder and kDoPublishLastSeq), then inserts the batch into the memtables
    /// (<see cref="UnorderedWriteMemtable"/>) without waiting for the writes of other groups.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask WriteUnorderedAsync(WriteOptions? options, WriteBatch batch, TransactionWriteContext? txn, UserWriteCallback? userCallback,
        bool synchronous,
        CancellationToken cancellationToken)
    {
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
                Debug.Assert(state is WriteThread.StateCompleted or WriteThread.StateGroupLeader);
                if (state == WriteThread.StateGroupLeader)
                {
                    ValueTask lead = LeadUnorderedWalGroupAsync(w, synchronous, cancellationToken);
                    if (synchronous) lead.GetAwaiter().GetResult();
                    else await lead.ConfigureAwait(false);
                }
            }
            else
            {
                ValueTask lead = LeadUnorderedWalGroupAsync(w, synchronous, cancellationToken);
                if (synchronous) lead.GetAwaiter().GetResult();
                else await lead.ConfigureAwait(false);
            }
            // The group counted this writer in pending_memtable_writes_ iff it should write to the memtables; nothing
            // touches the writer between the end of the group and here.
            if (w.ShouldWriteToMemtable)
            {
                TestHooks.Invoke("DBImpl::WriteImpl:BeforeUnorderedWriteMemtable");
                UnorderedWriteMemtable(w);
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
    /// The leader half of WriteImplWALOnly with kDoPublishLastSeq (unordered_write): PreprocessWrite, write callbacks,
    /// sequence allocation and WAL record of the group, pre-release callbacks, publication of the group's last sequence,
    /// then pending_memtable_writes_ += the writers that write to the memtables, before the followers are released.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask LeadUnorderedWalGroupAsync(WriteThread.Writer leader, bool synchronous, CancellationToken cancellationToken)
    {
        List<WriteThread.Writer> group = _group;
        _writeThread.EnterAsBatchGroupLeader(leader, group);
        try
        {
            if (IsDisposed) ThrowDisposed();
            ValueTask pre = PreprocessWriteAsync(leader.NoSlowdown, synchronous, cancellationToken);
            if (!pre.IsCompletedSuccessfully) await pre.ConfigureAwait(false);

            int valid = CheckGroupCallbacks(group, out ulong total, out bool needSync, out WriteThread.Writer? firstValid);
            ValueTask<(ulong Last, int WalBytes)> commit = AllocateAndWriteGroupToWalAsync(group, leader, firstValid, valid, total, needSync, synchronous);
            (ulong last, int walRecordBytes) = commit.IsCompletedSuccessfully ? commit.Result : await commit.ConfigureAwait(false);
            TestHooks.Invoke("DBImpl::WriteImpl:UnorderedWriteAfterWriteWAL");
            if (_seqPerBatch && valid > 0) RunGroupPreReleaseCallbacks(group, leader);
            if (total > 0)
            {
                // kDoPublishLastSeq: the group's sequence numbers become visible before its memtable inserts.
                Versions.SetLastSequence(last);
                NotifySequenceWaiters();
            }
            RecordWriteGroup(group, total, walRecordBytes, !leader.DisableWal, needSync);
            int memtableWriters = 0;
            for (int i = 0; i < group.Count; i++)
            {
                if (group[i].ShouldWriteToMemtable) memtableWriters++;
            }
            // Counted before leadership is handed on: the next leader's WaitForPendingWrites sees them.
            if (memtableWriters > 0) Interlocked.Add(ref _pendingMemtableWrites, memtableWriters);
        }
        catch (Exception e)
        {
            for (int i = 0; i < group.Count; i++) group[i].Error ??= e;
        }
        finally
        {
            _writeThread.ExitAsBatchGroupLeader(group);
        }
    }

    /// <summary>
    /// Port of DBImpl::UnorderedWriteMemtable: inserts the writer's batch at its assigned sequence numbers with concurrent
    /// memtable inserts, then counts down pending_memtable_writes_ and wakes a waiting memtable switch at 0.
    /// </summary>
    private void UnorderedWriteMemtable(WriteThread.Writer w)
    {
        try
        {
            TransactionWriteContext? t = w.Txn;
            WriteBatch mb = w.MemtableBatch;
            var inserter = MemTableInserter.ForConcurrentWriter(this, w.IgnoreMissingColumnFamilies);
            inserter.Sequence = _seqPerBatch ? t!.SequenceUsed : mb.Sequence;
            if (_seqPerBatch) mb.Sequence = inserter.Sequence;
            inserter.LogRef = t?.LogRef ?? 0;
            inserter.SetProtection(mb);
            try
            {
                inserter.Run(mb.Data);
            }
            catch (Exception e)
            {
                w.Error = e;
            }
            finally
            {
                inserter.PostProcess();
            }
            if (w.DisableWal) Volatile.Write(ref _hasUnpersistedData, true);
        }
        finally
        {
            if (Interlocked.Decrement(ref _pendingMemtableWrites) == 0) SignalPendingWritesDrained();
        }
    }

    /// <summary>
    /// The notify half of switch_cv_: taking the lock orders this with a waiter that registered after it saw a non-zero
    /// count, so the wake-up is never lost.
    /// </summary>
    private void SignalPendingWritesDrained()
    {
        TaskCompletionSource? waiter;
        lock (_switchLock)
        {
            waiter = _pendingDrained;
            _pendingDrained = null;
        }
        waiter?.TrySetResult();
    }

    /// <summary>
    /// The unordered_write part of DBImpl::WaitForPendingWrites: waits until the writers that already wrote the WAL
    /// finished their memtable inserts. REQUIRES: the caller leads the main write queue (batched or unbatched), so no
    /// new insert becomes pending meanwhile.
    /// </summary>
    private ValueTask WaitForPendingWritesAsync(bool synchronous)
    {
        if (!_unorderedWrite || Volatile.Read(ref _pendingMemtableWrites) == 0) return default;
        TestHooks.Invoke("DBImpl::WaitForPendingWrites:BeforeBlock");
        TaskCompletionSource tcs;
        lock (_switchLock)
        {
            if (Volatile.Read(ref _pendingMemtableWrites) == 0) return default;
            // Continuations run asynchronously: the inserting writer that counted down to 0 must not run the memtable
            // switch (or whatever waits) inline.
            tcs = _pendingDrained ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }
        if (synchronous)
        {
            tcs.Task.GetAwaiter().GetResult();
            return default;
        }
        return new ValueTask(tcs.Task);
    }
}
