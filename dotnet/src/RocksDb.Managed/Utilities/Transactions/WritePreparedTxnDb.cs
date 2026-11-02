using System.Runtime.CompilerServices;
using RocksDb.Managed.Db;

namespace RocksDb.Managed.Utilities.Transactions;

/// <summary>How a read callback relates to a DB snapshot (SnapshotBackup, write_prepared_txn_db.h).</summary>
internal enum SnapshotBackup : byte
{
    /// <summary>The read is not protected by a snapshot: it must be validated afterwards (ValidateSnapshot).</summary>
    UnbackedByDbSnapshot,
    BackedByDbSnapshot,
}

/// <summary>
/// The commit bookkeeping of a WritePrepared / WriteUnprepared transaction DB. Port of WritePreparedTxnDB
/// (utilities/transactions/write_prepared_txn_db.{h,cc}): data is written to the memtables at prepare time, so a reader
/// must know whether the transaction that wrote a sequence number committed before its snapshot.
/// <list type="bullet">
/// <item><b>Commit cache</b>: a lock-free array indexed by prepare_seq % size, each slot an atomic 64-bit
/// <c>CommitEntry64b</c> (prepare seq upper bits | commit delta). Evicting an entry advances
/// <c>max_evicted_seq</c>: anything at or below it that is not still prepared is committed.</item>
/// <item><b>Prepared heap</b> (<c>prepared_txns_</c>) of uncommitted prepare sequences, and <c>delayed_prepared_</c>
/// for those that fell below max_evicted_seq while still prepared (with <c>delayed_prepared_commits_</c>).</item>
/// <item><b>old_commit_map</b>: for evicted entries that overlap a live snapshot (prepared before it, committed
/// after it), the snapshot -&gt; prepare seqs not visible to it. Snapshots are mirrored in a small lock-free cache plus
/// a list (UpdateSnapshots / CheckAgainstSnapshots).</item>
/// <item><see cref="IsInSnapshot"/> combines them; reads go through <see cref="WritePreparedTxnReadCallback"/>,
/// compaction through <see cref="WritePreparedSnapshotChecker"/>.</item>
/// </list>
/// With two_write_queues the commit callbacks run on the WAL-only queue and publish the commit sequence numbers
/// (SetLastPublishedSequence); see DbImpl.TwoWriteQueues.cs.
/// </summary>
internal sealed class WritePreparedTxnDb : SeqPerBatchHooks
{
    private readonly DbImpl _db;
    private readonly TransactionDatabase _txnDb;
    private readonly Statistics? _stats;

    // SNAPSHOT_CACHE_BITS / SIZE, COMMIT_CACHE_BITS / SIZE, FORMAT (CommitEntry64bFormat).
    private readonly int _snapshotCacheSize;
    private readonly int _commitCacheBits;
    private readonly ulong _commitCacheSize;
    private const int PadBits = 8;
    private readonly int _commitBits;
    private readonly ulong _commitFilter;
    private readonly ulong _deltaUpperBound;
    private readonly ulong _incStepForMaxEvicted;

    private readonly long[] _commitCache;            // std::atomic<CommitEntry64b>[]
    private readonly long[] _snapshotCache;          // std::atomic<SequenceNumber>[]
    private int _snapshotsTotal;                     // snapshots_total_
    private readonly List<ulong> _snapshots = new(); // snapshots_ (beyond the cache), guarded by _snapshotsLock
    private List<ulong> _snapshotsAll = new();       // snapshots_all_
    private ulong _snapshotsVersion;                 // snapshots_version_
    private readonly Lock _snapshotsLock = new();    // snapshots_mutex_

    private readonly PreparedHeap _preparedTxns = new();
    private long _maxEvictedSeq;                     // max_evicted_seq_
    private long _futureMaxEvictedSeq;               // future_max_evicted_seq_
    private readonly SortedDictionary<ulong, List<ulong>> _oldCommitMap = new(); // old_commit_map_
    private readonly SortedSet<ulong> _delayedPrepared = new();                    // delayed_prepared_
    private readonly Dictionary<ulong, ulong> _delayedPreparedCommits = new();     // delayed_prepared_commits_
    private volatile bool _delayedPreparedEmpty = true;
    private volatile bool _oldCommitMapEmpty = true;
    private readonly Lock _preparedLock = new();      // prepared_mutex_
    private readonly Lock _oldCommitMapLock = new();  // old_commit_map_mutex_

    private readonly WritePreparedSnapshotChecker _checker;

    [ThreadStatic] private static WritePreparedTxnReadCallback? t_readCallback;

    public WritePreparedTxnDb(DbImpl db, TransactionDatabase txnDb, TransactionDbOptions options)
    {
        _db = db;
        _txnDb = txnDb;
        _stats = db.Options.Statistics;
        if (options.WpSnapshotCacheBits is < 0 or > 30) throw new ArgumentOutOfRangeException(nameof(options), "WpSnapshotCacheBits must be in [0, 30]");
        if (options.WpCommitCacheBits is < 0 or > 30) throw new ArgumentOutOfRangeException(nameof(options), "WpCommitCacheBits must be in [0, 30]");
        _snapshotCacheSize = 1 << options.WpSnapshotCacheBits;
        _commitCacheBits = options.WpCommitCacheBits;
        _commitCacheSize = 1UL << _commitCacheBits;
        int prepBits = 64 - PadBits - _commitCacheBits;
        _commitBits = 64 - prepBits;
        _commitFilter = (1UL << _commitBits) - 1;
        _deltaUpperBound = 1UL << _commitBits;
        // Init: INC_STEP_FOR_MAX_EVICTED = max(COMMIT_CACHE_SIZE / 100, 1)
        _incStepForMaxEvicted = Math.Max(_commitCacheSize / 100, 1);
        _commitCache = new long[_commitCacheSize];
        _snapshotCache = new long[_snapshotCacheSize];
        _checker = new WritePreparedSnapshotChecker(this);
        RollbackMergeOperands = options.RollbackMergeOperands;
    }

    internal DbImpl Db => _db;

    /// <summary>TransactionDBOptions::rollback_merge_operands.</summary>
    internal bool RollbackMergeOperands { get; }

    internal ulong MaxEvictedSeq => (ulong)Volatile.Read(ref _maxEvictedSeq);

    /// <summary>The prepared heap's push/pop lock (AddPreparedCallback adds the sub-batches of a write under one lock).</summary>
    internal Lock PreparedHeapLock => _preparedTxns.PushPopLock;

    private void RecordTick(Ticker t) => _stats?.RecordTick(t);

    // =====================================================================================================
    // CommitEntry64b
    // =====================================================================================================

    internal readonly record struct CommitEntry(ulong PrepSeq, ulong CommitSeq);

    private long Encode(ulong ps, ulong cs)
    {
        ulong delta = cs - ps + 1; // make initialized delta always >= 1
        if (delta >= _deltaUpperBound)
        {
            throw new InvalidOperationException("commit_seq >> prepare_seq. The allowed distance is " + _deltaUpperBound + " commit_seq is " + cs
                + " prepare_seq is " + ps);
        }
        ulong rep = (ps << PadBits) & ~_commitFilter;
        return (long)(rep | delta);
    }

    private bool Parse(ulong indexedSeq, long rep64, out CommitEntry entry)
    {
        ulong rep = (ulong)rep64;
        ulong delta = rep & _commitFilter;
        if (delta == 0)
        {
            entry = default;
            return false; // initialized entry would have non-zero delta
        }
        ulong prepUp = (rep & ~_commitFilter) >> PadBits;
        ulong prep = prepUp | indexedSeq;
        entry = new CommitEntry(prep, prep + delta - 1);
        return true;
    }

    /// <summary>GetCommitEntry: the entry at <paramref name="indexedSeq"/> (and its raw 64-bit form).</summary>
    private bool GetCommitEntry(ulong indexedSeq, out long entry64, out CommitEntry entry)
    {
        entry64 = Volatile.Read(ref _commitCache[indexedSeq]);
        return Parse(indexedSeq, entry64, out entry);
    }

    /// <summary>ExchangeCommitEntry: CAS of the slot.</summary>
    private bool ExchangeCommitEntry(ulong indexedSeq, long expected, CommitEntry newEntry) =>
        Interlocked.CompareExchange(ref _commitCache[indexedSeq], Encode(newEntry.PrepSeq, newEntry.CommitSeq), expected) == expected;

    // =====================================================================================================
    // IsInSnapshot
    // =====================================================================================================

    /// <summary>
    /// WritePreparedTxnDB::IsInSnapshot: whether the data written at <paramref name="prepSeq"/> is visible to
    /// <paramref name="snapshotSeq"/>. Sets <paramref name="snapReleased"/> when the answer depends on information that
    /// was dropped because the snapshot was released.
    /// </summary>
    public bool IsInSnapshot(ulong prepSeq, ulong snapshotSeq, ulong minUncommitted, ref bool snapReleased)
    {
        // Here we try to infer the return value without looking into prepare list. This would help avoiding
        // synchronization over a shared map.
        if (prepSeq == 0) return true; // We update the prepare_seq at compaction to zero for perf reasons.
        if (snapshotSeq < prepSeq) return false; // snapshot_seq < prep_seq <= commit_seq => snapshot_seq < commit_seq
        if (prepSeq < minUncommitted) return true;
        // Commit of delayed prepared has two non-atomic steps: add to commit cache, remove from delayed prepared. Our
        // reads from these two is also non-atomic. By looking into commit cache first thus we might not find the prep_seq
        // neither in commit cache not in delayed_prepared_. To fix that i) we check if there was any delayed prepared
        // BEFORE looking into commit cache, ii) if there was, we complete the search steps to be these: i)
        // commit_cache, ii) delayed prepared, commit cache again. In this way if the first query to commit cache missed
        // the commit, the 2nd will catch it.
        bool wasEmpty;
        ulong maxEvictedSeqLb, maxEvictedSeqUb;
        ulong indexedSeq = prepSeq % _commitCacheSize;
        int repeats = 0;
        CommitEntry cached;
        do
        {
            repeats++;
            if (repeats >= 100)
            {
                throw new InvalidOperationException("The read was intrupted 100 times by update to max_evicted_seq_. This is unexpected in all setups");
            }
            maxEvictedSeqLb = (ulong)Volatile.Read(ref _maxEvictedSeq);
            wasEmpty = _delayedPreparedEmpty;
            bool exist = GetCommitEntry(indexedSeq, out _, out cached);
            if (exist && prepSeq == cached.PrepSeq)
            {
                // It is committed and also not evicted from commit cache
                return cached.CommitSeq <= snapshotSeq;
            }
            // else it could be committed but not inserted in the map which could happen after recovery, or it could be
            // committed and evicted by another commit, or never committed.

            // At this point we don't know if it was committed or it is still prepared
            maxEvictedSeqUb = (ulong)Volatile.Read(ref _maxEvictedSeq);
            if (maxEvictedSeqLb != maxEvictedSeqUb) continue;
            // Note: max_evicted_seq_ when we did GetCommitEntry <= max_evicted_seq_ub
            if (maxEvictedSeqUb < prepSeq)
            {
                // Not evicted from cache and also not present, so must be still prepared
                return false;
            }
            if (!wasEmpty)
            {
                // We should not normally reach here
                RecordTick(Ticker.TxnPrepareMutexOverhead);
                lock (_preparedLock)
                {
                    if (_delayedPrepared.Contains(prepSeq))
                    {
                        // This is the order: 1) delayed_prepared_commits_ update, 2) publish 3) delayed_prepared_ clean
                        // up. So check if it is the case of a late clenaup.
                        if (!_delayedPreparedCommits.TryGetValue(prepSeq, out ulong commit)) return false; // Then it is not committed yet
                        return commit <= snapshotSeq;
                    }
                    // 2nd query to commit cache. Refer to was_empty comment above.
                    exist = GetCommitEntry(indexedSeq, out _, out cached);
                    if (exist && prepSeq == cached.PrepSeq) return cached.CommitSeq <= snapshotSeq;
                    maxEvictedSeqUb = (ulong)Volatile.Read(ref _maxEvictedSeq);
                }
            }
        }
        while (maxEvictedSeqLb != maxEvictedSeqUb);
        // When advancing max_evicted_seq_, we move older entires from prepared to delayed_prepared_. Also we move
        // evicted entries from commit cache to old_commit_map_ if it overlaps with any snapshot. Since prep_seq <=
        // max_evicted_seq_, we have three cases: i) in delayed_prepared_, ii) in old_commit_map_, iii) committed with
        // no conflict with any snapshot. Case (i) delayed_prepared_ is checked above
        if (maxEvictedSeqUb < snapshotSeq)
        {
            // then (ii) cannot be the case: only (iii) is the case: committed. commit_seq <= max_evicted_seq_ <
            // snapshot_seq => commit_seq < snapshot_seq
            return true;
        }
        // else (ii) might be the case: check the commit data saved for this snapshot. If there was no overlapping
        // commit entry, then it is committed with a commit_seq lower than any live snapshot, including snapshot_seq.
        if (_oldCommitMapEmpty)
        {
            snapReleased = true;
            return true;
        }
        {
            // We should not normally reach here unless sapshot_seq is old. This is a rare case and it is ok to pay the
            // cost of mutex ReadLock for such old, reading transactions.
            RecordTick(Ticker.TxnOldCommitMapMutexOverhead);
            lock (_oldCommitMapLock)
            {
                if (!_oldCommitMap.TryGetValue(snapshotSeq, out List<ulong>? vec))
                {
                    snapReleased = true;
                    return true;
                }
                // found prep_seq in old_commit_map_: not visible to the snapshot
                if (vec.BinarySearch(prepSeq) < 0) return true;
            }
        }
        // (ii) it the case: it is committed but after the snapshot_seq
        return false;
    }

    // =====================================================================================================
    // Prepared heap
    // =====================================================================================================

    /// <summary>
    /// PreparedHeap: a heap of prepare sequence numbers. Prepares arrive in increasing order (a deque); erase of a
    /// non-top entry is deferred to a min-heap of erased entries (the pop loop skips them).
    /// </summary>
    internal sealed class PreparedHeap
    {
        private readonly Lock _pushPopLock = new();       // push_pop_mutex_
        private readonly Queue<ulong> _heap = new();       // heap_ (a deque: entries arrive sorted)
        private readonly PriorityQueue<ulong, ulong> _erasedHeap = new(); // erased_heap_
        private long _heapTop = unchecked((long)DbFormat.MaxSequenceNumber); // heap_top_

        public Lock PushPopLock => _pushPopLock;

        public bool IsEmpty => Top == DbFormat.MaxSequenceNumber;

        public ulong Top => (ulong)Volatile.Read(ref _heapTop);

        /// <summary>REQUIRES: push_pop lock.</summary>
        public void Push(ulong v)
        {
            if (_heap.Count == 0) Volatile.Write(ref _heapTop, (long)v);
            _heap.Enqueue(v);
        }

        public void Pop(bool locked)
        {
            if (!locked) _pushPopLock.Enter();
            try
            {
                _heap.Dequeue();
                while (_heap.Count > 0 && _erasedHeap.Count > 0 && _heap.Peek() >= _erasedHeap.Peek())
                {
                    // heap_top_ > erased_heap_.top() could happen if we have erased a non-existent entry. Ideally the
                    // user should not do that but we should be resilient against it.
                    if (_heap.Peek() == _erasedHeap.Peek()) _heap.Dequeue();
                    _erasedHeap.Dequeue();
                }
                while (_heap.Count == 0 && _erasedHeap.Count > 0) _erasedHeap.Dequeue();
                Volatile.Write(ref _heapTop, _heap.Count > 0 ? (long)_heap.Peek() : unchecked((long)DbFormat.MaxSequenceNumber));
            }
            finally
            {
                if (!locked) _pushPopLock.Exit();
            }
        }

        /// <summary>Concurrent calls need external synchronization (prepared_mutex_). Safe to call on a non-existent entry.</summary>
        public void Erase(ulong seq)
        {
            if (IsEmpty) return;
            ulong top = Top;
            if (seq < top)
            {
                // Already popped, ignore it.
            }
            else if (top == seq)
            {
                Pop(locked: false);
            }
            else
            {
                // top() > seq: Down the heap, remember to pop it later
                _erasedHeap.Enqueue(seq, seq);
            }
        }
    }

    /// <summary>AddPrepared: adds a prepare sequence number (under the push/pop lock when <paramref name="locked"/>).</summary>
    public void AddPrepared(ulong seq, bool locked)
    {
        if (!locked) _preparedTxns.PushPopLock.Enter();
        try
        {
            _preparedTxns.Push(seq);
            ulong newMax = (ulong)Volatile.Read(ref _futureMaxEvictedSeq);
            if (seq <= newMax)
            {
                // This should not happen in normal case: "Added prepare_seq is not larger than max_evicted_seq_".
                CheckPreparedAgainstMax(newMax, locked: true);
            }
        }
        finally
        {
            if (!locked) _preparedTxns.PushPopLock.Exit();
        }
    }

    /// <summary>
    /// CheckPreparedAgainstMax: moves the prepared entries at or below <paramref name="newMax"/> to delayed_prepared_.
    /// With <paramref name="locked"/> the caller holds the push/pop lock (released while the prepared lock is taken).
    /// </summary>
    private void CheckPreparedAgainstMax(ulong newMax, bool locked)
    {
        ulong top = _preparedTxns.Top;
        bool empty = top == DbFormat.MaxSequenceNumber;
        // Preliminary check to avoid the synchronization cost
        if (empty || top > newMax) return;
        if (locked)
        {
            // Needed to avoid double locking in pop().
            _preparedTxns.PushPopLock.Exit();
        }
        try
        {
            lock (_preparedLock)
            {
                // Need to fetch fresh values of ::top after mutex is acquired
                while (!_preparedTxns.IsEmpty && _preparedTxns.Top <= newMax)
                {
                    ulong toBePopped = _preparedTxns.Top;
                    _delayedPrepared.Add(toBePopped);
                    _delayedPreparedEmpty = false;
                    _preparedTxns.Pop(locked: false);
                }
            }
        }
        finally
        {
            if (locked) _preparedTxns.PushPopLock.Enter();
        }
    }

    /// <summary>RemovePrepared: the prepared (or rolled back) batches starting at <paramref name="prepareSeq"/> are done.</summary>
    public void RemovePrepared(ulong prepareSeq, int batchCnt)
    {
        lock (_preparedLock)
        {
            for (int i = 0; i < batchCnt; i++)
            {
                ulong s = prepareSeq + (ulong)i;
                _preparedTxns.Erase(s);
                bool wasEmpty = _delayedPrepared.Count == 0;
                if (!wasEmpty)
                {
                    _delayedPrepared.Remove(s);
                    _delayedPreparedCommits.Remove(s);
                    bool isEmpty = _delayedPrepared.Count == 0;
                    if (wasEmpty != isEmpty) _delayedPreparedEmpty = isEmpty;
                }
            }
        }
    }

    // =====================================================================================================
    // AddCommitted / eviction
    // =====================================================================================================

    /// <summary>AddCommitted: records that the data prepared at <paramref name="prepareSeq"/> committed at <paramref name="commitSeq"/>.</summary>
    public void AddCommitted(ulong prepareSeq, ulong commitSeq)
    {
        ulong indexedSeq = prepareSeq % _commitCacheSize;
        for (int loopCnt = 0; ; loopCnt++)
        {
            bool toBeEvicted = GetCommitEntry(indexedSeq, out long evicted64, out CommitEntry evicted);
            if (toBeEvicted)
            {
                ulong prevMax = (ulong)Volatile.Read(ref _maxEvictedSeq);
                if (prevMax < evicted.CommitSeq)
                {
                    ulong last = _db.Versions.LastSequence; // could be 0
                    ulong maxEvictedSeq;
                    if (evicted.CommitSeq < last)
                    {
                        // Inc max in larger steps to avoid frequent updates
                        maxEvictedSeq = Math.Min(evicted.CommitSeq + _incStepForMaxEvicted, last - 1);
                    }
                    else
                    {
                        // legit when a commit entry in a write batch overwrite the previous one
                        maxEvictedSeq = evicted.CommitSeq;
                    }
                    AdvanceMaxEvictedSeq(prevMax, maxEvictedSeq);
                }
                if (!_delayedPreparedEmpty)
                {
                    lock (_preparedLock)
                    {
                        if (_delayedPrepared.Contains(evicted.PrepSeq))
                        {
                            // This is a rare case that txn is committed but prepared_txns_ is not cleaned up yet. Refer to
                            // delayed_prepared_commits_ definition for why it should be kept updated.
                            _delayedPreparedCommits[evicted.PrepSeq] = evicted.CommitSeq;
                        }
                    }
                }
                // After each eviction from commit cache, check if the commit entry should be kept around because it
                // overlaps with a live snapshot.
                CheckAgainstSnapshots(evicted);
            }
            if (ExchangeCommitEntry(indexedSeq, evicted64, new CommitEntry(prepareSeq, commitSeq))) return;
            // A very rare event, in which the commit entry is updated before we do. Here we apply a very simple
            // solution of retrying.
            if (loopCnt > 100) throw new InvalidOperationException("Infinite loop in AddCommitted!");
        }
    }

    /// <summary>
    /// AdvanceMaxEvictedSeq: moves max_evicted_seq to <paramref name="newMax"/>, moving older prepared entries to
    /// delayed_prepared_ and refreshing the snapshot list (old_commit_map_ gets an entry per snapshot).
    /// </summary>
    internal void AdvanceMaxEvictedSeq(ulong prevMax, ulong newMax)
    {
        // When max_evicted_seq_ advances, move older entries from prepared_txns_ to delayed_prepared_. This guarantees
        // that if a seq is lower than max, then it is not in prepared_txns_ and save an expensive, synchronized lookup
        // from a shared set. delayed_prepared_ is expected to be empty in normal cases.
        long updatedFutureMax = (long)prevMax;
        while ((ulong)updatedFutureMax < newMax)
        {
            long seen = Interlocked.CompareExchange(ref _futureMaxEvictedSeq, (long)newMax, updatedFutureMax);
            if (seen == updatedFutureMax) break;
            updatedFutureMax = seen;
        }
        CheckPreparedAgainstMax(newMax, locked: false);

        // With each change to max_evicted_seq_ fetch the live snapshots behind it. We use max as the version of
        // snapshots to identify how fresh are the snapshot list. This works because the snapshots are between 0 and
        // max, so the larger the max, the more complete they are.
        ulong newSnapshotsVersion = newMax;
        List<ulong>? snapshots = null;
        bool updateSnapshots = false;
        if (newSnapshotsVersion > _snapshotsVersion)
        {
            // This is to avoid updating the snapshots_ if it already updated with a more recent vesion by a concrrent
            // thread
            updateSnapshots = true;
            // We only care about snapshots lower then max
            snapshots = GetSnapshotListFromDb(newMax);
        }
        if (updateSnapshots)
        {
            UpdateSnapshots(snapshots!, newSnapshotsVersion);
            if (snapshots!.Count > 0)
            {
                lock (_oldCommitMapLock)
                {
                    foreach (ulong snap in snapshots)
                    {
                        // This allows IsInSnapshot to tell apart the reads from in valid snapshots from the reads from
                        // committed values in valid snapshots.
                        if (!_oldCommitMap.ContainsKey(snap)) _oldCommitMap[snap] = new List<ulong>();
                    }
                    _oldCommitMapEmpty = false;
                }
            }
        }
        long updatedPrevMax = (long)prevMax;
        while ((ulong)updatedPrevMax < newMax)
        {
            long seen = Interlocked.CompareExchange(ref _maxEvictedSeq, (long)newMax, updatedPrevMax);
            if (seen == updatedPrevMax) break;
            updatedPrevMax = seen;
        }
    }

    /// <summary>GetSnapshotListFromDB: the DB's live snapshots &lt;= <paramref name="max"/>, ascending.</summary>
    private List<ulong> GetSnapshotListFromDb(ulong max)
    {
        var list = new List<ulong>();
        _db.GetSnapshotSequences(list, max);
        return list;
    }

    /// <summary>ReleaseSnapshotInternal: drops the old_commit_map_ entry of a released snapshot.</summary>
    private void ReleaseSnapshotInternal(ulong snapSeq)
    {
        // TODO(myabandeh): relax should enough since the synchronizatin is already done by snapshots_mutex_ under which
        // this function is called.
        if (snapSeq <= (ulong)Volatile.Read(ref _maxEvictedSeq))
        {
            // This is a too-old snapshot: since it is released, remove it from old_commit_map_.
            bool needGc;
            RecordTick(Ticker.TxnOldCommitMapMutexOverhead);
            lock (_oldCommitMapLock) needGc = _oldCommitMap.ContainsKey(snapSeq);
            if (needGc)
            {
                RecordTick(Ticker.TxnOldCommitMapMutexOverhead);
                lock (_oldCommitMapLock)
                {
                    _oldCommitMap.Remove(snapSeq);
                    _oldCommitMapEmpty = _oldCommitMap.Count == 0;
                }
            }
        }
    }

    /// <summary>CleanupReleasedSnapshots: releases the snapshots in <paramref name="oldSnapshots"/> missing from <paramref name="newSnapshots"/>.</summary>
    private void CleanupReleasedSnapshots(List<ulong> newSnapshots, List<ulong> oldSnapshots)
    {
        int newi = 0, oldi = 0;
        while (newi < newSnapshots.Count && oldi < oldSnapshots.Count)
        {
            if (newSnapshots[newi] == oldSnapshots[oldi])
            {
                // still not released
                ulong value = newSnapshots[newi];
                // Skip the duplicates
                while (newi < newSnapshots.Count && newSnapshots[newi] == value) newi++;
                while (oldi < oldSnapshots.Count && oldSnapshots[oldi] == value) oldi++;
            }
            else
            {
                // *newi > *oldi: *oldi is released
                ReleaseSnapshotInternal(oldSnapshots[oldi]);
                oldi++;
            }
        }
        // Everything remained in old_snapshots is released and must be cleaned up
        for (; oldi < oldSnapshots.Count; oldi++) ReleaseSnapshotInternal(oldSnapshots[oldi]);
    }

    /// <summary>
    /// UpdateSnapshots: mirrors the snapshot list in the lock-free cache (first SNAPSHOT_CACHE_SIZE entries) and the
    /// list behind it.
    /// </summary>
    internal void UpdateSnapshots(List<ulong> snapshots, ulong version)
    {
        lock (_snapshotsLock)
        {
            _snapshotsVersion = version;
            // We update the list concurrently with the readers. Both new and old lists are sorted and the new list is
            // subset of the previous list plus some new items. Thus if a snapshot repeats in both new and old lists, it
            // will appear upper in the new list. So if we simply insert the new snapshots in order, if an overwritten
            // item is still valid in the new list is either written to the same place in the array or it is written in
            // a higher palce before it gets overwritten by another item. This guarantess a reader that reads the list
            // bottom-up will eventaully see a snapshot that repeats in the update, either before it gets overwritten by
            // the writer or afterwards.
            int i = 0;
            int idx = 0;
            for (; idx < snapshots.Count && i < _snapshotCacheSize; idx++, i++) Volatile.Write(ref _snapshotCache[i], (long)snapshots[idx]);
            _snapshots.Clear();
            for (; idx < snapshots.Count; idx++) _snapshots.Add(snapshots[idx]);
            // Update the size at the end. Otherwise a parallel reader might read items that are not set yet.
            Volatile.Write(ref _snapshotsTotal, snapshots.Count);
            // Note: this must be done after the snapshots data structures are updated with the new list of snapshots.
            CleanupReleasedSnapshots(snapshots, _snapshotsAll);
            _snapshotsAll = new List<ulong>(snapshots);
        }
    }

    /// <summary>
    /// CheckAgainstSnapshots: keeps an evicted commit entry in old_commit_map_ for every snapshot it overlaps
    /// (prepared before the snapshot, committed after it).
    /// </summary>
    private void CheckAgainstSnapshots(CommitEntry evicted)
    {
        // First check the snapshot cache that is efficient for concurrent access
        int cnt = Volatile.Read(ref _snapshotsTotal);
        // The list might get updated concurrently as we are reading from it. The reader should be able to read all the
        // snapshots that are still valid after the update. Since the survived snapshots are written in a higher place
        // before gets overwritten the reader that reads bottom-up will eventully see it.
        const bool nextIsLarger = true;
        // We will set to true if the border line snapshot suggests that.
        bool searchLargerList = false;
        int ip1 = Math.Min(cnt, _snapshotCacheSize);
        for (; 0 < ip1; ip1--)
        {
            ulong snapshotSeq = (ulong)Volatile.Read(ref _snapshotCache[ip1 - 1]);
            if (ip1 == _snapshotCacheSize)
            {
                // border line snapshot: snapshot_seq < commit_seq means the 2nd list might also have overlapping
                // snapshots.
                searchLargerList = snapshotSeq < evicted.CommitSeq;
            }
            if (!MaybeUpdateOldCommitMap(evicted.PrepSeq, evicted.CommitSeq, snapshotSeq, !nextIsLarger)) break;
        }
        if (_snapshotCacheSize < cnt && searchLargerList)
        {
            // Rarely expected to happen: it implies that there are many snapshots. Then the expensive mutex is used.
            RecordTick(Ticker.TxnSnapshotMutexOverhead);
            lock (_snapshotsLock)
            {
                // Items could have moved from the snapshots_ to snapshot_cache_ before accquiring the lock. To make sure
                // that we do not miss a valid snapshot, read snapshot_cache_ again while holding the lock.
                for (int i = 0; i < _snapshotCacheSize; i++)
                {
                    ulong snapshotSeq = (ulong)Volatile.Read(ref _snapshotCache[i]);
                    if (!MaybeUpdateOldCommitMap(evicted.PrepSeq, evicted.CommitSeq, snapshotSeq, nextIsLarger)) break;
                }
                foreach (ulong snapshotSeq2 in _snapshots)
                {
                    if (!MaybeUpdateOldCommitMap(evicted.PrepSeq, evicted.CommitSeq, snapshotSeq2, nextIsLarger)) break;
                }
            }
        }
    }

    /// <summary>MaybeUpdateOldCommitMap: returns whether the search should continue to the next snapshot.</summary>
    private bool MaybeUpdateOldCommitMap(ulong prepSeq, ulong commitSeq, ulong snapshotSeq, bool nextIsLarger)
    {
        // If we do not store an entry in old_commit_map_ we assume it is committed in all snapshots. If commit_seq <=
        // snapshot_seq, it is considered already in the snapshot so no need to keep the entry around for this snapshot.
        if (commitSeq <= snapshotSeq)
        {
            // continue the search if the next snapshot could be smaller than commit_seq
            return !nextIsLarger;
        }
        // then snapshot_seq < commit_seq
        if (prepSeq <= snapshotSeq)
        {
            // overlapping range
            RecordTick(Ticker.TxnOldCommitMapMutexOverhead);
            lock (_oldCommitMapLock)
            {
                _oldCommitMapEmpty = false;
                if (!_oldCommitMap.TryGetValue(snapshotSeq, out List<ulong>? vec)) _oldCommitMap[snapshotSeq] = vec = new List<ulong>();
                int at = vec.BinarySearch(prepSeq);
                // upper_bound
                if (at < 0) at = ~at;
                else
                {
                    while (at < vec.Count && vec[at] == prepSeq) at++;
                }
                vec.Insert(at, prepSeq);
            }
            // We need to store it once for each overlapping snapshot. Returning true to continue the search if there is
            // more overlapping snapshot.
            return true;
        }
        // continue the search if the next snapshot could be larger than prep_seq
        return nextIsLarger;
    }

    // =====================================================================================================
    // Snapshots and reads
    // =====================================================================================================

    /// <summary>
    /// SmallestUnCommittedSeq: the smallest sequence number that may not be committed (the next sequence number if
    /// nothing is prepared).
    /// </summary>
    public ulong SmallestUnCommittedSeq()
    {
        // Note: We have two lists to look into, but for performance reasons they are not read atomically. Since
        // CheckPreparedAgainstMax copies the entry to delayed_prepared_ before removing it from prepared_txns_, to
        // ensure that a prepared entry will not go unmissed, we look into them in opposite order: first read
        // prepared_txns_ and then delayed_prepared_.

        // This must be called before calling ::top. This is because the concurrent thread would call ::RemovePrepared
        // before updating GetLatestSequenceNumber(). Reading then in opposite order here guarantees that the ::top that
        // we read would be lower the ::top if we had otherwise update/read them atomically.
        ulong nextPrepare = _db.Versions.LastSequence + 1;
        ulong minPrepare = _preparedTxns.Top;
        // Since we update the prepare_heap always from the main write queue via PreReleaseCallback, the prepared_txns_.top()
        // indicates the smallest prepared data in 2pc transactions. For non-2pc transactions that are written in two
        // steps, we also update prepared_txns_ at the first step (via the same mechanism) so that their uncommitted data
        // is reflected in SmallestUnCommittedSeq.
        if (!_delayedPreparedEmpty)
        {
            lock (_preparedLock)
            {
                if (_delayedPrepared.Count > 0) return _delayedPrepared.Min;
            }
        }
        return minPrepare == DbFormat.MaxSequenceNumber ? nextPrepare : Math.Min(minPrepare, nextPrepare);
    }

    /// <summary>
    /// GetSnapshotInternal: a DB snapshot whose sequence is above future_max_evicted_seq (advancing the sequence with an
    /// empty transaction when needed), enhanced with min_uncommitted.
    /// </summary>
    public override Snapshot GetSnapshot()
    {
        // Note: for this optimization setting the last sequence number and obtaining the smallest uncommitted seq should
        // be done atomically. However to avoid the mutex overhead, we call SmallestUnCommittedSeq BEFORE taking the
        // snapshot. Since we always updated the list of unprepared seq (via AddPrepared) AFTER the last sequence is
        // updated, this guarantees that the smallest uncommitted seq that we pair with the snapshot is smaller or equal
        // the value that would be obtained otherwise atomically. That is ok since this optimization works as long as
        // min_uncommitted is less than or equal than the smallest uncommitted seq when the snapshot was taken.
        ulong minUncommitted = SmallestUnCommittedSeq();
        Snapshot snap = _db.GetSnapshotImpl();
        ulong snapSeq = snap.SequenceNumber;
        // Note: Check against future_max_evicted_seq_ (in contrast with max_evicted_seq_) in case there is a concurrent
        // AdvanceMaxEvictedSeq.
        if (snapSeq != 0 && snapSeq <= (ulong)Volatile.Read(ref _futureMaxEvictedSeq))
        {
            // There is a very rare case in which the commit entry evicts another commit entry that is not published yet
            // thus advancing max evicted seq beyond the last published seq. This case is not likely in real-world setup so
            // we handle it with a few retries.
            int retry = 0;
            ulong max;
            while ((max = (ulong)Volatile.Read(ref _futureMaxEvictedSeq)) != 0 && snap.SequenceNumber <= max && retry < 100)
            {
                snap.Dispose();
                AdvanceSeqByOne();
                snap = _db.GetSnapshotImpl();
                retry++;
            }
            if (snap.SequenceNumber <= max)
            {
                ulong s = snap.SequenceNumber;
                snap.Dispose();
                throw new InvalidOperationException("Snapshot seq " + s + " after " + retry + " retries is still less than future_max_evicted_seq_" + max);
            }
        }
        // EnhanceSnapshot
        snap.MinUncommitted = minUncommitted;
        return snap;
    }

    /// <summary>AdvanceSeqByOne: writes an empty prepared + committed transaction to advance the sequence number.</summary>
    private void AdvanceSeqByOne()
    {
        // Inserting an empty value will i) let the max evicted entry to be published, i.e., max == last_published,
        // increase the last published to be one beyond max, i.e., max < last_published.
        using Transaction txn0 = _txnDb.BeginTransaction(new WriteOptions());
        txn0.SetName("txn" + Environment.CurrentManagedThreadId.ToString(System.Globalization.CultureInfo.InvariantCulture) + "-"
            + Guid.NewGuid().ToString("N"));
        txn0.Prepare();
        txn0.Commit();
    }

    /// <summary>AssignMinMaxSeqs: (min_uncommitted, max) of a read at <paramref name="snapshot"/> (max = 0 for later assignment).</summary>
    public SnapshotBackup AssignMinMaxSeqs(Snapshot? snapshot, out ulong min, out ulong max)
    {
        if (snapshot is not null)
        {
            min = snapshot.MinUncommitted;
            max = snapshot.SequenceNumber;
            return SnapshotBackup.BackedByDbSnapshot;
        }
        min = SmallestUnCommittedSeq();
        max = 0; // to be assigned later after sv is referenced.
        return SnapshotBackup.UnbackedByDbSnapshot;
    }

    /// <summary>
    /// ValidateSnapshot: a read that is not backed by a snapshot is invalid if max_evicted_seq advanced past its
    /// sequence (the commit cache may have lost entries it relied on).
    /// </summary>
    public bool ValidateSnapshot(ulong snapSeq, SnapshotBackup backedBySnapshot)
    {
        if (backedBySnapshot == SnapshotBackup.BackedByDbSnapshot) return true;
        ulong max = (ulong)Volatile.Read(ref _maxEvictedSeq);
        return !(snapSeq <= max && snapSeq != 0);
    }

    /// <summary>The TryAgain status of a read whose snapshot could not be validated.</summary>
    internal RocksDbException TryAgain()
    {
        RecordTick(Ticker.TxnGetTryAgain);
        return new RocksDbException(StatusCode.TryAgain, "Operation failed. Try again.: ");
    }

    public override SnapshotChecker SnapshotChecker => _checker;

    public override ReadCallback BeginRead(Snapshot? snapshot)
    {
        WritePreparedTxnReadCallback cb = t_readCallback ?? new WritePreparedTxnReadCallback();
        t_readCallback = null;
        SnapshotBackup backed = AssignMinMaxSeqs(snapshot, out ulong min, out ulong max);
        cb.Init(this, max, min, backed);
        return cb;
    }

    public override void EndRead(ReadCallback callback, bool validate)
    {
        var cb = (WritePreparedTxnReadCallback)callback;
        bool ok = !validate || (cb.Valid && ValidateSnapshot(cb.MaxVisibleSeq, cb.Backed));
        t_readCallback = cb;
        if (!ok) throw TryAgain();
    }

    public override ReadCallback NewIteratorCallback(Snapshot snapshot)
    {
        var cb = new WritePreparedTxnReadCallback();
        cb.Init(this, snapshot.SequenceNumber, snapshot.MinUncommitted, SnapshotBackup.BackedByDbSnapshot);
        return cb;
    }

    // =====================================================================================================
    // Writes without a transaction (WriteInternal)
    // =====================================================================================================

    /// <summary>The options of the second write of WriteInternal with two write queues: no WAL, no sync.</summary>
    private static readonly WriteOptions s_commitOnlyNoWal = new() { DisableWal = true };

    /// <summary>
    /// WritePreparedTxnDB::WriteInternal for a batch that does not come from a transaction (batch_cnt is computed with
    /// SubBatchCounter). Without two write queues the batch is committed by its own write (commit-map callback). With
    /// two write queues the main queue writes it as prepared data (AddPreparedCallback) and an empty, WAL-less write on
    /// the second queue commits and publishes it.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public override async ValueTask WritePlainAsync(WriteOptions? options, WriteBatch batch, bool synchronous, CancellationToken cancellationToken)
    {
        PlainWriteState st = RentPlainWriteState();
        try
        {
            int batchCnt = SubBatchCounter.Count(batch, _db, st.Keys);
            RecordTick(Ticker.TxnDuplicateKeyOverhead);
            bool doOneWrite = !_db.TwoWriteQueues;
            WriteBatch copy = st.Batch;
            copy.SetContents(batch.Data);
            // In the absence of Prepare markers, use Noop as a batch separator
            copy.InsertNoop();
            TransactionWriteContext context = st.Context;
            context.Reset();
            context.BatchCount = batchCnt;
            if (doOneWrite)
            {
                st.Commit.Set(DbFormat.MaxSequenceNumber, 0, batchCnt);
                context.PreRelease = st.Commit;
            }
            else
            {
                // Since this is not 2pc, there is no need for AddPrepared but having it in the PreReleaseCallback enables
                // an optimization. Refer to SmallestUnCommittedSeq for more details.
                st.AddPrepared.Set(batchCnt, firstPrepareBatch: false);
                context.PreRelease = st.AddPrepared;
            }
            await _db.WriteAsync(options, copy, validateColumnFamilies: false, context, synchronous, cancellationToken).ConfigureAwait(false);
            if (doOneWrite) return;
            // else do the 2nd write for commit: an empty batch on the 2nd queue releases the commit sequence to readers.
            st.Commit.Set(context.SequenceUsed, batchCnt, 0);
            context.Reset();
            context.DisableMemtable = true;
            context.BatchCount = 1;
            context.PreRelease = st.Commit;
            copy.Clear();
            await _db.WriteAsync(s_commitOnlyNoWal, copy, validateColumnFamilies: false, context, synchronous, cancellationToken).ConfigureAwait(false);
            // Note: RemovePrepared is called from within PreReleaseCallback
        }
        finally
        {
            ReturnPlainWriteState(st);
        }
    }

    /// <summary>The reusable objects of one plain write (the batch copy, its write context and callbacks).</summary>
    private sealed class PlainWriteState(WritePreparedTxnDb db)
    {
        public readonly WriteBatch Batch = new();
        public readonly TransactionWriteContext Context = new();
        public readonly CommitEntryPreReleaseCallback Commit = new(db);
        public readonly AddPreparedCallback AddPrepared = new(db);
        public readonly SubBatchKeySet Keys = new();
    }

    private readonly PlainWriteState?[] _plainWriteStates = new PlainWriteState?[Math.Min(64, Environment.ProcessorCount * 4)];

    private PlainWriteState RentPlainWriteState()
    {
        PlainWriteState?[] pool = _plainWriteStates;
        for (int i = 0; i < pool.Length; i++)
        {
            PlainWriteState? s = pool[i];
            if (s is not null && Interlocked.CompareExchange(ref pool[i], null, s) == s) return s;
        }
        return new PlainWriteState(this);
    }

    private void ReturnPlainWriteState(PlainWriteState s)
    {
        if (s.Batch.DataSize > 1 << 20) return; // don't keep large buffers alive
        s.Batch.Clear();
        s.Context.Reset();
        PlainWriteState?[] pool = _plainWriteStates;
        for (int i = 0; i < pool.Length; i++)
        {
            if (pool[i] is null && Interlocked.CompareExchange(ref pool[i], s, null) is null) return;
        }
    }

    // =====================================================================================================
    // Recovery
    // =====================================================================================================

    /// <summary>
    /// WritePreparedTxnDB::Initialize (after the base transactions were rebuilt): the recovered prepared batches are
    /// added to the prepared heap and everything else up to the last sequence is considered committed
    /// (AdvanceMaxEvictedSeq); the sequence is then advanced by one.
    /// </summary>
    internal void InitializeAfterRecovery(SortedDictionary<ulong, int> orderedSeqCnt)
    {
        foreach (KeyValuePair<ulong, int> sc in orderedSeqCnt)
        {
            for (int i = 0; i < sc.Value; i++) AddPrepared(sc.Key + (ulong)i, locked: false);
        }
        ulong prevMax = (ulong)Volatile.Read(ref _maxEvictedSeq);
        ulong lastSeq = _db.Versions.LastSequence;
        AdvanceMaxEvictedSeq(prevMax, lastSeq);
        // Create a gap between max and the next snapshot. This simplifies the logic in IsInSnapshot by not having to
        // consider the special case of max == snapshot after recovery. This is tested in IsInSnapshotEmptyMapTest.
        if (lastSeq != 0)
        {
            // SetLastAllocatedSequence (raised by SetLastSequence), SetLastSequence, SetLastPublishedSequence.
            _db.Versions.SetLastSequence(lastSeq + 1);
            _db.Versions.SetLastPublishedSequence(lastSeq + 1);
        }
    }

    // =====================================================================================================
    // Test support
    // =====================================================================================================

    internal int CommitCacheSize => (int)_commitCacheSize;

    internal bool TryGetCommitEntry(ulong prepSeq, out ulong commitSeq)
    {
        bool exist = GetCommitEntry(prepSeq % _commitCacheSize, out _, out CommitEntry e);
        commitSeq = e.CommitSeq;
        return exist && e.PrepSeq == prepSeq;
    }

    internal bool IsPrepared(ulong seq)
    {
        lock (_preparedLock)
        {
            if (_delayedPrepared.Contains(seq)) return true;
        }
        return _preparedTxns.Top == seq;
    }

    internal int DelayedPreparedCount
    {
        get
        {
            lock (_preparedLock) return _delayedPrepared.Count;
        }
    }

    internal int OldCommitMapCount
    {
        get
        {
            lock (_oldCommitMapLock) return _oldCommitMap.Count;
        }
    }

    internal ulong PreparedHeapTop => _preparedTxns.Top;
}

/// <summary>
/// Port of WritePreparedTxnReadCallback (write_prepared_txn_db.h): a read at a snapshot sees a sequence number iff
/// <see cref="WritePreparedTxnDb.IsInSnapshot"/>. Reusable (<see cref="Init"/>).
/// </summary>
internal sealed class WritePreparedTxnReadCallback : ReadCallback
{
    private WritePreparedTxnDb? _db;
    private bool _snapReleased;

    public SnapshotBackup Backed { get; private set; }

    public void Init(WritePreparedTxnDb db, ulong snapshot, ulong minUncommitted, SnapshotBackup backed)
    {
        _db = db;
        _maxVisibleSeq = snapshot;
        _minUncommitted = minUncommitted;
        Backed = backed;
        _snapReleased = false;
    }

    /// <summary>valid(): false if the snapshot was released during the read (its answers may be wrong).</summary>
    public bool Valid => !_snapReleased;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected override bool IsVisibleFullCheck(ulong seq)
    {
        bool released = false;
        bool ret = _db!.IsInSnapshot(seq, _maxVisibleSeq, _minUncommitted, ref released);
        _snapReleased |= released;
        return ret;
    }
}

/// <summary>Port of WritePreparedSnapshotChecker (utilities/transactions/snapshot_checker.cc).</summary>
internal sealed class WritePreparedSnapshotChecker(WritePreparedTxnDb db) : SnapshotChecker
{
    public override SnapshotCheckerResult CheckInSnapshot(ulong sequence, ulong snapshotSequence)
    {
        bool released = false;
        // TODO(myabandeh): set min_uncommitted
        bool inSnapshot = db.IsInSnapshot(sequence, snapshotSequence, ReadCallback.MinUnCommittedSeq, ref released);
        if (released) return SnapshotCheckerResult.SnapshotReleased;
        return inSnapshot ? SnapshotCheckerResult.InSnapshot : SnapshotCheckerResult.NotInSnapshot;
    }
}

/// <summary>
/// Port of AddPreparedCallback (write_prepared_txn_db.h): adds the sub-batches of a prepare (or unprepared) write to
/// the prepared heap before the write is published; the first prepare batch marks its WAL as holding a prepared section.
/// </summary>
internal sealed class AddPreparedCallback(WritePreparedTxnDb db) : IPreReleaseCallback
{
    private int _subBatchCnt;
    private bool _firstPrepareBatch;

    public void Set(int subBatchCnt, bool firstPrepareBatch)
    {
        _subBatchCnt = subBatchCnt;
        _firstPrepareBatch = firstPrepareBatch;
    }

    public void Callback(ulong prepareSeq, bool isMemDisabled, ulong logNumber, int index, int total)
    {
        Lock l = db.PreparedHeapLock;
        l.Enter();
        try
        {
            for (int i = 0; i < _subBatchCnt; i++) db.AddPrepared(prepareSeq + (ulong)i, locked: true);
        }
        finally
        {
            l.Exit();
        }
        if (_firstPrepareBatch && logNumber != 0) db.Db.MarkLogAsContainingPrepSection(logNumber);
    }
}

/// <summary>
/// Port of WritePreparedCommitEntryPreReleaseCallback: adds the commit entries of a commit (prepared batches, an
/// auxiliary batch written before by the main queue, and / or the data written by the commit itself) to the commit
/// cache before the commit is published. With two write queues it runs on the WAL-only queue only and publishes the
/// commit sequence, then removes the prepared and auxiliary batches from the prepared heap (RemovePrepared must follow
/// the publication, or the SmallestUnCommittedSeq optimization breaks).
/// </summary>
internal sealed class CommitEntryPreReleaseCallback(WritePreparedTxnDb db) : IPreReleaseCallback
{
    private ulong _prepSeq;
    private int _prepBatchCnt;
    private int _dataBatchCnt;
    private ulong _auxSeq = DbFormat.MaxSequenceNumber;
    private int _auxBatchCnt;

    /// <param name="prepSeq">The prepare sequence (<see cref="DbFormat.MaxSequenceNumber"/> = no prepare phase).</param>
    public void Set(ulong prepSeq, int prepBatchCnt, int dataBatchCnt)
    {
        _prepSeq = prepSeq;
        _prepBatchCnt = prepBatchCnt;
        _dataBatchCnt = dataBatchCnt;
        _auxSeq = DbFormat.MaxSequenceNumber;
        _auxBatchCnt = 0;
    }

    /// <summary>aux_seq / aux_batch_cnt: a batch written before by the main queue that commits with this commit.</summary>
    public void SetAux(ulong auxSeq, int auxBatchCnt)
    {
        _auxSeq = auxSeq;
        _auxBatchCnt = auxBatchCnt;
    }

    public void Callback(ulong commitSeq, bool isMemDisabled, ulong logNumber, int index, int total)
    {
        // Data batch is what accompanied with the commit marker and affects the last seq in the commit batch.
        ulong lastCommitSeq = _dataBatchCnt <= 1 ? commitSeq : commitSeq + (ulong)_dataBatchCnt - 1;
        if (_prepSeq != DbFormat.MaxSequenceNumber)
        {
            for (int i = 0; i < _prepBatchCnt; i++) db.AddCommitted(_prepSeq + (ulong)i, lastCommitSeq);
        }
        // else there was no prepare phase
        for (int i = 0; i < _auxBatchCnt; i++) db.AddCommitted(_auxSeq + (ulong)i, lastCommitSeq);
        // Commit the data that is accompanied with the commit request.
        for (int i = 0; i < _dataBatchCnt; i++) db.AddCommitted(commitSeq + (ulong)i, lastCommitSeq);
        if (db.Db.TwoWriteQueues)
        {
            // Publish the sequence number. We can do that here assuming the callback is invoked only from one write
            // queue, which would guarantee that the publish sequence numbers will be in order.
            db.Db.SetLastPublishedSequence(lastCommitSeq);
            if (_prepSeq != DbFormat.MaxSequenceNumber) db.RemovePrepared(_prepSeq, _prepBatchCnt);
            if (_auxBatchCnt > 0) db.RemovePrepared(_auxSeq, _auxBatchCnt);
        }
        // else SequenceNumber that is updated as part of the write already does the publishing
    }
}

/// <summary>
/// Port of WritePreparedRollbackPreReleaseCallback (write_prepared_txn_db.h): with two write queues, the second write of
/// a rollback commits both the rolled back prepared batches and the rollback batch, then publishes the sequence.
/// </summary>
internal sealed class RollbackPreReleaseCallback(WritePreparedTxnDb db) : IPreReleaseCallback
{
    private ulong _prepSeq;
    private ulong _rollbackSeq;
    private int _prepBatchCnt;

    public void Set(ulong prepSeq, ulong rollbackSeq, int prepBatchCnt)
    {
        _prepSeq = prepSeq;
        _rollbackSeq = rollbackSeq;
        _prepBatchCnt = prepBatchCnt;
    }

    public void Callback(ulong commitSeq, bool isMemDisabled, ulong logNumber, int index, int total)
    {
        db.AddCommitted(_rollbackSeq, commitSeq);
        for (int i = 0; i < _prepBatchCnt; i++) db.AddCommitted(_prepSeq + (ulong)i, commitSeq);
        db.Db.SetLastPublishedSequence(commitSeq);
    }
}

/// <summary>
/// CommitSubBatchPreReleaseCallback of WritePreparedTxnDB::Initialize: commits one sub-batch of the recoverable state
/// (the last commit-time batch) when DBImpl::WriteRecoverableState inserts it into the memtables.
/// </summary>
internal sealed class CommitSubBatchPreReleaseCallback(WritePreparedTxnDb db) : IPreReleaseCallback
{
    public void Callback(ulong commitSeq, bool isMemDisabled, ulong logNumber, int index, int total) => db.AddCommitted(commitSeq, commitSeq);
}

/// <summary>
/// Port of WriteUnpreparedCommitEntryPreReleaseCallback (write_unprepared_txn_db.h): commits every unprepared batch of
/// a WriteUnprepared transaction (and the data of the commit / rollback batch itself); with two write queues it runs
/// on the WAL-only queue and publishes the commit sequence.
/// </summary>
internal sealed class UnpreparedCommitEntryPreReleaseCallback(WritePreparedTxnDb db) : IPreReleaseCallback
{
    private SortedDictionary<ulong, int>? _unprepSeqs;
    private int _dataBatchCnt;

    public void Set(SortedDictionary<ulong, int> unprepSeqs, int dataBatchCnt)
    {
        _unprepSeqs = unprepSeqs;
        _dataBatchCnt = dataBatchCnt;
    }

    public void Callback(ulong commitSeq, bool isMemDisabled, ulong logNumber, int index, int total)
    {
        ulong lastCommitSeq = _dataBatchCnt <= 1 ? commitSeq : commitSeq + (ulong)_dataBatchCnt - 1;
        foreach (KeyValuePair<ulong, int> s in _unprepSeqs!)
        {
            for (int i = 0; i < s.Value; i++) db.AddCommitted(s.Key + (ulong)i, lastCommitSeq);
        }
        for (int i = 0; i < _dataBatchCnt; i++) db.AddCommitted(commitSeq + (ulong)i, lastCommitSeq);
        if (db.Db.TwoWriteQueues) db.Db.SetLastPublishedSequence(lastCommitSeq);
    }
}

/// <summary>
/// Port of SubBatchCounter (write_prepared_txn_db.h): the number of sub-batches of a batch (a new one starts at a key
/// repeated within the current one, compared with the column family's comparator).
/// </summary>
internal struct SubBatchCounter : IWriteBatchHandler
{
    private readonly DbImpl _db;
    private readonly SubBatchKeySet? _fast;
    private Dictionary<uint, SortedSet<byte[]>>? _keys;
    private int _batches;

    private SubBatchCounter(DbImpl db, SubBatchKeySet? fast)
    {
        _db = db;
        _fast = fast;
        _keys = null;
        _batches = 1;
    }

    public static int Count(WriteBatch batch, DbImpl db) => Count(batch, db, null);

    /// <summary>
    /// Counts the sub-batches of <paramref name="batch"/>. With <paramref name="scratch"/>, keys of column families whose
    /// comparator only considers byte-identical keys equal are tracked in that reusable set (no allocation per key).
    /// </summary>
    public static int Count(WriteBatch batch, DbImpl db, SubBatchKeySet? scratch)
    {
        var c = new SubBatchCounter(db, scratch);
        try
        {
            batch.Iterate(ref c);
        }
        finally
        {
            scratch?.Clear();
        }
        return c._batches;
    }

    private void NewSubBatch()
    {
        _batches++;
        _keys?.Clear();
        _fast?.Clear();
    }

    private void AddKey(uint cf, ReadOnlySpan<byte> key)
    {
        Comparator cmp = _db.UserComparatorOf(cf);
        if (_fast is not null && !cmp.CanKeysWithDifferentByteContentsBeEqual)
        {
            if (_fast.Add(cf, key)) return;
            NewSubBatch();
            _fast.Add(cf, key);
            return;
        }
        _keys ??= new Dictionary<uint, SortedSet<byte[]>>();
        if (!_keys.TryGetValue(cf, out SortedSet<byte[]>? set)) _keys[cf] = set = NewSet(cmp);
        if (!set.Add(key.ToArray()))
        {
            // second is false if a element already existed.
            NewSubBatch();
            set = NewSet(cmp);
            set.Add(key.ToArray());
            _keys[cf] = set;
        }
    }

    private static SortedSet<byte[]> NewSet(Comparator cmp) => new(Comparer<byte[]>.Create((a, b) => cmp.Compare(a, b)));

    public readonly bool Continue() => true;
    public void Put(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => AddKey(columnFamilyId, key);
    public void TimedPut(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ulong writeUnixTime) => AddKey(columnFamilyId, key);
    public void PutEntity(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> entity) => AddKey(columnFamilyId, key);
    public void Delete(uint columnFamilyId, ReadOnlySpan<byte> key) => AddKey(columnFamilyId, key);
    public void SingleDelete(uint columnFamilyId, ReadOnlySpan<byte> key) => AddKey(columnFamilyId, key);
    public void DeleteRange(uint columnFamilyId, ReadOnlySpan<byte> beginKey, ReadOnlySpan<byte> endKey) => AddKey(columnFamilyId, beginKey);
    public void Merge(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => AddKey(columnFamilyId, key);
    public void PutBlobIndex(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> blobIndex) => AddKey(columnFamilyId, key);
    public readonly void LogData(ReadOnlySpan<byte> blob) { }
    public readonly void MarkBeginPrepare(bool unprepare) { }
    public readonly void MarkEndPrepare(ReadOnlySpan<byte> xid) { }
    public readonly void MarkNoop(bool emptyBatch) { }
    public readonly void MarkCommit(ReadOnlySpan<byte> xid) { }
    public readonly void MarkCommitWithTimestamp(ReadOnlySpan<byte> xid, ReadOnlySpan<byte> commitTimestamp) { }
    public readonly void MarkRollback(ReadOnlySpan<byte> xid) { }
}

/// <summary>
/// Reusable key set of <see cref="SubBatchCounter"/> for column families whose comparator never considers keys with
/// different bytes equal (bytewise and reverse bytewise): keys are compared by bytes and kept in recycled buffers.
/// Not thread-safe.
/// </summary>
internal sealed class SubBatchKeySet
{
    private readonly List<(uint Cf, HashSet<PooledKey> Set)> _sets = new();
    private KeyBufferPool _buffers;

    /// <summary>Adds the key; false if the column family already has it.</summary>
    public bool Add(uint cf, ReadOnlySpan<byte> key)
    {
        HashSet<PooledKey>? set = null;
        foreach ((uint c, HashSet<PooledKey> s) in _sets)
        {
            if (c == cf)
            {
                set = s;
                break;
            }
        }
        if (set is null)
        {
            set = new HashSet<PooledKey>(PooledKeyComparer.Instance);
            _sets.Add((cf, set));
        }
        if (set.GetAlternateLookup<ReadOnlySpan<byte>>().Contains(key)) return false;
        set.Add(_buffers.Rent(key));
        return true;
    }

    public void Clear()
    {
        foreach ((_, HashSet<PooledKey> set) in _sets)
        {
            if (set.Count == 0) continue;
            foreach (PooledKey k in set) _buffers.Return(k);
            set.Clear();
        }
    }
}
