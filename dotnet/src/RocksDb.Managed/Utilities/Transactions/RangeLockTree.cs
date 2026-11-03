using System.Diagnostics;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Utilities.Transactions;

/// <summary>A range lock held in a <see cref="RangeLockTree"/> (locktree row_lock / treenode).</summary>
internal sealed class RowLock
{
    /// <summary>Serialized endpoints (suffix byte + key; see <see cref="RangeLockTree.Serialize"/>).</summary>
    public byte[] Left = null!;
    public byte[] Right = null!;
    /// <summary>The single owner, or null when the shared lock has several owners (TXNID_SHARED; see <see cref="Owners"/>).</summary>
    public PessimisticTransaction? Txn;
    public bool IsShared;
    /// <summary>The owners of a shared lock held by several transactions (TxnidVector); null otherwise.</summary>
    public List<PessimisticTransaction>? Owners;

    public bool IsOwnedBy(PessimisticTransaction txn) => Txn == txn || (Owners is not null && Owners.Contains(txn));
}

/// <summary>A range of a lock buffer (range_buffer record).</summary>
internal readonly record struct LockRange(byte[] Left, byte[] Right, bool Exclusive);

/// <summary>
/// The range locks of one column family. Port of toku::locktree (utilities/transactions/lock/range/range_tree/lib/
/// locktree/locktree.cc, keyrange.cc, treenode.cc, concurrent_tree.cc, wfg.cc) with the pending lock requests of
/// lock_request.cc:
/// <list type="bullet">
/// <item>The held locks are disjoint closed ranges [left, right] kept in a list sorted by endpoint (the locktree's
/// concurrent binary tree; a sorted array is the idiomatic managed interval set: lookups binary-search the first range
/// whose right endpoint is not below the requested left endpoint, then scan while ranges start at or before the requested
/// right endpoint). One mutex guards it (upstream locks subtrees).</item>
/// <item>Acquisition consolidates: a request that overlaps only its own transaction's locks replaces them by their
/// union (shared only if all were shared). Any overlap with another transaction's lock is a conflict -- also between
/// shared locks, unless the request is shared and has exactly the endpoints of the single overlapping shared lock (then
/// it joins its owners).</item>
/// <item>Single transaction optimization (STO): while only one transaction locks the tree, its ranges are appended to a
/// buffer without consolidation; another transaction's request (or a partial release) migrates them into the tree.</item>
/// <item>Lock escalation merges each transaction's adjacent exclusive locks into one range (in batches of 128, never
/// across an escalation barrier) when the manager runs out of lock memory.</item>
/// <item>Pending requests are kept per tree, sorted by transaction id; every unlock retries them all. Deadlocks are
/// found by building the wait-for graph of the pending requests of this tree when a request starts waiting.</item>
/// </list>
/// Lock order: <see cref="RequestsMutex"/> before the tree mutex; the tree mutex is never held while waiting.
/// </summary>
internal sealed class RangeLockTree
{
    internal const byte SuffixInfimum = 0;
    internal const byte SuffixSupremum = 1;
    /// <summary>locktree::STO_BUFFER_MAX_SIZE.</summary>
    internal const int StoBufferMaxSize = 50 * 1024;
    /// <summary>locktree::STO_SCORE_THRESHOLD.</summary>
    internal const int StoScoreThreshold = 100;

    // Memory accounting estimates of the native structures (LP64): keyrange (two DBT copies + two pointers + flag),
    // treenode (toku_mutex_t, keyrange, txnid, owners, children, comparator, flags), range_buffer record_header.
    internal const int KeyRangeOverhead = 88;
    internal const int TreeNodeOverhead = 208;
    internal const int RangeBufferRecordHeader = 10;

    // Request results (DB_LOCK_* / TOKUDB_OUT_OF_LOCKS).
    internal const int Granted = 0;
    internal const int NotGranted = 1;
    internal const int LockDeadlock = 2;
    internal const int OutOfLocks = 3;
    internal const int Interrupted = 4;

    private readonly RangeLockManager _mgr;
    private readonly Comparator _cmp;
    private readonly Lock _treeMutex = new();
    private readonly List<RowLock> _locks = new();
    private readonly List<RowLock> _overlaps = new(); // scratch, under _treeMutex

    // STO state, under _treeMutex.
    private PessimisticTransaction? _stoTxn;
    private readonly List<LockRange> _stoBuffer = new();
    private long _stoBufferMemory;
    private int _stoScore = StoScoreThreshold;

    /// <summary>Guards the pending requests and the counters (lt_lock_request_info::mutex).</summary>
    internal readonly Lock RequestsMutex = new();
    private readonly List<RangeLockRequest> _pending = new(); // sorted by transaction id
    private volatile bool _pendingEmpty = true;
    private ulong _waitCount;
    private ulong _timeoutCount;

    public RangeLockTree(RangeLockManager mgr, uint cfId, Comparator cmp)
    {
        _mgr = mgr;
        CfId = cfId;
        _cmp = cmp;
    }

    public uint CfId { get; }

    public ulong WaitCount
    {
        get
        {
            lock (RequestsMutex) return _waitCount;
        }
    }

    // ---- endpoints ------------------------------------------------------------------------------------------

    /// <summary>serialize_endpoint: the suffix byte (SUFFIX_INFIMUM / SUFFIX_SUPREMUM) followed by the key.</summary>
    internal static byte[] Serialize(ReadOnlySpan<byte> key, bool infSuffix)
    {
        byte[] b = new byte[key.Length + 1];
        b[0] = infSuffix ? SuffixSupremum : SuffixInfimum;
        key.CopyTo(b.AsSpan(1));
        return b;
    }

    /// <summary>deserialize_endpoint.</summary>
    internal static RangeLockEndpoint Deserialize(byte[] e) => new(e.AsMemory(1), e[0] == SuffixSupremum);

    /// <summary>
    /// RangeTreeLockManager::CompareDbtEndpoints: the user keys' common prefix with the column family's comparator
    /// (without timestamps), then the suffix byte as a positional marker (infimum before, supremum after).
    /// </summary>
    internal int Compare(byte[] a, byte[] b)
    {
        int minLen = Math.Min(a.Length, b.Length);
        int res = _cmp.CompareWithoutTimestamp(a.AsSpan(1, minLen - 1), false, b.AsSpan(1, minLen - 1), false);
        if (res != 0) return res;
        if (b.Length > minLen) return a[0] == SuffixInfimum ? -1 : 1; // a's user key is a prefix of b's
        if (a.Length > minLen) return b[0] == SuffixInfimum ? 1 : -1; // b's user key is a prefix of a's
        return a[0].CompareTo(b[0]);
    }

    private static long RowLockSize(byte[] left, byte[] right) => left.Length + right.Length + KeyRangeOverhead + TreeNodeOverhead;

    private static long BufferRecordSize(byte[] left, byte[] right) =>
        RangeBufferRecordHeader + left.Length + (left.AsSpan().SequenceEqual(right) ? 0 : right.Length);

    // ---- the sorted interval set (REQUIRES: _treeMutex) --------------------------------------------------------

    /// <summary>Index of the first lock whose right endpoint is not below <paramref name="left"/>.</summary>
    private int LowerBound(byte[] left)
    {
        int lo = 0, hi = _locks.Count;
        while (lo < hi)
        {
            int mid = (lo + hi) >>> 1;
            if (Compare(_locks[mid].Right, left) < 0) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    /// <summary>The locks overlapping [left, right], in key order (iterate_and_get_overlapping_row_locks).</summary>
    private void FindOverlaps(byte[] left, byte[] right, List<RowLock> result)
    {
        result.Clear();
        for (int i = LowerBound(left); i < _locks.Count && Compare(_locks[i].Left, right) <= 0; i++) result.Add(_locks[i]);
    }

    /// <summary>
    /// Inserts a lock (treenode::insert): a new range goes to its position; a range equal to a held shared lock adds an
    /// owner. Returns false if <paramref name="txn"/> already owned that lock.
    /// </summary>
    private bool Insert(byte[] left, byte[] right, PessimisticTransaction txn, bool isShared)
    {
        int i = LowerBound(left);
        if (i < _locks.Count)
        {
            RowLock o = _locks[i];
            if (Compare(o.Left, left) == 0 && Compare(o.Right, right) == 0)
            {
                Debug.Assert(isShared && o.IsShared);
                return AddSharedOwner(o, txn);
            }
            Debug.Assert(Compare(o.Left, right) > 0, "range locks must not overlap");
        }
        _locks.Insert(i, new RowLock { Left = left, Right = right, Txn = txn, IsShared = isShared });
        return true;
    }

    /// <summary>
    /// treenode::add_shared_owner. Unlike upstream, a transaction already in the owner list is not added (and its
    /// memory not counted) again.
    /// </summary>
    private static bool AddSharedOwner(RowLock o, PessimisticTransaction txn)
    {
        Debug.Assert(o.IsShared);
        if (o.Txn == txn) return false; // acquiring a lock on the same range by the same trx
        if (o.Txn is not null)
        {
            o.Owners = new List<PessimisticTransaction>(2) { o.Txn };
            o.Txn = null;
        }
        else if (o.Owners!.Contains(txn))
        {
            return false;
        }
        o.Owners!.Add(txn);
        return true;
    }

    /// <summary>
    /// treenode::remove: removes <paramref name="txn"/> from the owners of a shared lock with several owners, otherwise
    /// (or with <paramref name="txn"/> null = TXNID_ANY) removes the lock.
    /// </summary>
    private void Remove(RowLock l, PessimisticTransaction? txn)
    {
        if (txn is not null && l.Owners is { } owners)
        {
            owners.Remove(txn);
            if (owners.Count == 1)
            {
                // if there is just one owner left, move it to m_txnid
                l.Txn = owners[0];
                l.Owners = null;
            }
            return;
        }
        int i = LowerBound(l.Left);
        Debug.Assert(ReferenceEquals(_locks[i], l));
        _locks.RemoveAt(i);
    }

    /// <summary>determine_conflicting_txnids: true if any overlapping lock is owned by another transaction.</summary>
    private static bool DetermineConflicts(List<RowLock> overlaps, PessimisticTransaction txn, List<PessimisticTransaction>? conflicts)
    {
        bool exist = false;
        foreach (RowLock l in overlaps)
        {
            if (l.Txn == txn) continue;
            if (conflicts is not null)
            {
                if (l.Txn is null)
                {
                    foreach (PessimisticTransaction o in l.Owners!)
                    {
                        if (o != txn && !conflicts.Contains(o)) conflicts.Add(o);
                    }
                }
                else if (!conflicts.Contains(l.Txn))
                {
                    conflicts.Add(l.Txn);
                }
            }
            exist = true;
        }
        return exist;
    }

    /// <summary>locktree::acquire_lock_consolidated. REQUIRES: tree mutex.</summary>
    private int AcquireConsolidated(PessimisticTransaction txn, byte[] left, byte[] right, bool isWrite, List<PessimisticTransaction>? conflicts)
    {
        List<RowLock> overlaps = _overlaps;
        FindOverlaps(left, right, overlaps);
        // A read request matching the endpoints of the one overlapping shared lock joins its owners.
        if (!isWrite && overlaps.Count == 1 && overlaps[0].IsShared && Compare(left, overlaps[0].Left) == 0 && Compare(right, overlaps[0].Right) == 0)
        {
            if (AddSharedOwner(overlaps[0], txn)) _mgr.NoteMemUsed(RowLockSize(left, right));
            overlaps.Clear();
            return Granted;
        }
        if (DetermineConflicts(overlaps, txn, conflicts))
        {
            overlaps.Clear();
            return NotGranted;
        }
        // Consolidate: replace this transaction's overlapping locks by their union.
        bool allShared = !isWrite;
        foreach (RowLock l in overlaps)
        {
            Debug.Assert(l.Txn == txn);
            if (Compare(l.Left, left) < 0) left = l.Left;
            if (Compare(l.Right, right) > 0) right = l.Right;
            Remove(l, null);
            _mgr.NoteMemReleased(RowLockSize(l.Left, l.Right));
            allShared &= l.IsShared;
        }
        overlaps.Clear();
        Insert(left, right, txn, allShared);
        _mgr.NoteMemUsed(RowLockSize(left, right));
        return Granted;
    }

    /// <summary>
    /// locktree::try_acquire_lock: checks the lock memory limit (escalating if needed), then acquires through the STO
    /// buffer or the tree. On NotGranted, <paramref name="conflicts"/> receives the conflicting transactions.
    /// </summary>
    public int AcquireLock(bool isWrite, PessimisticTransaction txn, byte[] left, byte[] right, List<PessimisticTransaction>? conflicts)
    {
        int r = _mgr.CheckCurrentLockConstraints();
        if (r != Granted) return r;
        lock (_treeMutex)
        {
            if (StoTryAcquire(txn, left, right, isWrite)) return Granted;
            return AcquireConsolidated(txn, left, right, isWrite, conflicts);
        }
    }

    /// <summary>locktree::get_conflicts.</summary>
    public void GetConflicts(PessimisticTransaction txn, byte[] left, byte[] right, List<PessimisticTransaction> conflicts)
    {
        lock (_treeMutex)
        {
            var overlaps = new List<RowLock>();
            FindOverlaps(left, right, overlaps);
            DetermineConflicts(overlaps, txn, conflicts);
        }
    }

    // ---- single transaction optimization (REQUIRES: tree mutex) -----------------------------------------------

    private bool StoTryAcquire(PessimisticTransaction txn, byte[] left, byte[] right, bool isWrite)
    {
        if (_locks.Count == 0 && _stoBuffer.Count == 0 && _stoScore >= StoScoreThreshold)
        {
            _stoTxn = txn; // sto_begin
        }
        else if (_stoTxn is not null && (_stoTxn != txn || _stoBuffer.Count > StoBufferMaxSize))
        {
            StoEndEarly();
        }
        if (_stoTxn is null) return false;
        Debug.Assert(_stoTxn == txn);
        // sto_append
        _stoBuffer.Add(new LockRange(left, right, isWrite));
        long size = BufferRecordSize(left, right);
        _stoBufferMemory += size;
        _mgr.NoteMemUsed(size);
        return true;
    }

    private void StoEnd()
    {
        _mgr.NoteMemReleased(_stoBufferMemory);
        _stoBuffer.Clear();
        _stoBufferMemory = 0;
        _stoTxn = null;
    }

    /// <summary>sto_end_early(_no_accounting): migrates the buffer's ranges (consolidated) into the tree.</summary>
    private void StoEndEarly()
    {
        Debug.Assert(_locks.Count == 0);
        PessimisticTransaction txn = _stoTxn!;
        foreach (LockRange r in _stoBuffer)
        {
            int res = AcquireConsolidated(txn, r.Left, r.Right, r.Exclusive, null);
            Debug.Assert(res == Granted);
        }
        StoEnd();
        _stoScore = 0;
    }

    // ---- release ----------------------------------------------------------------------------------------------

    /// <summary>
    /// locktree::release_locks: releases <paramref name="txn"/>'s locks overlapping <paramref name="ranges"/>. With
    /// <paramref name="allTrxLocks"/> (the transaction releases everything) the STO buffer is simply dropped.
    /// </summary>
    public void ReleaseLocks(PessimisticTransaction txn, List<LockRange> ranges, bool allTrxLocks)
    {
        lock (_treeMutex)
        {
            if (allTrxLocks)
            {
                if (_stoTxn is not null && _stoTxn == txn)
                {
                    StoEnd();
                    return;
                }
            }
            else if (_stoTxn is not null)
            {
                // Releasing some of the locks: leave the STO mode and let the tree code path handle it.
                StoEndEarly();
            }
            List<RowLock> overlaps = _overlaps;
            foreach (LockRange r in ranges)
            {
                FindOverlaps(r.Left, r.Right, overlaps);
                foreach (RowLock l in overlaps)
                {
                    if (!l.IsOwnedBy(txn)) continue;
                    long size = RowLockSize(l.Left, l.Right);
                    Remove(l, txn);
                    _mgr.NoteMemReleased(size);
                }
            }
            overlaps.Clear();
            if (_stoScore < StoScoreThreshold) _stoScore++;
        }
    }

    // ---- escalation -------------------------------------------------------------------------------------------

    /// <summary>
    /// locktree::escalate: merges each transaction's consecutive exclusive locks (batches of 128 locks, not across an
    /// escalation barrier) and hands every transaction its new lock list.
    /// </summary>
    public void Escalate()
    {
        lock (_treeMutex)
        {
            if (_stoTxn is not null) StoEndEarly();
            var buffers = new SortedDictionary<ulong, (PessimisticTransaction Txn, List<LockRange> Ranges)>();
            const int BatchSize = 128;
            var batch = new RowLock[BatchSize];
            while (_locks.Count > 0)
            {
                // extract_first_n_row_locks
                int n = Math.Min(BatchSize, _locks.Count);
                for (int i = 0; i < n; i++)
                {
                    batch[i] = _locks[i];
                    _mgr.NoteMemReleased(RowLockSize(batch[i].Left, batch[i].Right));
                }
                _locks.RemoveRange(0, n);
                int current = 0;
                while (current < n)
                {
                    RowLock cur = batch[current];
                    int next = current + 1;
                    while (next < n && cur.Txn is not null && batch[next].Txn == cur.Txn && !batch[next].IsShared && batch[next].Owners is null &&
                           !_mgr.IsEscalationBarrier(cur.Right, batch[next].Left))
                    {
                        next++;
                    }
                    var range = new LockRange(cur.Left, batch[next - 1].Right, !cur.IsShared);
                    if (cur.Owners is { } owners)
                    {
                        foreach (PessimisticTransaction o in owners) AddRange(buffers, o, range);
                    }
                    else
                    {
                        AddRange(buffers, cur.Txn!, range);
                    }
                    current = next;
                }
                Array.Clear(batch);
            }
            foreach ((PessimisticTransaction txn, List<LockRange> ranges) in buffers.Values)
            {
                foreach (LockRange r in ranges)
                {
                    Insert(r.Left, r.Right, txn, !r.Exclusive);
                    _mgr.NoteMemUsed(RowLockSize(r.Left, r.Right));
                }
                // RangeTreeLockManager::on_escalate
                txn.RangeLocks?.ReplaceLocks(CfId, ranges);
            }
        }
    }

    private static void AddRange(SortedDictionary<ulong, (PessimisticTransaction Txn, List<LockRange> Ranges)> buffers, PessimisticTransaction txn, LockRange r)
    {
        if (!buffers.TryGetValue(txn.Id, out var b)) buffers[txn.Id] = b = (txn, new List<LockRange>());
        b.Ranges.Add(r);
    }

    // ---- status -------------------------------------------------------------------------------------------------

    /// <summary>locktree::dump_locks: the STO buffer while in STO mode, else the tree's locks in key order.</summary>
    public void DumpLocks(List<RangeLockInfo> result)
    {
        lock (_treeMutex)
        {
            if (_stoTxn is { } sto)
            {
                foreach (LockRange r in _stoBuffer)
                {
                    result.Add(new RangeLockInfo(CfId, Deserialize(r.Left), Deserialize(r.Right), [sto.Id], r.Exclusive));
                }
                return;
            }
            foreach (RowLock l in _locks)
            {
                ulong[] ids;
                if (l.Txn is not null)
                {
                    ids = [l.Txn.Id];
                }
                else
                {
                    ids = new ulong[l.Owners!.Count];
                    for (int i = 0; i < ids.Length; i++) ids[i] = l.Owners[i].Id;
                }
                result.Add(new RangeLockInfo(CfId, Deserialize(l.Left), Deserialize(l.Right), ids, !l.IsShared));
            }
        }
    }

    // ---- pending requests (REQUIRES: RequestsMutex) -------------------------------------------------------------

    internal RangeLockRequest? FindPending(PessimisticTransaction txn)
    {
        int i = PendingIndex(txn.Id);
        return i < _pending.Count && _pending[i].Txn == txn ? _pending[i] : null;
    }

    private int PendingIndex(ulong id)
    {
        int lo = 0, hi = _pending.Count;
        while (lo < hi)
        {
            int mid = (lo + hi) >>> 1;
            if (_pending[mid].Txn.Id < id) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    internal void InsertPending(RangeLockRequest r)
    {
        int i = PendingIndex(r.Txn.Id);
        Debug.Assert(i == _pending.Count || _pending[i].Txn != r.Txn, "a transaction waits for one lock at a time");
        _pending.Insert(i, r);
        _pendingEmpty = false;
    }

    internal void RemovePending(RangeLockRequest r)
    {
        int i = PendingIndex(r.Txn.Id);
        Debug.Assert(i < _pending.Count && _pending[i] == r);
        _pending.RemoveAt(i);
        if (_pending.Count == 0) _pendingEmpty = true;
    }

    internal void CountWait(bool timedOut)
    {
        _waitCount++;
        if (timedOut) _timeoutCount++;
    }

    /// <summary>
    /// lock_request::retry_all_lock_requests: retries every pending request (in transaction id order) after locks were
    /// released, then reports who the still-waiting requests wait for.
    /// </summary>
    public void RetryAll()
    {
        if (_pendingEmpty) return;
        List<(RangeLockRequest Request, List<ulong> WaitIds)>? waits = null;
        lock (RequestsMutex)
        {
            for (int i = 0; i < _pending.Count;)
            {
                if (_pending[i].Retry(ref waits) != Granted) i++;
            }
            RangeLockRequest.ReportWaits(waits);
        }
    }

    // ---- deadlock detection (wfg) ------------------------------------------------------------------------------

    /// <summary>
    /// lock_request::deadlock_exists: builds the wait-for graph from <paramref name="request"/>'s conflicts through the
    /// pending requests of this tree and searches it for a cycle back to the requester. On a deadlock,
    /// <paramref name="path"/> receives the waiting requests of the cycle, starting after the requester and ending with it.
    /// REQUIRES: RequestsMutex.
    /// </summary>
    internal bool DeadlockExists(RangeLockRequest request, List<PessimisticTransaction> conflicts, List<RangeDeadlockInfo> path)
    {
        var graph = new Dictionary<PessimisticTransaction, List<PessimisticTransaction>>(ReferenceEqualityComparer.Instance);
        BuildWaitGraph(graph, request, conflicts);
        if (!graph.ContainsKey(request.Txn)) return false;
        var visited = new HashSet<PessimisticTransaction>(ReferenceEqualityComparer.Instance);
        if (!CycleExistsFromNode(graph, request.Txn, request.Txn, visited, path)) return false;
        path.Reverse();
        return true;
    }

    private void BuildWaitGraph(Dictionary<PessimisticTransaction, List<PessimisticTransaction>> graph, RangeLockRequest request,
        List<PessimisticTransaction> conflicts)
    {
        foreach (PessimisticTransaction c in conflicts)
        {
            RangeLockRequest? conflicting = FindPending(c);
            if (conflicting is null) continue;
            bool alreadyExists = graph.ContainsKey(c);
            // add_edge (find_create_node for both ends)
            if (!graph.TryGetValue(request.Txn, out List<PessimisticTransaction>? edges)) graph[request.Txn] = edges = new List<PessimisticTransaction>();
            if (!edges.Contains(c)) edges.Add(c);
            if (!graph.ContainsKey(c)) graph[c] = new List<PessimisticTransaction>();
            if (!alreadyExists)
            {
                var other = new List<PessimisticTransaction>();
                GetConflicts(conflicting.Txn, conflicting.Left, conflicting.Right, other);
                BuildWaitGraph(graph, conflicting, other);
            }
        }
    }

    /// <summary>wfg::cycle_exists_from_node: depth-first; the reporter runs for the target, then each node back up the path.</summary>
    private bool CycleExistsFromNode(Dictionary<PessimisticTransaction, List<PessimisticTransaction>> graph, PessimisticTransaction target,
        PessimisticTransaction head, HashSet<PessimisticTransaction> visited, List<RangeDeadlockInfo> path)
    {
        bool found = false;
        visited.Add(head);
        List<PessimisticTransaction> edges = graph[head];
        for (int i = 0; i < edges.Count && !found; i++)
        {
            PessimisticTransaction e = edges[i];
            if (e == target)
            {
                found = true;
                Report(e, path);
            }
            else if (graph.ContainsKey(e) && !visited.Contains(e))
            {
                found = CycleExistsFromNode(graph, target, e, visited, path);
                if (found) Report(e, path);
            }
        }
        visited.Remove(head);
        return found;
    }

    private void Report(PessimisticTransaction txn, List<RangeDeadlockInfo> path)
    {
        if (FindPending(txn) is { } req)
        {
            path.Add(new RangeDeadlockInfo(req.Txn.Id, CfId, req.IsWrite, Deserialize(req.Left), Deserialize(req.Right)));
        }
    }
}

/// <summary>
/// One lock request (toku::lock_request, lock_request.cc): start (acquire or become pending, with deadlock detection),
/// then wait (retry, wait for a release, honour the timeout and the kill callback).
/// </summary>
internal sealed class RangeLockRequest
{
    private readonly RangeLockTree _tree;
    private bool _pending;           // under the tree's RequestsMutex once inserted
    private int _completeR;
    private TaskCompletionSource? _signal;

    public RangeLockRequest(RangeLockTree tree, PessimisticTransaction txn, byte[] left, byte[] right, bool isWrite, byte[] waitKey)
    {
        _tree = tree;
        Txn = txn;
        Left = left;
        Right = right;
        IsWrite = isWrite;
        WaitKey = waitKey;
    }

    public PessimisticTransaction Txn { get; }
    public byte[] Left { get; }
    public byte[] Right { get; }
    public bool IsWrite { get; }
    /// <summary>The start endpoint's key, reported by GetWaitingTxns (wait_key / m_extra).</summary>
    public byte[] WaitKey { get; }
    public long StartTimeMs { get; private set; }
    public int Result => _completeR;

    /// <summary>
    /// lock_request::start. Returns true if the request completed (see <see cref="Result"/>); false if it is pending
    /// (call <see cref="WaitAsync"/>). On a deadlock, <paramref name="deadlockPath"/> holds the cycle.
    /// </summary>
    public bool Start(List<RangeDeadlockInfo> deadlockPath)
    {
        var conflicts = new List<PessimisticTransaction>();
        int r = _tree.AcquireLock(IsWrite, Txn, Left, Right, conflicts);
        if (r == RangeLockTree.NotGranted)
        {
            StartTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            lock (_tree.RequestsMutex)
            {
                _pending = true;
                _tree.InsertPending(this);
                if (_tree.DeadlockExists(this, conflicts, deadlockPath))
                {
                    _tree.RemovePending(this);
                    _pending = false;
                    r = RangeLockTree.LockDeadlock;
                }
                else
                {
                    return false;
                }
            }
        }
        _completeR = r;
        return true;
    }

    /// <summary>lock_request::retry. REQUIRES: RequestsMutex, pending.</summary>
    internal int Retry(ref List<(RangeLockRequest Request, List<ulong> WaitIds)>? waits)
    {
        var conflicts = new List<PessimisticTransaction>();
        int r = _tree.AcquireLock(IsWrite, Txn, Left, Right, conflicts);
        if (r == RangeLockTree.Granted)
        {
            _tree.RemovePending(this);
            Complete(r);
        }
        else
        {
            var ids = new List<ulong>(conflicts.Count);
            foreach (PessimisticTransaction c in conflicts) ids.Add(c.Id);
            (waits ??= new()).Add((this, ids));
        }
        return r;
    }

    /// <summary>REQUIRES: RequestsMutex.</summary>
    private void Complete(int r)
    {
        _completeR = r;
        _pending = false;
        _signal?.TrySetResult();
    }

    /// <summary>wait_callback_for_locktree: tells the waiting transactions whom they wait for.</summary>
    internal static void ReportWaits(List<(RangeLockRequest Request, List<ulong> WaitIds)>? waits)
    {
        if (waits is null) return;
        foreach ((RangeLockRequest req, List<ulong> ids) in waits)
        {
            if (req._pending) req.Txn.SetWaitingTxn(ids, req._tree.CfId, req.WaitKey);
        }
    }

    /// <summary>
    /// lock_request::wait: retries once, then waits for releases until granted, timed out (<paramref name="waitMicros"/>)
    /// or killed (<paramref name="isKilled"/>, polled every 100 ms). Counts the wait. Completes with the result code.
    /// </summary>
    public async ValueTask<int> WaitAsync(long waitMicros, Func<bool>? isKilled, bool synchronous, CancellationToken cancellationToken)
    {
        const long KillPollMicros = 100_000;
        long tStart = PointLockManager.NowMicros();
        long tEnd = tStart + waitMicros;
        bool timedOut = false;
        try
        {
            lock (_tree.RequestsMutex)
            {
                if (_pending)
                {
                    List<(RangeLockRequest, List<ulong>)>? waits = null;
                    Retry(ref waits);
                    if (_pending) ReportWaits(waits);
                }
            }
            while (true)
            {
                TaskCompletionSource signal;
                long wait;
                lock (_tree.RequestsMutex)
                {
                    if (!_pending) break;
                    if (isKilled is not null && isKilled())
                    {
                        _tree.RemovePending(this);
                        Complete(RangeLockTree.Interrupted);
                        break;
                    }
                    long now = PointLockManager.NowMicros();
                    long tWait = isKilled is null ? tEnd : Math.Min(now + KillPollMicros, tEnd);
                    wait = tWait - now;
                    _signal = signal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                }
                if (wait > 0)
                {
                    var timeout = TimeSpan.FromTicks(Math.Min(wait, (long)int.MaxValue * 1000) * 10);
                    if (synchronous) signal.Task.Wait(timeout, cancellationToken);
                    else await signal.Task.WaitAsync(timeout, cancellationToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
                    cancellationToken.ThrowIfCancellationRequested();
                }
                lock (_tree.RequestsMutex)
                {
                    _signal = null;
                    if (_pending && PointLockManager.NowMicros() >= tEnd)
                    {
                        timedOut = true;
                        _tree.RemovePending(this);
                        Complete(RangeLockTree.NotGranted);
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            lock (_tree.RequestsMutex)
            {
                if (_pending)
                {
                    _tree.RemovePending(this);
                    Complete(RangeLockTree.Interrupted);
                }
                _tree.CountWait(false);
            }
            throw;
        }
        lock (_tree.RequestsMutex) _tree.CountWait(timedOut);
        return _completeR;
    }
}

/// <summary>
/// The range locks a transaction holds, per column family (RangeLockList, range_tree_lock_tracker.cc): every point and
/// range lock it acquired, to release them at commit / rollback. Lock escalation replaces a column family's list with
/// the escalated ranges (ReplaceLocks), unless the transaction is releasing its locks.
/// </summary>
internal sealed class RangeLockList
{
    private readonly Lock _mutex = new();
    private readonly Dictionary<uint, List<LockRange>> _buffers = new();
    private bool _releasingLocks;

    /// <summary>Records an acquired range. Only the transaction's owner calls this.</summary>
    public void Append(uint cf, byte[] left, byte[] right)
    {
        lock (_mutex)
        {
            Debug.Assert(!_releasingLocks);
            if (!_buffers.TryGetValue(cf, out List<LockRange>? list)) _buffers[cf] = list = new List<LockRange>();
            list.Add(new LockRange(left, right, true));
        }
    }

    /// <summary>
    /// RangeLockList::ReleaseLocks. The releasing flag (set under the mutex, then the mutex is dropped) keeps lock
    /// escalation, which holds a tree's mutex while calling <see cref="ReplaceLocks"/>, from deadlocking with the release
    /// that takes the trees' mutexes.
    /// </summary>
    public void ReleaseLocks(RangeLockManager mgr, PessimisticTransaction txn, bool allTrxLocks)
    {
        lock (_mutex) _releasingLocks = true;
        try
        {
            foreach (KeyValuePair<uint, List<LockRange>> kv in _buffers)
            {
                if (kv.Value.Count == 0) continue;
                if (mgr.TryGetTree(kv.Key) is { } tree)
                {
                    tree.ReleaseLocks(txn, kv.Value, allTrxLocks);
                    tree.RetryAll();
                }
            }
        }
        finally
        {
            lock (_mutex)
            {
                _buffers.Clear();
                _releasingLocks = false;
            }
        }
    }

    /// <summary>RangeLockList::ReplaceLocks (lock escalation callback).</summary>
    public void ReplaceLocks(uint cf, List<LockRange> ranges)
    {
        lock (_mutex)
        {
            // The transaction is releasing its locks, so it will not care about having a correct list of ranges.
            if (_releasingLocks) return;
            _buffers[cf] = new List<LockRange>(ranges);
        }
    }
}
