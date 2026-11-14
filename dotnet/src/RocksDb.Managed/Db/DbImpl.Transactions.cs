using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using RocksDb.Managed.Table;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Db;

/// <summary>
/// A prepared (or, for WriteUnprepared, partially written) transaction rebuilt from the WAL during recovery
/// (DBImpl::RecoveredTransaction, db/db_impl/db_impl.h). WriteCommitted and WritePrepared transactions have one batch;
/// WriteUnprepared ones have one batch per unprepared / prepared section, keyed by the sequence number of its first key.
/// </summary>
internal sealed class RecoveredTransaction
{
    /// <summary>One section of the transaction (RecoveredTransaction::BatchInfo).</summary>
    internal readonly record struct BatchInfo(ulong LogNumber, WriteBatch Batch, int BatchCount);

    private readonly SortedList<ulong, BatchInfo> _batches = new();

    public RecoveredTransaction(string name, ulong logNumber, WriteBatch batch, ulong sequence, int batchCount, bool unprepared)
    {
        Name = name;
        Unprepared = unprepared;
        _batches.Add(sequence, new BatchInfo(logNumber, batch, batchCount));
    }

    public string Name { get; }

    /// <summary>unprepared_: the last section seen was unprepared (the transaction was never prepared).</summary>
    public bool Unprepared { get; private set; }

    /// <summary>The sections by first sequence number (batches_).</summary>
    public IList<ulong> Sequences => _batches.Keys;

    public IList<BatchInfo> Batches => _batches.Values;

    /// <summary>The WAL holding the (first) prepared section.</summary>
    public ulong LogNumber => _batches.Values[0].LogNumber;

    /// <summary>The prepared writes of the (first) section (no markers).</summary>
    public WriteBatch Batch => _batches.Values[0].Batch;

    /// <summary>The sequence number the inserter was at when the (first) section began.</summary>
    public ulong Sequence => _batches.Keys[0];

    /// <summary>RecoveredTransaction::AddBatch (WriteUnprepared: the prepare batch must be the last batch).</summary>
    public void AddBatch(ulong sequence, ulong logNumber, WriteBatch batch, int batchCount, bool unprepared)
    {
        if (!Unprepared) ThrowHelper.Corruption("WAL: a prepared transaction has another section: " + Name);
        _batches[sequence] = new BatchInfo(logNumber, batch, batchCount);
        Unprepared = unprepared;
    }

    public void Dispose()
    {
        foreach (BatchInfo b in _batches.Values) b.Batch.Dispose();
    }
}

/// <summary>
/// DB-layer support for transactions:
/// <list type="bullet">
/// <item>2PC bookkeeping: recovered prepared transactions (DBImpl::InsertRecoveredTransaction & co.), the WALs holding
/// outstanding prepared sections (db/logs_with_prep_tracker.cc) and the 2PC minimum WAL to keep
/// (PrecomputeMinLogNumberToKeep2PC / FindMinPrepLogReferencedByMemTable, db/db_impl/db_impl_files.cc).</item>
/// <item>Memtable history for conflict checks (MemTableList history trimmed to max_write_buffer_size_to_maintain).</item>
/// <item>Conflict checks: DBImpl::GetEarliestMemTableSequenceNumber / GetLatestSequenceForKey (db/db_impl/db_impl.cc)
/// and TransactionUtil::CheckKey (utilities/transactions/transaction_util.cc).</item>
/// </list>
/// </summary>
internal sealed partial class DbImpl
{
    /// <summary>The live column family with id <paramref name="id"/> (throws InvalidArgument for unknown / dropped ids).</summary>
    internal ColumnFamilyData GetColumnFamilyDataById(uint id)
    {
        ColumnFamilyData?[] map = Volatile.Read(ref _cfById);
        ColumnFamilyData? c = id < (uint)map.Length ? map[id] : null;
        if (c is null || c.Dropped) ThrowHelper.InvalidArgument("Invalid column family specified in write batch");
        return c!;
    }

    // ---- recovered transactions (guarded by the DB mutex; written during Open only) ----------------------

    private readonly Dictionary<string, RecoveredTransaction> _recoveredTransactions = new(StringComparer.Ordinal);

    /// <summary>Transaction names are stored in the WAL as their UTF-8 bytes.</summary>
    internal static string DecodeXid(ReadOnlySpan<byte> xid) => Encoding.UTF8.GetString(xid);

    /// <summary>
    /// DBImpl::InsertRecoveredTransaction: for WriteUnprepared it is called for every unprepared batch of the
    /// transaction; if the transaction is prepared, the last call has <paramref name="unprepared"/> = false.
    /// </summary>
    private void InsertRecoveredTransaction(ulong logNumber, ReadOnlySpan<byte> xid, WriteBatch batch, ulong sequence, int batchCount, bool unprepared)
    {
        string name = DecodeXid(xid);
        lock (_mutex)
        {
            if (_recoveredTransactions.TryGetValue(name, out RecoveredTransaction? rtxn) && rtxn.Unprepared)
            {
                rtxn.AddBatch(sequence, logNumber, batch, batchCount, unprepared);
            }
            else
            {
                // A name reused after its transaction finished (or a duplicate prepare in a corrupted WAL) replaces it.
                if (_recoveredTransactions.Remove(name, out RecoveredTransaction? old)) old.Dispose();
                _recoveredTransactions[name] = new RecoveredTransaction(name, logNumber, batch, sequence, batchCount, unprepared);
            }
        }
    }

    private RecoveredTransaction? GetRecoveredTransaction(ReadOnlySpan<byte> xid)
    {
        string name = DecodeXid(xid);
        lock (_mutex) return _recoveredTransactions.GetValueOrDefault(name);
    }

    private void DeleteRecoveredTransaction(RecoveredTransaction trx)
    {
        lock (_mutex) _recoveredTransactions.Remove(trx.Name);
        trx.Dispose();
    }

    /// <summary>
    /// Hands the prepared transactions recovered from the WAL to the caller (TransactionDB initialization) and forgets
    /// them (DBImpl::recovered_transactions + DeleteAllRecoveredTransactions). Their WALs stay marked as containing
    /// outstanding prepared sections until the caller commits or rolls them back.
    /// </summary>
    internal List<RecoveredTransaction> TakeRecoveredTransactions()
    {
        lock (_mutex)
        {
            var list = new List<RecoveredTransaction>(_recoveredTransactions.Values);
            _recoveredTransactions.Clear();
            list.Sort(static (a, b) => a.Sequence.CompareTo(b.Sequence));
            return list;
        }
    }

    /// <summary>Called at the end of WAL recovery: the WALs of recovered prepared sections must be kept.</summary>
    private void MarkRecoveredPrepSections()
    {
        lock (_mutex)
        {
            foreach (RecoveredTransaction t in _recoveredTransactions.Values)
            {
                foreach (RecoveredTransaction.BatchInfo b in t.Batches) MarkLogAsContainingPrepSection(b.LogNumber);
            }
        }
    }

    // ---- LogsWithPrepTracker ------------------------------------------------------------------------------

    private readonly Dictionary<ulong, int> _logsWithPrep = new(); // WAL -> outstanding prepared sections
    private readonly Lock _prepLock = new();

    /// <summary>LogsWithPrepTracker::MarkLogAsContainingPrepSection.</summary>
    internal void MarkLogAsContainingPrepSection(ulong log)
    {
        lock (_prepLock)
        {
            _logsWithPrep.TryGetValue(log, out int n);
            _logsWithPrep[log] = n + 1;
        }
    }

    /// <summary>LogsWithPrepTracker::MarkLogAsHavingPrepSectionFlushed: the prepared section was committed or rolled back.</summary>
    internal void MarkLogAsHavingPrepSectionFlushed(ulong log)
    {
        lock (_prepLock)
        {
            if (!_logsWithPrep.TryGetValue(log, out int n)) return;
            if (n <= 1) _logsWithPrep.Remove(log);
            else _logsWithPrep[log] = n - 1;
        }
    }

    /// <summary>LogsWithPrepTracker::FindMinLogContainingOutstandingPrep: 0 if none.</summary>
    internal ulong FindMinLogContainingOutstandingPrep()
    {
        lock (_prepLock)
        {
            ulong min = 0;
            foreach (ulong log in _logsWithPrep.Keys)
            {
                if (min == 0 || log < min) min = log;
            }
            return min;
        }
    }

    /// <summary>
    /// FindMinPrepLogReferencedByMemTable: the oldest WAL holding a prepared section whose committed data is still only
    /// in memtables (excluding <paramref name="beingFlushed"/>). 0 if none. REQUIRES: mutex.
    /// </summary>
    private ulong FindMinPrepLogReferencedByMemTable(List<ImmutableMemTable>? beingFlushed)
    {
        ulong min = 0;
        foreach (ColumnFamilyData c in Versions.ColumnFamilies)
        {
            if (c.Dropped) continue;
            foreach (ImmutableMemTable m in c.Imm)
            {
                // MemTableList::PrecomputeMinLogContainingPrepSection: every memtable still in the list, a completed flush
                // waiting to be committed included.
                if (beingFlushed is not null && beingFlushed.Contains(m)) continue;
                ulong log = m.Mem.MinLogContainingPrepSection;
                if (log > 0 && (min == 0 || log < min)) min = log;
            }
            ulong l = c.Mem.MinLogContainingPrepSection;
            if (l > 0 && (min == 0 || l < min)) min = l;
        }
        return min;
    }

    /// <summary>
    /// The smallest WAL number that must be kept (DBImpl::MinLogNumberToKeep for deletion): WALs with unflushed data
    /// and, with allow_2pc, WALs holding outstanding prepared sections or prepared sections of committed data that is
    /// still in memtables. REQUIRES: mutex.
    /// </summary>
    internal ulong MinLogNumberToKeepLive()
    {
        ulong min = Versions.MinLogNumberWithUnflushedData(null);
        if (!Options.Allow2Pc) return min;
        ulong prep = FindMinLogContainingOutstandingPrep();
        if (prep != 0 && prep < min) min = prep;
        ulong mem = FindMinPrepLogReferencedByMemTable(null);
        if (mem != 0 && mem < min) min = mem;
        return min;
    }

    /// <summary>
    /// PrecomputeMinLogNumberToKeep2PC for a flush of <paramref name="picked"/> of <paramref name="cfd"/> whose edit
    /// sets the CF log number to <paramref name="editLogNumber"/>. REQUIRES: mutex.
    /// </summary>
    private ulong PrecomputeMinLogNumberToKeep2PC(ColumnFamilyData cfd, ulong editLogNumber, List<ImmutableMemTable> picked)
    {
        ulong min = Math.Min(Versions.MinLogNumberWithUnflushedData(cfd), editLogNumber);
        // We must check min logs with outstanding prep before we check logs referenced by memtables because a log
        // referenced by the first data structure could transition to the second under us.
        ulong prep = FindMinLogContainingOutstandingPrep();
        if (prep != 0 && prep < min) min = prep;
        ulong mem = FindMinPrepLogReferencedByMemTable(picked);
        if (mem != 0 && mem < min) min = mem;
        return min;
    }

    // ---- memtable history (max_write_buffer_size_to_maintain) ---------------------------------------------

    /// <summary>
    /// A flushed memtable left the immutable list: keep it as history for conflict checking when the CF maintains
    /// history, otherwise release the list's reference. REQUIRES: mutex.
    /// </summary>
    private void RetireFlushedMemTable(ColumnFamilyData cfd, MemTable mem)
    {
        if (cfd.Options.MaxWriteBufferSizeToMaintain > 0 && !cfd.Dropped)
        {
            cfd.History.Add(mem); // keeps the list's reference
            TrimMemTableHistory(cfd);
            return;
        }
        if (mem.Unref()) mem.Dispose();
    }

    /// <summary>
    /// MemTableList::TrimHistory: drops the oldest history memtables while the memory of the mutable memtable,
    /// the immutable memtables and the history (excluding the oldest) reaches max_write_buffer_size_to_maintain.
    /// REQUIRES: mutex.
    /// </summary>
    internal void TrimMemTableHistory(ColumnFamilyData cfd)
    {
        List<MemTable> history = cfd.History;
        if (history.Count == 0) return;
        long maintain = cfd.Options.MaxWriteBufferSizeToMaintain;
        long usage = cfd.Mem.ApproximateMemoryUsageFast;
        foreach (ImmutableMemTable m in cfd.Imm) usage += m.Mem.ApproximateMemoryUsageFast;
        for (int i = 1; i < history.Count; i++) usage += history[i].ApproximateMemoryUsageFast;
        while (history.Count > 0 && (maintain <= 0 || usage >= maintain))
        {
            MemTable oldest = history[0];
            history.RemoveAt(0);
            if (history.Count > 0) usage -= history[0].ApproximateMemoryUsageFast;
            if (oldest.Unref()) oldest.Dispose();
        }
    }

    /// <summary>Releases the CF's history memtables. REQUIRES: mutex.</summary>
    private static void ReleaseMemTableHistory(ColumnFamilyData cfd)
    {
        foreach (MemTable m in cfd.History)
        {
            if (m.Unref()) m.Dispose();
        }
        cfd.History.Clear();
    }

    // ---- conflict checking ---------------------------------------------------------------------------------

    /// <summary>
    /// A referenced view of one column family's memtables (super version + history snapshot) used to check a set of
    /// keys for conflicts. Dispose to release the references.
    /// </summary>
    internal readonly struct ConflictCheckView : IDisposable
    {
        private readonly DbImpl _db;
        public readonly ColumnFamilyData Cfd;
        public readonly SuperVersion Sv;
        /// <summary>
        /// History memtables, newest first (referenced), in the first <see cref="HistoryCount"/> slots of an array rented
        /// from <see cref="RocksArrayPool{T}.Shared"/> (returned by <see cref="Dispose"/>); null if none.
        /// </summary>
        public readonly MemTable[]? History;
        public readonly int HistoryCount;
        /// <summary>GetEarliestMemTableSequenceNumber(sv, include_history = true).</summary>
        public readonly ulong EarliestSequence;

        public ConflictCheckView(DbImpl db, ColumnFamilyData cfd, SuperVersion sv, MemTable[]? history, int historyCount, ulong earliest)
        {
            _db = db;
            Cfd = cfd;
            Sv = sv;
            History = history;
            HistoryCount = historyCount;
            EarliestSequence = earliest;
        }

        public void Dispose()
        {
            if (_db is null) return;
            if (History is { } history)
            {
                lock (_db._mutex)
                {
                    for (int i = 0; i < HistoryCount; i++)
                    {
                        MemTable m = history[i];
                        if (m.Unref()) m.Dispose();
                    }
                }
                RocksArrayPool<MemTable>.Shared.Return(history, clearArray: true);
            }
            _db.ReturnSuperVersion(Sv);
        }
    }

    /// <summary>References the CF's memtables for conflict checks (GetAndRefSuperVersion + history).</summary>
    internal ConflictCheckView AcquireConflictCheckView(ColumnFamilyData cfd)
    {
        SuperVersion sv = GetReferencedSuperVersion(cfd);
        MemTable[]? history = null;
        int n;
        lock (_mutex)
        {
            n = cfd.History.Count;
            if (n > 0)
            {
                // Rented, not allocated: an optimistic commit takes a view per column family.
                history = RocksArrayPool<MemTable>.Shared.Rent(n);
                for (int i = 0; i < n; i++)
                {
                    MemTable m = cfd.History[n - 1 - i];
                    m.Ref();
                    history[i] = m;
                }
            }
        }
        // Find the earliest sequence number that we know we can rely on reading from the memtable without needing to
        // check sst files (the oldest memtable's earliest sequence number).
        ulong earliest = sv.Mem.EarliestSequenceNumber;
        if (sv.Imm.Length > 0) earliest = Math.Min(earliest, sv.Imm[^1].EarliestSequenceNumber);
        if (history is not null) earliest = Math.Min(earliest, history[n - 1].EarliestSequenceNumber);
        return new ConflictCheckView(this, cfd, sv, history, history is null ? 0 : n, earliest);
    }

    /// <summary>
    /// Port of TransactionUtil::CheckKey: returns null if <paramref name="key"/> was not written after
    /// <paramref name="snapSeq"/>, Busy if it was, TryAgain if the memtables do not hold enough history to tell and
    /// <paramref name="cacheOnly"/> is set (otherwise the table files are checked too).
    /// </summary>
    internal RocksDbException? CheckKey(in ConflictCheckView view, ReadOnlySpan<byte> key, ulong snapSeq, bool cacheOnly) =>
        CheckKey(view, key, snapSeq, default, enableUdtValidation: false, cacheOnly);

    /// <summary>
    /// <see cref="CheckKey(in ConflictCheckView, ReadOnlySpan{byte}, ulong, bool)"/> with timestamp-based conflict
    /// checking (TransactionUtil::CheckKey with read_ts): with <paramref name="enableUdtValidation"/> and a non-empty
    /// <paramref name="readTimestamp"/>, a key whose newest version has a newer timestamp than the read timestamp is a
    /// write conflict even when its sequence number is not newer than <paramref name="snapSeq"/>.
    /// </summary>
    internal RocksDbException? CheckKey(in ConflictCheckView view, ReadOnlySpan<byte> key, ulong snapSeq, ReadOnlySpan<byte> readTimestamp,
        bool enableUdtValidation, bool cacheOnly)
    {
        bool needToReadSst = false;
        ulong earliest = view.EarliestSequence;
        if (earliest == DbFormat.MaxSequenceNumber || snapSeq < earliest)
        {
            // The memtables do not contain a long enough history to check for writes after snapSeq.
            needToReadSst = true;
            if (cacheOnly) return TryAgainNoHistory(snapSeq, earliest);
        }
        int tsSize = view.Cfd.TimestampSize;
        Span<byte> ts = stackalloc byte[tsSize];
        GetLatestSequenceForKey(view, key, !needToReadSst, snapSeq, out ulong seq, out bool found, ts, out bool hasTs);
        if (!found) return null;
        bool conflict = snapSeq < seq;
        if (!conflict && enableUdtValidation && !readTimestamp.IsEmpty && hasTs)
        {
            // Write conflict if read_ts < timestamp.
            conflict = view.Cfd.UserComparator.CompareTimestamp(readTimestamp, ts) < 0;
        }
        return conflict ? new RocksDbException(StatusCode.Busy, "Resource busy: ") : null;
    }

    /// <summary>
    /// TransactionUtil::CheckKey with a read callback (WritePrepared / WriteUnprepared): the newest entry of the key is
    /// a conflict unless the callback sees it; entries below <paramref name="minUncommitted"/> are committed, so the
    /// memtables must reach back to it.
    /// </summary>
    internal RocksDbException? CheckKey(in ConflictCheckView view, ReadOnlySpan<byte> key, ulong snapSeq, ReadCallback snapChecker, ulong minUncommitted,
        bool cacheOnly)
    {
        bool needToReadSst = false;
        ulong earliest = view.EarliestSequence;
        if (earliest == DbFormat.MaxSequenceNumber || snapSeq < earliest || minUncommitted <= earliest)
        {
            // The memtables do not contain a long enough history to check for writes after snapSeq (or, with
            // min_uncommitted, for uncommitted writes since min_uncommitted).
            needToReadSst = true;
            if (cacheOnly) return TryAgainNoHistory(snapSeq, earliest);
        }
        // When min_uncommitted != kMaxSequenceNumber, keys lower than min_uncommitted will not trigger conflicts, while
        // keys larger than min_uncommitted might create conflicts, so we need to read them out from the DB, and call
        // the callback's IsVisibleFullCheck() to determine whether conflicts exist.
        ulong lowerBoundSeq = minUncommitted == DbFormat.MaxSequenceNumber ? snapSeq : minUncommitted;
        GetLatestSequenceForKey(view, key, !needToReadSst, lowerBoundSeq, out ulong seq, out bool found);
        if (!found) return null;
        return !snapChecker.IsVisible(seq) ? new RocksDbException(StatusCode.Busy, "Resource busy: ") : null;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static RocksDbException TryAgainNoHistory(ulong snapSeq, ulong earliest) =>
        new(StatusCode.TryAgain, earliest == DbFormat.MaxSequenceNumber
            ? "Operation failed. Try again.: Transaction could not check for conflicts as the MemTable does not contain a long enough history to check write at SequenceNumber: " + snapSeq
            : "Operation failed. Try again.: Transaction could not check for conflicts for operation at SequenceNumber " + snapSeq +
              " as the MemTable only contains changes newer than SequenceNumber " + earliest +
              ".  Increasing the value of the max_write_buffer_size_to_maintain option could reduce the frequency of this error.");

    /// <summary>
    /// Port of DBImpl::GetLatestSequenceForKey: the sequence number of the newest entry for <paramref name="key"/>
    /// (point entry or covering range tombstone) in the memtables, the history and, unless <paramref name="cacheOnly"/>,
    /// the table files. Sources older than <paramref name="lowerBoundSeq"/> are skipped once a memtable proves there
    /// is no newer entry.
    /// </summary>
    internal void GetLatestSequenceForKey(in ConflictCheckView view, ReadOnlySpan<byte> key, bool cacheOnly, ulong lowerBoundSeq, out ulong seq, out bool found) =>
        GetLatestSequenceForKey(view, key, cacheOnly, lowerBoundSeq, out seq, out found, default, out _);

    /// <summary>
    /// <see cref="GetLatestSequenceForKey(in ConflictCheckView, ReadOnlySpan{byte}, bool, ulong, out ulong, out bool)"/>
    /// that also reports the timestamp of the newest version for column families with user-defined timestamps (the
    /// lookup uses the maximum timestamp; <paramref name="timestamp"/> must be timestamp-size long).
    /// </summary>
    [SkipLocalsInit]
    internal void GetLatestSequenceForKey(in ConflictCheckView view, ReadOnlySpan<byte> key, bool cacheOnly, ulong lowerBoundSeq, out ulong seq, out bool found,
        Span<byte> timestamp, out bool hasTimestamp)
    {
        SuperVersion sv = view.Sv;
        ulong currentSeq = Versions.LastSequence;
        int tsSize = view.Cfd.TimestampSize;
        Span<byte> maxTs = stackalloc byte[tsSize];
        maxTs.Fill(0xff);
        Span<byte> scratch = stackalloc byte[LookupKey.StackBufferSize];
        var lkey = new LookupKey(key, currentSeq, maxTs, scratch);
        GetContext? tsSink = null;
        hasTimestamp = false;
        if (tsSize != 0)
        {
            tsSink = GetContext.Rent(null);
            tsSink.TimestampSize = tsSize;
        }
        try
        {
            seq = DbFormat.MaxSequenceNumber;
            found = false;
            // Check if there is a record for this key in the latest memtable
            if (MemTableSeq(sv.Mem, lkey, out seq, tsSink)) { found = true; return; }
            ulong lowerInMem = sv.Mem.EarliestSequenceNumber;
            if (lowerInMem != DbFormat.MaxSequenceNumber && lowerInMem < lowerBoundSeq) return;
            // Check if there is a record for this key in the immutable memtables
            MemTable[] imm = sv.Imm;
            for (int i = 0; i < imm.Length; i++)
            {
                if (MemTableSeq(imm[i], lkey, out seq, tsSink)) { found = true; return; }
            }
            if (imm.Length > 0)
            {
                ulong lowerInImm = imm[^1].EarliestSequenceNumber;
                if (lowerInImm != DbFormat.MaxSequenceNumber && lowerInImm < lowerBoundSeq) return;
            }
            if (view.History is { } history)
            {
                for (int i = 0; i < view.HistoryCount; i++)
                {
                    if (MemTableSeq(history[i], lkey, out seq, tsSink)) { found = true; return; }
                }
            }
            if (cacheOnly) return;
            // Check tables
            found = TableSeq(view.Cfd, sv.Current, lkey.InternalKey, lkey.UserKey, currentSeq, out seq, tsSink);
        }
        finally
        {
            lkey.Dispose();
            if (tsSink is not null)
            {
                if (tsSink.HasTimestamp && timestamp.Length == tsSize)
                {
                    tsSink.FoundTimestamp.CopyTo(timestamp);
                    hasTimestamp = true;
                }
                GetContext.Return(tsSink);
            }
        }
    }

    private static bool MemTableSeq(MemTable mem, scoped in LookupKey lkey, out ulong seq, GetContext? tsSink)
    {
        ulong cov = 0;
        mem.Get(lkey, out _, out _, out seq, mergeContext: null, ref cov, tsSink);
        if (seq != DbFormat.MaxSequenceNumber) return true;
        if (cov > 0)
        {
            seq = cov;
            return true;
        }
        return false;
    }

    private struct SeqVisitorState
    {
        public ulong Seq;
        public GetContext? TimestampSink;
    }

    private static readonly TableEntryVisitor<SeqVisitorState> s_recordSeq = static (ref SeqVisitorState s, ReadOnlySpan<byte> ikey, ReadOnlySpan<byte> value) =>
    {
        DbFormat.UnpackSequenceAndType(DbFormat.ExtractFooter(ikey), out ulong seq, out _);
        s.Seq = seq;
        s.TimestampSink?.RecordTimestampOfUserKey(DbFormat.ExtractUserKey(ikey), seq);
        return false;
    };

    /// <summary>
    /// Version::Get with a sequence output: newest entry (or covering tombstone) of the key in the table files.
    /// <paramref name="userKey"/> carries the lookup timestamp when the column family has timestamps.
    /// </summary>
    private static bool TableSeq(ColumnFamilyData cfd, DbVersion v, ReadOnlySpan<byte> ikey, ReadOnlySpan<byte> userKey, ulong snapshot, out ulong seq,
        GetContext? tsSink)
    {
        VersionStorageInfo s = v.Storage;
        Comparator ucmp = cfd.UserComparator;
        int tsSize = cfd.TimestampSize;
        ulong cov = 0;
        for (int level = 0; level < s.NumLevels; level++)
        {
            ReadOnlySpan<FileMetaData> files = s.LevelFiles(level);
            if (files.Length == 0) continue;
            int start = 0, end = files.Length;
            if (level > 0)
            {
                start = s.FindFile(level, ikey);
                if (start >= files.Length) continue;
                end = start + 1;
            }
            for (int i = start; i < end; i++)
            {
                FileMetaData f = files[i];
                if (CompareFileBoundary(ucmp, tsSize, userKey, f.Smallest) < 0 || CompareFileBoundary(ucmp, tsSize, userKey, f.Largest) > 0) continue;
                var state = new SeqVisitorState { Seq = DbFormat.MaxSequenceNumber, TimestampSink = tsSink };
                TableReaderLease lease = cfd.TableCache.GetReader(f, out FragmentedRangeTombstoneList? rangeDels);
                try
                {
                    if (rangeDels is not null)
                    {
                        ulong c = rangeDels.MaxCoveringTombstoneSeqnum(userKey, snapshot, tsSize == 0 ? default : userKey[^tsSize..],
                            out ReadOnlySpan<byte> tombstoneTs);
                        if (c > cov)
                        {
                            cov = c;
                            tsSink?.SetTimestampFromRangeTombstone(tombstoneTs);
                        }
                    }
                    lease.Reader.Get(ikey, ref state, s_recordSeq, TableReadFlags.None);
                }
                finally
                {
                    lease.Dispose();
                }
                if (state.Seq != DbFormat.MaxSequenceNumber)
                {
                    seq = Math.Max(state.Seq, cov);
                    return true;
                }
            }
        }
        seq = cov > 0 ? cov : DbFormat.MaxSequenceNumber;
        Debug.Assert(cov == 0 || seq == cov);
        return cov > 0;
    }
}
