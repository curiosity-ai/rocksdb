using System.Text;
using RocksDb.Managed.Db;

namespace RocksDb.Managed.Utilities.Transactions;

/// <summary>
/// Port of PessimisticTransaction / WriteCommittedTxn (utilities/transactions/pessimistic_transaction.cc): keys are
/// locked when read for update or written; with a snapshot, locking a key also validates that it was not written
/// after the snapshot. Commit writes the batch (or, after <see cref="Transaction.Prepare"/>, a commit marker to the WAL
/// with the prepared batch applied to the memtables).
/// </summary>
internal sealed class PessimisticTransaction : Transaction
{
    private static long s_txnIdCounter;

    private readonly TransactionDatabase _txnDb;
    private int _state; // TransactionState (CAS for expiration / lock stealing)
    private ulong _id;
    private string? _name;
    private byte[]? _nameBytes;
    private ulong _logNumber;
    private long _expirationTime; // µs (PointLockManager.NowMicros); 0 = none
    private long _lockTimeout;    // µs; < 0 = infinite
    private bool _deadlockDetect;
    private int _deadlockDetectDepth;
    private bool _skipConcurrencyControl;
    private bool _skipPrepare;
    private WriteBatch? _markerBatch;
    // Waiting info (GetWaitingTxns)
    private readonly Lock _waitLock = new();
    private ulong[] _waitingTxnIds = [];
    private uint _waitingCf;
    private byte[]? _waitingKey;

    internal readonly List<ulong> WaitIdsScratch = new();

    public PessimisticTransaction(TransactionDatabase txnDb, WriteOptions? writeOptions, TransactionOptions options)
        : base(txnDb.Database, writeOptions, options.MaxWriteBatchSize)
    {
        _txnDb = txnDb;
        Initialize(options);
    }

    private void Initialize(TransactionOptions options)
    {
        _id = (ulong)Interlocked.Increment(ref s_txnIdCounter);
        _state = (int)TransactionState.Started;
        _deadlockDetect = options.DeadlockDetect;
        _deadlockDetectDepth = options.DeadlockDetectDepth;
        _skipConcurrencyControl = options.SkipConcurrencyControl;
        _skipPrepare = options.SkipPrepare;
        _lockTimeout = ToMicros(options.LockTimeout ?? _txnDb.Options.TransactionLockTimeout);
        _expirationTime = options.Expiration is { } exp && exp >= TimeSpan.Zero ? PointLockManager.NowMicros() + Math.Max(1, (long)(exp.Ticks / 10)) : 0;
        _name = null;
        _nameBytes = null;
        _logNumber = 0;
        if (options.SetSnapshot) SetSnapshot();
        if (_expirationTime > 0) _txnDb.InsertExpirableTransaction(_id, this);
    }

    internal static long ToMicros(TimeSpan t) => t < TimeSpan.Zero ? -1 : t.Ticks / 10;

    internal void Reinitialize(WriteOptions? writeOptions, TransactionOptions options)
    {
        ThrowIfDisposed();
        if (_name is not null && State != TransactionState.Committed) _txnDb.UnregisterTransaction(this);
        if (_expirationTime > 0) _txnDb.RemoveExpirableTransaction(_id);
        Clear();
        ReleaseSnapshots();
        _writeOptions = writeOptions ?? new WriteOptions();
        Initialize(options);
    }

    public override TransactionState State => (TransactionState)Volatile.Read(ref _state);
    public override ulong Id => _id;
    public override string? Name => _name;

    internal long ExpirationTime => _expirationTime;
    internal long LockTimeoutMicros => _lockTimeout;
    internal bool DeadlockDetect => _deadlockDetect;
    internal int DeadlockDetectDepth => _deadlockDetectDepth;
    internal ulong LogNumber => _logNumber;

    /// <summary>Lock wait timeout for this transaction (SetLockTimeout); negative waits forever.</summary>
    internal void SetLockTimeout(TimeSpan timeout) => _lockTimeout = ToMicros(timeout);

    private bool IsExpired => _expirationTime > 0 && PointLockManager.NowMicros() >= _expirationTime;

    private bool CasState(TransactionState expected, TransactionState value) =>
        Interlocked.CompareExchange(ref _state, (int)value, (int)expected) == (int)expected;

    private void SetState(TransactionState s) => Volatile.Write(ref _state, (int)s);

    /// <summary>TryStealingLocks: an expired transaction gives up its locks (STARTED -> LOCKS_STOLEN).</summary>
    internal bool TryStealingLocks() => CasState(TransactionState.Started, TransactionState.LocksStolen);

    internal void SetWaitingTxn(List<ulong> ids, uint cf, byte[] key)
    {
        lock (_waitLock)
        {
            _waitingTxnIds = ids.ToArray();
            _waitingCf = cf;
            _waitingKey = key;
        }
    }

    internal void ClearWaitingTxn()
    {
        lock (_waitLock)
        {
            _waitingTxnIds = [];
            _waitingKey = null;
        }
    }

    /// <summary>The transactions this one is waiting for, with the column family and key (GetWaitingTxns).</summary>
    public IReadOnlyList<ulong> GetWaitingTransactions(out uint columnFamilyId, out byte[]? key)
    {
        lock (_waitLock)
        {
            columnFamilyId = _waitingCf;
            key = _waitingKey;
            return _waitingTxnIds;
        }
    }

    // ---- naming -------------------------------------------------------------------------------------------

    public override void SetName(string name)
    {
        ArgumentNullException.ThrowIfNull(name);
        ThrowIfDisposed();
        if (State != TransactionState.Started) throw Invalid("Transaction is beyond state for naming.");
        if (_name is not null) throw Invalid("Transaction has already been named.");
        int len = Encoding.UTF8.GetByteCount(name);
        if (len < 1 || len > 512) throw Invalid("Transaction name length must be between 1 and 512 chars.");
        _name = name;
        _nameBytes = Encoding.UTF8.GetBytes(name);
        if (!_txnDb.RegisterTransaction(this))
        {
            _name = null;
            _nameBytes = null;
            throw Invalid("Duplicate txn name " + name);
        }
    }

    private static RocksDbException Invalid(string msg) => new(StatusCode.InvalidArgument, "Invalid argument: " + msg);

    // ---- locking ------------------------------------------------------------------------------------------

    private protected override bool NeedsUnlockOnUntrack => true;

    private protected override void UnlockGetForUpdate(ColumnFamilyData cfd, ReadOnlySpan<byte> key) => _txnDb.LockManager.UnLock(this, cfd.Id, key);

    /// <summary>
    /// PessimisticTransaction::TryLock: locks the key unless already locked by this transaction (or an upgrade from
    /// shared to exclusive is needed), then validates it against the snapshot and tracks it.
    /// </summary>
    private protected override ValueTask<RocksDbException?> TryLock(ColumnFamilyData cfd, ReadOnlySpan<byte> key, bool readOnly, bool exclusive, bool doValidate,
        bool synchronous, CancellationToken cancellationToken)
    {
        if (_skipConcurrencyControl) return default;
        bool previouslyLocked = _tracked.TryGet(cfd.Id, key, out TrackedKeyInfo status);
        bool lockUpgrade = previouslyLocked && exclusive && !status.Exclusive;
        ulong trackedAtSeq = previouslyLocked ? status.Seq : DbFormat.MaxSequenceNumber;
        // Lock this key if this transactions hasn't already locked it or we require an upgrade.
        if (previouslyLocked && !lockUpgrade)
        {
            return new ValueTask<RocksDbException?>(AfterLock(null, cfd, key, readOnly, exclusive, doValidate, previouslyLocked, lockUpgrade, trackedAtSeq));
        }
        ValueTask<RocksDbException?> t = _txnDb.LockManager.TryLock(this, cfd.Id, key, exclusive, synchronous, cancellationToken);
        if (t.IsCompletedSuccessfully)
        {
            return new ValueTask<RocksDbException?>(AfterLock(t.Result, cfd, key, readOnly, exclusive, doValidate, previouslyLocked, lockUpgrade, trackedAtSeq));
        }
        return AfterLockAsync(t, cfd, key.ToArray(), readOnly, exclusive, doValidate, previouslyLocked, lockUpgrade, trackedAtSeq);
    }

    private async ValueTask<RocksDbException?> AfterLockAsync(ValueTask<RocksDbException?> t, ColumnFamilyData cfd, byte[] key, bool readOnly, bool exclusive,
        bool doValidate, bool previouslyLocked, bool lockUpgrade, ulong trackedAtSeq)
    {
        RocksDbException? s = await t.ConfigureAwait(false);
        return AfterLock(s, cfd, key, readOnly, exclusive, doValidate, previouslyLocked, lockUpgrade, trackedAtSeq);
    }

    private RocksDbException? AfterLock(RocksDbException? s, ColumnFamilyData cfd, ReadOnlySpan<byte> key, bool readOnly, bool exclusive, bool doValidate,
        bool previouslyLocked, bool lockUpgrade, ulong trackedAtSeq)
    {
        SetSnapshotIfNeeded();
        if (!doValidate || Snapshot is null)
        {
            // Need to remember the earliest sequence number that we know that this key has not been modified after.
            // Since we haven't checked a snapshot, we only know this key has not been modified since after we locked it.
            if (trackedAtSeq == DbFormat.MaxSequenceNumber) trackedAtSeq = _db.Versions.LastSequence;
        }
        else if (s is null)
        {
            // If a snapshot is set, we need to make sure the key hasn't been modified since the snapshot. This must be
            // done after we locked the key.
            s = ValidateSnapshot(cfd, key, ref trackedAtSeq);
            if (s is not null)
            {
                // Failed to validate key: unlock key we just locked
                if (lockUpgrade)
                {
                    RocksDbException? d = WaitSync(_txnDb.LockManager.TryLock(this, cfd.Id, key, exclusive: false, synchronous: true, CancellationToken.None));
                    System.Diagnostics.Debug.Assert(d is null);
                }
                else if (!previouslyLocked)
                {
                    _txnDb.LockManager.UnLock(this, cfd.Id, key);
                }
            }
        }
        if (s is null)
        {
            // We must track all the locked keys so that we can unlock them later.
            TrackKey(cfd, key, trackedAtSeq, readOnly, exclusive);
        }
        return s;
    }

    /// <summary>
    /// PessimisticTransaction::ValidateSnapshot: OK if the key has not been modified more recently than the
    /// transaction's snapshot (memtables and, if needed, table files).
    /// </summary>
    private RocksDbException? ValidateSnapshot(ColumnFamilyData cfd, ReadOnlySpan<byte> key, ref ulong trackedAtSeq)
    {
        ulong snapSeq = Snapshot!.SequenceNumber;
        // If the key has been previous validated (or locked) at a sequence number earlier than the current snapshot's
        // sequence number, we already know it has not been modified after snap_seq either.
        if (trackedAtSeq <= snapSeq) return null;
        trackedAtSeq = snapSeq;
        using DbImpl.ConflictCheckView view = _db.AcquireConflictCheckView(cfd);
        return _db.CheckKey(view, key, snapSeq, cacheOnly: false);
    }

    // ---- commit / prepare / rollback --------------------------------------------------------------------

    private protected override async ValueTask PrepareCoreAsync(bool synchronous, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        if (_name is null) throw Invalid("Cannot prepare a transaction that has not been named.");
        if (IsExpired) throw Expired();
        bool canPrepare = false;
        if (_expirationTime > 0)
        {
            // must concern ourselves with expiraton and/or lock stealing
            canPrepare = CasState(TransactionState.Started, TransactionState.AwaitingPrepare);
        }
        else if (State == TransactionState.Started)
        {
            SetState(TransactionState.AwaitingPrepare);
            canPrepare = true;
        }
        if (!canPrepare)
        {
            throw State switch
            {
                TransactionState.LocksStolen => Expired(),
                TransactionState.Prepared => Invalid("Transaction has already been prepared."),
                TransactionState.Committed => Invalid("Transaction has already been committed."),
                TransactionState.RolledBack => Invalid("Transaction has already been rolledback."),
                _ => Invalid("Transaction is not in state for commit."),
            };
        }
        // transaction can't expire after preparation
        if (_expirationTime > 0) _txnDb.RemoveExpirableTransaction(_id);
        _expirationTime = 0;

        // WriteCommittedTxn::PrepareInternal: the batch's leading Noop becomes the begin-prepare marker, an end-prepare
        // marker with the name is appended, and the batch is written to the WAL only; the WAL is marked as holding an
        // outstanding prepared section before the write is released.
        _batch.Batch.MarkEndPrepare(_nameBytes!, writeAfterCommit: true, unpreparedBatch: false);
        WriteOptions wo = _writeOptions.DisableWal ? CloneWithWal(_writeOptions) : _writeOptions;
        _writeContext.Reset();
        _writeContext.DisableMemtable = true;
        _writeContext.MarkLogContainingPrepSection = true;
        try
        {
            ValueTask w = _db.WriteAsync(wo, _batch.Batch, validateColumnFamilies: true, _writeContext, synchronous, cancellationToken);
            if (!w.IsCompletedSuccessfully) await w.ConfigureAwait(false);
            _logNumber = _writeContext.LogUsed;
        }
        finally
        {
            _writeContext.Reset();
        }
        SetState(TransactionState.Prepared);
    }

    private static WriteOptions CloneWithWal(WriteOptions o) => new()
    {
        Sync = o.Sync,
        DisableWal = false,
        IgnoreMissingColumnFamilies = o.IgnoreMissingColumnFamilies,
        NoSlowdown = o.NoSlowdown,
        LowPri = o.LowPri,
        MemtableInsertHintPerBatch = o.MemtableInsertHintPerBatch,
        ProtectionBytesPerKey = o.ProtectionBytesPerKey,
    };

    private static RocksDbException Expired() => new(StatusCode.Expired, "Operation expired: ");

    private protected override async ValueTask CommitCoreAsync(bool synchronous, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        if (IsExpired) throw Expired();
        bool commitWithoutPrepare = false;
        bool commitPrepared = false;
        if (_expirationTime > 0)
        {
            // we must atomicaly compare and exchange the state here because at this state in the transaction it is
            // possible for another thread to change our state out from under us in the even that we expire and have
            // our locks stolen.
            commitWithoutPrepare = CasState(TransactionState.Started, TransactionState.AwaitingCommit);
        }
        else if (State == TransactionState.Prepared)
        {
            commitPrepared = true;
        }
        else if (State == TransactionState.Started)
        {
            if (!_skipPrepare) throw new RocksDbException(StatusCode.InvalidArgument, "Invalid argument: Txn not prepared", StatusSubCode.TxnNotPrepared);
            commitWithoutPrepare = true;
        }

        if (commitWithoutPrepare)
        {
            SetState(TransactionState.AwaitingCommit);
            if (_logNumber > 0) _db.MarkLogAsHavingPrepSectionFlushed(_logNumber);
            try
            {
                ValueTask w = _db.WriteAsync(_writeOptions, _batch.Batch, validateColumnFamilies: true, txn: null, synchronous, cancellationToken);
                if (!w.IsCompletedSuccessfully) await w.ConfigureAwait(false);
            }
            finally
            {
                if (_name is not null) _txnDb.UnregisterTransaction(this);
                if (_expirationTime > 0)
                {
                    _txnDb.RemoveExpirableTransaction(_id);
                    _expirationTime = 0;
                }
                Clear();
            }
            SetState(TransactionState.Committed);
        }
        else if (commitPrepared)
        {
            SetState(TransactionState.AwaitingCommit);
            // WriteCommittedTxn::CommitInternal: the commit marker goes to the WAL; the prepared batch is inserted into
            // the memtables (they keep the WAL with the prepared section alive until flushed).
            WriteBatch marker = MarkerBatch();
            marker.MarkCommit(_nameBytes!);
            _writeContext.Reset();
            _writeContext.MemtableBatch = _batch.Batch;
            _writeContext.LogRef = _logNumber;
            try
            {
                ValueTask w = _db.WriteAsync(_writeOptions, marker, validateColumnFamilies: true, _writeContext, synchronous, cancellationToken);
                if (!w.IsCompletedSuccessfully) await w.ConfigureAwait(false);
            }
            catch
            {
                // Keep the transaction rollbackable after the commit marker write fails.
                SetState(TransactionState.Prepared);
                throw;
            }
            finally
            {
                _writeContext.Reset();
            }
            // FindObsoleteFiles must now look to the memtables to determine what prep logs must be kept around, not the
            // prep section heap.
            _db.MarkLogAsHavingPrepSectionFlushed(_logNumber);
            _txnDb.UnregisterTransaction(this);
            Clear();
            SetState(TransactionState.Committed);
        }
        else
        {
            throw State switch
            {
                TransactionState.LocksStolen => Expired(),
                TransactionState.Committed => Invalid("Transaction has already been committed."),
                TransactionState.RolledBack => Invalid("Transaction has already been rolledback."),
                _ => Invalid("Transaction is not in state for commit."),
            };
        }
    }

    private WriteBatch MarkerBatch()
    {
        WriteBatch b = _markerBatch ??= new WriteBatch();
        b.Clear();
        return b;
    }

    private protected override async ValueTask RollbackCoreAsync(bool synchronous, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        TransactionState state = State;
        if (state == TransactionState.Prepared)
        {
            SetState(TransactionState.AwaitingRollback);
            WriteBatch marker = MarkerBatch();
            marker.MarkRollback(_nameBytes!);
            try
            {
                ValueTask w = _db.WriteAsync(_writeOptions, marker, validateColumnFamilies: false, txn: null, synchronous, cancellationToken);
                if (!w.IsCompletedSuccessfully) await w.ConfigureAwait(false);
            }
            catch
            {
                // Rollback writes can fail under retryable IO errors. Preserve the state so callers can retry.
                SetState(TransactionState.Prepared);
                throw;
            }
            // we do not need to keep our prepared section around
            _db.MarkLogAsHavingPrepSectionFlushed(_logNumber);
            Clear();
            SetState(TransactionState.RolledBack);
        }
        else if (state == TransactionState.Started)
        {
            // prepare couldn't have taken place
            Clear();
        }
        else if (state == TransactionState.Committed)
        {
            throw Invalid("This transaction has already been committed.");
        }
        else
        {
            throw Invalid("Two phase transaction is not in state for rollback.");
        }
    }

    public override bool RollbackToSavePoint()
    {
        if (State != TransactionState.Started) throw Invalid("Transaction is beyond state for rollback.");
        return base.RollbackToSavePoint();
    }

    /// <summary>PessimisticTransaction::Clear: releases the locks, then drops the writes.</summary>
    private protected override void Clear()
    {
        _txnDb.LockManager.UnLock(this, _tracked);
        base.Clear();
    }

    /// <summary>
    /// Locks every key of <paramref name="batch"/> (sorted per column family, so concurrent batch writes cannot deadlock
    /// with each other), writes it and unlocks (CommitBatch / LockBatch).
    /// </summary>
    internal async ValueTask CommitBatchAsync(WriteBatch batch, bool synchronous, CancellationToken cancellationToken)
    {
        var collector = new KeyCollector(new Dictionary<uint, SortedSet<byte[]>>());
        batch.Iterate(ref collector);
        var keysToUnlock = new LockTracker();
        try
        {
            foreach (KeyValuePair<uint, SortedSet<byte[]>> cf in collector.Keys)
            {
                ColumnFamilyData cfd = _txnDb.Database.Impl.GetColumnFamilyDataById(cf.Key);
                foreach (byte[] key in cf.Value)
                {
                    RocksDbException? s = await _txnDb.LockManager.TryLock(this, cf.Key, key, exclusive: true, synchronous, cancellationToken).ConfigureAwait(false);
                    if (s is not null) throw s;
                    keysToUnlock.Track(cfd, key, DbFormat.MaxSequenceNumber, readOnly: false, exclusive: true);
                }
            }
            if (IsExpired) throw Expired();
            if (State != TransactionState.Started) throw State == TransactionState.LocksStolen ? Expired() : Invalid("Transaction is not in state for commit.");
            SetState(TransactionState.AwaitingCommit);
            ValueTask w = _db.WriteAsync(_writeOptions, batch, validateColumnFamilies: true, txn: null, synchronous, cancellationToken);
            if (!w.IsCompletedSuccessfully) await w.ConfigureAwait(false);
            SetState(TransactionState.Committed);
        }
        finally
        {
            _txnDb.LockManager.UnLock(this, keysToUnlock);
        }
    }

    private readonly struct KeyCollector(Dictionary<uint, SortedSet<byte[]>> keys) : IWriteBatchHandler
    {
        private static readonly IComparer<byte[]> s_bytewise = Comparer<byte[]>.Create(static (a, b) => a.AsSpan().SequenceCompareTo(b));

        public Dictionary<uint, SortedSet<byte[]>> Keys => keys;

        private void Record(uint cf, ReadOnlySpan<byte> key)
        {
            if (!keys.TryGetValue(cf, out SortedSet<byte[]>? set)) keys[cf] = set = new SortedSet<byte[]>(s_bytewise);
            set.Add(key.ToArray());
        }

        public bool Continue() => true;
        public void Put(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => Record(columnFamilyId, key);
        public void TimedPut(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ulong writeUnixTime) => Record(columnFamilyId, key);
        public void PutEntity(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> entity) => Record(columnFamilyId, key);
        public void Delete(uint columnFamilyId, ReadOnlySpan<byte> key) => Record(columnFamilyId, key);
        public void SingleDelete(uint columnFamilyId, ReadOnlySpan<byte> key) => Record(columnFamilyId, key);
        public void DeleteRange(uint columnFamilyId, ReadOnlySpan<byte> beginKey, ReadOnlySpan<byte> endKey) { }
        public void Merge(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => Record(columnFamilyId, key);
        public void PutBlobIndex(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> blobIndex) => Record(columnFamilyId, key);
        public void LogData(ReadOnlySpan<byte> blob) { }
        public void MarkBeginPrepare(bool unprepare) { }
        public void MarkEndPrepare(ReadOnlySpan<byte> xid) { }
        public void MarkNoop(bool emptyBatch) { }
        public void MarkCommit(ReadOnlySpan<byte> xid) { }
        public void MarkCommitWithTimestamp(ReadOnlySpan<byte> xid, ReadOnlySpan<byte> commitTimestamp) { }
        public void MarkRollback(ReadOnlySpan<byte> xid) { }
    }

    // ---- recovery -----------------------------------------------------------------------------------------

    /// <summary>
    /// Turns this new transaction into a recovered prepared one (PessimisticTransactionDB::Initialize: SetLogNumber,
    /// SetName, RebuildFromWriteBatch, SetState(PREPARED)).
    /// </summary>
    internal void RebuildPrepared(RecoveredTransaction recovered)
    {
        SetName(recovered.Name);
        _logNumber = recovered.LogNumber;
        var builder = new RebuildHandler(_batch);
        recovered.Batch.Iterate(ref builder);
        SetState(TransactionState.Prepared);
    }

    private readonly struct RebuildHandler(IndexedWriteBatch batch) : IWriteBatchHandler
    {
        public bool Continue() => true;
        public void Put(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => batch.Put(columnFamilyId, key, value);
        public void TimedPut(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ulong writeUnixTime) =>
            ThrowHelper.NotSupported("TimedPut in a recovered transaction is not supported");
        public void PutEntity(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> entity) =>
            ThrowHelper.NotSupported("PutEntity (wide columns) is not supported yet");
        public void Delete(uint columnFamilyId, ReadOnlySpan<byte> key) => batch.Delete(columnFamilyId, key);
        public void SingleDelete(uint columnFamilyId, ReadOnlySpan<byte> key) => batch.SingleDelete(columnFamilyId, key);
        public void DeleteRange(uint columnFamilyId, ReadOnlySpan<byte> beginKey, ReadOnlySpan<byte> endKey) =>
            ThrowHelper.NotSupported("DeleteRange in a transaction is not supported");
        public void Merge(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => batch.Merge(columnFamilyId, key, value);
        public void PutBlobIndex(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> blobIndex) =>
            ThrowHelper.NotSupported("Blob files are not supported yet");
        public void LogData(ReadOnlySpan<byte> blob) => batch.PutLogData(blob);
        public void MarkBeginPrepare(bool unprepare) { }
        public void MarkEndPrepare(ReadOnlySpan<byte> xid) { }
        public void MarkNoop(bool emptyBatch) { }
        public void MarkCommit(ReadOnlySpan<byte> xid) { }
        public void MarkCommitWithTimestamp(ReadOnlySpan<byte> xid, ReadOnlySpan<byte> commitTimestamp) { }
        public void MarkRollback(ReadOnlySpan<byte> xid) { }
    }

    private protected override void DisposeCore()
    {
        _txnDb.LockManager.UnLock(this, _tracked);
        if (_expirationTime > 0) _txnDb.RemoveExpirableTransaction(_id);
        if (_name is not null && State != TransactionState.Committed) _txnDb.UnregisterTransaction(this);
        _markerBatch?.Dispose();
    }
}
