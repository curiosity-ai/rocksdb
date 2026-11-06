using System.Runtime.CompilerServices;
using RocksDb.Managed.Db;

namespace RocksDb.Managed.Utilities.Transactions;

/// <summary>
/// A database with optimistic concurrency control (OptimisticTransactionDB). Port of
/// utilities/transactions/optimistic_transaction_db_impl.cc. Transactions take no locks while running; at commit each
/// key read with GetForUpdate or written is checked: if another writer modified it after the transaction first
/// touched it (or after its snapshot), the commit fails with <see cref="StatusCode.Busy"/>. Conflicts are detected from
/// the memtables only; when they do not hold enough history the commit fails with <see cref="StatusCode.TryAgain"/>
/// (memtable history is enabled automatically: <c>max_write_buffer_size_to_maintain</c> = -1 when it is 0).
/// <para>
/// Plain writes and reads go through <see cref="Database"/>. Dispose closes the database.
/// </para>
/// </summary>
public sealed class OptimisticTransactionDatabase : IDisposable, IAsyncDisposable
{
    private readonly RocksDatabase _db;
    private readonly OptimisticTransactionDbOptions _options;
    private readonly SemaphoreSlim?[]? _buckets;
    private readonly int _bucketCount;

    private OptimisticTransactionDatabase(RocksDatabase db, OptimisticTransactionDbOptions options)
    {
        _db = db;
        _options = options;
        if (options.ValidatePolicy == OccValidationPolicy.ValidateParallel)
        {
            _bucketCount = Math.Max(1, options.OccLockBuckets);
            _buckets = new SemaphoreSlim?[_bucketCount]; // created lazily
        }
    }

    /// <summary>The underlying database (non-transactional reads and writes, column families, flush...).</summary>
    public RocksDatabase Database => _db;

    /// <summary>The validation policy in effect.</summary>
    public OccValidationPolicy ValidatePolicy => _options.ValidatePolicy;

    public static OptimisticTransactionDatabase Open(string path, DbOptions options) =>
        Open(path, options, new OptimisticTransactionDbOptions(), [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions())]);

    public static OptimisticTransactionDatabase Open(string path, DbOptions options, OptimisticTransactionDbOptions occOptions,
        IReadOnlyList<ColumnFamilyDescriptor> columnFamilies)
    {
        ArgumentNullException.ThrowIfNull(occOptions);
        return new OptimisticTransactionDatabase(RocksDatabase.Open(path, options, PrepareColumnFamilies(columnFamilies)), occOptions);
    }

    public static ValueTask<OptimisticTransactionDatabase> OpenAsync(string path, DbOptions options, CancellationToken cancellationToken = default) =>
        OpenAsync(path, options, new OptimisticTransactionDbOptions(),
            [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions())], cancellationToken);

    public static async ValueTask<OptimisticTransactionDatabase> OpenAsync(string path, DbOptions options, OptimisticTransactionDbOptions occOptions,
        IReadOnlyList<ColumnFamilyDescriptor> columnFamilies, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(occOptions);
        RocksDatabase db = await RocksDatabase.OpenAsync(path, options, PrepareColumnFamilies(columnFamilies), cancellationToken).ConfigureAwait(false);
        return new OptimisticTransactionDatabase(db, occOptions);
    }

    /// <summary>Enable MemTable History if not already enabled (OptimisticTransactionDB::Open).</summary>
    internal static List<ColumnFamilyDescriptor> PrepareColumnFamilies(IReadOnlyList<ColumnFamilyDescriptor> columnFamilies)
    {
        var copy = new List<ColumnFamilyDescriptor>(columnFamilies.Count);
        foreach (ColumnFamilyDescriptor d in columnFamilies)
        {
            ColumnFamilyOptions o = d.Options.Clone();
            // Setting to -1 will set the History size to max_write_buffer_number * write_buffer_size.
            if (o.MaxWriteBufferSizeToMaintain == 0) o.MaxWriteBufferSizeToMaintain = -1;
            copy.Add(new ColumnFamilyDescriptor(d.Name, o));
        }
        return copy;
    }

    /// <summary>Starts a transaction (BeginTransaction).</summary>
    public Transaction BeginTransaction(WriteOptions writeOptions) => BeginTransaction(writeOptions, new OptimisticTransactionOptions());

    public Transaction BeginTransaction(WriteOptions writeOptions, OptimisticTransactionOptions transactionOptions)
    {
        ArgumentNullException.ThrowIfNull(transactionOptions);
        return new OptimisticTransaction(this, writeOptions, transactionOptions);
    }

    /// <summary>
    /// Reuses <paramref name="oldTransaction"/> (committed, rolled back or never used) for a new transaction instead of
    /// allocating one (BeginTransaction with old_txn).
    /// </summary>
    public Transaction BeginTransaction(WriteOptions writeOptions, OptimisticTransactionOptions transactionOptions, Transaction oldTransaction)
    {
        if (oldTransaction is not OptimisticTransaction t || t.Owner != this) throw new ArgumentException("not a transaction of this database", nameof(oldTransaction));
        t.Reinitialize(writeOptions, transactionOptions);
        return t;
    }

    /// <summary>Bucket lock for a key (OccLockBuckets / GetLockBucket), created lazily.</summary>
    internal SemaphoreSlim GetLockBucket(int index)
    {
        SemaphoreSlim? s = Volatile.Read(ref _buckets![index]);
        if (s is not null) return s;
        var created = new SemaphoreSlim(1, 1);
        return Interlocked.CompareExchange(ref _buckets[index], created, null) ?? created;
    }

    internal int BucketCount => _bucketCount;

    public void Dispose() => _db.Dispose();

    public ValueTask DisposeAsync() => _db.DisposeAsync();
}

/// <summary>Port of OptimisticTransaction (utilities/transactions/optimistic_transaction.cc).</summary>
internal sealed class OptimisticTransaction : Transaction, IWriteCallback
{
    private readonly OptimisticTransactionDatabase _owner;
    private TransactionState _state = TransactionState.Started;
    private List<int>? _bucketScratch;

    public OptimisticTransaction(OptimisticTransactionDatabase owner, WriteOptions? writeOptions, OptimisticTransactionOptions options)
        : base(owner.Database, writeOptions, 0)
    {
        _owner = owner;
        if (options.SetSnapshot) SetSnapshot();
    }

    internal OptimisticTransactionDatabase Owner => _owner;

    /// <summary>
    /// Like upstream, an optimistic transaction accepts operations on column families with user-defined timestamps (the
    /// write batch gets placeholder timestamps), but it cannot assign a commit timestamp (SetCommitTimestamp is
    /// NotSupported "timestamp not supported", the default of rocksdb::Transaction), so a commit with such writes fails in
    /// DBImpl::WriteImpl with InvalidArgument "write batch must have timestamp(s) set".
    /// </summary>
    private protected override bool SupportsUserDefinedTimestamps => true;

    public override TransactionState State => _state;

    internal void Reinitialize(WriteOptions? writeOptions, OptimisticTransactionOptions options)
    {
        ThrowIfDisposed();
        Clear();
        ReleaseSnapshots();
        _writeOptions = writeOptions ?? new WriteOptions();
        _state = TransactionState.Started;
        // TransactionBaseImpl::Reinitialize
        _startTimestamp = System.Diagnostics.Stopwatch.GetTimestamp();
        EnableIndexing();
        if (options.SetSnapshot) SetSnapshot();
    }

    private protected override ValueTask<RocksDbException?> TryLock(ColumnFamilyData cfd, ReadOnlySpan<byte> key, bool readOnly, bool exclusive, bool doValidate, bool assumeTracked,
        bool synchronous, CancellationToken cancellationToken)
    {
        // OptimisticTransaction::TryLock: assert(!assume_tracked) -- not supported; an assumed-tracked write is not
        // validated (do_validate = !assume_tracked), like a release build upstream.
        if (!doValidate) return default;
        SetSnapshotIfNeeded();
        ulong seq = Snapshot is { } s ? s.SequenceNumber : _db.Versions.LastSequence;
        TrackKey(cfd, key, seq, readOnly, exclusive);
        // Always return OK. Conflict checking will happen at commit time.
        return default;
    }

    private protected override ValueTask CommitCoreAsync(bool synchronous, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        // DBImpl::WriteImpl: WriteBatchInternal::TimestampsUpdateNeeded (placeholder timestamps never replaced).
        if (_batch.Wbwi.HasKeyWithTimestamp) ThrowHelper.InvalidArgument("write batch must have timestamp(s) set");
        // Like upstream, an optimistic transaction can keep being used (and committed) after a commit or rollback.
        return _owner.ValidatePolicy == OccValidationPolicy.ValidateSerial
            ? CommitWithSerialValidateAsync(synchronous, cancellationToken)
            : CommitWithParallelValidateAsync(synchronous, cancellationToken);
    }

    /// <summary>
    /// CommitWithSerialValidate: the conflict check runs as a write callback inside the write group, so no other write
    /// can be applied between the check and this transaction's write.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask CommitWithSerialValidateAsync(bool synchronous, CancellationToken cancellationToken)
    {
        _writeContext.Reset();
        _writeContext.Callback = this;
        try
        {
            ValueTask w = _db.WriteAsync(_writeOptions, _batch.Batch, validateColumnFamilies: true, _writeContext, synchronous, cancellationToken);
            if (!w.IsCompletedSuccessfully) await w.ConfigureAwait(false);
        }
        finally
        {
            _writeContext.Reset();
        }
        Clear();
        _state = TransactionState.Committed;
    }

    /// <summary>
    /// CommitWithParallelValidate: takes the bucket locks of all tracked keys in ascending order (so concurrent
    /// committers cannot deadlock), checks for conflicts against the memtables, then writes.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask CommitWithParallelValidateAsync(bool synchronous, CancellationToken cancellationToken)
    {
        List<int> buckets = _bucketScratch ??= new List<int>();
        buckets.Clear();
        int n = _owner.BucketCount;
        for (int i = 0; i < _tracked.ColumnFamilyCount; i++)
        {
            TrackedColumnFamily c = _tracked.GetColumnFamily(i);
            // To avoid the same key(s) contending across CFs or DBs, seed the hash independently.
            ulong seed = (ulong)_owner.GetHashCode() + 0xb83c07fbc6ced699UL * c.Cfd.Id;
            foreach (PooledKey key in c.Keys.Keys) buckets.Add(ByteKeyComparer.Bucket(key.Span, seed, n));
        }
        buckets.Sort();
        int taken = 0;
        try
        {
            int prev = -1;
            for (int i = 0; i < buckets.Count; i++)
            {
                int b = buckets[i];
                if (b == prev) continue;
                prev = b;
                SemaphoreSlim s = _owner.GetLockBucket(b);
                if (synchronous) s.Wait(cancellationToken);
                else await s.WaitAsync(cancellationToken).ConfigureAwait(false);
                buckets[taken++] = b; // compact the list to the buckets actually held
            }
            RocksDbException? conflict = CheckTransactionForConflicts();
            if (conflict is not null) throw conflict;
            ValueTask w = _db.WriteAsync(_writeOptions, _batch.Batch, validateColumnFamilies: true, txn: null, synchronous, cancellationToken);
            if (!w.IsCompletedSuccessfully) await w.ConfigureAwait(false);
        }
        finally
        {
            for (int i = 0; i < taken; i++) _owner.GetLockBucket(buckets[i]).Release();
        }
        Clear();
        _state = TransactionState.Committed;
    }

    /// <summary>
    /// CheckTransactionForConflicts / TransactionUtil::CheckKeysForConflicts with cache_only: each tracked key must
    /// not have been written after the sequence number it was tracked at.
    /// </summary>
    private RocksDbException? CheckTransactionForConflicts()
    {
        for (int i = 0; i < _tracked.ColumnFamilyCount; i++)
        {
            TrackedColumnFamily c = _tracked.GetColumnFamily(i);
            if (c.Keys.Count == 0) continue;
            if (c.Cfd.Dropped) return new RocksDbException(StatusCode.InvalidArgument, "Invalid argument: Could not access column family " + c.Cfd.Id);
            using DbImpl.ConflictCheckView view = _db.AcquireConflictCheckView(c.Cfd);
            foreach (KeyValuePair<PooledKey, TrackedKeyInfo> kv in c.Keys)
            {
                RocksDbException? e = _db.CheckKey(view, kv.Key.Span, kv.Value.Seq, cacheOnly: true);
                if (e is not null) return e;
            }
        }
        return null;
    }

    RocksDbException? IWriteCallback.Check(DbImpl db) => CheckTransactionForConflicts();

    bool IWriteCallback.AllowWriteBatching => false;

    private protected override ValueTask RollbackCoreAsync(bool synchronous, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        Clear();
        _state = TransactionState.RolledBack;
        return default;
    }
}
