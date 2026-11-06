using RocksDb.Managed.Db;

namespace RocksDb.Managed.Utilities.Transactions;

/// <summary>
/// Recycles the write batch (with its index) and the lock tracker of disposed transactions of one transaction database,
/// so that <c>BeginTransaction</c> without an old transaction does not allocate them again. Only those internals are
/// recycled, never the <see cref="Transaction"/> object itself: a disposed transaction keeps throwing
/// <see cref="ObjectDisposedException"/> and no longer references them. Not a port of RocksDB code (upstream
/// allocates a transaction per BeginTransaction unless old_txn is passed).
/// </summary>
internal sealed class TransactionResourcePool
{
    // Batches that grew beyond this (or trackers of many keys) are not kept, so the pool does not pin large buffers.
    private const int MaxPooledBatchBytes = 256 << 10;
    private const long MaxPooledTrackedKeys = 4096;

    private readonly IndexedWriteBatch?[] _batches;
    private readonly LockTracker?[] _trackers;
    private readonly TransactionWriteContext?[] _writeContexts;
    private readonly bool _withNoop;
    private readonly Func<uint, Comparator> _comparatorOf;

    public TransactionResourcePool(DbImpl db)
    {
        int size = Math.Min(32, Environment.ProcessorCount * 2);
        _batches = new IndexedWriteBatch?[size];
        _trackers = new LockTracker?[size];
        _writeContexts = new TransactionWriteContext?[size];
        // TransactionBaseImpl::InitWriteBatch: with allow_2pc the batch starts with a Noop that Prepare turns into the
        // begin-prepare marker.
        _withNoop = db.Options.Allow2Pc;
        _comparatorOf = db.UserComparatorOf;
    }

    /// <summary>An empty indexed batch for a new transaction (a recycled one when <paramref name="maxBytes"/> is 0).</summary>
    public IndexedWriteBatch RentBatch(int maxBytes)
    {
        if (maxBytes == 0)
        {
            IndexedWriteBatch?[] pool = _batches;
            for (int i = 0; i < pool.Length; i++)
            {
                IndexedWriteBatch? b = pool[i];
                if (b is not null && Interlocked.CompareExchange(ref pool[i], null, b) == b) return b;
            }
        }
        return new IndexedWriteBatch(_withNoop, maxBytes, _comparatorOf);
    }

    /// <summary>An empty lock tracker for a new transaction.</summary>
    public LockTracker RentTracker()
    {
        LockTracker?[] pool = _trackers;
        for (int i = 0; i < pool.Length; i++)
        {
            LockTracker? t = pool[i];
            if (t is not null && Interlocked.CompareExchange(ref pool[i], null, t) == t) return t;
        }
        return new LockTracker();
    }

    /// <summary>A write context for a new transaction (reset before each write, like a new one).</summary>
    public TransactionWriteContext RentWriteContext()
    {
        TransactionWriteContext?[] pool = _writeContexts;
        for (int i = 0; i < pool.Length; i++)
        {
            TransactionWriteContext? c = pool[i];
            if (c is not null && Interlocked.CompareExchange(ref pool[i], null, c) == c) return c;
        }
        return new TransactionWriteContext();
    }

    /// <summary>Takes back the internals of a disposed transaction (which no longer uses them).</summary>
    public void Return(IndexedWriteBatch batch, LockTracker tracker, TransactionWriteContext writeContext)
    {
        if (!TryReturn(batch)) batch.Dispose();
        writeContext.Reset();
        TransactionWriteContext?[] contexts = _writeContexts;
        for (int i = 0; i < contexts.Length; i++)
        {
            if (contexts[i] is null && Interlocked.CompareExchange(ref contexts[i], writeContext, null) is null) break;
        }
        if (tracker.KeyCapacity <= MaxPooledTrackedKeys)
        {
            tracker.Clear();
            LockTracker?[] pool = _trackers;
            for (int i = 0; i < pool.Length; i++)
            {
                if (pool[i] is null && Interlocked.CompareExchange(ref pool[i], tracker, null) is null) return;
            }
        }
    }

    private bool TryReturn(IndexedWriteBatch batch)
    {
        if (batch.MaxBytes != 0 || batch.Capacity > MaxPooledBatchBytes) return false;
        batch.ResetForReuse();
        IndexedWriteBatch?[] pool = _batches;
        for (int i = 0; i < pool.Length; i++)
        {
            if (pool[i] is null && Interlocked.CompareExchange(ref pool[i], batch, null) is null) return true;
        }
        return false;
    }
}
