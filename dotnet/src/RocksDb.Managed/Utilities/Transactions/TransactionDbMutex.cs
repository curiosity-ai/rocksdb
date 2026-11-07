namespace RocksDb.Managed.Utilities.Transactions;

/// <summary>
/// A mutex guarding one stripe of the point lock table of a <see cref="TransactionDatabase"/> configured with
/// <see cref="TransactionDbOptions.CustomMutexFactory"/>. Port of rocksdb::TransactionDBMutex
/// (include/rocksdb/utilities/transaction_db_mutex.h). A mutex is locked and unlocked by the same thread.
/// </summary>
public abstract class TransactionDbMutex
{
    /// <summary>Acquires the mutex, waiting as long as needed (TransactionDBMutex::Lock). Failures throw.</summary>
    public abstract void Lock();

    /// <summary>
    /// Tries to acquire the mutex within <paramref name="timeoutMicros"/> microseconds (0 = do not wait, negative = no
    /// limit). Returns false on timeout (upstream: Status::TimedOut(kMutexTimeout)); an implementation may ignore the
    /// timeout. Other failures throw a <see cref="RocksDbException"/>, which the lock request then reports.
    /// </summary>
    public abstract bool TryLockFor(long timeoutMicros);

    /// <summary>Releases the mutex (TransactionDBMutex::UnLock).</summary>
    public abstract void Unlock();
}

/// <summary>
/// A condition variable used with a <see cref="TransactionDbMutex"/> of the same factory. Port of
/// rocksdb::TransactionDBCondVar (include/rocksdb/utilities/transaction_db_mutex.h).
/// </summary>
public abstract class TransactionDbCondVar
{
    /// <summary>
    /// Atomically releases <paramref name="mutex"/> (held by the caller), blocks until notified (or a spurious wakeup),
    /// and re-acquires it before returning. Failures throw a <see cref="RocksDbException"/>, which ends the lock wait
    /// with that error.
    /// </summary>
    public abstract void Wait(TransactionDbMutex mutex);

    /// <summary>
    /// Like <see cref="Wait"/>, giving up after <paramref name="timeoutMicros"/> microseconds (negative = no limit).
    /// Returns false on timeout (upstream: Status::TimedOut), true when notified or woken spuriously; the mutex is held
    /// again in both cases.
    /// </summary>
    public abstract bool WaitFor(TransactionDbMutex mutex, long timeoutMicros);

    /// <summary>Wakes at least one waiter, if any.</summary>
    public abstract void Notify();

    /// <summary>Wakes every waiter.</summary>
    public abstract void NotifyAll();
}

/// <summary>
/// Creates the mutexes and condition variables of the point lock manager (one pair per lock-table stripe, allocated
/// when a column family's lock table is created). Port of rocksdb::TransactionDBMutexFactory
/// (include/rocksdb/utilities/transaction_db_mutex.h); set it as <see cref="TransactionDbOptions.CustomMutexFactory"/>,
/// e.g. to instrument lock contention. <see cref="Default"/> is a portable implementation to derive from or wrap.
/// Implementations must be thread-safe.
/// </summary>
public abstract class TransactionDbMutexFactory
{
    /// <summary>A new mutex (AllocateMutex).</summary>
    public abstract TransactionDbMutex AllocateMutex();

    /// <summary>A new condition variable (AllocateCondVar).</summary>
    public abstract TransactionDbCondVar AllocateCondVar();

    /// <summary>
    /// The managed counterpart of upstream's TransactionDBMutexFactoryImpl
    /// (utilities/transactions/transaction_db_mutex_impl.cc): a <see cref="System.Threading.Lock"/> per mutex and a
    /// monitor-based condition variable that works with any <see cref="TransactionDbMutex"/>.
    /// </summary>
    public static TransactionDbMutexFactory Default { get; } = new DefaultFactory();

    private sealed class DefaultFactory : TransactionDbMutexFactory
    {
        public override TransactionDbMutex AllocateMutex() => new DefaultMutex();

        public override TransactionDbCondVar AllocateCondVar() => new DefaultCondVar();
    }

    private sealed class DefaultMutex : TransactionDbMutex
    {
        private readonly Lock _lock = new();

        public override void Lock() => _lock.Enter();

        public override bool TryLockFor(long timeoutMicros)
        {
            if (timeoutMicros == 0) return _lock.TryEnter();
            if (timeoutMicros < 0)
            {
                _lock.Enter();
                return true;
            }
            return _lock.TryEnter(TimeSpan.FromTicks(Math.Max(1, timeoutMicros * 10)));
        }

        public override void Unlock() => _lock.Exit();
    }

    /// <summary>
    /// A generation-counting condition variable: a waiter records the generation before releasing the mutex and
    /// sleeps until a notification bumps it, so a notification between the release and the sleep is not lost.
    /// </summary>
    private sealed class DefaultCondVar : TransactionDbCondVar
    {
        private readonly object _sync = new();
        private long _generation;

        public override void Wait(TransactionDbMutex mutex) => WaitFor(mutex, -1);

        public override bool WaitFor(TransactionDbMutex mutex, long timeoutMicros)
        {
            ArgumentNullException.ThrowIfNull(mutex);
            long gen;
            lock (_sync) gen = _generation;
            mutex.Unlock();
            bool notified = true;
            try
            {
                lock (_sync)
                {
                    if (timeoutMicros < 0)
                    {
                        if (_generation == gen) Monitor.Wait(_sync);
                    }
                    else
                    {
                        // Like std::condition_variable::wait_for, never report a timeout before the time has passed
                        // (Monitor.Wait has millisecond granularity and may return slightly early).
                        long deadline = System.Diagnostics.Stopwatch.GetTimestamp() + timeoutMicros * System.Diagnostics.Stopwatch.Frequency / 1_000_000;
                        while (_generation == gen)
                        {
                            long remaining = deadline - System.Diagnostics.Stopwatch.GetTimestamp();
                            if (remaining <= 0)
                            {
                                notified = false;
                                break;
                            }
                            int ms = (int)Math.Min(int.MaxValue, (remaining * 1000 + System.Diagnostics.Stopwatch.Frequency - 1) / System.Diagnostics.Stopwatch.Frequency);
                            Monitor.Wait(_sync, ms);
                        }
                    }
                }
            }
            finally
            {
                mutex.Lock();
            }
            return notified;
        }

        public override void Notify()
        {
            lock (_sync)
            {
                _generation++;
                Monitor.Pulse(_sync);
            }
        }

        public override void NotifyAll()
        {
            lock (_sync)
            {
                _generation++;
                Monitor.PulseAll(_sync);
            }
        }
    }
}
