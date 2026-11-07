using System.Threading.Tasks.Sources;

namespace RocksDb.Managed.Util;

/// <summary>
/// Allocation-free broadcast wake-up for async waiters of a monotonic condition (a published sequence number, a commit
/// index): a replacement for "swap in a new <see cref="TaskCompletionSource"/> and complete the old one" that pools its
/// waiter nodes and handles cancellation without <c>Task.WaitAsync</c>. Also compiled into RocksDb.Managed.Replication
/// (linked source file), hence internal.
/// <code>
/// // waiter
/// while (!Condition())
/// {
///     AsyncBroadcastSignal.Waiter w = signal.Register();   // full fence after registering
///     if (Condition()) { signal.Unregister(w); break; }     // re-check after registering (Dekker with SignalAll)
///     await signal.WaitAsync(w, cancellationToken);         // each registered waiter is awaited (or unregistered) once
/// }
/// // producer: make Condition() true, then
/// signal.SignalAll();                                       // full fence, then wakes every registered waiter
/// </code>
/// <para>
/// Synchronization: <see cref="Register"/> links the node under the lock and then issues a full fence before the caller
/// re-reads its condition; <see cref="SignalAll"/> issues a full fence after the producer published the condition and
/// before it reads the list head. So either the waiter sees the new state, or the producer sees the waiter. Continuations
/// run asynchronously (never inline on the producer's thread, e.g. a write leader). Wake-ups may be spurious (a waiter
/// that unregistered can lose a race with a signal); waiters always re-check their condition. A cancelled wait throws
/// <see cref="OperationCanceledException"/> carrying the token.
/// </para>
/// </summary>
internal sealed class AsyncBroadcastSignal
{
    private const int MaxPooled = 64;

    private readonly Lock _lock = new();
    // Registered waiters (doubly linked, guarded by _lock; read without it only for the empty check in SignalAll).
    private Waiter? _head;
    // Free nodes (singly linked through Next, guarded by _lock).
    private Waiter? _pool;
    private int _pooled;

    /// <summary>True while at least one waiter is registered (a racy hint).</summary>
    public bool HasWaiters => Volatile.Read(ref _head) is not null;

    /// <summary>Registers a waiter; the caller must re-check its condition and then either await or unregister it.</summary>
    public Waiter Register()
    {
        Waiter? w;
        lock (_lock)
        {
            w = _pool;
            if (w is not null)
            {
                _pool = w.Next;
                _pooled--;
            }
            else
            {
                w = new Waiter(this);
            }
            w.Arm();
            w.Prev = null;
            w.Next = _head;
            if (_head is not null) _head.Prev = w;
            w.Linked = true;
            Volatile.Write(ref _head, w);
        }
        // StoreLoad: the caller re-reads its condition after this (pairs with the fence in SignalAll).
        Interlocked.MemoryBarrier();
        return w;
    }

    /// <summary>Withdraws a registered waiter that will not be awaited (its condition became true meanwhile).</summary>
    public void Unregister(Waiter waiter)
    {
        // A concurrent SignalAll that already completed the node owns it until its SetResult: leave it to the GC.
        if (!waiter.TryComplete()) return;
        lock (_lock)
        {
            UnlinkLocked(waiter);
            ReturnLocked(waiter);
        }
    }

    /// <summary>Awaits a registered waiter until the next <see cref="SignalAll"/> (or cancellation).</summary>
    public ValueTask WaitAsync(Waiter waiter, CancellationToken cancellationToken)
    {
        short version = waiter.Version;
        if (cancellationToken.CanBeCanceled)
        {
            // Runs the callback inline if already cancelled; the returned ValueTask then completes with the exception.
            waiter.Registration = cancellationToken.UnsafeRegister(static (s, token) => ((Waiter)s!).Cancel(token), waiter);
        }
        return new ValueTask(waiter, version);
    }

    /// <summary>Wakes every registered waiter. Cheap (a fence and a read) when nobody waits.</summary>
    public void SignalAll()
    {
        // StoreLoad: the producer's published state before the head read (pairs with the fence in Register).
        Interlocked.MemoryBarrier();
        if (Volatile.Read(ref _head) is null) return;
        Waiter? won = null;
        lock (_lock)
        {
            for (Waiter? w = _head; w is not null; w = w.Next)
            {
                w.Linked = false;
                if (w.TryComplete())
                {
                    w.NextSignalled = won;
                    won = w;
                }
            }
            Volatile.Write(ref _head, null);
        }
        // The nodes won above are ours until SetResult (their awaiter returns them to the pool in GetResult).
        while (won is not null)
        {
            Waiter next = won.NextSignalled!;
            won.NextSignalled = null;
            won.SetResult();
            won = next;
        }
    }

    private void UnlinkLocked(Waiter w)
    {
        if (!w.Linked) return;
        w.Linked = false;
        if (w.Prev is not null) w.Prev.Next = w.Next;
        else Volatile.Write(ref _head, w.Next);
        if (w.Next is not null) w.Next.Prev = w.Prev;
        w.Prev = null;
        w.Next = null;
    }

    private void ReturnLocked(Waiter w)
    {
        if (_pooled >= MaxPooled) return;
        w.Prev = null;
        w.Next = _pool;
        _pool = w;
        _pooled++;
    }

    private void Return(Waiter w)
    {
        lock (_lock) ReturnLocked(w);
    }

    /// <summary>A pooled waiter node; valid from <see cref="Register"/> until its wait completed or it was unregistered.</summary>
    internal sealed class Waiter : IValueTaskSource
    {
        private const int Idle = 0;
        private const int Waiting = 1;

        private readonly AsyncBroadcastSignal _owner;
        private ManualResetValueTaskSourceCore<bool> _core;
        private int _state;
        internal Waiter? Prev;
        internal Waiter? Next;
        internal Waiter? NextSignalled;
        internal bool Linked;
        internal CancellationTokenRegistration Registration;

        internal Waiter(AsyncBroadcastSignal owner)
        {
            _owner = owner;
            _core.RunContinuationsAsynchronously = true;
        }

        internal short Version => _core.Version;

        internal void Arm()
        {
            _core.Reset();
            Volatile.Write(ref _state, Waiting);
        }

        /// <summary>Claims the right to complete the node (exactly one of signal / cancel / unregister wins).</summary>
        internal bool TryComplete() => Interlocked.CompareExchange(ref _state, Idle, Waiting) == Waiting;

        internal void SetResult() => _core.SetResult(true);

        internal void Cancel(CancellationToken token)
        {
            if (!TryComplete()) return;
            lock (_owner._lock) _owner.UnlinkLocked(this);
            _core.SetException(new OperationCanceledException(token));
        }

        public ValueTaskSourceStatus GetStatus(short token) => _core.GetStatus(token);

        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
            _core.OnCompleted(continuation, state, token, flags);

        public void GetResult(short token)
        {
            // A stale or repeated await: let the core throw without recycling the node twice.
            if (token != _core.Version)
            {
                _core.GetResult(token);
                return;
            }
            try
            {
                _core.GetResult(token);
            }
            finally
            {
                // Waits for a cancellation callback that is still running (it never waits for us: continuations are async).
                Registration.Dispose();
                Registration = default;
                _owner.Return(this);
            }
        }
    }
}
