namespace RocksDb.Managed.Replication.Switchover;

/// <summary>
/// Admission control for client writes on a primary (port of rocksdb-sharp <c>PrimaryWriteGate</c>), with the one
/// property a planned switchover needs: it can stop admitting writes and then tell when every write it already admitted
/// has reached the database, so the sequence number read after <see cref="HoldAsync"/> is final.
/// <list type="bullet">
/// <item><b>Open</b>: writes are admitted.</item>
/// <item><b>Holding</b>: new writes wait (up to the caller's timeout) instead of failing, because the gate is about to
/// either reopen (an aborted switchover) or close.</item>
/// <item><b>Closed</b>: writes are refused; the caller should redirect them to the primary.</item>
/// </list>
/// The hot path is one interlocked increment and one read. Admission and holding form a Dekker pair: a writer
/// increments the in-flight count before it reads the state, and the switchover sets the state before it reads the
/// count, both through full fences - so either the writer sees the hold, or the drain sees the writer.
/// <para>Managed difference: <see cref="HoldAsync"/> waits for the drain on a signal raised by the last
/// <see cref="Exit"/> instead of spinning and polling with <c>Task.Delay(1)</c>; <see cref="TryEnterAsync"/> completes
/// synchronously (no allocation) while the gate is open.</para>
/// </summary>
public sealed class PrimaryWriteGate
{
    private const int Closed = 0;
    private const int Open = 1;
    private const int Holding = 2;

    private readonly Lock _transition = new();
    private int _state;
    private int _inFlight;
    private TaskCompletionSource _changed = NewSignal();
    private TaskCompletionSource _drained = NewSignal();

    public PrimaryWriteGate(bool open)
    {
        _state = open ? Open : Closed;
    }

    public bool IsOpen => Volatile.Read(ref _state) == Open;
    public bool IsHolding => Volatile.Read(ref _state) == Holding;
    public bool IsClosed => Volatile.Read(ref _state) == Closed;
    public int InFlight => Volatile.Read(ref _inFlight);

    /// <summary>
    /// Admits one write. True when the write may proceed - the caller must then call <see cref="Exit"/> once it has been
    /// applied. False when the gate is closed, or stayed held for longer than <paramref name="holdTimeout"/>.
    /// </summary>
    public ValueTask<bool> TryEnterAsync(TimeSpan holdTimeout, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _inFlight);
        if (Volatile.Read(ref _state) == Open) return new ValueTask<bool>(true);
        Exit();
        return TryEnterSlowAsync(holdTimeout, cancellationToken);
    }

    private async ValueTask<bool> TryEnterSlowAsync(TimeSpan holdTimeout, CancellationToken cancellationToken)
    {
        long deadline = Environment.TickCount64 + (long)holdTimeout.TotalMilliseconds;
        while (true)
        {
            // Taken before the state is read, so a transition in between still completes the signal waited on.
            Task changed = Volatile.Read(ref _changed).Task;
            Interlocked.Increment(ref _inFlight);
            int state = Volatile.Read(ref _state);
            if (state == Open) return true;
            Exit();
            if (state == Closed) return false;
            long remaining = deadline - Environment.TickCount64;
            if (remaining <= 0) return false;
            try
            {
                await changed.WaitAsync(TimeSpan.FromMilliseconds(remaining), cancellationToken).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                return false;
            }
        }
    }

    /// <summary>Marks a write admitted by <see cref="TryEnterAsync"/> as applied.</summary>
    public void Exit()
    {
        if (Interlocked.Decrement(ref _inFlight) == 0 && Volatile.Read(ref _state) == Holding) Volatile.Read(ref _drained).TrySetResult();
    }

    public void OpenGate() => Transition(Open);

    public void Close() => Transition(Closed);

    /// <summary>
    /// Stops admitting writes - new ones wait - and completes once every write admitted before the hold has called
    /// <see cref="Exit"/>.
    /// </summary>
    public async Task HoldAsync(CancellationToken cancellationToken)
    {
        Transition(Holding);
        while (true)
        {
            TaskCompletionSource drained = Volatile.Read(ref _drained);
            if (drained.Task.IsCompleted)
            {
                // Consumed by a transient enter/exit; arm a fresh one before re-checking the count (Dekker with Exit).
                var fresh = NewSignal();
                Interlocked.CompareExchange(ref _drained, fresh, drained);
                continue;
            }
            if (Interlocked.CompareExchange(ref _inFlight, 0, 0) == 0) return;
            await drained.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private void Transition(int state)
    {
        TaskCompletionSource previous;
        lock (_transition)
        {
            if (state == Holding) Volatile.Write(ref _drained, NewSignal());
            Interlocked.Exchange(ref _state, state);
            previous = Interlocked.Exchange(ref _changed, NewSignal());
        }
        previous.TrySetResult();
    }

    private static TaskCompletionSource NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);
}
