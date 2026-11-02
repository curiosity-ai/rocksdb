namespace RocksDb.Managed.Cluster;

/// <summary>
/// Randomized election timer of Raft followers and candidates (rocksdb-sharp <c>ElectionTimer</c>): reset by leader
/// heartbeats and granted votes; when it elapses, the callback fires (typically starting an election). Uses a
/// <see cref="TimeProvider"/> timer so tests can control time.
/// </summary>
public sealed class ElectionTimer : IDisposable
{
    private readonly Action _onTimeout;
    private readonly int _minMs;
    private readonly int _maxMs;
    private readonly TimeProvider _timeProvider;
    private readonly Lock _gate = new();

    private ITimer? _timer;
    private bool _disposed;
    private long _lastResetTicks;

    public ElectionTimer(int minMs, int maxMs, Action onTimeout) : this(minMs, maxMs, onTimeout, TimeProvider.System)
    {
    }

    public ElectionTimer(int minMs, int maxMs, Action onTimeout, TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(onTimeout);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(minMs);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxMs, minMs);
        _minMs = minMs;
        _maxMs = maxMs;
        _onTimeout = onTimeout;
        _timeProvider = timeProvider;
    }

    public DateTime LastResetUtc => new(Interlocked.Read(ref _lastResetTicks), DateTimeKind.Utc);

    public void Start() => Reset();

    /// <summary>Re-arms the timer with a fresh random timeout in [min, max] ms.</summary>
    public void Reset()
    {
        lock (_gate)
        {
            if (_disposed) return;
            TimeSpan next = TimeSpan.FromMilliseconds(Random.Shared.Next(_minMs, _maxMs + 1));
            Interlocked.Exchange(ref _lastResetTicks, _timeProvider.GetUtcNow().UtcTicks);
            if (_timer == null)
            {
                _timer = _timeProvider.CreateTimer(static s => ((ElectionTimer)s!).SafeTimeout(), this, next, Timeout.InfiniteTimeSpan);
            }
            else
            {
                _timer.Change(next, Timeout.InfiniteTimeSpan);
            }
        }
    }

    /// <summary>Stops the timer until the next <see cref="Reset"/>.</summary>
    public void Pause()
    {
        lock (_gate) _timer?.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
    }

    private void SafeTimeout()
    {
        try
        {
            _onTimeout();
        }
        catch (Exception)
        {
            // A failed election attempt must not take the timer thread down; the next timeout retries.
        }
    }

    public void Dispose()
    {
        lock (_gate)
        {
            _disposed = true;
            _timer?.Dispose();
            _timer = null;
        }
    }
}
