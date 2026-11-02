using System.Diagnostics;

namespace RocksDb.Managed.Env;

/// <summary>Which operations a rate limiter charges (RateLimiter::Mode, include/rocksdb/rate_limiter.h).</summary>
public enum RateLimiterMode
{
    ReadsOnly = 0,
    WritesOnly = 1,
    AllIo = 2,
}

/// <summary>
/// Token-bucket rate limiter shared by a DB's background writes. Port of GenericRateLimiter (util/rate_limiter.cc,
/// util/rate_limiter_impl.h; NewGenericRateLimiter). Set it as <see cref="DbOptions.RateLimiter"/>: flush outputs are
/// charged at <see cref="IoPriority.High"/>, compaction outputs at <see cref="IoPriority.Low"/>; WAL and MANIFEST writes
/// are not limited (IO_TOTAL), like upstream's defaults.
/// <para>
/// Every <c>refillPeriodMicros</c> the bucket is refilled with <c>bytesPerSecond * refillPeriod</c> bytes (unused bytes
/// do not carry over) and handed to the queued requests: <see cref="IoPriority.User"/> first, then high, mid and low,
/// except that with probability 1/<c>fairness</c> high is served after mid and low, and mid after low, so that lower
/// priorities are not starved. Requests larger than what is available are granted partially and keep their place.
/// With <c>autoTuned</c>, the rate adapts within [max/20, max] to how often the bucket is drained (TuneLocked).
/// </para>
/// <para>
/// Synchronization: all state is guarded by <c>_lock</c>. Waiting requests do not hold a thread: the refill that
/// grants them is driven by a one-shot timer while requests are queued (upstream elects a waiting thread for that
/// duty), and <see cref="RequestAsync"/> waits asynchronously.
/// </para>
/// </summary>
public sealed class GenericRateLimiter : IRateLimiter, IDisposable
{
    private const long MicrosPerSecond = 1_000_000;
    private const int RefillsPerTune = 100;

    private sealed class Req
    {
        public long RequestBytes;
        public readonly long Bytes;
        public readonly TaskCompletionSource Granted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public Req(long bytes)
        {
            RequestBytes = bytes;
            Bytes = bytes;
        }
    }

    private readonly Lock _lock = new();
    private readonly long _refillPeriodMicros;
    private readonly int _fairness;
    private readonly bool _autoTuned;
    private readonly long _maxBytesPerSecond;
    private readonly Queue<Req>[] _queues = new Queue<Req>[(int)IoPriority.Total];
    private readonly long[] _totalRequests = new long[(int)IoPriority.Total];
    private readonly long[] _totalBytesThrough = new long[(int)IoPriority.Total];
    private readonly Random _random = new((int)(DateTime.UtcNow.Ticks & int.MaxValue));
    private readonly Timer _timer;
    private long _rateBytesPerSecond;
    private long _refillBytesPerPeriod;
    private long _rawSingleBurstBytes;
    private long _availableBytes;
    private long _nextRefillMicros;
    private bool _timerPending;
    private bool _stopped;
    private long _numDrains;
    private long _tunedTimeMicros;

    /// <param name="bytesPerSecond">Rate (must be &gt; 0). With <paramref name="autoTuned"/> this is the maximum and the
    /// starting rate is half of it.</param>
    /// <param name="refillPeriodMicros">Refill period (default upstream: 100 ms).</param>
    /// <param name="fairness">1/fairness is the chance that lower priorities are served before high (clamped to 100).</param>
    /// <param name="mode">Which operations are charged.</param>
    /// <param name="autoTuned">Adapt the rate to the demand.</param>
    /// <param name="singleBurstBytes">Largest single grant; 0 = the refill amount.</param>
    public GenericRateLimiter(long bytesPerSecond, long refillPeriodMicros, int fairness, RateLimiterMode mode, bool autoTuned, long singleBurstBytes)
    {
        if (bytesPerSecond <= 0) throw new ArgumentOutOfRangeException(nameof(bytesPerSecond));
        if (refillPeriodMicros <= 0) throw new ArgumentOutOfRangeException(nameof(refillPeriodMicros));
        if (fairness <= 0) throw new ArgumentOutOfRangeException(nameof(fairness));
        if (singleBurstBytes < 0) throw new ArgumentOutOfRangeException(nameof(singleBurstBytes));
        _refillPeriodMicros = refillPeriodMicros;
        _fairness = Math.Min(fairness, 100);
        Mode = mode;
        _autoTuned = autoTuned;
        _maxBytesPerSecond = bytesPerSecond;
        _rateBytesPerSecond = autoTuned ? bytesPerSecond / 2 : bytesPerSecond;
        _refillBytesPerPeriod = CalculateRefillBytesPerPeriod(_rateBytesPerSecond);
        _rawSingleBurstBytes = singleBurstBytes;
        for (int i = 0; i < _queues.Length; i++) _queues[i] = new Queue<Req>();
        _nextRefillMicros = NowMicros();
        _tunedTimeMicros = _nextRefillMicros;
        _timer = new Timer(static s => ((GenericRateLimiter)s!).OnTimer(), this, Timeout.Infinite, Timeout.Infinite);
    }

    /// <summary>NewGenericRateLimiter(rate_bytes_per_sec) with upstream's defaults (100 ms, fairness 10, writes only).</summary>
    public GenericRateLimiter(long bytesPerSecond) : this(bytesPerSecond, 100 * 1000, 10, RateLimiterMode.WritesOnly, autoTuned: false, 0)
    {
    }

    public RateLimiterMode Mode { get; }

    /// <summary>Whether writes are charged (IsRateLimited(kWrite)).</summary>
    public bool LimitsWrites => Mode != RateLimiterMode.ReadsOnly;

    /// <summary>Whether reads are charged (IsRateLimited(kRead)). The DB does not rate limit reads yet.</summary>
    public bool LimitsReads => Mode != RateLimiterMode.WritesOnly;

    /// <summary>GetSingleBurstBytes: the configured burst, or the refill amount when 0.</summary>
    public long SingleBurstBytes
    {
        get
        {
            long raw = Volatile.Read(ref _rawSingleBurstBytes);
            return raw == 0 ? Volatile.Read(ref _refillBytesPerPeriod) : raw;
        }
    }

    /// <summary>GetBytesPerSecond.</summary>
    public long BytesPerSecond => Volatile.Read(ref _rateBytesPerSecond);

    /// <summary>SetBytesPerSecond: changes the rate for the following refills.</summary>
    public void SetBytesPerSecond(long bytesPerSecond)
    {
        if (bytesPerSecond <= 0) throw new ArgumentOutOfRangeException(nameof(bytesPerSecond));
        lock (_lock) SetBytesPerSecondLocked(bytesPerSecond);
    }

    /// <summary>SetSingleBurstBytes (0 = refill amount).</summary>
    public void SetSingleBurstBytes(long singleBurstBytes)
    {
        if (singleBurstBytes < 0) ThrowHelper.InvalidArgument("`single_burst_bytes` must be greater than or equal to 0");
        Volatile.Write(ref _rawSingleBurstBytes, singleBurstBytes);
    }

    /// <summary>GetTotalBytesThrough: bytes granted at <paramref name="priority"/> (Total = all priorities).</summary>
    public long GetTotalBytesThrough(IoPriority priority)
    {
        lock (_lock)
        {
            if (priority != IoPriority.Total) return _totalBytesThrough[(int)priority];
            long sum = 0;
            foreach (long b in _totalBytesThrough) sum += b;
            return sum;
        }
    }

    /// <summary>GetTotalRequests: requests made at <paramref name="priority"/> (Total = all priorities).</summary>
    public long GetTotalRequests(IoPriority priority)
    {
        lock (_lock)
        {
            if (priority != IoPriority.Total) return _totalRequests[(int)priority];
            long sum = 0;
            foreach (long r in _totalRequests) sum += r;
            return sum;
        }
    }

    /// <summary>GetTotalPendingRequests at <paramref name="priority"/> (Total = all).</summary>
    public int GetTotalPendingRequests(IoPriority priority)
    {
        lock (_lock)
        {
            if (priority != IoPriority.Total) return _queues[(int)priority].Count;
            int sum = 0;
            foreach (Queue<Req> q in _queues) sum += q.Count;
            return sum;
        }
    }

    /// <inheritdoc/>
    public void Request(long bytes, IoPriority priority)
    {
        Task? wait = RequestCore(bytes, priority);
        wait?.GetAwaiter().GetResult();
    }

    /// <inheritdoc/>
    public ValueTask RequestAsync(long bytes, IoPriority priority, CancellationToken cancellationToken)
    {
        Task? wait = RequestCore(bytes, priority);
        if (wait is null) return ValueTask.CompletedTask;
        // A cancelled waiter keeps its place in the queue (its tokens are still consumed when granted).
        return new ValueTask(cancellationToken.CanBeCanceled ? wait.WaitAsync(cancellationToken) : wait);
    }

    /// <summary>GenericRateLimiter::RequestImpl. Returns null when granted immediately, else the grant to wait for.</summary>
    private Task? RequestCore(long bytes, IoPriority priority)
    {
        if (priority >= IoPriority.Total || !LimitsWrites) return null;
        bytes = Math.Max(0, Math.Min(bytes, SingleBurstBytes));
        lock (_lock)
        {
            if (_autoTuned && NowMicros() - _tunedTimeMicros >= RefillsPerTune * _refillPeriodMicros) TuneLocked();
            if (_stopped) return null;
            _totalRequests[(int)priority]++;
            if (_availableBytes > 0)
            {
                long through = Math.Min(_availableBytes, bytes);
                _totalBytesThrough[(int)priority] += through;
                _availableBytes -= through;
                bytes -= through;
            }
            if (bytes == 0) return null;
            var r = new Req(bytes);
            _queues[(int)priority].Enqueue(r);
            long now = NowMicros();
            if (now >= _nextRefillMicros)
            {
                RefillBytesAndGrantRequestsLocked();
            }
            if (r.RequestBytes > 0) ScheduleRefillLocked();
            return r.Granted.Task;
        }
    }

    private void ScheduleRefillLocked()
    {
        if (_timerPending || _stopped) return;
        _timerPending = true;
        _numDrains++; // NUMBER_RATE_LIMITER_DRAINS: a request had to wait for a refill
        long dueMicros = Math.Max(0, _nextRefillMicros - NowMicros());
        _timer.Change(Math.Max(1, (dueMicros + 999) / 1000), Timeout.Infinite);
    }

    private void OnTimer()
    {
        lock (_lock)
        {
            _timerPending = false;
            if (_stopped) return;
            bool any = false;
            foreach (Queue<Req> q in _queues) any |= q.Count > 0;
            if (!any) return;
            if (NowMicros() >= _nextRefillMicros) RefillBytesAndGrantRequestsLocked();
            foreach (Queue<Req> q in _queues)
            {
                if (q.Count > 0)
                {
                    ScheduleRefillLocked();
                    break;
                }
            }
        }
    }

    /// <summary>GeneratePriorityIterationOrderLocked.</summary>
    private void PriorityIterationOrderLocked(Span<IoPriority> order)
    {
        // IO_USER is always served first.
        order[0] = IoPriority.User;
        bool highAfterMidLow = _random.Next(_fairness) == 0;
        bool midAfterLow = _random.Next(_fairness) == 0;
        if (highAfterMidLow)
        {
            order[3] = IoPriority.High;
            order[2] = midAfterLow ? IoPriority.Mid : IoPriority.Low;
            order[1] = order[2] == IoPriority.Mid ? IoPriority.Low : IoPriority.Mid;
        }
        else
        {
            order[1] = IoPriority.High;
            order[3] = midAfterLow ? IoPriority.Mid : IoPriority.Low;
            order[2] = order[3] == IoPriority.Mid ? IoPriority.Low : IoPriority.Mid;
        }
    }

    /// <summary>RefillBytesAndGrantRequestsLocked.</summary>
    private void RefillBytesAndGrantRequestsLocked()
    {
        _nextRefillMicros = NowMicros() + _refillPeriodMicros;
        // Leftover quota of the last period does not carry over.
        _availableBytes = Volatile.Read(ref _refillBytesPerPeriod);
        Span<IoPriority> order = stackalloc IoPriority[(int)IoPriority.Total];
        PriorityIterationOrderLocked(order);
        foreach (IoPriority pri in order)
        {
            Queue<Req> queue = _queues[(int)pri];
            while (queue.Count > 0)
            {
                Req next = queue.Peek();
                if (_availableBytes < next.RequestBytes)
                {
                    // Partial grant: the request keeps its place for the next refill.
                    next.RequestBytes -= _availableBytes;
                    _availableBytes = 0;
                    break;
                }
                _availableBytes -= next.RequestBytes;
                next.RequestBytes = 0;
                _totalBytesThrough[(int)pri] += next.Bytes;
                queue.Dequeue();
                next.Granted.TrySetResult();
            }
        }
    }

    private long CalculateRefillBytesPerPeriod(long rateBytesPerSecond) =>
        long.MaxValue / rateBytesPerSecond < _refillPeriodMicros
            ? long.MaxValue / MicrosPerSecond // overflow: large enough
            : rateBytesPerSecond * _refillPeriodMicros / MicrosPerSecond;

    private void SetBytesPerSecondLocked(long bytesPerSecond)
    {
        Volatile.Write(ref _rateBytesPerSecond, bytesPerSecond);
        Volatile.Write(ref _refillBytesPerPeriod, CalculateRefillBytesPerPeriod(bytesPerSecond));
    }

    /// <summary>TuneLocked: adjusts the rate to the fraction of refill periods in which the bucket was drained.</summary>
    private void TuneLocked()
    {
        const int lowWatermarkPct = 50;
        const int highWatermarkPct = 90;
        const int adjustFactorPct = 5;
        const int allowedRangeFactor = 20;
        long prevTuned = _tunedTimeMicros;
        _tunedTimeMicros = NowMicros();
        long elapsedIntervals = (_tunedTimeMicros - prevTuned + _refillPeriodMicros - 1) / _refillPeriodMicros;
        if (elapsedIntervals <= 0) return;
        long drainedPct = _numDrains * 100 / elapsedIntervals;
        long prev = _rateBytesPerSecond;
        long next;
        if (drainedPct == 0)
        {
            next = _maxBytesPerSecond / allowedRangeFactor;
        }
        else if (drainedPct < lowWatermarkPct)
        {
            long sanitized = Math.Min(prev, long.MaxValue / 100);
            next = Math.Max(_maxBytesPerSecond / allowedRangeFactor, sanitized * 100 / (100 + adjustFactorPct));
        }
        else if (drainedPct > highWatermarkPct)
        {
            long sanitized = Math.Min(prev, long.MaxValue / (100 + adjustFactorPct));
            next = Math.Min(_maxBytesPerSecond, sanitized * (100 + adjustFactorPct) / 100);
        }
        else
        {
            next = prev;
        }
        if (next != prev && next > 0) SetBytesPerSecondLocked(next);
        _numDrains = 0;
    }

    private static long NowMicros() => Stopwatch.GetTimestamp() / (Stopwatch.Frequency / 1_000_000);

    /// <summary>~GenericRateLimiter: releases all waiting requests (they return without being charged).</summary>
    public void Dispose()
    {
        lock (_lock)
        {
            if (_stopped) return;
            _stopped = true;
            foreach (Queue<Req> q in _queues)
            {
                while (q.Count > 0) q.Dequeue().Granted.TrySetResult();
            }
        }
        _timer.Dispose();
    }
}
