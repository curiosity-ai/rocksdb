namespace RocksDb.Managed.Replication;

/// <summary>One replica's lag (in sequence numbers / versions) reported to an <see cref="AdaptiveCommitDelayController"/>.</summary>
public readonly record struct ReplicaLagSample
{
    public ReplicaLagSample(int replicaIndex, long lagVersions)
    {
        ReplicaIndex = replicaIndex;
        LagVersions = Math.Max(0, lagVersions);
    }

    public int ReplicaIndex { get; }
    public long LagVersions { get; }
}

/// <summary>A consistent-enough view of an <see cref="AdaptiveCommitDelayController"/> for monitoring.</summary>
public sealed record CommitDelaySnapshot(
    long CurrentLag,
    double AverageLag,
    double EmaLag,
    double Trend,
    int RecommendedDelayMs,
    IReadOnlyList<long> History,
    IReadOnlyList<long> ReplicaLags);

/// <summary>Tuning of an <see cref="AdaptiveCommitDelayController"/> (the defaulted constructor parameters of rocksdb-sharp).</summary>
public sealed class AdaptiveCommitDelayOptions
{
    /// <summary>How many cluster-lag samples the moving average and trend cover.</summary>
    public int HistorySize { get; init; } = 20;
    /// <summary>Weight of the newest sample in the exponential moving average, in (0, 1].</summary>
    public double EmaAlpha { get; init; } = 0.25;
    public int MinDelayMs { get; init; }
    public int MaxDelayMs { get; init; } = 2000;
    /// <summary>Milliseconds of delay per <see cref="LagUnit"/> of score.</summary>
    public double DelayPerLagUnitMs { get; init; } = 5.0;
    /// <summary>Lag (in sequence numbers) one <see cref="DelayPerLagUnitMs"/> is charged for.</summary>
    public double LagUnit { get; init; } = 1.0;
    /// <summary>Weight of a growing lag (positive average delta between samples).</summary>
    public double TrendWeight { get; init; } = 2.0;
    /// <summary>Weight of the current lag's excess over the average.</summary>
    public double BurstWeight { get; init; } = 1.5;
    /// <summary>
    /// How long a replica's last report counts. A replica that has not reported for longer (it died, or was removed
    /// without <see cref="AdaptiveCommitDelayController.ForgetReplica"/>) no longer contributes to the cluster lag;
    /// without this its last lag stayed the maximum for good and writers were delayed for a replica that is not there.
    /// When no replica has a current report, no delay is recommended. <see cref="Timeout.InfiniteTimeSpan"/> keeps every
    /// report forever (the behaviour of rocksdb-sharp's controller). Default 30 s.
    /// </summary>
    public TimeSpan ReplicaLagStaleAfter { get; init; } = TimeSpan.FromSeconds(30);
}

/// <summary>
/// Back-pressure for a primary whose replicas fall behind (port of rocksdb-sharp <c>AdaptiveCommitDelayController</c>):
/// replicas report their lag, the controller keeps a lock-free ring of the cluster lag (max over replicas), an EMA and
/// a trend, and recommends how long a writer should pause: <c>(ema + trendWeight * max(0, trend) + burstWeight *
/// max(0, current - average)) * delayPerLagUnitMs / lagUnit</c>, clamped to [min, max]. Thread-safe and lock-free.
/// <para>Managed differences: tuning lives in <see cref="AdaptiveCommitDelayOptions"/> instead of defaulted constructor
/// parameters, <see cref="ReplicaLagSample"/> is a struct, and <see cref="DelayIfNeededAsync"/> returns a
/// <see cref="ValueTask"/> (no allocation when no delay is needed) and can use a <see cref="TimeProvider"/>.</para>
/// </summary>
public sealed class AdaptiveCommitDelayController
{
    private readonly int _historySize;
    private readonly double _emaAlpha;
    private readonly int _minDelayMs;
    private readonly int _maxDelayMs;
    private readonly double _delayPerLagUnitMs;
    private readonly double _lagUnit;
    private readonly double _trendWeight;
    private readonly double _burstWeight;
    private readonly TimeProvider _timeProvider;

    // Per-replica latest lag; one slot per replica.
    private readonly long[] _latestReplicaLag;
    // When each replica last reported (TimeProvider timestamp); Never until it does, and after ForgetReplica.
    private readonly long[] _reportedAt;
    private const long Never = long.MinValue;
    private readonly TimeSpan _staleAfter;
    // Ring buffer of recent cluster lag values.
    private readonly long[] _history;
    // Monotonic write counter. Each report claims one slot.
    private long _writeSequence;
    // Number of valid items currently in ring buffer (<= _historySize).
    private long _historyCount;
    // Rolling sum of values currently inside ring buffer.
    private long _historySum;
    // EMA stored as bits so it can be read/written atomically with Interlocked.
    private long _emaBits;

    public AdaptiveCommitDelayController(int replicaCount) : this(replicaCount, new AdaptiveCommitDelayOptions(), TimeProvider.System)
    {
    }

    public AdaptiveCommitDelayController(int replicaCount, AdaptiveCommitDelayOptions options) : this(replicaCount, options, TimeProvider.System)
    {
    }

    public AdaptiveCommitDelayController(int replicaCount, AdaptiveCommitDelayOptions options, TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(replicaCount, 0);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(options.HistorySize, 0, nameof(options.HistorySize));
        // Written so that NaN fails every check (a comparison with NaN is false), and infinities are refused: either
        // would turn the score into NaN, which no clamp brings back into [MinDelayMs, MaxDelayMs].
        if (!(options.EmaAlpha > 0 && options.EmaAlpha <= 1)) throw new ArgumentOutOfRangeException(nameof(options.EmaAlpha));
        ArgumentOutOfRangeException.ThrowIfNegative(options.MinDelayMs, nameof(options.MinDelayMs));
        ArgumentOutOfRangeException.ThrowIfLessThan(options.MaxDelayMs, options.MinDelayMs, nameof(options.MaxDelayMs));
        if (!(options.DelayPerLagUnitMs >= 0) || !double.IsFinite(options.DelayPerLagUnitMs)) throw new ArgumentOutOfRangeException(nameof(options.DelayPerLagUnitMs));
        if (!(options.LagUnit > 0) || !double.IsFinite(options.LagUnit)) throw new ArgumentOutOfRangeException(nameof(options.LagUnit));
        if (!(options.TrendWeight >= 0) || !double.IsFinite(options.TrendWeight)) throw new ArgumentOutOfRangeException(nameof(options.TrendWeight));
        if (!(options.BurstWeight >= 0) || !double.IsFinite(options.BurstWeight)) throw new ArgumentOutOfRangeException(nameof(options.BurstWeight));

        if (options.ReplicaLagStaleAfter <= TimeSpan.Zero && options.ReplicaLagStaleAfter != Timeout.InfiniteTimeSpan)
            throw new ArgumentOutOfRangeException(nameof(options.ReplicaLagStaleAfter));
        _staleAfter = options.ReplicaLagStaleAfter;
        _latestReplicaLag = new long[replicaCount];
        _reportedAt = new long[replicaCount];
        Array.Fill(_reportedAt, Never);
        _history = new long[options.HistorySize];
        _historySize = options.HistorySize;
        _emaAlpha = options.EmaAlpha;
        _minDelayMs = options.MinDelayMs;
        _maxDelayMs = options.MaxDelayMs;
        _delayPerLagUnitMs = options.DelayPerLagUnitMs;
        _lagUnit = options.LagUnit;
        _trendWeight = options.TrendWeight;
        _burstWeight = options.BurstWeight;
        _timeProvider = timeProvider;
        Interlocked.Exchange(ref _emaBits, BitConverter.DoubleToInt64Bits(0.0));
    }

    public void ReportLag(ReplicaLagSample sample)
    {
        if ((uint)sample.ReplicaIndex >= (uint)_latestReplicaLag.Length) throw new ArgumentOutOfRangeException(nameof(sample));

        // Update this replica's latest lag atomically.
        Interlocked.Exchange(ref _latestReplicaLag[sample.ReplicaIndex], sample.LagVersions);
        long now = _timeProvider.GetTimestamp();
        Interlocked.Exchange(ref _reportedAt[sample.ReplicaIndex], now == Never ? Never + 1 : now);
        // Compute cluster lag as current max across replicas.
        long clusterLag = ReadMaxReplicaLag();
        // Claim next slot in ring buffer.
        long sequence = Interlocked.Increment(ref _writeSequence) - 1;
        int slot = (int)(sequence % _historySize);
        // Replace old value in slot with new cluster lag.
        long oldValue = Interlocked.Exchange(ref _history[slot], clusterLag);
        // Track count until full.
        long priorCount;
        do
        {
            priorCount = Volatile.Read(ref _historyCount);
            if (priorCount >= _historySize) break;
        }
        while (Interlocked.CompareExchange(ref _historyCount, priorCount + 1, priorCount) != priorCount);

        bool overwriting = sequence >= _historySize;
        Interlocked.Add(ref _historySum, overwriting ? clusterLag - oldValue : clusterLag);
        UpdateEma(clusterLag);
    }

    public void ReportLag(int replicaIndex, long lagVersions) => ReportLag(new ReplicaLagSample(replicaIndex, lagVersions));

    /// <summary>The number of replicas this controller tracks (valid indexes are 0 .. ReplicaCount - 1).</summary>
    public int ReplicaCount => _latestReplicaLag.Length;

    /// <summary>
    /// Stops counting replica <paramref name="replicaIndex"/> (it was removed or is being rebuilt) until it reports again.
    /// </summary>
    public void ForgetReplica(int replicaIndex)
    {
        if ((uint)replicaIndex >= (uint)_latestReplicaLag.Length) throw new ArgumentOutOfRangeException(nameof(replicaIndex));
        Interlocked.Exchange(ref _reportedAt[replicaIndex], Never);
        Interlocked.Exchange(ref _latestReplicaLag[replicaIndex], 0);
    }

    private bool IsCurrent(int replicaIndex)
    {
        long at = Volatile.Read(ref _reportedAt[replicaIndex]);
        if (at == Never) return false;
        return _staleAfter == Timeout.InfiniteTimeSpan || _timeProvider.GetElapsedTime(at) <= _staleAfter;
    }

    private bool AnyReplicaCurrent()
    {
        for (int i = 0; i < _reportedAt.Length; i++)
        {
            if (IsCurrent(i)) return true;
        }
        return false;
    }

    public int GetRecommendedDelayMs()
    {
        // Nobody reports any more (every replica is gone or forgotten): the history describes replicas that are not
        // there, and nothing would ever change it.
        if (!AnyReplicaCurrent()) return _minDelayMs;
        long currentLag = ReadCurrentLag();
        long count = Math.Min(Volatile.Read(ref _historyCount), _historySize);
        if (count == 0) return _minDelayMs;
        double averageLag = (double)Volatile.Read(ref _historySum) / count;
        double emaLag = ReadEma();
        double trend = ComputeTrend(count);
        double burst = Math.Max(0, currentLag - averageLag);
        double score = emaLag + (_trendWeight * Math.Max(0, trend)) + (_burstWeight * burst);
        double delay = Math.Round(score * _delayPerLagUnitMs / _lagUnit);
        return (int)Math.Clamp(delay, _minDelayMs, _maxDelayMs);
    }

    /// <summary>Waits <see cref="GetRecommendedDelayMs"/> milliseconds (completes synchronously when that is 0).</summary>
    public ValueTask DelayIfNeededAsync(CancellationToken cancellationToken)
    {
        int delayMs = GetRecommendedDelayMs();
        return delayMs <= 0 ? ValueTask.CompletedTask : new ValueTask(Task.Delay(TimeSpan.FromMilliseconds(delayMs), _timeProvider, cancellationToken));
    }

    public CommitDelaySnapshot GetSnapshot()
    {
        long count = Math.Min(Volatile.Read(ref _historyCount), _historySize);
        long currentLag = ReadCurrentLag();
        double averageLag = count == 0 ? 0 : (double)Volatile.Read(ref _historySum) / count;
        return new CommitDelaySnapshot(
            CurrentLag: currentLag,
            AverageLag: averageLag,
            EmaLag: ReadEma(),
            Trend: ComputeTrend(count),
            RecommendedDelayMs: GetRecommendedDelayMs(),
            History: ReadHistorySnapshot(count),
            ReplicaLags: ReadReplicaLagSnapshot());
    }

    private long ReadMaxReplicaLag()
    {
        long max = 0;
        for (int i = 0; i < _latestReplicaLag.Length; i++)
        {
            // A replica that stopped reporting does not hold the maximum (ReplicaLagStaleAfter).
            if (!IsCurrent(i)) continue;
            long value = Volatile.Read(ref _latestReplicaLag[i]);
            if (value > max) max = value;
        }
        return max;
    }

    private long ReadCurrentLag()
    {
        long sequence = Volatile.Read(ref _writeSequence);
        if (sequence <= 0) return 0;
        int slot = (int)((sequence - 1) % _historySize);
        return Volatile.Read(ref _history[slot]);
    }

    private void UpdateEma(long clusterLag)
    {
        while (true)
        {
            long oldBits = Volatile.Read(ref _emaBits);
            double oldEma = BitConverter.Int64BitsToDouble(oldBits);
            double newEma = oldEma == 0.0 ? clusterLag : (_emaAlpha * clusterLag) + ((1.0 - _emaAlpha) * oldEma);
            long newBits = BitConverter.DoubleToInt64Bits(newEma);
            if (Interlocked.CompareExchange(ref _emaBits, newBits, oldBits) == oldBits) return;
        }
    }

    private double ReadEma() => BitConverter.Int64BitsToDouble(Volatile.Read(ref _emaBits));

    /// <summary>Average delta between consecutive history samples (oldest to newest), without allocating.</summary>
    private double ComputeTrend(long count)
    {
        if (count < 2) return 0;
        count = Math.Min(count, _historySize);
        long sequence = Volatile.Read(ref _writeSequence);
        long start = Math.Max(0, sequence - count);
        double totalDelta = 0;
        long prev = Volatile.Read(ref _history[(int)(start % _historySize)]);
        for (long i = 1; i < count; i++)
        {
            long cur = Volatile.Read(ref _history[(int)((start + i) % _historySize)]);
            totalDelta += cur - prev;
            prev = cur;
        }
        return totalDelta / (count - 1);
    }

    private long[] ReadHistorySnapshot(long count)
    {
        if (count <= 0) return [];
        count = Math.Min(count, _historySize);
        long sequence = Volatile.Read(ref _writeSequence);
        long start = Math.Max(0, sequence - count);
        long[] snapshot = new long[count];
        for (long i = 0; i < count; i++) snapshot[i] = Volatile.Read(ref _history[(int)((start + i) % _historySize)]);
        return snapshot;
    }

    private long[] ReadReplicaLagSnapshot()
    {
        var result = new long[_latestReplicaLag.Length];
        for (int i = 0; i < result.Length; i++) result[i] = Volatile.Read(ref _latestReplicaLag[i]);
        return result;
    }
}
