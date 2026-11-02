using System.Diagnostics;

namespace RocksDb.Managed.Db;

/// <summary>Port of rocksdb::WriteStallCondition.</summary>
internal enum WriteStallCondition : byte
{
    Normal,
    Delayed,
    Stopped,
}

/// <summary>Port of rocksdb::WriteStallCause.</summary>
internal enum WriteStallCause : byte
{
    None,
    MemtableLimit,
    L0FileCountLimit,
    PendingCompactionBytes,
}

/// <summary>
/// Stop / delay / compaction-pressure accounting and the delayed-write token bucket. Port of rocksdb::WriteController
/// (db/write_controller.{h,cc}). Tokens are modelled as counters adjusted by <see cref="ColumnFamilyStallState"/>.
/// <para>
/// <b>Synchronization</b>: the counters are only changed under the DB mutex (RecalculateWriteStallConditions);
/// <see cref="GetDelay"/> is called by the write leader and serialized by an internal lock.
/// </para>
/// </summary>
internal sealed class WriteController
{
    private const ulong MicrosPerSecond = 1_000_000;
    private const ulong MicrosPerRefill = 1000; // Refill every 1 ms

    private readonly Lock _lock = new();
    private int _totalStopped;
    private int _totalDelayed;
    private int _totalCompactionPressure;
    private ulong _creditInBytes;
    private ulong _nextRefillTime;
    private ulong _delayedWriteRate;

    public WriteController(ulong delayedWriteRate)
    {
        if (delayedWriteRate == 0) delayedWriteRate = 16UL * 1024 * 1024;
        MaxDelayedWriteRate = delayedWriteRate;
        _delayedWriteRate = delayedWriteRate;
    }

    public ulong MaxDelayedWriteRate { get; private set; }

    /// <summary>WriteController::set_max_delayed_write_rate (SetDBOptions delayed_write_rate).</summary>
    public void SetMaxDelayedWriteRate(ulong writeRate)
    {
        lock (_lock)
        {
            // avoid divide 0
            if (writeRate == 0) writeRate = 1;
            MaxDelayedWriteRate = writeRate;
            Volatile.Write(ref _delayedWriteRate, writeRate);
        }
    }
    public ulong DelayedWriteRate => Volatile.Read(ref _delayedWriteRate);

    public bool IsStopped => Volatile.Read(ref _totalStopped) > 0;

    /// <summary>NeedsDelay(): writes are rate limited.</summary>
    public bool NeedsDelay => Volatile.Read(ref _totalDelayed) > 0;

    /// <summary>NeedSpeedupCompaction(): use all compaction threads.</summary>
    public bool NeedSpeedupCompaction => IsStopped || NeedsDelay || Volatile.Read(ref _totalCompactionPressure) > 0;

    public void AcquireStop() => Interlocked.Increment(ref _totalStopped);
    public void ReleaseStop() => Interlocked.Decrement(ref _totalStopped);

    /// <summary>GetDelayToken: starts (or continues) delaying at <paramref name="writeRate"/> bytes/s.</summary>
    public void AcquireDelay(ulong writeRate)
    {
        lock (_lock)
        {
            if (_totalDelayed++ == 0)
            {
                // Starting delay, so reset counters.
                _nextRefillTime = 0;
                _creditInBytes = 0;
            }
            // For simplicity, any current credit or "debt" in next_refill_time is based on an old rate.
            Volatile.Write(ref _delayedWriteRate, writeRate == 0 ? 1 : Math.Min(writeRate, MaxDelayedWriteRate));
        }
    }

    public void ReleaseDelay()
    {
        lock (_lock) _totalDelayed--;
    }

    public void AcquireCompactionPressure() => Interlocked.Increment(ref _totalCompactionPressure);
    public void ReleaseCompactionPressure() => Interlocked.Decrement(ref _totalCompactionPressure);

    private static ulong NowMicrosMonotonic() => (ulong)(Stopwatch.GetTimestamp() / (double)Stopwatch.Frequency * MicrosPerSecond);

    /// <summary>
    /// Port of WriteController::GetDelay: how long (microseconds) a write of <paramref name="numBytes"/> must wait to
    /// respect the delayed write rate (0 if it can proceed now).
    /// </summary>
    public ulong GetDelay(ulong numBytes)
    {
        if (IsStopped || !NeedsDelay) return 0;
        lock (_lock)
        {
            if (_creditInBytes >= numBytes)
            {
                _creditInBytes -= numBytes;
                return 0;
            }
            ulong timeNow = NowMicrosMonotonic();
            ulong rate = _delayedWriteRate;
            if (_nextRefillTime == 0)
            {
                // Start with an initial allotment of bytes for one interval.
                _nextRefillTime = timeNow;
            }
            if (_nextRefillTime <= timeNow)
            {
                // Refill based on time interval plus any extra elapsed.
                ulong elapsed = timeNow - _nextRefillTime + MicrosPerRefill;
                _creditInBytes += (ulong)(1.0 * elapsed / MicrosPerSecond * rate + 0.999999);
                _nextRefillTime = timeNow + MicrosPerRefill;
                if (_creditInBytes >= numBytes)
                {
                    // Avoid delay if possible.
                    _creditInBytes -= numBytes;
                    return 0;
                }
            }
            // We need to delay to avoid exceeding write rate.
            ulong bytesOverBudget = numBytes - _creditInBytes;
            ulong neededDelay = (ulong)(1.0 * bytesOverBudget / rate * MicrosPerSecond);
            _creditInBytes = 0;
            _nextRefillTime += neededDelay;
            // Minimum delay of refill interval, to reduce DB mutex contention.
            return Math.Max(_nextRefillTime - timeNow, MicrosPerRefill);
        }
    }
}

/// <summary>
/// The write-controller token currently held by a column family (ColumnFamilyData::write_controller_token_) and
/// the compaction debt seen at the previous recalculation. Guarded by the DB mutex.
/// </summary>
internal sealed class ColumnFamilyStallState
{
    private enum Token : byte
    {
        None,
        Stop,
        Delay,
        Pressure,
    }

    private const double IncSlowdownRatio = 0.8;
    private const double DecSlowdownRatio = 1 / IncSlowdownRatio;
    private const double NearStopSlowdownRatio = 0.6;
    private const ulong MinWriteRate = 16 * 1024; // Minimum write rate 16KB/s.

    private Token _token;
    private ulong _prevCompactionNeededBytes;

    public WriteStallCondition Condition { get; private set; }
    public WriteStallCause Cause { get; private set; }

    public void Release(WriteController wc)
    {
        switch (_token)
        {
            case Token.Stop: wc.ReleaseStop(); break;
            case Token.Delay: wc.ReleaseDelay(); break;
            case Token.Pressure: wc.ReleaseCompactionPressure(); break;
        }
        _token = Token.None;
        Condition = WriteStallCondition.Normal;
        Cause = WriteStallCause.None;
    }

    /// <summary>Port of ColumnFamilyData::GetWriteStallConditionAndCause.</summary>
    public static (WriteStallCondition, WriteStallCause) GetWriteStallConditionAndCause(int numUnflushedMemtables, int numL0Files,
        ulong numCompactionNeededBytes, ColumnFamilyOptions o)
    {
        int maxWriteBufferNumber = Math.Max(2, o.MaxWriteBufferNumber);
        if (numUnflushedMemtables >= maxWriteBufferNumber) return (WriteStallCondition.Stopped, WriteStallCause.MemtableLimit);
        if (!o.DisableAutoCompactions && numL0Files >= o.Level0StopWritesTrigger) return (WriteStallCondition.Stopped, WriteStallCause.L0FileCountLimit);
        if (!o.DisableAutoCompactions && o.HardPendingCompactionBytesLimit > 0 && numCompactionNeededBytes >= o.HardPendingCompactionBytesLimit)
        {
            return (WriteStallCondition.Stopped, WriteStallCause.PendingCompactionBytes);
        }
        if (maxWriteBufferNumber > 3 && numUnflushedMemtables >= maxWriteBufferNumber - 1 && numUnflushedMemtables - 1 >= o.MinWriteBufferNumberToMerge)
        {
            return (WriteStallCondition.Delayed, WriteStallCause.MemtableLimit);
        }
        if (!o.DisableAutoCompactions && o.Level0SlowdownWritesTrigger >= 0 && numL0Files >= o.Level0SlowdownWritesTrigger)
        {
            return (WriteStallCondition.Delayed, WriteStallCause.L0FileCountLimit);
        }
        if (!o.DisableAutoCompactions && o.SoftPendingCompactionBytesLimit > 0 && numCompactionNeededBytes >= o.SoftPendingCompactionBytesLimit)
        {
            return (WriteStallCondition.Delayed, WriteStallCause.PendingCompactionBytes);
        }
        return (WriteStallCondition.Normal, WriteStallCause.None);
    }

    /// <summary>
    /// Port of ColumnFamilyData::RecalculateWriteStallConditions: replaces this CF's token according to its memtable
    /// count, L0 file count and pending compaction bytes. REQUIRES: DB mutex.
    /// </summary>
    public WriteStallCondition Recalculate(WriteController wc, int numUnflushedMemtables, VersionStorageInfo vstorage, ColumnFamilyOptions o)
    {
        ulong compactionNeededBytes = vstorage.EstimatedCompactionNeededBytes;
        int l0 = vstorage.L0DelayTriggerCount;
        (WriteStallCondition condition, WriteStallCause cause) = GetWriteStallConditionAndCause(numUnflushedMemtables, l0, compactionNeededBytes, o);
        bool wasStopped = wc.IsStopped;
        bool neededDelay = wc.NeedsDelay;
        Release(wc);
        Condition = condition;
        Cause = cause;
        if (condition == WriteStallCondition.Stopped)
        {
            wc.AcquireStop();
            _token = Token.Stop;
        }
        else if (condition == WriteStallCondition.Delayed)
        {
            bool nearStop = cause switch
            {
                // If the distance to hard limit is less than 1/4 of the gap between soft and hard bytes limit, we think
                // it is near stop and speed up the slowdown.
                WriteStallCause.PendingCompactionBytes => o.HardPendingCompactionBytesLimit > 0
                    && compactionNeededBytes - o.SoftPendingCompactionBytesLimit > 3 * (o.HardPendingCompactionBytesLimit - o.SoftPendingCompactionBytesLimit) / 4,
                // L0 is the last two files from stopping.
                WriteStallCause.L0FileCountLimit => l0 >= o.Level0StopWritesTrigger - 2,
                _ => false,
            };
            SetupDelay(wc, compactionNeededBytes, wasStopped || nearStop, o.DisableAutoCompactions, neededDelay);
            _token = Token.Delay;
        }
        else
        {
            // Increase compaction threads if bottommost-relative debt is high or L0 is getting close to the slowdown trigger.
            bool pressure = !o.DisableAutoCompactions && (l0 >= GetL0FileCountForCompactionSpeedup(o.Level0FileNumCompactionTrigger, o.Level0SlowdownWritesTrigger)
                || compactionNeededBytes >= o.SoftPendingCompactionBytesLimit / 4);
            if (pressure)
            {
                wc.AcquireCompactionPressure();
                _token = Token.Pressure;
            }
            else if (neededDelay && !wc.NeedsDelay)
            {
                // Writes are no longer delayed: reset the delayed write rate to the user's rate on the next delay.
            }
        }
        _prevCompactionNeededBytes = compactionNeededBytes;
        return condition;
    }

    /// <summary>Port of SetupDelay (db/column_family.cc).</summary>
    private void SetupDelay(WriteController wc, ulong compactionNeededBytes, bool penalizeStop, bool autoCompactionsDisabled, bool alreadyDelayed)
    {
        ulong maxWriteRate = wc.MaxDelayedWriteRate;
        ulong writeRate = wc.DelayedWriteRate;
        if (autoCompactionsDisabled)
        {
            // When auto compaction is disabled, always use the value user gave.
            writeRate = maxWriteRate;
        }
        else if (alreadyDelayed && maxWriteRate > MinWriteRate)
        {
            // If already delayed, need to adjust based on previous compaction debt.
            if (penalizeStop)
            {
                writeRate = Math.Max(MinWriteRate, (ulong)(writeRate * NearStopSlowdownRatio));
            }
            else if (_prevCompactionNeededBytes > 0 && _prevCompactionNeededBytes <= compactionNeededBytes)
            {
                writeRate = Math.Max(MinWriteRate, (ulong)(writeRate * IncSlowdownRatio));
            }
            else if (_prevCompactionNeededBytes > compactionNeededBytes)
            {
                // Speed up when we have paid compaction debt, never faster than the user's rate.
                writeRate = Math.Min(maxWriteRate, (ulong)(writeRate * DecSlowdownRatio));
            }
        }
        else if (!alreadyDelayed)
        {
            writeRate = maxWriteRate;
        }
        wc.AcquireDelay(writeRate);
    }

    /// <summary>Port of GetL0FileCountForCompactionSpeedup.</summary>
    private static int GetL0FileCountForCompactionSpeedup(int trigger, int slowdown)
    {
        if (trigger < 0) return int.MaxValue;
        long twice = (long)trigger * 2;
        long oneFourth = trigger + (long)(slowdown - trigger) / 4;
        long res = Math.Min(twice, oneFourth);
        return res >= int.MaxValue ? int.MaxValue : (int)res;
    }
}
