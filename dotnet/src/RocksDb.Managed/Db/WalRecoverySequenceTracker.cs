namespace RocksDb.Managed.Db;

/// <summary>A sequence range whose WAL write failed with an indeterminate outcome (WalRecoverySequenceGap).</summary>
/// <param name="WalNumber">The WAL the failed write went to.</param>
/// <param name="StartSequence">First sequence of the failed write group.</param>
/// <param name="EndSequence">Last sequence of the failed write group.</param>
/// <param name="Generation">The tracker generation that recorded it (1-based).</param>
internal readonly record struct WalRecoverySequenceGap(ulong WalNumber, ulong StartSequence, ulong EndSequence, ulong Generation);

/// <summary>
/// Port of WalRecoverySequenceTracker (db/wal_iterator_impl.{h,cc}): the DB-lifetime fence of sequence ranges whose WAL
/// write failed. Successful WAL write error recovery publishes such a range (so later writes never reuse it) without
/// the write being in the DB, even though its record may be complete in the failed WAL; <see cref="TransactionLogIterator"/>
/// stops before a recorded range instead of returning it. Thread-safe.
/// </summary>
internal sealed class WalRecoverySequenceTracker
{
    private readonly Lock _lock = new();
    // gaps_by_end_sequence_: keyed by the range's last sequence.
    private readonly SortedList<ulong, WalRecoverySequenceGap> _gapsByEnd = new();
    private ulong _generation;

    /// <summary>Records the failed range [<paramref name="start"/>, <paramref name="end"/>] of WAL <paramref name="walNumber"/>.</summary>
    public void Record(ulong walNumber, ulong start, ulong end)
    {
        if (walNumber == 0 || start == 0 || start > end) return;
        lock (_lock)
        {
            if (_gapsByEnd.ContainsKey(end)) return;
            _generation++;
            _gapsByEnd.Add(end, new WalRecoverySequenceGap(walNumber, start, end, _generation));
        }
    }

    /// <summary>The first recorded range that ends at or after <paramref name="sequence"/>.</summary>
    public bool FirstAtOrAfter(ulong sequence, out WalRecoverySequenceGap gap)
    {
        lock (_lock)
        {
            IList<ulong> keys = _gapsByEnd.Keys;
            int lo = 0, hi = keys.Count;
            while (lo < hi)
            {
                int mid = (lo + hi) >>> 1;
                if (keys[mid] < sequence) lo = mid + 1;
                else hi = mid;
            }
            if (lo == keys.Count)
            {
                gap = default;
                return false;
            }
            gap = _gapsByEnd.Values[lo];
            return true;
        }
    }

    /// <summary>The number of ranges recorded so far.</summary>
    public ulong CurrentGeneration
    {
        get
        {
            lock (_lock) return _generation;
        }
    }
}
