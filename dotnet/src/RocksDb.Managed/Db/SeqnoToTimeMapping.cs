using System.Buffers;
using System.Globalization;
using System.Text;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Db;

/// <summary>
/// Port of SeqnoToTimeMapping (db/seqno_to_time_mapping.{h,cc}): a sorted list of (sequence number, unix time) pairs
/// recording that each seqno was written at or before its time. Used to estimate the write time of data
/// (preserve_internal_time_seconds, preclude_last_level_data_seconds, TimedPut) and persisted per SST in the table
/// property <c>rocksdb.seqno.time.map</c>: <c>varint64(count)</c> then <c>varint64(seqno delta) varint64(time delta)</c>
/// per pair (empty string for an empty mapping). Not thread-safe: the DB-wide instance is guarded by the DB mutex and
/// readers get copies.
/// </summary>
internal sealed class SeqnoToTimeMapping
{
    /// <summary>kUnknownTimeBeforeAll.</summary>
    public const ulong UnknownTimeBeforeAll = 0;
    /// <summary>kUnknownSeqnoBeforeAll.</summary>
    public const ulong UnknownSeqnoBeforeAll = 0;
    /// <summary>kMaxSeqnoTimePairsPerSST.</summary>
    public const ulong MaxSeqnoTimePairsPerSst = 100;
    /// <summary>kMaxSeqnoTimePairsPerCF.</summary>
    public const ulong MaxSeqnoTimePairsPerCf = 100;
    /// <summary>kMaxSeqnoToTimeEntries.</summary>
    public const ulong MaxSeqnoToTimeEntries = MaxSeqnoTimePairsPerCf * 10;

    /// <summary>SeqnoTimePair.</summary>
    public struct SeqnoTimePair(ulong seqno, ulong time) : IComparable<SeqnoTimePair>
    {
        public ulong Seqno = seqno;
        public ulong Time = time;

        public readonly int CompareTo(SeqnoTimePair other)
        {
            int c = Seqno.CompareTo(other.Seqno);
            return c != 0 ? c : Time.CompareTo(other.Time);
        }

        /// <summary>
        /// SeqnoTimePair::Merge: combines <paramref name="other"/> (not sorting before this) into this entry when
        /// they are redundant or inconsistent; false when both are kept.
        /// </summary>
        public bool Merge(SeqnoTimePair other)
        {
            if (Seqno == other.Seqno)
            {
                // Favoring GetProximalSeqnoBeforeTime over GetProximalTimeBeforeSeqno by keeping the older time.
                Time = Math.Min(Time, other.Time);
                return true;
            }
            if (Time == other.Time)
            {
                // Keep the newer seqno: when a burst of writes ages out, the cutoff is the newest seqno from that burst.
                Seqno = Math.Max(Seqno, other.Seqno);
                return true;
            }
            if (Time > other.Time)
            {
                // Inconsistency (clock drift?): trust the earlier time entry.
                this = other;
                return true;
            }
            return false;
        }

        public override readonly string ToString() => Seqno.ToString(CultureInfo.InvariantCulture) + "->" + Time.ToString(CultureInfo.InvariantCulture);
    }

    private ulong _maxTimeSpan = ulong.MaxValue;
    private ulong _capacity = ulong.MaxValue;
    private readonly List<SeqnoTimePair> _pairs = new();
    private bool _enforced = true;

    public int Size => _pairs.Count;
    public ulong Capacity => _capacity;
    public bool Empty => _pairs.Count == 0;
    internal bool IsEnforced => _enforced;
    internal IReadOnlyList<SeqnoTimePair> Pairs => _pairs;

    /// <summary>Removes every pair (Clear).</summary>
    public void Clear()
    {
        _pairs.Clear();
        _enforced = true;
    }

    /// <summary>SetMaxTimeSpan: entries older than this before the newest (or now) are pruned, keeping one.</summary>
    public SeqnoToTimeMapping SetMaxTimeSpan(ulong maxTimeSpan)
    {
        _maxTimeSpan = maxTimeSpan;
        if (_enforced) EnforceMaxTimeSpan(0);
        return this;
    }

    /// <summary>SetCapacity: the maximum number of pairs kept (the least informative are dropped).</summary>
    public SeqnoToTimeMapping SetCapacity(ulong capacity)
    {
        _capacity = capacity;
        if (_enforced) EnforceCapacity(strict: true);
        return this;
    }

    /// <summary>Enforce: sorts and merges unenforced additions, applies the time span (relative to now if non-zero) and capacity.</summary>
    public SeqnoToTimeMapping Enforce(ulong now)
    {
        if (!_enforced)
        {
            SortAndMerge();
            EnforceMaxTimeSpan(now);
        }
        else if (now > 0)
        {
            EnforceMaxTimeSpan(now);
        }
        EnforceCapacity(strict: true);
        return this;
    }

    /// <summary>AddUnenforced: adds a pair without sorting (call <see cref="Enforce"/> before queries).</summary>
    public void AddUnenforced(ulong seqno, ulong time)
    {
        if (seqno == 0) return;
        _enforced = false;
        _pairs.Add(new SeqnoTimePair(seqno, time));
    }

    /// <summary>
    /// Append: adds a pair at the end (merging with the last one when redundant), then enforces the time span and,
    /// loosely, the capacity. Returns true if a new entry was added.
    /// </summary>
    public bool Append(ulong seqno, ulong time)
    {
        if (_capacity == 0) return false;
        bool added = false;
        if (seqno == 0)
        {
            // skip seq number 0, which may have special meaning, like zeroed out data
        }
        else if (_pairs.Count == 0)
        {
            _enforced = true;
            _pairs.Add(new SeqnoTimePair(seqno, time));
            return true;
        }
        else
        {
            SeqnoTimePair last = _pairs[^1];
            if (last.Seqno <= seqno)
            {
                bool merged = last.Merge(new SeqnoTimePair(seqno, time));
                if (merged)
                {
                    _pairs[^1] = last;
                }
                else if (!(_enforced && (seqno <= last.Seqno || time <= last.Time)))
                {
                    _pairs.Add(new SeqnoTimePair(seqno, time));
                    added = true;
                }
                // else: out of order append (clock reset); ignored like a release build of upstream.
            }
            else if (!_enforced)
            {
                _pairs.Add(new SeqnoTimePair(seqno, time));
                added = true;
            }
            // else: out of order append attempted; ignored.
        }
        if (!_enforced) SortAndMerge();
        EnforceMaxTimeSpan(0);
        EnforceCapacity(strict: false);
        return added;
    }

    /// <summary>
    /// PrePopulate: fills an empty mapping with one entry per seqno in [from, to], times interpolated linearly in
    /// [fromTime, toTime] (a newly opened DB with preserve / preclude, so existing data looks old).
    /// </summary>
    public void PrePopulate(ulong fromSeqno, ulong toSeqno, ulong fromTime, ulong toTime)
    {
        for (ulong i = fromSeqno; i <= toSeqno; i++)
        {
            ulong t = fromTime + (toTime - fromTime) * (i - fromSeqno) / (toSeqno - fromSeqno);
            _pairs.Add(new SeqnoTimePair(i, t));
        }
    }

    /// <summary>
    /// DecodeFrom: appends the pairs of an encoded mapping (unenforced when merging into a non-empty or limited
    /// mapping). Returns null on success, else the corruption message (the mapping is unchanged then).
    /// </summary>
    public string? DecodeFrom(ReadOnlySpan<byte> input)
    {
        int origSize = _pairs.Count;
        string? error = DecodeImpl(input, _pairs);
        if (error is not null)
        {
            _pairs.RemoveRange(origSize, _pairs.Count - origSize);
        }
        else if (origSize > 0 || _maxTimeSpan < ulong.MaxValue || _capacity < ulong.MaxValue)
        {
            _enforced = false;
        }
        return error;
    }

    private static string? DecodeImpl(ReadOnlySpan<byte> input, List<SeqnoTimePair> pairs)
    {
        if (input.IsEmpty) return null;
        if (!Coding.GetVarint64(ref input, out ulong count)) return "Invalid sequence number time size";
        var b = new SeqnoTimePair(0, 0);
        for (ulong i = 0; i < count; i++)
        {
            if (!Coding.GetVarint64(ref input, out ulong seqno)) return "Invalid sequence number";
            if (!Coding.GetVarint64(ref input, out ulong time)) return "Invalid time";
            b = new SeqnoTimePair(b.Seqno + seqno, b.Time + time);
            pairs.Add(b);
        }
        if (!input.IsEmpty) return "Extra bytes at end of sequence number time mapping";
        return null;
    }

    /// <summary>EncodeTo: the table property encoding (nothing for an empty mapping). REQUIRES: enforced.</summary>
    public void EncodeTo(IBufferWriter<byte> dest)
    {
        if (_pairs.Count == 0) return;
        Coding.PutVarint64(dest, (ulong)_pairs.Count);
        var b = new SeqnoTimePair(0, 0);
        foreach (SeqnoTimePair cur in _pairs)
        {
            Coding.PutVarint64(dest, cur.Seqno - b.Seqno);
            Coding.PutVarint64(dest, cur.Time - b.Time);
            b = cur;
        }
    }

    /// <summary>The encoded form as a byte array (empty for an empty mapping).</summary>
    public byte[] Encode()
    {
        var w = new ArrayBufferWriter<byte>();
        EncodeTo(w);
        return w.WrittenSpan.ToArray();
    }

    /// <summary>
    /// CopyFromSeqnoRange: appends the pairs of <paramref name="src"/> relevant to [from, to], including the entry
    /// before <paramref name="fromSeqno"/> (to answer GetProximalTimeBeforeSeqno(from)).
    /// </summary>
    public void CopyFromSeqnoRange(SeqnoToTimeMapping src, ulong fromSeqno, ulong toSeqno)
    {
        bool origEmpty = Empty;
        int srcIt = src.FindGreaterEqSeqno(fromSeqno);
        // Allow nonsensical ranges like [1000, 0] which might show up e.g. for an SST file with no entries.
        int srcItEnd = toSeqno < fromSeqno ? srcIt : src.FindGreaterSeqno(toSeqno);
        if (srcIt > 0) --srcIt;
        for (int i = srcIt; i < srcItEnd; i++) _pairs.Add(src._pairs[i]);
        if (!origEmpty || _maxTimeSpan < ulong.MaxValue || _capacity < ulong.MaxValue) _enforced = false;
    }

    /// <summary>CopyFrom: all pairs of <paramref name="src"/>.</summary>
    public void CopyFrom(SeqnoToTimeMapping src) => CopyFromSeqnoRange(src, UnknownSeqnoBeforeAll, DbFormat.MaxSequenceNumber);

    /// <summary>GetProximalTimeBeforeSeqno: the time of the last entry with a seqno strictly below <paramref name="seqno"/> (0 if none).</summary>
    public ulong GetProximalTimeBeforeSeqno(ulong seqno)
    {
        int it = FindGreaterEqSeqno(seqno);
        return it == 0 ? UnknownTimeBeforeAll : _pairs[it - 1].Time;
    }

    /// <summary>GetProximalSeqnoBeforeTime: the seqno of the last entry with a time &lt;= <paramref name="time"/> (0 if none).</summary>
    public ulong GetProximalSeqnoBeforeTime(ulong time)
    {
        int it = FindGreaterTime(time);
        return it == 0 ? UnknownSeqnoBeforeAll : _pairs[it - 1].Seqno;
    }

    /// <summary>
    /// GetCurrentTieringCutoffSeqnos: the minimum seqnos whose write time must be preserved
    /// (<paramref name="preserveTimeMinSeqno"/>) and that must stay out of the last level
    /// (<paramref name="precludeLastLevelMinSeqno"/>). Outputs are left unchanged when the features are disabled.
    /// </summary>
    public void GetCurrentTieringCutoffSeqnos(ulong currentTime, ulong preserveInternalTimeSeconds, ulong precludeLastLevelDataSeconds,
        ref ulong preserveTimeMinSeqno, ref ulong precludeLastLevelMinSeqno)
    {
        ulong preserveTimeDuration = Math.Max(preserveInternalTimeSeconds, precludeLastLevelDataSeconds);
        if (preserveTimeDuration == 0) return;
        ulong preserveTime = currentTime > preserveTimeDuration ? currentTime - preserveTimeDuration : 0;
        // GetProximalSeqnoBeforeTime tells us the last seqno known to have been written at or before the given time.
        // + 1 to get the minimum we should preserve without excluding anything that might have been written on or
        // after the given time.
        preserveTimeMinSeqno = GetProximalSeqnoBeforeTime(preserveTime) + 1;
        if (precludeLastLevelDataSeconds > 0)
        {
            ulong precludeLastLevelTime = currentTime > precludeLastLevelDataSeconds ? currentTime - precludeLastLevelDataSeconds : 0;
            precludeLastLevelMinSeqno = GetProximalSeqnoBeforeTime(precludeLastLevelTime) + 1;
        }
    }

    public string ToHumanString()
    {
        var sb = new StringBuilder();
        foreach (SeqnoTimePair p in _pairs)
        {
            sb.Append(p.Seqno.ToString(CultureInfo.InvariantCulture)).Append("->").Append(p.Time.ToString(CultureInfo.InvariantCulture)).Append(',');
        }
        return sb.ToString();
    }

    // ---- internals --------------------------------------------------------------------------------------------

    /// <summary>upper_bound by time.</summary>
    private int FindGreaterTime(ulong time)
    {
        int lo = 0, hi = _pairs.Count;
        while (lo < hi)
        {
            int mid = (lo + hi) >>> 1;
            if (_pairs[mid].Time <= time) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    /// <summary>lower_bound by seqno.</summary>
    private int FindGreaterEqSeqno(ulong seqno)
    {
        int lo = 0, hi = _pairs.Count;
        while (lo < hi)
        {
            int mid = (lo + hi) >>> 1;
            if (_pairs[mid].Seqno < seqno) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    /// <summary>upper_bound by seqno.</summary>
    private int FindGreaterSeqno(ulong seqno)
    {
        int lo = 0, hi = _pairs.Count;
        while (lo < hi)
        {
            int mid = (lo + hi) >>> 1;
            if (_pairs[mid].Seqno <= seqno) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    private void EnforceMaxTimeSpan(ulong now)
    {
        if (_pairs.Count <= 1) return;
        ulong cutoffTime;
        if (now > 0)
        {
            if (now < _maxTimeSpan) return; // Nothing eligible to prune / avoid underflow
            cutoffTime = now - _maxTimeSpan;
        }
        else
        {
            SeqnoTimePair last = _pairs[^1];
            if (last.Time < _maxTimeSpan) return;
            cutoffTime = last.Time - _maxTimeSpan;
        }
        // Keep one entry <= cutoff_time
        int remove = 0;
        while (_pairs.Count - remove >= 2 && _pairs[remove].Time <= cutoffTime && _pairs[remove + 1].Time <= cutoffTime) remove++;
        if (remove > 0) _pairs.RemoveRange(0, remove);
    }

    private void EnforceCapacity(bool strict)
    {
        ulong strictCap = _capacity;
        if (strictCap == 0)
        {
            _pairs.Clear();
            return;
        }
        // Treat cap of 1 as 2 to work with the below algorithm (etc.)
        if (strictCap == 1) strictCap = 2;
        // When !strict, allow being over nominal capacity by a modest fraction.
        ulong effectiveCap = strictCap + (strict ? 0 : strictCap / 8);
        if (effectiveCap < strictCap) effectiveCap = ulong.MaxValue; // Correct overflow
        if ((ulong)_pairs.Count <= effectiveCap) return;
        int toRemoveCount = _pairs.Count - (int)strictCap;

        // A priority queue of best removal candidates (smallest time gap remaining after removal); for the same gap the
        // newer entry is less attractive for removal.
        var pq = new PriorityQueue<int, (ulong Gap, ulong Seqno)>();
        ulong prevPrevTime = _pairs[0].Time;
        int prevIt = 1;
        for (int it = 2; it < _pairs.Count; it++)
        {
            ulong gap = _pairs[it].Time - prevPrevTime;
            pq.Enqueue(prevIt, (gap, _pairs[prevIt].Seqno));
            prevPrevTime = _pairs[prevIt].Time;
            prevIt = it;
        }
        // Greedily remove the best candidate, iteratively. Entries marked for deletion get time 0.
        while (toRemoveCount > 0)
        {
            pq.TryDequeue(out int rc, out (ulong Gap, ulong Seqno) prio);
            int a = rc + 1;
            ulong afterTime = _pairs[a].Time;
            while (afterTime == UnknownTimeBeforeAll) afterTime = _pairs[++a].Time;
            int bIdx = rc - 1;
            ulong beforeTime = _pairs[bIdx].Time;
            while (beforeTime == UnknownTimeBeforeAll) beforeTime = _pairs[--bIdx].Time;
            // Check whether the gap is still valid (or needs to be recomputed)
            if (prio.Gap == afterTime - beforeTime)
            {
                SeqnoTimePair p = _pairs[rc];
                p.Time = UnknownTimeBeforeAll;
                _pairs[rc] = p;
                --toRemoveCount;
            }
            else
            {
                pq.Enqueue(rc, (afterTime - beforeTime, _pairs[rc].Seqno));
            }
        }
        // Collapse away entries marked for deletion
        _pairs.RemoveAll(static p => p.Time == UnknownTimeBeforeAll);
    }

    private void SortAndMerge()
    {
        if (_pairs.Count > 0)
        {
            _pairs.Sort();
            int to = 0;
            for (int from = 1; from < _pairs.Count; from++)
            {
                SeqnoTimePair t = _pairs[to];
                if (t.Merge(_pairs[from]))
                {
                    _pairs[to] = t; // Merged with last entry
                }
                else
                {
                    _pairs[++to] = _pairs[from];
                }
            }
            _pairs.RemoveRange(to + 1, _pairs.Count - to - 1);
        }
        // Mark as "at least sorted"
        _enforced = true;
    }
}

/// <summary>
/// Port of MinAndMaxPreserveSeconds (db/seqno_to_time_mapping.h): the smallest and largest
/// max(preserve_internal_time_seconds, preclude_last_level_data_seconds) over column families.
/// </summary>
internal struct MinAndMaxPreserveSeconds
{
    public ulong MinPreserveSeconds;
    public ulong MaxPreserveSeconds;

    public MinAndMaxPreserveSeconds()
    {
        MinPreserveSeconds = ulong.MaxValue;
        MaxPreserveSeconds = ulong.MinValue;
    }

    public MinAndMaxPreserveSeconds(ColumnFamilyOptions opts) : this() => Combine(opts);

    public readonly bool IsEnabled => MinPreserveSeconds != ulong.MaxValue;

    public void Combine(ColumnFamilyOptions opts)
    {
        ulong preserveSeconds = Math.Max(opts.PreserveInternalTimeSeconds, opts.PrecludeLastLevelDataSeconds);
        if (preserveSeconds > 0)
        {
            MinPreserveSeconds = Math.Min(preserveSeconds, MinPreserveSeconds);
            MaxPreserveSeconds = Math.Max(preserveSeconds, MaxPreserveSeconds);
        }
    }

    /// <summary>GetRecodingCadence: seconds between two recorded pairs (0 = disabled).</summary>
    public readonly ulong GetRecodingCadence() =>
        IsEnabled ? (MinPreserveSeconds + SeqnoToTimeMapping.MaxSeqnoTimePairsPerCf - 1) / SeqnoToTimeMapping.MaxSeqnoTimePairsPerCf : 0;
}
