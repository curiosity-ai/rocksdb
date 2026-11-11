using System.Globalization;
using System.Text;

namespace RocksDb.Managed.Cluster;

/// <summary>
/// Sparse map of "log entry term" for the WAL (rocksdb-sharp <c>RaftLogTerms</c>). The RocksDB sequence number is the
/// Raft log index; instead of storing a term in every WriteBatch, each term records the WAL sequence number it starts
/// at, and the term of any sequence number is the term of the latest range starting at or before it (binary search).
/// Thread-safe. Serialized as one "startSeq,term" line per range.
/// </summary>
public sealed class RaftLogTerms
{
    // Sorted by StartSeq ascending.
    private readonly List<(ulong StartSeq, long Term)> _ranges = new();
    private readonly Lock _gate = new();

    /// <summary>
    /// Records that entries from <paramref name="startSeq"/> on belong to <paramref name="term"/>. Returns true if the
    /// map changed. Stale or out-of-order records are ignored rather than thrown on, because a WAL-stream consumer may
    /// legitimately re-observe old batches after a reconnect.
    /// </summary>
    public bool Record(ulong startSeq, long term)
    {
        lock (_gate)
        {
            if (_ranges.Count > 0)
            {
                (ulong lastStart, long lastTerm) = _ranges[^1];
                // We only record the first sequence number of each term.
                if (term <= lastTerm) return false;
                if (startSeq < lastStart) return false;
                if (startSeq == lastStart)
                {
                    // The previous term produced no entries (its range starts beyond the log tail); the new term
                    // supersedes it.
                    _ranges[^1] = (startSeq, term);
                    return true;
                }
            }
            _ranges.Add((startSeq, term));
            return true;
        }
    }

    /// <summary>Whether <see cref="Record"/> with these arguments would change the map.</summary>
    public bool WouldRecord(ulong startSeq, long term)
    {
        lock (_gate)
        {
            if (_ranges.Count == 0) return true;
            (ulong lastStart, long lastTerm) = _ranges[^1];
            return term > lastTerm && startSeq >= lastStart;
        }
    }

    /// <summary>An independent copy.</summary>
    public RaftLogTerms Clone()
    {
        var copy = new RaftLogTerms();
        lock (_gate) copy._ranges.AddRange(_ranges);
        return copy;
    }

    /// <summary>Removes every range that starts after <paramref name="seq"/>; returns true if the map changed.</summary>
    public bool TruncateAfter(ulong seq)
    {
        lock (_gate)
        {
            int n = _ranges.Count;
            while (_ranges.Count > 0 && _ranges[^1].StartSeq > seq) _ranges.RemoveAt(_ranges.Count - 1);
            return _ranges.Count != n;
        }
    }

    /// <summary>The term of the WAL entry at <paramref name="seq"/>; 0 if nothing is recorded at or before it.</summary>
    public long GetTermForSeq(ulong seq)
    {
        lock (_gate)
        {
            int lo = 0, hi = _ranges.Count - 1, idx = -1;
            while (lo <= hi)
            {
                int mid = (lo + hi) >> 1;
                if (_ranges[mid].StartSeq <= seq)
                {
                    idx = mid;
                    lo = mid + 1;
                }
                else
                {
                    hi = mid - 1;
                }
            }
            return idx < 0 ? 0 : _ranges[idx].Term;
        }
    }

    public (ulong StartSeq, long Term) GetLast()
    {
        lock (_gate) return _ranges.Count == 0 ? (0UL, 0L) : _ranges[^1];
    }

    /// <summary>A copy of the recorded ranges, oldest first.</summary>
    public IReadOnlyList<(ulong StartSeq, long Term)> GetRanges()
    {
        lock (_gate) return [.. _ranges];
    }

    public string Serialize()
    {
        lock (_gate)
        {
            var sb = new StringBuilder();
            foreach ((ulong s, long t) in _ranges)
            {
                sb.Append(s.ToString(CultureInfo.InvariantCulture)).Append(',').Append(t.ToString(CultureInfo.InvariantCulture)).Append('\n');
            }
            return sb.ToString();
        }
    }

    public static RaftLogTerms Deserialize(string? content)
    {
        var t = new RaftLogTerms();
        if (string.IsNullOrEmpty(content)) return t;
        foreach (string line in content.Split('\n', StringSplitOptions.RemoveEmptyEntries))
        {
            string[] parts = line.Split(',');
            if (parts.Length != 2) continue;
            if (ulong.TryParse(parts[0], NumberStyles.Integer, CultureInfo.InvariantCulture, out ulong s) &&
                long.TryParse(parts[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out long term))
            {
                t._ranges.Add((s, term));
            }
        }
        return t;
    }
}
