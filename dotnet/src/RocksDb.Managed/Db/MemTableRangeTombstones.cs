namespace RocksDb.Managed.Db;

/// <summary>
/// Immutable list of non-overlapping range tombstone fragments, each covering <c>[start, end)</c> with the
/// descending list of sequence numbers of the tombstones that cover it. Simplified port of
/// rocksdb::FragmentedRangeTombstoneList (db/range_tombstone_fragmenter.{h,cc}); snapshot-striping and
/// timestamps are not implemented yet.
/// </summary>
/// <remarks>Built rarely (once per change of the memtable's range-deletion set) and then shared by readers.</remarks>
public sealed class FragmentedRangeTombstoneList
{
    private readonly byte[][] _starts;
    private readonly byte[][] _ends;
    private readonly int[] _seqStart; // fragment i's seqnums are _seqs[_seqStart[i].._seqStart[i+1]]
    private readonly ulong[] _seqs;
    private readonly Comparator _ucmp;

    public static FragmentedRangeTombstoneList Empty(Comparator ucmp) => new(ucmp, [], [], [0], []);

    private FragmentedRangeTombstoneList(Comparator ucmp, byte[][] starts, byte[][] ends, int[] seqStart, ulong[] seqs)
    {
        _ucmp = ucmp;
        _starts = starts;
        _ends = ends;
        _seqStart = seqStart;
        _seqs = seqs;
    }

    /// <summary>Unfragmented input tombstone.</summary>
    public readonly record struct Tombstone(byte[] Start, byte[] End, ulong Sequence);

    public int Count => _starts.Length;
    public bool IsEmpty => _starts.Length == 0;
    public ReadOnlySpan<byte> GetStart(int i) => _starts[i];
    public ReadOnlySpan<byte> GetEnd(int i) => _ends[i];
    /// <summary>Sequence numbers covering fragment <paramref name="i"/>, in descending order.</summary>
    public ReadOnlySpan<ulong> GetSequences(int i) => _seqs.AsSpan(_seqStart[i], _seqStart[i + 1] - _seqStart[i]);

    /// <summary>Fragments overlapping tombstones (user comparator order). Empty ranges are dropped.</summary>
    public static FragmentedRangeTombstoneList Build(Comparator ucmp, List<Tombstone> tombstones)
    {
        // Drop empty ranges, sort by start.
        tombstones.RemoveAll(t => ucmp.Compare(t.Start, t.End) >= 0);
        if (tombstones.Count == 0) return Empty(ucmp);
        tombstones.Sort((a, b) => ucmp.Compare(a.Start, b.Start));

        // All distinct boundaries, sorted.
        var bounds = new List<byte[]>(tombstones.Count * 2);
        foreach (Tombstone t in tombstones)
        {
            bounds.Add(t.Start);
            bounds.Add(t.End);
        }
        bounds.Sort((a, b) => ucmp.Compare(a, b));
        int w = 1;
        for (int i = 1; i < bounds.Count; i++)
        {
            if (ucmp.Compare(bounds[i], bounds[w - 1]) != 0) bounds[w++] = bounds[i];
        }
        bounds.RemoveRange(w, bounds.Count - w);

        var starts = new List<byte[]>();
        var ends = new List<byte[]>();
        var seqStart = new List<int> { 0 };
        var seqs = new List<ulong>();
        var active = new List<Tombstone>();
        var scratch = new List<ulong>();
        int nextT = 0;
        for (int k = 0; k + 1 < bounds.Count; k++)
        {
            byte[] lo = bounds[k];
            while (nextT < tombstones.Count && ucmp.Compare(tombstones[nextT].Start, lo) <= 0) active.Add(tombstones[nextT++]);
            active.RemoveAll(t => ucmp.Compare(t.End, lo) <= 0);
            if (active.Count == 0) continue;
            scratch.Clear();
            foreach (Tombstone t in active) scratch.Add(t.Sequence);
            scratch.Sort();
            starts.Add(lo);
            ends.Add(bounds[k + 1]);
            ulong prev = ulong.MaxValue;
            for (int i = scratch.Count - 1; i >= 0; i--)
            {
                if (scratch[i] != prev) seqs.Add(scratch[i]);
                prev = scratch[i];
            }
            seqStart.Add(seqs.Count);
        }
        return new FragmentedRangeTombstoneList(ucmp, starts.ToArray(), ends.ToArray(), seqStart.ToArray(), seqs.ToArray());
    }

    /// <summary>
    /// Largest sequence number &lt;= <paramref name="readSequence"/> of a tombstone covering <paramref name="userKey"/>,
    /// or 0 if none. Port of FragmentedRangeTombstoneIterator::MaxCoveringTombstoneSeqnum.
    /// </summary>
    public ulong MaxCoveringTombstoneSeqnum(ReadOnlySpan<byte> userKey, ulong readSequence)
    {
        // Binary search for the last fragment whose start <= userKey.
        int lo = 0, hi = _starts.Length - 1, found = -1;
        while (lo <= hi)
        {
            int mid = (lo + hi) >>> 1;
            if (_ucmp.Compare(_starts[mid], userKey) <= 0)
            {
                found = mid;
                lo = mid + 1;
            }
            else hi = mid - 1;
        }
        if (found < 0 || _ucmp.Compare(userKey, _ends[found]) >= 0) return 0;
        foreach (ulong s in GetSequences(found))
        {
            if (s <= readSequence) return s;
        }
        return 0;
    }
}
