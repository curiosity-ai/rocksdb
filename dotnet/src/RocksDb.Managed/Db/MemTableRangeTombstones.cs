namespace RocksDb.Managed.Db;

/// <summary>
/// Immutable list of non-overlapping range tombstone fragments, each covering <c>[start, end)</c> with the
/// descending list of sequence numbers of the tombstones that cover it. Port of rocksdb::FragmentedRangeTombstoneList
/// and the lookup side of FragmentedRangeTombstoneIterator (db/range_tombstone_fragmenter.{h,cc}); snapshot striping is
/// applied at query time by <see cref="RangeDelAggregator"/>.
/// </summary>
/// <remarks>
/// <para>Built rarely (once per change of the memtable's range-deletion set) and then shared by readers.</para>
/// <para><b>User-defined timestamps.</b> With a timestamp-aware comparator every tombstone is stored as
/// <c>start | ts</c> -&gt; <c>end | ts</c>. Fragments are cut on the keys without timestamps
/// (CompareWithoutTimestamp) and, like upstream, their boundaries carry the maximum timestamp, so a plain user-key
/// comparison against a key with any real timestamp behaves like CompareWithoutTimestamp (start &lt;= key, key &lt; end).
/// The timestamps covering a fragment are kept next to its sequence numbers, both sorted newest first and
/// independently (upstream assumes they are ordered the same way); a reader with a timestamp upper bound only sees
/// tombstones at or below it (SetMaxVisibleSeqAndTimestamp).</para>
/// </remarks>
public sealed class FragmentedRangeTombstoneList
{
    private readonly byte[][] _starts;
    private readonly byte[][] _ends;
    private readonly int[] _seqStart; // fragment i's seqnums are _seqs[_seqStart[i].._seqStart[i+1]]
    private readonly ulong[] _seqs;
    private readonly byte[] _timestamps; // _seqs.Length * TimestampSize bytes, parallel to _seqs
    private readonly Comparator _ucmp;
    private readonly int _tsSize;

    public static FragmentedRangeTombstoneList Empty(Comparator ucmp) => new(ucmp, [], [], [0], [], []);

    private FragmentedRangeTombstoneList(Comparator ucmp, byte[][] starts, byte[][] ends, int[] seqStart, ulong[] seqs, byte[] timestamps)
    {
        _ucmp = ucmp;
        _tsSize = ucmp.TimestampSize;
        _starts = starts;
        _ends = ends;
        _seqStart = seqStart;
        _seqs = seqs;
        _timestamps = timestamps;
    }

    /// <summary>
    /// Unfragmented input tombstone. With user-defined timestamps <see cref="Start"/> and <see cref="End"/> are user
    /// keys with a timestamp (any; only the key part is used) and the tombstone's timestamp is <see cref="Timestamp"/>,
    /// or the one of <see cref="Start"/> when null.
    /// </summary>
    public readonly record struct Tombstone(byte[] Start, byte[] End, ulong Sequence, byte[]? Timestamp)
    {
        /// <summary>A tombstone without an explicit timestamp (none, or the one carried by <paramref name="Start"/>).</summary>
        public Tombstone(byte[] Start, byte[] End, ulong Sequence) : this(Start, End, Sequence, null) { }
    }

    public int Count => _starts.Length;
    public bool IsEmpty => _starts.Length == 0;
    /// <summary>User-defined timestamp size of the comparator (0 = none).</summary>
    public int TimestampSize => _tsSize;
    /// <summary>Start of fragment <paramref name="i"/> (with the maximum timestamp when timestamps are enabled).</summary>
    public ReadOnlySpan<byte> GetStart(int i) => _starts[i];
    /// <summary>Exclusive end of fragment <paramref name="i"/> (with the maximum timestamp when timestamps are enabled).</summary>
    public ReadOnlySpan<byte> GetEnd(int i) => _ends[i];
    /// <summary>Sequence numbers covering fragment <paramref name="i"/>, in descending order.</summary>
    public ReadOnlySpan<ulong> GetSequences(int i) => _seqs.AsSpan(_seqStart[i], _seqStart[i + 1] - _seqStart[i]);

    /// <summary>
    /// Timestamp of the <paramref name="k"/>-th tombstone covering fragment <paramref name="i"/> (newest first, same
    /// position as in <see cref="GetSequences"/>). REQUIRES: <see cref="TimestampSize"/> &gt; 0.
    /// </summary>
    public ReadOnlySpan<byte> GetTimestamp(int i, int k) => _timestamps.AsSpan((_seqStart[i] + k) * _tsSize, _tsSize);

    /// <summary>
    /// Index (into <see cref="GetSequences"/>) of the newest tombstone of fragment <paramref name="i"/> visible at
    /// <paramref name="readSequence"/> and, when non-empty, <paramref name="readTimestamp"/>; -1 if none. Port of
    /// FragmentedRangeTombstoneIterator::SetMaxVisibleSeqAndTimestamp.
    /// </summary>
    public int VisibleIndex(int i, ulong readSequence, ReadOnlySpan<byte> readTimestamp)
    {
        int begin = _seqStart[i], end = _seqStart[i + 1];
        int idx = begin;
        while (idx < end && _seqs[idx] > readSequence) idx++;
        if (_tsSize != 0 && !readTimestamp.IsEmpty)
        {
            int t = begin;
            while (t < end && _ucmp.CompareTimestamp(_timestamps.AsSpan(t * _tsSize, _tsSize), readTimestamp) > 0) t++;
            // seq and ts are ordered in non-increasing order: only move to a smaller sequence number and timestamp.
            if (t > idx) idx = t;
        }
        return idx < end ? idx - begin : -1;
    }

    /// <summary>Fragments overlapping tombstones (user comparator order). Empty ranges are dropped.</summary>
    public static FragmentedRangeTombstoneList Build(Comparator ucmp, List<Tombstone> tombstones) => Build(ucmp, tombstones, endIncludesTimestamp: true);

    /// <summary>
    /// <see cref="Build(Comparator, List{Tombstone})"/>; <paramref name="endIncludesTimestamp"/> = false when the end
    /// keys carry no timestamp (tables written with persist_user_defined_timestamps = false, upstream's
    /// tombstone_end_include_ts: the start keys were padded with the minimum timestamp by the reader, the end keys not).
    /// </summary>
    public static FragmentedRangeTombstoneList Build(Comparator ucmp, List<Tombstone> tombstones, bool endIncludesTimestamp)
    {
        int tsSize = ucmp.TimestampSize;
        if (tsSize == 0) return BuildWithoutTimestamps(ucmp, tombstones);

        // Work on the keys without timestamps; the timestamp travels with each tombstone.
        var items = new List<TsItem>(tombstones.Count);
        foreach (Tombstone t in tombstones)
        {
            if (t.Start.Length < tsSize || (endIncludesTimestamp && t.End.Length < tsSize)) ThrowHelper.Corruption("range tombstone key too short for its timestamp");
            byte[] ts = t.Timestamp ?? t.Start.AsSpan(t.Start.Length - tsSize).ToArray();
            ReadOnlyMemory<byte> s = t.Start.AsMemory(0, t.Start.Length - tsSize);
            ReadOnlyMemory<byte> e = endIncludesTimestamp ? t.End.AsMemory(0, t.End.Length - tsSize) : t.End;
            if (ucmp.CompareWithoutTimestamp(s.Span, false, e.Span, false) >= 0) continue;
            items.Add(new TsItem(s, e, t.Sequence, ts));
        }
        if (items.Count == 0) return Empty(ucmp);
        items.Sort((a, b) => ucmp.CompareWithoutTimestamp(a.Start.Span, false, b.Start.Span, false));

        var bounds = new List<ReadOnlyMemory<byte>>(items.Count * 2);
        foreach (TsItem t in items)
        {
            bounds.Add(t.Start);
            bounds.Add(t.End);
        }
        bounds.Sort((a, b) => ucmp.CompareWithoutTimestamp(a.Span, false, b.Span, false));
        int w = 1;
        for (int i = 1; i < bounds.Count; i++)
        {
            if (ucmp.CompareWithoutTimestamp(bounds[i].Span, false, bounds[w - 1].Span, false) != 0) bounds[w++] = bounds[i];
        }
        bounds.RemoveRange(w, bounds.Count - w);

        // Fragment boundaries carry the maximum timestamp (RangeTombstoneStack expects start_key and end_key to have it;
        // AppendUserKeyWithMaxTimestamp appends 0xff bytes).
        byte[] maxTs = new byte[tsSize];
        maxTs.AsSpan().Fill(0xff);

        var starts = new List<byte[]>();
        var ends = new List<byte[]>();
        var seqStart = new List<int> { 0 };
        var seqs = new List<ulong>();
        var tss = new List<byte[]>();
        var active = new List<TsItem>();
        var seqScratch = new List<ulong>();
        var tsScratch = new List<byte[]>();
        int nextT = 0;
        byte[]? prevEnd = null;
        for (int k = 0; k + 1 < bounds.Count; k++)
        {
            ReadOnlySpan<byte> lo = bounds[k].Span;
            while (nextT < items.Count && ucmp.CompareWithoutTimestamp(items[nextT].Start.Span, false, lo, false) <= 0) active.Add(items[nextT++]);
            for (int a = active.Count - 1; a >= 0; a--)
            {
                if (ucmp.CompareWithoutTimestamp(active[a].End.Span, false, lo, false) <= 0) active.RemoveAt(a);
            }
            if (active.Count == 0) continue;
            seqScratch.Clear();
            tsScratch.Clear();
            foreach (TsItem t in active)
            {
                seqScratch.Add(t.Sequence);
                tsScratch.Add(t.Timestamp);
            }
            // Both sorted newest first, independently (FragmentTombstones sorts seqnums_to_flush and timestamps_to_flush
            // separately and keeps every one of them when timestamps are enabled).
            seqScratch.Sort(static (a, b) => b.CompareTo(a));
            tsScratch.Sort((a, b) => ucmp.CompareTimestamp(b, a));
            // Adjacent fragments share the boundary buffer.
            byte[] startKey = prevEnd is not null && ucmp.CompareWithoutTimestamp(prevEnd.AsSpan(0, prevEnd.Length - tsSize), false, lo, false) == 0
                ? prevEnd
                : WithTimestamp(lo, maxTs);
            byte[] endKey = WithTimestamp(bounds[k + 1].Span, maxTs);
            starts.Add(startKey);
            ends.Add(endKey);
            prevEnd = endKey;
            for (int i = 0; i < seqScratch.Count; i++)
            {
                seqs.Add(seqScratch[i]);
                tss.Add(tsScratch[i]);
            }
            seqStart.Add(seqs.Count);
        }
        byte[] flat = new byte[tss.Count * tsSize];
        for (int i = 0; i < tss.Count; i++) tss[i].CopyTo(flat, i * tsSize);
        return new FragmentedRangeTombstoneList(ucmp, starts.ToArray(), ends.ToArray(), seqStart.ToArray(), seqs.ToArray(), flat);
    }

    private readonly record struct TsItem(ReadOnlyMemory<byte> Start, ReadOnlyMemory<byte> End, ulong Sequence, byte[] Timestamp);

    private static byte[] WithTimestamp(ReadOnlySpan<byte> key, ReadOnlySpan<byte> ts)
    {
        byte[] r = new byte[key.Length + ts.Length];
        key.CopyTo(r);
        ts.CopyTo(r.AsSpan(key.Length));
        return r;
    }

    private static FragmentedRangeTombstoneList BuildWithoutTimestamps(Comparator ucmp, List<Tombstone> tombstones)
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
        return new FragmentedRangeTombstoneList(ucmp, starts.ToArray(), ends.ToArray(), seqStart.ToArray(), seqs.ToArray(), []);
    }

    /// <summary>
    /// Index of the fragment covering <paramref name="userKey"/> (with a timestamp when timestamps are enabled), or -1.
    /// </summary>
    public int FindCovering(ReadOnlySpan<byte> userKey)
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
        if (found < 0 || _ucmp.Compare(userKey, _ends[found]) >= 0) return -1;
        return found;
    }

    /// <summary>
    /// Largest sequence number &lt;= <paramref name="readSequence"/> of a tombstone covering <paramref name="userKey"/>,
    /// or 0 if none. Port of FragmentedRangeTombstoneIterator::MaxCoveringTombstoneSeqnum.
    /// </summary>
    public ulong MaxCoveringTombstoneSeqnum(ReadOnlySpan<byte> userKey, ulong readSequence) =>
        MaxCoveringTombstoneSeqnum(userKey, readSequence, default, out _);

    /// <summary>
    /// <see cref="MaxCoveringTombstoneSeqnum(ReadOnlySpan{byte}, ulong)"/> with a timestamp upper bound
    /// (<paramref name="readTimestamp"/>, empty = none); <paramref name="tombstoneTimestamp"/> receives the timestamp of
    /// the tombstone found (empty without timestamps or when none is found).
    /// </summary>
    public ulong MaxCoveringTombstoneSeqnum(ReadOnlySpan<byte> userKey, ulong readSequence, ReadOnlySpan<byte> readTimestamp,
        out ReadOnlySpan<byte> tombstoneTimestamp)
    {
        tombstoneTimestamp = default;
        int f = FindCovering(userKey);
        if (f < 0) return 0;
        int k = VisibleIndex(f, readSequence, readTimestamp);
        if (k < 0) return 0;
        if (_tsSize != 0) tombstoneTimestamp = GetTimestamp(f, k);
        return GetSequences(f)[k];
    }
}
