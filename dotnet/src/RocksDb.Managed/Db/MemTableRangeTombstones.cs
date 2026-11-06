using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace RocksDb.Managed.Db;

/// <summary>
/// Immutable list of non-overlapping range tombstone fragments, each covering <c>[start, end)</c> with the
/// descending list of sequence numbers of the tombstones that cover it. Port of rocksdb::FragmentedRangeTombstoneList
/// and the lookup side of FragmentedRangeTombstoneIterator (db/range_tombstone_fragmenter.{h,cc}); snapshot striping is
/// applied at query time by <see cref="RangeDelAggregator"/>.
/// </summary>
/// <remarks>
/// <para>Built rarely (once per change of the memtable's range-deletion set, per flush, per table reader and per
/// compaction) by <see cref="RangeTombstoneFragmenter"/> and then shared by readers. A list owns a fixed number of
/// arrays (all fragment boundaries live in one key buffer), independent of the number of fragments.</para>
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
    private static readonly int[] s_zero = [0];

    // Every distinct fragment boundary is stored once, in order: boundary j is _keys[_boundOffsets[j].._boundOffsets[j + 1]].
    // Fragment i covers [boundary _fragBound[i], boundary _fragBound[i] + 1), so adjacent fragments share their common
    // boundary.
    private readonly byte[] _keys;
    private readonly int[] _boundOffsets;
    private readonly int[] _fragBound;
    private readonly int[] _seqStart; // fragment i's seqnums are _seqs[_seqStart[i].._seqStart[i+1]]
    private readonly ulong[] _seqs;
    private readonly byte[] _timestamps; // _seqs.Length * TimestampSize bytes, parallel to _seqs
    private readonly Comparator _ucmp;
    private readonly int _tsSize;

    public static FragmentedRangeTombstoneList Empty(Comparator ucmp) => new(ucmp, [], s_zero, [], s_zero, [], []);

    internal FragmentedRangeTombstoneList(Comparator ucmp, byte[] keys, int[] boundOffsets, int[] fragBound, int[] seqStart,
        ulong[] seqs, byte[] timestamps)
    {
        _ucmp = ucmp;
        _tsSize = ucmp.TimestampSize;
        _keys = keys;
        _boundOffsets = boundOffsets;
        _fragBound = fragBound;
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

    public int Count => _fragBound.Length;
    public bool IsEmpty => _fragBound.Length == 0;
    /// <summary>Number of (fragment, covering sequence number) pairs, i.e. the tombstones a writer of this list emits.</summary>
    public int SequenceCount => _seqs.Length;
    /// <summary>User-defined timestamp size of the comparator (0 = none).</summary>
    public int TimestampSize => _tsSize;

    /// <summary>Start of fragment <paramref name="i"/> (with the maximum timestamp when timestamps are enabled).</summary>
    public ReadOnlySpan<byte> GetStart(int i) => Bound(_fragBound[i]);

    /// <summary>Exclusive end of fragment <paramref name="i"/> (with the maximum timestamp when timestamps are enabled).</summary>
    public ReadOnlySpan<byte> GetEnd(int i) => Bound(_fragBound[i] + 1);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ReadOnlySpan<byte> Bound(int j)
    {
        int off = _boundOffsets[j];
        return _keys.AsSpan(off, _boundOffsets[j + 1] - off);
    }

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
    /// The input list is not modified.
    /// </summary>
    public static FragmentedRangeTombstoneList Build(Comparator ucmp, List<Tombstone> tombstones, bool endIncludesTimestamp)
    {
        ArgumentNullException.ThrowIfNull(ucmp);
        ArgumentNullException.ThrowIfNull(tombstones);
        RangeTombstoneFragmenter f = RangeTombstoneFragmenter.Rent(ucmp, endIncludesTimestamp);
        try
        {
            foreach (Tombstone t in CollectionsMarshal.AsSpan(tombstones)) f.Add(t.Start, t.End, t.Sequence, t.Timestamp);
            return f.Finish();
        }
        finally
        {
            RangeTombstoneFragmenter.Return(f);
        }
    }

    /// <summary>
    /// Index of the fragment covering <paramref name="userKey"/> (with a timestamp when timestamps are enabled), or -1.
    /// </summary>
    public int FindCovering(ReadOnlySpan<byte> userKey)
    {
        // Binary search for the last fragment whose start <= userKey.
        int lo = 0, hi = _fragBound.Length - 1, found = -1;
        while (lo <= hi)
        {
            int mid = (lo + hi) >>> 1;
            if (_ucmp.Compare(GetStart(mid), userKey) <= 0)
            {
                found = mid;
                lo = mid + 1;
            }
            else hi = mid - 1;
        }
        if (found < 0 || _ucmp.Compare(userKey, GetEnd(found)) >= 0) return -1;
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

/// <summary>
/// Builds a <see cref="FragmentedRangeTombstoneList"/> from unfragmented tombstones given as spans. Port of
/// FragmentedRangeTombstoneList::FragmentTombstones (db/range_tombstone_fragmenter.cc), sweeping the sorted distinct
/// boundaries with the set of active tombstones.
/// </summary>
/// <remarks>
/// <para>Input keys are copied into one pooled buffer and all scratch (entries, boundaries, the active set, sequence
/// lists) is rented from <see cref="ArrayPool{T}.Shared"/>, so a build allocates only the arrays of the resulting list.
/// Use <see cref="Rent"/> / <see cref="Return"/> (a per-thread cached instance); an instance is used by one thread at a
/// time and must not be used after <see cref="Return"/>.</para>
/// </remarks>
internal sealed class RangeTombstoneFragmenter
{
    [ThreadStatic] private static RangeTombstoneFragmenter? t_cached;

    private struct Entry
    {
        public int Start, StartLen, End, EndLen, Ts;
        public ulong Seq;
    }

    private readonly struct KeyRef(int off, int len)
    {
        public readonly int Off = off, Len = len;
    }

    private byte[] _buf = [];
    private int _bufLen;
    private Entry[] _entries = [];
    private int _count;
    private Comparator _ucmp = null!;
    private int _tsSize;
    private bool _endIncludesTs;
    private bool _rented;

    private RangeTombstoneFragmenter() { }

    /// <summary>
    /// A cleared fragmenter for <paramref name="ucmp"/>; <paramref name="endIncludesTimestamp"/> as in
    /// <see cref="FragmentedRangeTombstoneList.Build(Comparator, List{FragmentedRangeTombstoneList.Tombstone}, bool)"/>.
    /// </summary>
    public static RangeTombstoneFragmenter Rent(Comparator ucmp, bool endIncludesTimestamp)
    {
        RangeTombstoneFragmenter? f = t_cached;
        if (f is null) f = new RangeTombstoneFragmenter();
        else t_cached = null;
        f._ucmp = ucmp;
        f._tsSize = ucmp.TimestampSize;
        f._endIncludesTs = endIncludesTimestamp;
        f._count = 0;
        f._bufLen = 0;
        f._rented = true;
        return f;
    }

    /// <summary>Returns the scratch buffers to the pools and caches <paramref name="f"/> for the calling thread.</summary>
    public static void Return(RangeTombstoneFragmenter f)
    {
        if (!f._rented) throw new InvalidOperationException("RangeTombstoneFragmenter returned twice.");
        f._rented = false;
        if (f._buf.Length != 0) ArrayPool<byte>.Shared.Return(f._buf);
        if (f._entries.Length != 0) ArrayPool<Entry>.Shared.Return(f._entries);
        f._buf = [];
        f._entries = [];
        f._count = 0;
        f._bufLen = 0;
        f._ucmp = null!;
        t_cached = f;
    }

    /// <summary>Number of non-empty tombstones added.</summary>
    public int Count => _count;

    /// <summary>Adds the tombstone <c>[start, end)</c> at <paramref name="seq"/> (timestamp: the one of <paramref name="start"/>).</summary>
    public void Add(ReadOnlySpan<byte> start, ReadOnlySpan<byte> end, ulong seq) => Add(start, end, seq, default);

    /// <summary>
    /// Adds the tombstone <c>[start, end)</c> at <paramref name="seq"/>. With user-defined timestamps the keys carry a
    /// timestamp (only the key part is used; <paramref name="end"/> has none when the fragmenter was rented with
    /// endIncludesTimestamp = false) and the tombstone's timestamp is <paramref name="timestamp"/>, or the one of
    /// <paramref name="start"/> when empty. Empty ranges are dropped.
    /// </summary>
    public void Add(ReadOnlySpan<byte> start, ReadOnlySpan<byte> end, ulong seq, ReadOnlySpan<byte> timestamp)
    {
        if (!_rented) throw new ObjectDisposedException(nameof(RangeTombstoneFragmenter));
        int ts = _tsSize;
        if (ts == 0)
        {
            if (_ucmp.Compare(start, end) >= 0) return;
            Append(start, end, default, seq);
            return;
        }
        if (start.Length < ts || (_endIncludesTs && end.Length < ts)) ThrowHelper.Corruption("range tombstone key too short for its timestamp");
        if (timestamp.IsEmpty) timestamp = start[^ts..];
        else if (timestamp.Length != ts) ThrowHelper.InvalidArgument("range tombstone timestamp size mismatch");
        start = start[..^ts];
        if (_endIncludesTs) end = end[..^ts];
        if (_ucmp.CompareWithoutTimestamp(start, false, end, false) >= 0) return;
        Append(start, end, timestamp, seq);
    }

    private void Append(ReadOnlySpan<byte> start, ReadOnlySpan<byte> end, ReadOnlySpan<byte> ts, ulong seq)
    {
        int need = start.Length + end.Length + ts.Length;
        if (_buf.Length - _bufLen < need) GrowBuffer(need);
        if (_count == _entries.Length) GrowEntries();
        ref Entry e = ref _entries[_count++];
        e.Seq = seq;
        e.Start = _bufLen;
        e.StartLen = start.Length;
        start.CopyTo(_buf.AsSpan(_bufLen));
        _bufLen += start.Length;
        e.End = _bufLen;
        e.EndLen = end.Length;
        end.CopyTo(_buf.AsSpan(_bufLen));
        _bufLen += end.Length;
        e.Ts = _bufLen;
        ts.CopyTo(_buf.AsSpan(_bufLen));
        _bufLen += ts.Length;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void GrowBuffer(int need)
    {
        int size = Math.Max(_bufLen + need, Math.Max(_buf.Length * 2, 4096));
        if ((uint)size > (uint)Array.MaxLength) size = Math.Max(_bufLen + need, Array.MaxLength);
        byte[] n = ArrayPool<byte>.Shared.Rent(size);
        _buf.AsSpan(0, _bufLen).CopyTo(n);
        if (_buf.Length != 0) ArrayPool<byte>.Shared.Return(_buf);
        _buf = n;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void GrowEntries()
    {
        Entry[] n = ArrayPool<Entry>.Shared.Rent(Math.Max(_entries.Length * 2, 64));
        _entries.AsSpan(0, _count).CopyTo(n);
        if (_entries.Length != 0) ArrayPool<Entry>.Shared.Return(_entries);
        _entries = n;
    }

    private readonly struct StartComparer(byte[] buf, Comparator ucmp, bool ts) : IComparer<Entry>
    {
        public int Compare(Entry a, Entry b)
        {
            ReadOnlySpan<byte> x = buf.AsSpan(a.Start, a.StartLen), y = buf.AsSpan(b.Start, b.StartLen);
            return ts ? ucmp.CompareWithoutTimestamp(x, false, y, false) : ucmp.Compare(x, y);
        }
    }

    private readonly struct KeyComparer(byte[] buf, Comparator ucmp, bool ts) : IComparer<KeyRef>
    {
        public int Compare(KeyRef a, KeyRef b)
        {
            ReadOnlySpan<byte> x = buf.AsSpan(a.Off, a.Len), y = buf.AsSpan(b.Off, b.Len);
            return ts ? ucmp.CompareWithoutTimestamp(x, false, y, false) : ucmp.Compare(x, y);
        }
    }

    /// <summary>
    /// Newest timestamp first (offsets into the key buffer). A class, created once per <see cref="Finish"/>: the span
    /// sort wraps a comparer in a new delegate on every call; the Comparison made once per Finish is used directly.
    /// </summary>
    private sealed class TimestampDescComparer(byte[] buf, Comparator ucmp, int tsSize) : IComparer<int>
    {
        public int Compare(int a, int b) => ucmp.CompareTimestamp(buf.AsSpan(b, tsSize), buf.AsSpan(a, tsSize));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int CompareKeys(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) =>
        _tsSize != 0 ? _ucmp.CompareWithoutTimestamp(a, false, b, false) : _ucmp.Compare(a, b);

    /// <summary>Fragments the tombstones added since <see cref="Rent"/>. The fragmenter must still be returned.</summary>
    public FragmentedRangeTombstoneList Finish()
    {
        if (!_rented) throw new ObjectDisposedException(nameof(RangeTombstoneFragmenter));
        int n = _count;
        if (n == 0) return FragmentedRangeTombstoneList.Empty(_ucmp);
        int tsSize = _tsSize;
        bool hasTs = tsSize != 0;
        byte[] buf = _buf;
        Span<Entry> entries = _entries.AsSpan(0, n);
        entries.Sort(new StartComparer(buf, _ucmp, hasTs));

        KeyRef[] bounds = ArrayPool<KeyRef>.Shared.Rent(2 * n);
        int[] active = ArrayPool<int>.Shared.Rent(n);
        ulong[] seqScratch = ArrayPool<ulong>.Shared.Rent(n);
        int[] tsScratch = hasTs ? ArrayPool<int>.Shared.Rent(n) : [];
        int[] fragScratch = ArrayPool<int>.Shared.Rent(2 * n);
        int[] seqStartScratch = ArrayPool<int>.Shared.Rent(2 * n + 1);
        ulong[] seqOut = ArrayPool<ulong>.Shared.Rent(Math.Max(n, 16));
        int[] tsOut = hasTs ? ArrayPool<int>.Shared.Rent(Math.Max(n, 16)) : [];
        Comparison<int>? tsComparison = hasTs ? new TimestampDescComparer(buf, _ucmp, tsSize).Compare : null;
        try
        {
            // All distinct boundaries, sorted.
            for (int i = 0; i < n; i++)
            {
                ref Entry e = ref entries[i];
                bounds[2 * i] = new KeyRef(e.Start, e.StartLen);
                bounds[2 * i + 1] = new KeyRef(e.End, e.EndLen);
            }
            Span<KeyRef> b = bounds.AsSpan(0, 2 * n);
            b.Sort(new KeyComparer(buf, _ucmp, hasTs));
            int nb = 1;
            for (int i = 1; i < b.Length; i++)
            {
                if (CompareKeys(buf.AsSpan(b[i].Off, b[i].Len), buf.AsSpan(b[nb - 1].Off, b[nb - 1].Len)) != 0) b[nb++] = b[i];
            }

            int nf = 0, ns = 0, na = 0, nextT = 0;
            seqStartScratch[0] = 0;
            for (int k = 0; k + 1 < nb; k++)
            {
                ReadOnlySpan<byte> lo = buf.AsSpan(b[k].Off, b[k].Len);
                while (nextT < n && CompareKeys(buf.AsSpan(entries[nextT].Start, entries[nextT].StartLen), lo) <= 0) active[na++] = nextT++;
                // Drop the tombstones that end at or before lo (in place; the order of the active set does not matter).
                int w = 0;
                for (int a = 0; a < na; a++)
                {
                    int t = active[a];
                    if (CompareKeys(buf.AsSpan(entries[t].End, entries[t].EndLen), lo) > 0) active[w++] = t;
                }
                na = w;
                if (na == 0) continue;
                for (int a = 0; a < na; a++) seqScratch[a] = entries[active[a]].Seq;
                Span<ulong> seqs = seqScratch.AsSpan(0, na);
                seqs.Sort();
                if (seqOut.Length - ns < na) Grow(ref seqOut, ns, ns + na);
                if (!hasTs)
                {
                    // Descending, without duplicates.
                    ulong prev = ulong.MaxValue;
                    bool first = true;
                    for (int i = na - 1; i >= 0; i--)
                    {
                        if (first || seqs[i] != prev) seqOut[ns++] = seqs[i];
                        prev = seqs[i];
                        first = false;
                    }
                }
                else
                {
                    // Both sorted newest first, independently (FragmentTombstones sorts seqnums_to_flush and
                    // timestamps_to_flush separately and keeps every one of them when timestamps are enabled).
                    for (int a = 0; a < na; a++) tsScratch[a] = entries[active[a]].Ts;
                    tsScratch.AsSpan(0, na).Sort(tsComparison!);
                    if (tsOut.Length - ns < na) Grow(ref tsOut, ns, ns + na);
                    for (int i = 0; i < na; i++)
                    {
                        seqOut[ns] = seqs[na - 1 - i];
                        tsOut[ns] = tsScratch[i];
                        ns++;
                    }
                }
                fragScratch[nf++] = k;
                seqStartScratch[nf] = ns;
            }

            // Every distinct boundary is the start or the end of some fragment. With timestamps the boundaries carry the
            // maximum timestamp (RangeTombstoneStack expects start_key and end_key to have it; AppendUserKeyWithMaxTimestamp).
            long total = 0;
            for (int j = 0; j < nb; j++) total += b[j].Len + tsSize;
            if (total > Array.MaxLength) ThrowHelper.InvalidArgument("range tombstone keys too large");
            byte[] keys = total == 0 ? [] : GC.AllocateUninitializedArray<byte>((int)total);
            int[] boundOffsets = new int[nb + 1];
            int pos = 0;
            for (int j = 0; j < nb; j++)
            {
                boundOffsets[j] = pos;
                buf.AsSpan(b[j].Off, b[j].Len).CopyTo(keys.AsSpan(pos));
                pos += b[j].Len;
                if (hasTs)
                {
                    keys.AsSpan(pos, tsSize).Fill(0xff);
                    pos += tsSize;
                }
            }
            boundOffsets[nb] = pos;
            byte[] timestamps = [];
            if (hasTs && ns > 0)
            {
                timestamps = GC.AllocateUninitializedArray<byte>(ns * tsSize);
                for (int i = 0; i < ns; i++) buf.AsSpan(tsOut[i], tsSize).CopyTo(timestamps.AsSpan(i * tsSize));
            }
            return new FragmentedRangeTombstoneList(_ucmp, keys, boundOffsets, fragScratch.AsSpan(0, nf).ToArray(),
                seqStartScratch.AsSpan(0, nf + 1).ToArray(), seqOut.AsSpan(0, ns).ToArray(), timestamps);
        }
        finally
        {
            ArrayPool<KeyRef>.Shared.Return(bounds);
            ArrayPool<int>.Shared.Return(active);
            ArrayPool<ulong>.Shared.Return(seqScratch);
            if (hasTs) ArrayPool<int>.Shared.Return(tsScratch);
            ArrayPool<int>.Shared.Return(fragScratch);
            ArrayPool<int>.Shared.Return(seqStartScratch);
            ArrayPool<ulong>.Shared.Return(seqOut);
            if (hasTs) ArrayPool<int>.Shared.Return(tsOut);
        }
    }

    private static void Grow<T>(ref T[] array, int used, int needed)
    {
        T[] n = ArrayPool<T>.Shared.Rent(Math.Max(needed, array.Length * 2));
        array.AsSpan(0, used).CopyTo(n);
        ArrayPool<T>.Shared.Return(array);
        array = n;
    }
}
