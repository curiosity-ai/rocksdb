using System.Runtime.CompilerServices;
using RocksDb.Managed.Table;

namespace RocksDb.Managed.Db;

/// <summary>
/// Answers "is this key covered by a range tombstone?" across several fragmented tombstone lists (memtables and SST
/// files). Port of rocksdb::ReadRangeDelAggregator / CompactionRangeDelAggregator (db/range_del_aggregator.{h,cc})
/// together with TruncatedRangeDelIterator's file-boundary truncation.
/// </summary>
/// <remarks>
/// <para><b>Representation.</b> Each source is a <see cref="FragmentedRangeTombstoneList"/>: non-overlapping fragments
/// carrying the full, descending list of sequence numbers of the tombstones covering them. Because no sequence number
/// is dropped, a single list serves every snapshot; upstream's snapshot striping
/// (FragmentedRangeTombstoneList::SplitBySnapshot) is applied at query time instead:</para>
/// <list type="bullet">
/// <item>Read mode (<see cref="RangeDelAggregator(InternalKeyComparator, ulong)"/>): key (k, s) is deleted iff some
/// tombstone t covering k has <c>s &lt; t &lt;= readSequence</c> (ReadRangeDelAggregator with upper_bound).</item>
/// <item>Compaction mode (<see cref="ForCompaction"/>): key (k, s) is deleted iff some tombstone t covering k has
/// <c>s &lt; t &lt;= stripeUpper(s)</c>, where stripeUpper(s) is the smallest snapshot &gt;= s (or
/// kMaxSequenceNumber), i.e. t and s fall in the same snapshot stripe.</item>
/// </list>
/// <para><b>Truncation.</b> Tombstones read from an SST are truncated to the file's [smallest, largest] internal key
/// range exactly like TruncatedRangeDelIterator (smallest with type kTypeMaxValid; largest with seqno - 1 unless it is
/// a range-deletion sentinel or has seqno 0), so stale tombstone parts outside a file never hide newer data that was
/// compacted elsewhere (e.g. bottommost keys whose seqno was zeroed).</para>
/// <para><b>Positioning.</b> Upstream keeps forward/reverse heaps of tombstone iterators. Here each source caches the
/// fragment found by the previous query, so sequential traversal in either direction costs two or three user-key
/// comparisons per source; random access falls back to a binary search.</para>
/// <para>Not thread-safe (the cursors mutate); create one per iterator / compaction.</para>
/// </remarks>
public sealed class RangeDelAggregator
{
    /// <summary>kTypeMaxValid (db/dbformat.h).</summary>
    private const EntryType TypeMaxValid = EntryType.MaxValid;

    private struct Source
    {
        public FragmentedRangeTombstoneList List;
        public byte[]? Smallest; // inclusive internal key bound (already adjusted), or null
        public byte[]? Largest;  // exclusive internal key bound (already adjusted), or null
        public int Cursor;       // last fragment with start <= previous key (-1 = before the first)
    }

    private readonly InternalKeyComparator _icmp;
    private readonly ulong _readSequence;
    private readonly ulong[]? _snapshots;
    /// <summary>Timestamp upper bound of the visible tombstones (null = none): the read timestamp, or for compactions
    /// full_history_ts_low (CompactionRangeDelAggregator ts_upper_bound_).</summary>
    private readonly byte[]? _tsUpperBound;
    private Source[] _sources = new Source[4];
    private int _count;

    /// <summary>Read-path aggregator: tombstones with seqno &gt; <paramref name="readSequence"/> are invisible.</summary>
    public RangeDelAggregator(InternalKeyComparator comparator, ulong readSequence)
    {
        ArgumentNullException.ThrowIfNull(comparator);
        _icmp = comparator;
        _readSequence = readSequence;
    }

    /// <summary>
    /// Read-path aggregator with a user-defined timestamp upper bound (ReadOptions::timestamp): tombstones with a
    /// seqno &gt; <paramref name="readSequence"/> or a timestamp &gt; <paramref name="readTimestamp"/> are invisible.
    /// </summary>
    public RangeDelAggregator(InternalKeyComparator comparator, ulong readSequence, ReadOnlySpan<byte> readTimestamp)
        : this(comparator, readSequence)
    {
        if (!readTimestamp.IsEmpty && comparator.UserComparator.TimestampSize != 0) _tsUpperBound = readTimestamp.ToArray();
    }

    private RangeDelAggregator(InternalKeyComparator comparator, ulong[] snapshots, byte[]? tsUpperBound)
    {
        _icmp = comparator;
        _readSequence = DbFormat.MaxSequenceNumber;
        _snapshots = snapshots;
        _tsUpperBound = tsUpperBound;
    }

    /// <summary>
    /// Compaction aggregator: a tombstone only deletes keys in its own snapshot stripe.
    /// <paramref name="snapshots"/> are the live snapshot sequence numbers (any order).
    /// </summary>
    public static RangeDelAggregator ForCompaction(InternalKeyComparator comparator, ReadOnlySpan<ulong> snapshots)
    {
        ArgumentNullException.ThrowIfNull(comparator);
        ulong[] s = snapshots.ToArray();
        Array.Sort(s);
        return new RangeDelAggregator(comparator, s, null);
    }

    /// <summary>
    /// Compaction aggregator for a column family with user-defined timestamps: only tombstones whose timestamp is
    /// &lt;= <paramref name="fullHistoryTsLow"/> delete keys (null = no bound). Keys covered by newer tombstones must
    /// survive, a reader may use a timestamp between the tombstone and the key (CompactionRangeDelAggregator).
    /// </summary>
    public static RangeDelAggregator ForCompaction(InternalKeyComparator comparator, ReadOnlySpan<ulong> snapshots, byte[]? fullHistoryTsLow)
    {
        ArgumentNullException.ThrowIfNull(comparator);
        ulong[] s = snapshots.ToArray();
        Array.Sort(s);
        return new RangeDelAggregator(comparator, s, comparator.UserComparator.TimestampSize != 0 ? fullHistoryTsLow : null);
    }

    public InternalKeyComparator Comparator => _icmp;

    /// <summary>True if no source with at least one fragment was added.</summary>
    public bool IsEmpty => _count == 0;

    /// <summary>Number of non-empty sources.</summary>
    public int SourceCount => _count;

    /// <summary>Adds untruncated tombstones (e.g. a memtable's).</summary>
    public void AddTombstones(FragmentedRangeTombstoneList tombstones)
    {
        ArgumentNullException.ThrowIfNull(tombstones);
        if (tombstones.IsEmpty) return;
        Add(new Source { List = tombstones, Cursor = -1 });
    }

    /// <summary>
    /// Adds tombstones of an SST file, truncated to its boundaries (encoded internal keys from the file metadata).
    /// Port of TruncatedRangeDelIterator's constructor.
    /// </summary>
    public void AddTombstones(FragmentedRangeTombstoneList tombstones, ReadOnlySpan<byte> smallest, ReadOnlySpan<byte> largest)
    {
        ArgumentNullException.ThrowIfNull(tombstones);
        if (tombstones.IsEmpty) return;
        byte[]? lo = null, hi = null;
        if (smallest.Length >= DbFormat.InternalKeyFooterSize)
        {
            lo = smallest.ToArray();
            ulong seq = DbFormat.ExtractFooter(lo) >> 8;
            Util.Coding.EncodeFixed64(lo.AsSpan(lo.Length - DbFormat.InternalKeyFooterSize), DbFormat.PackSequenceAndType(seq, TypeMaxValid));
        }
        if (largest.Length >= DbFormat.InternalKeyFooterSize)
        {
            hi = largest.ToArray();
            DbFormat.UnpackSequenceAndType(DbFormat.ExtractFooter(hi), out ulong seq, out EntryType type);
            if (type == EntryType.RangeDeletion && seq == DbFormat.MaxSequenceNumber)
            {
                // The file boundary has been artificially extended by a range tombstone: no adjustment.
            }
            else if (seq == 0)
            {
                // No range tombstone in this file can cover largest (else the boundary would be extended).
            }
            else
            {
                // The same user key may straddle two sstable boundaries: reduce seqno by 1 so the truncated end key
                // covers largest; kTypeMaxValid keeps it from covering keys of the next file.
                Util.Coding.EncodeFixed64(hi.AsSpan(hi.Length - DbFormat.InternalKeyFooterSize), DbFormat.PackSequenceAndType(seq - 1, TypeMaxValid));
            }
        }
        Add(new Source { List = tombstones, Smallest = lo, Largest = hi, Cursor = -1 });
    }

    private void Add(in Source s)
    {
        if (_count == _sources.Length) Array.Resize(ref _sources, _count * 2);
        _sources[_count++] = s;
    }

    /// <summary>Removes all sources.</summary>
    public void Clear()
    {
        Array.Clear(_sources, 0, _count);
        _count = 0;
    }

    /// <summary>ShouldDelete for an encoded internal key.</summary>
    public bool ShouldDelete(ReadOnlySpan<byte> internalKey)
    {
        ulong footer = DbFormat.ExtractFooter(internalKey);
        return ShouldDelete(DbFormat.ExtractUserKey(internalKey), footer >> 8);
    }

    /// <summary>
    /// Port of RangeDelAggregator::ShouldDelete: true if the entry (<paramref name="userKey"/>,
    /// <paramref name="sequence"/>) is covered by a newer visible range tombstone.
    /// </summary>
    public bool ShouldDelete(ReadOnlySpan<byte> userKey, ulong sequence)
    {
        if (_count == 0) return false;
        ulong upper = _snapshots is null ? _readSequence : StripeUpper(sequence);
        if (sequence >= upper) return false;
        for (int i = 0; i < _count; i++)
        {
            ref Source s = ref _sources[i];
            if (!InBounds(ref s, userKey, sequence)) continue;
            int f = Locate(ref s, userKey);
            if (f < 0) continue;
            int k = s.List.VisibleIndex(f, upper, _tsUpperBound);
            if (k >= 0 && s.List.GetSequences(f)[k] > sequence) return true;
        }
        return false;
    }

    /// <summary>
    /// <see cref="ShouldDelete(ReadOnlySpan{byte}, ulong)"/> for the read path that also describes how far the covering
    /// tombstone reaches, so the caller can skip the covered range in a child iterator (the role of upstream's
    /// MergingIterator::SkipNextDeleted / SkipPrevDeleted reseeks). On true, <paramref name="tombstoneSeq"/> is the
    /// newest visible tombstone covering the entry and <paramref name="skipTarget"/> receives an internal key such that
    /// every entry strictly between the entry and the target (forward: up to, excluding, the target; backward: down to,
    /// excluding, the entries after the target, i.e. SeekForPrev semantics) with a seqno &lt; <paramref name="tombstoneSeq"/>
    /// is covered by that tombstone: the fragment end (start) clamped so that no user key at a truncated file boundary
    /// is crossed. Read mode only.
    /// </summary>
    public bool ShouldDeleteWithSkipTarget(ReadOnlySpan<byte> userKey, ulong sequence, bool forward, Util.PooledBufferWriter skipTarget,
        out ulong tombstoneSeq)
    {
        tombstoneSeq = 0;
        if (_count == 0 || _snapshots is not null) return false;
        ulong upper = _readSequence;
        if (sequence >= upper) return false;
        int best = -1, bestFragment = -1;
        for (int i = 0; i < _count; i++)
        {
            ref Source s = ref _sources[i];
            if (!InBounds(ref s, userKey, sequence)) continue;
            int f = Locate(ref s, userKey);
            if (f < 0) continue;
            int k = s.List.VisibleIndex(f, upper, _tsUpperBound);
            if (k < 0) continue;
            ulong t = s.List.GetSequences(f)[k];
            if (t > sequence && t > tombstoneSeq)
            {
                tombstoneSeq = t;
                best = i;
                bestFragment = f;
            }
        }
        if (best < 0) return false;
        ref Source src = ref _sources[best];
        ReadOnlySpan<byte> boundary;
        ulong seq;
        EntryType type;
        if (forward)
        {
            // Entries before (fragment end, kMaxSequenceNumber) are covered; with a truncated end, stop before the
            // first entry of the file's last user key (its older versions may lie outside the file's bounds).
            boundary = src.List.GetEnd(bestFragment);
            if (src.Largest is not null)
            {
                ReadOnlySpan<byte> lu = DbFormat.ExtractUserKey(src.Largest);
                if (_icmp.CompareUserKey(lu, boundary) < 0) boundary = lu;
            }
            seq = DbFormat.MaxSequenceNumber;
            type = DbFormat.ValueTypeForSeek;
        }
        else
        {
            boundary = src.List.GetStart(bestFragment);
            if (src.Smallest is not null && _icmp.CompareUserKey(DbFormat.ExtractUserKey(src.Smallest), boundary) >= 0)
            {
                // Truncated start: keep every version of the file's first user key (SeekForPrev to its oldest entry).
                boundary = DbFormat.ExtractUserKey(src.Smallest);
                seq = 0;
                type = EntryType.Deletion;
            }
            else
            {
                // SeekForPrev(fragment start, kMaxSequenceNumber): the last entry of a smaller user key.
                seq = DbFormat.MaxSequenceNumber;
                type = DbFormat.ValueTypeForSeek;
            }
        }
        skipTarget.Clear();
        Span<byte> dst = skipTarget.GetSpan(boundary.Length + DbFormat.InternalKeyFooterSize);
        skipTarget.Advance(DbFormat.AppendInternalKey(dst, boundary, seq, type));
        return true;
    }

    /// <summary>
    /// Largest seqno &lt;= the read sequence of a range tombstone covering <paramref name="userKey"/> (0 if none),
    /// across all sources (truncation applied at user-key granularity). Port of
    /// FragmentedRangeTombstoneIterator::MaxCoveringTombstoneSeqnum aggregated over the sources of a point lookup.
    /// </summary>
    public ulong MaxCoveringTombstoneSeqnum(ReadOnlySpan<byte> userKey)
    {
        ulong best = 0;
        for (int i = 0; i < _count; i++)
        {
            ref Source s = ref _sources[i];
            if (s.Smallest is not null && _icmp.CompareUserKey(userKey, DbFormat.ExtractUserKey(s.Smallest)) < 0) continue;
            if (s.Largest is not null)
            {
                int c = _icmp.CompareUserKey(userKey, DbFormat.ExtractUserKey(s.Largest));
                if (c > 0 || (c == 0 && DbFormat.ExtractFooter(s.Largest) >> 8 == DbFormat.MaxSequenceNumber)) continue;
            }
            int f = Locate(ref s, userKey);
            if (f < 0) continue;
            int k = s.List.VisibleIndex(f, _readSequence, _tsUpperBound);
            if (k >= 0 && s.List.GetSequences(f)[k] > best) best = s.List.GetSequences(f)[k];
        }
        return best;
    }

    /// <summary>
    /// Port of RangeDelAggregator::IsRangeOverlapped: true if a visible tombstone overlaps the user key range
    /// [<paramref name="start"/>, <paramref name="end"/>] (or [start, end) when <paramref name="endExclusive"/>).
    /// Truncation is applied at user-key granularity. Cold path (linear in the number of fragments).
    /// </summary>
    public bool IsRangeOverlapped(ReadOnlySpan<byte> start, ReadOnlySpan<byte> end, bool endExclusive)
    {
        for (int i = 0; i < _count; i++)
        {
            ref Source s = ref _sources[i];
            FragmentedRangeTombstoneList list = s.List;
            for (int f = 0; f < list.Count; f++)
            {
                if (list.VisibleIndex(f, _readSequence, _tsUpperBound) < 0) continue;
                ReadOnlySpan<byte> fs = list.GetStart(f), fe = list.GetEnd(f);
                if (s.Smallest is not null && _icmp.CompareUserKey(fs, DbFormat.ExtractUserKey(s.Smallest)) < 0) fs = DbFormat.ExtractUserKey(s.Smallest);
                bool feInclusive = false;
                if (s.Largest is not null)
                {
                    ReadOnlySpan<byte> lu = DbFormat.ExtractUserKey(s.Largest);
                    if (_icmp.CompareUserKey(lu, fe) < 0)
                    {
                        fe = lu;
                        feInclusive = DbFormat.ExtractFooter(s.Largest) >> 8 != DbFormat.MaxSequenceNumber;
                    }
                }
                int c1 = _icmp.CompareUserKey(start, fe);
                bool startBeforeEnd = feInclusive ? c1 <= 0 : c1 < 0;
                int c2 = _icmp.CompareUserKey(fs, end);
                bool fragStartsBeforeRangeEnd = endExclusive ? c2 < 0 : c2 <= 0;
                if (startBeforeEnd && fragStartsBeforeRangeEnd && _icmp.CompareUserKey(fs, fe) < (feInclusive ? 1 : 0)) return true;
            }
        }
        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool InBounds(ref Source s, ReadOnlySpan<byte> userKey, ulong sequence)
    {
        // key >= smallest' and key < largest' (internal key order); the key's type is irrelevant against
        // kTypeMaxValid / sentinel bounds, use kTypeValue.
        if (s.Smallest is not null && _icmp.Compare(s.Smallest, userKey, sequence, EntryType.Value) > 0) return false;
        if (s.Largest is not null && _icmp.Compare(s.Largest, userKey, sequence, EntryType.Value) <= 0) return false;
        return true;
    }

    /// <summary>Index of the fragment covering <paramref name="userKey"/>, or -1. Updates the source cursor.</summary>
    private int Locate(ref Source s, ReadOnlySpan<byte> userKey)
    {
        FragmentedRangeTombstoneList list = s.List;
        int n = list.Count;
        int c = s.Cursor;
        // Fast path: the previous fragment (or gap) still brackets the key, or a neighbour does.
        if (c < 0 || _icmp.CompareUserKey(list.GetStart(c), userKey) <= 0)
        {
            if (c + 1 >= n || _icmp.CompareUserKey(userKey, list.GetStart(c + 1)) < 0) goto Found;
            if (c + 2 >= n || _icmp.CompareUserKey(userKey, list.GetStart(c + 2)) < 0)
            {
                c++;
                goto Found;
            }
        }
        else if (c == 0 || _icmp.CompareUserKey(list.GetStart(c - 1), userKey) <= 0)
        {
            c--;
            goto Found;
        }
        // Binary search for the last fragment whose start <= userKey.
        {
            int lo = 0, hi = n - 1;
            c = -1;
            while (lo <= hi)
            {
                int mid = (lo + hi) >>> 1;
                if (_icmp.CompareUserKey(list.GetStart(mid), userKey) <= 0)
                {
                    c = mid;
                    lo = mid + 1;
                }
                else hi = mid - 1;
            }
        }
    Found:
        s.Cursor = c;
        if (c < 0 || _icmp.CompareUserKey(userKey, list.GetEnd(c)) >= 0) return -1;
        return c;
    }

    private ulong StripeUpper(ulong sequence)
    {
        ulong[] snaps = _snapshots!;
        int lo = 0, hi = snaps.Length;
        while (lo < hi)
        {
            int mid = (lo + hi) >>> 1;
            if (snaps[mid] >= sequence) hi = mid;
            else lo = mid + 1;
        }
        return lo < snaps.Length ? snaps[lo] : DbFormat.MaxSequenceNumber;
    }
}

/// <summary>
/// Loads and caches the fragmented range tombstones of an SST (its range-deletion meta block). Port of the
/// fragmented_range_dels cache of BlockBasedTable::Rep (table/block_based/block_based_table_reader.cc,
/// FragmentedRangeTombstoneList built in BlockBasedTable::Open).
/// </summary>
public static class RangeDelTableTombstones
{
    private static readonly ConditionalWeakTable<TableReader, FragmentedRangeTombstoneList> s_cache = new();

    /// <summary>The fragmented tombstones of <paramref name="reader"/>, or null if it has none. Cached per reader.</summary>
    public static FragmentedRangeTombstoneList? Get(TableReader reader)
    {
        ArgumentNullException.ThrowIfNull(reader);
        if (!reader.HasRangeTombstones) return null;
        if (s_cache.TryGetValue(reader, out FragmentedRangeTombstoneList? cached)) return cached;
        FragmentedRangeTombstoneList built = Build(reader);
        return s_cache.GetValue(reader, _ => built);
    }

    private static FragmentedRangeTombstoneList Build(TableReader reader)
    {
        var list = new List<FragmentedRangeTombstoneList.Tombstone>();
        using BlockIter? it = reader.NewRangeTombstoneIterator();
        if (it is not null)
        {
            for (it.SeekToFirst(); it.Valid; it.Next())
            {
                ReadOnlySpan<byte> k = it.Key;
                if (!DbFormat.TryParseInternalKey(k, out ParsedInternalKey p) || p.Type != EntryType.RangeDeletion)
                {
                    ThrowHelper.Corruption("invalid range tombstone key in range deletion block");
                }
                list.Add(new FragmentedRangeTombstoneList.Tombstone(k[..p.UserKeyLength].ToArray(), it.Value.ToArray(), p.Sequence));
            }
        }
        return FragmentedRangeTombstoneList.Build(reader.InternalComparator.UserComparator, list, endIncludesTimestamp: reader.PadTimestampSize == 0);
    }
}
