using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using RocksDb.Managed.Db.Iterators;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Db;

/// <summary>
/// Reusable list of byte strings (merge operands or internal keys) that can grow at both ends, exposing them oldest
/// first as a <see cref="MergeOperands"/> view without copying. Port of the operand list of rocksdb::MergeContext
/// (db/merge_context.h): forward scans (DBIter::MergeValuesNewToOld, MergeHelper::MergeUntil) meet operands newest
/// first and <see cref="PushOlder"/> them; reverse scans (DBIter::FindValueForCurrentKey) meet them oldest first and
/// <see cref="PushNewer"/> them.
/// </summary>
/// <remarks>
/// Storage is one pooled byte array plus one pooled array of boundaries, both used as a double-ended region; nothing
/// is allocated in steady state. Not thread-safe. Index 0 is always the oldest entry.
/// </remarks>
public sealed class MergeOperandBuffer : IDisposable
{
    private byte[] _data;
    private int[] _bounds;   // _bounds[_bStart.._bStart+_count+1) are absolute positions in _data (ascending)
    private int _bStart;
    private int _count;

    public MergeOperandBuffer() : this(256, 16) { }

    public MergeOperandBuffer(int initialBytes, int initialCount)
    {
        _data = ArrayPool<byte>.Shared.Rent(Math.Max(16, initialBytes));
        _bounds = ArrayPool<int>.Shared.Rent(Math.Max(4, initialCount + 1));
    }

    /// <summary>Number of entries.</summary>
    public int Count => _count;

    public void Clear() => _count = 0;

    /// <summary>Entry by age: index 0 is the oldest.</summary>
    public ReadOnlySpan<byte> this[int index]
    {
        get
        {
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual((uint)index, (uint)_count);
            int b = _bStart + index;
            return _data.AsSpan(_bounds[b], _bounds[b + 1] - _bounds[b]);
        }
    }

    /// <summary>Entry by recency: index 0 is the newest.</summary>
    public ReadOnlySpan<byte> GetNewestFirst(int index) => this[_count - 1 - index];

    /// <summary>The entries, oldest first, as a merge operand view (valid until the next mutation).</summary>
    public MergeOperands AsOperands() =>
        _count == 0 ? default : new MergeOperands(_data, _bounds.AsSpan(_bStart, _count + 1));

    /// <summary>Adds an entry older than all present ones.</summary>
    public void PushOlder(ReadOnlySpan<byte> value)
    {
        if (_count == 0)
        {
            // Lay out from the end so further PushOlder calls grow towards the front.
            EnsureEmptyCapacity(value.Length);
            int end = _data.Length;
            int start = end - value.Length;
            value.CopyTo(_data.AsSpan(start));
            _bStart = _bounds.Length - 2;
            _bounds[_bStart] = start;
            _bounds[_bStart + 1] = end;
            _count = 1;
            return;
        }
        int dataStart = _bounds[_bStart];
        if (dataStart < value.Length || _bStart == 0) Grow(value.Length, front: true);
        dataStart = _bounds[_bStart];
        int ns = dataStart - value.Length;
        value.CopyTo(_data.AsSpan(ns));
        _bounds[--_bStart] = ns;
        _count++;
    }

    /// <summary>Adds an entry newer than all present ones.</summary>
    public void PushNewer(ReadOnlySpan<byte> value)
    {
        if (_count == 0)
        {
            EnsureEmptyCapacity(value.Length);
            value.CopyTo(_data);
            _bStart = 0;
            _bounds[0] = 0;
            _bounds[1] = value.Length;
            _count = 1;
            return;
        }
        int bEnd = _bStart + _count; // index of the last boundary
        int dataEnd = _bounds[bEnd];
        if (_data.Length - dataEnd < value.Length || bEnd + 1 >= _bounds.Length) Grow(value.Length, front: false);
        bEnd = _bStart + _count;
        dataEnd = _bounds[bEnd];
        value.CopyTo(_data.AsSpan(dataEnd));
        _bounds[bEnd + 1] = dataEnd + value.Length;
        _count++;
    }

    /// <summary>Replaces all entries with <paramref name="value"/>.</summary>
    public void Reset(ReadOnlySpan<byte> value)
    {
        _count = 0;
        PushNewer(value);
    }

    private void EnsureEmptyCapacity(int bytes)
    {
        if (_data.Length < bytes)
        {
            ArrayPool<byte>.Shared.Return(_data);
            _data = ArrayPool<byte>.Shared.Rent(Math.Max(bytes, _data.Length * 2));
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void Grow(int extraBytes, bool front)
    {
        int dataStart = _bounds[_bStart];
        int dataEnd = _bounds[_bStart + _count];
        int used = dataEnd - dataStart;
        // Re-center in place when there is enough total room (e.g. after the other end was used); grow otherwise.
        byte[] nd = _data.Length - used >= extraBytes + 16 ? _data : ArrayPool<byte>.Shared.Rent(Math.Max(_data.Length * 2, used + extraBytes + 16));
        int[] nb = _bounds.Length >= _count + 4 ? _bounds : ArrayPool<int>.Shared.Rent(Math.Max(_bounds.Length * 2, _count + 4));
        // Leave at least the requested room on the growing side and split the rest, so alternating directions
        // cannot trigger repeated growth.
        int freeData = nd.Length - used;
        int newDataStart = front ? Math.Max(extraBytes, freeData / 2) : Math.Min(freeData - extraBytes, freeData / 2);
        int freeBounds = nb.Length - (_count + 1);
        int newBStart = front ? Math.Max(1, freeBounds / 2) : Math.Min(freeBounds - 1, freeBounds / 2);
        _data.AsSpan(dataStart, used).CopyTo(nd.AsSpan(newDataStart)); // memmove semantics when in place
        int delta = newDataStart - dataStart;
        if (nb == _bounds)
        {
            _bounds.AsSpan(_bStart, _count + 1).CopyTo(nb.AsSpan(newBStart));
            for (int i = 0; i <= _count; i++) nb[newBStart + i] += delta;
        }
        else
        {
            for (int i = 0; i <= _count; i++) nb[newBStart + i] = _bounds[_bStart + i] + delta;
            ArrayPool<int>.Shared.Return(_bounds);
        }
        if (nd != _data) ArrayPool<byte>.Shared.Return(_data);
        _data = nd;
        _bounds = nb;
        _bStart = newBStart;
    }

    public void Dispose()
    {
        _count = 0;
        byte[] d = _data;
        int[] b = _bounds;
        _data = [];
        _bounds = [];
        if (d.Length > 0) ArrayPool<byte>.Shared.Return(d);
        if (b.Length > 0) ArrayPool<int>.Shared.Return(b);
    }
}

/// <summary>Outcome of <see cref="MergeHelper.MergeUntil"/> (the OK / MergeInProgress statuses of upstream).</summary>
public enum MergeUntilResult : byte
{
    /// <summary>
    /// Either the operands were fully merged (one output entry of type Value), or the iterator was not on a merge
    /// chain worth processing (no output: the caller writes the current entry), or a compaction filter asked to skip.
    /// </summary>
    Ok = 0,
    /// <summary>The beginning of the key's history was not seen: outputs are (possibly partially merged) operands.</summary>
    MergeInProgress = 1,
}

/// <summary>
/// Merge resolution. Port of rocksdb::MergeHelper (db/merge_helper.{h,cc}): <see cref="TimedFullMerge"/> (used by
/// DBIter and point lookups) and <see cref="MergeUntil"/> (used by compaction/flush).
/// </summary>
/// <remarks>
/// Statistics, timers, blob base values and snapshot checkers are not ported (user-defined timestamps: see
/// <see cref="FullHistoryTsLow"/>).
/// A merge operator failure throws <see cref="RocksDbException"/> (Corruption / MergeOperatorFailed). An instance
/// is not thread-safe (it owns reusable buffers); the static helpers are.
/// </remarks>
public sealed class MergeHelper : IDisposable
{
    [ThreadStatic] private static MergeOperandBuffer? t_contextScratch;

    private readonly InternalKeyComparator _icmp;
    private readonly MergeOperator _mergeOperator;
    private readonly CompactionFilter? _compactionFilter;
    private readonly ulong _latestSnapshot;
    private readonly int _level;
    private readonly MergeOperandBuffer _keys = new();
    private readonly MergeOperandBuffer _operands = new();
    private readonly PooledBufferWriter _mergeResult = new(256);
    private readonly PooledBufferWriter _filterValue = new(64);
    private readonly PooledBufferWriter _skipUntilUser = new(64);
    private readonly IterKey _origKey = new();
    private readonly IterKey _skipUntil = new();

    /// <param name="comparator">Internal key comparator of the column family.</param>
    /// <param name="mergeOperator">The column family's merge operator.</param>
    /// <param name="compactionFilter">Compaction filter applied to merge operands newer than <paramref name="latestSnapshot"/>, or null.</param>
    /// <param name="latestSnapshot">Operands with seqno &lt;= this are always kept (visible to a snapshot).</param>
    /// <param name="level">Level passed to the compaction filter.</param>
    public MergeHelper(InternalKeyComparator comparator, MergeOperator mergeOperator, CompactionFilter? compactionFilter, ulong latestSnapshot, int level)
    {
        ArgumentNullException.ThrowIfNull(comparator);
        ArgumentNullException.ThrowIfNull(mergeOperator);
        _icmp = comparator;
        _mergeOperator = mergeOperator;
        _compactionFilter = compactionFilter;
        _latestSnapshot = latestSnapshot;
        _level = level;
    }

    /// <summary>Resolves blob base values in <see cref="MergeUntil"/> (the compaction's BlobFetcher); null = NotSupported.</summary>
    internal Blob.CompactionBlobProcessor? BlobProcessor { get; set; }

    // ------------------------------------------------------------------ TimedFullMerge

    /// <summary>
    /// Port of MergeHelper::TimedFullMerge (plain / no base value). Writes the merge result to
    /// <paramref name="result"/>; throws Corruption (subcode MergeOperatorFailed) if the operator fails.
    /// </summary>
    public static void TimedFullMerge(MergeOperator mergeOperator, ReadOnlySpan<byte> userKey, bool hasBaseValue, ReadOnlySpan<byte> baseValue,
        in MergeOperands operands, IBufferWriter<byte> result)
    {
        if (!mergeOperator.FullMerge(userKey, hasBaseValue, baseValue, operands, result)) ThrowMergeFailed();
    }

    /// <summary>
    /// <see cref="TimedFullMerge(MergeOperator, ReadOnlySpan{byte}, bool, ReadOnlySpan{byte}, in MergeOperands, IBufferWriter{byte})"/>
    /// over the operands collected by a point lookup in a <see cref="MergeContext"/> (newest first).
    /// </summary>
    public static void TimedFullMerge(MergeOperator mergeOperator, ReadOnlySpan<byte> userKey, bool hasBaseValue, ReadOnlySpan<byte> baseValue,
        MergeContext context, IBufferWriter<byte> result)
    {
        MergeOperandBuffer buf = t_contextScratch ??= new MergeOperandBuffer();
        buf.Clear();
        for (int i = 0; i < context.Count; i++) buf.PushOlder(context.GetOperandNewestFirst(i));
        try
        {
            TimedFullMerge(mergeOperator, userKey, hasBaseValue, baseValue, buf.AsOperands(), result);
        }
        finally
        {
            buf.Clear();
        }
    }

    [System.Diagnostics.CodeAnalysis.DoesNotReturn]
    internal static void ThrowMergeFailed() =>
        throw new RocksDbException(StatusCode.Corruption, "Corruption: Merge operator failed", StatusSubCode.MergeOperatorFailed);

    // ------------------------------------------------------------------ MergeUntil (compaction)

    /// <summary>
    /// assert_valid_internal_key: when true (default, as in compaction) a corrupted key throws Corruption; when false
    /// MergeUntil stops at it (upstream flush/test behaviour).
    /// </summary>
    public bool AssertValidInternalKey { get; set; } = true;

    /// <summary>
    /// full_history_ts_low of the compaction (null when unset or without user-defined timestamps): versions of a key
    /// with different timestamps are only merged together below it.
    /// </summary>
    internal byte[]? FullHistoryTsLow { get; set; }

    /// <summary>Number of output entries after <see cref="MergeUntil"/>.</summary>
    public int OutputCount => _keys.Count;

    /// <summary>Output internal key, newest first (MergeOutputIterator order).</summary>
    public ReadOnlySpan<byte> GetOutputKey(int index) => _keys.GetNewestFirst(index);

    /// <summary>Output value (merge result or operand), newest first.</summary>
    public ReadOnlySpan<byte> GetOutputValue(int index) => _operands.GetNewestFirst(index);

    /// <summary>True when the compaction filter returned RemoveAndSkipUntil during the last <see cref="MergeUntil"/>.</summary>
    public bool HasCompactionFilterSkipUntil { get; private set; }

    /// <summary>The internal key to skip to (user key + kMaxSequenceNumber/kValueTypeForSeek). REQUIRES: <see cref="HasCompactionFilterSkipUntil"/>.</summary>
    public ReadOnlySpan<byte> CompactionFilterSkipUntil => _skipUntil.Key;

    /// <summary>
    /// Port of MergeHelper::MergeUntil. PRE: <paramref name="iter"/> is on a merge entry. Consumes the merge chain of
    /// that user key, stopping at a different user key, at an entry with seqno &lt;= <paramref name="stopBefore"/>
    /// (when &gt; 0; visible to an earlier snapshot), or at a put/delete (which is merged in and consumed).
    /// Results are read with <see cref="OutputCount"/> / <see cref="GetOutputKey"/> / <see cref="GetOutputValue"/>.
    /// </summary>
    /// <param name="iter">Input; left on the first entry not consumed.</param>
    /// <param name="rangeDel">Range tombstones of the compaction (null if none).</param>
    /// <param name="stopBefore">Earliest snapshot boundary not to cross (0 for none).</param>
    /// <param name="atBottom">True if no older version of the key can exist below this compaction's output.</param>
    public MergeUntilResult MergeUntil(InternalIterator iter, RangeDelAggregator? rangeDel, ulong stopBefore, bool atBottom)
    {
        Debug.Assert(iter.Valid);
        _keys.Clear();
        _operands.Clear();
        HasCompactionFilterSkipUntil = false;
        bool firstKey = true;

        _origKey.SetInternalKey(iter.Key);
        if (!DbFormat.TryParseInternalKey(_origKey.Key, out ParsedInternalKey orig)) ThrowHelper.Corruption("MergeUntil: corrupted internal key");
        Debug.Assert(orig.Type == EntryType.Merge);

        bool hitTheNextUserKey = false;
        Comparator ucmp = _icmp.UserComparator;
        int tsSize = ucmp.TimestampSize;
        byte[]? tsLow = FullHistoryTsLow;
        int cmpWithFullHistoryTsLow = 0;
        for (; iter.Valid; iter.Next())
        {
            ReadOnlySpan<byte> ikeyBytes = iter.Key;
            if (!DbFormat.TryParseInternalKey(ikeyBytes, out ParsedInternalKey ikey))
            {
                // stop at corrupted key
                if (AssertValidInternalKey) ThrowHelper.Corruption("MergeUntil: corrupted internal key");
                break;
            }
            ReadOnlySpan<byte> userKey = ikeyBytes[..ikey.UserKeyLength];
            ReadOnlySpan<byte> origUserKey = _origKey.UserKey;
            if (tsLow is not null) cmpWithFullHistoryTsLow = ucmp.CompareTimestamp(DbFormat.ExtractTimestampFromUserKey(userKey, tsSize), tsLow);
            if (firstKey)
            {
                Debug.Assert(_icmp.EqualUserKey(userKey, origUserKey));
                firstKey = false;
            }
            else if (tsSize == 0
                ? !_icmp.EqualUserKey(userKey, origUserKey)
                : !ucmp.EqualWithoutTimestamp(userKey, origUserKey) || (!ucmp.Equal(userKey, origUserKey) && cmpWithFullHistoryTsLow >= 0))
            {
                // 1) hit a different user key, or 2) user-defined timestamp is enabled, and hit a version of the user key
                // NOT eligible for GC: stop right here.
                hitTheNextUserKey = true;
                break;
            }
            else if (stopBefore > 0 && ikey.Sequence <= stopBefore)
            {
                // hit an entry that's possibly visible by the previous snapshot, can't touch that
                break;
            }

            // At this point we are guaranteed that we need to process this key.
            if (ikey.Type != EntryType.Merge)
            {
                // hit a put/delete/single delete => merge the put value or nothing with the operands; done.
                // If there are no operands, the caller writes out the put/delete we are at.
                if (_keys.Count == 0) return MergeUntilResult.Ok;

                _mergeResult.Clear();
                if (rangeDel is not null && rangeDel.ShouldDelete(userKey, ikey.Sequence))
                {
                    TimedFullMerge(_mergeOperator, userKey, false, default, _operands.AsOperands(), _mergeResult);
                }
                else if (ikey.Type == EntryType.Value)
                {
                    TimedFullMerge(_mergeOperator, userKey, true, iter.Value, _operands.AsOperands(), _mergeResult);
                }
                else if (ikey.Type == EntryType.ValuePreferredSeqno)
                {
                    // TimedPut merged with operands: the result is a plain kTypeValue.
                    ReadOnlySpan<byte> v = iter.Value;
                    TimedFullMerge(_mergeOperator, userKey, true, v[..^sizeof(ulong)], _operands.AsOperands(), _mergeResult);
                }
                else if (ikey.Type == EntryType.WideColumnEntity)
                {
                    // TimedFullMergeWithEntity: the operands apply to the default column; the result is the entity with
                    // that column replaced (kTypeWideColumnEntity).
                    ReadOnlySpan<byte> entity = iter.Value;
                    TimedFullMerge(_mergeOperator, userKey, true, Wide.WideColumnSerialization.GetValueOfDefaultColumn(entity), _operands.AsOperands(),
                        _mergeResult);
                    PooledBufferWriter merged = ObjectPools.RentWriter();
                    try
                    {
                        Wide.WideColumnSerialization.ReplaceDefaultColumn(entity, _mergeResult.WrittenSpan, merged);
                        _mergeResult.Clear();
                        _mergeResult.Write(merged.WrittenSpan);
                    }
                    finally
                    {
                        ObjectPools.ReturnWriter(merged);
                    }
                    StoreSingleResult(EntryType.WideColumnEntity);
                    iter.Next();
                    return MergeUntilResult.Ok;
                }
                else if (ikey.Type is EntryType.BlobIndex)
                {
                    // MergeUntil with a blob base value: fetch it through the compaction's BlobFetcher.
                    if (BlobProcessor is null) ThrowHelper.NotSupported("merge on top of blob index");
                    PooledBufferWriter blob = ObjectPools.RentWriter();
                    try
                    {
                        BlobProcessor!.FetchBlob(userKey, iter.Value, blob);
                        TimedFullMerge(_mergeOperator, userKey, true, blob.WrittenSpan, _operands.AsOperands(), _mergeResult);
                    }
                    finally
                    {
                        ObjectPools.ReturnWriter(blob);
                    }
                }
                else
                {
                    TimedFullMerge(_mergeOperator, userKey, false, default, _operands.AsOperands(), _mergeResult);
                }
                StoreSingleResult();
                // move iter to the next entry
                iter.Next();
                return MergeUntilResult.Ok;
            }

            // hit a merge: apply the compaction filter, check range tombstones, queue the operand.
            ReadOnlySpan<byte> value = iter.Value;
            CompactionFilterDecision filter = ikey.Sequence <= _latestSnapshot
                ? CompactionFilterDecision.Keep
                : FilterMerge(origUserKey, value);
            // FIXME (upstream): should also check for kRemove here
            if (filter != CompactionFilterDecision.RemoveAndSkipUntil && rangeDel is not null && rangeDel.ShouldDelete(userKey, ikey.Sequence))
            {
                filter = CompactionFilterDecision.Remove;
            }
            if (filter is CompactionFilterDecision.Keep or CompactionFilterDecision.ChangeValue)
            {
                _keys.PushOlder(ikeyBytes);
                _operands.PushOlder(filter == CompactionFilterDecision.Keep ? value : _filterValue.WrittenSpan);
            }
            else if (filter == CompactionFilterDecision.RemoveAndSkipUntil)
            {
                // Compaction filter asked us to remove this key altogether, along with some keys following it.
                _keys.Clear();
                _operands.Clear();
                HasCompactionFilterSkipUntil = true;
                return MergeUntilResult.Ok;
            }
        }

        // we filtered out all the merge operands
        if (_operands.Count == 0) return MergeUntilResult.Ok;

        // We are sure we have seen this key's entire history if at_bottom and we either hit another key or the end.
        // With user-defined timestamps, the beginning is only known for versions below full_history_ts_low.
        bool surelySeenTheBeginning = (hitTheNextUserKey || !iter.Valid) && atBottom && (tsSize == 0 || cmpWithFullHistoryTsLow < 0);
        if (surelySeenTheBeginning)
        {
            // do a final merge with no existing value and say bye to the merge type (it's now converted to a Put)
            _mergeResult.Clear();
            TimedFullMerge(_mergeOperator, _origKey.UserKey, false, default, _operands.AsOperands(), _mergeResult);
            StoreSingleResult();
            return MergeUntilResult.Ok;
        }

        // We haven't seen the beginning of the key nor a Put/Delete: attempt a partial merge of the stacked operands.
        if (_operands.Count >= 2 || (_mergeOperator.AllowSingleOperand && _operands.Count == 1))
        {
            _mergeResult.Clear();
            if (_mergeOperator.PartialMerge(_origKey.UserKey, _operands.AsOperands(), _mergeResult))
            {
                // Replace operands with the merge result; keep only the newest key.
                _origKey.SetInternalKey(_keys.GetNewestFirst(0));
                _keys.Reset(_origKey.Key);
                _operands.Reset(_mergeResult.WrittenSpan);
            }
        }
        return MergeUntilResult.MergeInProgress;
    }

    /// <summary>keys_ = { newest key with type kTypeValue }, operands = { merge result }.</summary>
    private void StoreSingleResult() => StoreSingleResult(EntryType.Value);

    private void StoreSingleResult(EntryType resultType)
    {
        _origKey.SetInternalKey(_keys.GetNewestFirst(0));
        ulong seq = DbFormat.ExtractFooter(_origKey.Key) >> 8;
        _origKey.UpdateInternalKey(seq, resultType);
        _keys.Reset(_origKey.Key);
        _operands.Reset(_mergeResult.WrittenSpan);
    }

    /// <summary>Port of MergeHelper::FilterMerge.</summary>
    private CompactionFilterDecision FilterMerge(ReadOnlySpan<byte> userKey, ReadOnlySpan<byte> value)
    {
        if (_compactionFilter is null) return CompactionFilterDecision.Keep;
        _filterValue.Clear();
        _skipUntilUser.Clear();
        CompactionFilterDecision ret = _compactionFilter.Filter(_level, userKey, CompactionFilterValueType.MergeOperand, value, _filterValue, _skipUntilUser);
        if (ret == CompactionFilterDecision.RemoveAndSkipUntil)
        {
            if (_icmp.CompareUserKey(_skipUntilUser.WrittenSpan, userKey) <= 0)
            {
                // Invalid skip_until returned from compaction filter. Keep the key as per FilterV2 documentation.
                ret = CompactionFilterDecision.Keep;
            }
            else
            {
                _skipUntil.SetInternalKey(_skipUntilUser.WrittenSpan, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
            }
        }
        return ret;
    }

    public void Dispose()
    {
        _keys.Dispose();
        _operands.Dispose();
        _mergeResult.Dispose();
        _filterValue.Dispose();
        _skipUntilUser.Dispose();
        _origKey.Dispose();
        _skipUntil.Dispose();
    }
}
