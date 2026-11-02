using System.Runtime.CompilerServices;
using RocksDb.Managed.Db.Iterators;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Db;

/// <summary>Counters of a compaction iterator (subset of CompactionIterationStats, db/compaction/compaction_iteration_stats.h).</summary>
internal struct CompactionIterationStats
{
    public long NumInputRecords;
    public long NumInputDeletionRecords;
    public long NumRecordDropHidden;
    public long NumRecordDropObsolete;
    public long NumRecordDropRangeDel;
    public long NumRecordDropUser;
    public long NumSingleDelMismatch;
    public long NumSingleDelFallthru;
    public long NumOptimizedDelDropObsolete;
    public long NumRangeDelDropObsolete;
    public long TotalInputRawKeyBytes;
    public long TotalInputRawValueBytes;
}

/// <summary>
/// Turns the sorted stream of internal entries of the compaction inputs into the entries to write: drops versions
/// hidden by newer ones in the same snapshot stripe, obsolete deletions and single-delete pairs, entries covered by
/// range tombstones, merges operands with <see cref="MergeHelper.MergeUntil"/>, applies the compaction filter and
/// zeroes sequence numbers at the bottommost level. Port of rocksdb::CompactionIterator
/// (db/compaction/compaction_iterator.cc: NextFromInput, InvokeFilterIfNeeded, PrepareOutput,
/// findEarliestVisibleSnapshot).
/// <para>
/// Blob files (integrated BlobDB): see <see cref="BlobProcessor"/> (extraction, garbage collection, blob values for the
/// compaction filter and merges). Not ported: snapshot checkers (write-prepared transactions), user-defined
/// timestamps, wide-column filtering, range-tombstone sentinel keys (range tombstones are handled by the <see cref="RangeDelAggregator"/>
/// and written by the compaction job). Allocation-free per key: the current key lives in an <see cref="IterKey"/>
/// and values are spans into the input, the merge helper or the filter output.
/// </para>
/// </summary>
internal sealed class CompactionIterator : IDisposable
{
    private enum ValueSource : byte
    {
        Input,
        Empty,
        Filter,
        Merge,
        InputPrefix,
        BlobIndex,
        BlobValue,
    }

    private readonly InternalIterator _input;
    private readonly InternalKeyComparator _icmp;
    private readonly MergeHelper? _mergeHelper;
    private readonly ulong[] _snapshots;
    private readonly ulong _earliestSnapshot;
    private readonly ulong _earliestWriteConflictSnapshot;
    private readonly bool _visibleAtTip;
    private readonly RangeDelAggregator _rangeDelAgg;
    private readonly Compaction? _compaction;
    private readonly CompactionFilter? _compactionFilter;
    private readonly bool _bottommostLevel;
    private readonly bool _enforceSingleDelContracts;
    private readonly int _level;
    private readonly int[] _levelPtrs;
    private readonly CancellationToken _cancellation;

    private readonly IterKey _currentKey = new();
    private readonly PooledBufferWriter _filterValue = new(64);
    private readonly PooledBufferWriter _filterSkipUntil = new(64);
    private readonly IterKey _skipUntilKey = new();

    private ulong _ikeySequence;
    private EntryType _ikeyType;
    private bool _valid;
    private ValueSource _valueSource;
    private int _valuePrefixLength;
    private int _mergeOutIndex = -1;
    private bool _mergeInProgress;

    private bool _hasCurrentUserKey;
    private ulong _currentUserKeySequence;
    private ulong _currentUserKeySnapshot;
    private bool _hasOutputtedKey;
    private bool _lastKeySeqZeroed;
    private bool _clearAndOutputNextKey;
    private bool _atNext;
    private bool _outputPrepared;

    public CompactionIterationStats Stats;

    /// <summary>
    /// Blob extraction and garbage collection for the output (null: blob values pass through unchanged). Set before
    /// the first positioning call.
    /// </summary>
    public Blob.CompactionBlobProcessor? BlobProcessor { get; set; }

    /// <param name="input">Merged input, positioned by <see cref="SeekToFirst"/>.</param>
    /// <param name="icmp">Internal key comparator.</param>
    /// <param name="mergeHelper">Merge helper (null when the column family has no merge operator).</param>
    /// <param name="snapshots">Live snapshot sequence numbers, ascending.</param>
    /// <param name="earliestWriteConflictSnapshot">Earliest snapshot used for write-conflict checking (kMaxSequenceNumber if none).</param>
    /// <param name="rangeDelAgg">Compaction-mode aggregator over the inputs' range tombstones.</param>
    /// <param name="compaction">The compaction (null for flush-like use: nothing is known about older levels).</param>
    /// <param name="compactionFilter">Compaction filter or null.</param>
    /// <param name="enforceSingleDelContracts">Fail on SingleDelete followed by Delete (DbOptions.EnforceSingleDelContracts).</param>
    /// <param name="cancellation">Aborts the iteration (shutdown).</param>
    public CompactionIterator(InternalIterator input, InternalKeyComparator icmp, MergeHelper? mergeHelper, ulong[] snapshots,
        ulong earliestWriteConflictSnapshot, RangeDelAggregator rangeDelAgg, Compaction? compaction, CompactionFilter? compactionFilter,
        bool enforceSingleDelContracts, CancellationToken cancellation)
    {
        _input = input;
        _icmp = icmp;
        _mergeHelper = mergeHelper;
        _snapshots = snapshots;
        _visibleAtTip = snapshots.Length == 0;
        _earliestSnapshot = snapshots.Length == 0 ? DbFormat.MaxSequenceNumber : snapshots[0];
        _earliestWriteConflictSnapshot = earliestWriteConflictSnapshot;
        _rangeDelAgg = rangeDelAgg;
        _compaction = compaction;
        _compactionFilter = compactionFilter;
        _bottommostLevel = compaction is not null && compaction.BottommostLevel && !compaction.Options.CfAllowIngestBehind;
        _enforceSingleDelContracts = enforceSingleDelContracts;
        _level = compaction?.StartLevel ?? 0;
        _levelPtrs = new int[compaction?.NumberLevels ?? 0];
        _cancellation = cancellation;
    }

    public bool Valid => _valid;

    /// <summary>The current (output) internal key.</summary>
    public ReadOnlySpan<byte> Key { [MethodImpl(MethodImplOptions.AggressiveOptimization)] get => _currentKey.Key; }

    public ReadOnlySpan<byte> UserKey { [MethodImpl(MethodImplOptions.AggressiveOptimization)] get => _currentKey.UserKey; }

    public ulong Sequence => _ikeySequence;

    public EntryType Type => _ikeyType;

    public ReadOnlySpan<byte> Value => _valueSource switch
    {
        ValueSource.Input => _input.Value,
        ValueSource.Filter => _filterValue.WrittenSpan,
        ValueSource.Merge => _mergeHelper!.GetOutputValue(_mergeOutIndex),
        ValueSource.InputPrefix => _input.Value[.._valuePrefixLength],
        ValueSource.BlobIndex => BlobProcessor!.BlobIndexOutput,
        ValueSource.BlobValue => BlobProcessor!.BlobValueOutput,
        _ => default,
    };

    public void SeekToFirst()
    {
        NextFromInput();
        PrepareOutputIfNeeded();
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public void Next()
    {
        NextImpl();
        PrepareOutputIfNeeded();
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private void PrepareOutputIfNeeded()
    {
        if (!_outputPrepared)
        {
            PrepareOutput();
            _outputPrepared = true;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private void NextImpl()
    {
        _outputPrepared = false;
        // If there is a merge output, return it before continuing to process the input.
        if (_mergeOutIndex >= 0)
        {
            _mergeOutIndex++;
            if (_mergeOutIndex < _mergeHelper!.OutputCount)
            {
                SetFromMergeOutput();
                _valid = true;
            }
            else
            {
                _mergeOutIndex = -1;
                if (_mergeInProgress)
                {
                    // The previous MergeUntil() produced only merge operands, all written out. There may still be a base
                    // value (PUT, DELETE, ...) that needs to be written out: reset has_current_user_key_ so the snapshot
                    // stripe check does not skip it.
                    _hasCurrentUserKey = false;
                }
                // MergeHelper moves the iterator to the first record after the merged records.
                NextFromInput();
            }
        }
        else
        {
            // Only advance the input iterator if there is no merge output and the iterator is not already at the next record.
            if (!_atNext) _input.Next();
            NextFromInput();
        }
        if (_valid) _hasOutputtedKey = true;
    }

    private void SetFromMergeOutput()
    {
        ReadOnlySpan<byte> k = _mergeHelper!.GetOutputKey(_mergeOutIndex);
        DbFormat.UnpackSequenceAndType(DbFormat.ExtractFooter(k), out ulong seq, out EntryType type);
        _ikeySequence = seq;
        _ikeyType = type;
        // Keep current_key_ in sync (same user key).
        _currentKey.UpdateInternalKey(seq, type);
        _valueSource = ValueSource.Merge;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool DefinitelyInSnapshot(ulong seq, ulong snapshot) => seq <= snapshot;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool DefinitelyNotInSnapshot(ulong seq, ulong snapshot) => seq > snapshot;

    /// <summary>Port of findEarliestVisibleSnapshot: the earliest snapshot that can see <paramref name="seq"/>.</summary>
    private ulong FindEarliestVisibleSnapshot(ulong seq, out ulong prevSnapshot)
    {
        ulong[] s = _snapshots;
        int lo = 0, hi = s.Length;
        while (lo < hi)
        {
            int mid = (lo + hi) >>> 1;
            if (s[mid] < seq) lo = mid + 1;
            else hi = mid;
        }
        prevSnapshot = lo == 0 ? 0 : s[lo - 1];
        return lo < s.Length ? s[lo] : DbFormat.MaxSequenceNumber;
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private bool ParseInput(out ulong seq, out EntryType type, out int userKeyLength)
    {
        if (!DbFormat.TryParseInternalKey(_input.Key, out ParsedInternalKey p))
        {
            seq = 0;
            type = 0;
            userKeyLength = 0;
            return false;
        }
        seq = p.Sequence;
        type = p.Type;
        userKeyLength = p.UserKeyLength;
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private bool InputHasSameUserKey(out ulong seq, out EntryType type)
    {
        if (!_input.Valid || !ParseInput(out seq, out type, out int ukLen))
        {
            seq = 0;
            type = 0;
            return false;
        }
        return _icmp.EqualUserKey(_input.Key[..ukLen], _currentKey.UserKey);
    }

    /// <summary>
    /// Port of InvokeFilterIfNeeded (plain values, and blob values of the integrated BlobDB: the blob is read and the
    /// filter sees it as a plain value). Returns false if the iteration must stop with an error.
    /// </summary>
    private void InvokeFilterIfNeeded(ref bool needSkip)
    {
        if (_compactionFilter is null || _ikeyType is not (EntryType.Value or EntryType.BlobIndex)) return;
        _filterValue.Clear();
        _filterSkipUntil.Clear();
        ReadOnlySpan<byte> userKey = _currentKey.UserKey;
        ReadOnlySpan<byte> existing = _input.Value;
        if (_ikeyType == EntryType.BlobIndex)
        {
            // For the integrated BlobDB the compaction iterator reads the blob (FilterBlobByKey is not ported).
            if (BlobProcessor is null)
            {
                ThrowHelper.NotSupported("Blob-backed value filtering requires blob read support in the current table-file creation context");
            }
            existing = BlobProcessor!.FetchBlobForFilter(userKey, existing);
        }
        CompactionFilterDecision decision = _compactionFilter.Filter(_level, userKey, CompactionFilterValueType.Value, existing,
            _filterValue, _filterSkipUntil);
        if (decision == CompactionFilterDecision.RemoveAndSkipUntil && _icmp.CompareUserKey(_filterSkipUntil.WrittenSpan, userKey) <= 0)
        {
            // Can't skip to a key smaller than the current one. Keep the key as per FilterV2 documentation.
            decision = CompactionFilterDecision.Keep;
        }
        switch (decision)
        {
            case CompactionFilterDecision.Keep:
                break;
            case CompactionFilterDecision.Remove:
                // convert the current key to a delete
                _ikeyType = EntryType.Deletion;
                _currentKey.UpdateInternalKey(_ikeySequence, EntryType.Deletion);
                _valueSource = ValueSource.Empty;
                Stats.NumRecordDropUser++;
                break;
            case CompactionFilterDecision.Purge:
                // convert the current key to a single delete
                _ikeyType = EntryType.SingleDeletion;
                _currentKey.UpdateInternalKey(_ikeySequence, EntryType.SingleDeletion);
                _valueSource = ValueSource.Empty;
                Stats.NumRecordDropUser++;
                break;
            case CompactionFilterDecision.ChangeValue:
                if (_ikeyType != EntryType.Value)
                {
                    _ikeyType = EntryType.Value;
                    _currentKey.UpdateInternalKey(_ikeySequence, EntryType.Value);
                }
                _valueSource = ValueSource.Filter;
                break;
            case CompactionFilterDecision.RemoveAndSkipUntil:
                needSkip = true;
                _skipUntilKey.SetInternalKey(_filterSkipUntil.WrittenSpan, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
                break;
            default:
                throw new RocksDbException(StatusCode.NotSupported, "Compaction filter decision " + decision + " is not supported");
        }
    }

    /// <summary>Port of CompactionIterator::NextFromInput.</summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private void NextFromInput()
    {
        _atNext = false;
        _valid = false;
        while (!_valid && _input.Valid)
        {
            _cancellation.ThrowIfCancellationRequested();
            ReadOnlySpan<byte> inKey = _input.Key;
            if (!DbFormat.TryParseInternalKey(inKey, out ParsedInternalKey pik))
            {
                // Always fail compaction when encountering corrupted internal keys.
                ThrowHelper.Corruption("Corrupted internal key in compaction input");
            }
            ReadOnlySpan<byte> inUserKey = inKey[..pik.UserKeyLength];
            Stats.NumInputRecords++;
            if (pik.Type is EntryType.Deletion or EntryType.SingleDeletion) Stats.NumInputDeletionRecords++;
            Stats.TotalInputRawKeyBytes += inKey.Length;
            Stats.TotalInputRawValueBytes += _input.Value.Length;

            bool needSkip = false;
            bool skipFromMerge = false;
            _ikeySequence = pik.Sequence;
            _ikeyType = pik.Type;
            _valueSource = ValueSource.Input;

            // Check whether the user key changed. After this current_key_ is a copy of the current input key (maybe
            // converted to a delete by the compaction filter).
            if (!_hasCurrentUserKey || !_icmp.EqualUserKey(inUserKey, _currentKey.UserKey))
            {
                // First occurrence of this user key.
                _currentKey.SetInternalKey(inKey);
                _currentUserKeySequence = DbFormat.MaxSequenceNumber;
                _currentUserKeySnapshot = 0;
                _hasCurrentUserKey = true;
                _hasOutputtedKey = false;
                _lastKeySeqZeroed = false;
                // Apply the compaction filter to the first committed version of the user key.
                InvokeFilterIfNeeded(ref needSkip);
            }
            else
            {
                // Update the current key to reflect the new sequence number/type without copying the user key.
                _currentKey.UpdateInternalKey(_ikeySequence, _ikeyType);
            }

            // If there are no snapshots, then this kv affect visibility at tip. Otherwise, search though all existing
            // snapshots to find the earliest snapshot that is affected by this kv.
            ulong lastSequence = _currentUserKeySequence;
            _currentUserKeySequence = _ikeySequence;
            ulong lastSnapshot = _currentUserKeySnapshot;
            ulong prevSnapshot = 0; // 0 means no previous snapshot
            _currentUserKeySnapshot = _visibleAtTip ? _earliestSnapshot : FindEarliestVisibleSnapshot(_ikeySequence, out prevSnapshot);

            if (needSkip)
            {
                // This case is handled below.
            }
            else if (_clearAndOutputNextKey)
            {
                // In the previous iteration we encountered a single delete that we could not compact out. We will keep
                // this Put, but can drop its data (Optimization 3).
                if (_ikeyType is EntryType.BlobIndex or EntryType.WideColumnEntity or EntryType.ValuePreferredSeqno)
                {
                    _ikeyType = EntryType.Value;
                    _currentKey.UpdateInternalKey(_ikeySequence, _ikeyType);
                }
                _valueSource = ValueSource.Empty;
                _valid = true;
                _clearAndOutputNextKey = false;
            }
            else if (_ikeyType == EntryType.SingleDeletion)
            {
                HandleSingleDelete(prevSnapshot);
                if (_valid) _atNext = true;
            }
            else if (lastSequence != DbFormat.MaxSequenceNumber && lastSnapshot <= _currentUserKeySnapshot)
            {
                // rule (A): if the earliest snapshot in which this key is visible is the same as the visibility of a
                // previous instance of the same key, then this kv is not visible in any snapshot. Hidden by a newer
                // entry for the same user key.
                Stats.NumRecordDropHidden++;
                _input.Next();
            }
            else if (_compaction is not null && _ikeyType == EntryType.Deletion && !_compaction.Options.CfAllowIngestBehind
                     && DefinitelyInSnapshot(_ikeySequence, _earliestSnapshot)
                     && _compaction.KeyNotExistsBeyondOutputLevel(_currentKey.UserKey, _levelPtrs))
            {
                // For this user key: (1) there is no data in higher levels, (2) data in lower levels will have larger
                // sequence numbers, (3) data in layers being compacted here with smaller sequence numbers will be dropped
                // in the next few iterations (rule A). Therefore this deletion marker is obsolete and can be dropped.
                Stats.NumRecordDropObsolete++;
                if (!_bottommostLevel) Stats.NumOptimizedDelDropObsolete++;
                _input.Next();
            }
            else if (_ikeyType == EntryType.Deletion && _bottommostLevel)
            {
                // Handle the case where we have a delete key at the bottom most level. We can skip outputting the key
                // iff there are no subsequent puts for this key.
                _input.Next();
                // Skip over all versions of this key that happen to occur in the same snapshot range as the delete.
                while (InputHasSameUserKey(out ulong nextSeq, out _) && (prevSnapshot == 0 || DefinitelyNotInSnapshot(nextSeq, prevSnapshot)))
                {
                    _cancellation.ThrowIfCancellationRequested();
                    _input.Next();
                }
                // If you find you still need to output a row with this key, we need to output the delete too.
                if (InputHasSameUserKey(out _, out _))
                {
                    _valueSource = ValueSource.Empty;
                    _valid = true;
                    _atNext = true;
                }
            }
            else if (_ikeyType == EntryType.ValuePreferredSeqno && DefinitelyInSnapshot(_ikeySequence, _earliestSnapshot)
                     && (_bottommostLevel || (_compaction is not null && _compaction.KeyNotExistsBeyondOutputLevel(_currentKey.UserKey, _levelPtrs))))
            {
                // The entries with the same user key and smaller sequence numbers are all in this earliest snapshot range
                // to be iterated (and hidden by this entry), so it's safe to swap in the preferred seqno now.
                ReadOnlySpan<byte> packed = _input.Value;
                ulong preferredSeqno = Coding.DecodeFixed64(packed[^8..]);
                if (_rangeDelAgg.ShouldDelete(_currentKey.UserKey, _ikeySequence))
                {
                    Stats.NumRecordDropHidden++;
                    Stats.NumRecordDropRangeDel++;
                    _input.Next();
                }
                else
                {
                    ulong swapped = Math.Min(preferredSeqno, _ikeySequence);
                    if (_rangeDelAgg.ShouldDelete(_currentKey.UserKey, swapped))
                    {
                        // A range tombstone that doesn't cover this entry would cover it after the swap: output as is.
                        _valid = true;
                    }
                    else
                    {
                        if (_ikeySequence != 0) _ikeySequence = preferredSeqno;
                        _ikeyType = EntryType.Value;
                        _currentKey.UpdateInternalKey(_ikeySequence, _ikeyType);
                        _valueSource = ValueSource.InputPrefix;
                        _valuePrefixLength = packed.Length - 8;
                        _valid = true;
                    }
                }
            }
            else if (_ikeyType == EntryType.Merge)
            {
                if (_mergeHelper is null) ThrowHelper.InvalidArgument("merge_operator is not properly initialized.");
                // We know the merge type entry is not hidden, otherwise we would have hit (A).
                MergeUntilResult r = _mergeHelper!.MergeUntil(_input, _rangeDelAgg, prevSnapshot, _bottommostLevel);
                _mergeInProgress = r == MergeUntilResult.MergeInProgress;
                if (_mergeHelper.OutputCount > 0)
                {
                    _mergeOutIndex = 0;
                    SetFromMergeOutput();
                    _valid = true;
                }
                else
                {
                    // All merge operands were filtered out. Reset the user key, since the batch consumed by the merge
                    // operator should not shadow any keys coming after the merges.
                    _hasCurrentUserKey = false;
                    if (_mergeHelper.HasCompactionFilterSkipUntil)
                    {
                        needSkip = true;
                        skipFromMerge = true;
                    }
                }
            }
            else
            {
                // 1. new user key -OR- 2. different snapshot stripe
                if (_rangeDelAgg.ShouldDelete(_currentKey.UserKey, _ikeySequence))
                {
                    Stats.NumRecordDropHidden++;
                    Stats.NumRecordDropRangeDel++;
                    _input.Next();
                }
                else
                {
                    _valid = true;
                }
            }

            if (needSkip)
            {
                if (skipFromMerge) _input.Seek(_mergeHelper!.CompactionFilterSkipUntil);
                else _input.Seek(_skipUntilKey.Key);
            }
        }
        _cancellation.ThrowIfCancellationRequested();
    }

    /// <summary>The SingleDelete branch of NextFromInput (peeks at the next entry).</summary>
    private void HandleSingleDelete(ulong prevSnapshot)
    {
        // The easiest way to process a SingleDelete during iteration is to peek ahead at the next key.
        _input.Next();
        if (InputHasSameUserKey(out ulong nextSeq, out EntryType nextType))
        {
            if (_lastKeySeqZeroed)
            {
                // Drop SD and the next key since they are both in the last snapshot (since last key has seqno zeroed).
                Stats.NumRecordDropHidden++;
                Stats.NumRecordDropObsolete++;
                _input.Next();
            }
            else if (prevSnapshot == 0 || DefinitelyNotInSnapshot(nextSeq, prevSnapshot))
            {
                // The next key belongs to the same snapshot as the SingleDelete.
                if (nextType == EntryType.SingleDeletion)
                {
                    // Two SingleDeletes for the same key in a row: skip the first, let the next iteration decide about the second.
                    Stats.NumRecordDropObsolete++;
                    Stats.NumSingleDelMismatch++;
                }
                else if (nextType == EntryType.Deletion)
                {
                    Stats.NumRecordDropObsolete++;
                    Stats.NumSingleDelMismatch++;
                    if (_enforceSingleDelContracts)
                    {
                        ThrowHelper.Corruption("Found SD and type: " + (int)nextType + " on the same key, violating the contract of SingleDelete. " +
                                               "Check your application to make sure the application does not mix SingleDelete and Delete for the same key.");
                    }
                }
                else if (_hasOutputtedKey || DefinitelyInSnapshot(_ikeySequence, _earliestWriteConflictSnapshot)
                         || (_earliestSnapshot < _earliestWriteConflictSnapshot && DefinitelyInSnapshot(_ikeySequence, _earliestSnapshot)))
                {
                    // Found a matching value, we can drop the single delete and the value. It is safe to drop both
                    // records since we've already outputted a key in this snapshot, or there is no earlier snapshot.
                    if (nextType is not (EntryType.Value or EntryType.BlobIndex or EntryType.WideColumnEntity or EntryType.ValuePreferredSeqno))
                    {
                        Stats.NumSingleDelMismatch++;
                    }
                    Stats.NumRecordDropHidden++;
                    Stats.NumRecordDropObsolete++;
                    // Already called input_.Next() once. Call it a second time to skip past the second key.
                    _input.Next();
                }
                else
                {
                    // Found a matching value, but we cannot drop both keys since there is an earlier snapshot and we need
                    // to leave behind a record to know that a write happened in this snapshot. Output the SingleDelete and
                    // set up the Put to be outputted (without its value) in the next iteration.
                    _valid = true;
                    _clearAndOutputNextKey = true;
                }
            }
            else
            {
                // We hit the next snapshot without hitting a put, so the iterator returns the single delete.
                _valid = true;
            }
        }
        else
        {
            // We are at the end of the input or hit a different key. The iterator returns the single delete if the key
            // possibly exists beyond the current output level. Set has_current_user_key to false so that if the iterator
            // is at the next key, we do not compare it again against the previous key at the next iteration.
            _hasCurrentUserKey = false;
            if (_compaction is not null && !_compaction.Options.CfAllowIngestBehind && DefinitelyInSnapshot(_ikeySequence, _earliestSnapshot)
                && _compaction.KeyNotExistsBeyondOutputLevel(_currentKey.UserKey, _levelPtrs))
            {
                // Key doesn't exist outside of this range. Can compact out this SingleDelete.
                Stats.NumRecordDropObsolete++;
                Stats.NumSingleDelFallthru++;
                if (!_bottommostLevel) Stats.NumOptimizedDelDropObsolete++;
            }
            else if (_lastKeySeqZeroed)
            {
                Stats.NumRecordDropHidden++;
                Stats.NumRecordDropObsolete++;
            }
            else
            {
                // Output SingleDelete
                _valid = true;
            }
        }
        _valueSource = ValueSource.Empty;
    }

    /// <summary>Port of CompactionIterator::PrepareOutput: zeroes the sequence number at the bottommost level when safe.</summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private void PrepareOutput()
    {
        if (_valid && BlobProcessor is { } blob)
        {
            if (_ikeyType == EntryType.Value)
            {
                // ExtractLargeValueIfNeeded
                if (blob.TryExtract(_currentKey.UserKey, Value))
                {
                    _valueSource = ValueSource.BlobIndex;
                    _ikeyType = EntryType.BlobIndex;
                    _currentKey.UpdateInternalKey(_ikeySequence, _ikeyType);
                }
            }
            else if (_ikeyType == EntryType.BlobIndex)
            {
                // GarbageCollectBlobIfNeeded
                if (blob.TryRelocate(_currentKey.UserKey, Value, out bool extracted))
                {
                    if (extracted)
                    {
                        _valueSource = ValueSource.BlobIndex;
                    }
                    else
                    {
                        _valueSource = ValueSource.BlobValue;
                        _ikeyType = EntryType.Value;
                        _currentKey.UpdateInternalKey(_ikeySequence, _ikeyType);
                    }
                }
            }
        }
        // Zeroing out the sequence number leads to better compression. If this is the bottommost level (no files in lower
        // levels) and the earliest snapshot is larger than this seqno and the userkey differs from the last userkey in
        // compaction then we can squash the seqno to zero.
        if (_valid && _bottommostLevel && DefinitelyInSnapshot(_ikeySequence, _earliestSnapshot) && _ikeyType != EntryType.Merge
            && _ikeyType is not (EntryType.Deletion or EntryType.SingleDeletion))
        {
            // BottommostSeqnoCanBeZeroed: seq <= preserve_seqno_after_ (= earliest snapshot without time preservation).
            _currentKey.UpdateInternalKey(0, _ikeyType);
            _ikeySequence = 0;
            _lastKeySeqZeroed = true;
        }
    }

    public void Dispose()
    {
        _currentKey.Dispose();
        _filterValue.Dispose();
        _filterSkipUntil.Dispose();
        _skipUntilKey.Dispose();
    }
}
