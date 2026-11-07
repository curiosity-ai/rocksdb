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

    /// <summary>Adds the counters of another (sub)compaction (CompactionJobStats::Add of the iteration stats).</summary>
    public void Add(in CompactionIterationStats o)
    {
        NumInputRecords += o.NumInputRecords;
        NumInputDeletionRecords += o.NumInputDeletionRecords;
        NumRecordDropHidden += o.NumRecordDropHidden;
        NumRecordDropObsolete += o.NumRecordDropObsolete;
        NumRecordDropRangeDel += o.NumRecordDropRangeDel;
        NumRecordDropUser += o.NumRecordDropUser;
        NumSingleDelMismatch += o.NumSingleDelMismatch;
        NumSingleDelFallthru += o.NumSingleDelFallthru;
        NumOptimizedDelDropObsolete += o.NumOptimizedDelDropObsolete;
        NumRangeDelDropObsolete += o.NumRangeDelDropObsolete;
        TotalInputRawKeyBytes += o.TotalInputRawKeyBytes;
        TotalInputRawValueBytes += o.TotalInputRawValueBytes;
    }
}

/// <summary>
/// Turns the sorted stream of internal entries of the compaction inputs into the entries to write: drops versions
/// hidden by newer ones in the same snapshot stripe, obsolete deletions and single-delete pairs, entries covered by
/// range tombstones, merges operands with <see cref="MergeHelper.MergeUntil"/>, applies the compaction filter and
/// zeroes sequence numbers at the bottommost level. Port of rocksdb::CompactionIterator
/// (db/compaction/compaction_iterator.cc: NextFromInput, InvokeFilterIfNeeded, PrepareOutput,
/// findEarliestVisibleSnapshot).
/// <para>
/// User-defined timestamps: versions of a key with different timestamps are distinct keys unless they are older than
/// full_history_ts_low, in which case the older ones may be collapsed (rule A) and their sequence number and timestamp
/// zeroed at the bottommost level.
/// Blob files (integrated BlobDB): see <see cref="BlobProcessor"/> (extraction and garbage collection of values and entity
/// columns, blob values for the compaction filter and merges). Wide-column entities reach the compaction filter's FilterV3.
/// Snapshot checkers (WritePrepared / WriteUnprepared transactions): keys not committed in the job snapshot are output as
/// they are (KeyCommitted) and snapshot visibility is decided by the checker (DefinitelyInSnapshot /
/// findEarliestVisibleSnapshot with released snapshots). Not ported: range-tombstone sentinel keys (range tombstones are
/// handled by the <see cref="RangeDelAggregator"/>
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
        Entity,
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

    // User-defined timestamps (timestamp_size_, full_history_ts_low_, curr_ts_, cmp_with_history_ts_low_).
    private readonly int _tsSize;
    private readonly Comparator _ucmp;
    private readonly byte[]? _currTs;
    private bool _hasCurrTs;
    private int _cmpWithHistoryTsLow;

    /// <summary>snapshot_checker_: decides visibility of possibly uncommitted data (null: everything is committed).</summary>
    internal SnapshotChecker? SnapshotChecker { get; init; }

    /// <summary>job_snapshot_: the snapshot taken for the job when there is a snapshot checker.</summary>
    internal ulong JobSnapshot { get; init; }

    /// <summary>
    /// preserve_seqno_after_: sequence numbers above this are not zeroed at the bottommost level (their write time is
    /// preserved for preserve_internal_time_seconds / preclude_last_level_data_seconds). Null: the earliest snapshot.
    /// </summary>
    internal ulong? PreserveSeqnoAfter { get; init; }

    /// <summary>current_key_committed_: whether the current version of the user key is committed in the job snapshot.</summary>
    private bool _currentKeyCommitted;

    /// <summary>released_snapshots_: snapshots the checker reported as released.</summary>
    private HashSet<ulong>? _releasedSnapshots;

    /// <summary>full_history_ts_low of the compaction (null: all timestamp history is preserved).</summary>
    public byte[]? FullHistoryTsLow { get; init; }

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
        _bottommostLevel = compaction is not null && compaction.BottommostLevel && !compaction.Options.AllowIngestBehind;
        _enforceSingleDelContracts = enforceSingleDelContracts;
        _level = compaction?.StartLevel ?? 0;
        _levelPtrs = new int[compaction?.NumberLevels ?? 0];
        _cancellation = cancellation;
        _ucmp = icmp.UserComparator;
        _tsSize = _ucmp.TimestampSize;
        if (_tsSize != 0) _currTs = new byte[_tsSize];
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
        ValueSource.Entity => BlobProcessor!.EntityOutput,
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
        // Keep current_key_ in sync (same user key; with timestamps the merge output carries its own timestamp).
        if (_tsSize == 0) _currentKey.UpdateInternalKey(seq, type);
        else _currentKey.SetInternalKey(k);
        _valueSource = ValueSource.Merge;
    }

    /// <summary>DataIsDefinitelyInSnapshot (utilities/transactions/snapshot_checker.cc).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool DefinitelyInSnapshot(ulong seq, ulong snapshot) =>
        seq <= snapshot && (SnapshotChecker is null || SnapshotChecker.CheckInSnapshot(seq, snapshot) == SnapshotCheckerResult.InSnapshot);

    /// <summary>DataIsDefinitelyNotInSnapshot.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool DefinitelyNotInSnapshot(ulong seq, ulong snapshot) =>
        seq > snapshot || (SnapshotChecker is not null && SnapshotChecker.CheckInSnapshot(seq, snapshot) == SnapshotCheckerResult.NotInSnapshot);

    /// <summary>KeyCommitted: committed in the job snapshot.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool KeyCommitted(ulong seq) => SnapshotChecker is null || SnapshotChecker.CheckInSnapshot(seq, JobSnapshot) == SnapshotCheckerResult.InSnapshot;

    /// <summary>Port of findEarliestVisibleSnapshot: the earliest snapshot that can see <paramref name="seq"/>.</summary>
    private ulong FindEarliestVisibleSnapshot(ulong seq, out ulong prevSnapshot)
    {
        if (SnapshotChecker is not null) return FindEarliestVisibleSnapshotWithChecker(seq, out prevSnapshot);
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

    /// <summary>findEarliestVisibleSnapshot with a snapshot checker: the first snapshot the checker places the sequence in.</summary>
    private ulong FindEarliestVisibleSnapshotWithChecker(ulong seq, out ulong prevSnapshot)
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
        for (; lo < s.Length; lo++)
        {
            ulong cur = s[lo];
            // Skip if cur is in released_snapshots.
            if (_releasedSnapshots is { } released && released.Contains(cur)) continue;
            SnapshotCheckerResult res = SnapshotChecker!.CheckInSnapshot(seq, cur);
            if (res == SnapshotCheckerResult.InSnapshot) return cur;
            if (res == SnapshotCheckerResult.SnapshotReleased) (_releasedSnapshots ??= new HashSet<ulong>()).Add(cur);
            prevSnapshot = cur;
        }
        return DbFormat.MaxSequenceNumber;
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
        ReadOnlySpan<byte> uk = _input.Key[..ukLen];
        return _tsSize == 0 ? _icmp.EqualUserKey(uk, _currentKey.UserKey) : _ucmp.EqualWithoutTimestamp(uk, _currentKey.UserKey);
    }

    /// <summary>Port of UpdateTimestampAndCompareWithFullHistoryLow.</summary>
    private void UpdateTimestampAndCompareWithFullHistoryLow(ReadOnlySpan<byte> userKey)
    {
        if (_tsSize == 0) return;
        ReadOnlySpan<byte> ts = DbFormat.ExtractTimestampFromUserKey(userKey, _tsSize);
        ts.CopyTo(_currTs);
        _hasCurrTs = true;
        if (FullHistoryTsLow is { } low) _cmpWithHistoryTsLow = _ucmp.CompareTimestamp(ts, low);
    }

    private WideColumns? _filterColumns;
    private List<WideColumn>? _filterNewColumns;

    private Blob.CompactionBlobResolver? _filterBlobResolver;

    /// <summary>
    /// Port of InvokeFilterIfNeeded: plain values, blob values of the integrated BlobDB (FilterBlobByKey first; when
    /// undetermined the blob is read and the filter sees it as a plain value) and wide-column entities. Entities with
    /// blob-backed columns get a lazy <see cref="WideColumnBlobResolver"/> when the filter
    /// <see cref="CompactionFilter.SupportsFilterV4"/> (columns carry their blob index); otherwise their blob columns are
    /// resolved eagerly (the FilterV3 compatibility path of FilterV4).
    /// </summary>
    private void InvokeFilterIfNeeded(ref bool needSkip)
    {
        if (_compactionFilter is null || _ikeyType is not (EntryType.Value or EntryType.BlobIndex or EntryType.WideColumnEntity)) return;
        _filterValue.Clear();
        _filterSkipUntil.Clear();
        ReadOnlySpan<byte> userKey = _currentKey.UserKey;
        ReadOnlySpan<byte> existing = _input.Value;
        CompactionFilterValueType valueType = CompactionFilterValueType.Value;
        CompactionFilterDecision decision = CompactionFilterDecision.Undetermined;
        WideColumns? columns = null;
        Blob.CompactionBlobResolver? resolver = null;
        _filterNewColumns?.Clear();
        if (_ikeyType == EntryType.BlobIndex)
        {
            decision = _compactionFilter.FilterBlobByKey(_level, userKey, _filterValue, _filterSkipUntil);
            if (decision == CompactionFilterDecision.Undetermined)
            {
                // For the integrated BlobDB the compaction iterator reads the blob.
                if (BlobProcessor is null)
                {
                    ThrowHelper.NotSupported("Blob-backed value filtering requires blob read support in the current table-file creation context");
                }
                existing = BlobProcessor!.FetchBlobForFilter(userKey, existing);
            }
        }
        else if (_ikeyType == EntryType.WideColumnEntity)
        {
            columns = _filterColumns ??= new WideColumns();
            if (Wide.WideColumnSerialization.HasBlobColumns(existing))
            {
                if (_compactionFilter.SupportsFilterV4)
                {
                    // Lazy: blob columns keep their blob index; the filter resolves what it needs.
                    columns.SetSerializedWithBlobColumns(existing);
                    resolver = _filterBlobResolver ??= new Blob.CompactionBlobResolver(BlobProcessor);
                    resolver.Reset(userKey, columns);
                }
                else
                {
                    if (BlobProcessor is null)
                    {
                        ThrowHelper.NotSupported("Blob-backed value filtering requires blob read support in the current table-file creation context");
                    }
                    columns.SetSerialized(BlobProcessor!.ResolveEntityForFilter(userKey, existing));
                }
            }
            else
            {
                columns.SetSerialized(existing);
            }
            existing = default;
            valueType = CompactionFilterValueType.WideColumnEntity;
        }
        if (decision == CompactionFilterDecision.Undetermined)
        {
            List<WideColumn> newColumns = _filterNewColumns ??= new List<WideColumn>();
            try
            {
                decision = _compactionFilter.FilterV4(_level, userKey, valueType, existing, columns, _filterValue, newColumns, _filterSkipUntil,
                    resolver);
                // Lazy FilterV4 failure semantics match the eager path: a blob read failure fails the compaction even if
                // the filter noticed it and returned kKeep.
                if (resolver?.ResolveError is { } resolveError) throw resolveError;
            }
            finally
            {
                resolver?.Clear();
            }
            if (decision == CompactionFilterDecision.Undetermined)
            {
                throw new RocksDbException(StatusCode.NotSupported, "FilterV2/V3/V4 should never return kUndetermined");
            }
            if (decision == CompactionFilterDecision.ChangeWideColumnEntity) SerializeFilterColumns(newColumns);
        }
        else if (decision == CompactionFilterDecision.ChangeWideColumnEntity)
        {
            // FilterBlobByKey has no new_columns output: like upstream the entry becomes an entity without columns.
            SerializeFilterColumns(_filterNewColumns ??= new List<WideColumn>());
        }
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
            case CompactionFilterDecision.ChangeWideColumnEntity:
            {
                // _filterValue holds the serialized new columns (SerializeFilterColumns).
                if (_ikeyType != EntryType.WideColumnEntity)
                {
                    _ikeyType = EntryType.WideColumnEntity;
                    _currentKey.UpdateInternalKey(_ikeySequence, EntryType.WideColumnEntity);
                }
                _valueSource = ValueSource.Filter;
                break;
            }
            case CompactionFilterDecision.ChangeBlobIndex:
                throw new RocksDbException(StatusCode.NotSupported, "Only stacked BlobDB's internal compaction filter can return kChangeBlobIndex.");
            case CompactionFilterDecision.IOError:
                throw new RocksDbException(StatusCode.NotSupported, "CompactionFilter for integrated BlobDB should not return kIOError");
            default:
                throw new RocksDbException(StatusCode.NotSupported, "Compaction filter decision " + decision + " is not supported");
        }
    }

    /// <summary>kChangeWideColumnEntity: WideColumnsHelper::SortColumns + WideColumnSerialization::Serialize of the new columns into _filterValue.</summary>
    private void SerializeFilterColumns(List<WideColumn> newColumns)
    {
        WideColumn[] sorted = newColumns.ToArray();
        Wide.WideColumnSerialization.SortColumns(sorted);
        _filterValue.Clear();
        Wide.WideColumnSerialization.Serialize(sorted, _filterValue);
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
            if (pik.Type is EntryType.Deletion or EntryType.SingleDeletion or EntryType.DeletionWithTimestamp) Stats.NumInputDeletionRecords++;
            Stats.TotalInputRawKeyBytes += inKey.Length;
            Stats.TotalInputRawValueBytes += _input.Value.Length;

            bool needSkip = false;
            bool skipFromMerge = false;
            _ikeySequence = pik.Sequence;
            _ikeyType = pik.Type;
            _valueSource = ValueSource.Input;

            // Check whether the user key changed. After this current_key_ is a copy of the current input key (maybe
            // converted to a delete by the compaction filter).
            bool userKeyEqualWithoutTs = false;
            int cmpTs = 0;
            if (_hasCurrentUserKey)
            {
                if (_tsSize == 0)
                {
                    userKeyEqualWithoutTs = _icmp.EqualUserKey(inUserKey, _currentKey.UserKey);
                }
                else
                {
                    userKeyEqualWithoutTs = _ucmp.EqualWithoutTimestamp(inUserKey, _currentKey.UserKey);
                    cmpTs = _ucmp.CompareTimestamp(DbFormat.ExtractTimestampFromUserKey(inUserKey, _tsSize), _currTs);
                }
            }
            if (!_hasCurrentUserKey || !userKeyEqualWithoutTs || cmpTs != 0)
            {
                // First occurrence of this user key (or of this timestamp of it).
                _currentKey.SetInternalKey(inKey);
                int prevCmpWithTsLow = FullHistoryTsLow is { } low && _hasCurrTs ? _ucmp.CompareTimestamp(_currTs, low) : 0;
                UpdateTimestampAndCompareWithFullHistoryLow(inUserKey);
                // The user key is treated as a new key unless timestamps are enabled with full_history_ts_low and this
                // is an older version (below full_history_ts_low) of the previous user key, which then may be GC'ed.
                if (!_hasCurrentUserKey || _tsSize == 0 || FullHistoryTsLow is null || !userKeyEqualWithoutTs || _cmpWithHistoryTsLow >= 0
                    || prevCmpWithTsLow >= 0)
                {
                    _currentUserKeySequence = DbFormat.MaxSequenceNumber;
                    _currentUserKeySnapshot = 0;
                    _hasCurrentUserKey = true;
                }
                _hasOutputtedKey = false;
                _lastKeySeqZeroed = false;
                _currentKeyCommitted = KeyCommitted(_ikeySequence);
                // Apply the compaction filter to the first committed version of the user key.
                if (_currentKeyCommitted) InvokeFilterIfNeeded(ref needSkip);
            }
            else
            {
                // Update the current key to reflect the new sequence number/type without copying the user key.
                _currentKey.UpdateInternalKey(_ikeySequence, _ikeyType);
                // Note that newer version of a key is ordered before older versions. If a newer version of a key is
                // committed, so as the older version. No need to query snapshot_checker_ in that case.
                if (!_currentKeyCommitted)
                {
                    _currentKeyCommitted = KeyCommitted(_ikeySequence);
                    // Apply the compaction filter to the first committed version of the user key.
                    if (_currentKeyCommitted) InvokeFilterIfNeeded(ref needSkip);
                }
            }

            if (!_currentKeyCommitted)
            {
                // kCurrentKeyUncommitted: output the uncommitted version as it is.
                _valid = true;
                break;
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
            else if (_compaction is not null && (_ikeyType == EntryType.Deletion || (_ikeyType == EntryType.DeletionWithTimestamp && _cmpWithHistoryTsLow < 0))
                     && !_compaction.Options.AllowIngestBehind
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
            else if ((_ikeyType == EntryType.Deletion || (_ikeyType == EntryType.DeletionWithTimestamp && _cmpWithHistoryTsLow < 0)) && _bottommostLevel)
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
                // With user-defined timestamps only keys below full_history_ts_low are considered for GC (the
                // aggregator only considers tombstones at or below it).
                if ((_tsSize == 0 || (FullHistoryTsLow is not null && _cmpWithHistoryTsLow < 0))
                    && _rangeDelAgg.ShouldDelete(_currentKey.UserKey, _ikeySequence))
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
        // With timestamps, a SingleDelete may only be dropped below full_history_ts_low.
        bool isTimestampEligibleForGc = _tsSize == 0 || (FullHistoryTsLow is not null && _cmpWithHistoryTsLow < 0);
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
                else if (!isTimestampEligibleForGc)
                {
                    // We cannot drop the SingleDelete as timestamp is enabled, and timestamp of this key is greater than or
                    // equal to full_history_ts_low. We will output the SingleDelete.
                    _valid = true;
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
            if (_compaction is not null && !_compaction.Options.AllowIngestBehind && DefinitelyInSnapshot(_ikeySequence, _earliestSnapshot)
                && _compaction.KeyNotExistsBeyondOutputLevel(_currentKey.UserKey, _levelPtrs) && isTimestampEligibleForGc)
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
            else if (_ikeyType == EntryType.WideColumnEntity)
            {
                // GarbageCollectEntityBlobsIfNeeded (blob columns present) / ExtractLargeColumnValuesIfNeeded.
                if (blob.TryPrepareEntity(_currentKey.UserKey, Value)) _valueSource = ValueSource.Entity;
            }
        }
        // Zeroing out the sequence number leads to better compression. If this is the bottommost level (no files in lower
        // levels) and the earliest snapshot is larger than this seqno and the userkey differs from the last userkey in
        // compaction then we can squash the seqno to zero. current_key_committed_ was decided when the key was reached:
        // a version that was uncommitted then was output without hiding older versions, so it keeps its sequence even if
        // its transaction committed since (DefinitelyInSnapshot asks the snapshot checker again).
        if (_valid && _bottommostLevel && DefinitelyInSnapshot(_ikeySequence, _earliestSnapshot) && _ikeyType != EntryType.Merge
            && _currentKeyCommitted
            && _ikeySequence <= (PreserveSeqnoAfter ?? _earliestSnapshot)
            && _ikeyType != EntryType.Deletion && (_ikeyType != EntryType.SingleDeletion || _tsSize != 0))
        {
            // BottommostSeqnoCanBeZeroed: seq <= preserve_seqno_after_ (the earliest snapshot without time preservation);
            // with user-defined timestamps only below full_history_ts_low.
            if (_tsSize == 0)
            {
                _currentKey.UpdateInternalKey(0, _ikeyType);
            }
            else
            {
                if (FullHistoryTsLow is null || _cmpWithHistoryTsLow >= 0) return;
                // For UDT, the seqno and timestamp are zeroed together: the timestamp-based history of the key is collapsed.
                ZeroTimestampOfCurrentKey();
            }
            _ikeySequence = 0;
            _lastKeySeqZeroed = true;
        }
    }

    /// <summary>current_key_.UpdateInternalKey(0, type, &amp;kTsMin).</summary>
    private void ZeroTimestampOfCurrentKey()
    {
        ReadOnlySpan<byte> userKey = _currentKey.UserKey;
        int n = userKey.Length + DbFormat.InternalKeyFooterSize;
        byte[] buf = System.Buffers.ArrayPool<byte>.Shared.Rent(n);
        try
        {
            userKey[..^_tsSize].CopyTo(buf);
            buf.AsSpan(userKey.Length - _tsSize, _tsSize).Clear();
            Coding.EncodeFixed64(buf.AsSpan(userKey.Length), DbFormat.PackSequenceAndType(0, _ikeyType));
            _currentKey.SetInternalKey(buf.AsSpan(0, n));
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(buf);
        }
    }

    public void Dispose()
    {
        _currentKey.Dispose();
        _filterValue.Dispose();
        _filterSkipUntil.Dispose();
        _filterBlobResolver?.Dispose();
        _skipUntilKey.Dispose();
    }
}
