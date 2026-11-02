using System.Diagnostics;
using System.Runtime.CompilerServices;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Db.Iterators;

/// <summary>
/// User-facing iteration semantics over an internal iterator: snapshot visibility, deletions (point, single and
/// range), merge resolution, iterate bounds and direction switching. Port of rocksdb::DBIter (db/db_iter.cc).
/// </summary>
/// <remarks>
/// <para><b>What is ported.</b> FindNextUserEntry (incl. the max_sequential_skip_in_iterations reseek),
/// MergeValuesNewToOld, ReverseToForward / ReverseToBackward, PrevInternal, FindValueForCurrentKey(+UsingSeek),
/// FindUserKeyBeforeSavedKey, Seek / SeekForPrev / SeekToFirst / SeekToLast with iterate_lower_bound /
/// iterate_upper_bound, max_skippable_internal_keys (Incomplete). Range deletions use the pre-7.x scheme: every
/// visible entry is checked against a <see cref="RangeDelAggregator"/> (see <see cref="MergingIterator"/>).</para>
/// <para><b>Prefix seek.</b> With a prefix extractor and neither total_order_seek nor auto_prefix_mode, the inner
/// iterator is in prefix mode (tables skipped by their prefix filter): direction changes re-seek like upstream
/// (expect_total_order_inner_iter_ false) and the reseek optimization is disabled for SeekToFirst/SeekToLast.
/// prefix_same_as_start stops iteration at the end of the seek key's prefix (PrefixCheck).</para>
/// <para><b>Blob values</b> (integrated BlobDB, DBIter::SetValueAndColumnsFromBlob): a blob-index entry is resolved
/// lazily, when <see cref="Value"/> is first read (like allow_unprepared_value = true followed by PrepareValue), or
/// with async I/O by <see cref="PrepareValueAsync"/>; merges on top of a blob read the blob synchronously.</para>
/// <para><b>Not ported (yet).</b> User-defined timestamps, read callbacks, statistics/perf context, tombstone-run
/// conversion, multi-scan.</para>
/// <para><b>Errors.</b> Upstream sets status() and invalidates the iterator; here the positioning call throws
/// <see cref="RocksDbException"/> (Corruption, InvalidArgument for a merge without operator, Incomplete when
/// max_skippable_internal_keys is exceeded) and the iterator is left invalid.</para>
/// <para><b>Memory.</b> <see cref="Key"/> is a copy kept in a reusable buffer (saved_key_); <see cref="Value"/> points
/// into the child iterator when moving forward over a plain value, into a reusable copy when moving backward
/// (upstream pins blocks instead), or into a reusable merge-result buffer. Both spans are valid until the next
/// positioning call. Steady-state stepping does not allocate.</para>
/// <para><b>Async.</b> Forward positioning (<see cref="SeekAsync"/>, <see cref="SeekToFirstAsync"/>,
/// <see cref="NextAsync"/>) awaits the inner iterator and completes synchronously when no I/O is needed; the
/// forward state machine is shared with the sync path (step functions). Reverse operations are synchronous.</para>
/// <para>Not thread-safe.</para>
/// </remarks>
public sealed class DbIter : IDisposable
{
    private enum StepAction : byte { Done, Next, Seek, NextThenDone }
    private enum ValueSource : byte { None, Iter, IterPacked, Pinned, Merged }

    private readonly InternalIterator _iter;
    private readonly InternalKeyComparator _icmp;
    private readonly ulong _sequence;
    private readonly MergeOperator? _mergeOperator;
    private readonly RangeDelAggregator? _rangeDel;
    private ulong _maxSkip;
    private readonly SliceTransform? _prefixExtractor;
    private readonly bool _prefixSameAsStart;
    private readonly bool _expectTotalOrderInnerIter;
    private readonly PooledBufferWriter _prefix = new(16);
    private bool _hasPrefix;
    private readonly ulong _maxSkippableInternalKeys;
    private readonly bool _hasLower;
    private readonly bool _hasUpper;
    private readonly ReadOnlyMemory<byte> _lower;
    private readonly ReadOnlyMemory<byte> _upper;
    private IDisposable? _cleanup;

    private readonly IterKey _savedKey = new();
    private readonly PooledBufferWriter _seekKey = new(64);
    private readonly PooledBufferWriter _pinned = new(128);
    private readonly PooledBufferWriter _mergeResult = new(128);
    private PooledBufferWriter? _mergedEntity;      // entity produced by a merge on top of a wide-column entity
    private bool _valueIsEntity;                    // the current value is a wide-column entity (Value = its default column)
    private bool _valueIsBlob;                      // the current value source holds a blob index (Value = the blob)
    private bool _blobFetched;                      // _blobValue holds the blob of the current entry
    private PooledBufferWriter? _blobValue;
    private PooledBufferWriter? _blobKey;
    private Blob.BlobSource? _blobSource;
    private VersionStorageInfo? _blobVersion;
    private readonly ReadOptions? _readOptions;
    private readonly MergeOperandBuffer _operands = new();

    private bool _valid;
    private bool _forward = true;
    private bool _currentEntryIsMerged;
    private bool _isKeySeqnumZero;
    private ValueSource _valueSource;
    private ulong _numInternalKeysSkipped;

    // FindNextUserEntry / MergeValuesNewToOld loop state (see the step functions).
    private bool _skipping;
    private bool _reseekDone;
    private bool _merging;
    private ulong _numSkipped;
    private bool _disposed;

    /// <param name="comparator">Internal key comparator of the column family.</param>
    /// <param name="iter">Internal iterator (ownership transferred: disposed with this iterator).</param>
    /// <param name="sequence">Snapshot sequence number: entries with a larger seqno are invisible.</param>
    /// <param name="readOptions">Bounds and skip limits (null = defaults).</param>
    /// <param name="mergeOperator">The column family's merge operator (null: merge entries throw InvalidArgument).</param>
    /// <param name="rangeDel">Range tombstones to apply (null or empty = none).</param>
    /// <param name="maxSequentialSkipInIterations">max_sequential_skip_in_iterations (CF option, default 8).</param>
    /// <param name="cleanup">Disposed after the internal iterator (e.g. releases a super version), or null.</param>
    public DbIter(InternalKeyComparator comparator, InternalIterator iter, ulong sequence, ReadOptions? readOptions,
        MergeOperator? mergeOperator, RangeDelAggregator? rangeDel, ulong maxSequentialSkipInIterations, IDisposable? cleanup)
        : this(comparator, iter, sequence, readOptions, mergeOperator, rangeDel, maxSequentialSkipInIterations, cleanup, prefixExtractor: null)
    {
    }

    /// <param name="comparator">Internal key comparator of the column family.</param>
    /// <param name="iter">Internal iterator (ownership transferred: disposed with this iterator).</param>
    /// <param name="sequence">Snapshot sequence number: entries with a larger seqno are invisible.</param>
    /// <param name="readOptions">Bounds, skip limits and prefix seek mode (null = defaults).</param>
    /// <param name="mergeOperator">The column family's merge operator (null: merge entries throw InvalidArgument).</param>
    /// <param name="rangeDel">Range tombstones to apply (null or empty = none).</param>
    /// <param name="maxSequentialSkipInIterations">max_sequential_skip_in_iterations (CF option, default 8).</param>
    /// <param name="cleanup">Disposed after the internal iterator (e.g. releases a super version), or null.</param>
    /// <param name="prefixExtractor">The column family's prefix extractor (MutableCFOptions::prefix_extractor), or null.</param>
    public DbIter(InternalKeyComparator comparator, InternalIterator iter, ulong sequence, ReadOptions? readOptions,
        MergeOperator? mergeOperator, RangeDelAggregator? rangeDel, ulong maxSequentialSkipInIterations, IDisposable? cleanup,
        SliceTransform? prefixExtractor)
    {
        _prefixExtractor = prefixExtractor;
        _prefixSameAsStart = prefixExtractor is not null && readOptions?.PrefixSameAsStart == true;
        _expectTotalOrderInnerIter = prefixExtractor is null || readOptions?.TotalOrderSeek == true || readOptions?.AutoPrefixMode == true;
        ArgumentNullException.ThrowIfNull(comparator);
        ArgumentNullException.ThrowIfNull(iter);
        _icmp = comparator;
        _iter = iter;
        _sequence = sequence;
        _readOptions = readOptions;
        _mergeOperator = mergeOperator;
        _maxSkip = maxSequentialSkipInIterations;
        _cleanup = cleanup;
        if (readOptions is not null)
        {
            if (readOptions.IterateLowerBound is ReadOnlyMemory<byte> lb)
            {
                _hasLower = true;
                _lower = lb;
            }
            if (readOptions.IterateUpperBound is ReadOnlyMemory<byte> ub)
            {
                _hasUpper = true;
                _upper = ub;
            }
            _maxSkippableInternalKeys = readOptions.MaxSkippableInternalKeys;
            if (readOptions.IgnoreRangeDeletions) rangeDel = null;
        }
        _rangeDel = rangeDel is null || rangeDel.IsEmpty ? null : rangeDel;
    }

    public bool Valid => _valid;

    /// <summary>Enables blob values: blob indexes are resolved through <paramref name="source"/> against <paramref name="version"/>.</summary>
    internal void SetBlobSource(Blob.BlobSource source, VersionStorageInfo version)
    {
        _blobSource = source;
        _blobVersion = version;
    }

    /// <summary>The raw value of the current entry before blob resolution (the blob index when <see cref="_valueIsBlob"/>).</summary>
    private ReadOnlySpan<byte> RawValue => _valueSource switch
    {
        ValueSource.Iter => _iter.Value,
        ValueSource.Pinned => _pinned.WrittenSpan,
        _ => default,
    };

    /// <summary>The blob of the current blob-index entry, read on first use (DBIter::SetValueAndColumnsFromBlob).</summary>
    private ReadOnlySpan<byte> BlobValue()
    {
        if (!_blobFetched)
        {
            PooledBufferWriter buf = _blobValue ??= new PooledBufferWriter(256);
            buf.Clear();
            try
            {
                _blobSource!.GetBlob(_blobVersion!, _readOptions, _savedKey.UserKey, RawValue, buf);
            }
            catch
            {
                _valid = false;
                throw;
            }
            _blobFetched = true;
        }
        return _blobValue!.WrittenSpan;
    }

    /// <summary>
    /// Reads the value of the current entry if it is still pending (a blob not read yet; Iterator::PrepareValue) with
    /// async I/O. Completes synchronously when nothing is pending or the blob is cached. REQUIRES: <see cref="Valid"/>.
    /// </summary>
    public ValueTask PrepareValueAsync(CancellationToken cancellationToken = default)
    {
        if (!_valid || !_valueIsBlob || _blobFetched) return ValueTask.CompletedTask;
        PooledBufferWriter buf = _blobValue ??= new PooledBufferWriter(256);
        buf.Clear();
        // The key must outlive the await: copy it into a reusable buffer.
        PooledBufferWriter key = _blobKey ??= new PooledBufferWriter(64);
        key.Clear();
        key.Write(_savedKey.UserKey);
        ValueTask t = _blobSource!.GetBlobAsync(_blobVersion!, _readOptions, key.WrittenMemory, RawValue, buf, cancellationToken);
        if (t.IsCompletedSuccessfully)
        {
            t.GetAwaiter().GetResult();
            _blobFetched = true;
            return ValueTask.CompletedTask;
        }
        return AwaitBlobAsync(t);
    }

    private async ValueTask AwaitBlobAsync(ValueTask pending)
    {
        try
        {
            await pending.ConfigureAwait(false);
        }
        catch
        {
            _valid = false;
            throw;
        }
        _blobFetched = true;
    }

    /// <summary>Marks the current value as a blob index (or not) of the given entry type.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void SetValueKind(EntryType type)
    {
        _valueIsEntity = type == EntryType.WideColumnEntity;
        _valueIsBlob = type == EntryType.BlobIndex;
        _blobFetched = false;
        if (_valueIsBlob && _blobSource is null) ThrowNotSupportedType(type);
    }

    /// <summary>Reads the blob of a blob index into a scratch buffer (merge base values).</summary>
    private ReadOnlySpan<byte> FetchBlobForMerge(ReadOnlySpan<byte> blobIndex)
    {
        if (_blobSource is null) ThrowNotSupportedType(EntryType.BlobIndex);
        PooledBufferWriter buf = _blobValue ??= new PooledBufferWriter(256);
        buf.Clear();
        try
        {
            _blobSource!.GetBlob(_blobVersion!, _readOptions, _savedKey.UserKey, blobIndex, buf);
        }
        catch
        {
            _valid = false;
            throw;
        }
        _blobFetched = false;
        return buf.WrittenSpan;
    }

    /// <summary>The current user key. REQUIRES: <see cref="Valid"/>. Valid until the next positioning call.</summary>
    public ReadOnlySpan<byte> Key
    {
        get
        {
            Debug.Assert(_valid);
            return _savedKey.UserKey;
        }
    }

    /// <summary>The current value. REQUIRES: <see cref="Valid"/>. Valid until the next positioning call.</summary>
    public ReadOnlySpan<byte> Value
    {
        get
        {
            Debug.Assert(_valid);
            if (_valueIsBlob) return BlobValue();
            switch (_valueSource)
            {
                case ValueSource.Iter: return _valueIsEntity ? Wide.WideColumnSerialization.GetValueOfDefaultColumn(_iter.Value) : _iter.Value;
                case ValueSource.IterPacked: return ParsePackedValueForValue(_iter.Value);
                case ValueSource.Pinned: return _valueIsEntity ? Wide.WideColumnSerialization.GetValueOfDefaultColumn(_pinned.WrittenSpan) : _pinned.WrittenSpan;
                case ValueSource.Merged: return _mergeResult.WrittenSpan;
                default: return ReadOnlySpan<byte>.Empty;
            }
        }
    }

    /// <summary>
    /// True if the current entry is a wide-column entity (written with PutEntity); <see cref="Value"/> is then the value
    /// of its default column and <see cref="SerializedEntity"/> holds all columns. REQUIRES: <see cref="Valid"/>.
    /// </summary>
    public bool IsEntity => _valueIsEntity;

    /// <summary>The serialized wide-column entity of the current entry. REQUIRES: <see cref="IsEntity"/>.</summary>
    public ReadOnlySpan<byte> SerializedEntity => _valueSource switch
    {
        ValueSource.Iter => _iter.Value,
        ValueSource.Pinned => _pinned.WrittenSpan,
        ValueSource.Merged => _mergedEntity!.WrittenSpan,
        _ => default,
    };

    /// <summary>The internal key comparator (its user comparator orders <see cref="Key"/>).</summary>
    public InternalKeyComparator Comparator => _icmp;

    /// <summary>The snapshot sequence number this iterator reads at.</summary>
    public ulong Sequence => _sequence;

    /// <summary>The internal iterator (for tests and diagnostics).</summary>
    public InternalIterator InternalIterator => _iter;

    // ==================================================================== helpers

    /// <summary>ParsePackedValueForValue (db/dbformat.h): strips the packed preferred seqno of kTypeValuePreferredSeqno.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ReadOnlySpan<byte> ParsePackedValueForValue(ReadOnlySpan<byte> v) => v[..^sizeof(ulong)];

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ReadOnlySpan<byte> ParseCurrent(out ulong sequence, out EntryType type)
    {
        ReadOnlySpan<byte> k = _iter.Key;
        if (k.Length < DbFormat.InternalKeyFooterSize) ThrowCorruptKey();
        ulong footer = Coding.DecodeFixed64(k[^DbFormat.InternalKeyFooterSize..]);
        sequence = footer >> 8;
        type = (EntryType)(byte)footer;
        return k[..^DbFormat.InternalKeyFooterSize];
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void ThrowCorruptKey()
    {
        _valid = false;
        ThrowHelper.Corruption("In DBIter: corrupted internal key");
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void ThrowUnknownType(EntryType type)
    {
        _valid = false;
        ThrowHelper.Corruption("Unknown value type: " + ((int)type).ToString(System.Globalization.CultureInfo.InvariantCulture));
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void ThrowNotSupportedType(EntryType type)
    {
        _valid = false;
        ThrowHelper.NotSupported("blob values need a blob source (entry type " + ((int)type).ToString(System.Globalization.CultureInfo.InvariantCulture) + ")");
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void ThrowNoMergeOperator()
    {
        _valid = false;
        throw new RocksDbException(StatusCode.InvalidArgument, "Invalid argument: merge_operator_ must be set.");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool IsCoveredByRangeTombstone(ReadOnlySpan<byte> userKey, ulong sequence) =>
        _rangeDel is not null && _rangeDel.ShouldDelete(userKey, sequence);

    /// <summary>Port of TooManyInternalKeysSkipped; throws Incomplete instead of returning true.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void TooManyInternalKeysSkipped(bool increment)
    {
        if (_maxSkippableInternalKeys > 0 && _numInternalKeysSkipped > _maxSkippableInternalKeys) ThrowIncomplete();
        else if (increment) _numInternalKeysSkipped++;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void ThrowIncomplete()
    {
        _valid = false;
        throw new RocksDbException(StatusCode.Incomplete, "Incomplete: Too many internal keys skipped.");
    }

    private void SetSeekKey(ReadOnlySpan<byte> userKey, ulong sequence, EntryType type)
    {
        _seekKey.Clear();
        Span<byte> dst = _seekKey.GetSpan(userKey.Length + DbFormat.InternalKeyFooterSize);
        _seekKey.Advance(DbFormat.AppendInternalKey(dst, userKey, sequence, type));
    }

    /// <summary>Port of DBIter::PrefixCheck: the key has the seek key's prefix (or no prefix is being enforced).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool PrefixCheck(ReadOnlySpan<byte> userKey) =>
        !_hasPrefix || (_prefixExtractor!.InDomain(userKey) && _prefixExtractor.Transform(userKey).SequenceEqual(_prefix.WrittenSpan));

    /// <summary>
    /// Port of DBIter::ShouldSetPrefix + prefix_->SetUserKey(Transform(target)): remembers the prefix of
    /// <paramref name="target"/> for prefix_same_as_start (tombstone-run conversion is not ported).
    /// </summary>
    private void MaybeSetPrefix(ReadOnlySpan<byte> target)
    {
        if (!_prefixSameAsStart || !_prefixExtractor!.InDomain(target)) return;
        _prefix.Clear();
        _prefix.Write(_prefixExtractor.Transform(target));
        _hasPrefix = true;
    }

    private void ResetSeekState()
    {
        _hasPrefix = false;
        _numInternalKeysSkipped = 0;
        _isKeySeqnumZero = false;
        _valid = false;
        _merging = false;
        _currentEntryIsMerged = false;
        _valueSource = ValueSource.None;
        _valueIsEntity = false;
        _valueIsBlob = false;
    }

    private void MergeWithNoBaseValue()
    {
        _valid = false;
        _mergeResult.Clear();
        MergeHelper.TimedFullMerge(_mergeOperator!, _savedKey.UserKey, false, default, _operands.AsOperands(), _mergeResult);
        _valueSource = ValueSource.Merged;
        _valueIsEntity = false;
        _valueIsBlob = false;
        _valid = true;
    }

    private void MergeWithPlainBaseValue(ReadOnlySpan<byte> value)
    {
        _valid = false;
        _mergeResult.Clear();
        MergeHelper.TimedFullMerge(_mergeOperator!, _savedKey.UserKey, true, value, _operands.AsOperands(), _mergeResult);
        _valueSource = ValueSource.Merged;
        _valueIsEntity = false;
        _valueIsBlob = false;
        _valid = true;
    }

    /// <summary>
    /// Port of DBIter::MergeWithWideColumnBaseValue: the operands are applied to the entity's default column; the result
    /// is the entity with that column replaced (Value = the merged default column).
    /// </summary>
    private void MergeWithEntityBaseValue(ReadOnlySpan<byte> entity)
    {
        _valid = false;
        _mergeResult.Clear();
        MergeHelper.TimedFullMerge(_mergeOperator!, _savedKey.UserKey, true, Wide.WideColumnSerialization.GetValueOfDefaultColumn(entity),
            _operands.AsOperands(), _mergeResult);
        _mergedEntity ??= new PooledBufferWriter(128);
        _mergedEntity.Clear();
        Wide.WideColumnSerialization.ReplaceDefaultColumn(entity, _mergeResult.WrittenSpan, _mergedEntity);
        _valueSource = ValueSource.Merged;
        _valueIsEntity = true;
        _valueIsBlob = false;
        _valid = true;
    }

    // ==================================================================== forward (FindNextUserEntry)

    private void BeginFindNextUserEntry(bool skippingSavedKey)
    {
        Debug.Assert(_forward);
        _skipping = skippingSavedKey;
        _numSkipped = 0;
        _reseekDone = false;
        _merging = false;
        _currentEntryIsMerged = false;
        _valid = false;
    }

    /// <summary>
    /// One iteration of DBIter::FindNextUserEntryInternal's loop over the entry the inner iterator is on.
    /// Returns what to do with the inner iterator next.
    /// </summary>
    private StepAction FindNextUserEntryStep()
    {
        // Will update _isKeySeqnumZero as soon as we parsed the current key but we need to save the previous value.
        bool isPrevKeySeqnumZero = _isKeySeqnumZero;
        ReadOnlySpan<byte> userKey = ParseCurrent(out ulong seq, out EntryType type);
        _isKeySeqnumZero = seq == 0;

        if (_hasUpper && _icmp.CompareUserKey(userKey, _upper.Span) >= 0)
        {
            _valid = false;
            return StepAction.Done;
        }

        if (_hasPrefix && !PrefixCheck(userKey))
        {
            // Only prefix_same_as_start sets a prefix (tombstone-run conversion is not ported): stop at its end.
            _valid = false;
            return StepAction.Done;
        }

        TooManyInternalKeysSkipped(true);

        if (seq <= _sequence)
        {
            // If the previous entry is of seqnum 0, the current entry will not possibly be skipped.
            if (!isPrevKeySeqnumZero && _skipping && _icmp.CompareUserKey(userKey, _savedKey.UserKey) <= 0)
            {
                _numSkipped++; // skip this entry
            }
            else
            {
                _numSkipped = 0;
                _reseekDone = false;
                switch (type)
                {
                    case EntryType.Deletion:
                    case EntryType.DeletionWithTimestamp:
                    case EntryType.SingleDeletion:
                        // Arrange to skip all upcoming entries for this key since they are hidden by this deletion.
                        _savedKey.SetUserKey(userKey);
                        _skipping = true;
                        break;
                    case EntryType.Value:
                    case EntryType.ValuePreferredSeqno:
                    case EntryType.BlobIndex:
                    case EntryType.WideColumnEntity:
                        _savedKey.SetUserKey(userKey);
                        if (IsCoveredByRangeTombstone(userKey, seq))
                        {
                            // Covered by a range tombstone: behaves like a deletion.
                            _skipping = true;
                            break;
                        }
                        _valueSource = type == EntryType.ValuePreferredSeqno ? ValueSource.IterPacked : ValueSource.Iter;
                        SetValueKind(type);
                        _valid = true;
                        return StepAction.Done;
                    case EntryType.Merge:
                        _savedKey.SetUserKey(userKey);
                        if (IsCoveredByRangeTombstone(userKey, seq))
                        {
                            _skipping = true;
                            break;
                        }
                        // By now, we are sure the current ikey is going to yield a value.
                        if (_mergeOperator is null) ThrowNoMergeOperator();
                        _currentEntryIsMerged = true;
                        _valid = true;
                        // MergeValuesNewToOld: start the merge process by pushing the first operand.
                        _operands.Clear();
                        _operands.PushOlder(_iter.Value);
                        _merging = true;
                        return StepAction.Next;
                    default:
                        ThrowUnknownType(type);
                        break;
                }
            }
        }
        else
        {
            // This key was inserted after our snapshot was taken. If this happens too many times in a row for the
            // same user key, we want to seek to the target sequence number.
            int cmp = _icmp.CompareUserKey(userKey, _savedKey.UserKey);
            if (cmp == 0 || (_skipping && cmp < 0))
            {
                _numSkipped++;
            }
            else
            {
                _savedKey.SetUserKey(userKey);
                _skipping = false;
                _numSkipped = 0;
                _reseekDone = false;
            }
        }

        // If we have sequentially iterated via numerous equal keys, then it's better to seek so that we can avoid
        // too many key comparisons. To avoid infinite loops, do not reseek if we have already attempted to reseek.
        if (_numSkipped > _maxSkip && !_reseekDone)
        {
            _isKeySeqnumZero = false;
            _numSkipped = 0;
            _reseekDone = true;
            if (_skipping)
            {
                // We're looking for the next user-key but all we see are the same user-key with decreasing sequence
                // numbers. Fast forward to sequence number 0 and type deletion (the smallest type).
                SetSeekKey(_savedKey.UserKey, 0, EntryType.Deletion);
            }
            else
            {
                // We saw multiple entries with this user key and sequence numbers higher than sequence_. Fast
                // forward to sequence_.
                SetSeekKey(_savedKey.UserKey, _sequence, DbFormat.ValueTypeForSeek);
            }
            return StepAction.Seek;
        }
        return StepAction.Next;
    }

    /// <summary>One iteration of DBIter::MergeValuesNewToOld's loop (PRE: the first operand was pushed).</summary>
    private StepAction MergeStep()
    {
        ReadOnlySpan<byte> userKey = ParseCurrent(out ulong seq, out EntryType type);
        if (!_icmp.EqualUserKey(userKey, _savedKey.UserKey))
        {
            // hit the next user key, stop right here
            _merging = false;
            MergeWithNoBaseValue();
            return StepAction.Done;
        }
        switch (type)
        {
            case EntryType.Deletion:
            case EntryType.SingleDeletion:
            case EntryType.DeletionWithTimestamp:
                // hit a delete with the same user key, stop right here; iter_ is positioned after delete
                _merging = false;
                MergeWithNoBaseValue();
                return StepAction.NextThenDone;
            case EntryType.Value:
            case EntryType.ValuePreferredSeqno:
                _merging = false;
                if (IsCoveredByRangeTombstone(userKey, seq))
                {
                    MergeWithNoBaseValue();
                    return StepAction.NextThenDone;
                }
                // hit a put, merge the put value with operands and store the final result. We are done!
                ReadOnlySpan<byte> value = _iter.Value;
                MergeWithPlainBaseValue(type == EntryType.ValuePreferredSeqno ? ParsePackedValueForValue(value) : value);
                return StepAction.NextThenDone;
            case EntryType.Merge:
                if (IsCoveredByRangeTombstone(userKey, seq))
                {
                    _merging = false;
                    MergeWithNoBaseValue();
                    return StepAction.NextThenDone;
                }
                // hit a merge, add the value as an operand and continue.
                _operands.PushOlder(_iter.Value);
                return StepAction.Next;
            case EntryType.BlobIndex:
            case EntryType.WideColumnEntity:
                _merging = false;
                if (IsCoveredByRangeTombstone(userKey, seq))
                {
                    MergeWithNoBaseValue();
                    return StepAction.NextThenDone;
                }
                if (type == EntryType.BlobIndex)
                {
                    MergeWithPlainBaseValue(FetchBlobForMerge(_iter.Value));
                    return StepAction.NextThenDone;
                }
                MergeWithEntityBaseValue(_iter.Value);
                return StepAction.NextThenDone;
            default:
                _merging = false;
                ThrowUnknownType(type);
                return StepAction.Done;
        }
    }

    /// <summary>The inner iterator ran out during FindNextUserEntry / MergeValuesNewToOld.</summary>
    private void ForwardExhausted()
    {
        if (_merging)
        {
            // we exhausted all internal keys under this user key: merge with no base value.
            _merging = false;
            MergeWithNoBaseValue();
        }
        else
        {
            _valid = false;
        }
    }

    /// <summary>Sync driver of the forward state machine. PRE: the inner iterator is valid.</summary>
    private void RunForward()
    {
        while (true)
        {
            StepAction a = _merging ? MergeStep() : FindNextUserEntryStep();
            switch (a)
            {
                case StepAction.Done:
                    return;
                case StepAction.Seek:
                    _iter.Seek(_seekKey.WrittenSpan);
                    break;
                default:
                    _iter.Next();
                    if (a == StepAction.NextThenDone) return;
                    break;
            }
            if (!_iter.Valid)
            {
                ForwardExhausted();
                return;
            }
        }
    }

    /// <summary>Async driver of the forward state machine; completes synchronously when the inner iterator does.</summary>
    private ValueTask RunForwardAsync(CancellationToken ct)
    {
        while (true)
        {
            StepAction a = _merging ? MergeStep() : FindNextUserEntryStep();
            if (a == StepAction.Done) return ValueTask.CompletedTask;
            ValueTask t = a == StepAction.Seek ? _iter.SeekAsync(_seekKey.WrittenMemory, ct) : _iter.NextAsync(ct);
            if (!t.IsCompletedSuccessfully) return RunForwardSlowAsync(t, a, ct);
            t.GetAwaiter().GetResult();
            if (a == StepAction.NextThenDone) return ValueTask.CompletedTask;
            if (!_iter.Valid)
            {
                ForwardExhausted();
                return ValueTask.CompletedTask;
            }
        }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask RunForwardSlowAsync(ValueTask pending, StepAction a, CancellationToken ct)
    {
        await pending.ConfigureAwait(false);
        if (a == StepAction.NextThenDone) return;
        if (!_iter.Valid)
        {
            ForwardExhausted();
            return;
        }
        await RunForwardAsync(ct).ConfigureAwait(false);
    }

    // ==================================================================== public positioning (sync)

    public void Next()
    {
        if (!_valid) ThrowHelper.InvalidArgument("Next on an invalid iterator");
        _numInternalKeysSkipped = 0;
        if (!_forward)
        {
            _isKeySeqnumZero = false;
            ReverseToForward();
        }
        else if (!_currentEntryIsMerged)
        {
            // If the current value is not a merge, the iter position is the current key, which is already returned.
            // We can safely issue a Next() without checking the current key. If the current key is a merge, very
            // likely iter already points to the next internal position.
            Debug.Assert(_iter.Valid);
            _iter.Next();
        }
        if (_iter.Valid)
        {
            BeginFindNextUserEntry(skippingSavedKey: true);
            RunForward();
        }
        else
        {
            _isKeySeqnumZero = false;
            _valid = false;
        }
    }

    public void Prev()
    {
        if (!_valid) ThrowHelper.InvalidArgument("Prev on an invalid iterator");
        _numInternalKeysSkipped = 0;
        if (_forward) ReverseToBackward();
        PrevInternal();
    }

    public void Seek(ReadOnlySpan<byte> target)
    {
        ResetSeekState();
        SetSavedKeyToSeekTarget(target);
        _forward = true;
        _iter.Seek(_seekKey.WrittenSpan);
        if (!_iter.Valid) return;
        // Now the inner iterator is placed to the target position. From there, we need to find out the next key
        // that is visible to the user.
        MaybeSetPrefix(target);
        BeginFindNextUserEntry(skippingSavedKey: false);
        RunForward();
        if (!_valid) _hasPrefix = false;
    }

    public void SeekForPrev(ReadOnlySpan<byte> target)
    {
        ResetSeekState();
        SetSavedKeyToSeekForPrevTarget(target);
        _forward = false;
        _iter.SeekForPrev(_seekKey.WrittenSpan);
        if (!_iter.Valid) return;
        MaybeSetPrefix(target);
        PrevInternal();
        if (!_valid) _hasPrefix = false;
    }

    public void SeekToFirst()
    {
        if (_hasLower)
        {
            Seek(_lower.Span);
            return;
        }
        // Don't use iter_::Seek() if we set a prefix extractor because prefix seek will be used.
        if (!_expectTotalOrderInnerIter) _maxSkip = ulong.MaxValue;
        ResetSeekState();
        _forward = true;
        _iter.SeekToFirst();
        if (!_iter.Valid) return;
        _savedKey.SetUserKey(DbFormat.ExtractUserKey(_iter.Key));
        BeginFindNextUserEntry(skippingSavedKey: false);
        RunForward();
        if (_valid) MaybeSetPrefix(_savedKey.UserKey);
    }

    public void SeekToLast()
    {
        if (_hasUpper)
        {
            // Seek to last key strictly less than ReadOptions.iterate_upper_bound.
            SeekForPrev(_upper.Span);
            return;
        }
        // Don't use iter_::Seek() if we set a prefix extractor because prefix seek will be used.
        if (!_expectTotalOrderInnerIter) _maxSkip = ulong.MaxValue;
        ResetSeekState();
        _forward = false;
        _iter.SeekToLast();
        PrevInternal();
        if (_valid) MaybeSetPrefix(_savedKey.UserKey);
    }

    /// <summary>Port of SetSavedKeyToSeekTarget (clamped to the lower bound).</summary>
    private void SetSavedKeyToSeekTarget(ReadOnlySpan<byte> target)
    {
        if (_hasLower && _icmp.CompareUserKey(target, _lower.Span) < 0) target = _lower.Span; // seek key is smaller than the lower bound
        _savedKey.SetUserKey(target);
        SetSeekKey(target, _sequence, DbFormat.ValueTypeForSeek);
    }

    /// <summary>Port of SetSavedKeyToSeekForPrevTarget (clamped below the upper bound).</summary>
    private void SetSavedKeyToSeekForPrevTarget(ReadOnlySpan<byte> target)
    {
        if (_hasUpper && _icmp.CompareUserKey(target, _upper.Span) >= 0)
        {
            SetSeekKey(_upper.Span, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeekForPrev);
        }
        else
        {
            SetSeekKey(target, 0, DbFormat.ValueTypeForSeekForPrev);
        }
    }

    // ==================================================================== public positioning (async)

    /// <summary>Async <see cref="Seek"/>: completes synchronously unless a table block must be read.</summary>
    public ValueTask SeekAsync(ReadOnlyMemory<byte> target, CancellationToken cancellationToken = default)
    {
        ResetSeekState();
        SetSavedKeyToSeekTarget(target.Span);
        _forward = true;
        ValueTask t = _iter.SeekAsync(_seekKey.WrittenMemory, cancellationToken);
        if (!t.IsCompletedSuccessfully)
        {
            MaybeSetPrefix(target.Span);
            return AfterSeekSlowAsync(t, false, cancellationToken);
        }
        t.GetAwaiter().GetResult();
        if (!_iter.Valid) return ValueTask.CompletedTask;
        MaybeSetPrefix(target.Span);
        BeginFindNextUserEntry(skippingSavedKey: false);
        return RunForwardAsync(cancellationToken);
    }

    /// <summary>Async <see cref="SeekToFirst"/>.</summary>
    public ValueTask SeekToFirstAsync(CancellationToken cancellationToken = default)
    {
        if (_hasLower) return SeekAsync(_lower, cancellationToken);
        if (_prefixSameAsStart)
        {
            // The prefix is taken from the first key found (rare combination; use the sync path).
            SeekToFirst();
            return ValueTask.CompletedTask;
        }
        if (!_expectTotalOrderInnerIter) _maxSkip = ulong.MaxValue;
        ResetSeekState();
        _forward = true;
        ValueTask t = _iter.SeekToFirstAsync(cancellationToken);
        if (!t.IsCompletedSuccessfully) return AfterSeekSlowAsync(t, true, cancellationToken);
        t.GetAwaiter().GetResult();
        if (!_iter.Valid) return ValueTask.CompletedTask;
        _savedKey.SetUserKey(DbFormat.ExtractUserKey(_iter.Key));
        BeginFindNextUserEntry(skippingSavedKey: false);
        return RunForwardAsync(cancellationToken);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask AfterSeekSlowAsync(ValueTask pending, bool savedKeyFromFirstEntry, CancellationToken ct)
    {
        await pending.ConfigureAwait(false);
        if (!_iter.Valid) return;
        // For SeekToFirst the saved key starts at the first key; for Seek it already holds the target.
        if (savedKeyFromFirstEntry) _savedKey.SetUserKey(DbFormat.ExtractUserKey(_iter.Key));
        BeginFindNextUserEntry(skippingSavedKey: false);
        await RunForwardAsync(ct).ConfigureAwait(false);
    }

    /// <summary>Async <see cref="Next"/>: completes synchronously unless a table block must be read.</summary>
    public ValueTask NextAsync(CancellationToken cancellationToken = default)
    {
        if (!_valid) ThrowHelper.InvalidArgument("Next on an invalid iterator");
        _numInternalKeysSkipped = 0;
        if (!_forward)
        {
            _isKeySeqnumZero = false;
            ReverseToForward();
            return ContinueNextAsync(cancellationToken);
        }
        if (!_currentEntryIsMerged)
        {
            ValueTask t = _iter.NextAsync(cancellationToken);
            if (!t.IsCompletedSuccessfully) return NextSlowAsync(t, cancellationToken);
            t.GetAwaiter().GetResult();
        }
        return ContinueNextAsync(cancellationToken);
    }

    private ValueTask ContinueNextAsync(CancellationToken ct)
    {
        if (_iter.Valid)
        {
            BeginFindNextUserEntry(skippingSavedKey: true);
            return RunForwardAsync(ct);
        }
        _isKeySeqnumZero = false;
        _valid = false;
        return ValueTask.CompletedTask;
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask NextSlowAsync(ValueTask pending, CancellationToken ct)
    {
        await pending.ConfigureAwait(false);
        await ContinueNextAsync(ct).ConfigureAwait(false);
    }

    /// <summary>Reverse movement is synchronous (see remarks).</summary>
    public ValueTask PrevAsync(CancellationToken cancellationToken = default)
    {
        Prev();
        return ValueTask.CompletedTask;
    }

    /// <summary>Reverse movement is synchronous (see remarks).</summary>
    public ValueTask SeekForPrevAsync(ReadOnlyMemory<byte> target, CancellationToken cancellationToken = default)
    {
        SeekForPrev(target.Span);
        return ValueTask.CompletedTask;
    }

    /// <summary>Reverse movement is synchronous (see remarks).</summary>
    public ValueTask SeekToLastAsync(CancellationToken cancellationToken = default)
    {
        SeekToLast();
        return ValueTask.CompletedTask;
    }

    // ==================================================================== direction changes

    /// <summary>Port of DBIter::ReverseToForward (total-order inner iterator).</summary>
    private void ReverseToForward()
    {
        // When moving backwards, iter_ is positioned on the _previous_ key, which may not exist or may have different
        // prefix than the current key(). If that's the case, seek iter_ to current key.
        if (!_expectTotalOrderInnerIter || !_iter.Valid)
        {
            SetSeekKey(_savedKey.UserKey, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
            _iter.Seek(_seekKey.WrittenSpan);
        }
        _forward = true;
        // Skip keys less than the current key() (a.k.a. saved_key_).
        while (_iter.Valid)
        {
            ReadOnlySpan<byte> userKey = ParseCurrent(out _, out _);
            if (_icmp.CompareUserKey(userKey, _savedKey.UserKey) >= 0) return;
            _iter.Next();
        }
    }

    /// <summary>Port of DBIter::ReverseToBackward: move iter_ to the key before saved_key_.</summary>
    private void ReverseToBackward()
    {
        // When current_entry_is_merged_ is true, iter_ may be positioned on the next key, which may not exist. If
        // that's the case, seek to saved_key_ (kMaxSequenceNumber + kValueTypeForSeek: strictly before its entries).
        if (_currentEntryIsMerged && (!_expectTotalOrderInnerIter || !_iter.Valid))
        {
            SetSeekKey(_savedKey.UserKey, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
            if (!_expectTotalOrderInnerIter)
            {
                _iter.SeekForPrev(_seekKey.WrittenSpan);
            }
            else
            {
                // Some iterators may not support SeekForPrev(), so we avoid using it when prefix seek mode is disabled.
                _iter.Seek(_seekKey.WrittenSpan);
                if (!_iter.Valid) _iter.SeekToLast();
            }
        }
        _forward = false;
        FindUserKeyBeforeSavedKey();
    }

    // ==================================================================== reverse

    /// <summary>Port of DBIter::PrevInternal.</summary>
    private void PrevInternal()
    {
        while (_iter.Valid)
        {
            _savedKey.SetUserKey(DbFormat.ExtractUserKey(_iter.Key));
            if (_hasPrefix && !PrefixCheck(_savedKey.UserKey))
            {
                // prefix_same_as_start: we've iterated past the seek prefix.
                _valid = false;
                return;
            }
            if (_hasLower && _icmp.CompareUserKey(_savedKey.UserKey, _lower.Span) < 0)
            {
                // We've iterated earlier than the user-specified lower bound.
                _valid = false;
                return;
            }

            FindValueForCurrentKey(); // assigns _valid
            // Whether or not we found a value for current key, we need iter_ to end up on a smaller key.
            FindUserKeyBeforeSavedKey();
            if (_valid) return; // Found the value.
            TooManyInternalKeysSkipped(false);
        }
        // We haven't found any key - iterator is not valid
        _valid = false;
    }

    /// <summary>
    /// Port of DBIter::FindValueForCurrentKey. PRE: iter_ is on the last entry with user key saved_key_. Walks the
    /// entries of saved_key_ from oldest to newest visible, tracking the newest put/delete and the merge operands
    /// stacked on top of it.
    /// </summary>
    private void FindValueForCurrentKey()
    {
        Debug.Assert(_iter.Valid);
        _operands.Clear();
        _currentEntryIsMerged = false;
        _valid = false;
        // last entry before merge (could be a deletion, a value or a range deletion)
        EntryType lastNotMergeType = EntryType.Deletion;
        EntryType lastKeyEntryType = EntryType.Deletion;
        bool validEntrySeen = false;
        ulong numSkipped = 0;

        while (_iter.Valid)
        {
            ReadOnlySpan<byte> userKey = ParseCurrent(out ulong seq, out EntryType type);
            if (!_icmp.EqualUserKey(userKey, _savedKey.UserKey))
            {
                // Found a smaller user key, thus we are done with current user key.
                break;
            }
            if (seq > _sequence)
            {
                // Found an invisible version of the current user key, and it must have a higher sequence number.
                break;
            }

            TooManyInternalKeysSkipped(true);

            // This user key has lots of entries. We're going from old to new, and it's taking too long. Let's do a
            // Seek() and go from new to old. This helps when a key was overwritten many times.
            if (numSkipped >= _maxSkip)
            {
                FindValueForCurrentKeyUsingSeek();
                return;
            }

            validEntrySeen = true;
            lastKeyEntryType = type;
            switch (type)
            {
                case EntryType.Value:
                case EntryType.ValuePreferredSeqno:
                case EntryType.BlobIndex:
                case EntryType.WideColumnEntity:
                    if (IsCoveredByRangeTombstone(userKey, seq))
                    {
                        lastKeyEntryType = EntryType.RangeDeletion;
                        lastNotMergeType = EntryType.RangeDeletion;
                        _operands.Clear();
                        break;
                    }
                    // The child iterator moves on: keep a copy (upstream pins the block instead).
                    ReadOnlySpan<byte> v = _iter.Value;
                    _pinned.Clear();
                    _pinned.Write(type == EntryType.ValuePreferredSeqno ? ParsePackedValueForValue(v) : v);
                    _operands.Clear();
                    lastNotMergeType = type;
                    break;
                case EntryType.Deletion:
                case EntryType.DeletionWithTimestamp:
                case EntryType.SingleDeletion:
                    _operands.Clear();
                    lastNotMergeType = type;
                    break;
                case EntryType.Merge:
                    if (IsCoveredByRangeTombstone(userKey, seq))
                    {
                        lastKeyEntryType = EntryType.RangeDeletion;
                        lastNotMergeType = EntryType.RangeDeletion;
                        _operands.Clear();
                        break;
                    }
                    if (_mergeOperator is null) ThrowNoMergeOperator();
                    _operands.PushNewer(_iter.Value);
                    break;
                default:
                    ThrowUnknownType(type);
                    break;
            }

            _iter.Prev();
            ++numSkipped;
        }

        if (!validEntrySeen)
        {
            _valid = false;
            return;
        }

        switch (lastKeyEntryType)
        {
            case EntryType.Deletion:
            case EntryType.DeletionWithTimestamp:
            case EntryType.SingleDeletion:
            case EntryType.RangeDeletion:
                _valid = false;
                return;
            case EntryType.Merge:
                _currentEntryIsMerged = true;
                if (lastNotMergeType is EntryType.Value or EntryType.ValuePreferredSeqno) MergeWithPlainBaseValue(_pinned.WrittenSpan);
                else if (lastNotMergeType == EntryType.BlobIndex) MergeWithPlainBaseValue(FetchBlobForMerge(_pinned.WrittenSpan));
                else if (lastNotMergeType == EntryType.WideColumnEntity) MergeWithEntityBaseValue(_pinned.WrittenSpan);
                else MergeWithNoBaseValue();
                return;
            default: // Value / ValuePreferredSeqno / WideColumnEntity / BlobIndex
                _valueSource = ValueSource.Pinned;
                SetValueKind(lastKeyEntryType);
                _valid = true;
                return;
        }
    }

    /// <summary>
    /// Port of DBIter::FindValueForCurrentKeyUsingSeek: Seek to (saved_key_, sequence_) and resolve the key moving
    /// forward (new to old) instead of walking many old versions with Prev().
    /// </summary>
    private void FindValueForCurrentKeyUsingSeek()
    {
        SetSeekKey(_savedKey.UserKey, _sequence, DbFormat.ValueTypeForSeek);
        _iter.Seek(_seekKey.WrittenSpan);

        ulong seq;
        EntryType type;
        ReadOnlySpan<byte> userKey;
        while (true)
        {
            if (!_iter.Valid)
            {
                _valid = false;
                return;
            }
            userKey = ParseCurrent(out seq, out type);
            if (!_icmp.EqualUserKey(userKey, _savedKey.UserKey))
            {
                // No visible values for this key, even though FindValueForCurrentKey() has seen some (possible with a
                // tailing iterator when entries were compacted away).
                _valid = false;
                return;
            }
            if (seq <= _sequence) break;
            _iter.Next();
        }

        if (type is EntryType.Deletion or EntryType.SingleDeletion or EntryType.DeletionWithTimestamp ||
            IsCoveredByRangeTombstone(userKey, seq))
        {
            _valid = false;
            return;
        }
        if (type is EntryType.Value or EntryType.ValuePreferredSeqno or EntryType.WideColumnEntity or EntryType.BlobIndex)
        {
            ReadOnlySpan<byte> v = _iter.Value;
            _pinned.Clear();
            _pinned.Write(type == EntryType.ValuePreferredSeqno ? ParsePackedValueForValue(v) : v);
            _valueSource = ValueSource.Pinned;
            SetValueKind(type);
            _valid = true;
            return;
        }
        if (type != EntryType.Merge) ThrowUnknownType(type);

        // kTypeMerge. We need to collect all kTypeMerge values and save them in operands.
        if (_mergeOperator is null) ThrowNoMergeOperator();
        _currentEntryIsMerged = true;
        _operands.Clear();
        _operands.PushOlder(_iter.Value);
        while (true)
        {
            _iter.Next();
            if (!_iter.Valid) break;
            userKey = ParseCurrent(out seq, out type);
            if (!_icmp.EqualUserKey(userKey, _savedKey.UserKey)) break;
            if (type is EntryType.Deletion or EntryType.SingleDeletion or EntryType.DeletionWithTimestamp ||
                IsCoveredByRangeTombstone(userKey, seq))
            {
                break;
            }
            if (type is EntryType.Value or EntryType.ValuePreferredSeqno)
            {
                ReadOnlySpan<byte> v = _iter.Value;
                MergeWithPlainBaseValue(type == EntryType.ValuePreferredSeqno ? ParsePackedValueForValue(v) : v);
                return;
            }
            if (type == EntryType.Merge)
            {
                _operands.PushOlder(_iter.Value);
                continue;
            }
            if (type == EntryType.WideColumnEntity)
            {
                MergeWithEntityBaseValue(_iter.Value);
                return;
            }
            if (type is EntryType.BlobIndex)
            {
                MergeWithPlainBaseValue(FetchBlobForMerge(_iter.Value));
                return;
            }
            ThrowUnknownType(type);
        }

        MergeWithNoBaseValue();

        // Make sure we leave iter_ in a good state. If it's valid and we don't care about prefixes, that's already
        // good enough. Otherwise it needs to be seeked to the current key.
        if (!_expectTotalOrderInnerIter || !_iter.Valid)
        {
            SetSeekKey(_savedKey.UserKey, _sequence, DbFormat.ValueTypeForSeek);
            if (!_expectTotalOrderInnerIter)
            {
                _iter.SeekForPrev(_seekKey.WrittenSpan);
            }
            else
            {
                _iter.Seek(_seekKey.WrittenSpan);
                if (!_iter.Valid) _iter.SeekToLast();
            }
        }

        _valid = true;
    }

    /// <summary>Port of DBIter::FindUserKeyBeforeSavedKey: move backwards until the key smaller than saved_key_.</summary>
    private void FindUserKeyBeforeSavedKey()
    {
        ulong numSkipped = 0;
        while (_iter.Valid)
        {
            ReadOnlySpan<byte> userKey = ParseCurrent(out _, out _);
            if (_icmp.CompareUserKey(userKey, _savedKey.UserKey) < 0) return;

            TooManyInternalKeysSkipped(true);

            if (numSkipped >= _maxSkip)
            {
                numSkipped = 0;
                SetSeekKey(_savedKey.UserKey, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
                // It would be more efficient to use SeekForPrev() here, but some iterators may not support it.
                _iter.Seek(_seekKey.WrittenSpan);
                if (!_iter.Valid) break;
            }
            else
            {
                ++numSkipped;
            }
            _iter.Prev();
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _valid = false;
        _iter.Dispose();
        IDisposable? c = _cleanup;
        _cleanup = null;
        c?.Dispose();
        _savedKey.Dispose();
        _seekKey.Dispose();
        _pinned.Dispose();
        _prefix.Dispose();
        _mergeResult.Dispose();
        _operands.Dispose();
        _blobValue?.Dispose();
        _blobKey?.Dispose();
    }
}
