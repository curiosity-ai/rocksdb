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
/// <para><b>Not ported (yet).</b> Prefix seek / prefix_same_as_start (no prefix extractor exists; total order is
/// assumed - the hook is <see cref="ReadOptions.PrefixSameAsStart"/>, ignored like upstream without an extractor),
/// user-defined timestamps, read callbacks, blob values and wide-column entities (NotSupported), statistics/perf
/// context, tombstone-run conversion, multi-scan.</para>
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
    private readonly ulong _maxSkip;
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
    {
        ArgumentNullException.ThrowIfNull(comparator);
        ArgumentNullException.ThrowIfNull(iter);
        _icmp = comparator;
        _iter = iter;
        _sequence = sequence;
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
            switch (_valueSource)
            {
                case ValueSource.Iter: return _iter.Value;
                case ValueSource.IterPacked: return ParsePackedValueForValue(_iter.Value);
                case ValueSource.Pinned: return _pinned.WrittenSpan;
                case ValueSource.Merged: return _mergeResult.WrittenSpan;
                default: return ReadOnlySpan<byte>.Empty;
            }
        }
    }

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
        ThrowHelper.NotSupported(type == EntryType.BlobIndex ? "blob values are not supported by the iterator yet" : "wide-column entities are not supported by the iterator yet");
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

    private void ResetSeekState()
    {
        _numInternalKeysSkipped = 0;
        _isKeySeqnumZero = false;
        _valid = false;
        _merging = false;
        _currentEntryIsMerged = false;
        _valueSource = ValueSource.None;
    }

    private void MergeWithNoBaseValue()
    {
        _valid = false;
        _mergeResult.Clear();
        MergeHelper.TimedFullMerge(_mergeOperator!, _savedKey.UserKey, false, default, _operands.AsOperands(), _mergeResult);
        _valueSource = ValueSource.Merged;
        _valid = true;
    }

    private void MergeWithPlainBaseValue(ReadOnlySpan<byte> value)
    {
        _valid = false;
        _mergeResult.Clear();
        MergeHelper.TimedFullMerge(_mergeOperator!, _savedKey.UserKey, true, value, _operands.AsOperands(), _mergeResult);
        _valueSource = ValueSource.Merged;
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
                        if (type is EntryType.BlobIndex or EntryType.WideColumnEntity) ThrowNotSupportedType(type);
                        _valueSource = type == EntryType.ValuePreferredSeqno ? ValueSource.IterPacked : ValueSource.Iter;
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
                ThrowNotSupportedType(type);
                return StepAction.Done;
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
        BeginFindNextUserEntry(skippingSavedKey: false);
        RunForward();
    }

    public void SeekForPrev(ReadOnlySpan<byte> target)
    {
        ResetSeekState();
        SetSavedKeyToSeekForPrevTarget(target);
        _forward = false;
        _iter.SeekForPrev(_seekKey.WrittenSpan);
        if (!_iter.Valid) return;
        PrevInternal();
    }

    public void SeekToFirst()
    {
        if (_hasLower)
        {
            Seek(_lower.Span);
            return;
        }
        ResetSeekState();
        _forward = true;
        _iter.SeekToFirst();
        if (!_iter.Valid) return;
        _savedKey.SetUserKey(DbFormat.ExtractUserKey(_iter.Key));
        BeginFindNextUserEntry(skippingSavedKey: false);
        RunForward();
    }

    public void SeekToLast()
    {
        if (_hasUpper)
        {
            // Seek to last key strictly less than ReadOptions.iterate_upper_bound.
            SeekForPrev(_upper.Span);
            return;
        }
        ResetSeekState();
        _forward = false;
        _iter.SeekToLast();
        PrevInternal();
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
        if (!t.IsCompletedSuccessfully) return AfterSeekSlowAsync(t, false, cancellationToken);
        t.GetAwaiter().GetResult();
        if (!_iter.Valid) return ValueTask.CompletedTask;
        BeginFindNextUserEntry(skippingSavedKey: false);
        return RunForwardAsync(cancellationToken);
    }

    /// <summary>Async <see cref="SeekToFirst"/>.</summary>
    public ValueTask SeekToFirstAsync(CancellationToken cancellationToken = default)
    {
        if (_hasLower) return SeekAsync(_lower, cancellationToken);
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
        // When moving backwards, iter_ is positioned on the _previous_ key, which may not exist. If that's the case,
        // seek iter_ to current key.
        if (!_iter.Valid)
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
        if (_currentEntryIsMerged && !_iter.Valid)
        {
            SetSeekKey(_savedKey.UserKey, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
            _iter.Seek(_seekKey.WrittenSpan);
            if (!_iter.Valid) _iter.SeekToLast();
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
                    if (type is EntryType.BlobIndex or EntryType.WideColumnEntity) ThrowNotSupportedType(type);
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
                else MergeWithNoBaseValue();
                return;
            default: // Value / ValuePreferredSeqno
                _valueSource = ValueSource.Pinned;
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
        if (type is EntryType.Value or EntryType.ValuePreferredSeqno)
        {
            ReadOnlySpan<byte> v = _iter.Value;
            _pinned.Clear();
            _pinned.Write(type == EntryType.ValuePreferredSeqno ? ParsePackedValueForValue(v) : v);
            _valueSource = ValueSource.Pinned;
            _valid = true;
            return;
        }
        if (type is EntryType.BlobIndex or EntryType.WideColumnEntity) ThrowNotSupportedType(type);
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
            if (type is EntryType.BlobIndex or EntryType.WideColumnEntity) ThrowNotSupportedType(type);
            ThrowUnknownType(type);
        }

        MergeWithNoBaseValue();

        // Make sure we leave iter_ in a good state. If it's valid and we don't care about prefixes, that's already
        // good enough. Otherwise it needs to be seeked to the current key.
        if (!_iter.Valid)
        {
            SetSeekKey(_savedKey.UserKey, _sequence, DbFormat.ValueTypeForSeek);
            _iter.Seek(_seekKey.WrittenSpan);
            if (!_iter.Valid) _iter.SeekToLast();
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
        _mergeResult.Dispose();
        _operands.Dispose();
    }
}
