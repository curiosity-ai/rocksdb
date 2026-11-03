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
/// iterate_upper_bound, max_skippable_internal_keys (Incomplete). Range deletions: every visible entry is checked
/// against a <see cref="RangeDelAggregator"/> (pre-7.x scheme), and a covered entry lets the inner iterator skip the
/// covered range in one seek where it can prove the skipped entries are older than the tombstone (see
/// <see cref="MergingIterator"/>, <see cref="InternalIterator.SkipCoveredForward"/>).</para>
/// <para><b>Prefix seek.</b> With a prefix extractor and neither total_order_seek nor auto_prefix_mode, the inner
/// iterator is in prefix mode (tables skipped by their prefix filter): direction changes re-seek like upstream
/// (expect_total_order_inner_iter_ false) and the reseek optimization is disabled for SeekToFirst/SeekToLast.
/// prefix_same_as_start stops iteration at the end of the seek key's prefix (PrefixCheck).</para>
/// <para><b>User-defined timestamps.</b> With a timestamp-aware comparator, entries are visible when their sequence is
/// &lt;= the snapshot and their timestamp is in [iter_start_ts, timestamp] (IsVisible); <see cref="Key"/> excludes the
/// timestamp and <see cref="Timestamp"/> returns it. With iter_start_ts every visible version (deletions included) is
/// returned and <see cref="InternalKey"/> holds its internal key (upstream's key()).</para>
/// <para><b>Blob values</b> (integrated BlobDB, DBIter::SetValueAndColumnsFromBlob): a blob-index entry is resolved
/// lazily, when <see cref="Value"/> is first read (like allow_unprepared_value = true followed by PrepareValue), or
/// with async I/O by <see cref="PrepareValueAsync"/>; merges on top of a blob read the blob synchronously. Blob records
/// are looked up with the user key including its timestamp.</para>
/// <para><b>Not ported (yet).</b> Read callbacks, statistics/perf context, tombstone-run conversion, multi-scan.</para>
/// <para><b>Errors.</b> Upstream sets status() and invalidates the iterator; here the positioning call throws
/// <see cref="RocksDbException"/> (Corruption, InvalidArgument for a merge without operator, Incomplete when
/// max_skippable_internal_keys is exceeded) and the iterator is left invalid.</para>
/// <para><b>Memory.</b> <see cref="Key"/> is a copy kept in a reusable buffer (saved_key_); <see cref="Value"/> points
/// into the child iterator when moving forward over a plain value, into a data block (or memtable memory) pinned
/// by the <see cref="PinnedIteratorsManager"/> when moving backward (TempPinData / IsValuePinned; a copy when the child
/// cannot pin), or into a reusable merge-result buffer. Both spans are valid until the next
/// positioning call. Steady-state stepping does not allocate.</para>
/// <para><b>Async.</b> Every positioning call has an async variant that awaits the inner iterator and completes
/// synchronously when no I/O is needed. Forward and reverse movement are step functions shared with the sync path
/// (the forward FindNextUserEntry / MergeValuesNewToOld steps, and the reverse state machine of ReverseToBackward /
/// PrevInternal / FindValueForCurrentKey(+UsingSeek) / FindUserKeyBeforeSavedKey); the direction switch of
/// <see cref="NextAsync"/> after reverse movement (ReverseToForward) awaits the inner iterator too.</para>
/// <para>Not thread-safe.</para>
/// </remarks>
public sealed class DbIter : IDisposable
{
    private enum StepAction : byte { Done, Next, Seek, NextThenDone, SkipCovered }
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

    // User-defined timestamps (timestamp_size_, timestamp_ub_, timestamp_lb_, saved_timestamp_).
    private readonly int _tsSize;
    private readonly Comparator _ucmp;
    private readonly byte[]? _tsUb;
    private readonly byte[]? _tsLb;
    private readonly byte[]? _savedTimestamp;

    private readonly IterKey _savedKey = new();
    private readonly PooledBufferWriter _seekKey = new(64);
    private readonly PooledBufferWriter _pinned = new(128);   // saved_value_: copy of a value that is not pinned
    private PinnedSlice _pinnedSlice;                          // pinned_value_: a value in a pinned block / memtable
    private bool _pinnedIsSlice;                               // the Pinned value source is _pinnedSlice
    private readonly PinnedIteratorsManager _pinMgr = new();   // pinned_iters_mgr_
    private readonly bool _pinThruLifetime;                    // pin_thru_lifetime_ (ReadOptions::pin_data)
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
    // FindValueForCurrentKey (reverse): merge operands referenced in the pinned blocks / memtable instead of copied
    // (merge_context_.PushOperand(iter_.value(), iter_.iter()->IsValuePinned())), oldest first. Used while every
    // operand of the key could be pinned (_pinnedOperandsOnly); otherwise they are copied into _operands.
    private PinnedSlice[] _pinnedOperands = [];
    private int _pinnedOperandCount;
    private bool _pinnedOperandsOnly = true;

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
        // Covered-range skipping needs total order (a prefix-mode seek may stop at the prefix end) and plain seqno
        // visibility (no user-defined timestamps; read callbacks disable it in SetReadCallback).
        _skipCovered = _rangeDel is not null && comparator.UserComparator.TimestampSize == 0 && _expectTotalOrderInnerIter;
        if (_skipCovered) _skipTarget = new PooledBufferWriter(64);
        // DBIter::SetIter: iter_.iter()->SetPinnedItersMgr(&pinned_iters_mgr_); with pin_data everything is pinned until
        // the iterator is destroyed.
        _iter.SetPinnedItersMgr(_pinMgr);
        _pinThruLifetime = readOptions?.PinData == true;
        if (_pinThruLifetime) _pinMgr.StartPinning();
        _ucmp = comparator.UserComparator;
        _tsSize = _ucmp.TimestampSize;
        if (_tsSize > 0)
        {
            // timestamp_ub_: the read timestamp (the maximum timestamp when none was given).
            _tsUb = readOptions?.Timestamp is ReadOnlyMemory<byte> ub ? ub.ToArray() : _ucmp.MaxTimestamp.ToArray();
            _tsLb = readOptions?.IterStartTs is ReadOnlyMemory<byte> lb ? lb.ToArray() : null;
            if (_tsUb.Length != _tsSize || (_tsLb is not null && _tsLb.Length != _tsSize)) ThrowHelper.InvalidArgument("Timestamp size mismatch");
            _savedTimestamp = new byte[_tsSize];
        }
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
        ValueSource.Pinned => PinnedSpan,
        _ => default,
    };

    /// <summary>The saved value of a reverse step: pinned_value_ when the child value was pinned, else saved_value_.</summary>
    private ReadOnlySpan<byte> PinnedSpan
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => _pinnedIsSlice ? _pinnedSlice.Span : _pinned.WrittenSpan;
    }

    /// <summary>
    /// Saves the value the inner iterator is on for use after it moves (FindValueForCurrentKey): the value itself when
    /// the child pins it (IsValuePinned: memtable memory, or a data block the pinned iterators manager keeps alive),
    /// else a copy. Strips the packed seqno of kTypeValuePreferredSeqno.
    /// </summary>
    private void SaveCurrentValue(EntryType type)
    {
        int trim = type == EntryType.ValuePreferredSeqno ? sizeof(ulong) : 0;
        if (_iter.TryGetPinnedValue(out PinnedSlice pv))
        {
            _pinnedSlice = trim == 0 ? pv : pv.TrimEnd(trim);
            _pinnedIsSlice = true;
            return;
        }
        ReadOnlySpan<byte> v = _iter.Value;
        _pinned.Clear();
        _pinned.Write(v[..^trim]);
        _pinnedIsSlice = false;
    }

    /// <summary>Port of DBIter::TempPinData: pin the blocks the inner iterators leave until <see cref="ReleaseTempPinnedData"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void TempPinData()
    {
        if (!_pinThruLifetime) _pinMgr.StartPinning();
    }

    /// <summary>Port of DBIter::ReleaseTempPinnedData: called at the start of every positioning call.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ReleaseTempPinnedData()
    {
        if (!_pinThruLifetime && _pinMgr.IsPinning) _pinMgr.ReleasePinnedData();
    }

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
        _entityResolved = false;
        _valueIsEntity = type == EntryType.WideColumnEntity;
        _valueIsBlob = type == EntryType.BlobIndex;
        _blobFetched = false;
        if (_valueIsBlob && _blobSource is null) ThrowNotSupportedType(type);
    }

    private PooledBufferWriter? _resolvedEntity;
    private PooledBufferWriter? _entityFetchScratch;
    private List<Wide.WideColumnSlot>? _entitySlots;
    private bool _entityResolved;

    /// <summary>
    /// DBIter::SetValueAndColumnsFromEntity for a version 2 entity with blob-backed columns: the entity with its blob
    /// columns resolved (WideColumnSerialization::ResolveEntityBlobColumns), computed once per position;
    /// <paramref name="raw"/> itself when it has no blob columns.
    /// </summary>
    private ReadOnlySpan<byte> ResolveEntity(ReadOnlySpan<byte> raw)
    {
        if (_entityResolved) return _resolvedEntity!.WrittenSpan;
        if (!Wide.WideColumnSerialization.HasBlobColumns(raw)) return raw;
        if (_blobSource is null) ThrowHelper.Corruption("Cannot resolve blob columns in entity without a blob fetcher");
        PooledBufferWriter dst = _resolvedEntity ??= new PooledBufferWriter(256);
        dst.Clear();
        var fetcher = new Blob.VersionBlobFetcher(_blobSource!, _blobVersion!, _readOptions);
        try
        {
            Wide.WideColumnSerialization.ResolveEntityBlobColumns(raw, _savedKey.UserKey, ref fetcher, _entitySlots ??= new List<Wide.WideColumnSlot>(8),
                _entityFetchScratch ??= new PooledBufferWriter(256), dst);
        }
        catch
        {
            _valid = false;
            throw;
        }
        _entityResolved = true;
        return dst.WrittenSpan;
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
            ReadOnlySpan<byte> k = _savedKey.UserKey;
            return _tsSize == 0 ? k : k[..^_tsSize];
        }
    }

    /// <summary>
    /// The user-defined timestamp of the current entry (DBIter::timestamp()). REQUIRES: <see cref="Valid"/>; empty when
    /// the comparator has no timestamp.
    /// </summary>
    public ReadOnlySpan<byte> Timestamp
    {
        get
        {
            if (_tsSize == 0) return default;
            return _forward ? DbFormat.ExtractTimestampFromUserKey(_savedKey.UserKey, _tsSize) : _savedTimestamp;
        }
    }

    /// <summary>
    /// With <see cref="ReadOptions.IterStartTs"/>: the internal key (user key | timestamp | seq/type) of the current
    /// version, as returned by upstream's key(); empty for merge results and without IterStartTs.
    /// </summary>
    public ReadOnlySpan<byte> InternalKey => _tsLb is not null && !_savedKey.IsUserKey ? _savedKey.InternalKey : default;

    /// <summary>The current value. REQUIRES: <see cref="Valid"/>. Valid until the next positioning call.</summary>
    public ReadOnlySpan<byte> Value
    {
        get
        {
            Debug.Assert(_valid);
            if (_valueIsBlob) return BlobValue();
            switch (_valueSource)
            {
                case ValueSource.Iter: return _valueIsEntity ? Wide.WideColumnSerialization.GetValueOfDefaultColumn(ResolveEntity(_iter.Value)) : _iter.Value;
                case ValueSource.IterPacked: return ParsePackedValueForValue(_iter.Value);
                case ValueSource.Pinned:
                    return _valueIsEntity ? Wide.WideColumnSerialization.GetValueOfDefaultColumn(ResolveEntity(PinnedSpan)) : PinnedSpan;
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
        ValueSource.Iter => ResolveEntity(_iter.Value),
        ValueSource.Pinned => ResolveEntity(PinnedSpan),
        ValueSource.Merged => _mergedEntity!.WrittenSpan,
        _ => default,
    };

    /// <summary>The internal key comparator (its user comparator orders <see cref="Key"/>).</summary>
    public InternalKeyComparator Comparator => _icmp;

    /// <summary>The snapshot sequence number this iterator reads at.</summary>
    public ulong Sequence => _sequence;

    /// <summary>The internal iterator (for tests and diagnostics).</summary>
    public InternalIterator InternalIterator => _iter;

    /// <summary>The pinned iterators manager (tests and diagnostics).</summary>
    internal PinnedIteratorsManager PinnedItersMgr => _pinMgr;

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

    /// <summary>The read callback (read_callback_) of a WritePrepared / WriteUnprepared read, or null.</summary>
    private ReadCallback? _readCallback;

    /// <summary>Sets the read callback before the first positioning call.</summary>
    internal void SetReadCallback(ReadCallback callback)
    {
        _readCallback = callback;
        _skipCovered = false;
    }

    /// <summary>Port of DBIter::IsVisible: sequence (or read callback) and timestamp range.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool IsVisible(ulong sequence, ReadOnlySpan<byte> userKey)
    {
        if (sequence > _sequence) return false;
        if (_readCallback is not null && !_readCallback.IsVisible(sequence)) return false;
        if (_tsSize == 0) return true;
        return IsTimestampVisible(DbFormat.ExtractTimestampFromUserKey(userKey, _tsSize));
    }

    private bool IsTimestampVisible(ReadOnlySpan<byte> ts) =>
        _ucmp.CompareTimestamp(ts, _tsUb) <= 0 && (_tsLb is null || _ucmp.CompareTimestamp(ts, _tsLb) >= 0);

    /// <summary>User keys of the same key, ignoring timestamps (EqualWithoutTimestamp).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool SameUserKey(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) =>
        _tsSize == 0 ? _icmp.EqualUserKey(a, b) : _ucmp.EqualWithoutTimestamp(a, b);

    /// <summary>CompareWithoutTimestamp(a, b) of two user keys with timestamps.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int CompareWithoutTimestamp(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) =>
        _tsSize == 0 ? _icmp.CompareUserKey(a, b) : _ucmp.CompareWithoutTimestamp(a, b);

    /// <summary>DBIter::CompareKeyForSkip: with iter_start_ts every timestamp is a distinct key.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int CompareKeyForSkip(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) =>
        _tsLb is not null ? _icmp.CompareUserKey(a, b) : CompareWithoutTimestamp(a, b);

    /// <summary>A user key (with timestamp) against an iterate bound (without): CompareWithoutTimestamp(a, true, b, false).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int CompareToBound(ReadOnlySpan<byte> userKey, ReadOnlySpan<byte> bound) =>
        _tsSize == 0 ? _icmp.CompareUserKey(userKey, bound) : _ucmp.CompareWithoutTimestamp(userKey, true, bound, false);

    /// <summary>The user key without its timestamp.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ReadOnlySpan<byte> StripTs(ReadOnlySpan<byte> userKey) => _tsSize == 0 ? userKey : userKey[..^_tsSize];

    /// <summary>AppendInternalKeyWithDifferentTimestamp: seek key <c>userKeyWithoutTs | ts | (seq, type)</c>.</summary>
    private void SetSeekKeyWithTimestamp(ReadOnlySpan<byte> userKeyWithoutTs, ReadOnlySpan<byte> ts, ulong sequence, EntryType type)
    {
        _seekKey.Clear();
        Span<byte> dst = _seekKey.GetSpan(userKeyWithoutTs.Length + ts.Length + DbFormat.InternalKeyFooterSize);
        _seekKey.Advance(DbFormat.AppendInternalKey(dst, userKeyWithoutTs, ts, sequence, type));
    }

    /// <summary>Seek key for the saved key at the min/max timestamp (kTsMin / kTsMax), or the plain one without timestamps.</summary>
    [SkipLocalsInit]
    private void SetSeekKeyForSaved(ulong sequence, EntryType type, bool maxTimestamp)
    {
        if (_tsSize == 0)
        {
            SetSeekKey(_savedKey.UserKey, sequence, type);
            return;
        }
        Span<byte> ts = stackalloc byte[_tsSize];
        ts.Fill(maxTimestamp ? (byte)0xff : (byte)0);
        SetSeekKeyWithTimestamp(StripTs(_savedKey.UserKey), ts, sequence, type);
    }

    /// <summary>saved_timestamp_.assign(ts) for reverse iteration.</summary>
    private void SaveTimestamp(ReadOnlySpan<byte> userKey)
    {
        if (_tsSize != 0) DbFormat.ExtractTimestampFromUserKey(userKey, _tsSize).CopyTo(_savedTimestamp);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool IsCoveredByRangeTombstone(ReadOnlySpan<byte> userKey, ulong sequence) =>
        _rangeDel is not null && _rangeDel.ShouldDelete(userKey, sequence);

    // Covered-range skipping (the effect of upstream's MergingIterator::SkipNextDeleted / SkipPrevDeleted reseeks):
    // when a visible entry is covered by a range tombstone, the aggregator also returns how far the tombstone reaches
    // (_skipTarget) and its seqno (_skipSeq); the inner iterator then moves the child providing the entry past the
    // covered range in one seek when that child's file(s) only hold older entries (SkipCoveredForward / Backward), so a
    // long run of deleted keys is not visited key by key. Only entries the aggregator would report as covered are
    // skipped, so the results are those of checking every entry.
    private bool _skipCovered;
    private bool _skipPending;
    private ulong _skipSeq;
    private readonly PooledBufferWriter? _skipTarget;

    /// <summary>
    /// <see cref="IsCoveredByRangeTombstone"/> for a visible point entry about to be dropped as covered; also prepares
    /// the covered-range skip (<see cref="_skipPending"/>) in the given direction.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool IsCoveredWithSkip(ReadOnlySpan<byte> userKey, ulong sequence, bool forward)
    {
        if (_rangeDel is null) return false;
        if (!_skipCovered) return _rangeDel.ShouldDelete(userKey, sequence);
        return _skipPending = _rangeDel.ShouldDeleteWithSkipTarget(userKey, sequence, forward, _skipTarget!, out _skipSeq);
    }

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
        !_hasPrefix || PrefixCheckSlow(StripTs(userKey));

    private bool PrefixCheckSlow(ReadOnlySpan<byte> userKey) =>
        _prefixExtractor!.InDomain(userKey) && _prefixExtractor.Transform(userKey).SequenceEqual(_prefix.WrittenSpan);

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
        ReleaseTempPinnedData();
        _hasPrefix = false;
        _numInternalKeysSkipped = 0;
        _isKeySeqnumZero = false;
        _valid = false;
        _merging = false;
        _currentEntryIsMerged = false;
        _valueSource = ValueSource.None;
        _valueIsEntity = false;
        _valueIsBlob = false;
        _entityResolved = false;
    }

    /// <summary>Clears the merge operands (both the copied and the pinned ones).</summary>
    private void ClearOperands()
    {
        _operands.Clear();
        _pinnedOperandCount = 0;
        _pinnedOperandsOnly = true;
    }

    /// <summary>The collected merge operands, oldest first.</summary>
    private MergeOperands CurrentOperands()
    {
        if (_pinnedOperandCount != 0 && _pinnedOperandsOnly)
        {
            PinnedOperandMerges++;
            return new MergeOperands(_pinnedOperands.AsSpan(0, _pinnedOperandCount));
        }
        return _operands.AsOperands();
    }

    /// <summary>Merges done on operands referenced in place (tests).</summary>
    internal long PinnedOperandMerges { get; private set; }

    /// <summary>
    /// FindValueForCurrentKey: adds the merge operand the inner iterator is on as the newest one, referencing it in
    /// place when the iterator can pin its value (blocks are temp-pinned during the reverse scan); the first operand
    /// that cannot be pinned turns the list into copies.
    /// </summary>
    private void PushNewerOperand()
    {
        if (_pinnedOperandsOnly && _iter.TryGetPinnedValue(out PinnedSlice pv))
        {
            if (_pinnedOperandCount == _pinnedOperands.Length)
            {
                var n = new PinnedSlice[Math.Max(8, _pinnedOperandCount * 2)];
                _pinnedOperands.AsSpan(0, _pinnedOperandCount).CopyTo(n);
                _pinnedOperands = n;
            }
            _pinnedOperands[_pinnedOperandCount++] = pv;
            return;
        }
        if (_pinnedOperandsOnly)
        {
            // Copy the operands pinned so far (still valid: the blocks stay pinned until the next positioning call).
            _pinnedOperandsOnly = false;
            for (int i = 0; i < _pinnedOperandCount; i++) _operands.PushNewer(_pinnedOperands[i].Span);
            _pinnedOperandCount = 0;
        }
        _operands.PushNewer(_iter.Value);
    }

    private void MergeWithNoBaseValue()
    {
        _valid = false;
        _mergeResult.Clear();
        MergeHelper.TimedFullMerge(_mergeOperator!, _savedKey.UserKey, false, default, CurrentOperands(), _mergeResult);
        _valueSource = ValueSource.Merged;
        _valueIsEntity = false;
        _valueIsBlob = false;
        _valid = true;
    }

    private void MergeWithPlainBaseValue(ReadOnlySpan<byte> value)
    {
        _valid = false;
        _mergeResult.Clear();
        MergeHelper.TimedFullMerge(_mergeOperator!, _savedKey.UserKey, true, value, CurrentOperands(), _mergeResult);
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
        // MergeWithWideColumnBaseValue -> ResolveEntityForMerge: blob-backed columns are resolved first.
        entity = ResolveEntity(entity);
        _mergeResult.Clear();
        MergeHelper.TimedFullMerge(_mergeOperator!, _savedKey.UserKey, true, Wide.WideColumnSerialization.GetValueOfDefaultColumn(entity),
            CurrentOperands(), _mergeResult);
        _mergedEntity ??= new PooledBufferWriter(128);
        _mergedEntity.Clear();
        Wide.WideColumnSerialization.ReplaceDefaultColumn(entity, _mergeResult.WrittenSpan, _mergedEntity);
        _valueSource = ValueSource.Merged;
        _valueIsEntity = true;
        _valueIsBlob = false;
        _entityResolved = false;
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

        if (_hasUpper && CompareToBound(userKey, _upper.Span) >= 0)
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

        if (IsVisible(seq, userKey))
        {
            // If the previous entry is of seqnum 0, the current entry will not possibly be skipped. With user-defined
            // timestamps the same user key can have different timestamps and zero sequence numbers.
            if ((!isPrevKeySeqnumZero || _tsSize > 0) && _skipping && CompareKeyForSkip(userKey, _savedKey.UserKey) <= 0)
            {
                _numSkipped++; // skip this entry
            }
            else if (_tsSize != 0 && IsCoveredByRangeTombstone(userKey, seq))
            {
                // With user-defined timestamps an older version of the key may have a higher seqno than a newer one, so
                // a covered entry does not hide the next one: drop just this entry, like upstream's MergingIterator.
                return StepAction.Next;
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
                        if (_tsLb is not null)
                        {
                            // iter_start_ts: deletions are returned too (with an empty value).
                            _savedKey.SetInternalKey(_iter.Key);
                            _valueSource = ValueSource.None;
                            SetValueKind(type);
                            _valid = true;
                            return StepAction.Done;
                        }
                        // Arrange to skip all upcoming entries for this key since they are hidden by this deletion.
                        _savedKey.SetUserKey(userKey);
                        _skipping = true;
                        break;
                    case EntryType.Value:
                    case EntryType.ValuePreferredSeqno:
                    case EntryType.BlobIndex:
                    case EntryType.WideColumnEntity:
                        _savedKey.SetUserKey(userKey);
                        if (IsCoveredWithSkip(userKey, seq, forward: true))
                        {
                            // Covered by a range tombstone: behaves like a deletion.
                            _skipping = true;
                            if (_skipPending) return StepAction.SkipCovered;
                            break;
                        }
                        if (_tsLb is not null) _savedKey.SetInternalKey(_iter.Key);
                        _valueSource = type == EntryType.ValuePreferredSeqno ? ValueSource.IterPacked : ValueSource.Iter;
                        SetValueKind(type);
                        _valid = true;
                        return StepAction.Done;
                    case EntryType.Merge:
                        _savedKey.SetUserKey(userKey);
                        if (IsCoveredWithSkip(userKey, seq, forward: true))
                        {
                            _skipping = true;
                            if (_skipPending) return StepAction.SkipCovered;
                            break;
                        }
                        // By now, we are sure the current ikey is going to yield a value.
                        if (_mergeOperator is null) ThrowNoMergeOperator();
                        _currentEntryIsMerged = true;
                        _valid = true;
                        // MergeValuesNewToOld: start the merge process by pushing the first operand.
                        ClearOperands();
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
            // This key was inserted after our snapshot was taken or skipped by timestamp range. If this happens too many
            // times in a row for the same user key, we want to seek to the target sequence number.
            int cmp = CompareWithoutTimestamp(userKey, _savedKey.UserKey);
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
                SetSeekKeyForSaved(0, EntryType.Deletion, maxTimestamp: false);
            }
            else
            {
                // We saw multiple entries with this user key and sequence numbers higher than sequence_. Fast
                // forward to sequence_.
                if (_tsSize == 0) SetSeekKey(_savedKey.UserKey, _sequence, DbFormat.ValueTypeForSeek);
                else SetSeekKeyWithTimestamp(StripTs(_savedKey.UserKey), _tsUb, _sequence, DbFormat.ValueTypeForSeek);
            }
            return StepAction.Seek;
        }
        return StepAction.Next;
    }

    /// <summary>One iteration of DBIter::MergeValuesNewToOld's loop (PRE: the first operand was pushed).</summary>
    private StepAction MergeStep()
    {
        ReadOnlySpan<byte> userKey = ParseCurrent(out ulong seq, out EntryType type);
        if (!SameUserKey(userKey, _savedKey.UserKey))
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
                case StepAction.SkipCovered:
                    if (!_iter.SkipCoveredForward(_skipTarget!.WrittenSpan, _skipSeq)) _iter.Next();
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
            ValueTask t = a switch
            {
                StepAction.Seek => _iter.SeekAsync(_seekKey.WrittenMemory, ct),
                StepAction.SkipCovered => SkipCoveredAsync(forward: true, ct),
                _ => _iter.NextAsync(ct),
            };
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

    /// <summary>Async covered-range skip of the inner iterator, or Next / Prev when it cannot skip.</summary>
    private ValueTask SkipCoveredAsync(bool forward, CancellationToken ct)
    {
        ValueTask<bool> s = forward
            ? _iter.SkipCoveredForwardAsync(_skipTarget!.WrittenMemory, _skipSeq, ct)
            : _iter.SkipCoveredBackwardAsync(_skipTarget!.WrittenMemory, _skipSeq, ct);
        if (!s.IsCompletedSuccessfully) return SkipCoveredSlowAsync(s, forward, ct);
        if (s.Result) return ValueTask.CompletedTask;
        return forward ? _iter.NextAsync(ct) : _iter.PrevAsync(ct);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask SkipCoveredSlowAsync(ValueTask<bool> pending, bool forward, CancellationToken ct)
    {
        if (await pending.ConfigureAwait(false)) return;
        if (forward) await _iter.NextAsync(ct).ConfigureAwait(false);
        else await _iter.PrevAsync(ct).ConfigureAwait(false);
    }

    // ==================================================================== public positioning (sync)

    public void Next()
    {
        if (!_valid) ThrowHelper.InvalidArgument("Next on an invalid iterator");
        // Release temporarily pinned blocks from last operation
        ReleaseTempPinnedData();
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
        ReleaseTempPinnedData();
        _numInternalKeysSkipped = 0;
        BeginPrev();
        RunReverse();
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
        _revPhase = RevPhase.PrevTop;
        _revFinish = RevFinish.ClearPrefixIfInvalid;
        RunReverse();
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
        if (_valid) MaybeSetPrefix(StripTs(_savedKey.UserKey));
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
        _revPhase = RevPhase.PrevTop;
        _revFinish = RevFinish.SetPrefixIfValid;
        RunReverse();
    }

    /// <summary>Port of SetSavedKeyToSeekTarget (clamped to the lower bound).</summary>
    private void SetSavedKeyToSeekTarget(ReadOnlySpan<byte> target)
    {
        if (_tsSize != 0)
        {
            // 'target' does not contain a timestamp: seek to target | timestamp_ub_.
            if (_hasLower && _ucmp.CompareWithoutTimestamp(target, false, _lower.Span, false) < 0) target = _lower.Span;
            SetSeekKeyWithTimestamp(target, _tsUb, _sequence, DbFormat.ValueTypeForSeek);
            _savedKey.SetUserKey(DbFormat.ExtractUserKey(_seekKey.WrittenSpan));
            return;
        }
        if (_hasLower && _icmp.CompareUserKey(target, _lower.Span) < 0) target = _lower.Span; // seek key is smaller than the lower bound
        _savedKey.SetUserKey(target);
        SetSeekKey(target, _sequence, DbFormat.ValueTypeForSeek);
    }

    /// <summary>Port of SetSavedKeyToSeekForPrevTarget (clamped below the upper bound).</summary>
    [SkipLocalsInit]
    private void SetSavedKeyToSeekForPrevTarget(ReadOnlySpan<byte> target)
    {
        if (_tsSize != 0)
        {
            Span<byte> ts = stackalloc byte[_tsSize];
            if (_hasUpper && _ucmp.CompareWithoutTimestamp(target, false, _upper.Span, false) >= 0)
            {
                ts.Fill(0xff);
                SetSeekKeyWithTimestamp(_upper.Span, ts, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeekForPrev);
            }
            else
            {
                if (_tsLb is not null) _tsLb.CopyTo(ts);
                else ts.Clear();
                SetSeekKeyWithTimestamp(target, ts, 0, DbFormat.ValueTypeForSeekForPrev);
            }
            return;
        }
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
        // Release temporarily pinned blocks from last operation
        ReleaseTempPinnedData();
        _numInternalKeysSkipped = 0;
        if (!_forward)
        {
            _isKeySeqnumZero = false;
            ValueTask r = ReverseToForwardAsync(cancellationToken);
            if (!r.IsCompletedSuccessfully) return NextSlowAsync(r, cancellationToken);
            r.GetAwaiter().GetResult();
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

    /// <summary>Async <see cref="Prev"/>: completes synchronously unless a table block must be read.</summary>
    public ValueTask PrevAsync(CancellationToken cancellationToken = default)
    {
        if (!_valid) ThrowHelper.InvalidArgument("Prev on an invalid iterator");
        ReleaseTempPinnedData();
        _numInternalKeysSkipped = 0;
        BeginPrev();
        return RunReverseAsync(cancellationToken);
    }

    /// <summary>Async <see cref="SeekForPrev"/>: completes synchronously unless a table block must be read.</summary>
    public ValueTask SeekForPrevAsync(ReadOnlyMemory<byte> target, CancellationToken cancellationToken = default)
    {
        ResetSeekState();
        SetSavedKeyToSeekForPrevTarget(target.Span);
        _forward = false;
        return AfterReverseSeekAsync(_iter.SeekForPrevAsync(_seekKey.WrittenMemory, cancellationToken), target, RevFinish.ClearPrefixIfInvalid,
            cancellationToken);
    }

    /// <summary>Async <see cref="SeekToLast"/>: completes synchronously unless a table block must be read.</summary>
    public ValueTask SeekToLastAsync(CancellationToken cancellationToken = default)
    {
        if (_hasUpper) return SeekForPrevAsync(_upper, cancellationToken);
        if (!_expectTotalOrderInnerIter) _maxSkip = ulong.MaxValue;
        ResetSeekState();
        _forward = false;
        return AfterReverseSeekAsync(_iter.SeekToLastAsync(cancellationToken), default, RevFinish.SetPrefixIfValid, cancellationToken);
    }

    /// <summary>
    /// The rest of SeekForPrev / SeekToLast once the inner iterator is positioned: PrevInternal (with
    /// <paramref name="prefixTarget"/> remembered for prefix_same_as_start by SeekForPrev).
    /// </summary>
    private ValueTask AfterReverseSeekAsync(ValueTask positioned, ReadOnlyMemory<byte> prefixTarget, RevFinish finish, CancellationToken ct)
    {
        if (!positioned.IsCompletedSuccessfully) return AfterReverseSeekSlowAsync(positioned, prefixTarget, finish, ct);
        positioned.GetAwaiter().GetResult();
        if (!_iter.Valid) return ValueTask.CompletedTask;
        if (finish == RevFinish.ClearPrefixIfInvalid) MaybeSetPrefix(prefixTarget.Span);
        _revPhase = RevPhase.PrevTop;
        _revFinish = finish;
        return RunReverseAsync(ct);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask AfterReverseSeekSlowAsync(ValueTask positioned, ReadOnlyMemory<byte> prefixTarget, RevFinish finish, CancellationToken ct)
    {
        await positioned.ConfigureAwait(false);
        await AfterReverseSeekAsync(ValueTask.CompletedTask, prefixTarget, finish, ct).ConfigureAwait(false);
    }

    // ==================================================================== direction changes

    /// <summary>Port of DBIter::ReverseToForward (total-order inner iterator).</summary>
    private void ReverseToForward()
    {
        // When moving backwards, iter_ is positioned on the _previous_ key, which may not exist or may have different
        // prefix than the current key(). If that's the case, seek iter_ to current key.
        if (!_expectTotalOrderInnerIter || !_iter.Valid)
        {
            SetSeekKeyForSaved(DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek, maxTimestamp: true);
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

    /// <summary>
    /// Async <see cref="ReverseToForward"/>: the re-seek to saved_key_ and the skip of smaller keys await the inner
    /// iterator; completes synchronously when it does.
    /// </summary>
    private ValueTask ReverseToForwardAsync(CancellationToken ct)
    {
        if (!_expectTotalOrderInnerIter || !_iter.Valid)
        {
            SetSeekKeyForSaved(DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek, maxTimestamp: true);
            ValueTask s = _iter.SeekAsync(_seekKey.WrittenMemory, ct);
            if (!s.IsCompletedSuccessfully) return ReverseToForwardSlowAsync(s, ct);
            s.GetAwaiter().GetResult();
        }
        _forward = true;
        return SkipKeysBeforeSavedAsync(ct);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask ReverseToForwardSlowAsync(ValueTask seek, CancellationToken ct)
    {
        await seek.ConfigureAwait(false);
        _forward = true;
        await SkipKeysBeforeSavedAsync(ct).ConfigureAwait(false);
    }

    /// <summary>ReverseToForward's loop: skip keys less than the current key() (a.k.a. saved_key_).</summary>
    private ValueTask SkipKeysBeforeSavedAsync(CancellationToken ct)
    {
        while (_iter.Valid)
        {
            ReadOnlySpan<byte> userKey = ParseCurrent(out _, out _);
            if (_icmp.CompareUserKey(userKey, _savedKey.UserKey) >= 0) return ValueTask.CompletedTask;
            ValueTask t = _iter.NextAsync(ct);
            if (!t.IsCompletedSuccessfully) return SkipKeysBeforeSavedSlowAsync(t, ct);
            t.GetAwaiter().GetResult();
        }
        return ValueTask.CompletedTask;
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask SkipKeysBeforeSavedSlowAsync(ValueTask pending, CancellationToken ct)
    {
        await pending.ConfigureAwait(false);
        await SkipKeysBeforeSavedAsync(ct).ConfigureAwait(false);
    }

    // ==================================================================== reverse (state machine)
    //
    // DBIter::ReverseToBackward, PrevInternal, FindValueForCurrentKey(+UsingSeek) and FindUserKeyBeforeSavedKey as one
    // resumable state machine: ReverseStep runs the logic up to the next move of the inner iterator and returns that
    // move (InnerOp); the sync driver (RunReverse) performs it directly, the async driver (RunReverseAsync) awaits it
    // and completes synchronously when the inner iterator does. Every upstream loop maps to a phase that is re-entered
    // after the move, so the sync and async paths share all of the logic.

    /// <summary>The next move of the inner iterator requested by <see cref="ReverseStep"/>.</summary>
    private enum InnerOp : byte { Done, Continue, Prev, Next, Seek, SeekForPrev, SeekToLast, SkipCovered }

    private enum RevPhase : byte
    {
        /// <summary>ReverseToBackward: entry.</summary>
        ToBackward,
        /// <summary>ReverseToBackward: after Seek(saved_key_, kMaxSequenceNumber) (SeekToLast if invalid).</summary>
        ToBackwardAfterSeek,
        /// <summary>ReverseToBackward: inner iterator positioned; continue with FindUserKeyBeforeSavedKey.</summary>
        ToBackwardPositioned,
        /// <summary>PrevInternal: loop head.</summary>
        PrevTop,
        /// <summary>FindValueForCurrentKey: loop over the entries of saved_key_ (old to new).</summary>
        FindValue,
        /// <summary>FindValueForCurrentKey: the loop ended after the last Prev.</summary>
        FindValueEnd,
        /// <summary>FindValueForCurrentKeyUsingSeek: skipping invisible entries after the seek.</summary>
        SeekSkip,
        /// <summary>FindValueForCurrentKeyUsingSeek: collecting merge operands (new to old).</summary>
        SeekMerge,
        /// <summary>FindValueForCurrentKeyUsingSeek: after the Seek of the iterator restore (SeekToLast if invalid).</summary>
        SeekRestoreAfterSeek,
        /// <summary>FindValueForCurrentKeyUsingSeek: done (iterator restored).</summary>
        SeekRestored,
        /// <summary>FindUserKeyBeforeSavedKey: loop.</summary>
        BeforeSaved,
        /// <summary>FindUserKeyBeforeSavedKey: after the reseek (Seek(saved_key_, kMaxSequenceNumber)).</summary>
        BeforeSavedAfterSeek,
    }

    /// <summary>What a public reverse operation does once the state machine is done.</summary>
    private enum RevFinish : byte { None, ClearPrefixIfInvalid, SetPrefixIfValid }

    private RevPhase _revPhase;
    private RevFinish _revFinish;
    private bool _fukFromPrevInternal;      // FindUserKeyBeforeSavedKey was called by PrevInternal (else ReverseToBackward)
    private ulong _fukNumSkipped;           // FindUserKeyBeforeSavedKey's num_skipped
    // FindValueForCurrentKey's locals.
    private EntryType _fvLastNotMergeType;
    private EntryType _fvLastKeyEntryType;
    private bool _fvValidEntrySeen;
    private ulong _fvNumSkipped;

    /// <summary>Starts DBIter::Prev's reverse movement: ReverseToBackward (if moving forward) then PrevInternal.</summary>
    private void BeginPrev()
    {
        _revFinish = RevFinish.None;
        _revPhase = _forward ? RevPhase.ToBackward : RevPhase.PrevTop;
    }

    /// <summary>Sync driver of the reverse state machine.</summary>
    private void RunReverse()
    {
        while (true)
        {
            switch (ReverseStep())
            {
                case InnerOp.Done:
                    ReverseFinished();
                    return;
                case InnerOp.Continue:
                    break;
                case InnerOp.Prev:
                    _iter.Prev();
                    break;
                case InnerOp.Next:
                    _iter.Next();
                    break;
                case InnerOp.Seek:
                    _iter.Seek(_seekKey.WrittenSpan);
                    break;
                case InnerOp.SeekForPrev:
                    _iter.SeekForPrev(_seekKey.WrittenSpan);
                    break;
                case InnerOp.SeekToLast:
                    _iter.SeekToLast();
                    break;
                case InnerOp.SkipCovered:
                    if (!_iter.SkipCoveredBackward(_skipTarget!.WrittenSpan, _skipSeq)) _iter.Prev();
                    break;
            }
        }
    }

    /// <summary>Async driver of the reverse state machine; completes synchronously when the inner iterator does.</summary>
    private ValueTask RunReverseAsync(CancellationToken ct)
    {
        while (true)
        {
            InnerOp op = ReverseStep();
            if (op == InnerOp.Done)
            {
                ReverseFinished();
                return ValueTask.CompletedTask;
            }
            if (op == InnerOp.Continue) continue;
            ValueTask t = IssueAsync(op, ct);
            if (!t.IsCompletedSuccessfully) return RunReverseSlowAsync(t, ct);
            t.GetAwaiter().GetResult();
        }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask RunReverseSlowAsync(ValueTask pending, CancellationToken ct)
    {
        await pending.ConfigureAwait(false);
        while (true)
        {
            InnerOp op = ReverseStep();
            if (op == InnerOp.Done)
            {
                ReverseFinished();
                return;
            }
            if (op != InnerOp.Continue) await IssueAsync(op, ct).ConfigureAwait(false);
        }
    }

    private ValueTask IssueAsync(InnerOp op, CancellationToken ct) => op switch
    {
        InnerOp.Prev => _iter.PrevAsync(ct),
        InnerOp.Next => _iter.NextAsync(ct),
        InnerOp.Seek => _iter.SeekAsync(_seekKey.WrittenMemory, ct),
        InnerOp.SeekForPrev => _iter.SeekForPrevAsync(_seekKey.WrittenMemory, ct),
        InnerOp.SkipCovered => SkipCoveredAsync(forward: false, ct),
        _ => _iter.SeekToLastAsync(ct),
    };

    private void ReverseFinished()
    {
        switch (_revFinish)
        {
            case RevFinish.ClearPrefixIfInvalid:
                if (!_valid) _hasPrefix = false;
                break;
            case RevFinish.SetPrefixIfValid:
                if (_valid) MaybeSetPrefix(StripTs(_savedKey.UserKey));
                break;
        }
    }

    /// <summary>Runs the reverse logic up to the next move of the inner iterator (see the phases).</summary>
    private InnerOp ReverseStep()
    {
        switch (_revPhase)
        {
            case RevPhase.ToBackward:
                // Port of DBIter::ReverseToBackward: move iter_ to the key before saved_key_. When
                // current_entry_is_merged_ is true, iter_ may be positioned on the next key, which may not exist. If
                // that's the case, seek to saved_key_ (kMaxSequenceNumber + kValueTypeForSeek: strictly before its entries).
                if (_currentEntryIsMerged && (!_expectTotalOrderInnerIter || !_iter.Valid))
                {
                    SetSeekKey(_savedKey.UserKey, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
                    if (!_expectTotalOrderInnerIter)
                    {
                        _revPhase = RevPhase.ToBackwardPositioned;
                        return InnerOp.SeekForPrev;
                    }
                    // Some iterators may not support SeekForPrev(), so we avoid using it when prefix seek mode is disabled.
                    _revPhase = RevPhase.ToBackwardAfterSeek;
                    return InnerOp.Seek;
                }
                _revPhase = RevPhase.ToBackwardPositioned;
                return InnerOp.Continue;

            case RevPhase.ToBackwardAfterSeek:
                _revPhase = RevPhase.ToBackwardPositioned;
                return _iter.Valid ? InnerOp.Continue : InnerOp.SeekToLast;

            case RevPhase.ToBackwardPositioned:
                _forward = false;
                BeginFindUserKeyBeforeSavedKey(fromPrevInternal: false);
                return InnerOp.Continue;

            case RevPhase.PrevTop:
                return PrevInternalStep();

            case RevPhase.FindValue:
                return FindValueStep();

            case RevPhase.FindValueEnd:
                FindValueForCurrentKeyEnd();
                return InnerOp.Continue;

            case RevPhase.SeekSkip:
                return UsingSeekSkipStep();

            case RevPhase.SeekMerge:
                return UsingSeekMergeStep();

            case RevPhase.SeekRestoreAfterSeek:
                _revPhase = RevPhase.SeekRestored;
                return _iter.Valid ? InnerOp.Continue : InnerOp.SeekToLast;

            case RevPhase.SeekRestored:
                _valid = true;
                BeginFindUserKeyBeforeSavedKey(fromPrevInternal: true);
                return InnerOp.Continue;

            case RevPhase.BeforeSaved:
                return FindUserKeyBeforeSavedKeyStep();

            case RevPhase.BeforeSavedAfterSeek:
                if (!_iter.Valid) return FindUserKeyBeforeSavedKeyDone();
                _revPhase = RevPhase.BeforeSaved;
                return InnerOp.Prev;

            default:
                throw new InvalidOperationException();
        }
    }

    /// <summary>Port of the head of DBIter::PrevInternal's loop.</summary>
    private InnerOp PrevInternalStep()
    {
        if (!_iter.Valid)
        {
            // We haven't found any key - iterator is not valid
            _valid = false;
            return InnerOp.Done;
        }
        _savedKey.SetUserKey(DbFormat.ExtractUserKey(_iter.Key));
        if (_hasPrefix && !PrefixCheck(_savedKey.UserKey))
        {
            // prefix_same_as_start: we've iterated past the seek prefix.
            _valid = false;
            return InnerOp.Done;
        }
        if (_hasLower && CompareToBound(_savedKey.UserKey, _lower.Span) < 0)
        {
            // We've iterated earlier than the user-specified lower bound.
            _valid = false;
            return InnerOp.Done;
        }
        // FindValueForCurrentKey() assigns _valid; whether or not we found a value for the current key, iter_ then has to
        // end up on a smaller key (FindUserKeyBeforeSavedKey).
        BeginFindValueForCurrentKey();
        return InnerOp.Continue;
    }

    /// <summary>
    /// Port of the start of DBIter::FindValueForCurrentKey. PRE: iter_ is on the last entry with user key saved_key_.
    /// The loop walks the entries of saved_key_ from oldest to newest visible, tracking the newest put/delete and the
    /// merge operands stacked on top of it.
    /// </summary>
    private void BeginFindValueForCurrentKey()
    {
        Debug.Assert(_iter.Valid);
        // Temporarily pin blocks that hold the value (TempPinData).
        ReleaseTempPinnedData();
        TempPinData();
        ClearOperands();
        _currentEntryIsMerged = false;
        _valid = false;
        // last entry before merge (could be a deletion, a value or a range deletion)
        _fvLastNotMergeType = EntryType.Deletion;
        _fvLastKeyEntryType = EntryType.Deletion;
        _fvValidEntrySeen = false;
        _fvNumSkipped = 0;
        _revPhase = RevPhase.FindValue;
    }

    /// <summary>One iteration of FindValueForCurrentKey's loop (the entry iter_ is on).</summary>
    private InnerOp FindValueStep()
    {
        _skipPending = false;
        if (!_iter.Valid)
        {
            FindValueForCurrentKeyEnd();
            return InnerOp.Continue;
        }
        ReadOnlySpan<byte> userKey = ParseCurrent(out ulong seq, out EntryType type);
        if (!SameUserKey(userKey, _savedKey.UserKey))
        {
            // Found a smaller user key, thus we are done with current user key.
            FindValueForCurrentKeyEnd();
            return InnerOp.Continue;
        }
        bool visible = IsVisible(seq, userKey);
        if (!visible && (_tsLb is null || _ucmp.CompareTimestamp(DbFormat.ExtractTimestampFromUserKey(userKey, _tsSize), _tsUb) > 0))
        {
            // Found an invisible version of the current user key, and it must have a higher sequence number or
            // timestamp. Therefore, we are done with the current user key.
            FindValueForCurrentKeyEnd();
            return InnerOp.Continue;
        }
        if (visible && _tsSize != 0 && IsCoveredByRangeTombstone(userKey, seq))
        {
            // Dropped like by upstream's MergingIterator (see FindNextUserEntryStep).
            ++_fvNumSkipped;
            return InnerOp.Prev;
        }
        SaveTimestamp(userKey);

        TooManyInternalKeysSkipped(true);

        // This user key has lots of entries. We're going from old to new, and it's taking too long. Let's do a
        // Seek() and go from new to old. This helps when a key was overwritten many times.
        if (_fvNumSkipped >= _maxSkip) return BeginFindValueForCurrentKeyUsingSeek();

        if (_tsSize != 0)
        {
            if (_tsLb is not null) _savedKey.SetInternalKey(_iter.Key);
            else if (_icmp.CompareUserKey(userKey, _savedKey.UserKey) < 0) _savedKey.SetUserKey(userKey);
        }

        _fvValidEntrySeen = true;
        _fvLastKeyEntryType = type;
        switch (type)
        {
            case EntryType.Value:
            case EntryType.ValuePreferredSeqno:
            case EntryType.BlobIndex:
            case EntryType.WideColumnEntity:
                if (IsCoveredWithSkip(userKey, seq, forward: false))
                {
                    _fvLastKeyEntryType = EntryType.RangeDeletion;
                    _fvLastNotMergeType = EntryType.RangeDeletion;
                    ClearOperands();
                    break;
                }
                // The child iterator moves on: keep the value pinned (or a copy when the child cannot pin it).
                SaveCurrentValue(type);
                ClearOperands();
                _fvLastNotMergeType = type;
                break;
            case EntryType.Deletion:
            case EntryType.DeletionWithTimestamp:
            case EntryType.SingleDeletion:
                ClearOperands();
                _fvLastNotMergeType = type;
                break;
            case EntryType.Merge:
                if (IsCoveredWithSkip(userKey, seq, forward: false))
                {
                    _fvLastKeyEntryType = EntryType.RangeDeletion;
                    _fvLastNotMergeType = EntryType.RangeDeletion;
                    ClearOperands();
                    break;
                }
                if (_mergeOperator is null) ThrowNoMergeOperator();
                PushNewerOperand();
                break;
            default:
                ThrowUnknownType(type);
                break;
        }

        ++_fvNumSkipped;
        // With iter_start_ts every visible version is its own entry: stop at the first one.
        if (visible && _tsLb is not null) _revPhase = RevPhase.FindValueEnd;
        return _skipPending ? InnerOp.SkipCovered : InnerOp.Prev;
    }

    /// <summary>The end of FindValueForCurrentKey (after its loop); continues with FindUserKeyBeforeSavedKey.</summary>
    private void FindValueForCurrentKeyEnd()
    {
        BeginFindUserKeyBeforeSavedKey(fromPrevInternal: true);
        if (!_fvValidEntrySeen)
        {
            _valid = false;
            return;
        }

        switch (_fvLastKeyEntryType)
        {
            case EntryType.Deletion:
            case EntryType.DeletionWithTimestamp:
            case EntryType.SingleDeletion:
                // iter_start_ts: the deletion itself is returned (empty value).
                _valueSource = ValueSource.None;
                SetValueKind(_fvLastKeyEntryType);
                _valid = _tsLb is not null;
                return;
            case EntryType.RangeDeletion:
                _valid = false;
                return;
            case EntryType.Merge:
                _currentEntryIsMerged = true;
                if (_fvLastNotMergeType is EntryType.Value or EntryType.ValuePreferredSeqno) MergeWithPlainBaseValue(PinnedSpan);
                else if (_fvLastNotMergeType == EntryType.BlobIndex) MergeWithPlainBaseValue(FetchBlobForMerge(PinnedSpan));
                else if (_fvLastNotMergeType == EntryType.WideColumnEntity) MergeWithEntityBaseValue(PinnedSpan);
                else MergeWithNoBaseValue();
                return;
            default: // Value / ValuePreferredSeqno / WideColumnEntity / BlobIndex
                _valueSource = ValueSource.Pinned;
                SetValueKind(_fvLastKeyEntryType);
                _valid = true;
                return;
        }
    }

    /// <summary>
    /// Port of DBIter::FindValueForCurrentKeyUsingSeek: Seek to (saved_key_, sequence_) and resolve the key moving
    /// forward (new to old) instead of walking many old versions with Prev().
    /// </summary>
    private InnerOp BeginFindValueForCurrentKeyUsingSeek()
    {
        if (_tsSize == 0) SetSeekKey(_savedKey.UserKey, _sequence, DbFormat.ValueTypeForSeek);
        else SetSeekKeyWithTimestamp(StripTs(_savedKey.UserKey), _tsLb ?? _tsUb, _sequence, DbFormat.ValueTypeForSeek);
        _revPhase = RevPhase.SeekSkip;
        return InnerOp.Seek;
    }

    /// <summary>The leaves of FindValueForCurrentKeyUsingSeek: the key is resolved, continue with FindUserKeyBeforeSavedKey.</summary>
    private InnerOp UsingSeekDone()
    {
        BeginFindUserKeyBeforeSavedKey(fromPrevInternal: true);
        return InnerOp.Continue;
    }

    /// <summary>FindValueForCurrentKeyUsingSeek: the loop skipping invisible entries, then the found entry.</summary>
    private InnerOp UsingSeekSkipStep()
    {
        if (!_iter.Valid)
        {
            _valid = false;
            return UsingSeekDone();
        }
        ReadOnlySpan<byte> userKey = ParseCurrent(out ulong seq, out EntryType type);
        if (!SameUserKey(userKey, _savedKey.UserKey))
        {
            // No visible values for this key, even though FindValueForCurrentKey() has seen some (possible with a
            // tailing iterator when entries were compacted away).
            _valid = false;
            return UsingSeekDone();
        }
        if (!IsVisible(seq, userKey) || (_tsSize != 0 && IsCoveredByRangeTombstone(userKey, seq))) return InnerOp.Next;

        if (type is EntryType.Deletion or EntryType.SingleDeletion or EntryType.DeletionWithTimestamp ||
            IsCoveredByRangeTombstone(userKey, seq))
        {
            if (_tsLb is null || type is not (EntryType.Deletion or EntryType.SingleDeletion or EntryType.DeletionWithTimestamp))
            {
                _valid = false;
            }
            else
            {
                _savedKey.SetInternalKey(_iter.Key);
                SaveTimestamp(userKey);
                _valueSource = ValueSource.None;
                SetValueKind(type);
                _valid = true;
            }
            return UsingSeekDone();
        }
        SaveTimestamp(userKey);
        if (type is EntryType.Value or EntryType.ValuePreferredSeqno or EntryType.WideColumnEntity or EntryType.BlobIndex)
        {
            SaveCurrentValue(type);
            _valueSource = ValueSource.Pinned;
            SetValueKind(type);
            if (_tsLb is not null) _savedKey.SetInternalKey(_iter.Key);
            else if (_tsSize != 0) _savedKey.SetUserKey(userKey);
            _valid = true;
            return UsingSeekDone();
        }
        if (type != EntryType.Merge) ThrowUnknownType(type);

        // kTypeMerge. We need to collect all kTypeMerge values and save them in operands.
        if (_mergeOperator is null) ThrowNoMergeOperator();
        _currentEntryIsMerged = true;
        ClearOperands();
        _operands.PushOlder(_iter.Value);
        _revPhase = RevPhase.SeekMerge;
        return InnerOp.Next;
    }

    /// <summary>FindValueForCurrentKeyUsingSeek: one iteration of the merge-operand loop (after a Next).</summary>
    private InnerOp UsingSeekMergeStep()
    {
        if (_iter.Valid)
        {
            ReadOnlySpan<byte> userKey = ParseCurrent(out ulong seq, out EntryType type);
            if (SameUserKey(userKey, _savedKey.UserKey) &&
                !(type is EntryType.Deletion or EntryType.SingleDeletion or EntryType.DeletionWithTimestamp || IsCoveredByRangeTombstone(userKey, seq)))
            {
                switch (type)
                {
                    case EntryType.Value:
                    case EntryType.ValuePreferredSeqno:
                        ReadOnlySpan<byte> v = _iter.Value;
                        MergeWithPlainBaseValue(type == EntryType.ValuePreferredSeqno ? ParsePackedValueForValue(v) : v);
                        return UsingSeekDone();
                    case EntryType.Merge:
                        _operands.PushOlder(_iter.Value);
                        return InnerOp.Next;
                    case EntryType.WideColumnEntity:
                        MergeWithEntityBaseValue(_iter.Value);
                        return UsingSeekDone();
                    case EntryType.BlobIndex:
                        MergeWithPlainBaseValue(FetchBlobForMerge(_iter.Value));
                        return UsingSeekDone();
                    default:
                        ThrowUnknownType(type);
                        break;
                }
            }
        }

        MergeWithNoBaseValue();

        // Make sure we leave iter_ in a good state. If it's valid and we don't care about prefixes, that's already
        // good enough. Otherwise it needs to be seeked to the current key (last_key, still in _seekKey).
        if (!_expectTotalOrderInnerIter)
        {
            _revPhase = RevPhase.SeekRestored;
            return InnerOp.SeekForPrev;
        }
        if (!_iter.Valid)
        {
            _revPhase = RevPhase.SeekRestoreAfterSeek;
            return InnerOp.Seek;
        }
        _revPhase = RevPhase.SeekRestored;
        return InnerOp.Continue;
    }

    /// <summary>Port of the start of DBIter::FindUserKeyBeforeSavedKey: move backwards until the key smaller than saved_key_.</summary>
    private void BeginFindUserKeyBeforeSavedKey(bool fromPrevInternal)
    {
        _fukFromPrevInternal = fromPrevInternal;
        _fukNumSkipped = 0;
        _revPhase = RevPhase.BeforeSaved;
    }

    /// <summary>One iteration of FindUserKeyBeforeSavedKey's loop.</summary>
    private InnerOp FindUserKeyBeforeSavedKeyStep()
    {
        if (!_iter.Valid) return FindUserKeyBeforeSavedKeyDone();
        ReadOnlySpan<byte> userKey = ParseCurrent(out _, out _);
        if (CompareKeyForSkip(userKey, _savedKey.UserKey) < 0) return FindUserKeyBeforeSavedKeyDone();

        TooManyInternalKeysSkipped(true);

        if (_fukNumSkipped >= _maxSkip)
        {
            _fukNumSkipped = 0;
            SetSeekKeyForSaved(DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek, maxTimestamp: true);
            // It would be more efficient to use SeekForPrev() here, but some iterators may not support it.
            _revPhase = RevPhase.BeforeSavedAfterSeek;
            return InnerOp.Seek;
        }
        ++_fukNumSkipped;
        return InnerOp.Prev;
    }

    /// <summary>FindUserKeyBeforeSavedKey returned: back to PrevInternal's loop (or to it, after ReverseToBackward).</summary>
    private InnerOp FindUserKeyBeforeSavedKeyDone()
    {
        _revPhase = RevPhase.PrevTop;
        if (!_fukFromPrevInternal) return InnerOp.Continue;
        if (_valid) return InnerOp.Done; // Found the value.
        TooManyInternalKeysSkipped(false);
        return InnerOp.Continue;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _valid = false;
        _iter.Dispose();
        // Blocks handed over while the children were disposed go too.
        if (_pinMgr.IsPinning) _pinMgr.ReleasePinnedData();
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
        _skipTarget?.Dispose();
        _resolvedEntity?.Dispose();
        _entityFetchScratch?.Dispose();
    }
}
