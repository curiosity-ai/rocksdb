using System.Runtime.CompilerServices;
using System.Buffers;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Table;



/// <summary>
/// Builds blocks where keys are prefix-compressed. Port of table/block_based/block_builder.{h,cc} and
/// DataBlockFooter::EncodeTo (table/block_based/data_block_footer.cc), including separated key/value storage
/// (separate_key_value_in_data_block) and the uniformity hint (uniform_cv_threshold).
/// With the common-prefix feature (optimize_key_common_prefix, format_version &gt;= 8) the user-key prefix shared by
/// every key of the block is stored once in its leading bytes (restarts[0] = prefix length) and restart entries
/// (shared == 0) store their key without it; non-restart entries are unchanged (their shared bytes cover the prefix).
/// <code>
/// entry:   shared_bytes varint32 | unshared_bytes varint32 | value_length varint32 (omitted with value delta
///          encoding, i.e. format_version &gt;= 4 index blocks) | key_delta | value
/// separated: shared varint32 | unshared varint32 | value_length varint32 | value_offset varint32 (restart entries only) | key_delta
///          (values follow all entries, before the restart array)
/// trailer: restarts uint32[num_restarts] | [hash index] | [values_section_offset uint32] |
///          packed(num_restarts | hash bit 31 | uniform bit 29 | separated bit 28) uint32
/// </code>
/// </summary>
public sealed class BlockBuilder : IDisposable
{
    private const uint HashIndexBit = 1u << 31;
    private const uint UniformKeysBit = 1u << 29;
    private const uint SeparatedKvBit = 1u << 28;

    private readonly int _restartInterval;
    private readonly bool _useSeparatedKv;
    private readonly double _uniformCvThreshold;
    // values_buffer_: the values section while building (separated key/value storage only).
    private readonly PooledBufferWriter? _values;
    private bool _isUniform;
    private readonly bool _useDeltaEncoding;
    private readonly bool _useValueDeltaEncoding;
    private readonly DataBlockHashIndexBuilder? _hashIndexBuilder;
    private readonly bool _useCommonPrefix;
    private readonly bool _isUserKey;
    // strip_ts_sz_: user-defined timestamp size removed from every key before it is persisted
    // (persist_user_defined_timestamps = false); 0 keeps keys as given.
    private readonly int _stripTsSize;
    private readonly PooledBufferWriter? _stripBuf;
    private PooledBufferWriter _buffer;
    private readonly PooledBufferWriter _lastKey;
    // Common-prefix feature: the first key's user key and the running common prefix length (first_key_prefix_).
    private readonly PooledBufferWriter? _firstKeyPrefix;
    private int _prefixLength;
    private bool _finishing;
    private PooledBufferWriter? _rewriteBuffer;
    private uint[] _restarts;
    private int _numRestarts;
    private int _counter;
    private int _estimate;
    private bool _finished;

    /// <param name="restartInterval">Number of keys between restart points (&gt;= 1).</param>
    /// <param name="useDeltaEncoding">Share key prefixes with the previous key.</param>
    /// <param name="useValueDeltaEncoding">Index blocks with format_version &gt;= 4: omit value length; non-restart entries store the caller's delta value.</param>
    /// <param name="indexType">Data blocks may append a hash index (keys must be internal keys).</param>
    /// <param name="hashTableUtilRatio">data_block_hash_table_util_ratio.</param>
    public BlockBuilder(int restartInterval, bool useDeltaEncoding, bool useValueDeltaEncoding, DataBlockIndexType indexType, double hashTableUtilRatio)
        : this(restartInterval, useDeltaEncoding, useValueDeltaEncoding, indexType, hashTableUtilRatio, useCommonPrefix: false, isUserKey: false)
    {
    }

    /// <param name="restartInterval">Number of keys between restart points (&gt;= 1).</param>
    /// <param name="useDeltaEncoding">Share key prefixes with the previous key.</param>
    /// <param name="useValueDeltaEncoding">Index blocks with format_version &gt;= 4: omit value length; non-restart entries store the caller's delta value.</param>
    /// <param name="indexType">Data blocks may append a hash index (keys must be internal keys).</param>
    /// <param name="hashTableUtilRatio">data_block_hash_table_util_ratio.</param>
    /// <param name="useCommonPrefix">Store the block's common user-key prefix once (format_version &gt;= 8; requires delta encoding).</param>
    /// <param name="isUserKey">Keys are user keys (index without sequence numbers) rather than internal keys.</param>
    public BlockBuilder(int restartInterval, bool useDeltaEncoding, bool useValueDeltaEncoding, DataBlockIndexType indexType, double hashTableUtilRatio,
        bool useCommonPrefix, bool isUserKey)
        : this(restartInterval, useDeltaEncoding, useValueDeltaEncoding, indexType, hashTableUtilRatio, useCommonPrefix, isUserKey, stripTimestampSize: 0)
    {
    }

    /// <param name="restartInterval">Number of keys between restart points (&gt;= 1).</param>
    /// <param name="useDeltaEncoding">Share key prefixes with the previous key.</param>
    /// <param name="useValueDeltaEncoding">Index blocks with format_version &gt;= 4: omit value length; non-restart entries store the caller's delta value.</param>
    /// <param name="indexType">Data blocks may append a hash index (keys must be internal keys).</param>
    /// <param name="hashTableUtilRatio">data_block_hash_table_util_ratio.</param>
    /// <param name="useCommonPrefix">Store the block's common user-key prefix once (format_version &gt;= 8; requires delta encoding).</param>
    /// <param name="isUserKey">Keys are user keys (index without sequence numbers) rather than internal keys.</param>
    /// <param name="stripTimestampSize">
    /// Port of strip_ts_sz_ (persist_user_defined_timestamps = false): the trailing user-defined timestamp of this many
    /// bytes is removed from every key before it is written (MaybeStripTimestampFromKey); 0 keeps keys whole. The data
    /// block hash index still hashes the user key with its timestamp, like upstream.
    /// </param>
    public BlockBuilder(int restartInterval, bool useDeltaEncoding, bool useValueDeltaEncoding, DataBlockIndexType indexType, double hashTableUtilRatio,
        bool useCommonPrefix, bool isUserKey, int stripTimestampSize)
        : this(restartInterval, useDeltaEncoding, useValueDeltaEncoding, indexType, hashTableUtilRatio, useCommonPrefix, isUserKey, stripTimestampSize,
            useSeparatedKvStorage: false, uniformCvThreshold: -1.0)
    {
    }

    /// <param name="restartInterval">Number of keys between restart points (&gt;= 1).</param>
    /// <param name="useDeltaEncoding">Share key prefixes with the previous key.</param>
    /// <param name="useValueDeltaEncoding">Index blocks with format_version &gt;= 4: omit value length; non-restart entries store the caller's delta value.</param>
    /// <param name="indexType">Data blocks may append a hash index (keys must be internal keys).</param>
    /// <param name="hashTableUtilRatio">data_block_hash_table_util_ratio.</param>
    /// <param name="useCommonPrefix">Store the block's common user-key prefix once (format_version &gt;= 8; requires delta encoding).</param>
    /// <param name="isUserKey">Keys are user keys (index without sequence numbers) rather than internal keys.</param>
    /// <param name="stripTimestampSize">Timestamp size removed from every key before it is written (0 keeps keys whole).</param>
    /// <param name="useSeparatedKvStorage">
    /// use_separated_kv_storage (separate_key_value_in_data_block): keys go to the start of the block, values to a values
    /// section after them; restart entries carry the value's offset in that section.
    /// </param>
    /// <param name="uniformCvThreshold">
    /// uniform_cv_threshold: when &gt;= 0 the footer "is_uniform" bit is set if the coefficient of variation of the gaps
    /// between restart keys is below it (ScanForUniformity); negative disables the scan.
    /// </param>
    public BlockBuilder(int restartInterval, bool useDeltaEncoding, bool useValueDeltaEncoding, DataBlockIndexType indexType, double hashTableUtilRatio,
        bool useCommonPrefix, bool isUserKey, int stripTimestampSize, bool useSeparatedKvStorage, double uniformCvThreshold)
    {
        _useSeparatedKv = useSeparatedKvStorage;
        _uniformCvThreshold = uniformCvThreshold;
        if (useSeparatedKvStorage) _values = new PooledBufferWriter(4096);
        if (useCommonPrefix && !useDeltaEncoding) throw new ArgumentException("the common-prefix feature requires delta encoding", nameof(useCommonPrefix));
        // The common-prefix feature requires user keys without stripped timestamps (BlockBuilder ctor assert).
        if (useCommonPrefix && stripTimestampSize != 0) throw new ArgumentException("the common-prefix feature cannot strip timestamps", nameof(useCommonPrefix));
        ArgumentOutOfRangeException.ThrowIfNegative(stripTimestampSize);
        _useCommonPrefix = useCommonPrefix;
        _isUserKey = isUserKey;
        _stripTsSize = stripTimestampSize;
        if (stripTimestampSize != 0) _stripBuf = new PooledBufferWriter(64);
        if (useCommonPrefix)
        {
            _firstKeyPrefix = new PooledBufferWriter(64);
            _rewriteBuffer = new PooledBufferWriter(4096);
        }
        ArgumentOutOfRangeException.ThrowIfLessThan(restartInterval, 1);
        _restartInterval = restartInterval;
        _useDeltaEncoding = useDeltaEncoding;
        _useValueDeltaEncoding = useValueDeltaEncoding;
        if (indexType == DataBlockIndexType.BinaryAndHash)
        {
            _hashIndexBuilder = new DataBlockHashIndexBuilder();
            _hashIndexBuilder.Initialize(hashTableUtilRatio);
        }
        _buffer = new PooledBufferWriter(4096, TableBuilderBuffers.Bytes);
        _lastKey = new PooledBufferWriter(64);
        _restarts = TableBuilderBuffers.UInts.Rent(64);
        _restarts[0] = 0;
        _numRestarts = 1;
        _estimate = BaseEstimate;
    }

    /// <summary>Builder for meta blocks (metaindex, properties): delta encoding, binary search only.</summary>
    public static BlockBuilder ForMetaBlock(int restartInterval) => new(restartInterval, true, false, DataBlockIndexType.BinarySearch, 0.75);

    public bool IsEmpty => _buffer.WrittenCount == 0;

    /// <summary>Approximate size of the block being built (CurrentSizeEstimate).</summary>
    public int CurrentSizeEstimate
    {
        get
        {
            int est = _estimate + (_hashIndexBuilder is { Valid: true } ? _hashIndexBuilder.EstimateSize() : 0);
            if (_useCommonPrefix && !_finishing)
            {
                // `_estimate` is the format_version 7 size; the prefix is only stripped from restart-point keys at Finish.
                // Stripping removes P bytes from each of nr restart keys and stores the prefix once: saving P * (nr - 1).
                int saved = _prefixLength * (_numRestarts > 0 ? _numRestarts - 1 : 0);
                est = est > saved ? est - saved : est;
            }
            return est;
        }
    }

    public int NumEntriesInCurrentRestart => _counter;

    /// <summary>Whether the last finished block was marked "is_uniform" (IsUniform).</summary>
    public bool IsUniform => _isUniform;

    /// <summary>Initial estimate_: restart[0] + packed footer (+ values_section_offset with separated storage).</summary>
    private int BaseEstimate => 2 * sizeof(uint) + (_useSeparatedKv ? sizeof(uint) : 0);

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public void Reset()
    {
        _buffer.Clear();
        _restarts[0] = 0;
        _numRestarts = 1;
        _estimate = BaseEstimate;
        _isUniform = false;
        _values?.Clear();
        _counter = 0;
        _finished = false;
        _lastKey.Clear();
        _hashIndexBuilder?.Reset();
        _firstKeyPrefix?.Clear();
        _prefixLength = 0;
        _finishing = false;
    }

    /// <summary>Port of EstimateSizeAfterKV.</summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public int EstimateSizeAfterKV(int keyLength, int valueLength)
    {
        int estimate = CurrentSizeEstimate;
        estimate += keyLength - _stripTsSize;
        estimate += !_useValueDeltaEncoding || _counter >= _restartInterval ? valueLength : valueLength / 2;
        if (_counter >= _restartInterval) estimate += sizeof(uint);
        estimate += sizeof(int); // varint for shared prefix length (imprecise)
        estimate += Coding.VarintLength((ulong)keyLength);
        if (!_useValueDeltaEncoding || _counter >= _restartInterval) estimate += Coding.VarintLength((ulong)valueLength);
        // For separated KV storage, value_offset varint is written at restart points.
        if (_useSeparatedKv && (_counter == 0 || _counter >= _restartInterval)) estimate += Coding.VarintLength((ulong)_values!.WrittenCount);
        return estimate;
    }

    public void Add(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => Add(key, value, default, false);

    /// <summary>
    /// Port of BlockBuilder::Add. REQUIRES: Finish() not called since the last Reset; key larger than any previously
    /// added key. <paramref name="deltaValue"/> is used instead of <paramref name="value"/> for non-restart entries
    /// with value delta encoding. <paramref name="skipDeltaEncoding"/> forces a full key (shared = 0).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public void Add(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ReadOnlySpan<byte> deltaValue, bool skipDeltaEncoding)
    {
        System.Diagnostics.Debug.Assert(!_finished);
        if (_stripTsSize != 0)
        {
            AddStripped(key, value, deltaValue, skipDeltaEncoding);
            return;
        }
        AddCore(key, key, value, deltaValue, skipDeltaEncoding);
    }

    /// <summary>MaybeStripTimestampFromKey: persists the key without its timestamp (hash index keeps the original).</summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private void AddStripped(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ReadOnlySpan<byte> deltaValue, bool skipDeltaEncoding)
    {
        int ts = _stripTsSize;
        PooledBufferWriter b = _stripBuf!;
        b.Clear();
        if (_isUserKey)
        {
            if (key.Length < ts) ThrowHelper.InvalidArgument("key too short for its timestamp");
            b.Write(key[..^ts]);
        }
        else
        {
            // StripTimestampFromInternalKey: user key without timestamp + footer.
            if (key.Length < ts + Db.DbFormat.InternalKeyFooterSize) ThrowHelper.InvalidArgument("key too short for its timestamp");
            int userLen = key.Length - Db.DbFormat.InternalKeyFooterSize - ts;
            b.Write(key[..userLen]);
            b.Write(key[^Db.DbFormat.InternalKeyFooterSize..]);
        }
        AddCore(b.WrittenSpan, key, value, deltaValue, skipDeltaEncoding);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void AddCore(ReadOnlySpan<byte> key, ReadOnlySpan<byte> originalKey, ReadOnlySpan<byte> value, ReadOnlySpan<byte> deltaValue,
        bool skipDeltaEncoding)
    {
        int bufferSize = _buffer.WrittenCount;
        int shared = 0;
        if (_counter >= _restartInterval)
        {
            // Restart compression
            if (_numRestarts == _restarts.Length) GrowRestarts();
            _restarts[_numRestarts++] = (uint)bufferSize;
            _estimate += sizeof(uint);
            _counter = 0;
        }
        else if (_useDeltaEncoding && !skipDeltaEncoding)
        {
            shared = key.CommonPrefixLength(_lastKey.WrittenSpan);
        }

        if (_useCommonPrefix)
        {
            // Track the running common user-key prefix (BlockBuilder::AddWithLastKeyImpl). A non-restart key sharing
            // >= prefix bytes with its predecessor still starts with the whole prefix, so the prefix only shrinks to
            // `shared`; restart entries (shared forced to 0) are rechecked with a real comparison.
            System.Diagnostics.Debug.Assert(!skipDeltaEncoding);
            ReadOnlySpan<byte> uk = _isUserKey ? key : key[..^Db.DbFormat.InternalKeyFooterSize];
            if (bufferSize == 0)
            {
                _firstKeyPrefix!.Clear();
                _firstKeyPrefix.Write(uk);
                _prefixLength = uk.Length;
            }
            else if (_counter == 0)
            {
                int common = _firstKeyPrefix!.WrittenSpan[.._prefixLength].CommonPrefixLength(uk);
                if (common < _prefixLength) _prefixLength = common;
            }
            else if (shared < _prefixLength)
            {
                _prefixLength = shared;
            }
        }

        int nonShared = key.Length - shared;
        int prevValuesSize = _useSeparatedKv ? _values!.WrittenCount : 0;

        Span<byte> hdr = _buffer.GetSpan(4 * Coding.MaxVarint32Length);
        int n = Coding.EncodeVarint32(hdr, (uint)shared);
        n += Coding.EncodeVarint32(hdr[n..], (uint)nonShared);
        if (!_useValueDeltaEncoding) n += Coding.EncodeVarint32(hdr[n..], (uint)value.Length);
        // Separated storage: restart entries add "<value_offset>" (the value's offset in the values section).
        if (_useSeparatedKv && _counter == 0) n += Coding.EncodeVarint32(hdr[n..], (uint)prevValuesSize);
        _buffer.Advance(n);
        _buffer.Write(key[shared..]);
        PooledBufferWriter valuesBuffer = _useSeparatedKv ? _values! : _buffer;
        // Value delta encoding is only used when the key shares bytes, so the decoder can tell which form is used.
        if (shared != 0 && _useValueDeltaEncoding) valuesBuffer.Write(deltaValue);
        else valuesBuffer.Write(value);

        if (_hashIndexBuilder is { Valid: true })
        {
            _hashIndexBuilder.Add(originalKey[..^Db.DbFormat.InternalKeyFooterSize], _numRestarts - 1);
        }

        if (_useDeltaEncoding)
        {
            _lastKey.Clear();
            _lastKey.Write(key);
        }
        _counter++;
        _estimate += _buffer.WrittenCount - bufferSize + (_useSeparatedKv ? _values!.WrittenCount - prevValuesSize : 0);
    }

    /// <summary>Finishes building the block and returns its contents (valid until Reset/Dispose).</summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public ReadOnlySpan<byte> Finish()
    {
        if (_useCommonPrefix && _buffer.WrittenCount > 0 && _prefixLength > 0) RewriteRestartKeysStrippingPrefix();
        // Safe to run after the strip above: stripping removes the same block-common prefix from every restart key, so
        // the uniformity decision matches running on the original keys.
        _isUniform = ScanForUniformity();
        int valuesSectionOffset = _buffer.WrittenCount;
        if (_useSeparatedKv) _buffer.Write(_values!.WrittenSpan);
        for (int i = 0; i < _numRestarts; i++) Coding.PutFixed32(_buffer, _restarts[i]);
        // DataBlockFooter::EncodeTo.
        uint packed = (uint)_numRestarts;
        if (_hashIndexBuilder is { Valid: true } && CurrentSizeEstimate <= DataBlockHashIndex.MaxBlockSizeSupportedByHashIndex)
        {
            _hashIndexBuilder.Finish(_buffer);
            packed |= HashIndexBit;
        }
        if (_useSeparatedKv)
        {
            Coding.PutFixed32(_buffer, (uint)valuesSectionOffset);
            packed |= SeparatedKvBit;
        }
        if (_isUniform) packed |= UniformKeysBit;
        Coding.PutFixed32(_buffer, packed);
        _finished = true;
        return _buffer.WrittenSpan;
    }

    /// <summary>
    /// Port of BlockBuilder::RewriteRestartKeysStrippingPrefix: moves the common prefix to the block's leading bytes
    /// (restarts[0] becomes its length) and re-emits each restart entry's key without it; all other bytes are copied.
    /// </summary>
    private void RewriteRestartKeysStrippingPrefix()
    {
        _finishing = true;
        int p = _prefixLength;
        ReadOnlySpan<byte> src = _buffer.WrittenSpan;
        int keysEnd = src.Length;
        PooledBufferWriter dst = _rewriteBuffer!;
        dst.Clear();
        dst.Write(_firstKeyPrefix!.WrittenSpan[..p]);
        for (int k = 0; k < _numRestarts; k++)
        {
            int rStart = (int)_restarts[k];
            int intervalEnd = k + 1 < _numRestarts ? (int)_restarts[k + 1] : keysEnd;
            ReadOnlySpan<byte> input = src[rStart..intervalEnd];
            int before = input.Length;
            uint valueSize = 0, valueOffset = 0;
            bool ok = Coding.GetVarint32(ref input, out uint shared);
            ok &= Coding.GetVarint32(ref input, out uint nonShared);
            if (!_useValueDeltaEncoding) ok &= Coding.GetVarint32(ref input, out valueSize);
            if (_useSeparatedKv) ok &= Coding.GetVarint32(ref input, out valueOffset);
            System.Diagnostics.Debug.Assert(ok && shared == 0 && nonShared >= (uint)p);
            int keyOffset = rStart + (before - input.Length);

            // Re-emit the restart entry header with the prefix removed from the key length; shared stays 0.
            _restarts[k] = (uint)dst.WrittenCount;
            Span<byte> hdr = dst.GetSpan(4 * Coding.MaxVarint32Length);
            int n = Coding.EncodeVarint32(hdr, 0);
            n += Coding.EncodeVarint32(hdr[n..], nonShared - (uint)p);
            if (!_useValueDeltaEncoding) n += Coding.EncodeVarint32(hdr[n..], valueSize);
            if (_useSeparatedKv) n += Coding.EncodeVarint32(hdr[n..], valueOffset);
            dst.Advance(n);
            dst.Write(src.Slice(keyOffset + p, (int)nonShared - p));
            // The inline value plus all non-restart entries are independent of the common prefix.
            dst.Write(src[(keyOffset + (int)nonShared)..intervalEnd]);
        }
        _rewriteBuffer = _buffer;
        _buffer = dst;
        _estimate = _buffer.WrittenCount + (_useSeparatedKv ? _values!.WrittenCount : 0) + _numRestarts * sizeof(uint) + sizeof(uint)
            + (_useSeparatedKv ? sizeof(uint) : 0);
    }

    /// <summary>Port of BlockBuilder::GetRestartKey: the stored key of restart entry <paramref name="index"/>.</summary>
    private ReadOnlySpan<byte> GetRestartKey(int index)
    {
        ReadOnlySpan<byte> input = _buffer.WrittenSpan[(int)_restarts[index]..];
        Coding.GetVarint32(ref input, out _);
        Coding.GetVarint32(ref input, out uint nonShared);
        if (!_useValueDeltaEncoding) Coding.GetVarint32(ref input, out _);
        if (_useSeparatedKv) Coding.GetVarint32(ref input, out _);
        return input[..(int)nonShared];
    }

    /// <summary>
    /// Port of BlockBuilder::ScanForUniformity: with uniform_cv_threshold &gt;= 0 and at least 3 restarts, the gaps between
    /// the big-endian 64-bit values of the restart keys (after the prefix shared by the first and last restart key) have
    /// a coefficient of variation below the threshold. Cold-ish: once per finished block, only when enabled.
    /// </summary>
    private bool ScanForUniformity()
    {
        if (_uniformCvThreshold < 0 || _numRestarts < 3) return false;
        ReadOnlySpan<byte> firstKey = GetRestartKey(0);
        ReadOnlySpan<byte> lastKey = GetRestartKey(_numRestarts - 1);
        // Keys must be long enough for ReadBe64FromKey which strips internal bytes
        if (!_isUserKey && (firstKey.Length < Db.DbFormat.InternalKeyFooterSize || lastKey.Length < Db.DbFormat.InternalKeyFooterSize)) return false;
        int prefixLen = firstKey.CommonPrefixLength(lastKey);
        // UniformDataTracker (Welford's online variance over the key gaps).
        ulong prevKeyValue = 0;
        long numKeys = 0;
        double mean = 0, m2 = 0;
        for (int i = 0; i < _numRestarts; i++)
        {
            ReadOnlySpan<byte> key = GetRestartKey(i);
            if (!_isUserKey && key.Length < Db.DbFormat.InternalKeyFooterSize) return false;
            ulong keyValue = ReadBe64FromKey(key, _isUserKey, prefixLen);
            if (numKeys > 0)
            {
                double gap = (double)(keyValue - prevKeyValue);
                double delta = gap - mean;
                mean += delta / numKeys;
                double delta2 = gap - mean;
                m2 += delta * delta2;
            }
            prevKeyValue = keyValue;
            numKeys++;
        }
        long gapCount = numKeys > 0 ? numKeys - 1 : 0;
        double cv = gapCount < 2 || mean <= 0 ? -1.0 : Math.Sqrt(m2 / gapCount) / mean;
        return cv >= 0 && cv < _uniformCvThreshold;
    }

    /// <summary>Port of ReadBe64FromKey (table/block_based/block_util.h): 8 big-endian bytes at <paramref name="offset"/>, zero padded.</summary>
    internal static ulong ReadBe64FromKey(ReadOnlySpan<byte> s, bool isUserKey, int offset)
    {
        if (!isUserKey) s = s[..^Db.DbFormat.InternalKeyFooterSize];
        offset = Math.Min(offset, s.Length);
        int remaining = s.Length - offset;
        if (remaining >= 8) return System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian(s[offset..]);
        ulong val = 0;
        for (int i = 0; i < remaining; i++) val = (val << 8) | s[offset + i];
        if (remaining > 0) val <<= (8 - remaining) * 8; // Pad zeros on the right
        return val;
    }

    /// <summary>The finished block contents (after <see cref="Finish"/>).</summary>
    public ReadOnlySpan<byte> Contents => _buffer.WrittenSpan;
    public ReadOnlyMemory<byte> ContentsMemory => _buffer.WrittenMemory;

    private void GrowRestarts()
    {
        uint[] n = TableBuilderBuffers.UInts.Rent(_restarts.Length * 2);
        _restarts.AsSpan(0, _numRestarts).CopyTo(n);
        TableBuilderBuffers.UInts.Return(_restarts);
        _restarts = n;
    }

    public void Dispose()
    {
        _buffer.Dispose();
        _lastKey.Dispose();
        _stripBuf?.Dispose();
        _firstKeyPrefix?.Dispose();
        _rewriteBuffer?.Dispose();
        _values?.Dispose();
        _hashIndexBuilder?.Release();
        if (_restarts.Length > 0) TableBuilderBuffers.UInts.Return(_restarts);
        _restarts = [];
    }
}
