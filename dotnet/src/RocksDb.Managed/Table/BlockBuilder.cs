using System.Buffers;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Table;



/// <summary>
/// Builds blocks where keys are prefix-compressed. Port of table/block_based/block_builder.{h,cc}
/// (no separated KV storage, no uniformity bit).
/// With the common-prefix feature (optimize_key_common_prefix, format_version &gt;= 8) the user-key prefix shared by
/// every key of the block is stored once in its leading bytes (restarts[0] = prefix length) and restart entries
/// (shared == 0) store their key without it; non-restart entries are unchanged (their shared bytes cover the prefix).
/// <code>
/// entry:   shared_bytes varint32 | unshared_bytes varint32 | value_length varint32 (omitted with value delta
///          encoding, i.e. format_version &gt;= 4 index blocks) | key_delta | value
/// trailer: restarts uint32[num_restarts] | [hash index] | packed(num_restarts | hash bit 31) uint32
/// </code>
/// </summary>
public sealed class BlockBuilder : IDisposable
{
    private const uint HashIndexBit = 1u << 31;

    private readonly int _restartInterval;
    private readonly bool _useDeltaEncoding;
    private readonly bool _useValueDeltaEncoding;
    private readonly DataBlockHashIndexBuilder? _hashIndexBuilder;
    private readonly bool _useCommonPrefix;
    private readonly bool _isUserKey;
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
    {
        if (useCommonPrefix && !useDeltaEncoding) throw new ArgumentException("the common-prefix feature requires delta encoding", nameof(useCommonPrefix));
        _useCommonPrefix = useCommonPrefix;
        _isUserKey = isUserKey;
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
        _buffer = new PooledBufferWriter(4096);
        _lastKey = new PooledBufferWriter(64);
        _restarts = ArrayPool<uint>.Shared.Rent(64);
        _restarts[0] = 0;
        _numRestarts = 1;
        _estimate = 2 * sizeof(uint);
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

    public void Reset()
    {
        _buffer.Clear();
        _restarts[0] = 0;
        _numRestarts = 1;
        _estimate = 2 * sizeof(uint);
        _counter = 0;
        _finished = false;
        _lastKey.Clear();
        _hashIndexBuilder?.Reset();
        _firstKeyPrefix?.Clear();
        _prefixLength = 0;
        _finishing = false;
    }

    /// <summary>Port of EstimateSizeAfterKV.</summary>
    public int EstimateSizeAfterKV(int keyLength, int valueLength)
    {
        int estimate = CurrentSizeEstimate;
        estimate += keyLength;
        estimate += !_useValueDeltaEncoding || _counter >= _restartInterval ? valueLength : valueLength / 2;
        if (_counter >= _restartInterval) estimate += sizeof(uint);
        estimate += sizeof(int); // varint for shared prefix length (imprecise)
        estimate += Coding.VarintLength((ulong)keyLength);
        if (!_useValueDeltaEncoding || _counter >= _restartInterval) estimate += Coding.VarintLength((ulong)valueLength);
        return estimate;
    }

    public void Add(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => Add(key, value, default, false);

    /// <summary>
    /// Port of BlockBuilder::Add. REQUIRES: Finish() not called since the last Reset; key larger than any previously
    /// added key. <paramref name="deltaValue"/> is used instead of <paramref name="value"/> for non-restart entries
    /// with value delta encoding. <paramref name="skipDeltaEncoding"/> forces a full key (shared = 0).
    /// </summary>
    public void Add(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ReadOnlySpan<byte> deltaValue, bool skipDeltaEncoding)
    {
        System.Diagnostics.Debug.Assert(!_finished);
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

        Span<byte> hdr = _buffer.GetSpan(3 * Coding.MaxVarint32Length);
        int n = Coding.EncodeVarint32(hdr, (uint)shared);
        n += Coding.EncodeVarint32(hdr[n..], (uint)nonShared);
        if (!_useValueDeltaEncoding) n += Coding.EncodeVarint32(hdr[n..], (uint)value.Length);
        _buffer.Advance(n);
        _buffer.Write(key[shared..]);
        // Value delta encoding is only used when the key shares bytes, so the decoder can tell which form is used.
        if (shared != 0 && _useValueDeltaEncoding) _buffer.Write(deltaValue);
        else _buffer.Write(value);

        if (_hashIndexBuilder is { Valid: true })
        {
            _hashIndexBuilder.Add(key[..^Db.DbFormat.InternalKeyFooterSize], _numRestarts - 1);
        }

        if (_useDeltaEncoding)
        {
            _lastKey.Clear();
            _lastKey.Write(key);
        }
        _counter++;
        _estimate += _buffer.WrittenCount - bufferSize;
    }

    /// <summary>Finishes building the block and returns its contents (valid until Reset/Dispose).</summary>
    public ReadOnlySpan<byte> Finish()
    {
        if (_useCommonPrefix && _buffer.WrittenCount > 0 && _prefixLength > 0) RewriteRestartKeysStrippingPrefix();
        for (int i = 0; i < _numRestarts; i++) Coding.PutFixed32(_buffer, _restarts[i]);
        uint packed = (uint)_numRestarts;
        if (_hashIndexBuilder is { Valid: true } && CurrentSizeEstimate <= DataBlockHashIndex.MaxBlockSizeSupportedByHashIndex)
        {
            _hashIndexBuilder.Finish(_buffer);
            packed |= HashIndexBit;
        }
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
            uint valueSize = 0;
            bool ok = Coding.GetVarint32(ref input, out uint shared);
            ok &= Coding.GetVarint32(ref input, out uint nonShared);
            if (!_useValueDeltaEncoding) ok &= Coding.GetVarint32(ref input, out valueSize);
            System.Diagnostics.Debug.Assert(ok && shared == 0 && nonShared >= (uint)p);
            int keyOffset = rStart + (before - input.Length);

            // Re-emit the restart entry header with the prefix removed from the key length; shared stays 0.
            _restarts[k] = (uint)dst.WrittenCount;
            Span<byte> hdr = dst.GetSpan(3 * Coding.MaxVarint32Length);
            int n = Coding.EncodeVarint32(hdr, 0);
            n += Coding.EncodeVarint32(hdr[n..], nonShared - (uint)p);
            if (!_useValueDeltaEncoding) n += Coding.EncodeVarint32(hdr[n..], valueSize);
            dst.Advance(n);
            dst.Write(src.Slice(keyOffset + p, (int)nonShared - p));
            // The inline value plus all non-restart entries are independent of the common prefix.
            dst.Write(src[(keyOffset + (int)nonShared)..intervalEnd]);
        }
        _rewriteBuffer = _buffer;
        _buffer = dst;
        _estimate = _buffer.WrittenCount + _numRestarts * sizeof(uint) + sizeof(uint);
    }

    /// <summary>The finished block contents (after <see cref="Finish"/>).</summary>
    public ReadOnlySpan<byte> Contents => _buffer.WrittenSpan;
    public ReadOnlyMemory<byte> ContentsMemory => _buffer.WrittenMemory;

    private void GrowRestarts()
    {
        uint[] n = ArrayPool<uint>.Shared.Rent(_restarts.Length * 2);
        _restarts.AsSpan(0, _numRestarts).CopyTo(n);
        ArrayPool<uint>.Shared.Return(_restarts);
        _restarts = n;
    }

    public void Dispose()
    {
        _buffer.Dispose();
        _lastKey.Dispose();
        _firstKeyPrefix?.Dispose();
        _rewriteBuffer?.Dispose();
        _hashIndexBuilder?.Release();
        if (_restarts.Length > 0) ArrayPool<uint>.Shared.Return(_restarts);
        _restarts = [];
    }
}
