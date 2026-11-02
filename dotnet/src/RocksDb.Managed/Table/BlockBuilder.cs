using System.Buffers;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Table;

/// <summary>Mirrors BlockBasedTableOptions::DataBlockIndexType (include/rocksdb/table.h).</summary>
public enum DataBlockIndexType : byte
{
    BinarySearch = 0,
    BinaryAndHash = 1,
}

/// <summary>
/// Builds blocks where keys are prefix-compressed. Port of table/block_based/block_builder.{h,cc}
/// (format_version &lt;= 7 subset: no common-prefix section, no separated KV storage, no uniformity bit).
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
    private readonly PooledBufferWriter _buffer;
    private readonly PooledBufferWriter _lastKey;
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
    {
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
    public int CurrentSizeEstimate => _estimate + (_hashIndexBuilder is { Valid: true } ? _hashIndexBuilder.EstimateSize() : 0);

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
        _hashIndexBuilder?.Release();
        if (_restarts.Length > 0) ArrayPool<uint>.Shared.Return(_restarts);
        _restarts = [];
    }
}
