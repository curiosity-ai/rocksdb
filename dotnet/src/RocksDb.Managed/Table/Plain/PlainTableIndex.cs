using RocksDb.Managed.Util;

namespace RocksDb.Managed.Table.Plain;

/// <summary>
/// Port of PlainTableIndex (table/plain/plain_table_index.{h,cc}): the prefix hash index of a plain table.
/// <code>
///   varint32 index_size (hash buckets), varint32 num_prefixes,
///   fixed32 bucket[index_size]: kMaxFileSize (no prefix), a file offset (one record), or
///                               kSubIndexMask | offset into the sub index (several records),
///   sub index: per such bucket varint32 count + fixed32 file offsets (sorted by key).
/// </code>
/// Stored in the file as the "PlainTableIndexBlock" meta block with store_index_in_file, otherwise rebuilt at open.
/// </summary>
internal sealed class PlainTableIndex
{
    public enum SearchResult { NoPrefixForBucket = 0, DirectToFile = 1, Subindex = 2 }

    public const uint MaxFileSize = (1u << 31) - 1;
    public const uint SubIndexMask = 0x80000000;
    public const int OffsetLen = sizeof(uint);
    public const string PlainTableIndexBlock = "PlainTableIndexBlock";

    private byte[] _data = [];
    private int _indexOffset;     // start of the bucket array in _data
    private int _subIndexOffset;  // start of the sub index in _data
    private int _end;

    public uint IndexSize { get; private set; }
    public uint SubIndexSize { get; private set; }
    public uint NumPrefixes { get; private set; }

    /// <summary>InitFromRawData: <paramref name="data"/>[offset..offset+length) is the encoded index (not copied).</summary>
    public void InitFromRawData(byte[] data, int offset, int length)
    {
        ReadOnlySpan<byte> input = data.AsSpan(offset, length);
        if (!Coding.GetVarint32(ref input, out uint indexSize)) ThrowHelper.Corruption("Couldn't read the index size!");
        if (!Coding.GetVarint32(ref input, out uint numPrefixes)) ThrowHelper.Corruption("Couldn't read the index size!");
        if (indexSize == 0 || (ulong)indexSize * OffsetLen > (ulong)input.Length) ThrowHelper.Corruption("Bad plain table index size " + indexSize);
        _data = data;
        _indexOffset = offset + length - input.Length;
        _subIndexOffset = _indexOffset + (int)indexSize * OffsetLen;
        _end = offset + length;
        IndexSize = indexSize;
        NumPrefixes = numPrefixes;
        SubIndexSize = (uint)(input.Length - (int)indexSize * OffsetLen);
    }

    /// <summary>GetOffset: the bucket of <paramref name="prefixHash"/>.</summary>
    public SearchResult GetOffset(uint prefixHash, out uint bucketValue)
    {
        uint bucket = prefixHash % IndexSize;
        bucketValue = Coding.DecodeFixed32(_data.AsSpan(_indexOffset + (int)bucket * OffsetLen, OffsetLen));
        if ((bucketValue & SubIndexMask) == SubIndexMask)
        {
            bucketValue ^= SubIndexMask;
            return SearchResult.Subindex;
        }
        // point directly to the file, or no prefix for the bucket
        return bucketValue >= MaxFileSize ? SearchResult.NoPrefixForBucket : SearchResult.DirectToFile;
    }

    /// <summary>GetSubIndexBasePtrAndUpperBound: the record count of a sub index entry and its offset array.</summary>
    public ReadOnlySpan<byte> GetSubIndex(uint offset, out uint upperBound)
    {
        int p = _subIndexOffset + (int)offset;
        if (p >= _end) ThrowHelper.Corruption("plain table sub index offset out of range");
        int n = Coding.DecodeVarint32(_data.AsSpan(p, Math.Min(4, _end - p)), out upperBound);
        if (n == 0 || (ulong)(p + n) + (ulong)upperBound * OffsetLen > (ulong)_end) ThrowHelper.Corruption("bad plain table sub index");
        return _data.AsSpan(p + n, (int)upperBound * OffsetLen);
    }
}

/// <summary>
/// Port of PlainTableIndexBuilder (table/plain/plain_table_index.cc): collects (prefix hash, file offset) records for
/// the first key of each prefix and every index_sparseness-th key within a prefix, then buckets them by hash.
/// </summary>
internal sealed class PlainTableIndexBuilder
{
    private readonly List<(uint Hash, uint Offset)> _records = new();
    private bool _isFirstRecord = true;
    private bool _dueIndex;
    private uint _numPrefixes;
    private uint _numKeysPerPrefix;
    private uint _prevKeyPrefixHash;
    private readonly ulong _indexSparseness;
    private uint _indexSize;
    private uint _subIndexSize;
    private readonly bool _hasPrefixExtractor;
    private readonly double _hashTableRatio;
    private byte[] _prevKeyPrefix = [];
    private int _prevKeyPrefixLength;

    public PlainTableIndexBuilder(bool hasPrefixExtractor, ulong indexSparseness, double hashTableRatio)
    {
        _hasPrefixExtractor = hasPrefixExtractor;
        _indexSparseness = indexSparseness;
        _hashTableRatio = hashTableRatio;
    }

    public uint NumPrefixes => _numPrefixes;

    public void AddKeyPrefix(ReadOnlySpan<byte> keyPrefix, uint keyOffset)
    {
        if (_isFirstRecord || !keyPrefix.SequenceEqual(_prevKeyPrefix.AsSpan(0, _prevKeyPrefixLength)))
        {
            ++_numPrefixes;
            _numKeysPerPrefix = 0;
            if (_prevKeyPrefix.Length < keyPrefix.Length) _prevKeyPrefix = new byte[Math.Max(keyPrefix.Length, 16)];
            keyPrefix.CopyTo(_prevKeyPrefix);
            _prevKeyPrefixLength = keyPrefix.Length;
            _prevKeyPrefixHash = Hash.GetSliceHash(keyPrefix);
            _dueIndex = true;
        }
        if (_dueIndex)
        {
            // Add an index key for every kIndexIntervalForSamePrefixKeys keys
            _records.Add((_prevKeyPrefixHash, keyOffset));
            _dueIndex = false;
        }
        _numKeysPerPrefix++;
        if (_indexSparseness == 0 || _numKeysPerPrefix % _indexSparseness == 0) _dueIndex = true;
        _isFirstRecord = false;
    }

    /// <summary>Finish: the encoded index (see <see cref="PlainTableIndex"/>).</summary>
    public byte[] Finish()
    {
        AllocateIndex();
        // BucketizeIndexes: records of a bucket in insertion order (upstream links them newest first and fills the
        // sub index from the back, which yields the same order).
        var buckets = new List<uint>?[_indexSize];
        var single = new uint[_indexSize];
        var counts = new uint[_indexSize];
        foreach ((uint hash, uint offset) in _records)
        {
            uint b = hash % _indexSize;
            if (counts[b]++ == 0)
            {
                single[b] = offset;
            }
            else
            {
                List<uint> l = buckets[b] ??= [single[b]];
                l.Add(offset);
            }
        }
        _subIndexSize = 0;
        foreach (uint c in counts)
        {
            if (c <= 1) continue;
            // Only buckets with more than 1 entry will have subindex.
            _subIndexSize += (uint)Coding.VarintLength(c) + c * PlainTableIndex.OffsetLen;
        }
        // FillIndexes
        int total = Coding.VarintLength(_indexSize) + Coding.VarintLength(_numPrefixes) + PlainTableIndex.OffsetLen * (int)_indexSize + (int)_subIndexSize;
        byte[] dst = new byte[total];
        int p = Coding.EncodeVarint32(dst, _indexSize);
        p += Coding.EncodeVarint32(dst.AsSpan(p), _numPrefixes);
        int index = p;
        int subIndex = index + (int)_indexSize * PlainTableIndex.OffsetLen;
        uint subIndexOffset = 0;
        for (int i = 0; i < _indexSize; i++)
        {
            uint n = counts[i];
            Span<byte> slot = dst.AsSpan(index + i * PlainTableIndex.OffsetLen, PlainTableIndex.OffsetLen);
            switch (n)
            {
                case 0:
                    // No key for bucket
                    Coding.EncodeFixed32(slot, PlainTableIndex.MaxFileSize);
                    break;
                case 1:
                    // point directly to the file offset
                    Coding.EncodeFixed32(slot, single[i]);
                    break;
                default:
                    // point to second level indexes.
                    Coding.EncodeFixed32(slot, subIndexOffset | PlainTableIndex.SubIndexMask);
                    subIndexOffset += (uint)Coding.EncodeVarint32(dst.AsSpan(subIndex + (int)subIndexOffset), n);
                    List<uint> l = buckets[i]!;
                    for (int j = 0; j < l.Count; j++)
                        Coding.EncodeFixed32(dst.AsSpan(subIndex + (int)subIndexOffset + j * PlainTableIndex.OffsetLen), l[j]);
                    subIndexOffset += PlainTableIndex.OffsetLen * n;
                    break;
            }
        }
        return dst;
    }

    private void AllocateIndex()
    {
        if (!_hasPrefixExtractor || _hashTableRatio <= 0)
        {
            // Fall back to pure binary search if the user fails to specify a prefix extractor.
            _indexSize = 1;
        }
        else
        {
            double hashTableSizeMultipier = 1.0 / _hashTableRatio;
            _indexSize = (uint)(_numPrefixes * hashTableSizeMultipier) + 1;
        }
    }
}
