using System.Buffers;
using System.Runtime.CompilerServices;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Table;

/// <summary>
/// Optional hash index appended to data blocks (data_block_index_type = kDataBlockBinaryAndHash), mapping
/// hash(user_key) % num_buckets → restart interval index. Port of table/block_based/data_block_hash_index.{h,cc}.
/// Layout appended after the restart array: <c>uint8 buckets[num_buckets]; fixed16 num_buckets</c>, and bit 31 of
/// the packed restart-count word is set.
/// </summary>
internal static class DataBlockHashIndex
{
    public const byte NoEntry = 255;
    public const byte Collision = 254;
    public const int MaxRestartSupportedByHashIndex = 253;
    /// <summary>Because uint16 addresses are used, only blocks smaller than 64KiB carry a hash index.</summary>
    public const int MaxBlockSizeSupportedByHashIndex = 1 << 16;
    public const double DefaultUtilRatio = 0.75;

    /// <summary>Port of DataBlockHashIndex::Initialize. <paramref name="size"/> = bytes preceding the packed footer word.</summary>
    public static bool TryInitialize(ReadOnlySpan<byte> block, int size, out int mapOffset, out int numBuckets)
    {
        mapOffset = 0;
        numBuckets = 0;
        if (size < 2 || size >= MaxBlockSizeSupportedByHashIndex) return false;
        int nb = Coding.DecodeFixed16(block[(size - 2)..]);
        if (nb == 0 || nb + 2 > size) return false;
        numBuckets = nb;
        mapOffset = size - 2 - nb;
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static byte Lookup(ReadOnlySpan<byte> block, int mapOffset, int numBuckets, ReadOnlySpan<byte> userKey)
    {
        uint h = Hash.GetSliceHash(userKey); // GetSliceHash = Hash(data, n, 397), util/hash.h
        return block[mapOffset + (int)(h % (uint)numBuckets)];
    }
}

/// <summary>Port of DataBlockHashIndexBuilder (table/block_based/data_block_hash_index.{h,cc}).</summary>
internal sealed class DataBlockHashIndexBuilder
{
    private double _bucketPerKey = -1;
    private double _estimatedNumBuckets;
    private bool _valid;
    private ulong[] _pairs = []; // (hash << 8) | restartIndex
    private int _count;

    public void Initialize(double utilRatio)
    {
        if (utilRatio <= 0) utilRatio = DataBlockHashIndex.DefaultUtilRatio;
        _bucketPerKey = 1 / utilRatio;
        _valid = true;
    }

    public bool Valid => _valid && _bucketPerKey > 0;

    public void Add(ReadOnlySpan<byte> userKey, int restartIndex)
    {
        if (restartIndex > DataBlockHashIndex.MaxRestartSupportedByHashIndex)
        {
            _valid = false;
            return;
        }
        uint hash = Hash.GetSliceHash(userKey);
        if (_count == _pairs.Length)
        {
            ulong[] n = ArrayPool<ulong>.Shared.Rent(Math.Max(64, _count * 2));
            _pairs.AsSpan(0, _count).CopyTo(n);
            if (_pairs.Length > 0) ArrayPool<ulong>.Shared.Return(_pairs);
            _pairs = n;
        }
        _pairs[_count++] = ((ulong)hash << 8) | (byte)restartIndex;
        _estimatedNumBuckets += _bucketPerKey;
    }

    public int EstimateSize()
    {
        ushort est = (ushort)_estimatedNumBuckets;
        est |= 1;
        return 2 + est;
    }

    public void Finish(PooledBufferWriter buffer)
    {
        ushort numBuckets = (ushort)_estimatedNumBuckets;
        if (numBuckets == 0) numBuckets = 1;
        // The built-in hash distributes poorly with power-of-two bucket counts; force odd.
        numBuckets |= 1;
        Span<byte> buckets = buffer.GetSpan(numBuckets + 2)[..numBuckets];
        buckets.Fill(DataBlockHashIndex.NoEntry);
        for (int i = 0; i < _count; i++)
        {
            uint hash = (uint)(_pairs[i] >> 8);
            byte restart = (byte)_pairs[i];
            int idx = (int)(hash % numBuckets);
            if (buckets[idx] == DataBlockHashIndex.NoEntry) buckets[idx] = restart;
            else if (buckets[idx] != restart) buckets[idx] = DataBlockHashIndex.Collision;
        }
        buffer.Advance(numBuckets);
        Coding.PutFixed16(buffer, numBuckets);
    }

    public void Reset()
    {
        _estimatedNumBuckets = 0;
        _valid = true;
        _count = 0;
    }

    public void Release()
    {
        if (_pairs.Length > 0) ArrayPool<ulong>.Shared.Return(_pairs);
        _pairs = [];
        _count = 0;
    }
}
