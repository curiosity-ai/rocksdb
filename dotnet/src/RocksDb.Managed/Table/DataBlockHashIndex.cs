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

    /// <summary>
    /// GetSliceHash = Hash(data, n, 397) from util/hash.cc (MurmurHash1 variant). Part of the on-disk format.
    /// Kept private to the table module; stream C owns the general-purpose hash helpers.
    /// </summary>
    public static uint GetSliceHash(ReadOnlySpan<byte> data) => LegacyHash(data, 397);

    /// <summary>Port of rocksdb::Hash (util/hash.cc), including sign-extension of trailing bytes.</summary>
    public static uint LegacyHash(ReadOnlySpan<byte> data, uint seed)
    {
        const uint m = 0xc6a4a793;
        const int r = 24;
        uint h = seed ^ (uint)((ulong)data.Length * m);
        int i = 0;
        for (; i + 4 <= data.Length; i += 4)
        {
            h += Coding.DecodeFixed32(data[i..]);
            h *= m;
            h ^= h >> 16;
        }
        switch (data.Length - i)
        {
            case 3:
                h += (uint)(sbyte)data[i + 2] << 16;
                goto case 2;
            case 2:
                h += (uint)(sbyte)data[i + 1] << 8;
                goto case 1;
            case 1:
                h += (uint)(sbyte)data[i];
                h *= m;
                h ^= h >> r;
                break;
        }
        return h;
    }

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
        uint h = GetSliceHash(userKey);
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
        uint hash = DataBlockHashIndex.GetSliceHash(userKey);
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
