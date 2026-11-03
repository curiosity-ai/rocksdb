using System.Runtime.CompilerServices;

namespace RocksDb.Managed.Table.Plain;

/// <summary>
/// Port of PlainTableBloomV1 (table/plain/plain_table_bloom.{h,cc}): the plain table's bloom filter over 32-bit
/// GetSliceHash values of keys (total order mode) or prefixes. With bloom_locality &gt; 0 the filter is split into an
/// odd number of 64-byte cache lines probed with LegacyLocalityBloomImpl&lt;ExtraRotates=true&gt;; otherwise
/// LegacyNoLocalityBloomImpl probes the whole bit array. The raw bits are the "kBloomBlock" meta block when
/// store_index_in_file is set. CACHE_LINE_SIZE is 64 (x86-64 / aarch64 Linux builds).
/// </summary>
internal sealed class PlainTableBloom
{
    private const int CacheLineSize = 64;
    private const int Log2CacheLineSize = 6;

    private uint _totalBits;
    private uint _numBlocks;
    private readonly int _numProbes;
    private byte[] _data = [];
    private int _dataOffset;

    public PlainTableBloom(int numProbes) => _numProbes = numProbes;

    public uint NumBlocks => _numBlocks;
    public uint TotalBits => _totalBits;
    public bool IsInitialized => _numBlocks > 0 || _totalBits > 0;

    /// <summary>GetRawData: the filter bits (TotalBits / 8 bytes).</summary>
    public ReadOnlySpan<byte> RawData => _data.AsSpan(_dataOffset, (int)(_totalBits / 8));

    /// <summary>GetTotalBitsForLocality: whole cache lines, an odd number of them.</summary>
    private static uint GetTotalBitsForLocality(uint totalBits)
    {
        uint numBlocks = (totalBits + CacheLineSize * 8 - 1) / (CacheLineSize * 8);
        // Make num_blocks an odd number to make sure more bits are involved when determining which block.
        if (numBlocks % 2 == 0) numBlocks++;
        return numBlocks * (CacheLineSize * 8);
    }

    /// <summary>SetTotalBits: allocates a zeroed filter.</summary>
    public void SetTotalBits(uint totalBits, uint locality)
    {
        _totalBits = locality > 0 ? GetTotalBitsForLocality(totalBits) : (totalBits + 7) / 8 * 8;
        _numBlocks = locality > 0 ? _totalBits / (CacheLineSize * 8) : 0;
        _data = new byte[_totalBits / 8];
        _dataOffset = 0;
    }

    /// <summary>SetRawData: uses filter bits read from the file.</summary>
    public void SetRawData(byte[] data, int offset, uint totalBits, uint numBlocks)
    {
        _data = data;
        _dataOffset = offset;
        _totalBits = totalBits;
        _numBlocks = numBlocks;
    }

    public void AddHash(uint h)
    {
        Span<byte> data = _data.AsSpan(_dataOffset);
        if (_numBlocks != 0)
        {
            Span<byte> line = data.Slice((int)(GetLine(h, _numBlocks) << Log2CacheLineSize), CacheLineSize);
            uint delta = (h >> 17) | (h << 15);
            for (int i = 0; i < _numProbes; ++i)
            {
                uint bitpos = h & ((1u << (Log2CacheLineSize + 3)) - 1);
                line[(int)(bitpos / 8)] |= (byte)(1 << (int)(bitpos % 8));
                h = (h >> (Log2CacheLineSize + 3)) | (h << (32 - (Log2CacheLineSize + 3)));
                h += delta;
            }
        }
        else
        {
            uint delta = (h >> 17) | (h << 15); // Rotate right 17 bits
            for (int i = 0; i < _numProbes; i++)
            {
                uint bitpos = h % _totalBits;
                data[(int)(bitpos / 8)] |= (byte)(1 << (int)(bitpos % 8));
                h += delta;
            }
        }
    }

    public bool MayContainHash(uint h)
    {
        ReadOnlySpan<byte> data = _data.AsSpan(_dataOffset);
        if (_numBlocks != 0)
        {
            ReadOnlySpan<byte> line = data.Slice((int)(GetLine(h, _numBlocks) << Log2CacheLineSize), CacheLineSize);
            uint delta = (h >> 17) | (h << 15);
            for (int i = 0; i < _numProbes; ++i)
            {
                uint bitpos = h & ((1u << (Log2CacheLineSize + 3)) - 1);
                if ((line[(int)(bitpos / 8)] & (1 << (int)(bitpos % 8))) == 0) return false;
                h = (h >> (Log2CacheLineSize + 3)) | (h << (32 - (Log2CacheLineSize + 3)));
                h += delta;
            }
            return true;
        }
        else
        {
            uint delta = (h >> 17) | (h << 15);
            for (int i = 0; i < _numProbes; i++)
            {
                uint bitpos = h % _totalBits;
                if ((data[(int)(bitpos / 8)] & (1 << (int)(bitpos % 8))) == 0) return false;
                h += delta;
            }
            return true;
        }
    }

    /// <summary>LegacyLocalityBloomImpl&lt;true&gt;::GetLine: the cache line of a hash (with the extra rotation).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint GetLine(uint h, uint numLines) => ((h >> 11) | (h << 21)) % numLines;
}
