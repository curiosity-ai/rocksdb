using System.Numerics;

namespace RocksDb.Managed.Table;

/// <summary>
/// Port of BlockReadAmpBitmap (table/block_based/block.h): for BlockBasedTableOptions::read_amp_bytes_per_bit, a
/// bitmap over the entries section of one data block with one bit per bytes_per_bit bytes (rounded down to a power of
/// two), starting at a random offset in [0, bytes_per_bit). Creating it records the block size in
/// READ_AMP_TOTAL_READ_BYTES; the first read of an entry's value marks the bits it covers, adding the newly marked
/// bytes to READ_AMP_ESTIMATE_USEFUL_BYTES. Thread-safe (atomic bit updates, like upstream).
/// </summary>
internal sealed class BlockReadAmpBitmap
{
    private const int BitsPerEntry = 32;
    private readonly int[] _bitmap;
    private readonly int _bytesPerBitPow;
    private readonly uint _rnd;
    private readonly Statistics _statistics;

    public BlockReadAmpBitmap(int blockSize, uint bytesPerBit, Statistics statistics)
    {
        System.Diagnostics.Debug.Assert(blockSize > 0 && bytesPerBit > 0);
        _statistics = statistics;
        _rnd = (uint)Random.Shared.Next((int)Math.Min(bytesPerBit, int.MaxValue));
        // convert bytes_per_bit to be a power of 2
        _bytesPerBitPow = BitOperations.Log2(bytesPerBit);
        // num_bits_needed = ceil(block_size / bytes_per_bit)
        long numBitsNeeded = ((long)(blockSize - 1) >> _bytesPerBitPow) + 1;
        // bitmap_size = ceil(num_bits_needed / kBitsPerEntry)
        _bitmap = new int[(numBitsNeeded - 1) / BitsPerEntry + 1];
        statistics.RecordTick(Ticker.ReadAmpTotalReadBytes, (ulong)blockSize);
    }

    /// <summary>The bytes per bit actually used (a power of two).</summary>
    public uint BytesPerBit => 1u << _bytesPerBitPow;

    /// <summary>BlockReadAmpBitmap::Mark: the entry spanning [<paramref name="startOffset"/>, <paramref name="endOffset"/>] was read.</summary>
    public void Mark(uint startOffset, uint endOffset)
    {
        uint unit = 1u << _bytesPerBitPow;
        // Index of first bit in mask
        uint startBit = (startOffset + unit - _rnd - 1) >> _bytesPerBitPow;
        // Index of last bit in mask + 1
        uint exclusiveEndBit = (endOffset + unit - _rnd) >> _bytesPerBitPow;
        if (startBit >= exclusiveEndBit) return;
        if (!GetAndSet(startBit))
        {
            ulong newUsefulBytes = (ulong)(exclusiveEndBit - startBit) << _bytesPerBitPow;
            _statistics.RecordTick(Ticker.ReadAmpEstimateUsefulBytes, newUsefulBytes);
        }
    }

    // Get the current value of bit at `bit_idx` and set it to 1
    private bool GetAndSet(uint bitIdx)
    {
        int mask = 1 << (int)(bitIdx % BitsPerEntry);
        return (Interlocked.Or(ref _bitmap[bitIdx / BitsPerEntry], mask) & mask) != 0;
    }
}
