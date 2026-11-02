using System.Runtime.CompilerServices;
using RocksDb.Managed.Memory;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Memtable;

/// <summary>
/// Concurrent in-memory Bloom filter used by the memtable prefix / whole-key bloom
/// (memtable_prefix_bloom_size_ratio). Port of DynamicBloom (util/dynamic_bloom.{h,cc}): the bit array lives in the
/// memtable's arena (counted in its memory usage), two bit probes per 64-bit word with xor-offset words inside a small
/// block, 64-bit golden-ratio remix of the 32-bit <see cref="Hash.BloomHash"/>. Allocation-free after construction.
/// <para>Threading: <see cref="AddConcurrently"/> and <see cref="MayContain(ReadOnlySpan{byte})"/> may run concurrently
/// (relaxed atomics, like upstream; publication of the added keys is ordered by the DB's last sequence number).
/// <see cref="Add"/> requires a single writer.</para>
/// </summary>
public sealed unsafe class DynamicBloom
{
    private readonly uint _len;              // kLen: number of 64-bit words
    private readonly uint _numDoubleProbes;  // kNumDoubleProbes
    private readonly ulong* _data;

    /// <param name="arena">Allocator of the bit array (the memtable arena; must outlive this filter).</param>
    /// <param name="totalBits">Requested number of bits (rounded up to whole probe blocks).</param>
    /// <param name="numProbes">Even number of probes, at most 10 (memtables use 6).</param>
    public DynamicBloom(Allocator arena, uint totalBits, uint numProbes)
    {
        ArgumentNullException.ThrowIfNull(arena);
        if (numProbes % 2 != 0 || numProbes > 10 || numProbes == 0) throw new ArgumentOutOfRangeException(nameof(numProbes));
        // Round down, except round up with 1
        _numDoubleProbes = (numProbes + (numProbes == 1 ? 1u : 0u)) / 2;
        // Determine how much to round off + align by so that x ^ i (that's xor) is a valid u64 index if x is a valid u64
        // index and 0 <= i < kNumDoubleProbes.
        uint blockBytes = 8 * Math.Max(1u, RoundUpToPow2(_numDoubleProbes));
        uint blockBits = blockBytes * 8;
        uint blocks = (uint)(((ulong)totalBits + blockBits - 1) / blockBits);
        ulong sz = (ulong)blocks * blockBytes;
        if (sz > int.MaxValue / 2) throw new ArgumentOutOfRangeException(nameof(totalBits));
        _len = (uint)(sz / 8);
        // Padding to correct for allocation not originally aligned on block_bytes boundary
        int allocate = (int)sz + (int)blockBytes - 1;
        byte* raw = arena.AllocateAligned(allocate);
        new Span<byte>(raw, allocate).Clear();
        ulong blockOffset = (ulong)raw % blockBytes;
        if (blockOffset > 0) raw += blockBytes - blockOffset;
        _data = (ulong*)raw;
    }

    private static uint RoundUpToPow2(uint x)
    {
        uint rv = 1;
        while (rv < x) rv <<= 1;
        return rv;
    }

    /// <summary>Number of 64-bit words of the bit array.</summary>
    public uint Words => _len;

    /// <summary>Adds a key; single writer only.</summary>
    public void Add(ReadOnlySpan<byte> key) => AddHash(Hash.BloomHash(key), concurrent: false);

    /// <summary>Adds a key; may run concurrently with other adds and lookups.</summary>
    public void AddConcurrently(ReadOnlySpan<byte> key) => AddHash(Hash.BloomHash(key), concurrent: true);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool MayContain(ReadOnlySpan<byte> key) => MayContainHash(Hash.BloomHash(key));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool MayContainHash(uint h32) => DoubleProbe(h32, Hash.FastRange32(h32, _len));

    private bool DoubleProbe(uint h32, uint wordOffset)
    {
        // Expand/remix with 64-bit golden ratio
        ulong h = 0x9e3779b97f4a7c13UL * h32;
        for (uint i = 0; ; ++i)
        {
            // Two bit probes per uint64_t probe
            ulong mask = (1UL << (int)(h & 63)) | (1UL << (int)((h >> 6) & 63));
            ulong val = Volatile.Read(ref _data[wordOffset ^ i]);
            if (i + 1 >= _numDoubleProbes) return (val & mask) == mask;
            if ((val & mask) != mask) return false;
            h = (h >> 12) | (h << 52);
        }
    }

    private void AddHash(uint h32, bool concurrent)
    {
        uint a = Hash.FastRange32(h32, _len);
        // Expand/remix with 64-bit golden ratio
        ulong h = 0x9e3779b97f4a7c13UL * h32;
        for (uint i = 0; ; ++i)
        {
            // Two bit probes per uint64_t probe
            ulong mask = (1UL << (int)(h & 63)) | (1UL << (int)((h >> 6) & 63));
            ref ulong word = ref _data[a ^ i];
            if (!concurrent)
            {
                Volatile.Write(ref word, Volatile.Read(ref word) | mask);
            }
            else if ((mask & Volatile.Read(ref word)) != mask)
            {
                // Avoid races and do not lose bits (FetchOrRelaxed).
                Interlocked.Or(ref word, mask);
            }
            if (i + 1 >= _numDoubleProbes) return;
            h = (h >> 12) | (h << 52);
        }
    }
}
