using System.Buffers;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Table.Filters;

/// <summary>
/// Shared constants of the built-in filter formats (table/block_based/filter_policy.cc).
/// </summary>
public static class BuiltinFilterFormat
{
    /// <summary>Metadata trailer size for built-in filters (separate from the block trailer).</summary>
    public const int MetadataLen = 5;

    /// <summary>First metadata byte marking a newer Bloom implementation (FastLocalBloom).</summary>
    public const sbyte NewBloomMarker = -1;

    /// <summary>First metadata byte marking Standard128 Ribbon.</summary>
    public const sbyte RibbonMarker = -2;

    /// <summary>CACHE_LINE_SIZE assumed by the legacy Bloom filter writer (port/port_posix.h).</summary>
    public const int LegacyCacheLineSize = 64;

    /// <summary>log2(CACHE_LINE_SIZE).</summary>
    public const int LegacyLog2CacheLineSize = 6;

    /// <summary>kMaxRibbonEntries: more entries fall back to Bloom.</summary>
    public const uint MaxRibbonEntries = 950000000;

    /// <summary>"Always true" filter contents written when construction must fail open (FinishAlwaysTrue).</summary>
    public static ReadOnlySpan<byte> AlwaysTrueContents => [0, 0, 0, 0, 0, 0];
}

/// <summary>
/// Base for builders hashing keys with <see cref="Hash.GetSliceHash64"/> (port of XXPH3FilterBitsBuilder in
/// table/block_based/filter_policy.cc). Hash entries are kept in a pooled growable buffer.
/// Not thread-safe (like upstream, except <see cref="EstimatedEntries"/> which is a plain read).
/// </summary>
public abstract class XxPh3FilterBitsBuilder : IFilterBitsBuilder
{
    private ulong[] _entries = [];
    private int _count;
    private ulong? _prevAltHash;

    public int EstimatedEntries => Volatile.Read(ref _count);

    /// <summary>Hash entries added so far, in insertion order.</summary>
    protected ReadOnlySpan<ulong> Entries => _entries.AsSpan(0, _count);

    public void AddKey(ReadOnlySpan<byte> key)
    {
        ulong hash = Hash.GetSliceHash64(key);
        // Especially with prefixes, it is common to have repetition,
        // though only adjacent repetition, which we want to immediately
        // recognize and collapse for estimating true filter space
        // requirements.
        if (_count == 0 || hash != _entries[_count - 1])
        {
            AddHash(hash);
        }
    }

    /// <summary>
    /// AddKeyAndAlt: adds a key and an alternate (e.g. its prefix), de-duplicating against the previous
    /// key and alt the same way upstream does.
    /// </summary>
    public void AddKeyAndAlt(ReadOnlySpan<byte> key, ReadOnlySpan<byte> alt)
    {
        ulong keyHash = Hash.GetSliceHash64(key);
        ulong altHash = Hash.GetSliceHash64(alt);
        ulong? prevKeyHash = _count > 0 ? _entries[_count - 1] : null;
        ulong? prevAltHash = _prevAltHash;
        // Add alt first, so that entries.back() always contains previous key
        // ASSUMING a change from one alt to the next implies a change to
        // corresponding key
        if (altHash != prevAltHash && altHash != keyHash && altHash != prevKeyHash)
        {
            AddHash(altHash);
        }
        // Overwrite prev_alt_hash for cases like alt_hash == key_hash
        _prevAltHash = altHash;
        // NOTE: checking key_hash != prev_alt_hash for cases like
        // key == prefix(key) at the end of a prefix grouping as in reverse
        // byte-wise comparator
        if (keyHash != prevKeyHash && keyHash != prevAltHash)
        {
            AddHash(keyHash);
        }
    }

    /// <summary>Adds a precomputed 64-bit key hash (no de-duplication).</summary>
    public void AddHash(ulong hash)
    {
        if (_count == _entries.Length) Grow();
        _entries[_count] = hash;
        Volatile.Write(ref _count, _count + 1);
    }

    private void Grow()
    {
        ulong[] bigger = ArrayPool<ulong>.Shared.Rent(Math.Max(256, _entries.Length * 2));
        _entries.AsSpan(0, _count).CopyTo(bigger);
        ReturnEntries();
        _entries = bigger;
    }

    private void ReturnEntries()
    {
        if (_entries.Length > 0) ArrayPool<ulong>.Shared.Return(_entries);
        _entries = [];
    }

    /// <summary>Clears entries (keeps the buffer for reuse).</summary>
    protected void ResetEntries()
    {
        _count = 0;
        _prevAltHash = null;
    }

    /// <summary>Moves this builder's entries to <paramref name="other"/> (SwapEntriesWith for Bloom fallback).</summary>
    protected void SwapEntriesWith(XxPh3FilterBitsBuilder other)
    {
        (_entries, other._entries) = (other._entries, _entries);
        int c = _count;
        _count = other._count;
        other._count = c;
        (_prevAltHash, other._prevAltHash) = (other._prevAltHash, _prevAltHash);
    }

    public abstract void Finish(IBufferWriter<byte> output);

    /// <summary>Filter size in bytes (including metadata) for <paramref name="numEntries"/> keys.</summary>
    public abstract ulong CalculateSpace(ulong numEntries);

    /// <summary>Estimated FP rate for a filter of the given size holding <paramref name="keys"/> keys.</summary>
    public abstract double EstimatedFpRate(ulong keys, ulong lenWithMetadata);

    /// <summary>Approximate number of keys that fit a filter of <paramref name="bytes"/> bytes (for partitioning).</summary>
    public abstract ulong ApproximateNumEntries(ulong bytes);

    protected abstract ulong RoundDownUsableSpace(ulong availableSize);

    public virtual void Dispose()
    {
        ReturnEntries();
        _count = 0;
        GC.SuppressFinalize(this);
    }
}

/// <summary>
/// Port of FastLocalBloomBitsBuilder (format_version=5 Bloom). Metadata: [-1][0][num_probes][0][0].
/// optimize_filters_for_memory (malloc_usable_size rounding) is not implemented: sizes always follow
/// <see cref="CalculateSpace"/>, matching native output with optimize_filters_for_memory=false.
/// </summary>
public sealed class FastLocalBloomBitsBuilder : XxPh3FilterBitsBuilder
{
    private readonly int _millibitsPerKey;

    public FastLocalBloomBitsBuilder(int millibitsPerKey)
    {
        if (millibitsPerKey < 1000) throw new ArgumentOutOfRangeException(nameof(millibitsPerKey));
        _millibitsPerKey = millibitsPerKey;
    }

    public int MillibitsPerKey => _millibitsPerKey;

    public override void Finish(IBufferWriter<byte> output)
    {
        int numEntries = Entries.Length;
        if (numEntries == 0)
        {
            // FinishAlwaysFalse: missing metadata, treated as zero entries
            ResetEntries();
            return;
        }
        ulong lenWithMetadata = CalculateSpace((ulong)numEntries);
        if (lenWithMetadata > int.MaxValue) ThrowHelper.NotSupported("filter too large");
        int numProbes = FastLocalBloomImpl.ChooseNumProbes(_millibitsPerKey);
        int total = (int)lenWithMetadata;
        int len = total - BuiltinFilterFormat.MetadataLen;
        Span<byte> buf = output.GetSpan(total)[..total];
        buf.Clear();
        if (len > 0)
        {
            AddAllEntries(buf[..len], (uint)len, numProbes);
        }
        ResetEntries();
        // -1 = Marker for newer Bloom implementations
        buf[len] = unchecked((byte)BuiltinFilterFormat.NewBloomMarker);
        // 0 = Marker for this sub-implementation
        buf[len + 1] = 0;
        // num_probes (and 0 in upper bits for 64-byte block size)
        buf[len + 2] = (byte)numProbes;
        // rest of metadata stays zero
        output.Advance(total);
    }

    private void AddAllEntries(Span<byte> data, uint len, int numProbes)
    {
        foreach (ulong h in Entries)
        {
            FastLocalBloomImpl.AddHash(Hash.Lower32of64(h), Hash.Upper32of64(h), len, numProbes, data);
        }
    }

    public override ulong CalculateSpace(ulong numEntries)
    {
        // If not for cache line blocks in the filter, what would the target
        // length in bytes be?
        ulong rawTargetLen = ((numEntries * (ulong)_millibitsPerKey) + 7999) / 8000;
        if (rawTargetLen >= 0xffffffc0UL)
        {
            // Max supported for this data structure implementation
            rawTargetLen = 0xffffffc0UL;
        }
        // Round up to nearest multiple of 64 (block size). This adjustment is
        // used for target FP rate only so that we don't receive complaints about
        // lower FP rate vs. historic Bloom filter behavior.
        return ((rawTargetLen + 63) & ~63UL) + BuiltinFilterFormat.MetadataLen;
    }

    public override double EstimatedFpRate(ulong keys, ulong lenWithMetadata)
    {
        if (lenWithMetadata <= BuiltinFilterFormat.MetadataLen)
        {
            return keys > 0 ? 1.0 : 0.0;
        }
        int numProbes = FastLocalBloomImpl.ChooseNumProbes(_millibitsPerKey);
        return FastLocalBloomImpl.EstimatedFpRate(keys, lenWithMetadata - BuiltinFilterFormat.MetadataLen, numProbes, /*hash bits*/ 64);
    }

    public override ulong ApproximateNumEntries(ulong bytes)
    {
        ulong bytesNoMeta = bytes >= BuiltinFilterFormat.MetadataLen ? RoundDownUsableSpace(bytes) - BuiltinFilterFormat.MetadataLen : 0;
        return 8000UL * bytesNoMeta / (ulong)_millibitsPerKey;
    }

    protected override ulong RoundDownUsableSpace(ulong availableSize)
    {
        ulong rv = availableSize - BuiltinFilterFormat.MetadataLen;
        if (rv >= 0xffffffc0UL)
        {
            rv = 0xffffffc0UL;
        }
        // round down to multiple of 64 (block size)
        rv &= ~63UL;
        return rv + BuiltinFilterFormat.MetadataLen;
    }
}

/// <summary>
/// Port of Standard128RibbonBitsBuilder. Metadata: [-2][seed][num_blocks as 24-bit LE]. Falls back to
/// FastLocalBloom for tiny filters (when Bloom is smaller), too many entries, or failure to find a seed
/// in 256 attempts, exactly like upstream.
/// </summary>
public sealed class Standard128RibbonBitsBuilder : XxPh3FilterBitsBuilder
{
    private readonly double _desiredOneInFpRate;
    private readonly FastLocalBloomBitsBuilder _bloomFallback;

    public Standard128RibbonBitsBuilder(double desiredOneInFpRate, int bloomMillibitsPerKey)
    {
        if (!(desiredOneInFpRate >= 1.0)) throw new ArgumentOutOfRangeException(nameof(desiredOneInFpRate));
        _desiredOneInFpRate = desiredOneInFpRate;
        _bloomFallback = new FastLocalBloomBitsBuilder(bloomMillibitsPerKey);
    }

    public override void Finish(IBufferWriter<byte> output)
    {
        int count = Entries.Length;
        if ((uint)count > BuiltinFilterFormat.MaxRibbonEntries)
        {
            FinishWithBloom(output);
            return;
        }
        if (count == 0)
        {
            // Save a conditional in Ribbon queries by using alternate reader
            // for zero entries added.
            ResetEntries();
            return;
        }
        uint numEntries = (uint)count;
        CalculateSpaceAndSlots(numEntries, out ulong lenWithMetadata, out uint numSlots);

        // Bloom fall-back indicator
        if (numSlots == 0)
        {
            FinishWithBloom(output);
            return;
        }

        uint entropy = Hash.Lower32of64(Entries[0]);
        using var banding = new Standard128Ribbon.Banding();
        bool success = banding.ResetAndFindSeedToSolve(numSlots, Entries, /*starting seed*/ entropy & 255, /*seed mask*/ 255);
        if (!success)
        {
            // Too many re-seeds (256) for Ribbon filter
            FinishWithBloom(output);
            return;
        }
        ResetEntries();

        uint seed = banding.OrdinalSeed;
        if (lenWithMetadata > int.MaxValue) ThrowHelper.NotSupported("filter too large");
        int total = (int)lenWithMetadata;
        Span<byte> buf = output.GetSpan(total)[..total];
        buf.Clear();
        // SerializableInterleavedSolution(data, len_with_metadata): num segments = floor(len / 16)
        var layout = new Standard128Ribbon.Layout(numSlots / Standard128Ribbon.CoeffBits, (ulong)total);
        banding.BackSubstInto(buf, layout);
        uint numBlocks = layout.NumBlocks;

        // -2 = Marker for Standard128 Ribbon
        buf[total - 5] = unchecked((byte)BuiltinFilterFormat.RibbonMarker);
        // Hash seed
        buf[total - 4] = (byte)seed;
        // Number of blocks, in 24 bits
        buf[total - 3] = (byte)(numBlocks & 255);
        buf[total - 2] = (byte)((numBlocks >> 8) & 255);
        buf[total - 1] = (byte)((numBlocks >> 16) & 255);
        output.Advance(total);
    }

    private void FinishWithBloom(IBufferWriter<byte> output)
    {
        SwapEntriesWith(_bloomFallback);
        _bloomFallback.Finish(output);
    }

    private static uint NumEntriesToNumSlots(uint numEntries)
        => Standard128Ribbon.RoundUpNumSlots(Standard128Ribbon.GetNumSlots(numEntries));

    /// <summary>Setting numSlots to 0 means "fall back on Bloom filter."</summary>
    private void CalculateSpaceAndSlots(ulong numEntries, out ulong targetLenWithMetadata, out uint numSlots)
    {
        if (numEntries > BuiltinFilterFormat.MaxRibbonEntries)
        {
            // More entries than supported by this Ribbon
            numSlots = 0;
            targetLenWithMetadata = _bloomFallback.CalculateSpace(numEntries);
            return;
        }
        uint entropy = Entries.Length > 0 ? Hash.Upper32of64(Entries[0]) : 0;
        numSlots = NumEntriesToNumSlots((uint)numEntries);
        targetLenWithMetadata = Standard128Ribbon.GetBytesForOneInFpRate(numSlots, _desiredOneInFpRate, /*rounding*/ entropy) + BuiltinFilterFormat.MetadataLen;

        // Consider possible Bloom fallback for small filters
        if (numSlots < 1024)
        {
            ulong bloom = _bloomFallback.CalculateSpace(numEntries);
            if (bloom < targetLenWithMetadata)
            {
                numSlots = 0;
                targetLenWithMetadata = bloom;
            }
        }
    }

    public override ulong CalculateSpace(ulong numEntries)
    {
        if (numEntries == 0)
        {
            // See FinishAlwaysFalse
            return 0;
        }
        CalculateSpaceAndSlots(numEntries, out ulong target, out _);
        return target;
    }

    public override double EstimatedFpRate(ulong keys, ulong lenWithMetadata)
    {
        if (lenWithMetadata <= BuiltinFilterFormat.MetadataLen)
        {
            return keys > 0 ? 1.0 : 0.0;
        }
        if (keys > BuiltinFilterFormat.MaxRibbonEntries)
        {
            return _bloomFallback.EstimatedFpRate(keys, lenWithMetadata);
        }
        uint numSlots = NumEntriesToNumSlots((uint)keys);
        var layout = new Standard128Ribbon.Layout(numSlots / Standard128Ribbon.CoeffBits, lenWithMetadata);
        return layout.ExpectedFpRate();
    }

    public override ulong ApproximateNumEntries(ulong bytes)
    {
        ulong lenNoMetadata = RoundDownUsableSpace(Math.Max(bytes, (ulong)BuiltinFilterFormat.MetadataLen)) - BuiltinFilterFormat.MetadataLen;

        if (!(_desiredOneInFpRate > 1.0))
        {
            // Effectively asking for 100% FP rate, or NaN etc.
            return BuiltinFilterFormat.MaxRibbonEntries;
        }

        // Find a slight under-estimate for actual average bits per slot
        double minRealBitsPerSlot;
        if (_desiredOneInFpRate >= 1.0 + uint.MaxValue)
        {
            // Max of 32 solution columns (result bits)
            minRealBitsPerSlot = 32.0;
        }
        else
        {
            // Account for mix of b and b+1 solution columns being slightly
            // suboptimal vs. ideal log2(1/fp_rate) bits.
            uint rounded = (uint)_desiredOneInFpRate;
            int upperBitsPerKey = 1 + System.Numerics.BitOperations.Log2(rounded);
            double fpRateForUpper = Math.Pow(2.0, -upperBitsPerKey);
            double portionLower = ((1.0 / _desiredOneInFpRate) - fpRateForUpper) / fpRateForUpper;
            minRealBitsPerSlot = upperBitsPerKey - portionLower;
        }

        // An overestimate, but this should only be O(1) slots away from truth.
        double maxSlots = lenNoMetadata * 8.0 / minRealBitsPerSlot;

        // Let's not bother accounting for overflow to Bloom filter
        // (Includes NaN case)
        if (!(maxSlots < Standard128Ribbon.GetNumSlots(BuiltinFilterFormat.MaxRibbonEntries)))
        {
            return BuiltinFilterFormat.MaxRibbonEntries;
        }

        // Set up for short iteration
        uint slots = (uint)maxSlots;
        slots = Standard128Ribbon.RoundUpNumSlots(slots);

        // Iterate up to a few times to rather precisely account for small effects
        for (int i = 0; slots > 0; ++i)
        {
            ulong reqdBytes = Standard128Ribbon.GetBytesForOneInFpRate(slots, _desiredOneInFpRate, /*rounding*/ 0);
            if (reqdBytes <= lenNoMetadata)
            {
                break; // done
            }
            if (i >= 2)
            {
                // should have been enough iterations
                break;
            }
            slots = Standard128Ribbon.RoundDownNumSlots(slots - 1);
        }

        uint numEntries = Standard128Ribbon.GetNumToAdd(slots);

        // Consider possible Bloom fallback for small filters
        if (slots < 1024)
        {
            ulong bloom = _bloomFallback.ApproximateNumEntries(bytes);
            return bloom > numEntries ? bloom : numEntries;
        }
        return Math.Min(numEntries, BuiltinFilterFormat.MaxRibbonEntries);
    }

    protected override ulong RoundDownUsableSpace(ulong availableSize)
    {
        ulong rv = availableSize - BuiltinFilterFormat.MetadataLen;
        // round down to multiple of 16 (segment size)
        rv &= ~15UL;
        return rv + BuiltinFilterFormat.MetadataLen;
    }

    public override void Dispose()
    {
        _bloomFallback.Dispose();
        base.Dispose();
    }
}

/// <summary>
/// Port of LegacyBloomBitsBuilder (format_version &lt; 5 full filter, 32-bit <see cref="Hash.BloomHash"/>).
/// Metadata: [num_probes][num_lines as fixed32].
/// </summary>
public sealed class LegacyBloomBitsBuilder : IFilterBitsBuilder
{
    private readonly int _bitsPerKey;
    private readonly int _numProbes;
    private uint[] _entries = [];
    private int _count;
    private long _prevAltHash = -1; // A single-element cache to help AddKeyAndAlt. (-1 == empty)

    public LegacyBloomBitsBuilder(int bitsPerKey)
    {
        if (bitsPerKey <= 0) throw new ArgumentOutOfRangeException(nameof(bitsPerKey));
        _bitsPerKey = bitsPerKey;
        _numProbes = LegacyNoLocalityBloomImpl.ChooseNumProbes(bitsPerKey);
    }

    public int EstimatedEntries => Volatile.Read(ref _count);

    public void AddKey(ReadOnlySpan<byte> key)
    {
        uint hash = Hash.BloomHash(key);
        if (_count == 0 || _entries[_count - 1] != hash)
        {
            Push(hash);
        }
    }

    public void AddKeyAndAlt(ReadOnlySpan<byte> key, ReadOnlySpan<byte> alt)
    {
        // Modified from XXPH3FilterBitsBuilder::AddKeyAndAlt
        uint keyHash = Hash.BloomHash(key);
        uint altHash = Hash.BloomHash(alt);
        long prevKeyHash = _count > 0 ? _entries[_count - 1] : -1;
        long prevAltHash = _prevAltHash;
        if (altHash != prevAltHash && altHash != keyHash && altHash != prevKeyHash)
        {
            Push(altHash);
        }
        _prevAltHash = altHash;
        if (keyHash != prevKeyHash && keyHash != prevAltHash)
        {
            Push(keyHash);
        }
    }

    private void Push(uint h)
    {
        if (_count == _entries.Length)
        {
            uint[] bigger = ArrayPool<uint>.Shared.Rent(Math.Max(256, _entries.Length * 2));
            _entries.AsSpan(0, _count).CopyTo(bigger);
            if (_entries.Length > 0) ArrayPool<uint>.Shared.Return(_entries);
            _entries = bigger;
        }
        _entries[_count] = h;
        Volatile.Write(ref _count, _count + 1);
    }

    public void Finish(IBufferWriter<byte> output)
    {
        int numEntries = _count;
        if (numEntries == 0)
        {
            // FinishAlwaysFalse
            _prevAltHash = -1;
            return;
        }
        int sz = (int)CalculateSpace((ulong)numEntries, out uint totalBits, out uint numLines);
        Span<byte> data = output.GetSpan(sz)[..sz];
        data.Clear();
        if (totalBits != 0 && numLines != 0)
        {
            Span<byte> bits = data[..(int)(totalBits / 8)];
            foreach (uint h in _entries.AsSpan(0, numEntries))
            {
                LegacyLocalityBloomImpl.AddHash(h, numLines, _numProbes, bits, BuiltinFilterFormat.LegacyLog2CacheLineSize);
            }
        }
        // See BloomFilterPolicy::GetFilterBitsReader for metadata
        data[(int)(totalBits / 8)] = (byte)_numProbes;
        Coding.EncodeFixed32(data[((int)(totalBits / 8) + 1)..], numLines);
        output.Advance(sz);
        _count = 0;
        _prevAltHash = -1;
    }

    public ulong CalculateSpace(ulong numEntries) => CalculateSpace(numEntries, out _, out _);

    private static uint GetTotalBitsForLocality(uint totalBits)
    {
        const uint lineBits = BuiltinFilterFormat.LegacyCacheLineSize * 8;
        uint numLines = (totalBits + lineBits - 1) / lineBits;
        // Make num_lines an odd number to make sure more bits are involved
        // when determining which block.
        if (numLines % 2 == 0)
        {
            numLines++;
        }
        return numLines * lineBits;
    }

    private uint CalculateSpace(ulong numEntries, out uint totalBits, out uint numLines)
    {
        if (numEntries != 0)
        {
            ulong totalBitsTmp = numEntries * (ulong)_bitsPerKey;
            // total bits, including temporary computations, cannot exceed 2^32
            // for compatibility
            totalBitsTmp = Math.Min(totalBitsTmp, 0xffff0000UL);
            totalBits = GetTotalBitsForLocality((uint)totalBitsTmp);
            numLines = totalBits / (BuiltinFilterFormat.LegacyCacheLineSize * 8);
        }
        else
        {
            // filter is empty, just leave space for metadata
            totalBits = 0;
            numLines = 0;
        }
        return (totalBits / 8) + BuiltinFilterFormat.MetadataLen;
    }

    public double EstimatedFpRate(ulong keys, ulong lenWithMetadata)
    {
        if (lenWithMetadata <= BuiltinFilterFormat.MetadataLen)
        {
            return keys > 0 ? 1.0 : 0.0;
        }
        return LegacyLocalityBloomImpl.EstimatedFpRate(keys, lenWithMetadata - BuiltinFilterFormat.MetadataLen, _numProbes);
    }

    public ulong ApproximateNumEntries(ulong bytes)
    {
        ulong totalBitsTmp = bytes * 8;
        // total bits, including temporary computations, cannot exceed 2^32
        // for compatibility
        totalBitsTmp = Math.Min(totalBitsTmp, 0xffff0000UL);
        uint high = ((uint)totalBitsTmp / (uint)_bitsPerKey) + 1;
        uint low = 1;
        uint n = high;
        for (; n >= low; n--)
        {
            if (CalculateSpace(n) <= bytes)
            {
                break;
            }
        }
        return n;
    }

    public void Dispose()
    {
        if (_entries.Length > 0) ArrayPool<uint>.Shared.Return(_entries);
        _entries = [];
        _count = 0;
    }
}
