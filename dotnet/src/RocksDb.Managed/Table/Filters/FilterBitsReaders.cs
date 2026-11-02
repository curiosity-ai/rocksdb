using RocksDb.Managed.Util;

namespace RocksDb.Managed.Table.Filters;

/// <summary>
/// Port of BuiltinFilterBitsReader (table/block_based/filter_policy_internal.h): adds
/// <see cref="HashMayMatch"/> for callers that already have <see cref="Hash.GetSliceHash64"/> of the key.
/// Readers reference the filter contents (no copy) and never allocate on queries.
/// </summary>
public abstract class BuiltinFilterBitsReader : IFilterBitsReader
{
    public abstract bool MayMatch(ReadOnlySpan<byte> key);

    /// <summary>Query by 64-bit key hash (<see cref="Hash.GetSliceHash64"/>). Legacy Bloom always returns false (as upstream).</summary>
    public abstract bool HashMayMatch(ulong hash);

    /// <summary>
    /// Port of BuiltinFilterPolicy::GetBuiltinFilterBitsReader: reads the 5-byte metadata trailer and returns
    /// the matching reader (FastLocalBloom, Ribbon, legacy Bloom, or always-true/always-false for empty,
    /// unknown or reserved encodings).
    /// </summary>
    public static BuiltinFilterBitsReader Create(ReadOnlyMemory<byte> contents)
    {
        ReadOnlySpan<byte> span = contents.Span;
        int lenWithMeta = span.Length;
        if (lenWithMeta <= BuiltinFilterFormat.MetadataLen)
        {
            // filter is empty or broken. Treat like zero keys added.
            return AlwaysFalseFilter.Instance;
        }

        // Legacy Bloom filter data:
        //             0 +-----------------------------------+
        //               | Raw Bloom filter data             |
        //               | ...                               |
        //           len +-----------------------------------+
        //               | byte for num_probes or            |
        //               |   marker for new implementations  |
        //         len+1 +-----------------------------------+
        //               | four bytes for number of cache    |
        //               |   lines                           |
        // len_with_meta +-----------------------------------+
        sbyte rawNumProbes = unchecked((sbyte)span[lenWithMeta - BuiltinFilterFormat.MetadataLen]);
        if (rawNumProbes < 1)
        {
            // Note: < 0 (or unsigned > 127) indicate special new implementations
            // (or reserved for future use)
            switch (rawNumProbes)
            {
                case 0:
                    // Treat as zero probes (always FP)
                    return AlwaysTrueFilter.Instance;
                case BuiltinFilterFormat.NewBloomMarker:
                    return CreateBloomReader(contents);
                case BuiltinFilterFormat.RibbonMarker:
                    return CreateRibbonReader(contents);
                default:
                    // Reserved (treat as zero probes, always FP, for now)
                    return AlwaysTrueFilter.Instance;
            }
        }
        // else attempt decode for LegacyBloomBitsReader
        int numProbes = rawNumProbes;
        uint len = (uint)(lenWithMeta - BuiltinFilterFormat.MetadataLen);
        uint numLines = Coding.DecodeFixed32(span[(lenWithMeta - 4)..]);
        int log2CacheLineSize;
        if ((ulong)numLines * BuiltinFilterFormat.LegacyCacheLineSize == len)
        {
            // Common case
            log2CacheLineSize = BuiltinFilterFormat.LegacyLog2CacheLineSize;
        }
        else if (numLines == 0 || len % numLines != 0)
        {
            // Invalid (no solution to num_lines * x == len)
            // Treat as zero probes (always FP) for now.
            return AlwaysTrueFilter.Instance;
        }
        else
        {
            // Determine the non-native cache line size (from another system)
            log2CacheLineSize = 0;
            while (((ulong)numLines << log2CacheLineSize) < len)
            {
                ++log2CacheLineSize;
            }
            if (((ulong)numLines << log2CacheLineSize) != len)
            {
                // Invalid (block size not a power of two)
                // Treat as zero probes (always FP) for now.
                return AlwaysTrueFilter.Instance;
            }
        }
        return new LegacyBloomBitsReader(contents[..(int)len], numProbes, numLines, log2CacheLineSize);
    }

    private static BuiltinFilterBitsReader CreateRibbonReader(ReadOnlyMemory<byte> contents)
    {
        ReadOnlySpan<byte> span = contents.Span;
        int len = span.Length - BuiltinFilterFormat.MetadataLen;
        uint seed = span[len + 1];
        uint numBlocks = span[len + 2];
        numBlocks |= (uint)span[len + 3] << 8;
        numBlocks |= (uint)span[len + 4] << 16;
        if (numBlocks < 2)
        {
            // Not supported
            // num_blocks == 1 is not used because num_starts == 1 is problematic
            // for the hashing scheme. num_blocks == 0 is unused because there's
            // already a concise encoding of an "always false" filter.
            // Return something safe:
            return AlwaysTrueFilter.Instance;
        }
        return new Standard128RibbonBitsReader(contents[..len], numBlocks, seed);
    }

    private static BuiltinFilterBitsReader CreateBloomReader(ReadOnlyMemory<byte> contents)
    {
        ReadOnlySpan<byte> span = contents.Span;
        int lenWithMeta = span.Length;
        int len = lenWithMeta - BuiltinFilterFormat.MetadataLen;

        // New Bloom filter data:
        //             0 +-----------------------------------+
        //               | Raw Bloom filter data             |
        //               | ...                               |
        //           len +-----------------------------------+
        //               | char{-1} byte -> new Bloom filter |
        //         len+1 +-----------------------------------+
        //               | byte for subimplementation        |
        //               |   0: FastLocalBloom               |
        //               |   other: reserved                 |
        //         len+2 +-----------------------------------+
        //               | byte for block_and_probes         |
        //               |   0 in top 3 bits -> 6 -> 64-byte |
        //               |   reserved:                       |
        //               |   1 in top 3 bits -> 7 -> 128-byte|
        //               |   2 in top 3 bits -> 8 -> 256-byte|
        //               |   ...                             |
        //               |   num_probes in bottom 5 bits,    |
        //               |     except 0 and 31 reserved      |
        //         len+3 +-----------------------------------+
        //               | two bytes reserved                |
        //               |   possibly for hash seed          |
        // len_with_meta +-----------------------------------+
        byte subImplVal = span[lenWithMeta - 4];
        sbyte blockAndProbes = unchecked((sbyte)span[lenWithMeta - 3]);
        int log2BlockBytes = ((blockAndProbes >> 5) & 7) + 6;

        int numProbes = blockAndProbes & 31;
        if (numProbes < 1 || numProbes > 30)
        {
            // Reserved / future safe
            return AlwaysTrueFilter.Instance;
        }

        ushort rest = Coding.DecodeFixed16(span[(lenWithMeta - 2)..]);
        if (rest != 0)
        {
            // Reserved, possibly for hash seed
            // Future safe
            return AlwaysTrueFilter.Instance;
        }

        if (subImplVal == 0 && log2BlockBytes == 6) // FastLocalBloom; only block size supported for now
        {
            return new FastLocalBloomBitsReader(contents[..len], numProbes);
        }
        // otherwise
        // Reserved / future safe
        return AlwaysTrueFilter.Instance;
    }
}

/// <summary>Port of FastLocalBloomBitsReader.</summary>
public sealed class FastLocalBloomBitsReader : BuiltinFilterBitsReader
{
    private readonly ReadOnlyMemory<byte> _data;
    private readonly int _numProbes;
    private readonly uint _lenBytes;

    public FastLocalBloomBitsReader(ReadOnlyMemory<byte> data, int numProbes)
    {
        _data = data;
        _numProbes = numProbes;
        _lenBytes = (uint)data.Length;
    }

    public int NumProbes => _numProbes;

    public override bool MayMatch(ReadOnlySpan<byte> key) => HashMayMatch(Hash.GetSliceHash64(key));

    public override bool HashMayMatch(ulong h)
        => FastLocalBloomImpl.HashMayMatch(Hash.Lower32of64(h), Hash.Upper32of64(h), _lenBytes, _numProbes, _data.Span);
}

/// <summary>Port of Standard128RibbonBitsReader.</summary>
public sealed class Standard128RibbonBitsReader : BuiltinFilterBitsReader
{
    private readonly ReadOnlyMemory<byte> _data;
    private readonly Standard128Ribbon.Layout _layout;
    private readonly uint _rawSeed;

    public Standard128RibbonBitsReader(ReadOnlyMemory<byte> data, uint numBlocks, uint ordinalSeed)
    {
        _data = data;
        _layout = new Standard128Ribbon.Layout(numBlocks, (ulong)data.Length);
        _rawSeed = Standard128Ribbon.OrdinalToRawSeed(ordinalSeed);
        if ((ulong)_layout.NumSegments * Standard128Ribbon.BytesPerSegment > (ulong)data.Length)
        {
            ThrowHelper.Corruption("Ribbon filter too short for its metadata");
        }
    }

    public override bool MayMatch(ReadOnlySpan<byte> key) => HashMayMatch(Hash.GetSliceHash64(key));

    public override bool HashMayMatch(ulong h) => Standard128Ribbon.FilterQuery(h, _rawSeed, _data.Span, _layout);
}

/// <summary>Port of LegacyBloomBitsReader (32-bit BloomHash; <see cref="HashMayMatch"/> unsupported, returns false).</summary>
public sealed class LegacyBloomBitsReader : BuiltinFilterBitsReader
{
    private readonly ReadOnlyMemory<byte> _data;
    private readonly int _numProbes;
    private readonly uint _numLines;
    private readonly int _log2CacheLineSize;

    public LegacyBloomBitsReader(ReadOnlyMemory<byte> data, int numProbes, uint numLines, int log2CacheLineSize)
    {
        _data = data;
        _numProbes = numProbes;
        _numLines = numLines;
        _log2CacheLineSize = log2CacheLineSize;
    }

    public override bool MayMatch(ReadOnlySpan<byte> key)
        => LegacyLocalityBloomImpl.HashMayMatch(Hash.BloomHash(key), _numLines, _numProbes, _data.Span, _log2CacheLineSize);

    public override bool HashMayMatch(ulong hash) => false;
}

/// <summary>Port of AlwaysTrueFilter.</summary>
public sealed class AlwaysTrueFilter : BuiltinFilterBitsReader
{
    public static readonly AlwaysTrueFilter Instance = new();
    private AlwaysTrueFilter() { }
    public override bool MayMatch(ReadOnlySpan<byte> key) => true;
    public override bool HashMayMatch(ulong hash) => true;
}

/// <summary>Port of AlwaysFalseFilter (empty filter: zero keys added).</summary>
public sealed class AlwaysFalseFilter : BuiltinFilterBitsReader
{
    public static readonly AlwaysFalseFilter Instance = new();
    private AlwaysFalseFilter() { }
    public override bool MayMatch(ReadOnlySpan<byte> key) => false;
    public override bool HashMayMatch(ulong hash) => false;
}
