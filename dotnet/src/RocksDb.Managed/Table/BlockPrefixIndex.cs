using RocksDb.Managed.Util;

namespace RocksDb.Managed.Table;

/// <summary>
/// In-memory prefix → data block index of a kHashSearch table. Port of BlockPrefixIndex
/// (table/block_based/block_prefix_index.{h,cc}), built from the "rocksdb.hashindex.prefixes" and
/// "rocksdb.hashindex.metadata" meta blocks written by <see cref="HashIndexBuilder"/>.
/// <para>
/// Buckets (prefix hash modulo number of prefixes + 1) hold either nothing (<c>kNoneBlock</c>), a single data block
/// index, or (top bit set) an offset into the block array, where the first entry is the number of blocks followed by
/// the sorted block indexes of every prefix in that bucket. Immutable after construction; thread-safe.
/// </para>
/// </summary>
public sealed class BlockPrefixIndex
{
    private const uint NoneBlock = 0x7FFFFFFF;
    private const uint BlockArrayMask = 0x80000000;

    private readonly uint _numBuckets;
    private readonly uint[] _buckets;
    private readonly uint[] _blockArrayBuffer;

    private BlockPrefixIndex(uint numBuckets, uint[] buckets, uint[] blockArrayBuffer)
    {
        _numBuckets = numBuckets;
        _buckets = buckets;
        _blockArrayBuffer = blockArrayBuffer;
    }

    /// <summary>Number of hash buckets (number of prefixes + 1).</summary>
    public int NumBuckets => (int)_numBuckets;

    /// <summary>
    /// Port of BlockPrefixIndex::ApproximateMemoryUsage (table/block_based/block_prefix_index.h): sizeof(BlockPrefixIndex)
    /// (64 on x86-64 / libstdc++, main 11.14) plus the bucket and block array entries.
    /// </summary>
    public ulong ApproximateMemoryUsage => 64 + ((ulong)_blockArrayBuffer.Length + _numBuckets) * sizeof(uint);

    private static uint PrefixToBucket(ReadOnlySpan<byte> prefix, uint numBuckets) => Hash.Hash32(prefix, 0) % numBuckets;

    private sealed class PrefixRecord
    {
        public int PrefixOffset;
        public int PrefixLength;
        public uint StartBlock;
        public uint EndBlock;
        public uint NumBlocks;
        public PrefixRecord? Next;
    }

    /// <summary>
    /// Port of BlockPrefixIndex::Create: parses the two meta blocks. Throws Corruption when they are inconsistent (the
    /// table reader then falls back to binary search, like upstream which ignores the error).
    /// </summary>
    public static BlockPrefixIndex Create(ReadOnlySpan<byte> prefixes, ReadOnlySpan<byte> prefixMeta)
    {
        var records = new List<PrefixRecord>();
        long pos = 0;
        while (!prefixMeta.IsEmpty)
        {
            bool ok = Coding.GetVarint32(ref prefixMeta, out uint prefixSize);
            uint entryIndex = 0, numBlocks = 0;
            ok = ok && Coding.GetVarint32(ref prefixMeta, out entryIndex) && Coding.GetVarint32(ref prefixMeta, out numBlocks);
            if (!ok) ThrowHelper.Corruption("Corrupted prefix meta block: unable to read from it.");
            if (pos + prefixSize > prefixes.Length) ThrowHelper.Corruption("Corrupted prefix meta block: size inconsistency.");
            if (numBlocks == 0 || (ulong)entryIndex + numBlocks - 1 >= BlockArrayMask) ThrowHelper.Corruption("Corrupted prefix meta block: bad block range.");
            records.Add(new PrefixRecord
            {
                PrefixOffset = (int)pos,
                PrefixLength = (int)prefixSize,
                StartBlock = entryIndex,
                EndBlock = entryIndex + numBlocks - 1,
                NumBlocks = numBlocks,
            });
            pos += prefixSize;
        }
        if (pos != prefixes.Length) ThrowHelper.Corruption("Corrupted prefix meta block");
        return Finish(prefixes, records);
    }

    /// <summary>Port of BlockPrefixIndex::Builder::Finish.</summary>
    private static BlockPrefixIndex Finish(ReadOnlySpan<byte> prefixes, List<PrefixRecord> records)
    {
        // For now, use roughly 1:1 prefix to bucket ratio.
        uint numBuckets = (uint)records.Count + 1;
        // Collect prefix records that hash to the same bucket, into a single linklist.
        var prefixesPerBucket = new PrefixRecord?[numBuckets];
        var numBlocksPerBucket = new uint[numBuckets];
        foreach (PrefixRecord current in records)
        {
            uint bucket = PrefixToBucket(prefixes.Slice(current.PrefixOffset, current.PrefixLength), numBuckets);
            // merge the prefix block span if the first block of this prefix is connected to the last block of the
            // previous prefix.
            PrefixRecord? prev = prefixesPerBucket[bucket];
            if (prev is not null)
            {
                if (current.StartBlock < prev.EndBlock) ThrowHelper.Corruption("Corrupted prefix meta block: unordered blocks.");
                uint distance = current.StartBlock - prev.EndBlock;
                if (distance <= 1)
                {
                    prev.EndBlock = current.EndBlock;
                    prev.NumBlocks = prev.EndBlock - prev.StartBlock + 1;
                    numBlocksPerBucket[bucket] += current.NumBlocks + distance - 1;
                    continue;
                }
            }
            current.Next = prev;
            prefixesPerBucket[bucket] = current;
            numBlocksPerBucket[bucket] += current.NumBlocks;
        }

        // Calculate the block array buffer size
        ulong totalBlockArrayEntries = 0;
        for (uint i = 0; i < numBuckets; i++)
        {
            uint n = numBlocksPerBucket[i];
            if (n > 1) totalBlockArrayEntries += n + 1;
        }
        if (totalBlockArrayEntries >= BlockArrayMask) ThrowHelper.Corruption("Corrupted prefix meta block: too many blocks.");

        // Populate the final prefix block index
        var blockArrayBuffer = new uint[totalBlockArrayEntries];
        var buckets = new uint[numBuckets];
        uint offset = 0;
        for (uint i = 0; i < numBuckets; i++)
        {
            uint numBlocks = numBlocksPerBucket[i];
            if (numBlocks == 0)
            {
                buckets[i] = NoneBlock;
            }
            else if (numBlocks == 1)
            {
                buckets[i] = prefixesPerBucket[i]!.StartBlock;
            }
            else
            {
                buckets[i] = offset | BlockArrayMask;
                blockArrayBuffer[offset] = numBlocks;
                long lastBlock = offset + numBlocks;
                // populate block ids from largest to smallest
                for (PrefixRecord? current = prefixesPerBucket[i]; current is not null; current = current.Next)
                {
                    for (uint iter = 0; iter < current.NumBlocks; iter++)
                    {
                        if (lastBlock <= offset) ThrowHelper.Corruption("Corrupted prefix meta block: bucket overflow.");
                        blockArrayBuffer[lastBlock] = current.EndBlock - iter;
                        lastBlock--;
                    }
                }
                offset += numBlocks + 1;
            }
        }
        return new BlockPrefixIndex(numBuckets, buckets, blockArrayBuffer);
    }

    /// <summary>
    /// Port of BlockPrefixIndex::GetBlocks: the (ascending) indexes of the data blocks that may contain keys with
    /// <paramref name="prefix"/> (already transformed by the table's prefix extractor); empty when none.
    /// </summary>
    public ReadOnlySpan<uint> GetBlocks(ReadOnlySpan<byte> prefix)
    {
        uint bucket = PrefixToBucket(prefix, _numBuckets);
        uint blockId = _buckets[bucket];
        if (blockId == NoneBlock) return default;
        if ((blockId & BlockArrayMask) == 0) return new ReadOnlySpan<uint>(_buckets, (int)bucket, 1);
        int index = (int)(blockId ^ BlockArrayMask);
        int numBlocks = (int)_blockArrayBuffer[index];
        return new ReadOnlySpan<uint>(_blockArrayBuffer, index + 1, numBlocks);
    }
}
