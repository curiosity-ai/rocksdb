using System.Buffers;
using RocksDb.Managed.Db;
using RocksDb.Managed.Table.Filters;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Table;

/// <summary>
/// Filter block builder protocol used by <see cref="BlockBasedTableBuilder"/>. Port of FilterBlockBuilder
/// (table/block_based/filter_block.h): user keys (timestamp-stripped) are added in order, then <see cref="Finish"/> is
/// called until it reports completion, each call returning one block to write (filter partitions first, then the
/// top-level partition index).
/// </summary>
internal abstract class FilterBlockBuilder : IDisposable
{
    /// <summary>Adds a key (AddWithPrevKey: the builder tracks the previous key itself).</summary>
    public abstract void Add(ReadOnlySpan<byte> keyWithoutTs);

    /// <summary>Whether nothing was added (no filter block is written then).</summary>
    public abstract bool IsEmpty { get; }

    /// <summary>EstimateEntriesAdded: number of (de-duplicated) hash entries added, including finished partitions.</summary>
    public abstract ulong EstimateEntriesAdded { get; }

    /// <summary>PrevKeyBeforeFinish: called once with the last key added before the first <see cref="Finish"/>.</summary>
    public virtual void PrevKeyBeforeFinish(ReadOnlySpan<byte> prevKeyWithoutTs) { }

    /// <summary>
    /// Returns the next block to write in <paramref name="contents"/> (valid until the next call);
    /// <paramref name="lastPartitionHandle"/> is the handle of the previously returned block. Returns true when
    /// <paramref name="contents"/> is the last block (the full filter, or the partition index), false when more follow.
    /// </summary>
    public abstract bool Finish(BlockHandle lastPartitionHandle, out ReadOnlySpan<byte> contents);

    public abstract void Dispose();
}

/// <summary>
/// Port of FullFilterBlockBuilder (table/block_based/full_filter_block.cc): one filter over the whole table. With a
/// prefix extractor, the prefix of every key in its domain is added as well (as the "alt" of the key with
/// whole_key_filtering, alone without).
/// </summary>
internal sealed class FullFilterBlockBuilder : FilterBlockBuilder
{
    private readonly SliceTransform? _prefixExtractor;
    private readonly bool _wholeKeyFiltering;
    private readonly IFilterBitsBuilder _bits;
    private readonly PooledBufferWriter _filter = new(1024);

    public FullFilterBlockBuilder(SliceTransform? prefixExtractor, bool wholeKeyFiltering, IFilterBitsBuilder bits)
    {
        _prefixExtractor = prefixExtractor;
        _wholeKeyFiltering = wholeKeyFiltering;
        _bits = bits;
    }

    public override void Add(ReadOnlySpan<byte> keyWithoutTs)
    {
        if (_prefixExtractor is not null && _prefixExtractor.InDomain(keyWithoutTs))
        {
            ReadOnlySpan<byte> prefix = _prefixExtractor.Transform(keyWithoutTs);
            if (_wholeKeyFiltering)
                _bits.AddKeyAndAlt(keyWithoutTs, prefix);
            else
                _bits.AddKey(prefix);
        }
        else if (_wholeKeyFiltering)
        {
            _bits.AddKey(keyWithoutTs);
        }
    }

    public override bool IsEmpty => _bits.EstimatedEntries == 0;
    public override ulong EstimateEntriesAdded => (ulong)_bits.EstimatedEntries;

    public override bool Finish(BlockHandle lastPartitionHandle, out ReadOnlySpan<byte> contents)
    {
        _filter.Clear();
        _bits.Finish(_filter);
        contents = _filter.WrittenSpan;
        return true;
    }

    public override void Dispose()
    {
        _bits.Dispose();
        _filter.Dispose();
    }
}

/// <summary>
/// Port of PartitionedFilterBlockBuilder (table/block_based/partitioned_filter_block.cc): the filter is cut into
/// partitions of about metadata_block_size bytes (keys_per_partition from the bits builder's ApproximateNumEntries),
/// each keyed in a top-level index by the last key it covers ("decoupled", the default) or by the separator of the
/// aligned index partition (coupled). Partition boundaries add the neighbouring prefixes so prefix Seek and
/// SeekForPrev landing in the adjacent partition still find them.
/// </summary>
internal sealed class PartitionedFilterBlockBuilder : FilterBlockBuilder
{
    private readonly struct FilterEntry(byte[] ikey, byte[] filter, int length)
    {
        public readonly byte[] IKey = ikey;
        public readonly byte[] Filter = filter;
        public readonly int Length = length;
    }

    private readonly SliceTransform? _prefixExtractor;
    private readonly bool _wholeKeyFiltering;
    private readonly IFilterBitsBuilder _bits;
    private readonly PartitionedIndexBuilder _partitionedIndexBuilder;
    private readonly bool _decoupleFromIndexPartitions;
    private readonly bool _useValueDeltaEncoding;
    private readonly bool _valueDeltaEscape;
    private readonly BlockBuilder _indexOnFilterBlockBuilder;
    private readonly BlockBuilder _indexOnFilterBlockBuilderWithoutSeq;
    private readonly Queue<FilterEntry> _filters = new();
    private readonly PooledBufferWriter _prevKey = new(64);
    private readonly PooledBufferWriter _scratch = new(1024);
    private readonly PooledBufferWriter _encoded = new(32);
    private readonly PooledBufferWriter _deltaEncoded = new(32);
    private readonly uint _keysPerPartition;
    private ulong _totalAddedInBuilt;
    private bool _finishingFrontFilter;
    private BlockHandle _lastEncodedHandle;
    private byte[]? _returnedFilter;

    /// <summary>User-defined timestamp size of the comparator (ts_sz_): decoupled partition keys get the minimum timestamp.</summary>
    internal int TimestampSize { get; init; }

    public PartitionedFilterBlockBuilder(SliceTransform? prefixExtractor, bool wholeKeyFiltering, IFilterBitsBuilder bits, int indexBlockRestartInterval,
        bool useValueDeltaEncoding, int formatVersion, PartitionedIndexBuilder partitionedIndexBuilder, uint partitionSize,
        bool decoupleFromIndexPartitions, bool useCommonPrefix)
    {
        _prefixExtractor = prefixExtractor;
        _wholeKeyFiltering = wholeKeyFiltering;
        _bits = bits;
        _partitionedIndexBuilder = partitionedIndexBuilder;
        _decoupleFromIndexPartitions = decoupleFromIndexPartitions;
        _useValueDeltaEncoding = useValueDeltaEncoding;
        _valueDeltaEscape = BlockBasedTableConstants.FormatVersionUsesValueDeltaEscape(formatVersion);
        _indexOnFilterBlockBuilder = new BlockBuilder(indexBlockRestartInterval, true, useValueDeltaEncoding, DataBlockIndexType.BinarySearch, 0.75,
            useCommonPrefix, isUserKey: false);
        _indexOnFilterBlockBuilderWithoutSeq = new BlockBuilder(indexBlockRestartInterval, true, useValueDeltaEncoding, DataBlockIndexType.BinarySearch,
            0.75, useCommonPrefix, isUserKey: true);

        // Compute keys_per_partition_
        _keysPerPartition = (uint)_bits.ApproximateNumEntries(partitionSize);
        if (_keysPerPartition < 1)
        {
            // partition_size (minus buffer, ~10%) might be smaller than minimum filter size, sometimes based on cache
            // line size. Try to find that minimum size without CalculateSpace (not necessarily available).
            uint larger = Math.Max(partitionSize + 4, 16u);
            for (; ; )
            {
                _keysPerPartition = (uint)_bits.ApproximateNumEntries(larger);
                if (_keysPerPartition >= 1) break;
                larger += larger / 4;
                if (larger > 100000)
                {
                    // might be a broken implementation. substitute something reasonable: 1 key / byte.
                    _keysPerPartition = partitionSize;
                    break;
                }
            }
        }
        if (_keysPerPartition > 1 && _prefixExtractor is not null)
        {
            // Correct for adding next prefix in CutAFilterBlock *after* checking against this threshold
            _keysPerPartition--;
        }
    }

    public override bool IsEmpty => _bits.EstimatedEntries == 0 && _filters.Count == 0;
    public override ulong EstimateEntriesAdded => _totalAddedInBuilt + (ulong)_bits.EstimatedEntries;

    private bool DecideCutAFilterBlock()
    {
        ulong added = (ulong)_bits.EstimatedEntries;
        if (_decoupleFromIndexPartitions)
        {
            // NOTE: Can't just use ==, because estimated might be incremented by more than one.
            return added >= _keysPerPartition;
        }
        if (added >= _keysPerPartition)
        {
            // Currently only index builder is in charge of cutting a partition. We keep requesting until it is granted.
            _partitionedIndexBuilder.RequestPartitionCut();
        }
        return _partitionedIndexBuilder.ShouldCutFilterBlock();
    }

    /// <summary>Port of CutAFilterBlock.</summary>
    private void CutAFilterBlock(bool hasNextKey, ReadOnlySpan<byte> nextPrefix, bool hasNextPrefix, ReadOnlySpan<byte> prevKey)
    {
        // When there is a next partition, add the prefix of the first key in the next partition before closing this
        // one out. This is needed to support prefix Seek (see upstream comment).
        if (hasNextPrefix)
        {
            if (_wholeKeyFiltering)
            {
                // NOTE: At the end of building filter bits, we need a special case for treating prefix as an "alt"
                // entry. See AddKeyAndAlt() comment. This is a reasonable hack for that.
                _bits.AddKeyAndAlt(nextPrefix, nextPrefix);
            }
            else
            {
                _bits.AddKey(nextPrefix);
            }
        }

        // Cut the partition
        _totalAddedInBuilt += (ulong)_bits.EstimatedEntries;
        _scratch.Clear();
        _bits.Finish(_scratch);
        byte[] filter = ArrayPool<byte>.Shared.Rent(Math.Max(1, _scratch.WrittenCount));
        _scratch.WrittenSpan.CopyTo(filter);
        byte[] ikey;
        if (_decoupleFromIndexPartitions)
        {
            // prevKey has no timestamp: the partition key carries the minimum timestamp (AppendKeyWithMinTimestamp).
            int ts = TimestampSize;
            ikey = new byte[prevKey.Length + ts + DbFormat.InternalKeyFooterSize];
            prevKey.CopyTo(ikey);
            Coding.EncodeFixed64(ikey.AsSpan(prevKey.Length + ts), DbFormat.PackSequenceAndType(0, EntryType.Deletion));
        }
        else
        {
            ikey = _partitionedIndexBuilder.PartitionKey.ToArray();
        }
        _filters.Enqueue(new FilterEntry(ikey, filter, _scratch.WrittenCount));

        // If we are building another filter partition, the last prefix in the previous partition should be added to
        // support prefix SeekForPrev. (Analogous to above fix for prefix Seek.)
        if (hasNextKey && _prefixExtractor is not null && _prefixExtractor.InDomain(prevKey))
        {
            // NOTE: At the beginning of building filter bits, we don't need a special case for treating prefix as an
            // "alt" entry. See DBBloomFilterTest.FilterBitsBuilderDedup
            _bits.AddKey(_prefixExtractor.Transform(prevKey));
        }
    }

    public override void Add(ReadOnlySpan<byte> keyWithoutTs)
    {
        AddImpl(keyWithoutTs, _prevKey.WrittenSpan);
        _prevKey.Clear();
        _prevKey.Write(keyWithoutTs);
    }

    private void AddImpl(ReadOnlySpan<byte> keyWithoutTs, ReadOnlySpan<byte> prevKeyWithoutTs)
    {
        // When filter partitioning is coupled to index partitioning, we need to check for cutting a block even if we
        // aren't adding anything this time.
        bool cut = DecideCutAFilterBlock();
        if (_prefixExtractor is not null && _prefixExtractor.InDomain(keyWithoutTs))
        {
            ReadOnlySpan<byte> prefix = _prefixExtractor.Transform(keyWithoutTs);
            if (cut) CutAFilterBlock(true, prefix, true, prevKeyWithoutTs);
            if (_wholeKeyFiltering)
                _bits.AddKeyAndAlt(keyWithoutTs, prefix);
            else
                _bits.AddKey(prefix);
        }
        else
        {
            if (cut) CutAFilterBlock(true, default, false, prevKeyWithoutTs);
            if (_wholeKeyFiltering) _bits.AddKey(keyWithoutTs);
        }
    }

    public override void PrevKeyBeforeFinish(ReadOnlySpan<byte> prevKeyWithoutTs)
    {
        if (_bits.EstimatedEntries > 0) CutAFilterBlock(false, default, false, prevKeyWithoutTs);
    }

    /// <summary>Port of PartitionedFilterBlockBuilder::Finish.</summary>
    public override bool Finish(BlockHandle lastPartitionHandle, out ReadOnlySpan<byte> contents)
    {
        ReturnFilterBuffer();
        if (_finishingFrontFilter)
        {
            FilterEntry e = _filters.Dequeue();
            // Record the handle of the last written filter block in the index
            _encoded.Clear();
            lastPartitionHandle.EncodeTo(_encoded);
            _deltaEncoded.Clear();
            // Nothing to delta against for the first entry, whose value BlockBuilder writes in full anyway.
            if (_useValueDeltaEncoding && !(_lastEncodedHandle.Offset == 0 && _lastEncodedHandle.Size == 0))
                IndexValue.EncodeDelta(_deltaEncoded, lastPartitionHandle, _lastEncodedHandle, _valueDeltaEscape, false, default);
            _lastEncodedHandle = lastPartitionHandle;
            _indexOnFilterBlockBuilder.Add(e.IKey, _encoded.WrittenSpan, _deltaEncoded.WrittenSpan, false);
            if (!_partitionedIndexBuilder.SeparatorIsKeyPlusSeq)
                _indexOnFilterBlockBuilderWithoutSeq.Add(DbFormat.ExtractUserKey(e.IKey), _encoded.WrittenSpan, _deltaEncoded.WrittenSpan, false);
        }
        else if (_bits.EstimatedEntries > 0)
        {
            // PrevKeyBeforeFinish was not called
            CutAFilterBlock(false, default, false, _prevKey.WrittenSpan);
        }

        // If there is no filter partition left, then return the index on filter partitions
        if (_filters.Count == 0)
        {
            if (!_indexOnFilterBlockBuilder.IsEmpty)
            {
                // Simplest to just add them all at the end
                contents = _partitionedIndexBuilder.SeparatorIsKeyPlusSeq ? _indexOnFilterBlockBuilder.Finish() : _indexOnFilterBlockBuilderWithoutSeq.Finish();
            }
            else
            {
                // This is the rare case where no key was added to the filter
                contents = default;
            }
            return true;
        }
        // Return the next filter partition in line and set Incomplete() status to indicate we expect more calls to Finish
        _finishingFrontFilter = true;
        FilterEntry front = _filters.Peek();
        contents = front.Filter.AsSpan(0, front.Length);
        _returnedFilter = front.Filter;
        return false;
    }

    private void ReturnFilterBuffer()
    {
        if (_returnedFilter is not null)
        {
            ArrayPool<byte>.Shared.Return(_returnedFilter);
            _returnedFilter = null;
        }
    }

    public override void Dispose()
    {
        while (_filters.TryDequeue(out FilterEntry e))
        {
            if (!ReferenceEquals(e.Filter, _returnedFilter)) ArrayPool<byte>.Shared.Return(e.Filter);
        }
        ReturnFilterBuffer();
        _bits.Dispose();
        _indexOnFilterBlockBuilder.Dispose();
        _indexOnFilterBlockBuilderWithoutSeq.Dispose();
        _prevKey.Dispose();
        _scratch.Dispose();
        _encoded.Dispose();
        _deltaEncoded.Dispose();
    }
}
