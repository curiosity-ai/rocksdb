using RocksDb.Managed.Db;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Table;

/// <summary>
/// Index builder protocol used by <see cref="BlockBasedTableBuilder"/>. Port of IndexBuilder
/// (table/block_based/index_builder.h): one index entry per data block, then <see cref="Finish"/> is called until it
/// reports completion, each call returning one block to write (index partitions first, then the top-level index).
/// </summary>
internal abstract class IndexBuilder : IDisposable
{
    /// <summary>Called for every key added to a data block (before the block's index entry).</summary>
    public abstract void OnKeyAdded(ReadOnlySpan<byte> internalKey);

    /// <summary>
    /// Adds the index entry of a finished data block; returns the separator key used. <paramref name="skipDeltaEncoding"/>
    /// is set when super block alignment padding precedes the block (its handle is not contiguous with the previous one).
    /// </summary>
    public abstract ReadOnlySpan<byte> AddIndexEntry(ReadOnlySpan<byte> lastKeyInCurrentBlock, ReadOnlySpan<byte> firstKeyInNextBlock, bool hasNext,
        BlockHandle handle, bool skipDeltaEncoding);

    /// <summary>
    /// Parallel compression alternative to <see cref="AddIndexEntry"/>, 1/2 (IndexBuilder::PrepareIndexEntry): computes
    /// and records in <paramref name="entry"/> everything the index entry needs except the block handle, which is known
    /// only once the (compressed) block is written. Called in data block order, interleaved with
    /// <see cref="OnKeyAdded"/> exactly where <see cref="AddIndexEntry"/> would be called.
    /// </summary>
    public abstract void PrepareIndexEntry(ReadOnlySpan<byte> lastKeyInCurrentBlock, ReadOnlySpan<byte> firstKeyInNextBlock, bool hasNext,
        PreparedIndexEntry entry);

    /// <summary>
    /// Parallel compression alternative to <see cref="AddIndexEntry"/>, 2/2 (IndexBuilder::FinishIndexEntry): adds the
    /// prepared entry with its handle. Called for the prepared entries in the same order, possibly several blocks
    /// later (on the same thread). The resulting index blocks are byte-identical to the ones
    /// <see cref="AddIndexEntry"/> builds.
    /// </summary>
    public abstract void FinishIndexEntry(BlockHandle handle, PreparedIndexEntry entry, bool skipDeltaEncoding);

    /// <summary>NumUniformIndexBlocks: index blocks written with the "is_uniform" hint ("rocksdb.num.uniform.blocks").</summary>
    public abstract ulong NumUniformIndexBlocks { get; }

    /// <summary>"separator_is_key_plus_seq": index keys are internal keys.</summary>
    public abstract bool SeparatorIsKeyPlusSeq { get; }

    /// <summary>Total (uncompressed) size of all index blocks written; valid after the final <see cref="Finish"/>.</summary>
    public abstract int IndexSize { get; }

    /// <summary>
    /// CurrentIndexSizeEstimate (UpdateIndexSizeEstimate): approximate size of the index built so far, used to estimate
    /// the table's tail for target_file_size_is_upper_bound. <paramref name="numDataBlocks"/> is the number of index
    /// entries added so far.
    /// </summary>
    public abstract ulong CurrentIndexSizeEstimate(ulong numDataBlocks);

    /// <summary>Number of index partitions (0 unless partitioned).</summary>
    public virtual int NumPartitions => 0;

    /// <summary>Size of the top-level index block of a partitioned index (0 unless partitioned).</summary>
    public virtual int TopLevelIndexSize => 0;

    /// <summary>
    /// Port of IndexBuilder::Finish(IndexBlocks*, last_partition_block_handle): returns the next block to write in
    /// <paramref name="contents"/> (valid until the next call). <paramref name="lastPartitionHandle"/> is the handle the
    /// previously returned block was written at (ignored on the first call). Returns true when <paramref name="contents"/>
    /// is the final (top-level) index block, false (Status::Incomplete upstream) when more blocks follow.
    /// </summary>
    public abstract bool Finish(BlockHandle lastPartitionHandle, out ReadOnlySpan<byte> contents);

    public abstract void Dispose();
}

/// <summary>
/// Port of IndexBuilder::PreparedIndexEntry / ShortenedIndexBuilder::ShortenedPreparedIndexEntry: what
/// <see cref="IndexBuilder.PrepareIndexEntry"/> hands to <see cref="IndexBuilder.FinishIndexEntry"/>. Reused across
/// blocks (one per parallel compression ring slot).
/// </summary>
internal sealed class PreparedIndexEntry : IDisposable
{
    public readonly PooledBufferWriter Separator = new(64);
    public readonly PooledBufferWriter FirstInternalKey = new(0);
    public bool MustUseSeparatorWithSeq;
    public bool HasNext;
    /// <summary>Length of the block's last key (PartitionedIndexBuilder's flush policy estimates with it).</summary>
    public int LastKeyLength;

    public void Dispose()
    {
        Separator.Dispose();
        FirstInternalKey.Dispose();
    }
}

/// <summary>
/// Binary-search index builder. Port of ShortenedIndexBuilder (table/block_based/index_builder.{h,cc}).
/// Builds two index blocks in parallel — one keyed by internal-key separators and one by user-key separators —
/// and emits the user-key one unless two adjacent data blocks share a boundary user key (or format_version &lt;= 2),
/// in which case the separator must carry the sequence number ("index_key_is_user_key" = 0).
/// </summary>
internal sealed class ShortenedIndexBuilder : IndexBuilder
{
    private readonly InternalKeyComparator _icmp;
    private readonly BlockBuilder _withSeq;
    private readonly BlockBuilder _withoutSeq;
    private readonly bool _useValueDeltaEncoding;
    private readonly bool _useValueDeltaEscape;
    private readonly bool _useCommonPrefix;
    private readonly bool _includeFirstKey;
    private readonly IndexShorteningMode _shortening;
    // ts_sz_ when user-defined timestamps are not persisted (persist_user_defined_timestamps = false), else 0.
    private readonly int _stripTsSize;
    private readonly PooledBufferWriter _scratch = new(64);
    private readonly PooledBufferWriter _encoded = new(64);
    private readonly PooledBufferWriter _deltaEncoded = new(64);
    private readonly PooledBufferWriter _currentBlockFirstKey = new(64);
    private readonly PooledBufferWriter _firstKeyStripped = new(64);
    private bool _mustUseSeparatorWithSeq;
    private bool _hasLastHandle;
    private BlockHandle _lastHandle;
    private int _indexSize;
    private bool _isUniform;
    private bool _finished;

    /// <summary>The statistics the index block builders record BLOCK_KEY_DISTRIBUTION_CV in (ShortenedIndexBuilder's statistics argument).</summary>
    internal Statistics? Statistics
    {
        set
        {
            _withSeq.Statistics = value;
            _withoutSeq.Statistics = value;
        }
    }

    /// <param name="stripTimestampSize">Timestamp size stripped from every index key and first key
    /// (persist_user_defined_timestamps = false), 0 to keep keys whole.</param>
    /// <param name="uniformCvThreshold">uniform_cv_threshold for the index block (negative: never "is_uniform").</param>
    public ShortenedIndexBuilder(InternalKeyComparator icmp, int indexBlockRestartInterval, int formatVersion, bool useValueDeltaEncoding,
        IndexShorteningMode shortening, bool includeFirstKey, bool useCommonPrefix, int stripTimestampSize, double uniformCvThreshold)
    {
        _icmp = icmp;
        _stripTsSize = stripTimestampSize;
        _withSeq = new BlockBuilder(indexBlockRestartInterval, true, useValueDeltaEncoding, DataBlockIndexType.BinarySearch, 0.75,
            useCommonPrefix, isUserKey: false, stripTimestampSize, useSeparatedKvStorage: false, uniformCvThreshold);
        _withoutSeq = new BlockBuilder(indexBlockRestartInterval, true, useValueDeltaEncoding, DataBlockIndexType.BinarySearch, 0.75,
            useCommonPrefix, isUserKey: true, stripTimestampSize, useSeparatedKvStorage: false, uniformCvThreshold);
        _useValueDeltaEscape = BlockBasedTableConstants.FormatVersionUsesValueDeltaEscape(formatVersion);
        _useCommonPrefix = useCommonPrefix;
        _useValueDeltaEncoding = useValueDeltaEncoding;
        _includeFirstKey = includeFirstKey;
        _shortening = shortening;
        _mustUseSeparatorWithSeq = formatVersion <= 2;
    }

    /// <summary>"separator_is_key_plus_seq": index keys are internal keys.</summary>
    public override bool SeparatorIsKeyPlusSeq => _mustUseSeparatorWithSeq;

    /// <summary>must_use_separator_with_seq_ (forced by <see cref="PartitionedIndexBuilder"/> for all partitions).</summary>
    internal bool MustUseSeparatorWithSeq
    {
        get => _mustUseSeparatorWithSeq;
        set => _mustUseSeparatorWithSeq = value;
    }

    /// <summary>index_block_builder_ (internal-key separators).</summary>
    internal BlockBuilder WithSeqBuilder => _withSeq;
    /// <summary>index_block_builder_without_seq_ (user-key separators).</summary>
    internal BlockBuilder WithoutSeqBuilder => _withoutSeq;

    public override int IndexSize => _indexSize;
    public override ulong NumUniformIndexBlocks => _isUniform ? 1UL : 0UL;
    public int CurrentSizeEstimate => _mustUseSeparatorWithSeq ? _withSeq.CurrentSizeEstimate : _withoutSeq.CurrentSizeEstimate;

    /// <summary>Port of ShortenedIndexBuilder::UpdateIndexSizeEstimate (computed on demand).</summary>
    public override ulong CurrentIndexSizeEstimate(ulong numDataBlocks)
    {
        ulong currentSize = (ulong)CurrentSizeEstimate;
        // Add buffer to generously account (in most cases) for the next index entry.
        return numDataBlocks > 0 ? currentSize + 2 * (currentSize / numDataBlocks) : currentSize;
    }

    public override void OnKeyAdded(ReadOnlySpan<byte> internalKey)
    {
        if (_includeFirstKey && _currentBlockFirstKey.WrittenCount == 0) _currentBlockFirstKey.Write(internalKey);
    }

    /// <summary>Port of ShortenedIndexBuilder::AddIndexEntry (GetSeparatorWithSeq + AddIndexEntryImpl).</summary>
    public override ReadOnlySpan<byte> AddIndexEntry(ReadOnlySpan<byte> lastKeyInCurrentBlock, ReadOnlySpan<byte> firstKeyInNextBlock, bool hasNext,
        BlockHandle handle, bool skipDeltaEncoding)
    {
        _scratch.Clear();
        ReadOnlySpan<byte> separator = GetSeparatorWithSeq(lastKeyInCurrentBlock, firstKeyInNextBlock, hasNext, _scratch, ref _mustUseSeparatorWithSeq);
        AddIndexEntryImpl(separator, GetFirstInternalKey(), handle, _mustUseSeparatorWithSeq, skipDeltaEncoding);
        _currentBlockFirstKey.Clear();
        return separator;
    }

    /// <summary>Port of ShortenedIndexBuilder::PrepareIndexEntry: separator and first key are copied into the entry.</summary>
    public override void PrepareIndexEntry(ReadOnlySpan<byte> lastKeyInCurrentBlock, ReadOnlySpan<byte> firstKeyInNextBlock, bool hasNext,
        PreparedIndexEntry entry)
    {
        PrepareIndexEntry(lastKeyInCurrentBlock, firstKeyInNextBlock, hasNext, entry, ref _mustUseSeparatorWithSeq);
        _currentBlockFirstKey.Clear();
    }

    /// <summary>
    /// PrepareIndexEntry with an external must_use_separator_with_seq flag: <see cref="PartitionedIndexBuilder"/> prepares
    /// entries for sub-index builders that may not exist yet (upstream prepares with entries_.front()).
    /// </summary>
    internal void PrepareIndexEntry(ReadOnlySpan<byte> lastKeyInCurrentBlock, ReadOnlySpan<byte> firstKeyInNextBlock, bool hasNext,
        PreparedIndexEntry entry, ref bool mustUseSeparatorWithSeq)
    {
        PooledBufferWriter sep = entry.Separator;
        sep.Clear();
        ReadOnlySpan<byte> separator = GetSeparatorWithSeq(lastKeyInCurrentBlock, firstKeyInNextBlock, hasNext, sep, ref mustUseSeparatorWithSeq);
        // The separator is left in the scratch buffer unless it is the last key itself.
        if (sep.WrittenCount == 0 || !System.Runtime.CompilerServices.Unsafe.AreSame(
                ref System.Runtime.InteropServices.MemoryMarshal.GetReference(separator),
                ref System.Runtime.InteropServices.MemoryMarshal.GetReference(sep.WrittenSpan)))
        {
            sep.Clear();
            sep.Write(separator);
        }
        entry.FirstInternalKey.Clear();
        entry.FirstInternalKey.Write(GetFirstInternalKey());
        entry.MustUseSeparatorWithSeq = mustUseSeparatorWithSeq;
        entry.HasNext = hasNext;
        entry.LastKeyLength = lastKeyInCurrentBlock.Length;
    }

    /// <summary>Port of ShortenedIndexBuilder::FinishIndexEntry.</summary>
    public override void FinishIndexEntry(BlockHandle handle, PreparedIndexEntry entry, bool skipDeltaEncoding)
    {
        if (entry.MustUseSeparatorWithSeq) _mustUseSeparatorWithSeq = true;
        AddIndexEntryImpl(entry.Separator.WrittenSpan, entry.FirstInternalKey.WrittenSpan, handle, _mustUseSeparatorWithSeq, skipDeltaEncoding);
    }

    /// <summary>
    /// Port of ShortenedIndexBuilder::GetSeparatorWithSeq. Shortened separators are written to <paramref name="scratch"/>
    /// (cleared by the caller); otherwise the last key itself is returned and <paramref name="scratch"/> stays empty.
    /// Sets <paramref name="mustUseSeparatorWithSeq"/> when adjacent blocks share a boundary user key.
    /// </summary>
    private ReadOnlySpan<byte> GetSeparatorWithSeq(ReadOnlySpan<byte> lastKeyInCurrentBlock, ReadOnlySpan<byte> firstKeyInNextBlock, bool hasNext,
        PooledBufferWriter scratch, ref bool mustUseSeparatorWithSeq)
    {
        if (hasNext)
        {
            ReadOnlySpan<byte> separator;
            if (_shortening != IndexShorteningMode.NoShortening
                && _icmp.FindShortestSeparator(lastKeyInCurrentBlock, firstKeyInNextBlock, scratch))
                separator = scratch.WrittenSpan;
            else
                separator = lastKeyInCurrentBlock;
            // ShouldUseKeyPlusSeqAsSeparator: adjacent blocks share a boundary user key. Without persisted timestamps all
            // user keys act as if they had the minimum timestamp, so they must differ without their timestamps.
            if (!mustUseSeparatorWithSeq && SameBoundaryUserKey(DbFormat.ExtractUserKey(lastKeyInCurrentBlock), DbFormat.ExtractUserKey(firstKeyInNextBlock)))
                mustUseSeparatorWithSeq = true;
            return separator;
        }
        // Last entry: a short successor shares ~no prefix with the file's keys and would collapse the common-prefix
        // index block's prefix, so with common-prefix encoding the full last key is used (GetSeparatorWithSeq).
        if (_shortening == IndexShorteningMode.ShortenSeparatorsAndSuccessor && !_useCommonPrefix
            && _icmp.FindShortSuccessor(lastKeyInCurrentBlock, scratch))
            return scratch.WrittenSpan;
        return lastKeyInCurrentBlock;
    }

    /// <summary>Port of GetFirstInternalKey: the current block's first key (timestamp stripped when not persisted), or empty.</summary>
    private ReadOnlySpan<byte> GetFirstInternalKey()
    {
        if (!_includeFirstKey) return default;
        ReadOnlySpan<byte> firstKey = _currentBlockFirstKey.WrittenSpan;
        if (_stripTsSize != 0 && firstKey.Length >= _stripTsSize + DbFormat.InternalKeyFooterSize)
        {
            // The first internal key inside IndexValue is stripped here (block builders only strip the entry keys).
            _firstKeyStripped.Clear();
            _firstKeyStripped.Write(firstKey[..^(DbFormat.InternalKeyFooterSize + _stripTsSize)]);
            _firstKeyStripped.Write(firstKey[^DbFormat.InternalKeyFooterSize..]);
            firstKey = _firstKeyStripped.WrittenSpan;
        }
        return firstKey;
    }

    /// <summary>Port of ShortenedIndexBuilder::AddIndexEntryImpl.</summary>
    private void AddIndexEntryImpl(ReadOnlySpan<byte> separator, ReadOnlySpan<byte> firstKey, BlockHandle handle, bool mustUseSeparatorWithSeq,
        bool skipDeltaEncoding)
    {
        _encoded.Clear();
        IndexValue.EncodeFull(_encoded, handle, _includeFirstKey, firstKey);
        _deltaEncoded.Clear();
        // Under format_version >= 8, a non-contiguous handle (skip_delta_encoding) is expressed with the in-value escape,
        // so the value is still delta-encoded AND the key stays delta-encoded. Under format_version <= 7,
        // skip_delta_encoding forces both a full value and a full (shared == 0) key.
        bool forceFullKey = skipDeltaEncoding && !_useValueDeltaEscape;
        if (_useValueDeltaEncoding && _hasLastHandle && (_useValueDeltaEscape || !skipDeltaEncoding))
            IndexValue.EncodeDelta(_deltaEncoded, handle, _lastHandle, _useValueDeltaEscape, _includeFirstKey, firstKey);
        _lastHandle = handle;
        _hasLastHandle = true;

        _withSeq.Add(separator, _encoded.WrittenSpan, _deltaEncoded.WrittenSpan, forceFullKey);
        if (!mustUseSeparatorWithSeq)
            _withoutSeq.Add(DbFormat.ExtractUserKey(separator), _encoded.WrittenSpan, _deltaEncoded.WrittenSpan, forceFullKey);
    }

    private bool SameBoundaryUserKey(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) =>
        _stripTsSize == 0 ? _icmp.EqualUserKey(a, b) : _icmp.UserComparator.CompareWithoutTimestamp(a, true, b, true) == 0;

    /// <summary>Finishes and returns the index block contents (valid until disposal).</summary>
    public ReadOnlySpan<byte> Finish()
    {
        System.Diagnostics.Debug.Assert(!_finished);
        _finished = true;
        BlockBuilder b = _mustUseSeparatorWithSeq ? _withSeq : _withoutSeq;
        ReadOnlySpan<byte> contents = b.Finish();
        _isUniform = b.IsUniform;
        _indexSize = contents.Length;
        return contents;
    }

    public override bool Finish(BlockHandle lastPartitionHandle, out ReadOnlySpan<byte> contents)
    {
        contents = Finish();
        return true;
    }

    public override void Dispose()
    {
        _withSeq.Dispose();
        _withoutSeq.Dispose();
        _scratch.Dispose();
        _encoded.Dispose();
        _deltaEncoded.Dispose();
        _currentBlockFirstKey.Dispose();
        _firstKeyStripped.Dispose();
    }
}

/// <summary>
/// Two-level (kTwoLevelIndexSearch) index builder. Port of PartitionedIndexBuilder (table/block_based/index_builder.cc):
/// index entries go to a sub-index (<see cref="ShortenedIndexBuilder"/>) that is cut into a new partition when the
/// partition reaches metadata_block_size (FlushBlockBySizePolicy over the partition's index block) or when the
/// coupled partitioned filter requests a cut. <see cref="Finish"/> returns the partitions one by one and finally the
/// top-level index (partition separator -&gt; partition handle). Whether separators carry sequence numbers is decided
/// for the whole table: if any partition needs them, all partitions and the top-level index use them.
/// </summary>
internal sealed class PartitionedIndexBuilder : IndexBuilder
{
    private sealed class Entry(ShortenedIndexBuilder value)
    {
        public readonly PooledBufferWriter Key = new(32);
        public readonly ShortenedIndexBuilder Value = value;
    }

    private readonly InternalKeyComparator _icmp;
    private readonly int _indexBlockRestartInterval;
    private readonly int _formatVersion;
    private readonly IndexShorteningMode _shortening;
    private readonly bool _useValueDeltaEncoding;
    private readonly bool _valueDeltaEscape;
    private readonly bool _useCommonPrefixSub;
    private readonly int _stripTsSize;
    private readonly double _uniformCvThreshold;
    private ulong _numUniformIndexBlocks;
    private readonly int _metadataBlockSize;
    private readonly int _blockSizeDeviationLimit;
    private readonly BlockBuilder _indexBlockBuilder;
    private readonly BlockBuilder _indexBlockBuilderWithoutSeq;
    private readonly List<Entry> _entries = new();
    private int _front; // entries_.pop_front()
    private readonly PooledBufferWriter _encoded = new(32);
    private readonly PooledBufferWriter _deltaEncoded = new(32);
    private ShortenedIndexBuilder? _subIndexBuilder;
    private BlockBuilder _flushMonitor = null!;
    private bool _mustUseSeparatorWithSeq;
    private bool _prepareMustUseSeparatorWithSeq;
    private bool _partitionCutRequested;
    private bool _cutFilterBlock;
    private bool _finishingIndexes;
    private BlockHandle _lastEncodedHandle;
    private int _partitionCount;
    private int _indexSize;
    private int _topLevelIndexSize;

    private Statistics? _statistics;

    /// <summary>statistics_: given to every partition's index block builder (not to the top-level index).</summary>
    internal Statistics? Statistics
    {
        set
        {
            _statistics = value;
            if (_subIndexBuilder is { } sub) sub.Statistics = value;
        }
    }

    public PartitionedIndexBuilder(InternalKeyComparator icmp, int indexBlockRestartInterval, int formatVersion, bool useValueDeltaEncoding,
        IndexShorteningMode shortening, int metadataBlockSize, int blockSizeDeviation, bool useCommonPrefixTop, bool useCommonPrefixSub,
        int stripTimestampSize, double uniformCvThreshold)
    {
        _icmp = icmp;
        _stripTsSize = stripTimestampSize;
        _uniformCvThreshold = uniformCvThreshold;
        _indexBlockRestartInterval = indexBlockRestartInterval;
        _formatVersion = formatVersion;
        _shortening = shortening;
        _useValueDeltaEncoding = useValueDeltaEncoding;
        _valueDeltaEscape = BlockBasedTableConstants.FormatVersionUsesValueDeltaEscape(formatVersion);
        _useCommonPrefixSub = useCommonPrefixSub;
        _metadataBlockSize = metadataBlockSize;
        // FlushBlockBySizePolicy: block_size_deviation_limit_ = ((block_size * (100 - deviation)) + 99) / 100.
        _blockSizeDeviationLimit = (int)(((long)metadataBlockSize * (100 - blockSizeDeviation) + 99) / 100);
        _indexBlockBuilder = new BlockBuilder(indexBlockRestartInterval, true, useValueDeltaEncoding, DataBlockIndexType.BinarySearch, 0.75,
            useCommonPrefixTop, isUserKey: false, stripTimestampSize);
        _indexBlockBuilderWithoutSeq = new BlockBuilder(indexBlockRestartInterval, true, useValueDeltaEncoding, DataBlockIndexType.BinarySearch, 0.75,
            useCommonPrefixTop, isUserKey: true, stripTimestampSize);
        // We start by false. After each partition we revise the value based on what the sub_index_builder has decided.
        _mustUseSeparatorWithSeq = false;
        MakeNewSubIndexBuilder();
    }

    public override bool SeparatorIsKeyPlusSeq => _mustUseSeparatorWithSeq;
    public override int IndexSize => _indexSize;
    public override int NumPartitions => _partitionCount;
    public override int TopLevelIndexSize => _topLevelIndexSize;
    public override ulong NumUniformIndexBlocks => _numUniformIndexBlocks;

    /// <summary>
    /// Port of PartitionedIndexBuilder::UpdateIndexSizeEstimate (computed on demand): completed partitions (their
    /// block builders' current sizes) plus the active one, ~70 bytes per top-level entry and a buffer for the next
    /// partition and top-level entry.
    /// </summary>
    public override ulong CurrentIndexSizeEstimate(ulong numDataBlocks)
    {
        // Ignore last entry which is a placeholder for the partition being built.
        int live = _entries.Count - _front;
        ulong completedPartitions = live > 0 ? (ulong)(live - 1) : 0;
        ulong completedSize = 0;
        for (int i = _front; i < _entries.Count - 1; i++) completedSize += (ulong)_entries[i].Value.CurrentSizeEstimate;
        ulong currentSub = _subIndexBuilder is null ? 0 : _subIndexBuilder.CurrentIndexSizeEstimate(0);
        ulong total = completedSize + currentSub;
        if (completedPartitions > 0)
        {
            ulong estimatedTopLevelSize = completedPartitions * 70;
            total += estimatedTopLevelSize;
            total += 2 * (completedSize / completedPartitions + estimatedTopLevelSize / completedPartitions);
        }
        else if (_subIndexBuilder is not null)
        {
            // For the first partition, estimate using the current partition's state.
            total += 2 * currentSub;
        }
        return total;
    }

    private void MakeNewSubIndexBuilder()
    {
        var sub = new ShortenedIndexBuilder(_icmp, _indexBlockRestartInterval, _formatVersion, _useValueDeltaEncoding, _shortening,
            includeFirstKey: false, _useCommonPrefixSub, _stripTsSize, _uniformCvThreshold);
        // PartitionedIndexBuilder::MakeNewSubIndexBuilder passes statistics_ to the partition builders (the top-level
        // index gets none).
        sub.Statistics = _statistics;
        _subIndexBuilder = sub;
        // Start next partition entry, where we will modify the key
        _entries.Add(new Entry(sub));
        if (_mustUseSeparatorWithSeq)
        {
            sub.MustUseSeparatorWithSeq = true;
            _flushMonitor = sub.WithSeqBuilder;
        }
        else
        {
            _flushMonitor = sub.WithoutSeqBuilder;
        }
        _partitionCutRequested = false;
    }

    /// <summary>Called when the (coupled) filter partition builder requests cutting the next partition.</summary>
    public void RequestPartitionCut() => _partitionCutRequested = true;

    /// <summary>Port of ShouldCutFilterBlock: the policy is to align the partitions of index and filters.</summary>
    public bool ShouldCutFilterBlock()
    {
        if (_cutFilterBlock)
        {
            _cutFilterBlock = false;
            return true;
        }
        return false;
    }

    /// <summary>GetPartitionKey: the separator of the newest index entry (key of the partition being built).</summary>
    public ReadOnlySpan<byte> PartitionKey => _entries.Count > _front ? _entries[^1].Key.WrittenSpan : default;

    /// <summary>Port of FlushBlockBySizePolicy::Update over the monitored partition index block.</summary>
    private bool FlushPolicyUpdate(int keyLength, int valueLength)
    {
        BlockBuilder b = _flushMonitor;
        // it makes no sense to flush when the data block is empty
        if (b.IsEmpty) return false;
        int currSize = b.CurrentSizeEstimate;
        if (currSize >= _metadataBlockSize) return true;
        if (_blockSizeDeviationLimit == 0) return false;
        int after = b.EstimateSizeAfterKV(keyLength, valueLength);
        return after > _metadataBlockSize && currSize > _blockSizeDeviationLimit;
    }

    private void MaybeFlush(int indexKeyLength, BlockHandle indexValue)
    {
        bool doFlush = !_subIndexBuilder!.WithSeqBuilder.IsEmpty
            && (_partitionCutRequested || FlushPolicyUpdate(indexKeyLength, EncodedLength(indexValue)));
        if (doFlush)
        {
            _cutFilterBlock = true;
            MakeNewSubIndexBuilder();
        }
    }

    private static int EncodedLength(BlockHandle h) => Coding.VarintLength(h.Offset) + Coding.VarintLength(h.Size);

    public override void OnKeyAdded(ReadOnlySpan<byte> internalKey) => _subIndexBuilder!.OnKeyAdded(internalKey);

    /// <summary>
    /// Port of PartitionedIndexBuilder::PrepareIndexEntry: any sub-index builder can prepare an entry finished at
    /// another; <see cref="_prepareMustUseSeparatorWithSeq"/> tracks must_use_separator_with_seq along the pipeline
    /// (upstream: the first sub builder's flag).
    /// </summary>
    public override void PrepareIndexEntry(ReadOnlySpan<byte> lastKeyInCurrentBlock, ReadOnlySpan<byte> firstKeyInNextBlock, bool hasNext,
        PreparedIndexEntry entry)
        => _subIndexBuilder!.PrepareIndexEntry(lastKeyInCurrentBlock, firstKeyInNextBlock, hasNext, entry, ref _prepareMustUseSeparatorWithSeq);

    /// <summary>
    /// Port of PartitionedIndexBuilder::FinishIndexEntry. Deviation: replays exactly what <see cref="AddIndexEntry"/>
    /// does (no partition cut before the last entry, partition cut signal at the end) so that parallel compression
    /// produces the same partitions as the serial path; upstream's FinishIndexEntry may cut before the last entry.
    /// </summary>
    public override void FinishIndexEntry(BlockHandle handle, PreparedIndexEntry entry, bool skipDeltaEncoding)
    {
        if (entry.HasNext) MaybeFlush(entry.LastKeyLength, handle);
        ShortenedIndexBuilder sub = _subIndexBuilder!;
        sub.FinishIndexEntry(handle, entry, skipDeltaEncoding);
        PooledBufferWriter key = _entries[^1].Key;
        key.Clear();
        key.Write(entry.Separator.WrittenSpan);
        if (!_mustUseSeparatorWithSeq && sub.MustUseSeparatorWithSeq)
        {
            // We need to apply !must_use_separator_with_seq to all sub-index builders
            _mustUseSeparatorWithSeq = true;
            _flushMonitor = sub.WithSeqBuilder;
        }
        if (!entry.HasNext) _cutFilterBlock = true;
    }

    /// <summary>Port of PartitionedIndexBuilder::AddIndexEntry.</summary>
    public override ReadOnlySpan<byte> AddIndexEntry(ReadOnlySpan<byte> lastKeyInCurrentBlock, ReadOnlySpan<byte> firstKeyInNextBlock, bool hasNext,
        BlockHandle handle, bool skipDeltaEncoding)
    {
        // At least when running without parallel compression, maintain behavior of avoiding a last index partition
        // with just one entry
        if (hasNext) MaybeFlush(lastKeyInCurrentBlock.Length, handle);

        ShortenedIndexBuilder sub = _subIndexBuilder!;
        ReadOnlySpan<byte> sep = sub.AddIndexEntry(lastKeyInCurrentBlock, firstKeyInNextBlock, hasNext, handle, skipDeltaEncoding);
        PooledBufferWriter key = _entries[^1].Key;
        key.Clear();
        key.Write(sep);

        if (!_mustUseSeparatorWithSeq && sub.MustUseSeparatorWithSeq)
        {
            // We need to apply !must_use_separator_with_seq to all sub-index builders
            _mustUseSeparatorWithSeq = true;
            _flushMonitor = sub.WithSeqBuilder;
        }
        if (!hasNext)
        {
            // no more keys
            _cutFilterBlock = true;
        }
        return key.WrittenSpan;
    }

    /// <summary>Port of PartitionedIndexBuilder::Finish.</summary>
    public override bool Finish(BlockHandle lastPartitionHandle, out ReadOnlySpan<byte> contents)
    {
        if (_partitionCount == 0)
        {
            _subIndexBuilder = null;
            if (_entries.Count > _front)
            {
                // Remove the last entry if it is empty
                Entry last = _entries[^1];
                if (last.Value.WithSeqBuilder.IsEmpty)
                {
                    System.Diagnostics.Debug.Assert(last.Key.WrittenCount == 0);
                    last.Value.Dispose();
                    last.Key.Dispose();
                    _entries.RemoveAt(_entries.Count - 1);
                }
                _partitionCount = _entries.Count - _front;
            }
        }
        if (_finishingIndexes)
        {
            Entry lastEntry = _entries[_front];
            _encoded.Clear();
            Coding.PutVarint64(_encoded, lastPartitionHandle.Offset);
            Coding.PutVarint64(_encoded, lastPartitionHandle.Size);
            _deltaEncoded.Clear();
            // Nothing to delta against for the first entry; it is a restart point (shared == 0), so BlockBuilder writes
            // its value in full and never consults this (empty) delta.
            if (_useValueDeltaEncoding && !(_lastEncodedHandle.Offset == 0 && _lastEncodedHandle.Size == 0))
                IndexValue.EncodeDelta(_deltaEncoded, lastPartitionHandle, _lastEncodedHandle, _valueDeltaEscape, false, default);
            _lastEncodedHandle = lastPartitionHandle;
            _indexBlockBuilder.Add(lastEntry.Key.WrittenSpan, _encoded.WrittenSpan, _deltaEncoded.WrittenSpan, false);
            if (!_mustUseSeparatorWithSeq)
                _indexBlockBuilderWithoutSeq.Add(DbFormat.ExtractUserKey(lastEntry.Key.WrittenSpan), _encoded.WrittenSpan, _deltaEncoded.WrittenSpan, false);
            lastEntry.Value.Dispose();
            lastEntry.Key.Dispose();
            _entries[_front] = null!;
            _front++;
        }
        // If there is no sub_index left, then return the 2nd level index.
        if (_front >= _entries.Count)
        {
            BlockBuilder top = _mustUseSeparatorWithSeq ? _indexBlockBuilder : _indexBlockBuilderWithoutSeq;
            contents = top.Finish();
            _numUniformIndexBlocks += top.IsUniform ? 1UL : 0UL;
            _topLevelIndexSize = contents.Length;
            _indexSize += _topLevelIndexSize;
            return true;
        }
        // Finish the next partition index in line and Incomplete() to indicate we expect more calls to Finish
        Entry entry = _entries[_front];
        // Apply the policy to all sub-indexes
        entry.Value.MustUseSeparatorWithSeq = _mustUseSeparatorWithSeq;
        contents = entry.Value.Finish();
        _numUniformIndexBlocks += entry.Value.NumUniformIndexBlocks;
        _indexSize += contents.Length;
        _finishingIndexes = true;
        return false;
    }

    public override void Dispose()
    {
        for (int i = _front; i < _entries.Count; i++)
        {
            Entry? e = _entries[i];
            if (e is null) continue;
            e.Value.Dispose();
            e.Key.Dispose();
        }
        _entries.Clear();
        _indexBlockBuilder.Dispose();
        _indexBlockBuilderWithoutSeq.Dispose();
        _encoded.Dispose();
        _deltaEncoded.Dispose();
    }
}

/// <summary>
/// kHashSearch index builder. Port of HashIndexBuilder (table/block_based/index_builder.h): a binary-searchable primary
/// index (<see cref="ShortenedIndexBuilder"/>) plus two meta blocks describing, for every distinct key prefix (in key
/// order), the data blocks it spans: "rocksdb.hashindex.prefixes" (the prefixes concatenated) and
/// "rocksdb.hashindex.metadata" (varint32 prefix length, index of the first data block, number of data blocks). The
/// reader turns them into a <see cref="BlockPrefixIndex"/>.
/// </summary>
internal sealed class HashIndexBuilder : IndexBuilder
{
    private readonly ShortenedIndexBuilder _primary;
    private readonly SliceTransform _hashKeyExtractor;
    // stores a sequence of prefixes
    private readonly PooledBufferWriter _prefixBlock = new(1024);
    // stores the metadata of prefixes
    private readonly PooledBufferWriter _prefixMetaBlock = new(256);
    // The following 3 variables keep the unflushed prefix and its metadata.
    private uint _pendingBlockNum;
    private uint _pendingEntryIndex;
    private readonly PooledBufferWriter _pendingEntryPrefix = new(32);
    private ulong _currentRestartIndex;

    public HashIndexBuilder(InternalKeyComparator icmp, SliceTransform hashKeyExtractor, int indexBlockRestartInterval, int formatVersion,
        bool useValueDeltaEncoding, IndexShorteningMode shortening, bool useCommonPrefix, int stripTimestampSize, double uniformCvThreshold)
    {
        _primary = new ShortenedIndexBuilder(icmp, indexBlockRestartInterval, formatVersion, useValueDeltaEncoding, shortening, includeFirstKey: false,
            useCommonPrefix, stripTimestampSize, uniformCvThreshold);
        _hashKeyExtractor = hashKeyExtractor;
    }

    public override bool SeparatorIsKeyPlusSeq => _primary.SeparatorIsKeyPlusSeq;
    public override int IndexSize => _primary.IndexSize + _prefixBlock.WrittenCount + _prefixMetaBlock.WrittenCount;
    public override ulong NumUniformIndexBlocks => _primary.NumUniformIndexBlocks;
    // Upstream returns 0 (no tail size estimate for kHashSearch).
    public override ulong CurrentIndexSizeEstimate(ulong numDataBlocks) => 0;

    /// <summary>"rocksdb.hashindex.prefixes" contents (after <see cref="Finish"/>).</summary>
    public ReadOnlySpan<byte> PrefixesBlock => _prefixBlock.WrittenSpan;
    /// <summary>"rocksdb.hashindex.metadata" contents (after <see cref="Finish"/>).</summary>
    public ReadOnlySpan<byte> PrefixesMetadataBlock => _prefixMetaBlock.WrittenSpan;

    public override ReadOnlySpan<byte> AddIndexEntry(ReadOnlySpan<byte> lastKeyInCurrentBlock, ReadOnlySpan<byte> firstKeyInNextBlock, bool hasNext,
        BlockHandle handle, bool skipDeltaEncoding)
    {
        ++_currentRestartIndex;
        return _primary.AddIndexEntry(lastKeyInCurrentBlock, firstKeyInNextBlock, hasNext, handle, skipDeltaEncoding);
    }

    public override void PrepareIndexEntry(ReadOnlySpan<byte> lastKeyInCurrentBlock, ReadOnlySpan<byte> firstKeyInNextBlock, bool hasNext,
        PreparedIndexEntry entry)
    {
        ++_currentRestartIndex;
        _primary.PrepareIndexEntry(lastKeyInCurrentBlock, firstKeyInNextBlock, hasNext, entry);
    }

    public override void FinishIndexEntry(BlockHandle handle, PreparedIndexEntry entry, bool skipDeltaEncoding)
        => _primary.FinishIndexEntry(handle, entry, skipDeltaEncoding);

    public override void OnKeyAdded(ReadOnlySpan<byte> internalKey)
    {
        ReadOnlySpan<byte> keyPrefix = Prefix(internalKey);
        bool isFirstEntry = _pendingBlockNum == 0;
        // Keys may share the prefix
        if (isFirstEntry || !_pendingEntryPrefix.WrittenSpan.SequenceEqual(keyPrefix))
        {
            if (!isFirstEntry) FlushPendingPrefix();
            _pendingEntryPrefix.Clear();
            _pendingEntryPrefix.Write(keyPrefix);
            _pendingBlockNum = 1;
            _pendingEntryIndex = (uint)_currentRestartIndex;
        }
        else
        {
            // entry number increments when keys share the prefix reside in different data blocks.
            ulong lastRestartIndex = (ulong)_pendingEntryIndex + _pendingBlockNum - 1;
            System.Diagnostics.Debug.Assert(lastRestartIndex <= _currentRestartIndex);
            if (lastRestartIndex != _currentRestartIndex) ++_pendingBlockNum;
        }
    }

    /// <summary>
    /// InternalKeySliceTransform::Transform: the prefix of the user key. Upstream applies the extractor without an InDomain
    /// check; a fixed-length extractor then reads its length from the internal key (running into the footer for short user
    /// keys), which is reproduced here for byte-identical output. Other out-of-domain keys use the whole user key.
    /// </summary>
    private ReadOnlySpan<byte> Prefix(ReadOnlySpan<byte> internalKey)
    {
        ReadOnlySpan<byte> userKey = DbFormat.ExtractUserKey(internalKey);
        if (_hashKeyExtractor.InDomain(userKey)) return _hashKeyExtractor.Transform(userKey);
        if (_hashKeyExtractor.FullLengthEnabled(out int len) && len <= internalKey.Length) return internalKey[..len];
        return userKey;
    }

    private void FlushPendingPrefix()
    {
        _prefixBlock.Write(_pendingEntryPrefix.WrittenSpan);
        Coding.PutVarint32(_prefixMetaBlock, (uint)_pendingEntryPrefix.WrittenCount);
        Coding.PutVarint32(_prefixMetaBlock, _pendingEntryIndex);
        Coding.PutVarint32(_prefixMetaBlock, _pendingBlockNum);
    }

    public override bool Finish(BlockHandle lastPartitionHandle, out ReadOnlySpan<byte> contents)
    {
        if (_pendingBlockNum != 0)
        {
            FlushPendingPrefix();
            _pendingBlockNum = 0;
        }
        contents = _primary.Finish();
        return true;
    }

    public override void Dispose()
    {
        _primary.Dispose();
        _prefixBlock.Dispose();
        _prefixMetaBlock.Dispose();
        _pendingEntryPrefix.Dispose();
    }
}
