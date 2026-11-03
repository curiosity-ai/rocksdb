using System.Runtime.CompilerServices;
using System.Buffers;
using System.Security.Cryptography;
using System.Text;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table.Filters;
using RocksDb.Managed.Util;
using EntryType = RocksDb.Managed.Db.EntryType;
using CompressionOptions = RocksDb.Managed.CompressionOptions;

namespace RocksDb.Managed.Table;

/// <summary>
/// Builds a block-based table (SST file). Port of BlockBasedTableBuilder (table/block_based/block_based_table_builder.cc):
/// the emit path, optionally with parallel data block compression (CompressionOptions.parallel_threads &gt; 1, see
/// BlockBasedTableBuilder.Parallel.cs), dictionary compression (buffered state), all index
/// types (binary search, with first key, partitioned, kHashSearch), full and partitioned filters, separated key/value
/// data blocks, the index "is_uniform" hint, block_align and super block alignment, plus user table properties
/// collectors (<see cref="TablePropertiesCollector"/>).
/// <para>
/// Keys are internal keys (user key + 8-byte packed sequence/type) added in strictly increasing internal-key order;
/// range deletions (<see cref="EntryType.RangeDeletion"/>) may be interleaved and go to the "rocksdb.range_del" meta block.
/// </para>
/// <para>
/// I/O: serialized blocks are staged in an internal pooled buffer and handed to the <see cref="IWritableFile"/> in
/// large chunks. <see cref="Add"/> is synchronous (it may call <see cref="IWritableFile.Append"/>);
/// <see cref="AddAsync"/> / <see cref="FinishAsync"/> only perform asynchronous I/O. Not thread-safe.
/// The file is not closed by the builder.
/// </para>
/// </summary>
public sealed partial class BlockBasedTableBuilder : IDisposable
{
    /// <summary>Staged output is handed to the file once it exceeds this many bytes.</summary>
    private const int OutputFlushThreshold = 256 * 1024;
    /// <summary>kCompressionSizeLimit: blocks at least this large are never compressed.</summary>
    private const int CompressionSizeLimit = int.MaxValue;

    private readonly IWritableFile _file;
    private readonly TableFormatOptions _options;
    private readonly TableBuilderOptions _tbo;
    private readonly Comparator _ucmp;
    /// <summary>User-defined timestamp size (ts_sz): filters get user keys without it; min/max timestamps are collected.</summary>
    private readonly int _tsSize;
    /// <summary>
    /// _tsSize when user-defined timestamps are not persisted (persist_user_defined_timestamps = false, Rep's
    /// persist_user_defined_timestamps): every block builder strips the timestamp from its keys; else 0.
    /// </summary>
    private readonly int _stripTsSize;
    // TimestampTablePropertiesCollector state ("rocksdb.timestamp_min" / "rocksdb.timestamp_max").
    private byte[]? _timestampMin;
    private byte[]? _timestampMax;
    private readonly InternalKeyComparator _icmp;
    private readonly BlockBuilder _dataBlock;
    private readonly BlockBuilder _rangeDelBlock;
    private readonly IndexBuilder _indexBuilder;
    private readonly FilterBlockBuilder? _filterBuilder;
    /// <summary>
    /// Staging buffers of all builders. A dedicated (lock-based, untrimmed) pool: the 512 KiB arrays from
    /// ArrayPool.Shared were mostly not found again by the next builder (rented and returned on different flush /
    /// compaction threads) and were reallocated on the large object heap per output file.
    /// </summary>
    private static readonly ArrayPool<byte> s_stagingPool = ArrayPool<byte>.Create(1 << 20, 16);
    private readonly PooledBufferWriter _out = new(OutputFlushThreshold + 64 * 1024, s_stagingPool);
    private readonly PooledBufferWriter _lastKey = new(64);
    private readonly PooledBufferWriter _compressed = new(4096);
    private readonly TableProperties _props = new();
    private readonly bool _useDeltaEncodingForIndexValues;
    private readonly uint _baseContextChecksum;
    private readonly SliceTransform? _prefixExtractor;
    private readonly bool _partitionedFilter;
    // Dictionary compression (Rep::State kBuffered / kUnbuffered): while buffering, finished data blocks are kept
    // uncompressed (not yet indexed or filtered) until the dictionary can be built from samples of them.
    private bool _buffered;
    private readonly ulong _bufferLimit;
    private readonly int _maxSampleBytes;
    private ulong _dataBeginOffset;
    private readonly List<byte[]> _dataBlockBuffers = new();
    private CompressionDictionary? _dict;
    private readonly int _blockSizeDeviationLimit;
    private readonly int _alignment;
    private readonly TablePropertiesCollectors? _collectors;

    /// <summary>
    /// Port of BlockBasedTableBuilder::NeedCompact: a user table properties collector asks for the file to be compacted
    /// (sets FileMetaData::marked_for_compaction). Valid after <see cref="Finish"/>.
    /// </summary>
    public bool NeedCompact => _collectors is not null && _collectors.NeedCompact;
    private readonly int _compressionLevel;
    private readonly int _maxCompressedBytesPerKb;
    private ulong _offset;
    private uint _compressionTypesUsed; // bitset over CompressionType values (< 32)
    private bool _closed;

    public BlockBasedTableBuilder(IWritableFile file, TableFormatOptions options, TableBuilderOptions tableBuilderOptions)
    {
        ArgumentNullException.ThrowIfNull(file);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(tableBuilderOptions);
        options.Validate();
        if (tableBuilderOptions.CompressionType != CompressionType.None && !BlockCompression.IsSupported(tableBuilderOptions.CompressionType))
            ThrowHelper.NotSupported("compression type " + tableBuilderOptions.CompressionType + " is not supported");
        _file = file;
        _options = options;
        _tbo = tableBuilderOptions;
        _ucmp = tableBuilderOptions.Comparator;
        _tsSize = _ucmp.TimestampSize;
        _stripTsSize = tableBuilderOptions.PersistUserDefinedTimestamps ? 0 : _tsSize;
        _icmp = _ucmp.Kind == ComparatorKind.Bytewise ? InternalKeyComparator.Bytewise : new InternalKeyComparator(_ucmp);
        _offset = (ulong)file.Size;
        bool useCommonPrefix = UseCommonPrefix(options, _ucmp);
        // UseCommonPrefixForIndex: super block alignment triggers skip_delta_encoding on index entries, which stores a
        // non-restart key with shared == 0 that the reader cannot distinguish from a stripped restart key.
        bool useCommonPrefixIndex = useCommonPrefix && options.SuperBlockAlignmentSize == 0;
        // Rep::alignment: block_align pads data blocks to min(block_size, kDefaultPageSize).
        _alignment = options.BlockAlign ? Math.Min(options.BlockSize, TableFormatOptions.DefaultPageSize) : 0;
        if (_alignment > 0 && tableBuilderOptions.CompressionType != CompressionType.None)
            ThrowHelper.InvalidArgument("Enable block_align, but compression enabled");
        // The hash index needs keys whose bytes determine equality (CanKeysWithDifferentByteContentsBeEqual).
        _dataBlock = new BlockBuilder(options.BlockRestartInterval, options.UseDeltaEncoding, false,
            _ucmp.CanKeysWithDifferentByteContentsBeEqual ? DataBlockIndexType.BinarySearch : options.DataBlockIndexType,
            options.DataBlockHashTableUtilRatio, useCommonPrefix, isUserKey: false, _stripTsSize,
            options.SeparateKeyValueInDataBlock, uniformCvThreshold: -1.0);
        // RangeDelBlockBuilder(ts_sz, persist_user_defined_timestamps): the start keys are stripped by the block
        // builder, the end keys (values) in AddCore.
        _rangeDelBlock = new BlockBuilder(1, true, false, DataBlockIndexType.BinarySearch, 0.75, useCommonPrefix: false, isUserKey: false, _stripTsSize);
        _useDeltaEncodingForIndexValues = options.FormatVersion >= 4 && !options.BlockAlign;
        PartitionedIndexBuilder? partitionedIndex = null;
        if (options.IndexType == IndexType.TwoLevelIndexSearch)
        {
            // PartitionedIndexBuilder::CreateIndexBuilder (use_common_prefix applies to all index tiers).
            partitionedIndex = new PartitionedIndexBuilder(_icmp, options.IndexBlockRestartInterval, options.FormatVersion, _useDeltaEncodingForIndexValues,
                options.IndexShortening, options.MetadataBlockSize, options.BlockSizeDeviation, useCommonPrefixIndex, useCommonPrefixIndex, _stripTsSize,
                options.UniformCvThreshold);
            _indexBuilder = partitionedIndex;
        }
        else if (options.IndexType == IndexType.HashSearch)
        {
            // BlockBasedTableFactory::ValidateOptions: kHashSearch requires a prefix extractor. Currently kHashSearch is
            // incompatible with index_block_restart_interval > 1 (sanitized to 1 by InitializeOptions).
            if (tableBuilderOptions.PrefixExtractor is null)
                ThrowHelper.InvalidArgument("Hash index is specified for block-based table, but prefix_extractor is not given");
            _indexBuilder = new HashIndexBuilder(_icmp, tableBuilderOptions.PrefixExtractor, 1, options.FormatVersion, _useDeltaEncodingForIndexValues,
                options.IndexShortening, useCommonPrefixIndex, _stripTsSize, options.UniformCvThreshold);
        }
        else
        {
            _indexBuilder = new ShortenedIndexBuilder(_icmp, options.IndexBlockRestartInterval, options.FormatVersion, _useDeltaEncodingForIndexValues,
                options.IndexShortening, options.IndexType == IndexType.BinarySearchWithFirstKey, useCommonPrefixIndex, _stripTsSize,
                options.UniformCvThreshold);
        }
        _prefixExtractor = tableBuilderOptions.PrefixExtractor;
        // Port of CreateFilterBlockBuilder: the policy picks the filter implementation from the full building context
        // (format version, level, flush); a built-in policy with &lt; 0.5 bits/key builds no filter at all.
        if (options.FilterPolicy is { } policy && policy is not BloomLikeFilterPolicy { CreatesFilters: false })
        {
            IFilterBitsBuilder bits = policy.CreateBuilder(new FilterBuildingContext(options.FormatVersion, tableBuilderOptions.LevelAtCreation, tableBuilderOptions.IsFlush)
            {
                OptimizeFiltersForMemory = options.OptimizeFiltersForMemory,
            });
            if (options.PartitionFilters && partitionedIndex is not null)
            {
                // Since after partition cut request from filter builder it takes time until index builder actully cuts
                // the partition, until the end of a data block potentially with many keys, we take the lower bound as
                // partition size.
                uint partitionSize = (uint)(((long)options.MetadataBlockSize * (100 - options.BlockSizeDeviation) + 99) / 100);
                partitionSize = Math.Max(partitionSize, 1u);
                _filterBuilder = new PartitionedFilterBlockBuilder(_prefixExtractor, options.WholeKeyFiltering, bits, options.IndexBlockRestartInterval,
                    _useDeltaEncodingForIndexValues, options.FormatVersion, partitionedIndex, partitionSize, options.DecouplePartitionedFilters,
                    useCommonPrefix, _stripTsSize)
                { TimestampSize = _tsSize };
                _partitionedFilter = true;
            }
            else
            {
                _filterBuilder = new FullFilterBlockBuilder(_prefixExtractor, options.WholeKeyFiltering, bits);
            }
        }
        _compressionLevel = tableBuilderOptions.CompressionOptions.Level;
        // Rep::Rep: max_compressed_bytes_per_kb = min(1023, compression_opts.max_compressed_bytes_per_kb).
        _maxCompressedBytesPerKb = Math.Min(1023, tableBuilderOptions.CompressionOptions.MaxCompressedBytesPerKb);
        _blockSizeDeviationLimit = (int)(((long)options.BlockSize * (100 - options.BlockSizeDeviation) + 99) / 100);

        // Port of the data block dictionary guidance in Rep::Rep (GetDictGuidance of the built-in compressors):
        // sampling mode buffers data blocks up to buffer_limit before the dictionary is built.
        _maxSampleBytes = GetMaxSampleBytes(tableBuilderOptions.CompressionType, tableBuilderOptions.CompressionOptions);
        if (_maxSampleBytes > 0)
        {
            _buffered = true;
            ulong maxDictBufferBytes = tableBuilderOptions.CompressionOptions.MaxDictBufferBytes;
            ulong targetFileSize = tableBuilderOptions.TargetFileSize;
            _bufferLimit = targetFileSize == 0 ? maxDictBufferBytes : maxDictBufferBytes == 0 ? targetFileSize : Math.Min(targetFileSize, maxDictBufferBytes);
        }

        // Rep::Rep: one collector per configured factory (a factory may decline with null).
        _collectors = TablePropertiesCollectors.Create(tableBuilderOptions.TablePropertiesCollectorFactories,
            new TablePropertiesCollectorContext((uint)Math.Min(tableBuilderOptions.ColumnFamilyId, uint.MaxValue), tableBuilderOptions.LevelAtCreation,
                tableBuilderOptions.NumLevels, tableBuilderOptions.LastLevelInclusiveMaxSeqnoThreshold));
        _props.ColumnFamilyId = tableBuilderOptions.ColumnFamilyId;
        _props.ColumnFamilyName = tableBuilderOptions.ColumnFamilyName;
        _props.OldestKeyTime = tableBuilderOptions.OldestKeyTime;
        _props.NewestKeyTime = tableBuilderOptions.NewestKeyTime;
        _props.FileCreationTime = tableBuilderOptions.FileCreationTime;
        _props.CreationTime = tableBuilderOptions.CreationTime;
        _props.OriginalFileNumber = tableBuilderOptions.FileNumber;
        _props.DbId = tableBuilderOptions.DbId;
        _props.DbSessionId = tableBuilderOptions.DbSessionId;
        _props.DbHostId = tableBuilderOptions.DbHostId;
        _props.FormatVersion = (ulong)options.FormatVersion;
        _props.DataBlockRestartInterval = (ulong)options.BlockRestartInterval;
        _props.IndexBlockRestartInterval = (ulong)options.IndexBlockRestartInterval;
        _props.SeparateKeyValueInDataBlock = options.SeparateKeyValueInDataBlock ? 1UL : 0UL;
        _props.KeyLargestSeqno = 0;
        _props.KeySmallestSeqno = ulong.MaxValue;

        if (BlockBasedTableConstants.FormatVersionUsesContextChecksum(options.FormatVersion))
        {
            // Must be non-zero and semi-random.
            uint v = tableBuilderOptions.TestBaseContextChecksum;
            while (v == 0) v = (uint)RandomNumberGenerator.GetInt32(int.MinValue, int.MaxValue);
            _baseContextChecksum = v;
        }
        MaybeStartParallelCompression();
    }

    /// <summary>
    /// Port of UseCommonPrefixForDataBlock / UseCommonPrefixForIndex (block_based_table_builder.cc): format_version
    /// &gt;= 8, delta encoding, no user-defined timestamps; kIfFastSeek only for the (reverse-)bytewise comparators.
    /// UseCommonPrefixForIndex additionally requires super_block_alignment_size == 0.
    /// </summary>
    internal static bool UseCommonPrefix(TableFormatOptions options, Comparator ucmp)
    {
        if (!BlockBasedTableConstants.FormatVersionUsesCommonKeyPrefix(options.FormatVersion) || !options.UseDeltaEncoding || ucmp.TimestampSize != 0)
            return false;
        bool isBytewise = ucmp.Kind is ComparatorKind.Bytewise or ComparatorKind.ReverseBytewise;
        return options.OptimizeKeyCommonPrefix switch
        {
            OptimizeKeyCommonPrefix.IfFastSeek => isBytewise,
            OptimizeKeyCommonPrefix.Enabled => true,
            _ => false,
        };
    }

    /// <summary>
    /// DictSampling::max_sample_bytes of the built-in compressor for <paramref name="type"/>, 0 when dictionary
    /// compression is disabled (max_dict_bytes = 0, no compression, or an unsupported type). ZSTD samples up to
    /// zstd_max_train_bytes when set (training input), other types up to max_dict_bytes (raw dictionary).
    /// </summary>
    internal static int GetMaxSampleBytes(CompressionType type, CompressionOptions o)
    {
        if (o.MaxDictBytes == 0 || type == CompressionType.None || !BlockCompression.IsSupported(type)) return 0;
        uint bytes = type == CompressionType.Zstd && o.ZstdMaxTrainBytes > 0 ? o.ZstdMaxTrainBytes : o.MaxDictBytes;
        return (int)Math.Min(bytes, (uint)int.MaxValue);
    }

    /// <summary>The data block compression dictionary (after the builder left the buffered state), or null.</summary>
    public ReadOnlySpan<byte> CompressionDictionary => _dict is null ? default : _dict.Data;

    public ulong NumEntries => _props.NumEntries;
    public bool IsEmpty => _props.NumEntries == 0;
    /// <summary>Size of the file generated so far (final size after <see cref="Finish"/>).</summary>
    public ulong FileSize => _offset;

    /// <summary>
    /// Port of BlockBasedTableBuilder::EstimatedTailSize: the index and filter estimates, the compression dictionary,
    /// the range deletion block, conservative properties (2 KiB) and metaindex (1 KiB) sizes and the footer. Used by
    /// compaction with target_file_size_is_upper_bound. Upstream refreshes the index/filter estimates once per data
    /// block; the port caches the result per data block count.
    /// </summary>
    public ulong EstimatedTailSize()
    {
        ulong blocks = _props.NumDataBlocks;
        if (_tailEstimateBlocks == blocks && _tailEstimate != 0) return _tailEstimate;
        ulong estimate = _indexBuilder.CurrentIndexSizeEstimate(blocks);
        if (_filterBuilder is not null) estimate += _filterBuilder.CurrentFilterSizeEstimate(blocks);
        if (_dict is not null) estimate += (ulong)_dict.Data.Length;
        if (!_rangeDelBlock.IsEmpty) estimate += (ulong)_rangeDelBlock.CurrentSizeEstimate;
        estimate += 2048 + 1024 + Footer.EncodedLength;
        _tailEstimateBlocks = blocks;
        _tailEstimate = estimate;
        return estimate;
    }
    private ulong _tailEstimateBlocks;
    private ulong _tailEstimate;
    /// <summary>Properties of the table (complete after <see cref="Finish"/>).</summary>
    public TableProperties Properties => _props;
    /// <summary>The base context checksum written to the footer (format_version &gt;= 6).</summary>
    public uint BaseContextChecksum => _baseContextChecksum;

    /// <summary>Port of BlockBasedTableBuilder::Add. May synchronously append staged output to the file.</summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public void Add(ReadOnlySpan<byte> internalKey, ReadOnlySpan<byte> value)
    {
        AddCore(internalKey, value);
        if (_out.WrittenCount >= OutputFlushThreshold) DrainOutput();
    }

    /// <summary>Adds an entry; staged output beyond the threshold is written asynchronously.</summary>
    public ValueTask AddAsync(ReadOnlyMemory<byte> internalKey, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default)
    {
        AddCore(internalKey.Span, value.Span);
        return _out.WrittenCount >= OutputFlushThreshold ? DrainOutputAsync(cancellationToken) : ValueTask.CompletedTask;
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private void AddCore(ReadOnlySpan<byte> internalKey, ReadOnlySpan<byte> value)
    {
        if (_closed) throw new InvalidOperationException("Table builder already finished.");
        if (internalKey.Length < DbFormat.InternalKeyFooterSize) ThrowHelper.InvalidArgument("internal key too short");
        DbFormat.UnpackSequenceAndType(DbFormat.ExtractFooter(internalKey), out ulong seq, out EntryType type);
        if (seq > _props.KeyLargestSeqno) _props.KeyLargestSeqno = seq;
        if (seq < _props.KeySmallestSeqno) _props.KeySmallestSeqno = seq;
        if (_tsSize != 0 && _stripTsSize == 0) CollectTimestamp(internalKey);

        if (type == EntryType.RangeDeletion)
        {
            // RangeDelBlockBuilder: without persisted timestamps the end key (value) is stored without its timestamp.
            ReadOnlySpan<byte> endKey = value;
            if (_stripTsSize != 0)
            {
                if (value.Length < _stripTsSize) ThrowHelper.InvalidArgument("range tombstone end key too short for its timestamp");
                endKey = value[..^_stripTsSize];
            }
            _rangeDelBlock.Add(internalKey, endKey);
            _collectors?.Add(internalKey, value, _offset);
            _props.NumDeletions++;
            _props.NumRangeDeletions++;
        }
        else
        {
            if (!IsValueType(type)) ThrowHelper.InvalidArgument("BlockBasedBuilder::Add() received a key with invalid value type " + (byte)type);
            if (_lastKey.WrittenCount > 0 && _icmp.Compare(internalKey, _lastKey.WrittenSpan) <= 0)
                ThrowHelper.InvalidArgument("keys must be added in strictly increasing internal key order");

            if (ShouldFlush(internalKey.Length, value.Length)) Flush(internalKey, hasNext: true);

            // Note: PartitionedFilterBlockBuilder with decouple_partitioned_filters = false requires key being added
            // to filter builder after being added to the index builder (in Flush). Buffered keys will be replayed
            // from the buffered data blocks once the compression dictionary has been finalized.
            if (!_buffered) _filterBuilder?.Add(DbFormat.ExtractUserKeyAndStripTimestamp(internalKey, _tsSize));

            _dataBlock.Add(internalKey, value);
            _lastKey.Clear();
            _lastKey.Write(internalKey);
            if (!_buffered) _indexBuilder.OnKeyAdded(internalKey);
            // NotifyCollectTableCollectorsOnAdd (file_size: the offset after the data blocks written so far).
            _collectors?.Add(internalKey, value, _offset);
            if (type is EntryType.Deletion or EntryType.SingleDeletion or EntryType.DeletionWithTimestamp) _props.NumDeletions++;
            else if (type == EntryType.Merge) _props.NumMergeOperands++;
        }
        _props.NumEntries++;
        // raw_key_size excludes the stripped timestamp (persist_user_defined_timestamps = false).
        _props.RawKeySize += (ulong)(internalKey.Length - _stripTsSize);
        _props.RawValueSize += (ulong)value.Length;
    }

    private static bool IsValueType(EntryType t) =>
        t is EntryType.Deletion or EntryType.Value or EntryType.Merge or EntryType.SingleDeletion or EntryType.BlobIndex
            or EntryType.DeletionWithTimestamp or EntryType.WideColumnEntity or EntryType.ValuePreferredSeqno;

    /// <summary>Port of FlushBlockBySizePolicy::Update.</summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private bool ShouldFlush(int keyLength, int valueLength)
    {
        if (_dataBlock.IsEmpty) return false;
        int currSize = _dataBlock.CurrentSizeEstimate;
        if (currSize >= _options.BlockSize) return true;
        if (_blockSizeDeviationLimit == 0) return false;
        int after = _dataBlock.EstimateSizeAfterKV(keyLength, valueLength);
        // block_align: the block plus its trailer must fit the (aligned) block size.
        if (_alignment > 0) return after + BlockBasedTableConstants.BlockTrailerSize > _options.BlockSize;
        return after > _options.BlockSize && currSize > _blockSizeDeviationLimit;
    }

    /// <summary>Port of BlockBasedTableBuilder::Flush + EmitBlock (buffering the block while sampling for a dictionary).</summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private void Flush(ReadOnlySpan<byte> firstKeyInNextBlock, bool hasNext)
    {
        if (_dataBlock.IsEmpty) return;
        ReadOnlySpan<byte> raw = _dataBlock.Finish();
        // NotifyCollectTableCollectorsOnBlockAdd (no compression sampling: estimated compressed sizes are 0).
        _collectors?.BlockAdd((ulong)raw.Length);
        if (_buffered)
        {
            _dataBlockBuffers.Add(raw.ToArray());
            _dataBeginOffset += (ulong)raw.Length;
            _dataBlock.Reset();
            MaybeEnterUnbuffered(firstKeyInNextBlock, hasNext);
            return;
        }
        // Increment num_data_blocks when a data block is finalized (Flush's unbuffered branch). Upstream does not count
        // blocks replayed from the dictionary buffer (EmitBlock), so with dictionary compression "rocksdb.num.data.blocks"
        // only counts blocks written after the buffer was flushed; matched for identical properties.
        _props.NumDataBlocks++;
        if (_pc is not null) EmitBlockForParallel(raw, _lastKey.WrittenSpan, firstKeyInNextBlock, hasNext);
        else EmitBlock(raw, _lastKey.WrittenSpan, firstKeyInNextBlock, hasNext);
        _dataBlock.Reset();
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private void EmitBlock(ReadOnlySpan<byte> raw, ReadOnlySpan<byte> lastKey, ReadOnlySpan<byte> firstKeyInNextBlock, bool hasNext)
    {
        // When data blocks are aligned with super block alignment, delta encoding needs to be skipped for the first block
        // after padding.
        BlockHandle handle = WriteBlock(raw, isDataBlock: true, out bool skipDeltaEncoding);
        _indexBuilder.AddIndexEntry(lastKey, firstKeyInNextBlock, hasNext, handle, skipDeltaEncoding);
    }

    /// <summary>
    /// Port of BlockBasedTableBuilder::MaybeEnterUnbuffered: once the buffered data exceeds buffer_limit (or at the
    /// end of the file), samples the buffered blocks, builds the dictionary, and replays the buffered blocks through
    /// the filter and index builders while compressing them with the dictionary.
    /// </summary>
    private void MaybeEnterUnbuffered(ReadOnlySpan<byte> firstKeyInNextBlock, bool hasNext)
    {
        if (hasNext)
        {
            bool exceedsBufferLimit = _bufferLimit != 0 && _dataBeginOffset > _bufferLimit;
            if (!exceedsBufferLimit) return;
        }
        // Enter Unbuffered state
        _buffered = false;
        int numBlocksBuffered = _dataBlockBuffers.Count;
        if (numBlocksBuffered == 0) return;

        // A finite cyclic group (the additive group of integers modulo N) can be generated by a number coprime with N;
        // N varies, so a prime is used (kPrimeGenerator: the next prime after a random number below one trillion).
        const ulong primeGenerator = 545055921143UL;
        int primeGeneratorRemainder = (int)(primeGenerator % (ulong)numBlocksBuffered);
        int bufferIdx = numBlocksBuffered / 2;
        using var samples = new PooledBufferWriter(Math.Min(_maxSampleBytes, 1 << 20));
        var sampleLengths = new List<nuint>();
        for (int i = 0; i < numBlocksBuffered && samples.WrittenCount < _maxSampleBytes; i++)
        {
            byte[] block = _dataBlockBuffers[bufferIdx];
            int copyLength = Math.Min(_maxSampleBytes - samples.WrittenCount, block.Length);
            samples.Write(block.AsSpan(0, copyLength));
            sampleLengths.Add((nuint)copyLength);
            bufferIdx += primeGeneratorRemainder;
            if (bufferIdx >= numBlocksBuffered) bufferIdx -= numBlocksBuffered;
        }
        byte[] dict = BuildDictionary(samples.WrittenSpan, System.Runtime.InteropServices.CollectionsMarshal.AsSpan(sampleLengths));
        if (dict.Length > 0) _dict = new CompressionDictionary(dict);

        // Replay: keys of each buffered block go to the filter and index builders, then the block is emitted.
        using var it = new BlockIter();
        using var next = new BlockIter();
        using var lastKey = new PooledBufferWriter(64);
        using var nextFirstKey = new PooledBufferWriter(64);
        for (int i = 0; i < numBlocksBuffered; i++)
        {
            byte[] data = _dataBlockBuffers[i];
            Block block = Block.CreateCopy(data);
            try
            {
                it.InitializeData(block, _icmp, BlockIter.DisableGlobalSequenceNumber, _stripTsSize);
                for (it.SeekToFirst(); it.Valid; it.Next())
                {
                    ReadOnlySpan<byte> key = it.Key;
                    _filterBuilder?.Add(DbFormat.ExtractUserKeyAndStripTimestamp(key, _tsSize));
                    _indexBuilder.OnKeyAdded(key);
                    lastKey.Clear();
                    lastKey.Write(key);
                }
                it.Clear();
            }
            finally
            {
                block.Release();
            }
            bool blockHasNext;
            nextFirstKey.Clear();
            if (i + 1 < numBlocksBuffered)
            {
                Block nb = Block.CreateCopy(_dataBlockBuffers[i + 1]);
                try
                {
                    next.InitializeData(nb, _icmp, BlockIter.DisableGlobalSequenceNumber, _stripTsSize);
                    next.SeekToFirst();
                    nextFirstKey.Write(next.Key);
                    next.Clear();
                }
                finally
                {
                    nb.Release();
                }
                blockHasNext = true;
            }
            else
            {
                nextFirstKey.Write(firstKeyInNextBlock);
                blockHasNext = hasNext;
            }
            if (_pc is not null) EmitBlockForParallel(data, lastKey.WrittenSpan, nextFirstKey.WrittenSpan, blockHasNext);
            else EmitBlock(data, lastKey.WrittenSpan, nextFirstKey.WrittenSpan, blockHasNext);
        }
        _dataBlockBuffers.Clear();
        _dataBeginOffset = 0;
    }

    /// <summary>
    /// Port of MaybeCloneSpecialized(DictSamples) of the built-in compressors: ZSTD trains (ZDICT_trainFromBuffer) or
    /// finalizes (ZDICT_finalizeDictionary) a dictionary when zstd_max_train_bytes is set, otherwise (and for the other
    /// compression types) the samples are the raw content dictionary. Empty: no dictionary.
    /// </summary>
    private byte[] BuildDictionary(ReadOnlySpan<byte> samples, ReadOnlySpan<nuint> sampleLengths)
    {
        if (samples.IsEmpty) return [];
        CompressionOptions o = _tbo.CompressionOptions;
        if (_tbo.CompressionType == CompressionType.Zstd && o.ZstdMaxTrainBytes > 0)
        {
            int maxDictBytes = (int)Math.Min(o.MaxDictBytes, (uint)int.MaxValue);
            return o.UseZstdDictTrainer
                ? BlockCompression.ZstdTrainDictionary(samples, sampleLengths, maxDictBytes)
                : BlockCompression.ZstdFinalizeDictionary(samples, sampleLengths, maxDictBytes, o.Level);
        }
        // ZSTD "raw content dictionary" - "Any buffer is a valid raw content dictionary." Or similar for other
        // compressions.
        return samples.ToArray();
    }

    /// <summary>Port of WriteBlock + CompressAndVerifyBlock (compression via the facade, with the ratio check).</summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private BlockHandle WriteBlock(ReadOnlySpan<byte> raw, bool isDataBlock) => WriteBlock(raw, isDataBlock, out _);

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private BlockHandle WriteBlock(ReadOnlySpan<byte> raw, bool isDataBlock, out bool skipDeltaEncoding)
    {
        CompressionType type = CompressBlock(raw, isDataBlock, _compressed, out bool rejected);
        if (rejected) _props.NumDataBlocksCompressionRejected++;
        BlockHandle h = type == CompressionType.None
            ? WriteMaybeCompressedBlock(raw, type, isDataBlock, out skipDeltaEncoding)
            : WriteMaybeCompressedBlock(_compressed.WrittenSpan, type, isDataBlock, out skipDeltaEncoding);
        if (isDataBlock)
        {
            _props.DataSize = _offset;
            _props.UncompressedDataSize += (ulong)raw.Length;
        }
        return h;
    }

    /// <summary>
    /// Port of CompressAndVerifyBlock (compression via the facade, with the ratio check). Returns the block's type:
    /// <see cref="CompressionType.None"/> (store <paramref name="raw"/>) or the configured type (store
    /// <paramref name="compressed"/>, which is cleared first). <paramref name="rejected"/>: a data block whose compression
    /// was attempted but rejected ("rocksdb.num.data.blocks.compression.rejected"). Thread-safe for distinct outputs
    /// (parallel compression workers call it concurrently): reads only immutable builder state and the dictionary.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private CompressionType CompressBlock(ReadOnlySpan<byte> raw, bool isDataBlock, PooledBufferWriter compressed, out bool rejected)
    {
        rejected = false;
        bool compress = _tbo.CompressionType != CompressionType.None && (isDataBlock || _options.EnableIndexCompression) && raw.Length < CompressionSizeLimit;
        if (!compress) return CompressionType.None;
        compressed.Clear();
        long maxCompressed = ((long)_maxCompressedBytesPerKb * raw.Length) >> 10;
        if (BlockCompression.TryCompress(_tbo.CompressionType, raw, _options.FormatVersion, _compressionLevel, _maxCompressedBytesPerKb, compressed,
                isDataBlock ? _dict : null)
            && compressed.WrittenCount <= maxCompressed)
        {
            return _tbo.CompressionType;
        }
        rejected = isDataBlock;
        return CompressionType.None;
    }

    /// <summary>WriteMaybeCompressedBlockImpl for meta blocks (no alignment).</summary>
    private BlockHandle WriteMaybeCompressedBlock(ReadOnlySpan<byte> contents, CompressionType type) =>
        WriteMaybeCompressedBlock(contents, type, isDataBlock: false, out _);

    /// <summary>
    /// Port of WriteMaybeCompressedBlockImpl: payload + trailer (type byte, checksum with context). Data blocks may be
    /// preceded by super block alignment padding (<paramref name="skipDeltaEncoding"/> = true) and followed by block_align
    /// padding; padding is zero bytes (WritableFileWriter::Pad).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private BlockHandle WriteMaybeCompressedBlock(ReadOnlySpan<byte> contents, CompressionType type, bool isDataBlock, out bool skipDeltaEncoding)
        => WriteMaybeCompressedBlock(contents, type, isDataBlock, out skipDeltaEncoding, hasRawChecksum: false, rawChecksum: 0);

    /// <param name="contents">Block payload.</param>
    /// <param name="type">Compression type byte of the trailer.</param>
    /// <param name="isDataBlock">Data blocks may be aligned / padded.</param>
    /// <param name="skipDeltaEncoding">Set when super block alignment padding precedes the block.</param>
    /// <param name="hasRawChecksum">Whether <paramref name="rawChecksum"/> holds the precomputed checksum of
    /// <paramref name="contents"/> + type byte without the context modifier (computed by a parallel compression worker).</param>
    /// <param name="rawChecksum">See <paramref name="hasRawChecksum"/>.</param>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private BlockHandle WriteMaybeCompressedBlock(ReadOnlySpan<byte> contents, CompressionType type, bool isDataBlock, out bool skipDeltaEncoding,
        bool hasRawChecksum, uint rawChecksum)
    {
        skipDeltaEncoding = false;
        // try to align the data block page to the super alignment size, if enabled
        long superSize = _options.SuperBlockAlignmentSize;
        if (superSize != 0 && isDataBlock && _options.SuperBlockAlignmentSpaceOverheadRatio != 0)
        {
            ulong mask = (ulong)superSize - 1;
            if ((_offset & ~mask) != ((_offset + (ulong)contents.Length) & ~mask))
            {
                ulong allowedMaxPaddingSize = (ulong)superSize / (ulong)_options.SuperBlockAlignmentSpaceOverheadRatio;
                // new block would cross the super block boundary
                ulong padBytes = (ulong)superSize - (_offset & mask);
                if (padBytes < allowedMaxPaddingSize)
                {
                    Pad((int)padBytes);
                    // Skip delta encoding in index block builder when a super block alignment padding is added for data block.
                    skipDeltaEncoding = true;
                }
            }
        }
        var handle = new BlockHandle(_offset, (ulong)contents.Length);
        _compressionTypesUsed |= 1u << ((int)type & 31);
        _out.Write(contents);
        Span<byte> trailer = _out.GetSpan(BlockBasedTableConstants.BlockTrailerSize);
        trailer[0] = (byte)type;
        uint checksum = hasRawChecksum
            ? rawChecksum + BlockChecksum.ChecksumModifierForContext(_baseContextChecksum, _offset)
            : BlockChecksum.ComputeTrailerChecksum(_options.Checksum, _baseContextChecksum, contents, (byte)type, _offset);
        Coding.EncodeFixed32(trailer[1..], checksum);
        _out.Advance(BlockBasedTableConstants.BlockTrailerSize);
        _offset += (ulong)contents.Length + BlockBasedTableConstants.BlockTrailerSize;
        if (_alignment > 0 && isDataBlock)
        {
            int padBytes = (_alignment - ((contents.Length + BlockBasedTableConstants.BlockTrailerSize) & (_alignment - 1))) & (_alignment - 1);
            Pad(padBytes);
        }
        return handle;
    }

    /// <summary>WritableFileWriter::Pad: <paramref name="padBytes"/> zero bytes.</summary>
    private void Pad(int padBytes)
    {
        if (padBytes <= 0) return;
        Span<byte> pad = _out.GetSpan(padBytes)[..padBytes];
        pad.Clear();
        _out.Advance(padBytes);
        _offset += (ulong)padBytes;
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private void DrainOutput()
    {
        if (_out.WrittenCount == 0) return;
        _file.Append(_out.WrittenSpan);
        _out.Clear();
    }

    private async ValueTask DrainOutputAsync(CancellationToken ct)
    {
        if (_out.WrittenCount == 0) return;
        await _file.AppendAsync(_out.WrittenMemory, ct).ConfigureAwait(false);
        _out.Clear();
    }

    /// <summary>Finishes the table: writes remaining data, filter, index, range-del, properties, metaindex and footer, then flushes the file.</summary>
    public void Finish()
    {
        FinishCore();
        DrainOutput();
        _file.Flush();
    }

    /// <summary>Async <see cref="Finish"/>.</summary>
    public async ValueTask FinishAsync(CancellationToken cancellationToken = default)
    {
        FinishCore();
        await DrainOutputAsync(cancellationToken).ConfigureAwait(false);
        await _file.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Port of BlockBasedTableBuilder::Finish (meta block order: filter, index, [dict], range del, properties, metaindex, footer).</summary>
    private void FinishCore()
    {
        if (_closed) throw new InvalidOperationException("Table builder already finished.");
        Flush(default, hasNext: false);
        if (_buffered) MaybeEnterUnbuffered(default, hasNext: false);
        // StopParallelCompression: every data block is written (and indexed) before the meta blocks.
        if (_pc is not null) StopParallelCompression(abort: false);
        _closed = true;
        _props.TailStartOffset = _offset;

        using var metaIndex = new MetaIndexBuilder();
        WriteFilterBlock(metaIndex);
        BlockHandle indexHandle = WriteIndexBlock(metaIndex);
        WriteCompressionDictBlock(metaIndex);
        WriteRangeDelBlock(metaIndex);
        WritePropertiesBlock(metaIndex);
        BlockHandle metaindexHandle = WriteMaybeCompressedBlock(metaIndex.Finish(), CompressionType.None);
        WriteFooter(metaindexHandle, indexHandle);
    }

    /// <summary>Port of BlockBasedTableBuilder::WriteFilterBlock (full filter, or filter partitions + partition index).</summary>
    private void WriteFilterBlock(MetaIndexBuilder metaIndex)
    {
        if (_filterBuilder is null || _filterBuilder.IsEmpty) return;
        if (_lastKey.WrittenCount > 0)
        {
            // We might have been using AddWithPrevKey, so need PrevKeyBeforeFinish to be safe.
            _filterBuilder.PrevKeyBeforeFinish(DbFormat.ExtractUserKeyAndStripTimestamp(_lastKey.WrittenSpan, _tsSize));
        }
        _props.NumFilterEntries += _filterBuilder.EstimateEntriesAdded;
        BlockHandle h = default;
        bool done;
        do
        {
            done = _filterBuilder.Finish(h, out ReadOnlySpan<byte> contents);
            _props.FilterSize += (ulong)contents.Length;
            h = WriteMaybeCompressedBlock(contents, CompressionType.None);
        }
        while (!done);
        metaIndex.Add((_partitionedFilter ? BlockBasedTableConstants.PartitionedFilterBlockPrefix : BlockBasedTableConstants.FullFilterBlockPrefix)
            + _options.FilterPolicy!.CompatibilityName, h);
    }

    /// <summary>Port of WriteIndexBlock / BuiltinIndexFactoryBuilder::FinishAndWrite: index partitions, then the (top-level) index.</summary>
    private BlockHandle WriteIndexBlock(MetaIndexBuilder metaIndex)
    {
        BlockHandle h = default;
        bool done;
        bool first = true;
        do
        {
            done = _indexBuilder.Finish(h, out ReadOnlySpan<byte> contents);
            if (first && _indexBuilder is HashIndexBuilder hash)
            {
                // Auxiliary meta blocks first (IndexBlocks::meta_blocks is a std::map: "metadata" < "prefixes"), written
                // like index blocks (compressible).
                metaIndex.Add(BlockBasedTableConstants.HashIndexPrefixesMetadataBlockName, WriteBlock(hash.PrefixesMetadataBlock, isDataBlock: false));
                metaIndex.Add(BlockBasedTableConstants.HashIndexPrefixesBlockName, WriteBlock(hash.PrefixesBlock, isDataBlock: false));
            }
            first = false;
            h = WriteBlock(contents, isDataBlock: false);
        }
        while (!done);
        _props.NumUniformBlocks = _indexBuilder.NumUniformIndexBlocks;
        // index_size reports the logical (uncompressed) index size + trailer.
        _props.IndexSize = (ulong)_indexBuilder.IndexSize + BlockBasedTableConstants.BlockTrailerSize;
        if (_options.IndexType == IndexType.TwoLevelIndexSearch)
        {
            _props.IndexPartitions = (ulong)_indexBuilder.NumPartitions;
            _props.TopLevelIndexSize = (ulong)_indexBuilder.TopLevelIndexSize;
        }
        if (!BlockBasedTableConstants.FormatVersionUsesIndexHandleInFooter(_options.FormatVersion))
            metaIndex.Add(BlockBasedTableConstants.IndexBlockName, h);
        return h;
    }

    /// <summary>Port of WriteCompressionDictBlock: the dictionary used for data blocks, stored uncompressed.</summary>
    private void WriteCompressionDictBlock(MetaIndexBuilder metaIndex)
    {
        if (_dict is null || _dict.Length == 0) return;
        BlockHandle h = WriteMaybeCompressedBlock(_dict.Data, CompressionType.None);
        metaIndex.Add(BlockBasedTableConstants.CompressionDictBlockName, h);
    }

    private void WriteRangeDelBlock(MetaIndexBuilder metaIndex)
    {
        if (_rangeDelBlock.IsEmpty) return;
        BlockHandle h = WriteMaybeCompressedBlock(_rangeDelBlock.Finish(), CompressionType.None);
        metaIndex.Add(BlockBasedTableConstants.RangeDelBlockName, h);
    }

    /// <summary>Port of TimestampTablePropertiesCollector::InternalAdd (db/table_properties_collector.h).</summary>
    private void CollectTimestamp(ReadOnlySpan<byte> internalKey)
    {
        ReadOnlySpan<byte> userKey = DbFormat.ExtractUserKey(internalKey);
        if (userKey.Length < _tsSize) ThrowHelper.Corruption("User key size mismatch when comparing to timestamp size.");
        ReadOnlySpan<byte> ts = DbFormat.ExtractTimestampFromUserKey(userKey, _tsSize);
        if (_timestampMax is null)
        {
            _timestampMax = ts.ToArray();
            _timestampMin = ts.ToArray();
            return;
        }
        if (_ucmp.CompareTimestamp(ts, _timestampMax) > 0) ts.CopyTo(_timestampMax);
        if (_ucmp.CompareTimestamp(_timestampMin, ts) > 0) ts.CopyTo(_timestampMin);
    }

    /// <summary>Smallest user-defined timestamp added ("rocksdb.timestamp_min"); empty without timestamps or entries.</summary>
    public byte[] TimestampMin => _timestampMin ?? [];

    /// <summary>Largest user-defined timestamp added ("rocksdb.timestamp_max"); empty without timestamps or entries.</summary>
    public byte[] TimestampMax => _timestampMax ?? [];

    private void WritePropertiesBlock(MetaIndexBuilder metaIndex)
    {
        // WritePropertiesBlock: the policy's Name(), even when it built no filter.
        _props.FilterPolicyName = _options.FilterPolicy?.Name ?? "";
        _props.ComparatorName = _ucmp.Name;
        _props.MergeOperatorName = _tbo.MergeOperatorName;
        _props.PrefixExtractorName = _tbo.PrefixExtractorName;
        _props.PropertyCollectorsNames = TablePropertiesCollectors.Names(_tbo.TablePropertiesCollectorFactories);
        _props.IndexKeyIsUserKey = _indexBuilder.SeparatorIsKeyPlusSeq ? 0UL : 1UL;
        _props.IndexValueIsDeltaEncoded = _useDeltaEncodingForIndexValues ? 1UL : 0UL;
        _props.CompressionName = BuildCompressionName();
        _props.CompressionOptions = BuildCompressionOptions();
        // rep_->props.user_defined_timestamps_persisted (written only when 0).
        _props.UserDefinedTimestampsPersisted = _stripTsSize == 0 ? 1UL : 0UL;

        using var pb = new PropertyBlockBuilder();
        _props.AddTo(pb);
        // NotifyCollectTableCollectorsOnFinish: user collectors come first in the collector list, so on a name clash
        // their properties win over the internal collectors' (std::map insert keeps the first).
        if (_collectors is not null)
        {
            var user = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            _collectors.Finish(user);
            foreach (KeyValuePair<string, byte[]> kv in user)
            {
                pb.Add(kv.Key, kv.Value);
                _props.UserCollectedProperties.TryAdd(kv.Key, kv.Value);
            }
        }
        // Internal collectors (BlockBasedTablePropertiesCollector and, for external files, SstFileWriterPropertiesCollector).
        Span<byte> idx = stackalloc byte[4];
        Coding.EncodeFixed32(idx, (uint)_options.IndexType);
        pb.Add(TablePropertiesNames.BlockBasedIndexType, idx.ToArray());
        pb.Add(TablePropertiesNames.BlockBasedWholeKeyFiltering, _options.WholeKeyFiltering ? TablePropertiesNames.PropTrue : TablePropertiesNames.PropFalse);
        pb.Add(TablePropertiesNames.BlockBasedPrefixFiltering, _prefixExtractor is not null ? TablePropertiesNames.PropTrue : TablePropertiesNames.PropFalse);
        if (_options.DecouplePartitionedFilters) pb.Add(TablePropertiesNames.BlockBasedDecoupledPartitionedFilters, TablePropertiesNames.PropTrue);
        if (_tsSize > 0 && _stripTsSize == 0)
        {
            // TimestampTablePropertiesCollector::Finish (only added when timestamps are persisted).
            pb.Add(TablePropertiesNames.TimestampMin, TimestampMin);
            pb.Add(TablePropertiesNames.TimestampMax, TimestampMax);
        }

        if (_tbo.ExternalSstFile)
        {
            byte[] version = new byte[4];
            Coding.EncodeFixed32(version, 2);
            pb.Add(TablePropertiesNames.ExternalSstFileVersion, version);
            pb.Add(TablePropertiesNames.ExternalSstFileGlobalSeqno, new byte[8]);
        }
        BlockHandle h = WriteMaybeCompressedBlock(pb.Finish(), CompressionType.None);
        metaIndex.Add(BlockBasedTableConstants.PropertiesBlockName, h);
    }

    /// <summary>Port of Rep::PostPopulateCompressionProperties (built-in compression manager only).</summary>
    private string BuildCompressionName()
    {
        uint used = _compressionTypesUsed & ~1u; // "no compression" is not reported
        if (BlockBasedTableConstants.FormatVersionUsesCompressionManagerName(_options.FormatVersion))
        {
            var sb = new StringBuilder();
            if (_tbo.CompressionType != CompressionType.None) sb.Append("BuiltinV2");
            sb.Append(';');
            for (int t = 0; t < 32; t++)
                if ((used & (1u << t)) != 0) sb.Append(t.ToString("X2", System.Globalization.CultureInfo.InvariantCulture));
            sb.Append(';');
            return sb.ToString();
        }
        if (used == 0) return CompressionTypeToString(_tbo.CompressionType);
        if ((used & (1u << (int)CompressionType.Zstd)) != 0) return CompressionTypeToString(CompressionType.Zstd);
        for (int t = 1; t < 32; t++)
            if ((used & (1u << t)) != 0) return CompressionTypeToString((CompressionType)t);
        return CompressionTypeToString(CompressionType.None);
    }

    /// <summary>Port of CompressionTypeToString (util/compression.cc).</summary>
    public static string CompressionTypeToString(CompressionType t) => t switch
    {
        CompressionType.None => "NoCompression",
        CompressionType.Snappy => "Snappy",
        CompressionType.Zlib => "Zlib",
        CompressionType.BZip2 => "BZip2",
        CompressionType.LZ4 => "LZ4",
        CompressionType.LZ4HC => "LZ4HC",
        CompressionType.Xpress => "Xpress",
        CompressionType.Zstd => "ZSTD",
        CompressionType.DisableCompressionOption => "DisableOption",
        _ => "Reserved" + ((byte)t).ToString("X2", System.Globalization.CultureInfo.InvariantCulture),
    };

    /// <summary>
    /// Port of CompressionOptionsToString (util/compression.cc) plus the "_type=" pseudo-option appended by
    /// Rep::Rep (block_based_table_builder.cc). parallel_threads is skipped upstream; auto_skip is not supported.
    /// </summary>
    private string BuildCompressionOptions()
    {
        var ci = System.Globalization.CultureInfo.InvariantCulture;
        CompressionOptions o = _tbo.CompressionOptions;
        var sb = new StringBuilder(256);
        sb.Append("window_bits=").Append(o.WindowBits.ToString(ci)).Append("; ");
        sb.Append("level=").Append(o.Level.ToString(ci)).Append("; ");
        sb.Append("strategy=").Append(o.Strategy.ToString(ci)).Append("; ");
        sb.Append("max_dict_bytes=").Append(o.MaxDictBytes.ToString(ci)).Append("; ");
        sb.Append("zstd_max_train_bytes=").Append(o.ZstdMaxTrainBytes.ToString(ci)).Append("; ");
        sb.Append("enabled=").Append(o.Enabled ? '1' : '0').Append("; ");
        sb.Append("max_dict_buffer_bytes=").Append(o.MaxDictBufferBytes.ToString(ci)).Append("; ");
        sb.Append("use_zstd_dict_trainer=").Append(o.UseZstdDictTrainer ? '1' : '0').Append("; ");
        sb.Append("max_compressed_bytes_per_kb=").Append(o.MaxCompressedBytesPerKb.ToString(ci)).Append("; ");
        sb.Append("checksum=").Append(o.Checksum ? '1' : '0').Append("; ");
        sb.Append("_type=").Append(((int)_tbo.CompressionType).ToString(ci)).Append("; ");
        return sb.ToString();
    }

    private void WriteFooter(BlockHandle metaindexHandle, BlockHandle indexHandle)
    {
        Span<byte> footer = _out.GetSpan(Footer.EncodedLength);
        Footer.Build(footer, _options.FormatVersion, _offset, _options.Checksum, metaindexHandle, indexHandle, _baseContextChecksum);
        _out.Advance(Footer.EncodedLength);
        _offset += Footer.EncodedLength;
    }

    /// <summary>Stops building without writing the tail (the partially written file should be deleted by the caller).</summary>
    public void Abandon()
    {
        if (_pc is not null) StopParallelCompression(abort: true);
        _closed = true;
        _out.Clear();
    }

    public void Dispose()
    {
        if (_pc is not null) StopParallelCompression(abort: true);
        _closed = true;
        _dataBlock.Dispose();
        _rangeDelBlock.Dispose();
        _indexBuilder.Dispose();
        _filterBuilder?.Dispose();
        _out.Dispose();
        _lastKey.Dispose();
        _compressed.Dispose();
        _dataBlockBuffers.Clear();
        _dict?.Dispose();

    }
}
