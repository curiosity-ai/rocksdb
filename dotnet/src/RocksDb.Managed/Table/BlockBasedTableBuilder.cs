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
/// Builds a block-based table (SST file). Port of BlockBasedTableBuilder (table/block_based/block_based_table_builder.cc)
/// — the single-threaded, unbuffered path (no parallel compression, no dictionary compression, no partitioned
/// index/filters on the write side).
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
public sealed class BlockBasedTableBuilder : IDisposable
{
    /// <summary>Staged output is handed to the file once it exceeds this many bytes.</summary>
    private const int OutputFlushThreshold = 256 * 1024;
    /// <summary>kCompressionSizeLimit: blocks at least this large are never compressed.</summary>
    private const int CompressionSizeLimit = int.MaxValue;

    private readonly IWritableFile _file;
    private readonly TableFormatOptions _options;
    private readonly TableBuilderOptions _tbo;
    private readonly Comparator _ucmp;
    private readonly InternalKeyComparator _icmp;
    private readonly BlockBuilder _dataBlock;
    private readonly BlockBuilder _rangeDelBlock;
    private readonly IndexBuilder _indexBuilder;
    private readonly FilterBlockBuilder? _filterBuilder;
    private readonly PooledBufferWriter _out = new(OutputFlushThreshold + 64 * 1024);
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
        _icmp = _ucmp.Kind == ComparatorKind.Bytewise ? InternalKeyComparator.Bytewise : new InternalKeyComparator(_ucmp);
        _offset = (ulong)file.Size;
        bool useCommonPrefix = UseCommonPrefix(options, _ucmp);
        // The hash index needs keys whose bytes determine equality (CanKeysWithDifferentByteContentsBeEqual).
        _dataBlock = new BlockBuilder(options.BlockRestartInterval, options.UseDeltaEncoding, false,
            _ucmp.CanKeysWithDifferentByteContentsBeEqual ? DataBlockIndexType.BinarySearch : options.DataBlockIndexType,
            options.DataBlockHashTableUtilRatio, useCommonPrefix, isUserKey: false);
        _rangeDelBlock = new BlockBuilder(1, true, false, DataBlockIndexType.BinarySearch, 0.75);
        _useDeltaEncodingForIndexValues = options.FormatVersion >= 4;
        PartitionedIndexBuilder? partitionedIndex = null;
        if (options.IndexType == IndexType.TwoLevelIndexSearch)
        {
            // PartitionedIndexBuilder::CreateIndexBuilder (use_common_prefix applies to all index tiers).
            partitionedIndex = new PartitionedIndexBuilder(_icmp, options.IndexBlockRestartInterval, options.FormatVersion, _useDeltaEncodingForIndexValues,
                options.IndexShortening, options.MetadataBlockSize, options.BlockSizeDeviation, useCommonPrefix, useCommonPrefix);
            _indexBuilder = partitionedIndex;
        }
        else
        {
            _indexBuilder = new ShortenedIndexBuilder(_icmp, options.IndexBlockRestartInterval, options.FormatVersion, _useDeltaEncodingForIndexValues,
                options.IndexShortening, options.IndexType == IndexType.BinarySearchWithFirstKey, useCommonPrefix);
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
                    useCommonPrefix);
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
        _props.KeyLargestSeqno = 0;
        _props.KeySmallestSeqno = ulong.MaxValue;

        if (BlockBasedTableConstants.FormatVersionUsesContextChecksum(options.FormatVersion))
        {
            // Must be non-zero and semi-random.
            uint v;
            do { v = (uint)RandomNumberGenerator.GetInt32(int.MinValue, int.MaxValue); } while (v == 0);
            _baseContextChecksum = v;
        }
    }

    /// <summary>
    /// Port of UseCommonPrefixForDataBlock / UseCommonPrefixForIndex (block_based_table_builder.cc): format_version
    /// &gt;= 8, delta encoding, no user-defined timestamps; kIfFastSeek only for the (reverse-)bytewise comparators.
    /// (super_block_alignment_size is not supported, so the index gate is the same as the data block gate.)
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

        if (type == EntryType.RangeDeletion)
        {
            _rangeDelBlock.Add(internalKey, value);
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
            if (!_buffered) _filterBuilder?.Add(DbFormat.ExtractUserKey(internalKey));

            _dataBlock.Add(internalKey, value);
            _lastKey.Clear();
            _lastKey.Write(internalKey);
            if (!_buffered) _indexBuilder.OnKeyAdded(internalKey);
            if (type is EntryType.Deletion or EntryType.SingleDeletion or EntryType.DeletionWithTimestamp) _props.NumDeletions++;
            else if (type == EntryType.Merge) _props.NumMergeOperands++;
        }
        _props.NumEntries++;
        _props.RawKeySize += (ulong)internalKey.Length;
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
        return after > _options.BlockSize && currSize > _blockSizeDeviationLimit;
    }

    /// <summary>Port of BlockBasedTableBuilder::Flush + EmitBlock (buffering the block while sampling for a dictionary).</summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private void Flush(ReadOnlySpan<byte> firstKeyInNextBlock, bool hasNext)
    {
        if (_dataBlock.IsEmpty) return;
        ReadOnlySpan<byte> raw = _dataBlock.Finish();
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
        EmitBlock(raw, _lastKey.WrittenSpan, firstKeyInNextBlock, hasNext);
        _dataBlock.Reset();
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private void EmitBlock(ReadOnlySpan<byte> raw, ReadOnlySpan<byte> lastKey, ReadOnlySpan<byte> firstKeyInNextBlock, bool hasNext)
    {
        BlockHandle handle = WriteBlock(raw, isDataBlock: true);
        _indexBuilder.AddIndexEntry(lastKey, firstKeyInNextBlock, hasNext, handle);
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
                it.InitializeData(block, _icmp, BlockIter.DisableGlobalSequenceNumber);
                for (it.SeekToFirst(); it.Valid; it.Next())
                {
                    ReadOnlySpan<byte> key = it.Key;
                    _filterBuilder?.Add(DbFormat.ExtractUserKey(key));
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
                    next.InitializeData(nb, _icmp, BlockIter.DisableGlobalSequenceNumber);
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
            EmitBlock(data, lastKey.WrittenSpan, nextFirstKey.WrittenSpan, blockHasNext);
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
    private BlockHandle WriteBlock(ReadOnlySpan<byte> raw, bool isDataBlock)
    {
        CompressionType type = CompressionType.None;
        bool compress = _tbo.CompressionType != CompressionType.None && (isDataBlock || _options.EnableIndexCompression) && raw.Length < CompressionSizeLimit;
        if (compress)
        {
            _compressed.Clear();
            long maxCompressed = ((long)_maxCompressedBytesPerKb * raw.Length) >> 10;
            if (BlockCompression.TryCompress(_tbo.CompressionType, raw, _options.FormatVersion, _compressionLevel, _maxCompressedBytesPerKb, _compressed,
                    isDataBlock ? _dict : null)
                && _compressed.WrittenCount <= maxCompressed)
            {
                type = _tbo.CompressionType;
            }
            else if (isDataBlock)
            {
                _props.NumDataBlocksCompressionRejected++;
            }
        }
        BlockHandle h = type == CompressionType.None ? WriteMaybeCompressedBlock(raw, type) : WriteMaybeCompressedBlock(_compressed.WrittenSpan, type);
        if (isDataBlock)
        {
            _props.DataSize = _offset;
            _props.UncompressedDataSize += (ulong)raw.Length;
        }
        return h;
    }

    /// <summary>Port of WriteMaybeCompressedBlockImpl: payload + trailer (type byte, checksum with context).</summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private BlockHandle WriteMaybeCompressedBlock(ReadOnlySpan<byte> contents, CompressionType type)
    {
        var handle = new BlockHandle(_offset, (ulong)contents.Length);
        _compressionTypesUsed |= 1u << ((int)type & 31);
        _out.Write(contents);
        Span<byte> trailer = _out.GetSpan(BlockBasedTableConstants.BlockTrailerSize);
        trailer[0] = (byte)type;
        uint checksum = BlockChecksum.ComputeTrailerChecksum(_options.Checksum, _baseContextChecksum, contents, (byte)type, _offset);
        Coding.EncodeFixed32(trailer[1..], checksum);
        _out.Advance(BlockBasedTableConstants.BlockTrailerSize);
        _offset += (ulong)contents.Length + BlockBasedTableConstants.BlockTrailerSize;
        return handle;
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
            _filterBuilder.PrevKeyBeforeFinish(DbFormat.ExtractUserKey(_lastKey.WrittenSpan));
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
        do
        {
            done = _indexBuilder.Finish(h, out ReadOnlySpan<byte> contents);
            h = WriteBlock(contents, isDataBlock: false);
        }
        while (!done);
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

    private void WritePropertiesBlock(MetaIndexBuilder metaIndex)
    {
        // WritePropertiesBlock: the policy's Name(), even when it built no filter.
        _props.FilterPolicyName = _options.FilterPolicy?.Name ?? "";
        _props.ComparatorName = _ucmp.Name;
        _props.MergeOperatorName = _tbo.MergeOperatorName;
        _props.PrefixExtractorName = _tbo.PrefixExtractorName;
        _props.PropertyCollectorsNames = "[]";
        _props.IndexKeyIsUserKey = _indexBuilder.SeparatorIsKeyPlusSeq ? 0UL : 1UL;
        _props.IndexValueIsDeltaEncoded = _useDeltaEncodingForIndexValues ? 1UL : 0UL;
        _props.CompressionName = BuildCompressionName();
        _props.CompressionOptions = BuildCompressionOptions();

        using var pb = new PropertyBlockBuilder();
        _props.AddTo(pb);
        // Internal collectors (BlockBasedTablePropertiesCollector and, for external files, SstFileWriterPropertiesCollector).
        Span<byte> idx = stackalloc byte[4];
        Coding.EncodeFixed32(idx, (uint)_options.IndexType);
        pb.Add(TablePropertiesNames.BlockBasedIndexType, idx.ToArray());
        pb.Add(TablePropertiesNames.BlockBasedWholeKeyFiltering, _options.WholeKeyFiltering ? TablePropertiesNames.PropTrue : TablePropertiesNames.PropFalse);
        pb.Add(TablePropertiesNames.BlockBasedPrefixFiltering, _prefixExtractor is not null ? TablePropertiesNames.PropTrue : TablePropertiesNames.PropFalse);
        if (_options.DecouplePartitionedFilters) pb.Add(TablePropertiesNames.BlockBasedDecoupledPartitionedFilters, TablePropertiesNames.PropTrue);

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
        _closed = true;
        _out.Clear();
    }

    public void Dispose()
    {
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
