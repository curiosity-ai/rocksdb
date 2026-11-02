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
    private readonly ShortenedIndexBuilder _indexBuilder;
    private readonly IFilterBitsBuilder? _filterBuilder;
    private readonly PooledBufferWriter _out = new(OutputFlushThreshold + 64 * 1024);
    private readonly PooledBufferWriter _lastKey = new(64);
    private readonly PooledBufferWriter _compressed = new(4096);
    private readonly TableProperties _props = new();
    private readonly bool _useDeltaEncodingForIndexValues;
    private readonly uint _baseContextChecksum;
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
        _dataBlock = new BlockBuilder(options.BlockRestartInterval, options.UseDeltaEncoding, false, options.DataBlockIndexType, options.DataBlockHashTableUtilRatio);
        _rangeDelBlock = new BlockBuilder(1, true, false, DataBlockIndexType.BinarySearch, 0.75);
        _useDeltaEncodingForIndexValues = options.FormatVersion >= 4;
        _indexBuilder = new ShortenedIndexBuilder(_icmp, options.IndexBlockRestartInterval, options.FormatVersion, _useDeltaEncodingForIndexValues,
            options.IndexShortening, options.IndexType == IndexType.BinarySearchWithFirstKey);
        // Port of CreateFilterBlockBuilder: the policy picks the filter implementation from the full building context
        // (format version, level, flush); a built-in policy with &lt; 0.5 bits/key builds no filter at all.
        if (options.FilterPolicy is { } policy && policy is not BloomLikeFilterPolicy { CreatesFilters: false })
        {
            _filterBuilder = policy.CreateBuilder(new FilterBuildingContext(options.FormatVersion, tableBuilderOptions.LevelAtCreation, tableBuilderOptions.IsFlush));
        }
        _compressionLevel = tableBuilderOptions.CompressionOptions.Level;
        // Rep::Rep: max_compressed_bytes_per_kb = min(1023, compression_opts.max_compressed_bytes_per_kb).
        _maxCompressedBytesPerKb = Math.Min(1023, tableBuilderOptions.CompressionOptions.MaxCompressedBytesPerKb);
        _blockSizeDeviationLimit = (int)(((long)options.BlockSize * (100 - options.BlockSizeDeviation) + 99) / 100);

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

    public ulong NumEntries => _props.NumEntries;
    public bool IsEmpty => _props.NumEntries == 0;
    /// <summary>Size of the file generated so far (final size after <see cref="Finish"/>).</summary>
    public ulong FileSize => _offset;
    /// <summary>Properties of the table (complete after <see cref="Finish"/>).</summary>
    public TableProperties Properties => _props;
    /// <summary>The base context checksum written to the footer (format_version &gt;= 6).</summary>
    public uint BaseContextChecksum => _baseContextChecksum;

    /// <summary>Port of BlockBasedTableBuilder::Add. May synchronously append staged output to the file.</summary>
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

            if (_filterBuilder is not null && _options.WholeKeyFiltering) _filterBuilder.AddKey(DbFormat.ExtractUserKey(internalKey));

            _dataBlock.Add(internalKey, value);
            _lastKey.Clear();
            _lastKey.Write(internalKey);
            _indexBuilder.OnKeyAdded(internalKey);
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
    private bool ShouldFlush(int keyLength, int valueLength)
    {
        if (_dataBlock.IsEmpty) return false;
        int currSize = _dataBlock.CurrentSizeEstimate;
        if (currSize >= _options.BlockSize) return true;
        if (_blockSizeDeviationLimit == 0) return false;
        int after = _dataBlock.EstimateSizeAfterKV(keyLength, valueLength);
        return after > _options.BlockSize && currSize > _blockSizeDeviationLimit;
    }

    /// <summary>Port of BlockBasedTableBuilder::Flush + EmitBlock.</summary>
    private void Flush(ReadOnlySpan<byte> firstKeyInNextBlock, bool hasNext)
    {
        if (_dataBlock.IsEmpty) return;
        ReadOnlySpan<byte> raw = _dataBlock.Finish();
        _props.NumDataBlocks++;
        BlockHandle handle = WriteBlock(raw, isDataBlock: true);
        _indexBuilder.AddIndexEntry(_lastKey.WrittenSpan, firstKeyInNextBlock, hasNext, handle);
        _dataBlock.Reset();
    }

    /// <summary>Port of WriteBlock + CompressAndVerifyBlock (compression via the facade, with the ratio check).</summary>
    private BlockHandle WriteBlock(ReadOnlySpan<byte> raw, bool isDataBlock)
    {
        CompressionType type = CompressionType.None;
        bool compress = _tbo.CompressionType != CompressionType.None && (isDataBlock || _options.EnableIndexCompression) && raw.Length < CompressionSizeLimit;
        if (compress)
        {
            _compressed.Clear();
            long maxCompressed = ((long)_maxCompressedBytesPerKb * raw.Length) >> 10;
            if (BlockCompression.TryCompress(_tbo.CompressionType, raw, _options.FormatVersion, _compressionLevel, _compressed)
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
        _closed = true;
        _props.TailStartOffset = _offset;

        using var metaIndex = new MetaIndexBuilder();
        WriteFilterBlock(metaIndex);
        BlockHandle indexHandle = WriteIndexBlock(metaIndex);
        WriteRangeDelBlock(metaIndex);
        WritePropertiesBlock(metaIndex);
        BlockHandle metaindexHandle = WriteMaybeCompressedBlock(metaIndex.Finish(), CompressionType.None);
        WriteFooter(metaindexHandle, indexHandle);
    }

    private void WriteFilterBlock(MetaIndexBuilder metaIndex)
    {
        if (_filterBuilder is null || _filterBuilder.EstimatedEntries == 0) return;
        _props.NumFilterEntries += (ulong)_filterBuilder.EstimatedEntries;
        using var filter = new PooledBufferWriter(1024);
        _filterBuilder.Finish(filter);
        _props.FilterSize += (ulong)filter.WrittenCount;
        BlockHandle h = WriteMaybeCompressedBlock(filter.WrittenSpan, CompressionType.None);
        metaIndex.Add(BlockBasedTableConstants.FullFilterBlockPrefix + _options.FilterPolicy!.CompatibilityName, h);
    }

    private BlockHandle WriteIndexBlock(MetaIndexBuilder metaIndex)
    {
        ReadOnlySpan<byte> contents = _indexBuilder.Finish();
        BlockHandle h = WriteBlock(contents, isDataBlock: false);
        // index_size reports the logical (uncompressed) index size + trailer.
        _props.IndexSize = (ulong)_indexBuilder.IndexSize + BlockBasedTableConstants.BlockTrailerSize;
        if (!BlockBasedTableConstants.FormatVersionUsesIndexHandleInFooter(_options.FormatVersion))
            metaIndex.Add(BlockBasedTableConstants.IndexBlockName, h);
        return h;
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
        pb.Add(TablePropertiesNames.BlockBasedPrefixFiltering, TablePropertiesNames.PropFalse);
        pb.Add(TablePropertiesNames.BlockBasedDecoupledPartitionedFilters, TablePropertiesNames.PropTrue);
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
    }
}
