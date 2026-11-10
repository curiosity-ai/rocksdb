using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Table.Plain;

/// <summary>
/// Port of PlainTableBuilder (table/plain/plain_table_builder.{h,cc}) with PlainTableFactory::NewTableBuilder's
/// settings (num_probes 6). File layout:
/// <code>
///   data: entries (encoded key, varint32 value size, value) back to back
///   [meta block: bloom ("kBloomBlock")] and [meta block: index ("PlainTableIndexBlock")] - with store_index_in_file
///   [meta block: properties] [metaindex block] [footer: format_version 0, legacy plain magic]
/// </code>
/// Blocks have no trailer (no compression, no checksum). Range deletions are not supported (NotSupported), like upstream.
/// </summary>
public sealed class PlainTableBuilder : TableBuilder
{
    private const int OutputFlushThreshold = 256 * 1024;

    private readonly IWritableFile _file;
    private readonly TableBuilderOptions _tbo;
    private readonly PooledBufferWriter _out = new(OutputFlushThreshold + 64 * 1024, TableBuilderBuffers.Bytes);
    private readonly TableProperties _props = new();
    private readonly PlainTableKeyEncoder _encoder;
    private readonly PlainTableIndexBuilder? _indexBuilder;
    private readonly PlainTableBloom _bloom = new(6);
    private readonly uint _bloomBitsPerKey;
    private readonly bool _storeIndexInFile;
    private readonly SliceTransform? _prefixExtractor;
    private readonly List<uint> _keysOrPrefixesHashes = new();
    private readonly TablePropertiesCollectors? _collectors;
    private ulong _offset;
    private bool _closed;

    public PlainTableBuilder(IWritableFile file, TableBuilderOptions tableBuilderOptions, PlainTableOptions tableOptions)
    {
        ArgumentNullException.ThrowIfNull(file);
        ArgumentNullException.ThrowIfNull(tableBuilderOptions);
        ArgumentNullException.ThrowIfNull(tableOptions);
        _file = file;
        _tbo = tableBuilderOptions;
        _prefixExtractor = tableBuilderOptions.PrefixExtractor;
        _bloomBitsPerKey = (uint)tableOptions.BloomBitsPerKey;
        _storeIndexInFile = tableOptions.StoreIndexInFile;
        _encoder = new PlainTableKeyEncoder(tableOptions.EncodingType, tableOptions.UserKeyLength, _prefixExtractor, tableOptions.IndexSparseness);
        // Build index block and save it in the file if hash_table_ratio > 0
        if (_storeIndexInFile)
        {
            if (!(tableOptions.HashTableRatio > 0 || _prefixExtractor is null))
                ThrowHelper.InvalidArgument("store_index_in_file requires hash_table_ratio > 0 or total order mode");
            _indexBuilder = new PlainTableIndexBuilder(_prefixExtractor is not null, tableOptions.IndexSparseness, tableOptions.HashTableRatio);
            _props.UserCollectedProperties[PlainTablePropertyNames.BloomVersion] = "1"u8.ToArray(); // For future use
        }
        _props.FixedKeyLen = tableOptions.UserKeyLength;
        // for plain table, we put all the data in a big chuck.
        _props.NumDataBlocks = 1;
        // Fill it later if store_index_in_file_ == true
        _props.IndexSize = 0;
        _props.FilterSize = 0;
        // To support roll-back to previous version, now still use version 0 for plain encoding.
        // Upstream tests the configured encoding_type here, not encoder_.GetEncodingType(): kPrefix without a prefix
        // extractor writes kPlain keys (and the kPlain encoding-type property) but still records format_version 1.
        _props.FormatVersion = tableOptions.EncodingType == PlainTableEncodingType.Plain ? 0UL : 1UL;
        _props.ColumnFamilyId = tableBuilderOptions.ColumnFamilyId;
        _props.ColumnFamilyName = tableBuilderOptions.ColumnFamilyName;
        _props.DbId = tableBuilderOptions.DbId;
        _props.DbSessionId = tableBuilderOptions.DbSessionId;
        _props.DbHostId = tableBuilderOptions.DbHostId;
        _props.OriginalFileNumber = tableBuilderOptions.FileNumber;
        _props.PrefixExtractorName = tableBuilderOptions.PrefixExtractorName;
        byte[] enc = new byte[4];
        Coding.EncodeFixed32(enc, (uint)_encoder.EncodingType);
        _props.UserCollectedProperties[PlainTablePropertyNames.EncodingType] = enc;
        _collectors = TablePropertiesCollectors.Create(tableBuilderOptions.TablePropertiesCollectorFactories,
            new TablePropertiesCollectorContext((uint)Math.Min(tableBuilderOptions.ColumnFamilyId, uint.MaxValue), tableBuilderOptions.LevelAtCreation,
                tableBuilderOptions.NumLevels, tableBuilderOptions.LastLevelInclusiveMaxSeqnoThreshold));
        _offset = 0;
    }

    public override ulong NumEntries => _props.NumEntries;
    public override ulong FileSize => _offset;
    public override TableProperties Properties => _props;
    public override bool NeedCompact => _collectors is not null && _collectors.NeedCompact;

    /// <summary>Port of PlainTableBuilder::Add.</summary>
    public override void Add(ReadOnlySpan<byte> internalKey, ReadOnlySpan<byte> value)
    {
        if (_closed) throw new InvalidOperationException("Table builder already finished.");
        if (!DbFormat.TryParseInternalKey(internalKey, out ParsedInternalKey ikey)) ThrowHelper.Corruption("Corrupted key: invalid internal key");
        if (ikey.Type == EntryType.RangeDeletion) ThrowHelper.NotSupported("Range deletion unsupported");
        ReadOnlySpan<byte> userKey = internalKey[..ikey.UserKeyLength];

        // Store key hash
        if (_storeIndexInFile)
        {
            _keysOrPrefixesHashes.Add(_prefixExtractor is null
                ? Hash.GetSliceHash(userKey)
                : Hash.GetSliceHash(PlainTableReader.Transform(_prefixExtractor, userKey)));
        }

        // Write value
        if (_offset > uint.MaxValue) ThrowHelper.NotSupported("PlainTable file size exceeds 4GB");
        uint prevOffset = (uint)_offset;
        // temp buffer for metadata bytes between key and value.
        Span<byte> metaBytes = stackalloc byte[6];
        int metaBytesSize = 0;
        // Write out the key
        _encoder.AppendKey(internalKey, _out, ref _offset, metaBytes, ref metaBytesSize);
        _indexBuilder?.AddKeyPrefix(GetPrefixFromUserKey(userKey), prevOffset);

        // Write value length
        uint valueSize = (uint)value.Length;
        metaBytesSize += Coding.EncodeVarint32(metaBytes[metaBytesSize..], valueSize);
        _out.Write(metaBytes[..metaBytesSize]);
        // Write value
        _out.Write(value);
        _offset += valueSize + (uint)metaBytesSize;

        _props.NumEntries++;
        _props.RawKeySize += (ulong)internalKey.Length;
        _props.RawValueSize += (ulong)value.Length;
        if (ikey.Type is EntryType.Deletion or EntryType.SingleDeletion) _props.NumDeletions++;
        else if (ikey.Type == EntryType.Merge) _props.NumMergeOperands++;

        // notify property collectors
        _collectors?.Add(internalKey, value, _offset);
        if (_out.WrittenCount >= OutputFlushThreshold) DrainOutput();
    }

    private ReadOnlySpan<byte> GetPrefixFromUserKey(ReadOnlySpan<byte> userKey) =>
        _prefixExtractor is null ? default : PlainTableReader.Transform(_prefixExtractor, userKey);

    private void DrainOutput()
    {
        if (_out.WrittenCount == 0) return;
        _file.Append(_out.WrittenSpan);
        _out.Clear();
    }

    private BlockHandle WriteBlock(ReadOnlySpan<byte> contents)
    {
        var h = new BlockHandle(_offset, (ulong)contents.Length);
        _out.Write(contents);
        _offset += (ulong)contents.Length;
        return h;
    }

    /// <summary>Port of PlainTableBuilder::Finish.</summary>
    public override void Finish()
    {
        if (_closed) throw new InvalidOperationException("Table builder already finished.");
        _closed = true;
        _props.DataSize = _offset;

        //  Write the following blocks
        //  1. [meta block: bloom] - optional
        //  2. [meta block: index] - optional
        //  3. [meta block: properties]
        //  4. [metaindex block]
        //  5. [footer]
        using var metaIndex = new MetaIndexBuilder();
        if (_storeIndexInFile && _props.NumEntries > 0)
        {
            if (_props.NumEntries > uint.MaxValue) ThrowHelper.NotSupported("too many entries for a plain table index");
            if (_bloomBitsPerKey > 0)
            {
                _bloom.SetTotalBits(unchecked((uint)_props.NumEntries * _bloomBitsPerKey), _tbo.BloomLocality);
                byte[] nb = new byte[Coding.MaxVarint32Length];
                int n = Coding.EncodeVarint32(nb, _bloom.NumBlocks);
                _props.UserCollectedProperties[PlainTablePropertyNames.NumBloomBlocks] = nb[..n];
                foreach (uint h in _keysOrPrefixesHashes) _bloom.AddHash(h);
                ReadOnlySpan<byte> bloomData = _bloom.RawData;
                _props.FilterSize = (ulong)bloomData.Length;
                metaIndex.Add(PlainTablePropertyNames.BloomBlock, WriteBlock(bloomData));
            }
            byte[] index = _indexBuilder!.Finish();
            _props.IndexSize = (ulong)index.Length;
            metaIndex.Add(PlainTableIndex.PlainTableIndexBlock, WriteBlock(index));
        }

        // TODO (upstream): storing seqno to time mapping is not yet supported for plain table.
        _props.SequenceNumberTimeMapping = [];
        using (var pb = new PropertyBlockBuilder())
        {
            // -- Add basic properties
            _props.AddTo(pb);
            // -- Add existing user collected properties
            foreach (KeyValuePair<string, byte[]> kv in _props.UserCollectedProperties) pb.Add(kv.Key, kv.Value);
            // -- Add more user collected properties (NotifyCollectTableCollectorsOnFinish)
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
            if (_tbo.ExternalSstFile)
            {
                // SstFileWriterPropertiesCollector (an internal collector of SstFileWriter).
                byte[] version = new byte[4];
                Coding.EncodeFixed32(version, 2);
                pb.Add(TablePropertiesNames.ExternalSstFileVersion, version);
                pb.Add(TablePropertiesNames.ExternalSstFileGlobalSeqno, new byte[8]);
            }
            // -- Write property block
            metaIndex.Add(BlockBasedTableConstants.PropertiesBlockName, WriteBlock(pb.Finish()));
        }
        // -- write metaindex block
        BlockHandle metaindexHandle = WriteBlock(metaIndex.Finish());
        // Write Footer: no need to write out new footer if we're using default checksum
        Span<byte> footer = stackalloc byte[Footer.EncodedLength];
        int len = Footer.BuildForMagic(footer, TableMagicNumbers.PlainTable, 0, ChecksumType.NoChecksum, metaindexHandle, BlockHandle.Null);
        _out.Write(footer[..len]);
        _offset += (ulong)len;
        DrainOutput();
        _file.Flush();
    }

    public override void Abandon()
    {
        _closed = true;
        _out.Clear();
    }

    public override void Dispose()
    {
        _closed = true;
        _out.Dispose();
    }
}
