using System.Text;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Table;

/// <summary>Property names. Port of TablePropertiesNames (table/table_properties.cc) and related name constants.</summary>
public static class TablePropertiesNames
{
    public const string DbId = "rocksdb.creating.db.identity";
    public const string DbSessionId = "rocksdb.creating.session.identity";
    public const string DbHostId = "rocksdb.creating.host.identity";
    public const string OriginalFileNumber = "rocksdb.original.file.number";
    public const string DataSize = "rocksdb.data.size";
    public const string IndexSize = "rocksdb.index.size";
    public const string IndexPartitions = "rocksdb.index.partitions";
    public const string TopLevelIndexSize = "rocksdb.top-level.index.size";
    public const string IndexKeyIsUserKey = "rocksdb.index.key.is.user.key";
    public const string IndexValueIsDeltaEncoded = "rocksdb.index.value.is.delta.encoded";
    public const string UdiIsPrimaryIndex = "rocksdb.udi.is.primary.index";
    public const string StandardIndexIsStub = "rocksdb.standard.index.is.stub";
    public const string FilterSize = "rocksdb.filter.size";
    public const string RawKeySize = "rocksdb.raw.key.size";
    public const string RawValueSize = "rocksdb.raw.value.size";
    public const string NumDataBlocks = "rocksdb.num.data.blocks";
    public const string NumDataBlocksCompressionRejected = "rocksdb.num.data.blocks.compression.rejected";
    public const string NumDataBlocksCompressionBypassed = "rocksdb.num.data.blocks.compression.bypassed";
    public const string NumUniformBlocks = "rocksdb.num.uniform.blocks";
    public const string NumEntries = "rocksdb.num.entries";
    public const string NumFilterEntries = "rocksdb.num.filter_entries";
    public const string DeletedKeys = "rocksdb.deleted.keys";
    public const string MergeOperands = "rocksdb.merge.operands";
    public const string NumRangeDeletions = "rocksdb.num.range-deletions";
    public const string FilterPolicy = "rocksdb.filter.policy";
    public const string FormatVersion = "rocksdb.format.version";
    public const string FixedKeyLen = "rocksdb.fixed.key.length";
    public const string ColumnFamilyId = "rocksdb.column.family.id";
    public const string ColumnFamilyName = "rocksdb.column.family.name";
    public const string Comparator = "rocksdb.comparator";
    public const string MergeOperator = "rocksdb.merge.operator";
    public const string PrefixExtractorName = "rocksdb.prefix.extractor.name";
    public const string PropertyCollectors = "rocksdb.property.collectors";
    /// <summary>TimestampTablePropertiesCollector: smallest / largest user-defined timestamp of the table.</summary>
    public const string TimestampMin = "rocksdb.timestamp_min";
    public const string TimestampMax = "rocksdb.timestamp_max";
    public const string Compression = "rocksdb.compression";
    public const string CompressionOptions = "rocksdb.compression_options";
    /// <summary>kOldestAncestorTime (also kCreationTime).</summary>
    public const string CreationTime = "rocksdb.creation.time";
    public const string OldestKeyTime = "rocksdb.oldest.key.time";
    public const string NewestKeyTime = "rocksdb.newest.key.time";
    public const string FileCreationTime = "rocksdb.file.creation.time";
    public const string SlowCompressionEstimatedDataSize = "rocksdb.sample_for_compression.slow.data.size";
    public const string FastCompressionEstimatedDataSize = "rocksdb.sample_for_compression.fast.data.size";
    public const string SequenceNumberTimeMapping = "rocksdb.seqno.time.map";
    public const string TailStartOffset = "rocksdb.tail.start.offset";
    public const string UserDefinedTimestampsPersisted = "rocksdb.user.defined.timestamps.persisted";
    public const string KeyLargestSeqno = "rocksdb.key.largest.seqno";
    public const string KeySmallestSeqno = "rocksdb.key.smallest.seqno";
    public const string DataBlockRestartInterval = "rocksdb.data.block.restart.interval";
    public const string IndexBlockRestartInterval = "rocksdb.index.block.restart.interval";
    public const string SeparateKeyValueInDataBlock = "rocksdb.separate.key.value.in.data.block";
    public const string UncompressedDataSize = "rocksdb.uncompressed.data.size";
    public const string LsmInfoAtCreation = "rocksdb.lsm.info.at.creation";

    // BlockBasedTablePropertyNames (block_based_table_factory.cc)
    public const string BlockBasedIndexType = "rocksdb.block.based.table.index.type";
    /// <summary>
    /// kEmbeddedBlobSstStatsPropertyName (table/embedded_blob_sst.h): varint64 blob_count + varint64 payload_bytes of
    /// the same-file blob records; its presence marks an embedded-blob SST (the counters are diagnostic only).
    /// </summary>
    public const string EmbeddedBlobSstStats = "rocksdb.embedded.blob.stats";
    public const string BlockBasedWholeKeyFiltering = "rocksdb.block.based.table.whole.key.filtering";
    public const string BlockBasedPrefixFiltering = "rocksdb.block.based.table.prefix.filtering";
    public const string BlockBasedDecoupledPartitionedFilters = "rocksdb.block.based.table.decoupled.partitioned.filters";

    // ExternalSstFilePropertyNames (table/sst_file_writer.cc)
    public const string ExternalSstFileVersion = "rocksdb.external_sst_file.version";
    public const string ExternalSstFileGlobalSeqno = "rocksdb.external_sst_file.global_seqno";

    /// <summary>kPropTrue / kPropFalse.</summary>
    public const string PropTrue = "1";
    public const string PropFalse = "0";
}

/// <summary>
/// Table properties stored in the "rocksdb.properties" meta block. Port of TableProperties
/// (include/rocksdb/table_properties.h). Unknown properties go to <see cref="UserCollectedProperties"/> as raw bytes.
/// </summary>
public sealed class TableProperties
{
    /// <summary>Default for unknown sequence numbers / times.</summary>
    public const ulong Unknown = ulong.MaxValue;
    /// <summary>TablePropertiesCollectorFactory::Context::kUnknownColumnFamily.</summary>
    public const ulong UnknownColumnFamily = int.MaxValue;

    public ulong OriginalFileNumber { get; set; }
    public ulong DataSize { get; set; }
    public ulong UncompressedDataSize { get; set; }
    public ulong IndexSize { get; set; }
    public ulong IndexPartitions { get; set; }
    public ulong TopLevelIndexSize { get; set; }
    public ulong IndexKeyIsUserKey { get; set; }
    public ulong IndexValueIsDeltaEncoded { get; set; }
    public ulong UdiIsPrimaryIndex { get; set; }
    public ulong StandardIndexIsStub { get; set; }
    public ulong FilterSize { get; set; }
    public ulong RawKeySize { get; set; }
    public ulong RawValueSize { get; set; }
    public ulong NumDataBlocks { get; set; }
    public ulong NumDataBlocksCompressionRejected { get; set; }
    public ulong NumDataBlocksCompressionBypassed { get; set; }
    public ulong NumUniformBlocks { get; set; }
    public ulong NumEntries { get; set; }
    public ulong NumFilterEntries { get; set; }
    public ulong NumDeletions { get; set; }
    public ulong NumMergeOperands { get; set; }
    public ulong NumRangeDeletions { get; set; }
    public ulong FormatVersion { get; set; }
    public ulong FixedKeyLen { get; set; }
    public ulong ColumnFamilyId { get; set; } = UnknownColumnFamily;
    public ulong CreationTime { get; set; }
    public ulong OldestKeyTime { get; set; }
    public ulong NewestKeyTime { get; set; }
    public ulong FileCreationTime { get; set; }
    public ulong SlowCompressionEstimatedDataSize { get; set; }
    public ulong FastCompressionEstimatedDataSize { get; set; }
    public ulong TailStartOffset { get; set; }
    public ulong UserDefinedTimestampsPersisted { get; set; } = 1;
    public ulong KeyLargestSeqno { get; set; } = Unknown;
    public ulong KeySmallestSeqno { get; set; } = Unknown;
    public ulong DataBlockRestartInterval { get; set; }
    public ulong IndexBlockRestartInterval { get; set; }
    public ulong SeparateKeyValueInDataBlock { get; set; }
    public ulong LsmInfoAtCreation { get; set; }

    public string DbId { get; set; } = "";
    public string DbSessionId { get; set; } = "";
    public string DbHostId { get; set; } = "";
    public string FilterPolicyName { get; set; } = "";
    public string ColumnFamilyName { get; set; } = "";
    public string ComparatorName { get; set; } = "";
    public string MergeOperatorName { get; set; } = "";
    public string PrefixExtractorName { get; set; } = "";
    public string PropertyCollectorsNames { get; set; } = "";
    public string CompressionName { get; set; } = "";
    public string CompressionOptions { get; set; } = "";
    /// <summary>Raw (encoded) seqno-to-time mapping.</summary>
    public byte[] SequenceNumberTimeMapping { get; set; } = [];

    /// <summary>User-collected (and unknown) properties: name → raw value bytes.</summary>
    public Dictionary<string, byte[]> UserCollectedProperties { get; } = new(StringComparer.Ordinal);

    /// <summary>Offset of the external-SST global seqno value inside the file, or 0 if absent (set by the reader).</summary>
    public ulong ExternalSstFileGlobalSeqnoOffset { get; set; }

    /// <summary>Port of PropertyBlockBuilder::AddTableProperty (table/meta_blocks.cc).</summary>
    internal void AddTo(PropertyBlockBuilder b)
    {
        b.Add(TablePropertiesNames.OriginalFileNumber, OriginalFileNumber);
        b.Add(TablePropertiesNames.RawKeySize, RawKeySize);
        b.Add(TablePropertiesNames.RawValueSize, RawValueSize);
        b.Add(TablePropertiesNames.DataSize, DataSize);
        b.Add(TablePropertiesNames.UncompressedDataSize, UncompressedDataSize);
        b.Add(TablePropertiesNames.IndexSize, IndexSize);
        if (IndexPartitions != 0)
        {
            b.Add(TablePropertiesNames.IndexPartitions, IndexPartitions);
            b.Add(TablePropertiesNames.TopLevelIndexSize, TopLevelIndexSize);
        }
        b.Add(TablePropertiesNames.IndexKeyIsUserKey, IndexKeyIsUserKey);
        b.Add(TablePropertiesNames.IndexValueIsDeltaEncoded, IndexValueIsDeltaEncoded);
        if (UdiIsPrimaryIndex != 0) b.Add(TablePropertiesNames.UdiIsPrimaryIndex, UdiIsPrimaryIndex);
        if (StandardIndexIsStub != 0) b.Add(TablePropertiesNames.StandardIndexIsStub, StandardIndexIsStub);
        b.Add(TablePropertiesNames.NumEntries, NumEntries);
        b.Add(TablePropertiesNames.NumFilterEntries, NumFilterEntries);
        b.Add(TablePropertiesNames.DeletedKeys, NumDeletions);
        b.Add(TablePropertiesNames.MergeOperands, NumMergeOperands);
        b.Add(TablePropertiesNames.NumRangeDeletions, NumRangeDeletions);
        b.Add(TablePropertiesNames.NumDataBlocks, NumDataBlocks);
        if (NumDataBlocksCompressionRejected > 0) b.Add(TablePropertiesNames.NumDataBlocksCompressionRejected, NumDataBlocksCompressionRejected);
        if (NumDataBlocksCompressionBypassed > 0) b.Add(TablePropertiesNames.NumDataBlocksCompressionBypassed, NumDataBlocksCompressionBypassed);
        b.Add(TablePropertiesNames.NumUniformBlocks, NumUniformBlocks);
        b.Add(TablePropertiesNames.FilterSize, FilterSize);
        b.Add(TablePropertiesNames.FormatVersion, FormatVersion);
        b.Add(TablePropertiesNames.FixedKeyLen, FixedKeyLen);
        b.Add(TablePropertiesNames.ColumnFamilyId, ColumnFamilyId);
        b.Add(TablePropertiesNames.CreationTime, CreationTime);
        b.Add(TablePropertiesNames.OldestKeyTime, OldestKeyTime);
        b.Add(TablePropertiesNames.NewestKeyTime, NewestKeyTime);
        if (FileCreationTime > 0) b.Add(TablePropertiesNames.FileCreationTime, FileCreationTime);
        if (SlowCompressionEstimatedDataSize > 0) b.Add(TablePropertiesNames.SlowCompressionEstimatedDataSize, SlowCompressionEstimatedDataSize);
        if (FastCompressionEstimatedDataSize > 0) b.Add(TablePropertiesNames.FastCompressionEstimatedDataSize, FastCompressionEstimatedDataSize);
        b.Add(TablePropertiesNames.TailStartOffset, TailStartOffset);
        if (UserDefinedTimestampsPersisted == 0) b.Add(TablePropertiesNames.UserDefinedTimestampsPersisted, UserDefinedTimestampsPersisted);
        if (DbId.Length > 0) b.Add(TablePropertiesNames.DbId, DbId);
        if (DbSessionId.Length > 0) b.Add(TablePropertiesNames.DbSessionId, DbSessionId);
        if (DbHostId.Length > 0) b.Add(TablePropertiesNames.DbHostId, DbHostId);
        if (FilterPolicyName.Length > 0) b.Add(TablePropertiesNames.FilterPolicy, FilterPolicyName);
        if (ComparatorName.Length > 0) b.Add(TablePropertiesNames.Comparator, ComparatorName);
        if (MergeOperatorName.Length > 0) b.Add(TablePropertiesNames.MergeOperator, MergeOperatorName);
        if (PrefixExtractorName.Length > 0) b.Add(TablePropertiesNames.PrefixExtractorName, PrefixExtractorName);
        if (PropertyCollectorsNames.Length > 0) b.Add(TablePropertiesNames.PropertyCollectors, PropertyCollectorsNames);
        if (ColumnFamilyName.Length > 0) b.Add(TablePropertiesNames.ColumnFamilyName, ColumnFamilyName);
        if (CompressionName.Length > 0) b.Add(TablePropertiesNames.Compression, CompressionName);
        if (CompressionOptions.Length > 0) b.Add(TablePropertiesNames.CompressionOptions, CompressionOptions);
        if (SequenceNumberTimeMapping.Length > 0) b.Add(TablePropertiesNames.SequenceNumberTimeMapping, SequenceNumberTimeMapping);
        if (KeyLargestSeqno != Unknown) b.Add(TablePropertiesNames.KeyLargestSeqno, KeyLargestSeqno);
        if (KeySmallestSeqno != Unknown) b.Add(TablePropertiesNames.KeySmallestSeqno, KeySmallestSeqno);
        if (DataBlockRestartInterval > 0) b.Add(TablePropertiesNames.DataBlockRestartInterval, DataBlockRestartInterval);
        if (IndexBlockRestartInterval > 0) b.Add(TablePropertiesNames.IndexBlockRestartInterval, IndexBlockRestartInterval);
        if (SeparateKeyValueInDataBlock > 0) b.Add(TablePropertiesNames.SeparateKeyValueInDataBlock, SeparateKeyValueInDataBlock);
        if (LsmInfoAtCreation != 0) b.Add(TablePropertiesNames.LsmInfoAtCreation, LsmInfoAtCreation);
    }

    /// <summary>Port of ParsePropertiesBlock (table/meta_blocks.cc). <paramref name="blockOffset"/> is the file offset of the block.</summary>
    internal static TableProperties Parse(Block block, ulong blockOffset)
    {
        var props = new TableProperties();
        using var it = new BlockIter();
        it.InitializeMeta(block);
        byte[]? lastKey = null;
        for (it.SeekToFirst(); it.Valid; it.Next())
        {
            ReadOnlySpan<byte> keyBytes = it.Key;
            if (lastKey is not null && keyBytes.SequenceCompareTo(lastKey) <= 0) ThrowHelper.Corruption("properties unsorted");
            lastKey = keyBytes.ToArray();
            string key = Encoding.UTF8.GetString(keyBytes);
            ReadOnlySpan<byte> raw = it.Value;
            if (key == TablePropertiesNames.ExternalSstFileGlobalSeqno) props.ExternalSstFileGlobalSeqnoOffset = blockOffset + (ulong)it.ValueOffsetInBlock;
            if (TrySetUInt64(props, key, raw, out bool known))
            {
                if (key == TablePropertiesNames.DeletedKeys || key == TablePropertiesNames.MergeOperands)
                    props.UserCollectedProperties[key] = raw.ToArray(); // API backwards compatibility
                continue;
            }
            if (known) continue; // malformed predefined uint64 value: skipped (logged upstream)
            switch (key)
            {
                case TablePropertiesNames.DbId: props.DbId = Utf8(raw); break;
                case TablePropertiesNames.DbSessionId: props.DbSessionId = Utf8(raw); break;
                case TablePropertiesNames.DbHostId: props.DbHostId = Utf8(raw); break;
                case TablePropertiesNames.FilterPolicy: props.FilterPolicyName = Utf8(raw); break;
                case TablePropertiesNames.ColumnFamilyName: props.ColumnFamilyName = Utf8(raw); break;
                case TablePropertiesNames.Comparator: props.ComparatorName = Utf8(raw); break;
                case TablePropertiesNames.MergeOperator: props.MergeOperatorName = Utf8(raw); break;
                case TablePropertiesNames.PrefixExtractorName: props.PrefixExtractorName = Utf8(raw); break;
                case TablePropertiesNames.PropertyCollectors: props.PropertyCollectorsNames = Utf8(raw); break;
                case TablePropertiesNames.Compression: props.CompressionName = Utf8(raw); break;
                case TablePropertiesNames.CompressionOptions: props.CompressionOptions = Utf8(raw); break;
                case TablePropertiesNames.SequenceNumberTimeMapping: props.SequenceNumberTimeMapping = raw.ToArray(); break;
                default: props.UserCollectedProperties[key] = raw.ToArray(); break;
            }
        }
        return props;
    }

    private static string Utf8(ReadOnlySpan<byte> raw) => Encoding.UTF8.GetString(raw);

    private static bool TrySetUInt64(TableProperties p, string key, ReadOnlySpan<byte> raw, out bool known)
    {
        known = true;
        Action<TableProperties, ulong>? setter = key switch
        {
            TablePropertiesNames.OriginalFileNumber => static (p, v) => p.OriginalFileNumber = v,
            TablePropertiesNames.DataSize => static (p, v) => p.DataSize = v,
            TablePropertiesNames.UncompressedDataSize => static (p, v) => p.UncompressedDataSize = v,
            TablePropertiesNames.LsmInfoAtCreation => static (p, v) => p.LsmInfoAtCreation = v,
            TablePropertiesNames.IndexSize => static (p, v) => p.IndexSize = v,
            TablePropertiesNames.IndexPartitions => static (p, v) => p.IndexPartitions = v,
            TablePropertiesNames.TopLevelIndexSize => static (p, v) => p.TopLevelIndexSize = v,
            TablePropertiesNames.IndexKeyIsUserKey => static (p, v) => p.IndexKeyIsUserKey = v,
            TablePropertiesNames.IndexValueIsDeltaEncoded => static (p, v) => p.IndexValueIsDeltaEncoded = v,
            TablePropertiesNames.UdiIsPrimaryIndex => static (p, v) => p.UdiIsPrimaryIndex = v,
            TablePropertiesNames.StandardIndexIsStub => static (p, v) => p.StandardIndexIsStub = v,
            TablePropertiesNames.FilterSize => static (p, v) => p.FilterSize = v,
            TablePropertiesNames.RawKeySize => static (p, v) => p.RawKeySize = v,
            TablePropertiesNames.RawValueSize => static (p, v) => p.RawValueSize = v,
            TablePropertiesNames.NumDataBlocks => static (p, v) => p.NumDataBlocks = v,
            TablePropertiesNames.NumDataBlocksCompressionRejected => static (p, v) => p.NumDataBlocksCompressionRejected = v,
            TablePropertiesNames.NumDataBlocksCompressionBypassed => static (p, v) => p.NumDataBlocksCompressionBypassed = v,
            TablePropertiesNames.NumUniformBlocks => static (p, v) => p.NumUniformBlocks = v,
            TablePropertiesNames.NumEntries => static (p, v) => p.NumEntries = v,
            TablePropertiesNames.NumFilterEntries => static (p, v) => p.NumFilterEntries = v,
            TablePropertiesNames.DeletedKeys => static (p, v) => p.NumDeletions = v,
            TablePropertiesNames.MergeOperands => static (p, v) => p.NumMergeOperands = v,
            TablePropertiesNames.NumRangeDeletions => static (p, v) => p.NumRangeDeletions = v,
            TablePropertiesNames.FormatVersion => static (p, v) => p.FormatVersion = v,
            TablePropertiesNames.FixedKeyLen => static (p, v) => p.FixedKeyLen = v,
            TablePropertiesNames.ColumnFamilyId => static (p, v) => p.ColumnFamilyId = v,
            TablePropertiesNames.CreationTime => static (p, v) => p.CreationTime = v,
            TablePropertiesNames.OldestKeyTime => static (p, v) => p.OldestKeyTime = v,
            TablePropertiesNames.NewestKeyTime => static (p, v) => p.NewestKeyTime = v,
            TablePropertiesNames.FileCreationTime => static (p, v) => p.FileCreationTime = v,
            TablePropertiesNames.SlowCompressionEstimatedDataSize => static (p, v) => p.SlowCompressionEstimatedDataSize = v,
            TablePropertiesNames.FastCompressionEstimatedDataSize => static (p, v) => p.FastCompressionEstimatedDataSize = v,
            TablePropertiesNames.TailStartOffset => static (p, v) => p.TailStartOffset = v,
            TablePropertiesNames.UserDefinedTimestampsPersisted => static (p, v) => p.UserDefinedTimestampsPersisted = v,
            TablePropertiesNames.KeyLargestSeqno => static (p, v) => p.KeyLargestSeqno = v,
            TablePropertiesNames.KeySmallestSeqno => static (p, v) => p.KeySmallestSeqno = v,
            TablePropertiesNames.DataBlockRestartInterval => static (p, v) => p.DataBlockRestartInterval = v,
            TablePropertiesNames.IndexBlockRestartInterval => static (p, v) => p.IndexBlockRestartInterval = v,
            TablePropertiesNames.SeparateKeyValueInDataBlock => static (p, v) => p.SeparateKeyValueInDataBlock = v,
            _ => null,
        };
        if (setter is null)
        {
            known = false;
            return false;
        }
        if (!Coding.GetVarint64(ref raw, out ulong value)) return false;
        setter(p, value);
        return true;
    }

    /// <summary>Gets a user-collected property as a UTF-8 string, or null.</summary>
    public string? GetUserPropertyString(string name) => UserCollectedProperties.TryGetValue(name, out byte[]? v) ? Encoding.UTF8.GetString(v) : null;
}

/// <summary>
/// Port of PropertyBlockBuilder (table/meta_blocks.cc): properties sorted by name (bytewise), restart interval
/// "infinity" since the block is read sequentially.
/// </summary>
internal sealed class PropertyBlockBuilder : IDisposable
{
    private readonly SortedDictionary<byte[], byte[]> _props = new(ByteArrayComparer.Instance);
    private readonly BlockBuilder _block = BlockBuilder.ForMetaBlock(int.MaxValue);

    public void Add(string name, ulong value)
    {
        Span<byte> buf = stackalloc byte[Coding.MaxVarint64Length];
        int n = Coding.EncodeVarint64(buf, value);
        Add(name, buf[..n].ToArray());
    }

    public void Add(string name, string value) => Add(name, Encoding.UTF8.GetBytes(value));

    public void Add(string name, byte[] value)
    {
        byte[] key = Encoding.UTF8.GetBytes(name);
        _props.TryAdd(key, value);
    }

    public ReadOnlySpan<byte> Finish()
    {
        foreach (KeyValuePair<byte[], byte[]> kv in _props) _block.Add(kv.Key, kv.Value);
        return _block.Finish();
    }

    public void Dispose() => _block.Dispose();
}

/// <summary>Port of MetaIndexBuilder (table/meta_blocks.cc): meta block name → encoded handle, sorted by name.</summary>
internal sealed class MetaIndexBuilder : IDisposable
{
    private readonly SortedDictionary<byte[], byte[]> _handles = new(ByteArrayComparer.Instance);
    private readonly BlockBuilder _block = BlockBuilder.ForMetaBlock(1);

    public void Add(string name, BlockHandle handle)
    {
        Span<byte> buf = stackalloc byte[BlockHandle.MaxEncodedLength];
        int n = handle.EncodeTo(buf);
        _handles[Encoding.UTF8.GetBytes(name)] = buf[..n].ToArray();
    }

    public ReadOnlySpan<byte> Finish()
    {
        foreach (KeyValuePair<byte[], byte[]> kv in _handles) _block.Add(kv.Key, kv.Value);
        return _block.Finish();
    }

    public void Dispose() => _block.Dispose();
}

internal sealed class ByteArrayComparer : IComparer<byte[]>
{
    public static readonly ByteArrayComparer Instance = new();
    public int Compare(byte[]? x, byte[]? y) => x.AsSpan().SequenceCompareTo(y);
}
