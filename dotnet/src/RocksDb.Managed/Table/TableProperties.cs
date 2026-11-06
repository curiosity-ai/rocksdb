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

    /// <summary>
    /// Port of ParsePropertiesBlock (table/meta_blocks.cc). <paramref name="blockOffset"/> is the file offset of the block.
    /// Runs for every table file a DB opens (after each flush, compaction output and ingestion), so it avoids per-property
    /// garbage: names are matched as UTF-8 bytes against the known names (no key strings), and string values (comparator,
    /// compression, column family, DB and session ids, ...), which are the same for most files of a DB, are interned
    /// (<see cref="Utf8StringCache"/>). Only user-collected values and the seqno-to-time mapping are copied.
    /// </summary>
    internal static TableProperties Parse(Block block, ulong blockOffset)
    {
        var props = new TableProperties();
        using var it = new BlockIter();
        it.InitializeMeta(block);
        Span<byte> lastKeyStack = stackalloc byte[128];
        byte[]? lastKeyRented = null;
        Span<byte> lastKeyBuf = lastKeyStack;
        int lastKeyLength = -1;
        try
        {
            for (it.SeekToFirst(); it.Valid; it.Next())
            {
                ReadOnlySpan<byte> keyBytes = it.Key;
                if (lastKeyLength >= 0 && keyBytes.SequenceCompareTo(lastKeyBuf[..lastKeyLength]) <= 0) ThrowHelper.Corruption("properties unsorted");
                if (keyBytes.Length > lastKeyBuf.Length)
                {
                    if (lastKeyRented is not null) System.Buffers.ArrayPool<byte>.Shared.Return(lastKeyRented);
                    lastKeyRented = System.Buffers.ArrayPool<byte>.Shared.Rent(keyBytes.Length);
                    lastKeyBuf = lastKeyRented;
                }
                keyBytes.CopyTo(lastKeyBuf);
                lastKeyLength = keyBytes.Length;
                int known = FindKnownProperty(keyBytes);
                PropertyId id = known >= 0 ? s_knownProperties[known].Id : PropertyId.UserCollected;
                ReadOnlySpan<byte> raw = it.Value;
                if (id == PropertyId.ExternalSstFileGlobalSeqno) props.ExternalSstFileGlobalSeqnoOffset = blockOffset + (ulong)it.ValueOffsetInBlock;
                if (id < PropertyId.FirstString)
                {
                    ReadOnlySpan<byte> input = raw;
                    // A malformed predefined uint64 value is skipped (logged upstream).
                    if (!Coding.GetVarint64(ref input, out ulong value)) continue;
                    SetUInt64(props, id, value);
                    if (id is PropertyId.DeletedKeys or PropertyId.MergeOperands)
                        props.UserCollectedProperties[s_knownProperties[known].Name] = raw.ToArray(); // API backwards compatibility
                    continue;
                }
                switch (id)
                {
                    case PropertyId.DbId: props.DbId = Utf8StringCache.Get(raw); break;
                    case PropertyId.DbSessionId: props.DbSessionId = Utf8StringCache.Get(raw); break;
                    case PropertyId.DbHostId: props.DbHostId = Utf8StringCache.Get(raw); break;
                    case PropertyId.FilterPolicy: props.FilterPolicyName = Utf8StringCache.Get(raw); break;
                    case PropertyId.ColumnFamilyName: props.ColumnFamilyName = Utf8StringCache.Get(raw); break;
                    case PropertyId.Comparator: props.ComparatorName = Utf8StringCache.Get(raw); break;
                    case PropertyId.MergeOperator: props.MergeOperatorName = Utf8StringCache.Get(raw); break;
                    case PropertyId.PrefixExtractorName: props.PrefixExtractorName = Utf8StringCache.Get(raw); break;
                    case PropertyId.PropertyCollectors: props.PropertyCollectorsNames = Utf8StringCache.Get(raw); break;
                    case PropertyId.Compression: props.CompressionName = Utf8StringCache.Get(raw); break;
                    case PropertyId.CompressionOptions: props.CompressionOptions = Utf8StringCache.Get(raw); break;
                    case PropertyId.SequenceNumberTimeMapping: props.SequenceNumberTimeMapping = raw.ToArray(); break;
                    default:
                        props.UserCollectedProperties[known >= 0 ? s_knownProperties[known].Name : Utf8StringCache.Get(keyBytes)] = raw.ToArray();
                        break;
                }
            }
        }
        finally
        {
            if (lastKeyRented is not null) System.Buffers.ArrayPool<byte>.Shared.Return(lastKeyRented);
        }
        return props;
    }

    /// <summary>The properties <see cref="Parse"/> recognises: the predefined uint64 ones (up to <see cref="FirstString"/>), then strings, then the rest.</summary>
    private enum PropertyId : byte
    {
        OriginalFileNumber,
        DataSize,
        UncompressedDataSize,
        LsmInfoAtCreation,
        IndexSize,
        IndexPartitions,
        TopLevelIndexSize,
        IndexKeyIsUserKey,
        IndexValueIsDeltaEncoded,
        UdiIsPrimaryIndex,
        StandardIndexIsStub,
        FilterSize,
        RawKeySize,
        RawValueSize,
        NumDataBlocks,
        NumDataBlocksCompressionRejected,
        NumDataBlocksCompressionBypassed,
        NumUniformBlocks,
        NumEntries,
        NumFilterEntries,
        DeletedKeys,
        MergeOperands,
        NumRangeDeletions,
        FormatVersion,
        FixedKeyLen,
        ColumnFamilyId,
        CreationTime,
        OldestKeyTime,
        NewestKeyTime,
        FileCreationTime,
        SlowCompressionEstimatedDataSize,
        FastCompressionEstimatedDataSize,
        TailStartOffset,
        UserDefinedTimestampsPersisted,
        KeyLargestSeqno,
        KeySmallestSeqno,
        DataBlockRestartInterval,
        IndexBlockRestartInterval,
        SeparateKeyValueInDataBlock,
        /// <summary>First string-valued property.</summary>
        FirstString,
        DbId = FirstString,
        DbSessionId,
        DbHostId,
        FilterPolicy,
        ColumnFamilyName,
        Comparator,
        MergeOperator,
        PrefixExtractorName,
        PropertyCollectors,
        Compression,
        CompressionOptions,
        SequenceNumberTimeMapping,
        ExternalSstFileGlobalSeqno,
        /// <summary>Stored in <see cref="UserCollectedProperties"/> (a known name: its string is not allocated).</summary>
        UserCollected,
    }

    private readonly struct KnownProperty(string name, PropertyId id)
    {
        public readonly byte[] Utf8 = Encoding.UTF8.GetBytes(name);
        public readonly string Name = name;
        public readonly PropertyId Id = id;
    }

    /// <summary>Known property names, sorted bytewise (like the properties block) for <see cref="FindKnownProperty"/>.</summary>
    private static readonly KnownProperty[] s_knownProperties = SortKnown(
    [
        new(TablePropertiesNames.OriginalFileNumber, PropertyId.OriginalFileNumber),
        new(TablePropertiesNames.DataSize, PropertyId.DataSize),
        new(TablePropertiesNames.UncompressedDataSize, PropertyId.UncompressedDataSize),
        new(TablePropertiesNames.LsmInfoAtCreation, PropertyId.LsmInfoAtCreation),
        new(TablePropertiesNames.IndexSize, PropertyId.IndexSize),
        new(TablePropertiesNames.IndexPartitions, PropertyId.IndexPartitions),
        new(TablePropertiesNames.TopLevelIndexSize, PropertyId.TopLevelIndexSize),
        new(TablePropertiesNames.IndexKeyIsUserKey, PropertyId.IndexKeyIsUserKey),
        new(TablePropertiesNames.IndexValueIsDeltaEncoded, PropertyId.IndexValueIsDeltaEncoded),
        new(TablePropertiesNames.UdiIsPrimaryIndex, PropertyId.UdiIsPrimaryIndex),
        new(TablePropertiesNames.StandardIndexIsStub, PropertyId.StandardIndexIsStub),
        new(TablePropertiesNames.FilterSize, PropertyId.FilterSize),
        new(TablePropertiesNames.RawKeySize, PropertyId.RawKeySize),
        new(TablePropertiesNames.RawValueSize, PropertyId.RawValueSize),
        new(TablePropertiesNames.NumDataBlocks, PropertyId.NumDataBlocks),
        new(TablePropertiesNames.NumDataBlocksCompressionRejected, PropertyId.NumDataBlocksCompressionRejected),
        new(TablePropertiesNames.NumDataBlocksCompressionBypassed, PropertyId.NumDataBlocksCompressionBypassed),
        new(TablePropertiesNames.NumUniformBlocks, PropertyId.NumUniformBlocks),
        new(TablePropertiesNames.NumEntries, PropertyId.NumEntries),
        new(TablePropertiesNames.NumFilterEntries, PropertyId.NumFilterEntries),
        new(TablePropertiesNames.DeletedKeys, PropertyId.DeletedKeys),
        new(TablePropertiesNames.MergeOperands, PropertyId.MergeOperands),
        new(TablePropertiesNames.NumRangeDeletions, PropertyId.NumRangeDeletions),
        new(TablePropertiesNames.FormatVersion, PropertyId.FormatVersion),
        new(TablePropertiesNames.FixedKeyLen, PropertyId.FixedKeyLen),
        new(TablePropertiesNames.ColumnFamilyId, PropertyId.ColumnFamilyId),
        new(TablePropertiesNames.CreationTime, PropertyId.CreationTime),
        new(TablePropertiesNames.OldestKeyTime, PropertyId.OldestKeyTime),
        new(TablePropertiesNames.NewestKeyTime, PropertyId.NewestKeyTime),
        new(TablePropertiesNames.FileCreationTime, PropertyId.FileCreationTime),
        new(TablePropertiesNames.SlowCompressionEstimatedDataSize, PropertyId.SlowCompressionEstimatedDataSize),
        new(TablePropertiesNames.FastCompressionEstimatedDataSize, PropertyId.FastCompressionEstimatedDataSize),
        new(TablePropertiesNames.TailStartOffset, PropertyId.TailStartOffset),
        new(TablePropertiesNames.UserDefinedTimestampsPersisted, PropertyId.UserDefinedTimestampsPersisted),
        new(TablePropertiesNames.KeyLargestSeqno, PropertyId.KeyLargestSeqno),
        new(TablePropertiesNames.KeySmallestSeqno, PropertyId.KeySmallestSeqno),
        new(TablePropertiesNames.DataBlockRestartInterval, PropertyId.DataBlockRestartInterval),
        new(TablePropertiesNames.IndexBlockRestartInterval, PropertyId.IndexBlockRestartInterval),
        new(TablePropertiesNames.SeparateKeyValueInDataBlock, PropertyId.SeparateKeyValueInDataBlock),
        new(TablePropertiesNames.DbId, PropertyId.DbId),
        new(TablePropertiesNames.DbSessionId, PropertyId.DbSessionId),
        new(TablePropertiesNames.DbHostId, PropertyId.DbHostId),
        new(TablePropertiesNames.FilterPolicy, PropertyId.FilterPolicy),
        new(TablePropertiesNames.ColumnFamilyName, PropertyId.ColumnFamilyName),
        new(TablePropertiesNames.Comparator, PropertyId.Comparator),
        new(TablePropertiesNames.MergeOperator, PropertyId.MergeOperator),
        new(TablePropertiesNames.PrefixExtractorName, PropertyId.PrefixExtractorName),
        new(TablePropertiesNames.PropertyCollectors, PropertyId.PropertyCollectors),
        new(TablePropertiesNames.Compression, PropertyId.Compression),
        new(TablePropertiesNames.CompressionOptions, PropertyId.CompressionOptions),
        new(TablePropertiesNames.SequenceNumberTimeMapping, PropertyId.SequenceNumberTimeMapping),
        new(TablePropertiesNames.ExternalSstFileGlobalSeqno, PropertyId.ExternalSstFileGlobalSeqno),
        new(TablePropertiesNames.TimestampMin, PropertyId.UserCollected),
        new(TablePropertiesNames.TimestampMax, PropertyId.UserCollected),
        new(TablePropertiesNames.BlockBasedIndexType, PropertyId.UserCollected),
        new(TablePropertiesNames.EmbeddedBlobSstStats, PropertyId.UserCollected),
        new(TablePropertiesNames.BlockBasedWholeKeyFiltering, PropertyId.UserCollected),
        new(TablePropertiesNames.BlockBasedPrefixFiltering, PropertyId.UserCollected),
        new(TablePropertiesNames.BlockBasedDecoupledPartitionedFilters, PropertyId.UserCollected),
        new(TablePropertiesNames.ExternalSstFileVersion, PropertyId.UserCollected),
    ]);

    private static KnownProperty[] SortKnown(KnownProperty[] a)
    {
        Array.Sort(a, static (x, y) => x.Utf8.AsSpan().SequenceCompareTo(y.Utf8));
        return a;
    }

    /// <summary>Index of <paramref name="name"/> in <see cref="s_knownProperties"/>, or -1.</summary>
    private static int FindKnownProperty(ReadOnlySpan<byte> name)
    {
        KnownProperty[] k = s_knownProperties;
        int lo = 0, hi = k.Length - 1;
        while (lo <= hi)
        {
            int mid = (lo + hi) >>> 1;
            int c = name.SequenceCompareTo(k[mid].Utf8);
            if (c == 0) return mid;
            if (c < 0) hi = mid - 1;
            else lo = mid + 1;
        }
        return -1;
    }

    private static void SetUInt64(TableProperties p, PropertyId id, ulong v)
    {
        switch (id)
        {
            case PropertyId.OriginalFileNumber: p.OriginalFileNumber = v; break;
            case PropertyId.DataSize: p.DataSize = v; break;
            case PropertyId.UncompressedDataSize: p.UncompressedDataSize = v; break;
            case PropertyId.LsmInfoAtCreation: p.LsmInfoAtCreation = v; break;
            case PropertyId.IndexSize: p.IndexSize = v; break;
            case PropertyId.IndexPartitions: p.IndexPartitions = v; break;
            case PropertyId.TopLevelIndexSize: p.TopLevelIndexSize = v; break;
            case PropertyId.IndexKeyIsUserKey: p.IndexKeyIsUserKey = v; break;
            case PropertyId.IndexValueIsDeltaEncoded: p.IndexValueIsDeltaEncoded = v; break;
            case PropertyId.UdiIsPrimaryIndex: p.UdiIsPrimaryIndex = v; break;
            case PropertyId.StandardIndexIsStub: p.StandardIndexIsStub = v; break;
            case PropertyId.FilterSize: p.FilterSize = v; break;
            case PropertyId.RawKeySize: p.RawKeySize = v; break;
            case PropertyId.RawValueSize: p.RawValueSize = v; break;
            case PropertyId.NumDataBlocks: p.NumDataBlocks = v; break;
            case PropertyId.NumDataBlocksCompressionRejected: p.NumDataBlocksCompressionRejected = v; break;
            case PropertyId.NumDataBlocksCompressionBypassed: p.NumDataBlocksCompressionBypassed = v; break;
            case PropertyId.NumUniformBlocks: p.NumUniformBlocks = v; break;
            case PropertyId.NumEntries: p.NumEntries = v; break;
            case PropertyId.NumFilterEntries: p.NumFilterEntries = v; break;
            case PropertyId.DeletedKeys: p.NumDeletions = v; break;
            case PropertyId.MergeOperands: p.NumMergeOperands = v; break;
            case PropertyId.NumRangeDeletions: p.NumRangeDeletions = v; break;
            case PropertyId.FormatVersion: p.FormatVersion = v; break;
            case PropertyId.FixedKeyLen: p.FixedKeyLen = v; break;
            case PropertyId.ColumnFamilyId: p.ColumnFamilyId = v; break;
            case PropertyId.CreationTime: p.CreationTime = v; break;
            case PropertyId.OldestKeyTime: p.OldestKeyTime = v; break;
            case PropertyId.NewestKeyTime: p.NewestKeyTime = v; break;
            case PropertyId.FileCreationTime: p.FileCreationTime = v; break;
            case PropertyId.SlowCompressionEstimatedDataSize: p.SlowCompressionEstimatedDataSize = v; break;
            case PropertyId.FastCompressionEstimatedDataSize: p.FastCompressionEstimatedDataSize = v; break;
            case PropertyId.TailStartOffset: p.TailStartOffset = v; break;
            case PropertyId.UserDefinedTimestampsPersisted: p.UserDefinedTimestampsPersisted = v; break;
            case PropertyId.KeyLargestSeqno: p.KeyLargestSeqno = v; break;
            case PropertyId.KeySmallestSeqno: p.KeySmallestSeqno = v; break;
            case PropertyId.DataBlockRestartInterval: p.DataBlockRestartInterval = v; break;
            case PropertyId.IndexBlockRestartInterval: p.IndexBlockRestartInterval = v; break;
            case PropertyId.SeparateKeyValueInDataBlock: p.SeparateKeyValueInDataBlock = v; break;
        }
    }

    /// <summary>
    /// Process-wide intern cache for the UTF-8 strings of table properties: direct-mapped by hash, lock-free (entries are
    /// immutable; a racing replacement only costs a decode). Values longer than <see cref="MaxLength"/> bytes are not cached.
    /// </summary>
    internal static class Utf8StringCache
    {
        private const int Slots = 256;
        internal const int MaxLength = 512;
        private static readonly Entry?[] s_entries = new Entry?[Slots];

        private sealed class Entry(byte[] utf8, string value)
        {
            public readonly byte[] Utf8 = utf8;
            public readonly string Value = value;
        }

        /// <summary>The string for <paramref name="utf8"/> (same result as <see cref="Encoding.UTF8"/>.GetString).</summary>
        public static string Get(ReadOnlySpan<byte> utf8)
        {
            if (utf8.IsEmpty) return "";
            if (utf8.Length > MaxLength) return Encoding.UTF8.GetString(utf8);
            int slot = (int)(Hash.Hash64(utf8) & (Slots - 1));
            Entry? e = Volatile.Read(ref s_entries[slot]);
            if (e is not null && utf8.SequenceEqual(e.Utf8)) return e.Value;
            string value = Encoding.UTF8.GetString(utf8);
            Volatile.Write(ref s_entries[slot], new Entry(utf8.ToArray(), value));
            return value;
        }
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
