using System.Buffers;

namespace RocksDb.Managed;

/// <summary>
/// CONTRACT (phase 2). Value type of the entry passed to <see cref="CompactionFilter.Filter"/>. Port of
/// rocksdb::CompactionFilter::ValueType (include/rocksdb/compaction_filter.h).
/// </summary>
public enum CompactionFilterValueType : byte
{
    /// <summary>Plain key-value.</summary>
    Value = 0,
    /// <summary>Merge operand.</summary>
    MergeOperand = 1,
    /// <summary>Internal (stacked BlobDB); never passed to application code.</summary>
    BlobIndex = 2,
    /// <summary>Wide-column entity.</summary>
    WideColumnEntity = 3,
}

/// <summary>Port of rocksdb::CompactionFilter::Decision.</summary>
public enum CompactionFilterDecision : byte
{
    /// <summary>Keep the entry as-is.</summary>
    Keep = 0,
    /// <summary>Remove the entry. Plain values become a tombstone (Delete); merge operands are dropped.</summary>
    Remove = 1,
    /// <summary>Replace the value (or merge operand) with what was written to the <c>newValue</c> writer.</summary>
    ChangeValue = 2,
    /// <summary>
    /// Remove all entries with key in [key, skipUntil), skipUntil being what was written to the <c>skipUntil</c>
    /// writer. A skipUntil &lt;= key is treated as <see cref="Keep"/>. Ignores snapshots.
    /// </summary>
    RemoveAndSkipUntil = 3,
    /// <summary>Internal (stacked BlobDB); not supported from application code.</summary>
    ChangeBlobIndex = 4,
    /// <summary>Internal (stacked BlobDB); not supported from application code.</summary>
    IOError = 5,
    /// <summary>Convert a plain value into a SingleDelete tombstone. Not supported for merge operands.</summary>
    Purge = 6,
    /// <summary>
    /// Replace the entry with a wide-column entity made of the columns added to the <c>newColumns</c> list (any order,
    /// unique names). Only from <see cref="CompactionFilter.FilterV3"/> / <see cref="CompactionFilter.FilterV4"/>.
    /// </summary>
    ChangeWideColumnEntity = 7,
    /// <summary>
    /// Only from <see cref="CompactionFilter.FilterBlobByKey"/> ("read the blob and decide by value"); returning it from
    /// Filter / FilterV3 / FilterV4 fails the compaction.
    /// </summary>
    Undetermined = 8,
}

/// <summary>
/// CONTRACT (phase 2). Application hook that can drop or rewrite entries during compaction. Port of the
/// FilterV2 surface of rocksdb::CompactionFilter (include/rocksdb/compaction_filter.h), reshaped for spans:
/// outputs are written to caller-provided <see cref="IBufferWriter{T}"/>s instead of <c>std::string*</c>.
/// <para>
/// Implementations must be thread-safe unless they are created per compaction by a factory, and must not throw
/// (an exception aborts the compaction). The <see cref="Name"/> is persisted in the OPTIONS file
/// (<c>compaction_filter</c>). Entry points, most specific first: <see cref="FilterV4"/> (all value types, lazy blob
/// columns) -&gt; <see cref="FilterV3"/> (entities) -&gt; <see cref="Filter"/> (FilterV2: plain values and merge operands).
/// </para>
/// </summary>
public abstract class CompactionFilter
{
    /// <summary>Name persisted in the OPTIONS file.</summary>
    public abstract string Name { get; }

    /// <summary>
    /// Port of FilterV2. Decide what to do with one entry. <paramref name="newValue"/> receives the replacement for
    /// <see cref="CompactionFilterDecision.ChangeValue"/>; <paramref name="skipUntil"/> receives the exclusive user key
    /// end for <see cref="CompactionFilterDecision.RemoveAndSkipUntil"/>. Both writers start empty.
    /// </summary>
    /// <param name="level">Level of the compaction input (see upstream caveats about its meaning).</param>
    /// <param name="key">User key.</param>
    /// <param name="valueType">Type of <paramref name="existingValue"/>.</param>
    /// <param name="existingValue">Value or merge operand.</param>
    public abstract CompactionFilterDecision Filter(int level, ReadOnlySpan<byte> key, CompactionFilterValueType valueType,
        ReadOnlySpan<byte> existingValue, IBufferWriter<byte> newValue, IBufferWriter<byte> skipUntil);

    /// <summary>
    /// Port of FilterV3: called during compaction for plain values (blob values are read first) and wide-column
    /// entities. For <see cref="CompactionFilterValueType.Value"/>, <paramref name="existingValue"/> is the value and
    /// <paramref name="existingColumns"/> is null; for <see cref="CompactionFilterValueType.WideColumnEntity"/>,
    /// <paramref name="existingColumns"/> holds the entity's columns (blob-backed columns already resolved; valid only
    /// during the call) and <paramref name="existingValue"/> is empty. Besides the <see cref="Filter"/> decisions it may
    /// return <see cref="CompactionFilterDecision.ChangeValue"/> (the entry becomes a plain value written to
    /// <paramref name="newValue"/>) or <see cref="CompactionFilterDecision.ChangeWideColumnEntity"/> (the entry becomes
    /// an entity of the columns added to <paramref name="newColumns"/>). Merge operands still go to <see cref="Filter"/>.
    /// <para>
    /// The default implementation keeps entities (like upstream's default FilterV3) and forwards plain values to
    /// <see cref="Filter"/>. Override it to filter entities.
    /// </para>
    /// </summary>
    public virtual CompactionFilterDecision FilterV3(int level, ReadOnlySpan<byte> key, CompactionFilterValueType valueType,
        ReadOnlySpan<byte> existingValue, WideColumns? existingColumns, IBufferWriter<byte> newValue, List<WideColumn> newColumns,
        IBufferWriter<byte> skipUntil)
    {
        if (valueType == CompactionFilterValueType.WideColumnEntity) return CompactionFilterDecision.Keep;
        return Filter(level, key, valueType, existingValue, newValue, skipUntil);
    }

    /// <summary>
    /// Port of FilterV4: <see cref="FilterV3"/> plus lazy loading of blob-backed entity columns. Called for plain values
    /// (blob values are read first), merge operands and wide-column entities.
    /// <para>
    /// <paramref name="blobResolver"/> is non-null only when <see cref="SupportsFilterV4"/> is true and the entity has
    /// blob-backed columns (written by flush/compaction value separation or blob direct write). Then
    /// <paramref name="existingColumns"/> holds all columns, but a blob-backed column's value is its serialized blob
    /// reference: use <see cref="WideColumnBlobResolver.IsBlobColumn"/> and
    /// <see cref="WideColumnBlobResolver.ResolveColumn"/> to read only the blobs the filter needs. Otherwise (and for
    /// all filters that do not opt in) every column value is already resolved.
    /// </para>
    /// <para>The default implementation delegates to <see cref="FilterV3"/>.</para>
    /// </summary>
    public virtual CompactionFilterDecision FilterV4(int level, ReadOnlySpan<byte> key, CompactionFilterValueType valueType,
        ReadOnlySpan<byte> existingValue, WideColumns? existingColumns, IBufferWriter<byte> newValue, List<WideColumn> newColumns,
        IBufferWriter<byte> skipUntil, WideColumnBlobResolver? blobResolver) =>
        FilterV3(level, key, valueType, existingValue, existingColumns, newValue, newColumns, skipUntil);

    /// <summary>
    /// Port of SupportsFilterV4: true if the filter overrides <see cref="FilterV4"/> and handles a
    /// <see cref="WideColumnBlobResolver"/> (lazy blob loading). When false (default), blob-backed entity columns are
    /// resolved before the filter is called, so FilterV3-only filters see the actual values.
    /// </summary>
    public virtual bool SupportsFilterV4 => false;

    /// <summary>
    /// Port of FilterBlobByKey: for a plain value stored in a blob file, a chance to decide from the key alone without
    /// reading the blob. Return <see cref="CompactionFilterDecision.Undetermined"/> (the default) to have the blob read
    /// and passed to <see cref="FilterV4"/> as a <see cref="CompactionFilterValueType.Value"/>; any other decision is
    /// final (<paramref name="newValue"/> / <paramref name="skipUntil"/> as for <see cref="Filter"/>).
    /// </summary>
    public virtual CompactionFilterDecision FilterBlobByKey(int level, ReadOnlySpan<byte> key, IBufferWriter<byte> newValue,
        IBufferWriter<byte> skipUntil) => CompactionFilterDecision.Undetermined;

    /// <summary>
    /// Port of IgnoreSnapshots. Only true is supported (like upstream): a compaction or flush whose filter returns
    /// false fails with NotSupported.
    /// </summary>
    public virtual bool IgnoreSnapshots => true;

    /// <summary>The NotSupported message of CompactionJob::SetupAndValidateCompactionFilter and BuildTable.</summary>
    internal const string IgnoreSnapshotsFalseMessage = "CompactionFilter::IgnoreSnapshots() = false is not supported anymore.";
}

/// <summary>Port of CompactionFilter::Context: information about the compaction a filter is created for.</summary>
public readonly record struct CompactionFilterContext(bool IsFullCompaction, bool IsManualCompaction, uint ColumnFamilyId, bool IsFlush);

/// <summary>Port of rocksdb::CompactionFilterFactory: creates one (possibly stateful) filter per compaction.</summary>
public abstract class CompactionFilterFactory
{
    /// <summary>Name persisted in the OPTIONS file (<c>compaction_filter_factory</c>).</summary>
    public abstract string Name { get; }

    /// <summary>Creates the filter for one compaction (or flush, if <see cref="ShouldFilterTableFileCreation"/>), or null for none.</summary>
    public abstract CompactionFilter? CreateCompactionFilter(in CompactionFilterContext context);

    /// <summary>Port of ShouldFilterTableFileCreation: by default only compactions (not flushes) are filtered.</summary>
    public virtual bool ShouldFilterTableFileCreation(bool isFlush) => !isFlush;
}

/// <summary>
/// Port of rocksdb::WideColumnBlobResolver (include/rocksdb/compaction_filter.h): lazily reads the blob-backed columns of
/// the entity passed to <see cref="CompactionFilter.FilterV4"/>, so a filter only pays the I/O for the columns it
/// inspects. Valid only during that call; used by the compaction thread only.
/// </summary>
public abstract class WideColumnBlobResolver
{
    /// <summary>The number of columns of the entity (NumColumns).</summary>
    public abstract int ColumnCount { get; }

    /// <summary>True if column <paramref name="columnIndex"/> is stored in a blob file; false when out of range.</summary>
    public abstract bool IsBlobColumn(int columnIndex);

    /// <summary>
    /// The value of column <paramref name="columnIndex"/>: the blob for a blob-backed column (read once and cached),
    /// the inline value otherwise. The span stays valid until <see cref="CompactionFilter.FilterV4"/> returns, so several
    /// columns can be resolved and compared without copying. Throws <see cref="RocksDbException"/> (InvalidArgument
    /// for an index out of range, Corruption / IOError for blob read failures). After a failed read the filter should
    /// stop and return <see cref="CompactionFilterDecision.Keep"/>: the compaction fails with that error once the filter
    /// returns, even if the filter caught it.
    /// </summary>
    public abstract ReadOnlySpan<byte> ResolveColumn(int columnIndex);
}
