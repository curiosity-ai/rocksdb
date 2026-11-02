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
    /// unique names). Only from <see cref="CompactionFilter.FilterV3"/>.
    /// </summary>
    ChangeWideColumnEntity = 7,
    /// <summary>Only meaningful for blob-by-key filtering; not supported from <see cref="CompactionFilter.Filter"/>.</summary>
    Undetermined = 8,
}

/// <summary>
/// CONTRACT (phase 2). Application hook that can drop or rewrite entries during compaction. Port of the
/// FilterV2 surface of rocksdb::CompactionFilter (include/rocksdb/compaction_filter.h), reshaped for spans:
/// outputs are written to caller-provided <see cref="IBufferWriter{T}"/>s instead of <c>std::string*</c>.
/// <para>
/// Implementations must be thread-safe unless they are created per compaction by a factory, and must not throw
/// (an exception aborts the compaction). The <see cref="Name"/> is persisted in the OPTIONS file
/// (<c>compaction_filter</c>). Compaction itself is not implemented yet; <see cref="Db.MergeHelper"/> already calls
/// <see cref="Filter"/> for merge operands (FilterMerge).
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
    /// Port of IgnoreSnapshots. Upstream only supports true (filtering may hide versions still visible to snapshots).
    /// </summary>
    public virtual bool IgnoreSnapshots => true;
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
