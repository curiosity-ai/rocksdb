namespace RocksDb.Managed.Table.Filters;

/// <summary>
/// CONTRACT (owned by stream C). Builds a full (whole-table or per-partition) filter from key hashes.
/// Port of FilterBitsBuilder (table/block_based/filter_policy_internal.h).
/// </summary>
public interface IFilterBitsBuilder : IDisposable
{
    /// <summary>Adds a key (already prefix-transformed / timestamp-stripped as appropriate). Duplicates of the previous key are ignored.</summary>
    void AddKey(ReadOnlySpan<byte> key);
    /// <summary>
    /// Adds a key and an alternate entry (its prefix), de-duplicating both against the previous key and alternate
    /// (FilterBitsBuilder::AddKeyAndAlt).
    /// </summary>
    void AddKeyAndAlt(ReadOnlySpan<byte> key, ReadOnlySpan<byte> alt)
    {
        AddKey(alt);
        AddKey(key);
    }
    /// <summary>Approximate number of keys that fit a filter of <paramref name="bytes"/> bytes (for filter partitioning).</summary>
    ulong ApproximateNumEntries(ulong bytes) => 0;
    int EstimatedEntries { get; }
    /// <summary>Writes the serialized filter (including RocksDB's metadata trailer) to the writer and resets the builder.</summary>
    void Finish(System.Buffers.IBufferWriter<byte> output);
}

/// <summary>CONTRACT (owned by stream C). Queries a serialized filter. Port of FilterBitsReader.</summary>
public interface IFilterBitsReader
{
    bool MayMatch(ReadOnlySpan<byte> key);
}

/// <summary>CONTRACT (owned by stream C). Port of rocksdb::FilterPolicy (bloom / ribbon).</summary>
public abstract class FilterPolicy
{
    /// <summary>Name persisted in table properties (e.g. "rocksdb.BuiltinBloomFilter").</summary>
    public abstract string CompatibilityName { get; }
    /// <summary>
    /// Policy class name (Customizable::Name), persisted as the "rocksdb.filter.policy" table property whenever a
    /// policy is configured, e.g. "bloomfilter" / "ribbonfilter". Default: <see cref="CompatibilityName"/>.
    /// </summary>
    public virtual string Name => CompatibilityName;
    public abstract IFilterBitsBuilder CreateBuilder(int formatVersion);
    /// <summary>
    /// Creates a builder with full table-building context (port of GetBuilderWithContext). Policies whose
    /// choice depends on the level (e.g. Ribbon's bloom_before_level) override this; the default ignores
    /// everything but the format version.
    /// </summary>
    public virtual IFilterBitsBuilder CreateBuilder(in FilterBuildingContext context) => CreateBuilder(context.FormatVersion);
    /// <summary>Creates a reader over filter contents; the reader may reference <paramref name="contents"/> (no copy).</summary>
    public abstract IFilterBitsReader CreateReader(ReadOnlyMemory<byte> contents);
}

/// <summary>
/// Port of the parts of rocksdb::FilterBuildingContext (include/rocksdb/filter_policy.h) that built-in
/// policies use. <see cref="LevelAtCreation"/> is -1 when unknown; FIFO / no-compaction callers should
/// pass -1 (treated as bottommost, like upstream).
/// </summary>
public readonly record struct FilterBuildingContext(int FormatVersion, int LevelAtCreation, bool IsFlush)
{
    /// <summary>
    /// BlockBasedTableOptions::optimize_filters_for_memory of the table being built: built-in policies then round filter
    /// sizes to the allocator's usable size (see <see cref="FilterAllocationModel"/>).
    /// </summary>
    public bool OptimizeFiltersForMemory { get; init; }

    /// <summary>
    /// FilterBuildingContext::compaction_style of the column family. Level-aware policies (Ribbon's bloom_before_level)
    /// only use the level and flush information under level and universal compaction; FIFO and "none" are treated as
    /// bottommost.
    /// </summary>
    public CompactionStyle CompactionStyle { get; init; }
}
