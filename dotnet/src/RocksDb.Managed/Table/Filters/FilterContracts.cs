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
/// <remarks>
/// A filter block is found by <see cref="CompatibilityName"/>: tables written with another policy are read without a
/// filter. Readers are trusted like upstream (a false negative loses keys). A builder that throws fails the flush or
/// compaction with a fatal background error (no table is installed); a reader factory that throws fails the call that
/// opens the table (DB open, or the read).
/// </remarks>
public abstract class FilterPolicy
{
    /// <summary>
    /// Name persisted in table properties (e.g. "rocksdb.BuiltinBloomFilter"); a table's filter is read only by policies
    /// with the same compatibility name. A custom policy that wraps a built-in one (choosing a Bloom or Ribbon builder per
    /// level, for example) returns the wrapped policy's name so its filters stay readable by the built-in policies and
    /// native RocksDB; a policy with its own format returns its own name (usually <see cref="Name"/>).
    /// </summary>
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
    /// <summary>
    /// Creates a reader over filter contents; the reader may reference <paramref name="contents"/> (no copy). Port of
    /// GetFilterBitsReader: tables read with this policy use this reader for every full filter and filter partition
    /// stored under <see cref="CompatibilityName"/>, whichever policy wrote it. A policy that returns the built-in name
    /// "rocksdb.BuiltinBloomFilter" therefore reads filters written by the built-in Bloom and Ribbon policies (and by
    /// native RocksDB) and should delegate to a built-in policy's reader (e.g.
    /// <c>BloomFilterPolicy.CreateReader</c>), which reads every built-in format.
    /// </summary>
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
