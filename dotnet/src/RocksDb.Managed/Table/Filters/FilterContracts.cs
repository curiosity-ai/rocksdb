namespace RocksDb.Managed.Table.Filters;

/// <summary>
/// CONTRACT (owned by stream C). Builds a full (whole-table or per-partition) filter from key hashes.
/// Port of FilterBitsBuilder (table/block_based/filter_policy_internal.h).
/// </summary>
public interface IFilterBitsBuilder : IDisposable
{
    /// <summary>Adds a key (already prefix-transformed / timestamp-stripped as appropriate). Duplicates of the previous key are ignored.</summary>
    void AddKey(ReadOnlySpan<byte> key);
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
    public abstract IFilterBitsBuilder CreateBuilder(int formatVersion);
    /// <summary>Creates a reader over filter contents; the reader may reference <paramref name="contents"/> (no copy).</summary>
    public abstract IFilterBitsReader CreateReader(ReadOnlyMemory<byte> contents);
}
