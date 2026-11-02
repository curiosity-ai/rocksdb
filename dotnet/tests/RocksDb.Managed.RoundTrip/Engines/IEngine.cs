using RocksDb.Managed.RoundTrip.Protocol;

namespace RocksDb.Managed.RoundTrip.Engines;

public enum EngineKind
{
    Native,
    Managed,
}

/// <summary>
/// Engine-neutral database operations used by <see cref="ScriptRunner"/>. Implemented over native RocksDB
/// (rocksdb-sharp; only ever instantiated in a child process) and over the managed port.
/// </summary>
public interface IEngine : IDisposable
{
    EngineKind Kind { get; }

    /// <summary>Opens (creating if missing) the DB at <paramref name="path"/>: existing CFs are discovered on disk,
    /// a new DB gets the spec's initial CFs.</summary>
    void Open(string path, DbSpec spec);

    void Close();

    IReadOnlyList<string> ColumnFamilyNames { get; }

    /// <summary>Applies the data ops (Put/Merge/Delete/SingleDelete/DeleteRange) atomically as one write batch.</summary>
    void Write(IReadOnlyList<Op> ops, bool sync);

    void Flush(string cf);

    /// <summary>Full-range manual compaction; false when the engine doesn't support it (yet).</summary>
    bool CompactRange(string cf);

    void CreateColumnFamily(string cf);

    void DropColumnFamily(string cf);

    /// <summary>Writes an external SST file with <paramref name="cf"/>'s options; entries are Sst* ops in key order.</summary>
    void WriteSst(string cf, string path, IReadOnlyList<Op> entries);

    void Ingest(string cf, IReadOnlyList<string> files, bool move);

    void Checkpoint(string dir);

    void TakeSnapshot(long id);

    void ReleaseSnapshot(long id);

    /// <summary>All entries of <paramref name="cf"/> in key order (optionally as of a snapshot).</summary>
    IEnumerable<KeyValuePair<byte[], byte[]>> Scan(string cf, long? snapshot);

    /// <summary>Point lookup (optionally as of a snapshot); null when not found.</summary>
    byte[]? Get(string cf, byte[] key, long? snapshot);

    /// <summary>Number of table files per level of <paramref name="cf"/>.</summary>
    int[] FilesPerLevel(string cf);
}
