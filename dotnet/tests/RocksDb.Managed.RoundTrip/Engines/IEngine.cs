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
    void Write(IReadOnlyList<Op> ops, bool sync) => Write(ops, sync, disableWal: false);

    /// <summary>As <see cref="Write(IReadOnlyList{Op}, bool)"/>, optionally with WriteOptions.disableWAL.</summary>
    void Write(IReadOnlyList<Op> ops, bool sync, bool disableWal);

    void Flush(string cf);

    /// <summary>Flushes every column family (native: one rocksdb_flush_cfs call).</summary>
    void FlushAll();

    void FlushWal(bool sync);

    /// <summary>Full-range manual compaction; false when the engine doesn't support it (yet).</summary>
    bool CompactRange(string cf);

    /// <summary>CompactRange over [begin, end] (null = unbounded) with explicit CompactRangeOptions.</summary>
    void CompactRange(string cf, byte[]? begin, byte[]? end, CompactSpec spec);

    /// <summary>DeleteFilesInRange(begin, end, include_end = true).</summary>
    void DeleteFilesInRange(string cf, byte[] begin, byte[] end);

    // Transactions (only when the DB was opened with DbSpec.TransactionDb).
    void BeginTransaction(string name);

    void TransactionWrite(Op op);

    void PrepareTransaction(string name);

    /// <summary>Commits a transaction prepared in this session or recovered as prepared.</summary>
    void CommitTransaction(string name);

    void RollbackTransaction(string name);

    IReadOnlyList<string> PreparedTransactionNames();

    void CreateColumnFamily(string cf);

    void DropColumnFamily(string cf);

    /// <summary>Writes an external SST file with <paramref name="cf"/>'s options; entries are Sst* ops in key order.</summary>
    void WriteSst(string cf, string path, IReadOnlyList<Op> entries);

    void Ingest(string cf, IReadOnlyList<string> files, IngestFlags flags);

    void Checkpoint(string dir);

    /// <summary>Adds a backup of the DB to the BackupEngine directory <paramref name="backupDir"/> (created if missing).</summary>
    void Backup(string backupDir, bool flushBeforeBackup);

    /// <summary>Restores the latest backup of <paramref name="backupDir"/> into <paramref name="targetDir"/> (DB and WAL dir).</summary>
    void RestoreLatestBackup(string backupDir, string targetDir);

    void TakeSnapshot(long id);

    void ReleaseSnapshot(long id);

    /// <summary>All entries of <paramref name="cf"/> in key order (optionally as of a snapshot).</summary>
    IEnumerable<KeyValuePair<byte[], byte[]>> Scan(string cf, long? snapshot);

    /// <summary>Point lookup (optionally as of a snapshot); null when not found.</summary>
    byte[]? Get(string cf, byte[] key, long? snapshot);

    /// <summary>Number of table files per level of <paramref name="cf"/>.</summary>
    int[] FilesPerLevel(string cf);
}
