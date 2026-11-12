namespace RocksDb.Managed.Replication.Hosting;

/// <summary>
/// The largest payloads a host puts into one message of its streams. A transport with a message size limit reads them
/// to refuse, at start, a configuration whose every stream would fail.
/// </summary>
public interface IReplicationChunkSizes
{
    /// <summary>The most bytes of write batches in one message of the WAL stream.</summary>
    int MaxChunkBytes { get; }

    /// <summary>The most file bytes in one message of a snapshot.</summary>
    int SnapshotChunkBytes { get; }
}
