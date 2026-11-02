namespace RocksDb.Managed;

/// <summary>
/// Options for <see cref="RocksDatabase.IngestExternalFilesAsync(IReadOnlyList{string}, IngestExternalFileOptions, CancellationToken)"/>.
/// Port of rocksdb::IngestExternalFileOptions (include/rocksdb/options.h) with upstream defaults. Not persisted.
/// </summary>
public sealed class IngestExternalFileOptions
{
    /// <summary>Hard link the files into the DB instead of copying them (the originals are unlinked on success).</summary>
    public bool MoveFiles { get; set; }
    /// <summary>With <see cref="MoveFiles"/>, copy when hard linking is not possible (default true).</summary>
    public bool FailedMoveFallBackToCopy { get; set; } = true;
    /// <summary>
    /// Snapshots taken before the ingestion must not see the ingested keys (default true): with live snapshots the
    /// files get a new global sequence number.
    /// </summary>
    public bool SnapshotConsistency { get; set; } = true;
    /// <summary>Allow assigning a global sequence number to the files (default true); fail instead when false.</summary>
    public bool AllowGlobalSeqNo { get; set; } = true;
    /// <summary>Flush the memtable when it overlaps the ingested range (default true); fail instead when false.</summary>
    public bool AllowBlockingFlush { get; set; } = true;
    /// <summary>Ingest into the bottommost level with sequence number 0 (requires allow_ingest_behind). Not supported yet.</summary>
    public bool IngestBehind { get; set; }
    /// <summary>
    /// Write the assigned global sequence number into the file (legacy behavior; default false: the sequence number is
    /// recorded in the MANIFEST only). Not supported yet.
    /// </summary>
    public bool WriteGlobalSeqno { get; set; }
    /// <summary>Read every entry of the files (verifying block checksums) before ingesting them.</summary>
    public bool VerifyChecksumsBeforeIngest { get; set; }
    /// <summary>Fail if the files cannot be placed in the bottommost level.</summary>
    public bool FailIfNotBottommostLevel { get; set; }
}
