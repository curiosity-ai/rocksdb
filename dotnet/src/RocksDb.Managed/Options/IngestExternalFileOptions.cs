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
    /// <summary>Same as <see cref="MoveFiles"/> except that the input files are not unlinked (link_files).</summary>
    public bool LinkFiles { get; set; }
    /// <summary>
    /// Ingest the files into the last level with sequence number 0, below all existing data, so existing keys win
    /// (ingest_behind; back-filling historical data). Requires a column family created with
    /// <see cref="ColumnFamilyOptions.CfAllowIngestBehind"/> (or the deprecated <see cref="DbOptions.AllowIngestBehind"/>)
    /// before any write; not supported with user-defined timestamps or overlapping input files.
    /// </summary>
    public bool IngestBehind { get; set; }
    /// <summary>
    /// Legacy write_global_seqno (default false): also write the assigned global sequence number into the ingested
    /// file's properties block, as RocksDB versions before 5.16 require; otherwise the sequence number is recorded in
    /// the MANIFEST only. The value is overwritten in place without updating the block checksum, exactly like the
    /// removed upstream implementation (native RocksDB 11.12+ dropped the option but still reads such files). Skipped
    /// silently on file systems without random writes.
    /// </summary>
    public bool WriteGlobalSeqno { get; set; }
    /// <summary>Read every entry of the files (verifying block checksums) before ingesting them.</summary>
    public bool VerifyChecksumsBeforeIngest { get; set; }
    /// <summary>Fail if the files cannot be placed in the bottommost level.</summary>
    public bool FailIfNotBottommostLevel { get; set; }
    /// <summary>
    /// EXPERIMENTAL allow_db_generated_files: ingest SST files written by a live DB (e.g. a checkpoint) instead of
    /// <see cref="SstFileWriter"/>. Their column family id is not checked, their original sequence numbers are kept
    /// (the DB's last sequence is raised to the largest one) and they must not overlap any data of the column family
    /// (InvalidArgument otherwise). Use with <see cref="SnapshotConsistency"/> = false and
    /// <see cref="AllowBlockingFlush"/> = false; <see cref="LinkFiles"/> when the source DB is still running.
    /// </summary>
    public bool AllowDbGeneratedFiles { get; set; }
}
