namespace RocksDb.Managed;

/// <summary>
/// One ingestion request with per-file extras (rocksdb::IngestExternalFileArg, include/rocksdb/options.h): the files,
/// their options, optional checksums and the storage temperature hint. Used with
/// <see cref="RocksDatabase.IngestExternalFiles(ColumnFamily, IngestExternalFileArg)"/>.
/// </summary>
public sealed class IngestExternalFileArg
{
    /// <summary>external_files: SST files to ingest.</summary>
    public IReadOnlyList<string> ExternalFiles { get; init; } = [];
    public IngestExternalFileOptions Options { get; init; } = new();
    /// <summary>
    /// files_checksums: the whole-file checksum of each file (as the DB's <see cref="DbOptions.FileChecksumGenFactory"/>
    /// computes it), in the order of <see cref="ExternalFiles"/>; empty for none. Ignored without a checksum factory.
    /// </summary>
    public IReadOnlyList<byte[]> FilesChecksums { get; init; } = [];
    /// <summary>files_checksum_func_names: the checksum function name of each entry of <see cref="FilesChecksums"/>.</summary>
    public IReadOnlyList<string> FilesChecksumFuncNames { get; init; } = [];
    /// <summary>
    /// file_temperature: a hint as to the storage temperature of the files. Linked (moved) files are recorded with it in
    /// the MANIFEST; copies get the column family's last_level_temperature (ingest_behind / fail_if_not_bottommost_level)
    /// or default_write_temperature.
    /// </summary>
    public Temperature FileTemperature { get; init; } = Temperature.Unknown;
}

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
    /// <summary>
    /// verify_file_checksum (default true): with <see cref="DbOptions.FileChecksumGenFactory"/> set, the checksums
    /// provided in <see cref="IngestExternalFileArg.FilesChecksums"/> are verified against checksums generated from the
    /// files (Corruption on mismatch). When false and checksums are provided for every file, they are stored in the
    /// MANIFEST without reading the files (only the function name is checked against the factory).
    /// </summary>
    public bool VerifyFileChecksum { get; set; } = true;
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
