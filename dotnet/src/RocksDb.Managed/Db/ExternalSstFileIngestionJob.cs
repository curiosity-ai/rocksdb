using RocksDb.Managed.Env;
using RocksDb.Managed.Table;

namespace RocksDb.Managed.Db;

/// <summary>An external file being ingested (IngestedFileInfo, db/external_sst_file_ingestion_job.h).</summary>
internal sealed class IngestedFileInfo
{
    public required string ExternalFilePath;
    public string InternalFilePath = "";
    public ulong FileNumber;
    public ulong FileSize;
    public byte[] SmallestInternalKey = [];
    public byte[] LargestInternalKey = [];
    public ulong OriginalSeqno;
    /// <summary>File offset of the global seqno value in the properties block (0: none), for write_global_seqno.</summary>
    public ulong GlobalSeqnoOffset;
    public ulong NumEntries;
    public ulong NumRangeDeletions;
    /// <summary>External SST version: 1, 2 (SstFileWriter) or 0 (a DB-generated file, allow_db_generated_files).</summary>
    public uint Version;
    public ulong TailSize;
    public byte[] MinTimestamp = [];
    public byte[] MaxTimestamp = [];
    /// <summary>user_defined_timestamps_persisted of the file (false: keys stored without timestamps, read padded).</summary>
    public bool UserDefinedTimestampsPersisted = true;
    /// <summary>Sequence number bounds of a DB-generated file (allow_db_generated_files; GetSeqnoBoundaryForFile).</summary>
    public ulong SmallestSeqno = DbFormat.MaxSequenceNumber;
    public ulong LargestSeqno = DbFormat.MaxSequenceNumber;
    public string DbId = "";
    public string DbSessionId = "";
    public ulong OrigFileNumber;
    public ulong AssignedSeqno;
    public int PickedLevel;
    public bool Copied;

    public ReadOnlySpan<byte> SmallestUserKey => DbFormat.ExtractUserKey(SmallestInternalKey);
    public ReadOnlySpan<byte> LargestUserKey => DbFormat.ExtractUserKey(LargestInternalKey);
}

/// <summary>
/// Port of ExternalSstFileIngestionJob (db/external_sst_file_ingestion_job.cc): validates external files
/// (GetIngestedFileInfo / SanityCheckTableProperties), copies or links them into the DB (Prepare), divides them into
/// batches of non-overlapping files (DivideInputFilesIntoBatches), picks the level and global sequence number of every
/// file batch by batch (AssignLevelsForOneBatch / AssignLevelAndSeqnoForIngestedFile, or CheckLevelForIngestedBehindFile
/// for ingest_behind) and builds the VersionEdit (Run). The caller (DbImpl.Ingest) owns the write-thread / flush /
/// LogAndApply orchestration (DBImpl::IngestExternalFiles).
/// <para>
/// Supported: overlapping files in one call (later files in the argument list win: each batch of mutually
/// non-overlapping files is placed above the previous one and every file gets its own sequence number),
/// ingest_behind (last level, sequence number 0; requires cf_allow_ingest_behind or the deprecated
/// allow_ingest_behind), allow_db_generated_files (original sequence numbers kept; no overlap with the DB allowed) and
/// the legacy write_global_seqno (removed upstream in 11.12, see <see cref="IngestExternalFileOptions.WriteGlobalSeqno"/>).
/// Not ported: atomic_replace_range, file checksum generation, PreparedFileInfo, prefetch/temperature hints.
/// </para>
/// </summary>
internal sealed class ExternalSstFileIngestionJob
{
    private readonly DbImpl _db;
    private readonly ColumnFamilyData _cfd;
    private readonly IngestExternalFileOptions _options;
    private readonly IFileSystem _fs;
    /// <summary>files_to_ingest_, in argument order (later files win when files overlap).</summary>
    private readonly List<IngestedFileInfo> _files = new();
    /// <summary>file_batches_to_ingest_: older batches first.</summary>
    private readonly List<List<IngestedFileInfo>> _batches = new();

    public ExternalSstFileIngestionJob(DbImpl db, ColumnFamilyData cfd, IngestExternalFileOptions options)
    {
        _db = db;
        _cfd = cfd;
        _options = options;
        _fs = db.FileSystem;
    }

    public IReadOnlyList<IngestedFileInfo> Files => _files;
    public bool FilesOverlap { get; private set; }

    /// <summary>
    /// max_assigned_seqno_: the largest sequence number used by the ingested files (assigned global seqnos, or the
    /// largest seqno of DB-generated files); 0 if none. The DB's last sequence is raised to it after LogAndApply.
    /// </summary>
    public ulong MaxAssignedSeqno { get; private set; }

    /// <summary>
    /// Port of ExternalSstFileIngestionJob::Prepare: reads and validates every file, detects overlapping files, then
    /// links/copies them into the DB directory under new file numbers (registered as pending outputs).
    /// </summary>
    public async ValueTask PrepareAsync(IReadOnlyList<string> externalFiles, bool synchronous, CancellationToken ct)
    {
        var paths = new HashSet<string>(StringComparer.Ordinal);
        foreach (string path in externalFiles)
        {
            if (!paths.Add(path)) ThrowHelper.InvalidArgument("External files should be distinct: duplicate " + path);
            FileInfoResult f = GetIngestedFileInfo(path);
            // Files generated in another DB or CF may have a different column family ID, so we let it pass here.
            if (f.CfId != TableProperties.UnknownColumnFamily && f.CfId != _cfd.Id && !_options.AllowDbGeneratedFiles)
            {
                ThrowHelper.InvalidArgument("External file column family id don't match");
            }
            _files.Add(f.Info);
        }
        if (_files.Count == 0) ThrowHelper.InvalidArgument("The list of files is empty");
        FilesOverlap = ComputeFilesOverlap();
        if (FilesOverlap)
        {
            if (_options.IngestBehind) ThrowHelper.NotSupported("Files with overlapping ranges cannot be ingested with ingestion behind mode.");
            // Overlapping files need at least two different sequence numbers. If settings disables global seqno,
            // ingestion will fail anyway, so fail fast in prepare.
            if (!_options.AllowGlobalSeqNo && !_options.AllowDbGeneratedFiles)
            {
                ThrowHelper.InvalidArgument("Global seqno is required, but disabled (because external files key range overlaps).");
            }
            if (_cfd.TimestampSize > 0)
            {
                ThrowHelper.NotSupported("Files with overlapping ranges cannot be ingested to column family with user-defined timestamp enabled.");
            }
        }

        // Copy/Move external files into DB
        foreach (IngestedFileInfo f in _files)
        {
            f.FileNumber = _db.ReserveFileNumberForIngestion();
            string pathInsideDb = FileNames.TableFileName(_db.DbPaths, f.FileNumber, 0);
            bool linked = false;
            if (_options.MoveFiles || _options.LinkFiles)
            {
                try
                {
                    _fs.LinkFile(f.ExternalFilePath, pathInsideDb);
                    linked = true;
                }
                catch (RocksDbException e) when (e.Code == StatusCode.NotSupported && _options.FailedMoveFallBackToCopy)
                {
                    // Original file is on a different FS, use copy instead of hard linking.
                }
            }
            if (!linked)
            {
                await _db.CopyFileAsync(f.ExternalFilePath, pathInsideDb, 0, synchronous, ct).ConfigureAwait(false);
                f.Copied = true;
            }
            f.InternalFilePath = pathInsideDb;
        }
        _fs.SyncDirectory(_db.DbPaths[0]);
        DivideInputFilesIntoBatches();
    }

    /// <summary>ExternalSstFileIngestionJob::ComputeFilesOverlap: sort copies of the ranges and check adjacent pairs.</summary>
    private bool ComputeFilesOverlap()
    {
        if (_files.Count <= 1) return false;
        var sorted = new List<IngestedFileInfo>(_files);
        InternalKeyComparator icmp = _cfd.Icmp;
        sorted.Sort((a, b) => VersionStorageInfo.SstableKeyCompare(icmp, a.SmallestInternalKey, b.SmallestInternalKey));
        for (int i = 0; i + 1 < sorted.Count; i++)
        {
            // ExternalFileRangeChecker::Overlaps(known_sorted = true)
            if (VersionStorageInfo.SstableKeyCompare(icmp, sorted[i].LargestInternalKey, sorted[i + 1].SmallestInternalKey) >= 0) return true;
        }
        return false;
    }

    /// <summary>
    /// Port of ExternalSstFileIngestionJob::DivideInputFilesIntoBatches: without overlap all files form one batch;
    /// otherwise, in argument order, a file overlapping the current batch's range starts a new batch.
    /// </summary>
    private void DivideInputFilesIntoBatches()
    {
        if (!FilesOverlap)
        {
            _batches.Add(new List<IngestedFileInfo>(_files));
            return;
        }
        InternalKeyComparator icmp = _cfd.Icmp;
        var batch = new List<IngestedFileInfo>();
        byte[]? batchSmallest = null, batchLargest = null;
        foreach (IngestedFileInfo f in _files)
        {
            if (batch.Count > 0
                && VersionStorageInfo.SstableKeyCompare(icmp, batchLargest!, f.SmallestInternalKey) >= 0
                && VersionStorageInfo.SstableKeyCompare(icmp, batchSmallest!, f.LargestInternalKey) <= 0)
            {
                _batches.Add(batch);
                batch = new List<IngestedFileInfo>();
                batchSmallest = batchLargest = null;
            }
            batch.Add(f);
            // ExternalFileRangeChecker::MaybeUpdateRange
            if (batchSmallest is null || VersionStorageInfo.SstableKeyCompare(icmp, f.SmallestInternalKey, batchSmallest) < 0) batchSmallest = f.SmallestInternalKey;
            if (batchLargest is null || VersionStorageInfo.SstableKeyCompare(icmp, f.LargestInternalKey, batchLargest) > 0) batchLargest = f.LargestInternalKey;
        }
        _batches.Add(batch);
    }

    private readonly record struct FileInfoResult(IngestedFileInfo Info, ulong CfId);

    /// <summary>
    /// Port of ExternalSstFileIngestionJob::GetIngestedFileInfo / GetIngestedFileInfoFromFile /
    /// SanityCheckTableProperties.
    /// </summary>
    private FileInfoResult GetIngestedFileInfo(string path)
    {
        var info = new IngestedFileInfo { ExternalFilePath = path };
        info.FileSize = (ulong)_fs.GetFileSize(path);
        bool dbGenerated = _options.AllowDbGeneratedFiles;
        // Read the raw keys: the global sequence number must not be applied while validating.
        TableReaderOptions ro = _cfd.ReaderOptions with { LargestSeqno = BlockIter.DisableGlobalSequenceNumber, BlockCache = NoBlockCache.Instance };
        using BlockBasedTableReader reader = BlockBasedTableReader.Open(_fs.OpenRandomAccess(path), ro);
        TableProperties props = reader.Properties;
        SanityCheckTableProperties(props, info);
        info.NumEntries = props.NumEntries - props.NumRangeDeletions;
        info.NumRangeDeletions = props.NumRangeDeletions;
        if (props.NumEntries == 0 && props.NumRangeDeletions == 0) ThrowHelper.InvalidArgument("File contain no entries");
        IReadOnlyDictionary<string, byte[]> uprops = props.UserCollectedProperties;
        if (uprops.TryGetValue(TablePropertiesNames.TimestampMin, out byte[]? tsMin)) info.MinTimestamp = tsMin;
        if (uprops.TryGetValue(TablePropertiesNames.TimestampMax, out byte[]? tsMax)) info.MaxTimestamp = tsMax;
        info.TailSize = props.TailStartOffset > 0 && props.TailStartOffset <= info.FileSize ? info.FileSize - props.TailStartOffset : 0;
        info.DbId = props.DbId;
        info.DbSessionId = props.DbSessionId;
        info.OrigFileNumber = props.OriginalFileNumber;

        if (dbGenerated)
        {
            // We are ingesting a DB generated SST file for which we don't reassign sequence numbers. We need its
            // smallest sequence number and largest sequence number for FileMetaData.
            GetSeqnoBoundaryForFile(reader, props, info);
        }
        else if (props.KeyLargestSeqno != TableProperties.Unknown && props.KeyLargestSeqno > 0)
        {
            // UINT64_MAX means unknown and the file is generated before table property `key_largest_seqno` is introduced.
            ThrowHelper.Corruption("External file has non zero largest sequence number " + props.KeyLargestSeqno);
        }

        byte[]? smallest = null, largest = null;
        using (BlockBasedTableIterator it = reader.NewIterator())
        {
            if (_options.VerifyChecksumsBeforeIngest)
            {
                byte[]? prevBuf = null;
                for (it.SeekToFirst(); it.Valid; it.Next())
                {
                    CheckKey(it.Key, dbGenerated);
                    if (prevBuf is not null && _cfd.Icmp.Compare(prevBuf, it.Key) >= 0) ThrowHelper.Corruption("External file has keys out of order: " + path);
                    prevBuf = it.Key.ToArray();
                }
            }
            it.SeekToFirst();
            if (it.Valid)
            {
                CheckKey(it.Key, dbGenerated);
                smallest = it.Key.ToArray();
                it.SeekToLast();
                CheckKey(it.Key, dbGenerated);
                largest = it.Key.ToArray();
            }
        }
        if (reader.HasRangeTombstones)
        {
            using BlockIter rit = reader.NewRangeTombstoneIterator()!;
            for (rit.SeekToFirst(); rit.Valid; rit.Next())
            {
                if (!DbFormat.TryParseInternalKey(rit.Key, out ParsedInternalKey pk)) ThrowHelper.Corruption("external file corrupted range tombstone: " + path);
                if (pk.Sequence != 0 && !dbGenerated) ThrowHelper.Corruption("External file has a range deletion with non zero sequence number.");
                ReadOnlySpan<byte> start = rit.Key[..pk.UserKeyLength];
                // RangeTombstone(key, value).SerializeKey() / SerializeEndKey(): the start keeps the tombstone's seqno, the
                // end is the exclusive sentinel (kMaxSequenceNumber, kTypeRangeDeletion).
                byte[] startKey = new InternalKey(start, pk.Sequence, EntryType.RangeDeletion).Encoded.ToArray();
                // With user-defined timestamps the end key carries the maximum timestamp (files without persisted
                // timestamps store the end key without one: it is appended).
                int tsSize = _cfd.TimestampSize;
                int padded = reader.PadTimestampSize;
                byte[] endUser = new byte[rit.Value.Length + padded];
                rit.Value.CopyTo(endUser);
                byte[] endKey = new InternalKey(endUser, DbFormat.MaxSequenceNumber, EntryType.RangeDeletion).Encoded.ToArray();
                if (tsSize > 0 && endUser.Length >= tsSize) endKey.AsSpan(endUser.Length - tsSize, tsSize).Fill(0xff);
                if (smallest is null || _cfd.Icmp.Compare(startKey, smallest) < 0) smallest = startKey;
                if (largest is null || _cfd.Icmp.Compare(endKey, largest) > 0) largest = endKey;
            }
        }
        if (smallest is null || largest is null) ThrowHelper.Corruption("Generated table have corrupted keys");
        info.SmallestInternalKey = smallest!;
        info.LargestInternalKey = largest!;
        return new FileInfoResult(info, props.ColumnFamilyId);
    }

    /// <summary>Port of ExternalSstFileIngestionJob::SanityCheckTableProperties.</summary>
    private void SanityCheckTableProperties(TableProperties props, IngestedFileInfo info)
    {
        IReadOnlyDictionary<string, byte[]> uprops = props.UserCollectedProperties;
        // Get table version
        if (!uprops.TryGetValue(TablePropertiesNames.ExternalSstFileVersion, out byte[]? versionBytes))
        {
            // 0 is special version for when a file from live DB does not have the version table property
            if (!_options.AllowDbGeneratedFiles) ThrowHelper.Corruption("External file version not found");
            info.Version = 0;
        }
        else
        {
            if (versionBytes.Length < 4) ThrowHelper.Corruption("External file version not found");
            info.Version = Util.Coding.DecodeFixed32(versionBytes);
        }
        bool hasSeqno = uprops.TryGetValue(TablePropertiesNames.ExternalSstFileGlobalSeqno, out byte[]? seqnoBytes);
        if (info.Version == 2)
        {
            // version 2 imply that we have global sequence number
            if (!hasSeqno || seqnoBytes!.Length < 8) ThrowHelper.Corruption("External file global sequence number not found");
            // Set the global sequence number
            info.OriginalSeqno = Util.Coding.DecodeFixed64(seqnoBytes!);
            info.GlobalSeqnoOffset = props.ExternalSstFileGlobalSeqnoOffset;
        }
        else if (info.Version == 1)
        {
            // SST file V1 should not have global seqno field
            info.OriginalSeqno = 0;
            if (_options.AllowBlockingFlush || _options.AllowGlobalSeqNo) ThrowHelper.InvalidArgument("External SST file V1 does not support global seqno");
        }
        else if (info.Version != 0)
        {
            ThrowHelper.InvalidArgument("External file version " + info.Version + " is not supported");
        }

        // ValidateUserDefinedTimestampsOptions(cf comparator, file comparator, cf persist flag, file persist flag): a
        // column family with timestamps in memtable only also takes files written without timestamps (they are then
        // marked as not persisting timestamps and read with the minimum timestamp).
        info.UserDefinedTimestampsPersisted = props.UserDefinedTimestampsPersisted != 0;
        if (props.ComparatorName.Length > 0)
        {
            if (props.ComparatorName == _cfd.UserComparator.Name)
            {
                if (info.UserDefinedTimestampsPersisted != _cfd.Options.PersistUserDefinedTimestamps && _cfd.TimestampSize > 0)
                {
                    ThrowHelper.InvalidArgument("Cannot toggle the persist_user_defined_timestamps flag for a column family with user-defined " +
                                                "timestamps feature enabled.");
                }
            }
            else if (_cfd.TimestampsInMemtableOnly && BlockBasedTableReader.TimestampsStripped(props, _cfd.UserComparator))
            {
                info.UserDefinedTimestampsPersisted = false; // kEnableUDT: mark_sst_file_has_no_udt
            }
            else if (!(_cfd.TimestampSize == 0 && !info.UserDefinedTimestampsPersisted
                       && props.ComparatorName == _cfd.UserComparator.Name + BlockBasedTableReader.UdtComparatorSuffix))
            {
                ThrowHelper.InvalidArgument("External file comparator " + props.ComparatorName + " does not match column family comparator " +
                                            _cfd.UserComparator.Name);
            }
        }
    }

    /// <summary>
    /// Port of ExternalSstFileIngestionJob::GetSeqnoBoundaryForFile: the key_smallest/largest_seqno table properties,
    /// or a scan of the point and range-deletion entries for older files.
    /// </summary>
    private static void GetSeqnoBoundaryForFile(BlockBasedTableReader reader, TableProperties props, IngestedFileInfo info)
    {
        bool hasLargest = props.KeyLargestSeqno != TableProperties.Unknown;
        if (hasLargest)
        {
            info.LargestSeqno = props.KeyLargestSeqno;
            if (props.KeyLargestSeqno == 0)
            {
                info.SmallestSeqno = 0;
                return;
            }
            if (props.KeySmallestSeqno != TableProperties.Unknown)
            {
                info.SmallestSeqno = props.KeySmallestSeqno;
                return;
            }
        }
        // For older SST files they may not be recorded in table properties, so we scan the file to find out.
        ulong smallest = DbFormat.MaxSequenceNumber, largestFromIter = 0;
        using (BlockBasedTableIterator it = reader.NewIterator())
        {
            for (it.SeekToFirst(); it.Valid; it.Next())
            {
                if (!DbFormat.TryParseInternalKey(it.Key, out ParsedInternalKey pk)) ThrowHelper.Corruption("Corrupted key in external file. ");
                smallest = Math.Min(smallest, pk.Sequence);
                largestFromIter = Math.Max(largestFromIter, pk.Sequence);
            }
        }
        if (props.NumRangeDeletions > 0 && reader.HasRangeTombstones)
        {
            using BlockIter rit = reader.NewRangeTombstoneIterator()!;
            for (rit.SeekToFirst(); rit.Valid; rit.Next())
            {
                if (!DbFormat.TryParseInternalKey(rit.Key, out ParsedInternalKey pk)) ThrowHelper.Corruption("Corrupted key in external file. ");
                smallest = Math.Min(smallest, pk.Sequence);
                largestFromIter = Math.Max(largestFromIter, pk.Sequence);
            }
        }
        info.SmallestSeqno = smallest;
        info.LargestSeqno = hasLargest ? props.KeyLargestSeqno : largestFromIter;
        if (info.LargestSeqno == DbFormat.MaxSequenceNumber) ThrowHelper.InvalidArgument("Unknown smallest seqno for db generated file.");
        if (info.SmallestSeqno == DbFormat.MaxSequenceNumber) ThrowHelper.InvalidArgument("Unknown largest seqno for db generated file.");
    }

    private static void CheckKey(ReadOnlySpan<byte> internalKey, bool dbGenerated)
    {
        if (!DbFormat.TryParseInternalKey(internalKey, out ParsedInternalKey pk)) ThrowHelper.Corruption("Corrupted key in external file. ");
        if (dbGenerated) return; // Allows files with any sequence numbers (and DB value types) to be ingested.
        if (pk.Sequence != 0) ThrowHelper.Corruption("External file has non zero sequence number");
        if (pk.Type is not (EntryType.Value or EntryType.Merge or EntryType.Deletion or EntryType.SingleDeletion or EntryType.DeletionWithTimestamp
                or EntryType.WideColumnEntity))
        {
            ThrowHelper.Corruption("External file has a key with unsupported type " + (byte)pk.Type);
        }
    }

    /// <summary>True if the memtables of <paramref name="sv"/> may hold keys in the range of any file (IngestedFilesOverlapWithMemtable).</summary>
    public bool OverlapsWithMemtables(SuperVersion sv)
    {
        foreach (IngestedFileInfo f in _files)
        {
            if (MemOverlaps(sv.Mem, f)) return true;
            foreach (MemTable m in sv.Imm)
            {
                if (MemOverlaps(m, f)) return true;
            }
        }
        return false;
    }

    private bool MemOverlaps(MemTable mem, IngestedFileInfo f)
    {
        if (mem.IsEmpty) return false;
        if (mem.NumRangeDeletes > 0) return true; // conservative
        MemTableIterator it = mem.NewIterator();
        it.Seek(SeekKey(f));
        return it.Valid && _cfd.UserComparator.CompareWithoutTimestamp(DbFormat.ExtractUserKey(it.Key), f.LargestUserKey) <= 0;
    }

    /// <summary>
    /// Seek target for the file's smallest key: newest possible entry of it (with user-defined timestamps every version
    /// of the key, whatever its timestamp, counts as an overlap: the maximum timestamp sorts first).
    /// </summary>
    private byte[] SeekKey(IngestedFileInfo f)
    {
        byte[] seek = new InternalKey(f.SmallestUserKey, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek).Encoded.ToArray();
        int ts = _cfd.TimestampSize;
        if (ts > 0 && f.SmallestUserKey.Length >= ts) seek.AsSpan(f.SmallestUserKey.Length - ts, ts).Fill(0xff);
        return seek;
    }

    /// <summary>
    /// Port of ExternalSstFileIngestionJob::Run: assigns levels and sequence numbers batch by batch (older batches
    /// deeper in the LSM tree) and returns the edit. REQUIRES: the caller is the unbatched write leader (no concurrent
    /// writes) and the memtables do not overlap the files. <see cref="MaxAssignedSeqno"/> tells which sequence numbers
    /// were used.
    /// </summary>
    public VersionEdit Run(SuperVersion sv, ulong lastSeqno, bool hasSnapshots)
    {
        // We need to assign a global sequence number to all the files even if the don't overlap with any ranges since
        // we have snapshots.
        bool forceGlobalSeqno = _options.SnapshotConsistency && hasSnapshots;
        var edit = new VersionEdit { ColumnFamily = _cfd.Id };
        MaxAssignedSeqno = 0;
        int? prevBatchUppermostLevel = null;
        // Batches at the front contain older updates and are placed deeper in the LSM tree than later overlapping batches.
        foreach (List<IngestedFileInfo> batch in _batches)
        {
            int batchUppermostLevel = AssignLevelsForOneBatch(sv.Current.Storage, batch, forceGlobalSeqno, ref lastSeqno, prevBatchUppermostLevel, edit);
            prevBatchUppermostLevel = batchUppermostLevel;
        }
        return edit;
    }

    /// <summary>Port of ExternalSstFileIngestionJob::AssignLevelsForOneBatch. Returns the batch's uppermost level.</summary>
    private int AssignLevelsForOneBatch(VersionStorageInfo vstorage, List<IngestedFileInfo> batch, bool forceGlobalSeqno, ref ulong lastSeqno,
        int? prevBatchUppermostLevel, VersionEdit edit)
    {
        int batchUppermostLevel = int.MaxValue;
        ulong now = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        foreach (IngestedFileInfo f in batch)
        {
            ulong assignedSeqno = 0;
            if (_options.IngestBehind) CheckLevelForIngestedBehindFile(vstorage, f);
            else assignedSeqno = AssignLevelAndSeqnoForIngestedFile(vstorage, forceGlobalSeqno, lastSeqno, f, prevBatchUppermostLevel);

            // If any ingested file overlaps with the DB, it will fail here.
            if (_options.AllowDbGeneratedFiles && assignedSeqno != 0)
            {
                ThrowHelper.InvalidArgument("An ingested file overlaps with existing data in the DB and has been assigned a non-zero sequence " +
                                            "number, which is not allowed when 'allow_db_generated_files' is enabled.");
            }
            // Modify the smallest/largest internal key to include the sequence number that we just learned. Only overwrite
            // sequence number zero. There could be a nonzero sequence number already to indicate a range tombstone's
            // exclusive endpoint.
            if (assignedSeqno != 0)
            {
                f.SmallestInternalKey = WithSeqnoIfZero(f.SmallestInternalKey, assignedSeqno);
                f.LargestInternalKey = WithSeqnoIfZero(f.LargestInternalKey, assignedSeqno);
            }
            AssignGlobalSeqnoForIngestedFile(f, assignedSeqno);
            System.Diagnostics.Debug.Assert(assignedSeqno == 0 || assignedSeqno == lastSeqno + 1);
            if (assignedSeqno > lastSeqno) lastSeqno = assignedSeqno;
            MaxAssignedSeqno = Math.Max(MaxAssignedSeqno, assignedSeqno);

            ulong smallestSeqno = f.AssignedSeqno, largestSeqno = f.AssignedSeqno;
            if (_options.AllowDbGeneratedFiles)
            {
                smallestSeqno = f.SmallestSeqno;
                largestSeqno = f.LargestSeqno;
                MaxAssignedSeqno = Math.Max(MaxAssignedSeqno, f.LargestSeqno);
            }
            var meta = new FileMetaData
            {
                FileNumber = f.FileNumber,
                PathId = 0,
                FileSize = f.FileSize,
                Smallest = f.SmallestInternalKey,
                Largest = f.LargestInternalKey,
                SmallestSeqno = smallestSeqno,
                LargestSeqno = largestSeqno,
                // We use the import time as the ancestor time. This is the time the data is written to the database.
                OldestAncestorTime = now,
                FileCreationTime = now,
                // orders files ingested to L0
                EpochNumber = _options.IngestBehind ? FileMetaData.ReservedEpochNumberForFileIngestedBehind : _db.NewEpochNumberForIngestion(_cfd),
                TailSize = f.TailSize,
                MinTimestamp = f.MinTimestamp,
                MaxTimestamp = f.MaxTimestamp,
                UserDefinedTimestampsPersisted = f.UserDefinedTimestampsPersisted,
                MarkedForCompaction = f.NumRangeDeletions == 1 && f.NumEntries == 0,
            };
            if (f.DbId.Length > 0 && f.DbSessionId.Length > 0)
            {
                // GetSstInternalUniqueId(db_id, db_session_id, orig_file_number)
                Cache.OffsetableCacheKey.GetSstInternalUniqueId(f.DbId, f.DbSessionId, f.OrigFileNumber, out ulong id0, out ulong id1);
                meta.UniqueId = new UniqueId64x2(id0, id1);
            }
            edit.AddFile(f.PickedLevel, meta);
            batchUppermostLevel = Math.Min(batchUppermostLevel, f.PickedLevel);
        }
        return batchUppermostLevel;
    }

    /// <summary>Port of ExternalSstFileIngestionJob::AssignLevelAndSeqnoForIngestedFile. Returns the assigned seqno.</summary>
    private ulong AssignLevelAndSeqnoForIngestedFile(VersionStorageInfo vstorage, bool forceGlobalSeqno, ulong lastSeqno, IngestedFileInfo f,
        int? prevBatchUppermostLevel)
    {
        ulong assignedSeqno = 0;
        int numLevels = _cfd.NumLevels;
        bool mustAssignToL0 = prevBatchUppermostLevel == 0 || _cfd.Options.CompactionStyle == CompactionStyle.Fifo;
        if (forceGlobalSeqno || (!_options.AllowDbGeneratedFiles && (FilesOverlap || mustAssignToL0)))
        {
            assignedSeqno = lastSeqno + 1;
            if (mustAssignToL0)
            {
                f.PickedLevel = 0;
                if (_options.FailIfNotBottommostLevel && numLevels > 1)
                {
                    throw new RocksDbException(StatusCode.TryAgain,
                        "Operation failed. Try again.: Files cannot be ingested to Lmax. Please make sure key range of Lmax does not overlap with files to ingest.");
                }
                return assignedSeqno;
            }
        }

        bool overlapWithDb = false;
        int targetLevel = 0;
        int assignedLevelExclusiveEnd = mustAssignToL0 ? 0 : prevBatchUppermostLevel ?? numLevels;
        // When ingesting db generated files, we require that ingested files do not overlap with any file in the DB. So
        // we need to check all levels.
        int overlapCheckingExclusiveEnd = _options.AllowDbGeneratedFiles ? numLevels : assignedLevelExclusiveEnd;
        overlapCheckingExclusiveEnd = Math.Min(overlapCheckingExclusiveEnd, vstorage.NumLevels);
        for (int lvl = 0; lvl < overlapCheckingExclusiveEnd; lvl++)
        {
            if (lvl > 0 && lvl < vstorage.BaseLevel) continue;
            if (_db.RangeOverlapsRunningCompaction(_cfd, f.SmallestUserKey, f.LargestUserKey, lvl))
            {
                // We must use L0 or any level higher than `lvl` to be able to overwrite the compaction output keys that
                // we overlap with in this level, We also need to assign this file a seqno to overwrite the compaction
                // output keys in level `lvl`
                overlapWithDb = true;
                break;
            }
            if (vstorage.NumLevelFiles(lvl) > 0 && OverlapWithLevel(vstorage, lvl, f))
            {
                // We must use L0 or any level higher than `lvl` to be able to overwrite the keys that we overlap with
                // in this level, We also need to assign this file a seqno to overwrite the existing keys in level `lvl`
                overlapWithDb = true;
                break;
            }
            // We don't overlap with any keys in this level, but we still need to check if our file can fit in it
            if (lvl < assignedLevelExclusiveEnd && IngestedFileFitInLevel(vstorage, lvl, f)) targetLevel = lvl;
        }
        if (_options.FailIfNotBottommostLevel && targetLevel < numLevels - 1)
        {
            throw new RocksDbException(StatusCode.TryAgain,
                "Operation failed. Try again.: Files cannot be ingested to Lmax. Please make sure key range of Lmax and ongoing compaction's " +
                "output to Lmax does not overlap with files to ingest. Input files overlapping with each other can cause some file to be " +
                "assigned to non Lmax level.");
        }
        f.PickedLevel = targetLevel;
        if (overlapWithDb)
        {
            if (_cfd.TimestampSize > 0)
            {
                ThrowHelper.InvalidArgument("Column family enables user-defined timestamps, please make sure the key range (without timestamp) of " +
                                            "external file does not overlap with key range (without timestamp) in the db");
            }
            if (assignedSeqno == 0) assignedSeqno = lastSeqno + 1;
        }
        return assignedSeqno;
    }

    /// <summary>Port of ExternalSstFileIngestionJob::CheckLevelForIngestedBehindFile.</summary>
    private void CheckLevelForIngestedBehindFile(VersionStorageInfo vstorage, IngestedFileInfo f)
    {
        // First, check if new files fit in the last level
        int lastLvl = _cfd.NumLevels - 1;
        if (!IngestedFileFitInLevel(vstorage, lastLvl, f)) ThrowHelper.InvalidArgument("Can't ingest_behind file as it doesn't fit at the last level!");
        // Second, check if despite cf_allow_ingest_behind=true we still have 0 seqnums at some upper level
        for (int lvl = 0; lvl < lastLvl; lvl++)
        {
            foreach (FileMetaData file in vstorage.LevelFiles(lvl))
            {
                if (file.SmallestSeqno == 0)
                {
                    ThrowHelper.InvalidArgument("Can't ingest_behind file as despite cf_allow_ingest_behind=true there are files with 0 seqno in " +
                                                "database at upper levels!");
                }
            }
        }
        f.PickedLevel = lastLvl;
    }

    /// <summary>Version::OverlapWithLevelIterator: does level <paramref name="level"/> contain a key in the file's range?</summary>
    private bool OverlapWithLevel(VersionStorageInfo vstorage, int level, IngestedFileInfo f)
    {
        // AfterFile / BeforeFile / OverlapWithIterator compare user keys without timestamps.
        Comparator ucmp = _cfd.UserComparator;
        byte[] seek = SeekKey(f);
        foreach (FileMetaData lf in vstorage.LevelFiles(level))
        {
            if (ucmp.CompareWithoutTimestamp(DbFormat.ExtractUserKey(lf.Largest), f.SmallestUserKey) < 0 ||
                ucmp.CompareWithoutTimestamp(DbFormat.ExtractUserKey(lf.Smallest), f.LargestUserKey) > 0)
            {
                continue;
            }
            if (_cfd.TableCache.GetRangeTombstones(lf) is not null) return true; // conservative
            using Iterators.InternalIterator it = _cfd.TableCache.NewIterator(lf);
            it.Seek(seek);
            if (it.Valid && ucmp.CompareWithoutTimestamp(DbFormat.ExtractUserKey(it.Key), f.LargestUserKey) <= 0) return true;
        }
        return false;
    }

    /// <summary>Port of ExternalSstFileIngestionJob::IngestedFileFitInLevel (VersionStorageInfo::OverlapInLevel).</summary>
    private bool IngestedFileFitInLevel(VersionStorageInfo vstorage, int level, IngestedFileInfo f)
    {
        if (level == 0) return true; // Files can always fit in L0
        if (level >= vstorage.NumLevels) return false;
        Comparator ucmp = _cfd.UserComparator;
        foreach (FileMetaData lf in vstorage.LevelFiles(level))
        {
            if (ucmp.CompareWithoutTimestamp(DbFormat.ExtractUserKey(lf.Largest), f.SmallestUserKey) < 0 ||
                ucmp.CompareWithoutTimestamp(DbFormat.ExtractUserKey(lf.Smallest), f.LargestUserKey) > 0)
            {
                continue;
            }
            return false; // File overlap with another files in this level, we cannot add it to this level
        }
        // File did not overlap with level files, nor compaction output
        return true;
    }

    /// <summary>
    /// Port of ExternalSstFileIngestionJob::AssignGlobalSeqnoForIngestedFile, including the legacy write_global_seqno
    /// path removed upstream by #15287 (11.12): the assigned seqno is written in place over the fixed64 value of
    /// rocksdb.external_sst_file.global_seqno in the properties block (the block checksum is not updated, exactly like
    /// the old ingestion; readers verify it against the zero placeholder) and the file is synced. A file system without
    /// random writes skips the rewrite (NotSupported is ignored, like NewRandomRWFile failing with NotSupported).
    /// </summary>
    private void AssignGlobalSeqnoForIngestedFile(IngestedFileInfo f, ulong seqno)
    {
        if (f.OriginalSeqno != seqno)
        {
            if (!_options.AllowGlobalSeqNo) ThrowHelper.InvalidArgument("Global seqno is required, but disabled");
            if (_options.WriteGlobalSeqno && f.GlobalSeqnoOffset == 0)
            {
                ThrowHelper.InvalidArgument("Trying to set global seqno for a file that don't have a global seqno field");
            }
            if (f.OriginalSeqno != 0 && !_options.WriteGlobalSeqno)
            {
                ThrowHelper.InvalidArgument("External file global seqno does not match assigned seqno");
            }
            if (_options.WriteGlobalSeqno)
            {
                Span<byte> seqnoVal = stackalloc byte[8];
                Util.Coding.EncodeFixed64(seqnoVal, seqno);
                try
                {
                    _fs.WriteAt(f.InternalFilePath, (long)f.GlobalSeqnoOffset, seqnoVal);
                }
                catch (RocksDbException e) when (e.Code == StatusCode.NotSupported)
                {
                    // The file system does not support random writes: the seqno is recorded in the MANIFEST only.
                }
            }
        }
        f.AssignedSeqno = seqno;
    }

    private static byte[] WithSeqnoIfZero(byte[] internalKey, ulong seqno)
    {
        DbFormat.UnpackSequenceAndType(DbFormat.ExtractFooter(internalKey), out ulong s, out EntryType type);
        if (s != 0) return internalKey;
        return new InternalKey(DbFormat.ExtractUserKey(internalKey), seqno, type).Encoded.ToArray();
    }

    /// <summary>Deletes the files copied/linked into the DB (failed ingestion).</summary>
    public void DeleteInternalFiles()
    {
        foreach (IngestedFileInfo f in _files)
        {
            if (f.InternalFilePath.Length == 0) continue;
            try { _fs.DeleteFile(f.InternalFilePath); }
            catch (Exception e) when (e is IOException or RocksDbException) { }
        }
    }

    /// <summary>With move_files, the original links are removed after a successful ingestion.</summary>
    public void DeleteExternalLinks()
    {
        if (!_options.MoveFiles) return;
        foreach (IngestedFileInfo f in _files)
        {
            if (f.Copied) continue;
            try { _fs.DeleteFile(f.ExternalFilePath); }
            catch (Exception e) when (e is IOException or RocksDbException) { }
        }
    }
}
