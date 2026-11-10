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
    /// <summary>file_checksum / file_checksum_func_name recorded in the MANIFEST (unknown unless generated or provided).</summary>
    public byte[] FileChecksum = [];
    public string FileChecksumFuncName = FileMetaData.UnknownFileChecksumFuncName;
    /// <summary>file_temperature: the caller's hint, or the destination temperature of a copy.</summary>
    public Temperature Temperature;
    /// <summary>generated_for_ingestion: the range tombstone file of an atomic replace (no external file to unlink).</summary>
    public bool GeneratedForIngestion;

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
/// File checksums (file_checksum_gen_factory: generated, or provided and verified / trusted per verify_file_checksum)
/// and temperatures (the caller's hint for linked files, last_level_temperature / default_write_temperature for
/// copies) are recorded in the MANIFEST. atomic_replace_range (IngestExternalFileArg): the table files inside the range
/// are deleted in the same VersionEdit (AddFileToReplace), and with universal compaction a partial overlap is handled
/// by ingesting a generated range tombstone file below the new files (PrepareAtomicReplaceRangeTombstone /
/// ActivateAtomicReplaceRangeTombstone). The equivalent file-ingesting compactions are registered (and the replaced
/// files marked as being compacted) between <see cref="Run"/> and the end of LogAndApply (RegisterRange /
/// UnregisterRange). Files are opened with the column family's table factory (TableFactory::NewTableReader), so
/// PlainTable / CuckooTable external files are ingested into column families of that format (a file of another format
/// fails with "Bad table magic number"; the adaptive factory takes any), with upstream's validation: the largest key of
/// a plain table is found by a full scan (its iterator has no SeekToLast) and verify_checksums_before_ingest fails with
/// NotSupported for those formats (TableReader::VerifyChecksum). As upstream, plain and cuckoo files are then rejected
/// by the comparator check (their builders record no comparator name: InvalidArgument "... does not match existing
/// comparator "), and SstFileWriter cuckoo files already by the version check (the cuckoo builder runs no property
/// collectors: Corruption "External file version not found"). Not ported: PreparedFileInfo.
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
    /// <summary>Every file number reserved by this job (pending outputs until the ingestion ends).</summary>
    private readonly List<ulong> _reservedFileNumbers = new();

    // atomic_replace_range_: _hasReplaceRange false = no replacement; empty keys = the whole column family
    // (KeyRangeInfo::unset()).
    private bool _hasReplaceRange;
    private byte[] _replaceSmallest = [];
    private byte[] _replaceLargest = [];
    // atomic_replace_range_tombstone_ (prepared, not yet part of _files) / atomic_replace_range_tombstone_active_.
    private IngestedFileInfo? _tombstone;
    private bool _tombstoneActive;
    // files_to_replace_ (with their levels), files_to_replace_marked_.
    private readonly List<(int Level, FileMetaData File)> _filesToReplace = new();
    private bool _filesToReplaceMarked;
    // file_ingesting_compactions_.
    private readonly List<Compaction> _fileIngestingCompactions = new();

    public ExternalSstFileIngestionJob(DbImpl db, ColumnFamilyData cfd, IngestExternalFileOptions options)
    {
        _db = db;
        _cfd = cfd;
        _options = options;
        _fs = db.FileSystem;
    }

    public IReadOnlyList<IngestedFileInfo> Files => _files;

    /// <summary>File numbers reserved by this job (the caller releases them from the pending outputs at the end).</summary>
    public IReadOnlyList<ulong> ReservedFileNumbers => _reservedFileNumbers;

    /// <summary>Number of table files the edit of <see cref="Run"/> deletes (atomic_replace_range).</summary>
    public int NumFilesReplaced => _filesToReplace.Count;

    /// <summary>The generated range tombstone file was ingested (atomic replace with a partial overlap).</summary>
    public bool RangeTombstoneIngested => _tombstoneActive;
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
    public async ValueTask PrepareAsync(IReadOnlyList<string> externalFiles, IReadOnlyList<byte[]>? filesChecksums,
        IReadOnlyList<string>? filesChecksumFuncNames, Temperature fileTemperature, IngestReplaceRange? atomicReplaceRange, VersionStorageInfo vstorage,
        bool synchronous, CancellationToken ct)
    {
        // Like upstream, a path listed twice is not rejected: the copies are overlapping files with their own seqnos.
        foreach (string path in externalFiles)
        {
            FileInfoResult f = GetIngestedFileInfo(path);
            // Files generated in another DB or CF may have a different column family ID, so we let it pass here.
            if (f.CfId != TableProperties.UnknownColumnFamily && f.CfId != _cfd.Id && !_options.AllowDbGeneratedFiles)
            {
                ThrowHelper.InvalidArgument("External file column family id don't match");
            }
            // For temperature, first assume it matches provided hint.
            f.Info.Temperature = fileTemperature;
            _files.Add(f.Info);
        }
        if (_files.Count == 0) ThrowHelper.InvalidArgument("The list of files is empty");
        FilesOverlap = ComputeFilesOverlap();

        if (atomicReplaceRange is not null)
        {
            _hasReplaceRange = true;
            if (atomicReplaceRange.Start is { } start && atomicReplaceRange.Limit is { } limit)
            {
                if (_cfd.UserComparator.CompareWithoutTimestamp(start, limit) >= 0)
                {
                    ThrowHelper.InvalidArgument("Atomic replace range limit must be greater than start");
                }
                // User keys to internal keys (with timestamps): MaybeAddTimestampsToRange appends the maximum timestamp
                // to both bounds (the limit is exclusive).
                byte[] startTs = WithMaxTimestamp(start);
                byte[] limitTs = WithMaxTimestamp(limit);
                _replaceSmallest = new InternalKey(startTs, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek).Encoded.ToArray();
                // RangeTombstone(start, limit, kMaxSequenceNumber).SerializeEndKey()
                _replaceLargest = new InternalKey(limitTs, DbFormat.MaxSequenceNumber, EntryType.RangeDeletion).Encoded.ToArray();
                // Check files to ingest against replace range
                foreach (IngestedFileInfo f in _files)
                {
                    if (!ReplaceRangeContains(f.SmallestInternalKey, f.LargestInternalKey))
                    {
                        ThrowHelper.InvalidArgument("Atomic replace range does not contain all files");
                    }
                }
            }
        }

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
            _reservedFileNumbers.Add(f.FileNumber);
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
                // The destination of the copy will be ingested: "last level" temperature when the user requires
                // ingestion to the last level, else default_write_temperature (the level is only known in Run()).
                f.Temperature = _options.IngestBehind || _options.FailIfNotBottommostLevel
                    ? _cfd.Options.LastLevelTemperature
                    : _cfd.Options.DefaultWriteTemperature;
            }
            // Linking files is assumed not to cross temperatures: the hint stays.
            f.InternalFilePath = pathInsideDb;
        }
        if (CanUseAtomicReplaceRangeTombstone(atomicReplaceRange) && HasPartialOverlap(vstorage))
        {
            PrepareAtomicReplaceRangeTombstone(atomicReplaceRange!.Start!, atomicReplaceRange.Limit!);
        }
        _fs.SyncDirectory(_db.DbPaths[0]);
        GenerateAndVerifyChecksums(externalFiles, filesChecksums ?? [], filesChecksumFuncNames ?? []);
        DivideInputFilesIntoBatches();
    }

    private byte[] WithMaxTimestamp(byte[] userKey)
    {
        int ts = _cfd.TimestampSize;
        if (ts == 0) return userKey;
        byte[] r = new byte[userKey.Length + ts];
        userKey.CopyTo(r, 0);
        r.AsSpan(userKey.Length).Fill(0xff);
        return r;
    }

    /// <summary>ExternalFileRangeChecker::Overlaps(atomic_replace_range_, smallest, largest).</summary>
    private bool ReplaceRangeOverlaps(byte[] smallest, byte[] largest)
    {
        InternalKeyComparator icmp = _cfd.Icmp;
        return VersionStorageInfo.SstableKeyCompare(icmp, _replaceLargest, smallest) >= 0
            && VersionStorageInfo.SstableKeyCompare(icmp, _replaceSmallest, largest) <= 0;
    }

    /// <summary>ExternalFileRangeChecker::Contains(atomic_replace_range_, smallest, largest).</summary>
    private bool ReplaceRangeContains(byte[] smallest, byte[] largest)
    {
        InternalKeyComparator icmp = _cfd.Icmp;
        return VersionStorageInfo.SstableKeyCompare(icmp, _replaceSmallest, smallest) <= 0
            && VersionStorageInfo.SstableKeyCompare(icmp, _replaceLargest, largest) >= 0;
    }

    /// <summary>atomic_replace_range_->unset(): the whole column family is replaced.</summary>
    private bool ReplaceRangeUnset => _replaceSmallest.Length == 0;

    /// <summary>
    /// Port of SupportsAtomicReplaceRangeTombstone: a partial overlap can be handled with a range tombstone (universal
    /// compaction, a table format supporting range deletions, global seqnos, no DB-generated files, no snapshot
    /// consistency, no user-defined timestamps).
    /// </summary>
    private bool SupportsAtomicReplaceRangeTombstone() =>
        _cfd.Options.TableFactory == TableFactoryType.BlockBasedTable
        && _cfd.Options.CompactionStyle == CompactionStyle.Universal
        && _options.AllowGlobalSeqNo
        && !_options.AllowDbGeneratedFiles
        && !_options.SnapshotConsistency
        && _cfd.TimestampSize == 0;

    /// <summary>Port of CanUseAtomicReplaceRangeTombstone.</summary>
    private bool CanUseAtomicReplaceRangeTombstone(IngestReplaceRange? range) =>
        range is { Start: not null, Limit: not null } && SupportsAtomicReplaceRangeTombstone() && !_options.FailIfNotBottommostLevel;

    /// <summary>Port of HasPartialOverlap: a file of <paramref name="vstorage"/> overlaps the range without being inside it.</summary>
    private bool HasPartialOverlap(VersionStorageInfo vstorage)
    {
        for (int level = 0; level < vstorage.NumLevels; level++)
        {
            foreach (FileMetaData file in vstorage.LevelFiles(level))
            {
                if (ReplaceRangeOverlaps(file.Smallest, file.Largest) && !ReplaceRangeContains(file.Smallest, file.Largest)) return true;
            }
        }
        return false;
    }

    /// <summary>
    /// Port of PrepareAtomicReplaceRangeTombstone: writes a file inside the DB holding the range tombstone [start, limit)
    /// (SstFileWriter, default_write_temperature) and reads its info. It is only ingested if Run still sees a partial
    /// overlap (<see cref="ActivateAtomicReplaceRangeTombstone"/>), otherwise deleted at the end.
    /// </summary>
    private void PrepareAtomicReplaceRangeTombstone(byte[] start, byte[] limit)
    {
        ulong fileNumber = _db.ReserveFileNumberForIngestion();
        _reservedFileNumbers.Add(fileNumber);
        string filePath = FileNames.TableFileName(_db.DbPaths, fileNumber, 0);
        IngestedFileInfo tombstone;
        try
        {
            using (var writer = new SstFileWriter(_cfd.Options, _fs))
            {
                writer.Open(filePath);
                writer.DeleteRange(start, limit);
                writer.Finish();
            }
            tombstone = GetIngestedFileInfo(filePath).Info;
            // SstFileWriter::Finish reports the checksum computed with the DB's file_checksum_gen_factory.
            if (_db.Options.FileChecksumGenFactory is { } factory)
            {
                (tombstone.FileChecksum, tombstone.FileChecksumFuncName) = FileChecksumHelper.GenerateOneFileChecksum(_fs, filePath, factory, "", null);
            }
        }
        catch
        {
            try { _fs.DeleteFile(filePath); }
            catch (Exception e) when (e is IOException or RocksDbException) { }
            throw;
        }
        tombstone.Temperature = _cfd.Options.DefaultWriteTemperature;
        tombstone.FileNumber = fileNumber;
        tombstone.InternalFilePath = filePath;
        tombstone.Copied = true;
        tombstone.GeneratedForIngestion = true;
        _tombstone = tombstone;
    }

    /// <summary>
    /// Port of ActivateAtomicReplaceRangeTombstone: the tombstone becomes the first (oldest) file, so it forms its own
    /// batch below the ingested files.
    /// </summary>
    private void ActivateAtomicReplaceRangeTombstone()
    {
        // Prepare rejects an empty external file list and verifies every input file is contained in the replacement
        // range. The covering tombstone must therefore overlap at least one input file.
        System.Diagnostics.Debug.Assert(_tombstone is not null && _files.Count > 0);
        _batches.Clear();
        _files.Insert(0, _tombstone!);
        _tombstone = null;
        _tombstoneActive = true;
        FilesOverlap = ComputeFilesOverlap();
        System.Diagnostics.Debug.Assert(FilesOverlap);
        DivideInputFilesIntoBatches();
    }

    /// <summary>
    /// The checksum part of ExternalSstFileIngestionJob::Prepare. With a file_checksum_gen_factory: the checksum of every
    /// ingested (internal) file is generated, unless verify_file_checksum is false and checksums were provided for all
    /// files; provided checksums are then verified against the generated ones (function name: InvalidArgument; value:
    /// Corruption), or, with verify_file_checksum = false, trusted once the factory recognizes their function name.
    /// Without a factory nothing is recorded (provided checksums are ignored).
    /// </summary>
    private void GenerateAndVerifyChecksums(IReadOnlyList<string> externalFiles, IReadOnlyList<byte[]> checksums, IReadOnlyList<string> funcNames)
    {
        FileChecksumGenFactory? factory = _db.Options.FileChecksumGenFactory;
        if (factory is null) return;
        int n = _files.Count;
        bool provided = checksums.Count == n && funcNames.Count == n;
        bool generate = _options.VerifyFileChecksum || !provided;
        var generated = new (byte[] Checksum, string FuncName)[generate ? n : 0];
        if (generate)
        {
            for (int i = 0; i < n; i++)
            {
                string requested = i < funcNames.Count ? funcNames[i] : "";
                generated[i] = FileChecksumHelper.GenerateOneFileChecksum(_fs, _files[i].InternalFilePath, factory, requested, null);
                _files[i].FileChecksum = generated[i].Checksum;
                _files[i].FileChecksumFuncName = generated[i].FuncName;
            }
        }
        if (provided)
        {
            for (int i = 0; i < n; i++)
            {
                if (_options.VerifyFileChecksum)
                {
                    if (funcNames[i] != generated[i].FuncName)
                    {
                        ThrowHelper.InvalidArgument("DB file checksum gen factory " + factory.Name + " generated checksum function name "
                            + generated[i].FuncName + " for file " + externalFiles[i] + " which does not match requested/provided " + funcNames[i]);
                    }
                    if (!checksums[i].AsSpan().SequenceEqual(generated[i].Checksum))
                    {
                        ThrowHelper.Corruption("Checksum verification mismatch for ingestion file " + externalFiles[i] + " using function "
                            + generated[i].FuncName + ". Expected: " + FileChecksumHelper.Hex(checksums[i]) + " Computed: "
                            + FileChecksumHelper.Hex(generated[i].Checksum));
                    }
                }
                else
                {
                    // Only check that the factory recognizes the function name, then trust the provided checksum.
                    FileChecksumGenerator? gen = factory.CreateFileChecksumGenerator(new FileChecksumGenContext(_files[i].InternalFilePath, funcNames[i]));
                    if (gen is null || funcNames[i] != gen.Name)
                    {
                        ThrowHelper.InvalidArgument("Checksum function name " + funcNames[i] + " for file " + externalFiles[i]
                            + " not recognized by DB checksum gen factory" + factory.Name + (gen is null ? "" : " Returned function " + gen.Name));
                    }
                    _files[i].FileChecksum = checksums[i];
                    _files[i].FileChecksumFuncName = funcNames[i];
                }
            }
        }
        else if (checksums.Count != funcNames.Count || checksums.Count != 0)
        {
            ThrowHelper.InvalidArgument("The checksum information of ingested sst files are nonempty and the size of checksums or the size of the "
                + "checksum function names does not match with the number of ingested sst files");
        }
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
    /// SanityCheckTableProperties / ResetTableReader: the file is opened through the format-neutral
    /// <see cref="TableReaderFactory"/> with the column family's table factory.
    /// </summary>
    private FileInfoResult GetIngestedFileInfo(string path)
    {
        var info = new IngestedFileInfo { ExternalFilePath = path };
        info.FileSize = (ulong)_fs.GetFileSize(path);
        bool dbGenerated = _options.AllowDbGeneratedFiles;
        // Read the raw keys: the global sequence number must not be applied while validating.
        TableReaderOptions ro = _cfd.ReaderOptions with { LargestSeqno = BlockIter.DisableGlobalSequenceNumber, BlockCache = NoBlockCache.Instance };
        using TableReader reader = OpenTableReader(path, ro);
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
            GetSeqnoBoundaryForFile(reader, ro.PrefixExtractor, props, info);
        }
        else if (props.KeyLargestSeqno != TableProperties.Unknown && props.KeyLargestSeqno > 0)
        {
            // UINT64_MAX means unknown and the file is generated before table property `key_largest_seqno` is introduced.
            ThrowHelper.Corruption("External file has non zero largest sequence number " + props.KeyLargestSeqno);
        }

        byte[]? smallest = null, largest = null;
        // TableReader::VerifyChecksum: only block-based tables implement it.
        if (_options.VerifyChecksumsBeforeIngest && reader is not BlockBasedTableReader) ThrowHelper.NotSupported("VerifyChecksum() not supported");
        using (Iterators.InternalIterator it = reader.NewInternalIterator(TableReadFlags.DontFillCache, null, ro.PrefixExtractor))
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
                if (reader is Table.Plain.PlainTableReader)
                {
                    // PlainTable iterator does not support SeekToLast().
                    largest = smallest;
                    for (; it.Valid; it.Next())
                        if (_cfd.Icmp.Compare(it.Key, largest) > 0) largest = it.Key.ToArray();
                }
                else
                {
                    it.SeekToLast();
                    if (!it.Valid) ThrowHelper.Corruption("Can not find largest key in sst file");
                    largest = it.Key.ToArray();
                }
                CheckKey(largest, dbGenerated);
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
        // Like upstream, a file without a comparator name (PlainTable / CuckooTable builders record none) is a
        // comparator mismatch (CompareComparator: kInvalidChange).
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
            // Status::InvalidArgument(new_comparator->Name(), "does not match existing comparator " + old_comparator_name)
            ThrowHelper.InvalidArgument(_cfd.UserComparator.Name + ": does not match existing comparator " + props.ComparatorName);
        }
    }

    /// <summary>
    /// Port of ExternalSstFileIngestionJob::GetSeqnoBoundaryForFile: the key_smallest/largest_seqno table properties,
    /// or a scan of the point and range-deletion entries for older files.
    /// </summary>
    private static void GetSeqnoBoundaryForFile(TableReader reader, SliceTransform? prefixExtractor, TableProperties props, IngestedFileInfo info)
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
        using (Iterators.InternalIterator it = reader.NewInternalIterator(TableReadFlags.DontFillCache, null, prefixExtractor))
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

    /// <summary>Opens <paramref name="path"/> with the column family's table factory (the reader owns the file).</summary>
    private TableReader OpenTableReader(string path, TableReaderOptions ro)
    {
        IRandomAccessFile f = _fs.OpenRandomAccess(path);
        try
        {
            return TableReaderFactory.Open(f, ro);
        }
        catch
        {
            f.Dispose();
            throw;
        }
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

    /// <summary>
    /// The flush decision of ExternalSstFileIngestionJob::NeedsFlush: true if the memtables of <paramref name="sv"/> may
    /// hold keys in the range of any file (ColumnFamilyData::RangesOverlapWithMemtables), or, with atomic_replace_range,
    /// in the half-open replaced range; replacing the whole column family needs a flush whenever the memtables hold
    /// anything.
    /// </summary>
    public bool NeedsFlush(SuperVersion sv)
    {
        if (_hasReplaceRange && ReplaceRangeUnset)
        {
            // For replacing whole CF, we can simply check whether memtable is empty. (Upstream checks the mutable
            // memtable only; unflushed immutable memtables would survive the replacement, so they are checked too.)
            if (!sv.Mem.IsEmpty) return true;
            foreach (MemTable m in sv.Imm)
            {
                if (!m.IsEmpty) return true;
            }
            return false;
        }
        if (_hasReplaceRange)
        {
            // NOTE: Prepare already checked that the atomic_replace_range covers all the files to ingest.
            ReadOnlySpan<byte> start = DbFormat.ExtractUserKey(_replaceSmallest);
            ReadOnlySpan<byte> limit = DbFormat.ExtractUserKey(_replaceLargest);
            if (MemOverlaps(sv.Mem, start, limit, rangeLimitExclusive: true)) return true;
            foreach (MemTable m in sv.Imm)
            {
                if (MemOverlaps(m, start, limit, rangeLimitExclusive: true)) return true;
            }
            return false;
        }
        foreach (IngestedFileInfo f in _files)
        {
            if (MemOverlaps(sv.Mem, f.SmallestUserKey, f.LargestUserKey, rangeLimitExclusive: false)) return true;
            foreach (MemTable m in sv.Imm)
            {
                if (MemOverlaps(m, f.SmallestUserKey, f.LargestUserKey, rangeLimitExclusive: false)) return true;
            }
        }
        return false;
    }

    private bool MemOverlaps(MemTable mem, ReadOnlySpan<byte> start, ReadOnlySpan<byte> limit, bool rangeLimitExclusive)
    {
        if (mem.IsEmpty) return false;
        MemTableIterator it = mem.NewIterator();
        it.Seek(SeekKey(start));
        if (it.Valid)
        {
            int cmp = _cfd.UserComparator.CompareWithoutTimestamp(DbFormat.ExtractUserKey(it.Key), limit);
            if (rangeLimitExclusive ? cmp < 0 : cmp <= 0) return true;
        }
        // ColumnFamilyData::RangesOverlapWithMemtables: range_del_agg.IsRangeOverlapped(start, limit, exclusive). Only a
        // range tombstone intersecting the range counts, not the mere presence of one in the memtable.
        return mem.NumRangeDeletes > 0 && RangeTombstonesOverlap(mem.GetFragmentedRangeTombstones(), start, limit, rangeLimitExclusive, null);
    }

    /// <summary>
    /// Port of RangeDelAggregator::StripeRep::IsRangeOverlapped for one fragmented tombstone list: true if a tombstone
    /// [ts, te) intersects the user key range [<paramref name="start"/>, <paramref name="limit"/>] (or [start, limit)
    /// with <paramref name="limitExclusive"/>). With <paramref name="file"/> the tombstones are truncated to the table
    /// file's boundaries like TruncatedRangeDelIterator (a largest key that is a range tombstone sentinel is exclusive).
    /// Keys are compared without timestamps.
    /// </summary>
    private bool RangeTombstonesOverlap(FragmentedRangeTombstoneList list, ReadOnlySpan<byte> start, ReadOnlySpan<byte> limit, bool limitExclusive,
        FileMetaData? file)
    {
        Comparator ucmp = _cfd.UserComparator;
        for (int i = 0; i < list.Count; i++)
        {
            // Fragments are ordered by start key: nothing past the limit can overlap.
            int c = ucmp.CompareWithoutTimestamp(list.GetStart(i), limit);
            if (limitExclusive ? c >= 0 : c > 0) break;
            ReadOnlySpan<byte> end = list.GetEnd(i);
            if (file is not null)
            {
                ReadOnlySpan<byte> fileLargest = DbFormat.ExtractUserKey(file.Largest);
                if (ucmp.CompareWithoutTimestamp(end, fileLargest) > 0)
                {
                    // Truncated at the file's largest key: inclusive for a point key, exclusive for a sentinel.
                    DbFormat.UnpackSequenceAndType(DbFormat.ExtractFooter(file.Largest), out ulong seq, out EntryType type);
                    bool sentinel = seq == DbFormat.MaxSequenceNumber && type == EntryType.RangeDeletion;
                    int cl = ucmp.CompareWithoutTimestamp(start, fileLargest);
                    if (sentinel ? cl < 0 : cl <= 0) return true;
                    continue;
                }
            }
            if (ucmp.CompareWithoutTimestamp(start, end) < 0) return true;
        }
        return false;
    }

    /// <summary>
    /// Seek target for a range's smallest user key: newest possible entry of it (with user-defined timestamps every
    /// version of the key, whatever its timestamp, counts as an overlap: the maximum timestamp sorts first).
    /// </summary>
    private byte[] SeekKey(ReadOnlySpan<byte> smallestUserKey)
    {
        byte[] seek = new InternalKey(smallestUserKey, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek).Encoded.ToArray();
        int ts = _cfd.TimestampSize;
        if (ts > 0 && smallestUserKey.Length >= ts) seek.AsSpan(smallestUserKey.Length - ts, ts).Fill(0xff);
        return seek;
    }

    /// <summary>
    /// Port of ExternalSstFileIngestionJob::Run: with atomic_replace_range, selects the files to delete (failing on a
    /// running compaction over the range, or on a partial overlap that cannot use the prepared range tombstone); then
    /// assigns levels and sequence numbers batch by batch (older batches deeper in the LSM tree) from the current
    /// version, and returns the edit. REQUIRES: the DB mutex, the caller is the unbatched write leader (no concurrent
    /// writes) and the memtables do not overlap the files. <see cref="MaxAssignedSeqno"/> tells which sequence numbers
    /// were used. Follow with <see cref="RegisterRange"/> before releasing the mutex.
    /// </summary>
    public VersionEdit Run(ulong lastSeqno, bool hasSnapshots)
    {
        // We need to assign a global sequence number to all the files even if the don't overlap with any ranges since
        // we have snapshots.
        bool forceGlobalSeqno = _options.SnapshotConsistency && hasSnapshots;
        var edit = new VersionEdit { ColumnFamily = _cfd.Id };
        // CommitFileIngestionHandles: edit->MarkForegroundOperation() (relaxed MANIFEST rotation limit).
        edit.MarkForegroundOperation();
        MaxAssignedSeqno = 0;
        // A grouped MANIFEST write can advance current before publishing its SV: work on the current version.
        VersionStorageInfo vstorage = _cfd.Current.Storage;
        CompactionPicker picker = _cfd.CompactionState.Picker;

        if (_hasReplaceRange)
        {
            _filesToReplace.Clear();
            if (ReplaceRangeUnset)
            {
                if (picker.IsCompactionInProgress) ThrowHelper.InvalidArgument("Atomic replace range (full) overlaps with pending compaction");
                for (int lvl = 0; lvl < vstorage.NumLevels; lvl++)
                {
                    foreach (FileMetaData file in vstorage.LevelFiles(lvl)) AddFileToReplace(edit, lvl, file);
                }
            }
            else
            {
                bool hasPartialOverlap = false;
                ReadOnlySpan<byte> start = DbFormat.ExtractUserKey(_replaceSmallest);
                ReadOnlySpan<byte> limit = DbFormat.ExtractUserKey(_replaceLargest);
                for (int lvl = 0; lvl < vstorage.NumLevels; lvl++)
                {
                    if (picker.RangeOverlapWithCompaction(start, limit, lvl, rangeLimitExclusive: true))
                    {
                        ThrowHelper.InvalidArgument("Atomic replace range overlaps with pending compaction");
                    }
                    foreach (FileMetaData file in vstorage.LevelFiles(lvl))
                    {
                        if (!ReplaceRangeOverlaps(file.Smallest, file.Largest)) continue;
                        if (ReplaceRangeContains(file.Smallest, file.Largest)) AddFileToReplace(edit, lvl, file);
                        else hasPartialOverlap = true;
                    }
                }
                if (hasPartialOverlap)
                {
                    if (_options.FailIfNotBottommostLevel && SupportsAtomicReplaceRangeTombstone())
                    {
                        throw new RocksDbException(StatusCode.TryAgain,
                            "Operation failed. Try again.: Atomic replace range partially overlaps with an existing file, so replacement files " +
                            "cannot all be ingested to Lmax");
                    }
                    if (_tombstone is null)
                    {
                        if (SupportsAtomicReplaceRangeTombstone())
                        {
                            throw new RocksDbException(StatusCode.TryAgain,
                                "Operation failed. Try again.: Atomic replace range acquired a partial overlap after preparation; retry ingestion");
                        }
                        ThrowHelper.InvalidArgument("Atomic replace range partially overlaps with existing file");
                    }
                    ActivateAtomicReplaceRangeTombstone();
                }
            }
        }

        int? prevBatchUppermostLevel = null;
        // Batches at the front contain older updates and are placed deeper in the LSM tree than later overlapping batches.
        foreach (List<IngestedFileInfo> batch in _batches)
        {
            int batchUppermostLevel = AssignLevelsForOneBatch(vstorage, batch, forceGlobalSeqno, ref lastSeqno, prevBatchUppermostLevel, edit);
            prevBatchUppermostLevel = batchUppermostLevel;
        }
        CreateEquivalentFileIngestingCompactions(vstorage, edit);
        return edit;
    }

    /// <summary>
    /// Port of ExternalSstFileIngestionJob::AddFileToReplace. Range checks against registered compactions do not cover
    /// every input file, e.g. inputs of a compaction outputting to a different level.
    /// </summary>
    private void AddFileToReplace(VersionEdit edit, int level, FileMetaData file)
    {
        if (file.BeingCompacted) ThrowHelper.InvalidArgument("Atomic replace range overlaps with pending compaction");
        edit.DeleteFile(level, file.FileNumber);
        _filesToReplace.Add((level, file));
    }

    /// <summary>
    /// Port of ExternalSstFileIngestionJob::CreateEquivalentFileIngestingCompactions: one compaction per output level
    /// whose inputs are (copies of) the files ingested to it, with L0 as their source level.
    /// </summary>
    private void CreateEquivalentFileIngestingCompactions(VersionStorageInfo vstorage, VersionEdit edit)
    {
        var byLevel = new SortedDictionary<int, List<FileMetaData>>();
        foreach ((int level, FileMetaData f) in edit.NewFiles)
        {
            if (!byLevel.TryGetValue(level, out List<FileMetaData>? list)) byLevel[level] = list = new List<FileMetaData>();
            list.Add(f.Clone());
        }
        ColumnFamilyOptions o = _cfd.Options;
        foreach (KeyValuePair<int, List<FileMetaData>> kv in byLevel)
        {
            // Treat the source level of ingested files to be level 0.
            var input = new CompactionInputFiles(0, kv.Value);
            _fileIngestingCompactions.Add(new Compaction(vstorage, o, [input], kv.Key,
                CompactionPicker.MaxFileSizeForLevel(o, kv.Key, vstorage.BaseLevel), long.MaxValue, 0, o.Compression, new List<FileMetaData>(),
                CompactionReason.ExternalSstIngestion, -1, FilesOverlap, isManual: false));
        }
    }

    /// <summary>
    /// Port of ExternalSstFileIngestionJob::RegisterRange: registers the file-ingesting compactions and marks the
    /// replaced files as being compacted, so that no compaction picked while LogAndApply has released the DB mutex
    /// overlaps the ingested files or takes a replaced file as input. REQUIRES: the DB mutex.
    /// </summary>
    public void RegisterRange()
    {
        CompactionPicker picker = _cfd.CompactionState.Picker;
        foreach (Compaction c in _fileIngestingCompactions) picker.RegisterCompaction(c);
        foreach ((_, FileMetaData f) in _filesToReplace)
        {
            System.Diagnostics.Debug.Assert(!f.BeingCompacted);
            f.BeingCompacted = true;
        }
        _filesToReplaceMarked = true;
        // RemoveFilesBeingCompactedFromCompactionCandidates.
        if (_filesToReplace.Count > 0) _cfd.Current.Storage.ComputeCompactionScore();
    }

    /// <summary>
    /// Port of ExternalSstFileIngestionJob::UnregisterRange. With <paramref name="restoreCompactionCandidates"/> (the
    /// ingestion failed), the replaced files become compaction candidates again. REQUIRES: the DB mutex.
    /// </summary>
    public void UnregisterRange(bool restoreCompactionCandidates)
    {
        CompactionPicker picker = _cfd.CompactionState.Picker;
        foreach (Compaction c in _fileIngestingCompactions) picker.UnregisterCompaction(c);
        _fileIngestingCompactions.Clear();
        if (_filesToReplaceMarked)
        {
            foreach ((_, FileMetaData f) in _filesToReplace) f.BeingCompacted = false;
            _filesToReplaceMarked = false;
            if (restoreCompactionCandidates && _filesToReplace.Count > 0) _cfd.Current.Storage.ComputeCompactionScore();
        }
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
                FileChecksum = f.FileChecksum,
                FileChecksumFuncName = f.FileChecksumFuncName,
                Temperature = f.Temperature,
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
        bool isGeneratedRangeTombstone = _tombstoneActive && f.GeneratedForIngestion;
        for (int lvl = 0; lvl < overlapCheckingExclusiveEnd; lvl++)
        {
            if (lvl > 0 && lvl < vstorage.BaseLevel) continue;
            if (lvl < assignedLevelExclusiveEnd && _hasReplaceRange && !_tombstoneActive)
            {
                // Everything the files overlap is deleted by the same edit: the deepest level is free.
                targetLevel = lvl;
                continue;
            }
            if (_db.RangeOverlapsRunningCompaction(_cfd, f.SmallestUserKey, f.LargestUserKey, lvl, isGeneratedRangeTombstone))
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
        System.Diagnostics.Debug.Assert(!_hasReplaceRange);
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
        byte[] seek = SeekKey(f.SmallestUserKey);
        foreach (FileMetaData lf in vstorage.LevelFiles(level))
        {
            if (ucmp.CompareWithoutTimestamp(DbFormat.ExtractUserKey(lf.Largest), f.SmallestUserKey) < 0 ||
                ucmp.CompareWithoutTimestamp(DbFormat.ExtractUserKey(lf.Smallest), f.LargestUserKey) > 0)
            {
                continue;
            }
            using (Iterators.InternalIterator it = _cfd.TableCache.NewIterator(lf))
            {
                it.Seek(seek);
                if (it.Valid && ucmp.CompareWithoutTimestamp(DbFormat.ExtractUserKey(it.Key), f.LargestUserKey) <= 0) return true;
            }
            // range_del_agg.IsRangeOverlapped(smallest_user_key, largest_user_key, end_exclusive = false) over the range
            // tombstones of the files the level iterator opened.
            if (_cfd.TableCache.GetRangeTombstones(lf) is { } tombstones
                && RangeTombstonesOverlap(tombstones, f.SmallestUserKey, f.LargestUserKey, limitExclusive: false, lf))
            {
                return true;
            }
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
                    // Since the global sequence number is updated, the file checksum needs to be recalculated.
                    if (_db.Options.FileChecksumGenFactory is { } factory && f.FileChecksumFuncName != FileMetaData.UnknownFileChecksumFuncName)
                    {
                        (f.FileChecksum, f.FileChecksumFuncName) =
                            FileChecksumHelper.GenerateOneFileChecksum(_fs, f.InternalFilePath, factory, f.FileChecksumFuncName, null);
                    }
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

    /// <summary>Deletes the files copied/linked into the DB, and a prepared range tombstone file (failed ingestion).</summary>
    public void DeleteInternalFiles()
    {
        foreach (IngestedFileInfo f in _files)
        {
            if (f.InternalFilePath.Length == 0) continue;
            try { _fs.DeleteFile(f.InternalFilePath); }
            catch (Exception e) when (e is IOException or RocksDbException) { }
        }
        DeleteUnusedRangeTombstone();
    }

    /// <summary>The successful-ingestion part of Cleanup: a prepared range tombstone that Run did not need is deleted.</summary>
    public void DeleteUnusedRangeTombstone()
    {
        if (_tombstone is null) return;
        try { _fs.DeleteFile(_tombstone.InternalFilePath); }
        catch (Exception e) when (e is IOException or RocksDbException) { }
        _tombstone = null;
    }

    /// <summary>With move_files, the original links are removed after a successful ingestion.</summary>
    public void DeleteExternalLinks()
    {
        if (!_options.MoveFiles) return;
        foreach (IngestedFileInfo f in _files)
        {
            // Cleanup removes the original of every file but the generated one, also when Prepare fell back to a copy.
            if (f.GeneratedForIngestion) continue;
            try { _fs.DeleteFile(f.ExternalFilePath); }
            catch (Exception e) when (e is IOException or RocksDbException) { }
        }
    }
}
