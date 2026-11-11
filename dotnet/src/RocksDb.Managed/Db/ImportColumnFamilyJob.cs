using RocksDb.Managed.Env;
using RocksDb.Managed.Table;

namespace RocksDb.Managed.Db;

/// <summary>
/// Imports the table files of one or more <see cref="ExportImportFilesMetadata"/> into a new, empty column family. Port
/// of ImportColumnFamilyJob (db/import_column_family_job.cc):
/// <list type="bullet">
/// <item><see cref="Prepare"/> reads every file's bounds and properties (GetIngestedFileInfo), rejects empty files,
/// checks that the key ranges of different metadata sets do not overlap ("CFs have overlapping ranges") and hard-links
/// (<see cref="ImportColumnFamilyOptions.MoveFiles"/>) or copies the files into the DB under new file numbers;</item>
/// <item><see cref="Run"/> builds the version edit: files keep their level and sequence numbers (no global seqno), the
/// epoch numbers are reassigned per metadata set (VersionStorageInfo::RecoverEpochNumbers with force: bottom level
/// first, L0 oldest first; restarted for the first set only), and the DB's last sequence is raised to the largest
/// imported seqno;</item>
/// <item><see cref="Cleanup"/> deletes the internal copies on failure, or the originals after a successful move.</item>
/// </list>
/// </summary>
internal sealed class ImportColumnFamilyJob
{
    private readonly DbImpl _db;
    private readonly ColumnFamilyData _cfd;
    private readonly ImportColumnFamilyOptions _options;
    private readonly IReadOnlyList<ExportImportFilesMetadata> _metadatas;
    private readonly IFileSystem _fs;
    private readonly List<List<ImportedFile>> _files = new();

    private sealed class ImportedFile
    {
        public required LiveFileMetadata Source;
        public required string ExternalPath;
        public ulong FileNumber;
        public ulong FileSize;
        public byte[] Smallest = [];
        public byte[] Largest = [];
        public string InternalPath = "";
        public UniqueId64x2 UniqueId;
        public ulong TailSize;
        public bool UserDefinedTimestampsPersisted = true;
        public ulong NumEntries;
        public TableProperties? Properties;
    }

    public ImportColumnFamilyJob(DbImpl db, ColumnFamilyData cfd, ImportColumnFamilyOptions options, IReadOnlyList<ExportImportFilesMetadata> metadatas)
    {
        _db = db;
        _cfd = cfd;
        _options = options;
        _metadatas = metadatas;
        _fs = db.FileSystem;
    }

    /// <summary>The file numbers reserved for the imported files (pending outputs until the edit is installed).</summary>
    public List<ulong> ReservedFileNumbers { get; } = new();

    /// <summary>ImportColumnFamilyJob::Prepare: file infos, overlap check across metadata sets, link or copy into the DB.</summary>
    public async ValueTask PrepareAsync(Func<ulong> reserveFileNumber, bool synchronous, CancellationToken ct)
    {
        InternalKeyComparator icmp = _cfd.Icmp;
        Comparator ucmp = _cfd.UserComparator;
        var ranges = new List<(byte[] Smallest, byte[] Largest)>(_metadatas.Count);
        foreach (ExportImportFilesMetadata metadata in _metadatas)
        {
            var perCf = new List<ImportedFile>(metadata.Files.Count);
            byte[]? smallest = null, largest = null;
            foreach (LiveFileMetadata fm in metadata.Files)
            {
                ct.ThrowIfCancellationRequested();
                string path = fm.Directory + "/" + fm.Name;
                ulong number = reserveFileNumber();
                ReservedFileNumbers.Add(number);
                ImportedFile f = GetIngestedFileInfo(path, number, fm);
                if (f.NumEntries == 0) ThrowHelper.InvalidArgument("File contain no entries");
                if (f.Smallest.Length < DbFormat.InternalKeyFooterSize || f.Largest.Length < DbFormat.InternalKeyFooterSize)
                {
                    ThrowHelper.Corruption("File has corrupted keys");
                }
                perCf.Add(f);
                if (smallest is null || icmp.Compare(smallest, f.Smallest) > 0) smallest = f.Smallest;
                if (largest is null || icmp.Compare(largest, f.Largest) < 0) largest = f.Largest;
            }
            if (perCf.Count == 0) ThrowHelper.InvalidArgument("The list of files is empty");
            _files.Add(perCf);
            ranges.Add((smallest!, largest!));
        }

        // Check overlaps of the key ranges of the different metadata sets (CFs).
        ranges.Sort((a, b) => ucmp.Compare(DbFormat.ExtractUserKey(a.Smallest), DbFormat.ExtractUserKey(b.Smallest)));
        for (int i = 0; i + 1 < ranges.Count; i++)
        {
            if (ucmp.Compare(DbFormat.ExtractUserKey(ranges[i].Largest), DbFormat.ExtractUserKey(ranges[i + 1].Smallest)) >= 0)
            {
                ThrowHelper.InvalidArgument("CFs have overlapping ranges");
            }
        }

        // Copy/Move external files into DB.
        bool hardlinkFiles = _options.MoveFiles;
        foreach (List<ImportedFile> perCf in _files)
        {
            foreach (ImportedFile f in perCf)
            {
                string inside = FileNames.TableFileName(_db.DbPaths, f.FileNumber, 0);
                if (hardlinkFiles)
                {
                    try
                    {
                        _fs.LinkFile(f.ExternalPath, inside);
                        f.InternalPath = inside;
                        continue;
                    }
                    catch (RocksDbException e) when (e.Code == StatusCode.NotSupported)
                    {
                        hardlinkFiles = false;
                    }
                }
                f.InternalPath = inside;
                await _db.CopyFileAsync(f.ExternalPath, inside, 0, synchronous, ct).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// ImportColumnFamilyJob::Run: the version edit adding every file at its exported level. REQUIRES: writes stopped;
    /// mutex held (the epoch numbers come from the column family).
    /// </summary>
    public VersionEdit Run(out ulong largestSeqno)
    {
        ulong now = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var edit = new VersionEdit { ColumnFamily = _cfd.Id };
        // CreateColumnFamilyWithImport: edit->MarkForegroundOperation().
        edit.MarkForegroundOperation();
        largestSeqno = 0;
        int numLevels = _cfd.NumLevels;
        for (int i = 0; i < _files.Count; i++)
        {
            // The dummy VersionStorageInfo of upstream: files grouped by level, L0 newest first.
            var levels = new List<FileMetaData>[numLevels];
            for (int l = 0; l < numLevels; l++) levels[l] = new List<FileMetaData>();
            foreach (ImportedFile f in _files[i])
            {
                LiveFileMetadata src = f.Source;
                if (src.Level < 0 || src.Level >= numLevels) ThrowHelper.InvalidArgument("Invalid level " + src.Level + " for imported file " + src.Name);
                var meta = new FileMetaData
                {
                    FileNumber = f.FileNumber,
                    PathId = 0,
                    FileSize = f.FileSize,
                    Smallest = f.Smallest,
                    Largest = f.Largest,
                    SmallestSeqno = src.SmallestSeqno,
                    LargestSeqno = src.LargestSeqno,
                    Temperature = src.Temperature,
                    OldestAncestorTime = now,
                    FileCreationTime = now,
                    EpochNumber = src.EpochNumber,
                    UniqueId = f.UniqueId,
                    TailSize = f.TailSize,
                    UserDefinedTimestampsPersisted = f.UserDefinedTimestampsPersisted,
                };
                if (f.Properties is not null) meta.SetEntryStats(f.Properties);
                levels[src.Level].Add(meta);
            }
            // VersionBuilder::SaveTo on the dummy VersionStorageInfo (EpochNumberRequirement::kMightMissing): L0 is ordered
            // newest first by NewestFirstBySeqNo (largest seqno, smallest seqno, file number), not by the exported epoch
            // numbers, which are reassigned below; other levels by smallest key; the consistency check rejects
            // overlapping files in a sorted level.
            levels[0].Sort(static (a, b) =>
            {
                int c = b.LargestSeqno.CompareTo(a.LargestSeqno);
                if (c == 0) c = b.SmallestSeqno.CompareTo(a.SmallestSeqno);
                return c != 0 ? c : b.FileNumber.CompareTo(a.FileNumber);
            });
            InternalKeyComparator icmp = _cfd.Icmp;
            for (int l = 1; l < numLevels; l++)
            {
                List<FileMetaData> files = levels[l];
                files.Sort((a, b) => icmp.Compare(a.Smallest, b.Smallest));
                for (int k = 1; k < files.Count; k++)
                {
                    if (icmp.Compare(files[k - 1].Largest, files[k].Smallest) >= 0)
                    {
                        ThrowHelper.Corruption("L" + l + " has overlapping ranges: file #" + files[k - 1].FileNumber + " and file #" + files[k].FileNumber);
                    }
                }
            }
            // RecoverEpochNumbers(cfd, restart_epoch = i == 0, force = true).
            if (i == 0)
            {
                _cfd.NextEpochNumber = 1;
                if (_cfd.Options.AllowIngestBehind) _cfd.NewEpochNumber(); // kReservedEpochNumberForFileIngestedBehind
            }
            for (int l = numLevels - 1; l >= 1; l--)
            {
                if (levels[l].Count == 0) continue;
                ulong epoch = _cfd.NewEpochNumber();
                foreach (FileMetaData f in levels[l]) f.EpochNumber = epoch;
            }
            for (int k = levels[0].Count - 1; k >= 0; k--) levels[0][k].EpochNumber = _cfd.NewEpochNumber();
            for (int l = 0; l < numLevels; l++)
            {
                foreach (FileMetaData f in levels[l])
                {
                    edit.AddFile(l, f);
                    if (f.LargestSeqno > largestSeqno) largestSeqno = f.LargestSeqno;
                }
            }
        }
        return edit;
    }

    /// <summary>ImportColumnFamilyJob::Cleanup.</summary>
    public void Cleanup(bool ok)
    {
        foreach (List<ImportedFile> perCf in _files)
        {
            foreach (ImportedFile f in perCf)
            {
                string path = ok ? (_options.MoveFiles ? f.ExternalPath : "") : f.InternalPath;
                if (path.Length == 0) continue;
                try
                {
                    _fs.DeleteFile(path);
                }
                catch (Exception e) when (e is IOException or RocksDbException or UnauthorizedAccessException)
                {
                    // Best effort, like the upstream warning.
                }
            }
        }
    }

    /// <summary>ImportColumnFamilyJob::GetIngestedFileInfo.</summary>
    private ImportedFile GetIngestedFileInfo(string externalFile, ulong newFileNumber, LiveFileMetadata fm)
    {
        var f = new ImportedFile { Source = fm, ExternalPath = externalFile, FileNumber = newFileNumber };
        f.FileSize = fm.Size > 0 ? fm.Size : (ulong)_fs.GetFileSize(externalFile);
        TableReaderOptions ro = _cfd.ReaderOptions with { BlockCache = NoBlockCache.Instance };
        using BlockBasedTableReader reader = BlockBasedTableReader.Open(_fs.OpenRandomAccess(externalFile), ro);
        TableProperties props = reader.Properties;
        f.Properties = props;
        f.NumEntries = props.NumEntries;
        if (fm.SmallestInternalKey.Length == 0)
        {
            byte[]? smallest = null, largest = null;
            using (BlockBasedTableIterator it = reader.NewIterator())
            {
                it.SeekToFirst();
                if (it.Valid)
                {
                    smallest = it.Key.ToArray();
                    it.SeekToLast();
                    if (!it.Valid) ThrowHelper.Corruption("Can not find largest key in sst file");
                    largest = it.Key.ToArray();
                }
            }
            if (reader.HasRangeTombstones)
            {
                using BlockIter rit = reader.NewRangeTombstoneIterator()!;
                rit.SeekToFirst();
                if (rit.Valid)
                {
                    // RangeTombstone(first).SerializeKey() / RangeTombstone(last).SerializeEndKey().
                    if (!DbFormat.TryParseInternalKey(rit.Key, out ParsedInternalKey pk)) ThrowHelper.Corruption("Corrupted key in external file. ");
                    byte[] startKey = new InternalKey(rit.Key[..pk.UserKeyLength], pk.Sequence, EntryType.RangeDeletion).Encoded.ToArray();
                    if (smallest is null || _cfd.Icmp.Compare(startKey, smallest) < 0) smallest = startKey;
                    rit.SeekToLast();
                    if (!DbFormat.TryParseInternalKey(rit.Key, out _)) ThrowHelper.Corruption("Corrupted key in external file. ");
                    byte[] endKey = new InternalKey(rit.Value, DbFormat.MaxSequenceNumber, EntryType.RangeDeletion).Encoded.ToArray();
                    if (largest is null || _cfd.Icmp.Compare(endKey, largest) > 0) largest = endKey;
                }
            }
            f.Smallest = smallest ?? [];
            f.Largest = largest ?? [];
        }
        else
        {
            f.Smallest = fm.SmallestInternalKey;
            f.Largest = fm.LargestInternalKey;
        }
        f.TailSize = FileMetaData.CalculateTailSize(f.FileSize, props);
        f.UserDefinedTimestampsPersisted = props.UserDefinedTimestampsPersisted != 0;
        if (props.DbId.Length > 0 && props.DbSessionId.Length > 0)
        {
            try
            {
                Cache.OffsetableCacheKey.GetSstInternalUniqueId(props.DbId, props.DbSessionId, props.OriginalFileNumber, out ulong id0, out ulong id1);
                f.UniqueId = new UniqueId64x2(id0, id1);
            }
            catch (RocksDbException)
            {
                // "Failed to get SST unique id" is only a warning upstream.
            }
        }
        return f;
    }
}
