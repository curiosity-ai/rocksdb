using System.Globalization;
using RocksDb.Managed.Env;
using RocksDb.Managed.Logging;
using RocksDb.Managed.Table;

namespace RocksDb.Managed.Db;

/// <summary>
/// Port of the Repairer of db/repair.cc (DB::RepairDB): rebuilds the MANIFEST of a DB whose MANIFEST or CURRENT is
/// lost or corrupt, from the table files and WALs found in the DB directories.
/// <list type="number">
/// <item>FindFiles: every MANIFEST, WAL and table file of db_paths (and wal_dir); the next file number is past all of them.</item>
/// <item>Old MANIFESTs are moved to <c>lost/</c>; a new empty MANIFEST-000001 + CURRENT is written (NewDB).</item>
/// <item>ExtractMetaData / ScanTable: each table is opened and scanned for its key range, sequence numbers, column family
/// (from its properties) and unique id; unreadable tables are moved to <c>lost/</c>.</item>
/// <item>AddTables: per column family, an edit adds its tables to level 0 (epoch numbers recovered from the seqnos,
/// RecoverEpochNumbers) with the largest sequence number as last sequence.</item>
/// <item>ConvertLogFilesToTables: the WALs are replayed into the repaired DB and flushed to level-0 tables.</item>
/// </list>
/// Deviation (structure, same result): upstream converts the WALs into tables before writing the MANIFEST edits and
/// archives them to <c>lost/</c>; this port writes the MANIFEST first and converts the WALs by opening the repaired DB
/// once (recovery with <see cref="WalRecoveryMode.SkipAnyCorruptedRecords"/> + flush), which then deletes them like any
/// recovered WAL. Records of column families that have no table file are dropped in both (upstream: the batch fails
/// on the unknown column family; here the column family is unknown to recovery).
/// </summary>
internal sealed class Repairer
{
    private readonly string _dbname;
    private readonly DbOptions _options;
    private readonly IFileSystem _fs;
    private readonly IReadOnlyList<ColumnFamilyDescriptor> _columnFamilies;
    private readonly ColumnFamilyOptions _defaultCfOptions;
    private readonly ColumnFamilyOptions? _unknownCfOptions;
    private readonly List<string> _manifests = new();
    private readonly List<(ulong Number, uint PathId)> _tableFds = new();
    private readonly List<ulong> _logs = new();
    private readonly List<TableInfo> _tables = new();
    private ulong _nextFileNumber = 1;
    private readonly List<string> _paths = new();

    private sealed class TableInfo
    {
        public required FileMetaData Meta;
        public uint ColumnFamilyId;
        public string ColumnFamilyName = "";
    }

    public Repairer(string dbname, DbOptions options, IReadOnlyList<ColumnFamilyDescriptor> columnFamilies, ColumnFamilyOptions? unknownCfOptions)
    {
        _dbname = dbname.TrimEnd('/');
        _options = options;
        _fs = options.FileSystem;
        _columnFamilies = columnFamilies;
        ColumnFamilyOptions? def = null;
        foreach (ColumnFamilyDescriptor d in columnFamilies)
        {
            if (d.Name == ColumnFamilyDescriptor.DefaultColumnFamilyName) def = d.Options;
        }
        _defaultCfOptions = def ?? new ColumnFamilyOptions();
        _unknownCfOptions = unknownCfOptions;
    }

    /// <summary>Number of table files in the repaired MANIFEST and their total size (the "Repaired rocksdb" log line).</summary>
    public int RecoveredFiles => _tables.Count;
    public ulong RecoveredBytes
    {
        get
        {
            ulong b = 0;
            foreach (TableInfo t in _tables) b += t.Meta.FileSize;
            return b;
        }
    }

    private ColumnFamilyOptions? GetColumnFamilyOptions(string name)
    {
        foreach (ColumnFamilyDescriptor d in _columnFamilies)
        {
            if (d.Name == name) return d.Options;
        }
        return _unknownCfOptions;
    }

    /// <summary>Repairer::Run.</summary>
    public void Run()
    {
        using (_fs.LockFile(FileNames.LockFileName(_dbname)))
        {
            FindFiles();
            foreach (string m in _manifests) ArchiveFile(_dbname + "/" + m);
            ExtractMetaData();
            WriteManifest();
        }
        ConvertLogFilesToTables();
    }

    /// <summary>Repairer::FindFiles.</summary>
    private void FindFiles()
    {
        if (_options.DbPaths.Count == 0) _paths.Add(_dbname);
        foreach ((string p, _) in _options.DbPaths) _paths.Add(p.TrimEnd('/'));
        var searchPaths = new List<string>(_paths);
        string walDir = string.IsNullOrEmpty(_options.WalDir) ? _dbname : _options.WalDir.TrimEnd('/');
        if (walDir != _dbname) searchPaths.Add(walDir);
        bool foundFile = false;
        for (int pathId = 0; pathId < searchPaths.Count; pathId++)
        {
            List<string> names = [.. _fs.GetChildren(searchPaths[pathId])];
            if (names.Count > 0) foundFile = true;
            foreach (string name in names)
            {
                if (!FileNames.TryParseFileName(name, out ulong number, out FileType type)) continue;
                if (type == FileType.DescriptorFile)
                {
                    if (pathId == 0) _manifests.Add(name);
                    continue;
                }
                if (number + 1 > _nextFileNumber) _nextFileNumber = number + 1;
                if (type == FileType.WalFile) _logs.Add(number);
                else if (type == FileType.TableFile && pathId < _paths.Count) _tableFds.Add((number, (uint)pathId));
            }
        }
        if (!foundFile) throw RocksDbException.Corruption(_dbname + ": repair found no files");
        _tableFds.Sort();
    }

    /// <summary>Repairer::ExtractMetaData.</summary>
    private void ExtractMetaData()
    {
        foreach ((ulong number, uint pathId) in _tableFds)
        {
            string fname = FileNames.TableFileName(_paths, number, pathId);
            try
            {
                _tables.Add(ScanTable(fname, number, pathId));
            }
            catch (Exception e) when (e is RocksDbException or IOException)
            {
                _options.InfoLog?.Log(InfoLogLevel.Warn, "Table #" + number.ToString(CultureInfo.InvariantCulture) + ": ignoring " + e.Message);
                ArchiveFile(fname);
            }
        }
    }

    /// <summary>Repairer::ScanTable.</summary>
    private TableInfo ScanTable(string fname, ulong number, uint pathId)
    {
        ulong fileSize = (ulong)_fs.GetFileSize(fname);
        IRandomAccessFile file = _fs.OpenRandomAccess(fname);
        BlockBasedTableReader reader;
        try
        {
            ColumnFamilyOptions probe = _defaultCfOptions;
            TableReaderOptions ro = TableFactory.CreateReaderOptions(probe, TableFactory.CreateFormatOptions(probe.TableOptions)) with
            {
                FileNumber = number,
            };
            reader = BlockBasedTableReader.Open(file, ro);
        }
        catch
        {
            file.Dispose();
            throw;
        }
        using (reader)
        {
            TableProperties props = reader.Properties;
            var meta = new FileMetaData { FileNumber = number, PathId = pathId, FileSize = fileSize };
            if (props.DbId.Length > 0 && props.DbSessionId.Length > 0)
            {
                Cache.OffsetableCacheKey.GetSstInternalUniqueId(props.DbId, props.DbSessionId, props.OriginalFileNumber, out ulong id0, out ulong id1);
                meta.UniqueId = new UniqueId64x2(id0, id1);
            }
            var t = new TableInfo { Meta = meta, ColumnFamilyId = (uint)props.ColumnFamilyId, ColumnFamilyName = props.ColumnFamilyName };
            if (props.ColumnFamilyId == TableProperties.UnknownColumnFamily)
            {
                // "column family unknown (probably due to legacy format); adding to default column family id 0."
                t.ColumnFamilyId = 0;
                t.ColumnFamilyName = ColumnFamilyDescriptor.DefaultColumnFamilyName;
            }
            if (t.ColumnFamilyId == 0 && t.ColumnFamilyName.Length == 0) t.ColumnFamilyName = ColumnFamilyDescriptor.DefaultColumnFamilyName;
            foreach (TableInfo other in _tables)
            {
                if (other.ColumnFamilyId == t.ColumnFamilyId && other.ColumnFamilyName != t.ColumnFamilyName)
                    throw RocksDbException.Corruption(_dbname + ": inconsistent column family name");
            }
            if (GetColumnFamilyOptions(t.ColumnFamilyName) is null && t.ColumnFamilyName != ColumnFamilyDescriptor.DefaultColumnFamilyName)
            {
                throw RocksDbException.Corruption("Encountered unknown column family with name=" + t.ColumnFamilyName + ", id="
                    + t.ColumnFamilyId.ToString(CultureInfo.InvariantCulture));
            }
            meta.OldestAncestorTime = props.CreationTime;
            meta.UserDefinedTimestampsPersisted = props.UserDefinedTimestampsPersisted != 0;
            meta.TailSize = props.TailStartOffset > 0 && props.TailStartOffset <= fileSize ? fileSize - props.TailStartOffset : 0;

            InternalKeyComparator icmp = reader.InternalComparator;
            using (BlockBasedTableIterator it = reader.NewIterator(TableReadFlags.DontFillCache))
            {
                for (it.SeekToFirst(); it.Valid; it.Next())
                {
                    ReadOnlySpan<byte> key = it.Key;
                    if (!DbFormat.TryParseInternalKey(key, out ParsedInternalKey parsed)) continue; // unparsable key
                    UpdateBoundaries(meta, icmp, key, parsed.Sequence);
                }
            }
            if (reader.NewRangeTombstoneIterator() is { } rit)
            {
                try
                {
                    for (rit.SeekToFirst(); rit.Valid; rit.Next())
                    {
                        // UpdateBoundariesForRange: the start key and the end key with the range deletion sentinel.
                        ReadOnlySpan<byte> start = rit.Key;
                        if (!DbFormat.TryParseInternalKey(start, out ParsedInternalKey p)) continue;
                        UpdateBoundaries(meta, icmp, start, p.Sequence);
                        byte[] end = new byte[rit.Value.Length + 8];
                        rit.Value.CopyTo(end);
                        Util.Coding.EncodeFixed64(end.AsSpan(rit.Value.Length), DbFormat.PackSequenceAndType(DbFormat.MaxSequenceNumber, EntryType.RangeDeletion));
                        UpdateBoundaries(meta, icmp, end, p.Sequence);
                    }
                }
                finally
                {
                    rit.Dispose();
                }
            }
            return t;
        }
    }

    private static void UpdateBoundaries(FileMetaData meta, InternalKeyComparator icmp, ReadOnlySpan<byte> key, ulong seq)
    {
        if (meta.Smallest.Length == 0 || icmp.Compare(key, meta.Smallest) < 0) meta.Smallest = key.ToArray();
        if (meta.Largest.Length == 0 || icmp.Compare(key, meta.Largest) > 0) meta.Largest = key.ToArray();
        meta.SmallestSeqno = Math.Min(meta.SmallestSeqno, seq);
        meta.LargestSeqno = Math.Max(meta.LargestSeqno, seq);
    }

    /// <summary>DBImpl::NewDB + Repairer::AddColumnFamily + Repairer::AddTables, written to a new MANIFEST-000001.</summary>
    private void WriteManifest()
    {
        // Column families: default first, then by id.
        var cfs = new SortedDictionary<uint, string> { [0] = ColumnFamilyDescriptor.DefaultColumnFamilyName };
        ulong maxSequence = 0;
        foreach (TableInfo t in _tables)
        {
            cfs.TryAdd(t.ColumnFamilyId, t.ColumnFamilyName);
            maxSequence = Math.Max(maxSequence, t.Meta.LargestSeqno);
        }
        const ulong manifestNumber = 1;
        if (_nextFileNumber <= manifestNumber) _nextFileNumber = manifestNumber + 1;
        string manifest = FileNames.DescriptorFileName(_dbname, manifestNumber);
        using (ManifestLogWriter log = ManifestLogWriter.Create(_fs, manifest))
        {
            var writer = new ManifestWriter(log);
            var newDb = new VersionEdit
            {
                LogNumber = 0,
                NextFileNumber = _nextFileNumber,
                LastSequence = 0,
                ComparatorName = _defaultCfOptions.ComparatorName,
                PersistUserDefinedTimestamps = _defaultCfOptions.PersistUserDefinedTimestamps,
            };
            if (_options.WriteDbIdToManifest) newDb.DbId = FileNames.GenerateUniqueId();
            writer.AddEditAsync(newDb, 0, sync: false, CancellationToken.None).AsTask().GetAwaiter().GetResult();
            uint maxCf = 0;
            foreach ((uint id, string name) in cfs)
            {
                maxCf = Math.Max(maxCf, id);
                ColumnFamilyOptions o = GetColumnFamilyOptions(name) ?? _defaultCfOptions;
                if (id != 0)
                {
                    var add = new VersionEdit
                    {
                        ComparatorName = o.ComparatorName,
                        PersistUserDefinedTimestamps = o.PersistUserDefinedTimestamps,
                        LogNumber = 0,
                        ColumnFamily = id,
                        MaxColumnFamily = maxCf,
                    };
                    add.AddColumnFamily(name);
                    writer.AddEditAsync(add, 0, sync: false, CancellationToken.None).AsTask().GetAwaiter().GetResult();
                }
                List<TableInfo> files = _tables.FindAll(t => t.ColumnFamilyId == id);
                if (files.Count == 0) continue;
                // RecoverEpochNumbers on level 0 sorted newest first (NewestFirstBySeqNo): the oldest file gets epoch 1.
                files.Sort(static (a, b) =>
                {
                    int c = a.Meta.LargestSeqno.CompareTo(b.Meta.LargestSeqno);
                    if (c == 0) c = a.Meta.SmallestSeqno.CompareTo(b.Meta.SmallestSeqno);
                    return c != 0 ? c : a.Meta.FileNumber.CompareTo(b.Meta.FileNumber);
                });
                var edit = new VersionEdit
                {
                    ComparatorName = o.ComparatorName,
                    PersistUserDefinedTimestamps = o.PersistUserDefinedTimestamps,
                    LogNumber = 0,
                    NextFileNumber = _nextFileNumber,
                    ColumnFamily = id,
                };
                ulong epoch = 1;
                foreach (TableInfo t in files)
                {
                    t.Meta.EpochNumber = epoch++;
                    edit.AddFile(0, t.Meta);
                }
                edit.LastSequence = maxSequence;
                writer.AddEditAsync(edit, 0, sync: false, CancellationToken.None).AsTask().GetAwaiter().GetResult();
            }
            log.SyncAsync(CancellationToken.None).AsTask().GetAwaiter().GetResult();
        }
        FileNames.SetCurrentFileAsync(_fs, _dbname, manifestNumber, syncDirectory: true, CancellationToken.None).AsTask().GetAwaiter().GetResult();
    }

    /// <summary>ConvertLogFilesToTables: replays the WALs into the repaired DB and flushes them to tables.</summary>
    private void ConvertLogFilesToTables()
    {
        if (_logs.Count == 0) return;
        DbOptions o = _options.Clone();
        o.CreateIfMissing = false;
        o.WalRecoveryMode = WalRecoveryMode.SkipAnyCorruptedRecords;
        o.AvoidFlushDuringRecovery = false;
        var descriptors = new List<ColumnFamilyDescriptor>();
        foreach (string name in RocksDatabase.ListColumnFamilies(_dbname, _fs))
        {
            descriptors.Add(new ColumnFamilyDescriptor(name, GetColumnFamilyOptions(name) ?? _defaultCfOptions));
        }
        using RocksDatabase db = RocksDatabase.Open(_dbname, o, descriptors);
        foreach (ColumnFamily cf in db.ColumnFamilies) cf.Flush();
    }

    /// <summary>Repairer::ArchiveFile: moves <paramref name="fname"/> into a "lost" directory next to it.</summary>
    private void ArchiveFile(string fname)
    {
        int slash = fname.LastIndexOf('/');
        string dir = (slash < 0 ? "" : fname[..slash]) + "/lost";
        try
        {
            _fs.CreateDirectoryIfMissing(dir);
            _fs.RenameFile(fname, dir + "/" + (slash < 0 ? fname : fname[(slash + 1)..]));
        }
        catch (Exception e) when (e is RocksDbException or IOException)
        {
            // Ignored, as upstream (only logged).
            _options.InfoLog?.Log(InfoLogLevel.Info, "Archiving " + fname + ": " + e.Message);
        }
    }
}
