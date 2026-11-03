using RocksDb.Managed.Compression;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.RoundTrip.Protocol;
using RocksDb.Managed.Table.Filters;

namespace RocksDb.Managed.RoundTrip.Engines;

/// <summary><see cref="IEngine"/> over the managed port (<see cref="RocksDatabase"/> / <see cref="TtlDatabase"/>).</summary>
public sealed class ManagedEngine : IEngine
{
    private readonly Dictionary<long, Snapshot> _snapshots = new();
    private RocksDatabase? _db;
    private TtlDatabase? _ttl;
    private DbSpec _spec = new();

    public EngineKind Kind => EngineKind.Managed;

    /// <summary>Options of the last loaded OPTIONS file without a typed managed property (kept verbatim).</summary>
    public int LoadedUnknownOptions { get; private set; }

    private RocksDatabase Db => _db ?? throw new InvalidOperationException("managed DB is not open");

    public IReadOnlyList<string> ColumnFamilyNames
    {
        get
        {
            var names = new List<string>();
            foreach (ColumnFamily cf in Db.ColumnFamilies) names.Add(cf.Name);
            return names;
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // options

    public static ColumnFamilyOptions CfOptions(CfSpec s) => new()
    {
        Comparator = s.UserTimestamps ? Comparator.BytewiseWithU64Ts : Comparator.Bytewise,
        WriteBufferSize = s.WriteBufferSize,
        Compression = s.Compression,
        DisableAutoCompactions = s.DisableAutoCompactions,
        MergeOperator = MergeOperatorFor(s.Merge),
        EnableBlobFiles = s.EnableBlobFiles,
        MinBlobSize = s.MinBlobSize,
        BlobFileSize = s.BlobFileSize,
        BlobCompressionType = s.BlobCompression,
        EnableBlobGarbageCollection = s.EnableBlobGc,
        BlobGarbageCollectionAgeCutoff = s.BlobGcAgeCutoff,
        TableOptions = new BlockBasedTableOptions
        {
            FormatVersion = (uint)s.FormatVersion,
            BlockSize = s.BlockSize,
            FilterPolicy = s.Filter switch
            {
                FilterKind.None => null,
                FilterKind.Bloom => new BloomFilterPolicy(10),
                FilterKind.Ribbon => new RibbonFilterPolicy(10, 0), // NewRibbonFilterPolicy(10): bloom_before_level 0
                _ => throw new ArgumentException("unknown filter " + s.Filter),
            },
        },
    };

    private static MergeOperator? MergeOperatorFor(MergeKind kind) => kind switch
    {
        MergeKind.UInt64Add => UInt64AddOperator.Instance,
        MergeKind.StringAppend => new StringAppendOperator(),
        _ => null,
    };

    // ---------------------------------------------------------------------------------------------------------
    // open / close

    public void Open(string path, DbSpec spec)
    {
        _spec = spec;
        var dbOptions = new DbOptions { CreateIfMissing = true, CreateMissingColumnFamilies = true };
        if (spec.TtlSeconds > 0)
        {
            _ttl = TtlDatabase.Open(path, dbOptions, spec.TtlSeconds);
            _db = _ttl.Database;
            return;
        }
        var descriptors = new List<ColumnFamilyDescriptor>();
        if (spec.OptionsFromFile)
        {
            string file = OptionsFile.FindLatestOptionsFile(LocalFileSystem.Instance, path)
                ?? throw new InvalidOperationException("no OPTIONS file in " + path);
            // preserveUnknownOptions: the managed options model doesn't type every upstream option (they are kept
            // verbatim and written back), like upstream with ignore_unknown_options for newer files.
            OptionsFile options = OptionsFile.Load(LocalFileSystem.Instance, file, preserveUnknownOptions: true);
            dbOptions = options.DbOptions;
            int unknown = dbOptions.UnknownOptions.Count;
            foreach (ColumnFamilyDescriptor d in options.ColumnFamilies) unknown += d.Options.UnknownOptions.Count + d.Options.TableOptions.UnknownOptions.Count;
            LoadedUnknownOptions = unknown;
            foreach (ColumnFamilyDescriptor d in options.ColumnFamilies)
            {
                // OptionsFile recreates built-in merge operators (bare or struct form) like upstream's ObjectRegistry.
                if (d.Options.MergeOperator is null && d.Options.MergeOperatorName is { } unresolved)
                    throw new NotSupportedException("merge operator from OPTIONS file not known to the managed port: " + unresolved);
                descriptors.Add(d);
            }
        }
        else if (File.Exists(Path.Combine(path, "CURRENT")))
        {
            foreach (string name in RocksDatabase.ListColumnFamilies(path)) descriptors.Add(new ColumnFamilyDescriptor(name, CfOptions(spec.Cf(name))));
        }
        else
        {
            foreach (CfSpec cf in spec.ColumnFamilies)
            {
                if (cf.Initial) descriptors.Add(new ColumnFamilyDescriptor(cf.Name, CfOptions(cf)));
            }
        }
        _db = RocksDatabase.Open(path, dbOptions, descriptors);
    }

    public void Close()
    {
        foreach (Snapshot s in _snapshots.Values) s.Dispose();
        _snapshots.Clear();
        if (_ttl is not null) _ttl.Dispose();
        else _db?.Dispose();
        _db = null;
        _ttl = null;
    }

    public void Dispose() => Close();

    private ColumnFamily Cf(string name) => Db.GetColumnFamily(name);

    // ---------------------------------------------------------------------------------------------------------
    // writes

    public void Write(IReadOnlyList<Op> ops, bool sync)
    {
        using WriteBatch batch = Db.CreateWriteBatch();
        byte[] ts = UdtClock.Next();
        foreach (Op op in ops)
        {
            uint cf = Cf(op.Cf!).Id;
            if (_spec.Cf(op.Cf!).UserTimestamps)
            {
                switch (op.Kind)
                {
                    case OpKind.Put: batch.Put(cf, op.Key, ts, op.Value); break;
                    case OpKind.Merge: batch.Merge(cf, op.Key, ts, op.Value); break;
                    case OpKind.Delete: batch.Delete(cf, op.Key, ts); break;
                    case OpKind.SingleDelete: batch.SingleDelete(cf, op.Key, ts); break;
                    default: throw new InvalidOperationException("not supported with user-defined timestamps: " + op.Kind);
                }
                continue;
            }
            switch (op.Kind)
            {
                case OpKind.Put: batch.Put(cf, op.Key, op.Value); break;
                case OpKind.Merge: batch.Merge(cf, op.Key, op.Value); break;
                case OpKind.Delete: batch.Delete(cf, op.Key); break;
                case OpKind.SingleDelete: batch.SingleDelete(cf, op.Key); break;
                case OpKind.DeleteRange: batch.DeleteRange(cf, op.Key, op.Value); break;
                default: throw new InvalidOperationException("not a data op: " + op.Kind);
            }
        }
        if (_ttl is not null) _ttl.Write(batch);
        else Db.Write(new WriteOptions { Sync = sync }, batch);
    }

    public void Flush(string cf) => Cf(cf).Flush();

    public bool CompactRange(string cf)
    {
        Cf(cf).CompactRange();
        return true;
    }

    public void CreateColumnFamily(string cf) => Db.CreateColumnFamily(cf, CfOptions(_spec.Cf(cf)));

    public void DropColumnFamily(string cf) => Db.DropColumnFamily(Cf(cf));

    public void WriteSst(string cf, string path, IReadOnlyList<Op> entries)
    {
        using var writer = new SstFileWriter(CfOptions(_spec.Cf(cf)));
        writer.Open(path);
        foreach (Op e in entries)
        {
            switch (e.Kind)
            {
                case OpKind.SstPut: writer.Put(e.Key, e.Value); break;
                case OpKind.SstMerge: writer.Merge(e.Key, e.Value); break;
                case OpKind.SstDelete: writer.Delete(e.Key); break;
                case OpKind.SstDeleteRange: writer.DeleteRange(e.Key, e.Value); break;
                default: throw new InvalidOperationException("not an SST op: " + e.Kind);
            }
        }
        writer.Finish();
    }

    public void Ingest(string cf, IReadOnlyList<string> files, bool move) =>
        Db.IngestExternalFiles(Cf(cf), files, new IngestExternalFileOptions { MoveFiles = move });

    public void Checkpoint(string dir) => Db.CreateCheckpoint(dir);

    public void Backup(string backupDir, bool flushBeforeBackup)
    {
        using var be = Utilities.Backup.BackupEngine.Open(new Utilities.Backup.BackupEngineOptions(backupDir));
        be.CreateNewBackup(Db, flushBeforeBackup);
        be.VerifyBackup(be.GetBackupInfo()[^1].BackupId, verifyWithChecksum: true);
    }

    public void RestoreLatestBackup(string backupDir, string targetDir)
    {
        using var be = Utilities.Backup.BackupEngineReadOnly.Open(new Utilities.Backup.BackupEngineOptions(backupDir));
        be.RestoreDbFromLatestBackup(targetDir, targetDir, new Utilities.Backup.RestoreOptions());
    }

    public void TakeSnapshot(long id) => _snapshots.Add(id, Db.GetSnapshot());

    public void ReleaseSnapshot(long id)
    {
        _snapshots[id].Dispose();
        _snapshots.Remove(id);
    }

    // ---------------------------------------------------------------------------------------------------------
    // reads

    private ReadOptions? ReadOptions(long? snapshot, string cf)
    {
        bool udt = _spec.Cf(cf).UserTimestamps;
        if (snapshot is null && !udt) return null;
        var ro = new ReadOptions();
        if (snapshot is long id) ro.Snapshot = _snapshots[id];
        if (udt) ro.Timestamp = UdtClock.MaxTimestamp;
        return ro;
    }

    public IEnumerable<KeyValuePair<byte[], byte[]>> Scan(string cf, long? snapshot)
    {
        // TtlDatabase iterators strip the write-time suffix (TtlIterator).
        using Iterator it = _ttl is not null ? _ttl.NewIterator(ReadOptions(snapshot, cf)) : Cf(cf).NewIterator(ReadOptions(snapshot, cf));
        for (it.SeekToFirst(); it.Valid; it.Next())
        {
            yield return new(it.Key.ToArray(), it.Value.ToArray());
        }
    }

    public byte[]? Get(string cf, byte[] key, long? snapshot)
    {
        if (_ttl is not null) return _ttl.Get(key);
        return Cf(cf).Get(ReadOptions(snapshot, cf), key);
    }

    public int[] FilesPerLevel(string cf)
    {
        int[] r = new int[7];
        foreach (LiveFileMetadata f in Db.GetLiveFilesMetadata())
        {
            if (f.ColumnFamilyName == cf) r[f.Level]++;
        }
        return r;
    }

    public override string ToString() => "managed" + (_ttl is not null ? "(ttl)" : "");
}
