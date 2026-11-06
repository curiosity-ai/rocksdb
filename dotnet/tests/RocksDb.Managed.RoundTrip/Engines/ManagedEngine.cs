using RocksDb.Managed.Compression;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.RoundTrip.Protocol;
using RocksDb.Managed.Table.Filters;
using RocksDb.Managed.Utilities.Transactions;

namespace RocksDb.Managed.RoundTrip.Engines;

/// <summary><see cref="IEngine"/> over the managed port (<see cref="RocksDatabase"/> / <see cref="TtlDatabase"/> /
/// <see cref="TransactionDatabase"/>).</summary>
public sealed class ManagedEngine : IEngine
{
    private readonly Dictionary<long, Snapshot> _snapshots = new();
    private readonly Dictionary<string, Transaction> _txns = new(StringComparer.Ordinal);
    private RocksDatabase? _db;
    private TtlDatabase? _ttl;
    private TransactionDatabase? _txnDb;
    private Transaction? _openTxn;
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

    public static ColumnFamilyOptions CfOptions(CfSpec s)
    {
        var o = new ColumnFamilyOptions
        {
            Comparator = s.UserTimestamps ? Comparator.BytewiseWithU64Ts : Comparator.Bytewise,
            WriteBufferSize = s.WriteBufferSize,
            Compression = s.Compression,
            DisableAutoCompactions = s.DisableAutoCompactions,
            MergeOperator = MergeOperatorFor(s),
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
                IndexType = s.Index,
                PartitionFilters = s.PartitionFilters,
                Checksum = s.Checksum,
                DataBlockIndexType = s.DataBlockHashIndex ? DataBlockIndexType.BinaryAndHash : DataBlockIndexType.BinarySearch,
                BlockRestartInterval = s.BlockRestartInterval,
                IndexBlockRestartInterval = s.IndexBlockRestartInterval,
                CacheIndexAndFilterBlocks = s.CacheIndexAndFilterBlocks,
                WholeKeyFiltering = s.WholeKeyFiltering,
            },
            PrefixExtractor = s.Prefix switch
            {
                PrefixKind.None => null,
                PrefixKind.Fixed => SliceTransform.CreateFixedPrefix(s.PrefixLength),
                PrefixKind.Capped => SliceTransform.CreateCappedPrefix(s.PrefixLength),
                _ => throw new ArgumentException("unknown prefix " + s.Prefix),
            },
            MemtablePrefixBloomSizeRatio = s.MemtablePrefixBloomRatio,
            MemtableWholeKeyFiltering = s.MemtableWholeKeyFiltering,
            OptimizeFiltersForHits = s.OptimizeFiltersForHits,
            CompactionStyle = s.Style,
            LevelCompactionDynamicLevelBytes = s.DynamicLevelBytes,
            NumLevels = s.NumLevels,
            TargetFileSizeBase = s.TargetFileSizeBase,
            MaxBytesForLevelBase = s.MaxBytesForLevelBase,
            Level0FileNumCompactionTrigger = s.Level0FileNumCompactionTrigger,
            CompactionPri = s.Pri,
            MemtableFactory = s.Memtable switch
            {
                MemtableKind.SkipList => MemTableFactoryType.SkipList,
                MemtableKind.Vector => MemTableFactoryType.Vector,
                MemtableKind.HashSkipList => MemTableFactoryType.HashSkipList,
                _ => throw new ArgumentException("unknown memtable " + s.Memtable),
            },
            InplaceUpdateSupport = s.InplaceUpdateSupport,
            MaxWriteBufferNumber = s.MaxWriteBufferNumber,
            MinWriteBufferNumberToMerge = s.MinWriteBufferNumberToMerge,
        };
        o.CompactionOptionsUniversal.SizeRatio = s.UniversalSizeRatio;
        o.CompactionOptionsUniversal.MaxSizeAmplificationPercent = s.UniversalMaxSizeAmplificationPercent;
        o.CompactionOptionsUniversal.AllowTrivialMove = s.UniversalAllowTrivialMove;
        o.CompactionOptionsFifo.MaxTableFilesSize = s.FifoMaxTableFilesSize;
        o.CompactionOptionsFifo.AllowCompaction = s.FifoAllowCompaction;
        if (s.Ttl > 0) o.Ttl = s.Ttl;
        if (s.PeriodicCompactionSeconds > 0) o.PeriodicCompactionSeconds = s.PeriodicCompactionSeconds;
        if (s.CompressionPerLevel.Count > 0) o.CompressionPerLevel = [.. s.CompressionPerLevel];
        if (s.BottommostCompression is { } bottom) o.BottommostCompression = bottom;
        o.CompressionOpts.Level = s.CompressionLevel;
        o.CompressionOpts.MaxDictBytes = s.MaxDictBytes;
        o.CompressionOpts.ZstdMaxTrainBytes = s.ZstdMaxTrainBytes;
        o.CompressionOpts.ParallelThreads = s.ParallelThreads;
        if (s.BottommostDictionary)
        {
            o.BottommostCompressionOpts.Level = s.CompressionLevel;
            o.BottommostCompressionOpts.MaxDictBytes = s.MaxDictBytes;
            o.BottommostCompressionOpts.ZstdMaxTrainBytes = s.ZstdMaxTrainBytes;
            o.BottommostCompressionOpts.Enabled = true;
        }
        return o;
    }

    private static MergeOperator? MergeOperatorFor(CfSpec s) => s.Merge switch
    {
        MergeKind.UInt64Add => UInt64AddOperator.Instance,
        MergeKind.StringAppend => new StringAppendOperator(s.MergeDelimiter),
        MergeKind.Max => new MaxOperator(),
        MergeKind.BytesXor => new BytesXorOperator(),
        _ => null,
    };

    public static DbOptions DbOptionsFor(string path, DbSpec spec)
    {
        DbWideSpec d = spec.Db;
        var o = new DbOptions
        {
            CreateIfMissing = true,
            CreateMissingColumnFamilies = true,
            WalRecoveryMode = d.WalRecoveryMode,
            RecycleLogFileNum = d.RecycleLogFileNum,
            ManualWalFlush = d.ManualWalFlush,
            WalCompression = d.WalCompression,
            TrackAndVerifyWals = d.TrackAndVerifyWals,
            TrackAndVerifyWalsInManifest = d.TrackAndVerifyWalsInManifest,
            MaxTotalWalSize = d.MaxTotalWalSize,
            AvoidFlushDuringRecovery = d.AvoidFlushDuringRecovery,
            AvoidFlushDuringShutdown = d.AvoidFlushDuringShutdown,
            AtomicFlush = d.AtomicFlush,
            UnorderedWrite = d.UnorderedWrite,
            EnablePipelinedWrite = d.EnablePipelinedWrite,
            AllowConcurrentMemtableWrite = d.AllowConcurrentMemtableWrite,
            TwoWriteQueues = d.TwoWriteQueues,
            MaxSubcompactions = d.MaxSubcompactions,
            AllowIngestBehind = d.AllowIngestBehind,
        };
        if (d.SeparateWalDir) o.WalDir = DbWideSpec.WalDirFor(path);
        return o;
    }

    // ---------------------------------------------------------------------------------------------------------
    // open / close

    public void Open(string path, DbSpec spec)
    {
        _spec = spec;
        DbOptions dbOptions = DbOptionsFor(path, spec);
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
        if (spec.TransactionDb)
        {
            _txnDb = TransactionDatabase.Open(path, dbOptions, new TransactionDbOptions(), descriptors);
            _db = _txnDb.Database;
            _txns.Clear();
            foreach (Transaction t in _txnDb.GetAllPreparedTransactions()) _txns.Add(t.Name!, t);
            return;
        }
        _db = RocksDatabase.Open(path, dbOptions, descriptors);
    }

    public void Close()
    {
        foreach (Snapshot s in _snapshots.Values) s.Dispose();
        _snapshots.Clear();
        foreach (Transaction t in _txns.Values) t.Dispose();
        _txns.Clear();
        _openTxn?.Dispose();
        _openTxn = null;
        if (_ttl is not null) _ttl.Dispose();
        else if (_txnDb is not null) _txnDb.Dispose();
        else _db?.Dispose();
        _db = null;
        _ttl = null;
        _txnDb = null;
    }

    public void Dispose() => Close();

    private ColumnFamily Cf(string name) => Db.GetColumnFamily(name);

    // ---------------------------------------------------------------------------------------------------------
    // writes

    public void Write(IReadOnlyList<Op> ops, bool sync, bool disableWal)
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
        var wo = new WriteOptions { Sync = sync, DisableWal = disableWal };
        if (_ttl is not null) _ttl.Write(batch);
        else if (_txnDb is not null) _txnDb.Write(wo, batch);
        else Db.Write(wo, batch);
    }

    public void Flush(string cf) => Cf(cf).Flush();

    /// <summary>The managed port has no multi-CF DB::Flush overload: every CF is flushed in turn (with atomic_flush
    /// each call is an atomic flush of that CF).</summary>
    public void FlushAll()
    {
        foreach (ColumnFamily cf in Db.ColumnFamilies) cf.Flush();
    }

    public void FlushWal(bool sync) => Db.FlushWal(sync);

    public bool CompactRange(string cf)
    {
        Cf(cf).CompactRange();
        return true;
    }

    public void CompactRange(string cf, byte[]? begin, byte[]? end, CompactSpec spec) =>
        Cf(cf).CompactRange(new CompactRangeOptions
        {
            BottommostLevelCompaction = spec.Bottommost,
            ExclusiveManualCompaction = spec.Exclusive,
            ChangeLevel = spec.ChangeLevel,
            TargetLevel = spec.TargetLevel,
            MaxSubcompactions = spec.MaxSubcompactions,
        }, begin, end);

    public void DeleteFilesInRange(string cf, byte[] begin, byte[] end) => Cf(cf).DeleteFilesInRange(begin, end, includeEnd: true);

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

    public void Ingest(string cf, IReadOnlyList<string> files, IngestFlags flags) =>
        Db.IngestExternalFiles(Cf(cf), files, new IngestExternalFileOptions
        {
            MoveFiles = (flags & IngestFlags.Move) != 0,
            IngestBehind = (flags & IngestFlags.IngestBehind) != 0,
            AllowGlobalSeqNo = (flags & IngestFlags.NoGlobalSeqno) == 0,
        });

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
    // transactions (TransactionDatabase, WriteCommitted)

    private TransactionDatabase TxnDb => _txnDb ?? throw new InvalidOperationException("not a TransactionDB (DbSpec.TransactionDb)");

    public void BeginTransaction(string name)
    {
        if (_openTxn is not null) throw new InvalidOperationException("a transaction is already open");
        _openTxn = TxnDb.BeginTransaction(new WriteOptions { Sync = true });
        _openTxn.SetName(name);
    }

    public void TransactionWrite(Op op)
    {
        Transaction t = _openTxn ?? throw new InvalidOperationException("no open transaction");
        ColumnFamily cf = Cf(op.Cf!);
        switch (op.Kind)
        {
            case OpKind.Put: t.Put(cf, op.Key, op.Value); break;
            case OpKind.Merge: t.Merge(cf, op.Key, op.Value); break;
            case OpKind.Delete: t.Delete(cf, op.Key); break;
            case OpKind.SingleDelete: t.SingleDelete(cf, op.Key); break;
            default: throw new InvalidOperationException("not supported in a transaction: " + op.Kind);
        }
    }

    public void PrepareTransaction(string name)
    {
        Transaction t = _openTxn ?? throw new InvalidOperationException("no open transaction");
        if (t.Name != name) throw new InvalidOperationException($"open transaction is {t.Name}, not {name}");
        t.Prepare();
        _txns.Add(name, t);
        _openTxn = null;
    }

    public void CommitTransaction(string name)
    {
        Transaction t = _txns[name];
        t.Commit();
        t.Dispose();
        _txns.Remove(name);
    }

    public void RollbackTransaction(string name)
    {
        Transaction t = _txns[name];
        t.Rollback();
        t.Dispose();
        _txns.Remove(name);
    }

    public IReadOnlyList<string> PreparedTransactionNames()
    {
        var names = new List<string>();
        foreach (Transaction t in TxnDb.GetAllPreparedTransactions()) names.Add(t.Name!);
        return names;
    }

    // ---------------------------------------------------------------------------------------------------------
    // reads

    private ReadOptions? ReadOptions(long? snapshot, string cf)
    {
        CfSpec s = _spec.Cf(cf);
        if (snapshot is null && !s.UserTimestamps && !s.HasPrefix) return null;
        var ro = new ReadOptions();
        if (snapshot is long id) ro.Snapshot = _snapshots[id];
        if (s.UserTimestamps) ro.Timestamp = UdtClock.MaxTimestamp;
        // Full scans across prefixes (prefix extractors, hash index, hash skip list memtable) need total order.
        if (s.HasPrefix) ro.TotalOrderSeek = true;
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
        // Point lookups use the prefix bloom filters (no total order seek).
        ReadOptions? ro = ReadOptions(snapshot, cf);
        if (ro is not null) ro.TotalOrderSeek = false;
        return Cf(cf).Get(ro, key);
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

    public override string ToString() => "managed" + (_ttl is not null ? "(ttl)" : "") + (_txnDb is not null ? "(txn)" : "");
}
