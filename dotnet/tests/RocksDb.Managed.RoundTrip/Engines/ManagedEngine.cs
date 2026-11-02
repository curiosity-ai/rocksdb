using RocksDb.Managed.Compression;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.RoundTrip.Protocol;

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

    private static CompressionType Compression(string name) => name switch
    {
        "none" => CompressionType.None,
        "snappy" => CompressionType.Snappy,
        "lz4" => CompressionType.LZ4,
        "zstd" => CompressionType.Zstd,
        _ => throw new ArgumentException("unknown compression " + name),
    };

    public static ColumnFamilyOptions CfOptions(CfSpec s) => new()
    {
        WriteBufferSize = s.WriteBufferSize,
        Compression = Compression(s.Compression),
        DisableAutoCompactions = s.DisableAutoCompactions,
        MergeOperator = MergeOperatorFor(s.Merge),
        TableOptions = new BlockBasedTableOptions
        {
            FormatVersion = (uint)s.FormatVersion,
            BlockSize = s.BlockSize,
            FilterPolicy = s.Filter switch
            {
                "none" => null,
                "bloom" => "bloomfilter:10:false",
                "ribbon" => "ribbonfilter:10",
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

    /// <summary>Instantiates a built-in merge operator recorded in an OPTIONS file, either as a bare name
    /// ("StringAppendOperator") or in the struct form native writes for configurable objects
    /// ("{id=StringAppendOperator;delimiter=,;}"). Upstream's ObjectRegistry does this when loading options; the
    /// managed OptionsFile keeps only the string.</summary>
    private static MergeOperator? MergeOperatorByName(string? value)
    {
        if (value is null or "" or "nullptr") return null;
        string id = value;
        string? delimiter = null;
        if (value.StartsWith('{') && value.EndsWith('}'))
        {
            id = "";
            foreach (string part in value[1..^1].Split(';', StringSplitOptions.RemoveEmptyEntries))
            {
                int eq = part.IndexOf('=');
                if (eq < 0) continue;
                string k = part[..eq], v = part[(eq + 1)..];
                if (k == "id") id = v;
                else if (k == "delimiter") delimiter = v;
            }
        }
        return id switch
        {
            "UInt64AddOperator" or "uint64add" => UInt64AddOperator.Instance,
            "StringAppendOperator" or "stringappend" => delimiter is null ? new StringAppendOperator() : new StringAppendOperator(delimiter),
            _ => throw new NotSupportedException("merge operator from OPTIONS file not known to the round-trip tests: " + value),
        };
    }

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
                d.Options.MergeOperator = MergeOperatorByName(d.Options.MergeOperatorName);
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
        foreach (Op op in ops)
        {
            uint cf = Cf(op.Cf!).Id;
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

    public void TakeSnapshot(long id) => _snapshots.Add(id, Db.GetSnapshot());

    public void ReleaseSnapshot(long id)
    {
        _snapshots[id].Dispose();
        _snapshots.Remove(id);
    }

    // ---------------------------------------------------------------------------------------------------------
    // reads

    private ReadOptions? ReadOptions(long? snapshot) => snapshot is long id ? new ReadOptions { Snapshot = _snapshots[id] } : null;

    public IEnumerable<KeyValuePair<byte[], byte[]>> Scan(string cf, long? snapshot)
    {
        using Iterator it = Cf(cf).NewIterator(ReadOptions(snapshot));
        for (it.SeekToFirst(); it.Valid; it.Next())
        {
            byte[] v = _ttl is not null ? TtlValue.Strip(it.Value).ToArray() : it.Value.ToArray();
            yield return new(it.Key.ToArray(), v);
        }
    }

    public byte[]? Get(string cf, byte[] key, long? snapshot)
    {
        if (_ttl is not null) return _ttl.Get(key);
        return Cf(cf).Get(ReadOptions(snapshot), key);
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
