using RocksDb.Managed.Compression;
using System.Reflection;
using System.Runtime.InteropServices;
using RocksDb.Managed.RoundTrip.Protocol;
using N = RocksDbSharp;

namespace RocksDb.Managed.RoundTrip.Engines;

/// <summary>
/// <see cref="IEngine"/> over native RocksDB through rocksdb-sharp. Only ever used inside a child process (see
/// <see cref="Harness.ChildHost"/>): a native crash then kills the child, not the test host.
/// </summary>
public sealed class NativeEngine : IEngine
{
    // rocksdb-sharp resolves merge callbacks through its managed options objects, and its options finalizers destroy
    // the native handles: everything handed to native code is kept alive for the (child) process lifetime.
    private static readonly List<object> s_keepAlive = [];

    private readonly Dictionary<long, N.Snapshot> _snapshots = new();
    private readonly List<string> _cfNames = [];
    private N.RocksDb? _db;
    private DbSpec _spec = new();
    private bool _ttl;

    public EngineKind Kind => EngineKind.Native;

    public IReadOnlyList<string> ColumnFamilyNames => _cfNames;

    private N.RocksDb Db => _db ?? throw new InvalidOperationException("native DB is not open");

    private static T Keep<T>(T o) where T : notnull
    {
        lock (s_keepAlive) s_keepAlive.Add(o);
        return o;
    }

    // ---------------------------------------------------------------------------------------------------------
    // options

    private static N.Compression Compression(CompressionType compression) => compression switch
    {
        CompressionType.None => N.Compression.No,
        CompressionType.Snappy => N.Compression.Snappy,
        CompressionType.LZ4 => N.Compression.Lz4,
        CompressionType.Zstd => N.Compression.Zstd,
        _ => throw new ArgumentException("unsupported compression " + compression),
    };

    /// <summary>Merge operator with the semantics of the C++ StringAppendOperator (delimiter ',').</summary>
    private static N.MergeOperator StringAppend() => N.MergeOperators.Create(
        "StringAppendOperator",
        static (ReadOnlySpan<byte> key, N.MergeOperators.OperandsEnumerator operands, out bool success) =>
        {
            success = true;
            return Join(default, false, operands);
        },
        static (ReadOnlySpan<byte> key, bool hasExisting, ReadOnlySpan<byte> existing, N.MergeOperators.OperandsEnumerator operands, out bool success) =>
        {
            success = true;
            return Join(existing, hasExisting, operands);
        });

    private static byte[] Join(ReadOnlySpan<byte> existing, bool hasExisting, N.MergeOperators.OperandsEnumerator operands)
    {
        var result = new List<byte>();
        bool any = hasExisting;
        if (hasExisting) result.AddRange(existing.ToArray());
        for (int i = 0; i < operands.Count; i++)
        {
            if (any) result.Add((byte)',');
            result.AddRange(operands.Get(i).ToArray());
            any = true;
        }
        return result.ToArray();
    }

    private static T Configure<T>(T o, CfSpec s) where T : N.Options<T>
    {
        o.SetWriteBufferSize(s.WriteBufferSize)
         .SetCompression(Compression(s.Compression))
         .SetDisableAutoCompactions(s.DisableAutoCompactions ? 1 : 0);
        if (s.EnableBlobFiles)
        {
            // Blob options through the C API (rocksdb_options_set_*blob*), not wrapped by rocksdb-sharp.
            N.Native n = N.Native.Instance;
            n.rocksdb_options_set_enable_blob_files(o.Handle, 1);
            n.rocksdb_options_set_min_blob_size(o.Handle, s.MinBlobSize);
            n.rocksdb_options_set_blob_file_size(o.Handle, s.BlobFileSize);
            n.rocksdb_options_set_blob_compression_type(o.Handle, (int)s.BlobCompression);
            n.rocksdb_options_set_enable_blob_gc(o.Handle, s.EnableBlobGc ? (byte)1 : (byte)0);
            n.rocksdb_options_set_blob_gc_age_cutoff(o.Handle, s.BlobGcAgeCutoff);
        }
        var table = Keep(new N.BlockBasedTableOptions())
            .SetFormatVersion(s.FormatVersion)
            .SetBlockSize(s.BlockSize);
        switch (s.Filter)
        {
            case FilterKind.None: break;
            case FilterKind.Bloom: table.SetFilterPolicy(Keep(N.BloomFilterPolicy.Create(10, false))); break;
            case FilterKind.Ribbon: table.SetFilterPolicy(N.Native.Instance.rocksdb_filterpolicy_create_ribbon(10)); break;
            default: throw new ArgumentException("unknown filter " + s.Filter);
        }
        o.SetBlockBasedTableFactory(table);
        switch (s.Merge)
        {
            case MergeKind.UInt64Add: o.SetUint64addMergeOperator(); break;
            case MergeKind.StringAppend: o.SetMergeOperator(Keep(StringAppend())); break;
        }
        return o;
    }

    private static N.ColumnFamilyOptions CfOptions(CfSpec s) => Keep(Configure(new N.ColumnFamilyOptions(), s));

    // ---------------------------------------------------------------------------------------------------------
    // open / close

    public void Open(string path, DbSpec spec)
    {
        _spec = spec;
        _cfNames.Clear();
        bool exists = File.Exists(Path.Combine(path, "CURRENT"));
        if (spec.TtlSeconds > 0)
        {
            var o = Keep(Configure(new N.DbOptions(), spec.Cf("default")));
            o.SetCreateIfMissing(true);
            _db = N.RocksDb.OpenWithTtl(o, path, spec.TtlSeconds);
            _ttl = true;
            _cfNames.Add("default");
            return;
        }
        _ttl = false;
        if (spec.OptionsFromFile)
        {
            OpenWithLatestOptionsFile(path);
            return;
        }
        var dbOptions = Keep(new N.DbOptions().SetCreateIfMissing(true).SetCreateMissingColumnFamilies(true));
        var names = new List<string>();
        if (exists)
        {
            if (!N.RocksDb.TryListColumnFamilies(dbOptions, path, out string[] onDisk)) throw new InvalidOperationException("ListColumnFamilies failed for " + path);
            names.AddRange(onDisk);
        }
        else
        {
            foreach (CfSpec cf in spec.ColumnFamilies)
            {
                if (cf.Initial) names.Add(cf.Name);
            }
        }
        var cfs = Keep(new N.ColumnFamilies(CfOptions(spec.Cf("default"))));
        foreach (string name in names)
        {
            if (name != "default") cfs.Add(name, CfOptions(spec.Cf(name)));
        }
        _db = N.RocksDb.Open(dbOptions, path, cfs);
        _cfNames.Add("default");
        foreach (string name in names)
        {
            if (name != "default") _cfNames.Add(name);
        }
    }

    /// <summary>
    /// Opens with the options from the DB's latest OPTIONS file (rocksdb::LoadLatestOptions through the C API), so the
    /// native library has to parse and instantiate every option (incl. merge operators by name) the file records.
    /// </summary>
    private unsafe void OpenWithLatestOptionsFile(string path)
    {
        IntPtr env = N.Native.Instance.rocksdb_create_default_env();
        IntPtr cache = N.Native.Instance.rocksdb_cache_create_lru(8 << 20);
        IntPtr dbOpts = IntPtr.Zero, names = IntPtr.Zero, cfOpts = IntPtr.Zero;
        nuint count = 0;
        N.Native.Instance.rocksdb_load_latest_options(path, env, 0, cache,
            (IntPtr)(&dbOpts), (IntPtr)(&count), (IntPtr)(&names), (IntPtr)(&cfOpts));
        // The loaded handles are owned by the wrappers below and never destroyed (they are kept alive until exit).
        var dbOptions = Keep(Adopt(new N.DbOptions(), dbOpts));
        var all = new List<(string Name, N.ColumnFamilyOptions Options)>();
        for (int i = 0; i < (int)count; i++)
        {
            string name = Marshal.PtrToStringUTF8(Marshal.ReadIntPtr(names, i * IntPtr.Size))!;
            all.Add((name, Keep(Adopt(new N.ColumnFamilyOptions(), Marshal.ReadIntPtr(cfOpts, i * IntPtr.Size)))));
        }
        // ColumnFamilies() always contains "default"; replace its options with the loaded ones.
        var cfs = Keep(new N.ColumnFamilies(all.Find(c => c.Name == "default").Options));
        foreach ((string name, N.ColumnFamilyOptions o) in all)
        {
            if (name != "default") cfs.Add(name, o);
        }
        _db = N.RocksDb.Open(dbOptions, path, cfs);
        _cfNames.Add("default");
        foreach ((string name, _) in all)
        {
            if (name != "default") _cfNames.Add(name);
        }
    }

    private static readonly FieldInfo s_handleField = typeof(N.OptionsHandle).GetField("<Handle>k__BackingField", BindingFlags.Instance | BindingFlags.NonPublic)
        ?? throw new InvalidOperationException("rocksdb-sharp OptionsHandle layout changed");

    /// <summary>Makes a rocksdb-sharp options wrapper own <paramref name="handle"/> (the C API has no other way to
    /// wrap options returned by rocksdb_load_latest_options).</summary>
    private static T Adopt<T>(T wrapper, IntPtr handle) where T : N.OptionsHandle
    {
        N.Native.Instance.rocksdb_options_destroy(wrapper.Handle);
        s_handleField.SetValue(wrapper, handle);
        return wrapper;
    }

    public void Close()
    {
        foreach (N.Snapshot s in _snapshots.Values) s.Dispose();
        _snapshots.Clear();
        _db?.Dispose();
        _db = null;
    }

    public void Dispose() => Close();

    /// <summary>The CF handle; null (= default CF in rocksdb-sharp calls) for a TTL DB, which rocksdb-sharp opens
    /// without column family handles.</summary>
    private N.ColumnFamilyHandle Cf(string name)
    {
        if (!_ttl) return Db.GetColumnFamily(name);
        if (name != "default") throw new InvalidOperationException("TTL DBs only have the default column family");
        return null!;
    }

    // ---------------------------------------------------------------------------------------------------------
    // writes

    public void Write(IReadOnlyList<Op> ops, bool sync)
    {
        using var batch = new N.WriteBatch();
        foreach (Op op in ops)
        {
            N.ColumnFamilyHandle cf = Cf(op.Cf!);
            byte[] k = op.Key!;
            switch (op.Kind)
            {
                case OpKind.Put: batch.Put(k, (ulong)k.Length, op.Value!, (ulong)op.Value!.Length, cf); break;
                case OpKind.Merge: batch.Merge(k, (ulong)k.Length, op.Value!, (ulong)op.Value!.Length, cf); break;
                case OpKind.Delete: batch.Delete(k, (ulong)k.Length, cf); break;
                case OpKind.SingleDelete:
                    if (cf is null) N.Native.Instance.rocksdb_writebatch_singledelete(batch.Handle, k, (nuint)k.Length);
                    else N.Native.Instance.rocksdb_writebatch_singledelete_cf(batch.Handle, cf.Handle, k, (nuint)k.Length);
                    break;
                case OpKind.DeleteRange: batch.DeleteRange(k, (ulong)k.Length, op.Value!, (ulong)op.Value!.Length, cf); break;
                default: throw new InvalidOperationException("not a data op: " + op.Kind);
            }
        }
        var wo = new N.WriteOptions().SetSync(sync);
        Db.Write(batch, wo);
        GC.KeepAlive(wo);
    }

    public void Flush(string cf)
    {
        var fo = new N.FlushOptions().SetWaitForFlush(true);
        if (_ttl) Db.Flush(fo);
        else N.Native.Instance.rocksdb_flush_cf(Db.Handle, fo.Handle, Cf(cf).Handle);
        GC.KeepAlive(fo);
    }

    public bool CompactRange(string cf)
    {
        Db.CompactRange((byte[])null!, (byte[])null!, Cf(cf));
        return true;
    }

    public void CreateColumnFamily(string cf)
    {
        Db.CreateColumnFamily(CfOptions(_spec.Cf(cf)), cf);
        _cfNames.Add(cf);
    }

    public void DropColumnFamily(string cf)
    {
        Db.DropColumnFamily(cf);
        _cfNames.Remove(cf);
    }

    public void WriteSst(string cf, string path, IReadOnlyList<Op> entries)
    {
        using var writer = new N.SstFileWriter(Keep(new N.EnvOptions()), CfOptions(_spec.Cf(cf)));
        writer.Open(path);
        foreach (Op e in entries)
        {
            switch (e.Kind)
            {
                case OpKind.SstPut: writer.Put(e.Key!, e.Value!); break;
                case OpKind.SstMerge: writer.Merge(e.Key!, e.Value!); break;
                case OpKind.SstDelete: writer.Delete(e.Key!); break;
                case OpKind.SstDeleteRange:
                    N.Native.Instance.rocksdb_sstfilewriter_delete_range(writer.Handle, e.Key!, (nuint)e.Key!.Length, e.Value!, (nuint)e.Value!.Length);
                    break;
                default: throw new InvalidOperationException("not an SST op: " + e.Kind);
            }
        }
        writer.Finish();
    }

    public void Ingest(string cf, IReadOnlyList<string> files, bool move)
    {
        var o = Keep(new N.IngestExternalFileOptions().SetMoveFiles(move));
        Db.IngestExternalFiles([.. files], o, Cf(cf));
    }

    public void Checkpoint(string dir)
    {
        using N.Checkpoint cp = Db.Checkpoint();
        cp.Save(dir);
    }

    public void TakeSnapshot(long id) => _snapshots.Add(id, Db.CreateSnapshot());

    public void ReleaseSnapshot(long id)
    {
        _snapshots[id].Dispose();
        _snapshots.Remove(id);
    }

    // ---------------------------------------------------------------------------------------------------------
    // reads

    private N.ReadOptions ReadOptions(long? snapshot)
    {
        var ro = new N.ReadOptions();
        if (snapshot is long id) ro.SetSnapshot(_snapshots[id]);
        return ro;
    }

    public IEnumerable<KeyValuePair<byte[], byte[]>> Scan(string cf, long? snapshot)
    {
        N.ReadOptions ro = ReadOptions(snapshot);
        using N.Iterator it = Db.NewIterator(Cf(cf), ro);
        for (it.SeekToFirst(); it.Valid(); it.Next()) yield return new(it.Key(), it.Value());
        GC.KeepAlive(ro);
    }

    public byte[]? Get(string cf, byte[] key, long? snapshot)
    {
        N.ReadOptions ro = ReadOptions(snapshot);
        byte[]? v = Db.Get(key, Cf(cf), ro);
        GC.KeepAlive(ro);
        return v;
    }

    public int[] FilesPerLevel(string cf)
    {
        int[] r = new int[7];
        for (int level = 0; level < r.Length; level++)
        {
            string name = "rocksdb.num-files-at-level" + level.ToString(System.Globalization.CultureInfo.InvariantCulture);
            string? p = _ttl ? Db.GetProperty(name) : Db.GetProperty(name, Cf(cf));
            r[level] = p is null ? 0 : int.Parse(p, System.Globalization.CultureInfo.InvariantCulture);
        }
        return r;
    }

    public override string ToString() => "native" + (_ttl ? "(ttl)" : "");
}
