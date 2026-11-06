using RocksDb.Managed.Compression;
using System.Globalization;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using RocksDb.Managed.RoundTrip.Protocol;
using N = RocksDbSharp;

namespace RocksDb.Managed.RoundTrip.Engines;

/// <summary>
/// <see cref="IEngine"/> over native RocksDB through rocksdb-sharp. Only ever used inside a child process (see
/// <see cref="Harness.ChildHost"/>): a native crash then kills the child, not the test host.
/// A TransactionDB (<see cref="DbSpec.TransactionDb"/>) is driven through the raw C API (rocksdb_transactiondb_*),
/// which rocksdb-sharp does not wrap.
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

    // TransactionDB mode (raw C API handles).
    private IntPtr _txnDb, _txnBaseDb;
    private readonly Dictionary<string, IntPtr> _txnCfs = new(StringComparer.Ordinal);
    private readonly Dictionary<string, IntPtr> _txns = new(StringComparer.Ordinal);
    private IntPtr _openTxn;
    private string? _openTxnName;

    public EngineKind Kind => EngineKind.Native;

    public IReadOnlyList<string> ColumnFamilyNames => _cfNames;

    private N.RocksDb Db => _db ?? throw new InvalidOperationException("native DB is not open" + (_txnDb != IntPtr.Zero ? " as a plain DB (TransactionDB mode)" : ""));

    private bool TxnMode => _txnDb != IntPtr.Zero;

    /// <summary>The rocksdb_t* to use for DB-level calls (the TransactionDB's base DB in TransactionDB mode).</summary>
    private IntPtr DbHandle => TxnMode ? _txnBaseDb : Db.Handle;

    private static N.Native Api => N.Native.Instance;

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

    /// <summary>
    /// Options the C API has no setter for, in GetOptionsFromString syntax: built-in merge operators by name (the C++
    /// MaxOperator / BytesXOROperator / StringAppendOperator with a delimiter) and the capped prefix extractor.
    /// </summary>
    private static string OptionsStringFor(CfSpec s)
    {
        var parts = new List<string>();
        if (s.NativeBuiltinMerge)
        {
            parts.Add(s.Merge switch
            {
                MergeKind.Max => "merge_operator=max",
                MergeKind.BytesXor => "merge_operator=bytesxor",
                _ => "merge_operator={id=StringAppendOperator;delimiter=" + s.MergeDelimiter + "}",
            });
        }
        if (s.Prefix == PrefixKind.Capped) parts.Add("prefix_extractor=rocksdb.CappedPrefix." + s.PrefixLength.ToString(CultureInfo.InvariantCulture));
        return string.Join(";", parts);
    }

    private static readonly FieldInfo s_handleField = typeof(N.OptionsHandle).GetField("<Handle>k__BackingField", BindingFlags.Instance | BindingFlags.NonPublic)
        ?? throw new InvalidOperationException("rocksdb-sharp OptionsHandle layout changed");

    /// <summary>Makes a rocksdb-sharp options wrapper own <paramref name="handle"/> (the C API has no other way to
    /// wrap options returned by rocksdb_load_latest_options / rocksdb_get_options_from_string).</summary>
    private static T Adopt<T>(T wrapper, IntPtr handle) where T : N.OptionsHandle
    {
        Api.rocksdb_options_destroy(wrapper.Handle);
        s_handleField.SetValue(wrapper, handle);
        return wrapper;
    }

    private static T Configure<T>(T o, CfSpec s) where T : N.Options<T>
    {
        string extra = OptionsStringFor(s);
        if (extra.Length > 0)
        {
            IntPtr fresh = Api.rocksdb_options_create();
            IntPtr str = Marshal.StringToHGlobalAnsi(extra);
            try { Api.rocksdb_get_options_from_string(o.Handle, str, fresh); }
            finally { Marshal.FreeHGlobal(str); }
            Adopt(o, fresh);
        }
        o.SetWriteBufferSize(s.WriteBufferSize)
         .SetCompression(Compression(s.Compression))
         .SetDisableAutoCompactions(s.DisableAutoCompactions ? 1 : 0);
        IntPtr h = o.Handle;
        if (s.EnableBlobFiles)
        {
            // Blob options through the C API (rocksdb_options_set_*blob*), not wrapped by rocksdb-sharp.
            Api.rocksdb_options_set_enable_blob_files(h, 1);
            Api.rocksdb_options_set_min_blob_size(h, s.MinBlobSize);
            Api.rocksdb_options_set_blob_file_size(h, s.BlobFileSize);
            Api.rocksdb_options_set_blob_compression_type(h, (int)s.BlobCompression);
            Api.rocksdb_options_set_enable_blob_gc(h, s.EnableBlobGc ? (byte)1 : (byte)0);
            Api.rocksdb_options_set_blob_gc_age_cutoff(h, s.BlobGcAgeCutoff);
        }
        var table = Keep(new N.BlockBasedTableOptions())
            .SetFormatVersion(s.FormatVersion)
            .SetBlockSize(s.BlockSize);
        switch (s.Filter)
        {
            case FilterKind.None: break;
            case FilterKind.Bloom: table.SetFilterPolicy(Keep(N.BloomFilterPolicy.Create(10, false))); break;
            case FilterKind.Ribbon: table.SetFilterPolicy(Api.rocksdb_filterpolicy_create_ribbon(10)); break;
            default: throw new ArgumentException("unknown filter " + s.Filter);
        }
        IntPtr t = table.Handle;
        Api.rocksdb_block_based_options_set_index_type(t, (int)s.Index);
        Api.rocksdb_block_based_options_set_partition_filters(t, s.PartitionFilters ? (byte)1 : (byte)0);
        Api.rocksdb_block_based_options_set_checksum(t, (char)(int)s.Checksum);
        Api.rocksdb_block_based_options_set_data_block_index_type(t, s.DataBlockHashIndex ? 1 : 0);
        Api.rocksdb_block_based_options_set_block_restart_interval(t, s.BlockRestartInterval);
        Api.rocksdb_block_based_options_set_index_block_restart_interval(t, s.IndexBlockRestartInterval);
        Api.rocksdb_block_based_options_set_cache_index_and_filter_blocks(t, s.CacheIndexAndFilterBlocks ? (byte)1 : (byte)0);
        Api.rocksdb_block_based_options_set_whole_key_filtering(t, s.WholeKeyFiltering ? (byte)1 : (byte)0);
        o.SetBlockBasedTableFactory(table);
        switch (s.Merge)
        {
            case MergeKind.UInt64Add: o.SetUint64addMergeOperator(); break;
            case MergeKind.StringAppend when !s.NativeBuiltinMerge: o.SetMergeOperator(Keep(StringAppend())); break;
        }
        if (s.Prefix == PrefixKind.Fixed) Api.rocksdb_options_set_prefix_extractor(h, Api.rocksdb_slicetransform_create_fixed_prefix((UIntPtr)s.PrefixLength));
        if (s.MemtablePrefixBloomRatio > 0) Api.rocksdb_options_set_memtable_prefix_bloom_size_ratio(h, s.MemtablePrefixBloomRatio);
        if (s.MemtableWholeKeyFiltering) Api.rocksdb_options_set_memtable_whole_key_filtering(h, 1);
        if (s.OptimizeFiltersForHits) Api.rocksdb_options_set_optimize_filters_for_hits(h, 1);

        if (s.CompressionPerLevel.Count > 0)
        {
            int[] levels = new int[s.CompressionPerLevel.Count];
            for (int i = 0; i < levels.Length; i++) levels[i] = (int)s.CompressionPerLevel[i];
            Api.rocksdb_options_set_compression_per_level(h, levels, (UIntPtr)levels.Length);
        }
        if (s.BottommostCompression is { } bottom) Api.rocksdb_options_set_bottommost_compression(h, (int)bottom);
        if (s.CompressionLevel != 32767 || s.MaxDictBytes > 0)
            Api.rocksdb_options_set_compression_options(h, -14, s.CompressionLevel, 0, (int)s.MaxDictBytes);
        if (s.ZstdMaxTrainBytes > 0) Api.rocksdb_options_set_compression_options_zstd_max_train_bytes(h, (int)s.ZstdMaxTrainBytes);
        if (s.ParallelThreads > 1) Api.rocksdb_options_set_compression_options_parallel_threads(h, (int)s.ParallelThreads);
        if (s.BottommostDictionary)
        {
            Api.rocksdb_options_set_bottommost_compression_options(h, -14, s.CompressionLevel, 0, (int)s.MaxDictBytes, 1);
            Api.rocksdb_options_set_bottommost_compression_options_zstd_max_train_bytes(h, (int)s.ZstdMaxTrainBytes, 1);
        }

        Api.rocksdb_options_set_compaction_style(h, (int)s.Style);
        Api.rocksdb_options_set_level_compaction_dynamic_level_bytes(h, s.DynamicLevelBytes ? (byte)1 : (byte)0);
        Api.rocksdb_options_set_num_levels(h, s.NumLevels);
        Api.rocksdb_options_set_target_file_size_base(h, s.TargetFileSizeBase);
        Api.rocksdb_options_set_max_bytes_for_level_base(h, s.MaxBytesForLevelBase);
        Api.rocksdb_options_set_level0_file_num_compaction_trigger(h, s.Level0FileNumCompactionTrigger);
        Api.rocksdb_options_set_compaction_pri(h, (int)s.Pri);
        if (s.Style == CompactionStyle.Universal)
        {
            IntPtr u = Api.rocksdb_universal_compaction_options_create();
            Api.rocksdb_universal_compaction_options_set_size_ratio(u, (int)s.UniversalSizeRatio);
            Api.rocksdb_universal_compaction_options_set_max_size_amplification_percent(u, (int)s.UniversalMaxSizeAmplificationPercent);
            Api.rocksdb_universal_compaction_options_set_allow_trivial_move(u, s.UniversalAllowTrivialMove ? (byte)1 : (byte)0);
            Api.rocksdb_options_set_universal_compaction_options(h, u); // copied; the struct is leaked with the process
        }
        if (s.Style == CompactionStyle.Fifo)
        {
            IntPtr f = Api.rocksdb_fifo_compaction_options_create();
            Api.rocksdb_fifo_compaction_options_set_max_table_files_size(f, s.FifoMaxTableFilesSize);
            Api.rocksdb_fifo_compaction_options_set_allow_compaction(f, s.FifoAllowCompaction ? (byte)1 : (byte)0);
            Api.rocksdb_options_set_fifo_compaction_options(h, f);
        }
        if (s.Ttl > 0) Api.rocksdb_options_set_ttl(h, s.Ttl);
        if (s.PeriodicCompactionSeconds > 0) Api.rocksdb_options_set_periodic_compaction_seconds(h, s.PeriodicCompactionSeconds);

        switch (s.Memtable)
        {
            case MemtableKind.Vector: Api.rocksdb_options_set_memtable_vector_rep(h); break;
            case MemtableKind.HashSkipList: Api.rocksdb_options_set_hash_skip_list_rep(h, (UIntPtr)1_000_000, 4, 4); break; // NewHashSkipListRepFactory defaults
        }
        if (s.InplaceUpdateSupport) Api.rocksdb_options_set_inplace_update_support(h, 1);
        Api.rocksdb_options_set_max_write_buffer_number(h, s.MaxWriteBufferNumber);
        Api.rocksdb_options_set_min_write_buffer_number_to_merge(h, s.MinWriteBufferNumberToMerge);
        return o;
    }

    private static void ConfigureDb(IntPtr h, string path, DbWideSpec d)
    {
        Api.rocksdb_options_set_wal_recovery_mode(h, (int)d.WalRecoveryMode);
        Api.rocksdb_options_set_recycle_log_file_num(h, (UIntPtr)d.RecycleLogFileNum);
        Api.rocksdb_options_set_manual_wal_flush(h, d.ManualWalFlush ? (byte)1 : (byte)0);
        Api.rocksdb_options_set_wal_compression(h, (int)d.WalCompression);
        Api.rocksdb_options_set_track_and_verify_wals(h, d.TrackAndVerifyWals ? (byte)1 : (byte)0);
        Api.rocksdb_options_set_track_and_verify_wals_in_manifest(h, d.TrackAndVerifyWalsInManifest ? (byte)1 : (byte)0);
        Api.rocksdb_options_set_max_total_wal_size(h, d.MaxTotalWalSize);
        Api.rocksdb_options_set_avoid_flush_during_recovery(h, d.AvoidFlushDuringRecovery ? (byte)1 : (byte)0);
        Api.rocksdb_options_set_avoid_flush_during_shutdown(h, d.AvoidFlushDuringShutdown ? (byte)1 : (byte)0);
        Api.rocksdb_options_set_atomic_flush(h, d.AtomicFlush ? (byte)1 : (byte)0);
        Api.rocksdb_options_set_unordered_write(h, d.UnorderedWrite ? (byte)1 : (byte)0);
        Api.rocksdb_options_set_enable_pipelined_write(h, d.EnablePipelinedWrite ? (byte)1 : (byte)0);
        Api.rocksdb_options_set_allow_concurrent_memtable_write(h, d.AllowConcurrentMemtableWrite ? (byte)1 : (byte)0);
        Api.rocksdb_options_set_two_write_queues(h, d.TwoWriteQueues ? (byte)1 : (byte)0);
        Api.rocksdb_options_set_max_subcompactions(h, d.MaxSubcompactions);
        if (d.AllowIngestBehind) Api.rocksdb_options_set_allow_ingest_behind(h, 1);
        if (d.SeparateWalDir) Api.rocksdb_options_set_wal_dir(h, DbWideSpec.WalDirFor(path));
    }

    private static N.ColumnFamilyOptions CfOptions(CfSpec s)
    {
        N.ColumnFamilyOptions o = Keep(Configure(new N.ColumnFamilyOptions(), s));
        if (s.UserTimestamps) Api.rocksdb_options_set_comparator(o.Handle, NativeU64TsComparator.Handle);
        return o;
    }

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
            ConfigureDb(o.Handle, path, spec.Db);
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
        ConfigureDb(dbOptions.Handle, path, spec.Db);
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
        if (!names.Contains("default")) names.Insert(0, "default");
        if (spec.TransactionDb)
        {
            OpenTransactionDb(path, dbOptions, names);
            return;
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

    /// <summary>rocksdb_transactiondb_open_column_families with default TransactionDBOptions (WriteCommitted), then
    /// the recovered prepared transactions (rocksdb_transactiondb_get_prepared_transactions) by name.</summary>
    private unsafe void OpenTransactionDb(string path, N.DbOptions dbOptions, List<string> names)
    {
        int n = names.Count;
        IntPtr[] namePtrs = new IntPtr[n];
        IntPtr[] optPtrs = new IntPtr[n];
        IntPtr[] handles = new IntPtr[n];
        for (int i = 0; i < n; i++)
        {
            namePtrs[i] = Marshal.StringToHGlobalAnsi(names[i]);
            optPtrs[i] = CfOptions(_spec.Cf(names[i])).Handle;
        }
        IntPtr txnDbOptions = Api.rocksdb_transactiondb_options_create();
        try
        {
            fixed (IntPtr* pn = namePtrs, po = optPtrs, ph = handles)
            {
                _txnDb = Api.rocksdb_transactiondb_open_column_families(dbOptions.Handle, txnDbOptions, path, n, (IntPtr)pn, (IntPtr)po, (IntPtr)ph);
            }
        }
        finally
        {
            foreach (IntPtr p in namePtrs) Marshal.FreeHGlobal(p);
            Api.rocksdb_transactiondb_options_destroy(txnDbOptions);
        }
        _txnBaseDb = Api.rocksdb_transactiondb_get_base_db(_txnDb);
        _txnCfs.Clear();
        for (int i = 0; i < n; i++)
        {
            _txnCfs[names[i]] = handles[i];
            _cfNames.Add(names[i]);
        }
        _txns.Clear();
        nuint count = 0;
        IntPtr arr = Api.rocksdb_transactiondb_get_prepared_transactions(_txnDb, (IntPtr)(&count));
        for (int i = 0; i < (int)count; i++)
        {
            IntPtr txn = Marshal.ReadIntPtr(arr, i * IntPtr.Size);
            _txns.Add(TxnName(txn), txn);
        }
        if (arr != IntPtr.Zero) Api.rocksdb_free(arr);
    }

    private static string TxnName(IntPtr txn)
    {
        IntPtr p = Api.rocksdb_transaction_get_name(txn, out UIntPtr len);
        string name = Marshal.PtrToStringUTF8(p, (int)len);
        Api.rocksdb_free(p);
        return name;
    }

    /// <summary>
    /// Opens with the options from the DB's latest OPTIONS file (rocksdb::LoadLatestOptions through the C API), so the
    /// native library has to parse and instantiate every option (incl. merge operators by name) the file records.
    /// </summary>
    private unsafe void OpenWithLatestOptionsFile(string path)
    {
        IntPtr env = Api.rocksdb_create_default_env();
        IntPtr cache = Api.rocksdb_cache_create_lru(8 << 20);
        IntPtr dbOpts = IntPtr.Zero, names = IntPtr.Zero, cfOpts = IntPtr.Zero;
        nuint count = 0;
        Api.rocksdb_load_latest_options(path, env, 0, cache,
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

    public void Close()
    {
        foreach (N.Snapshot s in _snapshots.Values) s.Dispose();
        _snapshots.Clear();
        _db?.Dispose();
        _db = null;
        if (TxnMode)
        {
            if (_openTxn != IntPtr.Zero) Api.rocksdb_transaction_destroy(_openTxn);
            _openTxn = IntPtr.Zero;
            // Prepared transactions stay in the WAL and are recovered as prepared on the next open.
            foreach (IntPtr t in _txns.Values) Api.rocksdb_transaction_destroy(t);
            _txns.Clear();
            foreach (IntPtr cf in _txnCfs.Values) Api.rocksdb_column_family_handle_destroy(cf);
            _txnCfs.Clear();
            Api.rocksdb_transactiondb_close_base_db(_txnBaseDb);
            Api.rocksdb_transactiondb_close(_txnDb);
            _txnDb = _txnBaseDb = IntPtr.Zero;
        }
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

    /// <summary>Raw column family handle (IntPtr.Zero for a TTL DB's default CF).</summary>
    private IntPtr CfHandle(string name)
    {
        if (TxnMode) return _txnCfs[name];
        N.ColumnFamilyHandle cf = Cf(name);
        return cf is null ? IntPtr.Zero : cf.Handle;
    }

    private static void Check(IntPtr err, string what)
    {
        if (err != IntPtr.Zero) throw new N.RocksDbException(err);
        _ = what;
    }

    // ---------------------------------------------------------------------------------------------------------
    // writes

    public void Write(IReadOnlyList<Op> ops, bool sync, bool disableWal)
    {
        // A batch writing to a default CF with timestamps must know its timestamp size (default_cf_ts_sz), otherwise
        // WriteBatch::Put(cf, ...) fails with "Default cf timestamp size mismatch" (silently dropped by the C API).
        using var batch = !_ttl && _spec.Cf("default").UserTimestamps
            ? new N.WriteBatch(Api.rocksdb_writebatch_create_with_params(0, 0, 0, 8))
            : new N.WriteBatch();
        IntPtr b = batch.Handle;
        foreach (Op op in ops)
        {
            IntPtr cf = CfHandle(op.Cf!);
            byte[] k = op.Key!;
            byte[]? v = op.Value;
            bool hasCf = cf != IntPtr.Zero;
            switch (op.Kind)
            {
                case OpKind.Put:
                    if (hasCf) Api.rocksdb_writebatch_put_cf(b, cf, k, (UIntPtr)k.Length, v!, (UIntPtr)v!.Length);
                    else Api.rocksdb_writebatch_put(b, k, (UIntPtr)k.Length, v!, (UIntPtr)v!.Length);
                    break;
                case OpKind.Merge:
                    if (hasCf) Api.rocksdb_writebatch_merge_cf(b, cf, k, (UIntPtr)k.Length, v!, (UIntPtr)v!.Length);
                    else Api.rocksdb_writebatch_merge(b, k, (UIntPtr)k.Length, v!, (UIntPtr)v!.Length);
                    break;
                case OpKind.Delete:
                    if (hasCf) Api.rocksdb_writebatch_delete_cf(b, cf, k, (UIntPtr)k.Length);
                    else Api.rocksdb_writebatch_delete(b, k, (UIntPtr)k.Length);
                    break;
                case OpKind.SingleDelete:
                    if (hasCf) Api.rocksdb_writebatch_singledelete_cf(b, cf, k, (UIntPtr)k.Length);
                    else Api.rocksdb_writebatch_singledelete(b, k, (UIntPtr)k.Length);
                    break;
                case OpKind.DeleteRange:
                    if (hasCf) Api.rocksdb_writebatch_delete_range_cf(b, cf, k, (UIntPtr)k.Length, v!, (UIntPtr)v!.Length);
                    else Api.rocksdb_writebatch_delete_range(b, k, (UIntPtr)k.Length, v!, (UIntPtr)v!.Length);
                    break;
                default: throw new InvalidOperationException("not a data op: " + op.Kind);
            }
        }
        if (UsesTimestamps(ops)) UpdateTimestamps(batch);
        var wo = new N.WriteOptions().SetSync(sync).DisableWal(disableWal ? 1 : 0);
        if (TxnMode)
        {
            Api.rocksdb_transactiondb_write(_txnDb, wo.Handle, b, out IntPtr err);
            Check(err, "rocksdb_transactiondb_write");
        }
        else Db.Write(batch, wo);
        GC.KeepAlive(wo);
    }

    public void Flush(string cf)
    {
        var fo = new N.FlushOptions().SetWaitForFlush(true);
        if (_ttl) Db.Flush(fo);
        else Api.rocksdb_flush_cf(DbHandle, fo.Handle, CfHandle(cf));
        GC.KeepAlive(fo);
    }

    public void FlushAll()
    {
        var fo = new N.FlushOptions().SetWaitForFlush(true);
        var handles = new IntPtr[_cfNames.Count];
        for (int i = 0; i < handles.Length; i++) handles[i] = CfHandle(_cfNames[i]);
        if (_ttl) Db.Flush(fo);
        else Api.rocksdb_flush_cfs(DbHandle, fo.Handle, handles, handles.Length);
        GC.KeepAlive(fo);
    }

    public void FlushWal(bool sync) => Api.rocksdb_flush_wal(DbHandle, sync ? (byte)1 : (byte)0);

    public bool CompactRange(string cf)
    {
        if (TxnMode) Api.rocksdb_compact_range_cf(DbHandle, CfHandle(cf), (byte[]?)null, UIntPtr.Zero, (byte[]?)null, UIntPtr.Zero);
        else Db.CompactRange((byte[])null!, (byte[])null!, Cf(cf));
        return true;
    }

    public void CompactRange(string cf, byte[]? begin, byte[]? end, CompactSpec spec)
    {
        IntPtr o = Api.rocksdb_compactoptions_create();
        try
        {
            Api.rocksdb_compactoptions_set_bottommost_level_compaction(o, (byte)spec.Bottommost);
            Api.rocksdb_compactoptions_set_exclusive_manual_compaction(o, spec.Exclusive ? (byte)1 : (byte)0);
            Api.rocksdb_compactoptions_set_change_level(o, spec.ChangeLevel ? (byte)1 : (byte)0);
            Api.rocksdb_compactoptions_set_target_level(o, spec.TargetLevel);
            Api.rocksdb_compactoptions_set_max_subcompactions(o, (int)spec.MaxSubcompactions);
            Api.rocksdb_compact_range_cf_opt(DbHandle, CfHandle(cf), o,
                begin, begin is null ? UIntPtr.Zero : (UIntPtr)begin.Length, end, end is null ? UIntPtr.Zero : (UIntPtr)end.Length);
        }
        finally
        {
            Api.rocksdb_compactoptions_destroy(o);
        }
    }

    public void DeleteFilesInRange(string cf, byte[] begin, byte[] end) =>
        Api.rocksdb_delete_file_in_range_cf(DbHandle, CfHandle(cf), begin, (UIntPtr)begin.Length, end, (UIntPtr)end.Length);

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
                    Api.rocksdb_sstfilewriter_delete_range(writer.Handle, e.Key!, (nuint)e.Key!.Length, e.Value!, (nuint)e.Value!.Length);
                    break;
                default: throw new InvalidOperationException("not an SST op: " + e.Kind);
            }
        }
        writer.Finish();
    }

    public void Ingest(string cf, IReadOnlyList<string> files, IngestFlags flags)
    {
        var o = Keep(new N.IngestExternalFileOptions().SetMoveFiles((flags & IngestFlags.Move) != 0));
        if ((flags & IngestFlags.IngestBehind) != 0) Api.rocksdb_ingestexternalfileoptions_set_ingest_behind(o.Handle, 1);
        if ((flags & IngestFlags.NoGlobalSeqno) != 0) Api.rocksdb_ingestexternalfileoptions_set_allow_global_seqno(o.Handle, 0);
        Db.IngestExternalFiles([.. files], o, Cf(cf));
    }

    public void Checkpoint(string dir)
    {
        using N.Checkpoint cp = Db.Checkpoint();
        cp.Save(dir);
    }

    // BackupEngine through the C API (rocksdb_backup_engine_*), default BackupEngineOptions.
    public void Backup(string backupDir, bool flushBeforeBackup)
    {
        var options = new N.DbOptions();
        IntPtr be = Api.rocksdb_backup_engine_open(options.Handle, backupDir);
        try
        {
            Api.rocksdb_backup_engine_create_new_backup_flush(be, Db.Handle, flushBeforeBackup ? (byte)1 : (byte)0);
            // An empty meta file (librocksdb under a .NET 11 apphost without NativeStdCxxPreload) is reported by the
            // test (AssertNativeMetaIsNotEmpty) with a clearer message than the verify error.
            uint id = LatestBackupId(backupDir);
            if (new FileInfo(Path.Combine(backupDir, "meta", id.ToString(CultureInfo.InvariantCulture))).Length > 0)
            {
                Api.rocksdb_backup_engine_verify_backup(be, id);
            }
        }
        finally
        {
            Api.rocksdb_backup_engine_close(be);
            GC.KeepAlive(options);
        }
    }

    /// <summary>The newest backup id of <paramref name="backupDir"/> (largest numeric name in meta/).</summary>
    public static uint LatestBackupId(string backupDir)
    {
        uint max = 0;
        foreach (string f in Directory.GetFiles(Path.Combine(backupDir, "meta")))
        {
            if (uint.TryParse(Path.GetFileName(f), NumberStyles.None, CultureInfo.InvariantCulture, out uint id) && id > max) max = id;
        }
        return max;
    }

    public void RestoreLatestBackup(string backupDir, string targetDir)
    {
        var options = new N.DbOptions();
        IntPtr be = Api.rocksdb_backup_engine_open(options.Handle, backupDir);
        IntPtr ro = Api.rocksdb_restore_options_create();
        try
        {
            Api.rocksdb_backup_engine_restore_db_from_latest_backup(be, targetDir, targetDir, ro);
        }
        finally
        {
            Api.rocksdb_restore_options_destroy(ro);
            Api.rocksdb_backup_engine_close(be);
            GC.KeepAlive(options);
        }
    }

    public void TakeSnapshot(long id) => _snapshots.Add(id, Db.CreateSnapshot());

    public void ReleaseSnapshot(long id)
    {
        _snapshots[id].Dispose();
        _snapshots.Remove(id);
    }

    // ---------------------------------------------------------------------------------------------------------
    // transactions (rocksdb_transaction_*, WriteCommitted)

    public void BeginTransaction(string name)
    {
        if (!TxnMode) throw new InvalidOperationException("not a TransactionDB (DbSpec.TransactionDb)");
        if (_openTxn != IntPtr.Zero) throw new InvalidOperationException("a transaction is already open");
        var wo = new N.WriteOptions().SetSync(true);
        IntPtr to = Api.rocksdb_transaction_options_create();
        _openTxn = Api.rocksdb_transaction_begin(_txnDb, wo.Handle, to, IntPtr.Zero);
        Api.rocksdb_transaction_options_destroy(to);
        GC.KeepAlive(wo);
        byte[] n = Encoding.UTF8.GetBytes(name);
        IntPtr np = Marshal.StringToHGlobalAnsi(name);
        try { Api.rocksdb_transaction_set_name(_openTxn, np, (UIntPtr)n.Length); }
        finally { Marshal.FreeHGlobal(np); }
        _openTxnName = name;
    }

    public void TransactionWrite(Op op)
    {
        if (_openTxn == IntPtr.Zero) throw new InvalidOperationException("no open transaction");
        IntPtr cf = CfHandle(op.Cf!);
        byte[] k = op.Key!;
        IntPtr err;
        switch (op.Kind)
        {
            case OpKind.Put: Api.rocksdb_transaction_put_cf(_openTxn, cf, k, (UIntPtr)k.Length, op.Value!, (UIntPtr)op.Value!.Length, out err); break;
            case OpKind.Merge: Api.rocksdb_transaction_merge_cf(_openTxn, cf, k, (UIntPtr)k.Length, op.Value!, (UIntPtr)op.Value!.Length, out err); break;
            case OpKind.Delete: Api.rocksdb_transaction_delete_cf(_openTxn, cf, k, (UIntPtr)k.Length, out err); break;
            default: throw new InvalidOperationException("not supported in a native transaction: " + op.Kind);
        }
        Check(err, "transaction write");
    }

    public void PrepareTransaction(string name)
    {
        if (_openTxnName != name) throw new InvalidOperationException($"open transaction is {_openTxnName}, not {name}");
        Api.rocksdb_transaction_prepare(_openTxn, out IntPtr err);
        Check(err, "rocksdb_transaction_prepare");
        _txns.Add(name, _openTxn);
        _openTxn = IntPtr.Zero;
        _openTxnName = null;
    }

    public void CommitTransaction(string name)
    {
        IntPtr t = _txns[name];
        Api.rocksdb_transaction_commit(t, out IntPtr err);
        Check(err, "rocksdb_transaction_commit");
        Api.rocksdb_transaction_destroy(t);
        _txns.Remove(name);
    }

    public void RollbackTransaction(string name)
    {
        IntPtr t = _txns[name];
        Api.rocksdb_transaction_rollback(t, out IntPtr err);
        Check(err, "rocksdb_transaction_rollback");
        Api.rocksdb_transaction_destroy(t);
        _txns.Remove(name);
    }

    public unsafe IReadOnlyList<string> PreparedTransactionNames()
    {
        var names = new List<string>();
        nuint count = 0;
        IntPtr arr = Api.rocksdb_transactiondb_get_prepared_transactions(_txnDb, (IntPtr)(&count));
        for (int i = 0; i < (int)count; i++) names.Add(TxnName(Marshal.ReadIntPtr(arr, i * IntPtr.Size)));
        // The returned handles wrap the same transactions as _txns (C API wrappers; not destroyed here).
        if (arr != IntPtr.Zero) Api.rocksdb_free(arr);
        return names;
    }

    // ---------------------------------------------------------------------------------------------------------
    // reads

    private N.ReadOptions ReadOptions(long? snapshot, string cf, bool scan)
    {
        var ro = new N.ReadOptions();
        if (snapshot is long id) ro.SetSnapshot(_snapshots[id]);
        CfSpec s = _spec.Cf(cf);
        if (s.UserTimestamps) Api.rocksdb_readoptions_set_timestamp(ro.Handle, s_maxTs, 8);
        // Full scans across prefixes (prefix extractors, hash index, hash skip list memtable) need total order.
        if (scan && s.HasPrefix) Api.rocksdb_readoptions_set_total_order_seek(ro.Handle, 1);
        return ro;
    }

    // ---- user-defined timestamps -------------------------------------------------------------------------------

    private static readonly IntPtr s_maxTs = AllocMaxTs();

    private static IntPtr AllocMaxTs()
    {
        IntPtr p = Marshal.AllocHGlobal(8);
        Marshal.Copy(UdtClock.MaxTimestamp, 0, p, 8);
        return p;
    }

    private bool UsesTimestamps(IReadOnlyList<Op> ops)
    {
        foreach (Op op in ops)
        {
            if (_spec.Cf(op.Cf!).UserTimestamps)
            {
                if (op.Kind == OpKind.DeleteRange) throw new InvalidOperationException("DeleteRange is not used with user-defined timestamps");
                return true;
            }
        }
        return false;
    }

    /// <summary>
    /// WriteBatch::Put/Delete/... with a column family handle append a placeholder timestamp for CFs with timestamps;
    /// rocksdb_writebatch_update_timestamps then assigns the batch's timestamp to all of them.
    /// </summary>
    private unsafe void UpdateTimestamps(N.WriteBatch batch)
    {
        var sizes = new Dictionary<uint, nuint>();
        foreach (string name in _cfNames)
        {
            sizes[Api.rocksdb_column_family_handle_get_id(CfHandle(name))] = _spec.Cf(name).UserTimestamps ? 8u : 0u;
        }
        N.GetTsSizeDelegate getTsSize = (_, cf) => sizes.TryGetValue(cf, out nuint s) ? s : 0;
        byte[] ts = UdtClock.Next();
        fixed (byte* p = ts)
        {
            Api.rocksdb_writebatch_update_timestamps(batch.Handle, (IntPtr)p, 8, IntPtr.Zero, getTsSize, out IntPtr err);
            if (err != IntPtr.Zero) throw new InvalidOperationException("rocksdb_writebatch_update_timestamps: " + Marshal.PtrToStringAnsi(err));
        }
        GC.KeepAlive(getTsSize);
    }

    public IEnumerable<KeyValuePair<byte[], byte[]>> Scan(string cf, long? snapshot)
    {
        N.ReadOptions ro = ReadOptions(snapshot, cf, scan: true);
        if (TxnMode)
        {
            IntPtr it = Api.rocksdb_transactiondb_create_iterator_cf(_txnDb, ro.Handle, CfHandle(cf));
            try
            {
                for (Api.rocksdb_iter_seek_to_first(it); Api.rocksdb_iter_valid(it) != 0; Api.rocksdb_iter_next(it))
                    yield return new(Api.rocksdb_iter_key(it), Api.rocksdb_iter_value(it));
            }
            finally
            {
                Api.rocksdb_iter_destroy(it);
                GC.KeepAlive(ro);
            }
            yield break;
        }
        using N.Iterator iter = Db.NewIterator(Cf(cf), ro);
        for (iter.SeekToFirst(); iter.Valid(); iter.Next()) yield return new(iter.Key(), iter.Value());
        GC.KeepAlive(ro);
    }

    public byte[]? Get(string cf, byte[] key, long? snapshot)
    {
        N.ReadOptions ro = ReadOptions(snapshot, cf, scan: false);
        byte[]? v;
        if (TxnMode)
        {
            IntPtr p = Api.rocksdb_transactiondb_get_cf(_txnDb, ro.Handle, CfHandle(cf), key, (UIntPtr)key.Length, out UIntPtr len);
            if (p == IntPtr.Zero) v = null;
            else
            {
                v = new byte[(int)len];
                Marshal.Copy(p, v, 0, v.Length);
                Api.rocksdb_free(p);
            }
        }
        else v = Db.Get(key, Cf(cf), ro);
        GC.KeepAlive(ro);
        return v;
    }

    public int[] FilesPerLevel(string cf)
    {
        int[] r = new int[7];
        for (int level = 0; level < r.Length; level++)
        {
            string name = "rocksdb.num-files-at-level" + level.ToString(CultureInfo.InvariantCulture);
            string? p = _ttl ? Db.GetProperty(name) : Db.GetProperty(name, Cf(cf));
            r[level] = p is null ? 0 : int.Parse(p, CultureInfo.InvariantCulture);
        }
        return r;
    }

    public override string ToString() => "native" + (_ttl ? "(ttl)" : "") + (TxnMode ? "(txn)" : "");
}
