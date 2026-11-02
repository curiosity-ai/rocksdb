using System.Buffers;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;

namespace RocksDb.Managed;

/// <summary>
/// An open RocksDB database (rocksdb::DB). Port of the DB API (include/rocksdb/db.h) over <see cref="DbImpl"/>,
/// reshaped per the managed API design: the column family is the receiver (methods on this class act on the default
/// column family, <see cref="ColumnFamily"/> objects carry the same methods for other column families), options are
/// explicit overloads, reads are allocation-free (<see cref="TryGet(ReadOnlySpan{byte}, IBufferWriter{byte})"/>,
/// span callbacks, <see cref="PooledValue"/>), and I/O-bound operations have <see cref="ValueTask"/> variants.
/// <para>
/// The on-disk format is compatible with native RocksDB: a directory written by either can be opened by the other.
/// Named <c>RocksDatabase</c> (not <c>RocksDb</c>) because a class named like the root namespace's first segment
/// breaks namespace-qualified references (<c>RocksDb.Managed.X</c>) inside the <c>RocksDb.Managed.*</c> namespaces.
/// </para>
/// Thread-safe. Dispose (preferably <see cref="DisposeAsync"/>) to close it and release the LOCK file.
/// </summary>
public sealed class RocksDatabase : IDisposable, IAsyncDisposable
{
    private readonly DbImpl _db;
    private readonly ColumnFamily _default;
    private readonly Dictionary<uint, ColumnFamily> _handles = new();
    private readonly Lock _handlesLock = new();

    private RocksDatabase(DbImpl db)
    {
        _db = db;
        _default = Handle(db.GetColumnFamilyData(ColumnFamilyDescriptor.DefaultColumnFamilyName));
    }

    // =====================================================================================================
    // Open
    // =====================================================================================================

    /// <summary>Opens the DB with only the default column family (DB::Open). Fails if other column families exist.</summary>
    public static RocksDatabase Open(string path, DbOptions options) => Open(path, options, [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions())]);

    /// <summary>Opens the DB with the default column family configured by <paramref name="defaultColumnFamilyOptions"/>.</summary>
    public static RocksDatabase Open(string path, DbOptions options, ColumnFamilyOptions defaultColumnFamilyOptions) =>
        Open(path, options, [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, defaultColumnFamilyOptions)]);

    /// <summary>
    /// Opens the DB with the given column families (DB::Open with descriptors). Every existing column family must be
    /// listed; listed ones that don't exist are created when <see cref="DbOptions.CreateMissingColumnFamilies"/> is set.
    /// The default column family is added with default options when not listed.
    /// </summary>
    public static RocksDatabase Open(string path, DbOptions options, IReadOnlyList<ColumnFamilyDescriptor> columnFamilies)
    {
        ValueTask<DbImpl> t = DbImpl.OpenAsync(path, options, columnFamilies, synchronous: true, CancellationToken.None);
        return new RocksDatabase(t.IsCompleted ? t.Result : t.AsTask().GetAwaiter().GetResult());
    }

    public static ValueTask<RocksDatabase> OpenAsync(string path, DbOptions options, CancellationToken cancellationToken = default) =>
        OpenAsync(path, options, [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions())], cancellationToken);

    public static ValueTask<RocksDatabase> OpenAsync(string path, DbOptions options, ColumnFamilyOptions defaultColumnFamilyOptions,
        CancellationToken cancellationToken = default) =>
        OpenAsync(path, options, [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, defaultColumnFamilyOptions)], cancellationToken);

    public static async ValueTask<RocksDatabase> OpenAsync(string path, DbOptions options, IReadOnlyList<ColumnFamilyDescriptor> columnFamilies,
        CancellationToken cancellationToken = default)
    {
        DbImpl db = await DbImpl.OpenAsync(path, options, columnFamilies, synchronous: false, cancellationToken).ConfigureAwait(false);
        return new RocksDatabase(db);
    }

    /// <summary>Lists the column families of the DB at <paramref name="path"/> (DB::ListColumnFamilies), in id order.</summary>
    public static List<string> ListColumnFamilies(string path) => ListColumnFamilies(path, LocalFileSystem.Instance);

    public static List<string> ListColumnFamilies(string path, IFileSystem fileSystem)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        string dbname = path.Length > 1 ? path.TrimEnd('/') : path;
        RecoveredVersionState state = ManifestReader.RecoverFromDirectory(fileSystem, dbname, null, out _);
        var names = new List<string>(state.ColumnFamilies.Count);
        foreach (ColumnFamilyVersion cf in state.ColumnFamilies.Values) names.Add(cf.Name);
        return names;
    }

    // =====================================================================================================
    // Column families
    // =====================================================================================================

    private ColumnFamily Handle(ColumnFamilyData cfd)
    {
        lock (_handlesLock)
        {
            if (_handles.TryGetValue(cfd.Id, out ColumnFamily? h) && h.Data == cfd) return h;
            h = new ColumnFamily(_db, cfd);
            _handles[cfd.Id] = h;
            return h;
        }
    }

    /// <summary>The default column family.</summary>
    public ColumnFamily DefaultColumnFamily => _default;

    /// <summary>The column family named <paramref name="name"/>; throws InvalidArgument if it doesn't exist.</summary>
    public ColumnFamily GetColumnFamily(string name) => Handle(_db.GetColumnFamilyData(name));

    public bool TryGetColumnFamily(string name, out ColumnFamily? columnFamily)
    {
        ColumnFamilyData? cfd = _db.TryGetColumnFamilyData(name);
        columnFamily = cfd is null ? null : Handle(cfd);
        return cfd is not null;
    }

    /// <summary>All live column families, in id order.</summary>
    public IReadOnlyList<ColumnFamily> ColumnFamilies
    {
        get
        {
            List<ColumnFamilyData> cfds = _db.GetColumnFamilies();
            var list = new List<ColumnFamily>(cfds.Count);
            foreach (ColumnFamilyData c in cfds) list.Add(Handle(c));
            return list;
        }
    }

    public ColumnFamily CreateColumnFamily(string name, ColumnFamilyOptions options)
    {
        ValueTask<ColumnFamilyData> t = _db.CreateColumnFamilyAsync(name, options, synchronous: true, CancellationToken.None);
        return Handle(t.IsCompleted ? t.Result : t.AsTask().GetAwaiter().GetResult());
    }

    public async ValueTask<ColumnFamily> CreateColumnFamilyAsync(string name, ColumnFamilyOptions options, CancellationToken cancellationToken = default) =>
        Handle(await _db.CreateColumnFamilyAsync(name, options, synchronous: false, cancellationToken).ConfigureAwait(false));

    public void DropColumnFamily(ColumnFamily columnFamily)
    {
        ArgumentNullException.ThrowIfNull(columnFamily);
        _db.DropColumnFamilyAsync(columnFamily.Data, synchronous: true, CancellationToken.None).GetAwaiter().GetResult();
    }

    public ValueTask DropColumnFamilyAsync(ColumnFamily columnFamily, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(columnFamily);
        return _db.DropColumnFamilyAsync(columnFamily.Data, synchronous: false, cancellationToken);
    }

    // =====================================================================================================
    // Batches, snapshots, flush, properties
    // =====================================================================================================

    /// <summary>A pooled, empty write batch; disposing it returns it to the pool.</summary>
    public WriteBatch CreateWriteBatch() => WriteBatch.Rent();

    /// <summary>Atomically applies <paramref name="batch"/> (DB::Write). Records name column families by <see cref="ColumnFamily.Id"/>.</summary>
    public void Write(WriteBatch batch) => Write(null, batch);

    public void Write(WriteOptions? options, WriteBatch batch) =>
        _db.WriteAsync(options, batch, validateColumnFamilies: true, synchronous: true, CancellationToken.None).GetAwaiter().GetResult();

    public ValueTask WriteAsync(WriteBatch batch, CancellationToken cancellationToken = default) => WriteAsync(null, batch, cancellationToken);

    public ValueTask WriteAsync(WriteOptions? options, WriteBatch batch, CancellationToken cancellationToken = default) =>
        _db.WriteAsync(options, batch, validateColumnFamilies: true, synchronous: false, cancellationToken);

    /// <summary>Takes a snapshot of the current state (DB::GetSnapshot). Dispose it to release.</summary>
    public Snapshot GetSnapshot() => _db.GetSnapshot();

    /// <summary>The sequence number of the most recent write (DB::GetLatestSequenceNumber).</summary>
    public ulong GetLatestSequenceNumber() => _db.Versions.LastSequence;

    /// <summary>Flushes the default column family (DB::Flush).</summary>
    public void Flush() => _default.Flush();
    public void Flush(FlushOptions options) => _default.Flush(options);
    public ValueTask FlushAsync(CancellationToken cancellationToken = default) => _default.FlushAsync(cancellationToken);
    public ValueTask FlushAsync(FlushOptions options, CancellationToken cancellationToken = default) => _default.FlushAsync(options, cancellationToken);

    /// <summary>
    /// A DB property of the default column family, or null if unknown. Supported: rocksdb.num-entries-active-mem-table,
    /// rocksdb.num-entries-imm-mem-tables, rocksdb.num-deletes-active-mem-table, rocksdb.num-immutable-mem-table,
    /// rocksdb.cur-size-active-mem-table, rocksdb.estimate-num-keys, rocksdb.num-files-at-level&lt;N&gt;,
    /// rocksdb.total-sst-files-size, rocksdb.live-sst-files-size, rocksdb.levelstats (summary), rocksdb.num-snapshots,
    /// rocksdb.is-write-stopped.
    /// </summary>
    public string? GetProperty(string property) => _default.GetProperty(property);

    /// <summary>Metadata of all live table files (DB::GetLiveFilesMetaData).</summary>
    public List<LiveFileMetadata> GetLiveFilesMetadata()
    {
        _db.ThrowIfDisposed();
        return _db.GetLiveFilesMetadata();
    }

    /// <summary>The DB directory.</summary>
    public string Path => _db.DbName;

    /// <summary>The DB identity (IDENTITY file / MANIFEST db id).</summary>
    public string DbId => _db.Identity.DbId;

    /// <summary>The id of this DB session (written into table properties).</summary>
    public string DbSessionId => _db.Identity.SessionId;

    /// <summary>Iterator over the default column family (DB::NewIterator). See <see cref="ColumnFamily.NewIterator(ReadOptions?)"/>.</summary>
    public Iterator NewIterator() => _default.NewIterator(null);
    public Iterator NewIterator(ReadOptions? options) => _default.NewIterator(options);
    public Iterator NewIterator(ColumnFamily columnFamily, ReadOptions? options)
    {
        ArgumentNullException.ThrowIfNull(columnFamily);
        return columnFamily.NewIterator(options);
    }
    public ValueTask<Iterator> NewIteratorAsync(ReadOptions? options, CancellationToken cancellationToken = default) =>
        _default.NewIteratorAsync(options, cancellationToken);

    // =====================================================================================================
    // Default column family data operations (see ColumnFamily for documentation)
    // =====================================================================================================

    public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => _default.Put(key, value);
    public void Put(WriteOptions options, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => _default.Put(options, key, value);
    public ValueTask PutAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default) =>
        _default.PutAsync(key, value, cancellationToken);
    public ValueTask PutAsync(WriteOptions options, ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default) =>
        _default.PutAsync(options, key, value, cancellationToken);

    public void Delete(ReadOnlySpan<byte> key) => _default.Delete(key);
    public void Delete(WriteOptions options, ReadOnlySpan<byte> key) => _default.Delete(options, key);
    public ValueTask DeleteAsync(ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default) => _default.DeleteAsync(key, cancellationToken);
    public ValueTask DeleteAsync(WriteOptions options, ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default) =>
        _default.DeleteAsync(options, key, cancellationToken);

    public void SingleDelete(ReadOnlySpan<byte> key) => _default.SingleDelete(key);
    public void SingleDelete(WriteOptions options, ReadOnlySpan<byte> key) => _default.SingleDelete(options, key);
    public ValueTask SingleDeleteAsync(ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default) => _default.SingleDeleteAsync(key, cancellationToken);
    public ValueTask SingleDeleteAsync(WriteOptions options, ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default) =>
        _default.SingleDeleteAsync(options, key, cancellationToken);

    public void DeleteRange(ReadOnlySpan<byte> beginKey, ReadOnlySpan<byte> endKey) => _default.DeleteRange(beginKey, endKey);
    public void DeleteRange(WriteOptions options, ReadOnlySpan<byte> beginKey, ReadOnlySpan<byte> endKey) => _default.DeleteRange(options, beginKey, endKey);
    public ValueTask DeleteRangeAsync(ReadOnlyMemory<byte> beginKey, ReadOnlyMemory<byte> endKey, CancellationToken cancellationToken = default) =>
        _default.DeleteRangeAsync(beginKey, endKey, cancellationToken);
    public ValueTask DeleteRangeAsync(WriteOptions options, ReadOnlyMemory<byte> beginKey, ReadOnlyMemory<byte> endKey, CancellationToken cancellationToken = default) =>
        _default.DeleteRangeAsync(options, beginKey, endKey, cancellationToken);

    public void Merge(ReadOnlySpan<byte> key, ReadOnlySpan<byte> operand) => _default.Merge(key, operand);
    public void Merge(WriteOptions options, ReadOnlySpan<byte> key, ReadOnlySpan<byte> operand) => _default.Merge(options, key, operand);
    public ValueTask MergeAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> operand, CancellationToken cancellationToken = default) =>
        _default.MergeAsync(key, operand, cancellationToken);
    public ValueTask MergeAsync(WriteOptions options, ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> operand, CancellationToken cancellationToken = default) =>
        _default.MergeAsync(options, key, operand, cancellationToken);

    public bool TryGet(ReadOnlySpan<byte> key, IBufferWriter<byte> value) => _default.TryGet(key, value);
    public bool TryGet(ReadOptions? options, ReadOnlySpan<byte> key, IBufferWriter<byte> value) => _default.TryGet(options, key, value);
    public bool TryGet(ReadOnlySpan<byte> key, Span<byte> destination, out int valueLength) => _default.TryGet(key, destination, out valueLength);
    public bool TryGet(ReadOptions? options, ReadOnlySpan<byte> key, Span<byte> destination, out int valueLength) =>
        _default.TryGet(options, key, destination, out valueLength);
    public bool TryGet<TState, TResult>(ReadOnlySpan<byte> key, TState state, ReadOnlySpanFunc<TState, TResult> reader, out TResult result) =>
        _default.TryGet(key, state, reader, out result);
    public bool TryGet<TState, TResult>(ReadOptions? options, ReadOnlySpan<byte> key, TState state, ReadOnlySpanFunc<TState, TResult> reader, out TResult result) =>
        _default.TryGet(options, key, state, reader, out result);
    public TResult? Get<TState, TResult>(ReadOnlySpan<byte> key, TState state, ReadOnlySpanFunc<TState, TResult> reader) => _default.Get(key, state, reader);
    public PooledValue? GetPooled(ReadOnlySpan<byte> key) => _default.GetPooled(key);
    public PooledValue? GetPooled(ReadOptions? options, ReadOnlySpan<byte> key) => _default.GetPooled(options, key);
    public byte[]? Get(ReadOnlySpan<byte> key) => _default.Get(key);
    public byte[]? Get(ReadOptions? options, ReadOnlySpan<byte> key) => _default.Get(options, key);
    public bool ContainsKey(ReadOnlySpan<byte> key) => _default.ContainsKey(key);
    public byte[]?[] MultiGet(IReadOnlyList<byte[]> keys) => _default.MultiGet(keys);
    public byte[]?[] MultiGet(ReadOptions? options, IReadOnlyList<byte[]> keys) => _default.MultiGet(options, keys);

    public ValueTask<bool> TryGetAsync(ReadOnlyMemory<byte> key, IBufferWriter<byte> value, CancellationToken cancellationToken = default) =>
        _default.TryGetAsync(key, value, cancellationToken);
    public ValueTask<bool> TryGetAsync(ReadOptions? options, ReadOnlyMemory<byte> key, IBufferWriter<byte> value, CancellationToken cancellationToken = default) =>
        _default.TryGetAsync(options, key, value, cancellationToken);
    public ValueTask<PooledValue?> GetPooledAsync(ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default) =>
        _default.GetPooledAsync(key, cancellationToken);
    public ValueTask<PooledValue?> GetPooledAsync(ReadOptions? options, ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default) =>
        _default.GetPooledAsync(options, key, cancellationToken);
    public ValueTask<byte[]?> GetAsync(ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default) => _default.GetAsync(key, cancellationToken);
    public ValueTask<byte[]?> GetAsync(ReadOptions? options, ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default) =>
        _default.GetAsync(options, key, cancellationToken);

    // =====================================================================================================
    // Close
    // =====================================================================================================

    /// <summary>Closes the DB (DB::Close): waits for writes and background flushes, closes the WAL/MANIFEST, releases LOCK.</summary>
    public void Dispose()
    {
        ValueTask t = _db.CloseAsync(synchronous: true);
        if (!t.IsCompleted) t.AsTask().GetAwaiter().GetResult();
        else t.GetAwaiter().GetResult();
    }

    public ValueTask DisposeAsync() => _db.CloseAsync(synchronous: false);
}
