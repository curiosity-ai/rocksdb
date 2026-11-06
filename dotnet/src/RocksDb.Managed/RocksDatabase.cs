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
public sealed partial class RocksDatabase : IDisposable, IAsyncDisposable
{
    private readonly DbImpl _db;
    private readonly ColumnFamily _default;
    private readonly Dictionary<uint, ColumnFamily> _handles = new();
    private readonly Lock _handlesLock = new();

    private RocksDatabase(DbImpl db)
    {
        _db = db;
        _default = Handle(db.GetColumnFamilyData(ColumnFamilyDescriptor.DefaultColumnFamilyName));
        db.Owner = this;
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

    /// <summary>Async <see cref="Open(string, DbOptions)"/>: opens the DB with only the default column family.</summary>
    public static ValueTask<RocksDatabase> OpenAsync(string path, DbOptions options, CancellationToken cancellationToken = default) =>
        OpenAsync(path, options, [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions())], cancellationToken);

    /// <summary>Async <see cref="Open(string, DbOptions, ColumnFamilyOptions)"/>.</summary>
    public static ValueTask<RocksDatabase> OpenAsync(string path, DbOptions options, ColumnFamilyOptions defaultColumnFamilyOptions,
        CancellationToken cancellationToken = default) =>
        OpenAsync(path, options, [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, defaultColumnFamilyOptions)], cancellationToken);

    /// <summary>Async <see cref="Open(string, DbOptions, IReadOnlyList{ColumnFamilyDescriptor})"/>: recovery reads and writes asynchronously.</summary>
    public static async ValueTask<RocksDatabase> OpenAsync(string path, DbOptions options, IReadOnlyList<ColumnFamilyDescriptor> columnFamilies,
        CancellationToken cancellationToken = default)
    {
        DbImpl db = await DbImpl.OpenAsync(path, options, columnFamilies, synchronous: false, cancellationToken).ConfigureAwait(false);
        return new RocksDatabase(db);
    }

    /// <summary>Lists the column families of the DB at <paramref name="path"/> (DB::ListColumnFamilies), in id order.</summary>
    public static List<string> ListColumnFamilies(string path) => ListColumnFamilies(path, LocalFileSystem.Instance);

    /// <summary>Lists the column families of the DB at <paramref name="path"/> on <paramref name="fileSystem"/>, in id order.</summary>
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

    /// <summary>Gets the column family named <paramref name="name"/>; false if it doesn't exist.</summary>
    public bool TryGetColumnFamily(string name, out ColumnFamily? columnFamily)
    {
        ColumnFamilyData? cfd = _db.TryGetColumnFamilyData(name);
        columnFamily = cfd is null ? null : Handle(cfd);
        return cfd is not null;
    }

    /// <summary>
    /// All live column families, in id order: a read-only snapshot taken at the time of the call (later creates and drops
    /// are not reflected in it). The same snapshot object is returned until a column family is created or dropped, so
    /// reading this property in a loop does not allocate.
    /// </summary>
    public IReadOnlyList<ColumnFamily> ColumnFamilies
    {
        get
        {
            ColumnFamilyData[] cfds = _db.ColumnFamiliesSnapshot();
            HandleSnapshot? cached = Volatile.Read(ref _handleSnapshot);
            if (cached is not null && ReferenceEquals(cached.Source, cfds)) return cached.Handles;
            var handles = new ColumnFamily[cfds.Length];
            for (int i = 0; i < cfds.Length; i++) handles[i] = Handle(cfds[i]);
            cached = new HandleSnapshot(cfds, new System.Collections.ObjectModel.ReadOnlyCollection<ColumnFamily>(handles));
            Volatile.Write(ref _handleSnapshot, cached);
            return cached.Handles;
        }
    }

    // The handles of one DbImpl.ColumnFamiliesSnapshot array (published atomically; the handle of a live column family
    // never changes, see Handle).
    private sealed record HandleSnapshot(ColumnFamilyData[] Source, IReadOnlyList<ColumnFamily> Handles);
    private HandleSnapshot? _handleSnapshot;

    /// <summary>
    /// Creates a column family (DB::CreateColumnFamily); the change is recorded in the MANIFEST and the OPTIONS file.
    /// Throws InvalidArgument if a column family with that name exists.
    /// </summary>
    public ColumnFamily CreateColumnFamily(string name, ColumnFamilyOptions options)
    {
        ValueTask<ColumnFamilyData> t = _db.CreateColumnFamilyAsync(name, options, synchronous: true, CancellationToken.None);
        return Handle(t.IsCompleted ? t.Result : t.AsTask().GetAwaiter().GetResult());
    }

    /// <summary>Async <see cref="CreateColumnFamily"/>.</summary>
    public async ValueTask<ColumnFamily> CreateColumnFamilyAsync(string name, ColumnFamilyOptions options, CancellationToken cancellationToken = default) =>
        Handle(await _db.CreateColumnFamilyAsync(name, options, synchronous: false, cancellationToken).ConfigureAwait(false));

    /// <summary>
    /// Drops a column family (DB::DropColumnFamily); the drop is recorded in the MANIFEST. Throws InvalidArgument for the
    /// default column family or a column family that was already dropped.
    /// </summary>
    public void DropColumnFamily(ColumnFamily columnFamily)
    {
        ArgumentNullException.ThrowIfNull(columnFamily);
        _db.DropColumnFamilyAsync(columnFamily.Data, synchronous: true, CancellationToken.None).GetAwaiter().GetResult();
        ReleaseHandle(columnFamily);
    }

    /// <summary>Forgets a dropped column family's handle (OnColumnFamilyHandleDeletionStarted).</summary>
    private void ReleaseHandle(ColumnFamily columnFamily)
    {
        lock (_handlesLock)
        {
            if (!_handles.TryGetValue(columnFamily.Data.Id, out ColumnFamily? h) || h != columnFamily) return;
            _handles.Remove(columnFamily.Data.Id);
        }
        _db.NotifyOnColumnFamilyHandleDeletionStarted(columnFamily);
    }

    /// <summary>Close: every remaining handle is released (OnColumnFamilyHandleDeletionStarted).</summary>
    private void ReleaseAllHandles()
    {
        if (!_db.HasListeners || _db.IsDisposed) return;
        List<ColumnFamily> handles;
        lock (_handlesLock) handles = new List<ColumnFamily>(_handles.Values);
        foreach (ColumnFamily h in handles) _db.NotifyOnColumnFamilyHandleDeletionStarted(h);
    }

    /// <summary>Async <see cref="DropColumnFamily"/>.</summary>
    public async ValueTask DropColumnFamilyAsync(ColumnFamily columnFamily, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(columnFamily);
        await _db.DropColumnFamilyAsync(columnFamily.Data, synchronous: false, cancellationToken).ConfigureAwait(false);
        ReleaseHandle(columnFamily);
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

    /// <summary>
    /// Like <see cref="Write(WriteOptions?, WriteBatch)"/>, notifying <paramref name="callback"/> when the write is
    /// enqueued (<see cref="UserWriteCallback.OnWriteEnqueued"/>) and when its WAL record has been written
    /// (<see cref="UserWriteCallback.OnWalWriteFinish"/>, not called with <see cref="WriteOptions.DisableWal"/>), e.g. to
    /// order concurrent writers. Port of DB::WriteWithCallback(options, updates, user_write_cb). Not supported
    /// (NotSupported) on WritePrepared / WriteUnprepared transaction DBs.
    /// </summary>
    public void WriteWithCallback(WriteOptions? options, WriteBatch batch, UserWriteCallback callback) =>
        _db.WriteWithCallbackAsync(options, batch, callback, synchronous: true, CancellationToken.None).GetAwaiter().GetResult();

    /// <summary>Asynchronous <see cref="WriteWithCallback(WriteOptions?, WriteBatch, UserWriteCallback)"/>.</summary>
    public ValueTask WriteWithCallbackAsync(WriteOptions? options, WriteBatch batch, UserWriteCallback callback, CancellationToken cancellationToken = default) =>
        _db.WriteWithCallbackAsync(options, batch, callback, synchronous: false, cancellationToken);

    /// <summary>Takes a snapshot of the current state (DB::GetSnapshot). Dispose it to release.</summary>
    public Snapshot GetSnapshot() => _db.GetSnapshot();

    /// <summary>The sequence number of the most recent write (DB::GetLatestSequenceNumber).</summary>
    public ulong GetLatestSequenceNumber() => _db.Versions.LastSequence;

    /// <summary>Flushes the default column family (DB::Flush).</summary>
    public void Flush() => _default.Flush();
    public void Flush(FlushOptions options) => _default.Flush(options);
    public ValueTask FlushAsync(CancellationToken cancellationToken = default) => _default.FlushAsync(cancellationToken);
    public ValueTask FlushAsync(FlushOptions options, CancellationToken cancellationToken = default) => _default.FlushAsync(options, cancellationToken);

    /// <summary>Compacts all data of the default column family (see <see cref="ColumnFamily.CompactRange()"/>).</summary>
    public void CompactRange() => _default.CompactRange();
    public void CompactRange(byte[]? begin, byte[]? end) => _default.CompactRange(begin, end);
    public void CompactRange(CompactRangeOptions options, byte[]? begin, byte[]? end) => _default.CompactRange(options, begin, end);
    public ValueTask CompactRangeAsync(CancellationToken cancellationToken = default) => _default.CompactRangeAsync(cancellationToken);
    public ValueTask CompactRangeAsync(byte[]? begin, byte[]? end, CancellationToken cancellationToken = default) =>
        _default.CompactRangeAsync(begin, end, cancellationToken);
    public ValueTask CompactRangeAsync(CompactRangeOptions options, byte[]? begin, byte[]? end, CancellationToken cancellationToken = default) =>
        _default.CompactRangeAsync(options, begin, end, cancellationToken);

    /// <summary>Compacts table files of the default column family (see <see cref="ColumnFamily.CompactFiles(IEnumerable{string}, int)"/>).</summary>
    public IReadOnlyList<string> CompactFiles(IEnumerable<string> inputFileNames, int outputLevel) => _default.CompactFiles(inputFileNames, outputLevel);
    public IReadOnlyList<string> CompactFiles(CompactFilesOptions options, IEnumerable<string> inputFileNames, int outputLevel) =>
        _default.CompactFiles(options, inputFileNames, outputLevel);
    public IReadOnlyList<string> CompactFiles(CompactFilesOptions options, IEnumerable<ulong> inputFileNumbers, int outputLevel) =>
        _default.CompactFiles(options, inputFileNumbers, outputLevel);
    public ValueTask<IReadOnlyList<string>> CompactFilesAsync(IEnumerable<string> inputFileNames, int outputLevel, CancellationToken cancellationToken = default) =>
        _default.CompactFilesAsync(inputFileNames, outputLevel, cancellationToken);
    public ValueTask<IReadOnlyList<string>> CompactFilesAsync(CompactFilesOptions options, IEnumerable<string> inputFileNames, int outputLevel,
        CancellationToken cancellationToken = default) => _default.CompactFilesAsync(options, inputFileNames, outputLevel, cancellationToken);
    public ValueTask<IReadOnlyList<string>> CompactFilesAsync(CompactFilesOptions options, IEnumerable<ulong> inputFileNumbers, int outputLevel,
        CancellationToken cancellationToken = default) => _default.CompactFilesAsync(options, inputFileNumbers, outputLevel, cancellationToken);

    /// <summary>
    /// Waits until all scheduled flushes and compactions (of every column family) have finished and no automatic
    /// compaction is pending (DB::WaitForCompact with default options).
    /// </summary>
    public void WaitForCompact() => _db.WaitForCompactAsync(synchronous: true, CancellationToken.None).GetAwaiter().GetResult();

    /// <summary>Async <see cref="WaitForCompact"/>.</summary>
    public ValueTask WaitForCompactAsync(CancellationToken cancellationToken = default) => _db.WaitForCompactAsync(synchronous: false, cancellationToken);

    /// <summary>
    /// A DB property of the default column family, or null if unknown. The supported names are the
    /// <see cref="DbProperties"/> constants (e.g. <see cref="DbProperties.EstimateNumKeys"/>,
    /// <see cref="DbProperties.NumFilesAtLevel(int)"/>).
    /// </summary>
    public string? GetProperty(string property) => _default.GetProperty(property);

    /// <summary>A numeric DB property of the default column family (DB::GetIntProperty); false if unknown or not numeric.</summary>
    public bool TryGetIntProperty(string property, out ulong value) => _default.TryGetIntProperty(property, out value);

    /// <summary>
    /// Metadata of all live table files (DB::GetLiveFilesMetaData), in column family id, level and in-level order.
    /// Allocates a new list and one record (with copied keys and file name) per file; see
    /// <see cref="GetLiveFilesMetadata(List{LiveFileMetadata})"/> and <see cref="VisitLiveFiles{TState}"/>.
    /// </summary>
    public List<LiveFileMetadata> GetLiveFilesMetadata()
    {
        _db.ThrowIfDisposed();
        return _db.GetLiveFilesMetadata();
    }

    /// <summary>
    /// <see cref="GetLiveFilesMetadata()"/> into a caller-owned list (DB::GetLiveFilesMetaData(std::vector*)): clears
    /// <paramref name="result"/> and refills it, reusing its capacity. Still allocates one record per file.
    /// </summary>
    public void GetLiveFilesMetadata(List<LiveFileMetadata> result)
    {
        ArgumentNullException.ThrowIfNull(result);
        _db.ThrowIfDisposed();
        _db.GetLiveFilesMetadata(result);
    }

    /// <summary>
    /// Calls <paramref name="visitor"/> once per live table file, in the order of <see cref="GetLiveFilesMetadata()"/>,
    /// with the file's metadata read in place (<see cref="LiveFileInfo"/>: keys as spans, no file name string). Does not
    /// allocate when <paramref name="visitor"/> is a static lambda (state goes through <paramref name="state"/>).
    /// <para>
    /// Each column family's files are read from one consistent version, which stays referenced (its files cannot be
    /// deleted) while the visitor runs for that column family, so keep the visitor short. The visitor may call other
    /// read APIs of the database but must not dispose it. An exception thrown by the visitor stops the visit and is
    /// propagated.
    /// </para>
    /// </summary>
    /// <example>
    /// <code>
    /// var total = new StrongBox&lt;ulong&gt;();   // reuse it across calls to stay allocation-free
    /// db.VisitLiveFiles(total, static (in LiveFileInfo f, StrongBox&lt;ulong&gt; sum) =&gt; sum.Value += f.Size);
    /// </code>
    /// </example>
    public void VisitLiveFiles<TState>(TState state, LiveFileVisitor<TState> visitor)
    {
        ArgumentNullException.ThrowIfNull(visitor);
        _db.ThrowIfDisposed();
        _db.VisitLiveFiles(state, visitor, sizeHint: null);
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
    /// <summary>Creates an iterator over the default column family; table files are opened asynchronously.</summary>
    public ValueTask<Iterator> NewIteratorAsync(ReadOptions? options, CancellationToken cancellationToken = default) =>
        _default.NewIteratorAsync(options, cancellationToken);

    // =====================================================================================================
    // Default column family data operations (see ColumnFamily for documentation)
    // =====================================================================================================

    /// <summary>Sets <paramref name="key"/> to <paramref name="value"/> in the default column family (overloads: <see cref="ColumnFamily.Put(ReadOnlySpan{byte}, ReadOnlySpan{byte})"/>).</summary>
    public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => _default.Put(key, value);
    public void Put(WriteOptions options, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => _default.Put(options, key, value);
    public ValueTask PutAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default) =>
        _default.PutAsync(key, value, cancellationToken);
    public ValueTask PutAsync(WriteOptions options, ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default) =>
        _default.PutAsync(options, key, value, cancellationToken);

    /// <summary>Removes <paramref name="key"/> from the default column family (a no-op if it doesn't exist).</summary>
    public void Delete(ReadOnlySpan<byte> key) => _default.Delete(key);
    public void Delete(WriteOptions options, ReadOnlySpan<byte> key) => _default.Delete(options, key);
    public ValueTask DeleteAsync(ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default) => _default.DeleteAsync(key, cancellationToken);
    public ValueTask DeleteAsync(WriteOptions options, ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default) =>
        _default.DeleteAsync(options, key, cancellationToken);

    /// <summary>SingleDelete in the default column family; see <see cref="ColumnFamily.SingleDelete(ReadOnlySpan{byte})"/>.</summary>
    public void SingleDelete(ReadOnlySpan<byte> key) => _default.SingleDelete(key);
    public void SingleDelete(WriteOptions options, ReadOnlySpan<byte> key) => _default.SingleDelete(options, key);
    public ValueTask SingleDeleteAsync(ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default) => _default.SingleDeleteAsync(key, cancellationToken);
    public ValueTask SingleDeleteAsync(WriteOptions options, ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default) =>
        _default.SingleDeleteAsync(options, key, cancellationToken);

    /// <summary>Removes all keys in [<paramref name="beginKey"/>, <paramref name="endKey"/>) from the default column family.</summary>
    public void DeleteRange(ReadOnlySpan<byte> beginKey, ReadOnlySpan<byte> endKey) => _default.DeleteRange(beginKey, endKey);
    public void DeleteRange(WriteOptions options, ReadOnlySpan<byte> beginKey, ReadOnlySpan<byte> endKey) => _default.DeleteRange(options, beginKey, endKey);
    public ValueTask DeleteRangeAsync(ReadOnlyMemory<byte> beginKey, ReadOnlyMemory<byte> endKey, CancellationToken cancellationToken = default) =>
        _default.DeleteRangeAsync(beginKey, endKey, cancellationToken);
    public ValueTask DeleteRangeAsync(WriteOptions options, ReadOnlyMemory<byte> beginKey, ReadOnlyMemory<byte> endKey, CancellationToken cancellationToken = default) =>
        _default.DeleteRangeAsync(options, beginKey, endKey, cancellationToken);

    /// <summary>Adds a merge operand for <paramref name="key"/> in the default column family (requires a merge operator).</summary>
    public void Merge(ReadOnlySpan<byte> key, ReadOnlySpan<byte> operand) => _default.Merge(key, operand);
    public void Merge(WriteOptions options, ReadOnlySpan<byte> key, ReadOnlySpan<byte> operand) => _default.Merge(options, key, operand);
    public ValueTask MergeAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> operand, CancellationToken cancellationToken = default) =>
        _default.MergeAsync(key, operand, cancellationToken);
    public ValueTask MergeAsync(WriteOptions options, ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> operand, CancellationToken cancellationToken = default) =>
        _default.MergeAsync(options, key, operand, cancellationToken);

    /// <summary>Looks up <paramref name="key"/> in the default column family and appends its value to <paramref name="value"/>. Returns false if not found.</summary>
    public bool TryGet(ReadOnlySpan<byte> key, IBufferWriter<byte> value) => _default.TryGet(key, value);
    public bool TryGet(ReadOptions? options, ReadOnlySpan<byte> key, IBufferWriter<byte> value) => _default.TryGet(options, key, value);
    /// <summary>
    /// Looks up <paramref name="key"/> and copies up to <paramref name="destination"/>.Length bytes of its value. Returns
    /// false if not found; <paramref name="valueLength"/> is the full value length (which may exceed the destination).
    /// </summary>
    public bool TryGet(ReadOnlySpan<byte> key, Span<byte> destination, out int valueLength) => _default.TryGet(key, destination, out valueLength);
    public bool TryGet(ReadOptions? options, ReadOnlySpan<byte> key, Span<byte> destination, out int valueLength) =>
        _default.TryGet(options, key, destination, out valueLength);
    /// <summary>
    /// Zero-copy lookup: calls <paramref name="reader"/> with the value (valid only during the call). Returns false if the
    /// key was not found.
    /// </summary>
    public bool TryGet<TState, TResult>(ReadOnlySpan<byte> key, TState state, ReadOnlySpanFunc<TState, TResult> reader, out TResult result) =>
        _default.TryGet(key, state, reader, out result);
    public bool TryGet<TState, TResult>(ReadOptions? options, ReadOnlySpan<byte> key, TState state, ReadOnlySpanFunc<TState, TResult> reader, out TResult result) =>
        _default.TryGet(options, key, state, reader, out result);
    /// <summary>Zero-copy lookup returning <paramref name="reader"/>'s result, or default if the key was not found.</summary>
    public TResult? Get<TState, TResult>(ReadOnlySpan<byte> key, TState state, ReadOnlySpanFunc<TState, TResult> reader) => _default.Get(key, state, reader);
    /// <summary>The value of <paramref name="key"/> in a pooled buffer (dispose it to return the buffer), or null if not found.</summary>
    public PooledValue? GetPooled(ReadOnlySpan<byte> key) => _default.GetPooled(key);
    public PooledValue? GetPooled(ReadOptions? options, ReadOnlySpan<byte> key) => _default.GetPooled(options, key);
    /// <summary>The value of <paramref name="key"/> as a new array, or null if not found. Allocates; see the TryGet overloads.</summary>
    public byte[]? Get(ReadOnlySpan<byte> key) => _default.Get(key);
    public byte[]? Get(ReadOptions? options, ReadOnlySpan<byte> key) => _default.Get(options, key);
    public bool ContainsKey(ReadOnlySpan<byte> key) => _default.ContainsKey(key);
    /// <summary>Looks up several keys from one consistent view; null entries are keys that were not found. Allocates the results; see <see cref="MultiGetBatch"/>.</summary>
    public byte[]?[] MultiGet(IReadOnlyList<byte[]> keys) => _default.MultiGet(keys);
    public byte[]?[] MultiGet(ReadOptions? options, IReadOnlyList<byte[]> keys) => _default.MultiGet(options, keys);
    /// <summary>Runs a reusable <see cref="MultiGetBatch"/> against the default column family (DB::MultiGet).</summary>
    public void MultiGet(MultiGetBatch batch) => _default.MultiGet(batch);
    public void MultiGet(ReadOptions? options, MultiGetBatch batch) => _default.MultiGet(options, batch);
    /// <summary>Async <see cref="MultiGet(MultiGetBatch)"/>: data blocks missing from the cache are read asynchronously.</summary>
    public ValueTask MultiGetAsync(MultiGetBatch batch, CancellationToken cancellationToken = default) => _default.MultiGetAsync(batch, cancellationToken);
    public ValueTask MultiGetAsync(ReadOptions? options, MultiGetBatch batch, CancellationToken cancellationToken = default) =>
        _default.MultiGetAsync(options, batch, cancellationToken);
    /// <summary>
    /// Looks up <paramref name="keys"/> in the default column family with every result in caller memory (DB::MultiGet with
    /// a values and statuses array); see
    /// <see cref="ColumnFamily.MultiGet(ReadOptions?, ReadOnlySpan{ReadOnlyMemory{byte}}, IBufferWriter{byte}, Span{Range}, Span{MultiGetStatus})"/>.
    /// Does not allocate after warm-up.
    /// </summary>
    public void MultiGet(ReadOptions? options, ReadOnlySpan<ReadOnlyMemory<byte>> keys, IBufferWriter<byte> values, Span<Range> valueRanges,
        Span<MultiGetStatus> statuses) => _default.MultiGet(options, keys, values, valueRanges, statuses);
    /// <summary>
    /// Async <see cref="MultiGet(ReadOptions?, ReadOnlySpan{ReadOnlyMemory{byte}}, IBufferWriter{byte}, Span{Range}, Span{MultiGetStatus})"/>;
    /// see <see cref="ColumnFamily.MultiGetAsync(ReadOptions?, ReadOnlyMemory{ReadOnlyMemory{byte}}, IBufferWriter{byte}, Memory{Range}, Memory{MultiGetStatus}, CancellationToken)"/>.
    /// </summary>
    public ValueTask MultiGetAsync(ReadOptions? options, ReadOnlyMemory<ReadOnlyMemory<byte>> keys, IBufferWriter<byte> values,
        Memory<Range> valueRanges, Memory<MultiGetStatus> statuses, CancellationToken cancellationToken = default) =>
        _default.MultiGetAsync(options, keys, values, valueRanges, statuses, cancellationToken);

    /// <summary>Async <see cref="TryGet(ReadOnlySpan{byte}, IBufferWriter{byte})"/>; completes synchronously on memtable and cache hits.</summary>
    public ValueTask<bool> TryGetAsync(ReadOnlyMemory<byte> key, IBufferWriter<byte> value, CancellationToken cancellationToken = default) =>
        _default.TryGetAsync(key, value, cancellationToken);
    public ValueTask<bool> TryGetAsync(ReadOptions? options, ReadOnlyMemory<byte> key, IBufferWriter<byte> value, CancellationToken cancellationToken = default) =>
        _default.TryGetAsync(options, key, value, cancellationToken);
    /// <summary>Async <see cref="GetPooled(ReadOnlySpan{byte})"/>.</summary>
    public ValueTask<PooledValue?> GetPooledAsync(ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default) =>
        _default.GetPooledAsync(key, cancellationToken);
    public ValueTask<PooledValue?> GetPooledAsync(ReadOptions? options, ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default) =>
        _default.GetPooledAsync(options, key, cancellationToken);
    /// <summary>Async <see cref="Get(ReadOnlySpan{byte})"/>.</summary>
    public ValueTask<byte[]?> GetAsync(ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default) => _default.GetAsync(key, cancellationToken);
    public ValueTask<byte[]?> GetAsync(ReadOptions? options, ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default) =>
        _default.GetAsync(options, key, cancellationToken);

    // =====================================================================================================
    // Background errors
    // =====================================================================================================

    /// <summary>
    /// The current background error (a failed flush, compaction, WAL or MANIFEST write), with its
    /// <see cref="RocksDbException.Severity"/>, or null. With a hard (or worse) error writes fail with it; see
    /// <see cref="Resume"/>.
    /// </summary>
    public RocksDbException? BackgroundError => _db.BackgroundError;

    /// <summary>
    /// Recovers from a background error (DB::Resume): flushes the memtables (replacing a failed WAL or MANIFEST) and
    /// clears the error so writes and background work continue. Does nothing when there is no error that stops writes
    /// or background work. Throws the error if recovery is impossible (fatal or unrecoverable errors require reopening
    /// the DB) or failed again, Busy while an automatic recovery is running.
    /// </summary>
    public void Resume()
    {
        ValueTask t = _db.ResumeAsync(synchronous: true);
        if (!t.IsCompleted) t.AsTask().GetAwaiter().GetResult();
        else t.GetAwaiter().GetResult();
    }

    /// <inheritdoc cref="Resume"/>
    public ValueTask ResumeAsync() => _db.ResumeAsync(synchronous: false);

    // =====================================================================================================
    // Close
    // =====================================================================================================

    /// <summary>Closes the DB (DB::Close): waits for writes and background flushes, closes the WAL/MANIFEST, releases LOCK.</summary>
    public void Dispose()
    {
        ReleaseAllHandles();
        ValueTask t = _db.CloseAsync(synchronous: true);
        if (!t.IsCompleted) t.AsTask().GetAwaiter().GetResult();
        else t.GetAwaiter().GetResult();
    }

    public ValueTask DisposeAsync()
    {
        ReleaseAllHandles();
        return _db.CloseAsync(synchronous: false);
    }
}
