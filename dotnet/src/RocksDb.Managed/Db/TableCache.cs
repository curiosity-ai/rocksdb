using System.Runtime.CompilerServices;
using System.Collections.Concurrent;
using System.Globalization;
using RocksDb.Managed.Db.Iterators;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;

namespace RocksDb.Managed.Db;

/// <summary>
/// The DB-wide cache of open table readers (the <c>table_cache_</c> LRU cache of DBImpl, db/db_impl/db_impl.cc, shared by
/// all column families through their <see cref="TableCache"/>). Readers are ref-counted handles keyed by file number
/// (file numbers are unique across column families).
/// <para>
/// <b>Capacity</b> (table_cache_size): <c>max_open_files - 10</c> (kNumNonTableCacheFiles, at least 20) unreferenced
/// readers, evicting the least recently used ones; unbounded with <c>max_open_files == -1</c>
/// (TableCache::kInfiniteCapacity). Changed by SetDBOptions(max_open_files) like upstream's SetCapacity.
/// </para>
/// <para>
/// <b>Synchronization</b>: lookups of open readers are lock-free (ConcurrentDictionary + CAS reference counts); opening
/// and eviction are serialized by <c>_openLock</c>.
/// </para>
/// </summary>
internal sealed class TableCacheStore : IDisposable
{
    private readonly ConcurrentDictionary<ulong, Handle> _handles = new();
    private readonly Lock _openLock = new();
    private long _openSequence;
    private int _capacity;
    private bool _disposed;

    internal static readonly Action<object?> s_release = static h => ((Handle)h!).Release();

    /// <param name="maxOpenFiles">DbOptions.max_open_files (-1 = unbounded).</param>
    public TableCacheStore(int maxOpenFiles) => _capacity = CapacityFor(maxOpenFiles);

    /// <summary>
    /// table_cache_size = max_open_files - kNumNonTableCacheFiles (10; db/db_impl/db_impl.cc). DB::Open clips
    /// max_open_files to at least 20 (SanitizeOptions), so the capacity is at least 10 there; SetDBOptions does not clip
    /// (upstream then passes a negative size to the LRU cache), so the capacity is kept at least 1 here.
    /// </summary>
    private static int CapacityFor(int maxOpenFiles) => maxOpenFiles < 0 ? int.MaxValue : Math.Max(maxOpenFiles - 10, 1);

    /// <summary>Number of cached readers (open or being opened) over all column families.</summary>
    public int Count => _handles.Count;

    public int Capacity => Volatile.Read(ref _capacity);

    /// <summary>SetDBOptions(max_open_files): new capacity; excess unreferenced readers are closed right away.</summary>
    public void SetCapacity(int maxOpenFiles)
    {
        lock (_openLock)
        {
            Volatile.Write(ref _capacity, CapacityFor(maxOpenFiles));
            if (_handles.Count > _capacity) EvictUnreferenced();
        }
    }

    internal sealed class Handle
    {
        public readonly ulong FileNumber;
        public readonly TableCache Owner;
        public TableReader? Reader;
        public FragmentedRangeTombstoneList? RangeDels;
        public long LastUse;
        /// <summary>1 while the handle holds a pin reference (see <see cref="TableCacheStore.TryGetPinned"/>).</summary>
        public int Pinned;
        // One reference is held by the cache itself while the handle is in the dictionary.
        private int _refs = 1;
        private int _opened;

        public Handle(ulong fileNumber, TableCache owner)
        {
            FileNumber = fileNumber;
            Owner = owner;
        }

        public bool IsOpen => Volatile.Read(ref _opened) != 0;
        public void MarkOpen() => Volatile.Write(ref _opened, 1);

        public bool TryRef()
        {
            int r = Volatile.Read(ref _refs);
            while (r > 0)
            {
                int o = Interlocked.CompareExchange(ref _refs, r + 1, r);
                if (o == r) return true;
                r = o;
            }
            return false;
        }

        public int Refs => Volatile.Read(ref _refs);

        public void Release()
        {
            if (Interlocked.Decrement(ref _refs) == 0)
            {
                Reader?.Dispose();
                Reader = null;
            }
        }

        /// <summary>Drops the cache's reference and the pin reference, if any (the handle left the dictionary).</summary>
        public void ReleaseFromCache()
        {
            if (Interlocked.Exchange(ref Pinned, 0) != 0) Release();
            Release();
        }
    }

    /// <summary>
    /// The open handle of <paramref name="fileNumber"/> for a reader that holds a version containing the file, without a
    /// per-call reference, or null (not open yet, or not pinnable). Port of the pinned <c>fd.table_reader</c> of
    /// max_open_files == -1: while the cache is unbounded, the first such lookup pins the handle with one extra
    /// reference, which keeps the reader open (and out of capacity eviction, also after a later SetCapacity) until the
    /// file is evicted as obsolete (no referenced version contains it any more) or its column family / the DB is closed.
    /// </summary>
    internal Handle? TryGetPinned(ulong fileNumber)
    {
        if (!_handles.TryGetValue(fileNumber, out Handle? h) || !h.IsOpen) return null;
        if (Volatile.Read(ref h.Pinned) == 0 && !Pin(h)) return null;
        return h;
    }

    private bool Pin(Handle h)
    {
        if (Volatile.Read(ref _capacity) != int.MaxValue || !h.TryRef()) return false;
        // Another reader pinned it first: drop our extra reference. (Evict cannot race with this: a pinned lookup holds a
        // version containing the file, and only obsolete files are evicted.)
        if (Interlocked.CompareExchange(ref h.Pinned, 1, 0) != 0) h.Release();
        return true;
    }

    /// <summary>A referenced handle if the table is already open (FindTable with no_io), else false.</summary>
    internal bool TryGetOpen(ulong fileNumber, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out Handle? handle)
    {
        if (_handles.TryGetValue(fileNumber, out Handle? h) && h.IsOpen && h.TryRef())
        {
            h.LastUse = Interlocked.Increment(ref _openSequence);
            handle = h;
            return true;
        }
        handle = null;
        return false;
    }

    /// <summary>Port of TableCache::FindTable: returns a referenced, opened handle.</summary>
    internal Handle FindTable(TableCache owner, FileMetaData file)
    {
        ulong number = file.FileNumber;
        while (true)
        {
            if (_handles.TryGetValue(number, out Handle? h) && h.IsOpen && h.TryRef())
            {
                // LastUse only orders evictions: an unbounded cache skips the shared counter.
                if (Volatile.Read(ref _capacity) != int.MaxValue) h.LastUse = Interlocked.Increment(ref _openSequence);
                return h;
            }
            ObjectDisposedException.ThrowIf(_disposed || owner.IsDisposed, owner);
            lock (_openLock)
            {
                // Re-check under the lock: someone may have opened it meanwhile.
                if (_handles.TryGetValue(number, out h) && h.IsOpen)
                {
                    if (h.TryRef())
                    {
                        h.LastUse = Interlocked.Increment(ref _openSequence);
                        return h;
                    }
                    continue; // being evicted; retry
                }
                h = new Handle(number, owner);
                try
                {
                    owner.Open(h, file);
                }
                catch (Exception e) when (e is IOException or RocksDbException)
                {
                    // Don't cache failures (like upstream, a failed open is retried on the next lookup).
                    throw RocksDbException.FromIOException("While open table file", owner.TableFilePath(file), e);
                }
                h.MarkOpen();
                h.LastUse = Interlocked.Increment(ref _openSequence);
                _handles[number] = h;
                bool referenced = h.TryRef();
                System.Diagnostics.Debug.Assert(referenced);
                if (_handles.Count > _capacity) EvictUnreferenced();
                return h;
            }
        }
    }

    // Caller holds _openLock.
    private void EvictUnreferenced()
    {
        // Cold path: evict the least recently used unreferenced readers until within capacity. Lookups of open readers
        // stamp LastUse without the lock, so the stamps are copied before sorting: sorting by the live field let a
        // concurrent lookup change a key in the middle of the sort, and List.Sort threw ("IComparer.Compare() method
        // returns inconsistent results") out of the Get that triggered the eviction.
        var candidates = new List<(long LastUse, Handle Handle)>();
        foreach (KeyValuePair<ulong, Handle> kv in _handles)
        {
            if (kv.Value.Refs == 1) candidates.Add((Volatile.Read(ref kv.Value.LastUse), kv.Value));
        }
        candidates.Sort(static (a, b) => a.LastUse.CompareTo(b.LastUse));
        int excess = _handles.Count - _capacity;
        for (int i = 0; i < candidates.Count && excess > 0; i++)
        {
            Handle h = candidates[i].Handle;
            if (((ICollection<KeyValuePair<ulong, Handle>>)_handles).Remove(new KeyValuePair<ulong, Handle>(h.FileNumber, h)))
            {
                h.Release();
                excess--;
            }
        }
    }

    /// <summary>Port of TableCache::Evict: drops the cached reader of a deleted file (closed once unreferenced).</summary>
    public void Evict(ulong fileNumber)
    {
        if (_handles.TryRemove(fileNumber, out Handle? h)) h.ReleaseFromCache();
    }

    /// <summary>Drops every reader opened through <paramref name="owner"/> (its column family is being released).</summary>
    public void EvictOwner(TableCache owner)
    {
        foreach (KeyValuePair<ulong, Handle> kv in _handles)
        {
            if (kv.Value.Owner == owner && ((ICollection<KeyValuePair<ulong, Handle>>)_handles).Remove(kv)) kv.Value.ReleaseFromCache();
        }
    }

    public int CountOwnedBy(TableCache owner)
    {
        int n = 0;
        foreach (KeyValuePair<ulong, Handle> kv in _handles)
        {
            if (kv.Value.Owner == owner) n++;
        }
        return n;
    }

    public void Dispose()
    {
        _disposed = true;
        foreach (KeyValuePair<ulong, Handle> kv in _handles)
        {
            if (_handles.TryRemove(kv.Key, out Handle? h)) h.ReleaseFromCache();
        }
    }
}

/// <summary>
/// A column family's view of the table cache. Port of rocksdb::TableCache (db/table_cache.cc: FindTable /
/// GetTableReader / Evict / ReleaseHandle): readers are opened with this column family's reader options (comparator,
/// filter policy, block cache) and cached in the DB-wide <see cref="TableCacheStore"/>, which bounds the number of
/// open readers over all column families by max_open_files.
/// <para>
/// The fragmented range tombstones of each table are built once when it is opened (like the cached
/// fragmented_range_dels of BlockBasedTable::Rep) and exposed through <see cref="GetRangeTombstones"/>.
/// </para>
/// <para>
/// Readers share the column family's block cache (see <see cref="TableFactory.CreateReaderOptions(ColumnFamilyOptions, TableFormatOptions, Cache.BlockCache?, string?)"/>);
/// the file number is passed so cache keys are stable when table properties lack the original file number.
/// </para>
/// </summary>
internal sealed class TableCache : ITableCache, IDisposable
{
    private readonly IFileSystem _fs;
    private readonly IReadOnlyList<string> _dbPaths;
    private TableReaderOptions _readerOptions;
    private readonly TableCacheStore _store;
    private readonly bool _ownsStore;
    private volatile bool _disposed;

    /// <summary>A view on the DB-wide <paramref name="store"/>.</summary>
    /// <param name="dbPaths">db_paths (index = path id); the first entry is the DB directory.</param>
    public TableCache(TableCacheStore store, IFileSystem fs, IReadOnlyList<string> dbPaths, TableReaderOptions readerOptions)
    {
        _store = store;
        _fs = fs;
        _dbPaths = dbPaths;
        _readerOptions = readerOptions;
    }

    /// <summary>A stand-alone cache with its own store (tests and tools).</summary>
    /// <param name="maxOpenFiles">DbOptions.max_open_files (-1 = unbounded).</param>
    public TableCache(IFileSystem fs, IReadOnlyList<string> dbPaths, TableReaderOptions readerOptions, int maxOpenFiles)
        : this(new TableCacheStore(maxOpenFiles), fs, dbPaths, readerOptions)
    {
        _ownsStore = true;
    }

    /// <summary>
    /// The expected_unique_id check of BlockBasedTable::Open: the unique id the MANIFEST recorded for the file must be the
    /// one derived from the table's db_id / db_session_id / orig_file_number properties (GetSstInternalUniqueId, force).
    /// </summary>
    private static void VerifyUniqueId(TableReader reader, FileMetaData file)
    {
        TableProperties props = reader.Properties;
        Cache.OffsetableCacheKey.GetSstInternalUniqueId(props.DbId, props.DbSessionId, props.OriginalFileNumber, out ulong id0, out ulong id1);
        UniqueId64x2 expected = file.UniqueId;
        if (expected.Lo != id0 || expected.Hi != id1)
        {
            ThrowHelper.Corruption("Mismatch in unique ID on table file " + file.FileNumber.ToString(CultureInfo.InvariantCulture) + ". Expected: {"
                + expected.Lo.ToString(CultureInfo.InvariantCulture) + "," + expected.Hi.ToString(CultureInfo.InvariantCulture) + "} Actual: {"
                + id0.ToString(CultureInfo.InvariantCulture) + "," + id1.ToString(CultureInfo.InvariantCulture) + "}");
        }
    }

    /// <summary>Options for readers opened from now on (SetOptions replaces them; open readers keep theirs).</summary>
    internal TableReaderOptions ReaderOptions
    {
        get => Volatile.Read(ref _readerOptions);
        set => Volatile.Write(ref _readerOptions, value);
    }

    /// <summary>
    /// DBOptions::verify_sst_unique_id_in_manifest: opening a table whose MANIFEST entry carries a unique id fails with
    /// Corruption when the id derived from the file's properties differs (BlockBasedTable::Open with expected_unique_id).
    /// </summary>
    internal bool VerifyUniqueIdInManifest { get; set; }

    private Cache.RowCache? _rowCache;
    private ulong _rowCacheId;

    /// <summary>
    /// DBOptions::row_cache (TableCache::Get / MultiGet row cache lookups); setting it draws this table cache's key
    /// prefix (row_cache_id_ = row_cache->NewId()).
    /// </summary>
    internal Cache.RowCache? RowCache
    {
        get => _rowCache;
        init
        {
            _rowCache = value;
            _rowCacheId = value?.NewId() ?? 0;
        }
    }

    /// <summary>
    /// TableCache::CreateRowCacheKeyPrefix + the user key: row_cache_id | varint64(file number) | varint64(seq) | user key,
    /// where seq is 0 when the read sequence exposes all of the file, else 1 + the read sequence. Returns the length
    /// written to <paramref name="dst"/> and the sequence number entries are replayed with (seq - 1, or 0).
    /// Deviation: upstream uses 1 + snapshot for a snapshot equal to the file's largest seqno too (which also sees the
    /// whole file); only the cache key differs, not the result.
    /// </summary>
    internal int RowCacheKey(Span<byte> dst, FileMetaData file, ReadOnlySpan<byte> userKey, ulong readSequence, out ulong replaySeq)
    {
        ulong entrySeq = readSequence < file.LargestSeqno ? 1 + readSequence : 0;
        int n = Util.Coding.EncodeVarint64(dst, _rowCacheId);
        n += Util.Coding.EncodeVarint64(dst[n..], file.FileNumber);
        n += Util.Coding.EncodeVarint64(dst[n..], entrySeq);
        userKey.CopyTo(dst[n..]);
        replaySeq = entrySeq == 0 ? 0 : entrySeq - 1;
        return n + userKey.Length;
    }

    /// <summary>Maximum length of the row cache key prefix (three varint64s).</summary>
    internal const int RowCacheKeyPrefixMax = 30;

    /// <summary>
    /// The column family's internal stats: with <see cref="Statistics"/>, reads of a table are also recorded in the
    /// latency histogram of the level it was opened at (InternalStats::GetFileReadHist, shown by rocksdb.cfstats).
    /// </summary>
    internal InternalStats? LevelStats { get; set; }

    /// <summary>The column family's num_levels (a reader opened for a file of level num_levels - 1 is a last level reader); 0: unknown.</summary>
    internal int NumLevels { get; set; }

    /// <summary>When set (DBOptions::statistics), table file reads are timed (SST_READ_MICROS, FILE_READ_*_MICROS).</summary>
    internal Statistics? Statistics { get; set; }

    /// <summary>Readers of this column family currently cached.</summary>
    public int Count => _store.CountOwnedBy(this);

    internal bool IsDisposed => _disposed;

    /// <summary>Path of a table file (TableFileName with path id).</summary>
    public string TableFilePath(FileMetaData file) => FileNames.TableFileName(_dbPaths, file.FileNumber, file.PathId);

    /// <summary>
    /// True while DB::Open loads the tables of the recovered versions (VersionEditHandler::LoadTables with
    /// prefetch_index_and_filter_in_cache = false): tables opened meanwhile do not warm the block cache with their
    /// index, filter and partitions; only pinned metadata blocks are loaded. Set by the opening thread before the DB
    /// serves reads.
    /// </summary>
    internal volatile bool InitialLoad;

    public TableReaderLease GetReader(FileMetaData file)
    {
        TableCacheStore.Handle h = _store.FindTable(this, file);
        return new TableReaderLease(h.Reader!, TableCacheStore.s_release, h);
    }

    public ValueTask<TableReaderLease> GetReaderAsync(FileMetaData file, CancellationToken cancellationToken = default)
    {
        // Opening a table reads the footer and the meta blocks; the first open of a file does that synchronously (cold
        // path, once per file), later lookups are cache hits.
        return new ValueTask<TableReaderLease>(GetReader(file));
    }

    /// <summary>
    /// TableCache::FindTable(no_io = true): a lease on the reader if the table is already open, without opening it
    /// (kBlockCacheTier lookups must not do file I/O).
    /// </summary>
    public bool TryGetOpenReader(FileMetaData file, out TableReaderLease lease)
    {
        if (_store.TryGetOpen(file.FileNumber, out TableCacheStore.Handle? h))
        {
            lease = new TableReaderLease(h.Reader!, TableCacheStore.s_release, h);
            return true;
        }
        lease = default;
        return false;
    }

    /// <summary>
    /// Gets a reader lease plus the table's fragmented range tombstones (null if the table has none). The tombstone
    /// list is valid while the lease is held.
    /// </summary>
    public TableReaderLease GetReader(FileMetaData file, out FragmentedRangeTombstoneList? rangeTombstones)
    {
        TableCacheStore.Handle h = _store.FindTable(this, file);
        rangeTombstones = h.RangeDels;
        return new TableReaderLease(h.Reader!, TableCacheStore.s_release, h);
    }

    /// <summary>
    /// <see cref="GetReader(FileMetaData, out FragmentedRangeTombstoneList?)"/> for a file of a version the caller holds
    /// referenced (through a super version or a version reference) for as long as it uses the lease. With
    /// <c>max_open_files == -1</c> the reader is pinned (<see cref="TableCacheStore.TryGetPinned"/>) and stays open until
    /// its file becomes obsolete, which cannot happen while a referenced version contains it, so the lease takes no
    /// reference: no shared counter is written per lookup (like the pinned <c>fd.table_reader</c> that Version::Get uses
    /// upstream). Otherwise the lease is referenced as usual.
    /// </summary>
    public TableReaderLease GetReaderForVersion(FileMetaData file, out FragmentedRangeTombstoneList? rangeTombstones)
    {
        if (_store.TryGetPinned(file.FileNumber) is { Reader: { } reader } h)
        {
            rangeTombstones = h.RangeDels;
            return new TableReaderLease(reader, null, null);
        }
        return GetReader(file, out rangeTombstones);
    }

    /// <summary>
    /// <see cref="GetReaderForVersion(FileMetaData, out FragmentedRangeTombstoneList?)"/> for a read with
    /// <paramref name="flags"/>: with <see cref="TableReadFlags.BlockCacheTier"/> a table that is not open is not
    /// opened (TableCache::FindTable with no_io) and the read fails with Incomplete.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public TableReaderLease GetReaderForVersion(FileMetaData file, out FragmentedRangeTombstoneList? rangeTombstones, TableReadFlags flags)
    {
        if ((flags & TableReadFlags.BlockCacheTier) == 0) return GetReaderForVersion(file, out rangeTombstones);
        return GetOpenReaderNoIo(file, out rangeTombstones);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private TableReaderLease GetOpenReaderNoIo(FileMetaData file, out FragmentedRangeTombstoneList? rangeTombstones)
    {
        if (_store.TryGetPinned(file.FileNumber) is { Reader: { } reader } p)
        {
            rangeTombstones = p.RangeDels;
            return new TableReaderLease(reader, null, null);
        }
        if (_store.TryGetOpen(file.FileNumber, out TableCacheStore.Handle? h))
        {
            rangeTombstones = h.RangeDels;
            return new TableReaderLease(h.Reader!, TableCacheStore.s_release, h);
        }
        rangeTombstones = null;
        BlockBasedTableReader.ThrowNoIo();
        return default;
    }

    /// <summary>The fragmented range tombstones of a table (opening it if needed), or null if it has none.</summary>
    public FragmentedRangeTombstoneList? GetRangeTombstones(FileMetaData file)
    {
        using TableReaderLease lease = GetReader(file, out FragmentedRangeTombstoneList? r);
        return r;
    }

    /// <summary>Iterator over the table; disposing it releases the reader lease.</summary>
    public InternalIterator NewIterator(FileMetaData file) => TableInternalIterator.Create(GetReader(file), TableReadFlags.None, null, null);

    /// <summary>Recycles <paramref name="reuse"/> when it is a table iterator with default flags (no allocation per file).</summary>
    public InternalIterator NewIterator(FileMetaData file, InternalIterator? reuse) => Recycle(reuse, GetReader(file), TableReadFlags.None);

    /// <summary>Retargets <paramref name="reuse"/> to <paramref name="lease"/> if it is a table iterator with <paramref name="flags"/>; otherwise disposes it and creates a new one.</summary>
    internal static InternalIterator Recycle(InternalIterator? reuse, TableReaderLease lease, TableReadFlags flags)
    {
        if (reuse is TableInternalIterator t && t.Flags == flags && lease.Reader is BlockBasedTableReader)
        {
            t.Retarget(lease, null, null);
            return t;
        }
        reuse?.Dispose();
        return TableInternalIterator.Create(lease, flags, null, null);
    }

    /// <summary>Opens the reader of <paramref name="h"/> with this column family's options (called by the store).</summary>
    internal void Open(TableCacheStore.Handle h, FileMetaData file)
    {
        string path = TableFilePath(file);
        IRandomAccessFile f = _fs.OpenRandomAccess(path);
        Statistics?.RecordTick(Ticker.NoFileOpens); // TableCache::GetTableReader, after NewRandomAccessFile succeeded
        if (Statistics is { } st)
        {
            f = new Monitoring.StatisticsRandomAccessFile(f, st, LevelStats?.GetFileReadHist(file.LevelHint)) { IsLastLevel = file.LevelHint == NumLevels - 1 };
        }
        TableReader reader;
        try
        {
            // ReadFooterFromFile: the size recorded in the MANIFEST (FileDescriptor::file_size) must match the file.
            if (file.FileSize != 0 && (ulong)f.Length != file.FileSize)
            {
                ThrowHelper.Corruption("Sst file size mismatch between expected " + file.FileSize.ToString(CultureInfo.InvariantCulture)
                    + " and file system " + f.Length.ToString(CultureInfo.InvariantCulture) + " sstable: " + path);
            }
            TableReaderOptions opts = ReaderOptions;
            if (file.LargestSeqno != opts.LargestSeqno || file.FileNumber != opts.FileNumber || file.LevelHint != opts.Level)
                opts = opts with { LargestSeqno = file.LargestSeqno, FileNumber = file.FileNumber, Level = file.LevelHint };
            if (InitialLoad) opts = opts with { PrefetchIndexAndFilterInCache = false };
            reader = TableReaderFactory.Open(f, opts);
        }
        catch
        {
            f.Dispose();
            throw;
        }
        if (VerifyUniqueIdInManifest && file.UniqueId != UniqueId64x2.Null)
        {
            try
            {
                VerifyUniqueId(reader, file);
            }
            catch
            {
                reader.Dispose();
                throw;
            }
        }
        h.Reader = reader;
        // FileMetaData::TryGetNewestKeyTime reads the pinned reader's properties; remember it for the FIFO TTL picker.
        if (file.NewestKeyTime == 0) file.NewestKeyTime = reader.Properties.NewestKeyTime;
        if (reader.HasRangeTombstones)
        {
            // Tables without persisted timestamps: the start keys come back padded, the end keys (values) without a
            // timestamp (tombstone_end_include_ts = false). The keys are copied into the fragmenter's pooled buffer, and
            // the fragmented list is cached with the reader (BlockBasedTable::Rep::fragmented_range_dels).
            FragmentedRangeTombstoneList frag;
            RangeTombstoneFragmenter fragmenter = RangeTombstoneFragmenter.Rent(reader.InternalComparator.UserComparator,
                endIncludesTimestamp: reader.PadTimestampSize == 0);
            try
            {
                using (BlockIter it = reader.NewRangeTombstoneIterator()!)
                {
                    for (it.SeekToFirst(); it.Valid; it.Next())
                    {
                        ReadOnlySpan<byte> k = it.Key;
                        if (!DbFormat.TryParseInternalKey(k, out ParsedInternalKey pk)) ThrowHelper.Corruption("corrupted range tombstone key in " + path);
                        fragmenter.Add(k[..pk.UserKeyLength], it.Value, pk.Sequence);
                    }
                }
                frag = fragmenter.Finish();
            }
            finally
            {
                RangeTombstoneFragmenter.Return(fragmenter);
            }
            h.RangeDels = frag.IsEmpty ? null : frag;
        }
    }

    /// <summary>Port of TableCache::Evict: drops the cached reader of a deleted file (closed once unreferenced).</summary>
    public void Evict(ulong fileNumber) => _store.Evict(fileNumber);

    public void Dispose()
    {
        _disposed = true;
        if (_ownsStore) _store.Dispose();
        else _store.EvictOwner(this);
    }
}
