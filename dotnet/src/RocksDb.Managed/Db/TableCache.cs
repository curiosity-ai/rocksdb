using System.Collections.Concurrent;
using RocksDb.Managed.Db.Iterators;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;

namespace RocksDb.Managed.Db;

/// <summary>
/// Opens and caches <see cref="BlockBasedTableReader"/>s by file number. Port of rocksdb::TableCache
/// (db/table_cache.cc: FindTable / GetTableReader / Evict / ReleaseHandle) with a dictionary of ref-counted
/// handles in place of the LRU Cache of table readers.
/// <para>
/// One instance per column family (the reader options carry that CF's comparator and filter policy).
/// <b>Capacity</b>: with <c>max_open_files == -1</c> readers stay open until their file is deleted; otherwise the
/// cache keeps at most <c>max_open_files - 10</c> (kNumNonTableCacheFiles) unreferenced readers, evicting the least
/// recently opened ones.
/// </para>
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
    private readonly TableReaderOptions _readerOptions;
    private readonly int _capacity;
    private readonly ConcurrentDictionary<ulong, Handle> _handles = new();
    private readonly Lock _openLock = new();
    private long _openSequence;
    private bool _disposed;

    private static readonly Action<object?> s_release = static h => ((Handle)h!).Release();

    /// <param name="dbPaths">db_paths (index = path id); the first entry is the DB directory.</param>
    /// <param name="maxOpenFiles">DbOptions.max_open_files (-1 = unbounded).</param>
    public TableCache(IFileSystem fs, IReadOnlyList<string> dbPaths, TableReaderOptions readerOptions, int maxOpenFiles)
    {
        _fs = fs;
        _dbPaths = dbPaths;
        _readerOptions = readerOptions;
        // table_cache_size = max_open_files - kNumNonTableCacheFiles (db/db_impl/db_impl.cc)
        _capacity = maxOpenFiles < 0 ? int.MaxValue : Math.Max(maxOpenFiles - 10, 20);
    }

    public int Count => _handles.Count;

    /// <summary>When set (DBOptions::statistics), table file reads are timed (SST_READ_MICROS, FILE_READ_*_MICROS).</summary>
    internal Statistics? Statistics { get; set; }

    private sealed class Handle
    {
        public readonly ulong FileNumber;
        public BlockBasedTableReader? Reader;
        public FragmentedRangeTombstoneList? RangeDels;
        public long LastUse;
        // One reference is held by the cache itself while the handle is in the dictionary.
        private int _refs = 1;
        private int _opened;

        public Handle(ulong fileNumber) => FileNumber = fileNumber;

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
    }

    /// <summary>Path of a table file (TableFileName with path id).</summary>
    public string TableFilePath(FileMetaData file) => FileNames.TableFileName(_dbPaths, file.FileNumber, file.PathId);

    public TableReaderLease GetReader(FileMetaData file)
    {
        Handle h = FindTable(file);
        return new TableReaderLease(h.Reader!, s_release, h);
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
        if (_handles.TryGetValue(file.FileNumber, out Handle? h) && h.IsOpen && h.TryRef())
        {
            h.LastUse = Interlocked.Increment(ref _openSequence);
            lease = new TableReaderLease(h.Reader!, s_release, h);
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
        Handle h = FindTable(file);
        rangeTombstones = h.RangeDels;
        return new TableReaderLease(h.Reader!, s_release, h);
    }

    /// <summary>The fragmented range tombstones of a table (opening it if needed), or null if it has none.</summary>
    public FragmentedRangeTombstoneList? GetRangeTombstones(FileMetaData file)
    {
        using TableReaderLease lease = GetReader(file, out FragmentedRangeTombstoneList? r);
        return r;
    }

    /// <summary>Iterator over the table; disposing it releases the reader lease.</summary>
    public InternalIterator NewIterator(FileMetaData file) => new TableInternalIterator(GetReader(file));

    /// <summary>Recycles <paramref name="reuse"/> when it is a table iterator with default flags (no allocation per file).</summary>
    public InternalIterator NewIterator(FileMetaData file, InternalIterator? reuse) => Recycle(reuse, GetReader(file), TableReadFlags.None);

    /// <summary>Retargets <paramref name="reuse"/> to <paramref name="lease"/> if it is a table iterator with <paramref name="flags"/>; otherwise disposes it and creates a new one.</summary>
    internal static InternalIterator Recycle(InternalIterator? reuse, TableReaderLease lease, TableReadFlags flags)
    {
        if (reuse is TableInternalIterator t && t.Flags == flags)
        {
            t.Retarget(lease, null, null);
            return t;
        }
        reuse?.Dispose();
        return new TableInternalIterator(lease, flags);
    }

    /// <summary>Port of TableCache::FindTable: returns a referenced, opened handle.</summary>
    private Handle FindTable(FileMetaData file)
    {
        ulong number = file.FileNumber;
        while (true)
        {
            if (_handles.TryGetValue(number, out Handle? h) && h.IsOpen && h.TryRef())
            {
                h.LastUse = Interlocked.Increment(ref _openSequence);
                return h;
            }
            ObjectDisposedException.ThrowIf(_disposed, this);
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
                h = new Handle(number);
                try
                {
                    Open(h, file);
                }
                catch (Exception e) when (e is IOException or RocksDbException)
                {
                    // Don't cache failures (like upstream, a failed open is retried on the next lookup).
                    throw e is RocksDbException ? e : RocksDbException.IOError("While open table file " + TableFilePath(file) + ": " + e.Message, e);
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

    private void Open(Handle h, FileMetaData file)
    {
        string path = TableFilePath(file);
        IRandomAccessFile f = _fs.OpenRandomAccess(path);
        if (Statistics is { } st) f = new Monitoring.StatisticsRandomAccessFile(f, st);
        BlockBasedTableReader reader;
        try
        {
            TableReaderOptions opts = _readerOptions;
            if (file.LargestSeqno != opts.LargestSeqno || file.FileNumber != opts.FileNumber)
                opts = opts with { LargestSeqno = file.LargestSeqno, FileNumber = file.FileNumber };
            reader = BlockBasedTableReader.Open(f, opts);
        }
        catch
        {
            f.Dispose();
            throw;
        }
        h.Reader = reader;
        // FileMetaData::TryGetNewestKeyTime reads the pinned reader's properties; remember it for the FIFO TTL picker.
        if (file.NewestKeyTime == 0) file.NewestKeyTime = reader.Properties.NewestKeyTime;
        if (reader.HasRangeTombstones)
        {
            var list = new List<FragmentedRangeTombstoneList.Tombstone>();
            using (BlockIter it = reader.NewRangeTombstoneIterator()!)
            {
                for (it.SeekToFirst(); it.Valid; it.Next())
                {
                    ReadOnlySpan<byte> k = it.Key;
                    if (!DbFormat.TryParseInternalKey(k, out ParsedInternalKey pk)) ThrowHelper.Corruption("corrupted range tombstone key in " + path);
                    list.Add(new FragmentedRangeTombstoneList.Tombstone(k[..pk.UserKeyLength].ToArray(), it.Value.ToArray(), pk.Sequence));
                }
            }
            // Tables without persisted timestamps: the start keys come back padded, the end keys (values) without a
            // timestamp (tombstone_end_include_ts = false).
            FragmentedRangeTombstoneList frag = FragmentedRangeTombstoneList.Build(_readerOptions.Comparator, list, endIncludesTimestamp: reader.PadTimestampSize == 0);
            h.RangeDels = frag.IsEmpty ? null : frag;
        }
    }

    // Caller holds _openLock.
    private void EvictUnreferenced()
    {
        // Cold path: evict the least recently used unreferenced readers until within capacity.
        var candidates = new List<Handle>();
        foreach (KeyValuePair<ulong, Handle> kv in _handles)
        {
            if (kv.Value.Refs == 1) candidates.Add(kv.Value);
        }
        candidates.Sort(static (a, b) => a.LastUse.CompareTo(b.LastUse));
        int excess = _handles.Count - _capacity;
        for (int i = 0; i < candidates.Count && excess > 0; i++)
        {
            if (((ICollection<KeyValuePair<ulong, Handle>>)_handles).Remove(new KeyValuePair<ulong, Handle>(candidates[i].FileNumber, candidates[i])))
            {
                candidates[i].Release();
                excess--;
            }
        }
    }

    /// <summary>Port of TableCache::Evict: drops the cached reader of a deleted file (closed once unreferenced).</summary>
    public void Evict(ulong fileNumber)
    {
        if (_handles.TryRemove(fileNumber, out Handle? h)) h.Release();
    }

    public void Dispose()
    {
        _disposed = true;
        foreach (KeyValuePair<ulong, Handle> kv in _handles)
        {
            if (_handles.TryRemove(kv.Key, out Handle? h)) h.Release();
        }
    }
}
