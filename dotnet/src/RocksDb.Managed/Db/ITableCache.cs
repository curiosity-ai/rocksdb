using RocksDb.Managed.Db.Iterators;
using RocksDb.Managed.Table;

namespace RocksDb.Managed.Db;

/// <summary>
/// CONTRACT (phase 2). Opens and caches table readers by file number. Port of rocksdb::TableCache (db/table_cache.h).
/// Readers are ref-counted: callers must dispose the returned lease.
/// </summary>
public interface ITableCache
{
    /// <summary>Returns a lease on the (cached) reader for the file; dispose the lease to release it.</summary>
    ValueTask<TableReaderLease> GetReaderAsync(FileMetaData file, CancellationToken cancellationToken = default);
    TableReaderLease GetReader(FileMetaData file);
    /// <summary>Creates an internal iterator over the file; disposing the iterator releases the reader.</summary>
    InternalIterator NewIterator(FileMetaData file);
    /// <summary>
    /// Like <see cref="NewIterator(FileMetaData)"/>, but may recycle <paramref name="reuse"/> (an iterator previously
    /// returned by this cache, which the caller gives up) instead of creating a new one. The default disposes it.
    /// </summary>
    InternalIterator NewIterator(FileMetaData file, InternalIterator? reuse)
    {
        reuse?.Dispose();
        return NewIterator(file);
    }
    /// <summary>Drops the cached reader for a deleted file.</summary>
    void Evict(ulong fileNumber);
}

/// <summary>A reference to a cached <see cref="BlockBasedTableReader"/>. Dispose exactly once.</summary>
public readonly struct TableReaderLease : IDisposable
{
    private readonly Action<object?>? _release;
    private readonly object? _state;

    public TableReaderLease(BlockBasedTableReader reader, Action<object?>? release, object? state)
    {
        Reader = reader;
        _release = release;
        _state = state;
    }

    public BlockBasedTableReader Reader { get; }
    public void Dispose() => _release?.Invoke(_state);
}
