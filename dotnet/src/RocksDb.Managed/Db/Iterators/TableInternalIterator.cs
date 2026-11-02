using RocksDb.Managed.Table;

namespace RocksDb.Managed.Db.Iterators;

/// <summary>
/// <see cref="InternalIterator"/> over the point entries of a block-based table. Adapter over
/// <see cref="BlockBasedTableIterator"/> (port of BlockBasedTableIterator, table/block_based/block_based_table_iterator.cc):
/// the async overrides use the table iterator's async paths so a data block read awaits instead of blocking and
/// cache hits complete synchronously.
/// </summary>
/// <remarks>
/// Disposing the iterator disposes the table iterator (releasing its pinned block) and then the
/// <see cref="TableReaderLease"/> it was created with (returning the reader to the table cache). This is the type an
/// <see cref="ITableCache.NewIterator"/> implementation is expected to return.
/// </remarks>
public sealed class TableInternalIterator : InternalIterator
{
    private readonly BlockBasedTableIterator _it;
    private TableReaderLease _lease;
    private bool _disposed;

    /// <summary>Iterates <paramref name="lease"/>'s reader; the lease is disposed with the iterator.</summary>
    public TableInternalIterator(TableReaderLease lease) : this(lease, TableReadFlags.None) { }

    /// <summary>Iterates <paramref name="lease"/>'s reader with block-cache <paramref name="flags"/> (e.g. no cache fill for compaction).</summary>
    public TableInternalIterator(TableReaderLease lease, TableReadFlags flags)
    {
        if (lease.Reader is null) throw new ArgumentException("lease has no reader", nameof(lease));
        _lease = lease;
        Flags = flags;
        _it = lease.Reader.NewIterator(flags);
    }

    /// <summary>
    /// Iterates <paramref name="lease"/>'s reader in prefix seek mode as configured by <paramref name="readOptions"/>
    /// and the column family's <paramref name="prefixExtractor"/> (see <see cref="BlockBasedTableReader.GetPrefixOptions"/>).
    /// </summary>
    public TableInternalIterator(TableReaderLease lease, TableReadFlags flags, ReadOptions? readOptions, SliceTransform? prefixExtractor)
    {
        if (lease.Reader is null) throw new ArgumentException("lease has no reader", nameof(lease));
        _lease = lease;
        Flags = flags;
        try
        {
            _it = lease.Reader.NewIterator(flags, lease.Reader.GetPrefixOptions(readOptions, prefixExtractor));
        }
        catch
        {
            lease.Dispose();
            throw;
        }
    }


    /// <summary>Iterates <paramref name="reader"/> without owning it.</summary>
    public TableInternalIterator(BlockBasedTableReader reader) : this(new TableReaderLease(reader, null, null)) { }

    public BlockBasedTableReader Reader => _lease.Reader;

    public override bool Valid => _it.Valid;
    public override ReadOnlySpan<byte> Key => _it.Key;
    public override ReadOnlySpan<byte> Value => _it.Value;
    public override void SeekToFirst() => _it.SeekToFirst();
    public override void SeekToLast() => _it.SeekToLast();
    public override void Seek(ReadOnlySpan<byte> target) => _it.Seek(target);
    public override void SeekForPrev(ReadOnlySpan<byte> target) => _it.SeekForPrev(target);
    public override void Next() => _it.Next();
    public override void Prev() => _it.Prev();

    public override ValueTask SeekToFirstAsync(CancellationToken cancellationToken = default) => _it.SeekToFirstAsync(cancellationToken);
    public override ValueTask SeekAsync(ReadOnlyMemory<byte> target, CancellationToken cancellationToken = default) => _it.SeekAsync(target, cancellationToken);
    public override ValueTask NextAsync(CancellationToken cancellationToken = default) => _it.NextAsync(cancellationToken);

    /// <summary>The block-cache flags this iterator reads with.</summary>
    internal TableReadFlags Flags { get; private set; }

    /// <summary>
    /// Re-points this iterator at <paramref name="lease"/>'s table (same flags; prefix options recomputed from
    /// <paramref name="readOptions"/> / <paramref name="prefixExtractor"/> when given) and releases the previous lease.
    /// Lets <see cref="LevelIterator"/> move between the files of a level without allocating a new iterator per file.
    /// </summary>
    internal void Retarget(TableReaderLease lease, ReadOptions? readOptions, SliceTransform? prefixExtractor)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        TableReaderLease old = _lease;
        try
        {
            TableIteratorPrefixOptions prefix = prefixExtractor is null && readOptions is null
                ? default
                : lease.Reader.GetPrefixOptions(readOptions, prefixExtractor);
            _it.Retarget(lease.Reader, prefix);
        }
        catch
        {
            lease.Dispose();
            throw;
        }
        _lease = lease;
        old.Dispose();
    }

    /// <summary>
    /// Releases the blocks and the reader lease but keeps the iterator objects for a later <see cref="Retarget"/>
    /// (invalid until then).
    /// </summary>
    internal void Park()
    {
        if (_disposed) return;
        _it.Detach();
        TableReaderLease l = _lease;
        _lease = default;
        l.Dispose();
    }

    protected override void Dispose(bool disposing)
    {
        if (_disposed) return;
        _disposed = true;
        _it.Dispose();
        TableReaderLease l = _lease;
        _lease = default;
        l.Dispose();
    }
}
