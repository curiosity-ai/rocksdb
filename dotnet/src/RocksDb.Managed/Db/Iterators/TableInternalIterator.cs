using System.Runtime.CompilerServices;
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
        _it = Bbt(lease).NewIterator(flags);
        _embedded = Bbt(lease).HasEmbeddedBlobRecords;
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
            BlockBasedTableReader r = Bbt(lease);
            _it = r.NewIterator(flags, r.GetPrefixOptions(readOptions, prefixExtractor));
            _it.SetUpperBound(readOptions?.IterateUpperBound);
            _embedded = r.HasEmbeddedBlobRecords;
        }
        catch
        {
            lease.Dispose();
            throw;
        }
    }


    /// <summary>Iterates <paramref name="reader"/> without owning it.</summary>
    public TableInternalIterator(BlockBasedTableReader reader) : this(new TableReaderLease(reader, null, null)) { }

    public BlockBasedTableReader Reader => (BlockBasedTableReader)_lease.Reader;

    private static BlockBasedTableReader Bbt(TableReaderLease lease) =>
        lease.Reader as BlockBasedTableReader ?? throw new ArgumentException("TableInternalIterator iterates block-based tables only", nameof(lease));

    /// <summary>
    /// An iterator over <paramref name="lease"/>'s table that owns the lease: a <see cref="TableInternalIterator"/> for
    /// block-based tables, otherwise the format's own iterator (TableReader::NewIterator) wrapped to release the lease.
    /// </summary>
    public static InternalIterator Create(TableReaderLease lease, TableReadFlags flags, ReadOptions? readOptions, SliceTransform? prefixExtractor)
    {
        if (lease.Reader is BlockBasedTableReader)
        {
            return readOptions is null && prefixExtractor is null
                ? new TableInternalIterator(lease, flags)
                : new TableInternalIterator(lease, flags, readOptions, prefixExtractor);
        }
        try
        {
            return new LeasedTableIterator(lease.Reader.NewInternalIterator(flags, readOptions, prefixExtractor), lease);
        }
        catch
        {
            lease.Dispose();
            throw;
        }
    }

    /// <summary>
    /// The iterator over <paramref name="file"/> that a level iterator or an iterator graph uses: <paramref name="reuse"/>
    /// re-targeted when it is a table iterator with <paramref name="flags"/> (no allocation), else a new one
    /// (<paramref name="reuse"/> is disposed). Prefix options come from <paramref name="readOptions"/> only with a
    /// <paramref name="prefixExtractor"/>, like <see cref="DbIteratorFactory.NewInternalIterator"/>.
    /// </summary>
    internal static InternalIterator OpenOrRecycle(ITableCache tableCache, FileMetaData file, InternalIterator? reuse, TableReadFlags flags,
        ReadOptions? readOptions, SliceTransform? prefixExtractor)
    {
        if (prefixExtractor is null)
        {
            return flags == TableReadFlags.None
                ? tableCache.NewIterator(file, reuse)
                : TableCache.Recycle(reuse, tableCache.GetReader(file), flags);
        }
        TableReaderLease lease = tableCache.GetReader(file);
        if (reuse is TableInternalIterator t && t.Flags == flags && lease.Reader is BlockBasedTableReader)
        {
            t.Retarget(lease, readOptions, prefixExtractor);
            return t;
        }
        reuse?.Dispose();
        return Create(lease, flags, readOptions, prefixExtractor);
    }

    public override bool Valid { [MethodImpl(MethodImplOptions.AggressiveOptimization)] get => _it.Valid; }
    public override ReadOnlySpan<byte> Key { [MethodImpl(MethodImplOptions.AggressiveOptimization)] get => _embedded ? EmbeddedKey() : _it.Key; }
    public override ReadOnlySpan<byte> Value { [MethodImpl(MethodImplOptions.AggressiveOptimization)] get => _embedded ? EmbeddedValue() : _it.Value; }
    public override void SeekToFirst()
    {
        _stale = true;
        _it.SeekToFirst();
    }

    public override void SeekToLast()
    {
        _stale = true;
        _it.SeekToLast();
    }

    public override void Seek(ReadOnlySpan<byte> target)
    {
        _stale = true;
        _it.Seek(target);
    }

    public override void SeekForPrev(ReadOnlySpan<byte> target)
    {
        _stale = true;
        _it.SeekForPrev(target);
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public override void Next()
    {
        _stale = true;
        _it.Next();
    }

    public override void Prev()
    {
        _stale = true;
        _it.Prev();
    }

    public override void SetPinnedItersMgr(PinnedIteratorsManager? manager) => _it.SetPinnedItersMgr(manager);
    public override IterBoundCheck UpperBoundCheckResult => _it.UpperBoundCheckResult;
    internal override void SetIterateUpperBound(ReadOnlyMemory<byte>? upperBound) => _it.SetUpperBound(upperBound);
    public override bool TryGetPinnedValue(out PinnedSlice value)
    {
        if (_embedded && ResolveEmbedded())
        {
            value = default;
            return false;
        }
        return _it.TryGetPinnedValue(out value);
    }

    public override ValueTask SeekToFirstAsync(CancellationToken cancellationToken = default)
    {
        _stale = true;
        return _it.SeekToFirstAsync(cancellationToken);
    }

    public override ValueTask SeekAsync(ReadOnlyMemory<byte> target, CancellationToken cancellationToken = default)
    {
        _stale = true;
        return _it.SeekAsync(target, cancellationToken);
    }

    public override ValueTask NextAsync(CancellationToken cancellationToken = default)
    {
        _stale = true;
        return _it.NextAsync(cancellationToken);
    }

    public override ValueTask SeekToLastAsync(CancellationToken cancellationToken = default)
    {
        _stale = true;
        return _it.SeekToLastAsync(cancellationToken);
    }

    public override ValueTask SeekForPrevAsync(ReadOnlyMemory<byte> target, CancellationToken cancellationToken = default)
    {
        _stale = true;
        return _it.SeekForPrevAsync(target, cancellationToken);
    }

    public override ValueTask PrevAsync(CancellationToken cancellationToken = default)
    {
        _stale = true;
        return _it.PrevAsync(cancellationToken);
    }

    // ---- embedded-blob SSTs (EmbeddedBlobResolvingIterator, table/block_based/embedded_blob_resolving_iterator.h) ----
    // A table with same-file blob records exposes each blob index entry that references them as a kTypeValue entry
    // holding the payload (resolved on first access of the position's key or value).

    private bool _embedded;   // the table has embedded blob records
    private bool _stale = true; // the position moved since the last resolution
    private bool _useResolved;
    private byte[] _resolvedKey = [];
    private Util.PooledBufferWriter? _resolvedValue;

    [MethodImpl(MethodImplOptions.NoInlining)]
    private ReadOnlySpan<byte> EmbeddedKey() => ResolveEmbedded() ? _resolvedKey.AsSpan(0, _it.Key.Length) : _it.Key;

    [MethodImpl(MethodImplOptions.NoInlining)]
    private ReadOnlySpan<byte> EmbeddedValue() => ResolveEmbedded() ? _resolvedValue!.WrittenSpan : _it.Value;

    /// <summary>Whether the current entry is a resolved same-file blob reference (resolving it when the position moved).</summary>
    private bool ResolveEmbedded()
    {
        if (!_stale) return _useResolved;
        _stale = false;
        _useResolved = false;
        if (!_it.Valid) return false;
        ReadOnlySpan<byte> k = _it.Key;
        if (!BlockBasedTableReader.IsBlobIndexEntry(k)) return false;
        ReadOnlySpan<byte> v = _it.Value;
        if (!BlockBasedTableReader.IsSameFileBlobIndex(v)) return false;
        if (_resolvedKey.Length < k.Length) _resolvedKey = new byte[Math.Max(k.Length, 64)];
        Util.PooledBufferWriter w = _resolvedValue ??= new Util.PooledBufferWriter(256);
        w.Clear();
        ((BlockBasedTableReader)_lease.Reader).ResolveEmbeddedEntry(k, v, _resolvedKey, w, Flags);
        _useResolved = true;
        return true;
    }

    private ulong _smallestSeqno = DbFormat.MaxSequenceNumber;
    private ulong _largestSeqno;

    /// <summary>
    /// The seqno range of the file's entries (FileMetaData smallest / largest seqno); enables
    /// <see cref="SkipCoveredForward"/>. Unknown (no skipping) until set and after <see cref="Retarget"/>.
    /// </summary>
    internal void SetSeqnoRange(ulong smallest, ulong largest)
    {
        _smallestSeqno = smallest;
        _largestSeqno = largest;
    }

    /// <summary>Every entry of the file has a seqno below <paramref name="seq"/> (and the range is known).</summary>
    private bool AllSeqnosBelow(ulong seq) => _smallestSeqno <= _largestSeqno && _largestSeqno < seq;

    public override bool SkipCoveredForward(ReadOnlySpan<byte> target, ulong tombstoneSeq)
    {
        if (!AllSeqnosBelow(tombstoneSeq) || !_it.Valid || _lease.Reader.InternalComparator.Compare(target, _it.Key) <= 0) return false;
        _stale = true;
        _it.Seek(target);
        return true;
    }

    public override bool SkipCoveredBackward(ReadOnlySpan<byte> target, ulong tombstoneSeq)
    {
        if (!AllSeqnosBelow(tombstoneSeq) || !_it.Valid || _lease.Reader.InternalComparator.Compare(target, _it.Key) >= 0) return false;
        _stale = true;
        _it.SeekForPrev(target);
        return true;
    }

    public override ValueTask<bool> SkipCoveredForwardAsync(ReadOnlyMemory<byte> target, ulong tombstoneSeq, CancellationToken cancellationToken = default)
    {
        if (!AllSeqnosBelow(tombstoneSeq) || !_it.Valid || _lease.Reader.InternalComparator.Compare(target.Span, _it.Key) <= 0) return new(false);
        _stale = true;
        return Moved(_it.SeekAsync(target, cancellationToken));
    }

    public override ValueTask<bool> SkipCoveredBackwardAsync(ReadOnlyMemory<byte> target, ulong tombstoneSeq, CancellationToken cancellationToken = default)
    {
        if (!AllSeqnosBelow(tombstoneSeq) || !_it.Valid || _lease.Reader.InternalComparator.Compare(target.Span, _it.Key) >= 0) return new(false);
        _stale = true;
        return Moved(_it.SeekForPrevAsync(target, cancellationToken));
    }

    /// <summary>true once <paramref name="pending"/> completes.</summary>
    internal static ValueTask<bool> Moved(ValueTask pending)
    {
        if (!pending.IsCompletedSuccessfully) return MovedAsync(pending);
        pending.GetAwaiter().GetResult();
        return new(true);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private static async ValueTask<bool> MovedAsync(ValueTask pending)
    {
        await pending.ConfigureAwait(false);
        return true;
    }

    /// <summary>Reads uncached data blocks with sequential async readahead (see <see cref="BlockBasedTableIterator.EnableReadahead"/>).</summary>
    internal void EnableReadahead(int readaheadSize) => _it.EnableReadahead(readaheadSize);

    /// <summary>ReadOptions::readahead_size (see <see cref="BlockBasedTableIterator.SetUserReadahead"/>).</summary>
    internal void SetUserReadahead(ulong readaheadSize) => _it.SetUserReadahead(readaheadSize);

    /// <summary>The readahead buffer (tests / diagnostics), or null.</summary>
    internal Env.AsyncPrefetchBuffer? Readahead => _it.Readahead;

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
            BlockBasedTableReader r = Bbt(lease);
            TableIteratorPrefixOptions prefix = prefixExtractor is null && readOptions is null
                ? default
                : r.GetPrefixOptions(readOptions, prefixExtractor);
            _it.Retarget(r, prefix);
            _it.SetUpperBound(readOptions?.IterateUpperBound);
            _embedded = r.HasEmbeddedBlobRecords;
            _stale = true;
            _smallestSeqno = DbFormat.MaxSequenceNumber;
            _largestSeqno = 0;
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
        _resolvedValue?.Dispose();
        TableReaderLease l = _lease;
        _lease = default;
        l.Dispose();
    }
}

/// <summary>
/// The iterator of a table of a format other than block-based (plain, cuckoo) with the <see cref="TableReaderLease"/> it
/// was created from: disposing the iterator releases the lease (the reader stays pinned while iterating).
/// </summary>
internal sealed class LeasedTableIterator : InternalIterator
{
    private readonly InternalIterator _it;
    private TableReaderLease _lease;
    private bool _disposed;

    public LeasedTableIterator(InternalIterator it, TableReaderLease lease)
    {
        _it = it;
        _lease = lease;
    }

    public override bool Valid => _it.Valid;
    public override ReadOnlySpan<byte> Key => _it.Key;
    public override ReadOnlySpan<byte> Value => _it.Value;
    public override void SeekToFirst() => _it.SeekToFirst();
    public override void SeekToLast() => _it.SeekToLast();
    public override void Seek(ReadOnlySpan<byte> target) => _it.Seek(target);
    public override void SeekForPrev(ReadOnlySpan<byte> target) => _it.SeekForPrev(target);
    public override void Next() => _it.Next();
    public override void Prev() => _it.Prev();
    public override IterBoundCheck UpperBoundCheckResult => _it.UpperBoundCheckResult;

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
