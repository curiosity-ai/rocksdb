namespace RocksDb.Managed.Db.Iterators;

/// <summary>
/// CONTRACT (phase 2). Iterator over internal keys (user key + 8-byte seq/type footer) in
/// <see cref="InternalKeyComparator"/> order. Port of rocksdb::InternalIterator (table/internal_iterator.h).
/// Implemented by memtable/table/level/merging iterators; consumed by DBIter, flush and compaction.
/// <para>
/// <see cref="Key"/>/<see cref="Value"/> spans are valid until the next positioning call. Errors (corruption, I/O)
/// throw <see cref="RocksDbException"/> from the positioning call. The async methods default to the sync ones;
/// implementations that may do I/O (table/level iterators) override them so a block-cache miss awaits instead of
/// blocking, and complete synchronously on cache hits.
/// </para>
/// </summary>
public abstract class InternalIterator : IDisposable
{
    public abstract bool Valid { get; }
    public abstract ReadOnlySpan<byte> Key { get; }
    public abstract ReadOnlySpan<byte> Value { get; }

    public abstract void SeekToFirst();
    public abstract void SeekToLast();
    /// <summary>Position at the first entry with key &gt;= target (internal key).</summary>
    public abstract void Seek(ReadOnlySpan<byte> target);
    /// <summary>Position at the last entry with key &lt;= target (internal key).</summary>
    public abstract void SeekForPrev(ReadOnlySpan<byte> target);
    public abstract void Next();
    public abstract void Prev();

    public virtual ValueTask SeekToFirstAsync(CancellationToken cancellationToken = default) { SeekToFirst(); return default; }
    public virtual ValueTask SeekToLastAsync(CancellationToken cancellationToken = default) { SeekToLast(); return default; }
    public virtual ValueTask SeekAsync(ReadOnlyMemory<byte> target, CancellationToken cancellationToken = default) { Seek(target.Span); return default; }
    public virtual ValueTask SeekForPrevAsync(ReadOnlyMemory<byte> target, CancellationToken cancellationToken = default) { SeekForPrev(target.Span); return default; }
    public virtual ValueTask NextAsync(CancellationToken cancellationToken = default) { Next(); return default; }
    public virtual ValueTask PrevAsync(CancellationToken cancellationToken = default) { Prev(); return default; }

    /// <summary>Upper-level hint for iterate_upper_bound checks (port of IsKeyPinned/UpperBoundCheckResult is optional).</summary>
    public virtual bool MayBeOutOfUpperBound => true;

    protected virtual void Dispose(bool disposing) { }
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }
}
