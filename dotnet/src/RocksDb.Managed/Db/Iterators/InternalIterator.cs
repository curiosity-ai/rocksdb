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

    /// <summary>
    /// Port of InternalIterator::UpperBoundCheckResult (table/internal_iterator.h): if the iterator has checked the
    /// current key against iterate_upper_bound, the result, so that its user can skip its own check. While
    /// <see cref="Valid"/>, <see cref="IterBoundCheck.Unknown"/> is always a correct answer; when not valid,
    /// <see cref="IterBoundCheck.OutOfBound"/> says the iterator stopped at the upper bound (rather than at its end).
    /// </summary>
    public virtual IterBoundCheck UpperBoundCheckResult => IterBoundCheck.Unknown;

    /// <summary>
    /// Re-binds ReadOptions::iterate_upper_bound for iterators that check it themselves (block-based table iterators,
    /// and the composites holding them), after the DB iterator's bounds were replaced. Upstream table iterators read the
    /// bound through the DB iterator's ReadOptions, so a new bound reaches them at the next positioning call; this is
    /// the same. Default: the iterator does not check the bound.
    /// </summary>
    internal virtual void SetIterateUpperBound(ReadOnlyMemory<byte>? upperBound) { }

    /// <summary>
    /// Port of InternalIterator::SetPinnedItersMgr: iterators that hold data blocks hand the blocks they leave to
    /// <paramref name="manager"/> while it <see cref="PinnedIteratorsManager.IsPinning"/>; composite iterators pass it
    /// to their children. Default: nothing to pin.
    /// </summary>
    public virtual void SetPinnedItersMgr(PinnedIteratorsManager? manager) { }

    /// <summary>
    /// Port of IsValuePinned + value(): when the current value stays valid until the pinned iterators manager releases
    /// its data (or for the memtable, while the memtable is alive), returns it as a storable slice. False (the caller
    /// copies the value) when the value is not pinned. REQUIRES: <see cref="Valid"/>.
    /// </summary>
    public virtual bool TryGetPinnedValue(out PinnedSlice value)
    {
        value = default;
        return false;
    }

    /// <summary>
    /// Range-deletion skipping (the role of upstream's MergingIterator::SkipNextDeleted reseek). PRE: the current entry
    /// is covered by a range tombstone with seqno <paramref name="tombstoneSeq"/> that covers every entry in
    /// [current, <paramref name="target"/>) whose seqno is below <paramref name="tombstoneSeq"/>. The iterator may move
    /// to the first entry &gt;= <paramref name="target"/> when it can tell that every entry it skips that way has a
    /// smaller seqno (an SST file's largest seqno, or every file of a level up to the target); for a merging iterator,
    /// only the child providing the current entry moves. Returns false (and does not move) otherwise: the caller then
    /// steps with <see cref="Next"/>. Default: false.
    /// </summary>
    public virtual bool SkipCoveredForward(ReadOnlySpan<byte> target, ulong tombstoneSeq) => false;

    /// <summary>
    /// Backward counterpart of <see cref="SkipCoveredForward"/>: may move to the last entry &lt;= <paramref name="target"/>
    /// (SeekForPrev) when every skipped entry in (target, current] has a seqno below <paramref name="tombstoneSeq"/>.
    /// </summary>
    public virtual bool SkipCoveredBackward(ReadOnlySpan<byte> target, ulong tombstoneSeq) => false;

    /// <summary>Async <see cref="SkipCoveredForward"/>; completes synchronously unless a block must be read.</summary>
    public virtual ValueTask<bool> SkipCoveredForwardAsync(ReadOnlyMemory<byte> target, ulong tombstoneSeq, CancellationToken cancellationToken = default) =>
        new(SkipCoveredForward(target.Span, tombstoneSeq));

    /// <summary>Async <see cref="SkipCoveredBackward"/>; completes synchronously unless a block must be read.</summary>
    public virtual ValueTask<bool> SkipCoveredBackwardAsync(ReadOnlyMemory<byte> target, ulong tombstoneSeq, CancellationToken cancellationToken = default) =>
        new(SkipCoveredBackward(target.Span, tombstoneSeq));

    protected virtual void Dispose(bool disposing) { }
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }
}

/// <summary>Port of IterBoundCheck (table/internal_iterator.h).</summary>
public enum IterBoundCheck : byte
{
    /// <summary>kUnknown: the iterator did not check the key against the upper bound.</summary>
    Unknown,
    /// <summary>kOutOfBound: the iterator stopped at the upper bound (it is not valid).</summary>
    OutOfBound,
    /// <summary>kInbound: the current key is below the upper bound.</summary>
    Inbound,
}
