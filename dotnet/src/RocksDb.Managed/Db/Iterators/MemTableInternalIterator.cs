namespace RocksDb.Managed.Db.Iterators;

/// <summary>
/// <see cref="InternalIterator"/> over the point entries of a <see cref="MemTable"/>. Port of MemTableIterator
/// (db/memtable.cc) as seen through MemTable::NewIterator. Allocation-free per step: keys and values are spans into
/// arena memory, which stays valid while the memtable is alive.
/// </summary>
/// <remarks>
/// The iterator does not reference-count the memtable: the owner of the iterator stack (the DB's super version)
/// keeps the memtable alive until the iterator is disposed, like upstream. Reusable via <see cref="Reset"/>.
/// </remarks>
public sealed class MemTableInternalIterator : InternalIterator
{
    private MemTableIterator _it;
    private MemTable? _memtable;

    public MemTableInternalIterator(MemTable memtable)
    {
        ArgumentNullException.ThrowIfNull(memtable);
        _memtable = memtable;
        _it = memtable.NewIterator();
    }

    /// <summary>
    /// Iterator that consults the memtable prefix bloom on Seek / SeekForPrev when the read options select prefix mode
    /// and <paramref name="cfPrefixExtractor"/> is the memtable's extractor (MemTable::NewIterator).
    /// </summary>
    public MemTableInternalIterator(MemTable memtable, ReadOptions? readOptions, SliceTransform? cfPrefixExtractor)
    {
        ArgumentNullException.ThrowIfNull(memtable);
        _memtable = memtable;
        _it = memtable.NewIterator(memtable.UsesPrefixBloom(readOptions, cfPrefixExtractor));
    }

    /// <summary>Re-targets this iterator at another memtable (unpositioned).</summary>
    public void Reset(MemTable memtable)
    {
        ArgumentNullException.ThrowIfNull(memtable);
        _memtable = memtable;
        _it = memtable.NewIterator();
    }

    public MemTable? MemTable => _memtable;

    public override bool Valid => _it.Valid;
    public override ReadOnlySpan<byte> Key => _it.Key;
    public override ReadOnlySpan<byte> Value => _it.Value;
    public override void SeekToFirst() => _it.SeekToFirst();
    public override void SeekToLast() => _it.SeekToLast();
    public override void Seek(ReadOnlySpan<byte> target) => _it.Seek(target);
    public override void SeekForPrev(ReadOnlySpan<byte> target) => _it.SeekForPrev(target);
    public override void Next() => _it.Next();
    public override void Prev() => _it.Prev();

    /// <summary>Memtable values live in arena memory at a fixed address while the memtable is alive (MemTableIterator::IsValuePinned).</summary>
    public override bool TryGetPinnedValue(out PinnedSlice value)
    {
        value = PinnedSlice.FromFixed(_it.Value);
        return true;
    }

    protected override void Dispose(bool disposing) => _memtable = null;
}
