using RocksDb.Managed.Db.Iterators;

namespace RocksDb.Managed.Db;

/// <summary>
/// An iterator restricted to the internal key range [start, end) (null = unbounded). Port of ClippingIterator
/// (db/compaction/clipping_iterator.h), used to give each subcompaction its slice of the merged compaction input.
/// Owns and disposes the wrapped iterator.
/// </summary>
internal sealed class ClippingIterator : InternalIterator
{
    private readonly InternalIterator _iter;
    private readonly byte[]? _start;
    private readonly byte[]? _end;
    private readonly InternalKeyComparator _icmp;
    private bool _valid;

    public ClippingIterator(InternalIterator iter, byte[]? start, byte[]? end, InternalKeyComparator icmp)
    {
        _iter = iter;
        _start = start;
        _end = end;
        _icmp = icmp;
    }

    public override bool Valid => _valid;
    public override ReadOnlySpan<byte> Key => _iter.Key;
    public override ReadOnlySpan<byte> Value => _iter.Value;

    private void UpdateValid()
    {
        _valid = _iter.Valid && (_end is null || _icmp.Compare(_iter.Key, _end) < 0);
    }

    public override void SeekToFirst()
    {
        if (_start is not null) _iter.Seek(_start);
        else _iter.SeekToFirst();
        UpdateValid();
    }

    public override void SeekToLast()
    {
        if (_end is not null)
        {
            _iter.SeekForPrev(_end);
            // Upper bound is exclusive, so we need a key which is strictly smaller
            if (_iter.Valid && _icmp.Compare(_iter.Key, _end) == 0) _iter.Prev();
        }
        else
        {
            _iter.SeekToLast();
        }
        _valid = _iter.Valid && (_start is null || _icmp.Compare(_iter.Key, _start) >= 0);
    }

    public override void Seek(ReadOnlySpan<byte> target)
    {
        if (_start is not null && _icmp.Compare(target, _start) < 0) _iter.Seek(_start);
        else if (_end is not null && _icmp.Compare(target, _end) >= 0)
        {
            _valid = false;
            return;
        }
        else _iter.Seek(target);
        UpdateValid();
    }

    public override void SeekForPrev(ReadOnlySpan<byte> target)
    {
        if (_start is not null && _icmp.Compare(target, _start) < 0)
        {
            _valid = false;
            return;
        }
        if (_end is not null && _icmp.Compare(target, _end) >= 0)
        {
            SeekToLast();
            return;
        }
        _iter.SeekForPrev(target);
        _valid = _iter.Valid && (_start is null || _icmp.Compare(_iter.Key, _start) >= 0);
    }

    public override void Next()
    {
        _iter.Next();
        UpdateValid();
    }

    public override void Prev()
    {
        _iter.Prev();
        _valid = _iter.Valid && (_start is null || _icmp.Compare(_iter.Key, _start) >= 0);
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing) _iter.Dispose();
    }
}
