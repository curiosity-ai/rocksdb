using RocksDb.Managed.Db.Iterators;

namespace RocksDb.Managed.Db;

/// <summary>
/// Skips the entries whose user-defined timestamp is newer than a trim timestamp. Port of HistoryTrimmingIterator
/// (db/history_trimming_iterator.h), the compaction input filter of DB::OpenAndTrimHistory. Owns and disposes the
/// wrapped iterator.
/// </summary>
internal sealed class HistoryTrimmingIterator : InternalIterator
{
    private readonly InternalIterator _input;
    private readonly byte[] _filterTs;
    private readonly Comparator _cmp;
    private readonly int _tsSize;

    public HistoryTrimmingIterator(InternalIterator input, Comparator cmp, byte[] ts)
    {
        if (cmp.TimestampSize == 0 || ts.Length == 0) throw new ArgumentException("history trimming needs user-defined timestamps", nameof(ts));
        _input = input;
        _filterTs = ts;
        _cmp = cmp;
        _tsSize = cmp.TimestampSize;
    }

    /// <summary>True if the current entry is kept (or the input is exhausted).</summary>
    private bool Filter()
    {
        if (!_input.Valid) return true;
        ReadOnlySpan<byte> userKey = DbFormat.ExtractUserKey(_input.Key);
        return _cmp.CompareTimestamp(userKey[^_tsSize..], _filterTs) <= 0;
    }

    public override bool Valid => _input.Valid;
    public override ReadOnlySpan<byte> Key => _input.Key;
    public override ReadOnlySpan<byte> Value => _input.Value;

    public override void SeekToFirst()
    {
        _input.SeekToFirst();
        while (!Filter()) _input.Next();
    }

    public override void SeekToLast()
    {
        _input.SeekToLast();
        while (!Filter()) _input.Prev();
    }

    public override void Seek(ReadOnlySpan<byte> target)
    {
        _input.Seek(target);
        while (!Filter()) _input.Next();
    }

    public override void SeekForPrev(ReadOnlySpan<byte> target)
    {
        _input.SeekForPrev(target);
        while (!Filter()) _input.Prev();
    }

    public override void Next()
    {
        do
        {
            _input.Next();
        } while (!Filter());
    }

    public override void Prev()
    {
        do
        {
            _input.Prev();
        } while (!Filter());
    }

    public override void SetPinnedItersMgr(PinnedIteratorsManager? manager) => _input.SetPinnedItersMgr(manager);

    protected override void Dispose(bool disposing)
    {
        if (disposing) _input.Dispose();
    }
}
