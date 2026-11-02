namespace RocksDb.Managed.Db.Iterators;

/// <summary>
/// An iterator with no entries. Port of EmptyInternalIterator (table/iterator.cc, NewEmptyInternalIterator).
/// Stateless: <see cref="Instance"/> can be shared; disposing it is a no-op.
/// </summary>
public sealed class EmptyInternalIterator : InternalIterator
{
    public static EmptyInternalIterator Instance { get; } = new();

    public override bool Valid => false;
    public override ReadOnlySpan<byte> Key => ReadOnlySpan<byte>.Empty;
    public override ReadOnlySpan<byte> Value => ReadOnlySpan<byte>.Empty;
    public override void SeekToFirst() { }
    public override void SeekToLast() { }
    public override void Seek(ReadOnlySpan<byte> target) { }
    public override void SeekForPrev(ReadOnlySpan<byte> target) { }
    public override void Next() => ThrowHelper.InvalidArgument("Next on an invalid iterator");
    public override void Prev() => ThrowHelper.InvalidArgument("Prev on an invalid iterator");
    public override bool MayBeOutOfUpperBound => false;
}

/// <summary>
/// An iterator that fails every positioning call with a fixed error. Port of the error flavour of
/// NewErrorInternalIterator (table/iterator.cc): upstream reports the status via status(); the managed contract
/// throws from positioning calls instead.
/// </summary>
public sealed class ErrorInternalIterator : InternalIterator
{
    private readonly StatusCode _code;
    private readonly string _message;

    public ErrorInternalIterator(StatusCode code, string message)
    {
        _code = code;
        _message = message;
    }

    public override bool Valid => false;
    public override ReadOnlySpan<byte> Key => ReadOnlySpan<byte>.Empty;
    public override ReadOnlySpan<byte> Value => ReadOnlySpan<byte>.Empty;
    private void Fail() => throw new RocksDbException(_code, _message);
    public override void SeekToFirst() => Fail();
    public override void SeekToLast() => Fail();
    public override void Seek(ReadOnlySpan<byte> target) => Fail();
    public override void SeekForPrev(ReadOnlySpan<byte> target) => Fail();
    public override void Next() => Fail();
    public override void Prev() => Fail();
}
