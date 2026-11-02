namespace RocksDb.Managed.Db;

internal sealed partial class DbImpl
{
    /// <summary>
    /// The lookup of DB::GetEntityLazy / MultiGetEntityLazy (DBImpl::GetEntityLazyImpl / GetEntityLazyForBatch, the
    /// lazy_columns_* options of GetImpl): a point lookup on the referenced super version <paramref name="sv"/> at
    /// <paramref name="snapshot"/> that leaves the blob columns of a found version 2 entity unresolved
    /// (<see cref="GetContext.DeferEntityBlobColumns"/>). Entities that merge operands apply to and plain blob values
    /// are resolved as usual.
    /// </summary>
    internal void GetEntityLazy(ColumnFamilyData cfd, SuperVersion sv, ulong snapshot, ReadOptions? options, ReadOnlySpan<byte> key, GetContext ctx)
    {
        if (_txnHooks is not null)
        {
            // Upstream has no transaction counterpart of the lazy read APIs.
            throw new RocksDbException(StatusCode.NotSupported, "GetEntityLazy is not supported on WritePrepared / WriteUnprepared transaction DBs");
        }
        ctx.DeferEntityBlobColumns = true;
        GetWithSuperVersion(cfd, sv, snapshot, options, key, ctx);
    }

    /// <summary>
    /// References the super version and takes the read sequence of a lazy read (GetImpl order: super version first). The
    /// caller releases the reference with <see cref="ReturnSuperVersion"/>.
    /// </summary>
    internal SuperVersion BeginLazyRead(ColumnFamilyData cfd, ReadOptions? options, out ulong snapshot)
    {
        ValidateReadSnapshot(options);
        SuperVersion sv = GetReferencedSuperVersion(cfd);
        snapshot = ReadSequence(options);
        return sv;
    }
}
