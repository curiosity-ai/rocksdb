using RocksDb.Managed.Util;

namespace RocksDb.Managed.Db;

/// <summary>Transaction reads that need the DB internals (DBImpl::GetImpl with columns and a read callback).</summary>
internal sealed partial class DbImpl
{
    /// <summary>
    /// DBImpl::GetImpl (GetImplOptions::columns + callback) for WritePrepared / WriteUnprepared transactions: the
    /// resolved entity of <paramref name="key"/> (a plain value as a single default column) into
    /// <paramref name="columns"/>; returns false (columns emptied) if the key does not exist.
    /// </summary>
    internal bool GetEntityWithCallback(ColumnFamilyData cfd, ReadOptions? options, ReadOnlySpan<byte> key, ReadCallback callback, WideColumns columns)
    {
        ThrowIfDisposed();
        GetContext ctx = GetContext.Rent(cfd.MergeOperator);
        PooledBufferWriter entity = ObjectPools.RentWriter();
        try
        {
            ctx.Callback = callback;
            Get(cfd, options, key, ctx);
            if (!ctx.TryResolveEntity(key, entity))
            {
                columns.Clear();
                return false;
            }
            columns.SetSerialized(entity.WrittenSpan);
            return true;
        }
        finally
        {
            ctx.Callback = null;
            ObjectPools.ReturnWriter(entity);
            GetContext.Return(ctx);
        }
    }
}
