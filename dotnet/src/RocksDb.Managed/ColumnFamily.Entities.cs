using System.Runtime.CompilerServices;
using RocksDb.Managed.Db;
using RocksDb.Managed.Db.Wide;
using RocksDb.Managed.Util;

namespace RocksDb.Managed;

/// <summary>Wide-column entities (DB::PutEntity / GetEntity, include/rocksdb/db.h, db/wide/*).</summary>
public sealed partial class ColumnFamily
{
    /// <summary>
    /// Stores a wide-column entity for <paramref name="key"/> (DB::PutEntity), replacing any previous value. Columns may be
    /// given in any order; names must be unique. A plain Get of the key returns the default (empty-named) column.
    /// </summary>
    public void PutEntity(ReadOnlySpan<byte> key, ReadOnlySpan<WideColumn> columns) => PutEntity(null, key, columns);

    public void PutEntity(WriteOptions? options, ReadOnlySpan<byte> key, ReadOnlySpan<WideColumn> columns)
    {
        WriteBatch b = WriteBatch.Rent();
        try
        {
            b.PutEntity(_cfd.Id, key, columns);
            _db.WriteAsync(options, b, validateColumnFamilies: false, synchronous: true, CancellationToken.None).GetAwaiter().GetResult();
        }
        finally
        {
            b.Dispose();
        }
    }

    /// <summary>Async <see cref="PutEntity(ReadOnlySpan{byte}, ReadOnlySpan{WideColumn})"/> (group commit).</summary>
    public ValueTask PutEntityAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<WideColumn> columns, CancellationToken cancellationToken = default) =>
        PutEntityAsync(null, key, columns, cancellationToken);

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask PutEntityAsync(WriteOptions? options, ReadOnlyMemory<byte> key, ReadOnlyMemory<WideColumn> columns,
        CancellationToken cancellationToken = default)
    {
        WriteBatch b = WriteBatch.Rent();
        try
        {
            b.PutEntity(_cfd.Id, key.Span, columns.Span);
            await _db.WriteAsync(options, b, validateColumnFamilies: false, synchronous: false, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            b.Dispose();
        }
    }

    /// <summary>
    /// Reads the entity of <paramref name="key"/> into <paramref name="columns"/> (DB::GetEntity). A key written with a
    /// plain Put reads as an entity with only the default column; merge operands apply to the default column. Returns
    /// false if the key does not exist.
    /// </summary>
    public bool TryGetEntity(ReadOnlySpan<byte> key, WideColumns columns) => TryGetEntity(null, key, columns);

    public bool TryGetEntity(ReadOptions? options, ReadOnlySpan<byte> key, WideColumns columns)
    {
        ArgumentNullException.ThrowIfNull(columns);
        GetContext ctx = Lookup(options, key);
        PooledBufferWriter entity = ObjectPools.RentWriter();
        try
        {
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
            ObjectPools.ReturnWriter(entity);
            GetContext.Return(ctx);
        }
    }

    /// <summary>The entity of <paramref name="key"/>, or null (allocates; see <see cref="TryGetEntity(ReadOnlySpan{byte}, WideColumns)"/>).</summary>
    public WideColumns? GetEntity(ReadOnlySpan<byte> key) => GetEntity(null, key);

    public WideColumns? GetEntity(ReadOptions? options, ReadOnlySpan<byte> key)
    {
        var c = new WideColumns();
        return TryGetEntity(options, key, c) ? c : null;
    }

    /// <summary>Async <see cref="TryGetEntity(ReadOptions?, ReadOnlySpan{byte}, WideColumns)"/>; table blocks are read with async I/O.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<bool> TryGetEntityAsync(ReadOptions? options, ReadOnlyMemory<byte> key, WideColumns columns, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(columns);
        GetContext ctx = await LookupAsync(options, key, cancellationToken).ConfigureAwait(false);
        try
        {
            return ResolveEntity(ctx, key.Span, columns);
        }
        finally
        {
            GetContext.Return(ctx);
        }
    }

    /// <summary>Async <see cref="GetEntity(ReadOnlySpan{byte})"/>.</summary>
    public async ValueTask<WideColumns?> GetEntityAsync(ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default)
    {
        var c = new WideColumns();
        return await TryGetEntityAsync(null, key, c, cancellationToken).ConfigureAwait(false) ? c : null;
    }

    private static bool ResolveEntity(GetContext ctx, ReadOnlySpan<byte> key, WideColumns columns)
    {
        PooledBufferWriter entity = ObjectPools.RentWriter();
        try
        {
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
            ObjectPools.ReturnWriter(entity);
        }
    }
}

public sealed partial class RocksDatabase
{
    /// <summary><see cref="ColumnFamily.PutEntity(ReadOnlySpan{byte}, ReadOnlySpan{WideColumn})"/> on the default column family.</summary>
    public void PutEntity(ReadOnlySpan<byte> key, ReadOnlySpan<WideColumn> columns) => _default.PutEntity(key, columns);
    public void PutEntity(WriteOptions? options, ReadOnlySpan<byte> key, ReadOnlySpan<WideColumn> columns) => _default.PutEntity(options, key, columns);
    public ValueTask PutEntityAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<WideColumn> columns, CancellationToken cancellationToken = default) =>
        _default.PutEntityAsync(key, columns, cancellationToken);
    public bool TryGetEntity(ReadOnlySpan<byte> key, WideColumns columns) => _default.TryGetEntity(key, columns);
    public bool TryGetEntity(ReadOptions? options, ReadOnlySpan<byte> key, WideColumns columns) => _default.TryGetEntity(options, key, columns);
    public WideColumns? GetEntity(ReadOnlySpan<byte> key) => _default.GetEntity(key);
    public ValueTask<WideColumns?> GetEntityAsync(ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default) =>
        _default.GetEntityAsync(key, cancellationToken);
}
