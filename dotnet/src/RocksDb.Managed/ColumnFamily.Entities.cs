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
        if (_cfd.TimestampSize != 0) ThrowCfHasTimestamp(); // DBImpl::PutEntity: FailIfCfHasTs
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
        if (_cfd.TimestampSize != 0) ThrowCfHasTimestamp(); // DBImpl::PutEntity: FailIfCfHasTs
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
        GetContext ctx = Lookup(options, key, defaultColumnOnly: false);
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
        GetContext ctx = await LookupAsync(options, key, defaultColumnOnly: false, cancellationToken).ConfigureAwait(false);
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

    /// <summary>
    /// Looks up the entities of all keys of <paramref name="batch"/> from one consistent view (DB::MultiGetEntity for one
    /// column family; batched like <see cref="MultiGet(MultiGetBatch)"/>). Read the results with
    /// <see cref="MultiGetBatch.TryGetEntity"/> (columns) or <see cref="MultiGetBatch.GetValue"/> (default column). A
    /// plain value reads as an entity with only the default column. Does not allocate in steady state.
    /// </summary>
    public void MultiGetEntity(MultiGetBatch batch) => MultiGetEntity(null, batch);

    /// <inheritdoc cref="MultiGetEntity(MultiGetBatch)"/>
    public void MultiGetEntity(ReadOptions? options, MultiGetBatch batch)
    {
        ArgumentNullException.ThrowIfNull(batch);
        _db.ThrowIfDisposed();
        batch.EntityRun = true;
        if (batch.Count == 0) return;
        ValueTask t = _db.MultiGetAsync(_cfd, options, batch, synchronous: true, CancellationToken.None);
        t.GetAwaiter().GetResult();
    }

    /// <summary>Async <see cref="MultiGetEntity(ReadOptions?, MultiGetBatch)"/>: data blocks missing from the cache are read with async I/O.</summary>
    public ValueTask MultiGetEntityAsync(ReadOptions? options, MultiGetBatch batch, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(batch);
        _db.ThrowIfDisposed();
        batch.EntityRun = true;
        if (batch.Count == 0) return ValueTask.CompletedTask;
        return _db.MultiGetAsync(_cfd, options, batch, synchronous: false, cancellationToken);
    }

    /// <summary>
    /// EXPERIMENTAL. Lazy variant of <see cref="TryGetEntity(ReadOptions?, ReadOnlySpan{byte}, WideColumns)"/>
    /// (DB::GetEntityLazy): fills <paramref name="result"/> with the entity of <paramref name="key"/>, leaving its
    /// blob-backed columns as references that are read only when pulled (see <see cref="LazyWideColumns"/>). The result
    /// references the column family's super version until it is reset or disposed. Returns false (result emptied) if the
    /// key does not exist.
    /// </summary>
    public bool TryGetEntityLazy(ReadOptions? options, ReadOnlySpan<byte> key, LazyWideColumns result)
    {
        ArgumentNullException.ThrowIfNull(result);
        result.Reset();
        _db.ThrowIfDisposed();
        SuperVersion sv = _db.BeginLazyRead(_cfd, options, out ulong snapshot);
        LazyReadPin pin = result.RentPin(_db, sv);
        GetContext ctx = GetContext.Rent(_cfd.MergeOperator);
        try
        {
            _db.GetEntityLazy(_cfd, sv, snapshot, options, key, ctx);
            return result.Fill(ctx, key, _cfd, options, _db.Stats, pin, ownsPin: true);
        }
        catch
        {
            result.Reset();
            pin.Release();
            throw;
        }
        finally
        {
            GetContext.Return(ctx);
        }
    }

    /// <summary>The lazy entity of <paramref name="key"/> (dispose it), or null (allocates; see <see cref="TryGetEntityLazy"/>).</summary>
    public LazyWideColumns? GetEntityLazy(ReadOptions? options, ReadOnlySpan<byte> key)
    {
        var r = new LazyWideColumns();
        if (TryGetEntityLazy(options, key, r)) return r;
        r.Dispose();
        return null;
    }

    /// <summary>
    /// EXPERIMENTAL. Lazy, batched peer of <see cref="MultiGetEntity(ReadOptions?, MultiGetBatch)"/>
    /// (DB::MultiGetEntityLazy): looks up every key of <paramref name="keys"/> (only its keys are used) on one super
    /// version and sequence number and stores one <see cref="LazyWideColumns"/> per key in <paramref name="result"/>, with
    /// a per-key status. The batch keeps the super version referenced until it is reset or disposed.
    /// </summary>
    public void MultiGetEntityLazy(ReadOptions? options, MultiGetBatch keys, LazyWideColumnsBatch result)
    {
        ArgumentNullException.ThrowIfNull(keys);
        ArgumentNullException.ThrowIfNull(result);
        result.Reset();
        _db.ThrowIfDisposed();
        if (keys.Count == 0) return;
        result.Begin(keys.Count);
        SuperVersion sv = _db.BeginLazyRead(_cfd, options, out ulong snapshot);
        LazyReadPin pin = result.SetPin(_db, sv);
        GetContext ctx = GetContext.Rent(_cfd.MergeOperator);
        try
        {
            for (int i = 0; i < keys.Count; i++)
            {
                ReadOnlySpan<byte> key = keys.GetKey(i);
                try
                {
                    ctx.Reset(_cfd.MergeOperator);
                    _db.GetEntityLazy(_cfd, sv, snapshot, options, key, ctx);
                    bool found = result.Entry(i).Fill(ctx, key, _cfd, options, _db.Stats, pin, ownsPin: false);
                    result.SetStatus(i, found ? MultiGetStatus.Found : MultiGetStatus.NotFound, null);
                }
                catch (RocksDbException e)
                {
                    result.Entry(i).Reset();
                    result.SetStatus(i, MultiGetStatus.Error, e);
                }
            }
        }
        catch
        {
            result.Reset();
            throw;
        }
        finally
        {
            GetContext.Return(ctx);
        }
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
    /// <summary><see cref="ColumnFamily.MultiGetEntity(MultiGetBatch)"/> on the default column family.</summary>
    public void MultiGetEntity(MultiGetBatch batch) => _default.MultiGetEntity(batch);
    public void MultiGetEntity(ReadOptions? options, MultiGetBatch batch) => _default.MultiGetEntity(options, batch);
    public ValueTask MultiGetEntityAsync(ReadOptions? options, MultiGetBatch batch, CancellationToken cancellationToken = default) =>
        _default.MultiGetEntityAsync(options, batch, cancellationToken);
}
