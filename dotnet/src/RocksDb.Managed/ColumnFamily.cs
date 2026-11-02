using System.Buffers;
using System.Runtime.CompilerServices;
using RocksDb.Managed.Db;
using RocksDb.Managed.Db.Iterators;

namespace RocksDb.Managed;

/// <summary>
/// A column family of an open <see cref="RocksDatabase"/>: the receiver of reads and writes for that column family
/// (rocksdb::ColumnFamilyHandle + the DB methods taking a handle). Obtain it from
/// <see cref="RocksDatabase.DefaultColumnFamily"/>, <see cref="RocksDatabase.GetColumnFamily"/> or
/// <see cref="RocksDatabase.CreateColumnFamilyAsync(string, ColumnFamilyOptions, CancellationToken)"/>. Thread-safe.
/// <para>
/// Synchronous write methods never block on thread-pool work: the calling thread waits for its turn in the write
/// group and performs the group's WAL I/O itself when it is the leader. Async methods return <see cref="ValueTask"/>
/// and are batched with concurrent writers (group commit).
/// </para>
/// </summary>
public sealed class ColumnFamily
{
    private readonly DbImpl _db;
    private readonly ColumnFamilyData _cfd;

    internal ColumnFamily(DbImpl db, ColumnFamilyData cfd)
    {
        _db = db;
        _cfd = cfd;
    }

    internal ColumnFamilyData Data => _cfd;
    internal DbImpl Db => _db;

    /// <summary>The column family name.</summary>
    public string Name => _cfd.Name;
    /// <summary>The column family id (as used in <see cref="WriteBatch"/> records).</summary>
    public uint Id => _cfd.Id;
    /// <summary>The options the column family was opened with.</summary>
    public ColumnFamilyOptions Options => _cfd.Options;
    /// <summary>True once the column family was dropped.</summary>
    public bool IsDropped => _cfd.Dropped;

    public override string ToString() => Name;

    // =====================================================================================================
    // Writes
    // =====================================================================================================

    private enum Op : byte { Put, Delete, SingleDelete, DeleteRange, Merge }

    private void WriteOne(WriteOptions? options, Op op, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        WriteBatch b = WriteBatch.Rent();
        try
        {
            Encode(b, op, key, value);
            ValueTask t = _db.WriteAsync(options, b, validateColumnFamilies: false, synchronous: true, CancellationToken.None);
            t.GetAwaiter().GetResult();
        }
        finally
        {
            b.Dispose();
        }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask WriteOneAsync(WriteOptions? options, Op op, ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, CancellationToken ct)
    {
        WriteBatch b = WriteBatch.Rent();
        try
        {
            Encode(b, op, key.Span, value.Span);
            await _db.WriteAsync(options, b, validateColumnFamilies: false, synchronous: false, ct).ConfigureAwait(false);
        }
        finally
        {
            b.Dispose();
        }
    }

    private void Encode(WriteBatch b, Op op, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        uint id = _cfd.Id;
        switch (op)
        {
            case Op.Put: b.Put(id, key, value); break;
            case Op.Delete: b.Delete(id, key); break;
            case Op.SingleDelete: b.SingleDelete(id, key); break;
            case Op.DeleteRange: b.DeleteRange(id, key, value); break;
            case Op.Merge: b.Merge(id, key, value); break;
        }
    }

    /// <summary>Sets <paramref name="key"/> to <paramref name="value"/>.</summary>
    public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => WriteOne(null, Op.Put, key, value);
    public void Put(WriteOptions options, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => WriteOne(options, Op.Put, key, value);
    public ValueTask PutAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default) =>
        WriteOneAsync(null, Op.Put, key, value, cancellationToken);
    public ValueTask PutAsync(WriteOptions options, ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default) =>
        WriteOneAsync(options, Op.Put, key, value, cancellationToken);

    /// <summary>Removes <paramref name="key"/> (a no-op if it doesn't exist).</summary>
    public void Delete(ReadOnlySpan<byte> key) => WriteOne(null, Op.Delete, key, default);
    public void Delete(WriteOptions options, ReadOnlySpan<byte> key) => WriteOne(options, Op.Delete, key, default);
    public ValueTask DeleteAsync(ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default) =>
        WriteOneAsync(null, Op.Delete, key, default, cancellationToken);
    public ValueTask DeleteAsync(WriteOptions options, ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default) =>
        WriteOneAsync(options, Op.Delete, key, default, cancellationToken);

    /// <summary>
    /// Removes <paramref name="key"/>, which must have been written at most once since its last deletion (see
    /// DB::SingleDelete); mixing with Put overwrites or Merge is undefined behavior.
    /// </summary>
    public void SingleDelete(ReadOnlySpan<byte> key) => WriteOne(null, Op.SingleDelete, key, default);
    public void SingleDelete(WriteOptions options, ReadOnlySpan<byte> key) => WriteOne(options, Op.SingleDelete, key, default);
    public ValueTask SingleDeleteAsync(ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default) =>
        WriteOneAsync(null, Op.SingleDelete, key, default, cancellationToken);
    public ValueTask SingleDeleteAsync(WriteOptions options, ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default) =>
        WriteOneAsync(options, Op.SingleDelete, key, default, cancellationToken);

    /// <summary>Removes all keys in [<paramref name="beginKey"/>, <paramref name="endKey"/>).</summary>
    public void DeleteRange(ReadOnlySpan<byte> beginKey, ReadOnlySpan<byte> endKey) => WriteOne(null, Op.DeleteRange, beginKey, endKey);
    public void DeleteRange(WriteOptions options, ReadOnlySpan<byte> beginKey, ReadOnlySpan<byte> endKey) => WriteOne(options, Op.DeleteRange, beginKey, endKey);
    public ValueTask DeleteRangeAsync(ReadOnlyMemory<byte> beginKey, ReadOnlyMemory<byte> endKey, CancellationToken cancellationToken = default) =>
        WriteOneAsync(null, Op.DeleteRange, beginKey, endKey, cancellationToken);
    public ValueTask DeleteRangeAsync(WriteOptions options, ReadOnlyMemory<byte> beginKey, ReadOnlyMemory<byte> endKey, CancellationToken cancellationToken = default) =>
        WriteOneAsync(options, Op.DeleteRange, beginKey, endKey, cancellationToken);

    /// <summary>Adds a merge operand for <paramref name="key"/> (requires <see cref="ColumnFamilyOptions.MergeOperator"/>).</summary>
    public void Merge(ReadOnlySpan<byte> key, ReadOnlySpan<byte> operand) => WriteOne(null, Op.Merge, key, operand);
    public void Merge(WriteOptions options, ReadOnlySpan<byte> key, ReadOnlySpan<byte> operand) => WriteOne(options, Op.Merge, key, operand);
    public ValueTask MergeAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> operand, CancellationToken cancellationToken = default) =>
        WriteOneAsync(null, Op.Merge, key, operand, cancellationToken);
    public ValueTask MergeAsync(WriteOptions options, ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> operand, CancellationToken cancellationToken = default) =>
        WriteOneAsync(options, Op.Merge, key, operand, cancellationToken);

    // =====================================================================================================
    // Reads
    // =====================================================================================================

    /// <summary>Looks up <paramref name="key"/> and appends its value to <paramref name="value"/>. Returns false if not found.</summary>
    public bool TryGet(ReadOnlySpan<byte> key, IBufferWriter<byte> value) => TryGet(null, key, value);

    public bool TryGet(ReadOptions? options, ReadOnlySpan<byte> key, IBufferWriter<byte> value)
    {
        ArgumentNullException.ThrowIfNull(value);
        GetContext ctx = Lookup(options, key);
        try
        {
            if (!ctx.TryResolve(key, out ReadOnlySpan<byte> v)) return false;
            value.Write(v);
            return true;
        }
        finally
        {
            GetContext.Return(ctx);
        }
    }

    /// <summary>
    /// Looks up <paramref name="key"/> and copies up to <paramref name="destination"/>.Length bytes of its value.
    /// Returns false if not found; <paramref name="valueLength"/> is the full value length (which may exceed the
    /// destination).
    /// </summary>
    public bool TryGet(ReadOnlySpan<byte> key, Span<byte> destination, out int valueLength) => TryGet(null, key, destination, out valueLength);

    public bool TryGet(ReadOptions? options, ReadOnlySpan<byte> key, Span<byte> destination, out int valueLength)
    {
        GetContext ctx = Lookup(options, key);
        try
        {
            if (!ctx.TryResolve(key, out ReadOnlySpan<byte> v))
            {
                valueLength = 0;
                return false;
            }
            valueLength = v.Length;
            v[..Math.Min(v.Length, destination.Length)].CopyTo(destination);
            return true;
        }
        finally
        {
            GetContext.Return(ctx);
        }
    }

    /// <summary>
    /// Zero-copy lookup: calls <paramref name="reader"/> with the value (valid only during the call). Returns false
    /// if the key was not found.
    /// </summary>
    public bool TryGet<TState, TResult>(ReadOnlySpan<byte> key, TState state, ReadOnlySpanFunc<TState, TResult> reader, out TResult result) =>
        TryGet(null, key, state, reader, out result);

    public bool TryGet<TState, TResult>(ReadOptions? options, ReadOnlySpan<byte> key, TState state, ReadOnlySpanFunc<TState, TResult> reader, out TResult result)
    {
        ArgumentNullException.ThrowIfNull(reader);
        GetContext ctx = Lookup(options, key);
        try
        {
            if (!ctx.TryResolve(key, out ReadOnlySpan<byte> v))
            {
                result = default!;
                return false;
            }
            result = reader(v, state);
            return true;
        }
        finally
        {
            GetContext.Return(ctx);
        }
    }

    /// <summary>Zero-copy lookup returning <c>default</c> when the key is missing.</summary>
    public TResult? Get<TState, TResult>(ReadOnlySpan<byte> key, TState state, ReadOnlySpanFunc<TState, TResult> reader) =>
        TryGet(null, key, state, reader, out TResult result) ? result : default;

    /// <summary>Returns the value in a pooled buffer, or null if not found. Dispose the result.</summary>
    public PooledValue? GetPooled(ReadOnlySpan<byte> key) => GetPooled(null, key);

    public PooledValue? GetPooled(ReadOptions? options, ReadOnlySpan<byte> key)
    {
        GetContext ctx = Lookup(options, key);
        try
        {
            return ctx.TryResolve(key, out ReadOnlySpan<byte> v) ? PooledValue.Create(v) : null;
        }
        finally
        {
            GetContext.Return(ctx);
        }
    }

    /// <summary>Returns a copy of the value, or null if not found (allocates; prefer the other overloads on hot paths).</summary>
    public byte[]? Get(ReadOnlySpan<byte> key) => Get(null, key);

    public byte[]? Get(ReadOptions? options, ReadOnlySpan<byte> key)
    {
        GetContext ctx = Lookup(options, key);
        try
        {
            return ctx.TryResolve(key, out ReadOnlySpan<byte> v) ? v.ToArray() : null;
        }
        finally
        {
            GetContext.Return(ctx);
        }
    }

    /// <summary>True if the key exists (with the cost of a full lookup).</summary>
    public bool ContainsKey(ReadOnlySpan<byte> key)
    {
        GetContext ctx = Lookup(null, key);
        try
        {
            return ctx.TryResolve(key, out _);
        }
        finally
        {
            GetContext.Return(ctx);
        }
    }

    /// <summary>Looks up several keys (currently one lookup per key). Missing keys yield null.</summary>
    public byte[]?[] MultiGet(IReadOnlyList<byte[]> keys) => MultiGet(null, keys);

    public byte[]?[] MultiGet(ReadOptions? options, IReadOnlyList<byte[]> keys)
    {
        ArgumentNullException.ThrowIfNull(keys);
        // All lookups read at the same sequence number, like DB::MultiGet.
        Snapshot? temp = null;
        if (options?.Snapshot is null)
        {
            temp = _db.GetSnapshot();
            options = options is null ? new ReadOptions { Snapshot = temp } : CloneWithSnapshot(options, temp);
        }
        try
        {
            var result = new byte[]?[keys.Count];
            for (int i = 0; i < keys.Count; i++) result[i] = Get(options, keys[i]);
            return result;
        }
        finally
        {
            temp?.Dispose();
        }
    }

    private static ReadOptions CloneWithSnapshot(ReadOptions o, Snapshot s) => new()
    {
        Snapshot = s,
        VerifyChecksums = o.VerifyChecksums,
        FillCache = o.FillCache,
        ReadTier = o.ReadTier,
        IgnoreRangeDeletions = o.IgnoreRangeDeletions,
        AsyncIo = o.AsyncIo,
    };

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private GetContext Lookup(ReadOptions? options, ReadOnlySpan<byte> key)
    {
        _db.ThrowIfDisposed();
        GetContext ctx = GetContext.Rent(_cfd.MergeOperator);
        try
        {
            _db.Get(_cfd, options, key, ctx);
        }
        catch
        {
            GetContext.Return(ctx);
            throw;
        }
        return ctx;
    }

    // ---- async reads ----

    /// <summary>Async lookup appending the value to <paramref name="value"/>; table blocks are read with async I/O.</summary>
    public ValueTask<bool> TryGetAsync(ReadOnlyMemory<byte> key, IBufferWriter<byte> value, CancellationToken cancellationToken = default) =>
        TryGetAsync(null, key, value, cancellationToken);

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<bool> TryGetAsync(ReadOptions? options, ReadOnlyMemory<byte> key, IBufferWriter<byte> value, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(value);
        GetContext ctx = await LookupAsync(options, key, cancellationToken).ConfigureAwait(false);
        try
        {
            return Resolve(ctx, key.Span, value);
        }
        finally
        {
            GetContext.Return(ctx);
        }
    }

    private static bool Resolve(GetContext ctx, ReadOnlySpan<byte> key, IBufferWriter<byte> value)
    {
        if (!ctx.TryResolve(key, out ReadOnlySpan<byte> v)) return false;
        value.Write(v);
        return true;
    }

    /// <summary>Async lookup returning the value in a pooled buffer (dispose it), or null if not found.</summary>
    public ValueTask<PooledValue?> GetPooledAsync(ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default) =>
        GetPooledAsync(null, key, cancellationToken);

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<PooledValue?> GetPooledAsync(ReadOptions? options, ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default)
    {
        GetContext ctx = await LookupAsync(options, key, cancellationToken).ConfigureAwait(false);
        try
        {
            return ToPooled(ctx, key.Span);
        }
        finally
        {
            GetContext.Return(ctx);
        }
    }

    private static PooledValue? ToPooled(GetContext ctx, ReadOnlySpan<byte> key) =>
        ctx.TryResolve(key, out ReadOnlySpan<byte> v) ? PooledValue.Create(v) : null;

    /// <summary>Async lookup returning a copy of the value, or null if not found.</summary>
    public ValueTask<byte[]?> GetAsync(ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default) => GetAsync(null, key, cancellationToken);

    public async ValueTask<byte[]?> GetAsync(ReadOptions? options, ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default)
    {
        GetContext ctx = await LookupAsync(options, key, cancellationToken).ConfigureAwait(false);
        try
        {
            return ToArray(ctx, key.Span);
        }
        finally
        {
            GetContext.Return(ctx);
        }
    }

    private static byte[]? ToArray(GetContext ctx, ReadOnlySpan<byte> key) => ctx.TryResolve(key, out ReadOnlySpan<byte> v) ? v.ToArray() : null;

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<GetContext> LookupAsync(ReadOptions? options, ReadOnlyMemory<byte> key, CancellationToken ct)
    {
        _db.ThrowIfDisposed();
        GetContext ctx = GetContext.Rent(_cfd.MergeOperator);
        try
        {
            await _db.GetAsync(_cfd, options, key, ctx, ct).ConfigureAwait(false);
        }
        catch
        {
            GetContext.Return(ctx);
            throw;
        }
        return ctx;
    }

    // =====================================================================================================
    // Misc
    // =====================================================================================================

    /// <summary>Flushes the memtable to a table file and waits for completion (DB::Flush).</summary>
    public void Flush() => Flush(s_waitFlush);

    public void Flush(FlushOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _db.FlushColumnFamilyAsync(_cfd, options.Wait, synchronous: true, CancellationToken.None).GetAwaiter().GetResult();
    }

    public ValueTask FlushAsync(CancellationToken cancellationToken = default) => FlushAsync(s_waitFlush, cancellationToken);

    public ValueTask FlushAsync(FlushOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        return _db.FlushColumnFamilyAsync(_cfd, options.Wait, synchronous: false, cancellationToken);
    }

    private static readonly FlushOptions s_waitFlush = new() { Wait = true };

    /// <summary>
    /// Compacts all data of this column family (DB::CompactRange with null bounds and default options): flushes the
    /// memtable and compacts every level down to the last one. Blocks the caller for the duration.
    /// </summary>
    public void CompactRange() => CompactRange(s_defaultCompactRange, null, null);

    /// <summary>Compacts the user key range [<paramref name="begin"/>, <paramref name="end"/>] (null = unbounded).</summary>
    public void CompactRange(byte[]? begin, byte[]? end) => CompactRange(s_defaultCompactRange, begin, end);

    /// <summary>
    /// Port of DB::CompactRange: compacts [<paramref name="begin"/>, <paramref name="end"/>] (null = unbounded) honoring
    /// <see cref="CompactRangeOptions"/> (bottommost level compaction, exclusive manual compaction, change level /
    /// target level).
    /// </summary>
    public void CompactRange(CompactRangeOptions options, byte[]? begin, byte[]? end)
    {
        ArgumentNullException.ThrowIfNull(options);
        _db.CompactRangeAsync(_cfd, begin, end, options, synchronous: true, CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <summary>Async <see cref="CompactRange()"/>: the compaction runs on a background thread.</summary>
    public ValueTask CompactRangeAsync(CancellationToken cancellationToken = default) =>
        CompactRangeAsync(s_defaultCompactRange, null, null, cancellationToken);

    /// <summary>Async <see cref="CompactRange(byte[], byte[])"/>.</summary>
    public ValueTask CompactRangeAsync(byte[]? begin, byte[]? end, CancellationToken cancellationToken = default) =>
        CompactRangeAsync(s_defaultCompactRange, begin, end, cancellationToken);

    /// <summary>Async <see cref="CompactRange(CompactRangeOptions, byte[], byte[])"/>.</summary>
    public ValueTask CompactRangeAsync(CompactRangeOptions options, byte[]? begin, byte[]? end, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        return _db.CompactRangeAsync(_cfd, begin, end, options, synchronous: false, cancellationToken);
    }

    private static readonly CompactRangeOptions s_defaultCompactRange = new();

    /// <summary>A DB property of this column family (DB::GetProperty), or null if unknown. See <see cref="RocksDatabase.GetProperty(string)"/>.</summary>
    public string? GetProperty(string property)
    {
        ArgumentNullException.ThrowIfNull(property);
        _db.ThrowIfDisposed();
        return _db.GetProperty(_cfd, property);
    }

    // =====================================================================================================
    // Iterators
    // =====================================================================================================

    /// <summary>
    /// Creates an iterator over this column family (DB::NewIterator), reading at <see cref="ReadOptions.Snapshot"/> or the
    /// latest state, honoring the iterate bounds. Dispose it to release the memtables/files it pins.
    /// </summary>
    public Iterator NewIterator() => NewIterator(null);

    public Iterator NewIterator(ReadOptions? options)
    {
        _db.ThrowIfDisposed();
        DbIteratorSources sources = _db.NewIteratorSources(_cfd, options);
        try
        {
            return DbIteratorFactory.Create(sources);
        }
        catch
        {
            sources.Cleanup?.Dispose();
            throw;
        }
    }

    /// <summary>Async <see cref="NewIterator(ReadOptions?)"/>: table files are opened with async I/O.</summary>
    public async ValueTask<Iterator> NewIteratorAsync(ReadOptions? options, CancellationToken cancellationToken = default)
    {
        _db.ThrowIfDisposed();
        DbIteratorSources sources = _db.NewIteratorSources(_cfd, options);
        try
        {
            return await DbIteratorFactory.CreateAsync(sources, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            sources.Cleanup?.Dispose();
            throw;
        }
    }
}
