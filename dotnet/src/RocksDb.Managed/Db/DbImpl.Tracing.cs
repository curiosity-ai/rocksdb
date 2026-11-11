using System.Runtime.CompilerServices;
using RocksDb.Managed.Tracing;

namespace RocksDb.Managed.Db;

/// <summary>
/// Query, block cache and IO tracing of the DB (DBImpl::StartTrace / EndTrace / StartBlockCacheTrace /
/// EndBlockCacheTrace / StartIOTrace / EndIOTrace / TraceIteratorSeek*, db/db_impl/db_impl.cc).
/// <para>The read and write paths test <see cref="_queryTracer"/> once (null when no trace runs); the tracer serializes
/// its own records (upstream: trace_mutex_).</para>
/// </summary>
internal sealed partial class DbImpl
{
    private QueryTracer? _queryTracer;
    private readonly Lock _traceMutex = new();

    /// <summary>The DB's block cache tracer (DBImpl::block_cache_tracer_), handed to its table readers.</summary>
    internal readonly BlockCacheTracer BlockCacheTracer = new();

    /// <summary>The DB's IO tracer (DBImpl::io_tracer_), used by the file system wrapper.</summary>
    internal readonly IoTracer IoTracer = new();

    /// <summary>The running query tracer, or null.</summary>
    internal QueryTracer? QueryTracer
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => Volatile.Read(ref _queryTracer);
    }

    /// <summary>DBImpl::StartTrace: replaces a running trace (as upstream, which resets tracer_ without closing it).</summary>
    public void StartTrace(TraceOptions options, TraceWriter writer)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(writer);
        ThrowIfDisposed();
        lock (_traceMutex)
        {
            QueryTracer? old = _queryTracer;
            Volatile.Write(ref _queryTracer, new QueryTracer(options, writer));
            // The running tracer is dropped (~Tracer), not ended (Tracer::Close), so its file gets no footer.
            old?.Abandon();
        }
    }

    /// <summary>DBImpl::EndTrace: writes the footer; IOError "No trace file to close" when no trace runs.</summary>
    public void EndTrace()
    {
        QueryTracer? t;
        lock (_traceMutex)
        {
            t = _queryTracer;
            if (t is null) throw RocksDbException.IOError("No trace file to close");
            Volatile.Write(ref _queryTracer, null);
        }
        t.Close();
    }

    /// <summary>Closes all traces (DB close).</summary>
    private void EndAllTraces()
    {
        QueryTracer? t;
        lock (_traceMutex)
        {
            t = _queryTracer;
            Volatile.Write(ref _queryTracer, null);
        }
        t?.Close();
        BlockCacheTracer.EndTrace();
        IoTracer.EndTrace();
    }

    /// <summary>Traces a Get (GetImpl: tracer_->Get). Cold: only called while tracing.</summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    internal static void TraceGet(QueryTracer t, ColumnFamilyData cfd, ReadOnlySpan<byte> key, ReadOptions? options) => t.Get(cfd.Id, key, options);

    /// <summary>Traces a write submitted with <paramref name="options"/> (WriteImpl, non-preserved order).</summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void TraceWrite(QueryTracer t, WriteBatch batch, WriteOptions? options) =>
        t.Write(batch.Data, options?.Sync ?? false, options?.DisableWal ?? false, options?.NoSlowdown ?? false);

    /// <summary>Traces the valid writers of a write group in WAL order (WriteImpl with preserve_write_order).</summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void TraceWriteGroup(QueryTracer t, List<WriteThread.Writer> group)
    {
        foreach (WriteThread.Writer w in group)
        {
            if (w.CallbackFailed || w.Batch is null) continue;
            t.Write(w.Batch.Data, w.Sync, w.DisableWal, w.NoSlowdown);
        }
    }

    /// <summary>
    /// WriteImpl's tracing of a submitted write: traced here unless the order is preserved by the write-group leader
    /// (<see cref="TraceWriteGroup"/>). The outer call of a plain write into a sequence-per-batch DB is not traced: its
    /// inner, transformed call is. Unordered and WAL-only (two_write_queues) writes have no group leader here and are
    /// always traced at submission.
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private void MaybeTraceWrite(WriteOptions? options, WriteBatch batch, TransactionWriteContext? txn)
    {
        QueryTracer? t = QueryTracer;
        if (t is null) return;
        if (_seqPerBatch && (txn is null || txn.BatchCount == 0)) return;
        bool leaderTraces = !_unorderedWrite && !(_twoWriteQueues && txn is { DisableMemtable: true });
        if (t.PreserveWriteOrder && leaderTraces) return;
        TraceWrite(t, batch, options);
    }

    /// <summary>Traces a MultiGet of one column family (MultiGetCommon: tracer_->MultiGet).</summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    internal static void TraceMultiGet(QueryTracer t, ColumnFamilyData cfd, MultiGetBatch batch, ReadOptions? options) => t.MultiGet(cfd.Id, batch, options);

    /// <summary>
    /// Attributes the table lookups of the calling async method to <paramref name="caller"/> (scoped to that method by
    /// the AsyncLocal). Only called while a block cache trace runs.
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private void SetBlockCacheTraceContext(TableReaderCaller caller, ReadOnlySpan<byte> key, ReadOptions? options) =>
        BlockCacheTracer.SetCaller(caller, BlockCacheTracer.NextGetId(), key, options?.Snapshot is not null);
}
