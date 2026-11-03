using RocksDb.Managed.Tracing;

namespace RocksDb.Managed;

/// <summary>
/// Query, block cache and IO tracing (DB::StartTrace / EndTrace / NewDefaultReplayer / StartBlockCacheTrace /
/// EndBlockCacheTrace / StartIOTrace / EndIOTrace, include/rocksdb/db.h). The trace files use the native formats
/// (trace_replay/trace_replay.cc, block_cache_tracer.cc, io_tracer.cc): native <c>trace_analyzer</c>,
/// <c>block_cache_trace_analyzer</c>, <c>io_tracer_parser</c> and native replayers read them, and the managed readers
/// read native traces.
/// </summary>
public sealed partial class RocksDatabase
{
    /// <summary>
    /// DB::StartTrace: records writes, Get, MultiGet and iterator Seek / SeekForPrev into <paramref name="writer"/>
    /// (which the DB disposes at <see cref="EndTrace"/> or close). Starting a trace while one runs replaces it.
    /// </summary>
    public void StartTrace(TraceOptions options, TraceWriter writer) => _db.StartTrace(options, writer);

    /// <summary><see cref="StartTrace(TraceOptions, TraceWriter)"/> into a new file at <paramref name="tracePath"/> (on the DB's file system).</summary>
    public void StartTrace(TraceOptions options, string tracePath) => _db.StartTrace(options, TraceWriter.CreateFile(_db.Options.FileSystem, tracePath));

    /// <summary>DB::EndTrace: writes the trace footer and closes the writer. IOError when no trace runs.</summary>
    public void EndTrace() => _db.EndTrace();

    /// <summary>
    /// DB::NewDefaultReplayer: a replayer of <paramref name="reader"/> executing on the given column families (records are
    /// matched by column family ID; the default column family is always included). The replayer owns the reader.
    /// </summary>
    public Replayer NewDefaultReplayer(IReadOnlyList<ColumnFamily> columnFamilies, TraceReader reader)
    {
        ArgumentNullException.ThrowIfNull(columnFamilies);
        ArgumentNullException.ThrowIfNull(reader);
        _db.ThrowIfDisposed();
        return new Replayer(this, columnFamilies, reader);
    }

    /// <summary>
    /// <see cref="NewDefaultReplayer(IReadOnlyList{ColumnFamily}, TraceReader)"/> on every open column family
    /// (convenience; upstream maps only the handles passed in).
    /// </summary>
    public Replayer NewDefaultReplayer(TraceReader reader) => NewDefaultReplayer(ColumnFamilies, reader);

    /// <summary>
    /// DB::StartBlockCacheTrace: records every block cache lookup of block-based tables (index, filter and data blocks).
    /// Busy if a block cache trace already runs.
    /// </summary>
    public void StartBlockCacheTrace(BlockCacheTraceOptions options, TraceWriter writer)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(writer);
        _db.ThrowIfDisposed();
        _db.BlockCacheTracer.StartTrace(options, writer);
    }

    /// <summary><see cref="StartBlockCacheTrace(BlockCacheTraceOptions, TraceWriter)"/> into a new file.</summary>
    public void StartBlockCacheTrace(BlockCacheTraceOptions options, string tracePath)
    {
        TraceWriter w = TraceWriter.CreateFile(_db.Options.FileSystem, tracePath);
        try
        {
            StartBlockCacheTrace(options, w);
        }
        catch
        {
            w.Dispose();
            throw;
        }
    }

    /// <summary>DB::EndBlockCacheTrace (no-op when no trace runs).</summary>
    public void EndBlockCacheTrace() => _db.BlockCacheTracer.EndTrace();

    /// <summary>
    /// DB::StartIOTrace: records the DB's file system operations (opens, reads, appends, syncs, deletes...) with their
    /// latency. <see cref="TraceOptions.MaxTraceFileSize"/> applies; the other options are ignored. Busy if one runs.
    /// </summary>
    public void StartIOTrace(TraceOptions options, TraceWriter writer)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(writer);
        _db.ThrowIfDisposed();
        _db.IoTracer.StartTrace(options, writer);
    }

    /// <summary><see cref="StartIOTrace(TraceOptions, TraceWriter)"/> into a new file (not itself traced).</summary>
    public void StartIOTrace(TraceOptions options, string tracePath)
    {
        TraceWriter w = TraceWriter.CreateFile(_db.Options.FileSystem, tracePath);
        try
        {
            StartIOTrace(options, w);
        }
        catch
        {
            w.Dispose();
            throw;
        }
    }

    /// <summary>DB::EndIOTrace (no-op when no trace runs).</summary>
    public void EndIOTrace() => _db.IoTracer.EndTrace();
}
