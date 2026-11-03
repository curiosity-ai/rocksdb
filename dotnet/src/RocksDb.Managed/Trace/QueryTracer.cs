using RocksDb.Managed.Util;

namespace RocksDb.Managed.Trace;

/// <summary>
/// Port of Tracer (trace_replay/trace_replay.cc): encodes query records (writes, Get, iterator seeks, MultiGet) into a
/// <see cref="TraceWriter"/>, with the size limit, filter and sampling of <see cref="TraceOptions"/>.
/// <para>Synchronization: every record is encoded and written under <see cref="_lock"/> (upstream: DBImpl::trace_mutex_
/// held around each Tracer call). Calls after <see cref="Close"/> are ignored.</para>
/// </summary>
internal sealed class QueryTracer
{
    private readonly Lock _lock = new();
    private readonly TraceOptions _options;
    private readonly TraceWriter _writer;
    private readonly PooledBufferWriter _payload = new(256);
    private readonly PooledBufferWriter _record = new(256);
    private readonly PooledBufferWriter _scratch = new(64);
    private readonly PooledBufferWriter _scratch2 = new(64);
    private ulong _requestCount;
    private Exception? _writeError;
    private bool _closed;

    public QueryTracer(TraceOptions options, TraceWriter writer)
    {
        _options = options.Clone();
        _writer = writer;
        PreserveWriteOrder = _options.PreserveWriteOrder;
        lock (_lock) WriteTrace(TraceFormat.NowMicros(), TraceType.Begin, TraceFormat.QueryHeaderPayload());
    }

    /// <summary>Tracer::IsWriteOrderPreserved.</summary>
    public bool PreserveWriteOrder { get; }

    /// <summary>Tracer::Write.</summary>
    public void Write(ReadOnlySpan<byte> batchRep, bool sync, bool disableWal, bool noSlowdown)
    {
        lock (_lock)
        {
            if (ShouldSkipTrace(TraceType.Write)) return;
            ulong map = 0;
            TraceFormat.SetPayloadMap(ref map, TracePayloadType.WriteBatchData);
            if (_options.IncludeReadWriteOptions) TraceFormat.SetPayloadMap(ref map, TracePayloadType.WriteOptions);
            _payload.Clear();
            Coding.PutFixed64(_payload, map);
            Coding.PutLengthPrefixedSlice(_payload, batchRep);
            if (_options.IncludeReadWriteOptions) TraceFormat.EncodeWriteOptions(_payload, sync, disableWal, noSlowdown, Env.IoPriority.Total);
            WriteTrace(TraceFormat.NowMicros(), TraceType.Write, _payload.WrittenSpan);
        }
    }

    /// <summary>Tracer::Get.</summary>
    public void Get(uint cfId, ReadOnlySpan<byte> key, ReadOptions? options)
    {
        lock (_lock)
        {
            if (ShouldSkipTrace(TraceType.Get)) return;
            ulong map = 0;
            TraceFormat.SetPayloadMap(ref map, TracePayloadType.GetCfId);
            TraceFormat.SetPayloadMap(ref map, TracePayloadType.GetKey);
            if (_options.IncludeReadWriteOptions) TraceFormat.SetPayloadMap(ref map, TracePayloadType.GetReadOptions);
            _payload.Clear();
            Coding.PutFixed64(_payload, map);
            Coding.PutFixed32(_payload, cfId);
            Coding.PutLengthPrefixedSlice(_payload, key);
            if (_options.IncludeReadWriteOptions) TraceFormat.EncodeReadOptions(_payload, options);
            WriteTrace(TraceFormat.NowMicros(), TraceType.Get, _payload.WrittenSpan);
        }
    }

    /// <summary>Tracer::IteratorSeekImpl (empty bounds are not recorded).</summary>
    public void IteratorSeek(TraceType type, uint cfId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> lower, ReadOnlySpan<byte> upper, ReadOptions? options)
    {
        lock (_lock)
        {
            if (ShouldSkipTrace(type)) return;
            ulong map = 0;
            TraceFormat.SetPayloadMap(ref map, TracePayloadType.IterCfId);
            TraceFormat.SetPayloadMap(ref map, TracePayloadType.IterKey);
            if (lower.Length > 0) TraceFormat.SetPayloadMap(ref map, TracePayloadType.IterLowerBound);
            if (upper.Length > 0) TraceFormat.SetPayloadMap(ref map, TracePayloadType.IterUpperBound);
            if (_options.IncludeReadWriteOptions) TraceFormat.SetPayloadMap(ref map, TracePayloadType.IterReadOptions);
            _payload.Clear();
            Coding.PutFixed64(_payload, map);
            Coding.PutFixed32(_payload, cfId);
            Coding.PutLengthPrefixedSlice(_payload, key);
            if (lower.Length > 0) Coding.PutLengthPrefixedSlice(_payload, lower);
            if (upper.Length > 0) Coding.PutLengthPrefixedSlice(_payload, upper);
            if (_options.IncludeReadWriteOptions) TraceFormat.EncodeReadOptions(_payload, options);
            WriteTrace(TraceFormat.NowMicros(), type, _payload.WrittenSpan);
        }
    }

    /// <summary>Tracer::MultiGet for one column family.</summary>
    public void MultiGet(uint cfId, MultiGetBatch batch, ReadOptions? options)
    {
        int n = batch.Count;
        if (n == 0) return;
        lock (_lock)
        {
            if (ShouldSkipTrace(TraceType.MultiGet)) return;
            ulong map = 0;
            TraceFormat.SetPayloadMap(ref map, TracePayloadType.MultiGetSize);
            TraceFormat.SetPayloadMap(ref map, TracePayloadType.MultiGetCfIds);
            TraceFormat.SetPayloadMap(ref map, TracePayloadType.MultiGetKeys);
            if (_options.IncludeReadWriteOptions) TraceFormat.SetPayloadMap(ref map, TracePayloadType.MultiGetReadOptions);
            _scratch.Clear();
            _scratch2.Clear();
            for (int i = 0; i < n; i++)
            {
                Coding.PutFixed32(_scratch, cfId);
                Coding.PutLengthPrefixedSlice(_scratch2, batch.GetKey(i));
            }
            _payload.Clear();
            Coding.PutFixed64(_payload, map);
            Coding.PutFixed32(_payload, (uint)n);
            Coding.PutLengthPrefixedSlice(_payload, _scratch.WrittenSpan);
            Coding.PutLengthPrefixedSlice(_payload, _scratch2.WrittenSpan);
            if (_options.IncludeReadWriteOptions) TraceFormat.EncodeReadOptions(_payload, options);
            WriteTrace(TraceFormat.NowMicros(), TraceType.MultiGet, _payload.WrittenSpan);
        }
    }

    /// <summary>Tracer::Close: writes the footer and closes the writer.</summary>
    public void Close()
    {
        lock (_lock)
        {
            if (_closed) return;
            try
            {
                WriteTrace(TraceFormat.NowMicros(), TraceType.End, default);
            }
            finally
            {
                _closed = true;
                _writer.Close();
                _payload.Dispose();
                _record.Dispose();
                _scratch.Dispose();
                _scratch2.Dispose();
            }
        }
    }

    /// <summary>Tracer::ShouldSkipTrace. REQUIRES: _lock.</summary>
    private bool ShouldSkipTrace(TraceType type)
    {
        if (_closed || _writer.FileSize > _options.MaxTraceFileSize) return true;
        TraceFilter mask = type switch
        {
            TraceType.Write => TraceFilter.Write,
            TraceType.Get => TraceFilter.Get,
            TraceType.IteratorSeek => TraceFilter.IteratorSeek,
            TraceType.IteratorSeekForPrev => TraceFilter.IteratorSeekForPrev,
            TraceType.MultiGet => TraceFilter.MultiGet,
            _ => TraceFilter.None,
        };
        if (mask != TraceFilter.None && (_options.Filter & mask) != 0) return true;
        if (++_requestCount < _options.SamplingFrequency) return true;
        _requestCount = 0;
        return false;
    }

    /// <summary>Tracer::WriteTrace: the first write error stops tracing (later records fail with Incomplete upstream). REQUIRES: _lock.</summary>
    private void WriteTrace(ulong ts, TraceType type, ReadOnlySpan<byte> payload)
    {
        if (_writeError is not null) return;
        _record.Clear();
        TraceFormat.EncodeTrace(_record, ts, type, payload);
        try
        {
            _writer.Write(_record.WrittenSpan);
        }
        catch (Exception e)
        {
            _writeError = e;
        }
    }

    /// <summary>The first error the trace writer threw, if any.</summary>
    public Exception? WriteError
    {
        get
        {
            lock (_lock) return _writeError;
        }
    }
}
