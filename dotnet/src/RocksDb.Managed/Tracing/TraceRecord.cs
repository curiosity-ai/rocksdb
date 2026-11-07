namespace RocksDb.Managed.Tracing;

/// <summary>Seek kind of an <see cref="IteratorSeekQueryTraceRecord"/> (values = the trace types).</summary>
public enum IteratorSeekType : byte
{
    Seek = TraceType.IteratorSeek,
    SeekForPrev = TraceType.IteratorSeekForPrev,
}

/// <summary>A decoded query trace record (include/rocksdb/trace_record.h, trace_replay/trace_record.cc).</summary>
public abstract class TraceRecord
{
    private protected TraceRecord(ulong timestamp) => Timestamp = timestamp;

    /// <summary>Microseconds since the Unix epoch when the operation was traced.</summary>
    public ulong Timestamp { get; }

    public abstract TraceType TraceType { get; }
}

/// <summary>A write (WriteQueryTraceRecord): the WriteBatch representation and, with include_read_write_options, its options.</summary>
public sealed class WriteQueryTraceRecord : TraceRecord
{
    public WriteQueryTraceRecord(byte[] writeBatchRep, WriteOptions? writeOptions, ulong timestamp) : base(timestamp)
    {
        ArgumentNullException.ThrowIfNull(writeBatchRep);
        WriteBatchRep = writeBatchRep;
        WriteOptions = writeOptions;
    }

    public override TraceType TraceType => TraceType.Write;

    /// <summary>The traced WriteBatch representation (db/write_batch.cc format).</summary>
    public ReadOnlyMemory<byte> WriteBatchRep { get; }

    /// <summary>The recorded options, or null when the trace did not include them (replay uses defaults).</summary>
    public WriteOptions? WriteOptions { get; }
}

/// <summary>A point lookup (GetQueryTraceRecord).</summary>
public sealed class GetQueryTraceRecord : TraceRecord
{
    public GetQueryTraceRecord(uint columnFamilyId, byte[] key, ReadOptions? readOptions, ulong timestamp) : base(timestamp)
    {
        ArgumentNullException.ThrowIfNull(key);
        ColumnFamilyId = columnFamilyId;
        Key = key;
        ReadOptions = readOptions;
    }

    public override TraceType TraceType => TraceType.Get;
    public uint ColumnFamilyId { get; }
    public ReadOnlyMemory<byte> Key { get; }
    public ReadOptions? ReadOptions { get; }
}

/// <summary>An iterator Seek / SeekForPrev (IteratorSeekQueryTraceRecord). Empty bounds mean "not set".</summary>
public sealed class IteratorSeekQueryTraceRecord : TraceRecord
{
    public IteratorSeekQueryTraceRecord(IteratorSeekType seekType, uint columnFamilyId, byte[] key, byte[] lowerBound, byte[] upperBound,
        ReadOptions? readOptions, ulong timestamp) : base(timestamp)
    {
        SeekType = seekType;
        ColumnFamilyId = columnFamilyId;
        Key = key;
        LowerBound = lowerBound;
        UpperBound = upperBound;
        ReadOptions = readOptions;
    }

    public override TraceType TraceType => (TraceType)SeekType;
    public IteratorSeekType SeekType { get; }
    public uint ColumnFamilyId { get; }
    public ReadOnlyMemory<byte> Key { get; }
    public ReadOnlyMemory<byte> LowerBound { get; }
    public ReadOnlyMemory<byte> UpperBound { get; }
    public ReadOptions? ReadOptions { get; }
}

/// <summary>A MultiGet (MultiGetQueryTraceRecord): one column family ID per key.</summary>
public sealed class MultiGetQueryTraceRecord : TraceRecord
{
    public MultiGetQueryTraceRecord(uint[] columnFamilyIds, byte[][] keys, ReadOptions? readOptions, ulong timestamp) : base(timestamp)
    {
        ArgumentNullException.ThrowIfNull(columnFamilyIds);
        ArgumentNullException.ThrowIfNull(keys);
        ColumnFamilyIds = columnFamilyIds;
        Keys = keys;
        ReadOptions = readOptions;
    }

    public override TraceType TraceType => TraceType.MultiGet;
    public IReadOnlyList<uint> ColumnFamilyIds { get; }
    public IReadOnlyList<byte[]> Keys { get; }
    public ReadOptions? ReadOptions { get; }
}

/// <summary>Result of executing a trace record (include/rocksdb/trace_record_result.h). Timestamps are microseconds.</summary>
public abstract class TraceRecordResult
{
    private protected TraceRecordResult(TraceType type, ulong startTimestamp, ulong endTimestamp)
    {
        TraceType = type;
        StartTimestamp = startTimestamp;
        EndTimestamp = endTimestamp;
    }

    public TraceType TraceType { get; }
    public ulong StartTimestamp { get; }
    public ulong EndTimestamp { get; }
    public ulong Latency => EndTimestamp - StartTimestamp;
}

/// <summary>StatusOnlyTraceExecutionResult (writes): the outcome of the operation.</summary>
public sealed class StatusOnlyTraceExecutionResult(StatusCode status, ulong start, ulong end, TraceType type) : TraceRecordResult(type, start, end)
{
    public StatusCode Status { get; } = status;
}

/// <summary>SingleValueTraceExecutionResult (Get): status (Ok / NotFound / ...) and the value.</summary>
public sealed class SingleValueTraceExecutionResult(StatusCode status, byte[]? value, ulong start, ulong end, TraceType type)
    : TraceRecordResult(type, start, end)
{
    public StatusCode Status { get; } = status;
    public byte[]? Value { get; } = value;
}

/// <summary>MultiValuesTraceExecutionResult (MultiGet): one status and value per key.</summary>
public sealed class MultiValuesTraceExecutionResult(StatusCode[] statuses, byte[]?[] values, ulong start, ulong end, TraceType type)
    : TraceRecordResult(type, start, end)
{
    public IReadOnlyList<StatusCode> Statuses { get; } = statuses;
    public IReadOnlyList<byte[]?> Values { get; } = values;
}

/// <summary>IteratorTraceExecutionResult (Seek / SeekForPrev): the position reached.</summary>
public sealed class IteratorTraceExecutionResult(bool valid, StatusCode status, byte[]? key, byte[]? value, ulong start, ulong end, TraceType type)
    : TraceRecordResult(type, start, end)
{
    public bool Valid { get; } = valid;
    public StatusCode Status { get; } = status;
    public byte[]? Key { get; } = key;
    public byte[]? Value { get; } = value;
}
