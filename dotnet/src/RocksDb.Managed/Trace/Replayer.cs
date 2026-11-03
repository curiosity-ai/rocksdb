using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using RocksDb.Managed.Db;

namespace RocksDb.Managed.Trace;

/// <summary>Options of <see cref="Replayer.Replay"/> (include/rocksdb/utilities/replayer.h ReplayOptions).</summary>
public sealed class ReplayOptions
{
    /// <summary>Threads executing the records (0 or 1: the calling thread, in trace order). Default 1.</summary>
    public uint NumThreads { get; set; } = 1;

    /// <summary>Replay speed relative to the traced timing (2.0 = twice as fast). Must be &gt; 0. Default 1.0.</summary>
    public double FastForward { get; set; } = 1.0;
}

/// <summary>
/// Replays a query trace against a DB (utilities/trace/replayer_impl.cc ReplayerImpl, execution by
/// trace_replay/trace_record_handler.cc TraceExecutionHandler). Created by <see cref="RocksDatabase.NewDefaultReplayer(TraceReader)"/>.
/// Reads native and managed traces (trace file versions 0.1 and 0.2). Not thread-safe except where noted.
/// </summary>
public sealed class Replayer : IDisposable
{
    private readonly RocksDatabase _db;
    private readonly Dictionary<uint, ColumnFamily> _cfs = new();
    private readonly TraceReader _reader;
    private readonly Lock _readLock = new();
    private readonly ArrayBufferWriter<byte> _buffer = new(256);
    private bool _prepared;
    private bool _traceEnd;
    private int _traceFileVersion = -1;

    internal Replayer(RocksDatabase db, IReadOnlyList<ColumnFamily> columnFamilies, TraceReader reader)
    {
        _db = db;
        _reader = reader;
        foreach (ColumnFamily cf in columnFamilies) _cfs[cf.Id] = cf;
        // Every DB has a default column family, so records on it replay even when the caller did not pass its handle.
        _cfs[db.DefaultColumnFamily.Id] = db.DefaultColumnFamily;
    }

    /// <summary>Timestamp of the trace header (microseconds since the epoch). Valid after <see cref="Prepare"/>.</summary>
    public ulong HeaderTimestamp { get; private set; }

    /// <summary>The trace file version of the header ("0.2" = 2). Valid after <see cref="Prepare"/>.</summary>
    public int TraceFileVersion => _traceFileVersion;

    /// <summary>ReplayerImpl::Prepare: rewinds the reader and parses the header. Call again to replay from the start.</summary>
    public void Prepare()
    {
        _reader.Reset();
        _buffer.Clear();
        if (!_reader.TryRead(_buffer)) throw new RocksDbException(StatusCode.Incomplete, "Incomplete: empty trace");
        if (!TraceFormat.TryDecodeTrace(_buffer.WrittenMemory.ToArray(), out TraceEntry header))
            throw new RocksDbException(StatusCode.Incomplete, "Incomplete: Decode trace string failed");
        _traceFileVersion = TraceFormat.ParseQueryHeader(header, out _);
        HeaderTimestamp = header.Timestamp;
        _prepared = true;
        _traceEnd = false;
    }

    /// <summary>
    /// ReplayerImpl::Next: the next query record, or false at the end of the trace. Throws NotSupported for a record of
    /// an unsupported type (it is consumed; call again for the next one) and Incomplete when not prepared.
    /// </summary>
    public bool TryNext(out TraceRecord? record)
    {
        record = null;
        if (!_prepared) throw new RocksDbException(StatusCode.Incomplete, "Incomplete: Not prepared!");
        if (_traceEnd) return false;
        if (!ReadTrace(out TraceEntry trace)) return false;
        if (trace.Type == TraceType.End)
        {
            _traceEnd = true;
            return false;
        }
        record = TraceFormat.DecodeRecord(trace, _traceFileVersion);
        return true;
    }

    /// <summary>ReplayerImpl::Execute: runs one record; returns its result. Throws the failures that stop a replay.</summary>
    public TraceRecordResult Execute(TraceRecord record)
    {
        ArgumentNullException.ThrowIfNull(record);
        return record switch
        {
            WriteQueryTraceRecord w => ExecuteWrite(w),
            GetQueryTraceRecord g => ExecuteGet(g),
            IteratorSeekQueryTraceRecord s => ExecuteSeek(s),
            MultiGetQueryTraceRecord m => ExecuteMultiGet(m),
            _ => throw RocksDbException.NotSupported("Unsupported trace type."),
        };
    }

    /// <summary>
    /// ReplayerImpl::Replay: executes the remaining records, sleeping so that they run at the traced timing divided by
    /// <see cref="ReplayOptions.FastForward"/>. <paramref name="resultCallback"/> (may be null; called concurrently with
    /// more than one thread) receives each record's error (null on success) and result. The first failing record (by
    /// trace timestamp) stops the replay and is rethrown; reaching the end of the trace (even without a footer) is success.
    /// </summary>
    public void Replay(ReplayOptions options, Action<RocksDbException?, TraceRecordResult?>? resultCallback)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (options.FastForward <= 0.0) throw RocksDbException.InvalidArgument("Wrong fast forward speed!");
        if (!_prepared) throw new RocksDbException(StatusCode.Incomplete, "Incomplete: Not prepared!");
        if (_traceEnd) throw new RocksDbException(StatusCode.Incomplete, "Incomplete: Trace end.");
        long epoch = Stopwatch.GetTimestamp();
        if (options.NumThreads <= 1)
        {
            while (ReadTrace(out TraceEntry trace))
            {
                if (trace.Type == TraceType.End)
                {
                    _traceEnd = true;
                    return;
                }
                TraceRecord? record;
                try
                {
                    record = TraceFormat.DecodeRecord(trace, _traceFileVersion);
                }
                catch (RocksDbException e) when (e.Code == StatusCode.NotSupported)
                {
                    SleepUntil(epoch, trace.Timestamp, options.FastForward);
                    resultCallback?.Invoke(e, null);
                    continue;
                }
                SleepUntil(epoch, trace.Timestamp, options.FastForward);
                TraceRecordResult result;
                try
                {
                    result = Execute(record);
                }
                catch (Exception ex)
                {
                    RocksDbException e = RocksDbException.FromException(ex);
                    resultCallback?.Invoke(e, null);
                    throw e;
                }
                resultCallback?.Invoke(null, result);
            }
            _traceEnd = true;
            return;
        }

        // Multi-threaded replay: the scheduling thread sleeps, then hands the record to a worker that decodes and runs it.
        using var queue = new BlockingCollection<TraceEntry>();
        var errorLock = new Lock();
        RocksDbException? firstError = null;
        ulong firstErrorTs = ulong.MaxValue;
        int failed = 0;
        var workers = new Thread[options.NumThreads];
        for (int i = 0; i < workers.Length; i++)
        {
            workers[i] = new Thread(() =>
            {
                foreach (TraceEntry t in queue.GetConsumingEnumerable())
                {
                    try
                    {
                        TraceRecordResult r = Execute(TraceFormat.DecodeRecord(t, _traceFileVersion));
                        resultCallback?.Invoke(null, r);
                    }
                    catch (Exception ex)
                    {
                        RocksDbException e = RocksDbException.FromException(ex);
                        resultCallback?.Invoke(e, null);
                        lock (errorLock)
                        {
                            // Only the error of the earliest record is reported (same as single-threaded replay).
                            if (t.Timestamp < firstErrorTs)
                            {
                                firstError = e;
                                firstErrorTs = t.Timestamp;
                            }
                        }
                        Volatile.Write(ref failed, 1);
                    }
                }
            })
            { IsBackground = true, Name = "rocksdb:replay" };
            workers[i].Start();
        }
        try
        {
            while (Volatile.Read(ref failed) == 0 && ReadTrace(out TraceEntry trace))
            {
                if (trace.Type == TraceType.End)
                {
                    _traceEnd = true;
                    break;
                }
                SleepUntil(epoch, trace.Timestamp, options.FastForward);
                if (trace.Type is TraceType.Write or TraceType.Get or TraceType.IteratorSeek or TraceType.IteratorSeekForPrev or TraceType.MultiGet)
                    queue.Add(trace);
                else
                    resultCallback?.Invoke(RocksDbException.NotSupported("Unsupported trace type."), null);
            }
        }
        finally
        {
            queue.CompleteAdding();
            foreach (Thread w in workers) w.Join();
        }
        if (firstError is not null) throw firstError;
        _traceEnd = true;
    }

    private void SleepUntil(long epoch, ulong traceTs, double fastForward)
    {
        double micros = Math.Round((double)(traceTs - Math.Min(traceTs, HeaderTimestamp)) / fastForward);
        long due = epoch + (long)(micros * Stopwatch.Frequency / 1_000_000.0);
        long wait = due - Stopwatch.GetTimestamp();
        if (wait > 0) Thread.Sleep(TimeSpan.FromSeconds((double)wait / Stopwatch.Frequency));
    }

    /// <summary>ReplayerImpl::ReadTrace: reads are serialized; false at the end of the data.</summary>
    private bool ReadTrace(out TraceEntry trace)
    {
        byte[] encoded;
        lock (_readLock)
        {
            _buffer.Clear();
            if (!_reader.TryRead(_buffer))
            {
                trace = default;
                return false;
            }
            encoded = _buffer.WrittenSpan.ToArray();
        }
        if (!TraceFormat.TryDecodeTrace(encoded, out trace)) throw new RocksDbException(StatusCode.Incomplete, "Incomplete: Decode trace string failed");
        return true;
    }

    // ---- TraceExecutionHandler (trace_replay/trace_record_handler.cc) ----------------------------------------

    private ColumnFamily Cf(uint id) => _cfs.TryGetValue(id, out ColumnFamily? cf) ? cf : throw RocksDbException.Corruption("Invalid Column Family ID.");

    /// <summary>IsOutcomeOf (WriteOptions): a status the live call could also have returned is the call's result.</summary>
    private static bool IsOutcomeOf(WriteOptions? recorded, StatusCode s) =>
        recorded is not null && ((s == StatusCode.Incomplete && recorded.NoSlowdown) || (s == StatusCode.InvalidArgument && recorded.Sync && recorded.DisableWal));

    private static bool IsOutcomeOf(ReadOptions? recorded, StatusCode s) =>
        recorded is not null && ((s == StatusCode.Incomplete && recorded.ReadTier != ReadTier.ReadAllTier)
            || (s == StatusCode.Aborted && recorded.ValueSizeSoftLimit != ulong.MaxValue)
            || (s == StatusCode.NotSupported && recorded.ReadTier == ReadTier.PersistedTier));

    private static bool IsIteratorOutcomeOf(ReadOptions? recorded, StatusCode s) =>
        IsOutcomeOf(recorded, s) || (recorded is not null && ((s == StatusCode.Incomplete && recorded.MaxSkippableInternalKeys > 0)
            || (s == StatusCode.NotSupported && recorded.Tailing)));

    private TraceRecordResult ExecuteWrite(WriteQueryTraceRecord record)
    {
        ulong start = TraceFormat.NowMicros();
        StatusCode status = StatusCode.Ok;
        using (var batch = new WriteBatch(record.WriteBatchRep.Span))
        {
            try
            {
                _db.Write(record.WriteOptions, batch);
            }
            catch (RocksDbException e) when (IsOutcomeOf(record.WriteOptions, e.Code))
            {
                status = e.Code;
            }
        }
        return new StatusOnlyTraceExecutionResult(status, start, TraceFormat.NowMicros(), record.TraceType);
    }

    private TraceRecordResult ExecuteGet(GetQueryTraceRecord record)
    {
        ColumnFamily cf = Cf(record.ColumnFamilyId);
        ulong start = TraceFormat.NowMicros();
        StatusCode status;
        byte[]? value = null;
        try
        {
            var w = new ArrayBufferWriter<byte>();
            if (cf.TryGet(record.ReadOptions, record.Key.Span, w))
            {
                status = StatusCode.Ok;
                value = w.WrittenSpan.ToArray();
            }
            else
            {
                status = StatusCode.NotFound;
            }
        }
        catch (RocksDbException e) when (IsOutcomeOf(record.ReadOptions, e.Code))
        {
            status = e.Code;
        }
        return new SingleValueTraceExecutionResult(status, value, start, TraceFormat.NowMicros(), record.TraceType);
    }

    private TraceRecordResult ExecuteSeek(IteratorSeekQueryTraceRecord record)
    {
        ColumnFamily cf = Cf(record.ColumnFamilyId);
        ReadOptions ro = record.ReadOptions?.Clone() ?? new ReadOptions();
        if (!record.LowerBound.IsEmpty) ro.IterateLowerBound = record.LowerBound;
        if (!record.UpperBound.IsEmpty) ro.IterateUpperBound = record.UpperBound;
        ulong start = 0, end = 0;
        try
        {
            using Iterator it = cf.NewIterator(ro);
            start = TraceFormat.NowMicros();
            if (record.SeekType == IteratorSeekType.SeekForPrev) it.SeekForPrev(record.Key.Span);
            else it.Seek(record.Key.Span);
            end = TraceFormat.NowMicros();
            return it.Valid
                ? new IteratorTraceExecutionResult(true, StatusCode.Ok, it.Key.ToArray(), it.Value.ToArray(), start, end, record.TraceType)
                : new IteratorTraceExecutionResult(false, StatusCode.Ok, null, null, start, end, record.TraceType);
        }
        catch (RocksDbException e) when (IsIteratorOutcomeOf(record.ReadOptions, e.Code))
        {
            return new IteratorTraceExecutionResult(false, e.Code, null, null, start, end == 0 ? TraceFormat.NowMicros() : end, record.TraceType);
        }
    }

    private TraceRecordResult ExecuteMultiGet(MultiGetQueryTraceRecord record)
    {
        int n = record.Keys.Count;
        if (n == 0 || record.ColumnFamilyIds.Count == 0) throw RocksDbException.InvalidArgument("Empty MultiGet cf_ids or keys.");
        if (n != record.ColumnFamilyIds.Count) throw RocksDbException.InvalidArgument("MultiGet cf_ids and keys size mismatch.");
        var cfs = new ColumnFamily[n];
        for (int i = 0; i < n; i++) cfs[i] = Cf(record.ColumnFamilyIds[i]);
        var statuses = new StatusCode[n];
        var values = new byte[]?[n];
        ulong start = TraceFormat.NowMicros();
        // One batch per run of keys on the same column family (the managed MultiGet is per column family).
        int runStart = 0;
        while (runStart < n)
        {
            int runEnd = runStart + 1;
            while (runEnd < n && ReferenceEquals(cfs[runEnd], cfs[runStart])) runEnd++;
            using var batch = new MultiGetBatch(runEnd - runStart);
            for (int i = runStart; i < runEnd; i++) batch.Add(record.Keys[i]);
            cfs[runStart].MultiGet(record.ReadOptions, batch);
            for (int i = runStart; i < runEnd; i++)
            {
                int j = i - runStart;
                switch (batch.GetStatus(j))
                {
                    case MultiGetStatus.Found:
                        statuses[i] = StatusCode.Ok;
                        values[i] = batch.GetValue(j).ToArray();
                        break;
                    case MultiGetStatus.NotFound:
                        statuses[i] = StatusCode.NotFound;
                        break;
                    default:
                        RocksDbException? e = batch.GetError(j);
                        StatusCode code = e?.Code ?? StatusCode.Incomplete;
                        if (!IsOutcomeOf(record.ReadOptions, code)) throw e ?? new RocksDbException(code, "MultiGet failed");
                        statuses[i] = code;
                        break;
                }
            }
            runStart = runEnd;
        }
        return new MultiValuesTraceExecutionResult(statuses, values, start, TraceFormat.NowMicros(), record.TraceType);
    }

    public void Dispose() => _reader.Dispose();
}
