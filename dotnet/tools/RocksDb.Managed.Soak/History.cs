using System.Buffers.Binary;
using System.Diagnostics;

namespace RocksDb.Managed.Soak;

/// <summary>Jepsen op phases: :invoke, then :ok, :fail (definitely not applied) or :info (indeterminate).</summary>
internal enum Phase : byte
{
    Invoke = 1,
    Ok = 2,
    Fail = 3,
    Info = 4,
}

/// <summary>Operation types recorded in the history.</summary>
internal enum OpType : byte
{
    /// <summary>Register put: A = key index, V = version, Crc/Len = payload.</summary>
    RegPut = 1,
    /// <summary>Register delete: A = key index, V = version.</summary>
    RegDel = 2,
    /// <summary>Register read (online check): A = writer, B = key index, V = version read (-1 = absent).</summary>
    RegRead = 3,
    /// <summary>Bank transfer as a WriteBatch of two merges: A = from, B = to, V = amount.</summary>
    BankBatch = 4,
    /// <summary>Bank transfer as an optimistic transaction: A = from, B = to, V = amount.</summary>
    BankTxn = 5,
    /// <summary>Bank read under a snapshot: V = total, extra = balances (8 bytes each).</summary>
    BankRead = 6,
    /// <summary>Counter merge: A = counter, V = delta.</summary>
    CtrAdd = 7,
    /// <summary>Range-delete key space put: A = key, V = version, Crc/Len = payload.</summary>
    RdelPut = 8,
    /// <summary>Range-delete key space DeleteRange [A, B): V = version.</summary>
    RdelRange = 9,
    /// <summary>SyncWal / FlushWal(true): every write acknowledged before its invoke is durable once it is ok.</summary>
    SyncWal = 10,
    /// <summary>Admin / background op (flush, compaction, checkpoint, column family...): A = kind. Not checked.</summary>
    Admin = 11,
    /// <summary>Bank initialisation (supervisor init step): V = total.</summary>
    BankInit = 12,
}

[Flags]
internal enum OpFlags : byte
{
    None = 0,
    /// <summary>The write used WriteOptions.Sync = true.</summary>
    Sync = 1,
    /// <summary>The op used an async API.</summary>
    Async = 2,
}

/// <summary>One history record (48-byte header plus optional extra bytes).</summary>
internal struct HistoryRecord
{
    public const int Size = 48;
    public Phase Phase;
    public OpType Type;
    public byte Process;
    public OpFlags Flags;
    public int A;
    public int B;
    public long OpId;
    public long V;
    public uint Crc;
    public int Len;
    public long TimeUs;
    public byte[]? Extra;
    /// <summary>Position of the record in the history file (global order), set when read.</summary>
    public int Index;

    public readonly void Encode(Span<byte> d, int extraLen)
    {
        d[0] = (byte)Phase;
        d[1] = (byte)Type;
        d[2] = Process;
        d[3] = (byte)Flags;
        BinaryPrimitives.WriteInt32LittleEndian(d[4..], A);
        BinaryPrimitives.WriteInt32LittleEndian(d[8..], B);
        BinaryPrimitives.WriteInt64LittleEndian(d[12..], OpId);
        BinaryPrimitives.WriteInt64LittleEndian(d[20..], V);
        BinaryPrimitives.WriteUInt32LittleEndian(d[28..], Crc);
        BinaryPrimitives.WriteInt32LittleEndian(d[32..], Len);
        BinaryPrimitives.WriteInt64LittleEndian(d[36..], TimeUs);
        BinaryPrimitives.WriteInt32LittleEndian(d[44..], extraLen);
    }

    public static HistoryRecord Decode(ReadOnlySpan<byte> d, out int extraLen)
    {
        extraLen = BinaryPrimitives.ReadInt32LittleEndian(d[44..]);
        return new HistoryRecord
        {
            Phase = (Phase)d[0],
            Type = (OpType)d[1],
            Process = d[2],
            Flags = (OpFlags)d[3],
            A = BinaryPrimitives.ReadInt32LittleEndian(d[4..]),
            B = BinaryPrimitives.ReadInt32LittleEndian(d[8..]),
            OpId = BinaryPrimitives.ReadInt64LittleEndian(d[12..]),
            V = BinaryPrimitives.ReadInt64LittleEndian(d[20..]),
            Crc = BinaryPrimitives.ReadUInt32LittleEndian(d[28..]),
            Len = BinaryPrimitives.ReadInt32LittleEndian(d[32..]),
            TimeUs = BinaryPrimitives.ReadInt64LittleEndian(d[36..]),
        };
    }

    public override readonly string ToString() =>
        $"#{Index} {Phase} {Type} p{Process} op{OpId} a={A} b={B} v={V} crc={Crc:x8} len={Len}{((Flags & OpFlags.Sync) != 0 ? " sync" : "")}";
}

/// <summary>
/// Append-only history file. Every record is written straight to the OS (one write(2) per record, no user-space
/// buffer), so it survives a SIGKILL of the worker; no fsync (the history is never subject to the injected
/// power loss). Records are appended under one lock, so the file order is a total order of the recorded events.
/// </summary>
internal sealed class HistoryWriter : IDisposable
{
    private readonly FileStream _file;
    private readonly object _lock = new();
    private readonly byte[] _buf = new byte[HistoryRecord.Size + 4096];
    private readonly Stopwatch _clock;
    private readonly long _epochUs;
    private long _nextOpId;
    private volatile bool _frozen;

    public HistoryWriter(string path, long firstOpId)
    {
        _file = new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.ReadWrite | FileShare.Delete, bufferSize: 0);
        _clock = Stopwatch.StartNew();
        _epochUs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000;
        _nextOpId = firstOpId;
    }

    public long NewOpId() => Interlocked.Increment(ref _nextOpId);

    public long NowUs => _epochUs + _clock.Elapsed.Ticks / 10;

    /// <summary>Appends a record (blocks forever once <see cref="Freeze"/> was called).</summary>
    public void Append(Phase phase, OpType type, int process, OpFlags flags, long opId, int a, int b, long v, uint crc, int len,
        ReadOnlySpan<byte> extra = default)
    {
        var r = new HistoryRecord
        {
            Phase = phase, Type = type, Process = (byte)process, Flags = flags, OpId = opId, A = a, B = b, V = v, Crc = crc, Len = len,
        };
        lock (_lock)
        {
            while (_frozen) Monitor.Wait(_lock);
            r.TimeUs = NowUs;
            byte[] buf = extra.Length <= 4096 ? _buf : new byte[HistoryRecord.Size + extra.Length];
            r.Encode(buf, extra.Length);
            extra.CopyTo(buf.AsSpan(HistoryRecord.Size));
            _file.Write(buf, 0, HistoryRecord.Size + extra.Length);
        }
    }

    /// <summary>
    /// Stops recording: once this returns, no further record reaches the file (callers of <see cref="Append"/>
    /// block). Used before a simulated power loss so that no ack is recorded for an operation whose effect may be
    /// dropped by the fault.
    /// </summary>
    public void Freeze()
    {
        lock (_lock) _frozen = true;
    }

    public void Dispose() => _file.Dispose();
}

/// <summary>Reads a history file (a torn last record, from a kill during the write, is ignored).</summary>
internal static class HistoryReader
{
    /// <summary>Reads the parts of a cycle's history as one sequence (parts are written one after the other).</summary>
    public static List<HistoryRecord> ReadParts(IEnumerable<string> paths)
    {
        var list = new List<HistoryRecord>();
        foreach (string p in paths) Read(p, list);
        return list;
    }

    private static void Read(string path, List<HistoryRecord> list)
    {
        if (!File.Exists(path)) return;
        byte[] data = File.ReadAllBytes(path);
        int pos = 0;
        while (pos + HistoryRecord.Size <= data.Length)
        {
            HistoryRecord r = HistoryRecord.Decode(data.AsSpan(pos), out int extraLen);
            if (extraLen < 0 || pos + HistoryRecord.Size + extraLen > data.Length) break;
            if (extraLen > 0) r.Extra = data.AsSpan(pos + HistoryRecord.Size, extraLen).ToArray();
            r.Index = list.Count;
            list.Add(r);
            pos += HistoryRecord.Size + extraLen;
        }
    }
}

/// <summary>An operation: its invoke record and its completion (null = never completed, i.e. :info).</summary>
internal sealed class Op
{
    public HistoryRecord Invoke;
    public HistoryRecord? Completion;

    public Phase Outcome => Completion?.Phase ?? Phase.Info;

    /// <summary>Definitely applied (acknowledged).</summary>
    public bool IsOk => Outcome == Phase.Ok;

    /// <summary>Definitely not applied.</summary>
    public bool IsFail => Outcome == Phase.Fail;

    public static List<Op> Pair(List<HistoryRecord> records)
    {
        var byId = new Dictionary<long, Op>();
        var ops = new List<Op>();
        foreach (HistoryRecord r in records)
        {
            if (r.Phase == Phase.Invoke)
            {
                var op = new Op { Invoke = r };
                byId[r.OpId] = op;
                ops.Add(op);
            }
            else if (byId.TryGetValue(r.OpId, out Op? op) && op.Completion is null)
            {
                op.Completion = r;
            }
        }
        return ops;
    }
}
