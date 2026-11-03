using System.Buffers;
using System.Globalization;
using System.Numerics;
using System.Text;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Trace;

/// <summary>
/// The type byte of a trace record (include/rocksdb/trace_record.h TraceType). Values hit disk: they are copied
/// exactly. Query traces use <see cref="Write"/> .. <see cref="IteratorSeekForPrev"/> and <see cref="MultiGet"/>, block
/// cache traces use the <c>Block*</c> values and IO traces use <see cref="IoTracer"/>.
/// </summary>
public enum TraceType : byte
{
    None = 0,
    Begin = 1,
    End = 2,
    Write = 3,
    Get = 4,
    IteratorSeek = 5,
    IteratorSeekForPrev = 6,
    BlockIndexBlock = 7,
    BlockFilterBlock = 8,
    BlockDataBlock = 9,
    BlockUncompressionDictBlock = 10,
    BlockRangeDeletionBlock = 11,
    IoTracer = 12,
    MultiGet = 13,
    Max = 14,
}

/// <summary>Operations excluded from a query trace (include/rocksdb/options.h TraceFilterType). Bit values hit no disk but match the C API.</summary>
[Flags]
public enum TraceFilter : ulong
{
    /// <summary>Trace all the operations.</summary>
    None = 0,
    /// <summary>Do not trace Get.</summary>
    Get = 1 << 0,
    /// <summary>Do not trace writes.</summary>
    Write = 1 << 1,
    /// <summary>Do not trace Iterator.Seek.</summary>
    IteratorSeek = 1 << 2,
    /// <summary>Do not trace Iterator.SeekForPrev.</summary>
    IteratorSeekForPrev = 1 << 3,
    /// <summary>Do not trace MultiGet.</summary>
    MultiGet = 1 << 4,
}

/// <summary>Options of <see cref="RocksDatabase.StartTrace(TraceOptions, TraceWriter)"/> (include/rocksdb/options.h TraceOptions).</summary>
public sealed class TraceOptions
{
    /// <summary>Records are dropped once the trace file is larger than this many bytes (default 64 GiB).</summary>
    public ulong MaxTraceFileSize { get; set; } = 64UL * 1024 * 1024 * 1024;

    /// <summary>Capture one request out of this many (default 1: every request). Filtering happens before sampling.</summary>
    public ulong SamplingFrequency { get; set; } = 1;

    /// <summary>Operations not to trace.</summary>
    public TraceFilter Filter { get; set; } = TraceFilter.None;

    /// <summary>
    /// When true, write records are traced by the write-group leader in the order of the WAL; otherwise they are traced
    /// when the write is submitted (default false, cheaper). Block cache and IO traces ignore it.
    /// </summary>
    public bool PreserveWriteOrder { get; set; }

    /// <summary>
    /// When true, each query record also carries the options of its call (write: sync, disableWAL, no_slowdown,
    /// rate_limiter_priority; reads: the boolean read options, read_tier, rate_limiter_priority, readahead_size,
    /// max_skippable_internal_keys, value_size_soft_limit) and replay uses them instead of default options. Off by
    /// default because RocksDB releases before 11.14 do not know these record fields (their release builds skip them).
    /// </summary>
    public bool IncludeReadWriteOptions { get; set; }

    internal TraceOptions Clone() => (TraceOptions)MemberwiseClone();
}

/// <summary>Payload field bits of the query trace records (trace_replay.h TracePayloadType). Bit order is encoding order.</summary>
internal enum TracePayloadType
{
    EmptyPayload = 0,
    WriteBatchData = 1,
    GetCfId = 2,
    GetKey = 3,
    IterCfId = 4,
    IterKey = 5,
    IterLowerBound = 6,
    IterUpperBound = 7,
    MultiGetSize = 8,
    MultiGetCfIds = 9,
    MultiGetKeys = 10,
    WriteOptions = 11,
    GetReadOptions = 12,
    IterReadOptions = 13,
    MultiGetReadOptions = 14,
}

/// <summary>
/// One decoded trace record: <c>fixed64 timestamp | byte type | fixed32 payload length | payload</c>
/// (trace_replay.cc TracerHelper::EncodeTrace / DecodeTrace).
/// </summary>
internal readonly struct TraceEntry(ulong timestamp, TraceType type, ReadOnlyMemory<byte> payload)
{
    public ulong Timestamp { get; } = timestamp;
    public TraceType Type { get; } = type;
    public ReadOnlyMemory<byte> Payload { get; } = payload;
}

/// <summary>Port of TracerHelper (trace_replay/trace_replay.cc): header, record framing and query record decoding.</summary>
internal static class TraceFormat
{
    /// <summary>kTraceMagic.</summary>
    public const string Magic = "feedcafedeadbeef";
    public static ReadOnlySpan<byte> MagicBytes => "feedcafedeadbeef"u8;
    public const int TimestampSize = 8;
    public const int TypeSize = 1;
    public const int PayloadLengthSize = 4;
    /// <summary>kTraceMetadataSize.</summary>
    public const int MetadataSize = TimestampSize + TypeSize + PayloadLengthSize;
    public const int FileMajorVersion = 0;
    public const int FileMinorVersion = 2;

    // TracedWriteOptions' boolean fields, by bit (append only).
    private const uint WriteSyncBit = 1u << 0;
    private const uint WriteDisableWalBit = 1u << 1;
    private const uint WriteNoSlowdownBit = 1u << 2;

    /// <summary>Microseconds since the Unix epoch (SystemClock::NowMicros).</summary>
    public static ulong NowMicros() => (ulong)(DateTime.UtcNow.Ticks - DateTime.UnixEpoch.Ticks) / 10;

    /// <summary>A monotonic nanosecond clock (SystemClock::NowNanos on Linux is CLOCK_MONOTONIC).</summary>
    public static ulong NowNanos() => (ulong)((UInt128)(ulong)System.Diagnostics.Stopwatch.GetTimestamp() * 1_000_000_000UL / (ulong)System.Diagnostics.Stopwatch.Frequency);

    /// <summary>TracerHelper::EncodeTrace.</summary>
    public static void EncodeTrace(IBufferWriter<byte> dst, ulong timestamp, TraceType type, ReadOnlySpan<byte> payload)
    {
        Span<byte> head = dst.GetSpan(MetadataSize);
        Coding.EncodeFixed64(head, timestamp);
        head[8] = (byte)type;
        Coding.EncodeFixed32(head[9..], (uint)payload.Length);
        dst.Advance(MetadataSize);
        dst.Write(payload);
    }

    /// <summary>TracerHelper::DecodeTrace: false (Incomplete) if the record is shorter than its metadata.</summary>
    public static bool TryDecodeTrace(ReadOnlyMemory<byte> encoded, out TraceEntry entry)
    {
        ReadOnlySpan<byte> s = encoded.Span;
        if (s.Length < MetadataSize)
        {
            entry = default;
            return false;
        }
        // Like upstream, the payload is everything after the metadata (the length field is only used for framing).
        entry = new TraceEntry(Coding.DecodeFixed64(s), (TraceType)s[8], encoded[MetadataSize..]);
        return true;
    }

    /// <summary>The payload of the query trace header (Tracer::WriteHeader).</summary>
    public static byte[] QueryHeaderPayload()
    {
        Version v = OptionsFile.CurrentRocksDbVersion;
        string header = Magic + "\tTrace Version: " + FileMajorVersion.ToString(CultureInfo.InvariantCulture) + "."
            + FileMinorVersion.ToString(CultureInfo.InvariantCulture) + "\tRocksDB Version: " + v.Major.ToString(CultureInfo.InvariantCulture)
            + "." + v.Minor.ToString(CultureInfo.InvariantCulture) + "\tFormat: Timestamp OpType Payload\n";
        return Encoding.ASCII.GetBytes(header);
    }

    /// <summary>TracerHelper::DecodeHeader + ParseTraceHeader: validates the magic and returns the trace file version.</summary>
    public static int ParseQueryHeader(in TraceEntry header, out int dbVersion)
    {
        if (header.Type != TraceType.Begin) throw RocksDbException.Corruption("Corrupted trace file. Incorrect header.");
        ReadOnlySpan<byte> p = header.Payload.Span;
        if (!p.StartsWith(MagicBytes)) throw RocksDbException.Corruption("Corrupted trace file. Incorrect magic.");
        string text = Encoding.ASCII.GetString(p);
        string[] parts = text.Split('\t');
        if (parts.Length < 4 || !parts[1].StartsWith("Trace Version: ", StringComparison.Ordinal)
            || !parts[2].StartsWith("RocksDB Version: ", StringComparison.Ordinal))
        {
            throw RocksDbException.Corruption("Corrupted trace file. Incorrect version format.");
        }
        int traceVersion = ParseVersionString(parts[1]["Trace Version: ".Length..]);
        dbVersion = ParseVersionString(parts[2]["RocksDB Version: ".Length..]);
        return traceVersion;
    }

    /// <summary>TracerHelper::ParseVersionStr: "major.minor" with exactly one dot, digits concatenated ("0.2" = 2).</summary>
    public static int ParseVersionString(string v)
    {
        int dot = v.IndexOf('.', StringComparison.Ordinal);
        if (dot < 0 || dot != v.LastIndexOf('.')) throw RocksDbException.Corruption("Corrupted trace file. Incorrect version format.");
        int n = 0;
        foreach (char c in v)
        {
            if (c == '.') continue;
            if (c is < '0' or > '9') throw RocksDbException.Corruption("Corrupted trace file. Incorrect version format");
            n = n * 10 + (c - '0');
        }
        return n;
    }

    /// <summary>TracerHelper::SetPayloadMap.</summary>
    public static void SetPayloadMap(ref ulong map, TracePayloadType type) => map |= 1UL << (int)type;

    // ---- TracedWriteOptions / TracedReadOptions (trace_replay.cc) -----------------------------------------

    /// <summary>TracedWriteOptions::EncodeTo (length-prefixed; later fields are appended).</summary>
    public static void EncodeWriteOptions(IBufferWriter<byte> dst, bool sync, bool disableWal, bool noSlowdown, Env.IoPriority priority)
    {
        uint flags = (sync ? WriteSyncBit : 0) | (disableWal ? WriteDisableWalBit : 0) | (noSlowdown ? WriteNoSlowdownBit : 0);
        Span<byte> body = stackalloc byte[2 * Coding.MaxVarint32Length];
        int n = Coding.EncodeVarint32(body, flags);
        n += Coding.EncodeVarint32(body[n..], (uint)priority);
        Coding.PutLengthPrefixedSlice(dst, body[..n]);
    }

    /// <summary>TracedWriteOptions::DecodeFrom.</summary>
    public static WriteOptions DecodeWriteOptions(ref ReadOnlySpan<byte> input)
    {
        if (!Coding.GetLengthPrefixedSlice(ref input, out ReadOnlySpan<byte> body) || !Coding.GetVarint32(ref body, out uint flags)
            || !Coding.GetVarint32(ref body, out uint priority) || priority > (uint)Env.IoPriority.Total)
        {
            throw RocksDbException.Corruption("Unable to decode the traced WriteOptions");
        }
        // rate_limiter_priority has no managed WriteOptions counterpart (WAL writes are not rate limited here).
        return new WriteOptions { Sync = (flags & WriteSyncBit) != 0, DisableWal = (flags & WriteDisableWalBit) != 0, NoSlowdown = (flags & WriteNoSlowdownBit) != 0 };
    }

    /// <summary>The kReadOptionsFlags bits of <paramref name="o"/> (bit i = i-th boolean field, append only).</summary>
    private static uint ReadOptionFlags(ReadOptions? o)
    {
        o ??= s_defaultReadOptions;
        uint f = 0;
        if (o.VerifyChecksums) f |= 1u << 0;
        if (o.FillCache) f |= 1u << 1;
        if (o.IgnoreRangeDeletions) f |= 1u << 2;
        if (o.AsyncIo) f |= 1u << 3;
        if (o.OptimizeMultiGetForIo) f |= 1u << 4;
        if (o.Tailing) f |= 1u << 5;
        if (o.TotalOrderSeek) f |= 1u << 6;
        if (o.AutoPrefixMode) f |= 1u << 7;
        if (o.PrefixSameAsStart) f |= 1u << 8;
        if (o.PinData) f |= 1u << 9;
        if (o.AdaptiveReadahead) f |= 1u << 10;
        if (o.BackgroundPurgeOnIteratorCleanup) f |= 1u << 11;
        if (o.AutoReadaheadSize) f |= 1u << 12;
        if (o.AllowUnpreparedValue) f |= 1u << 13;
        return f;
    }

    private static readonly ReadOptions s_defaultReadOptions = new();

    /// <summary>TracedReadOptions::EncodeTo.</summary>
    public static void EncodeReadOptions(IBufferWriter<byte> dst, ReadOptions? o)
    {
        ReadOptions r = o ?? s_defaultReadOptions;
        Span<byte> body = stackalloc byte[3 * Coding.MaxVarint32Length + 3 * Coding.MaxVarint64Length];
        int n = Coding.EncodeVarint32(body, ReadOptionFlags(r));
        n += Coding.EncodeVarint32(body[n..], (uint)r.ReadTier);
        n += Coding.EncodeVarint32(body[n..], (uint)r.RateLimiterPriority);
        n += Coding.EncodeVarint64(body[n..], r.ReadaheadSize);
        n += Coding.EncodeVarint64(body[n..], r.MaxSkippableInternalKeys);
        n += Coding.EncodeVarint64(body[n..], r.ValueSizeSoftLimit);
        Coding.PutLengthPrefixedSlice(dst, body[..n]);
    }

    /// <summary>TracedReadOptions::DecodeFrom.</summary>
    public static ReadOptions DecodeReadOptions(ref ReadOnlySpan<byte> input)
    {
        if (!Coding.GetLengthPrefixedSlice(ref input, out ReadOnlySpan<byte> body) || !Coding.GetVarint32(ref body, out uint f)
            || !Coding.GetVarint32(ref body, out uint tier) || tier > (uint)ReadTier.MemtableTier
            || !Coding.GetVarint32(ref body, out uint priority) || priority > (uint)Env.IoPriority.Total
            || !Coding.GetVarint64(ref body, out ulong readahead) || !Coding.GetVarint64(ref body, out ulong maxSkip)
            || !Coding.GetVarint64(ref body, out ulong softLimit))
        {
            throw RocksDbException.Corruption("Unable to decode the traced ReadOptions");
        }
        return new ReadOptions
        {
            VerifyChecksums = (f & (1u << 0)) != 0,
            FillCache = (f & (1u << 1)) != 0,
            IgnoreRangeDeletions = (f & (1u << 2)) != 0,
            AsyncIo = (f & (1u << 3)) != 0,
            OptimizeMultiGetForIo = (f & (1u << 4)) != 0,
            Tailing = (f & (1u << 5)) != 0,
            TotalOrderSeek = (f & (1u << 6)) != 0,
            AutoPrefixMode = (f & (1u << 7)) != 0,
            PrefixSameAsStart = (f & (1u << 8)) != 0,
            PinData = (f & (1u << 9)) != 0,
            AdaptiveReadahead = (f & (1u << 10)) != 0,
            BackgroundPurgeOnIteratorCleanup = (f & (1u << 11)) != 0,
            AutoReadaheadSize = (f & (1u << 12)) != 0,
            AllowUnpreparedValue = (f & (1u << 13)) != 0,
            ReadTier = (ReadTier)tier,
            RateLimiterPriority = (Env.IoPriority)priority,
            ReadaheadSize = readahead,
            MaxSkippableInternalKeys = maxSkip,
            ValueSizeSoftLimit = softLimit,
        };
    }

    // ---- TracerHelper::DecodeTraceRecord ------------------------------------------------------------------

    /// <summary>
    /// TracerHelper::DecodeTraceRecord: the query record of <paramref name="trace"/>. Throws NotSupported for other
    /// trace types and Corruption for malformed records.
    /// </summary>
    public static TraceRecord DecodeRecord(in TraceEntry trace, int traceFileVersion)
    {
        ReadOnlySpan<byte> buf = trace.Payload.Span;
        switch (trace.Type)
        {
            case TraceType.Write:
            {
                if (traceFileVersion < 2) return new WriteQueryTraceRecord(trace.Payload.ToArray(), null, trace.Timestamp);
                scoped ReadOnlySpan<byte> rep = default;
                WriteOptions? wo = null;
                ulong map = ReadPayloadMap(ref buf);
                while (map != 0)
                {
                    switch ((TracePayloadType)BitOperations.TrailingZeroCount(map))
                    {
                        case TracePayloadType.WriteBatchData: if (!Coding.GetLengthPrefixedSlice(ref buf, out rep)) throw BadSlice(); break;
                        case TracePayloadType.WriteOptions: wo = DecodeWriteOptions(ref buf); break;
                        default: map = 0; continue; // a field from a newer version: it and later ones are skipped
                    }
                    map &= map - 1;
                }
                return new WriteQueryTraceRecord(rep.ToArray(), wo, trace.Timestamp);
            }
            case TraceType.Get:
            {
                uint cf = 0;
                scoped ReadOnlySpan<byte> key = default;
                ReadOptions? ro = null;
                if (traceFileVersion < 2)
                {
                    Coding.GetFixed32(ref buf, out cf);
                    if (!Coding.GetLengthPrefixedSlice(ref buf, out key)) throw BadSlice();
                }
                else
                {
                    ulong map = ReadPayloadMap(ref buf);
                    while (map != 0)
                    {
                        switch ((TracePayloadType)BitOperations.TrailingZeroCount(map))
                        {
                            case TracePayloadType.GetCfId: GetFixed32(ref buf, out cf); break;
                            case TracePayloadType.GetKey: if (!Coding.GetLengthPrefixedSlice(ref buf, out key)) throw BadSlice(); break;
                            case TracePayloadType.GetReadOptions: ro = DecodeReadOptions(ref buf); break;
                            default: map = 0; continue;
                        }
                        map &= map - 1;
                    }
                }
                return new GetQueryTraceRecord(cf, key.ToArray(), ro, trace.Timestamp);
            }
            case TraceType.IteratorSeek:
            case TraceType.IteratorSeekForPrev:
            {
                uint cf = 0;
                scoped ReadOnlySpan<byte> key = default, lower = default, upper = default;
                ReadOptions? ro = null;
                if (traceFileVersion < 2)
                {
                    Coding.GetFixed32(ref buf, out cf);
                    if (!Coding.GetLengthPrefixedSlice(ref buf, out key)) throw BadSlice();
                }
                else
                {
                    ulong map = ReadPayloadMap(ref buf);
                    while (map != 0)
                    {
                        switch ((TracePayloadType)BitOperations.TrailingZeroCount(map))
                        {
                            case TracePayloadType.IterCfId: GetFixed32(ref buf, out cf); break;
                            case TracePayloadType.IterKey: if (!Coding.GetLengthPrefixedSlice(ref buf, out key)) throw BadSlice(); break;
                            case TracePayloadType.IterLowerBound: if (!Coding.GetLengthPrefixedSlice(ref buf, out lower)) throw BadSlice(); break;
                            case TracePayloadType.IterUpperBound: if (!Coding.GetLengthPrefixedSlice(ref buf, out upper)) throw BadSlice(); break;
                            case TracePayloadType.IterReadOptions: ro = DecodeReadOptions(ref buf); break;
                            default: map = 0; continue;
                        }
                        map &= map - 1;
                    }
                }
                var seekType = trace.Type == TraceType.IteratorSeek ? IteratorSeekType.Seek : IteratorSeekType.SeekForPrev;
                return new IteratorSeekQueryTraceRecord(seekType, cf, key.ToArray(), lower.ToArray(), upper.ToArray(), ro, trace.Timestamp);
            }
            case TraceType.MultiGet:
            {
                if (traceFileVersion < 2) throw RocksDbException.Corruption("MultiGet is not supported.");
                uint size = 0;
                scoped ReadOnlySpan<byte> cfIds = default, keys = default;
                ReadOptions? ro = null;
                ulong map = ReadPayloadMap(ref buf);
                while (map != 0)
                {
                    switch ((TracePayloadType)BitOperations.TrailingZeroCount(map))
                    {
                        case TracePayloadType.MultiGetSize: GetFixed32(ref buf, out size); break;
                        case TracePayloadType.MultiGetCfIds: if (!Coding.GetLengthPrefixedSlice(ref buf, out cfIds)) throw BadSlice(); break;
                        case TracePayloadType.MultiGetKeys: if (!Coding.GetLengthPrefixedSlice(ref buf, out keys)) throw BadSlice(); break;
                        case TracePayloadType.MultiGetReadOptions: ro = DecodeReadOptions(ref buf); break;
                        default: map = 0; continue;
                    }
                    map &= map - 1;
                }
                if (size == 0) throw RocksDbException.InvalidArgument("Empty MultiGet cf_ids or keys.");
                var ids = new uint[size];
                var ks = new byte[size][];
                for (int i = 0; i < size; i++)
                {
                    if (!Coding.GetFixed32(ref cfIds, out ids[i]) || !Coding.GetLengthPrefixedSlice(ref keys, out ReadOnlySpan<byte> k))
                        throw RocksDbException.Corruption("Unable to decode the traced MultiGet keys");
                    ks[i] = k.ToArray();
                }
                return new MultiGetQueryTraceRecord(ids, ks, ro, trace.Timestamp);
            }
            default:
                throw RocksDbException.NotSupported("Unsupported trace type.");
        }
    }

    private static ulong ReadPayloadMap(ref ReadOnlySpan<byte> buf)
    {
        if (!Coding.GetFixed64(ref buf, out ulong map)) throw RocksDbException.Corruption("Unable to decode the trace payload map");
        return map;
    }

    private static void GetFixed32(ref ReadOnlySpan<byte> buf, out uint v)
    {
        if (!Coding.GetFixed32(ref buf, out v)) throw RocksDbException.Corruption("Unable to decode a traced column family ID");
    }

    private static RocksDbException BadSlice() => RocksDbException.Corruption("Unable to decode a traced slice");
}
