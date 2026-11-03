using System.Buffers;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Tracing;

/// <summary>Who looked a block up (include/rocksdb/table_reader_caller.h TableReaderCaller). Values hit disk.</summary>
public enum TableReaderCaller : byte
{
    UserGet = 1,
    UserMultiGet = 2,
    UserIterator = 3,
    UserApproximateSize = 4,
    UserVerifyChecksum = 5,
    SstDumpTool = 6,
    ExternalSstIngestion = 7,
    Repair = 8,
    Prefetch = 9,
    Compaction = 10,
    CompactionRefill = 11,
    Flush = 12,
    SstFileReader = 13,
    Uncategorized = 14,
    MaxBlockCacheLookupCaller = 15,
}

/// <summary>One block cache access (include/rocksdb/block_cache_trace_writer.h BlockCacheTraceRecord).</summary>
public sealed class BlockCacheTraceRecord
{
    /// <summary>kReservedGetId.</summary>
    public const ulong ReservedGetId = 0;

    public ulong AccessTimestamp { get; set; }
    /// <summary>The block cache key (16 bytes for block-based tables).</summary>
    public byte[] BlockKey { get; set; } = [];
    public TraceType BlockType { get; set; } = TraceType.Max;
    public ulong BlockSize { get; set; }
    public ulong ColumnFamilyId { get; set; }
    public string ColumnFamilyName { get; set; } = "";
    public uint Level { get; set; }
    public ulong SstFileNumber { get; set; }
    public TableReaderCaller Caller { get; set; } = TableReaderCaller.MaxBlockCacheLookupCaller;
    public bool IsCacheHit { get; set; }
    public bool NoInsert { get; set; }
    // Get / MultiGet only.
    public ulong GetId { get; set; } = ReservedGetId;
    public bool GetFromUserSpecifiedSnapshot { get; set; }
    /// <summary>The internal key looked up (Get / MultiGet only).</summary>
    public byte[] ReferencedKey { get; set; } = [];
    // Get / MultiGet on data blocks only.
    public ulong ReferencedDataSize { get; set; }
    public ulong NumKeysInBlock { get; set; }
    public bool ReferencedKeyExistInBlock { get; set; }

    /// <summary>BlockCacheTraceHelper::IsGetOrMultiGet.</summary>
    public static bool IsGetOrMultiGet(TableReaderCaller c) => c is TableReaderCaller.UserGet or TableReaderCaller.UserMultiGet;

    /// <summary>BlockCacheTraceHelper::IsGetOrMultiGetOnDataBlock.</summary>
    public static bool IsGetOrMultiGetOnDataBlock(TraceType t, TableReaderCaller c) => t == TraceType.BlockDataBlock && IsGetOrMultiGet(c);
}

/// <summary>BlockCacheTraceOptions.</summary>
public sealed class BlockCacheTraceOptions
{
    /// <summary>Trace the accesses of one block out of this many (spatial downsampling by block key; 0/1: all).</summary>
    public ulong SamplingFrequency { get; set; } = 1;

    /// <summary>BlockCacheTraceWriterOptions::max_trace_file_size: records are dropped beyond this many bytes (default 64 GiB).</summary>
    public ulong MaxTraceFileSize { get; set; } = 64UL * 1024 * 1024 * 1024;
}

/// <summary>Header of a block cache / IO trace file (BlockCacheTraceHeader / IOTraceHeader).</summary>
public readonly record struct TraceFileHeader(ulong StartTime, uint RocksDbMajorVersion, uint RocksDbMinorVersion);

/// <summary>
/// Port of BlockCacheTracer + BlockCacheTraceWriterImpl (trace_replay/block_cache_tracer.cc). One per DB; table
/// readers check <see cref="IsEnabled"/> (one volatile read) before building a record.
/// <para>Synchronization: records are encoded and written under <see cref="_lock"/> (trace_writer_mutex_).</para>
/// </summary>
internal sealed class BlockCacheTracer
{
    private readonly Lock _lock = new();
    private volatile TraceWriter? _writer;
    private BlockCacheTraceOptions _options = new();
    private long _getIdCounter;
    private readonly PooledBufferWriter _payload = new(256);
    private readonly PooledBufferWriter _record = new(256);

    // The lookup context of the current logical call (upstream: BlockCacheLookupContext passed down the read path).
    // An AsyncLocal so that it flows across awaits; it is only read and written while a trace runs.
    private static readonly AsyncLocal<LookupContext?> s_context = new();

    private sealed class LookupContext(TableReaderCaller caller, ulong getId, byte[]? referencedKey, bool userSnapshot)
    {
        public readonly TableReaderCaller Caller = caller;
        public readonly ulong GetId = getId;
        public readonly byte[]? ReferencedKey = referencedKey;
        public readonly bool UserSnapshot = userSnapshot;
    }

    public bool IsEnabled => _writer is not null;

    /// <summary>BlockCacheTracer::StartTrace: Busy if a trace is already running; writes the header.</summary>
    public void StartTrace(BlockCacheTraceOptions options, TraceWriter writer)
    {
        lock (_lock)
        {
            if (_writer is not null) throw new RocksDbException(StatusCode.Busy, "Busy: ");
            Interlocked.Exchange(ref _getIdCounter, 1);
            _options = new BlockCacheTraceOptions { SamplingFrequency = options.SamplingFrequency, MaxTraceFileSize = options.MaxTraceFileSize };
            _payload.Clear();
            Coding.PutLengthPrefixedSlice(_payload, TraceFormat.MagicBytes);
            Coding.PutFixed32(_payload, (uint)OptionsFile.CurrentRocksDbVersion.Major);
            Coding.PutFixed32(_payload, (uint)OptionsFile.CurrentRocksDbVersion.Minor);
            _record.Clear();
            TraceFormat.EncodeTrace(_record, TraceFormat.NowMicros(), TraceType.Begin, _payload.WrittenSpan);
            writer.Write(_record.WrittenSpan);
            _writer = writer;
        }
    }

    /// <summary>BlockCacheTracer::EndTrace.</summary>
    public void EndTrace()
    {
        lock (_lock)
        {
            TraceWriter? w = _writer;
            if (w is null) return;
            _writer = null;
            w.Close();
        }
    }

    /// <summary>BlockCacheTracer::NextGetId (never kReservedGetId).</summary>
    public ulong NextGetId()
    {
        if (_writer is null) return BlockCacheTraceRecord.ReservedGetId;
        ulong v = (ulong)Interlocked.Increment(ref _getIdCounter) - 1;
        return v == BlockCacheTraceRecord.ReservedGetId ? (ulong)Interlocked.Increment(ref _getIdCounter) - 1 : v;
    }

    /// <summary>
    /// Sets the lookup context of the current logical call; returns the previous one for <see cref="RestoreCaller"/>
    /// (async methods need no restore: their AsyncLocal changes end with them).
    /// </summary>
    public static object? SetCaller(TableReaderCaller caller, ulong getId, ReadOnlySpan<byte> referencedKey, bool userSnapshot)
    {
        LookupContext? prev = s_context.Value;
        s_context.Value = new LookupContext(caller, getId, referencedKey.IsEmpty ? null : referencedKey.ToArray(), userSnapshot);
        return prev;
    }

    public static void RestoreCaller(object? previous) => s_context.Value = (LookupContext?)previous;

    /// <summary>The caller of the current lookups (iterators when no context was set).</summary>
    public static TableReaderCaller CurrentCaller => s_context.Value?.Caller ?? TableReaderCaller.UserIterator;

    /// <summary>
    /// BlockBasedTable::SaveLookupContextOrTraceRecord + FinishTraceRecord + BlockCacheTraceWriterImpl::WriteBlockAccess
    /// for one lookup by a table reader.
    /// </summary>
    public void TraceBlockAccess(in Cache.CacheKey cacheKey, TraceType blockType, ulong blockSize, ulong numKeysInBlock, bool isCacheHit, bool noInsert,
        ulong cfId, string cfName, uint level, ulong sstNumber)
    {
        Span<byte> blockKey = stackalloc byte[16];
        Coding.EncodeFixed64(blockKey, cacheKey.FileNumEtc64);
        Coding.EncodeFixed64(blockKey[8..], cacheKey.OffsetEtc64);
        // ShouldTrace: spatial downsampling so that a sampled block has its complete access history.
        ulong freq = _options.SamplingFrequency;
        if (freq > 1 && Hash.GetSliceRangedNPHash(blockKey, freq) != 0) return;
        LookupContext? ctx = s_context.Value;
        TableReaderCaller caller = ctx?.Caller ?? TableReaderCaller.UserIterator;
        bool get = BlockCacheTraceRecord.IsGetOrMultiGet(caller);
        lock (_lock)
        {
            TraceWriter? w = _writer;
            if (w is null || w.FileSize > _options.MaxTraceFileSize) return;
            _payload.Clear();
            Coding.PutLengthPrefixedSlice(_payload, blockKey);
            Coding.PutFixed64(_payload, blockSize);
            Coding.PutFixed64(_payload, cfId);
            Coding.PutLengthPrefixedSlice(_payload, System.Text.Encoding.UTF8.GetBytes(cfName));
            Coding.PutFixed32(_payload, level);
            Coding.PutFixed64(_payload, sstNumber);
            _payload.WriteByte((byte)caller);
            _payload.WriteByte(isCacheHit ? (byte)1 : (byte)0);
            _payload.WriteByte(noInsert ? (byte)1 : (byte)0);
            if (get)
            {
                Coding.PutFixed64(_payload, ctx!.GetId);
                _payload.WriteByte(ctx.UserSnapshot ? (byte)1 : (byte)0);
                Coding.PutLengthPrefixedSlice(_payload, ctx.ReferencedKey);
            }
            if (BlockCacheTraceRecord.IsGetOrMultiGetOnDataBlock(blockType, caller))
            {
                // Deviation: the access is recorded at lookup time, before the block is searched, so the referenced data
                // size and "key exists in block" are not known (0 / false). num_keys_in_block is estimated as upstream.
                Coding.PutFixed64(_payload, 0);
                Coding.PutFixed64(_payload, numKeysInBlock);
                _payload.WriteByte(0);
            }
            _record.Clear();
            TraceFormat.EncodeTrace(_record, TraceFormat.NowMicros(), blockType, _payload.WrittenSpan);
            try
            {
                w.Write(_record.WrittenSpan);
            }
            catch (Exception)
            {
                // Upstream ignores the status of block access writes (PermitUncheckedError).
            }
        }
    }
}

/// <summary>Port of BlockCacheTraceReader (trace_replay/block_cache_tracer.cc): reads a block cache trace file.</summary>
public sealed class BlockCacheTraceReader(TraceReader reader) : IDisposable
{
    private readonly ArrayBufferWriter<byte> _buffer = new(256);

    /// <summary>BlockCacheTraceReader::ReadHeader.</summary>
    public TraceFileHeader ReadHeader() => ReadTraceFileHeader(reader, _buffer);

    internal static TraceFileHeader ReadTraceFileHeader(TraceReader reader, ArrayBufferWriter<byte> buffer)
    {
        buffer.Clear();
        if (!reader.TryRead(buffer)) throw new RocksDbException(StatusCode.Incomplete, "Incomplete: empty trace");
        if (!TraceFormat.TryDecodeTrace(buffer.WrittenMemory, out TraceEntry trace)) throw new RocksDbException(StatusCode.Incomplete, "Incomplete: Decode trace string failed");
        ReadOnlySpan<byte> p = trace.Payload.Span;
        if (!Coding.GetLengthPrefixedSlice(ref p, out ReadOnlySpan<byte> magic))
            throw RocksDbException.Corruption("Corrupted header in the trace file: Failed to read the magic number.");
        if (!magic.SequenceEqual(TraceFormat.MagicBytes)) throw RocksDbException.Corruption("Corrupted header in the trace file: Magic number does not match.");
        if (!Coding.GetFixed32(ref p, out uint major)) throw RocksDbException.Corruption("Corrupted header in the trace file: Failed to read rocksdb major version number.");
        if (!Coding.GetFixed32(ref p, out uint minor)) throw RocksDbException.Corruption("Corrupted header in the trace file: Failed to read rocksdb minor version number.");
        if (!p.IsEmpty) throw RocksDbException.Corruption("Corrupted header in the trace file: The length of header is too long.");
        return new TraceFileHeader(trace.Timestamp, major, minor);
    }

    /// <summary>BlockCacheTraceReader::ReadAccess: false at the end of the trace; throws Incomplete for a truncated record.</summary>
    public bool TryReadAccess(out BlockCacheTraceRecord? record)
    {
        record = null;
        _buffer.Clear();
        if (!reader.TryRead(_buffer)) return false;
        if (!TraceFormat.TryDecodeTrace(_buffer.WrittenMemory, out TraceEntry trace)) throw new RocksDbException(StatusCode.Incomplete, "Incomplete: Decode trace string failed");
        ReadOnlySpan<byte> p = trace.Payload.Span;
        var r = new BlockCacheTraceRecord { AccessTimestamp = trace.Timestamp, BlockType = trace.Type };
        if (!Coding.GetLengthPrefixedSlice(ref p, out ReadOnlySpan<byte> blockKey)) throw Incomplete("block key");
        r.BlockKey = blockKey.ToArray();
        if (!Coding.GetFixed64(ref p, out ulong blockSize)) throw Incomplete("block size");
        r.BlockSize = blockSize;
        if (!Coding.GetFixed64(ref p, out ulong cfId)) throw Incomplete("column family ID");
        r.ColumnFamilyId = cfId;
        if (!Coding.GetLengthPrefixedSlice(ref p, out ReadOnlySpan<byte> cfName)) throw Incomplete("column family name");
        r.ColumnFamilyName = System.Text.Encoding.UTF8.GetString(cfName);
        if (!Coding.GetFixed32(ref p, out uint level)) throw Incomplete("level");
        r.Level = level;
        if (!Coding.GetFixed64(ref p, out ulong sst)) throw Incomplete("SST file number");
        r.SstFileNumber = sst;
        if (p.Length < 3) throw Incomplete("caller");
        r.Caller = (TableReaderCaller)p[0];
        r.IsCacheHit = p[1] != 0;
        r.NoInsert = p[2] != 0;
        p = p[3..];
        if (BlockCacheTraceRecord.IsGetOrMultiGet(r.Caller))
        {
            if (!Coding.GetFixed64(ref p, out ulong getId)) throw Incomplete("the get id");
            r.GetId = getId;
            if (p.IsEmpty) throw Incomplete("get_from_user_specified_snapshot");
            r.GetFromUserSpecifiedSnapshot = p[0] != 0;
            p = p[1..];
            if (!Coding.GetLengthPrefixedSlice(ref p, out ReadOnlySpan<byte> rk)) throw Incomplete("the referenced key");
            r.ReferencedKey = rk.ToArray();
        }
        if (BlockCacheTraceRecord.IsGetOrMultiGetOnDataBlock(r.BlockType, r.Caller))
        {
            if (!Coding.GetFixed64(ref p, out ulong ds)) throw Incomplete("the referenced data size");
            r.ReferencedDataSize = ds;
            if (!Coding.GetFixed64(ref p, out ulong nk)) throw Incomplete("the number of keys in the block");
            r.NumKeysInBlock = nk;
            if (p.IsEmpty) throw Incomplete("referenced_key_exist_in_block");
            r.ReferencedKeyExistInBlock = p[0] != 0;
        }
        record = r;
        return true;
    }

    private static RocksDbException Incomplete(string what) => new(StatusCode.Incomplete, "Incomplete: Incomplete access record: Failed to read " + what + ".");

    public void Dispose() => reader.Dispose();
}
