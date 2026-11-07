using System.Buffers;
using System.Globalization;
using RocksDb.Managed.Env;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Cache;

// Port of include/rocksdb/utilities/cache_dump_load.h, utilities/cache_dump_load.cc and
// utilities/cache_dump_load_impl.{h,cc}. EXPERIMENTAL upstream: the classes and the dump format may change.
//
// Dump format (byte-identical with upstream on little-endian hosts): a sequence of dump units, each written as
//   WriteMetadata(fixed32 sequence_num | fixed32 crc32c(dump unit) | fixed64 dump unit size)
//   WritePacket(fixed64 timestamp | u8 CacheDumpUnitType | varint32 key size + key | fixed32 value size |
//               fixed32 crc32c(value) | varint32 value size + value)
// The first unit is the header (key "header", type kHeader), the last the footer (key "footer", type kFooter, value
// "cache dump completed"). The file writer prefixes every metadata / packet with its fixed32 length. The crc32c values
// are unmasked. Cache keys are the 16-byte block cache keys; values are the entries' saved forms
// (CacheItemHelper::saveto_cb: uncompressed block contents, raw filter payloads).

/// <summary>CacheDumpUnitType (utilities/cache_dump_load_impl.h). Values hit the dump file.</summary>
public enum CacheDumpUnitType : byte
{
    Header = 1,
    Footer = 2,
    Data = 3,
    Filter = 4,
    Properties = 5,
    CompressionDictionary = 6,
    RangeDeletion = 7,
    HashIndexPrefixes = 8,
    HashIndexMetadata = 9,
    MetaIndex = 10,
    Index = 11,
    /// <summary>Obsolete / deprecated.</summary>
    DeprecatedFilterBlock = 12,
    FilterMetaBlock = 13,
    BlockTypeMax = 14,
}

/// <summary>
/// Options of the cache dumper and loader (rocksdb::CacheDumpOptions). EXPERIMENTAL upstream.
/// </summary>
public sealed class CacheDumpOptions
{
    /// <summary>The clock that timestamps dump units and checks <see cref="Deadline"/> (clock).</summary>
    public TimeProvider Clock { get; init; } = TimeProvider.System;

    /// <summary>
    /// Entries visited at or after this time (of <see cref="Clock"/>) are not dumped (deadline, compared with the clock's
    /// NowMicros as upstream). Null: no deadline.
    /// </summary>
    public DateTimeOffset? Deadline { get; init; }

    /// <summary>
    /// Once more than this many value bytes were dumped, further entries are skipped (max_size_bytes; checked before each
    /// entry, so the total may exceed it by one entry). 0: no limit.
    /// </summary>
    public long MaxSizeBytes { get; init; }

    internal static ulong NowMicros(TimeProvider clock) =>
        (ulong)((clock.GetUtcNow() - DateTimeOffset.UnixEpoch).Ticks / TimeSpan.TicksPerMicrosecond);
}

/// <summary>
/// Stores or transfers the dump produced by a <see cref="CacheDumper"/> (rocksdb::CacheDumpWriter). One
/// <see cref="WriteMetadata"/> precedes every <see cref="WritePacket"/>. Implement it to send dumps elsewhere (e.g. over
/// the network); <see cref="CacheDump.NewToFileWriter"/> writes them to a file. Errors are thrown as
/// <see cref="RocksDbException"/>. Not thread-safe.
/// </summary>
public abstract class CacheDumpWriter : IDisposable
{
    /// <summary>Writes the serialized metadata of the next dump unit (WriteMetadata).</summary>
    public abstract void WriteMetadata(ReadOnlySpan<byte> metadata);

    /// <summary>Writes a serialized dump unit (WritePacket).</summary>
    public abstract void WritePacket(ReadOnlySpan<byte> data);

    /// <summary>Completes the dump (Close; the file writer syncs the file). Idempotent.</summary>
    public abstract void Close();

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>Releases resources; the default closes the writer.</summary>
    protected virtual void Dispose(bool disposing)
    {
        if (disposing) Close();
    }
}

/// <summary>
/// Reads back what a <see cref="CacheDumpWriter"/> stored (rocksdb::CacheDumpReader): <see cref="ReadMetadata"/> before
/// every <see cref="ReadPacket"/>. Errors (including a truncated stream) are thrown as <see cref="RocksDbException"/>.
/// Not thread-safe.
/// </summary>
public abstract class CacheDumpReader : IDisposable
{
    /// <summary>Appends the next metadata record to <paramref name="destination"/> (ReadMetadata).</summary>
    public abstract void ReadMetadata(IBufferWriter<byte> destination);

    /// <summary>Appends the next packet to <paramref name="destination"/> (ReadPacket).</summary>
    public abstract void ReadPacket(IBufferWriter<byte> destination);

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing) { }
}

/// <summary>
/// Dumps the blocks held by a block cache to a <see cref="CacheDumpWriter"/> (rocksdb::CacheDumper), e.g. before a DB
/// moves to another host, so the destination can warm its cache with <see cref="CacheDumpedLoader"/> before reopening it.
/// EXPERIMENTAL upstream.
/// </summary>
public abstract class CacheDumper : IDisposable
{
    /// <summary>
    /// Only dumps blocks of the table files of <paramref name="databases"/> (SetDumpFilter); the cache may be shared by
    /// other DBs. NotSupported unless overridden.
    /// </summary>
    public virtual void SetDumpFilter(IReadOnlyList<RocksDatabase> databases) => ThrowHelper.NotSupported("SetDumpFilter is not supported");

    /// <summary>
    /// Dumps every block that passes the filter, in one pass over the cache, then closes the writer
    /// (DumpCacheEntriesToWriter). NotSupported unless overridden.
    /// </summary>
    public virtual void DumpCacheEntriesToWriter() => ThrowHelper.NotSupported("DumpCacheEntriesToWriter is not supported");

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing) { }
}

/// <summary>
/// Loads a dump into a secondary cache (rocksdb::CacheDumpedLoader); a DB whose block cache uses that secondary cache
/// then finds the blocks there. EXPERIMENTAL upstream.
/// </summary>
public abstract class CacheDumpedLoader : IDisposable
{
    /// <summary>Inserts every dumped block into the secondary cache (RestoreCacheEntriesToSecondaryCache).</summary>
    public virtual void RestoreCacheEntriesToSecondaryCache() =>
        ThrowHelper.NotSupported("RestoreCacheEntriesToSecondaryCache is not supported");

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing) { }
}

/// <summary>Factories of the default cache dump / load implementations (utilities/cache_dump_load.cc).</summary>
public static class CacheDump
{
    /// <summary>kCacheDumpMajorVersion.</summary>
    public const int MajorVersion = 0;

    /// <summary>kCacheDumpMinorVersion.</summary>
    public const int MinorVersion = 1;

    /// <summary>A writer that stores the dump sequentially in a new file (NewToFileCacheDumpWriter).</summary>
    public static CacheDumpWriter NewToFileWriter(IFileSystem fileSystem, string fileName)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentNullException.ThrowIfNull(fileName);
        return new ToFileCacheDumpWriter(fileSystem.CreateWritable(fileName, truncate: true));
    }

    /// <summary>A reader of a dump file written by <see cref="NewToFileWriter"/> (NewFromFileCacheDumpReader).</summary>
    public static CacheDumpReader NewFromFileReader(IFileSystem fileSystem, string fileName)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentNullException.ThrowIfNull(fileName);
        return new FromFileCacheDumpReader(fileSystem.OpenRandomAccess(fileName));
    }

    /// <summary>
    /// The default dumper of <paramref name="cache"/> into <paramref name="writer"/> (NewDefaultCacheDumper). The dumper
    /// owns the writer.
    /// </summary>
    public static CacheDumper NewDefaultDumper(CacheDumpOptions options, BlockCache cache, CacheDumpWriter writer) =>
        new CacheDumperImpl(options, cache, writer);

    /// <summary>
    /// The default loader of the dump read by <paramref name="reader"/> into <paramref name="secondaryCache"/>
    /// (NewDefaultCacheDumpedLoader). <paramref name="tableOptions"/> is unused (as upstream). The loader owns the reader.
    /// </summary>
    public static CacheDumpedLoader NewDefaultLoader(CacheDumpOptions options, BlockBasedTableOptions? tableOptions, SecondaryCache secondaryCache,
        CacheDumpReader reader) =>
        new CacheDumpedLoaderImpl(options, secondaryCache, reader);
}

/// <summary>DumpUnitMeta: serialized to 16 bytes.</summary>
internal struct DumpUnitMeta
{
    public const int EncodedSize = 16;

    /// <summary>Monotonically increasing order of the units written; the header is 0.</summary>
    public uint SequenceNum;
    /// <summary>Crc32c of the serialized dump unit.</summary>
    public uint DumpUnitChecksum;
    /// <summary>Size of the serialized dump unit.</summary>
    public ulong DumpUnitSize;
}

/// <summary>DumpUnit: a block and its information (the decoded form refers into the packet buffer).</summary>
internal ref struct DumpUnit
{
    public ulong Timestamp;
    public CacheDumpUnitType Type;
    public ReadOnlySpan<byte> Key;
    public uint ValueChecksum;
    public ReadOnlySpan<byte> Value;
}

/// <summary>CacheDumperHelper: (de)serialization of dump units and their metadata.</summary>
internal static class CacheDumperHelper
{
    public static void EncodeDumpUnitMeta(in DumpUnitMeta meta, Span<byte> destination)
    {
        Coding.EncodeFixed32(destination, meta.SequenceNum);
        Coding.EncodeFixed32(destination[4..], meta.DumpUnitChecksum);
        Coding.EncodeFixed64(destination[8..], meta.DumpUnitSize);
    }

    public static void EncodeDumpUnit(ulong timestamp, CacheDumpUnitType type, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, uint valueChecksum,
        IBufferWriter<byte> destination)
    {
        Coding.PutFixed64(destination, timestamp);
        destination.GetSpan(1)[0] = (byte)type;
        destination.Advance(1);
        Coding.PutLengthPrefixedSlice(destination, key);
        Coding.PutFixed32(destination, (uint)value.Length);
        Coding.PutFixed32(destination, valueChecksum);
        Coding.PutLengthPrefixedSlice(destination, value);
    }

    /// <summary>DecodeDumpUnitMeta; Incomplete when <paramref name="encoded"/> is too short.</summary>
    public static void DecodeDumpUnitMeta(ReadOnlySpan<byte> encoded, out DumpUnitMeta meta)
    {
        meta = default;
        if (!Coding.GetFixed32(ref encoded, out meta.SequenceNum)) ThrowIncomplete("Decode dumped unit meta sequence_num failed");
        if (!Coding.GetFixed32(ref encoded, out meta.DumpUnitChecksum)) ThrowIncomplete("Decode dumped unit meta dump_unit_checksum failed");
        if (!Coding.GetFixed64(ref encoded, out meta.DumpUnitSize)) ThrowIncomplete("Decode dumped unit meta dump_unit_size failed");
    }

    /// <summary>DecodeDumpUnit; Incomplete when <paramref name="encoded"/> is malformed.</summary>
    public static DumpUnit DecodeDumpUnit(ReadOnlySpan<byte> encoded)
    {
        ReadOnlySpan<byte> rest = encoded;
        if (!Coding.GetFixed64(ref rest, out ulong timestamp) || rest.IsEmpty) ThrowIncomplete("Decode dumped unit string failed");
        var type = (CacheDumpUnitType)rest[0];
        rest = rest[1..];
        if (!Coding.GetVarint32(ref rest, out uint keyLen) || keyLen > (uint)rest.Length) ThrowIncomplete("Decode dumped unit string failed");
        int keyOffset = encoded.Length - rest.Length;
        rest = rest[(int)keyLen..];
        if (!Coding.GetFixed32(ref rest, out uint valueLen)) ThrowIncomplete("Decode dumped unit string failed");
        if (!Coding.GetFixed32(ref rest, out uint valueChecksum)) ThrowIncomplete("Decode dumped unit string failed");
        if (!Coding.GetVarint32(ref rest, out uint blockLen) || blockLen > (uint)rest.Length) ThrowIncomplete("Decode dumped unit string failed");
        int valueOffset = encoded.Length - rest.Length;
        // Upstream asserts block.size() == value_len.
        if (blockLen != valueLen) ThrowIncomplete("Decode dumped unit string failed");
        return new DumpUnit
        {
            Timestamp = timestamp, Type = type, Key = encoded.Slice(keyOffset, (int)keyLen), ValueChecksum = valueChecksum,
            Value = encoded.Slice(valueOffset, (int)blockLen),
        };
    }

    private static void ThrowIncomplete(string message) => throw new RocksDbException(StatusCode.Incomplete, "Result incomplete: " + message);
}

/// <summary>CacheDumperImpl: dumps the cache entries with a persistable form through <see cref="BlockCache.ApplyToAllEntries{TState}(TState, CacheEntryVisitor{TState})"/>.</summary>
internal sealed class CacheDumperImpl : CacheDumper
{
    private readonly CacheDumpOptions _options;
    private readonly BlockCache? _cache;
    private CacheDumpWriter? _writer;
    // The cache key prefix filter: the common prefix (first 8 key bytes) of the stable base cache keys of the filtered
    // DBs' table files. Upstream keeps them in a std::set<std::string>.
    private readonly HashSet<ulong> _prefixFilter = new();
    private bool _dumpAllKeys = true;
    private uint _sequenceNum;
    private long _dumpedSizeBytes;
    private ulong _deadlineMicros;
    private byte[] _buf = [];
    private PooledBufferWriter? _encoded;
    private Exception? _writeError;

    public CacheDumperImpl(CacheDumpOptions options, BlockCache? cache, CacheDumpWriter? writer)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _cache = cache;
        _writer = writer;
    }

    /// <summary>
    /// SetDumpFilter: collects the stable base cache key prefixes (BlockBasedTable::SetupBaseCacheKey with is_stable) of
    /// the live table files of <paramref name="databases"/>. Only entries with stable keys are portable to another
    /// DB::Open. Deviation: upstream reads the table properties of the default column family only
    /// (DB::GetPropertiesOfAllTables(TablePropertiesCollection*)); the port includes every column family.
    /// </summary>
    public override void SetDumpFilter(IReadOnlyList<RocksDatabase> databases)
    {
        ArgumentNullException.ThrowIfNull(databases);
        _dumpAllKeys = false;
        foreach (RocksDatabase db in databases)
        {
            ArgumentNullException.ThrowIfNull(db, nameof(databases));
            foreach (ColumnFamily cf in db.ColumnFamilies)
            {
                foreach (Table.TableProperties props in cf.GetPropertiesOfAllTables().Values)
                {
                    if (Table.BlockBasedTableReader.TryGetStableBaseCacheKey(props, out OffsetableCacheKey baseKey))
                        _prefixFilter.Add(baseKey.CommonPrefix);
                }
            }
        }
    }

    public override void DumpCacheEntriesToWriter()
    {
        // Prepare stage, check the parameters.
        if (_cache is null) ThrowHelper.InvalidArgument("Cache is null");
        if (_writer is null) ThrowHelper.InvalidArgument("CacheDumpWriter is null");
        if (_options.Clock is null) ThrowHelper.InvalidArgument("System clock is null");
        _deadlineMicros = _options.Deadline is { } d ? (ulong)Math.Max(0, (d - DateTimeOffset.UnixEpoch).Ticks / TimeSpan.TicksPerMicrosecond) : 0;
        _sequenceNum = 0;
        _writeError = null;
        _encoded ??= new PooledBufferWriter(4096);
        try
        {
            // Dump stage: the header, then every entry that is not filtered out, then the footer.
            WriteHeader();
            _cache!.ApplyToAllEntries(this, static (CacheDumperImpl self, in CacheKey key, in CacheValue value, long charge, CacheItemHelper? helper) =>
                self.DumpOneBlock(key, value, helper));
            // Upstream ignores the status of each block write (the footer write then reports a broken writer); the port
            // stops writing after the first failure and reports it.
            if (_writeError is not null) throw _writeError;
            WriteFooter();
            _writer!.Close();
        }
        finally
        {
            if (_buf.Length > 0) RocksArrayPool<byte>.Shared.Return(_buf);
            _buf = [];
        }
    }

    // DumpOneBlockCallBack.
    private void DumpOneBlock(in CacheKey key, in CacheValue value, CacheItemHelper? helper)
    {
        if (_writeError is not null) return;
        // Not compatible with dumping (no size_cb / saveto_cb). Skip this entry.
        if (helper is null || !helper.IsSecondaryCacheCompatible) return;
        if (_options.MaxSizeBytes > 0 && _dumpedSizeBytes > _options.MaxSizeBytes) return;
        ulong timestamp = CacheDumpOptions.NowMicros(_options.Clock);
        if (_deadlineMicros != 0 && timestamp >= _deadlineMicros) return;
        CacheDumpUnitType type;
        switch (helper.Role)
        {
            case CacheEntryRole.DataBlock: type = CacheDumpUnitType.Data; break;
            case CacheEntryRole.FilterBlock: type = CacheDumpUnitType.Filter; break;
            case CacheEntryRole.FilterMetaBlock: type = CacheDumpUnitType.FilterMetaBlock; break;
            case CacheEntryRole.IndexBlock: type = CacheDumpUnitType.Index; break;
            default: return; // filter out other entries
        }
        // Based on the key prefix, check if the block should be filtered out.
        if (!_dumpAllKeys && !_prefixFilter.Contains(key.FileNumEtc64)) return;
        // Use the cache item helper to get the persistable data.
        int len = helper.GetSaveSize(value);
        if (_buf.Length < len)
        {
            if (_buf.Length > 0) RocksArrayPool<byte>.Shared.Return(_buf);
            _buf = RocksArrayPool<byte>.Shared.Rent(Math.Max(len, 4096));
        }
        helper.SaveTo(value, _buf.AsSpan(0, len));
        Span<byte> k = stackalloc byte[CacheKey.Size];
        key.WriteTo(k);
        try
        {
            WriteBlock(type, k, _buf.AsSpan(0, len), timestamp);
        }
        catch (Exception e) when (e is RocksDbException or IOException or ObjectDisposedException)
        {
            _writeError = e;
            return;
        }
        _dumpedSizeBytes += len;
    }

    // WriteBlock: serializes the dump unit, then writes its metadata (sequence number, unit checksum, unit size) and the
    // unit itself.
    private void WriteBlock(CacheDumpUnitType type, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ulong timestamp)
    {
        uint valueChecksum = Crc32C.Value(value);
        PooledBufferWriter encoded = _encoded!;
        encoded.Clear();
        CacheDumperHelper.EncodeDumpUnit(timestamp, type, key, value, valueChecksum, encoded);
        var meta = new DumpUnitMeta
        {
            SequenceNum = _sequenceNum++,
            DumpUnitChecksum = Crc32C.Value(encoded.WrittenSpan),
            DumpUnitSize = (ulong)encoded.WrittenCount,
        };
        Span<byte> m = stackalloc byte[DumpUnitMeta.EncodedSize];
        CacheDumperHelper.EncodeDumpUnitMeta(meta, m);
        _writer!.WriteMetadata(m);
        _writer.WritePacket(encoded.WrittenSpan);
    }

    // WriteHeader: the dump format version, RocksDB version and a brief intro.
    private void WriteHeader()
    {
        Version v = OptionsFile.CurrentRocksDbVersion;
        string header = Tracing.TraceFormat.Magic + "\tCache dump format version: " +
                        CacheDump.MajorVersion.ToString(CultureInfo.InvariantCulture) + "." + CacheDump.MinorVersion.ToString(CultureInfo.InvariantCulture) +
                        "\tRocksDB Version: " + v.Major.ToString(CultureInfo.InvariantCulture) + "." + v.Minor.ToString(CultureInfo.InvariantCulture) +
                        "\tFormat: dump_unit_metadata <sequence_number, dump_unit_checksum, dump_unit_size>, dump_unit <timestamp, key, block_type, " +
                        "block_size, block_data, block_checksum> cache_value\n";
        WriteBlock(CacheDumpUnitType.Header, "header"u8, System.Text.Encoding.ASCII.GetBytes(header), CacheDumpOptions.NowMicros(_options.Clock));
    }

    // WriteFooter: indicates the end of the dump.
    private void WriteFooter() =>
        WriteBlock(CacheDumpUnitType.Footer, "footer"u8, "cache dump completed"u8, CacheDumpOptions.NowMicros(_options.Clock));

    protected override void Dispose(bool disposing)
    {
        if (!disposing) return;
        _writer?.Dispose();
        _writer = null;
        _encoded?.Dispose();
        _encoded = null;
    }
}

/// <summary>CacheDumpedLoaderImpl: reads the dump units and inserts them into the secondary cache with InsertSaved.</summary>
internal sealed class CacheDumpedLoaderImpl : CacheDumpedLoader
{
    private readonly CacheDumpOptions _options;
    private readonly SecondaryCache? _secondaryCache;
    private CacheDumpReader? _reader;

    public CacheDumpedLoaderImpl(CacheDumpOptions options, SecondaryCache? secondaryCache, CacheDumpReader? reader)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _secondaryCache = secondaryCache;
        _reader = reader;
    }

    /// <summary>
    /// RestoreCacheEntriesToSecondaryCache: checks the arguments, reads (and verifies) the header, then inserts every
    /// block until the footer. A dump without footer ends with the reader's error (Corruption for a truncated file).
    /// As upstream, the header's format and RocksDB versions are not checked and the options are not used.
    /// </summary>
    public override void RestoreCacheEntriesToSecondaryCache()
    {
        _ = _options;
        // Step 1: check that all the arguments are valid.
        if (_secondaryCache is null) ThrowHelper.InvalidArgument("Secondary Cache is null");
        if (_reader is null) ThrowHelper.InvalidArgument("CacheDumpReader is null");
        var meta = new PooledBufferWriter(DumpUnitMeta.EncodedSize);
        var data = new PooledBufferWriter(4096);
        try
        {
            // Step 2: read the header.
            ReadDumpUnit(meta, data, "Read header unit corrupted!");
            // Step 3: read the rest of the blocks until the footer.
            while (true)
            {
                DumpUnit unit = ReadDumpUnit(meta, data, "Checksum does not match! Read dumped unit corrupted!");
                if (unit.Type == CacheDumpUnitType.Footer) return;
                // The value is the block's saved form (there is no block trailer here as in a block-based SST file).
                _secondaryCache!.InsertSaved(unit.Key, unit.Value, Compression.CompressionType.None, CacheTier.VolatileTier);
            }
        }
        finally
        {
            meta.Dispose();
            data.Dispose();
        }
    }

    // ReadDumpUnitMeta + ReadDumpUnit + the unit checksum check of ReadHeader / ReadCacheBlock.
    private DumpUnit ReadDumpUnit(PooledBufferWriter meta, PooledBufferWriter data, string checksumMessage)
    {
        meta.Clear();
        data.Clear();
        _reader!.ReadMetadata(meta);
        CacheDumperHelper.DecodeDumpUnitMeta(meta.WrittenSpan, out DumpUnitMeta unitMeta);
        _reader.ReadPacket(data);
        if ((ulong)data.WrittenCount != unitMeta.DumpUnitSize)
            ThrowHelper.Corruption("The data being read out does not match the size stored in metadata!");
        DumpUnit unit = CacheDumperHelper.DecodeDumpUnit(data.WrittenSpan);
        if (Crc32C.Value(data.WrittenSpan) != unitMeta.DumpUnitChecksum) ThrowHelper.Corruption(checksumMessage);
        return unit;
    }

    protected override void Dispose(bool disposing)
    {
        if (!disposing) return;
        _reader?.Dispose();
        _reader = null;
    }
}

/// <summary>ToFileCacheDumpWriter: writes each metadata record / packet with a fixed32 length prefix.</summary>
internal sealed class ToFileCacheDumpWriter(IWritableFile file) : CacheDumpWriter
{
    private IWritableFile? _file = file;
    private bool _seenError;

    public override void WriteMetadata(ReadOnlySpan<byte> metadata) => Append(metadata);

    public override void WritePacket(ReadOnlySpan<byte> data) => Append(data);

    private void Append(ReadOnlySpan<byte> data)
    {
        IWritableFile f = _file ?? throw new ObjectDisposedException(nameof(ToFileCacheDumpWriter));
        Span<byte> prefix = stackalloc byte[4];
        Coding.EncodeFixed32(prefix, (uint)data.Length);
        try
        {
            f.Append(prefix);
            f.Append(data);
        }
        catch
        {
            _seenError = true;
            throw;
        }
    }

    // Close: sync unless a write failed, then release the file.
    public override void Close()
    {
        IWritableFile? f = _file;
        if (f is null) return;
        _file = null;
        try
        {
            if (!_seenError)
            {
                f.Flush();
                f.Sync();
            }
        }
        finally
        {
            f.Dispose();
        }
    }
}

/// <summary>FromFileCacheDumpReader: reads length-prefixed records sequentially (the offset is kept internally).</summary>
internal sealed class FromFileCacheDumpReader(IRandomAccessFile file) : CacheDumpReader
{
    private const int SizePrefixLength = 4;
    private IRandomAccessFile? _file = file;
    private long _offset;

    public override void ReadMetadata(IBufferWriter<byte> destination) => ReadRecord(destination);

    public override void ReadPacket(IBufferWriter<byte> destination) => ReadRecord(destination);

    private void ReadRecord(IBufferWriter<byte> destination)
    {
        Span<byte> prefix = stackalloc byte[SizePrefixLength];
        Read(prefix);
        uint len = Coding.DecodeFixed32(prefix);
        // Upstream reads the record and fails on the short read; checking the length first avoids a huge buffer for a
        // corrupted prefix.
        if (len > int.MaxValue || _offset + len > _file!.Length) ThrowHelper.Corruption("Corrupted cache dump file.");
        if (len == 0) return;
        Span<byte> dst = destination.GetSpan((int)len)[..(int)len];
        Read(dst);
        destination.Advance((int)len);
    }

    private void Read(Span<byte> dst)
    {
        IRandomAccessFile f = _file ?? throw new ObjectDisposedException(nameof(FromFileCacheDumpReader));
        int n = Table.BlockFetcher.ReadFully(f, _offset, dst);
        if (n < dst.Length) ThrowHelper.Corruption("Corrupted cache dump file.");
        _offset += n;
    }

    protected override void Dispose(bool disposing)
    {
        if (!disposing) return;
        _file?.Dispose();
        _file = null;
    }
}
