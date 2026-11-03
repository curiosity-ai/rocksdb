using RocksDb.Managed.Cache;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;
using RocksDb.Managed.Util;

namespace RocksDb.Managed;

/// <summary>Describes a finished external SST file (rocksdb::ExternalSstFileInfo, include/rocksdb/sst_file_writer.h).</summary>
public sealed record ExternalSstFileInfo(
    string FilePath,
    byte[] SmallestKey,
    byte[] LargestKey,
    byte[] SmallestRangeDelKey,
    byte[] LargestRangeDelKey,
    ulong SequenceNumber,
    ulong FileSize,
    ulong NumEntries,
    ulong NumRangeDelEntries,
    int Version);

/// <summary>
/// Writes an SST file that can be ingested with <see cref="RocksDatabase.IngestExternalFilesAsync(IReadOnlyList{string}, IngestExternalFileOptions, CancellationToken)"/>
/// (or by native RocksDB). Port of rocksdb::SstFileWriter (table/sst_file_writer.cc): point entries must be added
/// in strictly increasing user-key order (by the column family's comparator); all keys get sequence number 0 and the
/// file carries the external-file properties (version 2, global seqno 0). Range deletions may be added in any order.
/// Not thread-safe.
/// </summary>
public sealed class SstFileWriter : IDisposable, IAsyncDisposable
{
    private readonly ColumnFamilyOptions _options;
    private readonly IFileSystem _fs;
    private readonly Comparator _ucmp;
    private readonly InternalKeyComparator _icmp;
    private readonly TableFormatOptions _format;
    private readonly string _columnFamilyName;
    private readonly ulong _columnFamilyId;
    private TableBuilder? _builder;
    private IWritableFile? _file;
    private string _path = "";
    private readonly PooledBufferWriter _key = new(64);
    private byte[] _smallestKey = [];
    private readonly PooledBufferWriter _largestKey = new(64);
    private byte[] _smallestRangeDel = [];
    private byte[] _largestRangeDel = [];
    private ulong _numEntries;
    private ulong _numRangeDelEntries;

    /// <summary>Creates a writer for files ingested into a column family configured like <paramref name="options"/>.</summary>
    public SstFileWriter(ColumnFamilyOptions options) : this(options, LocalFileSystem.Instance)
    {
    }

    public SstFileWriter(ColumnFamilyOptions options, IFileSystem fileSystem) : this(options, fileSystem, null)
    {
    }

    /// <summary>
    /// Creates a writer whose files record <paramref name="columnFamily"/> (name and id) in their properties, like
    /// SstFileWriter with a column family handle.
    /// </summary>
    public SstFileWriter(ColumnFamilyOptions options, IFileSystem fileSystem, ColumnFamily? columnFamily)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(fileSystem);
        _options = options;
        _fs = fileSystem;
        _ucmp = TableFactory.ResolveComparator(options);
        _icmp = _ucmp.Kind == ComparatorKind.Bytewise ? InternalKeyComparator.Bytewise : new InternalKeyComparator(_ucmp);
        _format = TableFactory.CreateFormatOptions(options.TableOptions);
        _columnFamilyName = columnFamily?.Name ?? "";
        _columnFamilyId = columnFamily?.Id ?? TableProperties.UnknownColumnFamily;
    }

    /// <summary>Size of the file written so far.</summary>
    public ulong FileSize => _builder?.FileSize ?? 0;

    /// <summary>Port of SstFileWriter::Open: creates (truncates) <paramref name="filePath"/>.</summary>
    public void Open(string filePath)
    {
        ArgumentException.ThrowIfNullOrEmpty(filePath);
        if (_builder is not null) ThrowHelper.InvalidArgument("SstFileWriter is already open");
        ulong now = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        TableBuilderOptions tbo = TableFactory.CreateExternalSstFileBuilderOptions(_options) with
        {
            ColumnFamilyId = _columnFamilyId,
            ColumnFamilyName = _columnFamilyName,
            DbSessionId = NewSessionId(),
            CreationTime = 0,
            OldestKeyTime = 0,
            FileCreationTime = now,
        };
        _file = _fs.CreateWritable(filePath, truncate: true);
        try
        {
            _builder = TableBuilder.Create(_file, _format, tbo);
        }
        catch
        {
            _file.Dispose();
            _file = null;
            throw;
        }
        _path = filePath;
        _smallestKey = _smallestRangeDel = _largestRangeDel = [];
        _largestKey.Clear();
        _numEntries = _numRangeDelEntries = 0;
    }

    private static string NewSessionId()
    {
        Span<byte> b = stackalloc byte[16];
        System.Security.Cryptography.RandomNumberGenerator.Fill(b);
        return OffsetableCacheKey.EncodeSessionId(BitConverter.ToUInt64(b), BitConverter.ToUInt64(b[8..]) | 1);
    }

    /// <summary>Adds a Put entry (SstFileWriter::Put). Keys must be strictly increasing.</summary>
    public SstFileWriter Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => Add(key, value, EntryType.Value);

    /// <summary>Adds a Merge entry (SstFileWriter::Merge).</summary>
    public SstFileWriter Merge(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => Add(key, value, EntryType.Merge);

    /// <summary>Adds a point deletion (SstFileWriter::Delete).</summary>
    public SstFileWriter Delete(ReadOnlySpan<byte> key) => Add(key, default, EntryType.Deletion);

    /// <summary>
    /// Adds a Put of <paramref name="key"/> at <paramref name="timestamp"/> (SstFileWriter::Put(user_key, timestamp,
    /// value)) for a comparator with user-defined timestamps. Entries are ordered by key, then newest timestamp first.
    /// </summary>
    public SstFileWriter Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> timestamp, ReadOnlySpan<byte> value) =>
        AddWithTimestamp(key, timestamp, value, EntryType.Value);

    /// <summary>Adds a deletion of <paramref name="key"/> at <paramref name="timestamp"/> (kTypeDeletionWithTimestamp).</summary>
    public SstFileWriter Delete(ReadOnlySpan<byte> key, ReadOnlySpan<byte> timestamp) =>
        AddWithTimestamp(key, timestamp, default, EntryType.DeletionWithTimestamp);

    /// <summary>Rep::Add without a timestamp: the comparator must not carry one.</summary>
    private SstFileWriter Add(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, EntryType type)
    {
        if (_ucmp.TimestampSize != 0) ThrowHelper.InvalidArgument("Timestamp size mismatch");
        return AddImpl(key, value, type);
    }

    /// <summary>Rep::Add(user_key, timestamp, ...): appends the timestamp to the key.</summary>
    private SstFileWriter AddWithTimestamp(ReadOnlySpan<byte> key, ReadOnlySpan<byte> timestamp, ReadOnlySpan<byte> value, EntryType type)
    {
        if (_ucmp.TimestampSize != timestamp.Length) ThrowHelper.InvalidArgument("Timestamp size mismatch");
        if (StripTimestamp && !IsMinTimestamp(timestamp))
        {
            // Rep::AddImpl (strip_timestamp): in this mode, users always provide a min timestamp.
            ThrowHelper.InvalidArgument("persist_user_defined_timestamps flag is set to false, only minimum timestamp is accepted.");
        }
        byte[] rented = System.Buffers.ArrayPool<byte>.Shared.Rent(key.Length + timestamp.Length);
        try
        {
            key.CopyTo(rented);
            timestamp.CopyTo(rented.AsSpan(key.Length));
            return AddImpl(rented.AsSpan(0, key.Length + timestamp.Length), value, type);
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>Port of SstFileWriter::Rep::AddImpl.</summary>
    private SstFileWriter AddImpl(ReadOnlySpan<byte> userKey, ReadOnlySpan<byte> value, EntryType type)
    {
        TableBuilder builder = _builder ?? throw RocksDbException.InvalidArgument("File is not opened");
        if (_numEntries > 0 && _ucmp.Compare(userKey, _largestKey.WrittenSpan) <= 0)
        {
            // Make sure that keys are added in order.
            ThrowHelper.InvalidArgument("Keys must be added in strict ascending order.");
        }
        _key.Clear();
        _key.Write(userKey);
        Span<byte> footer = _key.GetSpan(8);
        Coding.EncodeFixed64(footer, DbFormat.PackSequenceAndType(0, type));
        _key.Advance(8);
        builder.Add(_key.WrittenSpan, value);
        if (_numEntries == 0) _smallestKey = userKey.ToArray();
        _largestKey.Clear();
        _largestKey.Write(userKey);
        _numEntries++;
        return this;
    }

    /// <summary>Adds a range deletion [<paramref name="beginKey"/>, <paramref name="endKey"/>) (SstFileWriter::DeleteRange).</summary>
    public SstFileWriter DeleteRange(ReadOnlySpan<byte> beginKey, ReadOnlySpan<byte> endKey)
    {
        if (_ucmp.TimestampSize != 0) ThrowHelper.InvalidArgument("Timestamp size mismatch");
        return DeleteRangeImpl(beginKey, endKey);
    }

    /// <summary>
    /// Adds a range deletion [<paramref name="beginKey"/>, <paramref name="endKey"/>) at <paramref name="timestamp"/>
    /// (SstFileWriter::DeleteRange(begin, end, ts)); both keys are given without the timestamp.
    /// </summary>
    public SstFileWriter DeleteRange(ReadOnlySpan<byte> beginKey, ReadOnlySpan<byte> endKey, ReadOnlySpan<byte> timestamp)
    {
        if (_ucmp.TimestampSize != timestamp.Length) ThrowHelper.InvalidArgument("Timestamp size mismatch");
        if (StripTimestamp && !IsMinTimestamp(timestamp))
        {
            ThrowHelper.InvalidArgument("persist_user_defined_timestamps flag is set to false, only minimum timestamp is accepted for start key.");
        }
        byte[] b = new byte[beginKey.Length + timestamp.Length];
        byte[] e = new byte[endKey.Length + timestamp.Length];
        beginKey.CopyTo(b);
        timestamp.CopyTo(b.AsSpan(beginKey.Length));
        endKey.CopyTo(e);
        timestamp.CopyTo(e.AsSpan(endKey.Length));
        return DeleteRangeImpl(b, e);
    }

    /// <summary>Port of Rep::DeleteRangeImpl (keys with their timestamp when the comparator has one).</summary>
    private SstFileWriter DeleteRangeImpl(ReadOnlySpan<byte> beginKey, ReadOnlySpan<byte> endKey)
    {
        TableBuilder builder = _builder ?? throw RocksDbException.InvalidArgument("File is not opened");
        int c = _ucmp.CompareWithoutTimestamp(beginKey, endKey);
        if (c > 0) ThrowHelper.InvalidArgument("end key comes before start key");
        if (c == 0) return this; // empty range: nothing to record (upstream skips it as well)
        if (_numRangeDelEntries == 0)
        {
            _smallestRangeDel = beginKey.ToArray();
            _largestRangeDel = endKey.ToArray();
        }
        else
        {
            if (_ucmp.Compare(beginKey, _smallestRangeDel) < 0) _smallestRangeDel = beginKey.ToArray();
            if (_ucmp.Compare(endKey, _largestRangeDel) > 0) _largestRangeDel = endKey.ToArray();
        }
        var ikey = new InternalKey(beginKey, 0, EntryType.RangeDeletion);
        builder.Add(ikey.Encoded, endKey);
        _numRangeDelEntries++;
        return this;
    }

    /// <summary>Port of SstFileWriter::Finish: finishes and syncs the file and returns its description.</summary>
    public ExternalSstFileInfo Finish()
    {
        TableBuilder builder = _builder ?? throw RocksDbException.InvalidArgument("File is not opened");
        if (_numEntries == 0 && _numRangeDelEntries == 0) ThrowHelper.InvalidArgument("Cannot create sst file with no entries");
        try
        {
            builder.Finish();
            _file!.Sync();
            return MakeInfo(builder);
        }
        finally
        {
            Close();
        }
    }

    /// <summary>Asynchronous <see cref="Finish"/>.</summary>
    public async ValueTask<ExternalSstFileInfo> FinishAsync(CancellationToken cancellationToken = default)
    {
        TableBuilder builder = _builder ?? throw RocksDbException.InvalidArgument("File is not opened");
        if (_numEntries == 0 && _numRangeDelEntries == 0) ThrowHelper.InvalidArgument("Cannot create sst file with no entries");
        try
        {
            await builder.FinishAsync(cancellationToken).ConfigureAwait(false);
            await _file!.SyncAsync(cancellationToken).ConfigureAwait(false);
            return MakeInfo(builder);
        }
        finally
        {
            Close();
        }
    }

    /// <summary>
    /// Rep::strip_timestamp: user-defined timestamps are not persisted (persist_user_defined_timestamps = false), so only
    /// the minimum timestamp is accepted and the file stores keys without it.
    /// </summary>
    private bool StripTimestamp => _ucmp.TimestampSize > 0 && !_options.PersistUserDefinedTimestamps;

    /// <summary>CompareTimestamp(ts, MinU64Ts()) == 0 (the minimum u64 timestamp is all zeros).</summary>
    private static bool IsMinTimestamp(ReadOnlySpan<byte> timestamp) => !timestamp.ContainsAnyExcept((byte)0);

    private ExternalSstFileInfo MakeInfo(TableBuilder builder)
    {
        byte[] smallest = _smallestKey;
        byte[] largest = _largestKey.WrittenSpan.ToArray();
        byte[] smallestRangeDel = _smallestRangeDel;
        byte[] largestRangeDel = _largestRangeDel;
        if (StripTimestamp)
        {
            // SstFileWriter::Finish: the file info keys are reported without the (stripped) timestamp.
            int ts = _ucmp.TimestampSize;
            smallest = StripUserKey(smallest, ts);
            largest = StripUserKey(largest, ts);
            if (_numRangeDelEntries > 0)
            {
                smallestRangeDel = StripUserKey(smallestRangeDel, ts);
                largestRangeDel = StripUserKey(largestRangeDel, ts);
            }
        }
        return new(_path, smallest, largest, smallestRangeDel, largestRangeDel, 0, builder.FileSize, _numEntries, _numRangeDelEntries, 2);
    }

    private static byte[] StripUserKey(byte[] key, int ts) => key.Length >= ts ? key.AsSpan(0, key.Length - ts).ToArray() : key;

    private void Close()
    {
        _builder?.Dispose();
        _builder = null;
        _file?.Dispose();
        _file = null;
    }

    public void Dispose()
    {
        if (_builder is not null)
        {
            // Abandoned (not finished): like the destructor, drop the builder.
            _builder.Abandon();
        }
        Close();
        _key.Dispose();
        _largestKey.Dispose();
    }

    public ValueTask DisposeAsync()
    {
        Dispose();
        return default;
    }
}
