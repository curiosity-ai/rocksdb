using System.Buffers;
using RocksDb.Managed.Db;
using RocksDb.Managed.Util;

namespace RocksDb.Managed;

/// <summary>
/// The value encoding of TTL databases. Port of the DBWithTTLImpl helpers (utilities/ttl/db_ttl_impl.{h,cc}): every
/// stored value (and merge operand) carries a 4-byte little-endian int32 suffix with the unix time of the write.
/// </summary>
public static class TtlValue
{
    /// <summary>kTSLength.</summary>
    public const int TimestampLength = 4;
    /// <summary>kMinTimestamp (05/09/2013): smaller suffixes are corruption (or a non-TTL DB opened in TTL mode).</summary>
    public const int MinTimestamp = 1368146402;
    /// <summary>kMaxTimestamp.</summary>
    public const int MaxTimestamp = int.MaxValue;

    /// <summary>DBWithTTLImpl::AppendTS: writes <paramref name="value"/> followed by fixed32(<paramref name="unixTime"/>).</summary>
    public static void AppendTimestamp(ReadOnlySpan<byte> value, int unixTime, IBufferWriter<byte> destination)
    {
        ArgumentNullException.ThrowIfNull(destination);
        Span<byte> dst = destination.GetSpan(value.Length + TimestampLength);
        value.CopyTo(dst);
        Coding.EncodeFixed32(dst[value.Length..], unchecked((uint)unixTime));
        destination.Advance(value.Length + TimestampLength);
    }

    /// <summary>The current unix time as written by AppendTS ((int32_t)curtime).</summary>
    public static int Now => unchecked((int)DateTimeOffset.UtcNow.ToUnixTimeSeconds());

    /// <summary>DBWithTTLImpl::SanityCheckTimestamp; throws Corruption for a missing or too old suffix.</summary>
    public static void SanityCheckTimestamp(ReadOnlySpan<byte> valueWithTimestamp)
    {
        if (valueWithTimestamp.Length < TimestampLength) ThrowHelper.Corruption("Error: value's length less than timestamp's\n");
        // Checks that TS is not lesser than kMinTimestamp. Guards against corruption & normal database opened
        // incorrectly in ttl mode.
        if (GetTimestamp(valueWithTimestamp) < MinTimestamp) ThrowHelper.Corruption("Error: Timestamp < ttl feature release time!\n");
    }

    /// <summary>The write time stored in the suffix.</summary>
    public static int GetTimestamp(ReadOnlySpan<byte> valueWithTimestamp) =>
        unchecked((int)Coding.DecodeFixed32(valueWithTimestamp[^TimestampLength..]));

    /// <summary>DBWithTTLImpl::StripTS: the user value (throws Corruption when too short).</summary>
    public static ReadOnlySpan<byte> Strip(ReadOnlySpan<byte> valueWithTimestamp)
    {
        if (valueWithTimestamp.Length < TimestampLength) ThrowHelper.Corruption("Bad timestamp in key-value");
        return valueWithTimestamp[..^TimestampLength];
    }

    /// <summary>DBWithTTLImpl::IsStale: true if the value is older than <paramref name="ttlSeconds"/> (non-positive TTL = never).</summary>
    public static bool IsStale(ReadOnlySpan<byte> valueWithTimestamp, int ttlSeconds, long nowUnixSeconds)
    {
        if (ttlSeconds <= 0) return false; // Data is fresh if TTL is non-positive
        if (valueWithTimestamp.Length < TimestampLength) return false;
        // int32_t may overflow when timestamp_value + ttl: use int64.
        // int64_t timestamp_value = DecodeFixed32(...): the suffix is zero-extended here (unlike SanityCheckTimestamp).
        long timestamp = Coding.DecodeFixed32(valueWithTimestamp[^TimestampLength..]);
        return timestamp + ttlSeconds < nowUnixSeconds;
    }
}

/// <summary>
/// Removes expired entries during compaction. Port of TtlCompactionFilter (utilities/ttl/db_ttl_impl.cc): stale
/// values are removed; otherwise an optional user filter sees the value without its timestamp, and a changed value
/// gets the original timestamp re-appended. Merge operands are kept.
/// </summary>
public sealed class TtlCompactionFilter : CompactionFilter
{
    private readonly int _ttl;
    private readonly CompactionFilter? _user;
    private readonly Func<long> _clock;

    public TtlCompactionFilter(int ttlSeconds, CompactionFilter? userFilter) : this(ttlSeconds, userFilter, static () => DateTimeOffset.UtcNow.ToUnixTimeSeconds())
    {
    }

    /// <param name="clock">Current unix time in seconds (tests).</param>
    public TtlCompactionFilter(int ttlSeconds, CompactionFilter? userFilter, Func<long> clock)
    {
        ArgumentNullException.ThrowIfNull(clock);
        _ttl = ttlSeconds;
        _user = userFilter;
        _clock = clock;
    }

    /// <summary>kClassName of TtlCompactionFilter.</summary>
    public override string Name => "TtlCompactionFilter";

    public override CompactionFilterDecision Filter(int level, ReadOnlySpan<byte> key, CompactionFilterValueType valueType, ReadOnlySpan<byte> existingValue,
        IBufferWriter<byte> newValue, IBufferWriter<byte> skipUntil)
    {
        if (valueType != CompactionFilterValueType.Value) return CompactionFilterDecision.Keep;
        if (TtlValue.IsStale(existingValue, _ttl, _clock())) return CompactionFilterDecision.Remove;
        if (_user is null || existingValue.Length < TtlValue.TimestampLength) return CompactionFilterDecision.Keep;
        var changed = new ArrayBufferWriter<byte>();
        CompactionFilterDecision d = _user.Filter(level, key, valueType, existingValue[..^TtlValue.TimestampLength], changed, skipUntil);
        if (d == CompactionFilterDecision.ChangeValue)
        {
            newValue.Write(changed.WrittenSpan);
            newValue.Write(existingValue[^TtlValue.TimestampLength..]);
        }
        return d;
    }
}

/// <summary>
/// A database whose values expire after a TTL (DBWithTTL::Open, utilities/ttl/db_ttl_impl.cc). Values are stored
/// with the 4-byte write-time suffix of <see cref="TtlValue"/>, so the directory is interchangeable with native
/// <c>DBWithTTL</c>. Reads strip the suffix (expired values remain readable until a compaction removes them, like
/// upstream). Expired values are dropped by compactions through a <see cref="TtlCompactionFilterFactory"/> installed
/// on the default column family (the OPTIONS file records <c>compaction_filter_factory=TtlCompactionFilterFactory</c>).
/// </summary>
public sealed class TtlDatabase : IDisposable, IAsyncDisposable
{
    private readonly RocksDatabase _db;

    private TtlDatabase(RocksDatabase db, int ttlSeconds, TtlCompactionFilterFactory factory)
    {
        _db = db;
        TtlSeconds = ttlSeconds;
        CompactionFilter = factory.Filter;
    }

    /// <summary>Opens (creating if allowed) a TTL database (DBWithTTL::Open). A non-positive TTL means "never expire".</summary>
    public static ValueTask<TtlDatabase> OpenAsync(string path, DbOptions options, int ttlSeconds, CancellationToken cancellationToken = default) =>
        OpenAsync(path, options, new ColumnFamilyOptions(), ttlSeconds, cancellationToken);

    /// <summary>
    /// Opens a TTL database whose default column family uses <paramref name="columnFamilyOptions"/>; its compaction
    /// filter factory is replaced by the TTL one (a user <see cref="ColumnFamilyOptions.CompactionFilter"/> runs after
    /// the TTL check on unexpired values, like DBWithTTLImpl::SanitizeOptions).
    /// </summary>
    public static async ValueTask<TtlDatabase> OpenAsync(string path, DbOptions options, ColumnFamilyOptions columnFamilyOptions, int ttlSeconds,
        CancellationToken cancellationToken = default)
    {
        (ColumnFamilyOptions cf, TtlCompactionFilterFactory factory) = Sanitize(columnFamilyOptions, ttlSeconds, null);
        RocksDatabase db = await RocksDatabase.OpenAsync(path, options, cf, cancellationToken).ConfigureAwait(false);
        return new TtlDatabase(db, ttlSeconds, factory);
    }

    public static TtlDatabase Open(string path, DbOptions options, int ttlSeconds) => Open(path, options, new ColumnFamilyOptions(), ttlSeconds);

    public static TtlDatabase Open(string path, DbOptions options, ColumnFamilyOptions columnFamilyOptions, int ttlSeconds) =>
        Open(path, options, columnFamilyOptions, ttlSeconds, null);

    /// <summary>Test hook: <paramref name="clock"/> supplies the current unix time used by the compaction filter.</summary>
    internal static TtlDatabase Open(string path, DbOptions options, ColumnFamilyOptions columnFamilyOptions, int ttlSeconds, Func<long>? clock)
    {
        (ColumnFamilyOptions cf, TtlCompactionFilterFactory factory) = Sanitize(columnFamilyOptions, ttlSeconds, clock);
        return new TtlDatabase(RocksDatabase.Open(path, options, cf), ttlSeconds, factory);
    }

    private static (ColumnFamilyOptions, TtlCompactionFilterFactory) Sanitize(ColumnFamilyOptions o, int ttlSeconds, Func<long>? clock)
    {
        ArgumentNullException.ThrowIfNull(o);
        ColumnFamilyOptions cf = o.Clone();
        var filter = clock is null ? new TtlCompactionFilter(ttlSeconds, cf.CompactionFilter) : new TtlCompactionFilter(ttlSeconds, cf.CompactionFilter, clock);
        var factory = new TtlCompactionFilterFactory(filter);
        cf.CompactionFilter = null;
        cf.CompactionFilterFactory = factory;
        return (cf, factory);
    }

    /// <summary>Compacts the whole key space, dropping expired values (DB::CompactRange).</summary>
    public void CompactRange() => _db.CompactRange();

    public ValueTask CompactRangeAsync(CancellationToken cancellationToken = default) => _db.CompactRangeAsync(cancellationToken);

    public int TtlSeconds { get; }
    /// <summary>The underlying database (values carry the timestamp suffix).</summary>
    public RocksDatabase Database => _db;
    /// <summary>The compaction filter removing expired values.</summary>
    public TtlCompactionFilter CompactionFilter { get; }

    public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        using WriteBatch b = WriteBatch.Rent();
        PutWithTimestamp(b, 0, key, value, TtlValue.Now);
        _db.Write(b);
    }

    public async ValueTask PutAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default)
    {
        WriteBatch b = WriteBatch.Rent();
        try
        {
            PutWithTimestamp(b, 0, key.Span, value.Span, TtlValue.Now);
            await _db.WriteAsync(b, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            b.Dispose();
        }
    }

    private static void PutWithTimestamp(WriteBatch b, uint cf, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, int now)
    {
        byte[] buf = ArrayPool<byte>.Shared.Rent(value.Length + TtlValue.TimestampLength);
        try
        {
            value.CopyTo(buf);
            Coding.EncodeFixed32(buf.AsSpan(value.Length), unchecked((uint)now));
            b.Put(cf, key, buf.AsSpan(0, value.Length + TtlValue.TimestampLength));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buf);
        }
    }

    public void Delete(ReadOnlySpan<byte> key) => _db.Delete(key);

    /// <summary>
    /// Applies <paramref name="batch"/> with timestamps appended to its Put and Merge values (DBWithTTLImpl::Write).
    /// </summary>
    public void Write(WriteBatch batch)
    {
        ArgumentNullException.ThrowIfNull(batch);
        using WriteBatch rewritten = WriteBatch.Rent();
        var h = new TimestampAppender(rewritten, TtlValue.Now);
        batch.Iterate(ref h);
        _db.Write(rewritten);
    }

    /// <summary>Copies the value of <paramref name="key"/> without its timestamp into <paramref name="value"/>.</summary>
    public bool TryGet(ReadOnlySpan<byte> key, IBufferWriter<byte> value) =>
        _db.TryGet(key, value, static (v, w) =>
        {
            TtlValue.SanityCheckTimestamp(v);
            w.Write(TtlValue.Strip(v));
            return true;
        }, out _);

    public byte[]? Get(ReadOnlySpan<byte> key)
    {
        var w = new ArrayBufferWriter<byte>();
        return TryGet(key, w) ? w.WrittenSpan.ToArray() : null;
    }

    /// <summary>Iterator over the raw (timestamped) values; use <see cref="TtlValue.Strip"/> on <see cref="Iterator.Value"/>.</summary>
    public Iterator NewIterator() => _db.NewIterator();

    public void Flush() => _db.Flush();

    public void Dispose() => _db.Dispose();

    public ValueTask DisposeAsync() => _db.DisposeAsync();

    private readonly struct TimestampAppender(WriteBatch dst, int now) : IWriteBatchHandler
    {
        public bool Continue() => true;
        public void Put(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => PutWithTimestamp(dst, columnFamilyId, key, value, now);
        public void TimedPut(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ulong writeUnixTime) =>
            ThrowHelper.NotSupported("TimedPut is not supported by TTL databases");
        public void PutEntity(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> entity) =>
            ThrowHelper.NotSupported("PutEntity is not supported by TTL databases");
        public void Delete(uint columnFamilyId, ReadOnlySpan<byte> key) => dst.Delete(columnFamilyId, key);
        public void SingleDelete(uint columnFamilyId, ReadOnlySpan<byte> key) => dst.SingleDelete(columnFamilyId, key);
        public void DeleteRange(uint columnFamilyId, ReadOnlySpan<byte> beginKey, ReadOnlySpan<byte> endKey) => dst.DeleteRange(columnFamilyId, beginKey, endKey);

        public void Merge(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            byte[] buf = ArrayPool<byte>.Shared.Rent(value.Length + TtlValue.TimestampLength);
            try
            {
                value.CopyTo(buf);
                Coding.EncodeFixed32(buf.AsSpan(value.Length), unchecked((uint)now));
                dst.Merge(columnFamilyId, key, buf.AsSpan(0, value.Length + TtlValue.TimestampLength));
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buf);
            }
        }

        public void PutBlobIndex(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> blobIndex) =>
            ThrowHelper.NotSupported("Blob indexes are not supported by TTL databases");
        public void LogData(ReadOnlySpan<byte> blob) => dst.PutLogData(blob);
        public void MarkBeginPrepare(bool unprepare) => ThrowHelper.NotSupported("Two-phase commit is not supported by TTL databases");
        public void MarkEndPrepare(ReadOnlySpan<byte> xid) => ThrowHelper.NotSupported("Two-phase commit is not supported by TTL databases");
        public void MarkNoop(bool emptyBatch) { }
        public void MarkCommit(ReadOnlySpan<byte> xid) => ThrowHelper.NotSupported("Two-phase commit is not supported by TTL databases");
        public void MarkCommitWithTimestamp(ReadOnlySpan<byte> xid, ReadOnlySpan<byte> commitTimestamp) =>
            ThrowHelper.NotSupported("Two-phase commit is not supported by TTL databases");
        public void MarkRollback(ReadOnlySpan<byte> xid) => ThrowHelper.NotSupported("Two-phase commit is not supported by TTL databases");
    }
}

/// <summary>
/// Port of TtlCompactionFilterFactory (utilities/ttl/db_ttl_impl.h): hands the TTL filter to every compaction. The
/// name is what native DBWithTTL records in the OPTIONS file.
/// </summary>
public sealed class TtlCompactionFilterFactory : CompactionFilterFactory
{
    public TtlCompactionFilterFactory(TtlCompactionFilter filter)
    {
        ArgumentNullException.ThrowIfNull(filter);
        Filter = filter;
    }

    /// <summary>The (thread-safe) filter shared by all compactions.</summary>
    public TtlCompactionFilter Filter { get; }

    public override string Name => "TtlCompactionFilterFactory";

    public override CompactionFilter? CreateCompactionFilter(in CompactionFilterContext context) => Filter;
}
