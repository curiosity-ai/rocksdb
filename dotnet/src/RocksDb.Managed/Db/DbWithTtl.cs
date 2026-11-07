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
    private volatile int _ttl;
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

    /// <summary>The TTL in seconds (non-positive: never expire).</summary>
    public int TtlSeconds => _ttl;

    /// <summary>The user filter run on unexpired values, or null.</summary>
    public CompactionFilter? UserFilter => _user;

    internal Func<long> Clock => _clock;

    /// <summary>TtlCompactionFilterFactory::SetTtl: applies to the filter decisions that follow.</summary>
    internal void SetTtl(int ttlSeconds) => _ttl = ttlSeconds;

    public override CompactionFilterDecision Filter(int level, ReadOnlySpan<byte> key, CompactionFilterValueType valueType, ReadOnlySpan<byte> existingValue,
        IBufferWriter<byte> newValue, IBufferWriter<byte> skipUntil)
    {
        if (valueType != CompactionFilterValueType.Value) return CompactionFilterDecision.Keep;
        if (TtlValue.IsStale(existingValue, _ttl, _clock())) return CompactionFilterDecision.Remove;
        if (_user is null || existingValue.Length < TtlValue.TimestampLength) return CompactionFilterDecision.Keep;
        PooledBufferWriter changed = ObjectPools.RentWriter();
        try
        {
            CompactionFilterDecision d = _user.Filter(level, key, valueType, existingValue[..^TtlValue.TimestampLength], changed, skipUntil);
            if (d == CompactionFilterDecision.ChangeValue)
            {
                newValue.Write(changed.WrittenSpan);
                newValue.Write(existingValue[^TtlValue.TimestampLength..]);
            }
            return d;
        }
        finally
        {
            ObjectPools.ReturnWriter(changed);
        }
    }
}

/// <summary>
/// Port of TtlMergeOperator (utilities/ttl/db_ttl_impl.{h,cc}): wraps a column family's merge operator in a TTL
/// database. The 4-byte write-time suffix is removed from the existing value and from every operand before the user
/// operator runs, and the current time is appended to its result (full and partial merges). A base value or operand
/// shorter than the suffix makes the merge fail (upstream logs "Could not remove timestamp" and returns false), which
/// surfaces as Corruption / MergeOperatorFailed. The user operator's failure scope is passed through.
/// The OPTIONS file records it as native DBWithTTL does: <c>{id=TtlMergeOperator;user_operator=...}</c>.
/// </summary>
public sealed class TtlMergeOperator : MergeOperator
{
    private readonly MergeOperator _user;
    private readonly Func<long>? _clock;

    public TtlMergeOperator(MergeOperator userMergeOperator) : this(userMergeOperator, null)
    {
    }

    /// <param name="userMergeOperator">The column family's merge operator.</param>
    /// <param name="clock">Current unix time in seconds (tests); null = system clock.</param>
    internal TtlMergeOperator(MergeOperator userMergeOperator, Func<long>? clock)
    {
        ArgumentNullException.ThrowIfNull(userMergeOperator);
        _user = userMergeOperator;
        _clock = clock;
    }

    /// <summary>kClassName.</summary>
    public const string ClassName = "TtlMergeOperator";

    public override string Name => ClassName;

    /// <summary>The wrapped operator (Customizable::Inner).</summary>
    public MergeOperator UserMergeOperator => _user;

    // ttl_merge_op_type_info: {"user_operator", AsCustomSharedPtr<MergeOperator>} -- serialized like native.
    internal override string OptionsId => "{id=" + ClassName + ";user_operator=" + _user.OptionsId + ";}";

    private int Now => _clock is null ? TtlValue.Now : unchecked((int)_clock());

    public override bool FullMerge(ReadOnlySpan<byte> key, bool hasExistingValue, ReadOnlySpan<byte> existingValue, in MergeOperands operands,
        IBufferWriter<byte> result) =>
        FullMerge(key, hasExistingValue, existingValue, operands, result, out _);

    /// <summary>TtlMergeOperator::FullMergeV2.</summary>
    public override bool FullMerge(ReadOnlySpan<byte> key, bool hasExistingValue, ReadOnlySpan<byte> existingValue, in MergeOperands operands,
        IBufferWriter<byte> result, out MergeOperationFailureScope failureScope)
    {
        const int tsLen = TtlValue.TimestampLength;
        failureScope = MergeOperationFailureScope.Default;
        // "Error: Could not remove timestamp from existing value."
        if (hasExistingValue && existingValue.Length < tsLen) return false;
        // Extract time-stamp from each operand to be passed to user_merge_op_
        for (int i = 0; i < operands.Count; i++)
        {
            // "Error: Could not remove timestamp from operand value."
            if (operands[i].Length < tsLen) return false;
        }
        // Apply the user merge operator (store result in *new_value)
        bool good = _user.FullMerge(key, hasExistingValue, hasExistingValue ? existingValue[..^tsLen] : default, operands.WithoutSuffix(tsLen), result,
            out failureScope);
        // Return false if the user merge operator returned false
        if (!good) return false;
        AppendNow(result);
        return true;
    }

    /// <summary>TtlMergeOperator::PartialMergeMulti.</summary>
    public override bool PartialMerge(ReadOnlySpan<byte> key, in MergeOperands operands, IBufferWriter<byte> result)
    {
        for (int i = 0; i < operands.Count; i++)
        {
            // "Error: Could not remove timestamp from value."
            if (operands[i].Length < TtlValue.TimestampLength) return false;
        }
        // Apply the user partial-merge operator (store result in *new_value)
        if (!_user.PartialMerge(key, operands.WithoutSuffix(TtlValue.TimestampLength), result)) return false;
        AppendNow(result);
        return true;
    }

    /// <summary>Augment the *new_value with the ttl time-stamp.</summary>
    private void AppendNow(IBufferWriter<byte> result)
    {
        Span<byte> ts = result.GetSpan(TtlValue.TimestampLength);
        Coding.EncodeFixed32(ts, unchecked((uint)Now));
        result.Advance(TtlValue.TimestampLength);
    }
}

/// <summary>
/// A column family of a <see cref="TtlDatabase"/> (a column family handle of DBWithTTL): writes append the write
/// time, reads and iterators strip it, and compactions drop values older than <see cref="TtlSeconds"/>.
/// </summary>
public sealed class TtlColumnFamily
{
    private readonly TtlDatabase _owner;
    private readonly TtlCompactionFilterFactory? _factory;

    internal TtlColumnFamily(TtlDatabase owner, ColumnFamily columnFamily, TtlCompactionFilter filter, TtlCompactionFilterFactory? factory)
    {
        _owner = owner;
        ColumnFamily = columnFamily;
        CompactionFilter = filter;
        _factory = factory;
    }

    /// <summary>The underlying column family (its values carry the timestamp suffix).</summary>
    public ColumnFamily ColumnFamily { get; }

    public string Name => ColumnFamily.Name;

    /// <summary>
    /// The TTL of the column family (DBWithTTL::GetTtl). Throws InvalidArgument when the column family was opened with
    /// a user <see cref="ColumnFamilyOptions.CompactionFilter"/> (the TTL filter then wraps it directly and there is no
    /// TtlCompactionFilterFactory, like upstream).
    /// </summary>
    public int TtlSeconds
    {
        get
        {
            if (_factory is null) ThrowHelper.InvalidArgument("TTLCompactionFilterFactory is not set for TTLDB");
            return _factory!.TtlSeconds;
        }
    }

    /// <summary>The TTL compaction filter of the column family (the factory's, or the one wrapping the user filter).</summary>
    public TtlCompactionFilter CompactionFilter { get; }

    /// <summary>
    /// DBWithTTL::SetTtl: changes the TTL used by later compactions. Like upstream, it has no effect on a column family
    /// opened with a user <see cref="ColumnFamilyOptions.CompactionFilter"/> (no TtlCompactionFilterFactory).
    /// </summary>
    public void SetTtl(int ttlSeconds) => _factory?.SetTtl(ttlSeconds);

    public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => _owner.WriteOne(null, ColumnFamily.Id, TtlDatabase.OpPut, key, value);
    public void Put(WriteOptions? options, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => _owner.WriteOne(options, ColumnFamily.Id, TtlDatabase.OpPut, key, value);
    public void Merge(ReadOnlySpan<byte> key, ReadOnlySpan<byte> operand) => _owner.WriteOne(null, ColumnFamily.Id, TtlDatabase.OpMerge, key, operand);
    public void Merge(WriteOptions? options, ReadOnlySpan<byte> key, ReadOnlySpan<byte> operand) => _owner.WriteOne(options, ColumnFamily.Id, TtlDatabase.OpMerge, key, operand);
    public void Delete(ReadOnlySpan<byte> key) => _owner.WriteOne(null, ColumnFamily.Id, TtlDatabase.OpDelete, key, default);
    public void Delete(WriteOptions? options, ReadOnlySpan<byte> key) => _owner.WriteOne(options, ColumnFamily.Id, TtlDatabase.OpDelete, key, default);

    public ValueTask PutAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default) =>
        _owner.WriteOneAsync(null, ColumnFamily.Id, TtlDatabase.OpPut, key, value, cancellationToken);
    public ValueTask MergeAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> operand, CancellationToken cancellationToken = default) =>
        _owner.WriteOneAsync(null, ColumnFamily.Id, TtlDatabase.OpMerge, key, operand, cancellationToken);
    public ValueTask DeleteAsync(ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default) =>
        _owner.WriteOneAsync(null, ColumnFamily.Id, TtlDatabase.OpDelete, key, default, cancellationToken);

    /// <summary>
    /// Copies the value of <paramref name="key"/> without its timestamp into <paramref name="value"/>
    /// (DBWithTTLImpl::Get: SanityCheckTimestamp then StripTS; Corruption for a missing or too old suffix).
    /// </summary>
    public bool TryGet(ReadOnlySpan<byte> key, IBufferWriter<byte> value) => TryGet(null, key, value);

    public bool TryGet(ReadOptions? options, ReadOnlySpan<byte> key, IBufferWriter<byte> value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return ColumnFamily.TryGet(options, key, value, static (v, w) =>
        {
            TtlValue.SanityCheckTimestamp(v);
            w.Write(TtlValue.Strip(v));
            return true;
        }, out _);
    }

    public byte[]? Get(ReadOnlySpan<byte> key)
    {
        PooledBufferWriter w = ObjectPools.RentWriter();
        try
        {
            return TryGet(null, key, w) ? w.WrittenSpan.ToArray() : null;
        }
        finally
        {
            ObjectPools.ReturnWriter(w);
        }
    }

    /// <summary>TtlIterator over this column family (see <see cref="TtlDatabase.NewIterator()"/>).</summary>
    public Iterator NewIterator() => NewIterator(null);

    public Iterator NewIterator(ReadOptions? options)
    {
        Iterator it = ColumnFamily.NewIterator(options);
        it.StripValueSuffix(TtlValue.TimestampLength);
        return it;
    }

    public void Flush() => ColumnFamily.Flush();

    public void CompactRange() => ColumnFamily.CompactRange();

    public ValueTask CompactRangeAsync(CancellationToken cancellationToken = default) => ColumnFamily.CompactRangeAsync(cancellationToken);

    public override string ToString() => Name;
}

/// <summary>
/// A database whose values expire after a TTL (DBWithTTL, utilities/ttl/db_ttl_impl.cc). Values and merge operands
/// are stored with the 4-byte write-time suffix of <see cref="TtlValue"/>, so the directory is interchangeable with
/// native <c>DBWithTTL</c>. Every column family has its own TTL (<see cref="Open(string, DbOptions, IReadOnlyList{ColumnFamilyDescriptor}, IReadOnlyList{int})"/>,
/// <see cref="CreateColumnFamilyWithTtl"/>, <see cref="TtlColumnFamily.SetTtl"/>). Reads and iterators strip the suffix
/// (expired values remain readable until a compaction removes them, like upstream). Like
/// DBWithTTLImpl::SanitizeOptions, each column family's options are rewritten: a <see cref="TtlCompactionFilterFactory"/>
/// (wrapping a user <see cref="ColumnFamilyOptions.CompactionFilterFactory"/>) or, when a user
/// <see cref="ColumnFamilyOptions.CompactionFilter"/> is set, a <see cref="TtlCompactionFilter"/> wrapping it removes
/// expired values, and the merge operator is wrapped in a <see cref="TtlMergeOperator"/>. The methods of this class act on
/// the default column family; <see cref="TtlColumnFamily"/> objects carry the same methods for the others.
/// </summary>
public sealed class TtlDatabase : IDisposable, IAsyncDisposable
{
    internal const byte OpPut = 0, OpMerge = 1, OpDelete = 2;

    private readonly RocksDatabase _db;
    private readonly Func<long>? _clock;
    private readonly Dictionary<uint, TtlColumnFamily> _columnFamilies = new();
    private readonly Lock _lock = new();
    private readonly TtlColumnFamily _default;

    private TtlDatabase(RocksDatabase db, List<(TtlCompactionFilter Filter, TtlCompactionFilterFactory? Factory)> sanitized,
        IReadOnlyList<ColumnFamilyDescriptor> columnFamilies, Func<long>? clock)
    {
        _db = db;
        _clock = clock;
        for (int i = 0; i < columnFamilies.Count; i++)
        {
            ColumnFamily cf = db.GetColumnFamily(columnFamilies[i].Name);
            _columnFamilies[cf.Id] = new TtlColumnFamily(this, cf, sanitized[i].Filter, sanitized[i].Factory);
        }
        _default = _columnFamilies[db.DefaultColumnFamily.Id];
    }

    // ------------------------------------------------------------------ open

    /// <summary>Opens (creating if allowed) a TTL database (DBWithTTL::Open). A non-positive TTL means "never expire".</summary>
    public static ValueTask<TtlDatabase> OpenAsync(string path, DbOptions options, int ttlSeconds, CancellationToken cancellationToken = default) =>
        OpenAsync(path, options, new ColumnFamilyOptions(), ttlSeconds, cancellationToken);

    /// <summary>
    /// Opens a TTL database whose default column family uses <paramref name="columnFamilyOptions"/> (sanitized as
    /// described on the class).
    /// </summary>
    public static ValueTask<TtlDatabase> OpenAsync(string path, DbOptions options, ColumnFamilyOptions columnFamilyOptions, int ttlSeconds,
        CancellationToken cancellationToken = default) =>
        OpenAsync(path, options, [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, columnFamilyOptions)], [ttlSeconds],
            cancellationToken);

    /// <summary>
    /// Opens a TTL database with column families (DBWithTTL::Open with descriptors): <paramref name="ttls"/>[i] is the
    /// TTL of <paramref name="columnFamilies"/>[i]. InvalidArgument if the counts differ.
    /// </summary>
    public static async ValueTask<TtlDatabase> OpenAsync(string path, DbOptions options, IReadOnlyList<ColumnFamilyDescriptor> columnFamilies,
        IReadOnlyList<int> ttls, CancellationToken cancellationToken = default)
    {
        (List<ColumnFamilyDescriptor> cfs, var sanitized) = SanitizeAll(columnFamilies, ttls, null);
        RocksDatabase db = await RocksDatabase.OpenAsync(path, options, cfs, cancellationToken).ConfigureAwait(false);
        return Create(db, sanitized, cfs, null);
    }

    public static TtlDatabase Open(string path, DbOptions options, int ttlSeconds) => Open(path, options, new ColumnFamilyOptions(), ttlSeconds);

    public static TtlDatabase Open(string path, DbOptions options, ColumnFamilyOptions columnFamilyOptions, int ttlSeconds) =>
        Open(path, options, columnFamilyOptions, ttlSeconds, null);

    /// <summary>Synchronous <see cref="OpenAsync(string, DbOptions, IReadOnlyList{ColumnFamilyDescriptor}, IReadOnlyList{int}, CancellationToken)"/>.</summary>
    public static TtlDatabase Open(string path, DbOptions options, IReadOnlyList<ColumnFamilyDescriptor> columnFamilies, IReadOnlyList<int> ttls) =>
        Open(path, options, columnFamilies, ttls, readOnly: false, null);

    /// <summary>DBWithTTL::Open with <c>read_only</c> = true (DB::OpenForReadOnly with the sanitized column families).</summary>
    public static TtlDatabase OpenReadOnly(string path, DbOptions options, IReadOnlyList<ColumnFamilyDescriptor> columnFamilies, IReadOnlyList<int> ttls) =>
        Open(path, options, columnFamilies, ttls, readOnly: true, null);

    /// <summary>Test hook: <paramref name="clock"/> supplies the current unix time used by compaction filters and merges.</summary>
    internal static TtlDatabase Open(string path, DbOptions options, ColumnFamilyOptions columnFamilyOptions, int ttlSeconds, Func<long>? clock) =>
        Open(path, options, [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, columnFamilyOptions)], [ttlSeconds], readOnly: false, clock);

    internal static TtlDatabase Open(string path, DbOptions options, IReadOnlyList<ColumnFamilyDescriptor> columnFamilies, IReadOnlyList<int> ttls, bool readOnly,
        Func<long>? clock)
    {
        (List<ColumnFamilyDescriptor> cfs, var sanitized) = SanitizeAll(columnFamilies, ttls, clock);
        RocksDatabase db = readOnly ? RocksDatabase.OpenReadOnly(path, options, cfs, false) : RocksDatabase.Open(path, options, cfs);
        return Create(db, sanitized, cfs, clock);
    }

    private static TtlDatabase Create(RocksDatabase db, List<(TtlCompactionFilter, TtlCompactionFilterFactory?)> sanitized, List<ColumnFamilyDescriptor> cfs,
        Func<long>? clock)
    {
        try
        {
            return new TtlDatabase(db, sanitized, cfs, clock);
        }
        catch
        {
            db.Dispose();
            throw;
        }
    }

    private static (List<ColumnFamilyDescriptor>, List<(TtlCompactionFilter, TtlCompactionFilterFactory?)>) SanitizeAll(
        IReadOnlyList<ColumnFamilyDescriptor> columnFamilies, IReadOnlyList<int> ttls, Func<long>? clock)
    {
        ArgumentNullException.ThrowIfNull(columnFamilies);
        ArgumentNullException.ThrowIfNull(ttls);
        if (ttls.Count != columnFamilies.Count) ThrowHelper.InvalidArgument("ttls size has to be the same as number of column families");
        var cfs = new List<ColumnFamilyDescriptor>(columnFamilies.Count + 1);
        var sanitized = new List<(TtlCompactionFilter, TtlCompactionFilterFactory?)>(columnFamilies.Count + 1);
        bool hasDefault = false;
        for (int i = 0; i < columnFamilies.Count; i++)
        {
            ArgumentNullException.ThrowIfNull(columnFamilies[i]);
            (ColumnFamilyOptions o, TtlCompactionFilter f, TtlCompactionFilterFactory? fac) = Sanitize(columnFamilies[i].Options, ttls[i], clock);
            cfs.Add(new ColumnFamilyDescriptor(columnFamilies[i].Name, o));
            sanitized.Add((f, fac));
            hasDefault |= columnFamilies[i].Name == ColumnFamilyDescriptor.DefaultColumnFamilyName;
        }
        if (!hasDefault)
        {
            // DB::Open adds the default column family with default options when it is not listed; give it the TTL
            // machinery (TTL 0 = never expire) so its values are still read and written with timestamps.
            (ColumnFamilyOptions o, TtlCompactionFilter f, TtlCompactionFilterFactory? fac) = Sanitize(new ColumnFamilyOptions(), 0, clock);
            cfs.Add(new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, o));
            sanitized.Add((f, fac));
        }
        return (cfs, sanitized);
    }

    /// <summary>DBWithTTLImpl::SanitizeOptions on a copy of <paramref name="o"/>.</summary>
    private static (ColumnFamilyOptions, TtlCompactionFilter, TtlCompactionFilterFactory?) Sanitize(ColumnFamilyOptions o, int ttlSeconds, Func<long>? clock)
    {
        ArgumentNullException.ThrowIfNull(o);
        ColumnFamilyOptions cf = o.Clone();
        TtlCompactionFilter filter;
        TtlCompactionFilterFactory? factory = null;
        if (cf.CompactionFilter is { } userFilter)
        {
            filter = clock is null ? new TtlCompactionFilter(ttlSeconds, userFilter) : new TtlCompactionFilter(ttlSeconds, userFilter, clock);
            cf.CompactionFilter = filter;
        }
        else
        {
            filter = clock is null ? new TtlCompactionFilter(ttlSeconds, null) : new TtlCompactionFilter(ttlSeconds, null, clock);
            factory = new TtlCompactionFilterFactory(filter, cf.CompactionFilterFactory);
            cf.CompactionFilterFactory = factory;
        }
        if (cf.MergeOperator is { } mergeOperator) cf.MergeOperator = new TtlMergeOperator(mergeOperator, clock);
        return (cf, filter, factory);
    }

    // ------------------------------------------------------------------ column families

    /// <summary>The default column family.</summary>
    public TtlColumnFamily DefaultColumnFamily => _default;

    /// <summary>The column family named <paramref name="name"/>; throws InvalidArgument if it does not exist.</summary>
    public TtlColumnFamily GetColumnFamily(string name)
    {
        ColumnFamily cf = _db.GetColumnFamily(name);
        lock (_lock)
        {
            if (_columnFamilies.TryGetValue(cf.Id, out TtlColumnFamily? t) && t.ColumnFamily == cf) return t;
        }
        ThrowHelper.InvalidArgument("Column family not opened through the TTL database: " + name);
        return null!;
    }

    /// <summary>
    /// DBWithTTLImpl::CreateColumnFamilyWithTtl: creates a column family whose options are sanitized like at open and
    /// whose values expire after <paramref name="ttlSeconds"/>.
    /// </summary>
    public TtlColumnFamily CreateColumnFamilyWithTtl(string name, ColumnFamilyOptions options, int ttlSeconds)
    {
        (ColumnFamilyOptions o, TtlCompactionFilter f, TtlCompactionFilterFactory? fac) = Sanitize(options, ttlSeconds, _clock);
        return Register(_db.CreateColumnFamily(name, o), f, fac);
    }

    /// <summary>Async <see cref="CreateColumnFamilyWithTtl"/>.</summary>
    public async ValueTask<TtlColumnFamily> CreateColumnFamilyWithTtlAsync(string name, ColumnFamilyOptions options, int ttlSeconds,
        CancellationToken cancellationToken = default)
    {
        (ColumnFamilyOptions o, TtlCompactionFilter f, TtlCompactionFilterFactory? fac) = Sanitize(options, ttlSeconds, _clock);
        return Register(await _db.CreateColumnFamilyAsync(name, o, cancellationToken).ConfigureAwait(false), f, fac);
    }

    /// <summary>DBWithTTLImpl::CreateColumnFamily: <see cref="CreateColumnFamilyWithTtl"/> with TTL 0 (never expire).</summary>
    public TtlColumnFamily CreateColumnFamily(string name, ColumnFamilyOptions options) => CreateColumnFamilyWithTtl(name, options, 0);

    private TtlColumnFamily Register(ColumnFamily cf, TtlCompactionFilter filter, TtlCompactionFilterFactory? factory)
    {
        var t = new TtlColumnFamily(this, cf, filter, factory);
        lock (_lock) _columnFamilies[cf.Id] = t;
        return t;
    }

    /// <summary>Drops a column family (DB::DropColumnFamily).</summary>
    public void DropColumnFamily(TtlColumnFamily columnFamily)
    {
        ArgumentNullException.ThrowIfNull(columnFamily);
        _db.DropColumnFamily(columnFamily.ColumnFamily);
        lock (_lock) _columnFamilies.Remove(columnFamily.ColumnFamily.Id);
    }

    /// <summary>DBWithTTL::SetTtl(ttl): the TTL of the default column family.</summary>
    public void SetTtl(int ttlSeconds) => _default.SetTtl(ttlSeconds);

    /// <summary>DBWithTTL::SetTtl(column_family, ttl).</summary>
    public void SetTtl(TtlColumnFamily columnFamily, int ttlSeconds)
    {
        ArgumentNullException.ThrowIfNull(columnFamily);
        columnFamily.SetTtl(ttlSeconds);
    }

    /// <summary>DBWithTTL::GetTtl(column_family) (see <see cref="TtlColumnFamily.TtlSeconds"/>).</summary>
    public int GetTtl(TtlColumnFamily columnFamily)
    {
        ArgumentNullException.ThrowIfNull(columnFamily);
        return columnFamily.TtlSeconds;
    }

    // ------------------------------------------------------------------ default column family

    /// <summary>Compacts the whole key space of the default column family, dropping expired values (DB::CompactRange).</summary>
    public void CompactRange() => _db.CompactRange();

    public ValueTask CompactRangeAsync(CancellationToken cancellationToken = default) => _db.CompactRangeAsync(cancellationToken);

    /// <summary>The TTL of the default column family (see <see cref="TtlColumnFamily.TtlSeconds"/>).</summary>
    public int TtlSeconds => _default.TtlSeconds;

    /// <summary>The underlying database (values carry the timestamp suffix).</summary>
    public RocksDatabase Database => _db;

    /// <summary>The compaction filter removing expired values of the default column family.</summary>
    public TtlCompactionFilter CompactionFilter => _default.CompactionFilter;

    public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => _default.Put(key, value);

    public ValueTask PutAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default) =>
        _default.PutAsync(key, value, cancellationToken);

    /// <summary>DBWithTTLImpl::Merge: the operand is stored with the write time (the merge operator must be set).</summary>
    public void Merge(ReadOnlySpan<byte> key, ReadOnlySpan<byte> operand) => _default.Merge(key, operand);

    public ValueTask MergeAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> operand, CancellationToken cancellationToken = default) =>
        _default.MergeAsync(key, operand, cancellationToken);

    public void Delete(ReadOnlySpan<byte> key) => _default.Delete(key);

    public ValueTask DeleteAsync(ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default) => _default.DeleteAsync(key, cancellationToken);

    /// <summary>
    /// Applies <paramref name="batch"/> with timestamps appended to its Put and Merge values (DBWithTTLImpl::Write).
    /// Records name column families by <see cref="ColumnFamily.Id"/>.
    /// </summary>
    public void Write(WriteBatch batch) => Write(null, batch);

    public void Write(WriteOptions? options, WriteBatch batch)
    {
        ArgumentNullException.ThrowIfNull(batch);
        using WriteBatch rewritten = WriteBatch.Rent();
        var h = new TimestampAppender(rewritten, Now);
        batch.Iterate(ref h);
        _db.Write(options, rewritten);
    }

    [System.Runtime.CompilerServices.AsyncMethodBuilder(typeof(System.Runtime.CompilerServices.PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask WriteAsync(WriteOptions? options, WriteBatch batch, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(batch);
        WriteBatch rewritten = WriteBatch.Rent();
        try
        {
            var h = new TimestampAppender(rewritten, Now);
            batch.Iterate(ref h);
            await _db.WriteAsync(options, rewritten, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            rewritten.Dispose();
        }
    }

    /// <summary>Copies the value of <paramref name="key"/> without its timestamp into <paramref name="value"/>.</summary>
    public bool TryGet(ReadOnlySpan<byte> key, IBufferWriter<byte> value) => _default.TryGet(key, value);

    public bool TryGet(ReadOptions? options, ReadOnlySpan<byte> key, IBufferWriter<byte> value) => _default.TryGet(options, key, value);

    public byte[]? Get(ReadOnlySpan<byte> key) => _default.Get(key);

    /// <summary>
    /// Iterator whose <see cref="Iterator.Value"/> is the user value without its write-time suffix (port of TtlIterator,
    /// utilities/ttl/db_ttl_impl.h). Like upstream, expired entries stay visible until a compaction removes them (the
    /// iterator does not check the TTL); a value shorter than the suffix fails with Corruption.
    /// </summary>
    public Iterator NewIterator() => _default.NewIterator(null);

    /// <summary>TtlIterator over <paramref name="options"/> (see <see cref="NewIterator()"/>).</summary>
    public Iterator NewIterator(ReadOptions? options) => _default.NewIterator(options);

    public void Flush() => _db.Flush();

    public void Dispose() => _db.Dispose();

    public ValueTask DisposeAsync() => _db.DisposeAsync();

    private int Now => _clock is null ? TtlValue.Now : unchecked((int)_clock());

    /// <summary>DBWithTTLImpl::Put / Merge / Delete: a one-entry batch through <see cref="Write(WriteOptions?, WriteBatch)"/>.</summary>
    internal void WriteOne(WriteOptions? options, uint cf, byte op, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        using WriteBatch b = WriteBatch.Rent();
        AppendOne(b, cf, op, key, value, Now);
        _db.Write(options, b);
    }

    [System.Runtime.CompilerServices.AsyncMethodBuilder(typeof(System.Runtime.CompilerServices.PoolingAsyncValueTaskMethodBuilder))]
    internal async ValueTask WriteOneAsync(WriteOptions? options, uint cf, byte op, ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value,
        CancellationToken cancellationToken)
    {
        WriteBatch b = WriteBatch.Rent();
        try
        {
            AppendOne(b, cf, op, key.Span, value.Span, Now);
            await _db.WriteAsync(options, b, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            b.Dispose();
        }
    }

    private static void AppendOne(WriteBatch b, uint cf, byte op, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, int now)
    {
        switch (op)
        {
            case OpPut: WithTimestamp(b, cf, key, value, now, merge: false); break;
            case OpMerge: WithTimestamp(b, cf, key, value, now, merge: true); break;
            default: b.Delete(cf, key); break;
        }
    }

    /// <summary>DBWithTTLImpl::AppendTS + WriteBatchInternal::Put / Merge.</summary>
    private static void WithTimestamp(WriteBatch b, uint cf, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, int now, bool merge)
    {
        byte[] buf = ArrayPool<byte>.Shared.Rent(value.Length + TtlValue.TimestampLength);
        try
        {
            value.CopyTo(buf);
            Coding.EncodeFixed32(buf.AsSpan(value.Length), unchecked((uint)now));
            ReadOnlySpan<byte> v = buf.AsSpan(0, value.Length + TtlValue.TimestampLength);
            if (merge) b.Merge(cf, key, v);
            else b.Put(cf, key, v);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buf);
        }
    }

    private readonly struct TimestampAppender(WriteBatch dst, int now) : IWriteBatchHandler
    {
        public bool Continue() => true;
        public void Put(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => WithTimestamp(dst, columnFamilyId, key, value, now, merge: false);
        public void TimedPut(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ulong writeUnixTime) =>
            ThrowHelper.NotSupported("TimedPut is not supported by TTL databases");
        public void PutEntity(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> entity) =>
            ThrowHelper.NotSupported("PutEntity is not supported by TTL databases");
        public void Delete(uint columnFamilyId, ReadOnlySpan<byte> key) => dst.Delete(columnFamilyId, key);
        public void SingleDelete(uint columnFamilyId, ReadOnlySpan<byte> key) => dst.SingleDelete(columnFamilyId, key);
        public void DeleteRange(uint columnFamilyId, ReadOnlySpan<byte> beginKey, ReadOnlySpan<byte> endKey) => dst.DeleteRange(columnFamilyId, beginKey, endKey);
        public void Merge(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => WithTimestamp(dst, columnFamilyId, key, value, now, merge: true);
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
/// Port of TtlCompactionFilterFactory (utilities/ttl/db_ttl_impl.{h,cc}): hands the TTL filter to every compaction,
/// wrapping the filter of a user <see cref="CompactionFilterFactory"/> when there is one. The name is what native
/// DBWithTTL records in the OPTIONS file.
/// </summary>
public sealed class TtlCompactionFilterFactory : CompactionFilterFactory
{
    private readonly CompactionFilterFactory? _user;

    public TtlCompactionFilterFactory(TtlCompactionFilter filter) : this(filter, null)
    {
    }

    /// <param name="filter">The TTL filter used when the user factory creates no filter (also carries the TTL and clock).</param>
    /// <param name="userFactory">The column family's own compaction filter factory, or null.</param>
    public TtlCompactionFilterFactory(TtlCompactionFilter filter, CompactionFilterFactory? userFactory)
    {
        ArgumentNullException.ThrowIfNull(filter);
        Filter = filter;
        _user = userFactory;
    }

    /// <summary>The (thread-safe) TTL filter shared by compactions without a user filter.</summary>
    public TtlCompactionFilter Filter { get; }

    /// <summary>The wrapped user factory, or null.</summary>
    public CompactionFilterFactory? UserFactory => _user;

    /// <summary>The current TTL (TtlCompactionFilterFactory::GetTtl).</summary>
    public int TtlSeconds => Filter.TtlSeconds;

    /// <summary>TtlCompactionFilterFactory::SetTtl: compactions that start afterwards use <paramref name="ttlSeconds"/>.</summary>
    public void SetTtl(int ttlSeconds) => Filter.SetTtl(ttlSeconds);

    public override string Name => "TtlCompactionFilterFactory";

    public override CompactionFilter? CreateCompactionFilter(in CompactionFilterContext context)
    {
        CompactionFilter? user = _user?.CreateCompactionFilter(context);
        return user is null ? Filter : new TtlCompactionFilter(Filter.TtlSeconds, user, Filter.Clock);
    }
}
