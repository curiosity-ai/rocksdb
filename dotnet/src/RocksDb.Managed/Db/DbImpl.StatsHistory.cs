using System.Globalization;
using System.Text;
using RocksDb.Managed.Db.Iterators;

namespace RocksDb.Managed.Db;

/// <summary>
/// Statistics history. Port of DBImpl::PersistStats / FindStatsByTime / GetStatsHistory /
/// EstimateInMemoryStatsHistorySize / InitPersistStatsColumnFamily / PersistentStatsProcessFormatVersion
/// (db/db_impl/db_impl.cc, db/db_impl/db_impl_open.cc), monitoring/persistent_stats_history.cc and
/// monitoring/in_memory_stats_history.cc.
/// <para>
/// Every stats_persist_period_sec the ticker values of <see cref="DbOptions.Statistics"/> are sampled and the deltas
/// since the previous sample are kept: in memory (bounded by stats_history_buffer_size, oldest samples dropped first),
/// or with persist_stats_to_disk in the column family "___rocksdb_stats_history___" under the key
/// "%010d#&lt;ticker name&gt;" (seconds since the epoch, zero padded) with the decimal delta as value, next to the format
/// version keys "__persistent_stats_format_version__" / "__persistent_stats_compatible_version__" (both "1"). That column
/// family is opened implicitly with OptimizeForPersistentStats options whenever the MANIFEST has it, like
/// VersionEditHandler, so a DB written with persist_stats_to_disk opens without naming it.
/// </para>
/// </summary>
internal sealed partial class DbImpl
{
    /// <summary>kPersistentStatsColumnFamilyName (db/db_impl/db_impl.cc).</summary>
    internal const string PersistentStatsColumnFamilyName = "___rocksdb_stats_history___";

    private const string StatsFormatVersionKey = "__persistent_stats_format_version__";
    private const string StatsCompatibleVersionKey = "__persistent_stats_compatible_version__";
    private const ulong StatsCfCurrentFormatVersion = 1;
    private const ulong StatsCfCompatibleFormatVersion = 1;
    private const int NowSecondsStringLength = 10;

    private readonly Lock _statsHistoryLock = new();
    // stats_history_: sample time -> ticker deltas, ordered by time (guarded by _statsHistoryLock).
    private readonly SortedList<ulong, Dictionary<string, ulong>> _statsHistory = new();
    // stats_slice_ / stats_slice_initialized_: the ticker values of the previous sample (only touched by PersistStats,
    // which the timer never runs concurrently with itself).
    private Dictionary<string, ulong>? _statsSlice;
    private Timer? _statsPersistTimer;
    private int _persistStatsRunning;
    private ColumnFamilyData? _persistStatsCfd;

    /// <summary>
    /// Port of OptimizeForPersistentStats (monitoring/persistent_stats_history.cc): the options the stats history column
    /// family is created and opened with.
    /// </summary>
    internal static ColumnFamilyOptions OptimizeForPersistentStats(ColumnFamilyOptions cfo)
    {
        cfo.WriteBufferSize = 2 << 20;
        cfo.TargetFileSizeBase = 2 * 1048576;
        cfo.MaxBytesForLevelBase = 10 * 1048576;
        cfo.SoftPendingCompactionBytesLimit = 256 * 1048576;
        cfo.HardPendingCompactionBytesLimit = 1073741824UL;
        cfo.Compression = Compression.CompressionType.None;
        return cfo;
    }

    /// <summary>
    /// VersionEditHandler (db/version_edit_handler.cc): the stats history column family is opened implicitly, always with
    /// OptimizeForPersistentStats options (even if the caller named it). Applied to the descriptors of DB::Open when the
    /// recovered MANIFEST has that column family.
    /// </summary>
    private static void AddImplicitStatsColumnFamily(Dictionary<string, ColumnFamilyOptions> descriptors, bool inManifest)
    {
        if (inManifest) descriptors[PersistentStatsColumnFamilyName] = OptimizeForPersistentStats(new ColumnFamilyOptions());
    }

    /// <summary>
    /// DB::Open with persist_stats_to_disk: InitPersistStatsColumnFamily (create the column family when the DB has none)
    /// and PersistentStatsProcessFormatVersion (write the version keys for a new column family; drop and recreate one
    /// whose version keys are missing or incompatible).
    /// </summary>
    private async ValueTask InitPersistStatsColumnFamilyAsync(bool synchronous, CancellationToken ct)
    {
        ColumnFamilyData? cfd = TryGetColumnFamilyData(PersistentStatsColumnFamilyName);
        bool existed = cfd is not null;
        cfd ??= await CreateColumnFamilyCoreAsync(PersistentStatsColumnFamilyName, OptimizeForPersistentStats(new ColumnFamilyOptions()),
            writeOptionsFile: false, synchronous, ct).ConfigureAwait(false);
        bool persistFormatVersion = !existed;
        if (existed)
        {
            bool okFormat = TryReadStatsVersion(cfd, StatsFormatVersionKey, out ulong formatRecovered);
            bool okCompatible = TryReadStatsVersion(cfd, StatsCompatibleVersionKey, out ulong compatibleRecovered);
            if (!okFormat || !okCompatible
                || (StatsCfCurrentFormatVersion < formatRecovered && StatsCfCompatibleFormatVersion < compatibleRecovered))
            {
                LogWarn(!okFormat || !okCompatible
                    ? "Recreating persistent stats column family since reading persistent stats version key failed."
                    : "Recreating persistent stats column family due to corrupted or incompatible format version. Recovered format: "
                      + formatRecovered.ToString(CultureInfo.InvariantCulture) + "; recovered format compatible since: "
                      + compatibleRecovered.ToString(CultureInfo.InvariantCulture));
                await DropColumnFamilyAsync(cfd, synchronous, ct).ConfigureAwait(false);
                cfd = await CreateColumnFamilyCoreAsync(PersistentStatsColumnFamilyName, OptimizeForPersistentStats(new ColumnFamilyOptions()),
                    writeOptionsFile: false, synchronous, ct).ConfigureAwait(false);
                persistFormatVersion = true;
            }
        }
        _persistStatsCfd = cfd;
        if (persistFormatVersion)
        {
            using var batch = new WriteBatch();
            batch.Put(cfd.Id, Encoding.ASCII.GetBytes(StatsFormatVersionKey),
                Encoding.ASCII.GetBytes(StatsCfCurrentFormatVersion.ToString(CultureInfo.InvariantCulture)));
            batch.Put(cfd.Id, Encoding.ASCII.GetBytes(StatsCompatibleVersionKey),
                Encoding.ASCII.GetBytes(StatsCfCompatibleFormatVersion.ToString(CultureInfo.InvariantCulture)));
            await WriteAsync(StatsWriteOptions(), batch, validateColumnFamilies: false, synchronous, ct).ConfigureAwait(false);
        }
    }

    private static WriteOptions StatsWriteOptions() => new() { LowPri = true, NoSlowdown = true, Sync = false };

    /// <summary>DecodePersistentStatsVersionNumber: Get of the version key (ParseUint64 of its value).</summary>
    private bool TryReadStatsVersion(ColumnFamilyData cfd, string key, out ulong version)
    {
        version = 0;
        byte[] k = Encoding.ASCII.GetBytes(key);
        using Iterator it = DbIteratorFactory.Create(NewIteratorSources(cfd, new ReadOptions { VerifyChecksums = true }));
        it.Seek(k);
        if (!it.Valid || !it.Key.SequenceEqual(k) || it.Value.Length == 0) return false;
        version = ParseUInt64Lenient(it.Value);
        return true;
    }

    /// <summary>ParseUint64 (util/string_util.cc) as used here: leading decimal digits, 0 when there are none.</summary>
    private static ulong ParseUInt64Lenient(ReadOnlySpan<byte> s)
    {
        ulong v = 0;
        foreach (byte b in s)
        {
            if (b is < (byte)'0' or > (byte)'9') break;
            v = unchecked(v * 10 + (ulong)(b - '0'));
        }
        return v;
    }

    /// <summary>EncodePersistentStatsKey: "%010d#name" (the time as a C int, zero padded to 10 digits).</summary>
    internal static string EncodePersistentStatsKey(ulong nowSeconds, string name) => FormatStatsTime(nowSeconds) + "#" + name;

    private static string FormatStatsTime(ulong seconds) =>
        unchecked((int)seconds).ToString("D" + NowSecondsStringLength.ToString(CultureInfo.InvariantCulture), CultureInfo.InvariantCulture);

    /// <summary>Registers PersistStats every stats_persist_period_sec (first run right away, like run_immediately).</summary>
    private void StartStatsPersist()
    {
        uint period = Options.StatsPersistPeriodSec;
        if (period == 0) return;
        _statsPersistTimer = new Timer(static s => ((DbImpl)s!).PersistStats(), this, TimeSpan.Zero, TimeSpan.FromSeconds(period));
    }

    /// <summary>SetDBOptions(stats_persist_period_sec): re-registers (or unregisters, for 0) the persist task.</summary>
    private void RestartStatsPersist()
    {
        _statsPersistTimer?.Dispose();
        _statsPersistTimer = null;
        if (!IsDisposed) StartStatsPersist();
    }

    /// <summary>The periodic persist task (null when stats_persist_period_sec = 0), for tests.</summary>
    internal Timer? StatsPersistTimerForTest => _statsPersistTimer;

    private void StopStatsPersist()
    {
        _statsPersistTimer?.Dispose();
        _statsPersistTimer = null;
    }

    /// <summary>The timer callback: <see cref="PersistStats(ulong)"/> at the current time.</summary>
    private void PersistStats() => PersistStats((ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds());

    /// <summary>
    /// Port of DBImpl::PersistStats at <paramref name="nowSeconds"/>: samples the tickers and stores the deltas since the
    /// previous sample (the first sample only initializes the baseline). Errors writing the stats column family are
    /// logged and ignored, like upstream.
    /// </summary>
    internal void PersistStats(ulong nowSeconds)
    {
        if (IsDisposed) return;
        if (Interlocked.Exchange(ref _persistStatsRunning, 1) != 0) return;
        try
        {
            Statistics? statistics = Options.Statistics;
            if (statistics is null) return;
            ulong sizeLimit;
            lock (_mutex) sizeLimit = Options.StatsHistoryBufferSize;
            Dictionary<string, ulong> statsMap = statistics.GetTickerMap();
            LogInfo("------- PERSISTING STATS -------");
            if (Options.PersistStatsToDisk)
            {
                ColumnFamilyData? cfd = _persistStatsCfd;
                if (cfd is null) return;
                using var batch = new WriteBatch();
                Dictionary<string, ulong>? slice = _statsSlice;
                if (slice is not null)
                {
                    LogInfo("Reading " + slice.Count.ToString(CultureInfo.InvariantCulture) + " stats from statistics\n");
                    foreach (KeyValuePair<string, ulong> stat in statsMap)
                    {
                        // calculate the delta from last time
                        if (slice.TryGetValue(stat.Key, out ulong old))
                        {
                            batch.Put(cfd.Id, Encoding.UTF8.GetBytes(EncodePersistentStatsKey(nowSeconds, stat.Key)),
                                Encoding.ASCII.GetBytes(unchecked(stat.Value - old).ToString(CultureInfo.InvariantCulture)));
                        }
                    }
                }
                _statsSlice = statsMap;
                try
                {
                    WriteAsync(StatsWriteOptions(), batch, validateColumnFamilies: false, synchronous: true, CancellationToken.None)
                        .GetAwaiter().GetResult();
                    LogInfo("Writing " + statsMap.Count.ToString(CultureInfo.InvariantCulture) + " stats with timestamp "
                        + nowSeconds.ToString(CultureInfo.InvariantCulture) + " to persistent stats CF succeeded");
                }
                catch (Exception e) when (e is RocksDbException or IOException or ObjectDisposedException)
                {
                    LogInfo("Writing to persistent stats CF failed -- " + e.Message);
                }
            }
            else
            {
                lock (_statsHistoryLock)
                {
                    Dictionary<string, ulong>? slice = _statsSlice;
                    if (slice is not null)
                    {
                        var delta = new Dictionary<string, ulong>(statsMap.Count, StringComparer.Ordinal);
                        foreach (KeyValuePair<string, ulong> stat in statsMap)
                        {
                            if (slice.TryGetValue(stat.Key, out ulong old)) delta[stat.Key] = unchecked(stat.Value - old);
                        }
                        LogInfo("Storing " + slice.Count.ToString(CultureInfo.InvariantCulture) + " stats with timestamp "
                            + nowSeconds.ToString(CultureInfo.InvariantCulture) + " to in-memory stats history");
                        _statsHistory[nowSeconds] = delta;
                    }
                    _statsSlice = statsMap;
                    // delete older stats snapshots to control memory consumption
                    ulong size = EstimateInMemoryStatsHistorySize();
                    bool purgeNeeded = size > sizeLimit;
                    LogInfo("[Pre-GC] In-memory stats history size: " + size.ToString(CultureInfo.InvariantCulture) + " bytes, slice count: "
                        + _statsHistory.Count.ToString(CultureInfo.InvariantCulture));
                    while (purgeNeeded && _statsHistory.Count > 0)
                    {
                        _statsHistory.RemoveAt(0);
                        purgeNeeded = EstimateInMemoryStatsHistorySize() > sizeLimit;
                    }
                    LogInfo("[Post-GC] In-memory stats history size: " + size.ToString(CultureInfo.InvariantCulture) + " bytes, slice count: "
                        + _statsHistory.Count.ToString(CultureInfo.InvariantCulture));
                }
            }
        }
        catch (Exception e) when (e is RocksDbException or ObjectDisposedException or InvalidOperationException)
        {
            // The DB is closing.
        }
        finally
        {
            Volatile.Write(ref _persistStatsRunning, 0);
        }
    }

    /// <summary>
    /// Port of EstimateInMemoryStatsHistorySize with upstream's 64-bit libstdc++ sizes: sizeof(std::map) = 48,
    /// sizeof(std::string) = 32, a string's capacity at least 15 (small string buffer); every slice is assumed to be as
    /// large as the oldest one. REQUIRES: _statsHistoryLock.
    /// </summary>
    private ulong EstimateInMemoryStatsHistorySize()
    {
        const ulong mapSize = 48, stringSize = 32, u64Size = 8;
        ulong total = mapSize;
        if (_statsHistory.Count == 0) return total;
        ulong perSlice = u64Size + mapSize;
        foreach (KeyValuePair<string, ulong> kv in _statsHistory.Values[0])
        {
            perSlice += (ulong)Math.Max(15, kv.Key.Length) + stringSize + u64Size;
        }
        return perSlice * (ulong)_statsHistory.Count;
    }

    /// <summary>TEST_EstimateInMemoryStatsHistorySize.</summary>
    internal ulong EstimateInMemoryStatsHistorySizeForTest()
    {
        lock (_statsHistoryLock) return EstimateInMemoryStatsHistorySize();
    }

    /// <summary>
    /// Port of DBImpl::GetStatsHistory: the samples with start_time &lt;= time &lt; end_time, oldest first, read from the
    /// stats history column family (persist_stats_to_disk) or memory. Like upstream's PersistentStatsHistoryIterator,
    /// a persisted sample at exactly end_time is included.
    /// </summary>
    public List<StatsHistorySlice> GetStatsHistory(ulong startTime, ulong endTime)
    {
        ThrowIfDisposed();
        var result = new List<StatsHistorySlice>();
        if (Options.PersistStatsToDisk)
        {
            ColumnFamilyData? cfd = _persistStatsCfd;
            if (cfd is null) return result;
            ReadPersistentStatsHistory(cfd, startTime, endTime, result);
            return result;
        }
        lock (_statsHistoryLock)
        {
            // FindStatsByTime: lower_bound(start_time), < end_time.
            IList<ulong> keys = _statsHistory.Keys;
            int lo = 0, hi = keys.Count;
            while (lo < hi)
            {
                int mid = (lo + hi) >>> 1;
                if (keys[mid] < startTime) lo = mid + 1;
                else hi = mid;
            }
            for (int i = lo; i < keys.Count && keys[i] < endTime; i++)
            {
                result.Add(new StatsHistorySlice(keys[i], new Dictionary<string, ulong>(_statsHistory.Values[i], StringComparer.Ordinal)));
            }
        }
        return result;
    }

    /// <summary>PersistentStatsHistoryIterator::AdvanceIteratorByTime over the whole window.</summary>
    private void ReadPersistentStatsHistory(ColumnFamilyData cfd, ulong startTime, ulong endTime, List<StatsHistorySlice> result)
    {
        using Iterator it = DbIteratorFactory.Create(NewIteratorSources(cfd, null));
        ulong from = startTime;
        while (true)
        {
            it.Seek(Encoding.ASCII.GetBytes(FormatStatsTime(from)));
            // no more entries with timestamp >= start_time is found or version key is found to be incompatible
            if (!it.Valid) return;
            ulong time = ParseStatsKey(it.Key, startTime, out _);
            // check parsed time and invalid if it exceeds end_time
            if (time > endTime) return;
            var stats = new Dictionary<string, ulong>(StringComparer.Ordinal);
            for (; it.Valid; it.Next())
            {
                ulong t = ParseStatsKey(it.Key, startTime, out string name);
                if (t != time) break;
                if (name == StatsFormatVersionKey) continue;
                stats[name] = ParseUInt64Lenient(it.Value);
            }
            result.Add(new StatsHistorySlice(time, stats));
            // increment start_time by 1 to avoid infinite loop
            from = time + 1;
        }
    }

    /// <summary>parseKey: "time#name"; ulong.MaxValue for keys without '#' or with a time below start_time.</summary>
    private static ulong ParseStatsKey(ReadOnlySpan<byte> key, ulong startTime, out string name)
    {
        int pos = key.IndexOf((byte)'#');
        name = "";
        if (pos < 0) return ulong.MaxValue;
        ulong parsed = ParseUInt64Lenient(key[..pos]);
        if (parsed < startTime) return ulong.MaxValue;
        name = Encoding.UTF8.GetString(key[(pos + 1)..]);
        return parsed;
    }
}
