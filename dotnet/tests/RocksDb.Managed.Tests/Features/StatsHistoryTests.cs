using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Features;

/// <summary>
/// Statistics history (DB::GetStatsHistory, persist_stats_to_disk, stats_history_buffer_size); port of the spirit of
/// monitoring/stats_history_test.cc. Samples are taken by calling PersistStats with explicit times (the periodic task
/// is disabled with stats_persist_period_sec = 0).
/// </summary>
[TestClass]
public sealed class StatsHistoryTests
{
    private const string StatsCf = "___rocksdb_stats_history___";
    private static readonly string s_keysWritten = Statistics.GetTickerName(Ticker.NumberKeysWritten);

    private static DbOptions Options(MemoryFileSystem fs, bool persist)
    {
        DbOptions o = NewOptions(fs);
        o.Statistics = new Statistics();
        o.StatsPersistPeriodSec = 0;
        o.PersistStatsToDisk = persist;
        return o;
    }

    private static void WriteKeys(RocksDatabase db, int from, int count)
    {
        for (int i = from; i < from + count; i++) db.Put(Key(i), Value(i, 0));
    }

    private static void Sample(RocksDatabase db, ulong time) => db.DefaultColumnFamily.Db.PersistStats(time);

    [TestMethod]
    public void InMemoryHistoryKeepsDeltasInTheWindow()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, Options(fs, persist: false));
        WriteKeys(db, 0, 5);
        Sample(db, 100); // the first sample only sets the baseline
        WriteKeys(db, 5, 3);
        Sample(db, 200);
        WriteKeys(db, 8, 4);
        Sample(db, 300);
        IReadOnlyList<StatsHistorySlice> all = db.GetStatsHistory(0, 1000);
        Assert.AreEqual(2, all.Count);
        Assert.AreEqual(200UL, all[0].Time);
        Assert.AreEqual(3UL, all[0].Stats[s_keysWritten]);
        Assert.AreEqual(300UL, all[1].Time);
        Assert.AreEqual(4UL, all[1].Stats[s_keysWritten]);
        // [start, end): end is exclusive.
        Assert.AreEqual(1, db.GetStatsHistory(0, 300).Count);
        Assert.AreEqual(1, db.GetStatsHistory(201, 301).Count);
        Assert.AreEqual(0, db.GetStatsHistory(301, 1000).Count);
        Assert.IsFalse(db.TryGetColumnFamily(StatsCf, out _), "no stats column family without persist_stats_to_disk");
    }

    [TestMethod]
    public void InMemoryHistoryIsBoundedByStatsHistoryBufferSize()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = Options(fs, persist: false);
        using RocksDatabase db = RocksDatabase.Open(DbPath, o);
        for (ulong t = 1; t <= 20; t++)
        {
            WriteKeys(db, (int)t, 1);
            Sample(db, t);
        }
        Assert.AreEqual(19, db.GetStatsHistory(0, 100).Count);
        ulong oneSlice = db.DefaultColumnFamily.Db.EstimateInMemoryStatsHistorySizeForTest() / 19;
        // Shrink the buffer to about 5 slices: the next sample drops the oldest ones.
        db.SetDBOptions([new("stats_history_buffer_size", (oneSlice * 5 + 60).ToString(System.Globalization.CultureInfo.InvariantCulture))]);
        WriteKeys(db, 100, 1);
        Sample(db, 21);
        IReadOnlyList<StatsHistorySlice> left = db.GetStatsHistory(0, 100);
        Assert.AreEqual(5, left.Count);
        Assert.AreEqual(21UL, left[^1].Time);
        Assert.AreEqual(17UL, left[0].Time);
    }

    [TestMethod]
    public void PersistedHistorySurvivesReopenAndTheColumnFamilyOpensImplicitly()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, Options(fs, persist: true)))
        {
            Assert.IsTrue(db.TryGetColumnFamily(StatsCf, out ColumnFamily? cf));
            Assert.AreEqual("1", GetString(cf!, "__persistent_stats_format_version__"));
            Assert.AreEqual("1", GetString(cf!, "__persistent_stats_compatible_version__"));
            Assert.AreEqual(2UL << 20, cf!.Options.WriteBufferSize, "OptimizeForPersistentStats");
            WriteKeys(db, 0, 2);
            Sample(db, 1000);
            WriteKeys(db, 2, 7);
            Sample(db, 2000);
            Assert.AreEqual("7", GetString(cf, "0000002000#" + s_keysWritten));
            IReadOnlyList<StatsHistorySlice> h = db.GetStatsHistory(0, 5000);
            Assert.AreEqual(1, h.Count);
            Assert.AreEqual(7UL, h[0].Stats[s_keysWritten]);
            // Like upstream's persistent iterator, a sample at exactly end_time is included.
            Assert.AreEqual(1, db.GetStatsHistory(0, 2000).Count);
            Assert.AreEqual(0, db.GetStatsHistory(2001, 5000).Count);
        }
        CollectionAssert.Contains(RocksDatabase.ListColumnFamilies(DbPath, fs), StatsCf);
        // Without persist_stats_to_disk and without naming the column family (VersionEditHandler opens it implicitly).
        using (RocksDatabase db = RocksDatabase.Open(DbPath, Options(fs, persist: false)))
        {
            Assert.IsTrue(db.TryGetColumnFamily(StatsCf, out ColumnFamily? cf));
            Assert.AreEqual(2UL << 20, cf!.Options.WriteBufferSize);
            Assert.AreEqual(0, db.GetStatsHistory(0, 5000).Count, "in-memory history");
        }
        using (RocksDatabase db = RocksDatabase.OpenReadOnly(DbPath, Options(fs, persist: false)))
        {
            Assert.IsTrue(db.TryGetColumnFamily(StatsCf, out _));
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, Options(fs, persist: true)))
        {
            IReadOnlyList<StatsHistorySlice> h = db.GetStatsHistory(0, 5000);
            Assert.AreEqual(1, h.Count);
            Assert.AreEqual(2000UL, h[0].Time);
        }
    }

    [TestMethod]
    public void MissingVersionKeysRecreateTheStatsColumnFamily()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, Options(fs, persist: true)))
        {
            ColumnFamily cf = db.GetColumnFamily(StatsCf);
            cf.Put(B("0000000005#x"), B("1"));
            cf.Delete(B("__persistent_stats_format_version__"));
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, Options(fs, persist: true)))
        {
            ColumnFamily cf = db.GetColumnFamily(StatsCf);
            Assert.AreEqual("1", GetString(cf, "__persistent_stats_format_version__"));
            Assert.IsNull(GetString(cf, "0000000005#x"), "the old column family was dropped");
        }
    }

    [TestMethod]
    public void StatsPersistPeriodIsReRegisteredBySetDbOptions()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, Options(fs, persist: false));
        Assert.IsNull(db.DefaultColumnFamily.Db.StatsPersistTimerForTest);
        db.SetDBOptions([new("stats_persist_period_sec", "3600")]);
        Assert.IsNotNull(db.DefaultColumnFamily.Db.StatsPersistTimerForTest);
        db.SetDBOptions([new("stats_persist_period_sec", "0")]);
        Assert.IsNull(db.DefaultColumnFamily.Db.StatsPersistTimerForTest);
    }
}
