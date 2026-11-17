using System.Buffers;
using System.Text;
using RocksDb.Managed.Monitoring;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #11, row cache (DBOptions::row_cache) + MergeOperator::ShouldMerge. On a row cache miss TableCache::Get /
/// MultiGet (db/table_cache_sync_and_async.h) record the entries GetContext::SaveValue visits in the file
/// (appendToReplayLog) and insert them as the file's row cache entry for the key ("only if something was found"). When
/// ShouldMerge ends the lookup inside the file (SaveValue, kTypeMerge: MergeWithNoBaseValue, return false), that log holds
/// only the operands up to that point. A later lookup with the same row cache key that reaches the file with fewer
/// operands (a snapshot read above the file's largest seqno that does not see the newer memtable operands, or
/// GetMergeOperands, which does not call ShouldMerge) replays the truncated entry, finds the file "exhausted" in state
/// kMerge and continues with older files (Version::Get): it merges the wrong base value. Reproduced against native
/// RocksDB 11.12 with a C++ program (docs/upstream-issues/16-row-cache-should-merge-truncated-entry.md). Fix (port and
/// proposed upstream): a lookup that ShouldMerge ends inside a file does not cache that file's entry.
/// </summary>
[TestClass]
public sealed class Audit11RowCacheShouldMergeTests
{
    /// <summary>
    /// String append with ','; ShouldMerge (operands newest first) is true once <c>operands.Count &gt;= limit</c> (upstream's
    /// LimitedStringAppendMergeOp, db/db_merge_operator_test.cc). limit 0: never.
    /// </summary>
    private sealed class LimitedAppend(int limit) : MergeOperator
    {
        public override string Name => "Audit11RowCacheLimitedAppend";

        public override bool FullMerge(ReadOnlySpan<byte> key, bool hasExistingValue, ReadOnlySpan<byte> existingValue, in MergeOperands operands,
            IBufferWriter<byte> result)
        {
            bool first = true;
            if (hasExistingValue)
            {
                result.Write(existingValue);
                first = false;
            }
            for (int i = 0; i < operands.Count; i++)
            {
                if (!first) result.Write(","u8);
                result.Write(operands[i]);
                first = false;
            }
            return true;
        }

        public override bool ShouldMerge(in MergeOperands operands) => limit > 0 && operands.Count >= limit;
    }

    private static RocksDatabase Open(bool rowCache, int limit, Statistics? stats = null)
    {
        DbOptions o = NewOptions(NewFs());
        if (rowCache) o.RowCache = new Managed.Cache.RowCache(1 << 20);
        o.Statistics = stats;
        return RocksDatabase.Open(DbPath, o, new ColumnFamilyOptions { MergeOperator = new LimitedAppend(limit), DisableAutoCompactions = true });
    }

    /// <summary>
    /// The sequence of the native reproduction: an older file with "old"; a newer file with base, a, b (a snapshot held
    /// during the flush keeps the operands); a write to another key, so a snapshot taken now is above the newer file's
    /// largest seqno (CreateRowCacheKeyPrefix then uses the no-snapshot row cache key); two memtable operands.
    /// </summary>
    private static Snapshot Populate(RocksDatabase db)
    {
        db.Put(B("k"), B("old"));
        db.Flush();
        db.Put(B("k"), B("base"));
        using (db.GetSnapshot())
        {
            db.Merge(B("k"), B("a"));
            db.Merge(B("k"), B("b"));
            db.Flush();
        }
        db.Put(B("other"), B("x"));
        Snapshot snap = db.GetSnapshot();
        db.Merge(B("k"), B("m1"));
        db.Merge(B("k"), B("m2"));
        return snap;
    }

    private static string? GetVia(RocksDatabase db, ReadOptions? ro, int path)
    {
        switch (path)
        {
            case 0:
                return S(db.Get(ro, B("k")));
            case 1:
                return S(db.GetAsync(ro, B("k")).AsTask().GetAwaiter().GetResult());
            case 2:
                return S(db.MultiGet(ro, [B("k"), B("other")])[0]);
            default:
            {
                using var batch = new MultiGetBatch();
                batch.Add(B("k"));
                batch.Add(B("zzz"));
                db.MultiGetAsync(ro, batch).AsTask().GetAwaiter().GetResult();
                return batch.GetStatus(0) == MultiGetStatus.Found ? S(batch.GetValue(0).ToArray()) : null;
            }
        }
    }

    private static readonly string[] PathNames = ["Get", "GetAsync", "MultiGet", "MultiGetAsync"];

    /// <summary>
    /// TableCache::Get / MultiGet + GetContext::SaveValue (kTypeMerge, ShouldMerge true) + Version::Get (kMerge goes on to
    /// the next file). The latest read stops at "b" in the newer file (m2, m1, b: three operands) and caches that file's
    /// entry; the snapshot read then needs b, a and the base value of the same file. Without the row cache (and with the
    /// fix) it returns "base,a,b"; native 11.12 and the port before the fix returned "old,b" (the overwritten value of
    /// the older file, "a" and "base" lost), for every combination of populating and reading path.
    /// </summary>
    [TestMethod]
    public void SnapshotRead_AfterLookupEndedByShouldMerge_MatchesNoRowCache()
    {
        for (int populate = 0; populate < 4; populate++)
        {
            for (int read = 0; read < 4; read++)
            {
                string ctx = $"populate={PathNames[populate]} read={PathNames[read]}";
                foreach (bool rowCache in new[] { false, true })
                {
                    using RocksDatabase db = Open(rowCache, limit: 3);
                    using Snapshot snap = Populate(db);
                    Assert.AreEqual("b,m1,m2", GetVia(db, null, populate), ctx);
                    var ro = new ReadOptions { Snapshot = snap };
                    Assert.AreEqual("base,a,b", GetVia(db, ro, read), ctx + " rowCache=" + rowCache);
                    // GetEntity reads through the same GetContext (plain value as the default column).
                    WideColumns? e = db.DefaultColumnFamily.GetEntity(ro, B("k"));
                    Assert.IsNotNull(e, ctx);
                    Assert.AreEqual("base,a,b", S(e.DefaultColumnValue.ToArray()), ctx);
                    // The latest read is unchanged (and still answered from the table, not from a truncated entry).
                    Assert.AreEqual("b,m1,m2", GetVia(db, null, read), ctx);
                }
            }
        }
    }

    /// <summary>
    /// GetMergeOperands (do_merge_ = false, ShouldMerge not called) after a Get that ShouldMerge ended inside the newer
    /// file: upstream replays the truncated row cache entry and continues with the older file, returning
    /// [old, b, m1, m2] instead of [base, a, b, m1, m2] (found by reading; the C API has no GetMergeOperands).
    /// </summary>
    [TestMethod]
    public void GetMergeOperands_AfterLookupEndedByShouldMerge_MatchesNoRowCache()
    {
        foreach (bool rowCache in new[] { false, true })
        {
            using RocksDatabase db = Open(rowCache, limit: 3);
            using Snapshot snap = Populate(db);
            Assert.AreEqual("b,m1,m2", GetString(db, "k"));
            using var r = new MergeOperandsResult();
            Assert.IsTrue(db.TryGetMergeOperands(B("k"), new GetMergeOperandsOptions { ExpectedMaxNumberOfOperands = 10 }, r));
            var got = new List<string>();
            for (int i = 0; i < r.Count; i++) got.Add(Encoding.UTF8.GetString(r[i]));
            Assert.AreEqual("base,a,b,m1,m2", string.Join(",", got), "rowCache=" + rowCache);
        }
    }

    /// <summary>
    /// The fix only skips the insert for a lookup that ShouldMerge ended inside the file: lookups that end at a base value
    /// in the file (or below the ShouldMerge limit) are still cached and replayed (ROW_CACHE_HIT), and a lookup ended by
    /// ShouldMerge is a ROW_CACHE_MISS every time (it reads the table) instead of hitting a truncated entry.
    /// </summary>
    [TestMethod]
    public void OnlyLookupsEndedByShouldMergeAreNotCached()
    {
        var stats = new Statistics();
        using RocksDatabase db = Open(rowCache: true, limit: 3, stats);
        using Snapshot snap = Populate(db);

        // Latest: ShouldMerge ends the lookup in the newer file; nothing is cached for it.
        ulong hits = stats.GetTickerCount(Ticker.RowCacheHit);
        ulong misses = stats.GetTickerCount(Ticker.RowCacheMiss);
        Assert.AreEqual("b,m1,m2", GetString(db, "k"));
        Assert.AreEqual("b,m1,m2", GetString(db, "k"));
        Assert.AreEqual(hits, stats.GetTickerCount(Ticker.RowCacheHit));
        Assert.AreEqual(misses + 2, stats.GetTickerCount(Ticker.RowCacheMiss));

        // Snapshot read: ends at the base value of the newer file; cached on the first read, replayed on the second.
        Assert.AreEqual("base,a,b", GetString(db, "k", snap));
        Assert.AreEqual(hits, stats.GetTickerCount(Ticker.RowCacheHit));
        Assert.AreEqual("base,a,b", GetString(db, "k", snap));
        Assert.AreEqual(hits + 1, stats.GetTickerCount(Ticker.RowCacheHit));
        // The latest read replays the complete entry (b) and ShouldMerge stops it there, as reading the table does.
        Assert.AreEqual("b,m1,m2", GetString(db, "k"));
        Assert.AreEqual(hits + 2, stats.GetTickerCount(Ticker.RowCacheHit));
    }

    /// <summary>
    /// Differential: the same seeded workload (puts, merges, deletes, flushes, compactions, snapshots taken and released,
    /// Get / GetAsync / MultiGet / GetEntity / GetMergeOperands at the latest state and at live snapshots) on a DB with a
    /// row cache and one without, for ShouldMerge limits 0 (never) to 4; every read must agree.
    /// </summary>
    [TestMethod]
    public void Randomized_RowCacheMatchesNoRowCache()
    {
        int baseSeed = Environment.TickCount;
        for (int run = 0; run < 25; run++)
        {
            int seed = baseSeed + run;
            int limit = run % 5;
            try
            {
                RunRandomized(seed, limit);
            }
            catch (Exception ex) when (ex is not AssertFailedException)
            {
                throw new AssertFailedException($"seed={seed} limit={limit}: {ex}", ex);
            }
        }
    }

    private static void RunRandomized(int seed, int limit)
    {
        var rnd = new Random(seed);
        using RocksDatabase plain = Open(rowCache: false, limit);
        using RocksDatabase cached = Open(rowCache: true, limit);
        RocksDatabase[] dbs = [plain, cached];
        var snaps = new List<Snapshot[]>();
        try
        {
            string[] keys = ["a", "b", "c", "d"];
            for (int step = 0; step < 400; step++)
            {
                string key = keys[rnd.Next(keys.Length)];
                int op = rnd.Next(100);
                string where = $"seed={seed} limit={limit} step={step} op={op} key={key}";
                if (op < 15)
                {
                    string v = "p" + step;
                    foreach (RocksDatabase db in dbs) db.Put(B(key), B(v));
                }
                else if (op < 45)
                {
                    string v = "m" + step;
                    foreach (RocksDatabase db in dbs) db.Merge(B(key), B(v));
                }
                else if (op < 49)
                {
                    foreach (RocksDatabase db in dbs) db.Delete(B(key));
                }
                else if (op < 56)
                {
                    foreach (RocksDatabase db in dbs) db.Flush();
                }
                else if (op < 58)
                {
                    foreach (RocksDatabase db in dbs) db.CompactRange();
                }
                else if (op < 62)
                {
                    snaps.Add([plain.GetSnapshot(), cached.GetSnapshot()]);
                }
                else if (op < 64 && snaps.Count > 0)
                {
                    int i = rnd.Next(snaps.Count);
                    snaps[i][0].Dispose();
                    snaps[i][1].Dispose();
                    snaps.RemoveAt(i);
                }
                else
                {
                    int s = snaps.Count > 0 && rnd.Next(2) == 0 ? rnd.Next(snaps.Count) : -1;
                    int path = rnd.Next(6);
                    string? a = Read(plain, s < 0 ? null : snaps[s][0], key, path);
                    string? b = Read(cached, s < 0 ? null : snaps[s][1], key, path);
                    Assert.AreEqual(a, b, $"{where} snapshot={s} path={path}");
                }
            }
        }
        finally
        {
            foreach (Snapshot[] s in snaps)
            {
                s[0].Dispose();
                s[1].Dispose();
            }
        }
    }

    private static string? Read(RocksDatabase db, Snapshot? snap, string key, int path)
    {
        ReadOptions? ro = snap is null ? null : new ReadOptions { Snapshot = snap };
        switch (path)
        {
            case 0:
                return S(db.Get(ro, B(key)));
            case 1:
                return S(db.GetAsync(ro, B(key)).AsTask().GetAwaiter().GetResult());
            case 2:
                return S(db.MultiGet(ro, [B("a"), B(key), B("d")])[1]);
            case 3:
            {
                WideColumns? e = db.DefaultColumnFamily.GetEntity(ro, B(key));
                return e is null ? null : "E:" + S(e.DefaultColumnValue.ToArray());
            }
            case 4:
            {
                using var batch = new MultiGetBatch();
                batch.Add(B(key));
                db.MultiGetAsync(ro, batch).AsTask().GetAwaiter().GetResult();
                return batch.GetStatus(0) == MultiGetStatus.Found ? S(batch.GetValue(0).ToArray()) : batch.GetStatus(0).ToString();
            }
            default:
            {
                using var r = new MergeOperandsResult();
                if (!db.TryGetMergeOperands(ro, B(key), new GetMergeOperandsOptions { ExpectedMaxNumberOfOperands = 1000 }, r)) return "NotFound";
                var sb = new StringBuilder("O:");
                for (int i = 0; i < r.Count; i++) sb.Append(Encoding.UTF8.GetString(r[i])).Append('|');
                return sb.ToString();
            }
        }
    }
}
