using System.Buffers.Binary;
using System.Diagnostics;
using System.Text;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Utilities.Transactions;
using RocksDb.Managed.Wal;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>
/// The managed WAL matrix: WAL settings x write modes x failure modes on the in-memory fault-injection file system.
/// <para>
/// Factors (<see cref="Factor"/>): wal_recovery_mode (4), paranoid_checks, recycle_log_file_num 0/1,
/// track_and_verify_wals, track_and_verify_wals_in_manifest, manual_wal_flush, wal_compression none/zstd, a separate
/// wal_dir, WAL archival (WAL_ttl_seconds), a small max_total_wal_size (forces SwitchWAL), avoid_flush_during_recovery,
/// avoid_flush_during_shutdown, the write mode (default, two_write_queues, pipelined, unordered, WritePrepared 2PC with a
/// prepared section spanning WALs and one left prepared at the crash), user-defined timestamps on cf1 and blob direct
/// write on the default CF. The 15 factors are covered pairwise (a greedy AETG covering array, about 30 rows, coverage
/// asserted); every row is run against every <see cref="UnitFault"/> (crash types: clean close, process kill, power loss,
/// a WAL sync that fails before the power loss, a MANIFEST write that fails at a flush; damage: torn / corrupted / zeroed
/// / missing / stale / swapped WALs and a dropped predecessor record). <c>WAL_MATRIX_LONG=1</c> adds 400 random rows.
/// </para>
/// <para>
/// Invariants asserted for each scenario (the exact outcome of each damaged image is compared with native RocksDB by
/// the compat matrix, <c>WalMatrixCompatTests</c>): the reopen never throws anything but a RocksDbException, and fails only
/// with Corruption (or the documented codes of the damage); crash-type faults always reopen, in every recovery mode, and
/// keep every write acknowledged before the last successful sync (no acknowledged-synced write lost); outside
/// kSkipAnyCorruptedRecords the recovered contents of both column families are the model's state after one prefix of the
/// committed writes (point-in-time / consistency across column families, no resurrected deletes, prepared but
/// uncommitted 2PC data invisible); with kSkipAnyCorruptedRecords every recovered value is one the key had; the
/// recovered DB accepts writes and reopens cleanly with the same contents.
/// </para>
/// </summary>
[TestClass]
public sealed class WalMatrixTests
{
    public TestContext TestContext { get; set; } = null!;

    [TestCleanup]
    public void Cleanup() => TestHooks.ClearAll();

    internal enum WriteMode
    {
        Default,
        TwoWriteQueues,
        Pipelined,
        Unordered,
        WritePrepared2Pc,
    }

    internal enum UnitFault
    {
        CleanClose,
        ProcessKill,
        PowerLoss,
        WalSyncFailsThenPowerLoss,
        ManifestWriteFailsThenPowerLoss,
        TornLastRecord,
        CorruptMiddleChecksum,
        CorruptOldestWalChecksum,
        ZeroedTail,
        MissingOldestWal,
        MissingLatestWal,
        StaleWalAbove,
        EmptyNewWal,
        SwappedLatestWals,
        DroppedPredecessorRecord,
    }

    private static bool IsPowerLoss(UnitFault f) => f is UnitFault.PowerLoss or UnitFault.WalSyncFailsThenPowerLoss or UnitFault.ManifestWriteFailsThenPowerLoss;

    private static bool IsCrashFault(UnitFault f) => f is UnitFault.CleanClose or UnitFault.ProcessKill or UnitFault.PowerLoss
        or UnitFault.WalSyncFailsThenPowerLoss or UnitFault.ManifestWriteFailsThenPowerLoss or UnitFault.EmptyNewWal;

    /// <summary>Factor indices and their level counts.</summary>
    private static readonly int[] s_levels = [4, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 5, 2, 2];

    internal sealed record Factor(
        WalRecoveryMode Mode, bool Paranoid, bool Recycle, bool TrackWals, bool TrackInManifest, bool ManualWalFlush, bool Zstd,
        bool WalDir, bool Archival, bool SmallMaxTotalWalSize, bool AvoidFlushDuringRecovery, bool AvoidFlushDuringShutdown,
        WriteMode Write, bool Udt, bool BlobDirectWrite)
    {
        public static Factor FromRow(int[] r) => new(
            (WalRecoveryMode)r[0], r[1] == 1, r[2] == 1, r[3] == 1, r[4] == 1, r[5] == 1, r[6] == 1, r[7] == 1, r[8] == 1, r[9] == 1,
            r[10] == 1, r[11] == 1, (WriteMode)r[12], r[13] == 1, r[14] == 1);

        public override string ToString()
        {
            var sb = new StringBuilder(Mode.ToString());
            if (!Paranoid) sb.Append("+noParanoid");
            if (Recycle) sb.Append("+recycle");
            if (TrackWals) sb.Append("+trackWals");
            if (TrackInManifest) sb.Append("+trackManifest");
            if (ManualWalFlush) sb.Append("+manualFlush");
            if (Zstd) sb.Append("+zstd");
            if (WalDir) sb.Append("+walDir");
            if (Archival) sb.Append("+archival");
            if (SmallMaxTotalWalSize) sb.Append("+smallMaxTotalWal");
            if (AvoidFlushDuringRecovery) sb.Append("+avoidFlushRecovery");
            if (AvoidFlushDuringShutdown) sb.Append("+avoidFlushShutdown");
            if (Write != WriteMode.Default) sb.Append('+').Append(Write);
            if (Udt) sb.Append("+udt");
            if (BlobDirectWrite) sb.Append("+blobDirectWrite");
            return sb.ToString();
        }
    }

    /// <summary>
    /// A strength-2 covering array over mixed-level factors (AETG-style greedy: each new row is the best of 400 random
    /// candidates, seeded, so the array is deterministic). Every pair of levels of every two factors occurs in a row.
    /// </summary>
    internal static List<int[]> Pairwise(int[] levels, int seed, Func<int, int, int, int, bool> excluded)
    {
        var rnd = new Random(seed);
        var uncovered = new HashSet<(int, int, int, int)>();
        for (int a = 0; a < levels.Length; a++)
        {
            for (int b = a + 1; b < levels.Length; b++)
            {
                for (int x = 0; x < levels[a]; x++)
                {
                    for (int y = 0; y < levels[b]; y++)
                    {
                        if (!excluded(a, x, b, y)) uncovered.Add((a, b, x, y));
                    }
                }
            }
        }
        var rows = new List<int[]>();
        while (uncovered.Count > 0)
        {
            int[]? best = null;
            int bestCount = -1;
            for (int c = 0; c < 400; c++)
            {
                var row = new int[levels.Length];
                for (int i = 0; i < levels.Length; i++) row[i] = rnd.Next(levels[i]);
                // Seed the candidate with one uncovered pair so that progress is guaranteed.
                if (c == 0)
                {
                    foreach ((int a, int b, int x, int y) in uncovered)
                    {
                        row[a] = x;
                        row[b] = y;
                        break;
                    }
                }
                Repair(row);
                int count = 0;
                foreach ((int a, int b, int x, int y) in uncovered)
                {
                    if (row[a] == x && row[b] == y) count++;
                }
                if (count > bestCount)
                {
                    best = row;
                    bestCount = count;
                }
            }
            rows.Add(best!);
            uncovered.RemoveWhere(p => best![p.Item1] == p.Item3 && best![p.Item2] == p.Item4);
        }
        return rows;
    }

    private const int ModeFactor = 12, UdtFactor = 13, BlobFactor = 14;

    /// <summary>
    /// Combinations upstream rejects or does not support, left out of the matrix (their rejection is checked by
    /// <see cref="UnsupportedCombinationsAreRejected"/>): blob direct write with two_write_queues, pipelined or
    /// unordered writes (NotSupported), and user-defined timestamps with WritePrepared (WritePreparedTxnDB has no UDT).
    /// </summary>
    private static bool Excluded(int a, int x, int b, int y)
    {
        if (a > b) return Excluded(b, y, a, x);
        if (a == ModeFactor && b == BlobFactor) return y == 1 && (WriteMode)x is WriteMode.TwoWriteQueues or WriteMode.Pipelined or WriteMode.Unordered;
        if (a == ModeFactor && b == UdtFactor) return y == 1 && (WriteMode)x == WriteMode.WritePrepared2Pc;
        return false;
    }

    private static void Repair(int[] row)
    {
        if (Excluded(ModeFactor, row[ModeFactor], BlobFactor, row[BlobFactor])) row[BlobFactor] = 0;
        if (Excluded(ModeFactor, row[ModeFactor], UdtFactor, row[UdtFactor])) row[UdtFactor] = 0;
    }

    private static bool Long =>
        Environment.GetEnvironmentVariable("WAL_MATRIX_LONG") == "1" || Environment.GetEnvironmentVariable("ROUNDTRIP_LONG") == "1";

    private static List<Factor> Rows()
    {
        var rows = new List<Factor>();
        foreach (int[] r in Pairwise(s_levels, seed: 20261104, Excluded)) rows.Add(Factor.FromRow(r));
        if (Long)
        {
            var rnd = new Random(42);
            for (int i = 0; i < 400; i++)
            {
                var r = new int[s_levels.Length];
                for (int j = 0; j < r.Length; j++) r[j] = rnd.Next(s_levels[j]);
                Repair(r);
                rows.Add(Factor.FromRow(r));
            }
        }
        return rows;
    }

    [TestMethod]
    public void PairwiseCoversEveryPairOfLevels()
    {
        List<int[]> rows = Pairwise(s_levels, seed: 20261104, Excluded);
        for (int a = 0; a < s_levels.Length; a++)
        {
            for (int b = a + 1; b < s_levels.Length; b++)
            {
                for (int x = 0; x < s_levels[a]; x++)
                {
                    for (int y = 0; y < s_levels[b]; y++)
                    {
                        if (Excluded(a, x, b, y)) continue;
                        Assert.IsTrue(rows.Exists(r => r[a] == x && r[b] == y), "factors " + a + "," + b + " levels " + x + "," + y);
                    }
                }
            }
        }
        Assert.IsTrue(rows.Count <= 40, "rows " + rows.Count);
    }

    [TestMethod]
    public void UnsupportedCombinationsAreRejected()
    {
        foreach (WriteMode mode in new[] { WriteMode.TwoWriteQueues, WriteMode.Pipelined, WriteMode.Unordered })
        {
            var f = new Factor(WalRecoveryMode.PointInTimeRecovery, true, false, false, false, false, false, false, false, false, false, false, mode, false, true);
            var e = Assert.ThrowsExactly<RocksDbException>(() => Open(NewFs(), f).Dispose(), mode.ToString());
            Assert.AreEqual(StatusCode.NotSupported, e.Code, mode.ToString());
            StringAssert.Contains(e.Message, "Blob direct write");
        }
    }

    // ---- model ---------------------------------------------------------------------------------------------------

    /// <summary>One committed change set (a write, a batch or a 2PC commit) in commit order.</summary>
    private sealed record Event(List<(int Cf, string Key, string? Value)> Changes, bool Synced);

    private sealed class Model
    {
        public readonly List<Event> Events = new();
        /// <summary>Index of the last event acknowledged by a successful sync write (or a successful SyncWal), or -1.</summary>
        public int LastSynced = -1;
        /// <summary>Every value each key had ("cf/key" -> values), for the kSkipAnyCorruptedRecords check.</summary>
        public readonly Dictionary<string, HashSet<string>> History = new(StringComparer.Ordinal);

        public void Add(Event e)
        {
            Events.Add(e);
            foreach ((int cf, string k, string? v) in e.Changes)
            {
                if (v is null) continue;
                string id = cf + "/" + k;
                if (!History.TryGetValue(id, out HashSet<string>? set)) History[id] = set = new HashSet<string>(StringComparer.Ordinal);
                set.Add(v);
            }
            if (e.Synced) LastSynced = Events.Count - 1;
        }

        /// <summary>The prefix lengths whose state equals <paramref name="state"/> (empty when none does).</summary>
        public List<int> MatchingPrefixes(SortedDictionary<string, string> state)
        {
            var r = new List<int>();
            var cur = new SortedDictionary<string, string>(StringComparer.Ordinal);
            if (Equal(cur, state)) r.Add(0);
            for (int i = 0; i < Events.Count; i++)
            {
                foreach ((int cf, string k, string? v) in Events[i].Changes)
                {
                    string id = cf + "/" + k;
                    if (v is null) cur.Remove(id);
                    else cur[id] = v;
                }
                if (Equal(cur, state)) r.Add(i + 1);
            }
            return r;
        }

        /// <summary>The prefix closest to <paramref name="state"/> and the keys that differ from it (diagnostics).</summary>
        public string Explain(SortedDictionary<string, string> state)
        {
            var cur = new SortedDictionary<string, string>(StringComparer.Ordinal);
            int best = 0, bestDiff = int.MaxValue;
            string bestText = "";
            for (int i = 0; i <= Events.Count; i++)
            {
                if (i > 0)
                {
                    foreach ((int cf, string k, string? v) in Events[i - 1].Changes)
                    {
                        string id = cf + "/" + k;
                        if (v is null) cur.Remove(id);
                        else cur[id] = v;
                    }
                }
                var diff = new List<string>();
                foreach (KeyValuePair<string, string> kv in cur)
                {
                    if (!state.TryGetValue(kv.Key, out string? v)) diff.Add("-" + kv.Key);
                    else if (v != kv.Value) diff.Add("~" + kv.Key);
                }
                foreach (string k in state.Keys)
                {
                    if (!cur.ContainsKey(k)) diff.Add("+" + k);
                }
                if (diff.Count < bestDiff)
                {
                    best = i;
                    bestDiff = diff.Count;
                    bestText = string.Join(",", diff);
                }
            }
            return "closest prefix " + best + " (last synced " + LastSynced + "), differs in " + bestText;
        }

        private static bool Equal(SortedDictionary<string, string> a, SortedDictionary<string, string> b)
        {
            if (a.Count != b.Count) return false;
            foreach (KeyValuePair<string, string> kv in a)
            {
                if (!b.TryGetValue(kv.Key, out string? v) || v != kv.Value) return false;
            }
            return true;
        }
    }

    // ---- DB handle over the write modes ------------------------------------------------------------------------------

    private sealed class Handle : IDisposable
    {
        public RocksDatabase Db = null!;
        public TransactionDatabase? Txn;
        public ColumnFamily Cf(int i) => i == 0 ? Db.DefaultColumnFamily : Db.GetColumnFamily("cf1");

        public void Dispose()
        {
            if (Txn is not null) Txn.Dispose();
            else Db.Dispose();
        }
    }

    private const string WalDirPath = "/wal";

    private static DbOptions Options(IFileSystem fs, Factor f)
    {
        var o = new DbOptions
        {
            CreateIfMissing = true,
            CreateMissingColumnFamilies = true,
            FileSystem = fs,
            WalRecoveryMode = f.Mode,
            ParanoidChecks = f.Paranoid,
            RecycleLogFileNum = f.Recycle ? 1UL : 0,
            TrackAndVerifyWals = f.TrackWals,
            TrackAndVerifyWalsInManifest = f.TrackInManifest,
            ManualWalFlush = f.ManualWalFlush,
            WalCompression = f.Zstd ? CompressionType.Zstd : CompressionType.None,
            WalDir = f.WalDir ? WalDirPath : "",
            WalTtlSeconds = f.Archival ? 3600UL : 0,
            MaxTotalWalSize = f.SmallMaxTotalWalSize ? 16 * 1024UL : 0,
            AvoidFlushDuringRecovery = f.AvoidFlushDuringRecovery,
            AvoidFlushDuringShutdown = f.AvoidFlushDuringShutdown,
            TwoWriteQueues = f.Write == WriteMode.TwoWriteQueues,
            EnablePipelinedWrite = f.Write == WriteMode.Pipelined,
            UnorderedWrite = f.Write == WriteMode.Unordered,
            Allow2Pc = f.Write == WriteMode.WritePrepared2Pc,
        };
        // Blob direct write needs non-concurrent memtable writes (except with unordered_write, which is rejected).
        if (f.BlobDirectWrite && f.Write != WriteMode.Unordered) o.AllowConcurrentMemtableWrite = false;
        return o;
    }

    private static ColumnFamilyDescriptor[] Cfs(Factor f)
    {
        var cf0 = new ColumnFamilyOptions { DisableAutoCompactions = true };
        if (f.BlobDirectWrite)
        {
            cf0.EnableBlobFiles = true;
            cf0.EnableBlobDirectWrite = true;
            cf0.MinBlobSize = 64;
        }
        var cf1 = new ColumnFamilyOptions { DisableAutoCompactions = true };
        if (f.Udt && f.Write != WriteMode.WritePrepared2Pc) cf1.Comparator = Comparator.BytewiseWithU64Ts;
        return [new("default", cf0), new("cf1", cf1)];
    }

    private static bool UdtOn(Factor f) => f.Udt && f.Write != WriteMode.WritePrepared2Pc;

    private static Handle Open(IFileSystem fs, Factor f)
    {
        var h = new Handle();
        if (f.Write == WriteMode.WritePrepared2Pc)
        {
            h.Txn = TransactionDatabase.Open(DbPath, Options(fs, f), new TransactionDbOptions { WritePolicy = TransactionWritePolicy.WritePrepared }, Cfs(f));
            h.Db = h.Txn.Database;
        }
        else
        {
            h.Db = RocksDatabase.Open(DbPath, Options(fs, f), Cfs(f));
        }
        return h;
    }

    private static byte[] Ts(ulong t)
    {
        var b = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(b, t);
        return b;
    }

    private static readonly byte[] s_maxTs = Ts(ulong.MaxValue);

    // ---- workload --------------------------------------------------------------------------------------------------

    /// <summary>What the workload left behind for the fault: the open DB (null after a clean close) and the model.</summary>
    private sealed class Run
    {
        public Handle? Db;
        public Model Model = new();
        public string Rejected = "";
    }

    /// <summary>
    /// The workload: puts and deletes over both column families (sync every 8th), cross-CF batches, a large value
    /// (fragmented record), flushes of the default CF and of cf1 at different points (different flush progress), and
    /// with 2PC a transaction prepared early and committed after both flushes (its prepared section spans WALs) plus one
    /// left prepared at the crash. <paramref name="beforeLastSync"/> runs right before the last sync write.
    /// Stops at the first write error (writes after a background error fail).
    /// </summary>
    private static Run Workload(FaultInjectionFileSystem fs, Factor f, Action? beforeLastSync, Action? beforeSecondFlush)
    {
        var run = new Run();
        Handle h;
        try
        {
            h = Open(fs, f);
        }
        catch (RocksDbException e) when (e.Code is StatusCode.InvalidArgument or StatusCode.NotSupported)
        {
            run.Rejected = e.Code + ": " + e.Message;
            return run;
        }
        run.Db = h;
        Model m = run.Model;
        bool udt = UdtOn(f);
        ulong ts = 0;
        Transaction? spanning = null, leftPrepared = null;
        var spanningChanges = new List<(int, string, string?)>();
        try
        {
            // A write that fails may still be durable: it becomes the model's last (unacknowledged) event.
            void Track(List<(int, string, string?)> changes, bool sync, Action write)
            {
                try
                {
                    write();
                }
                catch (RocksDbException)
                {
                    m.Add(new Event(changes, false));
                    throw;
                }
                m.Add(new Event(changes, sync));
            }
            void Put(int cf, string k, string v, bool sync)
            {
                WriteOptions wo = sync ? WriteOptions.SyncWrites : new WriteOptions();
                Track([(cf, k, v)], sync, () =>
                {
                    if (cf == 1 && udt) h.Cf(1).Put(wo, B(k), Ts(++ts), B(v));
                    else if (h.Txn is not null) h.Txn.Put(wo, h.Cf(cf), B(k), B(v));
                    else h.Cf(cf).Put(wo, B(k), B(v));
                });
            }
            void Del(int cf, string k, bool sync)
            {
                WriteOptions wo = sync ? WriteOptions.SyncWrites : new WriteOptions();
                Track([(cf, k, null)], sync, () =>
                {
                    if (cf == 1 && udt) h.Cf(1).Delete(wo, B(k), Ts(++ts));
                    else if (h.Txn is not null) h.Txn.Delete(wo, h.Cf(cf), B(k));
                    else h.Cf(cf).Delete(wo, B(k));
                });
            }
            void Batch(string k, string v)
            {
                using WriteBatch b = h.Db.CreateWriteBatch();
                b.Put(h.Cf(0), B(k), B(v));
                if (udt) b.Put(h.Cf(1), B(k), Ts(++ts), B(v));
                else b.Put(h.Cf(1), B(k), B(v));
                Track([(0, k, v), (1, k, v)], false, () =>
                {
                    if (h.Txn is not null) h.Txn.Write(new WriteOptions(), b);
                    else h.Db.Write(new WriteOptions(), b);
                });
            }
            string V(string tag, int i) => tag + i + new string((char)('a' + i % 26), 10 + i * 7 % 90);

            for (int i = 0; i < 24; i++)
            {
                if (i % 6 == 5) Del(i % 2, "a" + (i - 2), sync: false);
                else Put(i % 2, "a" + i, V("a", i), sync: i % 8 == 7);
                if (i == 4) Batch("ab", "x1");
                if (i == 6 && h.Txn is not null)
                {
                    spanning = h.Txn.BeginTransaction(new WriteOptions());
                    spanning.SetName("spanning");
                    spanning.Put(h.Cf(0), B("t1"), B("tv1"));
                    spanning.Put(h.Cf(1), B("t2"), B("tv2"));
                    spanning.Prepare();
                    spanningChanges.Add((0, "t1", "tv1"));
                    spanningChanges.Add((1, "t2", "tv2"));
                }
            }
            Put(0, "big", new string('B', 40_000), sync: false);
            h.Cf(0).Flush();
            WaitIdle(h.Db);
            for (int i = 0; i < 24; i++)
            {
                if (i % 7 == 3) Del(i % 2, "a" + i, sync: false);
                else Put(i % 2, "b" + i, V("b", i), sync: i % 8 == 7);
            }
            Batch("bb", "x2");
            beforeSecondFlush?.Invoke();
            h.Cf(1).Flush();
            WaitIdle(h.Db);
            if (spanning is not null)
            {
                spanning.Commit();
                m.Add(new Event(spanningChanges, false));
                spanning.Dispose();
                spanning = null;
                leftPrepared = h.Txn!.BeginTransaction(new WriteOptions());
                leftPrepared.SetName("left");
                leftPrepared.Put(h.Cf(0), B("p1"), B("pv1"));
                leftPrepared.Prepare();
            }
            for (int i = 0; i < 16; i++) Put(i % 2, "c" + i, V("c", i), sync: false);
            beforeLastSync?.Invoke();
            Put(0, "synced-last", "s", sync: true);
            for (int i = 0; i < 4; i++) Put(i % 2, "d" + i, V("d", i), sync: false);
            if (f.ManualWalFlush) h.Db.FlushWal(sync: false);
        }
        catch (RocksDbException)
        {
            // The DB stopped (an injected fault): the model has every acknowledged write.
        }
        // Transactions left open are abandoned with the crashed instance (never disposed: Dispose would roll back).
        GC.KeepAlive(leftPrepared);
        GC.KeepAlive(spanning);
        return run;
    }

    private static void WaitIdle(RocksDatabase db)
    {
        db.WaitForCompact();
        db.DefaultColumnFamily.Db.WaitForBackgroundWorkForTest();
    }

    // ---- faults ----------------------------------------------------------------------------------------------------

    private static string WalDirOf(Factor f) => f.WalDir ? WalDirPath : DbPath;

    private static List<ulong> Wals(MemoryFileSystem fs, Factor f)
    {
        var r = new List<ulong>();
        foreach (string c in fs.GetChildren(WalDirOf(f)))
        {
            if (FileNames.TryParseFileName(c, out ulong n, out FileType t) && t == FileType.WalFile) r.Add(n);
        }
        r.Sort();
        return r;
    }

    private static string WalPath(Factor f, ulong n) => FileNames.LogFileName(WalDirOf(f), n);

    private static void WriteFile(MemoryFileSystem fs, string path, ReadOnlySpan<byte> data)
    {
        using IWritableFile w = fs.CreateWritable(path, truncate: true);
        w.Append(data);
        w.Sync();
    }

    /// <summary>The physical records of a WAL (crc checked; stops at a record of another log number).</summary>
    private static List<(int Offset, int Header, int Length, byte Type)> Records(byte[] wal, ulong number)
    {
        var r = new List<(int, int, int, byte)>();
        int pos = 0;
        while (pos + LogFormat.HeaderSize <= wal.Length)
        {
            int blockLeft = LogFormat.BlockSize - pos % LogFormat.BlockSize;
            if (blockLeft < LogFormat.HeaderSize) { pos += blockLeft; continue; }
            int len = wal[pos + 4] | wal[pos + 5] << 8;
            byte type = wal[pos + 6];
            if (type == 0 && len == 0) break;
            bool recyclable = LogFormat.IsRecyclableType(type);
            int header = recyclable ? LogFormat.RecyclableHeaderSize : LogFormat.HeaderSize;
            if (pos + header + len > wal.Length) break;
            if (recyclable && BitConverter.ToUInt32(wal, pos + 7) != (uint)number) break;
            if (RocksDb.Managed.Util.Crc32C.Value(wal.AsSpan(pos + 6, header - 6 + len)) != RocksDb.Managed.Util.Crc32C.Unmask(BitConverter.ToUInt32(wal, pos))) break;
            bool data = type is (byte)LogRecordType.Full or (byte)LogRecordType.First or (byte)LogRecordType.Middle or (byte)LogRecordType.Last
                or (byte)LogRecordType.RecyclableFull or (byte)LogRecordType.RecyclableFirst or (byte)LogRecordType.RecyclableMiddle or (byte)LogRecordType.RecyclableLast;
            if (data) r.Add((pos, header, len, type));
            pos += header + len;
        }
        return r;
    }

    /// <summary>Damages the (crashed, process-kill) image. Returns false when the fault does not apply to it.</summary>
    private static bool Damage(MemoryFileSystem fs, Factor f, UnitFault fault)
    {
        List<ulong> wals = Wals(fs, f);
        if (wals.Count == 0) return false;
        ulong latest = wals[^1];
        string lp = WalPath(f, latest);
        byte[] lb = fs.ReadAllBytes(lp);
        var lrecs = Records(lb, latest);
        switch (fault)
        {
            case UnitFault.TornLastRecord:
                if (lrecs.Count == 0 || lrecs[^1].Length < 2) return false;
                WriteFile(fs, lp, lb.AsSpan(0, lrecs[^1].Offset + lrecs[^1].Header + lrecs[^1].Length / 2));
                return true;
            case UnitFault.CorruptMiddleChecksum:
                if (lrecs.Count < 3 || lrecs[lrecs.Count / 2].Length == 0) return false;
                lb[lrecs[lrecs.Count / 2].Offset + lrecs[lrecs.Count / 2].Header + lrecs[lrecs.Count / 2].Length / 2] ^= 0x5A;
                WriteFile(fs, lp, lb);
                return true;
            case UnitFault.CorruptOldestWalChecksum:
            {
                if (wals.Count < 2) return false;
                string p = WalPath(f, wals[0]);
                byte[] b = fs.ReadAllBytes(p);
                var recs = Records(b, wals[0]);
                if (recs.Count < 3 || recs[recs.Count / 2].Length == 0) return false;
                b[recs[recs.Count / 2].Offset + recs[recs.Count / 2].Header + recs[recs.Count / 2].Length / 2] ^= 0x5A;
                WriteFile(fs, p, b);
                return true;
            }
            case UnitFault.ZeroedTail:
                if (lrecs.Count == 0) return false;
                Array.Clear(lb, lrecs[^1].Offset, lb.Length - lrecs[^1].Offset);
                WriteFile(fs, lp, lb);
                return true;
            case UnitFault.MissingOldestWal:
                if (wals.Count < 2) return false;
                fs.DeleteFile(WalPath(f, wals[0]));
                return true;
            case UnitFault.MissingLatestWal:
                if (wals.Count < 2) return false;
                fs.DeleteFile(lp);
                return true;
            case UnitFault.StaleWalAbove:
                WriteFile(fs, WalPath(f, latest + 100), fs.ReadAllBytes(WalPath(f, wals[0])));
                return true;
            case UnitFault.EmptyNewWal:
                WriteFile(fs, WalPath(f, latest + 100), []);
                return true;
            case UnitFault.SwappedLatestWals:
            {
                if (wals.Count < 2) return false;
                byte[] prev = fs.ReadAllBytes(WalPath(f, wals[^2]));
                WriteFile(fs, WalPath(f, wals[^2]), lb);
                WriteFile(fs, lp, prev);
                return true;
            }
            case UnitFault.DroppedPredecessorRecord:
            {
                if (wals.Count < 2) return false;
                string p = WalPath(f, wals[^2]);
                byte[] b = fs.ReadAllBytes(p);
                var recs = Records(b, wals[^2]);
                if (recs.Count < 2) return false;
                WriteFile(fs, p, b.AsSpan(0, recs[^1].Offset));
                return true;
            }
            default:
                return true;
        }
    }

    // ---- reading -------------------------------------------------------------------------------------------------

    private static SortedDictionary<string, string> State(Handle h, Factor f)
    {
        var state = new SortedDictionary<string, string>(StringComparer.Ordinal);
        for (int cf = 0; cf < 2; cf++)
        {
            ReadOptions? ro = cf == 1 && UdtOn(f) ? new ReadOptions { Timestamp = s_maxTs } : null;
            using Iterator it = h.Cf(cf).NewIterator(ro);
            for (it.SeekToFirst(); it.Valid; it.Next())
            {
                state[cf + "/" + Encoding.UTF8.GetString(it.Key)] = Encoding.UTF8.GetString(it.Value);
            }
        }
        return state;
    }

    // ---- the matrix ------------------------------------------------------------------------------------------------

    private sealed class Outcome
    {
        public int Scenarios, NotApplicable, Rejected;
        public readonly SortedDictionary<string, int> Counts = new(StringComparer.Ordinal);
        public readonly List<string> Failures = new();
    }

    [TestMethod]
    public void WalSettingsAndFailureModes()
    {
        var sw = Stopwatch.StartNew();
        var outcome = new Outcome();
        List<Factor> rows = Rows();
        // WAL_MATRIX_ONLY=<text>: only the rows whose description contains the text (to replay one failure).
        if (Environment.GetEnvironmentVariable("WAL_MATRIX_ONLY") is { Length: > 0 } only) rows.RemoveAll(r => r.ToString() != only);
        foreach (Factor f in rows)
        {
            foreach (UnitFault fault in Enum.GetValues<UnitFault>())
            {
                try
                {
                    RunScenario(f, fault, outcome);
                }
                catch (Exception e) when (e is not AssertFailedException)
                {
                    outcome.Failures.Add(f + " / " + fault + ": unexpected " + e.GetType().Name + ": " + e.Message);
                }
                finally
                {
                    TestHooks.ClearAll();
                }
            }
        }
        var sb = new StringBuilder();
        sb.Append(rows.Count).Append(" rows x ").Append(Enum.GetValues<UnitFault>().Length).Append(" faults: ").Append(outcome.Scenarios)
            .Append(" scenarios, ").Append(outcome.NotApplicable).Append(" not applicable, ").Append(outcome.Rejected).Append(" rejected configurations, ")
            .Append(sw.Elapsed.TotalSeconds.ToString("F1")).Append(" s\n");
        foreach (KeyValuePair<string, int> kv in outcome.Counts) sb.Append("  ").Append(kv.Key).Append(": ").Append(kv.Value).Append('\n');
        TestContext.WriteLine(sb.ToString());
        if (Environment.GetEnvironmentVariable("WAL_MATRIX_REPORT") is { Length: > 0 } report)
        {
            File.WriteAllText(report, sb + "\n" + string.Join("\n", outcome.Failures) + "\n");
        }
        if (outcome.Failures.Count > 0)
        {
            Assert.Fail(outcome.Failures.Count + " failures (first 30):\n" + string.Join("\n", outcome.Failures.GetRange(0, Math.Min(30, outcome.Failures.Count))));
        }
    }

    private static void RunScenario(Factor f, UnitFault fault, Outcome outcome)
    {
        MemoryFileSystem mem = NewFs();
        if (f.WalDir) mem.CreateDirectoryIfMissing(WalDirPath);
        mem.SyncDirectory("/");
        var fs = new FaultInjectionFileSystem(mem);
        Action? beforeLastSync = fault == UnitFault.WalSyncFailsThenPowerLoss
            ? () => fs.FailMatching(".log", FaultOps.Sync, 1, InjectedFaultKind.IOError)
            : null;
        Action? beforeSecondFlush = fault == UnitFault.ManifestWriteFailsThenPowerLoss
            ? () => fs.FailMatching("MANIFEST", FaultOps.Append | FaultOps.Sync, 1, InjectedFaultKind.IOError)
            : null;
        Run run = Workload(fs, f, beforeLastSync, beforeSecondFlush);
        string scenario = f + " / " + fault;
        if (run.Db is null)
        {
            outcome.Rejected++;
            outcome.Counts["rejected " + run.Rejected[..Math.Min(80, run.Rejected.Length)]] =
                outcome.Counts.GetValueOrDefault("rejected " + run.Rejected[..Math.Min(80, run.Rejected.Length)]) + 1;
            return;
        }
        fs.ClearFailMatching();
        // Crash or close.
        if (fault == UnitFault.CleanClose)
        {
            run.Db.Dispose();
        }
        else
        {
            mem.SetFilesystemActive(false);
            if (fault is UnitFault.PowerLoss or UnitFault.WalSyncFailsThenPowerLoss or UnitFault.ManifestWriteFailsThenPowerLoss) mem.SimulateCrash();
            try { run.Db.Dispose(); }
            catch (Exception e) when (e is RocksDbException or IOException or ObjectDisposedException or InvalidOperationException) { }
            mem.SetFilesystemActive(true);
        }
        if (!Damage(mem, f, fault))
        {
            outcome.NotApplicable++;
            return;
        }
        outcome.Scenarios++;
        Model m = run.Model;
        Handle? h = null;
        SortedDictionary<string, string> state;
        try
        {
            h = Open(fs, f);
        }
        catch (RocksDbException e)
        {
            outcome.Counts[fault + " " + f.Mode + " " + e.Code] = outcome.Counts.GetValueOrDefault(fault + " " + f.Mode + " " + e.Code) + 1;
            if (IsCrashFault(fault)) outcome.Failures.Add(scenario + ": a crash must reopen, got " + e.Code + ": " + e.Message);
            else if (e.Code != StatusCode.Corruption) outcome.Failures.Add(scenario + ": damage must fail with Corruption, got " + e.Code + ": " + e.Message);
            else if (ExpectedToOpen(f, fault)) outcome.Failures.Add(scenario + ": expected to open (upstream), got " + e.Message);
            return;
        }
        using (h)
        {
            outcome.Counts[fault + " " + f.Mode + " OK"] = outcome.Counts.GetValueOrDefault(fault + " " + f.Mode + " OK") + 1;
            try
            {
                state = State(h, f);
            }
            catch (RocksDbException e) when (f.BlobDirectWrite && IsPowerLoss(fault) && e.Message.Contains("blob", StringComparison.OrdinalIgnoreCase))
            {
                // Upstream blob direct write syncs blob files only for sync writes (SyncBlobDirectWriteManagers); a WAL
                // synced otherwise (the closed-WAL sync before a flush, SyncWAL) can outlive the blob bytes its records
                // point to across a power loss. Same in native; not a WAL recovery outcome.
                outcome.Counts["blob direct write: blob bytes lost by the power loss"] = outcome.Counts.GetValueOrDefault("blob direct write: blob bytes lost by the power loss") + 1;
                return;
            }
            if (!PrefixExpected(f, fault))
            {
                foreach (KeyValuePair<string, string> kv in state)
                {
                    if (!m.History.TryGetValue(kv.Key, out HashSet<string>? values) || !values.Contains(kv.Value))
                    {
                        outcome.Failures.Add(scenario + ": recovered " + kv.Key + "=" + kv.Value[..Math.Min(20, kv.Value.Length)] + " was never written");
                        break;
                    }
                }
            }
            else
            {
                List<int> prefixes = m.MatchingPrefixes(state);
                if (prefixes.Count == 0)
                {
                    outcome.Failures.Add(scenario + ": the recovered state is no prefix of the committed writes (" + state.Count + " keys, " + m.Events.Count + " events): " + m.Explain(state));
                }
                else if (IsCrashFault(fault) && prefixes[^1] <= m.LastSynced)
                {
                    outcome.Failures.Add(scenario + ": lost acknowledged synced writes: recovered prefix " + prefixes[^1] + ", last synced event " + m.LastSynced);
                }
            }
            // The recovered DB works and reopens with the same contents.
            if (h.Txn is not null)
            {
                foreach (Transaction t in h.Txn.GetAllPreparedTransactions()) t.Rollback();
                state = State(h, f);
                h.Txn.Put(WriteOptions.SyncWrites, h.Cf(0), B("after"), B("1"));
            }
            else
            {
                h.Cf(0).Put(WriteOptions.SyncWrites, B("after"), B("1"));
            }
            state["0/after"] = "1";
        }
        using Handle again = Open(fs, f);
        SortedDictionary<string, string> reread = State(again, f);
        if (!SameState(state, reread)) outcome.Failures.Add(scenario + ": contents changed across a clean reopen");
    }

    /// <summary>
    /// Whether upstream recovers a point-in-time prefix: after a crash in every mode; after damage confined to the
    /// latest WAL (torn, corrupted or zeroed tail, the latest WAL missing) when reader errors are recorded (paranoid_checks)
    /// and not skipped. Damage to an older WAL, kSkipAnyCorruptedRecords and paranoid_checks = false can leave holes
    /// (the reader drops the damaged block and goes on, a missing WAL in the middle is not noticed without
    /// track_and_verify_wals), so only the weaker "every value was written" holds there.
    /// </summary>
    private static bool PrefixExpected(Factor f, UnitFault fault)
    {
        if (IsCrashFault(fault)) return true;
        if (!f.Paranoid || f.Mode == WalRecoveryMode.SkipAnyCorruptedRecords) return false;
        return fault is UnitFault.TornLastRecord or UnitFault.CorruptMiddleChecksum or UnitFault.ZeroedTail or UnitFault.MissingLatestWal;
    }

    private static bool SameState(SortedDictionary<string, string> a, SortedDictionary<string, string> b)
    {
        if (a.Count != b.Count) return false;
        foreach (KeyValuePair<string, string> kv in a)
        {
            if (!b.TryGetValue(kv.Key, out string? v) || v != kv.Value) return false;
        }
        return true;
    }

    /// <summary>
    /// Damage that upstream recovers from (log_reader.cc / ProcessLogFiles): kSkipAnyCorruptedRecords ignores reader
    /// errors and kPointInTimeRecovery stops at them, so a torn or corrupted record or a zeroed tail in the latest WAL
    /// (no column family can be ahead of it) reopens; without paranoid_checks reader errors are not recorded at all;
    /// kTolerateCorruptedTailRecords drops a torn last record; a zeroed tail reads as skipped zero-length records.
    /// </summary>
    private static bool ExpectedToOpen(Factor f, UnitFault fault)
    {
        // A zeroed region reads as zero-length kZeroType records, which the reader skips silently in every mode.
        if (fault == UnitFault.ZeroedTail) return true;
        bool latestWalTail = fault is UnitFault.TornLastRecord;
        bool latestWal = latestWalTail || fault == UnitFault.CorruptMiddleChecksum;
        if (!latestWal) return false;
        if (f.Mode is WalRecoveryMode.SkipAnyCorruptedRecords or WalRecoveryMode.PointInTimeRecovery) return true;
        if (!f.Paranoid) return true;
        return f.Mode == WalRecoveryMode.TolerateCorruptedTailRecords && latestWalTail;
    }

    // ---- regressions found by the matrices (managed vs native disagreements) -------------------------------------

    private sealed class CollectingLogger() : InfoLogger(InfoLogLevel.Info)
    {
        private readonly List<string> _lines = new();

        protected override void LogCore(InfoLogLevel level, string message)
        {
            lock (_lines) _lines.Add(message);
        }

        public bool Any(string s)
        {
            lock (_lines) return _lines.Exists(l => l.Contains(s, StringComparison.Ordinal));
        }
    }

    private static readonly WalRecoveryMode[] s_modes =
    [
        WalRecoveryMode.TolerateCorruptedTailRecords, WalRecoveryMode.AbsoluteConsistency,
        WalRecoveryMode.PointInTimeRecovery, WalRecoveryMode.SkipAnyCorruptedRecords,
    ];

    private static List<ulong> DbWals(MemoryFileSystem fs)
    {
        var r = new List<ulong>();
        foreach (string c in fs.GetChildren(DbPath))
        {
            if (FileNames.TryParseFileName(c, out ulong n, out FileType t) && t == FileType.WalFile) r.Add(n);
        }
        r.Sort();
        return r;
    }

    /// <summary>
    /// DBImpl::CheckSeqnoNotSetBackDuringRecovery: a copy of an older WAL under a newer number replays sequence numbers
    /// that were already recovered; the re-inserted key+seq fails the batch (two TryAgain) without advancing the sequence,
    /// and the open fails in every recovery mode. Before, the duplicate inserts were skipped and the open succeeded.
    /// </summary>
    [TestMethod]
    public void StaleWalCopyFailsWithSequenceSetBack()
    {
        foreach (WalRecoveryMode mode in s_modes)
        {
            MemoryFileSystem fs = NewFs();
            DbOptions o = NewOptions(fs);
            o.WalRecoveryMode = mode;
            o.AvoidFlushDuringShutdown = true;
            using (RocksDatabase db = RocksDatabase.Open(DbPath, o))
            {
                for (int i = 0; i < 5; i++) db.Put(Key(i), Value(i, 0));
            }
            ulong wal = DbWals(fs)[^1];
            WriteFile(fs, FileNames.LogFileName(DbPath, wal + 100), fs.ReadAllBytes(FileNames.LogFileName(DbPath, wal)));
            var e = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, o).Dispose(), mode.ToString());
            Assert.AreEqual(StatusCode.Corruption, e.Code);
            Assert.AreEqual("Corruption: Sequence number is being set backwards during recovery, this is likely a software bug or a data corruption. " +
                "Prev next seqno: 6 , current next seqno: 1", e.Message, mode.ToString());
        }
    }

    /// <summary>
    /// DBOpenLogRecordReadReporter::Corruption: the WAL's status is the reported status as is ("Corruption: checksum
    /// mismatch"); the file name and the dropped bytes go to the info LOG. Before, the file name was part of the status.
    /// </summary>
    [TestMethod]
    public void WalCorruptionStatusIsTheReaderStatus()
    {
        MemoryFileSystem fs = NewFs();
        var log = new CollectingLogger();
        DbOptions o = NewOptions(fs);
        o.WalRecoveryMode = WalRecoveryMode.AbsoluteConsistency;
        o.AvoidFlushDuringShutdown = true;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o))
        {
            for (int i = 0; i < 20; i++) db.Put(Key(i), Value(i, 0));
        }
        ulong wal = DbWals(fs)[^1];
        string path = FileNames.LogFileName(DbPath, wal);
        byte[] b = fs.ReadAllBytes(path);
        var recs = Records(b, wal);
        b[recs[10].Offset + recs[10].Header + 2] ^= 1;
        WriteFile(fs, path, b);
        o.InfoLog = log;
        var e = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, o).Dispose());
        Assert.AreEqual(StatusCode.Corruption, e.Code);
        Assert.AreEqual("Corruption: checksum mismatch", e.Message);
        Assert.IsTrue(log.Any(path + ": dropping "), "the LOG names the file and the dropped bytes");
    }

    /// <summary>
    /// DBImpl::Open after a point-in-time recovery that found a corrupted WAL: an empty batch with the recovered next
    /// sequence is the first record of the new WAL (synced), so that a later recovery resumes after the corrupted WAL.
    /// </summary>
    [TestMethod]
    public void PointInTimeStopWritesTheRecoveredSequenceRecord()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.WalRecoveryMode = WalRecoveryMode.PointInTimeRecovery;
        o.AvoidFlushDuringShutdown = true;
        o.AvoidFlushDuringRecovery = true;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o))
        {
            for (int i = 0; i < 20; i++) db.Put(Key(i), Value(i, 0));
        }
        ulong wal = DbWals(fs)[^1];
        string path = FileNames.LogFileName(DbPath, wal);
        byte[] b = fs.ReadAllBytes(path);
        var recs = Records(b, wal);
        b[recs[10].Offset + recs[10].Header + 2] ^= 1;
        WriteFile(fs, path, b);
        ulong recovered;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o))
        {
            recovered = db.GetLatestSequenceNumber();
            Assert.AreEqual(10UL, recovered, "records 1..10 recovered");
            ulong newWal = db.GetCurrentWalFile().LogNumber;
            byte[] nb = fs.ReadAllBytes(FileNames.LogFileName(DbPath, newWal));
            var first = Records(nb, newWal)[0];
            Assert.AreEqual(WriteBatch.HeaderSize, first.Length);
            Assert.AreEqual(recovered + 1, RocksDb.Managed.Util.Coding.DecodeFixed64(nb.AsSpan(first.Offset + first.Header)));
            Assert.AreEqual(0u, RocksDb.Managed.Util.Coding.DecodeFixed32(nb.AsSpan(first.Offset + first.Header + 8)));
            db.Put(B("after"), B("1"));
        }
        // The corrupted WAL is still there (avoid_flush_during_recovery keeps it): the next recovery stops in it again
        // and resumes at the dummy record.
        Assert.IsTrue(fs.FileExists(path));
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o))
        {
            Assert.AreEqual("1", GetString(db, "after"));
            Assert.IsNull(db.Get(Key(15)));
        }
    }

    /// <summary>
    /// Blob direct write after a crash: blob indexes recovered from the WAL point to blob files no flush registered.
    /// Upstream reads them through the write-path fallback (ResolveBlobDirectWriteIndex opens the file by number,
    /// footer optional) and keeps footer-less blob files in obsolete-file purges (ShouldKeepFooterlessBlobFile). Before,
    /// the open's full scan deleted the file and reads failed with "Invalid blob file number" (native reads them).
    /// </summary>
    [TestMethod]
    public void BlobDirectWriteCrashRecoveryReadsUnregisteredBlobFiles()
    {
        foreach (bool avoidFlushDuringRecovery in new[] { false, true })
        {
            MemoryFileSystem fs = NewFs();
            var f = new Factor(WalRecoveryMode.PointInTimeRecovery, true, false, false, false, false, false, false, false, false,
                avoidFlushDuringRecovery, false, WriteMode.Default, false, true);
            Handle h = Open(fs, f);
            for (int i = 0; i < 30; i++)
            {
                h.Cf(0).Put(B("k" + i), B(new string((char)('a' + i % 26), 100 + i)));
                if (i == 10) h.Cf(0).Flush();
            }
            // Process kill: the file system keeps everything, the instance is abandoned.
            fs.SetFilesystemActive(false);
            try { h.Dispose(); }
            catch (RocksDbException) { }
            fs.SetFilesystemActive(true);
            for (int round = 0; round < 2; round++)
            {
                using Handle r = Open(fs, f);
                for (int i = 0; i < 30; i++)
                {
                    Assert.AreEqual(new string((char)('a' + i % 26), 100 + i), S(r.Cf(0).Get(B("k" + i))), "avoidFlush=" + avoidFlushDuringRecovery + " round " + round + " k" + i);
                }
            }
        }
    }
}
