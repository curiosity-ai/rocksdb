using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Tests.Db;

namespace RocksDb.Managed.CompatTests.Wal;

/// <summary>
/// WAL write error recovery with WAL recycling on, against native RocksDB 11.12. Native refuses this recovery
/// (DBImpl::ResumeImpl: NotSupported with recycle_log_file_num != 0) and stays write-stopped; the managed port runs it
/// and never recycles the failed WAL. The managed DB lives on a fault-injection file system; the poisoned WAL write and
/// its recovery run with the file system dying at each of its mutating operations in turn (power loss: unsynced data
/// dropped; process kill: everything kept). Every crash image is copied to disk and opened by both engines with the
/// same recycle_log_file_num and recovery mode: they recover the same contents, and those hold every acknowledged
/// write and the failed write only if the recovery had not committed.
/// </summary>
[TestClass]
[DoNotParallelize]
public sealed class WalErrorRecyclingCompatTests
{
    private const string DbPath = "/db";
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);
    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);

    /// <summary>How the WAL write fails (see WalErrorRecyclingTests.Poison).</summary>
    public enum Poison
    {
        AppendNothingWritten,
        AppendTorn,
        AppendFullyWritten,
        SyncError,
    }

    private sealed class Listener : EventListener
    {
        public readonly ManualResetEventSlim Ended = new(false);
        public volatile BackgroundErrorRecoveryInfo? Info;

        public override void OnErrorRecoveryEnd(BackgroundErrorRecoveryInfo info)
        {
            Info = info;
            Ended.Set();
        }
    }

    private readonly record struct Image(List<string> Acked, bool Crashed, bool Recovered, string? CrashedAt);

    /// <summary>
    /// Writes the managed DB (warm-up flushes that fill the recycle list, sync writes, the poisoned write) and lets the
    /// recovery run with a crash at the <paramref name="k"/>-th mutating operation (0 = no crash: the recovery, three
    /// post-recovery writes and a close complete). Leaves the crash image in <paramref name="fs"/>.
    /// </summary>
    private static Image WriteImage(FaultInjectionFileSystem fs, Poison poison, ulong recycle, WalRecoveryMode mode, long k, bool powerLoss)
    {
        var acked = new List<string>();
        var listener = new Listener();
        var o = new DbOptions
        {
            CreateIfMissing = true,
            FileSystem = fs,
            RecycleLogFileNum = recycle,
            WalRecoveryMode = mode,
            BgErrorResumeRetryInterval = 10_000,
        };
        o.Listeners.Add(listener);
        using var gate = new ManualResetEventSlim(false);
        TestHooks.Set("RecoverFromRetryableBGIOError:BeforeResume", () => gate.Wait(Timeout));
        bool recovered = false;
        RocksDatabase? db = null;
        try
        {
            db = RocksDatabase.Open(DbPath, o);
            void Put(string key, bool sync)
            {
                db.Put(new WriteOptions { Sync = sync }, U(key), U("v-" + key));
                acked.Add(key);
            }
            for (int r = 0; r < (int)recycle + 2; r++)
            {
                for (int i = 0; i < 20; i++) Put("warm-" + r + "-" + i, sync: i % 5 == 0);
                db.Flush();
                db.WaitForCompact();
                db.DefaultColumnFamily.Db.WaitForBackgroundWorkForTest();
            }
            for (int i = 0; i < 5; i++) Put("pre-" + i, sync: true);
            bool sync = false;
            byte[] lost = U("lost-key-0001");
            switch (poison)
            {
                case Poison.AppendNothingWritten:
                    fs.FailAppend(".log", null, AppendFailMode.NothingWritten, InjectedFaultKind.FileScope);
                    break;
                case Poison.AppendTorn:
                    fs.FailAppend(".log", lost, AppendFailMode.Torn, InjectedFaultKind.FileScope);
                    break;
                case Poison.AppendFullyWritten:
                    fs.FailAppend(".log", lost, AppendFailMode.FullyWritten, InjectedFaultKind.FileScope);
                    break;
                case Poison.SyncError:
                    fs.FailMatching(".log", FaultOps.Sync, 1, InjectedFaultKind.FileScope);
                    sync = true;
                    break;
            }
            RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.Put(new WriteOptions { Sync = sync }, lost, U("lost-value-0001")));
            Assert.AreEqual(StatusCode.IOError, ex.Code);
            if (k > 0) fs.CrashAtOperation = fs.Operations + k;
            gate.Set();
            SpinWait.SpinUntil(() => listener.Ended.IsSet || fs.Crashed, Timeout);
            recovered = listener.Ended.IsSet && listener.Info!.NewBackgroundError is null;
            if (k == 0) Assert.IsTrue(recovered, "recovery failed: " + listener.Info?.NewBackgroundError);
            if (recovered)
            {
                for (int i = 0; i < 3; i++)
                {
                    try
                    {
                        Put("post-" + i, sync: true);
                    }
                    catch (RocksDbException)
                    {
                        break;
                    }
                }
            }
        }
        finally
        {
            gate.Set();
            TestHooks.ClearAll();
            try { db?.Dispose(); }
            catch (Exception e) when (e is RocksDbException or IOException or AggregateException) { }
        }
        bool crashed = fs.Crashed;
        string? at = fs.CrashedAt;
        if (crashed)
        {
            if (powerLoss) fs.PowerLoss();
            fs.Restart();
        }
        return new Image(acked, crashed, recovered, at);
    }

    /// <summary>Copies the DB directory of the in-memory file system to <paramref name="target"/> (no LOCK, no LOG).</summary>
    private static void Export(MemoryFileSystem fs, string target)
    {
        Directory.CreateDirectory(target);
        foreach (string child in fs.GetChildren(DbPath))
        {
            if (child == "LOCK" || child.StartsWith("LOG", StringComparison.Ordinal)) continue;
            string path = DbPath + "/" + child;
            if (!fs.FileExists(path)) continue;
            File.WriteAllBytes(Path.Combine(target, child), fs.ReadAllBytes(path));
        }
    }

    private static string ManagedContents(string dir, ulong recycle, WalRecoveryMode mode)
    {
        try
        {
            using RocksDatabase db = RocksDatabase.Open(dir, new DbOptions { RecycleLogFileNum = recycle, WalRecoveryMode = mode });
            var sb = new StringBuilder();
            using RocksDb.Managed.Iterator it = db.NewIterator();
            for (it.SeekToFirst(); it.Valid; it.Next()) sb.Append(Encoding.UTF8.GetString(it.Key)).Append('=').Append(Encoding.UTF8.GetString(it.Value)).Append(' ');
            return sb.ToString();
        }
        catch (RocksDbException e)
        {
            return "error " + e.Code + ": " + e.Message;
        }
    }

    private static string NativeContents(string dir, ulong recycle, WalRecoveryMode mode)
    {
        var o = new RocksDbSharp.DbOptions();
        RocksDbSharp.Native n = RocksDbSharp.Native.Instance;
        n.rocksdb_options_set_recycle_log_file_num(o.Handle, (UIntPtr)recycle);
        n.rocksdb_options_set_wal_recovery_mode(o.Handle, (int)mode);
        try
        {
            using RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(o, dir);
            var sb = new StringBuilder();
            using RocksDbSharp.Iterator it = db.NewIterator();
            for (it.SeekToFirst(); it.Valid(); it.Next()) sb.Append(Encoding.UTF8.GetString(it.Key())).Append('=').Append(Encoding.UTF8.GetString(it.Value())).Append(' ');
            return sb.ToString();
        }
        catch (RocksDbSharp.RocksDbException e)
        {
            return "error " + WalMatrixUtil.NativeCode(e.Message) + ": " + e.Message;
        }
    }

    /// <summary>
    /// Opens copies of the image with both engines (twice each) and checks they agree and hold the acknowledged writes.
    /// Returns true if both recovered the failed write.
    /// </summary>
    private static bool AssertBothRecover(MemoryFileSystem mem, Image image, Poison poison, ulong recycle, WalRecoveryMode mode, string what)
    {
        using var src = new TempDir("walerr-recycle");
        Export(mem, src.Path);
        using var m = new TempDir("walerr-recycle-m");
        using var nd = new TempDir("walerr-recycle-n");
        WalMatrixUtil.CopyTree(src.Path, m.Path);
        WalMatrixUtil.CopyTree(src.Path, nd.Path);
        bool lostPresent = false;
        for (int round = 0; round < 2; round++)
        {
            string native = NativeContents(nd.Path, recycle, mode);
            string managed = ManagedContents(m.Path, recycle, mode);
            Assert.AreEqual(native, managed, what + " open " + round);
            Assert.IsFalse(managed.StartsWith("error", StringComparison.Ordinal), what + ": " + managed);
            foreach (string k in image.Acked) StringAssert.Contains(managed, k + "=v-" + k + " ", what);
            if (managed.Contains("lost-key-0001=", StringComparison.Ordinal))
            {
                // Only an ambiguous write whose complete record survived, and only before the recovery committed.
                Assert.IsTrue(poison is Poison.AppendFullyWritten or Poison.SyncError && !image.Recovered, what + ": the failed write was resurrected");
                lostPresent = true;
            }
        }
        return lostPresent;
    }

    [TestMethod]
    [DataRow(Poison.AppendNothingWritten)]
    [DataRow(Poison.AppendTorn)]
    [DataRow(Poison.AppendFullyWritten)]
    [DataRow(Poison.SyncError)]
    public void RecoveryCrashImagesRecoverLikeNative(Poison poison)
    {
        foreach ((ulong recycle, WalRecoveryMode mode) in new[] { (1UL, WalRecoveryMode.PointInTimeRecovery), (4UL, WalRecoveryMode.SkipAnyCorruptedRecords) })
        {
            // The completed recovery, then every crash point.
            int crashes = 0;
            int lostPresent = 0;
            for (long k = 0; k < 500; k++)
            {
                bool any = false;
                foreach (bool powerLoss in new[] { true, false })
                {
                    if (k == 0 && !powerLoss) continue;
                    var mem = new MemoryFileSystem();
                    mem.CreateDirectoryIfMissing(DbPath);
                    mem.SyncDirectory("/");
                    var fs = new FaultInjectionFileSystem(mem);
                    Image image = WriteImage(fs, poison, recycle, mode, k, powerLoss);
                    if (k > 0 && !image.Crashed) continue;
                    any = true;
                    if (image.Crashed) crashes++;
                    string what = poison + " recycle=" + recycle + " " + mode + " k=" + k + (powerLoss ? " power loss" : " process kill") + " at " + image.CrashedAt;
                    if (AssertBothRecover(mem, image, poison, recycle, mode, what)) lostPresent++;
                }
                if (k > 0 && !any) break;
            }
            Assert.IsTrue(crashes > 20, "only " + crashes + " crash points");
            // The ambiguous outcome is exercised: images where both engines replay the complete failed record (a crash
            // before the recovery's MANIFEST commit), and none for the variants whose record is incomplete or absent.
            if (poison is Poison.AppendFullyWritten or Poison.SyncError) Assert.IsTrue(lostPresent > 0, poison + ": no image holds the failed write");
            else Assert.AreEqual(0, lostPresent);
        }
    }
}
