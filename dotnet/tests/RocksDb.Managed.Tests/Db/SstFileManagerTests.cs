using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>
/// SstFileManager: file tracking, space limit, rate limited trash deletion and out-of-space recovery (ports of the
/// scenarios of db/db_sst_test.cc and the SFM cases of db/error_handler_fs_test.cc).
/// </summary>
[TestClass]
public sealed class SstFileManagerTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);

    private sealed class RecoveryListener : EventListener
    {
        public readonly ManualResetEventSlim Ended = new(false);
        public BackgroundErrorRecoveryInfo? Info;

        public override void OnErrorRecoveryEnd(BackgroundErrorRecoveryInfo info)
        {
            Info = info;
            Ended.Set();
        }
    }

    private static ulong LiveTableBytes(MemoryFileSystem fs)
    {
        ulong total = 0;
        foreach (string c in fs.GetChildren(DbPath))
        {
            if (FileNames.TryParseFileName(c, out _, out FileType t) && t == FileType.TableFile) total += (ulong)fs.GetFileSize(DbPath + "/" + c);
        }
        return total;
    }

    [TestMethod]
    public void TracksTableFilesAndDeletesThroughTrashAtTheRate()
    {
        MemoryFileSystem fs = NewFs();
        using var sfm = new SstFileManager(fs, 1 << 20, 1.0, 64UL << 20);
        DbOptions o = NewOptions(fs);
        o.SstFileManager = sfm;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o))
        {
            for (int round = 0; round < 3; round++)
            {
                for (int i = 0; i < 200; i++) db.Put(Key(i), Value(i, round));
                db.Flush();
            }
            Assert.AreEqual(LiveTableBytes(fs), sfm.TotalSize);
            db.CompactRange(null, null);
            sfm.WaitForEmptyTrash();
            Assert.AreEqual(0UL, sfm.TotalTrashSize);
            Assert.AreEqual(LiveTableBytes(fs), sfm.TotalSize);
            foreach (string c in fs.GetChildren(DbPath)) Assert.IsFalse(c.EndsWith(".trash", StringComparison.Ordinal), c);
            Assert.AreEqual(S(Value(7, 2)), GetString(db, S(Key(7))!));
        }
    }

    [TestMethod]
    public void TrashLeftByAnEarlierProcessIsDeletedAtOpen()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs))) db.Put(B("a"), B("b"));
        using (IWritableFile f = fs.CreateWritable(DbPath + "/000123.sst.trash", truncate: true)) f.Append(new byte[100]);
        using var sfm = new SstFileManager(fs, 0, 0.25, 64UL << 20);
        DbOptions o = NewOptions(fs);
        o.SstFileManager = sfm;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o))
        {
            sfm.WaitForEmptyTrash();
            Assert.IsFalse(fs.FileExists(DbPath + "/000123.sst.trash"));
        }
    }

    [TestMethod]
    public void MaxAllowedSpaceStopsWritesUntilRaised()
    {
        MemoryFileSystem fs = NewFs();
        using var sfm = new SstFileManager(fs, 0, 0.25, 64UL << 20) { ErrorRecoveryPollInterval = TimeSpan.FromMilliseconds(20) };
        var listener = new RecoveryListener();
        DbOptions o = NewOptions(fs);
        o.SstFileManager = sfm;
        o.Listeners.Add(listener);
        using RocksDatabase db = RocksDatabase.Open(DbPath, o);
        sfm.SetMaxAllowedSpaceUsage(1000);
        for (int i = 0; i < 200; i++) db.Put(Key(i), Value(i, 0));
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
        Assert.AreEqual(StatusSubCode.SpaceLimit, ex.SubCode, ex.Message);
        StringAssert.Contains(ex.Message, "Max allowed space was reached");
        ex = Assert.ThrowsExactly<RocksDbException>(() => db.Put(B("x"), B("y")));
        Assert.AreEqual(StatusSubCode.SpaceLimit, ex.SubCode);
        Assert.IsTrue(sfm.IsMaxAllowedSpaceReached);
        // Raising the limit lets the SstFileManager's recovery resume the DB.
        sfm.SetMaxAllowedSpaceUsage(0);
        Assert.IsTrue(listener.Ended.Wait(Timeout), "recovery did not end");
        Assert.IsNull(listener.Info!.NewBackgroundError);
        db.Put(B("x"), B("y"));
        Assert.AreEqual("y", GetString(db, "x"));
    }

    [TestMethod]
    public void NoSpaceErrorRecoversAutomaticallyOnceSpaceIsFree()
    {
        var fs = new FaultInjectionFileSystem(NewFs());
        using var sfm = new SstFileManager(fs, 0, 0.25, 64UL << 20) { ErrorRecoveryPollInterval = TimeSpan.FromMilliseconds(20) };
        var listener = new RecoveryListener();
        var o = new DbOptions { CreateIfMissing = true, FileSystem = fs, SstFileManager = sfm };
        o.Listeners.Add(listener);
        using RocksDatabase db = RocksDatabase.Open(DbPath, o);
        db.Put(B("a"), B("v-a"));
        fs.Inner.FreeSpace = 1000; // below the reserved disk buffer (the write buffer size)
        fs.FailMatching(".sst", FaultOps.Append, 1, InjectedFaultKind.NoSpace);
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
        Assert.AreEqual(StatusSubCode.NoSpace, ex.SubCode);
        Assert.AreEqual(ErrorSeverity.HardError, ex.Severity);
        Assert.IsFalse(listener.Ended.Wait(TimeSpan.FromMilliseconds(200)), "recovered without free space");
        Assert.ThrowsExactly<RocksDbException>(() => db.Put(B("b"), B("v-b")));
        fs.Inner.FreeSpace = long.MaxValue;
        Assert.IsTrue(listener.Ended.Wait(Timeout), "recovery did not end");
        Assert.IsNull(listener.Info!.NewBackgroundError);
        db.Put(B("b"), B("v-b"));
        Assert.AreEqual("v-a", GetString(db, "a"));
        Assert.AreEqual("v-b", GetString(db, "b"));
    }

    /// <summary>EnoughRoomForCompaction: compactions that would not fit the space limit (with the compaction buffer) are cancelled.</summary>
    [TestMethod]
    public void CompactionsThatDoNotFitAreCancelled()
    {
        MemoryFileSystem fs = NewFs();
        using var sfm = new SstFileManager(fs, 0, 0.25, 64UL << 20);
        sfm.SetMaxAllowedSpaceUsage(10UL << 20);
        sfm.SetCompactionBufferSize(10UL << 20);
        var stats = new Statistics();
        DbOptions o = NewOptions(fs);
        o.SstFileManager = sfm;
        o.Statistics = stats;
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, new ColumnFamilyOptions { Level0FileNumCompactionTrigger = 2 });
        for (int round = 0; round < 4; round++)
        {
            for (int i = 0; i < 50; i++) db.Put(Key(i), Value(i, round));
            db.Flush();
        }
        db.WaitForCompact();
        Assert.IsTrue(stats.GetTickerCount(Ticker.CompactionCancelled) > 0);
        Assert.AreEqual("4", db.GetProperty("rocksdb.num-files-at-level0"));
        Assert.AreEqual(0UL, sfm.CompactionsReservedSize);
        Assert.AreEqual(S(Value(3, 3)), GetString(db, S(Key(3))!));
    }
}
