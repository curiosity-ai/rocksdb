using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Wal;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>
/// DB-level WAL recycling (recycle_log_file_num): obsolete WALs kept in the recycle list (FindObsoleteFiles), reused by
/// the next memtable switch (CreateWAL / ReuseWritableFile: rename + overwrite in place), the recyclable record format,
/// the end of the new data at a record of the previous use, SanitizeOptions and the disableWAL check. Ports of
/// DBWALTest.FullPurgePreservesRecycledLog, FullPurgePreservesLogPendingReuse, TrackAndVerifyWALsRecycleWAL
/// (db/db_wal_test.cc) and DBWriteTest.RecycleLogToggleTest (db/db_write_test.cc).
/// </summary>
[TestClass]
public sealed class WalRecyclingTests
{
    private ManualResetEventSlim _flushGate = new(true);

    [TestCleanup]
    public void Cleanup()
    {
        TestHooks.ClearAll();
        _flushGate.Set();
    }

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

    private static DbOptions Options(IFileSystem fs, ulong recycle, WalRecoveryMode mode = WalRecoveryMode.PointInTimeRecovery) => new()
    {
        CreateIfMissing = true,
        FileSystem = fs,
        RecycleLogFileNum = recycle,
        WalRecoveryMode = mode,
    };

    internal static List<ulong> WalNumbers(MemoryFileSystem fs, string dir = DbPath)
    {
        var r = new List<ulong>();
        foreach (string c in fs.GetChildren(dir))
        {
            if (FileNames.TryParseFileName(c, out ulong n, out FileType t) && t == FileType.WalFile) r.Add(n);
        }
        r.Sort();
        return r;
    }

    /// <summary>The physical record types of a WAL (either header format), stopping at a record of another log number.</summary>
    internal static List<byte> RecordTypes(byte[] wal, ulong logNumber)
    {
        var r = new List<byte>();
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
            if (recyclable && BitConverter.ToUInt32(wal, pos + 7) != (uint)logNumber) break;
            // A record of the previous use cut by the new data: its checksum does not match.
            uint expected = RocksDb.Managed.Util.Crc32C.Unmask(BitConverter.ToUInt32(wal, pos));
            if (RocksDb.Managed.Util.Crc32C.Value(wal.AsSpan(pos + 6, header - 6 + len)) != expected) break;
            r.Add(type);
            pos += header + len;
        }
        return r;
    }

    private static string OptionsFileText(MemoryFileSystem fs)
    {
        string? latest = null;
        foreach (string c in fs.GetChildren(DbPath))
        {
            if (c.StartsWith("OPTIONS-", StringComparison.Ordinal) && (latest is null || string.CompareOrdinal(c, latest) > 0)) latest = c;
        }
        return Encoding.UTF8.GetString(fs.ReadAllBytes(DbPath + "/" + latest));
    }

    [TestMethod]
    public void ObsoleteWalIsRecycledAndReused()
    {
        MemoryFileSystem fs = NewFs();
        var log = new CollectingLogger();
        DbOptions o = Options(fs, recycle: 1);
        o.InfoLog = log;
        using RocksDatabase db = RocksDatabase.Open(DbPath, o);
        for (int i = 0; i < 100; i++) db.Put(Key(i), Value(i, 0));
        ulong first = db.GetCurrentWalFile().LogNumber;
        long firstSize = fs.GetFileSize(FileNames.LogFileName(DbPath, first));
        db.Flush();
        WaitIdle(db);
        // The obsolete WAL stays on disk, in the recycle list.
        Assert.IsTrue(fs.FileExists(FileNames.LogFileName(DbPath, first)));
        Assert.IsTrue(log.Any("adding log " + first + " to recycle list"));
        Assert.IsTrue(StringContains(OptionsFileText(fs), "recycle_log_file_num=1"));

        db.Put(B("small"), B("v"));
        ulong second = db.GetCurrentWalFile().LogNumber;
        db.Flush();
        WaitIdle(db);
        ulong third = db.GetCurrentWalFile().LogNumber;
        // The switch reused `first` as `third`: renamed, overwritten in place, not truncated.
        Assert.IsTrue(log.Any("reusing log " + first + " from recycle list"));
        Assert.IsFalse(fs.FileExists(FileNames.LogFileName(DbPath, first)));
        Assert.IsTrue(third > second);
        Assert.AreEqual(firstSize, fs.GetFileSize(FileNames.LogFileName(DbPath, third)));
        // `second` is recycled now (the list holds one).
        Assert.IsTrue(fs.FileExists(FileNames.LogFileName(DbPath, second)));

        db.Put(B("after"), B("x"));
        // Every record is in the recyclable format with the file's own log number (the stale tail ends the walk).
        List<byte> types = RecordTypes(fs.ReadAllBytes(FileNames.LogFileName(DbPath, third)), third);
        Assert.IsTrue(types.Count > 0);
        foreach (byte t in types) Assert.IsTrue(LogFormat.IsRecyclableType(t), "type " + t);
        Assert.AreEqual("x", GetString(db, "after"));
    }

    /// <summary>Flushes are done and their obsolete-file purges too (TEST_WaitForBackgroundWork).</summary>
    internal static void WaitIdle(RocksDatabase db)
    {
        db.WaitForCompact();
        db.DefaultColumnFamily.Db.WaitForBackgroundWorkForTest();
    }

    private static bool StringContains(string s, string part) => s.Contains(part, StringComparison.Ordinal);

    /// <summary>SanitizeOptions: archival, kTolerateCorruptedTailRecords and kAbsoluteConsistency disable recycling.</summary>
    [TestMethod]
    public void SanitizeOptionsDisablesRecycling()
    {
        foreach ((string name, Action<DbOptions> set, bool kept) in new (string, Action<DbOptions>, bool)[]
                 {
                     ("pit", static _ => { }, true),
                     ("skip", static o => o.WalRecoveryMode = WalRecoveryMode.SkipAnyCorruptedRecords, true),
                     ("tolerate", static o => o.WalRecoveryMode = WalRecoveryMode.TolerateCorruptedTailRecords, false),
                     ("absolute", static o => o.WalRecoveryMode = WalRecoveryMode.AbsoluteConsistency, false),
                     ("ttl", static o => o.WalTtlSeconds = 1000, false),
                     ("sizelimit", static o => o.WalSizeLimitMb = 10, false),
                 })
        {
            MemoryFileSystem fs = NewFs();
            DbOptions o = Options(fs, recycle: 2);
            set(o);
            using (RocksDatabase db = RocksDatabase.Open(DbPath, o))
            {
                db.Put(B("k"), B("v"));
                ulong first = db.GetCurrentWalFile().LogNumber;
                List<byte> types = RecordTypes(fs.ReadAllBytes(FileNames.LogFileName(DbPath, first)), first);
                Assert.AreEqual(kept, LogFormat.IsRecyclableType(types[0]), name);
                db.Flush();
                WaitIdle(db);
                Assert.AreEqual(kept, fs.FileExists(FileNames.LogFileName(DbPath, first)), name);
                // The caller's object is not modified; the OPTIONS file holds the sanitized value.
                Assert.AreEqual(2UL, o.RecycleLogFileNum);
                Assert.IsTrue(StringContains(OptionsFileText(fs), "recycle_log_file_num=" + (kept ? "2" : "0")), name);
            }
        }
    }

    [TestMethod]
    public void DisableWalIsRejected()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, Options(fs, recycle: 1));
        var e = Assert.ThrowsExactly<RocksDbException>(() => db.Put(new WriteOptions { DisableWal = true }, B("k"), B("v")));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        StringAssert.Contains(e.Message, "WriteOptions::disableWAL option is not supported if DBOptions::recycle_log_file_num > 0");
        // Sanitized away: accepted.
        MemoryFileSystem fs2 = NewFs();
        using RocksDatabase db2 = RocksDatabase.Open(DbPath, Options(fs2, recycle: 1, WalRecoveryMode.AbsoluteConsistency));
        db2.Put(new WriteOptions { DisableWal = true }, B("k"), B("v"));
    }

    /// <summary>Port of DBWALTest.FullPurgePreservesRecycledLog (also with a separate wal_dir).</summary>
    [TestMethod]
    public void FullPurgePreservesRecycledLog()
    {
        foreach (bool separateWalDir in new[] { false, true })
        {
            MemoryFileSystem fs = NewFs();
            DbOptions o = Options(fs, recycle: 2);
            string walDir = DbPath;
            if (separateWalDir)
            {
                walDir = "/wal";
                o.WalDir = walDir;
            }
            using RocksDatabase db = RocksDatabase.Open(DbPath, o);
            db.Put(B("foo"), B("v1"));
            ulong first = db.GetCurrentWalFile().LogNumber;
            db.Flush();
            WaitIdle(db);
            // DisableFileDeletions / EnableFileDeletions runs a purge; the recycled WAL survives it.
            db.DisableFileDeletions();
            db.EnableFileDeletions();
            WaitIdle(db);
            Assert.IsTrue(fs.FileExists(FileNames.LogFileName(walDir, first)), "walDir=" + walDir);
        }
    }

    /// <summary>
    /// Port of DBWALTest.FullPurgePreservesLogPendingReuse: a purge running after the WAL to reuse was chosen and before
    /// it is renamed does not delete it.
    /// </summary>
    [TestMethod]
    public void FullPurgePreservesLogPendingReuse()
    {
        MemoryFileSystem fs = NewFs();
        var log = new CollectingLogger();
        DbOptions o = Options(fs, recycle: 1);
        o.InfoLog = log;
        using RocksDatabase db = RocksDatabase.Open(DbPath, o);
        db.Put(B("foo"), B("bar"));
        ulong first = db.GetCurrentWalFile().LogNumber;
        db.Flush();
        WaitIdle(db);
        bool purged = false;
        TestHooks.Set("DBImpl::CreateWAL:BeforeReuseWritableFile", () =>
        {
            TestHooks.Set("DBImpl::CreateWAL:BeforeReuseWritableFile", null);
            // Runs on the switching thread (the full purge cannot run concurrently in this port without a hook in
            // the purge; the purge path is the same: the number is still in the recycle list).
            db.DisableFileDeletions();
            db.EnableFileDeletions();
            purged = true;
        });
        db.Put(B("foo"), B("bar"));
        db.Flush();
        WaitIdle(db);
        Assert.IsTrue(purged);
        Assert.IsTrue(log.Any("reusing log " + first + " from recycle list"));
        Assert.AreEqual("bar", GetString(db, "foo"));
    }

    /// <summary>
    /// A tailing GetUpdatesSince iterator follows writes into a reused WAL. The reader buffers the stale records of the
    /// file's previous use past the live end, and the writer then overwrites them in place: the iterator re-reads the file
    /// from the end of the last complete record (managed fix; it used to miss every later record of the reused WAL).
    /// Also across a rotation into a reused WAL.
    /// </summary>
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void TailingIteratorFollowsReusedWal(bool acrossRotation)
    {
        MemoryFileSystem fs = NewFs();
        var log = new CollectingLogger();
        DbOptions o = Options(fs, recycle: 1);
        o.InfoLog = log;
        using RocksDatabase db = RocksDatabase.Open(DbPath, o);
        for (int r = 0; r < 3; r++)
        {
            for (int i = 0; i < 50; i++) db.Put(Key(i), Value(i, r));
            db.Flush();
            WaitIdle(db);
        }
        Assert.IsTrue(log.Any("from recycle list"), "the current WAL is not a reused file");
        db.Put(WriteOptions.SyncWrites, B("a"), B("1"));
        using TransactionLogIterator it = db.GetUpdatesSince(db.GetLatestSequenceNumber());
        var seen = new List<ulong>();
        void DrainIt()
        {
            if (!it.Valid) it.Next();
            for (; it.Valid; it.Next()) seen.Add(it.Current.Sequence);
        }
        DrainIt();
        for (int round = 0; round < 3; round++)
        {
            for (int i = 0; i < 5; i++) db.Put(WriteOptions.SyncWrites, B("b" + round + "-" + i), B(new string('v', 100 * i)));
            if (acrossRotation)
            {
                db.Flush();
                WaitIdle(db);
            }
            DrainIt();
        }
        Assert.IsNull(it.Error, it.Error?.ToString());
        Assert.AreEqual(16, seen.Count);
        for (int i = 1; i < seen.Count; i++) Assert.AreEqual(seen[i - 1] + 1, seen[i]);
        Assert.AreEqual(db.GetLatestSequenceNumber(), seen[^1]);
    }

    /// <summary>
    /// A key and a value of fixed size, so that every WAL record of every use of a recycled WAL has the same size and the
    /// live end of a reused WAL is a header of the previous use (an old record).
    /// </summary>
    private static byte[] FixedValue(int i, int round) => B("v" + round.ToString("D4", System.Globalization.CultureInfo.InvariantCulture) + "-"
        + i.ToString("D6", System.Globalization.CultureInfo.InvariantCulture) + new string('x', 40));

    private static byte[] RecordValue(bool sameSize, int i, int round) => sameSize ? FixedValue(i, round) : Value(i, round);

    /// <summary>
    /// A tailing GetUpdatesSince iterator when every record has the same size: the live end of the reused WAL is the
    /// header of a record of the previous use, which the reader reports as an old record (it used to spin on it).
    /// </summary>
    [TestMethod]
    [Timeout(60_000)]
    public void TailingIteratorFollowsReusedWalAtOldRecordHeader()
    {
        MemoryFileSystem fs = NewFs();
        var log = new CollectingLogger();
        DbOptions o = Options(fs, recycle: 1);
        o.InfoLog = log;
        using RocksDatabase db = RocksDatabase.Open(DbPath, o);
        for (int r = 0; r < 3; r++)
        {
            for (int i = 0; i < 200; i++) db.Put(Key(i), FixedValue(i, r));
            db.Flush();
            WaitIdle(db);
        }
        Assert.IsTrue(log.Any("from recycle list"), "the current WAL is not a reused file");
        db.Put(Key(0), FixedValue(0, 9));
        using TransactionLogIterator it = db.GetUpdatesSince(db.GetLatestSequenceNumber());
        var seen = new List<ulong>();
        void DrainIt()
        {
            if (!it.Valid) it.Next();
            for (; it.Valid; it.Next()) seen.Add(it.Current.Sequence);
        }
        DrainIt();
        for (int round = 0; round < 4; round++)
        {
            for (int i = 0; i < 7; i++) db.Put(Key(i), FixedValue(i, 10 + round));
            DrainIt();
        }
        Assert.IsNull(it.Error, it.Error?.ToString());
        Assert.AreEqual(29, seen.Count);
        for (int i = 1; i < seen.Count; i++) Assert.AreEqual(seen[i - 1] + 1, seen[i]);
        Assert.AreEqual(db.GetLatestSequenceNumber(), seen[^1]);
    }

    /// <summary>
    /// A tailing iterator that falls behind until the WAL it reads is recycled and reused: the file is renamed and
    /// overwritten, so the rest of its records are gone. The iterator returns only genuine records of that WAL (those
    /// still buffered), then ends with NotFound "Gap in sequence numbers" (a WalTailer replica must resync), never with
    /// records of the new use.
    /// </summary>
    [TestMethod]
    [Timeout(60_000)]
    public void TailingIteratorGetsNotFoundWhenItsWalIsReused()
    {
        MemoryFileSystem fs = NewFs();
        var log = new CollectingLogger();
        DbOptions o = Options(fs, recycle: 1);
        o.InfoLog = log;
        using RocksDatabase db = RocksDatabase.Open(DbPath, o);
        ulong start = db.GetLatestSequenceNumber() + 1;
        // Several blocks of WAL: the iterator buffers only the first one.
        for (int i = 0; i < 1000; i++) db.Put(Key(i), FixedValue(i, 0));
        ulong walNumber = db.GetCurrentWalFile().LogNumber;
        using TransactionLogIterator it = db.GetUpdatesSince(start);
        var seen = new List<ulong>();
        for (int n = 0; n < 5 && it.Valid; n++, it.Next()) seen.Add(it.Current.Sequence);
        Assert.AreEqual(5, seen.Count);
        // The tailed WAL becomes obsolete and is reused (renamed and overwritten) by the next switch.
        db.Flush();
        WaitIdle(db);
        db.Put(B("x"), B("1"));
        db.Flush();
        WaitIdle(db);
        Assert.IsTrue(log.Any("reusing log " + walNumber + " from recycle list"), "the tailed WAL was not reused");
        for (int i = 0; i < 1000; i++) db.Put(Key(i), FixedValue(i, 1)); // overwrite the reused file well past block 0
        for (; it.Valid; it.Next()) seen.Add(it.Current.Sequence);
        it.Next();
        for (; it.Valid; it.Next()) seen.Add(it.Current.Sequence);
        Assert.IsNotNull(it.Error, "the iterator went on past the overwritten records");
        Assert.AreEqual(StatusCode.NotFound, it.Error.Code);
        StringAssert.Contains(it.Error.Message, "Gap in sequence numbers");
        // Only records of the original use, in order, all from the first 1000 writes.
        for (int i = 0; i < seen.Count; i++) Assert.AreEqual(start + (ulong)i, seen[i]);
        Assert.IsLessThan(1000, seen.Count);
    }

    /// <summary>
    /// A secondary that catches up repeatedly while the primary writes into reused WALs, also in the middle of a reused
    /// WAL: the secondary's WAL reader stops at the stale tail of the previous use (a cut record or, with records of the
    /// same size, an old record header) and must re-read the file from there once the primary overwrote it
    /// (DBImplSecondary::RecoverLogFiles with a log::FragmentBufferedReader per WAL). It used to miss every later record
    /// of the reused WAL.
    /// </summary>
    [TestMethod]
    [Timeout(60_000)]
    [DataRow(false)]
    [DataRow(true)]
    public void SecondaryCatchUpFollowsReusedWal(bool sameSizeRecords)
    {
        MemoryFileSystem fs = NewFs();
        var log = new CollectingLogger();
        DbOptions o = Options(fs, recycle: 2);
        o.InfoLog = log;
        using RocksDatabase primary = RocksDatabase.Open(DbPath, o);
        // Large previous uses, so that every later use of a recycled WAL ends inside stale data.
        for (int r = 0; r < 3; r++)
        {
            for (int i = 0; i < 300; i++) primary.Put(Key(i), RecordValue(sameSizeRecords, i, r));
            primary.Flush();
            WaitIdle(primary);
        }
        Assert.IsTrue(log.Any("from recycle list"), "the current WAL is not a reused file");
        using RocksDatabase sec = RocksDatabase.OpenAsSecondary(DbPath, "/sec-recycle", new DbOptions { FileSystem = fs },
            [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions())]);
        void CatchUpAndCompare(string context)
        {
            sec.TryCatchUpWithPrimary();
            Assert.AreEqual(primary.GetLatestSequenceNumber(), sec.GetLatestSequenceNumber(), context);
            for (int i = 0; i < 300; i++) Assert.AreEqual(GetString(primary, S(Key(i))!), GetString(sec, S(Key(i))!), context + " key " + i);
        }
        CatchUpAndCompare("open");
        for (int round = 3; round < 12; round++)
        {
            // Several catch-ups in the middle of the (reused) WAL.
            for (int i = 0; i < 60; i++)
            {
                primary.Put(Key(i), RecordValue(sameSizeRecords, i, round));
                if (i % 13 == 0) CatchUpAndCompare("round " + round + " write " + i);
            }
            CatchUpAndCompare("round " + round);
            if (round % 3 != 2)
            {
                primary.Flush(); // switches to a reused WAL
                WaitIdle(primary);
                CatchUpAndCompare("round " + round + " after flush");
            }
        }
        Assert.IsTrue(log.Any("reusing log"));
    }

    /// <summary>Port of DBWriteTest.RecycleLogToggleTest: WALs of a previous instance are deleted, not recycled.</summary>
    [TestMethod]
    public void RecycleLogToggle()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = Options(fs, recycle: 0);
        o.AvoidFlushDuringRecovery = true;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o)) db.Put(Key(1), B("val1"));
        o.RecycleLogFileNum = 1;
        ulong oldWal;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o))
        {
            oldWal = WalNumbers(fs)[0];
            db.Put(Key(2), B("val1"));
            db.Flush();
            WaitIdle(db);
            // Created by the previous instance: deleted, not recycled.
            Assert.IsFalse(fs.FileExists(FileNames.LogFileName(DbPath, oldWal)));
            db.Put(Key(1), B("val2"));
            db.Flush();
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o)) Assert.AreEqual("val2", S(db.Get(Key(1))));
    }

    /// <summary>
    /// Port of DBWALTest.TrackAndVerifyWALsRecycleWAL: two recycled WALs in use, the first one truncated to 0 bytes
    /// (a hole on the sequence number): point-in-time recovery drops the second WAL's write.
    /// </summary>
    [TestMethod]
    public void TrackAndVerifyWalsRecycleWal()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = Options(fs, recycle: 1);
        o.AvoidFlushDuringShutdown = true;
        o.TrackAndVerifyWals = true;
        RocksDatabase db = RocksDatabase.Open(DbPath, o);
        for (int round = 0; round < 2; round++)
        {
            db.Put(B("key_ignore"), B("wal_to_recycle"));
            db.Put(B("key_ignore1"), B("wal_to_recycle"));
            db.Put(B("key_ignore2"), B("wal_to_recycle"));
            db.Flush();
            WaitIdle(db);
        }
        // Stop background flushes so that no WAL is deleted.
        ManualResetEventSlim gate = _flushGate = new ManualResetEventSlim(false);
        TestHooks.Set("DBImpl::FlushMemTables:Start", () => gate.Wait());
        db.Put(B("key1"), B("old_value"));
        db.DefaultColumnFamily.Db.SwitchMemtableForTest(); // reuses the second recycled WAL
        db.Put(B("key1"), B("new_value"));
        List<ulong> wals = WalNumbers(fs);
        Assert.AreEqual(2, wals.Count);
        fs.SetFilesystemActive(false);
        TestHooks.ClearAll();
        gate.Set();
        try { db.Dispose(); }
        catch (RocksDbException) { }
        fs.SetFilesystemActive(true);
        // Drop Put("key1", "old_value") in the first WAL.
        using (IWritableFile f = fs.CreateWritable(FileNames.LogFileName(DbPath, wals[0]), truncate: true)) f.Sync();
        using (RocksDatabase reopened = RocksDatabase.Open(DbPath, o))
        {
            Assert.AreEqual("wal_to_recycle", GetString(reopened, "key_ignore2"));
            Assert.IsNull(GetString(reopened, "key1"));
        }
    }

    /// <summary>
    /// A reused WAL holds the new (synced) records followed by records of its previous use. After a crash the new
    /// records are recovered and the old ones end the log (no resurrected data, point-in-time) in both recovery modes
    /// that keep recycling.
    /// </summary>
    [TestMethod]
    public void StaleTailOfReusedWalEndsTheLogAfterCrash()
    {
        foreach (WalRecoveryMode mode in new[] { WalRecoveryMode.PointInTimeRecovery, WalRecoveryMode.SkipAnyCorruptedRecords })
        {
            MemoryFileSystem fs = NewFs();
            DbOptions o = Options(fs, recycle: 1, mode);
            RocksDatabase db = RocksDatabase.Open(DbPath, o);
            for (int i = 0; i < 200; i++) db.Put(Key(i), Value(i, 0));
            db.Delete(Key(7));
            db.Flush();
            WaitIdle(db);
            // Overwrite and delete keys of the flushed data: replaying the stale records would resurrect them.
            db.Put(Key(1000), B("x"));
            db.Flush();
            WaitIdle(db);
            ulong reused = db.GetCurrentWalFile().LogNumber;
            db.Put(WriteOptions.SyncWrites, Key(3), B("new3"));
            db.Delete(WriteOptions.SyncWrites, Key(4));
            ulong lastSeq = db.GetLatestSequenceNumber();
            long size = fs.GetFileSize(FileNames.LogFileName(DbPath, reused));
            // Crash: unsynced data and directory entries are lost (the WAL directory was synced by the sync write).
            fs.SetFilesystemActive(false);
            fs.SimulateCrash();
            try { db.Dispose(); }
            catch (RocksDbException) { }
            fs.SetFilesystemActive(true);
            Assert.AreEqual(size, fs.GetFileSize(FileNames.LogFileName(DbPath, reused)), "stale tail kept");
            using RocksDatabase r = RocksDatabase.Open(DbPath, o);
            string ctx = "mode=" + mode;
            Assert.AreEqual(lastSeq, r.GetLatestSequenceNumber(), ctx);
            Assert.AreEqual("new3", S(r.Get(Key(3))), ctx);
            Assert.IsNull(r.Get(Key(4)), ctx);
            Assert.IsNull(r.Get(Key(7)), ctx);
            Assert.AreEqual(S(Value(5, 0)), S(r.Get(Key(5))), ctx);
            Assert.AreEqual("x", S(r.Get(Key(1000))), ctx);
        }
    }

    /// <summary>
    /// Regression: a reused WAL closed by a memtable switch is cut to its new size, like upstream's WAL files, which are
    /// preallocated and trimmed on close (PosixWritableFile::Close ftruncate). Before, its stale tail stayed: the next
    /// point-in-time recovery stopped there while a column family had flushed past it into a fresh (empty) WAL, and the
    /// open failed with "Column family inconsistency: SST file contains data beyond the point of corruption".
    /// </summary>
    [TestMethod]
    public void ReusedWalIsCutToItsSizeWhenClosed()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = Options(fs, recycle: 1);
        o.CreateMissingColumnFamilies = true;
        ColumnFamilyDescriptor[] cfs = [new("default", new ColumnFamilyOptions()), new("cf1", new ColumnFamilyOptions())];
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o, cfs))
        {
            ColumnFamily cf1 = db.GetColumnFamily("cf1");
            for (int i = 0; i < 300; i++)
            {
                db.Put(Key(i), Value(i, 0));
                cf1.Put(Key(i), Value(i, 0));
            }
            db.Flush();
            cf1.Flush();
            WaitIdle(db); // the first WAL is recycled
            db.Put(B("d"), B("1"));
            cf1.Put(B("c"), B("1"));
            cf1.Flush(); // reuses the first WAL
            WaitIdle(db);
            ulong reused = db.GetCurrentWalFile().LogNumber;
            long staleSize = fs.GetFileSize(FileNames.LogFileName(DbPath, reused));
            db.Put(B("d2"), B("2"));
            cf1.Put(B("c2"), B("2"));
            cf1.Flush(); // a fresh WAL (the recycle list is empty); the reused one is closed
            WaitIdle(db);
            Assert.IsTrue(fs.FileExists(FileNames.LogFileName(DbPath, reused)));
            Assert.IsTrue(fs.GetFileSize(FileNames.LogFileName(DbPath, reused)) < staleSize / 4, "cut to the new data");
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o, cfs))
        {
            Assert.AreEqual("1", GetString(db, "d"));
            Assert.AreEqual("2", GetString(db, "d2"));
            Assert.AreEqual("2", GetString(db.GetColumnFamily("cf1"), "c2"));
        }
    }
}
