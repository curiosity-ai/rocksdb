using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Wal;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>
/// track_and_verify_wals (predecessor-WAL-info records): ports of DBWALTrackAndVerifyWALsWithParamsTest.Basic
/// (db/db_wal_test.cc) plus checks of the records the DB writes when it switches WALs.
/// </summary>
[TestClass]
public sealed class TrackAndVerifyWalsTests
{
    // Holds back the flush of a crashed instance; released once the test no longer uses its file system.
    private ManualResetEventSlim _flushGate = new(false);

    [TestCleanup]
    public void Cleanup()
    {
        TestHooks.ClearAll();
        _flushGate.Set();
    }

    private static DbOptions Options(MemoryFileSystem fs, WalRecoveryMode mode) => new()
    {
        CreateIfMissing = true,
        FileSystem = fs,
        TrackAndVerifyWals = true,
        WalRecoveryMode = mode,
        AvoidFlushDuringShutdown = true,
    };

    private static List<ulong> WalNumbers(MemoryFileSystem fs)
    {
        var r = new List<ulong>();
        foreach (string c in fs.GetChildren(DbPath))
        {
            if (FileNames.TryParseFileName(c, out ulong n, out FileType t) && t == FileType.WalFile) r.Add(n);
        }
        r.Sort();
        return r;
    }

    private static void Truncate(MemoryFileSystem fs, string path)
    {
        using IWritableFile f = fs.CreateWritable(path, truncate: true);
        f.Sync();
    }

    /// <summary>Reads the physical records of a WAL (type byte + payload), non-recyclable format.</summary>
    private static List<(byte Type, byte[] Payload)> PhysicalRecords(byte[] wal)
    {
        var r = new List<(byte, byte[])>();
        int pos = 0;
        while (pos + LogFormat.HeaderSize <= wal.Length)
        {
            int blockLeft = LogFormat.BlockSize - pos % LogFormat.BlockSize;
            if (blockLeft < LogFormat.HeaderSize) { pos += blockLeft; continue; }
            int len = wal[pos + 4] | wal[pos + 5] << 8;
            byte type = wal[pos + 6];
            if (type == 0 && len == 0) break;
            r.Add((type, wal.AsSpan(pos + LogFormat.HeaderSize, len).ToArray()));
            pos += LogFormat.HeaderSize + len;
        }
        return r;
    }

    /// <summary>
    /// Writes key1=old_value, switches the WAL (the flush of the old memtable is held back so that the first WAL stays)
    /// and writes key1=new_value, all synced; then "crashes". Returns the two WAL numbers and the last sequence of the
    /// first WAL.
    /// </summary>
    private (ulong First, ulong Second, ulong LastSeqInFirst) CreateTwoWals(MemoryFileSystem fs, WalRecoveryMode mode, bool letFlushFinish)
    {
        using var flushReached = new ManualResetEventSlim(false);
        _flushGate.Set();
        ManualResetEventSlim gate = _flushGate = new ManualResetEventSlim(false);
        if (!letFlushFinish)
        {
            TestHooks.Set("DBImpl::FlushMemTables:Start", () =>
            {
                flushReached.Set();
                gate.Wait();
            });
        }
        RocksDatabase db = RocksDatabase.Open(DbPath, Options(fs, mode), new ColumnFamilyOptions { WriteBufferSize = 64 << 20, MaxWriteBufferNumber = 6 });
        db.Put(WriteOptions.SyncWrites, B("key1"), B("old_value"));
        ulong lastSeqInFirst = db.GetLatestSequenceNumber();
        db.DefaultColumnFamily.Db.SwitchMemtableForTest();
        // The flush of the old memtable is blocked before it does anything: the first WAL stays.
        if (!letFlushFinish) Assert.IsTrue(flushReached.Wait(TimeSpan.FromSeconds(30)));
        db.Put(WriteOptions.SyncWrites, B("key1"), B("new_value"));
        List<ulong> wals = WalNumbers(fs);
        Assert.AreEqual(2, wals.Count);
        if (letFlushFinish)
        {
            db.Flush();
            db.Dispose();
            return (wals[0], wals[1], lastSeqInFirst);
        }
        fs.SimulateCrash();
        GC.KeepAlive(db);
        return (wals[0], wals[1], lastSeqInFirst);
    }

    [TestMethod]
    public void NewWalRecordsItsPredecessor()
    {
        MemoryFileSystem fs = NewFs();
        (ulong first, ulong second, ulong lastSeq) = CreateTwoWals(fs, WalRecoveryMode.PointInTimeRecovery, letFlushFinish: false);
        TestHooks.ClearAll(); // the crashed instance's flush stays blocked (until cleanup): it must not touch the files
        List<(byte Type, byte[] Payload)> records = PhysicalRecords(fs.ReadAllBytes(FileNames.LogFileName(DbPath, second)));
        Assert.AreEqual((byte)LogRecordType.PredecessorWalInfo, records[0].Type, "first record of the switched-to WAL");
        Assert.IsTrue(PredecessorWalInfo.TryDecode(records[0].Payload, out PredecessorWalInfo info));
        Assert.AreEqual(first, info.LogNumber);
        Assert.AreEqual((ulong)fs.GetFileSize(FileNames.LogFileName(DbPath, first)), info.SizeBytes);
        Assert.AreEqual(lastSeq, info.LastSequenceRecorded);
        // The first WAL of an open has no predecessor record (like upstream).
        Assert.AreNotEqual((byte)LogRecordType.PredecessorWalInfo, PhysicalRecords(fs.ReadAllBytes(FileNames.LogFileName(DbPath, first)))[0].Type);

        // Without the option nothing is written.
        MemoryFileSystem fs2 = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs2)))
        {
            db.Put(B("a"), B("1"));
            db.DefaultColumnFamily.Db.SwitchMemtableForTest();
            db.Put(B("a"), B("2"));
            foreach (ulong n in WalNumbers(fs2))
            {
                foreach ((byte type, _) in PhysicalRecords(fs2.ReadAllBytes(FileNames.LogFileName(DbPath, n))))
                {
                    Assert.AreNotEqual((byte)LogRecordType.PredecessorWalInfo, type);
                }
            }
        }
    }

    private static readonly WalRecoveryMode[] s_modes =
    [
        WalRecoveryMode.TolerateCorruptedTailRecords, WalRecoveryMode.AbsoluteConsistency,
        WalRecoveryMode.PointInTimeRecovery, WalRecoveryMode.SkipAnyCorruptedRecords,
    ];

    /// <summary>Port of DBWALTrackAndVerifyWALsWithParamsTest.Basic for every wal_recovery_mode.</summary>
    [TestMethod]
    public void BasicForEveryRecoveryMode()
    {
        foreach (WalRecoveryMode mode in s_modes)
        {
            for (int i = 0; i < 5; i++)
            {
                string ctx = "mode=" + mode + " case=" + i;
                TestHooks.ClearAll();
                MemoryFileSystem fs = NewFs();
                (ulong first, ulong second, ulong lastSeqInFirst) = CreateTwoWals(fs, mode, letFlushFinish: i == 0);
                string firstName = FileNames.LogFileName(DbPath, first);
                TestHooks.ClearAll(); // the crashed instance's flush stays blocked until cleanup
                switch (i)
                {
                    case 0:
                        // The obsolete first WAL was deleted: not a WAL hole.
                        Assert.IsFalse(fs.FileExists(firstName), ctx);
                        break;
                    case 1:
                        // A hole on the WAL number.
                        fs.DeleteFile(firstName);
                        break;
                    case 2:
                        // A hole on the sequence number.
                        Truncate(fs, firstName);
                        break;
                    case 3:
                        // A hole on the size: the truncated WAL is mocked to report the right last sequence.
                        Truncate(fs, firstName);
                        TestHooks.SetSpan("DBImpl::UpdatePredecessorWALInfo", data =>
                        {
                            if (RocksDb.Managed.Util.Coding.DecodeFixed64(data) == first) RocksDb.Managed.Util.Coding.EncodeFixed64(data[8..], lastSeqInFirst);
                        });
                        break;
                    case 4:
                        fs.DeleteFile(firstName);
                        fs.DeleteFile(FileNames.LogFileName(DbPath, second));
                        break;
                }
                RocksDatabase? db = null;
                RocksDbException? error = null;
                try { db = RocksDatabase.Open(DbPath, Options(fs, mode)); }
                catch (RocksDbException e) { error = e; }
                TestHooks.ClearAll();
                try
                {
                    if (i == 0)
                    {
                        Assert.IsNull(error, ctx + ": " + error?.Message);
                        Assert.AreEqual("new_value", GetString(db!, "key1"), ctx);
                        continue;
                    }
                    if (i == 4)
                    {
                        Assert.IsNotNull(error, ctx);
                        Assert.AreEqual(StatusCode.Corruption, error.Code, ctx);
                        StringAssert.Contains(error.Message, "Opening an existing DB with no WAL files", ctx);
                        continue;
                    }
                    switch (mode)
                    {
                        case WalRecoveryMode.PointInTimeRecovery:
                            Assert.IsNull(error, ctx + ": " + error?.Message);
                            Assert.IsNull(GetString(db!, "key1"), ctx);
                            break;
                        case WalRecoveryMode.AbsoluteConsistency:
                        case WalRecoveryMode.TolerateCorruptedTailRecords:
                            Assert.IsNotNull(error, ctx);
                            Assert.AreEqual(StatusCode.Corruption, error.Code, ctx);
                            string msg = i switch
                            {
                                1 => "Missing WAL",
                                2 => "Mismatched last sequence number recorded in the WAL",
                                _ => "Mismatched size of the WAL",
                            };
                            StringAssert.Contains(error.Message, msg, ctx);
                            break;
                        default:
                            Assert.IsNull(error, ctx + ": " + error?.Message);
                            Assert.AreEqual("new_value", GetString(db!, "key1"), ctx);
                            break;
                    }
                }
                finally
                {
                    db?.Dispose();
                    _flushGate.Set();
                }
            }
        }
    }

    /// <summary>A missing predecessor is also detected by read-only opens (DBImpl::Recover with read_only).</summary>
    [TestMethod]
    public void ReadOnlyOpenVerifiesPredecessors()
    {
        MemoryFileSystem fs = NewFs();
        (ulong first, _, _) = CreateTwoWals(fs, WalRecoveryMode.TolerateCorruptedTailRecords, letFlushFinish: false);
        TestHooks.ClearAll(); // the crashed instance's flush stays blocked (until cleanup): it must not touch the files
        DbOptions o = Options(fs, WalRecoveryMode.TolerateCorruptedTailRecords);
        using (RocksDatabase ro = RocksDatabase.OpenReadOnly(DbPath, o))
        {
            Assert.AreEqual("new_value", GetString(ro, "key1"));
        }
        fs.DeleteFile(FileNames.LogFileName(DbPath, first));
        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.OpenReadOnly(DbPath, o).Dispose());
        Assert.AreEqual(StatusCode.Corruption, e.Code);
        StringAssert.Contains(e.Message, "Missing WAL of log number " + first);
        // Without the option the predecessor records are ignored.
        o.TrackAndVerifyWals = false;
        using RocksDatabase ro2 = RocksDatabase.OpenReadOnly(DbPath, o);
        Assert.AreEqual("new_value", GetString(ro2, "key1"));
    }

    /// <summary>The option round-trips through the OPTIONS file.</summary>
    [TestMethod]
    public void OptionIsPersisted()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, Options(fs, WalRecoveryMode.PointInTimeRecovery))) { }
        OptionsFile f = OptionsFile.Load(fs, OptionsFile.FindLatestOptionsFile(fs, DbPath)!, preserveUnknownOptions: false);
        Assert.IsTrue(f.DbOptions.TrackAndVerifyWals);
        StringAssert.Contains(System.Text.Encoding.UTF8.GetString(fs.ReadAllBytes(OptionsFile.FindLatestOptionsFile(fs, DbPath)!)), "track_and_verify_wals=true");
    }
}
