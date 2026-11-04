using System.Text;
using RocksDb.Managed.CompatTests.Api;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Db;
using RocksDb.Managed.Utilities.Transactions;
using RocksDb.Managed.Wal;

namespace RocksDb.Managed.CompatTests.Wal;

/// <summary>
/// WAL recycling (recycle_log_file_num) against native RocksDB 11.12 (rocksdb_options_set_recycle_log_file_num): both
/// engines reuse obsolete WALs (LOG "reusing log N from recycle list"), write the same recyclable records (types
/// 5..8, 131 with track_and_verify_wals, the log number in each header), and each recovers the other's crash images,
/// whose reused WALs end in records of their previous use, to the same contents. Also: the WAL a point-in-time stop
/// leaves behind in a 2PC DB is replayed by the next open in both engines without failing (the recovered-sequence record).
/// </summary>
[TestClass]
[DoNotParallelize]
public sealed class WalRecyclingCompatTests
{
    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);

    private static WalWriteConfig Recycling(WalMatrixWriter writer, bool trackWals) =>
        new(writer, Recycle: true, TrackWals: trackWals, TrackInManifest: false, Zstd: false, ManualWalFlush: false, SeparateWalDir: false,
            AvoidFlushDuringRecovery: false);

    /// <summary>The WAL numbers of <paramref name="image"/> whose file holds bytes past its valid records (a stale tail).</summary>
    private static List<ulong> WalsWithStaleTail(WalImage image)
    {
        var r = new List<ulong>();
        foreach (ulong n in image.Wals())
        {
            byte[] b = File.ReadAllBytes(image.WalPath(n));
            List<PhysicalRecord> recs = WalImage.PhysicalRecords(b, n);
            int end = recs.Count == 0 ? 0 : recs[^1].End;
            if (b.Length - end > 1024) r.Add(n);
        }
        return r;
    }

    private static void AssertRecyclableRecords(WalImage image, bool expectPredecessor)
    {
        bool sawPredecessor = false;
        foreach (ulong n in image.Wals())
        {
            foreach (PhysicalRecord p in WalImage.PhysicalRecords(File.ReadAllBytes(image.WalPath(n)), n))
            {
                Assert.IsTrue(LogFormat.IsRecyclableType(p.Type) || p.Type == (byte)LogRecordType.SetCompressionType, "WAL " + n + " type " + p.Type);
                sawPredecessor |= p.Type == (byte)LogRecordType.RecyclePredecessorWalInfo;
            }
        }
        Assert.AreEqual(expectPredecessor, sawPredecessor, "recyclable predecessor records (type 131)");
    }

    private static void AssertBothRecover(WalImage image)
    {
        foreach (WalRecoveryMode mode in new[] { WalRecoveryMode.PointInTimeRecovery, WalRecoveryMode.SkipAnyCorruptedRecords,
                     WalRecoveryMode.TolerateCorruptedTailRecords, WalRecoveryMode.AbsoluteConsistency })
        {
            var open = new WalOpenConfig(mode, Paranoid: true, WalMatrixFilter.None);
            using var m = new WalImage { Root = image.Root + "-m", Config = image.Config };
            using var n = new WalImage { Root = image.Root + "-n", Config = image.Config };
            WalMatrixUtil.CopyTree(image.Root, m.Root);
            WalMatrixUtil.CopyTree(image.Root, n.Root);
            WalOpenOutcome mo = WalMatrixEngines.OpenManaged(m, open);
            WalOpenOutcome no = WalMatrixEngines.OpenNative(n, open);
            Assert.AreEqual(no.ToString(), mo.ToString(), mode.ToString());
            Assert.AreEqual(no.Contents, mo.Contents, mode.ToString());
            if (mode is WalRecoveryMode.PointInTimeRecovery or WalRecoveryMode.SkipAnyCorruptedRecords)
            {
                Assert.IsTrue(mo.Ok, mode + ": " + mo);
                StringAssert.Contains(mo.Contents, " " + image.LastSyncedKey + "=" + image.LastSyncedValue);
            }
        }
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void ManagedRecycledWalCrashImageRecoversInNative(bool trackWals)
    {
        string root = Path.Combine(Path.GetTempPath(), "rocksdb-managed-recycle-m-" + Guid.NewGuid().ToString("N"));
        using WalImage image = WalMatrixEngines.WriteImage(Recycling(WalMatrixWriter.Managed, trackWals), root, seed: 7);
        try
        {
            Assert.IsTrue(WalsWithStaleTail(image).Count > 0, "a reused WAL ends in records of its previous use");
            AssertRecyclableRecords(image, expectPredecessor: trackWals);
            AssertBothRecover(image);
        }
        finally
        {
            foreach (string d in new[] { root + "-m", root + "-n" }) TryDelete(d);
        }
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void NativeRecycledWalCrashImageRecoversInManaged(bool trackWals)
    {
        string root = Path.Combine(Path.GetTempPath(), "rocksdb-managed-recycle-n-" + Guid.NewGuid().ToString("N"));
        using WalImage image = WalMatrixEngines.WriteImage(Recycling(WalMatrixWriter.Native, trackWals), root, seed: 7);
        try
        {
            Assert.IsTrue(WalsWithStaleTail(image).Count > 0, "native reused a WAL (stale tail)");
            AssertRecyclableRecords(image, expectPredecessor: trackWals);
            AssertBothRecover(image);
        }
        finally
        {
            foreach (string d in new[] { root + "-m", root + "-n" }) TryDelete(d);
        }
    }

    /// <summary>
    /// The same writes produce the same recyclable records in both engines: types, lengths and payloads equal record by
    /// record, and each header carries the file's own log number (bytes 7..10) with a matching checksum. Both engines
    /// reuse the first WAL for the third one (LOG "reusing log").
    /// </summary>
    [TestMethod]
    public void RecyclableRecordsMatchNative()
    {
        using var md = new TempDir("recycle-bytes-m");
        using var nd = new TempDir("recycle-bytes-n");
        var managed = new List<(ulong Wal, List<(byte, byte[])> Records)>();
        var native = new List<(ulong Wal, List<(byte, byte[])> Records)>();
        var mo = new DbOptions
        {
            CreateIfMissing = true,
            RecycleLogFileNum = 1,
            WalRecoveryMode = WalRecoveryMode.PointInTimeRecovery,
            TrackAndVerifyWals = true,
            AvoidFlushDuringShutdown = true,
        };
        using (RocksDatabase db = RocksDatabase.Open(md.Path, mo))
        {
            for (int round = 0; round < 3; round++)
            {
                for (int i = 0; i < 20; i++) db.Put(U("k" + round + "-" + i), U(new string((char)('a' + i), 10 + i * 13)));
                using (WriteBatch b = db.CreateWriteBatch())
                {
                    b.Put(U("batch" + round), U("x")).Delete(U("k" + round + "-3"));
                    db.Write(b);
                }
                db.Put(U("big" + round), U(new string('Z', 50_000)));
                managed.Add(Snapshot(md.Path, db.GetCurrentWalFile().LogNumber));
                if (round < 2)
                {
                    db.Flush();
                    db.DefaultColumnFamily.Db.WaitForBackgroundWorkForTest();
                }
            }
        }
        var no = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        RocksDbSharp.Native n = RocksDbSharp.Native.Instance;
        n.rocksdb_options_set_recycle_log_file_num(no.Handle, (UIntPtr)1);
        n.rocksdb_options_set_wal_recovery_mode(no.Handle, (int)WalRecoveryMode.PointInTimeRecovery);
        n.rocksdb_options_set_track_and_verify_wals(no.Handle, 1);
        n.rocksdb_options_set_avoid_flush_during_shutdown(no.Handle, 1);
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(no, nd.Path))
        {
            for (int round = 0; round < 3; round++)
            {
                for (int i = 0; i < 20; i++) db.Put(U("k" + round + "-" + i), U(new string((char)('a' + i), 10 + i * 13)));
                using (var b = new RocksDbSharp.WriteBatch())
                {
                    b.Put(U("batch" + round), U("x")).Delete(U("k" + round + "-3"));
                    db.Write(b);
                }
                db.Put(U("big" + round), U(new string('Z', 50_000)));
                native.Add(Snapshot(nd.Path, WalMatrixUtil.Wals(nd.Path)[^1]));
                if (round < 2)
                {
                    db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
                    IntPtr w = n.rocksdb_wait_for_compact_options_create();
                    n.rocksdb_wait_for_compact(db.Handle, w);
                    n.rocksdb_wait_for_compact_options_destroy(w);
                }
            }
        }
        Assert.AreEqual(native.Count, managed.Count);
        for (int r = 0; r < native.Count; r++)
        {
            List<(byte, byte[])> a = managed[r].Records, b = native[r].Records;
            // The sequence numbers in the batch payloads match too: both engines number the same writes the same way.
            Assert.AreEqual(b.Count, a.Count, "round " + r + " record count");
            for (int i = 0; i < a.Count; i++)
            {
                Assert.AreEqual(b[i].Item1, a[i].Item1, "round " + r + " record " + i + " type");
                if (b[i].Item1 is (byte)LogRecordType.RecyclePredecessorWalInfo or (byte)LogRecordType.PredecessorWalInfo) continue; // names the engine's WAL numbers
                CollectionAssert.AreEqual(b[i].Item2, a[i].Item2, "round " + r + " record " + i + " payload");
            }
        }
        StringAssert.Contains(File.ReadAllText(Path.Combine(nd.Path, "LOG")), "reusing log " + native[0].Wal + " from recycle list");
    }

    /// <summary>The valid recyclable records (type, payload) of a WAL, checking the log number field and the checksum.</summary>
    private static (ulong, List<(byte, byte[])>) Snapshot(string dir, ulong wal)
    {
        byte[] b = File.ReadAllBytes(FileNames.LogFileName(dir, wal));
        var r = new List<(byte, byte[])>();
        foreach (PhysicalRecord p in WalImage.PhysicalRecords(b, wal))
        {
            if (LogFormat.IsRecyclableType(p.Type)) Assert.AreEqual((uint)wal, BitConverter.ToUInt32(b, p.Offset + 7), "log number in the header");
            r.Add((p.Type, b.AsSpan(p.Offset + p.HeaderSize, p.Length).ToArray()));
        }
        return (wal, r);
    }

    /// <summary>
    /// A 2PC (WritePrepared) DB whose latest WAL has a corrupted record: kPointInTimeRecovery stops there and opens in
    /// both engines. A 2PC recovery does not advance min_log_number_to_keep, so the corrupted WAL is kept and replayed
    /// again by the next open; the empty batch with the recovered sequence that DBImpl::Open writes to the new WAL lets
    /// that replay resume after the corrupted WAL. Regression: the managed port did not write it, so the next open failed
    /// with "Column family inconsistency" while native opened.
    /// </summary>
    [TestMethod]
    public void TwoPcPointInTimeStopIsReplayedByTheNextOpen()
    {
        using var src = new TempDir("2pc-pit");
        using (var native = new NativeTxnDb(src.Path, TransactionWritePolicy.WritePrepared))
        {
            for (int i = 0; i < 30; i++) native.Put("k" + i, new string('v', 40 + i));
            IntPtr t = native.Begin();
            NativeTxnDb.SetName(t, "x");
            NativeTxnDb.Put(t, "t1", "tv");
            NativeTxnDb.Prepare(t);
            NativeTxnDb.Commit(t);
            NativeTxnDb.Destroy(t);
            for (int i = 30; i < 60; i++) native.Put("k" + i, new string('w', 40 + i));
        }
        var cfg = new WalWriteConfig(WalMatrixWriter.Native, false, false, false, false, false, false, false);
        using var image = new WalImage { Root = src.Path + "-img", Config = cfg };
        Directory.CreateDirectory(image.Root);
        WalMatrixUtil.CopyTree(src.Path, image.DbDir);
        string mRoot = image.Root + "-m", nRoot = image.Root + "-n";
        try
        {
            Assert.IsTrue(image.CopyWithFault(mRoot, WalFault.CorruptMiddleChecksum));
            Assert.IsTrue(image.CopyWithFault(nRoot, WalFault.CorruptMiddleChecksum));
            string mDb = Path.Combine(mRoot, "db"), nDb = Path.Combine(nRoot, "db");
            string? m1, n1;
            using (TransactionDatabase m = TransactionDatabase.Open(mDb, new DbOptions(), new TransactionDbOptions { WritePolicy = TransactionWritePolicy.WritePrepared }))
            {
                m1 = Encoding.UTF8.GetString(m.Database.Get(U("k3"))!);
                m.Put(WriteOptions.SyncWrites, null, U("after"), U("1"));
            }
            using (var n = new NativeTxnDb(nDb, TransactionWritePolicy.WritePrepared))
            {
                n1 = n.Get("k3");
                n.Put("after", "1");
            }
            Assert.AreEqual(n1, m1);
            string? mErr = null, nErr = null;
            try
            {
                using TransactionDatabase m = TransactionDatabase.Open(mDb, new DbOptions(), new TransactionDbOptions { WritePolicy = TransactionWritePolicy.WritePrepared });
            }
            catch (RocksDbException e) { mErr = e.Message; }
            try
            {
                using var n = new NativeTxnDb(nDb, TransactionWritePolicy.WritePrepared);
            }
            catch (RocksDbSharp.RocksDbException e) { nErr = e.Message; }
            Assert.AreEqual(nErr, mErr);
            Assert.IsNull(nErr);
        }
        finally
        {
            TryDelete(mRoot);
            TryDelete(nRoot);
        }
    }

    private static void TryDelete(string d)
    {
        try { if (Directory.Exists(d)) Directory.Delete(d, recursive: true); }
        catch (IOException) { }
    }
}
