using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>
/// track_and_verify_wals_in_manifest (ports of the db_wal_test.cc / db_basic_test.cc WAL tracking scenarios):
/// WalAddition / WalDeletion edits are written on WAL close and obsoletion and verified at open.
/// </summary>
[TestClass]
public sealed class WalTrackingTests
{
    private static readonly ColumnFamilyDescriptor[] s_cfs = [new("default", new ColumnFamilyOptions()), new("cf1", new ColumnFamilyOptions())];

    private static DbOptions Options(MemoryFileSystem fs, bool track) => new()
    {
        CreateIfMissing = true,
        CreateMissingColumnFamilies = true,
        FileSystem = fs,
        TrackAndVerifyWalsInManifest = track,
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

    private static RecoveredVersionState ReadManifest(MemoryFileSystem fs) => ManifestReader.RecoverFromDirectory(fs, DbPath, null, out _);

    /// <summary>
    /// Writes to both column families and flushes only the default one: the closed WAL still holds cf1's data, so it stays
    /// alive and is tracked with its synced size. Returns that WAL's number.
    /// </summary>
    private static ulong CreateDbWithTrackedWal(MemoryFileSystem fs)
    {
        using RocksDatabase db = RocksDatabase.Open(DbPath, Options(fs, track: true), s_cfs);
        ColumnFamily cf1 = db.GetColumnFamily("cf1");
        for (int i = 0; i < 100; i++)
        {
            db.Put(Key(i), Value(i, 0));
            cf1.Put(Key(i), Value(i, 1));
        }
        ulong closed = WalNumbers(fs)[^1];
        db.Flush(); // closes WAL `closed` (memtable switch) and records it as synced at the flush
        cf1.Put(B("later"), B("x"));
        db.Flush(); // a second flush: the closed WAL is still needed by cf1
        RecoveredVersionState s = ReadManifest(fs);
        Assert.IsTrue(s.Wals.TryGetValue(closed, out ulong? size), "closed WAL is tracked");
        Assert.AreEqual((ulong)fs.GetFileSize(FileNames.LogFileName(DbPath, closed)), size);
        return closed;
    }

    [TestMethod]
    public void ClosedWalsAreTrackedAndObsoleteWalsDeleted()
    {
        MemoryFileSystem fs = NewFs();
        ulong closed = CreateDbWithTrackedWal(fs);
        using (RocksDatabase db = RocksDatabase.Open(DbPath, Options(fs, track: true), s_cfs))
        {
            Assert.AreEqual("x", GetString(db.GetColumnFamily("cf1"), "later"));
            Assert.AreEqual(S(Value(5, 1)), GetString(db.GetColumnFamily("cf1"), S(Key(5))!));
            db.GetColumnFamily("cf1").Flush();
            db.Flush();
        }
        RecoveredVersionState s = ReadManifest(fs);
        Assert.IsFalse(s.Wals.ContainsKey(closed), "deleted WAL is untracked");
        Assert.IsTrue(s.MinWalNumberToKeep > closed);
        Assert.IsFalse(WalNumbers(fs).Contains(closed));
    }

    [TestMethod]
    public void MissingTrackedWalIsCorruption()
    {
        MemoryFileSystem fs = NewFs();
        ulong closed = CreateDbWithTrackedWal(fs);
        fs.DeleteFile(FileNames.LogFileName(DbPath, closed));
        var ex = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, Options(fs, track: true), s_cfs).Dispose());
        Assert.AreEqual(StatusCode.Corruption, ex.Code, ex.Message);
        StringAssert.Contains(ex.Message, "Missing WAL with log number: " + closed);
        // Without tracking the open succeeds (the tracked WALs are then untracked).
        using (RocksDatabase db = RocksDatabase.Open(DbPath, Options(fs, track: false), s_cfs)) { }
        Assert.AreEqual(0, ReadManifest(fs).Wals.Count);
        using (RocksDatabase db = RocksDatabase.Open(DbPath, Options(fs, track: true), s_cfs)) { }
    }

    [TestMethod]
    public void ShrunkTrackedWalIsCorruption()
    {
        MemoryFileSystem fs = NewFs();
        ulong closed = CreateDbWithTrackedWal(fs);
        string path = FileNames.LogFileName(DbPath, closed);
        long size = fs.GetFileSize(path);
        byte[] data = new byte[size - 10];
        using (IRandomAccessFile f = fs.OpenRandomAccess(path)) f.Read(0, data);
        using (IWritableFile w = fs.CreateWritable(path, truncate: true))
        {
            w.Append(data);
            w.Sync();
        }
        var ex = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, Options(fs, track: true), s_cfs).Dispose());
        Assert.AreEqual(StatusCode.Corruption, ex.Code, ex.Message);
        StringAssert.Contains(ex.Message, "Size mismatch: WAL (log number: " + closed + ") in MANIFEST is " + size + " bytes , but actually is " +
                                          (size - 10) + " bytes on disk.");
    }

    [TestMethod]
    public void WalSetSemantics()
    {
        var wals = new SortedDictionary<ulong, ulong?>();
        ulong min = 0;
        WalSet.AddWal(wals, min, new WalAddition(5, null));
        WalSet.AddWal(wals, min, new WalAddition(5, 100));
        WalSet.AddWal(wals, min, new WalAddition(5, 50)); // out of order: keeps 100
        Assert.AreEqual(100UL, wals[5]);
        var ex = Assert.ThrowsExactly<RocksDbException>(() => WalSet.AddWal(wals, min, new WalAddition(5, null)));
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
        WalSet.AddWal(wals, min, new WalAddition(7, 10));
        WalSet.DeleteWalsBefore(wals, ref min, 6);
        Assert.AreEqual(6UL, min);
        Assert.IsFalse(wals.ContainsKey(5));
        WalSet.AddWal(wals, min, new WalAddition(4, 1)); // obsolete: ignored
        Assert.IsFalse(wals.ContainsKey(4));
        WalSet.DeleteWalsBefore(wals, ref min, 3); // never goes backwards
        Assert.AreEqual(6UL, min);
        WalSet.CheckWals(wals, new Dictionary<ulong, string> { [7] = "x" }, static _ => 10);
        ex = Assert.ThrowsExactly<RocksDbException>(() => WalSet.CheckWals(wals, new Dictionary<ulong, string>(), static _ => 10));
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
    }
}
