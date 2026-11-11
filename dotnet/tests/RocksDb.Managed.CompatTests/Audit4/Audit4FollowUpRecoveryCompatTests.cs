using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Wal;

namespace RocksDb.Managed.CompatTests.Audit4;

/// <summary>
/// Audit #4 follow-up (recovery): what native RocksDB does with a WAL record whose checksum is valid but whose
/// WriteBatch body is malformed or refused by the memtable (DBImpl::ProcessLogRecord / InsertLogRecordToMemtable /
/// WriteBatchInternal::InsertInto / WriteBatchInternal::Iterate), in every wal_recovery_mode, with and without
/// paranoid_checks. The same crafted directory is opened by both engines; outcome (status, visible keys, last
/// sequence) must agree.
/// </summary>
[TestClass]
public sealed class Audit4FollowUpRecoveryCompatTests
{
    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);

    private static readonly string[] s_keys = ["a", "b", "c", "d", "e", "f", "x", "y"];

    private static byte[] Batch(ulong sequence, Action<WriteBatch> fill, uint? count = null, byte[]? tail = null)
    {
        using var b = new WriteBatch();
        fill(b);
        b.Sequence = sequence;
        byte[] rep = b.Data.ToArray();
        if (tail is not null)
        {
            byte[] r = new byte[rep.Length + tail.Length];
            rep.CopyTo(r, 0);
            tail.CopyTo(r, rep.Length);
            rep = r;
        }
        if (count is { } c) RocksDb.Managed.Util.Coding.EncodeFixed32(rep.AsSpan(8), c);
        return rep;
    }

    private static byte[] Put(ulong sequence, string key, string value) => Batch(sequence, b => b.Put(U(key), U(value)));

    /// <summary>The crafted WALs: every one starts with Put(c)@3 and ends with a well-formed Put(d).</summary>
    private static List<byte[]> Scenario(string name) => name switch
    {
        // Put(e) then a byte that is no ValueType: Iterate applies the Put, then Corruption("unknown WriteBatch tag").
        "unknown-tag-after-entry" => [Put(3, "c", "3"), Batch(4, b => b.Put(U("e"), U("4")), count: 2, tail: [0xEE]), Put(6, "d", "6")],
        // The bad tag is the first entry: nothing of the batch is applied.
        "unknown-tag-first" => [Put(3, "c", "3"), Batch(4, _ => { }, count: 1, tail: [0xEE]), Put(5, "d", "5")],
        // Header count larger than the entries: all entries are applied, then Corruption("WriteBatch has wrong count").
        "count-too-large" => [Put(3, "c", "3"), Batch(4, b => b.Put(U("e"), U("4")).Put(U("f"), U("5")), count: 3), Put(7, "d", "7")],
        // Header count smaller than the entries.
        "count-too-small" => [Put(3, "c", "3"), Batch(4, b => b.Put(U("e"), U("4")).Put(U("f"), U("5")), count: 1), Put(6, "d", "6")],
        // A truncated entry: Put with a key length running past the end ("bad WriteBatch Put").
        "truncated-entry" => [Put(3, "c", "3"), Batch(4, b => b.Put(U("e"), U("4")), count: 2, tail: [0x01, 0x7F, 0x41]), Put(6, "d", "6")],
        // A batch the memtable refuses (DeleteRange with end before start), between two Puts of the same batch.
        "inverted-delete-range" => [Put(3, "c", "3"), Batch(4, b => b.Put(U("x"), U("1")).DeleteRange(U("c"), U("a")).Put(U("y"), U("1"))), Put(7, "d", "7")],
        _ => throw new ArgumentException(name),
    };

    private static readonly string[] s_scenarios =
        ["unknown-tag-after-entry", "unknown-tag-first", "count-too-large", "count-too-small", "truncated-entry", "inverted-delete-range"];

    /// <summary>Native DB holding a=1, b=2 (sequences 1, 2), closed, plus a newer WAL with <paramref name="records"/>.</summary>
    private static void BuildDb(string path, List<byte[]> records)
    {
        using (var db = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions().SetCreateIfMissing(true), path))
        {
            db.Put(U("a"), U("1"));
            db.Put(U("b"), U("2"));
        }
        ulong max = 0;
        foreach (string f in Directory.GetFiles(path))
        {
            if (FileNames.TryParseFileName(Path.GetFileName(f), out ulong n, out _)) max = Math.Max(max, n);
        }
        ulong number = max + 10;
        using var log = new LogWriter(LocalFileSystem.Instance.CreateWritable(FileNames.LogFileName(path, number), truncate: true), number, recycleLogFiles: false);
        foreach (byte[] r in records) log.AddRecord(r);
        log.WriteBuffer();
        log.Sync();
    }

    private static void CopyDir(string from, string to)
    {
        foreach (string f in Directory.GetFiles(from))
        {
            if (Path.GetFileName(f) == "LOCK") continue;
            File.Copy(f, Path.Combine(to, Path.GetFileName(f)));
        }
    }

    private static string NativeOutcome(string path, RocksDbSharp.Recovery mode, bool paranoid)
    {
        RocksDbSharp.DbOptions o = new RocksDbSharp.DbOptions().SetWalRecoveryMode(mode).SetParanoidChecks(paranoid);
        try
        {
            using var db = RocksDbSharp.RocksDb.Open(o, path);
            var sb = new StringBuilder("ok");
            foreach (string k in s_keys)
            {
                byte[]? v = db.Get(U(k));
                if (v is not null) sb.Append(' ').Append(k).Append('=').Append(Encoding.UTF8.GetString(v));
            }
            return sb.Append(" seq=").Append(db.GetLatestSequenceNumber()).ToString();
        }
        catch (RocksDbSharp.RocksDbException e)
        {
            int colon = e.Message.IndexOf(':');
            return "error " + (colon < 0 ? e.Message : e.Message[..colon]).Replace(" ", "");
        }
    }

    private static string ManagedOutcome(string path, WalRecoveryMode mode, bool paranoid)
    {
        try
        {
            using RocksDatabase db = RocksDatabase.Open(path, new DbOptions { WalRecoveryMode = mode, ParanoidChecks = paranoid });
            var sb = new StringBuilder("ok");
            foreach (string k in s_keys)
            {
                byte[]? v = db.Get(U(k));
                if (v is not null) sb.Append(' ').Append(k).Append('=').Append(Encoding.UTF8.GetString(v));
            }
            return sb.Append(" seq=").Append(db.GetLatestSequenceNumber()).ToString();
        }
        catch (RocksDbException e)
        {
            return "error " + (e.Code == StatusCode.InvalidArgument ? "Invalidargument" : e.Code.ToString());
        }
    }

    private static readonly (RocksDbSharp.Recovery Native, WalRecoveryMode Managed)[] s_modes =
    [
        (RocksDbSharp.Recovery.TolerateCorruptedTailRecords, WalRecoveryMode.TolerateCorruptedTailRecords),
        (RocksDbSharp.Recovery.AbsoluteConsistency, WalRecoveryMode.AbsoluteConsistency),
        (RocksDbSharp.Recovery.PointInTime, WalRecoveryMode.PointInTimeRecovery),
        (RocksDbSharp.Recovery.SkipAnyCorruptedRecords, WalRecoveryMode.SkipAnyCorruptedRecords),
    ];

    [TestMethod]
    public void MalformedOrRefusedBatchInWalRecoversLikeNative()
    {
        var diffs = new StringBuilder();
        var table = new StringBuilder();
        foreach (string scenario in s_scenarios)
        {
            using var template = new TempDir("a4fr-t");
            BuildDb(template.Path, Scenario(scenario));
            foreach ((RocksDbSharp.Recovery nativeMode, WalRecoveryMode managedMode) in s_modes)
            {
                foreach (bool paranoid in new[] { true, false })
                {
                    using var nd = new TempDir("a4fr-n");
                    using var md = new TempDir("a4fr-m");
                    CopyDir(template.Path, nd.Path);
                    CopyDir(template.Path, md.Path);
                    string n = NativeOutcome(nd.Path, nativeMode, paranoid);
                    string m = ManagedOutcome(md.Path, managedMode, paranoid);
                    string id = scenario + " / " + managedMode + " / paranoid=" + paranoid;
                    table.Append(id).Append(": ").Append(n).Append('\n');
                    if (n != m) diffs.Append(id).Append("\n  native : ").Append(n).Append("\n  managed: ").Append(m).Append('\n');
                }
            }
        }
        Console.WriteLine(table.ToString());
        Assert.AreEqual("", diffs.ToString());
    }

    private static string WalRecords(string dir)
    {
        var sb = new StringBuilder();
        string[] logs = Directory.GetFiles(dir, "*.log");
        Array.Sort(logs, StringComparer.Ordinal);
        foreach (string f in logs)
        {
            using var reader = new LogReader(LocalFileSystem.Instance.OpenSequential(f), null, checksum: true, 0);
            while (reader.TryReadRecord(out ReadOnlySpan<byte> rec)) sb.Append('[').Append(RocksDb.Managed.CompatTests.Wal.NativeDbHelper.Describe(rec)).Append(']');
        }
        return sb.ToString();
    }

    [TestMethod]
    public void WalIsCutBackAtCloseAfterARefusedWriteLikeNative()
    {
        // DBImpl::WriteImpl: a failed memtable insert calls logs_.back().SetAttemptTruncateSize(wal_context.prev_size), and
        // LogWriterNumber::ClearWriter (DB close) truncates the WAL to that size: the refused write's record, already
        // in the WAL, is removed, so the next open does not replay it. This (not anything in recovery) is why native's
        // last sequence is unchanged after reopening. The port kept the record; its partial inserts then came back.
        using var nd = new TempDir("a4fr-n");
        using var md = new TempDir("a4fr-m");
        using (var n = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions().SetCreateIfMissing(true), nd.Path))
        {
            n.Put(U("a"), U("1"));
            var e = Assert.ThrowsExactly<RocksDbSharp.RocksDbException>(() =>
            {
                using var b = new RocksDbSharp.WriteBatch();
                b.Put(U("x"), U("1")).DeleteRange(U("c"), 1, U("a"), 1).Put(U("y"), U("1"));
                n.Write(b);
            });
            StringAssert.StartsWith(e.Message, "Invalid argument: end key comes before start key");
        }
        using (RocksDatabase m = RocksDatabase.Open(md.Path, new DbOptions { CreateIfMissing = true }))
        {
            m.Put(U("a"), U("1"));
            var e = Assert.ThrowsExactly<RocksDbException>(() =>
            {
                using WriteBatch b = m.CreateWriteBatch();
                b.Put(U("x"), U("1")).DeleteRange(U("c"), U("a")).Put(U("y"), U("1"));
                m.Write(b);
            });
            Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        }
        Assert.AreEqual("[Put(0,a,1)@1]", WalRecords(nd.Path));
        Assert.AreEqual(WalRecords(nd.Path), WalRecords(md.Path));
        using (RocksDatabase m = RocksDatabase.Open(md.Path, new DbOptions()))
        {
            Assert.AreEqual(1UL, m.GetLatestSequenceNumber());
            m.Put(U("z"), U("1"));
            // The next write reuses the refused batch's first sequence: nothing of that batch may be there.
            Assert.IsNull(m.Get(U("x")));
            Assert.AreEqual(2UL, m.GetLatestSequenceNumber());
        }
    }

    [TestMethod]
    public void RepairWithAStaleWalKeepsTheNewerTableDataLikeNative()
    {
        // Repairer::Run (db/repair.cc): the WALs are converted to tables BEFORE AddTables, which orders all tables of a
        // column family by sequence number (RecoverEpochNumbers on a kMightMissing VersionStorageInfo: NewestFirstBySeqNo).
        // A WAL whose data was flushed long ago (still on disk: kept alive by another column family, not purged yet
        // before a crash, a recycled WAL) therefore ends up older than the tables written after it. The port replayed
        // the WALs after writing the MANIFEST: their table got the newest epoch number and shadowed newer data.
        using var template = new TempDir("a4fr-t");
        string staleWalName;
        byte[] staleWal;
        using (var db = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions().SetCreateIfMissing(true), template.Path))
        {
            var flush = new RocksDbSharp.FlushOptions().SetWaitForFlush(true);
            db.Put(U("k"), U("old"));
            db.Put(U("only-in-wal-and-first-table"), U("1"));
            string wal = RocksDb.Managed.CompatTests.Wal.NativeDbHelper.FindWal(template.Path)!;
            staleWalName = Path.GetFileName(wal);
            staleWal = File.ReadAllBytes(wal);
            db.Flush(flush);
            db.Put(U("k"), U("new"));
            db.Flush(flush);
        }
        Assert.IsTrue(staleWal.Length > 0);
        File.WriteAllBytes(Path.Combine(template.Path, staleWalName), staleWal);
        foreach (string f in Directory.GetFiles(template.Path, "MANIFEST-*")) File.Delete(f);
        File.Delete(Path.Combine(template.Path, "CURRENT"));
        using var nd = new TempDir("a4fr-n");
        using var md = new TempDir("a4fr-m");
        CopyDir(template.Path, nd.Path);
        CopyDir(template.Path, md.Path);

        var o = new RocksDbSharp.DbOptions();
        RocksDbSharp.Native.Instance.rocksdb_repair_db(o.Handle, nd.Path, out IntPtr err);
        Assert.AreEqual(IntPtr.Zero, err);
        string nativeValue;
        using (var db = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions(), nd.Path))
        {
            nativeValue = Encoding.UTF8.GetString(db.Get(U("k")));
            Assert.AreEqual("1", Encoding.UTF8.GetString(db.Get(U("only-in-wal-and-first-table"))));
        }
        Assert.AreEqual("new", nativeValue);

        RocksDatabase.Repair(md.Path, new DbOptions());
        using (RocksDatabase db = RocksDatabase.Open(md.Path, new DbOptions()))
        {
            Assert.AreEqual(nativeValue, Encoding.UTF8.GetString(db.Get(U("k"))!));
            Assert.AreEqual("1", Encoding.UTF8.GetString(db.Get(U("only-in-wal-and-first-table"))!));
        }
        // And native reads the managed repair result the same way.
        using (var db = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions(), md.Path))
        {
            Assert.AreEqual(nativeValue, Encoding.UTF8.GetString(db.Get(U("k"))));
        }
    }

    private static string NativeLevels(RocksDbSharp.RocksDb db)
    {
        var sb = new StringBuilder();
        for (int l = 0; l < 7; l++) sb.Append(db.GetProperty("rocksdb.num-files-at-level" + l)).Append(l < 6 ? "," : "");
        return sb.ToString();
    }

    private static string ManagedLevels(RocksDatabase db)
    {
        var sb = new StringBuilder();
        for (int l = 0; l < 7; l++) sb.Append(db.GetProperty("rocksdb.num-files-at-level" + l)).Append(l < 6 ? "," : "");
        return sb.ToString();
    }

    [TestMethod]
    public void OpenMovesFilesDownForDynamicLevelBytesLikeNative()
    {
        // S25. DBImpl::Recover (db_impl_open.cc, "Trivially move files down the LSM when open with
        // level_compaction_dynamic_level_bytes=true"): a DB built without dynamic level bytes has its data in L1; opened
        // with the option on (the default), the files of the non-empty levels above the last one are moved down at
        // open (L0 stays). The port left them where they were.
        using var template = new TempDir("a4fr-t");
        using (var db = RocksDbSharp.RocksDb.Open(
            new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetLevelCompactionDynamicLevelBytes(false), template.Path))
        {
            var flush = new RocksDbSharp.FlushOptions().SetWaitForFlush(true);
            for (int i = 0; i < 100; i++) db.Put(U("k" + i.ToString("D4")), U("v" + i));
            db.Flush(flush);
            db.CompactRange((byte[]?)null, null);
            Assert.AreEqual("0,1,0,0,0,0,0", NativeLevels(db));
            db.Put(U("z"), U("1"));
            db.Flush(flush);
            Assert.AreEqual("1,1,0,0,0,0,0", NativeLevels(db));
        }
        using var nd = new TempDir("a4fr-n");
        using var md = new TempDir("a4fr-m");
        CopyDir(template.Path, nd.Path);
        CopyDir(template.Path, md.Path);
        string native;
        using (var db = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions(), nd.Path))
        {
            native = NativeLevels(db);
        }
        Assert.AreEqual("1,0,0,0,0,0,1", native);
        using (RocksDatabase db = RocksDatabase.Open(md.Path, new DbOptions()))
        {
            Assert.AreEqual(native, ManagedLevels(db));
            CollectionAssert.AreEqual(U("v7"), db.Get(U("k0007")));
            CollectionAssert.AreEqual(U("1"), db.Get(U("z")));
        }
        // The moved state is in the MANIFEST: native reads it back.
        using (var db = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions(), md.Path))
        {
            Assert.AreEqual(native, NativeLevels(db));
            CollectionAssert.AreEqual(U("v7"), db.Get(U("k0007")));
        }
        // Not with automatic compactions disabled (and not without the option).
        using var md2 = new TempDir("a4fr-m2");
        CopyDir(template.Path, md2.Path);
        using (RocksDatabase db = RocksDatabase.Open(md2.Path, new DbOptions(), new ColumnFamilyOptions { DisableAutoCompactions = true }))
        {
            Assert.AreEqual("1,1,0,0,0,0,0", ManagedLevels(db));
        }
    }
}
