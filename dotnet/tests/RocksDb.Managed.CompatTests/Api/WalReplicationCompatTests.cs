using System.Buffers.Binary;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Db;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>
/// The WAL / replication behaviour a WAL-shipping replication layer builds on (a primary hands out a checkpoint, then
/// streams <c>GetUpdatesSince</c> batches that a replica applies with plain writes so both stay on the same sequence
/// numbers), run on native RocksDB (rocksdb-sharp) and the managed port with the same scenario and compared. The
/// options are those of such a deployment: a separate WAL directory, archiving kept on by a WAL_ttl_seconds no file
/// reaches (WAL_size_limit_MB = 0, so the engine never purges the archive), kSkipAnyCorruptedRecords recovery, a small
/// max_total_wal_size and wal_bytes_per_sync.
/// <list type="bullet">
/// <item>GetUpdatesSince: every start position (inside a batch, on a boundary, 0, latest, beyond latest), over
/// archived and live WALs, after recovery, after archived WALs were deleted behind the engine's back (a start inside a
/// deleted file yields the next file's first batch with an OK status; a run crossing a deleted file ends with
/// NotFound), and after a torn or corrupted WAL was recovered with kSkipAnyCorruptedRecords.</item>
/// <item>DisableFileDeletions nesting holds back WAL archiving; the last EnableFileDeletions archives.</item>
/// <item>A checkpoint (log_size_for_flush = 0) copied flat into a replica directory that is opened with its own empty
/// WAL directory, then caught up from the source's WAL, in all four engine pairings.</item>
/// <item>Applying replicated batches: merged runs of batches, a batch trimmed of its leading entries (a replica that
/// resumes inside a batch), the header sequence being ignored, an unknown column family id.</item>
/// </list>
/// </summary>
[TestClass]
public sealed class WalReplicationCompatTests
{
    private const ulong WalTtlNeverSeconds = 100UL * 365 * 24 * 3600;
    private const string TrackerFamily = "replica-change-tracker";

    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);

    // ---------------------------------------------------------------------------------------------------
    // Engines

    /// <summary>What a GetUpdatesSince iteration produced: the batches, and the status it ended with (null = OK).</summary>
    private sealed record Updates(List<(ulong Seq, byte[] Rep)> Batches, string? Error)
    {
        public override string ToString() =>
            "[" + string.Join(", ", Batches.Select(b => b.Seq + "+" + BinaryPrimitives.ReadInt32LittleEndian(b.Rep.AsSpan(8)))) + "] " + (Error ?? "OK");
    }

    private interface IEngine : IDisposable
    {
        string Name { get; }
        string DbPath { get; }
        string WalPath { get; }
        ulong Latest { get; }
        void Write(byte[] rep);
        void Flush();
        Updates GetUpdates(ulong from);
        void DisableFileDeletions();
        void EnableFileDeletions();
        void Checkpoint(string dir);
        Dictionary<string, string> Contents(string? family);
    }

    public enum Kind { Native, Managed }

    /// <summary>The status kind of a native message, spelled like <see cref="StatusCode"/> ("Invalid argument: ..." -> "InvalidArgument").</summary>
    private static string KindOf(string message)
    {
        int c = message.IndexOf(':');
        string[] words = (c < 0 ? message : message[..c]).Split(' ', StringSplitOptions.RemoveEmptyEntries);
        return string.Concat(words.Select(static w => char.ToUpperInvariant(w[0]) + w[1..]));
    }

    private sealed class NativeEngine : IEngine
    {
        private readonly RocksDbSharp.RocksDb _db;

        public NativeEngine(string db, string wal, bool tracker)
        {
            DbPath = db;
            WalPath = wal;
            var o = new RocksDbSharp.DbOptions()
                .SetCreateIfMissing(true)
                .SetCreateMissingColumnFamilies(true)
                .SetWalDir(wal)
                .SetWalTtlSeconds(WalTtlNeverSeconds)
                .SetWalSizeLimitMB(0)
                .SetWalRecoveryMode(RocksDbSharp.Recovery.SkipAnyCorruptedRecords)
                .SetBytesPerSync(1_048_576)
                .SetMaxTotalWalSize(1024UL * 1024)
                .SetKeepLogFileNum(10)
                .SetDeleteObsoleteFilesPeriodMicros(1_000_000UL * 900);
            RocksDbSharp.Native.Instance.rocksdb_options_set_wal_bytes_per_sync(o.Handle, 1_048_576);
            s_keepAlive.Add(o);
            if (tracker)
            {
                var cfs = new RocksDbSharp.ColumnFamilies { { TrackerFamily, new RocksDbSharp.ColumnFamilyOptions() } };
                s_keepAlive.Add(cfs);
                _db = RocksDbSharp.RocksDb.Open(o, db, cfs);
            }
            else
            {
                _db = RocksDbSharp.RocksDb.Open(o, db);
            }
        }

        public string Name => "native";
        public string DbPath { get; }
        public string WalPath { get; }
        public ulong Latest => _db.GetLatestSequenceNumber();

        public void Write(byte[] rep)
        {
            using var b = new RocksDbSharp.WriteBatch(rep);
            _db.Write(b);
        }

        public void Flush() => _db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));

        public Updates GetUpdates(ulong from)
        {
            var list = new List<(ulong, byte[])>();
            RocksDbSharp.TransactionLogIterator it;
            try
            {
                it = _db.GetUpdatesSince(from);
            }
            catch (RocksDbSharp.RocksDbException e)
            {
                return new Updates(list, KindOf(e.Message));
            }
            using (it)
            {
                for (; it.Valid(); it.Next())
                {
                    using RocksDbSharp.WriteBatch b = it.GetBatch(out ulong seq);
                    byte[] rep = b.ToBytes();
                    Assert.AreEqual(seq, BinaryPrimitives.ReadUInt64LittleEndian(rep), "GetBatch sequence vs batch header");
                    list.Add((seq, rep));
                }
                try
                {
                    it.Status();
                }
                catch (RocksDbSharp.RocksDbException e)
                {
                    return new Updates(list, KindOf(e.Message));
                }
            }
            return new Updates(list, null);
        }

        public void DisableFileDeletions() => _db.DisableFileDeletions();
        public void EnableFileDeletions() => _db.EnableFileDeletions();

        public void Checkpoint(string dir)
        {
            using RocksDbSharp.Checkpoint c = _db.Checkpoint();
            c.Save(dir); // log_size_for_flush = 0
        }

        public Dictionary<string, string> Contents(string? family)
        {
            var d = new Dictionary<string, string>(StringComparer.Ordinal);
            using RocksDbSharp.Iterator it = family is null ? _db.NewIterator() : _db.NewIterator(_db.GetColumnFamily(family));
            for (it.SeekToFirst(); it.Valid(); it.Next()) d[Encoding.UTF8.GetString(it.Key())] = Encoding.UTF8.GetString(it.Value());
            return d;
        }

        public void Dispose() => _db.Dispose();
    }

    private sealed class ManagedEngine : IEngine
    {
        private readonly RocksDatabase _db;

        public ManagedEngine(string db, string wal, bool tracker)
        {
            DbPath = db;
            WalPath = wal;
            var o = new DbOptions
            {
                CreateIfMissing = true,
                CreateMissingColumnFamilies = true,
                WalDir = wal,
                WalTtlSeconds = WalTtlNeverSeconds,
                WalSizeLimitMb = 0,
                WalRecoveryMode = WalRecoveryMode.SkipAnyCorruptedRecords,
                BytesPerSync = 1_048_576,
                WalBytesPerSync = 1_048_576,
                MaxTotalWalSize = 1024UL * 1024,
                KeepLogFileNum = 10,
                DeleteObsoleteFilesPeriodMicros = 1_000_000UL * 900,
            };
            var cfs = new List<ColumnFamilyDescriptor> { new(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions()) };
            if (tracker) cfs.Add(new ColumnFamilyDescriptor(TrackerFamily, new ColumnFamilyOptions()));
            _db = RocksDatabase.Open(db, o, cfs);
            Assert.AreEqual(wal, _db.WalDir);
        }

        public RocksDatabase Database => _db;

        public string Name => "managed";
        public string DbPath { get; }
        public string WalPath { get; }
        public ulong Latest => _db.GetLatestSequenceNumber();

        public void Write(byte[] rep)
        {
            using var b = new WriteBatch(rep);
            _db.Write(b);
        }

        public void Flush() => _db.Flush();

        public Updates GetUpdates(ulong from)
        {
            var list = new List<(ulong, byte[])>();
            TransactionLogIterator it;
            try
            {
                it = _db.GetUpdatesSince(from);
            }
            catch (RocksDbException e)
            {
                return new Updates(list, e.Code.ToString());
            }
            using (it)
            {
                for (; it.Valid; it.Next()) list.Add((it.Current.Sequence, it.Current.Span.ToArray()));
                return new Updates(list, it.Error?.Code.ToString());
            }
        }

        public void DisableFileDeletions() => _db.DisableFileDeletions();
        public void EnableFileDeletions() => _db.EnableFileDeletions();
        public void Checkpoint(string dir) => _db.CreateCheckpoint(dir);

        public Dictionary<string, string> Contents(string? family)
        {
            var d = new Dictionary<string, string>(StringComparer.Ordinal);
            using Iterator it = family is null ? _db.NewIterator() : _db.NewIterator(_db.GetColumnFamily(family), null);
            for (it.SeekToFirst(); it.Valid; it.Next()) d[Encoding.UTF8.GetString(it.Key)] = Encoding.UTF8.GetString(it.Value);
            return d;
        }

        public void Dispose() => _db.Dispose();
    }

    private static IEngine Open(Kind kind, string db, string wal, bool tracker = false)
    {
        // Native create_if_missing creates the last path component only.
        Directory.CreateDirectory(Path.GetDirectoryName(db)!);
        return kind == Kind.Native ? new NativeEngine(db, wal, tracker) : new ManagedEngine(db, wal, tracker);
    }

    private static IEngine Open(Kind kind, string root, bool tracker = false) =>
        Open(kind, Path.Combine(root, "db"), Path.Combine(root, "journal"), tracker);

    // ---------------------------------------------------------------------------------------------------
    // Workload: batch reps built once, written identically to both engines

    private static string K(int i) => "key" + i.ToString("D5");

    /// <summary>Batch <paramref name="i"/> of the workload: 1 to 5 entries (puts, deletes, range deletes, log data).</summary>
    private static byte[] Batch(int i, uint trackerCf = 0)
    {
        using var b = new WriteBatch();
        if (i % 5 == 0) b.PutLogData(U("log-" + i));
        b.Put(U(K(i)), U("v" + i + new string('x', i % 50)));
        if (i % 3 == 0) b.Put(U(K(i + 1)), U("w" + i));
        if (i % 4 == 0) b.Delete(U(K(i / 2)));
        if (i % 7 == 0) b.DeleteRange(U(K(i - 7 < 0 ? 0 : i - 7)), U(K(i - 4 < 0 ? 1 : i - 4)));
        if (trackerCf != 0) b.Put(trackerCf, U("t" + i.ToString("D5")), U("change-" + i));
        return b.Data.ToArray();
    }

    private static void WriteBatches(IEngine e, int from, int to, uint trackerCf = 0)
    {
        for (int i = from; i < to; i++) e.Write(Batch(i, trackerCf));
    }

    private static string[] LogsIn(string dir) =>
        Directory.Exists(dir) ? [.. Directory.GetFiles(dir, "*.log").Select(f => Path.GetFileName(f)).Order(StringComparer.Ordinal)] : [];

    private static void AssertSame(Updates expected, Updates actual, string what)
    {
        Assert.AreEqual(expected.Error, actual.Error, what + ": status (native " + expected + " / managed " + actual + ")");
        Assert.AreEqual(expected.Batches.Count, actual.Batches.Count, what + ": batch count (native " + expected + " / managed " + actual + ")");
        for (int i = 0; i < expected.Batches.Count; i++)
        {
            Assert.AreEqual(expected.Batches[i].Seq, actual.Batches[i].Seq, what + ": seq of batch " + i);
            CollectionAssert.AreEqual(expected.Batches[i].Rep, actual.Batches[i].Rep, what + ": bytes of batch " + i);
        }
    }

    private static void AssertSameContents(Dictionary<string, string> expected, Dictionary<string, string> actual, string what)
    {
        Assert.AreEqual(expected.Count, actual.Count, what + ": key count");
        foreach (KeyValuePair<string, string> kv in expected)
        {
            Assert.IsTrue(actual.TryGetValue(kv.Key, out string? v), what + ": missing " + kv.Key);
            Assert.AreEqual(kv.Value, v, what + ": value of " + kv.Key);
        }
    }

    /// <summary>Runs GetUpdatesSince from every start in 0..latest+2 on both engines and compares.</summary>
    private static void AssertSameFromEveryStart(IEngine native, IEngine managed, string what)
    {
        Assert.AreEqual(native.Latest, managed.Latest, what + ": latest sequence");
        for (ulong s = 0; s <= native.Latest + 2; s++)
        {
            AssertSame(native.GetUpdates(s), managed.GetUpdates(s), what + " from " + s);
        }
    }

    private static ulong LastSeq(Updates u)
    {
        (ulong seq, byte[] rep) = u.Batches[^1];
        return seq + (ulong)BinaryPrimitives.ReadInt32LittleEndian(rep.AsSpan(8)) - 1;
    }

    // ---------------------------------------------------------------------------------------------------
    // GetUpdatesSince

    [TestMethod]
    public void GetUpdatesSinceFromEveryStartOverArchivedAndLiveWals()
    {
        using var dir = new TempDir("walrep-starts");
        using IEngine n = Open(Kind.Native, Path.Combine(dir.Path, "n"));
        using IEngine m = Open(Kind.Managed, Path.Combine(dir.Path, "m"));
        foreach (IEngine e in new[] { n, m })
        {
            WriteBatches(e, 0, 20);
            e.Flush(); // the WAL is archived (WAL_ttl_seconds > 0)
            WriteBatches(e, 20, 40);
            e.Flush();
            WriteBatches(e, 40, 55); // live WAL
        }
        foreach (IEngine e in new[] { n, m })
        {
            Assert.AreEqual(0, LogsIn(e.DbPath).Length, e.Name + ": no WAL in the DB directory");
            Assert.AreEqual(2, LogsIn(Path.Combine(e.WalPath, "archive")).Length, e.Name + ": archived WALs");
            Assert.AreEqual(1, LogsIn(e.WalPath).Length, e.Name + ": live WALs");
        }
        AssertSameFromEveryStart(n, m, "archived + live");

        // Both engines persist these options identically in their OPTIONS files.
        Dictionary<string, string> nOpts = DbOptionsInOptionsFile(n.DbPath), mOpts = DbOptionsInOptionsFile(m.DbPath);
        foreach (string key in new[] { "WAL_ttl_seconds", "WAL_size_limit_MB", "wal_recovery_mode", "wal_bytes_per_sync", "bytes_per_sync",
                     "max_total_wal_size", "keep_log_file_num", "delete_obsolete_files_period_micros" })
        {
            Assert.AreEqual(nOpts[key], mOpts[key], key);
        }
        Assert.AreEqual("3153600000", mOpts["WAL_ttl_seconds"]);
        Assert.AreEqual("kSkipAnyCorruptedRecords", mOpts["wal_recovery_mode"]);
        Assert.AreEqual(n.WalPath, nOpts["wal_dir"]);
        Assert.AreEqual(m.WalPath, mOpts["wal_dir"]);

        // The iterator starts at the batch containing the requested sequence.
        Updates mid = m.GetUpdates(3);
        Assert.IsTrue(mid.Batches[0].Seq <= 3 && mid.Batches[0].Seq + (ulong)BinaryPrimitives.ReadInt32LittleEndian(mid.Batches[0].Rep.AsSpan(8)) > 3);
        Assert.AreEqual("NotFound", m.GetUpdates(m.Latest + 1).Error, "a sequence not written yet");
    }

    [TestMethod]
    public void GetUpdatesSinceAfterRecovery()
    {
        using var dir = new TempDir("walrep-recovery");
        foreach (Kind k in new[] { Kind.Native, Kind.Managed })
        {
            using IEngine e = Open(k, Path.Combine(dir.Path, k.ToString()));
            WriteBatches(e, 0, 15);
            e.Flush();
            WriteBatches(e, 15, 30); // only in the live WAL when closed
        }
        foreach (Kind writer in new[] { Kind.Native, Kind.Managed })
        {
            // Reopen what each engine wrote with both engines.
            string src = Path.Combine(dir.Path, writer.ToString());
            string nRoot = Path.Combine(dir.Path, writer + "-by-native"), mRoot = Path.Combine(dir.Path, writer + "-by-managed");
            CopyDirectory(src, nRoot);
            CopyDirectory(src, mRoot);
            using IEngine n = Open(Kind.Native, nRoot);
            using IEngine m = Open(Kind.Managed, mRoot);
            AssertSameFromEveryStart(n, m, "written by " + writer + ", reopened");
            Updates all = m.GetUpdates(1);
            Assert.IsNull(all.Error, all.ToString());
            Assert.AreEqual(30, all.Batches.Count, "the recovered WAL is archived and still served");
            // New writes continue the sequence and the run stays contiguous across the recovery.
            WriteBatches(n, 30, 35);
            WriteBatches(m, 30, 35);
            AssertSameFromEveryStart(n, m, "written by " + writer + ", written after reopen");
            AssertSameContents(n.Contents(null), m.Contents(null), "written by " + writer);
        }
    }

    [TestMethod]
    public void GetUpdatesSinceAfterArchivedWalsWereDeletedBehindTheEngine()
    {
        using var dir = new TempDir("walrep-deleted");
        using IEngine n = Open(Kind.Native, Path.Combine(dir.Path, "n"));
        using IEngine m = Open(Kind.Managed, Path.Combine(dir.Path, "m"));
        var roundStarts = new List<ulong>();
        foreach (IEngine e in new[] { n, m })
        {
            roundStarts.Clear();
            for (int round = 0; round < 4; round++)
            {
                roundStarts.Add(e.Latest + 1);
                WriteBatches(e, round * 10, round * 10 + 10);
                if (round < 3) e.Flush();
            }
            Assert.AreEqual(3, LogsIn(Path.Combine(e.WalPath, "archive")).Length, e.Name);
        }
        AssertSameFromEveryStart(n, m, "before deleting");
        // The engine's WAL listing reports each archived file's first sequence (what a retention pass deletes by).
        IReadOnlyList<WalFileInfo> sorted = ((ManagedEngine)m).Database.GetSortedWalFiles();
        CollectionAssert.AreEqual(roundStarts, sorted.Select(f => f.StartSequence).ToList());
        CollectionAssert.AreEqual(new[] { WalFileType.ArchivedLogFile, WalFileType.ArchivedLogFile, WalFileType.ArchivedLogFile, WalFileType.AliveLogFile },
            sorted.Select(f => f.Type).ToArray());
        foreach (WalFileInfo f in sorted.Take(3))
        {
            Assert.AreEqual(f.StartSequence, FirstSequenceOf(Path.Combine(m.WalPath, f.PathName.TrimStart('/'))));
        }

        // Delete the second archived WAL (rounds are engine-independent: identified by their first sequence).
        foreach (IEngine e in new[] { n, m }) DeleteArchivedWalStartingAt(e, roundStarts[1]);
        AssertSameFromEveryStart(n, m, "middle archived WAL deleted");
        foreach (IEngine e in new[] { n, m })
        {
            // A start inside the deleted file silently gets the next file's first batch.
            for (ulong s = roundStarts[1]; s < roundStarts[2]; s++)
            {
                Updates u = e.GetUpdates(s);
                Assert.IsNull(u.Error, e.Name + " from " + s + ": " + u);
                Assert.AreEqual(roundStarts[2], u.Batches[0].Seq, e.Name + " from " + s);
                Assert.AreEqual(e.Latest, LastSeq(u), e.Name + " from " + s);
            }
            // A run that starts before the deleted file ends at it with NotFound (gap in sequence numbers).
            Updates before = e.GetUpdates(roundStarts[0]);
            Assert.AreEqual("NotFound", before.Error, e.Name + ": " + before);
            Assert.AreEqual(roundStarts[1] - 1, LastSeq(before), e.Name);
        }

        // Delete the oldest one too: a start before every remaining file gets the oldest remaining batch.
        foreach (IEngine e in new[] { n, m }) DeleteArchivedWalStartingAt(e, roundStarts[0]);
        AssertSameFromEveryStart(n, m, "oldest and middle archived WALs deleted");
        foreach (IEngine e in new[] { n, m })
        {
            Updates u = e.GetUpdates(1);
            Assert.IsNull(u.Error, e.Name + ": " + u);
            Assert.AreEqual(roundStarts[2], u.Batches[0].Seq, e.Name);
        }

        // New writes and a flush (which archives the live WAL again) keep working after the files vanished.
        foreach (IEngine e in new[] { n, m })
        {
            WriteBatches(e, 40, 45);
            e.Flush();
            WriteBatches(e, 45, 50);
        }
        AssertSameFromEveryStart(n, m, "written after the deletions");
    }

    /// <summary>
    /// wal_dir and db_log_dir are persisted in the OPTIONS file (db_options.cc; empty values are left out), so a tool that
    /// opens a DB from its OPTIONS file finds the WAL; and the managed parser reads them from a native OPTIONS file.
    /// </summary>
    [TestMethod]
    public void WalDirAndDbLogDirArePersistedInTheOptionsFile()
    {
        using var dir = new TempDir("walrep-optsdirs");
        string nDb = Path.Combine(dir.Path, "n"), mDb = Path.Combine(dir.Path, "m");
        string nWal = Path.Combine(dir.Path, "n-wal"), mWal = Path.Combine(dir.Path, "m-wal");
        string nLog = Path.Combine(dir.Path, "n-log"), mLog = Path.Combine(dir.Path, "m-log");
        var no = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetWalDir(nWal).SetDbLogDir(nLog);
        s_keepAlive.Add(no);
        using (RocksDbSharp.RocksDb.Open(no, nDb))
        {
        }
        using (RocksDatabase.Open(mDb, new DbOptions { CreateIfMissing = true, WalDir = mWal, DbLogDir = mLog }))
        {
        }
        Dictionary<string, string> nOpts = DbOptionsInOptionsFile(nDb), mOpts = DbOptionsInOptionsFile(mDb);
        Assert.AreEqual(nWal, nOpts["wal_dir"]);
        Assert.AreEqual(nLog, nOpts["db_log_dir"]);
        Assert.AreEqual(mWal, mOpts["wal_dir"]);
        Assert.AreEqual(mLog, mOpts["db_log_dir"]);

        // The managed parser accepts native's file strictly (no unknown options) and reads both directories.
        string nFile = Directory.GetFiles(nDb, "OPTIONS-*").Order(StringComparer.Ordinal).Last();
        OptionsFile parsed = OptionsFile.Parse(File.ReadAllText(nFile), preserveUnknownOptions: false);
        Assert.AreEqual(nWal, parsed.DbOptions.WalDir);
        Assert.AreEqual(nLog, parsed.DbOptions.DbLogDir);

        // Without them set, neither engine writes the keys.
        using (var plain = new TempDir("walrep-optsdirs-plain"))
        {
            using (RocksDatabase.Open(plain.Path, new DbOptions { CreateIfMissing = true }))
            {
            }
            Dictionary<string, string> p = DbOptionsInOptionsFile(plain.Path);
            Assert.IsFalse(p.ContainsKey("wal_dir"));
            Assert.IsFalse(p.ContainsKey("db_log_dir"));
        }
    }

    /// <summary>The [DBOptions] section of the newest OPTIONS file of a DB.</summary>
    private static Dictionary<string, string> DbOptionsInOptionsFile(string dbPath)
    {
        string file = Directory.GetFiles(dbPath, "OPTIONS-*").OrderBy(f => ulong.Parse(Path.GetFileName(f)["OPTIONS-".Length..])).Last();
        var d = new Dictionary<string, string>(StringComparer.Ordinal);
        bool inDbOptions = false;
        foreach (string raw in File.ReadAllLines(file))
        {
            string line = raw.Trim();
            if (line.StartsWith('[')) inDbOptions = line == "[DBOptions]";
            else if (inDbOptions && line.IndexOf('=') is int eq and > 0) d[line[..eq]] = line[(eq + 1)..];
        }
        return d;
    }

    private static void DeleteArchivedWalStartingAt(IEngine e, ulong firstSequence)
    {
        string archive = Path.Combine(e.WalPath, "archive");
        foreach (string f in Directory.GetFiles(archive, "*.log"))
        {
            if (FirstSequenceOf(f) == firstSequence)
            {
                File.Delete(f);
                return;
            }
        }
        Assert.Fail(e.Name + ": no archived WAL starts at " + firstSequence);
    }

    /// <summary>The sequence of the first record of a WAL (first physical record: 7-byte header, then the batch header).</summary>
    private static ulong FirstSequenceOf(string walFile)
    {
        byte[] head = new byte[7 + 8];
        using FileStream s = File.OpenRead(walFile);
        s.ReadExactly(head);
        return BinaryPrimitives.ReadUInt64LittleEndian(head.AsSpan(7));
    }

    [TestMethod]
    public void RecoveryOfATornOrCorruptedWalWithSkipAnyCorruptedRecords()
    {
        using var dir = new TempDir("walrep-torn");
        foreach (Kind writer in new[] { Kind.Native, Kind.Managed })
        {
            string src = Path.Combine(dir.Path, writer.ToString());
            using (IEngine e = Open(writer, src))
            {
                WriteBatches(e, 0, 40); // never flushed: everything is in the live WAL at close
            }
            string wal = Directory.GetFiles(Path.Combine(src, "journal"), "*.log").Single();
            long length = new FileInfo(wal).Length;
            foreach ((string damage, Action<string> apply) in new (string, Action<string>)[]
            {
                ("intact", static _ => { }),
                ("torn tail", f => { using var s = new FileStream(f, FileMode.Open); s.SetLength(length - 5); }),
                ("torn header", f => { using var s = new FileStream(f, FileMode.Open); s.SetLength(length - (length - 7) % 97 - 3); }),
                ("corrupt middle record", f =>
                {
                    using var s = new FileStream(f, FileMode.Open);
                    s.Position = length / 2;
                    int b = s.ReadByte();
                    s.Position = length / 2;
                    s.WriteByte((byte)(b ^ 0x5A));
                }),
            })
            {
                string what = writer + " WAL, " + damage;
                string nRoot = Path.Combine(dir.Path, what.Replace(' ', '_').Replace(',', '_') + "-n");
                string mRoot = Path.Combine(dir.Path, what.Replace(' ', '_').Replace(',', '_') + "-m");
                CopyDirectory(src, nRoot);
                apply(Directory.GetFiles(Path.Combine(nRoot, "journal"), "*.log").Single());
                CopyDirectory(nRoot, mRoot);
                using IEngine n = Open(Kind.Native, nRoot);
                using IEngine m = Open(Kind.Managed, mRoot);
                Assert.AreEqual(n.Latest, m.Latest, what + ": recovered latest sequence");
                AssertSameContents(n.Contents(null), m.Contents(null), what + ": recovered contents");
                AssertSameFromEveryStart(n, m, what);
                WriteBatches(n, 40, 45);
                WriteBatches(m, 40, 45);
                AssertSameFromEveryStart(n, m, what + ", written after recovery");
                AssertSameContents(n.Contents(null), m.Contents(null), what + ": contents after recovery");
            }
        }
    }

    [TestMethod]
    public void MaxTotalWalSizeRollsAndArchivesWalsHeldByAnIdleColumnFamily()
    {
        using var dir = new TempDir("walrep-maxwal");
        using IEngine n = Open(Kind.Native, Path.Combine(dir.Path, "n"), tracker: true);
        using IEngine m = Open(Kind.Managed, Path.Combine(dir.Path, "m"), tracker: true);
        foreach (IEngine e in new[] { n, m })
        {
            using (var b = new WriteBatch())
            {
                b.Put(1, U("t00000"), U("idle family holds the first WAL"));
                e.Write(b.Data.ToArray());
            }
            for (int i = 0; i < 300; i++)
            {
                using var b = new WriteBatch();
                b.Put(U(K(i)), new byte[10_000]);
                e.Write(b.Data.ToArray());
            }
            // The WAL switch schedules background flushes of both families; the old WAL is archived when they finish (no
            // sync point reaches into the native engine, so wait for the file to move, bounded). How many more switches
            // follow depends on when those flushes finish relative to the writes, in both engines.
            var sw = System.Diagnostics.Stopwatch.StartNew();
            while (LogsIn(Path.Combine(e.WalPath, "archive")).Length == 0 && sw.Elapsed < TimeSpan.FromSeconds(20)) Thread.Sleep(10);
            Assert.AreNotEqual(0, LogsIn(Path.Combine(e.WalPath, "archive")).Length,
                e.Name + ": max_total_wal_size (1 MB) switched the WAL, flushed the idle family and archived the old WAL");
            Assert.IsTrue(LogsIn(e.WalPath).Length <= 2, e.Name + ": live WALs");
        }
        AssertSame(n.GetUpdates(1), m.GetUpdates(1), "from 1");
        AssertSame(n.GetUpdates(150), m.GetUpdates(150), "from 150");
    }

    // ---------------------------------------------------------------------------------------------------
    // File deletions

    [TestMethod]
    public void DisabledFileDeletionsNestAndHoldBackWalArchiving()
    {
        using var dir = new TempDir("walrep-deletions");
        var layouts = new Dictionary<Kind, List<string>>();
        foreach (Kind k in new[] { Kind.Native, Kind.Managed })
        {
            var steps = new List<string>();
            using IEngine e = Open(k, Path.Combine(dir.Path, k.ToString()));
            string Layout(string step) => step + ": live " + LogsIn(e.WalPath).Length + ", archived " + LogsIn(Path.Combine(e.WalPath, "archive")).Length;

            WriteBatches(e, 0, 10);
            e.DisableFileDeletions();
            e.DisableFileDeletions();
            e.Flush();
            WriteBatches(e, 10, 20);
            e.Flush();
            WriteBatches(e, 20, 25);
            steps.Add(Layout("disabled twice, two flushes"));
            AssertNoGapFrom1(e, "while disabled");
            e.EnableFileDeletions();
            steps.Add(Layout("enabled once"));
            e.EnableFileDeletions();
            steps.Add(Layout("enabled twice"));
            e.EnableFileDeletions(); // not below zero
            e.DisableFileDeletions();
            e.Flush();
            steps.Add(Layout("enabled thrice, disabled once, flushed"));
            e.EnableFileDeletions();
            steps.Add(Layout("enabled again"));
            AssertNoGapFrom1(e, "after archiving");
            layouts[k] = steps;
        }
        CollectionAssert.AreEqual(layouts[Kind.Native], layouts[Kind.Managed],
            "native: " + string.Join(" | ", layouts[Kind.Native]) + "\nmanaged: " + string.Join(" | ", layouts[Kind.Managed]));
        Assert.AreEqual("disabled twice, two flushes: live 3, archived 0", layouts[Kind.Managed][0]);
        Assert.AreEqual("enabled twice: live 1, archived 2", layouts[Kind.Managed][2]);
    }

    private static void AssertNoGapFrom1(IEngine e, string what)
    {
        Updates u = e.GetUpdates(1);
        Assert.IsNull(u.Error, e.Name + " " + what + ": " + u);
        Assert.AreEqual(1UL, u.Batches[0].Seq, e.Name + " " + what);
        Assert.AreEqual(e.Latest, LastSeq(u), e.Name + " " + what);
    }

    // ---------------------------------------------------------------------------------------------------
    // Checkpoint -> replica -> catch up from the WAL

    [TestMethod]
    [DataRow(Kind.Native, Kind.Native)]
    [DataRow(Kind.Native, Kind.Managed)]
    [DataRow(Kind.Managed, Kind.Native)]
    [DataRow(Kind.Managed, Kind.Managed)]
    public void CheckpointCopiedFlatIntoAReplicaThenCaughtUpFromTheWal(Kind sourceKind, Kind replicaKind)
    {
        using var dir = new TempDir("walrep-cp");
        using IEngine source = Open(sourceKind, Path.Combine(dir.Path, "source"), tracker: true);
        const uint trackerCf = 1;
        WriteBatches(source, 0, 30, trackerCf);
        source.Flush();
        WriteBatches(source, 30, 60, trackerCf);
        source.DisableFileDeletions(); // a replica is registered
        ulong atCheckpoint = source.Latest;
        string cp = Path.Combine(dir.Path, "cp");
        source.Checkpoint(cp);
        WriteBatches(source, 60, 90, trackerCf); // after the checkpoint

        // The replica copies the checkpoint's files into its DB directory and opens it with its own (empty) WAL directory.
        string replicaDb = Path.Combine(dir.Path, "replica", "db");
        Directory.CreateDirectory(replicaDb);
        foreach (string f in Directory.GetFiles(cp)) File.Copy(f, Path.Combine(replicaDb, Path.GetFileName(f)));
        using IEngine replica = Open(replicaKind, replicaDb, Path.Combine(dir.Path, "replica", "journal"), tracker: true);
        Assert.AreEqual(atCheckpoint, replica.Latest, "the replica starts at the checkpoint's sequence");

        CatchUp(source, replica);
        Assert.AreEqual(source.Latest, replica.Latest);
        AssertSameContents(source.Contents(null), replica.Contents(null), "default");
        AssertSameContents(source.Contents(TrackerFamily), replica.Contents(TrackerFamily), TrackerFamily);

        // Keeps following.
        WriteBatches(source, 90, 100, trackerCf);
        source.Flush();
        WriteBatches(source, 100, 110, trackerCf);
        CatchUp(source, replica);
        Assert.AreEqual(source.Latest, replica.Latest);
        AssertSameContents(source.Contents(TrackerFamily), replica.Contents(TrackerFamily), TrackerFamily + " later");
        source.EnableFileDeletions();
    }

    /// <summary>
    /// Streams the source's WAL from the replica's latest + 1 and applies it as merged runs of batches (one write per
    /// run: a single header with the summed count followed by the batches' records), like a WAL tailer.
    /// </summary>
    private static void CatchUp(IEngine source, IEngine replica)
    {
        ulong next = replica.Latest + 1;
        Updates u = source.GetUpdates(next);
        Assert.IsNull(u.Error, u.ToString());
        var chunk = new List<byte>();
        int count = 0;
        ulong first = 0;
        void Apply()
        {
            if (count == 0) return;
            byte[] rep = new byte[12 + chunk.Count];
            BinaryPrimitives.WriteUInt64LittleEndian(rep, first);
            BinaryPrimitives.WriteInt32LittleEndian(rep.AsSpan(8), count);
            chunk.CopyTo(rep, 12);
            Assert.AreEqual(replica.Latest + 1, first, "chunk starts at the replica's next sequence");
            replica.Write(rep);
            Assert.AreEqual(first + (ulong)count - 1, replica.Latest, "the replica advances by the chunk's count");
            chunk.Clear();
            count = 0;
        }
        foreach ((ulong seq, byte[] rep) in u.Batches)
        {
            int c = BinaryPrimitives.ReadInt32LittleEndian(rep.AsSpan(8));
            Assert.AreEqual(next, seq, "the stream starts on a batch boundary here and is contiguous");
            if (count == 0) first = seq;
            chunk.AddRange(rep.AsSpan(12).ToArray());
            count += c;
            next = seq + (ulong)c;
            if (chunk.Count > 2048) Apply();
        }
        Apply();
    }

    // ---------------------------------------------------------------------------------------------------
    // Applying replicated batches

    [TestMethod]
    [DataRow(Kind.Native, Kind.Native)]
    [DataRow(Kind.Native, Kind.Managed)]
    [DataRow(Kind.Managed, Kind.Native)]
    [DataRow(Kind.Managed, Kind.Managed)]
    public void ResumingInsideABatchAppliesTheBatchTrimmedOfTheEntriesTheReplicaHas(Kind sourceKind, Kind replicaKind)
    {
        using var dir = new TempDir("walrep-trim");
        using IEngine source = Open(sourceKind, Path.Combine(dir.Path, "source"));
        using IEngine replica = Open(replicaKind, Path.Combine(dir.Path, "replica"));
        using (var b = new WriteBatch())
        {
            b.PutLogData(U("not a write"));
            b.Put(U("a"), U("1"));
            b.Put(U("b"), U("2"));
            b.Delete(U("a"));
            b.DeleteRange(U("x0"), U("x9"));
            b.Put(U("c"), U("3"));
            source.Write(b.Data.ToArray());
        }
        Assert.AreEqual(5UL, source.Latest, "log data consumes no sequence number");

        // The replica got the first two writes of that batch from the node it followed before.
        using (var first = new WriteBatch())
        {
            first.Put(U("a"), U("1"));
            first.Put(U("b"), U("2"));
            replica.Write(first.Data.ToArray());
        }
        ulong expected = replica.Latest + 1;
        Updates u = source.GetUpdates(expected);
        Assert.IsNull(u.Error);
        Assert.AreEqual(1, u.Batches.Count);
        Assert.AreEqual(1UL, u.Batches[0].Seq, "the iterator starts at the batch holding the sequence number asked for");

        byte[] trimmed = TrimLeadingEntries(u.Batches[0].Rep, (int)(expected - u.Batches[0].Seq));
        Assert.AreEqual(expected, BinaryPrimitives.ReadUInt64LittleEndian(trimmed));
        Assert.AreEqual(3, BinaryPrimitives.ReadInt32LittleEndian(trimmed.AsSpan(8)));
        replica.Write(trimmed);

        Assert.AreEqual(source.Latest, replica.Latest);
        Dictionary<string, string> c = replica.Contents(null);
        Assert.IsFalse(c.ContainsKey("a"));
        Assert.AreEqual("2", c["b"]);
        Assert.AreEqual("3", c["c"]);
        AssertSameContents(source.Contents(null), c, "replica");
    }

    /// <summary>A rep without its first <paramref name="skip"/> sequence-consuming records (and the uncounted ones before them).</summary>
    private static byte[] TrimLeadingEntries(byte[] rep, int skip)
    {
        var reader = new WriteBatchReader(rep);
        int skipped = 0;
        while (skipped < skip)
        {
            Assert.IsTrue(reader.TryReadNext(out WriteBatchEntry e));
            if (e.IsCounted) skipped++;
        }
        int rest = reader.RemainingBytes;
        int kept = (int)reader.Count - skip;
        while (reader.TryReadNext(out _))
        {
        }
        byte[] result = new byte[12 + rest];
        BinaryPrimitives.WriteUInt64LittleEndian(result, BinaryPrimitives.ReadUInt64LittleEndian(rep) + (ulong)skip);
        BinaryPrimitives.WriteInt32LittleEndian(result.AsSpan(8), kept);
        rep.AsSpan(rep.Length - rest).CopyTo(result.AsSpan(12));
        return result;
    }

    [TestMethod]
    [DataRow(Kind.Native, Kind.Native)]
    [DataRow(Kind.Native, Kind.Managed)]
    [DataRow(Kind.Managed, Kind.Native)]
    [DataRow(Kind.Managed, Kind.Managed)]
    public void TailingIntoAReplicaKeepsBothOnTheSameSequenceNumbers(Kind sourceKind, Kind replicaKind)
    {
        using var dir = new TempDir("walrep-tail");
        using IEngine source = Open(sourceKind, Path.Combine(dir.Path, "source"));
        using IEngine replica = Open(replicaKind, Path.Combine(dir.Path, "replica"));
        source.DisableFileDeletions();
        for (int i = 0; i < 500; i++)
        {
            using var b = new WriteBatch();
            b.Put(U("key_" + i.ToString("D4")), U("value_" + i));
            source.Write(b.Data.ToArray());
        }
        // A multi-entry batch consumes one sequence number per entry.
        using (var b = new WriteBatch())
        {
            b.Put(U("a"), U("1"));
            b.Put(U("b"), U("2"));
            b.Put(U("c"), U("3"));
            source.Write(b.Data.ToArray());
        }
        using (var b = new WriteBatch())
        {
            b.Put(U("removed"), U("yes"));
            b.Delete(U("removed"));
            source.Write(b.Data.ToArray());
        }
        CatchUp(source, replica);
        Assert.AreEqual(source.Latest, replica.Latest);
        AssertSameContents(source.Contents(null), replica.Contents(null), "after catching up");

        // Nothing newer: asking for latest + 1 is NotFound on both engines (a tailer must not ask).
        Assert.AreEqual("NotFound", source.GetUpdates(source.Latest + 1).Error);

        for (int i = 500; i < 515; i++)
        {
            using var b = new WriteBatch();
            b.Put(U("key_" + i.ToString("D4")), U("second"));
            source.Write(b.Data.ToArray());
        }
        CatchUp(source, replica);
        Assert.AreEqual(source.Latest, replica.Latest);
        AssertSameContents(source.Contents(null), replica.Contents(null), "after resuming");
    }

    [TestMethod]
    public void AppliedBatchIgnoresItsHeaderSequenceAndRejectsAnUnknownColumnFamily()
    {
        using var dir = new TempDir("walrep-apply");
        var outcomes = new Dictionary<Kind, string>();
        foreach (Kind k in new[] { Kind.Native, Kind.Managed })
        {
            using IEngine e = Open(k, Path.Combine(dir.Path, k.ToString()));
            WriteBatches(e, 0, 3);
            ulong before = e.Latest;
            byte[] rep;
            using (var b = new WriteBatch())
            {
                b.Put(U("x"), U("1"));
                b.Put(U("y"), U("2"));
                rep = b.Data.ToArray();
            }
            BinaryPrimitives.WriteUInt64LittleEndian(rep, 999_999);
            e.Write(rep);
            string result = "advanced " + (e.Latest - before);
            using (var b = new WriteBatch())
            {
                b.Put(U("z"), U("3"));
                b.Put(7, U("k"), U("v"));
                rep = b.Data.ToArray();
            }
            try
            {
                e.Write(rep);
                result += ", unknown family accepted";
            }
            catch (RocksDbSharp.RocksDbException ex)
            {
                result += ", unknown family " + KindOf(ex.Message);
            }
            catch (RocksDbException ex)
            {
                result += ", unknown family " + ex.Code;
            }
            result += ", then latest +" + (e.Latest - before) + ", z " + (e.Contents(null).ContainsKey("z") ? "written" : "not written");
            outcomes[k] = result;
        }
        Assert.AreEqual(outcomes[Kind.Native], outcomes[Kind.Managed]);
        Assert.AreEqual("advanced 2, unknown family InvalidArgument, then latest +2, z not written", outcomes[Kind.Managed]);
    }

    // ---------------------------------------------------------------------------------------------------

    private static void CopyDirectory(string from, string to)
    {
        Directory.CreateDirectory(to);
        foreach (string f in Directory.GetFiles(from))
        {
            string name = Path.GetFileName(f);
            if (name == "LOCK") continue;
            File.Copy(f, Path.Combine(to, name));
        }
        foreach (string d in Directory.GetDirectories(from)) CopyDirectory(d, Path.Combine(to, Path.GetFileName(d)));
    }
}
