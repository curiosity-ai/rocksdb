using System.Globalization;
using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Tests.Db;
using RocksDb.Managed.Util;
using RocksDb.Managed.Wal;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #7, area "MetadataCorruption": untrusted bytes in the WAL, WriteBatch, MANIFEST / VersionEdit, CURRENT,
/// IDENTITY and OPTIONS files, and DB open / recovery over them. Seeded mutation fuzzing (seed logged) plus targeted
/// cases. Only <see cref="RocksDbException"/> may escape; a failed open must leave the directory as it was, and a
/// successful open must recover what the recovery mode promises and reopen to the same data.
/// </summary>
[TestClass]
public sealed class Audit7MetadataCorruptionTests
{
    public TestContext TestContext { get; set; } = null!;

    private static readonly TimeSpan HangTimeout = TimeSpan.FromSeconds(30);

    private static int Seed()
    {
        string? env = Environment.GetEnvironmentVariable("AUDIT7_SEED");
        return env is not null ? int.Parse(env, CultureInfo.InvariantCulture) : Environment.TickCount & 0x7fffffff;
    }

    /// <summary>Runs <paramref name="action"/> on the thread pool and fails on a hang.</summary>
    private static void RunWithTimeout(Action action, string what)
    {
        Task t = Task.Run(action);
        if (!t.Wait(HangTimeout)) Assert.Fail("hang: " + what);
    }

    /// <summary>Asserts that <paramref name="e"/> is a RocksDbException (the only failure allowed at the public boundary).</summary>
    private static void AssertRocksDbFailure(Exception e, string what)
    {
        if (e is AggregateException { InnerExceptions.Count: 1 } ae) e = ae.InnerException!;
        if (e is not RocksDbException) Assert.Fail(what + ": unexpected " + e.GetType().Name + ": " + e);
    }

    // ---- mutation helpers ----------------------------------------------------------------------------------------

    private static byte[] Mutate(Random rnd, byte[] src)
    {
        byte[] b = (byte[])src.Clone();
        switch (rnd.Next(7))
        {
            case 0: // flip one bit
                if (b.Length > 0) b[rnd.Next(b.Length)] ^= (byte)(1 << rnd.Next(8));
                return b;
            case 1: // random byte(s)
                for (int i = 0, n = 1 + rnd.Next(3); i < n && b.Length > 0; i++) b[rnd.Next(b.Length)] = (byte)rnd.Next(256);
                return b;
            case 2: // truncate
                return b.AsSpan(0, b.Length == 0 ? 0 : rnd.Next(b.Length)).ToArray();
            case 3: // zero a range
            {
                if (b.Length == 0) return b;
                int at = rnd.Next(b.Length);
                b.AsSpan(at, Math.Min(b.Length - at, 1 + rnd.Next(64))).Clear();
                return b;
            }
            case 4: // append garbage or zeros (preallocated tail)
            {
                byte[] tail = new byte[1 + rnd.Next(100)];
                if (rnd.Next(2) == 0) rnd.NextBytes(tail);
                return [.. b, .. tail];
            }
            case 5: // varint-ish large value
            {
                if (b.Length == 0) return b;
                int at = rnd.Next(b.Length);
                for (int i = at; i < Math.Min(b.Length, at + 5); i++) b[i] = i == at + 4 ? (byte)0x0f : (byte)0xff;
                return b;
            }
            default: // delete a range
            {
                if (b.Length == 0) return b;
                int at = rnd.Next(b.Length);
                int n = Math.Min(b.Length - at, 1 + rnd.Next(16));
                return [.. b.AsSpan(0, at), .. b.AsSpan(at + n)];
            }
        }
    }

    // ---- WriteBatch from untrusted bytes ------------------------------------------------------------------------

    private struct CountingHandler : IWriteBatchHandler
    {
        public int Records;
        public bool Continue() => true;
        public void Put(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => Records++;
        public void TimedPut(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ulong writeUnixTime) => Records++;
        public void PutEntity(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> entity) => Records++;
        public void Delete(uint columnFamilyId, ReadOnlySpan<byte> key) => Records++;
        public void SingleDelete(uint columnFamilyId, ReadOnlySpan<byte> key) => Records++;
        public void DeleteRange(uint columnFamilyId, ReadOnlySpan<byte> beginKey, ReadOnlySpan<byte> endKey) => Records++;
        public void Merge(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => Records++;
        public void PutBlobIndex(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> blobIndex) => Records++;
        public void LogData(ReadOnlySpan<byte> blob) => Records++;
        public void MarkBeginPrepare(bool unprepare) => Records++;
        public void MarkEndPrepare(ReadOnlySpan<byte> xid) => Records++;
        public void MarkNoop(bool emptyBatch) => Records++;
        public void MarkCommit(ReadOnlySpan<byte> xid) => Records++;
        public void MarkCommitWithTimestamp(ReadOnlySpan<byte> xid, ReadOnlySpan<byte> commitTimestamp) => Records++;
        public void MarkRollback(ReadOnlySpan<byte> xid) => Records++;
    }

    private static byte[] RichBatch(uint cf)
    {
        using var b = new WriteBatch();
        b.Put(B("k1"), B("v1"));
        b.Put(cf, B("k2"), B("v2"));
        b.Delete(B("k3"));
        b.Delete(cf, B("k4"));
        b.SingleDelete(B("k5"));
        b.SingleDelete(cf, B("k6"));
        b.DeleteRange(B("r1"), B("r2"));
        b.DeleteRange(cf, B("r3"), B("r4"));
        b.Merge(B("m1"), B("x"));
        b.Merge(cf, B("m2"), B("y"));
        b.PutEntity(B("e1"), [new WideColumn(B("c1"), B("cv1")), new WideColumn(B("c2"), B("cv2"))]);
        b.TimedPut(cf, B("t1"), B("tv1"), 12345);
        b.PutLogData(B("blob"));
        return b.Data.ToArray();
    }

    /// <summary>
    /// WriteBatchInternal::Iterate / ReadRecordFromWriteBatch (db/write_batch.cc) over mutated reps: every malformed
    /// input is Corruption ("malformed WriteBatch (too small)", "bad WriteBatch ...", "unknown WriteBatch tag",
    /// "WriteBatch has wrong count"); the content-flag accessors never throw (BatchContentClassifier ignores errors).
    /// </summary>
    [TestMethod]
    public void WriteBatchMutationFuzzOnlyCorruptionEscapes()
    {
        int seed = Seed();
        TestContext.WriteLine("seed=" + seed);
        var rnd = new Random(seed);
        byte[] baseRep = RichBatch(1);
        var sw = System.Diagnostics.Stopwatch.StartNew();
        for (int iter = 0; iter < 20000 && sw.Elapsed < TimeSpan.FromSeconds(5); iter++)
        {
            byte[] rep = Mutate(rnd, baseRep);
            if (rnd.Next(4) == 0) rep = Mutate(rnd, rep);
            string what = "seed=" + seed + " iter=" + iter;
            try
            {
                var h = new CountingHandler();
                WriteBatch.Iterate(rep, ref h);
            }
            catch (Exception e)
            {
                AssertRocksDbFailure(e, what + " Iterate");
                Assert.AreEqual(StatusCode.Corruption, ((RocksDbException)e).Code, what);
            }
            try
            {
                foreach (WriteBatchEntry _ in new WriteBatchReader(rep)) { }
            }
            catch (Exception e)
            {
                AssertRocksDbFailure(e, what + " reader");
                Assert.AreEqual(StatusCode.Corruption, ((RocksDbException)e).Code, what);
            }
            if (rep.Length < WriteBatch.HeaderSize) continue;
            using var wb = new WriteBatch(rep);
            try
            {
                _ = wb.HasPut | wb.HasDelete | wb.HasMerge | wb.HasDeleteRange | wb.HasSingleDelete | wb.HasPutEntity | wb.HasTimedPut |
                    wb.HasBeginPrepare | wb.HasCommit | wb.HasRollback | wb.HasEndPrepare;
                _ = wb.Count;
            }
            catch (Exception e)
            {
                Assert.Fail(what + " content flags threw " + e);
            }
        }
    }

    /// <summary>
    /// DBImpl::WriteImpl with a malformed batch (WriteBatchInternal::InsertInto fails): only RocksDbException escapes,
    /// and none of the batch is visible ("Don't publish a partial batch write").
    /// </summary>
    [TestMethod]
    public void WriteOfMutatedBatchFailsWithoutPublishingPartialBatch()
    {
        int seed = Seed();
        TestContext.WriteLine("seed=" + seed);
        var rnd = new Random(seed);
        byte[] baseRep;
        using (var b = new WriteBatch())
        {
            for (int i = 0; i < 6; i++) b.Put(B("key" + i), B("value" + i));
            b.Delete(B("gone"));
            b.Merge(B("merged"), B("m"));
            baseRep = b.Data.ToArray();
        }
        MemoryFileSystem fs = NewFs();
        DbOptions opts = NewOptions(fs);
        var cfo = new ColumnFamilyOptions { MergeOperator = MergeOperator.StringAppend(",") };
        RocksDatabase db = RocksDatabase.Open(DbPath, opts, cfo);
        try
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            for (int iter = 0; iter < 400 && sw.Elapsed < TimeSpan.FromSeconds(8); iter++)
            {
                byte[] rep = Mutate(rnd, baseRep);
                if (rep.Length < WriteBatch.HeaderSize) continue;
                Coding.EncodeFixed64(rep, 0); // the DB assigns the sequence
                string what = "seed=" + seed + " iter=" + iter;
                using var wb = new WriteBatch(rep);
                try
                {
                    db.Write(wb);
                }
                catch (Exception e)
                {
                    AssertRocksDbFailure(e, what + " Write");
                    for (int i = 0; i < 6; i++) Assert.IsNull(db.Get(B("key" + i)), what + ": partial batch visible");
                    Assert.IsNull(db.Get(B("merged")), what + ": partial batch visible");
                    // A failed memtable insert stops the DB (background error); start over.
                    db.Dispose();
                    fs = NewFs();
                    opts = NewOptions(fs);
                    db = RocksDatabase.Open(DbPath, opts, cfo);
                    continue;
                }
                // Applied: clean up for the next iteration.
                using var del = new WriteBatch();
                del.DeleteRange(B(""), B("zzzzzzzz"));
                db.Write(del);
            }
        }
        finally
        {
            db.Dispose();
        }
    }

    // ---- VersionEdit / MANIFEST ------------------------------------------------------------------------------------

    private static byte[] RichEdit()
    {
        var e = new VersionEdit
        {
            DbId = "db-id",
            ComparatorName = "leveldb.BytewiseComparator",
            PersistUserDefinedTimestamps = true,
            LogNumber = 7,
            PrevLogNumber = 0,
            NextFileNumber = 20,
            MaxColumnFamily = 1,
            MinLogNumberToKeep = 5,
            LastSequence = 100,
            ColumnFamily = 0,
            FullHistoryTsLow = null,
        };
        FileMetaData f = VersionEditTests.File(9, 0, 1234, VersionEditTests.IKey("a", 5, EntryType.Value),
            VersionEditTests.IKey("z", 9, EntryType.Value), 5, 9, 3);
        f.FileChecksum = [1, 2, 3, 4];
        f.FileChecksumFuncName = "crc32c";
        f.OldestBlobFileNumber = 11;
        e.AddFile(1, f);
        e.AddFile(0, VersionEditTests.File(10, 0, 99, VersionEditTests.IKey("b", 10, EntryType.Value),
            VersionEditTests.IKey("c", 12, EntryType.Deletion), 10, 12, 4));
        e.DeleteFile(2, 4);
        e.BlobFileAdditions.Add(new BlobFileAddition { BlobFileNumber = 11, TotalBlobCount = 10, TotalBlobBytes = 1000, ChecksumMethod = "", ChecksumValue = [] });
        e.BlobFileGarbages.Add(new BlobFileGarbage(11, 1, 50));
        e.CompactCursors.Add(new CompactCursor(1, VersionEditTests.IKey("m", 3, EntryType.Value)));
        return e.Encode(0);
    }

    private static VersionEdit FirstEdit() =>
        new() { ComparatorName = "leveldb.BytewiseComparator", PersistUserDefinedTimestamps = true, LogNumber = 0, NextFileNumber = 2, LastSequence = 0 };

    /// <summary>
    /// VersionEdit::DecodeFrom (db/version_edit.cc) over mutated encodings, then the decoded edit replayed like
    /// VersionEditHandler::ApplyVersionEdit: only RocksDbException escapes, and nothing hangs or exhausts memory.
    /// </summary>
    [TestMethod]
    public void VersionEditMutationFuzzOnlyRocksDbExceptionEscapes()
    {
        int seed = Seed();
        TestContext.WriteLine("seed=" + seed);
        var rnd = new Random(seed);
        byte[] baseEdit = RichEdit();
        RunWithTimeout(() =>
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            for (int iter = 0; iter < 20000 && sw.Elapsed < TimeSpan.FromSeconds(5); iter++)
            {
                byte[] enc = Mutate(rnd, baseEdit);
                if (rnd.Next(3) == 0) enc = Mutate(rnd, enc);
                string what = "seed=" + seed + " iter=" + iter;
                VersionEdit edit;
                try
                {
                    edit = VersionEdit.DecodeFrom(enc);
                }
                catch (Exception e)
                {
                    AssertRocksDbFailure(e, what + " DecodeFrom");
                    Assert.AreEqual(StatusCode.Corruption, ((RocksDbException)e).Code, what);
                    continue;
                }
                try
                {
                    var r = new ManifestReplayer(null);
                    r.Apply(FirstEdit());
                    r.Apply(edit);
                    r.Finish();
                }
                catch (Exception e)
                {
                    AssertRocksDbFailure(e, what + " replay");
                }
            }
        }, "seed=" + seed);
    }

    // ---- OPTIONS file --------------------------------------------------------------------------------------------

    private static string SampleOptionsText()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o))
        {
            db.CreateColumnFamily("cf1", new ColumnFamilyOptions());
        }
        string? path = OptionsFile.FindLatestOptionsFile(fs, DbPath);
        Assert.IsNotNull(path);
        return Encoding.UTF8.GetString(fs.ReadAllBytes(path));
    }

    /// <summary>
    /// RocksDBOptionsParser::Parse (options/options_parser.cc) over mutated OPTIONS text: malformed sections,
    /// statements, values and version numbers are InvalidArgument / Corruption; nothing else escapes.
    /// </summary>
    [TestMethod]
    public void OptionsFileMutationFuzzOnlyRocksDbExceptionEscapes()
    {
        int seed = Seed();
        TestContext.WriteLine("seed=" + seed);
        var rnd = new Random(seed);
        byte[] text = Encoding.UTF8.GetBytes(SampleOptionsText());
        var sw = System.Diagnostics.Stopwatch.StartNew();
        for (int iter = 0; iter < 5000 && sw.Elapsed < TimeSpan.FromSeconds(5); iter++)
        {
            byte[] m = Mutate(rnd, text);
            if (rnd.Next(2) == 0)
            {
                // Replace a value with a hostile one.
                string s = Encoding.UTF8.GetString(m);
                string[] hostile = ["99999999999999999999999", "-1", "1.5e400", "kNoSuchEnum", "", "{", "}", "0x", "true;", "9223372036854775808", "18446744073709551616", "1.2.3.4", "99999999999.1"];
                int eq = s.IndexOf('=', rnd.Next(Math.Max(1, s.Length)));
                if (eq > 0)
                {
                    int nl = s.IndexOf('\n', eq);
                    if (nl < 0) nl = s.Length;
                    s = s[..(eq + 1)] + hostile[rnd.Next(hostile.Length)] + s[nl..];
                    m = Encoding.UTF8.GetBytes(s);
                }
            }
            string what = "seed=" + seed + " iter=" + iter;
            foreach (bool preserve in new[] { false, true })
            {
                try
                {
                    OptionsFile.Parse(Encoding.UTF8.GetString(m), preserve);
                }
                catch (Exception e)
                {
                    AssertRocksDbFailure(e, what + " preserve=" + preserve);
                }
            }
        }
    }

    // ---- WAL reader ---------------------------------------------------------------------------------------------

    private sealed class CollectingReporter : LogReporter
    {
        public int Corruptions;
        public override void Corruption(long bytes, StatusCode code, string reason, ulong logNumber) => Corruptions++;
    }

    private static byte[] BuildWal(bool recycle, ulong logNumber, int records, int seed)
    {
        var fs = new MemoryFileSystem();
        fs.CreateDirectoryIfMissing("/w");
        var rnd = new Random(seed);
        using (var w = new LogWriter(fs.CreateWritable("/w/1.log"), logNumber, recycle))
        {
            for (int i = 0; i < records; i++)
            {
                byte[] r = new byte[rnd.Next(4) == 0 ? 20000 + rnd.Next(50000) : 1 + rnd.Next(300)];
                rnd.NextBytes(r);
                w.AddRecord(r);
            }
        }
        return fs.ReadAllBytes("/w/1.log");
    }

    /// <summary>
    /// log::Reader::ReadRecord / ReadPhysicalRecord (db/log_reader.cc) over mutated logs in every recovery mode, legacy
    /// and recyclable formats: only reporter calls, never an exception, and the read terminates.
    /// </summary>
    [TestMethod]
    public void LogReaderMutationFuzzReportsOnly()
    {
        int seed = Seed();
        TestContext.WriteLine("seed=" + seed);
        var rnd = new Random(seed);
        byte[][] bases = [BuildWal(false, 5, 12, seed), BuildWal(true, 5, 12, seed + 1)];
        RunWithTimeout(() =>
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            for (int iter = 0; iter < 3000 && sw.Elapsed < TimeSpan.FromSeconds(6); iter++)
            {
                byte[] wal = Mutate(rnd, bases[iter & 1]);
                ulong logNumber = rnd.Next(4) == 0 ? 6UL : 5UL;
                var mode = (WalRecoveryMode)rnd.Next(4);
                var fs = new MemoryFileSystem();
                fs.CreateDirectoryIfMissing("/w");
                using (IWritableFile f = fs.CreateWritable("/w/5.log")) f.Append(wal);
                var rep = new CollectingReporter();
                using var reader = new LogReader(fs.OpenSequential("/w/5.log"), rep, checksum: true, logNumber) { RecoveryMode = mode };
                int n = 0;
                try
                {
                    while (reader.TryReadRecord(out _)) Assert.IsTrue(++n < 1000, "seed=" + seed + " iter=" + iter + ": too many records");
                }
                catch (Exception e)
                {
                    Assert.Fail("seed=" + seed + " iter=" + iter + " mode=" + mode + ": " + e);
                }
            }
        }, "seed=" + seed);
    }

    // ---- DB open over corrupted metadata files ------------------------------------------------------------------

    private const int KeysPerPhase = 8;

    private static string K(int i) => "key" + i.ToString("D3", CultureInfo.InvariantCulture);
    private static string V(int i) => "value" + i.ToString(CultureInfo.InvariantCulture);

    /// <summary>
    /// A DB whose keys 0..N-1 were written one per batch, in order: phase 1 flushed to an SST, phase 2 in a WAL that
    /// stays alive (avoid_flush_during_recovery on reopen), phase 3 in a second WAL.
    /// </summary>
    private static MemoryFileSystem BuildDb(bool recycle, bool walZstd, out int totalKeys)
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.AvoidFlushDuringRecovery = true;
        if (walZstd) o.WalCompression = RocksDb.Managed.Compression.CompressionType.Zstd;
        if (recycle)
        {
            o.RecycleLogFileNum = 2;
            o.WalRecoveryMode = WalRecoveryMode.PointInTimeRecovery;
        }
        int k = 0;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o))
        {
            for (int i = 0; i < KeysPerPhase; i++, k++) db.Put(B(K(k)), B(V(k)));
            db.Flush();
            for (int i = 0; i < KeysPerPhase; i++, k++) db.Put(B(K(k)), B(V(k)));
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o))
        {
            for (int i = 0; i < KeysPerPhase; i++, k++) db.Put(B(K(k)), B(V(k)));
        }
        totalKeys = k;
        return fs;
    }

    private static Dictionary<string, byte[]> Snapshot(MemoryFileSystem fs, bool includeLogs)
    {
        var d = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        foreach (string child in fs.GetChildren(DbPath))
        {
            if (!includeLogs && child.StartsWith("LOG", StringComparison.Ordinal)) continue;
            string p = DbPath + "/" + child;
            if (fs.DirectoryExists(p)) continue;
            d[child] = fs.ReadAllBytes(p);
        }
        return d;
    }

    private static MemoryFileSystem Restore(Dictionary<string, byte[]> files)
    {
        MemoryFileSystem fs = NewFs();
        foreach ((string name, byte[] data) in files)
        {
            using IWritableFile f = fs.CreateWritable(DbPath + "/" + name);
            f.Append(data);
            f.Sync();
        }
        fs.SyncDirectory(DbPath);
        return fs;
    }

    private static string DescribeDiff(Dictionary<string, byte[]> a, Dictionary<string, byte[]> b)
    {
        var sb = new StringBuilder();
        foreach (string n in a.Keys) if (!b.ContainsKey(n)) sb.Append(" removed ").Append(n);
        foreach (string n in b.Keys) if (!a.ContainsKey(n)) sb.Append(" added ").Append(n);
        foreach ((string n, byte[] d) in a) if (b.TryGetValue(n, out byte[]? d2) && !d.AsSpan().SequenceEqual(d2)) sb.Append(" changed ").Append(n);
        return sb.ToString();
    }

    /// <summary>Reads keys 0..total-1: null where absent. Fails on a wrong value.</summary>
    private static bool[] ReadKeys(RocksDatabase db, int total, string what)
    {
        bool[] present = new bool[total];
        for (int i = 0; i < total; i++)
        {
            string? v = GetString(db, K(i));
            if (v is null) continue;
            Assert.AreEqual(V(i), v, what + ": wrong value for " + K(i));
            present[i] = true;
        }
        using Iterator it = db.NewIterator();
        int n = 0;
        for (it.SeekToFirst(); it.Valid; it.Next()) n++;
        int expected = 0;
        foreach (bool p in present) if (p) expected++;
        Assert.AreEqual(expected, n, what + ": iterator sees other keys than Get");
        return present;
    }

    private static string Show(bool[] present)
    {
        var sb = new StringBuilder();
        foreach (bool p in present) sb.Append(p ? '1' : '0');
        return sb.ToString();
    }

    /// <summary>
    /// DBImpl::Open / Recover / RecoverLogFiles (db/db_impl/db_impl_open.cc), VersionSet::Recover and
    /// GetCurrentManifestPath over a mutated CURRENT, IDENTITY, MANIFEST, OPTIONS or WAL file, for every
    /// wal_recovery_mode, with legacy and recycled WALs. Checks: only RocksDbException escapes and nothing hangs; a
    /// failed open changes no file and fails the same way again; a successful open recovers a prefix of the write order
    /// (kPointInTimeRecovery), everything (kAbsoluteConsistency) or a subset with correct values (the other modes), and
    /// reopens (same mode) to the same data.
    /// </summary>
    [TestMethod]
    [DataRow(false, false)]
    [DataRow(true, false)]
    [DataRow(false, true)]
    public void OpenOverMutatedMetadataFilesKeepsRecoveryContracts(bool recycle, bool walZstd)
    {
        int seed = Seed();
        TestContext.WriteLine("seed=" + seed);
        var rnd = new Random(seed);
        Dictionary<string, byte[]> pristine = Snapshot(BuildDb(recycle, walZstd, out int total), includeLogs: false);
        var targets = new List<string>();
        foreach (string n in pristine.Keys)
        {
            if (!n.EndsWith(".sst", StringComparison.Ordinal)) targets.Add(n);
        }
        targets.Sort(StringComparer.Ordinal);
        TestContext.WriteLine("targets: " + string.Join(", ", targets));
        var sw = System.Diagnostics.Stopwatch.StartNew();
        for (int iter = 0; iter < 400 && sw.Elapsed < TimeSpan.FromSeconds(12); iter++)
        {
            var files = new Dictionary<string, byte[]>(pristine, StringComparer.Ordinal);
            string target = targets[rnd.Next(targets.Count)];
            files[target] = Mutate(rnd, files[target]);
            if (rnd.Next(8) == 0) files.Remove(target);
            var mode = (WalRecoveryMode)rnd.Next(4);
            string what = "seed=" + seed + " iter=" + iter + " target=" + target + " mode=" + mode;
            // The recovery-mode contract is checked for single bit flips only, which the record CRC always catches. Native
            // loses data silently in every mode for a WAL that is gone or cut at a record boundary (without
            // track_and_verify_wals) and for a zeroed header (kZeroType with length 0 drops the rest of the block
            // unreported, log::Reader::ReadPhysicalRecord); the compat fuzz compares those cases with native directly. In a
            // recycled WAL a flipped log number field (recyclable header bytes 7..10) is a previous user's record, i.e.
            // the end of the log, not a corruption, so recycled WALs are not checked either.
            bool singleBitFlip = files.TryGetValue(target, out byte[]? mutated) && mutated.Length == pristine[target].Length && BitDistance(mutated, pristine[target]) == 1;
            bool recycledWal = recycle && target.EndsWith(".log", StringComparison.Ordinal);
            OpenAndCheck(files, mode, recycle, total, what, checkContract: singleBitFlip && !recycledWal);
        }
    }

    private static int BitDistance(byte[] a, byte[] b)
    {
        int d = 0;
        for (int i = 0; i < a.Length; i++) d += System.Numerics.BitOperations.PopCount((uint)(a[i] ^ b[i]));
        return d;
    }

    private static void OpenAndCheck(Dictionary<string, byte[]> files, WalRecoveryMode mode, bool recycle, int total, string what, bool checkContract)
    {
        MemoryFileSystem fs = Restore(files);
        DbOptions o = NewOptions(fs);
        o.CreateIfMissing = false;
        o.WalRecoveryMode = mode;
        o.AvoidFlushDuringRecovery = true;
        if (recycle) o.RecycleLogFileNum = 2;
        if (recycle && mode is WalRecoveryMode.TolerateCorruptedTailRecords or WalRecoveryMode.AbsoluteConsistency) o.RecycleLogFileNum = 0;
        Exception? failure = null;
        bool[]? present = null;
        RunWithTimeout(() =>
        {
            try
            {
                using RocksDatabase db = RocksDatabase.Open(DbPath, o);
                present = ReadKeys(db, total, what);
            }
            catch (Exception e) when (e is not AssertFailedException)
            {
                failure = e;
            }
        }, what);
        if (failure is not null)
        {
            AssertRocksDbFailure(failure, what);
            Dictionary<string, byte[]> after = Snapshot(fs, includeLogs: false);
            string diff = DescribeDiff(files, after);
            Assert.AreEqual("", diff, what + ": failed open (" + failure.Message + ") changed files");
            // Fails the same way again.
            MemoryFileSystem fs2 = Restore(files);
            DbOptions o2 = o.Clone();
            o2.FileSystem = fs2;
            var ex = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, o2).Dispose(), what);
            Assert.AreEqual(((RocksDbException)failure).Code, ex.Code, what);
            return;
        }
        Assert.IsNotNull(present);
        string shown = Show(present!);
        switch (checkContract ? mode : WalRecoveryMode.SkipAnyCorruptedRecords)
        {
            case WalRecoveryMode.AbsoluteConsistency:
                Assert.IsFalse(Array.IndexOf(present!, false) >= 0, what + ": absolute consistency lost data: " + shown);
                break;
            case WalRecoveryMode.PointInTimeRecovery:
                // (kTolerateCorruptedTailRecords tolerates a torn tail in every WAL and replays the next one: holes are
                // possible there, as in native, see Audit7MetadataCorruptionCompatTests.DamagedWalOrManifestRecoversLikeNative.)
                int firstMissing = Array.IndexOf(present!, false);
                if (firstMissing >= 0)
                {
                    for (int i = firstMissing; i < total; i++) Assert.IsFalse(present![i], what + ": hole in recovered data: " + shown);
                }
                break;
        }
        // Reopen in the same mode: the recovered state is durable and unchanged (a damaged WAL kept alive by
        // avoid_flush_during_recovery is read again the same way).
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o))
        {
            bool[] again = ReadKeys(db, total, what + " (reopen)");
            Assert.AreEqual(shown, Show(again), what + ": reopen changed the data");
        }
    }

    // ---- targeted regressions -------------------------------------------------------------------------------------

    /// <summary>A legacy (non-recycled) WAL of five one-put batches whose third record's type byte was flipped from
    /// kFullType (1) to kRecyclableFullType (5), a single bit.</summary>
    private static MemoryFileSystem DbWithRecyclableTypeFlip(out string walName)
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o))
        {
            for (int i = 0; i < 5; i++) db.Put(B(K(i)), B(V(i)));
        }
        walName = "";
        foreach (string child in fs.GetChildren(DbPath))
        {
            if (child.EndsWith(".log", StringComparison.Ordinal) && string.CompareOrdinal(child, walName) > 0) walName = child;
        }
        byte[] wal = fs.ReadAllBytes(DbPath + "/" + walName);
        int off = 0;
        for (int r = 0; r < 2; r++) off += 7 + (wal[off + 4] | (wal[off + 5] << 8));
        Assert.AreEqual((byte)1, wal[off + 6]);
        wal[off + 6] = 5;
        using (IWritableFile f = fs.CreateWritable(DbPath + "/" + walName)) f.Append(wal);
        return fs;
    }

    /// <summary>
    /// log::Reader::ReadPhysicalRecord (db/log_reader.cc): "A recycled log should have started with a recycled record"
    /// returns kBadRecord without consuming the header, and ReadRecord parses it again forever; native DB::Open hangs on
    /// this single bit flip (checked with the RocksDB 11.12 NuGet in a child process). Deliberate deviation: the record is
    /// dropped like a checksum mismatch, so every recovery mode terminates with the result of a bad checksum there.
    /// </summary>
    [TestMethod]
    [DataRow(WalRecoveryMode.PointInTimeRecovery, null, "11000")]
    [DataRow(WalRecoveryMode.SkipAnyCorruptedRecords, null, "11000")]
    [DataRow(WalRecoveryMode.TolerateCorruptedTailRecords, StatusCode.Corruption, null)]
    [DataRow(WalRecoveryMode.AbsoluteConsistency, StatusCode.Corruption, null)]
    public void RecyclableTypeInLegacyWalDoesNotHangOpen(WalRecoveryMode mode, StatusCode? error, string? keys)
    {
        MemoryFileSystem fs = DbWithRecyclableTypeFlip(out _);
        DbOptions o = NewOptions(fs);
        o.WalRecoveryMode = mode;
        RunWithTimeout(() =>
        {
            if (error is StatusCode code)
            {
                var ex = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, o).Dispose());
                Assert.AreEqual(code, ex.Code);
                StringAssert.Contains(ex.Message, "checksum mismatch");
                return;
            }
            using RocksDatabase db = RocksDatabase.Open(DbPath, o);
            var sb = new StringBuilder();
            for (int i = 0; i < 5; i++) sb.Append(GetString(db, K(i)) == V(i) ? '1' : '0');
            Assert.AreEqual(keys, sb.ToString());
        }, "open " + mode);
    }

    /// <summary>The same WAL through the tailing reader (FragmentBufferedReader::TryReadFragment has the same upstream
    /// loop): the damaged record is reported as a checksum mismatch and the reader does not get stuck on it.</summary>
    [TestMethod]
    public void RecyclableTypeInLegacyWalIsReportedByFragmentBufferedReader()
    {
        MemoryFileSystem fs = DbWithRecyclableTypeFlip(out string walName);
        var rep = new CollectingReporter();
        ulong logNumber = ulong.Parse(walName.AsSpan(0, walName.Length - 4), CultureInfo.InvariantCulture);
        RunWithTimeout(() =>
        {
            using var reader = new FragmentBufferedLogReader(fs.OpenSequential(DbPath + "/" + walName), rep, checksum: true, logNumber);
            int n = 0;
            for (int i = 0; i < 10; i++)
            {
                while (reader.TryReadRecord(out _)) n++;
            }
            Assert.AreEqual(2, n);
        }, "fragment reader");
        Assert.AreEqual(1, rep.Corruptions);
    }

    /// <summary>
    /// VersionBuilder::Rep::ApplyFileAddition / CheckConsistencyForNumLevels: a file on a level &gt;= num_levels fails
    /// recovery with InvalidArgument "db has more levels than options.num_levels" (native, see
    /// Audit7MetadataCorruptionCompatTests.ManifestFileOnHugeLevelIsInvalidArgumentLikeNative). The replayer allocated
    /// a list per level up to the decoded level number (up to 2^31 - 1).
    /// </summary>
    [TestMethod]
    [DataRow(4096)]
    [DataRow(1 << 30)]
    [DataRow(int.MaxValue)]
    public void ManifestFileOnHugeLevelIsInvalidArgument(int level)
    {
        var edit = new VersionEdit();
        edit.AddFile(level, VersionEditTests.File(9, 0, 100, VersionEditTests.IKey("a", 1, EntryType.Value), VersionEditTests.IKey("b", 2, EntryType.Value), 1, 2, 1));
        VersionEdit decoded = VersionEdit.DecodeFrom(edit.Encode(0));
        RunWithTimeout(() =>
        {
            var r = new ManifestReplayer(null);
            r.Apply(FirstEdit());
            var ex = Assert.ThrowsExactly<RocksDbException>(() => r.Apply(decoded));
            Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
            StringAssert.Contains(ex.Message, "db has more levels than options.num_levels");
        }, "level " + level);
    }

    /// <summary>
    /// RocksDBOptionsParser::ParseVersionNumber (options/options_parser.cc) accumulates each component in an int
    /// ("current_number * 10 + digit") and EndSection then checks options_file_version's major &gt;= 1. A component that
    /// wraps negative threw ArgumentOutOfRangeException from System.Version; native: InvalidArgument for a wrapped
    /// options_file_version major, OK otherwise (Audit7MetadataCorruptionCompatTests.OptionsVersionNumberOverflowMatchesNative).
    /// </summary>
    [TestMethod]
    [DataRow("  rocksdb_version=11.14.0\n  options_file_version=2147483648.1\n", StatusCode.InvalidArgument)]
    [DataRow("  rocksdb_version=2147483648.0.0\n  options_file_version=1.1\n", null)]
    [DataRow("  rocksdb_version=11.14.0\n  options_file_version=1.2147483648\n", null)]
    public void OptionsVersionNumberOverflowIsNotAnArgumentException(string versionLines, StatusCode? error)
    {
        string text = "[Version]\n" + versionLines + "[DBOptions]\n[CFOptions \"default\"]\n";
        if (error is StatusCode code)
        {
            var ex = Assert.ThrowsExactly<RocksDbException>(() => OptionsFile.Parse(text, preserveUnknownOptions: false));
            Assert.AreEqual(code, ex.Code);
            return;
        }
        OptionsFile.Parse(text, preserveUnknownOptions: false);
    }

    /// <summary>LineFileReader::ReadLine drops a last line without '\n' (an OPTIONS file torn mid-statement).</summary>
    [TestMethod]
    public void OptionsFileTornLastLineIsIgnored()
    {
        OptionsFile f = OptionsFile.Parse("[DBOptions]\n[CFOptions \"default\"]\n  num_levels=4", preserveUnknownOptions: false);
        Assert.AreEqual(new ColumnFamilyOptions().NumLevels, f.ColumnFamilies[0].Options.NumLevels);
        f = OptionsFile.Parse("[DBOptions]\n[CFOptions \"default\"]\n  num_levels=4\n", preserveUnknownOptions: false);
        Assert.AreEqual(4, f.ColumnFamilies[0].Options.NumLevels);
        // A torn statement that would not parse is dropped, not an error.
        OptionsFile.Parse("[DBOptions]\n[CFOptions \"default\"]\n  num_levels=ab", preserveUnknownOptions: false);
    }

    /// <summary>
    /// An unregistered Customizable id (memtable_factory, table_factory, flush_block_policy_factory) is NotSupported,
    /// which ConfigOptions::ignore_unsupported_options (default true) ignores, leaving the option unchanged
    /// (options/configurable.cc ConfigureOptions). read_amp_bytes_per_bit uses ParseUint64 truncated to 32 bits;
    /// block_protection_bytes_per_key is a uint8_t (ParseUint8).
    /// </summary>
    [TestMethod]
    public void OptionValueParsingMatchesNativeConfigOptions()
    {
        OptionsFile f = OptionsFile.Parse("[DBOptions]\n[CFOptions \"default\"]\n  memtable_factory=NoSuchFactory\n  table_factory={\n" +
            "[TableOptions/BlockBasedTable \"default\"]\n  flush_block_policy_factory=Nope\n  read_amp_bytes_per_bit=4294967297\n", preserveUnknownOptions: false);
        var defaults = new ColumnFamilyOptions();
        Assert.AreEqual(defaults.MemtableFactory, f.ColumnFamilies[0].Options.MemtableFactory);
        Assert.AreEqual(defaults.TableFactory, f.ColumnFamilies[0].Options.TableFactory);
        Assert.AreEqual(1u, f.ColumnFamilies[0].Options.TableOptions!.ReadAmpBytesPerBit);
        var ex = Assert.ThrowsExactly<RocksDbException>(() =>
            OptionsFile.Parse("[DBOptions]\n[CFOptions \"default\"]\n  block_protection_bytes_per_key=256\n", preserveUnknownOptions: false));
        Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
    }
}
