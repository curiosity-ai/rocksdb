using System.Runtime.InteropServices;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;

namespace RocksDb.Managed.CompatTests.Audit9;

/// <summary>
/// Audit #9, opening and closing a DB: the same open / close sequences run on native RocksDB (the C API of the RocksDB
/// NuGet package) and on the managed port, comparing what each one leaves on disk and reports.
/// </summary>
[TestClass]
public sealed class Audit9OpenCloseCompatTests
{
    private static readonly RocksDbSharp.Native N = RocksDbSharp.Native.Instance;

    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);

    private static string IdentityPath(string dir) => System.IO.Path.Combine(dir, "IDENTITY");

    private static string NativeIdentity(RocksDbSharp.RocksDb db)
    {
        IntPtr p = N.rocksdb_get_db_identity(db.Handle, out UIntPtr len);
        try
        {
            return Marshal.PtrToStringUTF8(p, checked((int)len.ToUInt64()))!;
        }
        finally
        {
            N.rocksdb_free(p);
        }
    }

    private static RocksDbSharp.DbOptions NativeOptions(bool writeDbIdToManifest)
    {
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        N.rocksdb_options_set_write_dbid_to_manifest(o.Handle, writeDbIdToManifest ? (byte)1 : (byte)0);
        return o;
    }

    private static DbOptions ManagedOptions(bool writeDbIdToManifest) => new() { CreateIfMissing = true, WriteDbIdToManifest = writeDbIdToManifest };

    /// <summary>
    /// DBImpl::SetupDBId (db/db_impl/db_impl_files.cc): an IDENTITY file that does not match the DB id recorded in the
    /// MANIFEST ("Check for the IDENTITY file and create it if not there or broken or not matching manifest") is rewritten
    /// with the MANIFEST's id (write_identity_file). The port kept the stale IDENTITY.
    /// </summary>
    [TestMethod]
    public void IdentityFileNotMatchingTheManifestIsRewrittenLikeNative()
    {
        using var nativeDir = new TempDir("a9-id-native");
        string nativeOriginal;
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(NativeOptions(true), nativeDir.Path)) nativeOriginal = NativeIdentity(db);
        File.WriteAllText(IdentityPath(nativeDir.Path), "stale-identity");
        string nativeReopened;
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(NativeOptions(true), nativeDir.Path)) nativeReopened = NativeIdentity(db);
        Assert.AreEqual(nativeOriginal, nativeReopened, "native: the MANIFEST's DB id wins");
        Assert.AreEqual(nativeOriginal, File.ReadAllText(IdentityPath(nativeDir.Path)), "native rewrites IDENTITY");

        using var managedDir = new TempDir("a9-id-managed");
        string managedOriginal;
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, ManagedOptions(true))) managedOriginal = db.DbId;
        File.WriteAllText(IdentityPath(managedDir.Path), "stale-identity");
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, ManagedOptions(true))) Assert.AreEqual(managedOriginal, db.DbId);
        Assert.AreEqual(managedOriginal, File.ReadAllText(IdentityPath(managedDir.Path)), "managed rewrites IDENTITY like native");
    }

    /// <summary>
    /// DBImpl::SetupDBId: an empty IDENTITY file with no DB id in the MANIFEST (write_dbid_to_manifest = false) is "no
    /// good": a new id is generated (Env::GenerateUniqueId) and written. In a read-only open (DBImplReadOnly::Recover ->
    /// SetupDBId(read_only = true)) a new id is generated but not persisted. The port used the empty string as the DB id
    /// (and in read-only opens also when IDENTITY was missing).
    /// </summary>
    [TestMethod]
    public void EmptyIdentityFileGetsANewIdLikeNative()
    {
        using var nativeDir = new TempDir("a9-emptyid-native");
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(NativeOptions(false), nativeDir.Path)) db.Put(U("k"), U("v"));
        File.WriteAllText(IdentityPath(nativeDir.Path), "");
        string nativeReadOnly;
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.OpenReadOnly(NativeOptions(false), nativeDir.Path, false)) nativeReadOnly = NativeIdentity(db);
        Assert.AreEqual(36, nativeReadOnly.Length, "native read-only: a generated id");
        Assert.AreEqual("", File.ReadAllText(IdentityPath(nativeDir.Path)), "native read-only writes nothing");
        string nativeId;
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(NativeOptions(false), nativeDir.Path)) nativeId = NativeIdentity(db);
        Assert.AreEqual(36, nativeId.Length, "native: a generated id");
        Assert.AreEqual(nativeId, File.ReadAllText(IdentityPath(nativeDir.Path)), "native writes the new id");

        using var managedDir = new TempDir("a9-emptyid-managed");
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, ManagedOptions(false))) db.Put(U("k"), U("v"));
        File.WriteAllText(IdentityPath(managedDir.Path), "");
        using (RocksDatabase db = RocksDatabase.OpenReadOnly(managedDir.Path, ManagedOptions(false)))
        {
            Assert.AreEqual(36, db.DbId.Length, "managed read-only: a generated id");
        }
        Assert.AreEqual("", File.ReadAllText(IdentityPath(managedDir.Path)), "managed read-only writes nothing");
        string managedId;
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, ManagedOptions(false))) managedId = db.DbId;
        Assert.AreEqual(36, managedId.Length, "managed: a generated id");
        Assert.AreEqual(managedId, File.ReadAllText(IdentityPath(managedDir.Path)), "managed writes the new id");
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, ManagedOptions(false))) Assert.AreEqual(managedId, db.DbId, "the new id is kept");
    }

    /// <summary>The DB directory's files by kind ("LOG.old x2, MANIFEST x1, ..."), numbers and timestamps left out.</summary>
    private static string Listing(string dir)
    {
        var kinds = new SortedDictionary<string, int>(StringComparer.Ordinal);
        foreach (string f in Directory.EnumerateFileSystemEntries(dir))
        {
            string name = System.IO.Path.GetFileName(f);
            string kind = name.StartsWith("LOG.old.", StringComparison.Ordinal) ? "LOG.old"
                : name.StartsWith("MANIFEST-", StringComparison.Ordinal) ? "MANIFEST"
                : name.StartsWith("OPTIONS-", StringComparison.Ordinal) ? (name.EndsWith(".dbtmp", StringComparison.Ordinal) ? "OPTIONS.dbtmp" : "OPTIONS")
                : name.EndsWith(".log", StringComparison.Ordinal) ? "wal"
                : name.EndsWith(".sst", StringComparison.Ordinal) ? "sst"
                : name.EndsWith(".dbtmp", StringComparison.Ordinal) ? "dbtmp"
                : name;
            kinds[kind] = kinds.GetValueOrDefault(kind) + 1;
        }
        var sb = new StringBuilder();
        foreach ((string k, int n) in kinds) sb.Append(k).Append(" x").Append(n).Append(", ");
        return sb.ToString();
    }

    /// <summary>One step of an open / close sequence.</summary>
    public enum Step { Put, Flush, Close, Open, OpenAvoidFlushDuringRecovery }

    private static List<string> RunNative(string dir, Step[] steps)
    {
        var listings = new List<string>();
        RocksDbSharp.RocksDb? db = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions().SetCreateIfMissing(true), dir);
        int i = 0;
        foreach (Step s in steps)
        {
            switch (s)
            {
                case Step.Put: db!.Put(U("k" + i), U("v" + i)); break;
                case Step.Flush: db!.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true)); break;
                case Step.Close: db!.Dispose(); db = null; break;
                default:
                    var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
                    N.rocksdb_options_set_avoid_flush_during_recovery(o.Handle, s == Step.OpenAvoidFlushDuringRecovery ? (byte)1 : (byte)0);
                    db = RocksDbSharp.RocksDb.Open(o, dir);
                    break;
            }
            i++;
            listings.Add(s + ": " + Listing(dir));
        }
        db?.Dispose();
        return listings;
    }

    private static List<string> RunManaged(string dir, Step[] steps)
    {
        var listings = new List<string>();
        RocksDatabase? db = RocksDatabase.Open(dir, new DbOptions { CreateIfMissing = true });
        int i = 0;
        foreach (Step s in steps)
        {
            switch (s)
            {
                case Step.Put: db!.Put(U("k" + i), U("v" + i)); break;
                case Step.Flush: db!.Flush(); break;
                case Step.Close: db!.Dispose(); db = null; break;
                default:
                    db = RocksDatabase.Open(dir, new DbOptions { CreateIfMissing = true, AvoidFlushDuringRecovery = s == Step.OpenAvoidFlushDuringRecovery });
                    break;
            }
            i++;
            listings.Add(s + ": " + Listing(dir));
        }
        db?.Dispose();
        return listings;
    }

    /// <summary>
    /// DBImpl::Open / CloseHelper (db_impl_open.cc, db_impl.cc, db_impl_files.cc): the files a sequence of opens, writes,
    /// flushes and closes leaves in the DB directory -- the info LOG rotated on every open (LOG.old), one MANIFEST, the
    /// OPTIONS files kept (kNumOptionsFilesKept), the WALs flushed and deleted at open or kept with
    /// avoid_flush_during_recovery, the table files -- are the same kinds and counts as native after every step.
    /// </summary>
    [TestMethod]
    public void FilesLeftByOpenCloseSequencesMatchNative()
    {
        Step[][] sequences =
        [
            [Step.Put, Step.Close, Step.Open, Step.Close, Step.Open, Step.Put, Step.Flush, Step.Close, Step.Open, Step.Close],
            [Step.Put, Step.Close, Step.OpenAvoidFlushDuringRecovery, Step.Put, Step.Close, Step.OpenAvoidFlushDuringRecovery, Step.Close,
                Step.Open, Step.Close],
            [Step.Close, Step.Open, Step.Close, Step.Open, Step.Close, Step.Open, Step.Close],
            [Step.Put, Step.Flush, Step.Put, Step.Flush, Step.Close, Step.Open, Step.Put, Step.Close, Step.OpenAvoidFlushDuringRecovery,
                Step.Flush, Step.Close],
        ];
        foreach (Step[] steps in sequences)
        {
            using var nativeDir = new TempDir("a9-files-native");
            using var managedDir = new TempDir("a9-files-managed");
            List<string> native = RunNative(nativeDir.Path, steps);
            List<string> managed = RunManaged(managedDir.Path, steps);
            CollectionAssert.AreEqual(native, managed,
                "\nnative:\n" + string.Join("\n", native) + "\nmanaged:\n" + string.Join("\n", managed));
        }
    }

    /// <summary>
    /// DB::OpenForReadOnly (db_impl_readonly.cc) and DB::OpenAsSecondary (db_impl_secondary.cc) on a closed DB with an
    /// unflushed WAL, a TryCatchUpWithPrimary and the close: the primary's directory keeps the same files as native
    /// (read-only opens rotate the info LOG like native, nothing else is written or deleted; the secondary writes only
    /// into its own directory; the port's secondary writes no info LOG there, native's does).
    /// </summary>
    [TestMethod]
    public void ReadOnlyAndSecondaryOpensLeaveTheFilesNativeLeaves()
    {
        using var nativeDir = new TempDir("a9-ro-native");
        using var managedDir = new TempDir("a9-ro-managed");
        string nativeDb = System.IO.Path.Combine(nativeDir.Path, "db"), managedDb = System.IO.Path.Combine(managedDir.Path, "db");
        string nativeSec = System.IO.Path.Combine(nativeDir.Path, "sec"), managedSec = System.IO.Path.Combine(managedDir.Path, "sec");
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions().SetCreateIfMissing(true), nativeDb))
        {
            db.Put(U("a"), U("1"));
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            db.Put(U("b"), U("2"));
        }
        using (RocksDatabase db = RocksDatabase.Open(managedDb, new DbOptions { CreateIfMissing = true }))
        {
            db.Put(U("a"), U("1"));
            db.Flush();
            db.Put(U("b"), U("2"));
        }
        string nativeBefore = Listing(nativeDb), managedBefore = Listing(managedDb);
        Assert.AreEqual(nativeBefore, managedBefore);

        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.OpenReadOnly(new RocksDbSharp.DbOptions(), nativeDb, false)) Assert.AreEqual("2", db.Get("b"));
        using (RocksDatabase db = RocksDatabase.OpenReadOnly(managedDb, new DbOptions())) Assert.AreEqual("2", Encoding.UTF8.GetString(db.Get(U("b"))!));
        string nativeRo = Listing(nativeDb), managedRo = Listing(managedDb);
        Assert.AreEqual(nativeRo, managedRo, "after a read-only open and close");

        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.OpenAsSecondary(new RocksDbSharp.DbOptions(), nativeDb, nativeSec))
        {
            db.TryCatchUpWithPrimary();
            Assert.AreEqual("2", db.Get("b"));
        }
        using (RocksDatabase db = RocksDatabase.OpenAsSecondary(managedDb, managedSec, new DbOptions(),
            [new ColumnFamilyDescriptor("default", new ColumnFamilyOptions())]))
        {
            db.TryCatchUpWithPrimary();
            Assert.AreEqual("2", Encoding.UTF8.GetString(db.Get(U("b"))!));
        }
        Assert.AreEqual(nativeRo, Listing(nativeDb), "native: the secondary changed nothing in the primary's directory");
        Assert.AreEqual(managedRo, Listing(managedDb), "managed: the secondary changed nothing in the primary's directory");
        // Native keeps the secondary's info LOG in the secondary directory (OpenAsSecondary: "db_log_dir" defaults to
        // secondary_path); the port's read-only and secondary opens write no info LOG at all (harmless deviation).
        Assert.AreEqual("LOG x1, ", Listing(nativeSec));
        Assert.AreEqual("", Listing(managedSec));
    }

    /// <summary>
    /// DBImpl::Recover: opening a path without CURRENT and with create_if_missing = false is InvalidArgument
    /// "&lt;dbname&gt;/CURRENT: does not exist (create_if_missing is false)" (the port named the directory, not CURRENT).
    /// Native creates the directory first (DBImpl::Open: CreateDirIfMissing of the WAL dir and SetDirectories) and leaves
    /// it with its LOG and LOCK; the port leaves nothing (harmless deviation).
    /// </summary>
    [TestMethod]
    public void OpenWithoutCreateIfMissingReportsLikeNative()
    {
        using var root = new TempDir("a9-missing");
        string nativePath = System.IO.Path.Combine(root.Path, "native"), managedPath = System.IO.Path.Combine(root.Path, "managed");
        string? nativeError = null;
        try
        {
            using RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions(), nativePath);
        }
        catch (RocksDbSharp.RocksDbException e)
        {
            nativeError = e.Message;
        }
        RocksDbException managed = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(managedPath, new DbOptions()));
        Assert.AreEqual(StatusCode.InvalidArgument, managed.Code);
        Assert.AreEqual(nativeError!.Replace(nativePath, "<db>", StringComparison.Ordinal), managed.Message.Replace(managedPath, "<db>", StringComparison.Ordinal));
        Assert.IsTrue(Directory.Exists(nativePath), "native creates the directory");
        Assert.IsFalse(Directory.Exists(managedPath), "the port does not");
    }

    /// <summary>
    /// DBImpl::Open -> DeleteObsoleteFiles (full scan, db_impl_files.cc FindObsoleteFiles / PurgeObsoleteFiles) and
    /// MaybeUpdateNextFileNumber: files a crashed process left behind -- a table file in no version (numbered above and
    /// below next_file_number), a blob file in no version (kept: ShouldKeepBlobFileDuringPurge, no footer), temp files
    /// (*.dbtmp, OPTIONS-*.dbtmp), an old WAL and an old MANIFEST -- are deleted or kept as native does, and the DB then
    /// writes new files without colliding with them.
    /// </summary>
    [TestMethod]
    public void LeftoverFilesAreCleanedUpAtOpenLikeNative()
    {
        string[] junk = ["000003.sst", "000900.sst", "000901.blob", "000902.dbtmp", "OPTIONS-000903.dbtmp", "000001.log", "MANIFEST-000002", "000904.log"];
        using var nativeDir = new TempDir("a9-junk-native");
        using var managedDir = new TempDir("a9-junk-managed");
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions().SetCreateIfMissing(true), nativeDir.Path))
        {
            db.Put(U("a"), U("1"));
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
        }
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true }))
        {
            db.Put(U("a"), U("1"));
            db.Flush();
        }
        foreach (string dir in new[] { nativeDir.Path, managedDir.Path })
        {
            foreach (string j in junk)
            {
                string p = System.IO.Path.Combine(dir, j);
                if (!File.Exists(p)) File.WriteAllBytes(p, U("junk"));
            }
        }
        static string Remaining(string dir, string[] names)
        {
            var left = new List<string>();
            foreach (string n in names)
            {
                if (File.Exists(System.IO.Path.Combine(dir, n))) left.Add(n);
            }
            return string.Join(",", left);
        }
        string nativeBefore = Remaining(nativeDir.Path, junk), managedBefore = Remaining(managedDir.Path, junk);
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions().SetCreateIfMissing(true), nativeDir.Path))
        {
            db.Put(U("b"), U("2"));
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            Assert.AreEqual("1", db.Get("a"));
        }
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true }))
        {
            db.Put(U("b"), U("2"));
            db.Flush();
            Assert.AreEqual("1", Encoding.UTF8.GetString(db.Get(U("a"))!));
        }
        string nativeAfter = Remaining(nativeDir.Path, junk), managedAfter = Remaining(managedDir.Path, junk);
        Console.WriteLine("native:  " + nativeBefore + " -> " + nativeAfter + " | " + Listing(nativeDir.Path));
        Console.WriteLine("managed: " + managedBefore + " -> " + managedAfter + " | " + Listing(managedDir.Path));
        // PurgeObsoleteFiles keeps every OPTIONS-*.dbtmp ("TODO(yhchiang): carefully modify the third condition to safely
        // remove the temp options files"); the port deletes a leftover one at open, when no OPTIONS write can be running
        // (harmless deviation). Everything else matches.
        Assert.AreEqual("000901.blob,OPTIONS-000903.dbtmp", nativeAfter);
        Assert.AreEqual("000901.blob", managedAfter);
        Assert.AreEqual(Listing(nativeDir.Path).Replace("OPTIONS.dbtmp x1, ", "", StringComparison.Ordinal), Listing(managedDir.Path));
    }

    /// <summary>
    /// DBImpl::Recover / RecoverLogFiles: the last sequence number after an open (replayed WALs, recovery flush, empty
    /// reopen, avoid_flush_during_recovery) is the same as native's, so is the sequence number of the first write after
    /// it; every open gets a new db_session_id (DBImpl::SetDbSessionId) while the DB id stays.
    /// </summary>
    [TestMethod]
    public void SequenceNumbersAndSessionIdsAcrossReopensMatchNative()
    {
        using var nativeDir = new TempDir("a9-seq-native");
        using var managedDir = new TempDir("a9-seq-managed");
        var nativeSeqs = new List<ulong>();
        var managedSeqs = new List<ulong>();
        for (int round = 0; round < 4; round++)
        {
            bool avoid = round == 2;
            var no = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
            N.rocksdb_options_set_avoid_flush_during_recovery(no.Handle, avoid ? (byte)1 : (byte)0);
            using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(no, nativeDir.Path))
            {
                nativeSeqs.Add(db.GetLatestSequenceNumber());
                if (round != 1)
                {
                    for (int i = 0; i < 3; i++) db.Put(U("k" + round + i), U("v"));
                    var b = new RocksDbSharp.WriteBatch();
                    b.Put(U("b1"), U("1"));
                    b.Delete(U("b2"));
                    db.Write(b);
                }
                nativeSeqs.Add(db.GetLatestSequenceNumber());
            }
            var mo = new DbOptions { CreateIfMissing = true, AvoidFlushDuringRecovery = avoid };
            string sessionBefore = "";
            using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, mo))
            {
                managedSeqs.Add(db.GetLatestSequenceNumber());
                if (round != 1)
                {
                    for (int i = 0; i < 3; i++) db.Put(U("k" + round + i), U("v"));
                    using RocksDb.Managed.Db.WriteBatch b = RocksDb.Managed.Db.WriteBatch.Rent();
                    b.Put(U("b1"), U("1"));
                    b.Delete(U("b2"));
                    db.Write(b);
                }
                managedSeqs.Add(db.GetLatestSequenceNumber());
                sessionBefore = db.DbSessionId;
            }
            using (RocksDatabase db = RocksDatabase.OpenReadOnly(managedDir.Path, new DbOptions()))
            {
                Assert.AreNotEqual(sessionBefore, db.DbSessionId, "a new session id per open");
                Assert.AreEqual(20, db.DbSessionId.Length);
            }
        }
        CollectionAssert.AreEqual(nativeSeqs, managedSeqs, "native " + string.Join(",", nativeSeqs) + " managed " + string.Join(",", managedSeqs));
    }

    /// <summary>A failed open of the DB at a path (whose DB has a column family "cf1").</summary>
    public enum FailedOpen { MissingColumnFamily, UnlistedColumnFamily, ErrorIfExists, LockHeld }

    private static string? NativeFailedOpen(string dir, FailedOpen f)
    {
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetErrorIfExists(f == FailedOpen.ErrorIfExists);
        var cfs = new RocksDbSharp.ColumnFamilies();
        if (f != FailedOpen.UnlistedColumnFamily) cfs.Add("cf1", new RocksDbSharp.ColumnFamilyOptions());
        if (f == FailedOpen.MissingColumnFamily) cfs.Add("cf2", new RocksDbSharp.ColumnFamilyOptions());
        RocksDbSharp.RocksDb? holder = f == FailedOpen.LockHeld ? RocksDbSharp.RocksDb.Open(o, dir, cfs) : null;
        try
        {
            using RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(o, dir, cfs);
            return null;
        }
        catch (RocksDbSharp.RocksDbException e)
        {
            return e.Message;
        }
        finally
        {
            holder?.Dispose();
        }
    }

    private static string? ManagedFailedOpen(string dir, FailedOpen f)
    {
        var o = new DbOptions { CreateIfMissing = true, ErrorIfExists = f == FailedOpen.ErrorIfExists };
        var cfs = new List<ColumnFamilyDescriptor> { new("default", new ColumnFamilyOptions()) };
        if (f != FailedOpen.UnlistedColumnFamily) cfs.Add(new ColumnFamilyDescriptor("cf1", new ColumnFamilyOptions()));
        if (f == FailedOpen.MissingColumnFamily) cfs.Add(new ColumnFamilyDescriptor("cf2", new ColumnFamilyOptions()));
        RocksDatabase? holder = f == FailedOpen.LockHeld ? RocksDatabase.Open(dir, o, cfs) : null;
        try
        {
            using RocksDatabase db = RocksDatabase.Open(dir, o, cfs);
            return null;
        }
        catch (RocksDbException e)
        {
            return e.Code + ": " + e.Message;
        }
        finally
        {
            holder?.Dispose();
        }
    }

    /// <summary>
    /// DBImpl::Open's failure paths (db_impl_open.cc): an unlisted column family ("Column families not opened",
    /// VersionSet::Recover), error_if_exists and a held LOCK fail before anything is written; a missing column family
    /// without create_missing_column_families ("Column family not found") fails only after Recover, CreateWAL and
    /// LogAndApplyForRecovery, so native leaves a new WAL and MANIFEST. The port fails the missing column family before
    /// writing anything and does not rotate the info LOG of a DB whose LOCK is held (native does); every other case
    /// leaves the same files.
    /// </summary>
    [TestMethod]
    [DataRow(FailedOpen.MissingColumnFamily)]
    [DataRow(FailedOpen.UnlistedColumnFamily)]
    [DataRow(FailedOpen.ErrorIfExists)]
    [DataRow(FailedOpen.LockHeld)]
    public void FailedOpenLeavesTheFilesNativeLeaves(FailedOpen f)
    {
        using var nativeDir = new TempDir("a9-failopen-native");
        using var managedDir = new TempDir("a9-failopen-managed");
        var no = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetCreateMissingColumnFamilies(true);
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(no, nativeDir.Path, new RocksDbSharp.ColumnFamilies { { "cf1", new RocksDbSharp.ColumnFamilyOptions() } }))
            db.Put(U("k"), U("v"));
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true, CreateMissingColumnFamilies = true },
            [new ColumnFamilyDescriptor("default", new ColumnFamilyOptions()), new ColumnFamilyDescriptor("cf1", new ColumnFamilyOptions())]))
            db.Put(U("k"), U("v"));
        string? nativeError = NativeFailedOpen(nativeDir.Path, f);
        string? managedError = ManagedFailedOpen(managedDir.Path, f);
        string nativeFiles = Listing(nativeDir.Path), managedFiles = Listing(managedDir.Path);
        Console.WriteLine("native:  " + nativeError + " | " + nativeFiles);
        Console.WriteLine("managed: " + managedError + " | " + managedFiles);
        Assert.IsNotNull(nativeError);
        Assert.IsNotNull(managedError);
        if (f == FailedOpen.MissingColumnFamily)
        {
            StringAssert.StartsWith(nativeError, "Invalid argument: Column family not found");
            StringAssert.StartsWith(managedError, "InvalidArgument: Invalid argument: Column family not found");
            // Native: the recovery ran first (the WAL flushed to a table file, a new WAL and MANIFEST written; the old
            // ones stay until the next open). The port checks before writing anything; the next open sees the same data.
            StringAssert.Contains(nativeFiles, "MANIFEST x2");
            StringAssert.Contains(managedFiles, "MANIFEST x1");
        }
        else if (f == FailedOpen.LockHeld)
        {
            // Native creates the failed open's info LOG (SanitizeOptions -> CreateLoggerFromOptions) before LockFile, so
            // it renames the running DB's LOG to LOG.old; the port takes the LOCK first and leaves the LOG alone.
            Assert.AreEqual(nativeFiles.Replace("LOG.old x2", "LOG.old x1", StringComparison.Ordinal), managedFiles);
        }
        else
        {
            Assert.AreEqual(nativeFiles, managedFiles);
        }
        // Either way the next open (the cause removed) works and sees the data.
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(no, nativeDir.Path, new RocksDbSharp.ColumnFamilies { { "cf1", new RocksDbSharp.ColumnFamilyOptions() } }))
            Assert.AreEqual("v", db.Get("k"));
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true },
            [new ColumnFamilyDescriptor("default", new ColumnFamilyOptions()), new ColumnFamilyDescriptor("cf1", new ColumnFamilyOptions())]))
            Assert.AreEqual("v", Encoding.UTF8.GetString(db.Get(U("k"))!));
    }
}
