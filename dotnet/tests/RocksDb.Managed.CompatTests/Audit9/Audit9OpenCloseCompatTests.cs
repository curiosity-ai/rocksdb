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
