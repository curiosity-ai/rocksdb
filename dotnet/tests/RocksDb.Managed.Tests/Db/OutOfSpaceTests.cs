using System.Collections.Concurrent;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>
/// A full disk on the local file system: the error carries <see cref="StatusSubCode.NoSpace"/> (env/io_posix.cc
/// IOError), is classified like upstream (db/error_handler.cc ErrorSeverityMap: kFlush / kWriteCallback + kNoSpace ->
/// kHardError) and recovered by the SstFileManager once space is free, or by Resume. The full device is /dev/full:
/// the files the DB is about to create are symbolic links to it, and removing the links "frees the space".
/// </summary>
[TestClass]
public sealed class OutOfSpaceTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    private static bool HasDevFull => OperatingSystem.IsLinux() && File.Exists("/dev/full");

    private sealed class RecoveryListener : EventListener
    {
        public readonly ConcurrentQueue<BackgroundErrorRecoveryInfo> Ends = new();
        public readonly ManualResetEventSlim Ended = new(false);
        public bool DisableAutoRecovery { get; set; }

        public override void OnErrorRecoveryBegin(ErrorRecoveryBeginEventArgs e)
        {
            if (DisableAutoRecovery) e.AutoRecovery = false;
        }

        public override void OnErrorRecoveryEnd(BackgroundErrorRecoveryInfo info)
        {
            Ends.Enqueue(info);
            Ended.Set();
        }
    }

    private static string TempDir()
    {
        string d = Path.Combine(Path.GetTempPath(), "rocks-nospace-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(d);
        return d;
    }

    private static void DeleteDir(string dir)
    {
        try { Directory.Delete(dir, true); }
        catch (IOException) { }
    }

    /// <summary>Points the next <paramref name="count"/> files with <paramref name="extension"/> at /dev/full.</summary>
    private static List<string> FillDisk(string dir, string extension, int count)
    {
        var links = new List<string>();
        for (int n = 1; n < count; n++)
        {
            string p = Path.Combine(dir, n.ToString("D6", System.Globalization.CultureInfo.InvariantCulture) + extension);
            if (File.Exists(p)) continue;
            File.CreateSymbolicLink(p, "/dev/full");
            links.Add(p);
        }
        return links;
    }

    private static void FreeDisk(List<string> links)
    {
        foreach (string l in links)
        {
            if (new FileInfo(l).LinkTarget is not null) File.Delete(l);
        }
    }

    /// <summary>Retries a write until the automatic recovery has cleared the out-of-space error.</summary>
    private static void PutWhenRecovered(RocksDatabase db, string key, string value)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        while (true)
        {
            try
            {
                db.Put(B(key), B(value));
                return;
            }
            catch (RocksDbException e) when (e.SubCode == StatusSubCode.NoSpace && sw.Elapsed < Timeout)
            {
                Thread.Sleep(20);
            }
        }
    }

    [TestMethod]
    public void ErrnoMapsLikeIoPosix()
    {
        RocksDbException e = RocksDbException.FromErrno("While appending to file", "/x", 28, "No space left on device");
        Assert.AreEqual(StatusCode.IOError, e.Code);
        Assert.AreEqual(StatusSubCode.NoSpace, e.SubCode);
        Assert.IsTrue(e.Retryable, "io_posix.cc IOError: ENOSPC is retryable");
        e = RocksDbException.FromErrno("While open a file", "/x", 2, "No such file or directory");
        Assert.AreEqual(StatusSubCode.PathNotFound, e.SubCode);
        Assert.IsFalse(e.Retryable);
        e = RocksDbException.FromErrno("While fsync", "/x", 5, "Input/output error");
        Assert.AreEqual(StatusCode.IOError, e.Code);
        Assert.AreEqual(StatusSubCode.None, e.SubCode);
        if (!OperatingSystem.IsWindows())
        {
            // .NET carries the errno as the HResult of the IOExceptions it raises on Unix.
            e = RocksDbException.FromException(new IOException("No space left on device", 28));
            Assert.AreEqual(StatusSubCode.NoSpace, e.SubCode);
            Assert.IsTrue(e.Retryable);
            e = RocksDbException.FromException(new IOException("quota", OperatingSystem.IsMacOS() ? 69 : 122));
            Assert.AreEqual(StatusSubCode.NoSpace, e.SubCode);
        }
        else
        {
            e = RocksDbException.FromException(new IOException("disk full", unchecked((int)0x80070070)));
            Assert.AreEqual(StatusSubCode.NoSpace, e.SubCode);
        }
        e = RocksDbException.FromException(new FileNotFoundException("gone"));
        Assert.AreEqual(StatusSubCode.PathNotFound, e.SubCode);
        // An exception that already is a status is kept as is.
        RocksDbException own = RocksDbException.NoSpace("x");
        Assert.AreSame(own, RocksDbException.FromException(own));
    }

    // io_posix.cc IOError(): ENOSPC -> IOStatus::NoSpace (retryable). Writes to /dev/full fail with ENOSPC.
    [TestMethod]
    public void AppendToFullDeviceCarriesNoSpace()
    {
        if (!HasDevFull) Assert.Inconclusive("needs /dev/full");
        IWritableFile f = LocalFileSystem.Instance.CreateWritable("/dev/full", truncate: false);
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() =>
        {
            f.Append(new byte[64 * 1024]);
            f.Flush();
        });
        Assert.AreEqual(StatusCode.IOError, ex.Code);
        Assert.AreEqual(StatusSubCode.NoSpace, ex.SubCode, ex.Message);
        Assert.IsTrue(ex.Retryable);
        // Close flushes what is still buffered (PosixWritableFile::Close returns the error too).
        ex = Assert.ThrowsExactly<RocksDbException>(f.Dispose);
        Assert.AreEqual(StatusSubCode.NoSpace, ex.SubCode, ex.Message);
    }

    [TestMethod]
    public async Task AsyncAppendToFullDeviceCarriesNoSpace()
    {
        if (!HasDevFull) Assert.Inconclusive("needs /dev/full");
        IWritableFile f = LocalFileSystem.Instance.CreateWritable("/dev/full", truncate: false);
        RocksDbException ex = await Assert.ThrowsExactlyAsync<RocksDbException>(async () =>
        {
            await f.AppendAsync(new byte[64 * 1024]);
            await f.FlushAsync();
        });
        Assert.AreEqual(StatusSubCode.NoSpace, ex.SubCode, ex.Message);
        ex = await Assert.ThrowsExactlyAsync<RocksDbException>(async () => await f.DisposeAsync());
        Assert.AreEqual(StatusSubCode.NoSpace, ex.SubCode, ex.Message);
    }

    [TestMethod]
    public void OpenMissingFileIsPathNotFound()
    {
        string dir = TempDir();
        try
        {
            RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => LocalFileSystem.Instance.OpenRandomAccess(Path.Combine(dir, "missing.sst")));
            Assert.AreEqual(StatusSubCode.PathNotFound, ex.SubCode);
        }
        finally
        {
            DeleteDir(dir);
        }
    }

    /// <summary>
    /// A flush that hits ENOSPC is a HardError with NoSpace (not a FatalError), writes fail meanwhile, and the default
    /// SstFileManager recovers the DB on its own once space is free (ErrorHandler::RecoverFromNoSpace). No data is lost.
    /// </summary>
    [TestMethod]
    public void FlushToFullDiskRecoversAutomaticallyOnceSpaceIsFree()
    {
        if (!HasDevFull) Assert.Inconclusive("needs /dev/full");
        string dir = TempDir();
        try
        {
            using var sfm = new SstFileManager(LocalFileSystem.Instance, 0, 0.25, 64UL << 20) { ErrorRecoveryPollInterval = TimeSpan.FromMilliseconds(50) };
            var listener = new RecoveryListener();
            var o = new DbOptions { CreateIfMissing = true, SstFileManager = sfm };
            o.Listeners.Add(listener);
            using (var db = RocksDatabase.Open(dir, o))
            {
                db.Put(B("a"), B("v-a"));
                List<string> links = FillDisk(dir, ".sst", 64);
                RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
                Assert.AreEqual(StatusSubCode.NoSpace, ex.SubCode, ex.Message);
                Assert.AreEqual(ErrorSeverity.HardError, ex.Severity);
                RocksDbException? bg = db.BackgroundError;
                Assert.IsNotNull(bg);
                Assert.AreEqual(StatusSubCode.NoSpace, bg.SubCode);
                Assert.AreEqual(ErrorSeverity.HardError, bg.Severity);
                ex = Assert.ThrowsExactly<RocksDbException>(() => db.Put(B("b"), B("v-b")));
                Assert.AreEqual(StatusSubCode.NoSpace, ex.SubCode);
                // Still full: the recovery attempts keep failing (and keep the DB stopped).
                Assert.IsFalse(listener.Ended.Wait(TimeSpan.FromMilliseconds(300)), "recovered while the disk is still full");
                Assert.IsNotNull(db.BackgroundError);
                FreeDisk(links);
                Assert.IsTrue(listener.Ended.Wait(Timeout), "recovery did not end");
                Assert.IsTrue(listener.Ends.TryPeek(out BackgroundErrorRecoveryInfo? info));
                Assert.IsNull(info!.NewBackgroundError, info.NewBackgroundError?.Message);
                Assert.IsNull(db.BackgroundError);
                db.Put(B("b"), B("v-b"));
                db.Flush();
                Assert.AreEqual("v-a", GetString(db, "a"));
                Assert.AreEqual("v-b", GetString(db, "b"));
            }
            using (var db = RocksDatabase.Open(dir, new DbOptions()))
            {
                Assert.AreEqual("v-a", GetString(db, "a"));
                Assert.AreEqual("v-b", GetString(db, "b"));
            }
        }
        finally
        {
            DeleteDir(dir);
        }
    }

    /// <summary>The same with the automatic recovery vetoed: Resume fails while the disk is full and succeeds after.</summary>
    [TestMethod]
    public void FlushToFullDiskRecoversOnResume()
    {
        if (!HasDevFull) Assert.Inconclusive("needs /dev/full");
        string dir = TempDir();
        try
        {
            var listener = new RecoveryListener { DisableAutoRecovery = true };
            var o = new DbOptions { CreateIfMissing = true };
            o.Listeners.Add(listener);
            using (var db = RocksDatabase.Open(dir, o))
            {
                db.Put(B("a"), B("v-a"));
                List<string> links = FillDisk(dir, ".sst", 64);
                RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
                Assert.AreEqual(StatusSubCode.NoSpace, ex.SubCode, ex.Message);
                Assert.AreEqual(ErrorSeverity.HardError, ex.Severity);
                ex = Assert.ThrowsExactly<RocksDbException>(() => db.Resume());
                Assert.AreEqual(StatusSubCode.NoSpace, ex.SubCode, ex.Message);
                Assert.IsNotNull(db.BackgroundError);
                FreeDisk(links);
                db.Resume();
                Assert.IsNull(db.BackgroundError);
                db.Put(B("b"), B("v-b"));
            }
            using (var db = RocksDatabase.Open(dir, new DbOptions()))
            {
                Assert.AreEqual("v-a", GetString(db, "a"));
                Assert.AreEqual("v-b", GetString(db, "b"));
            }
        }
        finally
        {
            DeleteDir(dir);
        }
    }

    /// <summary>
    /// A WAL write that hits ENOSPC (kWriteCallback + kNoSpace -> kHardError): the write fails, later writes fail, and the
    /// DB recovers on its own (to a new WAL) once space is free. Every acknowledged write survives a reopen.
    /// </summary>
    [TestMethod]
    public void WalWriteToFullDiskRecoversAutomaticallyOnceSpaceIsFree()
    {
        if (!HasDevFull) Assert.Inconclusive("needs /dev/full");
        string dir = TempDir();
        try
        {
            using var sfm = new SstFileManager(LocalFileSystem.Instance, 0, 0.25, 64UL << 20) { ErrorRecoveryPollInterval = TimeSpan.FromMilliseconds(50) };
            var listener = new RecoveryListener();
            var o = new DbOptions { CreateIfMissing = true, SstFileManager = sfm };
            o.Listeners.Add(listener);
            using (var db = RocksDatabase.Open(dir, o))
            {
                db.Put(B("a"), B("v-a"));
                // The flush switches to a new WAL, which is one of the links.
                List<string> links = FillDisk(dir, ".log", 64);
                db.Flush();
                RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.Put(B("lost"), new byte[256 << 10]));
                Assert.AreEqual(StatusSubCode.NoSpace, ex.SubCode, ex.Message);
                RocksDbException? bg = db.BackgroundError;
                Assert.IsNotNull(bg);
                Assert.AreEqual(StatusSubCode.NoSpace, bg.SubCode);
                Assert.AreEqual(ErrorSeverity.HardError, bg.Severity);
                ex = Assert.ThrowsExactly<RocksDbException>(() => db.Put(B("b"), B("v-b")));
                Assert.AreEqual(StatusSubCode.NoSpace, ex.SubCode);
                FreeDisk(links);
                // A recovery that ran before the links were removed may have opened its new WAL on the full device (the
                // open succeeds, like on a real full disk): the next write then fails again and starts another recovery.
                PutWhenRecovered(db, "b", "v-b");
                Assert.IsNull(db.BackgroundError);
                Assert.AreEqual("v-a", GetString(db, "a"));
                Assert.AreEqual("v-b", GetString(db, "b"));
            }
            using (var db = RocksDatabase.Open(dir, new DbOptions()))
            {
                Assert.AreEqual("v-a", GetString(db, "a"));
                Assert.AreEqual("v-b", GetString(db, "b"));
            }
        }
        finally
        {
            DeleteDir(dir);
        }
    }
}
