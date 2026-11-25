using System.Diagnostics;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.TestShared;
using RocksDb.Managed.Utilities.Backup;

namespace RocksDb.Managed.CompatTests.Audit15;

/// <summary>
/// Audit #15, the error contract Mosaik's storage code depends on, side by side with native 11.12 (rocksdb-sharp, which
/// Mosaik's default build calls). Mosaik reacts to an engine error by its exception type and status
/// (<c>Mosaik/Graph/src/Graph/Storage/SafeRocksDb.cs</c> TryRecoverException: the managed build tests SubCode NoSpace /
/// PathNotFound and Code Corruption, the default build the message text; <c>RocksDbBase.cs</c> FlushRocksDBMemory: both
/// builds catch only a RocksDbException whose message contains "No space left", anything else ends the flush thread
/// and the process). Each test drives the same fault on a database opened with Mosaik's options on both engines and
/// compares, call for call, what Mosaik's code would do with the outcome.
/// </summary>
/// <remarks>
/// A full disk is a size-limited tmpfs filled with a ballast file (needs root; the test is inconclusive otherwise): the
/// /dev/full links of OutOfSpaceTests are not faithful for native, whose fdatasync on the character device fails with
/// EINVAL, which replaces the out-of-space background error with a plain IOError no real disk returns.
/// </remarks>
[TestClass]
[DoNotParallelize]
public sealed class Audit15MosaikErrorsCompatTests
{
    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);

    private static readonly TimeSpan RecoveryTimeout = TimeSpan.FromSeconds(45);

    // ---- Mosaik's reactions ---------------------------------------------------------------------------------------------

    /// <summary>The flush thread: only a RocksDbException whose message contains "No space left" is caught (both builds).</summary>
    private static string FlushThreadReaction(Exception? e) => e switch
    {
        null => "ok",
        RocksDbException r when r.Message.Contains("No space left", StringComparison.Ordinal) => "ignored",
        RocksDbSharp.RocksDbException r when r.Message.Contains("No space left", StringComparison.Ordinal) => "ignored",
        _ => "thread dies",
    };

    /// <summary>SafeRocksDb.CompactRange (managed build) swallows RocksDbException; rocksdb-sharp's CompactRange discards the status.</summary>
    private static string CompactRangeReaction(Exception? e) => e switch
    {
        null or RocksDbException or RocksDbSharp.RocksDbException => "ok",
        _ => "escapes " + e.GetType().Name,
    };

    /// <summary>A call Mosaik does not wrap (Get, iterators, DeleteRange, backups): the caller sees the exception or not.</summary>
    private static string PlainReaction(Exception? e) => e switch
    {
        null => "ok",
        RocksDbException r => "throws " + r.Code,
        RocksDbSharp.RocksDbException r => "throws " + NativeCode(r.Message),
        _ => "escapes " + e.GetType().Name,
    };

    /// <summary>The status code native's Status::ToString prefix names (status.cc).</summary>
    private static string NativeCode(string message) =>
        message.StartsWith("Corruption: ", StringComparison.Ordinal) ? nameof(StatusCode.Corruption)
        : message.StartsWith("NotFound: ", StringComparison.Ordinal) ? nameof(StatusCode.NotFound)
        : message.StartsWith("IO error: ", StringComparison.Ordinal) ? nameof(StatusCode.IOError)
        : message.StartsWith("Invalid argument: ", StringComparison.Ordinal) ? nameof(StatusCode.InvalidArgument)
        : message.StartsWith("Not implemented: ", StringComparison.Ordinal) ? nameof(StatusCode.NotSupported)
        : message.StartsWith("Result incomplete: ", StringComparison.Ordinal) ? nameof(StatusCode.Incomplete)
        : message.StartsWith("Shutdown in progress", StringComparison.Ordinal) ? nameof(StatusCode.ShutdownInProgress)
        : message.StartsWith("Operation failed. Try again.", StringComparison.Ordinal) ? nameof(StatusCode.TryAgain)
        : message.StartsWith("Resource busy", StringComparison.Ordinal) ? nameof(StatusCode.Busy)
        : message.StartsWith("Operation aborted", StringComparison.Ordinal) ? nameof(StatusCode.Aborted)
        : "unknown(" + message + ")";

    private static Exception? Catch(Action a)
    {
        try
        {
            a();
            return null;
        }
        catch (Exception e)
        {
            return e;
        }
    }

    private sealed record Step(string Name, string Reaction, string Detail);

    /// <summary>The scenario's record of one engine: what Mosaik's code does with each call's outcome.</summary>
    private sealed class Log(IMosaikDb db)
    {
        public List<Step> Steps { get; } = [];
        public void Write(string name, Exception? e) => Steps.Add(new Step(name, db.WriteReaction(e), db.Describe(e)));
        public void FlushThread(string name, Exception? e) => Steps.Add(new Step(name, FlushThreadReaction(e), db.Describe(e)));
        public void Compact(string name, Exception? e) => Steps.Add(new Step(name, CompactRangeReaction(e), db.Describe(e)));
        public void Plain(string name, Exception? e) => Steps.Add(new Step(name, PlainReaction(e), db.Describe(e)));
        public void Value(string name, string value) => Steps.Add(new Step(name, value, value));
        /// <summary>
        /// GraphStorage.Close -> RocksDbBase.Dispose -> SafeRocksDb.Dispose (which catches only SEHException):
        /// rocksdb-sharp's Dispose is rocksdb_close (delete db: ~DBImpl drops CloseHelper's status); the managed Dispose
        /// throws DB::Close's status.
        /// </summary>
        public void Close(string name)
        {
            Exception? e = db.Close();
            Steps.Add(new Step(name, PlainReaction(e), db.Describe(e)));
        }
    }

    /// <summary>
    /// Fails unless every step got the same reaction on both engines, except the steps listed in
    /// <paramref name="knownDeviations"/>, whose managed reaction must be the one given (and differ from native's).
    /// </summary>
    private static void AssertSameReactions(List<Step> managed, List<Step> native, (string Step, string ManagedReaction)[]? knownDeviations = null)
    {
        var report = new StringBuilder();
        for (int i = 0; i < Math.Max(managed.Count, native.Count); i++)
        {
            Step? m = i < managed.Count ? managed[i] : null;
            Step? n = i < native.Count ? native[i] : null;
            report.Append(m?.Name ?? n?.Name).Append(": managed ").Append(m?.Reaction).Append(" [").Append(m?.Detail).Append("]  native ")
                .Append(n?.Reaction).Append(" [").Append(n?.Detail).Append(']').AppendLine();
        }
        Console.WriteLine(report.ToString());
        Assert.AreEqual(native.Count, managed.Count, report.ToString());
        for (int i = 0; i < native.Count; i++)
        {
            Assert.AreEqual(native[i].Name, managed[i].Name, report.ToString());
            string? deviation = null;
            foreach ((string step, string reaction) in knownDeviations ?? [])
            {
                if (step == native[i].Name) deviation = reaction;
            }
            if (deviation is null)
            {
                Assert.AreEqual(native[i].Reaction, managed[i].Reaction, native[i].Name + "\n" + report);
            }
            else
            {
                Assert.AreEqual(deviation, managed[i].Reaction, native[i].Name + " (known deviation)\n" + report);
                Assert.AreNotEqual(deviation, native[i].Reaction, native[i].Name + " (known deviation no longer differs)\n" + report);
            }
        }
    }

    // ---- the two engines behind Mosaik's call shapes ------------------------------------------------------------------------

    /// <summary>The calls Mosaik makes, each returning the exception it threw (null when it returned).</summary>
    private interface IMosaikDb : IDisposable
    {
        Exception? Put(string cf, byte[] key, byte[] value);
        Exception? Write(string cf, byte[] key, byte[] value);
        Exception? Delete(string cf, byte[] key);
        /// <summary>RocksDbBase.FlushRocksDBMemory: Flush(FlushOptions { Wait = true }) of the default family.</summary>
        Exception? FlushDefault();
        Exception? Flush(string cf);
        /// <summary>SafeRocksDb.CompactRange(null, null, cf).</summary>
        Exception? CompactRange(string cf);
        Exception? Get(string cf, byte[] key, out byte[]? value);
        /// <summary><c>SeekToFirst(); while (it.Valid()) { ...; it.Next(); }</c></summary>
        Exception? Scan(string cf, out int count);
        string Property(string name);
        /// <summary>SafeRocksDb.Put / Write's reaction to the exception.</summary>
        string WriteReaction(Exception? e);
        string Describe(Exception? e);
        /// <summary>Closes the DB (once; Dispose afterwards does nothing): what its Dispose threw.</summary>
        Exception? Close();
        /// <summary>RocksDbBase.Backup: open the engine, CreateNewBackup(flushBeforeBackup: true), PurgeOldBackups(2), GetBackupInfo.</summary>
        Exception? Backup(string dir, out int backups);
        /// <summary>RocksDbStorage / SafeRocksDb.Checkpoint().Save(dir).</summary>
        Exception? Checkpoint(string dir);
    }

    private sealed class ManagedDb(RocksDatabase db) : IMosaikDb
    {
        private static readonly FlushOptions s_wait = new() { Wait = true };

        public static ManagedDb Open(string path, string wal, bool lowMemoryMode) =>
            new(new MosaikProfile(wal, lowMemoryMode, lowMemoryMode ? 2 : 8, cacheBytes: 16L << 20, writeBufferBytes: 32L << 20).Open(path));

        private ColumnFamily Cf(string name) => db.GetColumnFamily(name);
        public Exception? Put(string cf, byte[] key, byte[] value) => Catch(() => Cf(cf).Put(key, value));
        public Exception? Write(string cf, byte[] key, byte[] value) => Catch(() =>
        {
            using var wb = new Db.WriteBatch();
            wb.Put(Cf(cf), key, value);
            db.Write(null, wb);
        });
        public Exception? Delete(string cf, byte[] key) => Catch(() => Cf(cf).Delete(key));
        public Exception? FlushDefault() => Catch(() => db.Flush(s_wait));
        public Exception? Flush(string cf) => Catch(() => Cf(cf).Flush(s_wait));
        public Exception? CompactRange(string cf) => Catch(() => Cf(cf).CompactRange(null, null));
        public Exception? Get(string cf, byte[] key, out byte[]? value)
        {
            byte[]? v = null;
            Exception? e = Catch(() => v = Cf(cf).Get(key));
            value = v;
            return e;
        }
        public Exception? Scan(string cf, out int count)
        {
            int c = 0;
            Exception? e = Catch(() =>
            {
                using Iterator it = Cf(cf).NewIterator();
                it.SeekToFirst();
                while (it.Valid)
                {
                    c++;
                    it.Next();
                }
            });
            count = c;
            return e;
        }
        public string Property(string name) => db.GetProperty(name) ?? "null";
        public Exception? Backup(string dir, out int backups)
        {
            int n = -1;
            Exception? e = Catch(() =>
            {
                using BackupEngine be = BackupEngine.Open(new BackupEngineOptions(dir));
                be.CreateNewBackup(db, flushBeforeBackup: true);
                be.PurgeOldBackups(2);
                n = be.GetBackupInfo().Count;
            });
            backups = n;
            return e;
        }
        public Exception? Checkpoint(string dir) => Catch(() => db.CreateCheckpoint(dir, 0));
        public string WriteReaction(Exception? e) => e switch
        {
            null => "ok",
            RocksDbException { SubCode: StatusSubCode.NoSpace } => "retry",
            RocksDbException { SubCode: StatusSubCode.PathNotFound } => "failfast",
            RocksDbException { Code: StatusCode.Corruption } => "failfast",
            RocksDbException => "rethrow",
            _ => "escapes " + e.GetType().Name,
        };
        public string Describe(Exception? e) => e switch
        {
            null => "OK",
            RocksDbException r => r.Code + "/" + r.SubCode + "/" + r.Severity + " " + r.Message,
            _ => e.GetType().Name + " " + e.Message,
        };
        private bool _closed;
        public Exception? Close()
        {
            if (_closed) return null;
            _closed = true;
            return Catch(db.Dispose);
        }
        public void Dispose() => Close();
    }

    private sealed class NativeDb(RocksDbSharp.RocksDb db) : IMosaikDb
    {
        private static readonly RocksDbSharp.Native N = RocksDbSharp.Native.Instance;

        public static NativeDb Open(string path, string wal, bool lowMemoryMode) =>
            new(new NativeMosaikProfile(wal, lowMemoryMode, lowMemoryMode ? 2 : 8, cacheBytes: 16L << 20, writeBufferBytes: 32L << 20).Open(path));

        private RocksDbSharp.ColumnFamilyHandle Cf(string name) => db.GetColumnFamily(name);
        public Exception? Put(string cf, byte[] key, byte[] value) => Catch(() => db.Put(key, value, Cf(cf)));
        public Exception? Write(string cf, byte[] key, byte[] value) => Catch(() =>
        {
            using var wb = new RocksDbSharp.WriteBatch();
            wb.Put(key, value, Cf(cf));
            db.Write(wb);
        });
        public Exception? Delete(string cf, byte[] key) => Catch(() => db.Remove(key, Cf(cf)));
        // RocksDbBase: Native.Instance.rocksdb_flush(db.Handle, flushOptions), the throwing wrapper.
        public Exception? FlushDefault() => Catch(() => db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true)));
        public Exception? Flush(string cf) => Catch(() => N.rocksdb_flush_cf(db.Handle, new RocksDbSharp.FlushOptions().SetWaitForFlush(true).Handle, Cf(cf).Handle));
        public Exception? CompactRange(string cf) => Catch(() => db.CompactRange((byte[]?)null, null, Cf(cf)));
        public Exception? Get(string cf, byte[] key, out byte[]? value)
        {
            byte[]? v = null;
            Exception? e = Catch(() => v = db.Get(key, Cf(cf)));
            value = v;
            return e;
        }
        public Exception? Scan(string cf, out int count)
        {
            int c = 0;
            Exception? e = Catch(() =>
            {
                using RocksDbSharp.Iterator it = db.NewIterator(Cf(cf));
                it.SeekToFirst();
                while (it.Valid())
                {
                    c++;
                    it.Next();
                }
            });
            count = c;
            return e;
        }
        public string Property(string name) => db.GetProperty(name) ?? "null";
        public Exception? Backup(string dir, out int backups)
        {
            int n = -1;
            Exception? e = Catch(() =>
            {
                IntPtr be = N.rocksdb_backup_engine_open(new RocksDbSharp.DbOptions().Handle, dir);
                try
                {
                    N.rocksdb_backup_engine_create_new_backup_flush(be, db.Handle, 1);
                    N.rocksdb_backup_engine_purge_old_backups(be, 2);
                    IntPtr info = N.rocksdb_backup_engine_get_backup_info(be);
                    n = N.rocksdb_backup_engine_info_count(info);
                    N.rocksdb_backup_engine_info_destroy(info);
                }
                finally
                {
                    N.rocksdb_backup_engine_close(be);
                }
            });
            backups = n;
            return e;
        }
        public Exception? Checkpoint(string dir) => Catch(() =>
        {
            using RocksDbSharp.Checkpoint cp = db.Checkpoint();
            cp.Save(dir, 0);
        });
        public string WriteReaction(Exception? e) => e switch
        {
            null => "ok",
            RocksDbSharp.RocksDbException r when r.Message.StartsWith("IO error: No space left on device", StringComparison.Ordinal) => "retry",
            RocksDbSharp.RocksDbException r when r.Message.Contains("No such file or directory", StringComparison.Ordinal) => "failfast",
            RocksDbSharp.RocksDbException r when r.Message.Contains("Corruption", StringComparison.Ordinal) => "failfast",
            RocksDbSharp.RocksDbException => "rethrow",
            _ => "escapes " + e.GetType().Name,
        };
        public string Describe(Exception? e) => e?.GetType().Name is { } t && e is not RocksDbSharp.RocksDbException ? t + " " + e.Message : e?.Message ?? "OK";
        private bool _closed;
        public Exception? Close()
        {
            if (_closed) return null;
            _closed = true;
            return Catch(db.Dispose);
        }
        public void Dispose() => Close();
    }

    private delegate IMosaikDb Opener(string path, string wal, bool lowMemoryMode);

    private static readonly Opener s_managed = ManagedDb.Open;
    private static readonly Opener s_native = NativeDb.Open;

    /// <summary>Retries <paramref name="put"/> until it succeeds or the timeout passes; the last outcome.</summary>
    private static Exception? PutWhenRecovered(Func<Exception?> put)
    {
        var sw = Stopwatch.StartNew();
        Exception? last;
        while ((last = put()) is not null && sw.Elapsed < RecoveryTimeout) Thread.Sleep(50);
        return last;
    }

    // ---- a small file system that can be filled ------------------------------------------------------------------------

    /// <summary>A size-limited tmpfs mounted on a temporary directory; <see cref="Fill"/> leaves no free block.</summary>
    private sealed class SmallFileSystem : IDisposable
    {
        public string Path { get; }
        private string Ballast => System.IO.Path.Combine(Path, "ballast");

        private SmallFileSystem(string path) => Path = path;

        public static SmallFileSystem? TryMount(long megabytes)
        {
            if (!OperatingSystem.IsLinux()) return null;
            string path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "a15-tmpfs-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(path);
            if (Run("mount", "-t tmpfs -o size=" + megabytes + "m tmpfs " + path) == 0) return new SmallFileSystem(path);
            Directory.Delete(path);
            return null;
        }

        private static int Run(string file, string args)
        {
            try
            {
                using Process p = Process.Start(new ProcessStartInfo(file, args) { RedirectStandardError = true, RedirectStandardOutput = true })!;
                p.WaitForExit();
                return p.ExitCode;
            }
            catch (System.ComponentModel.Win32Exception)
            {
                return -1;
            }
        }

        /// <summary>Writes the ballast until the file system has no free block left.</summary>
        public void Fill()
        {
            using var f = new FileStream(Ballast, FileMode.Append, FileAccess.Write, FileShare.None, 1, FileOptions.None);
            foreach (int chunk in new[] { 1 << 20, 4096 })
            {
                byte[] b = new byte[chunk];
                while (true)
                {
                    try
                    {
                        f.Write(b);
                        f.Flush();
                    }
                    catch (IOException)
                    {
                        break;
                    }
                }
            }
        }

        public void Free() => File.Delete(Ballast);

        public void Dispose()
        {
            Run("umount", "-l " + Path);
            try { Directory.Delete(Path, recursive: true); }
            catch (IOException) { }
        }
    }

    private static string Sh(string cmd)
    {
        using Process p = Process.Start(new ProcessStartInfo("/bin/sh", new[] { "-c", cmd }) { RedirectStandardOutput = true, RedirectStandardError = true })!;
        string o = p.StandardOutput.ReadToEnd() + p.StandardError.ReadToEnd();
        p.WaitForExit();
        return o;
    }

    private static SmallFileSystem MountOrInconclusive(long megabytes) =>
        SmallFileSystem.TryMount(megabytes) ?? throw new AssertInconclusiveException("mounting a tmpfs needs root (CAP_SYS_ADMIN)");

    // The SstFileManager of each DB recovers a hard out-of-space error once the free space reaches the largest
    // write_buffer_size (64 MiB, ReserveDiskBuffer at DB::Open), so the file system is larger than that.
    private const long SmallFsMegabytes = 160;

    // ---- out of space ----------------------------------------------------------------------------------------------

    /// <summary>
    /// A WAL append hits ENOSPC, in both resource branches (the normal one also writes its info LOG at Info level to the
    /// full disk; its put is larger than the WAL's 32 MiB preallocation block): the failing Put; Put, Write and Delete
    /// while the DB is stopped; the flush thread's Flush of the (empty) default family during the stop; the automatic
    /// recovery once the space is freed (ErrorHandler::RecoverFromNoSpace through the SstFileManager); the flush thread
    /// after it.
    /// </summary>
    [TestMethod]
    [Timeout(180_000)]
    [DataRow(true, 4)]
    [DataRow(false, 40)]
    public void WalOutOfSpaceGetsTheSameMosaikReactionsAsNative(bool lowMemoryMode, int putMegabytes)
    {
        List<Step> Run(Opener open)
        {
            using SmallFileSystem fs = MountOrInconclusive(SmallFsMegabytes);
            string db = Path.Combine(fs.Path, "db"), wal = Path.Combine(fs.Path, "journal");
            using IMosaikDb d = open(db, wal, lowMemoryMode);
            var log = new Log(d);
            Assert.IsNull(d.Put("nodes", U("a"), U("v-a")));
            fs.Fill();
            log.Write("put (wal append fails)", d.Put("nodes", U("lost"), new byte[putMegabytes << 20]));
            log.Write("put (db stopped)", d.Put("nodes", U("b"), U("v-b")));
            log.Write("write (db stopped)", d.Write("edges", U("c"), U("v-c")));
            log.Write("delete (db stopped)", d.Delete("nodes", U("a")));
            log.FlushThread("flush thread (db stopped)", d.FlushDefault());
            log.Compact("compact range (db stopped)", d.CompactRange("nodes"));
            log.Plain("get (db stopped)", d.Get("nodes", U("a"), out _));
            fs.Free();
            log.Write("put after the space is freed", PutWhenRecovered(() => d.Put("nodes", U("b"), U("v-b"))));
            log.FlushThread("flush thread (recovered)", d.FlushDefault());
            log.Plain("get (recovered)", d.Get("nodes", U("b"), out byte[]? v));
            log.Value("value (recovered)", v is null ? "null" : Encoding.UTF8.GetString(v));
            log.Close("close");
            return log.Steps;
        }
        AssertSameReactions(Run(s_managed), Run(s_native));
    }

    /// <summary>
    /// The disk is still full when Mosaik shuts down (a WAL append failed, the DB is stopped). Known deviation: native's
    /// rocksdb_close (delete db) drops CloseHelper's status (the WAL buffer that cannot be written out), the managed
    /// Dispose throws it, so Mosaik's GraphStorage.Close stops half way (SafeRocksDb.Dispose catches only SEHException).
    /// </summary>
    [TestMethod]
    [Timeout(120_000)]
    public void CloseOnAFullDiskGetsTheSameMosaikReactionsAsNative()
    {
        List<Step> Run(Opener open)
        {
            using SmallFileSystem fs = MountOrInconclusive(SmallFsMegabytes);
            string db = Path.Combine(fs.Path, "db"), wal = Path.Combine(fs.Path, "journal");
            using IMosaikDb d = open(db, wal, lowMemoryMode: true);
            var log = new Log(d);
            Assert.IsNull(d.Put("nodes", U("a"), U("v-a")));
            fs.Fill();
            log.Write("put (wal append fails)", d.Put("nodes", U("lost"), new byte[4 << 20]));
            log.Close("close (disk still full)");
            return log.Steps;
        }
        AssertSameReactions(Run(s_managed), Run(s_native), [("close (disk still full)", "throws IOError")]);
    }

    /// <summary>
    /// The disk fills with a small memtable in the WAL (low-resource branch): the flush still succeeds on both engines,
    /// because the memtable switch closes the WAL and that releases what is left of its preallocation block
    /// (PosixWritableFile::Close; native closes it in SyncClosedWals before the flush job writes its table file), which
    /// is room for the table file.
    /// </summary>
    [TestMethod]
    [Timeout(180_000)]
    public void FlushOnAFullDiskUsesTheReleasedWalPreallocationLikeNative()
    {
        List<Step> Run(Opener open)
        {
            using SmallFileSystem fs = MountOrInconclusive(SmallFsMegabytes);
            string db = Path.Combine(fs.Path, "db"), wal = Path.Combine(fs.Path, "journal");
            using IMosaikDb d = open(db, wal, lowMemoryMode: true);
            var log = new Log(d);
            for (int i = 0; i < 200; i++) Assert.IsNull(d.Put("edges", U("k" + i), new byte[1000]));
            fs.Fill();
            log.Write("flush of a family", d.Flush("edges"));
            log.Write("put after the flush", d.Put("nodes", U("b"), U("v-b")));
            log.FlushThread("flush thread", d.FlushDefault());
            log.Close("close");
            return log.Steps;
        }
        AssertSameReactions(Run(s_managed), Run(s_native));
    }

    /// <summary>
    /// A flush hits ENOSPC writing its table file (kFlush + NoSpace = HardError; the memtable's incompressible values need
    /// more room than its WAL's preallocation gives back): the failing flush of a family, writes during the stop, the
    /// flush thread's call, and the recovery once space is freed.
    /// </summary>
    [TestMethod]
    [Timeout(180_000)]
    public void FlushOutOfSpaceGetsTheSameMosaikReactionsAsNative()
    {
        List<Step> Run(Opener open)
        {
            using SmallFileSystem fs = MountOrInconclusive(SmallFsMegabytes);
            string db = Path.Combine(fs.Path, "db"), wal = Path.Combine(fs.Path, "journal");
            using IMosaikDb d = open(db, wal, lowMemoryMode: true);
            var log = new Log(d);
            var random = new Random(15);
            byte[] value = new byte[1000];
            // 880 KB: below max_total_wal_size (1 MiB), so no write switches the WAL on its own.
            for (int i = 0; i < 880; i++)
            {
                random.NextBytes(value);
                Assert.IsNull(d.Put("edges", U("k" + i), value));
            }
            fs.Fill();
            log.Write("flush of a family (table write fails)", d.Flush("edges"));
            log.Write("put (db stopped)", d.Put("nodes", U("b"), U("v-b")));
            log.Write("write (db stopped)", d.Write("edges", U("c"), U("v-c")));
            log.FlushThread("flush thread (db stopped)", d.FlushDefault());
            log.Compact("compact range (db stopped)", d.CompactRange("edges"));
            fs.Free();
            log.Write("put after the space is freed", PutWhenRecovered(() => d.Put("nodes", U("b"), U("v-b"))));
            log.FlushThread("flush thread (recovered)", d.FlushDefault());
            log.Write("flush of the family (recovered)", d.Flush("edges"));
            log.Plain("get (recovered)", d.Get("edges", U("k7"), out byte[]? v));
            log.Value("value length (recovered)", v?.Length.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "null");
            log.Close("close");
            return log.Steps;
        }
        AssertSameReactions(Run(s_managed), Run(s_native));
    }


    /// <summary>
    /// A manual compaction (Mosaik's CompactRange threads) hits ENOSPC (kCompaction + NoSpace = SoftError): CompactRange
    /// reports it, the next write fails on the full WAL, the flush thread's call, and the state after the space is freed.
    /// </summary>
    [TestMethod]
    [Timeout(180_000)]
    public void CompactionOutOfSpaceGetsTheSameMosaikReactionsAsNative()
    {
        List<Step> Run(Opener open)
        {
            using SmallFileSystem fs = MountOrInconclusive(SmallFsMegabytes);
            string db = Path.Combine(fs.Path, "db"), wal = Path.Combine(fs.Path, "journal");
            using IMosaikDb d = open(db, wal, lowMemoryMode: true);
            var log = new Log(d);
            for (int round = 0; round < 3; round++)
            {
                for (int i = 0; i < 100; i++) Assert.IsNull(d.Put("edges", U("k" + i.ToString("D4", System.Globalization.CultureInfo.InvariantCulture)), new byte[2000]));
                Assert.IsNull(d.Flush("edges"));
            }
            fs.Fill();
            log.Compact("compact range (output fails)", d.CompactRange("edges"));
            log.Value("background-errors after the compaction", d.Property("rocksdb.background-errors") == "0" ? "0" : "> 0");
            log.Write("put", d.Put("nodes", U("b"), U("v-b")));
            log.FlushThread("flush thread", d.FlushDefault());
            fs.Free();
            log.Write("put after the space is freed", PutWhenRecovered(() => d.Put("nodes", U("c"), U("v-c"))));
            log.Compact("compact range after the space is freed", d.CompactRange("edges"));
            log.Plain("scan (recovered)", d.Scan("edges", out int count));
            log.Value("keys (recovered)", count.ToString(System.Globalization.CultureInfo.InvariantCulture));
            log.Close("close");
            return log.Steps;
        }
        AssertSameReactions(Run(s_managed), Run(s_native));
    }

    /// <summary>
    /// The disk fills after a WAL's first append: native reserved the WAL's preallocation block with fallocate at that
    /// append (PosixWritableFile::PrepareWrite, block size DBImpl::GetWalPreallocateBlockSize = max_total_wal_size, 1 MiB,
    /// in the low-resource branch), so the writes that fit in it still succeed; the first one past it fails with NoSpace.
    /// </summary>
    [TestMethod]
    [Timeout(180_000)]
    public void WalPreallocationKeepsWritesGoingOnAFullDiskLikeNative()
    {
        List<Step> Run(Opener open)
        {
            using SmallFileSystem fs = MountOrInconclusive(SmallFsMegabytes);
            string db = Path.Combine(fs.Path, "db"), wal = Path.Combine(fs.Path, "journal");
            using IMosaikDb d = open(db, wal, lowMemoryMode: true);
            var log = new Log(d);
            Assert.IsNull(d.Put("nodes", U("a"), U("v-a")));
            fs.Fill();
            for (int i = 0; i < 5; i++) log.Write("put of 100 KiB #" + i + " (inside the WAL's preallocation)", d.Put("nodes", U("k" + i), new byte[100 << 10]));
            log.Write("put of 2 MiB (past the preallocation)", d.Put("nodes", U("big"), new byte[2 << 20]));
            fs.Free();
            log.Write("put after the space is freed", PutWhenRecovered(() => d.Put("nodes", U("b"), U("v-b"))));
            log.Close("close");
            return log.Steps;
        }
        AssertSameReactions(Run(s_managed), Run(s_native));
    }

    // ---- missing files and directories ------------------------------------------------------------------------------

    /// <summary>
    /// The journal folder (Mosaik's separate wal_dir) is removed while the DB runs: the open WAL keeps working, and the
    /// first write that switches the WAL (max_total_wal_size, 1 MiB in the low-resource branch) fails to create the new
    /// one with ENOENT (PathNotFound: Mosaik fails fast on both builds).
    /// </summary>
    [TestMethod]
    [Timeout(120_000)]
    public void RemovedJournalFolderGetsTheSameMosaikReactionsAsNative()
    {
        List<Step> Run(Opener open)
        {
            using var dir = new TempDir("a15-nojournal");
            string db = Path.Combine(dir.Path, "db"), wal = Path.Combine(dir.Path, "journal");
            using IMosaikDb d = open(db, wal, lowMemoryMode: true);
            var log = new Log(d);
            Assert.IsNull(d.Put("nodes", U("a"), U("v-a")));
            Directory.Delete(wal, recursive: true);
            log.Write("put into the open WAL", d.Put("nodes", U("k1"), new byte[600 << 10]));
            log.Write("put into the open WAL (now over max_total_wal_size)", d.Put("nodes", U("k2"), new byte[600 << 10]));
            log.Write("put that switches the WAL", d.Put("edges", U("k3"), U("v")));
            log.Write("put after the failed switch", d.Put("nodes", U("k4"), U("v")));
            log.FlushThread("flush thread", d.FlushDefault());
            log.Plain("get", d.Get("nodes", U("a"), out _));
            log.Close("close");
            return log.Steps;
        }
        AssertSameReactions(Run(s_managed), Run(s_native));
    }

    /// <summary>
    /// The database folder is removed while the DB runs: the next flush cannot create its table file (ENOENT ->
    /// PathNotFound, kFlush: a fatal background error), writes then fail with it, and the close cannot verify the
    /// MANIFEST (VersionSet::Close writes a new one, which fails): native's rocksdb_close drops that status, the managed
    /// Dispose throws it (known deviation: Mosaik's GraphStorage.Close then stops half way).
    /// </summary>
    [TestMethod]
    [Timeout(120_000)]
    public void RemovedDatabaseFolderGetsTheSameMosaikReactionsAsNative()
    {
        List<Step> Run(Opener open)
        {
            using var dir = new TempDir("a15-nodb");
            string db = Path.Combine(dir.Path, "db"), wal = Path.Combine(dir.Path, "journal");
            using IMosaikDb d = open(db, wal, lowMemoryMode: true);
            var log = new Log(d);
            Assert.IsNull(d.Put("nodes", U("a"), U("v-a")));
            Directory.Delete(db, recursive: true);
            log.Write("put", d.Put("nodes", U("b"), U("v-b")));
            log.Write("flush of a family", d.Flush("nodes"));
            log.Write("put after the failed flush", d.Put("nodes", U("c"), U("v-c")));
            log.Write("write after the failed flush", d.Write("edges", U("c"), U("v-c")));
            log.FlushThread("flush thread", d.FlushDefault());
            log.Compact("compact range", d.CompactRange("nodes"));
            log.Plain("get", d.Get("nodes", U("a"), out _));
            log.Close("close");
            return log.Steps;
        }
        AssertSameReactions(Run(s_managed), Run(s_native), [("close", "throws IOError")]);
    }

    /// <summary>Writes 300 keys to <paramref name="cf"/> and flushes them; returns the new table file.</summary>
    private static string WriteTableFile(IMosaikDb d, string db, string cf)
    {
        var before = new HashSet<string>(Directory.GetFiles(db, "*.sst"));
        for (int i = 0; i < 300; i++) Assert.IsNull(d.Put(cf, U("key" + i.ToString("D5", System.Globalization.CultureInfo.InvariantCulture)), new byte[200]));
        Assert.IsNull(d.Flush(cf));
        return Directory.GetFiles(db, "*.sst").Single(f => !before.Contains(f));
    }

    /// <summary>
    /// A table file disappears (deleted behind the DB's back) while the DB is closed, and the DB is opened again (the
    /// low-resource branch: max_open_files 8, so the open loads no more than two readers, those of the first families,
    /// and does not look at the others): the read that opens the file fails with PathNotFound, an iterator over it
    /// throws on the managed engine where native's turns invalid (the scan ends silently), a compaction that needs it
    /// fails, and the writes after it.
    /// </summary>
    [TestMethod]
    [Timeout(120_000)]
    public void VanishedTableFileGetsTheSameMosaikReactionsAsNative()
    {
        List<Step> Run(Opener open, bool managed)
        {
            using var dir = new TempDir("a15-nosst");
            string db = Path.Combine(dir.Path, "db"), wal = Path.Combine(dir.Path, "journal");
            string nodesFile;
            using (IMosaikDb first = open(db, wal, lowMemoryMode: true))
            {
                // The families before "nodes" in Mosaik's order take the readers the open loads.
                foreach (string cf in new[] { "nodeschemas", "edgeschemas", "types" }) WriteTableFile(first, db, cf);
                WriteTableFile(first, db, "nodes");
                nodesFile = WriteTableFile(first, db, "nodes");
            }
            File.Delete(nodesFile);
            using IMosaikDb d = open(db, wal, lowMemoryMode: true);
            var log = new Log(d);
            log.Plain("get", d.Get("nodes", U("key00001"), out _));
            Exception? scan = d.Scan("nodes", out int count);
            // Native's iterator turns invalid at the error: the scan counts no key and ends without one.
            log.Value("scan", managed ? PlainReaction(scan) : (scan is null && count == 0 ? "throws IOError" : PlainReaction(scan) + " " + count));
            log.Compact("compact range", d.CompactRange("nodes"));
            log.Write("put", d.Put("nodes", U("b"), U("v-b")));
            log.FlushThread("flush thread", d.FlushDefault());
            log.Close("close");
            return log.Steps;
        }
        AssertSameReactions(Run(s_managed, managed: true), Run(s_native, managed: false));
    }

    // ---- corruption ------------------------------------------------------------------------------------------------

    /// <summary>
    /// A data block of a table file is damaged on disk (block checksum mismatch): Get fails with Corruption, an iterator
    /// throws on the managed engine where native's turns invalid, a compaction that reads it is an unrecoverable
    /// background error (kCompaction + Corruption with paranoid_checks), and writes then fail with Corruption (Mosaik
    /// fails fast), as does the flush thread.
    /// </summary>
    [TestMethod]
    [Timeout(120_000)]
    public void CorruptTableBlockGetsTheSameMosaikReactionsAsNative()
    {
        List<Step> Run(Opener open, bool managed)
        {
            using var dir = new TempDir("a15-corrupt");
            string db = Path.Combine(dir.Path, "db"), wal = Path.Combine(dir.Path, "journal");
            string nodesFile;
            using (IMosaikDb first = open(db, wal, lowMemoryMode: true))
            {
                // Two overlapping files: the compaction has to read both (no trivial move).
                WriteTableFile(first, db, "nodes");
                nodesFile = WriteTableFile(first, db, "nodes");
            }
            using (FileStream f = File.Open(nodesFile, FileMode.Open, FileAccess.ReadWrite))
            {
                f.Position = 64;
                byte[] garbage = new byte[64];
                for (int i = 0; i < garbage.Length; i++) garbage[i] = (byte)(0xA5 ^ i);
                f.Write(garbage);
            }
            using IMosaikDb d = open(db, wal, lowMemoryMode: true);
            var log = new Log(d);
            log.Plain("get", d.Get("nodes", U("key00001"), out _));
            Exception? scan = d.Scan("nodes", out int count);
            log.Value("scan", managed ? PlainReaction(scan) : (scan is null && count == 0 ? "throws Corruption" : PlainReaction(scan) + " " + count));
            log.Compact("compact range", d.CompactRange("nodes"));
            log.Write("put", d.Put("nodes", U("b"), U("v-b")));
            log.Write("write", d.Write("edges", U("b"), U("v-b")));
            log.FlushThread("flush thread", d.FlushDefault());
            log.Close("close");
            return log.Steps;
        }
        AssertSameReactions(Run(s_managed, managed: true), Run(s_native, managed: false));
    }

    // ---- backups and checkpoints ---------------------------------------------------------------------------------------

    /// <summary>RocksDbBase.RestoreFrom (managed build): restore the latest (index &lt; 0) or a given backup, with the separate journal folder.</summary>
    private static Exception? ManagedRestore(string backupDir, string dbDir, string walDir, int backupIndex) => Catch(() =>
    {
        using BackupEngine be = BackupEngine.Open(new BackupEngineOptions(backupDir));
        if (backupIndex < 0) be.RestoreDbFromLatestBackup(dbDir, walDir, new RestoreOptions());
        else be.RestoreDbFromBackup((uint)backupIndex, dbDir, walDir, new RestoreOptions());
    });

    /// <summary>RocksDbBase.RestoreFrom (default build): rocksdb_backup_engine_restore_db_from_[latest_]backup.</summary>
    private static Exception? NativeRestore(string backupDir, string dbDir, string walDir, int backupIndex) => Catch(() =>
    {
        RocksDbSharp.Native n = RocksDbSharp.Native.Instance;
        IntPtr be = n.rocksdb_backup_engine_open(new RocksDbSharp.DbOptions().Handle, backupDir);
        IntPtr ro = n.rocksdb_restore_options_create();
        try
        {
            if (backupIndex < 0) n.rocksdb_backup_engine_restore_db_from_latest_backup(be, dbDir, walDir, ro);
            else n.rocksdb_backup_engine_restore_db_from_backup(be, dbDir, walDir, ro, (uint)backupIndex);
        }
        finally
        {
            n.rocksdb_backup_engine_close(be);
            n.rocksdb_restore_options_destroy(ro);
        }
    });

    /// <summary>
    /// Mosaik's backup and restore and a checkpoint, with the backup folder on a full disk, with no backup to restore, with
    /// a backup id that does not exist, and with a database whose writes are stopped by an out-of-space error; then a
    /// successful backup, restored into a new database with a separate journal folder. Same reaction on both engines.
    /// </summary>
    [TestMethod]
    [Timeout(180_000)]
    public void BackupAndCheckpointFailuresGetTheSameMosaikReactionsAsNative()
    {
        List<Step> Run(Opener open, Func<string, string, string, int, Exception?> restore)
        {
            using var dir = new TempDir("a15-backup");
            using SmallFileSystem fs = MountOrInconclusive(SmallFsMegabytes);
            string db = Path.Combine(dir.Path, "db"), wal = Path.Combine(dir.Path, "journal");
            string backups = Path.Combine(fs.Path, "backups");
            using IMosaikDb d = open(db, wal, lowMemoryMode: true);
            var log = new Log(d);
            for (int i = 0; i < 100; i++) Assert.IsNull(d.Put("nodes", U("k" + i), new byte[500]));
            log.Plain("restore latest from an empty folder", restore(Path.Combine(dir.Path, "none"), Path.Combine(dir.Path, "r0"), Path.Combine(dir.Path, "r0-journal"), -1));
            fs.Fill();
            log.Plain("backup to a full disk", d.Backup(backups, out _));
            log.Plain("checkpoint to a full disk", d.Checkpoint(Path.Combine(fs.Path, "checkpoint")));
            fs.Free();
            log.Plain("backup", d.Backup(backups, out int count));
            log.Value("backups", count.ToString(System.Globalization.CultureInfo.InvariantCulture));
            log.Plain("restore a backup id that does not exist", restore(backups, Path.Combine(dir.Path, "r1"), Path.Combine(dir.Path, "r1-journal"), 99));
            log.Plain("restore latest", restore(backups, Path.Combine(dir.Path, "r2"), Path.Combine(dir.Path, "r2-journal"), -1));
            using (IMosaikDb restored = open(Path.Combine(dir.Path, "r2"), Path.Combine(dir.Path, "r2-journal"), lowMemoryMode: true))
            {
                log.Plain("get from the restored database", restored.Get("nodes", U("k7"), out byte[]? v));
                log.Value("value length", v?.Length.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "null");
                Assert.IsNull(restored.Close());
            }
            // The database's own disk is not the full one here: stop it with a missing journal folder instead.
            Directory.Delete(wal, recursive: true);
            for (int i = 0; i < 3; i++) d.Put("nodes", U("big" + i), new byte[600 << 10]);
            log.Write("put (db stopped)", d.Put("nodes", U("x"), U("y")));
            log.Plain("backup of a stopped database (flush before backup)", d.Backup(backups, out _));
            log.Plain("checkpoint of a stopped database", d.Checkpoint(Path.Combine(dir.Path, "checkpoint2")));
            log.Close("close");
            return log.Steps;
        }
        AssertSameReactions(Run(s_managed, ManagedRestore), Run(s_native, NativeRestore));
    }

    // ---- properties ------------------------------------------------------------------------------------------------

    // RocksDbBase.UpdateStats, RocksDbBase / SafeRocksDb debug output and Server/src/TemporaryMemoryCapture.cs.
    private static readonly string[] s_mosaikProperties =
    [
        "rocksdb.num-immutable-mem-table", "rocksdb.mem-table-flush-pending", "rocksdb.num-running-flushes",
        "rocksdb.compaction-pending", "rocksdb.num-running-compactions", "rocksdb.background-errors",
        "rocksdb.cur-size-active-mem-table", "rocksdb.cur-size-all-mem-tables", "rocksdb.size-all-mem-tables",
        "rocksdb.num-entries-active-mem-table", "rocksdb.num-entries-imm-mem-tables", "rocksdb.num-deletes-active-mem-table",
        "rocksdb.num-deletes-imm-mem-tables", "rocksdb.estimate-num-keys", "rocksdb.estimate-table-readers-mem",
        "rocksdb.num-snapshots", "rocksdb.oldest-snapshot-time", "rocksdb.num-live-versions",
        "rocksdb.current-super-version-number", "rocksdb.estimate-live-data-size", "rocksdb.live-sst-files-size",
        "rocksdb.obsolete-sst-files-size", "rocksdb.base-level", "rocksdb.estimate-pending-compaction-bytes",
        "rocksdb.actual-delayed-write-rate", "rocksdb.is-write-stopped", "rocksdb.total-sst-files-size",
        "rocksdb.num-files-at-level0", "rocksdb.block-cache-capacity", "rocksdb.block-cache-usage",
        "rocksdb.block-cache-pinned-usage", "rocksdb.dbstats", "rocksdb.stats", "rocksdb.sstables",
        "rocksdb.no-such-property", "rocksdb.num-files-at-level99", "",
    ];

    private static string Shape(Func<string?> get)
    {
        try
        {
            string? v = get();
            if (v is null) return "null";
            if (long.TryParse(v, out _)) return "long";
            return v.Length == 0 ? "empty" : "text";
        }
        catch (Exception e)
        {
            return "throws " + e.GetType().Name;
        }
    }

    /// <summary>
    /// Every property name Mosaik reads, asked of the DB and of four families (default, a plain family, "content" and the
    /// change tracker with its custom comparator), DB-wide names per family included, and unknown names: null, a number
    /// (Mosaik parses with long.TryParse) or text on both engines alike, never an exception.
    /// </summary>
    [TestMethod]
    public void MosaikPropertyNamesAnswerLikeNative()
    {
        using var dir = new TempDir("a15-props");
        string[] families = ["default", "nodes", "content", MosaikProfile.ReplicaChangeTrackerFamily];
        var mismatches = new List<string>();
        var managed = new MosaikProfile(Path.Combine(dir.Path, "mwal"), lowMemoryMode: true, processorCount: 2, cacheBytes: 16L << 20, writeBufferBytes: 32L << 20);
        var native = new NativeMosaikProfile(Path.Combine(dir.Path, "nwal"), lowMemoryMode: true, processorCount: 2, cacheBytes: 16L << 20, writeBufferBytes: 32L << 20);
        using (RocksDatabase m = managed.Open(Path.Combine(dir.Path, "m")))
        using (RocksDbSharp.RocksDb n = native.Open(Path.Combine(dir.Path, "n")))
        {
            foreach (string fam in new[] { "nodes", "content" })
            {
                for (int i = 0; i < 50; i++)
                {
                    m.GetColumnFamily(fam).Put(U("key" + i), U("value" + i));
                    n.Put(U("key" + i), U("value" + i), n.GetColumnFamily(fam));
                }
                m.GetColumnFamily(fam).Flush();
                RocksDbSharp.Native.Instance.rocksdb_flush_cf(n.Handle, new RocksDbSharp.FlushOptions().SetWaitForFlush(true).Handle, n.GetColumnFamily(fam).Handle);
                m.GetColumnFamily(fam).Put(U("key-unflushed"), U("v"));
                n.Put(U("key-unflushed"), U("v"), n.GetColumnFamily(fam));
            }
            foreach (string p in s_mosaikProperties)
            {
                string md = Shape(() => m.GetProperty(p));
                string nd = Shape(() => n.GetProperty(p));
                if (md != nd) mismatches.Add("db " + p + ": managed " + md + ", native " + nd);
                foreach (string fam in families)
                {
                    string mf = Shape(() => m.GetColumnFamily(fam).GetProperty(p));
                    string nf = Shape(() => n.GetProperty(p, n.GetColumnFamily(fam)));
                    if (mf != nf) mismatches.Add(fam + " " + p + ": managed " + mf + ", native " + nf);
                }
            }
        }
        Assert.AreEqual(0, mismatches.Count, string.Join("\n", mismatches));
    }
}
