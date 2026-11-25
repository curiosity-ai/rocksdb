using System.Collections.Concurrent;
using System.Diagnostics;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.TestShared;
using RocksDb.Managed.Tests.Db;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #15, the error contract Mosaik depends on: the managed-only half (the native differentials are in
/// <c>RocksDb.Managed.CompatTests/Audit15/Audit15MosaikErrorsCompatTests.cs</c>).
/// </summary>
[TestClass]
public sealed class Audit15MosaikErrorsTests
{
    /// <summary>Records the preallocation block of every writable file the DB creates.</summary>
    private sealed class PreallocationRecordingFileSystem(MemoryFileSystem inner) : IFileSystem
    {
        public ConcurrentQueue<(string Path, long Block)> Created { get; } = new();

        public IWritableFile CreateWritable(string path, bool truncate = true)
        {
            Created.Enqueue((path, 0));
            return inner.CreateWritable(path, truncate);
        }

        public IWritableFile CreateWritable(string path, bool truncate, WritableFileOptions options)
        {
            Created.Enqueue((path, options.PreallocationBlockSize));
            return inner.CreateWritable(path, truncate, options);
        }

        public IWritableFile ReopenWritable(string path, WritableFileOptions options) => inner.ReopenWritable(path, options);
        public IRandomAccessFile OpenRandomAccess(string path) => inner.OpenRandomAccess(path);
        public ISequentialFile OpenSequential(string path) => inner.OpenSequential(path);
        public bool FileExists(string path) => inner.FileExists(path);
        public long GetFileSize(string path) => inner.GetFileSize(path);
        public void DeleteFile(string path) => inner.DeleteFile(path);
        public void RenameFile(string source, string target) => inner.RenameFile(source, target);
        public void CreateDirectoryIfMissing(string path) => inner.CreateDirectoryIfMissing(path);
        public IEnumerable<string> GetChildren(string directory) => inner.GetChildren(directory);
        public IDisposable LockFile(string path) => inner.LockFile(path);
        public void SyncDirectory(string path) => inner.SyncDirectory(path);
        public bool DirectoryExists(string path) => inner.DirectoryExists(path);
        public void DeleteDirectory(string path) => inner.DeleteDirectory(path);
        public long GetFreeSpace(string path) => inner.GetFreeSpace(path);

        public long BlockOf(string fileName)
        {
            long? block = null;
            foreach ((string path, long b) in Created)
            {
                if (Path.GetFileName(path) == fileName) block = b;
            }
            Assert.IsNotNull(block, fileName + " was not created");
            return block.Value;
        }

        public List<string> CreatedNames(string extension)
        {
            var names = new List<string>();
            foreach ((string path, _) in Created)
            {
                if (path.EndsWith(extension, StringComparison.Ordinal)) names.Add(Path.GetFileName(path));
            }
            return names;
        }
    }

    /// <summary>
    /// The files upstream preallocates (PosixWritableFile::PrepareWrite, FSWritableFile::SetPreallocationBlockSize) with
    /// Mosaik's options: a WAL in blocks of DBImpl::GetWalPreallocateBlockSize (write_buffer_size + 10%, at most
    /// max_total_wal_size and the write buffer manager's size: 1 MiB in the low-resource branch, 64 MiB + 10% in the normal one
    /// with a manager over 70 MiB), the MANIFEST in blocks of manifest_preallocation_size (4 MiB), a compaction output in blocks of
    /// Compaction::OutputFilePreallocationSize (the inputs' size + 10%); a flush output not at all (builder.cc), and
    /// nothing without allow_fallocate. Before the fix no file was preallocated: on a disk that fills up, native kept
    /// writing into the WAL's reserved block and its flush used what the closed WAL gave back, while the managed engine
    /// failed both with NoSpace (Audit15MosaikErrorsCompatTests.WalPreallocationKeepsWritesGoingOnAFullDiskLikeNative,
    /// FlushOnAFullDiskUsesTheReleasedWalPreallocationLikeNative).
    /// </summary>
    [TestMethod]
    [DataRow(true, 32L << 20, 1L << 20)]
    [DataRow(false, 256L << 20, (64L << 20) + (64L << 20) / 10)]
    public void FilesArePreallocatedLikeUpstream(bool lowMemoryMode, long writeBufferManagerBytes, long walBlock)
    {
        var fs = new PreallocationRecordingFileSystem(NewFs());
        var profile = new MosaikProfile("/a15/journal", lowMemoryMode, lowMemoryMode ? 2 : 8, fs, cacheBytes: 16L << 20, writeBufferBytes: writeBufferManagerBytes);
        List<string> walsAtOpen;
        using (RocksDatabase db = profile.Open("/a15/db"))
        {
            walsAtOpen = fs.CreatedNames(".log");
            Assert.AreEqual(1, walsAtOpen.Count);
            Assert.AreEqual(walBlock, fs.BlockOf(walsAtOpen[0]), "the WAL created by the open");
            int manifests = 0;
            foreach ((string path, long block) in fs.Created)
            {
                if (!Path.GetFileName(path).StartsWith("MANIFEST-", StringComparison.Ordinal)) continue;
                Assert.AreEqual(4L << 20, block, path);
                manifests++;
            }
            Assert.AreNotEqual(0, manifests);
            ColumnFamily edges = db.GetColumnFamily("edges");
            for (int round = 0; round < 2; round++)
            {
                for (int i = 0; i < 200; i++) edges.Put(B("key" + i.ToString("D4", System.Globalization.CultureInfo.InvariantCulture)), new byte[100]);
                edges.Flush();
            }
            List<string> wals = fs.CreatedNames(".log");
            Assert.IsTrue(wals.Count >= 3, wals.Count + " WALs");
            foreach (string w in wals) Assert.AreEqual(walBlock, fs.BlockOf(w), w);
            ulong inputs = 0;
            var flushed = new List<string>();
            foreach (LiveFileMetadata f in db.GetLiveFilesMetadata())
            {
                if (f.ColumnFamilyName != "edges") continue;
                inputs += f.Size;
                flushed.Add(Path.GetFileName(f.Name));
            }
            Assert.AreEqual(2, flushed.Count);
            foreach (string f in flushed) Assert.AreEqual(0, fs.BlockOf(f), "flush output " + f);
            edges.CompactRange(null, null); // SafeRocksDb.CompactRange: one compaction of both level-0 files
            string output = Path.GetFileName(db.GetLiveFilesMetadata().Single(f => f.ColumnFamilyName == "edges").Name);
            Assert.AreEqual((long)(inputs + inputs / 10), fs.BlockOf(output), "compaction output " + output);
        }

        var noFallocate = new PreallocationRecordingFileSystem(NewFs());
        var profile2 = new MosaikProfile("/a15/journal", lowMemoryMode, lowMemoryMode ? 2 : 8, noFallocate, cacheBytes: 16L << 20, writeBufferBytes: writeBufferManagerBytes);
        profile2.DbOptions.AllowFallocate = false;
        using (RocksDatabase db = profile2.Open("/a15/db"))
        {
            ColumnFamily edges = db.GetColumnFamily("edges");
            edges.Put(B("a"), B("1"));
            edges.Flush();
        }
        foreach ((string path, long block) in noFallocate.Created) Assert.AreEqual(0, block, path + " without allow_fallocate");
    }

    // RocksDbBase.UpdateStats and Server/src/TemporaryMemoryCapture.cs.
    private static readonly string[] s_mosaikProperties =
    [
        "rocksdb.num-immutable-mem-table", "rocksdb.mem-table-flush-pending", "rocksdb.num-running-flushes", "rocksdb.compaction-pending",
        "rocksdb.num-running-compactions", "rocksdb.background-errors", "rocksdb.cur-size-active-mem-table", "rocksdb.cur-size-all-mem-tables",
        "rocksdb.size-all-mem-tables", "rocksdb.num-entries-active-mem-table", "rocksdb.num-entries-imm-mem-tables",
        "rocksdb.num-deletes-active-mem-table", "rocksdb.num-deletes-imm-mem-tables", "rocksdb.estimate-num-keys",
        "rocksdb.estimate-table-readers-mem", "rocksdb.num-snapshots", "rocksdb.oldest-snapshot-time", "rocksdb.num-live-versions",
        "rocksdb.current-super-version-number", "rocksdb.estimate-live-data-size", "rocksdb.live-sst-files-size",
        "rocksdb.obsolete-sst-files-size", "rocksdb.base-level", "rocksdb.estimate-pending-compaction-bytes",
        "rocksdb.actual-delayed-write-rate", "rocksdb.is-write-stopped", "rocksdb.total-sst-files-size", "rocksdb.num-files-at-level0",
        "rocksdb.block-cache-capacity", "rocksdb.block-cache-usage", "rocksdb.dbstats",
    ];

    /// <summary>
    /// Mosaik's calls (SafeRocksDb, RocksDbBase's flush thread, RocksDbStorage / RocksDbIndexStorage), each wrapped so that
    /// only the exception types Mosaik's handlers expect may leave it: a RocksDbException (any code). Returns the
    /// descriptions of anything else that escaped.
    /// </summary>
    private static void RunMosaikCalls(RocksDatabase db, List<string> escaped, string label)
    {
        void Call(string name, Action a)
        {
            try
            {
                a();
            }
            catch (RocksDbException)
            {
            }
            catch (Exception e)
            {
                escaped.Add(label + ": " + name + " threw " + e.GetType().Name + ": " + e.Message);
            }
        }
        Dictionary<string, ColumnFamily> cfs = MosaikProfile.Handles(db);
        ColumnFamily nodes = cfs["nodes"], edges = cfs["edges"], changelog = cfs["changelog"], queue = cfs["to-index-queue"];
        byte[] value = new byte[300];
        for (int i = 0; i < 40; i++)
        {
            string k = "key" + i.ToString("D4", System.Globalization.CultureInfo.InvariantCulture);
            Call("put", () => nodes.Put(B(k), value));
            Call("write", () =>
            {
                using var wb = new WriteBatch();
                wb.Put(edges, B(k), value).Delete(nodes, B("gone" + k));
                db.Write(null, wb);
            });
            Call("delete", () => queue.Delete(B("01234567" + k)));
            if (i % 10 == 9) Call("flush of a family", () => nodes.Flush());
        }
        Call("flush thread flush", () => db.Flush(new FlushOptions { Wait = true }));
        Call("flush thread properties", () =>
        {
            foreach (string p in s_mosaikProperties)
            {
                _ = db.GetProperty(p);
                foreach (ColumnFamily cf in cfs.Values) _ = cf.GetProperty(p);
            }
        });
        Call("flush thread memory usage", () => MemoryUtil.GetApproximateMemoryUsageByType([db], []));
        Call("get", () => nodes.Get(B("key0007")));
        Call("iterator loop", () =>
        {
            using Iterator it = nodes.NewIterator();
            it.SeekToFirst();
            while (it.Valid) it.Next();
        });
        Call("SafeRocksDb.CompactRange", () => edges.CompactRange(null, null));
        Call("RocksDbIndexStorage compaction", () =>
            queue.CompactRange(new CompactRangeOptions { ExclusiveManualCompaction = false, TargetLevel = -1 }, B("0"), B("z")));
        Call("ClearDeprecatedChangeLog", () =>
        {
            byte[] start = new byte[8], end = new byte[8];
            Array.Fill(end, byte.MaxValue);
            changelog.DeleteFilesInRange(start, end, includeEnd: true);
            changelog.DeleteRange(new WriteOptions(), start, end);
        });
        Call("checkpoint", () => db.CreateCheckpoint("/a15/checkpoint-" + label, 0));
        Call("live files", () => db.GetLiveFileNames());
    }

    /// <summary>
    /// A transient I/O error (plain, retryable, out of space) at every mutating file operation of a Mosaik-shaped
    /// workload in turn (writes, family flushes, the flush thread's calls, both CompactRange shapes, DeleteFilesInRange +
    /// DeleteRange, a checkpoint, the close): Mosaik catches RocksDbException only (SafeRocksDb.TryRecoverException, the
    /// flush thread, SafeRocksDb.CompactRange), so any other exception type ends a Mosaik thread or request where native
    /// returns a status. Nothing else escapes.
    /// </summary>
    [TestMethod]
    [Timeout(120_000)]
    public void MosaikCallsThrowOnlyRocksDbExceptionUnderFileErrors()
    {
        var escaped = new List<string>();
        long total;
        {
            var fs = new FaultInjectionFileSystem(NewFs());
            using RocksDatabase db = new MosaikProfile("/a15/journal", lowMemoryMode: true, 2, fs, cacheBytes: 8L << 20, writeBufferBytes: 32L << 20).Open("/a15/db");
            long start = fs.Operations;
            RunMosaikCalls(db, escaped, "clean");
            total = fs.Operations - start;
        }
        Assert.AreEqual(0, escaped.Count, string.Join("\n", escaped));
        InjectedFaultKind[] kinds = [InjectedFaultKind.IOError, InjectedFaultKind.Retryable, InjectedFaultKind.NoSpace];
        var sw = Stopwatch.StartNew();
        int runs = 0;
        // Every operation once (+ the close), cut short at 60 s on a slow machine (the operations are visited in a
        // stride so a cut-short run still spreads over the whole workload).
        const int Stride = 7;
        for (int phase = 0; phase < Stride && sw.Elapsed < TimeSpan.FromSeconds(60); phase++)
        {
            for (long n = 1 + phase; n <= total + 8 && sw.Elapsed < TimeSpan.FromSeconds(60); n += Stride)
            {
                var fs = new FaultInjectionFileSystem(NewFs());
                RocksDatabase db = new MosaikProfile("/a15/journal", lowMemoryMode: true, 2, fs, cacheBytes: 8L << 20, writeBufferBytes: 32L << 20).Open("/a15/db");
                fs.ErrorKind = kinds[n % kinds.Length];
                fs.TransientFailure = true;
                fs.CrashAtOperation = fs.Operations + n;
                string label = "op " + n + " (" + fs.ErrorKind + ")";
                RunMosaikCalls(db, escaped, label);
                try
                {
                    db.Dispose();
                }
                catch (RocksDbException)
                {
                }
                catch (Exception e)
                {
                    escaped.Add(label + ": close threw " + e.GetType().Name + ": " + e.Message);
                }
                runs++;
            }
        }
        Console.WriteLine(runs + " runs over " + total + " operations");
        Assert.AreEqual(0, escaped.Count, string.Join("\n", escaped));
    }

    /// <summary>
    /// io_posix.cc IOError(): ENOENT is PathNotFound, ENOTDIR (a component of the path is a regular file) a plain IOError
    /// "Not a directory". .NET throws DirectoryNotFoundException for both on Unix, and the port reported ENOTDIR as
    /// PathNotFound, on which Mosaik's SafeRocksDb.TryRecoverException fails fast where the default build rethrows
    /// (Audit15MosaikErrorsCompatTests.JournalFolderReplacedByAFileGetsTheSameMosaikReactionsAsNative).
    /// </summary>
    [TestMethod]
    public void NotADirectoryIsAPlainIOErrorAndAMissingDirectoryIsPathNotFound()
    {
        if (OperatingSystem.IsWindows()) Assert.Inconclusive("ENOTDIR is a POSIX errno (Windows reports ERROR_PATH_NOT_FOUND for both)");
        string dir = Path.Combine(Path.GetTempPath(), "a15-enotdir-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            string file = Path.Combine(dir, "journal");
            File.WriteAllBytes(file, [1]);
            string missing = Path.Combine(dir, "missing");
            Action[] underFile =
            [
                () => LocalFileSystem.Instance.CreateWritable(Path.Combine(file, "000006.log"), truncate: true, WritableFileOptions.Default).Dispose(),
                () => LocalFileSystem.Instance.OpenRandomAccess(Path.Combine(file, "000007.sst")).Dispose(),
                () => LocalFileSystem.Instance.CreateDirectoryIfMissing(Path.Combine(file, "archive")),
                () => LocalFileSystem.Instance.DeleteFile(Path.Combine(file, "000006.log")),
            ];
            foreach (Action a in underFile)
            {
                RocksDbException e = Assert.ThrowsExactly<RocksDbException>(a);
                Assert.AreEqual(StatusCode.IOError, e.Code, e.Message);
                Assert.AreEqual(StatusSubCode.None, e.SubCode, e.Message);
                StringAssert.Contains(e.Message, "Not a directory");
            }
            Action[] underMissing =
            [
                () => LocalFileSystem.Instance.CreateWritable(Path.Combine(missing, "000006.log"), truncate: true, WritableFileOptions.Default).Dispose(),
                () => LocalFileSystem.Instance.OpenRandomAccess(Path.Combine(missing, "000007.sst")).Dispose(),
                () => LocalFileSystem.Instance.OpenRandomAccess(Path.Combine(dir, "000008.sst")).Dispose(),
            ];
            foreach (Action a in underMissing)
            {
                RocksDbException e = Assert.ThrowsExactly<RocksDbException>(a);
                Assert.AreEqual(StatusSubCode.PathNotFound, e.SubCode, e.Message);
            }
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    /// <summary>The blocks a file has allocated (stat %b, 512-byte units), or -1 when stat is not available.</summary>
    private static long AllocatedBlocks(string path)
    {
        try
        {
            using Process p = Process.Start(new ProcessStartInfo("stat", new[] { "-c", "%b", path }) { RedirectStandardOutput = true, RedirectStandardError = true })!;
            string o = p.StandardOutput.ReadToEnd();
            p.WaitForExit();
            return p.ExitCode == 0 && long.TryParse(o.Trim(), out long b) ? b : -1;
        }
        catch (System.ComponentModel.Win32Exception)
        {
            return -1;
        }
    }

    /// <summary>
    /// PosixWritableFile::PrepareWrite / Close on the local file system: the first append reserves a whole block past
    /// the end of the file (fallocate with FALLOC_FL_KEEP_SIZE, the size does not change), and the close releases what
    /// was not written (ftruncate, then FALLOC_FL_PUNCH_HOLE for file systems where ftruncate to the same size keeps
    /// the blocks).
    /// </summary>
    [TestMethod]
    public void ClosingAPreallocatedFileReleasesTheUnusedBlocks()
    {
        if (!OperatingSystem.IsLinux()) Assert.Inconclusive("fallocate is Linux only");
        string dir = Path.Combine(Path.GetTempPath(), "a15-prealloc-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            string path = Path.Combine(dir, "000001.log");
            IWritableFile f = LocalFileSystem.Instance.CreateWritable(path, truncate: true, new WritableFileOptions { PreallocationBlockSize = 4L << 20 });
            f.Append(new byte[1000]);
            f.Flush();
            long reserved = AllocatedBlocks(path);
            if (reserved < 0) Assert.Inconclusive("stat is not available");
            if (reserved < (4L << 20) / 512) Assert.Inconclusive("the file system does not reserve blocks with fallocate (" + reserved + " blocks)");
            Assert.AreEqual(1000, new FileInfo(path).Length, "KEEP_SIZE does not move the end of the file");
            f.Dispose();
            Assert.AreEqual(1000, new FileInfo(path).Length);
            Assert.IsTrue(AllocatedBlocks(path) <= 64, "after the close the file keeps " + AllocatedBlocks(path) + " blocks");
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }
}
