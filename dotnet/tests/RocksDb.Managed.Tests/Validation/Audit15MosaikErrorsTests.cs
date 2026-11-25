using System.Collections.Concurrent;
using System.Diagnostics;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.TestShared;
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
