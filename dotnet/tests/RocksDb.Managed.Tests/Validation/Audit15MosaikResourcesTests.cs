using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.TestShared;
using RocksDb.Managed.Utilities.Backup;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #15, memory, resources and background scheduling with Mosaik's options (<see cref="MosaikProfile"/>): the WAL
/// archive that Mosaik's RocksDbBase.CleanOldWalArchive empties behind the engine, and the background job limits of
/// Mosaik's two resource branches. The differential half is RocksDb.Managed.CompatTests.Audit15.
/// </summary>
[TestClass]
public sealed class Audit15MosaikResourcesTests
{
    public TestContext TestContext { get; set; } = null!;

    private sealed class TempDirectory : IDisposable
    {
        public string Path { get; } = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "a15res-" + Guid.NewGuid().ToString("N"));
        public TempDirectory() => Directory.CreateDirectory(Path);
        public void Dispose()
        {
            try { Directory.Delete(Path, recursive: true); } catch (IOException) { } catch (UnauthorizedAccessException) { }
        }
    }

    /// <summary>
    /// The local file system, except that while <see cref="Armed"/> a listing of a WAL archive directory deletes every
    /// archived WAL right after it was listed: what Mosaik's CleanOldWalArchive does when it runs between the engine's
    /// listing and its per-file calls.
    /// </summary>
    private sealed class ArchiveCleaningFileSystem(IFileSystem inner) : IFileSystem
    {
        public volatile bool Armed;
        public int Deleted;

        public IEnumerable<string> GetChildren(string directory)
        {
            List<string> children = new(inner.GetChildren(directory));
            if (Armed && directory.TrimEnd('/').EndsWith("/archive", StringComparison.Ordinal))
            {
                foreach (string c in children)
                {
                    if (!c.EndsWith(".log", StringComparison.Ordinal)) continue;
                    inner.DeleteFile(directory.TrimEnd('/') + "/" + c);
                    Interlocked.Increment(ref Deleted);
                }
            }
            return children;
        }

        public IWritableFile CreateWritable(string path, bool truncate = true) => inner.CreateWritable(path, truncate);
        public IWritableFile CreateWritable(string path, bool truncate, WritableFileOptions options) => inner.CreateWritable(path, truncate, options);
        public IWritableFile ReopenWritable(string path, WritableFileOptions options) => inner.ReopenWritable(path, options);
        public IWritableFile ReuseWritable(string path, string oldPath, WritableFileOptions options) => inner.ReuseWritable(path, oldPath, options);
        public IRandomAccessFile OpenRandomAccess(string path) => inner.OpenRandomAccess(path);
        public IRandomAccessFile OpenRandomAccess(string path, RandomAccessFileOptions options) => inner.OpenRandomAccess(path, options);
        public ISequentialFile OpenSequential(string path) => inner.OpenSequential(path);
        public ISequentialFile OpenSequential(string path, SequentialFileOptions options) => inner.OpenSequential(path, options);
        public bool FileExists(string path) => inner.FileExists(path);
        public long GetFileSize(string path) => inner.GetFileSize(path);
        public void DeleteFile(string path) => inner.DeleteFile(path);
        public void RenameFile(string source, string target) => inner.RenameFile(source, target);
        public void CreateDirectoryIfMissing(string path) => inner.CreateDirectoryIfMissing(path);
        public IDisposable LockFile(string path) => inner.LockFile(path);
        public void SyncDirectory(string path) => inner.SyncDirectory(path);
        public bool DirectoryExists(string path) => inner.DirectoryExists(path);
        public void DeleteDirectory(string path) => inner.DeleteDirectory(path);
        public void LinkFile(string source, string target) => inner.LinkFile(source, target);
        public DateTimeOffset GetFileModificationTime(string path) => inner.GetFileModificationTime(path);
        public long GetFreeSpace(string path) => inner.GetFreeSpace(path);
        public void WriteAt(string path, long offset, ReadOnlySpan<byte> data) => inner.WriteAt(path, offset, data);
    }

    private static byte[] Key(int i) => BitConverter.GetBytes((long)i).Concat(new byte[8]).ToArray();

    /// <summary>Writes to two families and flushes both: the WAL that held the data is archived (WAL_ttl_seconds > 0).</summary>
    private static void WriteAndArchive(Dictionary<string, ColumnFamily> cf, int round)
    {
        for (int i = 0; i < 200; i++)
        {
            cf["nodes"].Put(Key(round * 1000 + i), new byte[100]);
            cf["content"].Put(Key(round * 1000 + i), new byte[300]);
        }
        cf["nodes"].Flush();
        cf["content"].Flush();
    }

    /// <summary>
    /// Mosaik archives every obsolete WAL (WAL_ttl_seconds of 100 years, WAL_size_limit_MB 0) and deletes archived files
    /// itself, from its flush thread, while the engine may be listing the archive: for WAL tailing, a checkpoint
    /// (GetLiveFilesStorageInfo), a backup, and the engine's own archive purge (WalManager::PurgeObsoleteWALFiles every
    /// min(600 s, ttl / 2)). A file that vanishes between the listing and the engine's next call on it is skipped, as
    /// upstream skips it in PurgeObsoleteWALFiles and in ReadFirstRecord (deviation, more tolerant than upstream:
    /// WalManager::GetSortedWalsOfType returns the failed GetFileSize of an archived file as the listing's error).
    /// </summary>
    [TestMethod]
    [Timeout(60_000, CooperativeCancellation = true)]
    public void ArchivedWalsDeletedBehindTheEngineFailNothing()
    {
        using var dir = new TempDirectory();
        var fs = new ArchiveCleaningFileSystem(LocalFileSystem.Instance);
        var profile = new MosaikProfile(dir.Path + "/wal", lowMemoryMode: true, processorCount: 2, fs, cacheBytes: 8L << 20, writeBufferBytes: 32L << 20);
        using RocksDatabase db = profile.Open(dir.Path + "/db");
        Dictionary<string, ColumnFamily> cf = MosaikProfile.Handles(db);

        WriteAndArchive(cf, 1);
        Assert.IsTrue(Directory.GetFiles(dir.Path + "/wal/archive", "*.log").Length > 0, "nothing archived");
        fs.Armed = true;
        IReadOnlyList<WalFileInfo> wals = db.GetSortedWalFiles();
        foreach (WalFileInfo w in wals) Assert.AreEqual(WalFileType.AliveLogFile, w.Type, "an archived WAL deleted while listing is not reported");

        WriteAndArchive(cf, 2);
        db.CreateCheckpoint(dir.Path + "/checkpoint");
        using (RocksDatabase cp = RocksDatabase.Open(dir.Path + "/checkpoint", new MosaikProfile(dir.Path + "/checkpoint", true, 2).DbOptions,
                   new MosaikProfile(dir.Path + "/checkpoint", true, 2).Descriptors))
        {
            Assert.IsNotNull(cp.GetColumnFamily("nodes").Get(Key(2000)));
        }

        WriteAndArchive(cf, 3);
        using (BackupEngine backup = BackupEngine.Open(new BackupEngineOptions(dir.Path + "/backup")))
        {
            backup.CreateNewBackup(db, flushBeforeBackup: true);
            Assert.AreEqual(1, backup.GetBackupInfo().Count);
        }

        WriteAndArchive(cf, 4);
        db.Impl.PurgeObsoleteWalFiles(force: true);
        Assert.IsTrue(fs.Deleted >= 4, "the archive was not listed by every step: " + fs.Deleted);
        fs.Armed = false;
        Assert.IsNotNull(cf["content"].Get(Key(4000)));
    }

    /// <summary>
    /// WalManager::PurgeObsoleteWALFiles with Mosaik's 100-year WAL_ttl_seconds: the purge runs every
    /// min(kDefaultIntervalToDeleteObsoleteWAL = 600 s, max(1, ttl / 2)) = 600 s, and no archived WAL is old enough to be
    /// deleted, including one whose modification time is ahead of the clock (upstream skips it rather than letting
    /// now_seconds - file_m_time wrap; Audit15MosaikResourcesCompatTests.ArchivedWalStampedInTheFutureIsKeptLikeNative).
    /// </summary>
    [TestMethod]
    [Timeout(60_000, CooperativeCancellation = true)]
    public void HundredYearTtlKeepsEveryArchivedWal()
    {
        Assert.AreEqual(600, DbImpl.WalPurgeIntervalSeconds(MosaikProfile.WalArchiveTtlNeverSeconds));
        Assert.AreEqual(1, DbImpl.WalPurgeIntervalSeconds(1));
        Assert.AreEqual(600, DbImpl.WalPurgeIntervalSeconds(ulong.MaxValue));
        using var dir = new TempDirectory();
        var profile = new MosaikProfile(dir.Path + "/wal", lowMemoryMode: false, processorCount: 8, cacheBytes: 8L << 20, writeBufferBytes: 256L << 20);
        using RocksDatabase db = profile.Open(dir.Path + "/db");
        Dictionary<string, ColumnFamily> cf = MosaikProfile.Handles(db);
        WriteAndArchive(cf, 1);
        WriteAndArchive(cf, 2);
        string[] archived = Directory.GetFiles(dir.Path + "/wal/archive", "*.log");
        Assert.IsTrue(archived.Length >= 2, "archived: " + archived.Length);
        File.SetLastWriteTimeUtc(archived[0], DateTime.UtcNow.AddDays(1));
        File.SetLastWriteTimeUtc(archived[1], new DateTime(1971, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        db.Impl.PurgeObsoleteWalFiles(force: true);
        CollectionAssert.AreEquivalent(archived, Directory.GetFiles(dir.Path + "/wal/archive", "*.log"));
    }

    private sealed class FlushConcurrencyListener : EventListener
    {
        private int _running;
        public int MaxRunning;

        public override void OnFlushBegin(RocksDatabase? db, FlushJobInfo info)
        {
            int now = Interlocked.Increment(ref _running);
            int max;
            while (now > (max = Volatile.Read(ref MaxRunning)) && Interlocked.CompareExchange(ref MaxRunning, now, max) != max) { }
        }

        public override void OnFlushCompleted(RocksDatabase? db, FlushJobInfo info) => Interlocked.Decrement(ref _running);
    }

    /// <summary>
    /// DBImpl::GetBGJobLimits for Mosaik's branches. Low resources: max_background_jobs max(2, processors / 4) with the
    /// explicit max_background_flushes 2 and max_background_compactions max(1, jobs - 1), so two flushes at a time.
    /// Normal: max_background_jobs max(2, processors / 2) with both explicit limits at -1, so a quarter of the jobs (at
    /// least one) flush: one at a time below 8 processors, two at 8 to 11. With every family's flush requested at once
    /// (what a SwitchWAL round does), no more flushes than that run together.
    /// </summary>
    [TestMethod]
    [Timeout(60_000, CooperativeCancellation = true)]
    [DataRow(true, 2, 2)]
    [DataRow(false, 4, 1)]
    [DataRow(false, 8, 1)]
    [DataRow(false, 16, 2)]
    public void FlushConcurrencyStaysWithinMosaiksJobLimits(bool lowMemoryMode, int processors, int maxFlushes)
    {
        using var dir = new TempDirectory();
        var profile = new MosaikProfile(dir.Path + "/wal", lowMemoryMode, processors, cacheBytes: 8L << 20, writeBufferBytes: 256L << 20);
        var listener = new FlushConcurrencyListener();
        profile.DbOptions.Listeners.Add(listener);
        using RocksDatabase db = profile.Open(dir.Path + "/db");
        Dictionary<string, ColumnFamily> cf = MosaikProfile.Handles(db);
        for (int round = 0; round < 3; round++)
        {
            foreach (ColumnFamily c in cf.Values)
            {
                for (int i = 0; i < 50; i++) c.Put(BitConverter.GetBytes((long)(round * 100 + i)).Concat(new byte[8]).ToArray(), new byte[200]);
            }
            foreach (ColumnFamily c in cf.Values) c.Flush(new FlushOptions { Wait = false });
            db.WaitForCompact();
        }
        TestContext.WriteLine("max concurrent flushes " + listener.MaxRunning + ", limit " + maxFlushes + ", high pool threads " + BackgroundThreadPool.High.ThreadCount);
        Assert.IsTrue(listener.MaxRunning >= 1 && listener.MaxRunning <= maxFlushes, "max concurrent flushes " + listener.MaxRunning);
    }
}
