using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.TestShared;

namespace RocksDb.Managed.CompatTests.Audit15;

/// <summary>
/// Audit #15: Mosaik's concurrent mix (<see cref="MosaikSoak"/>) run on the managed engine, then everything it left on disk
/// opened by native RocksDB with Mosaik's native options (<see cref="NativeMosaikProfile"/>): the database, every
/// checkpoint and every kept backup (restored). Native must see exactly the model's final state, the same point in time
/// for each snapshot, and byte for byte what the managed engine reads from the same files.
/// </summary>
[TestClass]
[DoNotParallelize]
public sealed class Audit15MosaikSoakCompatTests
{
    /// <summary>
    /// Both resource branches; the normal one with Mosaik's write buffer manager floor (64 MiB, a host with less than
    /// 1.3 GiB available). With the content arena block Mosaik used before curiosity-ai/mosaik#2776 that manager was over
    /// its limit from the start (see <see cref="ContentArenaChargesTheWriteBufferManagerAsNativeDoes"/>); with the engine's
    /// default block it is not. The unit test runs the normal branch with a larger write buffer manager.
    /// </summary>
    [TestMethod]
    [DataRow(true, 2)]
    [DataRow(false, 8)]
    public void NativeReadsTheSoakedDatabaseCheckpointsAndBackupsLikeManaged(bool lowMemoryMode, int processorCount)
    {
        using var dir = new TempDir("a15-soak");
        var settings = new SoakSettings
        {
            Root = Path.Combine(dir.Path, "run"),
            LowMemoryMode = lowMemoryMode,
            ProcessorCount = processorCount,
            CacheBytes = lowMemoryMode ? 0 : 64L << 20,
            WriteBufferBytes = 0,
            Seed = SoakSettings.SeedFromEnvironment(),
            Duration = SoakSettings.DurationFromEnvironment(20),
        };
        var soak = new MosaikSoak(settings);
        soak.Run();
        Console.WriteLine(soak.Summary());
        string failures = SoakChecks.RunFailures(soak);
        Assert.AreEqual("", failures, settings + Environment.NewLine + failures);

        // The files as the managed engine closed them, before the managed reopen changes them.
        string nativeDb = Path.Combine(dir.Path, "native-db"), nativeWal = Path.Combine(dir.Path, "native-wal");
        SoakChecks.CopyDirectory(soak.DbPath, nativeDb);
        SoakChecks.CopyDirectory(soak.WalDir, nativeWal);

        var errors = new List<string>();
        var notes = new List<string>();
        SoakDump managed = SoakChecks.ReopenAndCheck(soak, errors, notes);
        SoakDump native = NativeDump(settings, nativeDb, nativeWal);
        SoakVerifier.CheckFinal(soak.M, native, "native open of the soaked database", errors, out _);
        SoakVerifier.CompareDumps(managed, native, "soaked database, managed vs native", errors);

        SoakChecks.CheckSnapshots(soak, errors, notes, (db, wal, rec, managedDump) =>
        {
            string what = "native open of " + rec.Kind + " " + (rec.Kind == "backup" ? rec.BackupId.ToString() : Path.GetFileName(rec.Path));
            SoakDump n = NativeDump(settings, db, wal);
            notes.Add(SoakVerifier.CheckCut(soak.M, n, rec, what, errors));
            SoakVerifier.CompareDumps(managedDump, n, what + ", managed vs native", errors);
        });
        foreach (string n in notes) Console.WriteLine(n);
        Assert.AreEqual(0, errors.Count, settings + Environment.NewLine + string.Join(Environment.NewLine, errors));
    }

    /// <summary>
    /// Mosaik's normal branch gave "content" a 128 MiB arena block before curiosity-ai/mosaik#2776 (RocksDbStorage's family
    /// map; it now uses the engine's default). The first allocation of a
    /// new memtable takes a whole block, and the point-lookup families' memtable blooms (0.02 x 64 MiB each) are
    /// allocated with the memtable, so a database charges about 141 MB to the write buffer manager before it holds any
    /// data, and every new "content" memtable another 128 MiB. Both engines charge the same: with Mosaik's write buffer
    /// manager below about 160 MiB (clamp(available / 20, 64 MiB, 4 GiB)) it is over its limit from the open on, on
    /// native as on managed.
    /// </summary>
    [TestMethod]
    public void ContentArenaChargesTheWriteBufferManagerAsNativeDoes()
    {
        var value = new byte[100];
        long[] managed, native;
        string managedImm, nativeImm;
        using (var dir = new TempDir("a15-wbm-m"))
        {
            var p = new MosaikProfile(Path.Combine(dir.Path, "wal"), lowMemoryMode: false, processorCount: 8, cacheBytes: 64L << 20,
                contentArenaBlockSize: MosaikProfile.ContentArenaBlockSizeBefore2776(lowMemoryMode: false));
            using RocksDatabase db = p.Open(Path.Combine(dir.Path, "db"));
            long open = p.WriteBufferManager.MemoryUsage;
            db.GetColumnFamily("content").Put([1, 2, 3], value);
            long afterContent = p.WriteBufferManager.MemoryUsage;
            for (int i = 0; i < 50; i++) db.GetColumnFamily("nodes").Put([(byte)i, 9], value);
            managed = [open, afterContent, p.WriteBufferManager.MemoryUsage];
            managedImm = db.GetColumnFamily("content").GetProperty("rocksdb.num-immutable-mem-table")!;
        }
        using (var dir = new TempDir("a15-wbm-n"))
        {
            var p = new NativeMosaikProfile(Path.Combine(dir.Path, "wal"), lowMemoryMode: false, processorCount: 8, cacheBytes: 64L << 20,
                contentArenaBlockSize: MosaikProfile.ContentArenaBlockSizeBefore2776(lowMemoryMode: false));
            using RocksDbSharp.RocksDb db = p.Open(Path.Combine(dir.Path, "db"));
            RocksDbSharp.Native n = RocksDbSharp.Native.Instance;
            long open = (long)(ulong)n.rocksdb_write_buffer_manager_memory_usage(p.WriteBufferManager);
            db.Put(new byte[] { 1, 2, 3 }, value, db.GetColumnFamily("content"));
            long afterContent = (long)(ulong)n.rocksdb_write_buffer_manager_memory_usage(p.WriteBufferManager);
            for (int i = 0; i < 50; i++) db.Put(new byte[] { (byte)i, 9 }, value, db.GetColumnFamily("nodes"));
            native = [open, afterContent, (long)(ulong)n.rocksdb_write_buffer_manager_memory_usage(p.WriteBufferManager)];
            nativeImm = db.GetProperty("rocksdb.num-immutable-mem-table", db.GetColumnFamily("content"));
        }
        // The later readings race with the background flush of the switched memtable: printed, not compared.
        Console.WriteLine("write buffer manager usage (open, one content put, 50 node puts): managed " + string.Join("/", managed) + " (content immutables " + managedImm +
            "), native " + string.Join("/", native) + " (content immutables " + nativeImm + ")");
        // Within 0.1%: arena blocks are counted at the requested size by the port, at malloc_usable_size natively.
        Assert.IsTrue(Math.Abs(managed[0] - native[0]) <= native[0] / 1000, "at open: managed " + managed[0] + ", native " + native[0]);
        Assert.IsTrue(managed[0] > 128L << 20, "the content arena block is charged at open: " + managed[0]);
    }

    /// <summary>
    /// Mosaik's CleanOldWalArchive deletes archived WALs behind the engine's back (RocksDbBase.cs:1078). A checkpoint
    /// (and a backup: both go through GetLiveFilesStorageInfo) lists the WALs with GetSortedWalFilesImpl, which includes
    /// the archive; upstream's WalManager::GetSortedWalsOfType returns the GetFileSize error of an archived file deleted
    /// between GetChildren and the size read (only an alive WAL moved to the archive is retried), so the native checkpoint
    /// fails. The port does not list the archive for a checkpoint (only alive WALs are copied, upstream too) and skips an
    /// archived WAL that vanished while listing: its checkpoints never fail this way. Archived WALs are created and deleted
    /// in a loop while checkpoints are taken; native failures are printed (upstream behaviour), managed ones fail the test.
    /// </summary>
    [TestMethod]
    public void CheckpointsWhileArchivedWalsAreDeletedBehindTheEngine()
    {
        string Run(Action<string> checkpoint, string dir, string wal)
        {
            string archive = Path.Combine(wal, "archive");
            Directory.CreateDirectory(archive);
            using var stop = new CancellationTokenSource();
            // About 200 archived files at any time, each deleted and written again in turn: a listing sees many files, and
            // any of them may be gone before its size is read.
            for (int n = 0; n < 200; n++) File.WriteAllBytes(Path.Combine(archive, (10 + n).ToString("D6") + ".log"), new byte[16]);
            var churn = new Thread(() =>
            {
                for (int n = 0; !stop.IsCancellationRequested; n = (n + 1) % 200)
                {
                    string f = Path.Combine(archive, (10 + n).ToString("D6") + ".log");
                    try
                    {
                        File.Delete(f);
                        File.WriteAllBytes(f, new byte[16]);
                    }
                    catch (IOException)
                    {
                    }
                }
            });
            churn.Start();
            int failures = 0, taken = 0;
            string? first = null;
            var sw = System.Diagnostics.Stopwatch.StartNew();
            try
            {
                for (int i = 0; sw.Elapsed < TimeSpan.FromSeconds(3); i++)
                {
                    string cp = Path.Combine(dir, "cp-" + i);
                    try
                    {
                        checkpoint(cp);
                        taken++;
                    }
                    catch (Exception e) when (e is RocksDbSharp.RocksDbException or RocksDbException)
                    {
                        failures++;
                        first ??= e.Message;
                    }
                    if (Directory.Exists(cp)) Directory.Delete(cp, recursive: true);
                }
            }
            finally
            {
                stop.Cancel();
                churn.Join();
            }
            return taken + " checkpoints, " + failures + " failed" + (first is null ? "" : " (first: " + first + ")");
        }

        string managedResult, nativeResult;
        using (var dir = new TempDir("a15-archive-m"))
        {
            string wal = Path.Combine(dir.Path, "wal");
            var p = new MosaikProfile(wal, lowMemoryMode: true, processorCount: 2);
            using RocksDatabase db = p.Open(Path.Combine(dir.Path, "db"));
            db.GetColumnFamily("nodes").Put([1], [2]);
            managedResult = Run(cp => db.CreateCheckpoint(cp, 0), dir.Path, wal);
        }
        using (var dir = new TempDir("a15-archive-n"))
        {
            string wal = Path.Combine(dir.Path, "wal");
            var p = new NativeMosaikProfile(wal, lowMemoryMode: true, processorCount: 2);
            using RocksDbSharp.RocksDb db = p.Open(Path.Combine(dir.Path, "db"));
            db.Put(new byte[] { 1 }, new byte[] { 2 }, db.GetColumnFamily("nodes"));
            nativeResult = Run(cp =>
            {
                using RocksDbSharp.Checkpoint c = db.Checkpoint();
                c.Save(cp, 0);
            }, dir.Path, wal);
        }
        Console.WriteLine("managed: " + managedResult);
        Console.WriteLine("native: " + nativeResult);
        StringAssert.Contains(managedResult, " 0 failed", managedResult);
    }

    private static SoakDump NativeDump(SoakSettings settings, string db, string wal)
    {
        var profile = new NativeMosaikProfile(wal, settings.LowMemoryMode, settings.ProcessorCount, settings.CacheBytes, settings.WriteBufferBytes);
        using RocksDbSharp.RocksDb n = profile.Open(db);
        var d = new SoakDump();
        for (int f = 0; f < SoakModel.FamilyCount; f++)
        {
            var ro = new RocksDbSharp.ReadOptions().SetTotalOrderSeek(true);
            using RocksDbSharp.Iterator it = n.NewIterator(n.GetColumnFamily(MosaikProfile.Families[f].Name), ro);
            for (it.SeekToFirst(); it.Valid(); it.Next()) d.Families[f].Add((it.Key(), it.Value()));
        }
        return d;
    }
}
