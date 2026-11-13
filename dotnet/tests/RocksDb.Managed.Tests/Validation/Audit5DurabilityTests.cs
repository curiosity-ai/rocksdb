using RocksDb.Managed.Env;
using RocksDb.Managed.Tests.Db;
using RocksDb.Managed.Utilities.Backup;
using RocksDb.Managed.Utilities.Transactions;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #5: power-loss durability sweep (docs/validation.md). Each scenario runs a deterministic sequence of
/// operations against a <see cref="FaultInjectionFileSystem"/> and is repeated with a simulated power loss at every
/// create, sync, rename, link, delete and directory sync it performs (the operation fails, then unsynced file data and
/// unsynced directory entries are dropped, see <see cref="MemoryFileSystem.SimulateCrash"/>). After each power loss the
/// DB must open, hold every key whose write was acknowledged as durable (sync write, flush, ingestion, ...), not hold
/// any key whose deletion was acknowledged as durable, and keep working. Scenarios the sweep cannot reach (a state
/// that needs a held-back flush, a process that dies without closing) are single tests.
/// </summary>
[TestClass]
public sealed class Audit5DurabilityTests
{
    private readonly ManualResetEventSlim _flushGate = new(false);

    /// <summary>Holds background flushes at their start until <see cref="Cleanup"/> (see WalSizeAndSyncTests).</summary>
    private void BlockFlushes() => RocksDb.Managed.Db.TestHooks.Set("DBImpl::FlushMemTables:Start", () => _flushGate.Wait());

    [TestCleanup]
    public void Cleanup()
    {
        RocksDb.Managed.Db.TestHooks.ClearAll();
        _flushGate.Set();
    }

    /// <summary>What a scenario has been told is durable so far: key -> value (null: durably deleted).</summary>
    private sealed class Expectations
    {
        public readonly Dictionary<string, string?> Default = new(StringComparer.Ordinal);
        public readonly Dictionary<string, Dictionary<string, string?>> ByColumnFamily = new(StringComparer.Ordinal);
        /// <summary>Column families whose creation (true) or drop (false) was acknowledged.</summary>
        public readonly Dictionary<string, bool> ColumnFamilies = new(StringComparer.Ordinal);
        /// <summary>Other acknowledged steps of a scenario (a checkpoint, an export, a backup), checked by its own verifier.</summary>
        public readonly HashSet<string> Acked = new(StringComparer.Ordinal);

        public void Durable(string key, string? value) => Default[key] = value;

        /// <summary>The key is about to be rewritten: until that is acknowledged either state may survive.</summary>
        public void Pending(string key) => Default.Remove(key);

        public void Pending(string cf, string key)
        {
            if (ByColumnFamily.TryGetValue(cf, out Dictionary<string, string?>? m)) m.Remove(key);
        }

        public void Durable(string cf, string key, string? value)
        {
            if (!ByColumnFamily.TryGetValue(cf, out Dictionary<string, string?>? m)) ByColumnFamily[cf] = m = new(StringComparer.Ordinal);
            m[key] = value;
        }
    }

    private delegate void ScenarioBody(FaultInjectionFileSystem fs, Expectations durable);

    private static readonly WriteOptions Sync = new() { Sync = true };
    private static readonly WriteOptions NoSync = new();

    private static ColumnFamilyOptions ManualCf() => new()
    {
        DisableAutoCompactions = true,
        Compression = RocksDb.Managed.Compression.CompressionType.None,
    };

    private static List<ColumnFamilyDescriptor> Descriptors(IFileSystem fs, Func<ColumnFamilyOptions> cfOptions)
    {
        var result = new List<ColumnFamilyDescriptor>();
        List<string> names = fs.FileExists(DbPath + "/CURRENT") ? RocksDatabase.ListColumnFamilies(DbPath, fs) : ["default"];
        foreach (string name in names) result.Add(new ColumnFamilyDescriptor(name, cfOptions()));
        return result;
    }

    private static bool IsInjected(Exception e) => e is RocksDbException or IOException;

    /// <summary>
    /// Runs <paramref name="body"/> once to count its mutating operations, then once per operation with a power loss
    /// at that operation, and checks the recovered DB against what the body recorded as durable before the loss.
    /// </summary>
    private static void PowerLossSweep(Func<IFileSystem, DbOptions> options, Func<ColumnFamilyOptions> cfOptions, ScenarioBody body,
        bool skipAppends = true, Func<RocksDbException, Expectations, bool>? acceptOpenFailure = null,
        Action<FaultInjectionFileSystem, Expectations, string>? verifyMore = null)
    {
        var dry = new FaultInjectionFileSystem(NewFs()) { OpLog = new() };
        body(dry, new Expectations());
        string[] log = [.. dry.OpLog!];
        // AUDIT5_TRACE=<file>: appends every scenario's operation sequence (without appends), for reading the sync order.
        if (Environment.GetEnvironmentVariable("AUDIT5_TRACE") is { Length: > 0 } trace)
        {
            var lines = new List<string> { "==== " + body.Method.Name };
            foreach (string l in log)
                if (!l.StartsWith("append ", StringComparison.Ordinal) && !l.StartsWith("flush ", StringComparison.Ordinal)) lines.Add(l);
            File.AppendAllLines(trace, lines);
        }
        Assert.IsTrue(log.Length > 3, "scenario too small");
        Assert.AreEqual(0, dry.OpenFiles, "scenario leaked file handles");

        for (int at = 1; at <= log.Length; at++)
        {
            // A power loss before an append leaves the same durable state as one before the next sync / create /
            // rename / delete / directory sync, so only those are swept (appends are the bulk of the operations).
            if (skipAppends && (log[at - 1].StartsWith("append ", StringComparison.Ordinal) || log[at - 1].StartsWith("flush ", StringComparison.Ordinal))) continue;
            var fs = new FaultInjectionFileSystem(NewFs());
            var durable = new Expectations();
            fs.CrashAtOperation = at;
            try
            {
                body(fs, durable);
            }
            catch (Exception e) when (IsInjected(e))
            {
            }
            string context = "power loss at operation " + at + " of " + log.Length + " (" + (fs.CrashedAt ?? log[at - 1]) + ")";
            Assert.AreEqual(0, fs.OpenLocks, "LOCK not released: " + context);
            fs.PowerLoss();
            fs.Restart();
            try
            {
                verifyMore?.Invoke(fs, durable, context);
                VerifyRecovered(fs, options, cfOptions, durable, context, acceptOpenFailure);
            }
            catch (Exception e) when (IsInjected(e))
            {
                Assert.Fail("the recovered DB failed: " + context + ": " + e);
            }
        }
    }

    private static void VerifyRecovered(FaultInjectionFileSystem fs, Func<IFileSystem, DbOptions> options, Func<ColumnFamilyOptions> cfOptions, Expectations durable,
        string context, Func<RocksDbException, Expectations, bool>? acceptOpenFailure)
    {
        for (int round = 0; round < 2; round++)
        {
            RocksDatabase db;
            try
            {
                db = RocksDatabase.Open(DbPath, options(fs), Descriptors(fs, cfOptions));
            }
            catch (RocksDbException e) when (round == 0 && acceptOpenFailure is not null && acceptOpenFailure(e, durable))
            {
                return;
            }
            catch (Exception e)
            {
                Assert.Fail("reopen " + round + " failed: " + context + ": " + e);
                throw;
            }
            using (db)
            {
                foreach ((string key, string? value) in durable.Default)
                    Assert.AreEqual(value, GetString(db, key), "key " + key + " (reopen " + round + "): " + context);
                foreach ((string cfName, Dictionary<string, string?> keys) in durable.ByColumnFamily)
                {
                    if (durable.ColumnFamilies.TryGetValue(cfName, out bool alive) && !alive) continue;
                    ColumnFamily? cf = TryCf(db, cfName);
                    Assert.IsNotNull(cf, "column family " + cfName + " missing (reopen " + round + "): " + context);
                    foreach ((string key, string? value) in keys)
                        Assert.AreEqual(value, GetString(cf, key), "key " + cfName + "/" + key + " (reopen " + round + "): " + context);
                }
                foreach ((string cfName, bool alive) in durable.ColumnFamilies)
                    Assert.AreEqual(alive, TryCf(db, cfName) is not null, "column family " + cfName + " (reopen " + round + "): " + context);
                // The recovered DB keeps working.
                db.Put(Sync, B("after-crash"), B("v" + round));
                db.Flush();
                db.CompactRange();
            }
        }
    }

    private static ColumnFamily? TryCf(RocksDatabase db, string name) => db.TryGetColumnFamily(name, out ColumnFamily? cf) ? cf : null;

    private static DbOptions Plain(IFileSystem fs) => new() { CreateIfMissing = true, FileSystem = fs };

    private static RocksDatabase OpenDb(FaultInjectionFileSystem fs, Func<IFileSystem, DbOptions> options, Func<ColumnFamilyOptions> cfOptions) =>
        RocksDatabase.Open(DbPath, options(fs), Descriptors(fs, cfOptions));

    private static string Big(string prefix) => prefix + new string('x', 200);

    // ---- scenarios ----------------------------------------------------------------------------------------------

    /// <summary>DBImpl::Open of a new DB (NewDB, IDENTITY, CURRENT, first WAL, OPTIONS), then the first sync write.</summary>
    [TestMethod]
    public void NewDbFirstSyncWriteAndFlushSurvive()
    {
        PowerLossSweep(Plain, ManualCf, static (fs, durable) =>
        {
            using RocksDatabase db = OpenDb(fs, Plain, ManualCf);
            db.Put(Sync, B("a"), B("1"));
            durable.Durable("a", "1");
            db.Put(NoSync, B("b"), B("2"));
            db.Flush();
            durable.Durable("b", "2");
            durable.Pending("a");
            db.Delete(Sync, B("a"));
            durable.Durable("a", null);
            db.Put(NoSync, B("c"), B("3"));
            db.Flush();
            durable.Durable("c", "3");
            db.CompactRange();
            db.Put(Sync, B("d"), B("4"));
            durable.Durable("d", "4");
        }, skipAppends: false);
    }

    /// <summary>
    /// DBImpl::NewDB deletes MANIFEST-000001 only when writing it failed. When SetCurrentFile fails after its rename
    /// (here: the directory fsync), CURRENT already names the MANIFEST, which therefore has to stay: the next open then
    /// finds a complete, empty DB.
    /// </summary>
    [TestMethod]
    public void FailedDirectoryFsyncOfTheFirstCurrentLeavesAnOpenableDb()
    {
        static DbOptions Options(IFileSystem fs) => new() { CreateIfMissing = true, FileSystem = fs, WriteIdentityFile = false };
        var fs = new FaultInjectionFileSystem(NewFs());
        // Without an IDENTITY file the first directory fsync of the open is the one after CURRENT was renamed into place.
        fs.FailMatching(DbPath, FaultOps.SyncDirectory, 1, InjectedFaultKind.IOError);
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => OpenDb(fs, Options, ManualCf));
        Assert.AreEqual(StatusCode.IOError, ex.Code);
        Assert.AreEqual(1, fs.TargetedFaults);
        Assert.IsTrue(fs.FileExists(DbPath + "/CURRENT"));
        using RocksDatabase db = OpenDb(fs, Options, ManualCf);
        db.Put(Sync, B("a"), B("1"));
        Assert.AreEqual("1", GetString(db, "a"));
    }

    /// <summary>
    /// Sync writes, WAL rotations, flushes, a durable delete, a manual compaction and a clean reopen in the middle
    /// (WAL recovery, a new MANIFEST), over two column families.
    /// </summary>
    private static void CoreWorkload(FaultInjectionFileSystem fs, Expectations durable, Func<IFileSystem, DbOptions> options, Func<ColumnFamilyOptions> cfOptions)
    {
        using (RocksDatabase db = OpenDb(fs, options, cfOptions))
        {
            db.Put(Sync, B("a"), B(Big("1")));
            durable.Durable("a", Big("1"));
            ColumnFamily cf = db.CreateColumnFamily("cf1", cfOptions());
            durable.ColumnFamilies["cf1"] = true;
            cf.Put(Sync, B("ca"), B(Big("c1")));
            durable.Durable("cf1", "ca", Big("c1"));
            db.Put(NoSync, B("b"), B(Big("2")));
            db.Flush();
            durable.Durable("b", Big("2"));
            cf.Put(NoSync, B("cb"), B(Big("c2")));
            db.Put(Sync, B("c"), B(Big("3")));
            durable.Durable("c", Big("3"));
            durable.Durable("cf1", "cb", Big("c2"));
            cf.Flush();
            durable.Pending("a");
            db.Delete(Sync, B("a"));
            durable.Durable("a", null);
            db.Put(NoSync, B("d"), B(Big("4")));
            db.Flush();
            durable.Durable("d", Big("4"));
            db.CompactRange();
            cf.Put(Sync, B("cc"), B(Big("c3")));
            durable.Durable("cf1", "cc", Big("c3"));
            db.Put(Sync, B("e"), B(Big("5")));
            durable.Durable("e", Big("5"));
        }
        using (RocksDatabase db = OpenDb(fs, options, cfOptions))
        {
            ColumnFamily cf = db.GetColumnFamily("cf1");
            db.Put(Sync, B("f"), B(Big("6")));
            durable.Durable("f", Big("6"));
            cf.Put(NoSync, B("cd"), B(Big("c4")));
            cf.Flush();
            durable.Durable("cf1", "cd", Big("c4"));
            db.Put(NoSync, B("g"), B(Big("7")));
            db.Flush();
            durable.Durable("g", Big("7"));
            cf.CompactRange();
            db.CompactRange();
            durable.Pending("cf1", "ca");
            cf.Delete(Sync, B("ca"));
            durable.Durable("cf1", "ca", null);
            db.Put(Sync, B("h"), B(Big("8")));
            durable.Durable("h", Big("8"));
        }
    }

    private static void SweepCore(Func<IFileSystem, DbOptions> options, Func<ColumnFamilyOptions> cfOptions,
        Func<RocksDbException, Expectations, bool>? acceptOpenFailure = null) =>
        PowerLossSweep(options, cfOptions, (fs, durable) => CoreWorkload(fs, durable, options, cfOptions), acceptOpenFailure: acceptOpenFailure);

    [TestMethod]
    public void CoreWorkloadDefaultOptions() => SweepCore(Plain, ManualCf);

    /// <summary>wal_dir different from the DB directory: the WAL directory needs its own fsyncs.</summary>
    [TestMethod]
    public void CoreWorkloadSeparateWalDir() =>
        SweepCore(static fs => new DbOptions { CreateIfMissing = true, FileSystem = fs, WalDir = "/wal" }, ManualCf);

    /// <summary>recycle_log_file_num: WALs are renamed and overwritten in place.</summary>
    [TestMethod]
    public void CoreWorkloadRecycledWals() =>
        SweepCore(static fs => new DbOptions { CreateIfMissing = true, FileSystem = fs, RecycleLogFileNum = 3 }, ManualCf);

    /// <summary>WAL_ttl_seconds: obsolete WALs are moved to the archive directory.</summary>
    [TestMethod]
    public void CoreWorkloadRecycledWalsSeparateWalDir() =>
        SweepCore(static fs => new DbOptions { CreateIfMissing = true, FileSystem = fs, RecycleLogFileNum = 3, WalDir = "/wal" }, ManualCf);

    [TestMethod]
    public void CoreWorkloadArchivedWals() =>
        SweepCore(static fs => new DbOptions { CreateIfMissing = true, FileSystem = fs, WalTtlSeconds = 3600 }, ManualCf);

    [TestMethod]
    public void CoreWorkloadArchivedWalsSeparateWalDir() =>
        SweepCore(static fs => new DbOptions { CreateIfMissing = true, FileSystem = fs, WalTtlSeconds = 3600, WalDir = "/wal" }, ManualCf);

    /// <summary>track_and_verify_wals_in_manifest: a WAL recorded as synced in the MANIFEST must exist after a power loss.</summary>
    [TestMethod]
    public void CoreWorkloadWalsTrackedInManifest() =>
        SweepCore(static fs => new DbOptions { CreateIfMissing = true, FileSystem = fs, TrackAndVerifyWalsInManifest = true }, ManualCf);

    [TestMethod]
    public void CoreWorkloadWalsTrackedInManifestSeparateWalDir() =>
        SweepCore(static fs => new DbOptions { CreateIfMissing = true, FileSystem = fs, TrackAndVerifyWalsInManifest = true, WalDir = "/wal" }, ManualCf);

    /// <summary>
    /// track_and_verify_wals: the predecessor chain must not report a hole after a power loss. Same as upstream: the
    /// directory entry of the first WAL is only made durable by the first sync write (DBImpl::WriteToWAL,
    /// need_wal_dir_sync), so a power loss between the first CURRENT and that write leaves "an existing DB with no WAL
    /// files", which this option reports as corruption (DBImpl::Recover). Nothing was acknowledged at that point.
    /// </summary>
    [TestMethod]
    public void CoreWorkloadPredecessorWalInfo() =>
        SweepCore(static fs => new DbOptions { CreateIfMissing = true, FileSystem = fs, TrackAndVerifyWals = true }, ManualCf,
            static (e, durable) => e.Code == StatusCode.Corruption && durable.Default.Count == 0
                && e.Message.Contains("Opening an existing DB with no WAL files", StringComparison.Ordinal));

    /// <summary>A tiny max_manifest_file_size: every version edit starts a new MANIFEST and switches CURRENT.</summary>
    [TestMethod]
    public void CoreWorkloadManifestRollOnEveryEdit() =>
        SweepCore(static fs => new DbOptions { CreateIfMissing = true, FileSystem = fs, MaxManifestFileSize = 1 }, ManualCf);

    /// <summary>
    /// The other write paths: pipelined writes, unordered writes and two write queues, each with a separate wal_dir so
    /// that their WAL directory fsyncs are not covered by the DB directory's.
    /// </summary>
    [TestMethod]
    [DataRow(true, false, false)]
    [DataRow(false, true, false)]
    [DataRow(false, false, true)]
    public void CoreWorkloadOtherWritePaths(bool pipelined, bool unordered, bool twoWriteQueues) =>
        SweepCore(fs => new DbOptions
        {
            CreateIfMissing = true,
            FileSystem = fs,
            WalDir = "/wal",
            EnablePipelinedWrite = pipelined,
            UnorderedWrite = unordered,
            TwoWriteQueues = twoWriteQueues,
        }, ManualCf);

    [TestMethod]
    public void CoreWorkloadAvoidFlushDuringRecovery() =>
        SweepCore(static fs => new DbOptions { CreateIfMissing = true, FileSystem = fs, AvoidFlushDuringRecovery = true }, ManualCf);

    [TestMethod]
    public void CoreWorkloadAtomicFlush() =>
        SweepCore(static fs => new DbOptions { CreateIfMissing = true, FileSystem = fs, AtomicFlush = true }, ManualCf);

    /// <summary>db_paths: table files in two directories, each needing its own fsync.</summary>
    [TestMethod]
    public void CoreWorkloadTwoDbPaths() =>
        SweepCore(static fs =>
        {
            var o = new DbOptions { CreateIfMissing = true, FileSystem = fs };
            o.DbPaths.Add(("/p0", 1));
            o.DbPaths.Add(("/p1", 1UL << 30));
            return o;
        }, ManualCf);

    /// <summary>Blob files written by flush and rewritten by compaction (garbage collection).</summary>
    [TestMethod]
    public void CoreWorkloadBlobFiles() =>
        SweepCore(Plain, static () => new ColumnFamilyOptions
        {
            DisableAutoCompactions = true,
            Compression = RocksDb.Managed.Compression.CompressionType.None,
            EnableBlobFiles = true,
            MinBlobSize = 16,
            EnableBlobGarbageCollection = true,
            BlobGarbageCollectionAgeCutoff = 1.0,
        });

    private static ColumnFamilyOptions BlobCf() => new()
    {
        DisableAutoCompactions = true,
        Compression = RocksDb.Managed.Compression.CompressionType.None,
        EnableBlobFiles = true,
        MinBlobSize = 16,
        EnableBlobGarbageCollection = true,
        BlobGarbageCollectionAgeCutoff = 1.0,
    };

    private static DbOptions TwoPaths(IFileSystem fs)
    {
        var o = new DbOptions { CreateIfMissing = true, FileSystem = fs };
        o.DbPaths.Add(("/p0", 1));
        o.DbPaths.Add(("/p1", 1UL << 30));
        return o;
    }

    /// <summary>
    /// db_paths with blob files: a compaction writes its tables to the second path and its blob files to the first
    /// (cf_paths.front()). CompactionJob::SyncOutputDirectories fsyncs both directories before the MANIFEST edit.
    /// </summary>
    [TestMethod]
    public void CoreWorkloadTwoDbPathsWithBlobFiles() => SweepCore(TwoPaths, BlobCf);

    /// <summary>
    /// A manual compaction to the second db_path (CompactRangeOptions::target_path_id) that also writes blob files
    /// (garbage collection relocates the live blobs): the tables go to /p1, the blob files to /p0.
    /// </summary>
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void CompactionToSecondDbPathSurvivesPowerLoss(bool blobFiles)
    {
        Func<ColumnFamilyOptions> cfOptions = blobFiles ? BlobCf : ManualCf;
        PowerLossSweep(TwoPaths, cfOptions, (fs, durable) =>
        {
            using RocksDatabase db = OpenDb(fs, TwoPaths, cfOptions);
            for (int round = 0; round < 3; round++)
            {
                for (int i = 0; i < 4; i++) db.Put(NoSync, B("k" + round + i), B(Big("v" + round + i)));
                db.Put(NoSync, B("overwritten"), B(Big("o" + round)));
                db.Flush();
                for (int i = 0; i < 4; i++) durable.Durable("k" + round + i, Big("v" + round + i));
            }
            durable.Durable("overwritten", Big("o2"));
            db.CompactRange(new CompactRangeOptions { TargetPathId = 1, BottommostLevelCompaction = BottommostLevelCompaction.Force }, null, null);
            db.Put(Sync, B("end"), B("1"));
            durable.Durable("end", "1");
        });
    }

    /// <summary>An SstFileManager with a delete rate: obsolete files go through the trash.</summary>
    [TestMethod]
    public void CoreWorkloadSstFileManagerTrash() =>
        SweepCore(static fs => new DbOptions
        {
            CreateIfMissing = true,
            FileSystem = fs,
            SstFileManager = new SstFileManager(fs, 1 << 30, 1000.0, 64UL << 20),
        }, ManualCf);

    // ---- external file ingestion ----------------------------------------------------------------------------------

    /// <summary>Writes an external table file holding <paramref name="keys"/> (value = key + "-v") with SstFileWriter.</summary>
    private static void WriteExternalFile(IFileSystem fs, string path, params string[] keys)
    {
        using var writer = new SstFileWriter(ManualCf(), fs);
        writer.Open(path);
        foreach (string k in keys) writer.Put(B(k), B(k + "-v"));
        writer.Finish();
    }

    /// <summary>
    /// Copies <paramref name="source"/> to <paramref name="target"/> without syncing the copy, like an application that
    /// produced (or downloaded) a table file and never fsynced it.
    /// </summary>
    private static void CopyWithoutSync(FaultInjectionFileSystem fs, string source, string target)
    {
        byte[] bytes = fs.Inner.ReadAllBytes(source);
        using IWritableFile f = fs.CreateWritable(target, truncate: true);
        f.Append(bytes);
    }

    private static void IngestWorkload(FaultInjectionFileSystem fs, Expectations durable, IngestExternalFileOptions ingest, bool unsyncedSource,
        Func<IFileSystem, DbOptions>? options = null)
    {
        fs.CreateDirectoryIfMissing("/ext");
        using RocksDatabase db = OpenDb(fs, options ?? Plain, ManualCf);
        db.Put(Sync, B("m"), B("1"));
        durable.Durable("m", "1");
        WriteExternalFile(fs, "/ext/a.sst", "a1", "a2");
        string first = "/ext/a.sst";
        if (unsyncedSource)
        {
            CopyWithoutSync(fs, "/ext/a.sst", "/ext/a-copy.sst");
            first = "/ext/a-copy.sst";
        }
        db.IngestExternalFiles([first], ingest);
        durable.Durable("a1", "a1-v");
        durable.Durable("a2", "a2-v");
        // Overlaps the memtable ("m") and the first file: needs a flush and a global sequence number.
        WriteExternalFile(fs, "/ext/b.sst", "a2", "m", "z");
        durable.Pending("a2");
        durable.Pending("m");
        db.IngestExternalFiles(["/ext/b.sst"], ingest);
        durable.Durable("a2", "a2-v");
        durable.Durable("m", "m-v");
        durable.Durable("z", "z-v");
        db.Put(Sync, B("n"), B("2"));
        durable.Durable("n", "2");
    }

    /// <summary>
    /// ExternalSstFileIngestionJob::Prepare: copied files are synced by CopyFile, linked files by FileSystem::SyncFile
    /// ("It is unsafe to assume application had sync the file ... before ingest the file"), then the data directory is
    /// fsynced, all before the MANIFEST edit that adds the files.
    /// </summary>
    [TestMethod]
    [DataRow(false, false, false)]
    [DataRow(false, false, true)]
    [DataRow(true, false, false)]
    [DataRow(true, false, true)]
    [DataRow(false, true, true)]
    [DataRow(true, false, true, true)]
    public void IngestedFilesSurvivePowerLoss(bool move, bool link, bool unsyncedSource, bool writeGlobalSeqno = false)
    {
        // Also with db_paths: ingested files go to the first path, whose directory is the one to fsync.
        PowerLossSweep(TwoPaths, ManualCf, (fs, durable) =>
            IngestWorkload(fs, durable, new IngestExternalFileOptions { MoveFiles = move, LinkFiles = link, WriteGlobalSeqno = writeGlobalSeqno }, unsyncedSource,
                TwoPaths));
        PowerLossSweep(Plain, ManualCf, (fs, durable) =>
            IngestWorkload(fs, durable, new IngestExternalFileOptions { MoveFiles = move, LinkFiles = link, WriteGlobalSeqno = writeGlobalSeqno }, unsyncedSource));
    }

    /// <summary>
    /// The single case behind the sweep above: a table file that was never fsynced is ingested with move_files (a hard
    /// link). Once the ingestion returns, a power loss must not lose it.
    /// </summary>
    [TestMethod]
    [DataRow(true, false)]
    [DataRow(false, true)]
    public void IngestWithLinkSyncsTheLinkedFile(bool move, bool link)
    {
        var fs = new FaultInjectionFileSystem(NewFs());
        fs.CreateDirectoryIfMissing("/ext");
        using (RocksDatabase db = OpenDb(fs, Plain, ManualCf))
        {
            WriteExternalFile(fs, "/ext/a.sst", "a1", "a2");
            CopyWithoutSync(fs, "/ext/a.sst", "/ext/unsynced.sst");
            fs.SyncDirectory("/ext");
            db.IngestExternalFiles(["/ext/unsynced.sst"], new IngestExternalFileOptions { MoveFiles = move, LinkFiles = link });
            Assert.AreEqual("a1-v", GetString(db, "a1"));
        }
        fs.PowerLoss();
        fs.Restart();
        using RocksDatabase reopened = OpenDb(fs, Plain, ManualCf);
        Assert.AreEqual("a1-v", GetString(reopened, "a1"));
        Assert.AreEqual("a2-v", GetString(reopened, "a2"));
    }

    // ---- blob direct write ------------------------------------------------------------------------------------------

    /// <summary>
    /// enable_blob_direct_write: a sync write is durable only if the blob file holding its value is, contents and
    /// directory entry. The value is written after the WAL directory was fsynced for an earlier sync write, so only a
    /// directory fsync for the blob file itself can make its entry durable. (Only the crash right after the write is
    /// tested: like upstream's v1, a blob file that was sealed but not yet registered in the MANIFEST when the process
    /// died is deleted as obsolete on the next open, see docs/validation.md.)
    /// </summary>
    [TestMethod]
    public void BlobDirectWriteSyncWriteSurvivesPowerLoss()
    {
        static ColumnFamilyOptions Cf() => new()
        {
            DisableAutoCompactions = true,
            Compression = RocksDb.Managed.Compression.CompressionType.None,
            EnableBlobFiles = true,
            MinBlobSize = 64,
            EnableBlobDirectWrite = true,
        };
        static DbOptions Options(IFileSystem fs) => new() { CreateIfMissing = true, FileSystem = fs, AllowConcurrentMemtableWrite = false };
        var fs = new FaultInjectionFileSystem(NewFs());
        RocksDatabase db = OpenDb(fs, Options, Cf);
        db.Put(Sync, B("small"), B("1"));
        db.Put(Sync, B("big"), B(Big("blob")));
        // The process dies here: nothing more reaches the file system (a clean close would flush and register the file).
        fs.CrashAtOperation = fs.Operations + 1;
        try { db.Dispose(); }
        catch (Exception e) when (IsInjected(e)) { }
        fs.PowerLoss();
        fs.Restart();
        using RocksDatabase reopened = OpenDb(fs, Options, Cf);
        Assert.AreEqual("1", GetString(reopened, "small"));
        Assert.AreEqual(Big("blob"), GetString(reopened, "big"));
    }

    // ---- writes without WAL, manual WAL flush, SyncWAL --------------------------------------------------------------

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void SyncWalAndFlushWalMakeEarlierWritesDurable(bool manualWalFlush)
    {
        DbOptions Options(IFileSystem fs) => new() { CreateIfMissing = true, FileSystem = fs, ManualWalFlush = manualWalFlush, WalDir = "/wal" };
        PowerLossSweep(Options, ManualCf, (fs, durable) =>
        {
            using RocksDatabase db = OpenDb(fs, Options, ManualCf);
            ColumnFamily cf = db.CreateColumnFamily("cf1", ManualCf());
            durable.ColumnFamilies["cf1"] = true;
            db.Put(NoSync, B("a"), B("1"));
            cf.Put(NoSync, B("ca"), B("1"));
            db.SyncWal();
            durable.Durable("a", "1");
            durable.Durable("cf1", "ca", "1");
            db.Put(NoSync, B("b"), B("2"));
            db.Flush(); // a new WAL; "b" is in a table file
            durable.Durable("b", "2");
            db.Put(NoSync, B("c"), B("3"));
            cf.Put(NoSync, B("cb"), B("2"));
            db.FlushWal(sync: true);
            durable.Durable("c", "3");
            durable.Durable("cf1", "cb", "2");
            var noWal = new WriteOptions { DisableWal = true };
            db.Put(noWal, B("d"), B("4"));
            db.Flush();
            durable.Durable("d", "4");
        });
    }

    // ---- two-phase commit ---------------------------------------------------------------------------------------------

    /// <summary>
    /// allow_2pc: a WAL holding a prepared section must survive flushes of other data until the transaction commits
    /// (PrecomputeMinLogNumberToKeep2PC), and a committed transaction is durable once its sync commit returns. Each
    /// committed transaction is written while another one is only prepared, across flushes and a reopen.
    /// </summary>
    [TestMethod]
    public void TwoPhaseCommitSurvivesPowerLoss()
    {
        static DbOptions Options(IFileSystem fs) => new() { CreateIfMissing = true, FileSystem = fs, Allow2Pc = true };
        PowerLossSweep(Options, ManualCf, static (fs, durable) =>
        {
            for (int open = 0; open < 2; open++)
            {
                using TransactionDatabase db = TransactionDatabase.Open(DbPath, Options(fs), new TransactionDbOptions(),
                    Descriptors(fs, ManualCf));
                // A transaction recovered as prepared from the previous open is rolled back (it was never acknowledged).
                foreach (Transaction recovered in db.GetAllPreparedTransactions()) recovered.Rollback();
                using Transaction early = db.BeginTransaction(Sync);
                early.SetName("early" + open);
                early.Put(B("t" + open + "a"), B("1"));
                early.Prepare();
                // Other data is flushed while "early" is prepared: its WAL has to stay.
                db.Database.Put(NoSync, B("p" + open), B("1"));
                db.Database.Flush();
                durable.Durable("p" + open, "1");
                using (Transaction late = db.BeginTransaction(Sync))
                {
                    late.SetName("late" + open);
                    late.Put(B("t" + open + "b"), B("2"));
                    late.Prepare();
                    late.Commit();
                }
                durable.Durable("t" + open + "b", "2");
                db.Database.Put(NoSync, B("q" + open), B("1"));
                db.Database.Flush();
                durable.Durable("q" + open, "1");
                early.Commit();
                durable.Durable("t" + open + "a", "1");
            }
        });
    }

    // ---- column family create / drop, DeleteFilesInRange ------------------------------------------------------------

    [TestMethod]
    public void ColumnFamilyCreateDropAndDeleteFilesInRange()
    {
        PowerLossSweep(Plain, ManualCf, static (fs, durable) =>
        {
            using RocksDatabase db = OpenDb(fs, Plain, ManualCf);
            ColumnFamily cf1 = db.CreateColumnFamily("cf1", ManualCf());
            durable.ColumnFamilies["cf1"] = true;
            ColumnFamily cf2 = db.CreateColumnFamily("cf2", ManualCf());
            durable.ColumnFamilies["cf2"] = true;
            cf1.Put(Sync, B("a"), B("1"));
            durable.Durable("cf1", "a", "1");
            cf2.Put(NoSync, B("a"), B("2"));
            cf2.Flush();
            durable.Durable("cf2", "a", "2");
            durable.ColumnFamilies.Remove("cf1");
            durable.ByColumnFamily.Remove("cf1");
            db.DropColumnFamily(cf1);
            durable.ColumnFamilies["cf1"] = false;
            for (int i = 0; i < 3; i++)
            {
                db.Put(NoSync, B("r" + i), B("v" + i));
                db.Flush();
                durable.Durable("r" + i, "v" + i);
            }
            db.CompactRange(new CompactRangeOptions { BottommostLevelCompaction = BottommostLevelCompaction.Force }, null, null);
            for (int i = 0; i < 3; i++) durable.Pending("r" + i);
            db.DeleteFilesInRange(null, null, includeEnd: true);
            for (int i = 0; i < 3; i++) durable.Durable("r" + i, null);
            db.Put(Sync, B("end"), B("1"));
            durable.Durable("end", "1");
        });
    }

    // ---- checkpoint -------------------------------------------------------------------------------------------------

    /// <summary>
    /// CheckpointImpl::CreateCheckpoint: files are linked / copied (copies synced) into a staging directory, which is
    /// renamed and then fsynced. Once the call returns the checkpoint must be a complete DB after a power loss.
    /// </summary>
    [TestMethod]
    [DataRow(0UL)]
    [DataRow(ulong.MaxValue)]
    public void CheckpointSurvivesPowerLoss(ulong logSizeForFlush)
    {
        static DbOptions Options(IFileSystem fs) => new() { CreateIfMissing = true, FileSystem = fs, WalDir = "/wal" };
        PowerLossSweep(Options, BlobCf, (fs, durable) =>
        {
            fs.CreateDirectoryIfMissing("/cps");
            using RocksDatabase db = OpenDb(fs, Options, BlobCf);
            db.Put(Sync, B("a"), B(Big("1")));
            durable.Durable("a", Big("1"));
            db.Put(NoSync, B("b"), B(Big("2")));
            db.Flush();
            durable.Durable("b", Big("2"));
            db.Put(NoSync, B("c"), B(Big("3")));
            db.CreateCheckpoint("/cps/cp1", logSizeForFlush);
            durable.Acked.Add("cp1");
            db.Put(Sync, B("d"), B(Big("4")));
            durable.Durable("d", Big("4"));
            db.CreateCheckpoint("/cps/cp2", logSizeForFlush);
            durable.Acked.Add("cp2");
        }, verifyMore: static (fs, durable, context) =>
        {
            foreach (string cp in durable.Acked)
            {
                using RocksDatabase db = RocksDatabase.Open("/cps/" + cp, new DbOptions { FileSystem = fs }, [new ColumnFamilyDescriptor("default", BlobCf())]);
                Assert.AreEqual(Big("1"), GetString(db, "a"), cp + ": " + context);
                Assert.AreEqual(Big("2"), GetString(db, "b"), cp + ": " + context);
                Assert.AreEqual(Big("3"), GetString(db, "c"), cp + ": " + context);
                Assert.AreEqual(cp == "cp2" ? Big("4") : null, GetString(db, "d"), cp + ": " + context);
            }
        });
    }

    /// <summary>
    /// A checkpoint taken without a flush captures the live WALs. DBImpl::GetLiveFilesStorageInfo hard links only the
    /// WALs that are "known fully synced and no future writes"; every WAL still open in logs_ (GetOpenWalSizes: the
    /// current one and the closed ones not synced yet) is copied, and CopyFile syncs the copy. A hard link to a closed
    /// WAL that was never synced shares its unsynced data: a power loss would take the writes out of the checkpoint.
    /// </summary>
    [TestMethod]
    public void CheckpointCopiesClosedWalsThatWereNotSynced()
    {
        MemoryFileSystem fs = NewFs();
        fs.CreateDirectoryIfMissing("/cps");
        // A huge write buffer: the memtable is switched by hand and its flush is held back, so the closed WAL stays live.
        RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { WriteBufferSize = 64 << 20, MaxWriteBufferNumber = 6 });
        BlockFlushes();
        db.Put(B("a"), B("1")); // not synced, in the first WAL
        db.DefaultColumnFamily.Db.SwitchMemtableForTest();
        db.Put(B("b"), B("2")); // not synced, in the second WAL
        ulong sequence = db.CreateCheckpoint("/cps/cp", ulong.MaxValue);
        Assert.AreEqual(2UL, sequence);
        fs.SimulateCrash();
        using (RocksDatabase checkpoint = RocksDatabase.Open("/cps/cp", new DbOptions { FileSystem = fs }))
        {
            Assert.AreEqual("1", GetString(checkpoint, "a"));
            Assert.AreEqual("2", GetString(checkpoint, "b"));
        }
        GC.KeepAlive(db);
    }

    // ---- export / import --------------------------------------------------------------------------------------------

    /// <summary>
    /// Checkpoint::ExportColumnFamily and DB::CreateColumnFamilyWithImport: once the import returns, the imported
    /// column family and its files must survive a power loss.
    /// </summary>
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void ImportedColumnFamilySurvivesPowerLoss(bool moveFiles)
    {
        PowerLossSweep(Plain, ManualCf, (fs, durable) =>
        {
            fs.CreateDirectoryIfMissing("/exports");
            using RocksDatabase db = OpenDb(fs, Plain, ManualCf);
            ColumnFamily src = db.CreateColumnFamily("src", ManualCf());
            durable.ColumnFamilies["src"] = true;
            src.Put(NoSync, B("a"), B("1"));
            src.Flush();
            src.Put(NoSync, B("b"), B("2"));
            durable.Durable("src", "a", "1");
            ExportImportFilesMetadata exported = src.ExportColumnFamily("/exports/e1");
            durable.Durable("src", "b", "2");
            db.CreateColumnFamilyWithImport("imp", ManualCf(), new ImportColumnFamilyOptions { MoveFiles = moveFiles }, exported);
            durable.ColumnFamilies["imp"] = true;
            durable.Durable("imp", "a", "1");
            durable.Durable("imp", "b", "2");
            db.Put(Sync, B("end"), B("1"));
            durable.Durable("end", "1");
        });
    }

    // ---- backup -----------------------------------------------------------------------------------------------------

    /// <summary>
    /// BackupEngineImpl::CreateNewBackupWithMetadata with sync: files are copied to temporary names, synced and renamed,
    /// the meta file is written to a temporary name, synced and renamed, then the directories are fsynced. Once the call
    /// returns the backup must verify and restore after a power loss.
    /// </summary>
    [TestMethod]
    [DataRow(true)]
    [DataRow(false)]
    public void BackupSurvivesPowerLoss(bool shareTableFiles)
    {
        BackupEngineOptions BackupOptions() => new("/backup") { ShareTableFiles = shareTableFiles };
        PowerLossSweep(Plain, ManualCf, (fs, durable) =>
        {
            using RocksDatabase db = OpenDb(fs, Plain, ManualCf);
            db.Put(NoSync, B("a"), B("1"));
            db.Flush();
            durable.Durable("a", "1");
            db.Put(Sync, B("b"), B("2"));
            durable.Durable("b", "2");
            using BackupEngine engine = BackupEngine.Open(BackupOptions(), fs);
            engine.CreateNewBackup(db, flushBeforeBackup: false);
            durable.Acked.Add("1");
            db.Put(NoSync, B("c"), B("3"));
            engine.CreateNewBackup(db, flushBeforeBackup: true);
            durable.Acked.Add("2");
        }, verifyMore: (fs, durable, context) =>
        {
            if (durable.Acked.Count == 0) return;
            using BackupEngine engine = BackupEngine.Open(BackupOptions(), fs);
            foreach (string id in durable.Acked)
            {
                uint backupId = uint.Parse(id, System.Globalization.CultureInfo.InvariantCulture);
                engine.VerifyBackup(backupId, verifyWithChecksum: true);
                string target = "/restored" + id;
                engine.RestoreDbFromBackup(backupId, target, target, new RestoreOptions());
                using RocksDatabase db = RocksDatabase.Open(target, new DbOptions { FileSystem = fs }, [new ColumnFamilyDescriptor("default", ManualCf())]);
                Assert.AreEqual("1", GetString(db, "a"), "backup " + id + ": " + context);
                Assert.AreEqual("2", GetString(db, "b"), "backup " + id + ": " + context);
                Assert.AreEqual(id == "2" ? "3" : null, GetString(db, "c"), "backup " + id + ": " + context);
            }
        });
    }
}
