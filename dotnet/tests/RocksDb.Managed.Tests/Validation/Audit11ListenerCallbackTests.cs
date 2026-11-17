using System.Buffers.Binary;
using System.Collections.Concurrent;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Tests.Db;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #11, EventListener callbacks the port lacked (include/rocksdb/listener.h on main): OnSubcompactionBegin /
/// OnSubcompactionCompleted (CompactionJob::NotifyOnSubcompaction*), OnCompactionPreCommit
/// (DBImpl::NotifyOnCompactionPreCommit), OnTableFileCreationStarted (EventHelpers::NotifyTableFileCreationStarted),
/// OnManualFlushScheduled (DBImpl::NotifyOnManualFlushScheduled), OnBlobFileCreationStarted / OnBlobFileCreated /
/// OnBlobFileDeleted (BlobFileCompletionCallback, LogAndNotifyBlobFileDeletion), OnErrorRecoveryCompleted
/// (EventHelpers::NotifyOnErrorRecoveryEnd), and the file I/O notifications ShouldBeNotifiedOnFileIO / OnFile*Finish /
/// OnIOError (RandomAccessFileReader, WritableFileWriter, SequentialFileReader, VersionSet::Close).
/// </summary>
[TestClass]
public sealed class Audit11ListenerCallbackTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);

    /// <summary>A finished file operation as a listener saw it (FileOperationInfo is only valid during the callback).</summary>
    private readonly record struct FileOp(FileOperationType Type, string Path, ulong Offset, ulong Length, RocksDbException? Status, TimeSpan Duration,
        DateTime Start, int Thread);

    /// <summary>Records every callback, in order, with its thread; optionally throws from the named callbacks.</summary>
    private sealed class Recorder : EventListener
    {
        public bool WantsFileIo;
        public volatile string? ThrowFrom; // a callback name, or "*"
        public readonly ConcurrentQueue<(string Name, object? Info, int Thread)> Events = new();
        public readonly ConcurrentQueue<FileOp> FileOps = new();
        public readonly ConcurrentQueue<IOErrorInfo> IoErrors = new();
        public Func<string>? OnPreCommitProbe;
        public Func<string>? OnCompletedProbe;
        public readonly ConcurrentQueue<string> Probes = new();

        private void Add(string name, object? info)
        {
            Events.Enqueue((name, info, Environment.CurrentManagedThreadId));
            string? t = ThrowFrom;
            if (t == "*" || t == name) throw new InvalidOperationException("listener failure in " + name);
        }

        private void AddFile(string name, in FileOperationInfo i)
        {
            FileOps.Enqueue(new FileOp(i.Type, i.Path, i.Offset, i.Length, i.Status, i.Duration, i.StartTime, Environment.CurrentManagedThreadId));
            string? t = ThrowFrom;
            if (t == "*" || t == name) throw new InvalidOperationException("listener failure in " + name);
        }

        public override bool ShouldBeNotifiedOnFileIO() => WantsFileIo;
        public override void OnFlushBegin(RocksDatabase? db, FlushJobInfo info) => Add("FlushBegin", info);
        public override void OnFlushCompleted(RocksDatabase? db, FlushJobInfo info) => Add("FlushCompleted", info);
        public override void OnManualFlushScheduled(RocksDatabase? db, IReadOnlyList<ManualFlushInfo> info) => Add("ManualFlushScheduled", info);
        public override void OnCompactionBegin(RocksDatabase? db, CompactionJobInfo info) => Add("CompactionBegin", info);

        public override void OnCompactionPreCommit(RocksDatabase? db, CompactionJobInfo info)
        {
            if (OnPreCommitProbe is { } p) Probes.Enqueue("precommit:" + p());
            Add("CompactionPreCommit", info);
        }

        public override void OnCompactionCompleted(RocksDatabase? db, CompactionJobInfo info)
        {
            if (OnCompletedProbe is { } p) Probes.Enqueue("completed:" + p());
            Add("CompactionCompleted", info);
        }

        public override void OnSubcompactionBegin(SubcompactionJobInfo info) => Add("SubcompactionBegin", info);
        public override void OnSubcompactionCompleted(SubcompactionJobInfo info) => Add("SubcompactionCompleted", info);
        public override void OnTableFileCreationStarted(TableFileCreationBriefInfo info) => Add("TableFileCreationStarted", info);
        public override void OnTableFileCreated(TableFileCreationInfo info) => Add("TableFileCreated", info);
        public override void OnTableFileDeleted(TableFileDeletionInfo info) => Add("TableFileDeleted", info);
        public override void OnBlobFileCreationStarted(BlobFileCreationBriefInfo info) => Add("BlobFileCreationStarted", info);
        public override void OnBlobFileCreated(BlobFileCreationInfo info) => Add("BlobFileCreated", info);
        public override void OnBlobFileDeleted(BlobFileDeletionInfo info) => Add("BlobFileDeleted", info);
#pragma warning disable CS0672 // overriding the deprecated callback on purpose
        public override void OnErrorRecoveryCompleted(RocksDbException oldBackgroundError) => Add("ErrorRecoveryCompleted", oldBackgroundError);
#pragma warning restore CS0672
        public override void OnErrorRecoveryEnd(BackgroundErrorRecoveryInfo info) => Add("ErrorRecoveryEnd", info);
        public override void OnFileReadFinish(in FileOperationInfo info) => AddFile("FileReadFinish", info);
        public override void OnFileWriteFinish(in FileOperationInfo info) => AddFile("FileWriteFinish", info);
        public override void OnFileFlushFinish(in FileOperationInfo info) => AddFile("FileFlushFinish", info);
        public override void OnFileSyncFinish(in FileOperationInfo info) => AddFile("FileSyncFinish", info);
        public override void OnFileRangeSyncFinish(in FileOperationInfo info) => AddFile("FileRangeSyncFinish", info);
        public override void OnFileTruncateFinish(in FileOperationInfo info) => AddFile("FileTruncateFinish", info);
        public override void OnFileCloseFinish(in FileOperationInfo info) => AddFile("FileCloseFinish", info);

        public override void OnIOError(IOErrorInfo info)
        {
            IoErrors.Enqueue(info);
            Add("IOError", info);
        }

        public List<string> Names(params string[] only)
        {
            var r = new List<string>();
            foreach ((string n, _, _) in Events)
            {
                if (only.Length == 0 || Array.IndexOf(only, n) >= 0) r.Add(n);
            }
            return r;
        }

        public List<T> All<T>(string name)
        {
            var r = new List<T>();
            foreach ((string n, object? i, _) in Events)
            {
                if (n == name) r.Add((T)i!);
            }
            return r;
        }

        public List<(string Name, object? Info, int Thread)> Entries(string name)
        {
            var r = new List<(string, object?, int)>();
            foreach ((string n, object? i, int t) e in Events)
            {
                if (e.n == name) r.Add(e);
            }
            return r;
        }
    }

    internal static byte[] K(int i)
    {
        byte[] k = new byte[12];
        "key-"u8.CopyTo(k);
        BinaryPrimitives.WriteInt64BigEndian(k.AsSpan(4), i);
        return k;
    }

    private static (DbOptions Options, Recorder Listener) Setup(IFileSystem fs, bool fileIo = false)
    {
        var l = new Recorder { WantsFileIo = fileIo };
        DbOptions o = NewOptions(fs);
        o.Listeners.Add(l);
        return (o, l);
    }

    private static ColumnFamilyOptions NoAutoCompaction() => new() { DisableAutoCompactions = true };

    private static void WriteL0Files(RocksDatabase db, int files, int keysPerFile, int valueSize)
    {
        var value = new byte[valueSize];
        var rnd = new Random(files * 7919 + keysPerFile);
        for (int f = 0; f < files; f++)
        {
            for (int i = 0; i < keysPerFile; i++)
            {
                rnd.NextBytes(value); // incompressible: the file sizes do not depend on the compression available
                db.Put(K(i), value);
            }
            db.Flush();
        }
    }

    /// <summary>
    /// <paramref name="files"/> overlapping L0 files of distinct keys (file f holds keys i * files + f): the anchors of
    /// the files differ, so GenSubcompactionBoundaries can split them (identical key sets collapse to one file's anchors
    /// after std::unique).
    /// </summary>
    internal static void WriteInterleavedL0Files(RocksDatabase db, int files, int keysPerFile, int valueSize)
    {
        var value = new byte[valueSize];
        var rnd = new Random(31);
        for (int f = 0; f < files; f++)
        {
            for (int i = 0; i < keysPerFile; i++)
            {
                rnd.NextBytes(value);
                db.Put(K(i * files + f), value);
            }
            db.Flush();
        }
    }

    private static DbImpl ImplOf(RocksDatabase db) => db.Impl;

    // ---- subcompactions, pre-commit, table file creation started ---------------------------------------------

    // CompactionJob::ProcessLocalKeyValueCompaction: NotifyOnSubcompactionBegin after SetupAndValidateCompactionFilter,
    // FinishCompactionOutputFile (OpenCompactionOutputFile's NotifyTableFileCreationStarted, then
    // LogAndNotifyTableFileCreationFinished), NotifyOnSubcompactionCompleted; BackgroundCompaction /
    // CompactRange: NotifyOnCompactionBegin, NotifyOnCompactionPreCommit before LogAndApply, NotifyOnCompactionCompleted.
    [TestMethod]
    public void SingleSubcompactionIsReportedBetweenBeginAndCompleted()
    {
        MemoryFileSystem fs = NewFs();
        (DbOptions o, Recorder l) = Setup(fs);
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, NoAutoCompaction());
        WriteL0Files(db, 2, 100, 10);
        l.OnPreCommitProbe = () => db.GetProperty("rocksdb.num-files-at-level0")!;
        l.OnCompletedProbe = () => db.GetProperty("rocksdb.num-files-at-level0")!;
        db.CompactRange();
        string[] compactionEvents = ["CompactionBegin", "SubcompactionBegin", "TableFileCreationStarted", "TableFileCreated", "SubcompactionCompleted",
            "CompactionPreCommit", "CompactionCompleted"];
        List<string> seq = l.Names(compactionEvents);
        // The flushes' table files come first: keep the compaction's part.
        seq = seq.GetRange(seq.IndexOf("CompactionBegin"), seq.Count - seq.IndexOf("CompactionBegin"));
        CollectionAssert.AreEqual(compactionEvents, seq, string.Join(", ", seq));

        CompactionJobInfo begin = l.All<CompactionJobInfo>("CompactionBegin").Single();
        CompactionJobInfo pre = l.All<CompactionJobInfo>("CompactionPreCommit").Single();
        CompactionJobInfo done = l.All<CompactionJobInfo>("CompactionCompleted").Single();
        var subEntries = l.Entries("SubcompactionBegin");
        var subDoneEntries = l.Entries("SubcompactionCompleted");
        var sb = (SubcompactionJobInfo)subEntries.Single().Info!;
        var sc = (SubcompactionJobInfo)subDoneEntries.Single().Info!;
        Assert.AreEqual(begin.JobId, sb.JobId);
        Assert.AreEqual(begin.JobId, sc.JobId);
        Assert.AreEqual(0, sb.SubcompactionJobId);
        Assert.AreEqual(0, sc.SubcompactionJobId);
        Assert.AreEqual("default", sb.ColumnFamilyName);
        Assert.AreEqual(0u, sb.ColumnFamilyId);
        Assert.AreEqual(0, sb.BaseInputLevel);
        Assert.AreEqual(done.OutputLevel, sb.OutputLevel);
        Assert.AreEqual(CompactionReason.ManualCompaction, sb.CompactionReason);
        Assert.AreEqual(done.CompactionReason, sc.CompactionReason);
        Assert.IsNull(sb.Status);
        Assert.IsNull(sc.Status);
        // Begin: fresh stats; completed: the input side of the subcompaction's (FinalizeSubcompactionJobStats).
        Assert.AreEqual(0L, sb.NumInputRecords);
        Assert.AreEqual(0L, sb.TotalInputRawKeyBytes);
        Assert.AreEqual(200L, sc.NumInputRecords);
        Assert.AreEqual(0L, sc.NumInputDeletionRecords);
        Assert.AreEqual(200L * 12, sc.TotalInputRawKeyBytes - 200L * 8); // user key + 8-byte internal footer
        Assert.AreEqual(200L * 10, sc.TotalInputRawValueBytes);
        // Both on the thread that ran the subcompaction.
        Assert.AreEqual(subEntries.Single().Thread, subDoneEntries.Single().Thread);

        // Pre-commit: outputs written, the inputs still live (the MANIFEST write is next); completed: committed.
        Assert.IsNull(pre.Status);
        Assert.AreEqual(1, pre.OutputFiles.Count);
        Assert.AreEqual(2, pre.InputFiles.Count);
        CollectionAssert.AreEqual(new[] { "precommit:2", "completed:0" }, l.Probes.ToArray());
        CollectionAssert.AreEqual(done.OutputFiles.ToArray(), pre.OutputFiles.ToArray());

        // The started / created events of the output name the same path, with the compaction's job id.
        var started = l.All<TableFileCreationBriefInfo>("TableFileCreationStarted").Last();
        var created = l.All<TableFileCreationInfo>("TableFileCreated").Last();
        Assert.AreEqual(TableFileCreationReason.Compaction, started.Reason);
        Assert.AreEqual(created.FilePath, started.FilePath);
        Assert.AreEqual(done.OutputFiles[0], created.FilePath);
        Assert.AreEqual(begin.JobId, started.JobId);
        Assert.AreEqual(DbPath, started.DbName);
        Assert.IsNotNull(created.TableProperties);
        Assert.AreEqual(100UL, created.TableProperties.NumEntries);
    }

    // Compaction::ShouldFormSubcompactions + GenSubcompactionBoundaries: a manual L0->L1 compaction with
    // max_subcompactions = 4 is split; "If a compaction is split to 2 sub-compactions, it will trigger one
    // OnCompactionBegin() first, then two OnSubcompactionBegin()" (listener.h). Subcompactions 1..n-1 run on their own threads.
    [TestMethod]
    public void SplitCompactionReportsEverySubcompaction()
    {
        MemoryFileSystem fs = NewFs();
        (DbOptions o, Recorder l) = Setup(fs);
        o.MaxSubcompactions = 4;
        var cf = new ColumnFamilyOptions { DisableAutoCompactions = true, TargetFileSizeBase = 32 << 10 };
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, cf);
        WriteInterleavedL0Files(db, 4, 2000, 100);
        db.CompactRange();
        CompactionJobInfo done = l.All<CompactionJobInfo>("CompactionCompleted").First();
        var begins = l.Entries("SubcompactionBegin");
        var ends = l.Entries("SubcompactionCompleted");
        Assert.IsTrue(begins.Count > 1, "the compaction was not split: " + begins.Count + " " + string.Join("; ", l.All<CompactionJobInfo>("CompactionCompleted").Select(c => c.BaseInputLevel + "->" + c.OutputLevel + " in " + c.InputFiles.Count + " bytes " + c.TotalInputBytes + " " + c.CompactionReason)));
        Assert.AreEqual(begins.Count, ends.Count);
        var ids = new HashSet<int>();
        long inputs = 0;
        foreach ((_, object? info, int thread) in ends)
        {
            var s = (SubcompactionJobInfo)info!;
            Assert.AreEqual(done.JobId, s.JobId);
            Assert.IsNull(s.Status);
            Assert.IsTrue(ids.Add(s.SubcompactionJobId));
            inputs += s.NumInputRecords;
            // Begin and completed of one subcompaction run on the same thread.
            int beginThread = begins.Single(b => ((SubcompactionJobInfo)b.Info!).SubcompactionJobId == s.SubcompactionJobId).Thread;
            Assert.AreEqual(beginThread, thread);
        }
        CollectionAssert.AreEquivalent(Enumerable.Range(0, begins.Count).ToArray(), ids.ToArray());
        Assert.AreEqual(done.NumInputRecords, inputs);
        List<string> seq = l.Names("CompactionBegin", "SubcompactionBegin", "SubcompactionCompleted", "CompactionPreCommit", "CompactionCompleted");
        Assert.AreEqual("CompactionBegin", seq[0]);
        Assert.AreEqual("CompactionPreCommit", seq[^2]);
        Assert.AreEqual("CompactionCompleted", seq[^1]);
        for (int i = 0; i < 8000; i += 397) Assert.IsNotNull(db.Get(K(i)));
    }

    // BackgroundCompaction's trivial move: NotifyOnCompactionBegin / PrepareTrivialMoveEdit / NotifyOnCompactionPreCommit /
    // CommitTrivialMove / NotifyOnCompactionCompleted; no CompactionJob runs, so no subcompaction is reported.
    [TestMethod]
    public void TrivialMoveRaisesPreCommitButNoSubcompaction()
    {
        MemoryFileSystem fs = NewFs();
        (DbOptions o, Recorder l) = Setup(fs);
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, NoAutoCompaction());
        WriteL0Files(db, 1, 10, 10);
        db.CompactRange();
        List<string> seq = l.Names("CompactionBegin", "SubcompactionBegin", "SubcompactionCompleted", "CompactionPreCommit", "CompactionCompleted");
        CollectionAssert.AreEqual(new[] { "CompactionBegin", "CompactionPreCommit", "CompactionCompleted" }, seq, string.Join(", ", seq));
        Assert.IsTrue(l.All<CompactionJobInfo>("CompactionPreCommit").Single().IsTrivialMove);
    }

    // A compaction that fails in its subcompaction: OnSubcompactionCompleted carries the failure, the safety-net
    // OnCompactionPreCommit (before ReleaseCompactionFiles) fires once with it, then OnCompactionCompleted.
    [TestMethod]
    public void FailedSubcompactionReportsItsStatus()
    {
        MemoryFileSystem mem = NewFs();
        var fs = new FaultInjectionFileSystem(mem);
        (DbOptions o, Recorder l) = Setup(fs);
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, NoAutoCompaction());
        WriteL0Files(db, 2, 100, 10);
        fs.FailMatching(".sst", FaultOps.Sync, 1, InjectedFaultKind.IOError);
        Assert.ThrowsExactly<RocksDbException>(() => db.CompactRange());
        var sc = l.All<SubcompactionJobInfo>("SubcompactionCompleted").Single();
        Assert.IsNotNull(sc.Status);
        Assert.AreEqual(StatusCode.IOError, sc.Status.Code);
        CompactionJobInfo pre = l.All<CompactionJobInfo>("CompactionPreCommit").Single();
        Assert.IsNotNull(pre.Status);
        Assert.AreEqual(StatusCode.IOError, pre.Status.Code);
        List<string> seq = l.Names("SubcompactionCompleted", "CompactionPreCommit", "CompactionCompleted");
        CollectionAssert.AreEqual(new[] { "SubcompactionCompleted", "CompactionPreCommit", "CompactionCompleted" }, seq);
    }

    // BuildTable (db/builder.cc): NotifyTableFileCreationStarted for flushes (kFlush) and the recovery flush (kRecovery).
    [TestMethod]
    public void TableFileCreationStartedForFlushAndRecovery()
    {
        MemoryFileSystem fs = NewFs();
        (DbOptions o, Recorder l) = Setup(fs);
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o))
        {
            db.Put(B("a"), B("1"));
            db.Flush();
            db.Put(B("b"), B("2"));
        }
        var flushStarted = l.All<TableFileCreationBriefInfo>("TableFileCreationStarted").Single();
        var flushCreated = l.All<TableFileCreationInfo>("TableFileCreated").Single();
        Assert.AreEqual(TableFileCreationReason.Flush, flushStarted.Reason);
        Assert.AreEqual(flushCreated.FilePath, flushStarted.FilePath);
        Assert.AreEqual(flushCreated.JobId, flushStarted.JobId);
        CollectionAssert.AreEqual(new[] { "FlushBegin", "TableFileCreationStarted", "TableFileCreated", "FlushCompleted" },
            l.Names("FlushBegin", "TableFileCreationStarted", "TableFileCreated", "FlushCompleted"));

        // Reopen without flush on close (avoid_flush_during_shutdown is off by default, so close flushed "b"): write
        // to the WAL of an instance that is not closed cleanly.
        var l2 = new Recorder();
        DbOptions o2 = NewOptions(fs);
        o2.AvoidFlushDuringShutdown = true;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o2)) db.Put(B("c"), B("3"));
        DbOptions o3 = NewOptions(fs);
        o3.Listeners.Add(l2);
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o3))
        {
            Assert.AreEqual("3", GetString(db, "c"));
        }
        var recovered = l2.All<TableFileCreationBriefInfo>("TableFileCreationStarted");
        Assert.AreEqual(1, recovered.Count, string.Join(", ", l2.Names()));
        Assert.AreEqual(TableFileCreationReason.Recovery, recovered[0].Reason);
        Assert.AreEqual(recovered[0].FilePath, l2.All<TableFileCreationInfo>("TableFileCreated").Single().FilePath);
    }

    // ---- manual flush scheduled -------------------------------------------------------------------------------

    // DBImpl::FlushMemTable: "if (flush_reason != FlushReason::kWriteBufferManager) NotifyOnManualFlushScheduled({cfd},
    // flush_reason)" after the switch, also when there was nothing to flush; on the calling thread.
    [TestMethod]
    public void ManualFlushScheduledForFlushAndCompactRange()
    {
        MemoryFileSystem fs = NewFs();
        (DbOptions o, Recorder l) = Setup(fs);
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, NoAutoCompaction());
        ColumnFamily cf = db.CreateColumnFamily("cf1", new ColumnFamilyOptions());
        db.Put(B("a"), B("1"));
        db.Flush();
        var entry = l.Entries("ManualFlushScheduled").Single();
        var infos = (IReadOnlyList<ManualFlushInfo>)entry.Info!;
        Assert.AreEqual(Environment.CurrentManagedThreadId, entry.Thread);
        Assert.AreEqual(1, infos.Count);
        Assert.AreEqual("default", infos[0].ColumnFamilyName);
        Assert.AreEqual(0u, infos[0].ColumnFamilyId);
        Assert.AreEqual(FlushReason.ManualFlush, infos[0].FlushReason);
        // Scheduled before the flush job finished.
        List<string> seq = l.Names("ManualFlushScheduled", "FlushCompleted");
        Assert.AreEqual("ManualFlushScheduled", seq[0]);

        // Nothing to flush: still reported.
        cf.Flush();
        infos = l.All<IReadOnlyList<ManualFlushInfo>>("ManualFlushScheduled")[1];
        Assert.AreEqual("cf1", infos[0].ColumnFamilyName);
        Assert.AreEqual(cf.Id, infos[0].ColumnFamilyId);

        // CompactRange flushes the memtable first (kManualCompaction).
        db.Put(B("b"), B("2"));
        db.CompactRange();
        infos = l.All<IReadOnlyList<ManualFlushInfo>>("ManualFlushScheduled")[2];
        Assert.AreEqual(FlushReason.ManualCompaction, infos[0].FlushReason);
    }

    // AtomicFlushMemTables: one ManualFlushInfo per column family flushed together.
    [TestMethod]
    public void ManualFlushScheduledListsEveryColumnFamilyOfAnAtomicFlush()
    {
        MemoryFileSystem fs = NewFs();
        (DbOptions o, Recorder l) = Setup(fs);
        o.AtomicFlush = true;
        using RocksDatabase db = RocksDatabase.Open(DbPath, o);
        ColumnFamily cf = db.CreateColumnFamily("cf1", new ColumnFamilyOptions());
        db.Put(B("a"), B("1"));
        cf.Put(B("b"), B("2"));
        db.Flush();
        var infos = l.All<IReadOnlyList<ManualFlushInfo>>("ManualFlushScheduled").Single();
        CollectionAssert.AreEquivalent(new[] { "default", "cf1" }, infos.Select(i => i.ColumnFamilyName).ToArray());
    }

    // ---- blob files ---------------------------------------------------------------------------------------

    // BlobFileBuilder::OpenBlobFileIfNeeded (OnBlobFileCreationStarted) / CloseBlobFile (OnBlobFileCompleted ->
    // LogAndNotifyBlobFileCreationFinished), within BuildTable (table started ... table created); obsolete blob files
    // (blob garbage collection) are reported by LogAndNotifyBlobFileDeletion.
    [TestMethod]
    public void BlobFileCreationAndDeletionAreReported()
    {
        MemoryFileSystem fs = NewFs();
        (DbOptions o, Recorder l) = Setup(fs);
        var cf = new ColumnFamilyOptions
        {
            DisableAutoCompactions = true,
            EnableBlobFiles = true,
            MinBlobSize = 0,
            EnableBlobGarbageCollection = true,
            BlobGarbageCollectionAgeCutoff = 1.0,
        };
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, cf);
        for (int i = 0; i < 20; i++) db.Put(K(i), B("value-" + i));
        db.Flush();
        var started = l.All<BlobFileCreationBriefInfo>("BlobFileCreationStarted").Single();
        var created = l.All<BlobFileCreationInfo>("BlobFileCreated").Single();
        Assert.AreEqual(BlobFileCreationReason.Flush, started.Reason);
        Assert.AreEqual(created.FilePath, started.FilePath);
        StringAssert.EndsWith(created.FilePath, ".blob");
        Assert.AreEqual(DbPath, created.DbName);
        Assert.AreEqual("default", created.ColumnFamilyName);
        Assert.IsNull(created.Status);
        Assert.AreEqual(20UL, created.TotalBlobCount);
        Assert.IsTrue(created.TotalBlobBytes > 20UL * 7);
        Assert.AreEqual("", created.FileChecksum);
        Assert.AreEqual("Unknown", created.FileChecksumFuncName);
        var flushInfo = l.All<FlushJobInfo>("FlushCompleted").Single();
        Assert.AreEqual(flushInfo.JobId, created.JobId);
        CollectionAssert.AreEqual(new[] { "TableFileCreationStarted", "BlobFileCreationStarted", "BlobFileCreated", "TableFileCreated" },
            l.Names("TableFileCreationStarted", "BlobFileCreationStarted", "BlobFileCreated", "TableFileCreated"));
        // Overwrite everything and compact with GC: the old blob file becomes obsolete and is deleted.
        for (int i = 0; i < 20; i++) db.Put(K(i), B("other-" + i));
        db.CompactRange();
        Assert.IsTrue(SpinWait.SpinUntil(() => l.All<BlobFileDeletionInfo>("BlobFileDeleted").Count > 0, Timeout), string.Join(", ", l.Names()));
        BlobFileDeletionInfo deleted = l.All<BlobFileDeletionInfo>("BlobFileDeleted")[0];
        Assert.AreEqual(created.FilePath, deleted.FilePath);
        Assert.IsNull(deleted.Status);
        Assert.IsFalse(fs.FileExists(deleted.FilePath));
        // The compaction's own blob file was reported with kCompaction.
        Assert.IsTrue(l.All<BlobFileCreationInfo>("BlobFileCreated").Any(b => b.Reason == BlobFileCreationReason.Compaction));
        for (int i = 0; i < 20; i++) Assert.AreEqual("other-" + i, S(db.Get(K(i))));
    }

    // ---- listeners that throw (audit #10 rule: contained, the operation completes) --------------------------------

    [TestMethod]
    [DataRow("SubcompactionBegin")]
    [DataRow("SubcompactionCompleted")]
    [DataRow("CompactionPreCommit")]
    [DataRow("TableFileCreationStarted")]
    [DataRow("ManualFlushScheduled")]
    [DataRow("BlobFileCreationStarted")]
    [DataRow("BlobFileCreated")]
    [DataRow("*")]
    public void ThrowingCallbacksDoNotFailTheOperation(string callback)
    {
        MemoryFileSystem fs = NewFs();
        (DbOptions o, Recorder l) = Setup(fs, fileIo: true);
        l.ThrowFrom = callback;
        var cf = new ColumnFamilyOptions { DisableAutoCompactions = true, EnableBlobFiles = true, MinBlobSize = 8 };
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o, cf))
        {
            WriteL0Files(db, 2, 50, 16);
            db.CompactRange();
            Assert.IsNull(db.BackgroundError);
            Assert.AreEqual("0", db.GetProperty("rocksdb.num-files-at-level0"));
            for (int i = 0; i < 50; i++) Assert.AreEqual(16, db.Get(K(i))!.Length);
        }
        Assert.IsTrue(l.Names(callback == "*" ? "SubcompactionCompleted" : callback).Count > 0, "callback " + callback + " was not raised");
        using RocksDatabase again = RocksDatabase.Open(DbPath, NewOptions(fs), cf);
        for (int i = 0; i < 50; i++) Assert.AreEqual(16, again.Get(K(i))!.Length);
    }

    // ---- deprecated OnErrorRecoveryCompleted -----------------------------------------------------------------

    // EventHelpers::NotifyOnErrorRecoveryEnd: "listener->OnErrorRecoveryCompleted(old_bg_error_cp);
    // listener->OnErrorRecoveryEnd(info);" -- a flush to a full disk recovers on Resume.
    [TestMethod]
    public void ErrorRecoveryCompletedPrecedesErrorRecoveryEnd()
    {
        if (!OperatingSystem.IsLinux() || !File.Exists("/dev/full")) Assert.Inconclusive("needs /dev/full");
        string dir = TempDir();
        try
        {
            var l = new Recorder { WantsFileIo = true };
            var o = new DbOptions { CreateIfMissing = true };
            o.Listeners.Add(l);
            using var db = RocksDatabase.Open(dir, o);
            db.Put(B("a"), B("v-a"));
            List<string> links = FillDisk(dir, ".sst", 16);
            RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
            Assert.AreEqual(StatusSubCode.NoSpace, ex.SubCode, ex.Message);
            // The physical write to the full device: OnFileWriteFinish with the failure, OnIOError(kAppend) with the
            // offset and length of the write (WritableFileWriter::WriteBuffered).
            FileOp failedWrite = l.FileOps.First(f => f.Type == FileOperationType.Write && f.Status is not null);
            StringAssert.EndsWith(failedWrite.Path, ".sst");
            Assert.AreEqual(StatusSubCode.NoSpace, failedWrite.Status!.SubCode);
            IOErrorInfo io = l.IoErrors.First(e => e.Operation == FileOperationType.Append);
            Assert.AreEqual(failedWrite.Path, io.FilePath);
            Assert.AreEqual(failedWrite.Offset, io.Offset);
            Assert.AreEqual(failedWrite.Length, io.Length);
            Assert.AreEqual(StatusSubCode.NoSpace, io.Status.SubCode);
            foreach (string link in links)
            {
                if (new FileInfo(link).LinkTarget is not null) File.Delete(link);
            }
            Assert.IsTrue(SpinWait.SpinUntil(() =>
            {
                try
                {
                    db.Resume();
                    return true;
                }
                catch (RocksDbException)
                {
                    return false;
                }
            }, Timeout));
            Assert.IsTrue(SpinWait.SpinUntil(() => l.Names("ErrorRecoveryEnd").Count > 0, Timeout));
            List<string> seq = l.Names("ErrorRecoveryCompleted", "ErrorRecoveryEnd");
            Assert.AreEqual("ErrorRecoveryCompleted", seq[0]);
            Assert.AreEqual("ErrorRecoveryEnd", seq[1]);
            var old = l.All<RocksDbException>("ErrorRecoveryCompleted")[0];
            Assert.AreEqual(StatusSubCode.NoSpace, old.SubCode);
            Assert.AreSame(old, l.All<BackgroundErrorRecoveryInfo>("ErrorRecoveryEnd")[0].OldBackgroundError);
            Assert.AreEqual("v-a", GetString(db, "a"));
        }
        finally
        {
            try { Directory.Delete(dir, true); }
            catch (IOException) { }
        }
    }

    private static string TempDir()
    {
        string d = Path.Combine(Path.GetTempPath(), "rocks-audit11-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(d);
        return d;
    }

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

    // ---- file I/O ---------------------------------------------------------------------------------------------

    private static string Kind(string path)
    {
        string name = Path.GetFileName(path);
        if (name.EndsWith(".sst", StringComparison.Ordinal)) return "sst";
        if (name.EndsWith(".log", StringComparison.Ordinal)) return "log";
        if (name.EndsWith(".blob", StringComparison.Ordinal)) return "blob";
        if (name.StartsWith("MANIFEST-", StringComparison.Ordinal)) return "manifest";
        return "other:" + name;
    }

    /// <summary>The writes of each file are contiguous from offset 0 and add up to its size.</summary>
    private static void AssertContiguousWrites(IEnumerable<FileOp> ops, string path, long size)
    {
        ulong next = 0;
        foreach (FileOp w in ops)
        {
            if (w.Path != path || w.Type != FileOperationType.Write) continue;
            Assert.AreEqual(next, w.Offset, path);
            Assert.IsNull(w.Status);
            next += w.Length;
        }
        Assert.AreEqual((ulong)size, next, path);
    }

    // The files upstream reports: table files (builder.cc / compaction_job.cc writers, TableCache readers), the WAL
    // (CreateWAL), the MANIFEST (CreateManifestWriter, VersionSet::Recover's SequentialFileReader); never the info LOG,
    // OPTIONS, CURRENT or IDENTITY. On the local file system the writes are the writer's physical writes, contiguous.
    [TestMethod]
    public void LocalFileSystemReportsTheFilesUpstreamReports()
    {
        string dir = TempDir();
        try
        {
            var l = new Recorder { WantsFileIo = true };
            var o = new DbOptions { CreateIfMissing = true };
            o.Listeners.Add(l);
            var cf = new ColumnFamilyOptions { DisableAutoCompactions = true };
            cf.TableOptions.NoBlockCache = true;
            string sst;
            string wal;
            using (var db = RocksDatabase.Open(dir, o, cf))
            {
                Assert.IsNotNull(ImplOf(db).FileIo);
                for (int i = 0; i < 1000; i++) db.Put(K(i), new byte[100]);
                db.Flush();
                sst = l.All<TableFileCreationInfo>("TableFileCreated").Single().FilePath;
                // Reads of the table (no block cache: every Get reads its blocks).
                int readsBefore = l.FileOps.Count(f => f.Type == FileOperationType.Read && f.Path == sst);
                Assert.AreEqual(100, db.Get(K(500))!.Length);
                List<FileOp> reads = l.FileOps.Where(f => f.Type == FileOperationType.Read && f.Path == sst).ToList();
                Assert.IsTrue(reads.Count > readsBefore);
                long sstSize = new FileInfo(sst).Length;
                foreach (FileOp r in reads)
                {
                    Assert.IsTrue(r.Offset + r.Length <= (ulong)sstSize);
                    Assert.IsTrue(r.Length > 0);
                    Assert.IsNull(r.Status);
                    Assert.IsTrue(r.Duration >= TimeSpan.Zero);
                    Assert.IsTrue((DateTime.UtcNow - r.Start).Duration() < TimeSpan.FromMinutes(5));
                }
                // The Get's read ran on this thread.
                Assert.AreEqual(Environment.CurrentManagedThreadId, reads[^1].Thread);
                wal = l.FileOps.First(f => Kind(f.Path) == "log").Path;
            }
            List<FileOp> ops = l.FileOps.ToList();
            var kinds = ops.Select(f => Kind(f.Path)).Distinct().OrderBy(k => k, StringComparer.Ordinal).ToArray();
            CollectionAssert.AreEqual(new[] { "log", "manifest", "sst" }, kinds, string.Join(", ", kinds));
            AssertContiguousWrites(ops, sst, new FileInfo(sst).Length);
            // The table: synced and closed after its last write; the close reports the flush before it (OK).
            List<FileOp> sstOps = ops.Where(f => f.Path == sst && f.Type != FileOperationType.Read).ToList();
            Assert.AreEqual(FileOperationType.Close, sstOps[^1].Type);
            Assert.IsTrue(sstOps.Any(f => f.Type == FileOperationType.Sync));
            Assert.IsTrue(sstOps.FindLastIndex(f => f.Type == FileOperationType.Write) < sstOps.FindIndex(f => f.Type == FileOperationType.Sync));
            // The WAL: a write and a flush per write group (log::Writer flushes after each record).
            List<FileOp> walOps = ops.Where(f => f.Path == wal).ToList();
            Assert.IsTrue(walOps.Count(f => f.Type == FileOperationType.Write) >= 1000, walOps.Count.ToString());
            Assert.IsTrue(walOps.Count(f => f.Type == FileOperationType.Flush) >= 1000);
            Assert.IsFalse(walOps.Any(f => f.Type == FileOperationType.Read), "WAL recovery reads are not reported");
            Assert.IsFalse(l.IoErrors.Any());

            // Reopen: the MANIFEST is read (recovery), from offset 0, contiguously; the WAL replay is not reported.
            var l2 = new Recorder { WantsFileIo = true };
            var o2 = new DbOptions();
            o2.Listeners.Add(l2);
            using (var db = RocksDatabase.Open(dir, o2, cf))
            {
                Assert.AreEqual(100, db.Get(K(1))!.Length);
            }
            List<FileOp> manifestReads = l2.FileOps.Where(f => f.Type == FileOperationType.Read && Kind(f.Path) == "manifest").ToList();
            Assert.IsTrue(manifestReads.Count > 0);
            ulong offset = 0;
            string firstManifest = manifestReads[0].Path;
            foreach (FileOp r in manifestReads.Where(r => r.Path == firstManifest))
            {
                Assert.AreEqual(offset, r.Offset);
                offset += r.Length;
            }
            Assert.IsFalse(l2.FileOps.Any(f => f.Type == FileOperationType.Read && Kind(f.Path) == "log"));
        }
        finally
        {
            try { Directory.Delete(dir, true); }
            catch (IOException) { }
        }
    }

    // WritableFileWriter::RangeSync with bytes_per_sync: OnFileRangeSyncFinish(offset = last_sync_size_, length).
    [TestMethod]
    public void RangeSyncsOfTableFilesAreReported()
    {
        string dir = TempDir();
        try
        {
            var l = new Recorder { WantsFileIo = true };
            var o = new DbOptions { CreateIfMissing = true, BytesPerSync = 64 << 10 };
            o.Listeners.Add(l);
            using (var db = RocksDatabase.Open(dir, o, new ColumnFamilyOptions { WriteBufferSize = 64UL << 20 }))
            {
                var rnd = new Random(11);
                var value = new byte[1000];
                for (int i = 0; i < 4000; i++)
                {
                    rnd.NextBytes(value);
                    db.Put(K(i), value);
                }
                db.Flush();
            }
            List<FileOp> syncs = l.FileOps.Where(f => f.Type == FileOperationType.RangeSync).ToList();
            Assert.IsTrue(syncs.Count > 0);
            ulong next = 0;
            foreach (FileOp s in syncs.Where(s => s.Path == syncs[0].Path))
            {
                Assert.AreEqual(next, s.Offset);
                Assert.IsTrue(s.Length >= 64 << 10);
                next += s.Length;
            }
            Assert.IsTrue(syncs.All(s => Kind(s.Path) == "sst"), "only the table: without wal_bytes_per_sync the WAL writeback is not reported");
        }
        finally
        {
            try { Directory.Delete(dir, true); }
            catch (IOException) { }
        }
    }

    // TransactionLogIteratorImpl::OpenLogFile: SequentialFileReader(..., options_->listeners, ...): GetUpdatesSince reads
    // of the WAL are reported (WalManager::ReadFirstLine's are not).
    [TestMethod]
    public void GetUpdatesSinceReadsOfTheWalAreReported()
    {
        MemoryFileSystem fs = NewFs();
        (DbOptions o, Recorder l) = Setup(fs, fileIo: true);
        using RocksDatabase db = RocksDatabase.Open(DbPath, o);
        for (int i = 0; i < 10; i++) db.Put(K(i), B("v"));
        int before = l.FileOps.Count(f => f.Type == FileOperationType.Read && Kind(f.Path) == "log");
        int n = 0;
        using (TransactionLogIterator it = db.GetUpdatesSince(1))
        {
            for (; it.Valid; it.Next()) n++;
        }
        Assert.AreEqual(10, n);
        List<FileOp> reads = l.FileOps.Where(f => f.Type == FileOperationType.Read && Kind(f.Path) == "log").ToList();
        Assert.IsTrue(reads.Count > before);
        Assert.AreEqual(0UL, reads[before].Offset);
    }

    // RandomAccessFileReader::Read: "NotifyOnFileReadFinish(offset + pos, tmp_result.size(), ..., io_s); if (!io_s.ok())
    // NotifyOnIOError(io_s, kRead, file_name(), tmp_result.size(), offset + pos)". The Get fails, the DB stays usable.
    [TestMethod]
    public void FailedTableReadIsReportedAndOnIOErrorRaised()
    {
        var fs = new ReadFailingFileSystem(NewFs());
        (DbOptions o, Recorder l) = Setup(fs, fileIo: true);
        var cf = new ColumnFamilyOptions();
        cf.TableOptions.NoBlockCache = true;
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, cf);
        for (int i = 0; i < 100; i++) db.Put(K(i), B("value-" + i));
        db.Flush();
        fs.FailReads = true;
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.Get(K(7)));
        Assert.AreEqual(StatusCode.IOError, ex.Code);
        FileOp failed = l.FileOps.Last(f => f.Type == FileOperationType.Read);
        Assert.IsNotNull(failed.Status);
        Assert.AreEqual(StatusCode.IOError, failed.Status.Code);
        Assert.AreEqual(0UL, failed.Length);
        IOErrorInfo io = l.IoErrors.Single();
        Assert.AreEqual(FileOperationType.Read, io.Operation);
        Assert.AreEqual(failed.Path, io.FilePath);
        Assert.AreEqual(failed.Offset, io.Offset);
        Assert.AreEqual(StatusCode.IOError, io.Status.Code);
        fs.FailReads = false;
        Assert.AreEqual("value-7", S(db.Get(K(7))));
    }

    // WritableFileWriter::SyncInternal: "NotifyOnFileSyncFinish(..., s, kSync); if (!s.ok()) NotifyOnIOError(s, kSync,
    // file_name())"; then the writer has seen an error: its close is not reported (Close returns early).
    [TestMethod]
    public void FailedTableSyncIsReportedAndOnIOErrorRaised()
    {
        var fs = new FaultInjectionFileSystem(NewFs());
        (DbOptions o, Recorder l) = Setup(fs, fileIo: true);
        using RocksDatabase db = RocksDatabase.Open(DbPath, o);
        db.Put(B("a"), B("1"));
        fs.FailMatching(".sst", FaultOps.Sync, 1, InjectedFaultKind.IOError);
        Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
        FileOp sync = l.FileOps.Single(f => f.Type == FileOperationType.Sync && f.Status is not null);
        StringAssert.EndsWith(sync.Path, ".sst");
        IOErrorInfo io = l.IoErrors.Single(e => e.Operation == FileOperationType.Sync);
        Assert.AreEqual(sync.Path, io.FilePath);
        Assert.IsFalse(l.FileOps.Any(f => f.Path == sync.Path && f.Type == FileOperationType.Close));
        // Appends of other file systems are reported one by one, contiguously (no physical write reporting there).
        ulong next = 0;
        foreach (FileOp w in l.FileOps.Where(f => f.Path == sync.Path && f.Type == FileOperationType.Write))
        {
            Assert.AreEqual(next, w.Offset);
            next += w.Length;
        }
        Assert.IsTrue(next > 0);
    }

    // File I/O callbacks that throw: the I/O and the operation go on (Get, Put, flush, compaction, reopen).
    [TestMethod]
    [DataRow("FileReadFinish")]
    [DataRow("FileWriteFinish")]
    [DataRow("FileFlushFinish")]
    [DataRow("FileSyncFinish")]
    [DataRow("FileCloseFinish")]
    public void ThrowingFileIoCallbacksDoNotFailTheIo(string callback)
    {
        string dir = TempDir();
        try
        {
            var l = new Recorder { WantsFileIo = true, ThrowFrom = callback };
            var o = new DbOptions { CreateIfMissing = true };
            o.Listeners.Add(l);
            var cf = new ColumnFamilyOptions { DisableAutoCompactions = true };
            cf.TableOptions.NoBlockCache = true;
            using (var db = RocksDatabase.Open(dir, o, cf))
            {
                WriteL0Files(db, 2, 200, 50);
                db.CompactRange();
                for (int i = 0; i < 200; i++) Assert.AreEqual(50, db.Get(K(i))!.Length);
                Assert.IsNull(db.BackgroundError);
            }
            Assert.IsTrue(l.FileOps.Any(f => "File" + f.Type + "Finish" == callback), "no " + callback);
            using var again = RocksDatabase.Open(dir, o, cf);
            for (int i = 0; i < 200; i++) Assert.AreEqual(50, again.Get(K(i))!.Length);
        }
        finally
        {
            try { Directory.Delete(dir, true); }
            catch (IOException) { }
        }
    }

    // ---- zero cost when nobody wants file I/O ----------------------------------------------------------------

    private sealed class CountingFileIo : EventListener
    {
        public long Reads;
        public long Bytes;
        public override bool ShouldBeNotifiedOnFileIO() => true;

        public override void OnFileReadFinish(in FileOperationInfo info)
        {
            Interlocked.Increment(ref Reads);
            Interlocked.Add(ref Bytes, (long)info.Length);
        }
    }

    private sealed class NotFileIo : EventListener
    {
        public int Flushes;
        public override void OnFlushCompleted(RocksDatabase? db, FlushJobInfo info) => Interlocked.Increment(ref Flushes);
    }

    // WritableFileWriter / RandomAccessFileReader keep only the listeners whose ShouldBeNotifiedOnFileIO() is true; with
    // none, ShouldNotifyListeners() is false: no file is wrapped, the reads and writes allocate nothing.
    [TestMethod]
    [DataRow("none")]
    [DataRow("plain")]
    [DataRow("fileio")]
    public void FileIoNotificationsCostNothingWhenNotWanted(string mode)
    {
        string dir = TempDir();
        try
        {
            var o = new DbOptions { CreateIfMissing = true };
            CountingFileIo? rec = null;
            if (mode == "plain") o.Listeners.Add(new NotFileIo());
            if (mode == "fileio") o.Listeners.Add(rec = new CountingFileIo());
            var cf = new ColumnFamilyOptions { WriteBufferSize = 64UL << 20 };
            cf.TableOptions.NoBlockCache = true;
            using var db = RocksDatabase.Open(dir, o, cf);
            Assert.AreEqual(mode == "fileio", ImplOf(db).FileIo is not null);
            const int N = 2000;
            byte[] value = new byte[100];
            byte[] dst = new byte[256];
            for (int i = 0; i < N; i++) db.Put(K(i), value);
            db.Flush();
            for (int round = 0; round < 3; round++)
            {
                for (int i = 0; i < N; i++) db.TryGet(K(i), dst, out _);
            }
            byte[][] keys = new byte[N][];
            for (int i = 0; i < N; i++) keys[i] = K(i);
            long readsBefore = rec?.Reads ?? 0;
            long before = GC.GetAllocatedBytesForCurrentThread();
            int found = 0;
            for (int i = 0; i < N; i++)
            {
                if (db.TryGet(keys[i], dst, out _)) found++;
            }
            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            Assert.AreEqual(N, found);
            Assert.AreEqual(0L, allocated, mode + ": the reads allocated");
            // With a listener that wants file I/O the reads are reported, still without allocating (FileOperationInfo is a
            // struct passed by reference).
            if (rec is not null) Assert.IsTrue(rec.Reads - readsBefore >= N, "the reads were reported");
        }
        finally
        {
            try { Directory.Delete(dir, true); }
            catch (IOException) { }
        }
    }

    // ---- MANIFEST checks on close (kVerify) -------------------------------------------------------------------

    // VersionSet::Close: a corrupt MANIFEST found by verify_manifest_content_on_close is reported to every listener as
    // IOErrorInfo(IOStatus::Corruption("MANIFEST content validation failed"), kVerify, content_manifest_name, 0, 0),
    // whether or not it asked for file I/O notifications.
    [TestMethod]
    public void CorruptManifestOnCloseRaisesOnIOErrorVerify()
    {
        string dir = TempDir();
        try
        {
            var l = new Recorder();
            var o = new DbOptions { CreateIfMissing = true, VerifyManifestContentOnClose = true };
            o.Listeners.Add(l);
            ulong corrupted;
            using (RocksDatabase db = RocksDatabase.Open(dir, o, new ColumnFamilyOptions()))
            {
                for (int i = 0; i < 5; i++)
                {
                    db.Put(K(i), B("v"));
                    db.Flush();
                }
                corrupted = db.Impl.Versions.ManifestFileNumber;
                using var s = new FileStream(FileNames.DescriptorFileName(dir, corrupted), FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
                s.Position = s.Length / 2;
                int b = s.ReadByte();
                s.Position = s.Length / 2;
                s.WriteByte((byte)(b ^ 0xff));
            }
            IOErrorInfo io = l.IoErrors.Single();
            Assert.AreEqual(FileOperationType.Verify, io.Operation);
            Assert.AreEqual(FileNames.DescriptorFileName(dir, corrupted), io.FilePath);
            Assert.AreEqual(StatusCode.Corruption, io.Status.Code);
            StringAssert.Contains(io.Status.Message, "MANIFEST content validation failed");
            Assert.AreEqual(0UL, io.Length);
        }
        finally
        {
            try { Directory.Delete(dir, true); }
            catch (IOException) { }
        }
    }

    /// <summary>A <see cref="MemoryFileSystem"/> whose random access reads fail with IOError while <see cref="FailReads"/>.</summary>
    private sealed class ReadFailingFileSystem(MemoryFileSystem inner) : IFileSystem
    {
        public volatile bool FailReads;

        private sealed class Failing(ReadFailingFileSystem owner, IRandomAccessFile f) : IRandomAccessFile
        {
            public long Length => f.Length;

            public int Read(long offset, Span<byte> destination)
            {
                if (owner.FailReads) throw new RocksDbException(StatusCode.IOError, "IO error: injected read error");
                return f.Read(offset, destination);
            }

            public ValueTask<int> ReadAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken = default) =>
                owner.FailReads ? ValueTask.FromException<int>(new RocksDbException(StatusCode.IOError, "IO error: injected read error"))
                    : f.ReadAsync(offset, destination, cancellationToken);

            public void Dispose() => f.Dispose();
        }

        public IWritableFile CreateWritable(string path, bool truncate = true) => inner.CreateWritable(path, truncate);
        public IWritableFile CreateWritable(string path, bool truncate, WritableFileOptions options) => inner.CreateWritable(path, truncate, options);
        public IWritableFile ReopenWritable(string path, WritableFileOptions options) => inner.ReopenWritable(path, options);
        public IRandomAccessFile OpenRandomAccess(string path) => new Failing(this, inner.OpenRandomAccess(path));
        public IRandomAccessFile OpenRandomAccess(string path, RandomAccessFileOptions options) => new Failing(this, inner.OpenRandomAccess(path, options));
        public ISequentialFile OpenSequential(string path) => inner.OpenSequential(path);
        public ISequentialFile OpenSequential(string path, SequentialFileOptions options) => inner.OpenSequential(path, options);
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
        public void LinkFile(string source, string target) => inner.LinkFile(source, target);
        public void WriteAt(string path, long offset, ReadOnlySpan<byte> data) => inner.WriteAt(path, offset, data);
        public DateTimeOffset GetFileModificationTime(string path) => inner.GetFileModificationTime(path);
        public long GetFreeSpace(string path) => inner.GetFreeSpace(path);
    }
}
