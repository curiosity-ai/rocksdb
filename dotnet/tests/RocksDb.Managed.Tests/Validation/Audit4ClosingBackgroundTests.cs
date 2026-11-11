using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4 closing (remote compaction, background jobs, file metadata, best-efforts recovery): behaviours pinned
/// against db/compaction/compaction_service_job.cc, db/db_impl/db_impl_secondary.cc and db/version_edit.h.
/// </summary>
[TestClass]
public sealed class Audit4ClosingBackgroundTests
{
    /// <summary>A compaction service whose answers are scripted by the test.</summary>
    private sealed class ScriptedService : CompactionService
    {
        public CompactionServiceJobStatus ScheduleStatus = CompactionServiceJobStatus.Success;
        public CompactionServiceWaitResult WaitResult = new(CompactionServiceJobStatus.UseLocal, null);
        public byte[]? Input;
        public readonly List<CompactionServiceJobStatus> Installed = new();

        public override string Name => "Scripted";

        public override ValueTask<CompactionServiceScheduleResponse> ScheduleAsync(CompactionServiceJobInfo info, ReadOnlyMemory<byte> input,
            CancellationToken cancellationToken)
        {
            Input = input.ToArray();
            return ValueTask.FromResult(new CompactionServiceScheduleResponse("job", ScheduleStatus));
        }

        public override ValueTask<CompactionServiceWaitResult> WaitAsync(string scheduledJobId, CancellationToken cancellationToken) =>
            ValueTask.FromResult(WaitResult);

        public override void OnInstallation(string scheduledJobId, CompactionServiceJobStatus status)
        {
            lock (Installed) Installed.Add(status);
        }
    }

    private static RocksDatabase OpenWithService(MemoryFileSystem fs, ScriptedService svc)
    {
        DbOptions o = NewOptions(fs);
        o.CompactionService = svc;
        return RocksDatabase.Open(DbPath, o, new ColumnFamilyOptions { DisableAutoCompactions = true });
    }

    private static void FillTwoFiles(RocksDatabase db)
    {
        for (int f = 0; f < 2; f++)
        {
            for (int i = 0; i < 10; i++) db.Put(Key(i), Value(i, f));
            db.Flush();
        }
    }

    /// <summary>A primary whose service fails at Schedule: the job input is captured and the input files stay in place.</summary>
    private static byte[] CaptureInput(MemoryFileSystem fs)
    {
        var svc = new ScriptedService { ScheduleStatus = CompactionServiceJobStatus.Failure };
        using RocksDatabase db = OpenWithService(fs, svc);
        FillTwoFiles(db);
        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => db.CompactRange());
        Assert.AreEqual(StatusCode.Incomplete, e.Code);
        Assert.IsNotNull(svc.Input);
        return svc.Input;
    }

    // CompactionJob::ProcessKeyValueCompactionWithCompactionService (compaction_service_job.cc:259-263): the primary
    // sends its per-key placement decisions; without placement these are kInvalidLevel (-1), kNotSupported (0) and
    // kMaxSequenceNumber, and DBImplSecondary::CompactWithoutInstallation declines when the worker's differ. A
    // proximal_level of 0 means "an older primary that sent no decisions" and disables the worker's check.
    [TestMethod]
    public void PrimarySendsItsPerKeyPlacementDecision()
    {
        CompactionServiceInput ci = CompactionServiceFormat.ReadInput(CaptureInput(NewFs()));
        Assert.AreEqual(-1, ci.ProximalLevel);
        Assert.AreEqual(0, ci.ProximalOutputRangeType);
        Assert.AreEqual(DbFormat.MaxSequenceNumber, ci.KeepInLastLevelThroughSeqno);
    }

    // DB::OpenAndCompact, step 5b (db_impl_secondary.cc:2269-2293): below_floor is also "the same MANIFEST number, but
    // recovered short of min_manifest_file_size" (a truncated or torn MANIFEST) -> Status::Incomplete; a view at the
    // floor is accepted.
    [TestMethod]
    public void WorkerDeclinesAManifestRecoveredShortOfThePrimarysSize()
    {
        MemoryFileSystem fs = NewFs();
        byte[] input = CaptureInput(fs);
        var ov = new CompactionServiceOptionsOverride { FileSystem = fs };
        CompactionServiceInput ci = CompactionServiceFormat.ReadInput(input);
        Assert.AreNotEqual(0UL, ci.MinManifestFileNumber);
        Assert.AreNotEqual(0UL, ci.MinManifestFileSize);
        ci.MinManifestFileSize += 1;
        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.OpenAndCompact(DbPath, "short", CompactionServiceFormat.Write(ci), ov));
        Assert.AreEqual(StatusCode.Incomplete, e.Code);
        StringAssert.Contains(e.Message, "older MANIFEST view");

        // Exactly at the floor: the compaction runs.
        CompactionServiceResult r = CompactionServiceFormat.ReadResult(RocksDatabase.OpenAndCompact(DbPath, "atfloor", input, ov));
        Assert.IsTrue(r.Ok);
        Assert.AreEqual(1, r.OutputFiles.Count);
    }

    // FileMetaData::CalculateTailSize (db/version_edit.h:472-496), used by ExternalSstFileIngestionJob::GetIngestedFileInfo
    // (external_sst_file_ingestion_job.cc:875): a file without data blocks (range deletions only) has tail_start_offset
    // 0 and its whole size is tail; with data blocks the tail starts at tail_start_offset. The tail size goes into the
    // MANIFEST (kTailSize) and sizes the tail prefetch of every later open.
    [TestMethod]
    public void IngestedFileWithoutDataBlocksIsAllTail()
    {
        MemoryFileSystem fs = NewFs();
        fs.CreateDirectoryIfMissing("/ext");
        using (var w = new SstFileWriter(new ColumnFamilyOptions(), fs))
        {
            w.Open("/ext/rangedel.sst");
            w.DeleteRange(B("a"), B("m"));
            w.Finish();
        }
        using (var w = new SstFileWriter(new ColumnFamilyOptions(), fs))
        {
            w.Open("/ext/data.sst");
            w.Put(B("x"), B("1"));
            w.Finish();
        }
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { DisableAutoCompactions = true });
        db.IngestExternalFiles(["/ext/rangedel.sst", "/ext/data.sst"], new IngestExternalFileOptions());
        int seen = 0;
        VersionStorageInfo storage = db.DefaultColumnFamily.Data.Current.Storage;
        for (int level = 0; level < storage.NumLevels; level++)
        {
            foreach (FileMetaData f in storage.LevelFiles(level))
            {
                seen++;
                bool rangeDelOnly = f.Smallest.AsSpan(0, 1).SequenceEqual(B("a"));
                if (rangeDelOnly) Assert.AreEqual(f.FileSize, f.TailSize);
                else Assert.IsTrue(f.TailSize > 0 && f.TailSize < f.FileSize, "tail " + f.TailSize + " of " + f.FileSize);
            }
        }
        Assert.AreEqual(2, seen);
    }

    // ProcessKeyValueCompactionWithCompactionService (compaction_service_job.cc:463-473 before 475-493): a result whose
    // status is not OK fails the compaction with that status and OnInstallation(kFailure); the BlobDB checks that
    // decline a result (kUseLocal) only come afterwards.
    [TestMethod]
    public void NonOkRemoteResultFailsBeforeTheBlobChecks()
    {
        MemoryFileSystem fs = NewFs();
        var failed = new CompactionServiceResult { StatusCode = StatusCode.IOError, StatusMessage = "IO error: worker disk" };
        failed.OutputFiles.Add(new CompactionServiceOutputFile { FileName = "000099.sst", FileSize = 1, OldestBlobFileNumber = 7 });
        var svc = new ScriptedService { WaitResult = new(CompactionServiceJobStatus.Success, CompactionServiceFormat.Write(failed)) };
        using RocksDatabase db = OpenWithService(fs, svc);
        FillTwoFiles(db);
        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => db.CompactRange());
        Assert.AreEqual(StatusCode.IOError, e.Code);
        CollectionAssert.AreEqual(new[] { CompactionServiceJobStatus.Failure }, svc.Installed);
    }
}
