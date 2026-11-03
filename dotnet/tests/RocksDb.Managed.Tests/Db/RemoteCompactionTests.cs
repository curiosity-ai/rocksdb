using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>
/// Remote compaction (db/compaction/compaction_service_test.cc): the CompactionServiceInput / Result wire format, and a
/// primary whose "remote" worker runs <see cref="RocksDatabase.OpenAndCompact(OpenAndCompactOptions, string, string, ReadOnlySpan{byte}, CompactionServiceOptionsOverride)"/>
/// in-process on the same file system.
/// </summary>
[TestClass]
public sealed class RemoteCompactionTests
{
    // ---- format -----------------------------------------------------------------------------------------

    /// <summary>An input written by native RocksDB 11.12 (CompactRange of three L0 files into L6).</summary>
    private const string NativeInput =
        "min_manifest_file_size=473;options_file_number=7;has_end=false;has_begin=false;min_manifest_file_number=5;" +
        "db_id=63386566303262622D323330382D346133622D613261312D623737633634623831383763;output_level=6;end=;" +
        "input_files=3030303031332E737374:3030303031312E737374:3030303030392E737374;begin=;snapshots=;cf_name=64656661756C74;";

    private static byte[] WithVersion(string s)
    {
        byte[] b = new byte[4 + s.Length];
        b[0] = 1;
        Encoding.UTF8.GetBytes(s, b.AsSpan(4));
        return b;
    }

    [TestMethod]
    public void ReadsNativeInput()
    {
        CompactionServiceInput i = CompactionServiceFormat.ReadInput(WithVersion(NativeInput));
        Assert.AreEqual("default", i.CfName);
        Assert.AreEqual(6, i.OutputLevel);
        CollectionAssert.AreEqual(new[] { "000013.sst", "000011.sst", "000009.sst" }, i.InputFiles);
        Assert.AreEqual(0, i.Snapshots.Count);
        Assert.AreEqual("c8ef02bb-2308-4a3b-a2a1-b77c64b8187c", i.DbId);
        Assert.AreEqual(7UL, i.OptionsFileNumber);
        Assert.AreEqual(5UL, i.MinManifestFileNumber);
        Assert.AreEqual(473UL, i.MinManifestFileSize);
        Assert.IsFalse(i.HasBegin);
        Assert.AreEqual(0, i.ProximalLevel);
    }

    [TestMethod]
    public void InputAndResultRoundTrip()
    {
        var input = new CompactionServiceInput
        {
            CfName = "cf;with=special:chars",
            Snapshots = [3, 9, 100],
            InputFiles = ["000001.sst", "000007.sst"],
            OutputLevel = 3,
            DbId = "id",
            HasBegin = true,
            Begin = [0, 1, 2, 0xff],
            OptionsFileNumber = 12,
            ProximalLevel = -1,
        };
        CompactionServiceInput back = CompactionServiceFormat.ReadInput(CompactionServiceFormat.Write(input));
        Assert.AreEqual(input.CfName, back.CfName);
        CollectionAssert.AreEqual(input.Snapshots, back.Snapshots);
        CollectionAssert.AreEqual(input.InputFiles, back.InputFiles);
        Assert.AreEqual(3, back.OutputLevel);
        Assert.IsTrue(back.HasBegin);
        CollectionAssert.AreEqual(input.Begin, back.Begin);
        Assert.IsFalse(back.HasEnd);
        Assert.AreEqual(-1, back.ProximalLevel);

        var props = new TableProperties { NumEntries = 7, TailStartOffset = 100, DbId = "x", ColumnFamilyName = "c" };
        props.UserCollectedProperties["rocksdb.block.based.table.index.type"] = [0, 0, 0, 0];
        var result = new CompactionServiceResult
        {
            OutputLevel = 3,
            OutputPath = "/db/job",
            BytesRead = 10,
            BytesWritten = 20,
            OutputFiles =
            {
                new CompactionServiceOutputFile { FileName = "000020.sst", FileSize = 1234, SmallestSeqno = 1, LargestSeqno = 9, SmallestInternalKey = [1, 2],
                    LargestInternalKey = [3, 4], UniqueId = new UniqueId64x2(5, 6), TableProperties = props, FileTemperature = Temperature.Warm },
                new CompactionServiceOutputFile { FileName = "000021.sst", FileSize = 99, MarkedForCompaction = true },
            },
        };
        result.OutputLevelStats.Counts[9] = 1;
        CompactionServiceResult r = CompactionServiceFormat.ReadResult(CompactionServiceFormat.Write(result));
        Assert.IsTrue(r.Ok);
        Assert.AreEqual("/db/job", r.OutputPath);
        Assert.AreEqual(2, r.OutputFiles.Count);
        CompactionServiceOutputFile f = r.OutputFiles[0];
        Assert.AreEqual("000020.sst", f.FileName);
        Assert.AreEqual(1234UL, f.FileSize);
        Assert.AreEqual(new UniqueId64x2(5, 6), f.UniqueId);
        Assert.AreEqual(Temperature.Warm, f.FileTemperature);
        Assert.AreEqual(7UL, f.TableProperties.NumEntries);
        Assert.AreEqual(100UL, f.TableProperties.TailStartOffset);
        CollectionAssert.AreEqual(new byte[] { 0, 0, 0, 0 }, f.TableProperties.UserCollectedProperties["rocksdb.block.based.table.index.type"]);
        Assert.IsTrue(r.OutputFiles[1].MarkedForCompaction);
        Assert.AreEqual(1, r.OutputLevelStats.Counts[9]);

        var failed = new CompactionServiceResult { StatusCode = StatusCode.Incomplete, StatusSubCode = StatusSubCode.ManualCompactionPaused, StatusMessage = "paused" };
        CompactionServiceResult fr = CompactionServiceFormat.ReadResult(CompactionServiceFormat.Write(failed));
        Assert.AreEqual(StatusCode.Incomplete, fr.StatusCode);
        Assert.AreEqual(StatusSubCode.ManualCompactionPaused, fr.StatusSubCode);
        Assert.AreEqual("paused", fr.StatusMessage);
    }

    [TestMethod]
    public void RejectsBadInput()
    {
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => CompactionServiceFormat.ReadInput([1, 0, 0, 0])).Code);
        byte[] v2 = WithVersion(NativeInput);
        v2[0] = 2;
        Assert.AreEqual(StatusCode.NotSupported, Assert.ThrowsExactly<RocksDbException>(() => CompactionServiceFormat.ReadInput(v2)).Code);
        Assert.AreEqual(StatusCode.InvalidArgument,
            Assert.ThrowsExactly<RocksDbException>(() => CompactionServiceFormat.ReadInput(WithVersion("snapshots=5:3;"))).Code);
        // Unknown fields are ignored.
        Assert.AreEqual("default", CompactionServiceFormat.ReadInput(WithVersion(NativeInput + "some_future_field={a=b;c=d};")).CfName);
    }

    // ---- end to end ---------------------------------------------------------------------------------------

    /// <summary>A compaction service whose worker runs OpenAndCompact in-process, like the upstream test's MyTestCompactionService.</summary>
    private sealed class InProcessService(string dbPath, IFileSystem fs, MergeOperator? merge) : CompactionService
    {
        private readonly Dictionary<string, byte[]> _jobs = new();
        private int _next;
        public CompactionServiceJobStatus ScheduleStatus = CompactionServiceJobStatus.Success;
        public CompactionServiceJobStatus? ForcedWaitStatus;
        public bool CorruptResult;
        public readonly List<CompactionServiceJobInfo> Infos = new();
        public readonly List<(string, CompactionServiceJobStatus)> Installed = new();
        public int Completed;

        public override string Name => "InProcess";

        public override ValueTask<CompactionServiceScheduleResponse> ScheduleAsync(CompactionServiceJobInfo info, ReadOnlyMemory<byte> input,
            CancellationToken cancellationToken)
        {
            lock (_jobs)
            {
                Infos.Add(info);
                string id = "job-" + (++_next).ToString(System.Globalization.CultureInfo.InvariantCulture);
                _jobs[id] = input.ToArray();
                return ValueTask.FromResult(new CompactionServiceScheduleResponse(id, ScheduleStatus));
            }
        }

        public override async ValueTask<CompactionServiceWaitResult> WaitAsync(string scheduledJobId, CancellationToken cancellationToken)
        {
            byte[] input;
            lock (_jobs) input = _jobs[scheduledJobId];
            if (ForcedWaitStatus is { } forced) return new CompactionServiceWaitResult(forced, null);
            try
            {
                byte[] result = await RocksDatabase.OpenAndCompactAsync(new OpenAndCompactOptions(), dbPath, scheduledJobId, input,
                    new CompactionServiceOptionsOverride { FileSystem = fs, MergeOperator = merge }, cancellationToken);
                Interlocked.Increment(ref Completed);
                if (CorruptResult) result = [1, 0, 0, 0, (byte)'x'];
                return new CompactionServiceWaitResult(CompactionServiceJobStatus.Success, result);
            }
            catch (OpenAndCompactException e)
            {
                return new CompactionServiceWaitResult(CompactionServiceJobStatus.Failure, e.Result);
            }
        }

        public override void OnInstallation(string scheduledJobId, CompactionServiceJobStatus status)
        {
            lock (Installed) Installed.Add((scheduledJobId, status));
        }
    }

    private static (RocksDatabase Db, InProcessService Service) Open(MemoryFileSystem fs, bool autoCompactions)
    {
        var merge = new StringAppendMergeOperator();
        var svc = new InProcessService(DbPath, fs, merge);
        DbOptions o = NewOptions(fs);
        o.CompactionService = svc;
        var cfo = new ColumnFamilyOptions { MergeOperator = merge, DisableAutoCompactions = !autoCompactions, Level0FileNumCompactionTrigger = 4 };
        return (RocksDatabase.Open(DbPath, o, cfo), svc);
    }

    private static void FillFiles(RocksDatabase db, int files, int keys)
    {
        for (int f = 0; f < files; f++)
        {
            for (int i = 0; i < keys; i++) db.Put(Key(i), Value(i, f));
            db.Merge(B("m"), B("op" + f));
            db.Delete(Key(f));
            db.Flush();
        }
    }

    [TestMethod]
    public void ManualCompactionRunsRemotely()
    {
        MemoryFileSystem fs = NewFs();
        (RocksDatabase db, InProcessService svc) = Open(fs, autoCompactions: false);
        using (db)
        {
            FillFiles(db, 3, 50);
            using Snapshot snap = db.GetSnapshot();
            db.Put(Key(10), B("after-snapshot"));
            db.Flush();
            db.CompactRange();
            Assert.AreEqual(1, svc.Completed);
            Assert.AreEqual(1, svc.Infos.Count);
            Assert.IsTrue(svc.Infos[0].IsManualCompaction);
            Assert.AreEqual(db.DbSessionId, svc.Infos[0].DbSessionId);
            Assert.AreEqual(CompactionServiceJobStatus.Success, svc.Installed[0].Item2);
            foreach (LiveFileMetadata f in db.GetLiveFilesMetadata()) Assert.AreEqual(6, f.Level);
            Assert.AreEqual("op0,op1,op2", GetString(db, "m"));
            Assert.IsNull(GetString(db, S(Key(2))!));
            Assert.AreEqual("after-snapshot", GetString(db, S(Key(10))!));
            // The snapshot still sees the older version (the worker honored the snapshot list).
            Assert.AreEqual(S(Value(10, 2)), GetString(db, S(Key(10))!, snap));
            // The worker's output directory is empty now (files renamed into the DB).
            foreach (string child in fs.GetChildren(DbPath + "/job-1")) Assert.IsFalse(child.EndsWith(".sst", StringComparison.Ordinal), child);
        }
        using RocksDatabase re = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { MergeOperator = new StringAppendMergeOperator() });
        Assert.AreEqual("op0,op1,op2", GetString(re, "m"));
        Assert.AreEqual(S(Value(20, 2)), GetString(re, S(Key(20))!));
    }

    [TestMethod]
    public void AutomaticCompactionsRunRemotely()
    {
        MemoryFileSystem fs = NewFs();
        (RocksDatabase db, InProcessService svc) = Open(fs, autoCompactions: true);
        using (db)
        {
            FillFiles(db, 8, 30);
            db.WaitForCompact();
            Assert.IsTrue(svc.Completed >= 1);
            Assert.IsFalse(svc.Infos[0].IsManualCompaction);
            Assert.AreEqual(CompactionReason.LevelL0FilesNum, svc.Infos[0].CompactionReason);
            Assert.AreEqual("op0,op1,op2,op3,op4,op5,op6,op7", GetString(db, "m"));
            for (int i = 8; i < 30; i++) Assert.AreEqual(S(Value(i, 7)), GetString(db, S(Key(i))!));
        }
    }

    [TestMethod]
    public void UseLocalAndFailures()
    {
        MemoryFileSystem fs = NewFs();
        (RocksDatabase db, InProcessService svc) = Open(fs, autoCompactions: false);
        using (db)
        {
            FillFiles(db, 2, 20);
            // kUseLocal at Schedule: compacted locally.
            svc.ScheduleStatus = CompactionServiceJobStatus.UseLocal;
            db.CompactRange();
            Assert.AreEqual(0, svc.Completed);
            Assert.AreEqual(S(Value(5, 1)), GetString(db, S(Key(5))!));

            // A malformed result is declined (OnInstallation(kUseLocal)) and the compaction runs locally.
            svc.ScheduleStatus = CompactionServiceJobStatus.Success;
            svc.CorruptResult = true;
            FillFiles(db, 2, 20);
            db.CompactRange();
            Assert.AreEqual(CompactionServiceJobStatus.UseLocal, svc.Installed[^1].Item2);
            Assert.AreEqual(S(Value(5, 1)), GetString(db, S(Key(5))!));

            // kFailure fails the compaction.
            svc.CorruptResult = false;
            svc.ForcedWaitStatus = CompactionServiceJobStatus.Failure;
            FillFiles(db, 2, 20);
            RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => db.CompactRange());
            Assert.AreEqual(StatusCode.Incomplete, e.Code);
            // Like upstream (kCompaction errors are fatal with paranoid_checks), the failure stops background work.
            Assert.IsNotNull(db.BackgroundError);
            Assert.AreEqual(S(Value(5, 1)), GetString(db, S(Key(5))!));
        }
        (db, svc) = Open(fs, autoCompactions: false);
        using (db)
        {
            svc.ForcedWaitStatus = CompactionServiceJobStatus.Aborted;
            RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => db.CompactRange());
            Assert.AreEqual(StatusCode.Aborted, e.Code, e.Message);
            Assert.AreEqual(S(Value(5, 1)), GetString(db, S(Key(5))!));
        }
    }

    [TestMethod]
    public void WorkerReportsCancellationAndMissingFiles()
    {
        MemoryFileSystem fs = NewFs();
        byte[]? captured = null;
        var capture = new CaptureService(b => captured = b);
        DbOptions o = NewOptions(fs);
        o.CompactionService = capture;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o, new ColumnFamilyOptions { DisableAutoCompactions = true }))
        {
            FillFilesPlain(db);
            db.CompactRange(); // UseLocal after capturing the input
        }
        Assert.IsNotNull(captured);
        var ov = new CompactionServiceOptionsOverride { FileSystem = fs };
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() =>
            RocksDatabase.OpenAndCompact(new OpenAndCompactOptions { Canceled = cts.Token }, DbPath, "jobx", captured, ov));
        Assert.AreEqual(StatusCode.Incomplete, e.Code);
        Assert.AreEqual(StatusSubCode.ManualCompactionPaused, e.SubCode);
        // The input files were compacted away locally: the worker fails with InvalidArgument and a serialized result.
        OpenAndCompactException oe = Assert.ThrowsExactly<OpenAndCompactException>(() => RocksDatabase.OpenAndCompact(DbPath, "joby", captured, ov));
        Assert.AreEqual(StatusCode.InvalidArgument, oe.Code);
        Assert.AreEqual(StatusCode.InvalidArgument, CompactionServiceFormat.ReadResult(oe.Result).StatusCode);
    }

    private static void FillFilesPlain(RocksDatabase db)
    {
        for (int f = 0; f < 2; f++)
        {
            for (int i = 0; i < 10; i++) db.Put(Key(i), Value(i, f));
            db.Flush();
        }
    }

    private sealed class CaptureService(Action<byte[]> onInput) : CompactionService
    {
        public override string Name => "Capture";

        public override ValueTask<CompactionServiceScheduleResponse> ScheduleAsync(CompactionServiceJobInfo info, ReadOnlyMemory<byte> input,
            CancellationToken cancellationToken)
        {
            onInput(input.ToArray());
            return ValueTask.FromResult(new CompactionServiceScheduleResponse("", CompactionServiceJobStatus.UseLocal));
        }
    }
}
