using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4 residuals (second pass): who verifies the outputs of a remote compaction
/// (CompactionServiceCompactionJob::Run vs CompactionJob::VerifyOutputFiles on the primary).
/// </summary>
[TestClass]
public sealed class Audit4Residuals2Tests
{
    /// <summary>A compaction service whose worker runs OpenAndCompact in-process; the result can be altered before it is returned.</summary>
    private sealed class TamperingService(MemoryFileSystem fs) : CompactionService
    {
        private readonly Dictionary<string, byte[]> _jobs = new();
        private int _next;
        public Action<CompactionServiceResult>? Tamper;
        public CompactionServiceResult? LastResult;
        public readonly List<CompactionServiceJobStatus> Installed = new();

        public override string Name => "Tampering";

        public override ValueTask<CompactionServiceScheduleResponse> ScheduleAsync(CompactionServiceJobInfo info, ReadOnlyMemory<byte> input,
            CancellationToken cancellationToken)
        {
            lock (_jobs)
            {
                string id = "job-" + (++_next).ToString(System.Globalization.CultureInfo.InvariantCulture);
                _jobs[id] = input.ToArray();
                return ValueTask.FromResult(new CompactionServiceScheduleResponse(id, CompactionServiceJobStatus.Success));
            }
        }

        public override async ValueTask<CompactionServiceWaitResult> WaitAsync(string scheduledJobId, CancellationToken cancellationToken)
        {
            byte[] input;
            lock (_jobs) input = _jobs[scheduledJobId];
            byte[] bytes = await RocksDatabase.OpenAndCompactAsync(new OpenAndCompactOptions(), DbPath, scheduledJobId, input,
                new CompactionServiceOptionsOverride { FileSystem = fs }, cancellationToken);
            CompactionServiceResult result = CompactionServiceFormat.ReadResult(bytes);
            LastResult = result;
            if (Tamper is not null)
            {
                Tamper(result);
                bytes = CompactionServiceFormat.Write(result);
            }
            return new CompactionServiceWaitResult(CompactionServiceJobStatus.Success, bytes);
        }

        public override void OnInstallation(string scheduledJobId, CompactionServiceJobStatus status)
        {
            lock (Installed) Installed.Add(status);
        }
    }

    private static (RocksDatabase Db, TamperingService Service) OpenRemote(MemoryFileSystem fs, VerifyOutputFlags flags)
    {
        var svc = new TamperingService(fs);
        DbOptions o = NewOptions(fs);
        o.CompactionService = svc;
        var db = RocksDatabase.Open(DbPath, o, new ColumnFamilyOptions { DisableAutoCompactions = true, VerifyOutputFlags = flags });
        for (int f = 0; f < 3; f++)
        {
            for (int i = 0; i < 40; i++) db.Put(Key(i), Value(i, f));
            db.Flush();
        }
        return (db, svc);
    }

    /// <summary>
    /// CompactionServiceCompactionJob::Run (compaction_service_job.cc): the worker reports each output's
    /// "output_file.validator.GetHash()" as paranoid_hash. The hash is enabled by kVerifyIteration
    /// (CompactionJob::ProcessKeyValueCompaction), and the primary's VerifyOutputFiles compares it.
    /// </summary>
    [TestMethod]
    public void WorkerReportsTheOutputHashAndThePrimaryAcceptsIt()
    {
        MemoryFileSystem fs = NewFs();
        (RocksDatabase db, TamperingService svc) = OpenRemote(fs, VerifyOutputFlags.Iteration | VerifyOutputFlags.EnableForRemoteCompaction);
        using (db)
        {
            db.CompactRange();
            Assert.IsNotNull(svc.LastResult);
            Assert.AreEqual(1, svc.LastResult.OutputFiles.Count);
            Assert.AreNotEqual(0UL, svc.LastResult.OutputFiles[0].ParanoidHash, "paranoid_hash of the worker's output");
            Assert.AreEqual(Value(7, 2).Length, db.Get(Key(7))!.Length);
            Assert.AreEqual(1, db.GetLiveFilesMetadata().Count);
        }
    }

    /// <summary>
    /// CompactionJob::VerifyOutputFiles runs on the primary for the subcompaction a service ran (is_remote_compaction
    /// with kEnableForRemoteCompaction): the file is iterated and its hash compared with the validator built from the
    /// result's paranoid_hash (compaction_service_job.cc: "false, true, file.paranoid_hash"). A result whose hash does
    /// not match the file fails the compaction with Corruption and nothing is installed.
    /// </summary>
    [TestMethod]
    public void PrimaryVerifiesRemoteOutputAgainstTheWorkerHash()
    {
        MemoryFileSystem fs = NewFs();
        (RocksDatabase db, TamperingService svc) = OpenRemote(fs, VerifyOutputFlags.Iteration | VerifyOutputFlags.EnableForRemoteCompaction);
        using (db)
        {
            svc.Tamper = static r => r.OutputFiles[0].ParanoidHash ^= 1;
            RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.CompactRange());
            Assert.AreEqual(StatusCode.Corruption, ex.Code);
            Assert.AreEqual(3, db.GetLiveFilesMetadata().Count, "the inputs stay");
            // Upstream reports the installation (OnInstallation(kSuccess)) before the outputs are verified.
            CollectionAssert.AreEqual(new[] { CompactionServiceJobStatus.Success }, svc.Installed);
        }
    }

    /// <summary>
    /// Without kEnableForRemoteCompaction the hash is not compared (should_verify is false), like a local compaction
    /// without kEnableForLocalCompaction.
    /// </summary>
    [TestMethod]
    public void RemoteOutputHashIsIgnoredWithoutTheRemoteFlag()
    {
        MemoryFileSystem fs = NewFs();
        (RocksDatabase db, TamperingService svc) = OpenRemote(fs, VerifyOutputFlags.Iteration | VerifyOutputFlags.EnableForLocalCompaction);
        using (db)
        {
            svc.Tamper = static r => r.OutputFiles[0].ParanoidHash ^= 1;
            db.CompactRange();
            Assert.AreEqual(1, db.GetLiveFilesMetadata().Count);
        }
    }

    /// <summary>
    /// "Verify that the table is usable" (verify_table in CompactionJob::VerifyOutputFiles): every output, also one a
    /// service produced, is opened through the table cache before it is installed, whatever verify_output_flags says.
    /// A remote output that does not open fails the compaction.
    /// </summary>
    [TestMethod]
    public void PrimaryOpensRemoteOutputsWithoutVerifyFlags()
    {
        MemoryFileSystem fs = NewFs();
        (RocksDatabase db, TamperingService svc) = OpenRemote(fs, VerifyOutputFlags.None);
        using (db)
        {
            svc.Tamper = r =>
            {
                string path = r.OutputPath + "/" + r.OutputFiles[0].FileName;
                // Overwrite the footer (magic number included).
                fs.WriteAt(path, fs.GetFileSize(path) - 16, new byte[16]);
            };
            RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.CompactRange());
            Assert.AreEqual(StatusCode.Corruption, ex.Code);
            Assert.AreEqual(3, db.GetLiveFilesMetadata().Count, "the inputs stay");
            svc.Tamper = null;
        }
    }
}
