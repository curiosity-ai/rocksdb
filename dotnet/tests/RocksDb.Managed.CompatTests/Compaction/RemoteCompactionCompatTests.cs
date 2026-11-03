using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Db;

namespace RocksDb.Managed.CompatTests.Compaction;

/// <summary>
/// Remote compaction across engines through native 11.12's C API (rocksdb_compactionservice_create,
/// rocksdb_open_and_compact): the managed reader parses native CompactionServiceInput / Result bytes, a native primary
/// installs results of the managed OpenAndCompact worker, and a managed primary installs results of the native worker.
/// </summary>
[TestClass]
public sealed class RemoteCompactionCompatTests
{
    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);
    private static string Key(int i) => "key" + i.ToString("D6");

    /// <summary>A native primary's compaction service whose worker is <paramref name="worker"/> (native or managed OpenAndCompact).</summary>
    private sealed class NativeService(Func<string, byte[], (int Status, byte[]? Result)> worker) : NativeRemoteCompaction.IService
    {
        public readonly List<byte[]> Inputs = new();
        public readonly List<byte[]> Results = new();
        public readonly List<int> Installed = new();
        private readonly Dictionary<string, byte[]> _jobs = new();
        private int _next;

        public (int Status, string JobId) Schedule(byte[] input, int outputLevel)
        {
            lock (_jobs)
            {
                string id = "job" + (++_next);
                _jobs[id] = input;
                Inputs.Add(input);
                return (0, id);
            }
        }

        public (int Status, byte[]? Result) Wait(string jobId)
        {
            byte[] input;
            lock (_jobs) input = _jobs[jobId];
            (int status, byte[]? r) = worker(jobId, input);
            lock (_jobs) if (r is not null) Results.Add(r);
            return (status, r);
        }

        public void OnInstallation(string jobId, int status)
        {
            lock (_jobs) Installed.Add(status);
        }
    }

    private static void FillNative(RocksDbSharp.RocksDb db, int files, int keys)
    {
        for (int f = 0; f < files; f++)
        {
            for (int i = 0; i < keys; i++) db.Put(U(Key(i)), U("v" + f + "-" + i));
            db.Remove(U(Key(f)));
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
        }
    }

    private static void VerifyNative(RocksDbSharp.RocksDb db, int files, int keys)
    {
        for (int i = 0; i < keys; i++)
        {
            string? expected = i == files - 1 ? null : "v" + (files - 1) + "-" + i;
            Assert.AreEqual(expected, db.Get(Key(i)), "key " + i);
        }
    }

    [TestMethod]
    public void ManagedReaderParsesNativeJobs()
    {
        using var dir = new TempDir("rcnative");
        string db = Path.Combine(dir.Path, "db");
        var workerOptions = new RocksDbSharp.DbOptions();
        var svc = new NativeService((job, input) => (0, NativeRemoteCompaction.OpenAndCompact(db, job, input, workerOptions)));
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetDisableAutoCompactions(1);
        NativeRemoteCompaction.SetCompactionService(o, svc);
        using (var ndb = RocksDbSharp.RocksDb.Open(o, db))
        {
            FillNative(ndb, 3, 100);
            using RocksDbSharp.Snapshot snap = ndb.CreateSnapshot();
            ndb.CompactRange((byte[]?)null, null);
            VerifyNative(ndb, 3, 100);
            Assert.AreEqual(1, svc.Inputs.Count);
            CompactionServiceInput input = CompactionServiceFormat.ReadInput(svc.Inputs[0]);
            Assert.AreEqual("default", input.CfName);
            Assert.AreEqual(3, input.InputFiles.Count);
            Assert.AreEqual(1, input.Snapshots.Count);
            Assert.AreEqual(6, input.OutputLevel);
            Assert.AreNotEqual(0UL, input.OptionsFileNumber);
            CompactionServiceResult result = CompactionServiceFormat.ReadResult(svc.Results[0]);
            Assert.IsTrue(result.Ok);
            Assert.AreEqual(1, result.OutputFiles.Count);
            CompactionServiceOutputFile f = result.OutputFiles[0];
            Assert.AreEqual(99UL, f.TableProperties.NumEntries);
            Assert.AreNotEqual(0UL, f.FileSize);
            Assert.AreNotEqual(0UL, f.TableProperties.TailStartOffset);
            Assert.IsFalse(f.UniqueId.IsNull);
            Assert.IsTrue(result.OutputPath.EndsWith("/job1", StringComparison.Ordinal), result.OutputPath);
            Assert.AreEqual(1UL, result.OutputLevelStats.Count);
            // The managed writer's re-encoding is read back identically.
            CompactionServiceInput again = CompactionServiceFormat.ReadInput(CompactionServiceFormat.Write(input));
            CollectionAssert.AreEqual(input.InputFiles, again.InputFiles);
            CompactionServiceResult againResult = CompactionServiceFormat.ReadResult(CompactionServiceFormat.Write(result));
            Assert.AreEqual(f.TableProperties.DbSessionId, againResult.OutputFiles[0].TableProperties.DbSessionId);
        }
    }

    [TestMethod]
    public void NativePrimaryInstallsManagedWorkerResults()
    {
        using var dir = new TempDir("rcmanagedworker");
        string db = Path.Combine(dir.Path, "db");
        var svc = new NativeService((job, input) =>
        {
            try
            {
                return (0, RocksDatabase.OpenAndCompact(db, job, input, new CompactionServiceOptionsOverride()));
            }
            catch (OpenAndCompactException e)
            {
                return (1, e.Result);
            }
        });
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetDisableAutoCompactions(1);
        NativeRemoteCompaction.SetCompactionService(o, svc);
        using (var ndb = RocksDbSharp.RocksDb.Open(o, db))
        {
            FillNative(ndb, 4, 200);
            ndb.CompactRange((byte[]?)null, null);
            Assert.AreEqual(1, svc.Results.Count);
            Assert.AreEqual(0, svc.Installed[0], "OnInstallation(kSuccess)");
            VerifyNative(ndb, 4, 200);
            string? files = ndb.GetProperty("rocksdb.num-files-at-level6");
            Assert.AreEqual("1", files, ndb.GetProperty("rocksdb.levelstats"));
            // Automatic compactions too.
            ndb.SetOptions(new Dictionary<string, string> { ["disable_auto_compactions"] = "false", ["level0_file_num_compaction_trigger"] = "2" });
            FillNative(ndb, 3, 200);
            ndb.CompactRange((byte[]?)null, null);
            VerifyNative(ndb, 3, 200);
        }
        // The managed port opens the natively-installed result.
        using RocksDatabase m = RocksDatabase.Open(db, new DbOptions());
        Assert.AreEqual("v2-7", Encoding.UTF8.GetString(m.Get(U(Key(7)))!));
        Assert.IsNull(m.Get(U(Key(2))));
    }

    /// <summary>A managed compaction service whose worker is native rocksdb_open_and_compact.</summary>
    private sealed class ManagedServiceNativeWorker(string db) : CompactionService
    {
        private readonly Dictionary<string, byte[]> _jobs = new();
        private int _next;
        public int Completed;
        public readonly List<CompactionServiceJobStatus> Installed = new();

        public override string Name => "NativeWorker";

        public override ValueTask<CompactionServiceScheduleResponse> ScheduleAsync(CompactionServiceJobInfo info, ReadOnlyMemory<byte> input,
            CancellationToken cancellationToken)
        {
            lock (_jobs)
            {
                string id = "mjob" + (++_next);
                _jobs[id] = input.ToArray();
                return ValueTask.FromResult(new CompactionServiceScheduleResponse(id, CompactionServiceJobStatus.Success));
            }
        }

        public override ValueTask<CompactionServiceWaitResult> WaitAsync(string scheduledJobId, CancellationToken cancellationToken)
        {
            byte[] input;
            lock (_jobs) input = _jobs[scheduledJobId];
            byte[] r = NativeRemoteCompaction.OpenAndCompact(db, scheduledJobId, input, new RocksDbSharp.DbOptions());
            Interlocked.Increment(ref Completed);
            return ValueTask.FromResult(new CompactionServiceWaitResult(CompactionServiceJobStatus.Success, r));
        }

        public override void OnInstallation(string scheduledJobId, CompactionServiceJobStatus status)
        {
            lock (_jobs) Installed.Add(status);
        }
    }

    [TestMethod]
    public void ManagedPrimaryInstallsNativeWorkerResults()
    {
        using var dir = new TempDir("rcnativeworker");
        string db = Path.Combine(dir.Path, "db");
        var svc = new ManagedServiceNativeWorker(db);
        var o = new DbOptions { CreateIfMissing = true, CompactionService = svc };
        using (RocksDatabase m = RocksDatabase.Open(db, o, new ColumnFamilyOptions { DisableAutoCompactions = true }))
        {
            for (int f = 0; f < 3; f++)
            {
                for (int i = 0; i < 150; i++) m.Put(U(Key(i)), U("v" + f + "-" + i));
                m.Delete(U(Key(f)));
                m.Flush();
            }
            using Snapshot snap = m.GetSnapshot();
            m.Put(U(Key(9)), U("new"));
            m.Flush();
            m.CompactRange();
            Assert.AreEqual(1, svc.Completed);
            Assert.AreEqual(CompactionServiceJobStatus.Success, svc.Installed[0]);
            foreach (LiveFileMetadata f in m.GetLiveFilesMetadata()) Assert.AreEqual(6, f.Level);
            Assert.AreEqual("new", Encoding.UTF8.GetString(m.Get(U(Key(9)))!));
            Assert.AreEqual("v2-9", Encoding.UTF8.GetString(m.Get(new ReadOptions { Snapshot = snap }, U(Key(9)))!));
            Assert.IsNull(m.Get(U(Key(2))));
            Assert.AreEqual("v2-100", Encoding.UTF8.GetString(m.Get(U(Key(100)))!));
        }
        // Native reopens the DB with the installed (natively written) files.
        using var ndb = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions(), db);
        Assert.AreEqual("new", ndb.Get(Key(9)));
        Assert.AreEqual("v2-100", ndb.Get(Key(100)));
    }
}
