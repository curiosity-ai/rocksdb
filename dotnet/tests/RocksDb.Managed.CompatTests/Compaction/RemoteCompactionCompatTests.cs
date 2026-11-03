using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;

namespace RocksDb.Managed.CompatTests.Compaction;

[TestClass]
public sealed class RemoteCompactionCompatTests
{
    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);
    private static string Key(int i) => "key" + i.ToString("D6");

    /// <summary>A native compaction service whose "remote" worker is native rocksdb_open_and_compact.</summary>
    private sealed class NativeWorkerService(string dbPath, string workRoot, RocksDbSharp.DbOptions workerOptions) : NativeRemoteCompaction.IService
    {
        public readonly List<byte[]> Inputs = new();
        public readonly List<byte[]> Results = new();
        private int _next;

        public (int Status, string JobId) Schedule(byte[] input, int outputLevel)
        {
            Inputs.Add(input);
            return (0, "job" + Interlocked.Increment(ref _next));
        }

        public (int Status, byte[]? Result) Wait(string jobId)
        {
            // The output directory is relative to the DB directory (RemoteCompactionJobDir).
            byte[] r = NativeRemoteCompaction.OpenAndCompact(dbPath, jobId, Inputs[^1], workerOptions);
            Results.Add(r);
            return (0, r);
        }

        public void OnInstallation(string jobId, int status)
        {
        }
    }

    [TestMethod]
    public void CaptureNativeFormat()
    {
        using var dir = new TempDir("rcnative");
        string db = Path.Combine(dir.Path, "db");
        string work = Path.Combine(dir.Path, "work");
        Directory.CreateDirectory(work);
        var workerOptions = new RocksDbSharp.DbOptions();
        var svc = new NativeWorkerService(db, work, workerOptions);
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetDisableAutoCompactions(1);
        NativeRemoteCompaction.SetCompactionService(o, svc);
        using (var ndb = RocksDbSharp.RocksDb.Open(o, db))
        {
            for (int f = 0; f < 3; f++)
            {
                for (int i = 0; i < 100; i++) ndb.Put(U(Key(i)), U("v" + f + "-" + i));
                ndb.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            }
            ndb.CompactRange((byte[]?)null, null);
            Assert.AreEqual("v2-5", ndb.Get(Key(5)));
        }
        Assert.IsTrue(svc.Inputs.Count > 0);
        string outDir = Environment.GetEnvironmentVariable("RC_CAPTURE_DIR") ?? dir.Path;
        File.WriteAllBytes(Path.Combine(outDir, "native-input.bin"), svc.Inputs[0]);
        File.WriteAllBytes(Path.Combine(outDir, "native-result.bin"), svc.Results[0]);
    }
}
