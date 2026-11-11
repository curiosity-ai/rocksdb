using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4 closing remainder: behaviours pinned against db/db_impl/db_impl_secondary.cc (remote compaction worker).
/// </summary>
[TestClass]
public sealed class Audit4ClosingRemainderTests
{
    /// <summary>A compaction service that fails at Schedule and keeps the job input.</summary>
    private sealed class CapturingService : CompactionService
    {
        public byte[]? Input;

        public override string Name => "Capturing";

        public override ValueTask<CompactionServiceScheduleResponse> ScheduleAsync(CompactionServiceJobInfo info, ReadOnlyMemory<byte> input,
            CancellationToken cancellationToken)
        {
            Input = input.ToArray();
            return ValueTask.FromResult(new CompactionServiceScheduleResponse("job", CompactionServiceJobStatus.Failure));
        }

        public override ValueTask<CompactionServiceWaitResult> WaitAsync(string scheduledJobId, CancellationToken cancellationToken) =>
            ValueTask.FromResult(new CompactionServiceWaitResult(CompactionServiceJobStatus.UseLocal, null));
    }

    /// <summary>The job input of a CompactRange over two L0 files; the files stay in place (Schedule failed).</summary>
    private static CompactionServiceInput CaptureInput(MemoryFileSystem fs)
    {
        var svc = new CapturingService();
        DbOptions o = NewOptions(fs);
        o.CompactionService = svc;
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, new ColumnFamilyOptions { DisableAutoCompactions = true });
        for (int f = 0; f < 2; f++)
        {
            for (int i = 0; i < 10; i++) db.Put(Key(i), Value(i, f));
            db.Flush();
        }
        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => db.CompactRange());
        Assert.AreEqual(StatusCode.Incomplete, e.Code);
        Assert.IsNotNull(svc.Input);
        return CompactionServiceFormat.ReadInput(svc.Input);
    }

    private static RocksDbException RunWorker(MemoryFileSystem fs, CompactionServiceInput ci, string job) =>
        Assert.ThrowsExactly<OpenAndCompactException>(() =>
            RocksDatabase.OpenAndCompact(DbPath, job, CompactionServiceFormat.Write(ci), new CompactionServiceOptionsOverride { FileSystem = fs }));

    // DBImplSecondary::CompactWithoutInstallation (db_impl_secondary.cc:2017-2023): placement_mismatch is
    // proximal_level != 0 && (proximal level differs || proximal_output_range_type differs ||
    // keep_in_last_level_through_seqno differs) -> Status::Incomplete. The worker's compaction here has no per-key
    // placement (kInvalidLevel, kNotSupported = 0, kMaxSequenceNumber), so an input that agrees on the level but not on
    // the range type or on the sequence number must be declined.
    [TestMethod]
    public void WorkerDeclinesADifferentProximalRangeTypeOrKeepThroughSeqno()
    {
        MemoryFileSystem fs = NewFs();
        CompactionServiceInput ci = CaptureInput(fs);
        Assert.AreEqual(-1, ci.ProximalLevel);

        ci.KeepInLastLevelThroughSeqno = 5;
        RocksDbException e = RunWorker(fs, ci, "seqno");
        Assert.AreEqual(StatusCode.Incomplete, e.Code);
        StringAssert.Contains(e.Message, "per-key placement");

        ci.KeepInLastLevelThroughSeqno = DbFormat.MaxSequenceNumber;
        ci.ProximalOutputRangeType = 2; // kNonLastRange
        e = RunWorker(fs, ci, "range");
        Assert.AreEqual(StatusCode.Incomplete, e.Code);
        StringAssert.Contains(e.Message, "per-key placement");

        // proximal_level 0: an older primary that sent no decisions; the check is skipped and the compaction runs.
        ci.ProximalLevel = 0;
        CompactionServiceResult r = CompactionServiceFormat.ReadResult(
            RocksDatabase.OpenAndCompact(DbPath, "old", CompactionServiceFormat.Write(ci), new CompactionServiceOptionsOverride { FileSystem = fs }));
        Assert.IsTrue(r.Ok);
        Assert.AreEqual(1, r.OutputFiles.Count);
    }
}
