using RocksDb.Managed.Cluster;
using RocksDb.Managed.Replication.Hosting;

namespace RocksDb.Managed.Replication.Tests;

/// <summary>
/// Audit #11 follow-up A: the WAL tail when the database is ahead of what the WAL can deliver. The iterator now ends
/// such a run with TryAgain like native (WalIteratorImpl::NextImpl, db/wal_iterator_impl.cc); the tailer waits it out
/// for writes that are not in the WAL (disableWAL, ingestion) or not written yet (manual_wal_flush), and turns a damaged
/// record below the last sequence - which used to stall the replica silently until the next WAL switch - into NotFound,
/// which the hosts answer with a resync.
/// </summary>
[TestClass]
public sealed class Audit11WalTailerTests
{
    private static WriteOptions NoWal => new() { DisableWal = true };

    /// <summary>
    /// Appends one batch for <paramref name="key"/> to the live WAL and damages its record payload in place (checksum
    /// mismatch) while the database keeps running.
    /// </summary>
    private static void PutDamaged(RocksDatabase db, string dbPath, string key) =>
        WriteDamagedAsync(db, dbPath, () =>
        {
            db.Put(U(key), U("damaged"));
            return Task.CompletedTask;
        }).GetAwaiter().GetResult();

    /// <summary>Runs <paramref name="write"/> (one batch) and damages its live WAL record in place.</summary>
    private static async Task WriteDamagedAsync(RocksDatabase db, string dbPath, Func<Task> write)
    {
        string wal = dbPath + db.GetSortedWalFiles()[^1].PathName;
        long before = new FileInfo(wal).Length;
        await write();
        Assert.IsTrue(new FileInfo(wal).Length > before + 20, "the batch went to " + wal);
        using var f = new FileStream(wal, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete);
        f.Position = before + 7 + 12 + 3; // record header, batch header, then the record: a byte of the key
        int b = f.ReadByte();
        f.Position = before + 7 + 12 + 3;
        f.WriteByte((byte)(b ^ 0x40));
    }

    /// <summary>
    /// Reads chunks the way <see cref="WalStreamSender"/> does (TryReadChunk, else WaitForUpdatesAsync) for at most
    /// <paramref name="duration"/>: the sequence numbers received, and the error that ended the tail (or null).
    /// </summary>
    private static async Task<(List<ulong> Firsts, RocksDbException? Error, int Polls)> TailForAsync(WalTailer tailer, TimeSpan duration)
    {
        var firsts = new List<ulong>();
        int polls = 0;
        using var cts = new CancellationTokenSource(duration);
        try
        {
            while (!cts.IsCancellationRequested)
            {
                polls++;
                if (tailer.TryReadChunk(out WalChunk chunk))
                {
                    firsts.Add(chunk.FirstSequenceNumber);
                    continue;
                }
                if (!await tailer.WaitForUpdatesAsync(cts.Token)) break;
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (RocksDbException e)
        {
            return (firsts, e, polls);
        }
        return (firsts, null, polls);
    }

    /// <summary>
    /// A damaged live WAL record below the last sequence with nothing readable behind it: the tail delivers what is
    /// before it and then throws NotFound naming the sequence and the WAL, after one confirming re-read - not the
    /// endless "nothing new" (with a 1 ms back-off) it used to report.
    /// </summary>
    [TestMethod]
    public async Task DamagedTailRecordEndsTheTailWithNotFound()
    {
        using var dir = new TempDir("a11-damaged");
        string path = dir.Sub("db");
        await using RocksDatabase db = await RocksDatabase.OpenAsync(path, DbOpts(null));
        for (int i = 0; i < 3; i++) db.Put(U(K(i)), U(V(i, 0)));
        using WalTailer tailer = new ReplicationSource(db).TailWal(1);
        Assert.IsTrue(tailer.TryReadChunk(out WalChunk first));
        Assert.AreEqual(3, first.EntryCount);
        PutDamaged(db, path, K(3));
        Assert.IsTrue(tailer.HasUpdates);

        (List<ulong> firsts, RocksDbException? error, int polls) = await TailForAsync(tailer, TimeSpan.FromSeconds(5));
        Assert.AreEqual(0, firsts.Count);
        Assert.IsNotNull(error, "the tail stalled for 5 s (" + polls + " polls) instead of reporting the damaged record");
        Assert.AreEqual(StatusCode.NotFound, error.Code);
        StringAssert.Contains(error.Message, "sequence 4 is damaged in WAL #" + db.GetSortedWalFiles()[^1].LogNumber);
        StringAssert.Contains(error.Message, "checksum mismatch");
        Assert.IsTrue(polls <= 4, "confirmed after " + polls + " polls");
    }

    /// <summary>
    /// Writes with disableWAL and an ingested file at the end of the log, and a record manual_wal_flush keeps in the
    /// writer's buffer: published, not readable (TryAgain), nothing damaged. The tail keeps waiting (no error), and
    /// continues with the buffered record once FlushWAL writes it.
    /// </summary>
    [TestMethod]
    public async Task UnloggedAndBufferedWritesDoNotEndTheTail()
    {
        using var dir = new TempDir("a11-unlogged");
        string path = dir.Sub("db");
        DbOptions o = DbOpts(null);
        o.ManualWalFlush = true;
        await using RocksDatabase db = await RocksDatabase.OpenAsync(path, o);
        db.Put(U(K(0)), U(V(0, 0)));
        db.FlushWal(sync: false);
        using WalTailer tailer = new ReplicationSource(db).TailWal(1);
        Assert.IsTrue(tailer.TryReadChunk(out _));

        // manual_wal_flush: a logged record still in the writer's buffer.
        db.Put(U(K(1)), U(V(1, 0)));
        (List<ulong> firsts, RocksDbException? error, int polls) = await TailForAsync(tailer, TimeSpan.FromMilliseconds(300));
        Assert.IsNull(error, error?.Message);
        Assert.AreEqual(0, firsts.Count, "the record is still in the writer's buffer");
        Assert.IsTrue(polls > 10, "the tail polled " + polls + " times");
        db.FlushWal(sync: false);
        (firsts, error, _) = await TailForAsync(tailer, TimeSpan.FromMilliseconds(300));
        Assert.IsNull(error, error?.Message);
        CollectionAssert.AreEqual(new ulong[] { 2 }, firsts);

        // disableWAL at the tail, also for a fresh tail at that sequence (its iterator does not find its start).
        db.Put(NoWal, U(K(2)), U(V(2, 0)));
        (firsts, error, _) = await TailForAsync(tailer, TimeSpan.FromMilliseconds(300));
        Assert.IsNull(error, error?.Message);
        Assert.AreEqual(0, firsts.Count);
        using (WalTailer fresh = new ReplicationSource(db).TailWal(3))
        {
            (firsts, error, _) = await TailForAsync(fresh, TimeSpan.FromMilliseconds(300));
            Assert.IsNull(error, error?.Message);
            Assert.AreEqual(0, firsts.Count);
        }

        // An ingested file that overlaps the memtable takes a sequence number (and flushes the memtable first).
        string sst = dir.Sub("ext.sst");
        using (var w = new SstFileWriter(new ColumnFamilyOptions()))
        {
            w.Open(sst);
            w.Put(U(K(0)), U("ingested"));
            w.Finish();
        }
        db.IngestExternalFiles([sst], new IngestExternalFileOptions());
        Assert.AreEqual(4UL, db.GetLatestSequenceNumber(), "precondition: the ingestion took a sequence number");
        (firsts, error, _) = await TailForAsync(tailer, TimeSpan.FromMilliseconds(300));
        Assert.IsNull(error, error?.Message);
        Assert.AreEqual(0, firsts.Count);
    }

    /// <summary>
    /// Pair replication: the replica is offline while the primary writes a batch whose live WAL record gets damaged.
    /// When the replica attaches again, the primary's stream ends with a resync request (NotFound from the tailer) and
    /// the replica rebuilds from a snapshot, which holds the batch (the checkpoint flushes the memtable), instead of
    /// waiting forever below the primary's last sequence.
    /// </summary>
    [TestMethod]
    public async Task ReplicaResyncsPastADamagedPrimaryWalRecord()
    {
        await using var h = new PairHarness();
        PairNodeHost a = await h.StartAsync("A");
        for (int i = 0; i < 10; i++) a.Db.Put(U(K(i)), U(V(i, 0)));
        await h.StartAsync("B");
        await h.AssertConvergedAsync("A", "B");
        await h.StopAsync("B");
        a.Db.Put(U(K(10)), U(V(10, 0)));
        PutDamaged(a.Db, h.DbPath("A"), K(11));

        PairNodeHost b = await h.StartAsync("B");
        int resyncsBefore = b.Resyncs;
        await WaitUntilAsync(() => b.Resyncs > resyncsBefore, "the replica to resync past the damaged record\n" + h.LogText);
        await h.AssertConvergedAsync("A", "B");
        Assert.AreEqual("damaged", System.Text.Encoding.UTF8.GetString(b.Db.Get(U(K(11)))!));
    }

    /// <summary>
    /// Raft cluster: a follower that was down while the leader committed a batch (with the other follower) whose record
    /// in the leader's live WAL is then damaged. The returning follower's stream ends with a resync request and it is
    /// rebuilt from a snapshot instead of staying behind the commit index.
    /// </summary>
    [TestMethod]
    public async Task ClusterFollowerResyncsPastADamagedLeaderWalRecord()
    {
        await using var c = new ClusterHarness(3, null);
        ClusterNodeHost n1 = await c.StartAsync("n1");
        for (int i = 0; i < 10; i++) Assert.IsTrue(await n1.TryWriteAsLeaderAsync(U(K(i)), U(V(i, 0)), CancellationToken.None));
        await c.StartAsync("n2");
        await c.StartAsync("n3");
        await WaitUntilAsync(() => c.Statuses().TrueForAll(s => s.Mode == ClusterMode.Cluster), "every node in Cluster mode\n" + c.LogText);
        await c.AssertConvergedAsync(n1);
        await c.StopAsync("n3");
        Assert.IsTrue(await n1.TryWriteAsLeaderAsync(U(K(10)), U(V(10, 0)), CancellationToken.None));
        await WriteDamagedAsync(n1.Db, c.DbPath("n1"), async () =>
            Assert.IsTrue(await n1.TryWriteAsLeaderAsync(U(K(11)), U("damaged"), CancellationToken.None)));

        ClusterNodeHost n3 = await c.StartAsync("n3");
        int resyncsBefore = n3.Resyncs;
        await WaitUntilAsync(() => n3.Resyncs > resyncsBefore, "the follower to resync past the damaged record\n" + c.LogText);
        await c.AssertConvergedAsync(n1);
        Assert.AreEqual("damaged", System.Text.Encoding.UTF8.GetString(n3.Db.Get(U(K(11)))!));
    }

    /// <summary>
    /// Pair replication: writes with disableWAL and an ingested file at the end of the primary's log are not readable
    /// from its WAL (TryAgain); the replica must not be rebuilt for them while nothing shows a gap.
    /// </summary>
    [TestMethod]
    public async Task UnloggedPrimaryWritesDoNotTriggerAResync()
    {
        await using var h = new PairHarness();
        PairNodeHost a = await h.StartAsync("A");
        for (int i = 0; i < 10; i++) a.Db.Put(U(K(i)), U(V(i, 0)));
        PairNodeHost b = await h.StartAsync("B");
        await h.AssertConvergedAsync("A", "B");
        int resyncs = b.Resyncs;

        a.Db.Put(NoWal, U(K(20)), U("unlogged"));
        string sst = Path.Combine(h.DbPath("A") + "-ext.sst");
        using (var w = new SstFileWriter(new ColumnFamilyOptions()))
        {
            w.Open(sst);
            w.Put(U(K(20)), U("ingested"));
            w.Finish();
        }
        a.Db.IngestExternalFiles([sst], new IngestExternalFileOptions());
        await Task.Delay(500);
        Assert.AreEqual(resyncs, b.Resyncs, h.LogText);
        Assert.IsFalse(b.IsResyncing);
        Assert.AreEqual(0, a.WalStreamFaults);
    }
}
