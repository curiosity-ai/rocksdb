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

    // ---------------------------------------------------------------------------------------------------------
    // Follow-up: the start sequence check and data the tailing reader cannot get past.

    private readonly record struct Rec(long Offset, int HeaderSize, int Length);

    /// <summary>The records of this incarnation of a WAL (legacy, or recyclable with this log number), in file order.</summary>
    private static List<Rec> LiveRecords(string walPath, ulong logNumber)
    {
        byte[] log;
        using (var f = new FileStream(walPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete))
        {
            log = new byte[f.Length];
            f.ReadExactly(log);
        }
        var list = new List<Rec>();
        bool? recycled = null;
        long off = 0;
        while (off + 7 <= log.Length)
        {
            int inBlock = (int)(off % 32768);
            int type = log[off + 6];
            int length = log[off + 4] | (log[off + 5] << 8);
            int header = type is >= 5 and <= 8 or 11 or 131 ? 11 : 7;
            if (32768 - inBlock < header)
            {
                off += 32768 - inBlock;
                continue;
            }
            recycled ??= header == 11;
            if ((type == 0 && length == 0) || (header == 11) != recycled) break;
            if (header == 11 && System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(log.AsSpan((int)off + 7)) != unchecked((uint)logNumber)) break;
            list.Add(new Rec(off, header, length));
            off += header + length;
        }
        return list;
    }

    private static string CurrentWal(RocksDatabase db, string path) => System.IO.Path.Combine(path, FileNames.LogFileName(db.CurrentWalNumber));

    private static void Patch(string walPath, long offset, Func<int, int> change)
    {
        using var f = new FileStream(walPath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete);
        f.Position = offset;
        int b = f.ReadByte();
        f.Position = offset;
        f.WriteByte((byte)change(b));
    }

    /// <summary>
    /// manual_wal_flush, right after a WAL switch: the new WAL holds no record on disk, so the WAL listing skips it
    /// (WalManager::GetSortedWalsOfType: "if (sequence == 0) { // empty file continue; }") and the older WAL was purged.
    /// A tail that starts at the buffered record used to get NotFound ("no WAL files") - a needless resync of a replica
    /// that is exactly caught up. It now waits (CurrentWalBaseSequence) and reads the record once FlushWAL writes it.
    /// Native's GetUpdatesSince returns an OK iterator here (Audit11FollowUpCompatTests); the purge check is managed.
    /// </summary>
    [TestMethod]
    public async Task FreshTailAtARecordStillInTheManualFlushBufferWaits()
    {
        using var dir = new TempDir("a11-buffered-start");
        string path = dir.Sub("db");
        DbOptions o = DbOpts(null);
        o.ManualWalFlush = true;
        await using RocksDatabase db = await RocksDatabase.OpenAsync(path, o);
        db.Put(U(K(0)), U(V(0, 0)));
        db.FlushWal(sync: false);
        await db.FlushAsync();
        await WaitUntilAsync(() => db.GetSortedWalFiles().Count == 0, "the flushed WAL to be purged");
        db.Put(U(K(1)), U(V(1, 0))); // sequence 2, buffered
        Assert.AreEqual(1UL, db.CurrentWalBaseSequence);

        using WalTailer tailer = new ReplicationSource(db).TailWal(2);
        (List<ulong> firsts, RocksDbException? error, _) = await TailForAsync(tailer, TimeSpan.FromMilliseconds(300));
        Assert.IsNull(error, error?.Message);
        Assert.AreEqual(0, firsts.Count);
        db.FlushWal(sync: false);
        (firsts, error, _) = await TailForAsync(tailer, TimeSpan.FromMilliseconds(300));
        Assert.IsNull(error, error?.Message);
        CollectionAssert.AreEqual(new ulong[] { 2 }, firsts);
    }

    /// <summary>A start sequence whose WAL was purged (written before the current WAL) is still NotFound on the first read.</summary>
    [TestMethod]
    public async Task PurgedStartIsNotFoundOnTheFirstRead()
    {
        using var dir = new TempDir("a11-purged");
        string path = dir.Sub("db");
        await using RocksDatabase db = await RocksDatabase.OpenAsync(path, DbOpts(null));
        for (int i = 0; i < 3; i++) db.Put(U(K(i)), U(V(i, 0)));
        await db.FlushAsync();
        await WaitUntilAsync(() => db.GetSortedWalFiles().Count == 0, "the flushed WAL to be purged");
        Assert.AreEqual(3UL, db.CurrentWalBaseSequence);
        using WalTailer tailer = new ReplicationSource(db).TailWal(2);
        var e = Assert.ThrowsExactly<RocksDbException>(() => tailer.TryReadChunk(out _));
        Assert.AreEqual(StatusCode.NotFound, e.Code);
        StringAssert.Contains(e.Message, "no longer available");
    }

    /// <summary>
    /// A damaged record in a recycled live WAL with records written after it: log::FragmentBufferedReader ends a recycled
    /// log at a bad checksum without a report (db/log_reader.cc, ReadRecord: "case kBadRecordChecksum: if (recycled_) {
    /// fragments_.clear(); return false; }"), so the records behind it are never read and the tail used to wait until the
    /// next WAL switch. The iterator now counts what the writer completed past the reader's end, and the tail throws
    /// NotFound after one confirming re-read.
    /// </summary>
    [TestMethod]
    public async Task DamagedRecordOfARecycledWalEndsTheTailWithNotFound()
    {
        using var dir = new TempDir("a11-recycled");
        string path = dir.Sub("db");
        DbOptions o = DbOpts(null);
        o.RecycleLogFileNum = 2;
        await using RocksDatabase db = await RocksDatabase.OpenAsync(path, o);
        for (int round = 0; round < 3; round++)
        {
            for (int i = 0; i < 20; i++) db.Put(U("old" + i), U(new string('o', 100)));
            await db.FlushAsync();
            await db.WaitForCompactAsync();
        }
        ulong first = db.GetLatestSequenceNumber() + 1;
        for (int i = 0; i < 6; i++) db.Put(U(K(i)), U(V(i, 0)));
        string wal = CurrentWal(db, path);
        List<Rec> live = LiveRecords(wal, db.CurrentWalNumber);
        Assert.AreEqual(11, live[^1].HeaderSize, "precondition: a recycled WAL");
        Rec damaged = live[^3]; // K(3)
        Patch(wal, damaged.Offset + damaged.HeaderSize + 12 + 3, static b => b ^ 0x40);

        using WalTailer tailer = new ReplicationSource(db).TailWal(first);
        (List<ulong> firsts, RocksDbException? error, int polls) = await TailForAsync(tailer, TimeSpan.FromSeconds(5));
        CollectionAssert.AreEqual(new[] { first }, firsts, "K(0) .. K(2) in one chunk");
        Assert.IsNotNull(error, "the tail stalled for 5 s (" + polls + " polls) at the damaged recycled record");
        Assert.AreEqual(StatusCode.NotFound, error.Code);
        StringAssert.Contains(error.Message, "sequence " + (first + 3) + " is damaged");
        StringAssert.Contains(error.Message, "recycled");
    }

    /// <summary>
    /// The legitimate end of a recycled WAL (records of its previous use after the live end, log::Reader kOldRecord "Treat
    /// a record from a previous instance of the log as EOF") with the tail caught up: no error, and new writes arrive.
    /// </summary>
    [TestMethod]
    public async Task RecycledWalEndDoesNotEndTheTail()
    {
        using var dir = new TempDir("a11-recycled-end");
        string path = dir.Sub("db");
        DbOptions o = DbOpts(null);
        o.RecycleLogFileNum = 2;
        await using RocksDatabase db = await RocksDatabase.OpenAsync(path, o);
        for (int round = 0; round < 3; round++)
        {
            for (int i = 0; i < 20; i++) db.Put(U("old" + i), U(new string('o', 100)));
            await db.FlushAsync();
            await db.WaitForCompactAsync();
        }
        ulong first = db.GetLatestSequenceNumber() + 1;
        db.Put(U(K(0)), U(V(0, 0)));
        using WalTailer tailer = new ReplicationSource(db).TailWal(first);
        (List<ulong> firsts, RocksDbException? error, _) = await TailForAsync(tailer, TimeSpan.FromMilliseconds(300));
        Assert.IsNull(error, error?.Message);
        CollectionAssert.AreEqual(new[] { first }, firsts);
        db.Put(U(K(1)), U(V(1, 0)));
        (firsts, error, _) = await TailForAsync(tailer, TimeSpan.FromMilliseconds(300));
        Assert.IsNull(error, error?.Message);
        CollectionAssert.AreEqual(new[] { first + 1 }, firsts);
    }

    /// <summary>
    /// A record length that reaches past the end of the written data (log::FragmentBufferedReader::TryReadFragment waits
    /// for more data: "while (header_size + length &gt; buffer_.size()) ... return false"): the tail used to wait until the
    /// next write. Past what the WAL writer completed it is damage: NotFound.
    /// </summary>
    [TestMethod]
    public async Task RecordLengthPastTheWrittenEndEndsTheTailWithNotFound()
    {
        using var dir = new TempDir("a11-length");
        string path = dir.Sub("db");
        await using RocksDatabase db = await RocksDatabase.OpenAsync(path, DbOpts(null));
        for (int i = 0; i < 4; i++) db.Put(U(K(i)), U(V(i, 0)));
        string wal = CurrentWal(db, path);
        Rec last = LiveRecords(wal, db.CurrentWalNumber)[^1];
        int grown = last.Length + 200;
        Patch(wal, last.Offset + 4, _ => grown & 0xff);
        Patch(wal, last.Offset + 5, _ => grown >> 8);

        using WalTailer tailer = new ReplicationSource(db).TailWal(1);
        (List<ulong> firsts, RocksDbException? error, int polls) = await TailForAsync(tailer, TimeSpan.FromSeconds(5));
        CollectionAssert.AreEqual(new ulong[] { 1 }, firsts);
        Assert.IsNotNull(error, "the tail stalled for 5 s (" + polls + " polls) at the record length past the end");
        Assert.AreEqual(StatusCode.NotFound, error.Code);
        StringAssert.Contains(error.Message, "sequence 4 is damaged");
    }

    /// <summary>
    /// A writer in the middle of a record on the active WAL (manual_wal_flush, a batch larger than the writer's buffer:
    /// part of it is on disk until FlushWAL) is not damage: the tail waits, then reads it.
    /// </summary>
    [TestMethod]
    public async Task RecordPartlyWrittenByTheActiveWriterDoesNotEndTheTail()
    {
        using var dir = new TempDir("a11-midrecord");
        string path = dir.Sub("db");
        DbOptions o = DbOpts(null);
        o.ManualWalFlush = true;
        o.WritableFileMaxBufferSize = 64 << 10;
        await using RocksDatabase db = await RocksDatabase.OpenAsync(path, o);
        db.Put(U(K(0)), U(V(0, 0)));
        db.FlushWal(sync: false);
        using WalTailer tailer = new ReplicationSource(db).TailWal(1);
        Assert.IsTrue(tailer.TryReadChunk(out _));
        string wal = CurrentWal(db, path);
        long before = new FileInfo(wal).Length;
        db.Put(U(K(1)), new byte[300 << 10]);
        long partial = new FileInfo(wal).Length;
        Assert.IsTrue(partial > before && partial < before + (300 << 10), "precondition: part of the record is on disk");
        (List<ulong> firsts, RocksDbException? error, _) = await TailForAsync(tailer, TimeSpan.FromMilliseconds(300));
        Assert.IsNull(error, error?.Message);
        Assert.AreEqual(0, firsts.Count);
        db.FlushWal(sync: false);
        (firsts, error, _) = await TailForAsync(tailer, TimeSpan.FromMilliseconds(300));
        Assert.IsNull(error, error?.Message);
        CollectionAssert.AreEqual(new ulong[] { 2 }, firsts);
    }
}
