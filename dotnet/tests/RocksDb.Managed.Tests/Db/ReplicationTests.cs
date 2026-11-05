using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Wal;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>
/// GetUpdatesSince / WAL tailing / archival / file deletions / FragmentBufferedLogReader (ports the spirit of
/// db/db_log_iter_test.cc and db/wal_manager_test.cc).
/// </summary>
[TestClass]
public sealed class ReplicationTests
{
    /// <summary>Collects every batch from <paramref name="start"/>, asserting exact sequence continuity.</summary>
    private static List<(ulong Seq, int Count, byte[] Data)> ReadAll(RocksDatabase db, ulong start)
    {
        var result = new List<(ulong, int, byte[])>();
        using TransactionLogIterator it = db.GetUpdatesSince(start);
        ulong expected = 0;
        for (; it.Valid; it.Next())
        {
            WalBatch b = it.Current;
            if (expected != 0) Assert.AreEqual(expected, b.Sequence, "sequence continuity");
            else Assert.IsTrue(b.Sequence <= start && b.LastSequence >= start, $"first batch {b.Sequence}..{b.LastSequence} must contain {start}");
            expected = b.LastSequence + 1;
            result.Add((b.Sequence, b.Count, b.Span.ToArray()));
        }
        Assert.IsNull(it.Error, it.Error?.Message);
        return result;
    }

    private static void WriteBatches(RocksDatabase db, int from, int to, int perBatch)
    {
        for (int i = from; i < to; i++)
        {
            using WriteBatch b = db.CreateWriteBatch();
            for (int j = 0; j < perBatch; j++) b.Put(Key(i * 10 + j), Value(i, j));
            db.Write(b);
        }
    }

    [TestMethod]
    public void GetUpdatesSinceAcrossFlushesWithArchival()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.WalTtlSeconds = 3600;
        using RocksDatabase db = RocksDatabase.Open(DbPath, o);
        WriteBatches(db, 0, 20, 3);
        db.Flush();
        WriteBatches(db, 20, 40, 2);
        db.Flush();
        db.Delete(B("x"));
        WriteBatches(db, 40, 50, 1);
        Assert.IsTrue(fs.GetChildren(DbPath + "/archive").Any(), "flushed WALs are archived");
        IReadOnlyList<WalFileInfo> wals = db.GetSortedWalFiles();
        Assert.IsTrue(wals.Count >= 3);
        Assert.IsTrue(wals.Any(w => w.Type == WalFileType.ArchivedLogFile));
        for (int i = 1; i < wals.Count; i++) Assert.IsTrue(wals[i].StartSequence > wals[i - 1].StartSequence);

        List<(ulong Seq, int Count, byte[] Data)> all = ReadAll(db, 1);
        Assert.AreEqual(1UL, all[0].Seq);
        Assert.AreEqual(db.GetLatestSequenceNumber(), all[^1].Seq + (ulong)all[^1].Count - 1);
        Assert.AreEqual(20 + 20 + 1 + 10, all.Count);

        // Start in the middle of a batch: the iterator starts with the batch containing it.
        ulong mid = all[25].Seq + 1;
        List<(ulong Seq, int Count, byte[] Data)> tail = ReadAll(db, mid);
        Assert.AreEqual(all[25].Seq, tail[0].Seq);
        CollectionAssert.AreEqual(all[25].Data, tail[0].Data);
        Assert.AreEqual(all.Count - 25, tail.Count);

        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => db.GetUpdatesSince(db.GetLatestSequenceNumber() + 1));
        Assert.AreEqual(StatusCode.NotFound, e.Code);
    }

    [TestMethod]
    public void DisableFileDeletionsKeepsWals()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        Assert.AreEqual(1, db.DisableFileDeletions());
        Assert.AreEqual(2, db.DisableFileDeletions());
        WriteBatches(db, 0, 10, 2);
        db.Flush();
        WriteBatches(db, 10, 20, 2);
        db.Flush();
        WriteBatches(db, 20, 25, 2);
        Assert.AreEqual(25, ReadAll(db, 1).Count);
        Assert.AreEqual(1, db.EnableFileDeletions());
        Assert.IsFalse(db.IsFileDeletionsEnabled);
        Assert.AreEqual(0, db.EnableFileDeletions());
        Assert.IsTrue(db.IsFileDeletionsEnabled);
        // The flushed WALs are gone now: the iterator starts at the oldest available batch.
        using TransactionLogIterator it = db.GetUpdatesSince(1);
        Assert.IsTrue(it.Valid);
        Assert.AreEqual(41UL, it.Current.Sequence);
    }

    [TestMethod]
    public void GetLiveFilesAndCurrentWal()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        WriteBatches(db, 0, 5, 2);
        List<string> files = db.GetLiveFiles(flushMemtable: true, out ulong manifestSize);
        Assert.IsTrue(files.Contains("/CURRENT"));
        Assert.AreEqual(1, files.Count(f => f.EndsWith(".sst", StringComparison.Ordinal)));
        string manifest = files.Single(f => f.StartsWith("/MANIFEST-", StringComparison.Ordinal));
        Assert.AreEqual((ulong)fs.GetFileSize(DbPath + manifest), manifestSize);
        Assert.IsTrue(files.Any(f => f.StartsWith("/OPTIONS-", StringComparison.Ordinal)));
        CollectionAssert.AreEqual(files.Where(f => f.EndsWith(".sst", StringComparison.Ordinal)).ToList(), db.GetLiveFileNames());
        WalFileInfo cur = db.GetCurrentWalFile();
        Assert.AreEqual(WalFileType.AliveLogFile, cur.Type);
        Assert.IsTrue(fs.FileExists(DbPath + cur.PathName));
        db.Put(B("a"), B("b"));
        db.SyncWal();
        db.FlushWal(sync: false);
        Assert.IsTrue(db.GetCurrentWalFile().SizeBytes > 0);
    }

    [TestMethod]
    public async Task TailingWhileWritingConcurrently()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.WalTtlSeconds = 3600;
        await using RocksDatabase db = await RocksDatabase.OpenAsync(DbPath, o);
        const int batches = 600;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        Task writer = Task.Run(async () =>
        {
            for (int i = 0; i < batches; i++)
            {
                using WriteBatch b = db.CreateWriteBatch();
                b.Put(Key(i), Value(i, 0));
                if (i % 3 == 0) b.Put(Key(i + 100000), Value(i, 1));
                await db.WriteAsync(b);
                if (i % 150 == 149) await db.FlushAsync(); // WAL rotation while tailing
            }
        });
        ulong expected = 1;
        int seen = 0;
        await foreach (WalBatch b in db.TailUpdatesAsync(1, cts.Token))
        {
            Assert.AreEqual(expected, b.Sequence);
            expected = b.LastSequence + 1;
            if (++seen == batches) break;
        }
        await writer;
        ulong nextSeq = expected;
        Assert.AreEqual(db.GetLatestSequenceNumber() + 1, nextSeq);
    }

    [TestMethod]
    public async Task WaitForUpdatesIsSignalledByWrites()
    {
        MemoryFileSystem fs = NewFs();
        await using RocksDatabase db = await RocksDatabase.OpenAsync(DbPath, NewOptions(fs));
        db.Put(B("a"), B("1"));
        using TransactionLogIterator it = db.GetUpdatesSince(1);
        Assert.IsTrue(it.Valid);
        it.Next();
        Assert.IsFalse(it.Valid);
        Assert.IsNull(it.Error);
        ValueTask<bool> wait = it.WaitForUpdatesAsync();
        Assert.IsFalse(wait.IsCompleted);
        await db.PutAsync(B("b"), B("2"));
        Assert.IsTrue(await wait.AsTask().WaitAsync(TimeSpan.FromSeconds(10)));
        it.Next();
        Assert.IsTrue(it.Valid);
        Assert.AreEqual(2UL, it.Current.Sequence);
        using WriteBatch copy = it.Current.ToWriteBatch();
        Assert.AreEqual(1, copy.Count);
    }

    [TestMethod]
    public void FragmentBufferedReaderReadsGrowingFile()
    {
        var fs = new MemoryFileSystem();
        fs.CreateDirectoryIfMissing("/w");
        var rnd = new Random(42);
        var records = new List<byte[]>();
        using (var log = new LogWriter(fs.CreateWritable("/w/full.log"), 7, recycleLogFiles: false))
        {
            for (int i = 0; i < 40; i++)
            {
                byte[] r = new byte[rnd.Next(1, i % 7 == 0 ? 70000 : 3000)];
                rnd.NextBytes(r);
                records.Add(r);
                log.AddRecord(r);
            }
        }
        byte[] full = fs.ReadAllBytes("/w/full.log");
        IWritableFile growing = fs.CreateWritable("/w/grow.log");
        using var reader = new FragmentBufferedLogReader(fs.OpenSequential("/w/grow.log"), null, checksum: true, 7);
        var got = new List<byte[]>();
        int pos = 0;
        while (pos < full.Length)
        {
            int n = Math.Min(full.Length - pos, rnd.Next(1, 9000));
            growing.Append(full.AsSpan(pos, n));
            growing.Flush();
            pos += n;
            while (reader.TryReadRecord(out ReadOnlySpan<byte> rec)) got.Add(rec.ToArray());
        }
        growing.Dispose();
        Assert.AreEqual(records.Count, got.Count);
        for (int i = 0; i < records.Count; i++) CollectionAssert.AreEqual(records[i], got[i], "record " + i);
    }

    /// <summary>Writes <paramref name="count"/> random records (sizes 1..<paramref name="maxSize"/>) as one recycled-format log.</summary>
    private static byte[] RecycledLogBytes(MemoryFileSystem fs, string path, ulong logNumber, Random rnd, int count, int maxSize,
        List<byte[]>? records)
    {
        using (var log = new LogWriter(fs.CreateWritable(path), logNumber, recycleLogFiles: true))
        {
            for (int i = 0; i < count; i++)
            {
                byte[] r = new byte[rnd.Next(1, maxSize)];
                rnd.NextBytes(r);
                records?.Add(r);
                log.AddRecord(r);
            }
        }
        return fs.ReadAllBytes(path);
    }

    /// <summary>
    /// Reads up to <paramref name="max"/> records, re-reading the file from the stale tail when the reader stops there
    /// (what TransactionLogIterator and the secondary catch-up do).
    /// </summary>
    private static void ReadRecycled(MemoryFileSystem fs, string path, FragmentBufferedLogReader reader, List<byte[]> got, int max)
    {
        bool repositioned = false;
        while (got.Count < max || max < 0)
        {
            if (reader.TryReadRecord(out ReadOnlySpan<byte> rec))
            {
                got.Add(rec.ToArray());
                repositioned = false;
                continue;
            }
            if (!reader.AtStaleTail || repositioned) return;
            reader.Reposition(fs.OpenSequential(path));
            repositioned = true;
        }
    }

    /// <summary>
    /// A tailing reader of a recycled log, whose writer overwrites the previous use in place while the reader holds
    /// buffered stale bytes. The reader must never consume bytes it has not validated: a stale header whose length runs
    /// past the block used to make it skip to the next block (upstream FragmentBufferedReader does the same), losing the
    /// records the writer then wrote over the skipped bytes; an old record header used to make it spin.
    /// </summary>
    [TestMethod]
    [Timeout(60_000)]
    public void FragmentBufferedReaderNeverSkipsStaleBytesOfARecycledLog()
    {
        for (int seed = 1; seed <= 100; seed++)
        {
            var fs = new MemoryFileSystem();
            fs.CreateDirectoryIfMissing("/w");
            var rnd = new Random(seed);
            // Same-size records (an old record header right at the live end) for some seeds, random sizes otherwise.
            int maxSize = seed % 4 == 0 ? 2 : 400;
            byte[] old = RecycledLogBytes(fs, "/w/old.log", 5, rnd, maxSize == 2 ? 3000 : 600, maxSize, null);
            var records = new List<byte[]>();
            byte[] fresh = RecycledLogBytes(fs, "/w/fresh.log", 6, rnd, maxSize == 2 ? 2500 : 500, maxSize, records);
            using (IWritableFile w = fs.CreateWritable("/w/r.log")) w.Append(old);
            IWritableFile overwrite = fs.CreateWritable("/w/r.log", truncate: false);
            using var reader = new FragmentBufferedLogReader(fs.OpenSequential("/w/r.log"), null, checksum: true, 6);
            var got = new List<byte[]>();
            for (int pos = 0; pos < fresh.Length;)
            {
                int n = Math.Min(fresh.Length - pos, rnd.Next(1, 3000));
                overwrite.Append(fresh.AsSpan(pos, n));
                overwrite.Flush();
                pos += n;
                // Read only a few records, so the reader keeps buffered bytes that the next append overwrites.
                ReadRecycled(fs, "/w/r.log", reader, got, got.Count + rnd.Next(0, 4));
            }
            overwrite.Dispose();
            ReadRecycled(fs, "/w/r.log", reader, got, -1);
            Assert.AreEqual(records.Count, got.Count, "seed " + seed);
            for (int i = 0; i < records.Count; i++) CollectionAssert.AreEqual(records[i], got[i], "seed " + seed + " record " + i);
        }
    }
}
