using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>Checkpoint, SstFileWriter + ingestion, read-only and secondary instances on <see cref="MemoryFileSystem"/>.</summary>
[TestClass]
public sealed class CheckpointIngestReadOnlyTests
{
    // ---- checkpoint --------------------------------------------------------------------------------------

    [TestMethod]
    public void CheckpointContainsFlushedAndWalData()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            for (int i = 0; i < 100; i++) db.Put(Key(i), Value(i, 0));
            db.Flush();
            for (int i = 50; i < 150; i++) db.Put(Key(i), Value(i, 1));
            // Never flush: the WAL tail is copied.
            ulong seq = db.CreateCheckpoint("/cp1", ulong.MaxValue);
            Assert.AreEqual(db.GetLatestSequenceNumber(), seq);
            // Flush first (default).
            db.Put(B("late"), B("x"));
            db.CreateCheckpoint("/cp2");
            RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => db.CreateCheckpoint("/cp2"));
            Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
            Assert.IsFalse(fs.DirectoryExists("/cp1.tmp"));
        }
        foreach (string cp in new[] { "/cp1", "/cp2" })
        {
            using RocksDatabase c = RocksDatabase.Open(cp, NewOptions(fs));
            for (int i = 0; i < 150; i++) Assert.AreEqual(S(Value(i, i < 50 ? 0 : 1)), GetString(c, S(Key(i))!), cp + " key " + i);
            Assert.AreEqual(cp == "/cp2" ? "x" : null, GetString(c, "late"));
        }
    }

    [TestMethod]
    public async Task CheckpointIsConsistentUnderConcurrentWrites()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        var cfo = new ColumnFamilyOptions { WriteBufferSize = 256 * 1024, ArenaBlockSize = 32 * 1024 };
        ulong[] seqs = new ulong[3];
        int written;
        await using (RocksDatabase db = await RocksDatabase.OpenAsync(DbPath, o, cfo))
        {
            using var stop = new CancellationTokenSource();
            int n = 0;
            Task writer = Task.Run(async () =>
            {
                while (!stop.IsCancellationRequested)
                {
                    int i = n;
                    await db.PutAsync(Key(i), Value(i, 0));
                    Volatile.Write(ref n, i + 1);
                }
            });
            while (Volatile.Read(ref n) < 500) await Task.Delay(1);
            for (int c = 0; c < 3; c++)
            {
                seqs[c] = await db.CreateCheckpointAsync("/cp" + c, c == 1 ? ulong.MaxValue : 0);
                await Task.Delay(5);
            }
            stop.Cancel();
            await writer;
            written = n;
        }
        for (int c = 0; c < 3; c++)
        {
            using RocksDatabase cp = RocksDatabase.Open("/cp" + c, NewOptions(fs));
            ulong last = cp.GetLatestSequenceNumber();
            Assert.IsTrue(last >= seqs[c], $"checkpoint {c}: {last} < {seqs[c]}");
            // Keys were written in order, one per sequence number: the checkpoint holds exactly a prefix.
            for (int i = 0; i < written; i++)
            {
                bool expected = (ulong)i < last;
                Assert.AreEqual(expected, cp.ContainsKey(Key(i)), $"checkpoint {c} key {i} last {last}");
            }
        }
    }

    // ---- SstFileWriter + ingestion ------------------------------------------------------------------------

    private static string WriteSst(IFileSystem fs, string path, int from, int to, string tag)
    {
        using var w = new SstFileWriter(new ColumnFamilyOptions(), fs);
        w.Open(path);
        for (int i = from; i < to; i++) w.Put(Key(i), B(tag + i));
        ExternalSstFileInfo info = w.Finish();
        Assert.AreEqual((ulong)(to - from), info.NumEntries);
        CollectionAssert.AreEqual(Key(from), info.SmallestKey);
        CollectionAssert.AreEqual(Key(to - 1), info.LargestKey);
        Assert.AreEqual((ulong)fs.GetFileSize(path), info.FileSize);
        return path;
    }

    [TestMethod]
    public void SstFileWriterValidatesOrder()
    {
        MemoryFileSystem fs = NewFs();
        using var w = new SstFileWriter(new ColumnFamilyOptions(), fs);
        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => w.Put(B("a"), B("1")));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        w.Open("/db/x.sst");
        w.Put(B("b"), B("1"));
        e = Assert.ThrowsExactly<RocksDbException>(() => w.Put(B("a"), B("1")));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        e = Assert.ThrowsExactly<RocksDbException>(() => w.Put(B("b"), B("1")));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        e = Assert.ThrowsExactly<RocksDbException>(() => w.DeleteRange(B("z"), B("a")));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        w.Delete(B("c")).Merge(B("d"), B("m")).DeleteRange(B("x"), B("y"));
        ExternalSstFileInfo info = w.Finish();
        Assert.AreEqual(3UL, info.NumEntries);
        Assert.AreEqual(1UL, info.NumRangeDelEntries);

        using var empty = new SstFileWriter(new ColumnFamilyOptions(), fs);
        empty.Open("/db/empty.sst");
        e = Assert.ThrowsExactly<RocksDbException>(() => empty.Finish());
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
    }

    [TestMethod]
    public void IngestPicksLevelsAndSeqnos()
    {
        MemoryFileSystem fs = NewFs();
        fs.CreateDirectoryIfMissing("/ext");
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        // Empty DB: bottommost level, seqno 0.
        db.IngestExternalFiles([WriteSst(fs, "/ext/1.sst", 0, 100, "a")], new IngestExternalFileOptions());
        Assert.AreEqual(0UL, db.GetLatestSequenceNumber());
        LiveFileMetadata f1 = db.GetLiveFilesMetadata().Single();
        Assert.AreEqual(6, f1.Level);
        Assert.AreEqual(0UL, f1.LargestSeqno);
        Assert.AreEqual("a5", GetString(db, S(Key(5))!));
        Assert.IsTrue(fs.FileExists("/ext/1.sst"), "copied, not moved");

        // Non-overlapping file, moved: also bottommost.
        db.IngestExternalFiles([WriteSst(fs, "/ext/2.sst", 200, 300, "b")], new IngestExternalFileOptions { MoveFiles = true });
        Assert.IsFalse(fs.FileExists("/ext/2.sst"), "moved");
        Assert.AreEqual(2, db.GetLiveFilesMetadata().Count(m => m.Level == 6));

        // Overlapping with L6 data: goes above, gets a global seqno and wins.
        db.Put(B("zzz"), B("1")); // seq 1, unrelated memtable data
        using (Snapshot snap = db.GetSnapshot())
        {
            db.IngestExternalFiles([WriteSst(fs, "/ext/3.sst", 50, 60, "c")], new IngestExternalFileOptions());
            Assert.AreEqual(2UL, db.GetLatestSequenceNumber());
            LiveFileMetadata f3 = db.GetLiveFilesMetadata().Single(m => m.SmallestKey.AsSpan().SequenceEqual(Key(50)));
            Assert.IsTrue(f3.Level < 6);
            Assert.AreEqual(2UL, f3.LargestSeqno);
            Assert.AreEqual("c55", GetString(db, S(Key(55))!));
            Assert.AreEqual("a55", GetString(db, S(Key(55))!, snap), "snapshot must not see the ingested file");
        }

        // Memtable overlap: flushed first (or rejected without allow_blocking_flush).
        db.Put(Key(70), B("mem"));
        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() =>
            db.IngestExternalFiles([WriteSst(fs, "/ext/4.sst", 65, 75, "d")], new IngestExternalFileOptions { AllowBlockingFlush = false }));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        Assert.AreEqual("mem", GetString(db, S(Key(70))!));
        db.IngestExternalFiles(["/ext/4.sst"], new IngestExternalFileOptions());
        Assert.AreEqual("d70", GetString(db, S(Key(70))!));
        Assert.AreEqual("0", db.GetProperty("rocksdb.num-entries-active-mem-table"));

        // Overlapping files in one call: the later file wins; no global seqno allowed but needed -> rejected.
        db.IngestExternalFiles([WriteSst(fs, "/ext/5.sst", 400, 410, "e"), WriteSst(fs, "/ext/6.sst", 405, 420, "f")], new IngestExternalFileOptions());
        Assert.AreEqual("e400", GetString(db, S(Key(400))!));
        Assert.AreEqual("f405", GetString(db, S(Key(405))!));
        e = Assert.ThrowsExactly<RocksDbException>(() => db.IngestExternalFiles(
            [WriteSst(fs, "/ext/7.sst", 0, 10, "g")], new IngestExternalFileOptions { AllowGlobalSeqNo = false }));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        Assert.AreEqual("a5", GetString(db, S(Key(5))!));

        // Multiple non-overlapping files at once, then reopen.
        db.IngestExternalFiles([WriteSst(fs, "/ext/8.sst", 500, 510, "h"), WriteSst(fs, "/ext/9.sst", 520, 530, "i")], new IngestExternalFileOptions());
        int liveBefore = db.GetLiveFilesMetadata().Count;
        db.Dispose();
        using RocksDatabase re = RocksDatabase.Open(DbPath, NewOptions(fs));
        Assert.AreEqual(liveBefore, re.GetLiveFilesMetadata().Count);
        Assert.AreEqual("c55", GetString(re, S(Key(55))!));
        Assert.AreEqual("d70", GetString(re, S(Key(70))!));
        Assert.AreEqual("i525", GetString(re, S(Key(525))!));
        Assert.AreEqual("b250", GetString(re, S(Key(250))!));
        int count = 0;
        using (Iterator it = re.NewIterator())
        {
            for (it.SeekToFirst(); it.Valid; it.Next()) count++;
        }
        Assert.AreEqual(100 + 100 + 1 + 20 + 10 + 10, count);
    }

    // ---- read-only / secondary ---------------------------------------------------------------------------

    [TestMethod]
    public void ReadOnlyRejectsWritesAndReplaysWal()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase primary = RocksDatabase.Open(DbPath, NewOptions(fs));
        for (int i = 0; i < 20; i++) primary.Put(Key(i), Value(i, 0));
        primary.Flush();
        for (int i = 10; i < 30; i++) primary.Put(Key(i), Value(i, 1));
        var before = fs.GetChildren(DbPath).OrderBy(x => x, StringComparer.Ordinal).ToList();

        // No LOCK conflict with the open primary.
        using RocksDatabase ro = RocksDatabase.OpenReadOnly(DbPath, new DbOptions { FileSystem = fs });
        Assert.IsTrue(ro.IsReadOnly);
        for (int i = 0; i < 30; i++) Assert.AreEqual(S(Value(i, i < 10 ? 0 : 1)), GetString(ro, S(Key(i))!));
        Assert.AreEqual(primary.GetLatestSequenceNumber(), ro.GetLatestSequenceNumber());
        Assert.AreEqual(StatusCode.NotSupported, Assert.ThrowsExactly<RocksDbException>(() => ro.Put(B("a"), B("b"))).Code);
        Assert.AreEqual(StatusCode.NotSupported, Assert.ThrowsExactly<RocksDbException>(() => ro.Flush()).Code);
        Assert.AreEqual(StatusCode.NotSupported,
            Assert.ThrowsExactly<RocksDbException>(() => ro.CreateColumnFamily("x", new ColumnFamilyOptions())).Code);
        CollectionAssert.AreEqual(before, fs.GetChildren(DbPath).OrderBy(x => x, StringComparer.Ordinal).ToList(), "read-only open must not write");
        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() =>
            RocksDatabase.OpenReadOnly(DbPath, new DbOptions { FileSystem = fs }, [new ColumnFamilyDescriptor("default", new ColumnFamilyOptions())], true));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
    }

    [TestMethod]
    public async Task SecondaryCatchesUpWithPrimary()
    {
        MemoryFileSystem fs = NewFs();
        await using RocksDatabase primary = await RocksDatabase.OpenAsync(DbPath, NewOptions(fs), new ColumnFamilyOptions { WriteBufferSize = 256 * 1024, ArenaBlockSize = 32 * 1024 });
        for (int i = 0; i < 50; i++) primary.Put(Key(i), Value(i, 0));
        await using RocksDatabase sec = await RocksDatabase.OpenAsSecondaryAsync(DbPath, "/sec", new DbOptions { FileSystem = fs });
        Assert.IsTrue(sec.IsSecondary);
        Assert.AreEqual(S(Value(7, 0)), GetString(sec, S(Key(7))!));
        Assert.IsNull(GetString(sec, S(Key(60))!));

        // Appended WAL data only.
        for (int i = 50; i < 70; i++) primary.Put(Key(i), Value(i, 0));
        await sec.TryCatchUpWithPrimaryAsync();
        Assert.AreEqual(S(Value(65, 0)), GetString(sec, S(Key(65))!));

        // Flushes (MANIFEST changes + WAL rotation), overwrites and deletes.
        for (int round = 1; round <= 3; round++)
        {
            for (int i = 0; i < 400; i++) primary.Put(Key(i), Value(i, round));
            primary.Delete(Key(round));
            await primary.FlushAsync();
            for (int i = 0; i < 10; i++) primary.Put(Key(i + 1000), Value(i, round));
            await sec.TryCatchUpWithPrimaryAsync();
            Assert.AreEqual(primary.GetLatestSequenceNumber(), sec.GetLatestSequenceNumber());
            for (int i = 0; i < 400; i++) Assert.AreEqual(GetString(primary, S(Key(i))!), GetString(sec, S(Key(i))!), $"round {round} key {i}");
            Assert.AreEqual(S(Value(5, round)), GetString(sec, S(Key(1005))!));
        }
        Assert.AreEqual(StatusCode.NotSupported, Assert.ThrowsExactly<RocksDbException>(() => sec.Put(B("a"), B("b"))).Code);
    }
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task SecondaryOpenedOnEmptyWalCatchesUpLaterWrites(bool localFs)
    {
        // Regression: a secondary that opens while the primary's current WAL is still empty (e.g. right after Flush)
        // must pick up records appended to that WAL later.
        string root = localFs ? Path.Combine(Path.GetTempPath(), "rocksdb-managed-sec-" + Guid.NewGuid().ToString("N")) : "";
        RocksDb.Managed.Env.IFileSystem fs = localFs ? RocksDb.Managed.Env.LocalFileSystem.Instance : NewFs();
        string dbPath = localFs ? root + "/db" : DbPath;
        string secPath = localFs ? root + "/sec" : "/sec-empty";
        try
        {
            await using RocksDatabase primary = await RocksDatabase.OpenAsync(dbPath, new DbOptions { CreateIfMissing = true, FileSystem = fs });
            for (int i = 0; i < 20; i++) primary.Put(Key(i), Value(i, 0));
            await primary.FlushAsync(); // the current WAL is now empty
            await using RocksDatabase sec = await RocksDatabase.OpenAsSecondaryAsync(dbPath, secPath, new DbOptions { FileSystem = fs });
            Assert.AreEqual(S(Value(3, 0)), GetString(sec, S(Key(3))!));
            for (int round = 1; round <= 3; round++)
            {
                for (int i = 100 * round; i < 100 * round + 10; i++) primary.Put(Key(i), Value(i, round));
                await sec.TryCatchUpWithPrimaryAsync();
                Assert.AreEqual(primary.GetLatestSequenceNumber(), sec.GetLatestSequenceNumber(), "round " + round);
                Assert.AreEqual(S(Value(100 * round + 5, round)), GetString(sec, S(Key(100 * round + 5))!), "round " + round);
            }
        }
        finally
        {
            if (localFs) try { Directory.Delete(root, recursive: true); } catch (IOException) { }
        }
    }

    // ---- TTL -----------------------------------------------------------------------------------------------

    [TestMethod]
    public void TtlValuesAndCompactionFilter()
    {
        MemoryFileSystem fs = NewFs();
        using (TtlDatabase db = TtlDatabase.Open(DbPath, NewOptions(fs), 100))
        {
            db.Put(B("a"), B("1"));
            using (WriteBatch b = db.Database.CreateWriteBatch())
            {
                b.Put(B("b"), B("2")).Delete(B("a"));
                db.Write(b);
            }
            Assert.IsNull(db.Get(B("a")));
            Assert.AreEqual("2", S(db.Get(B("b"))));
            byte[] raw = db.Database.Get(B("b"))!;
            Assert.AreEqual(5, raw.Length);
            int ts = TtlValue.GetTimestamp(raw);
            Assert.IsTrue(Math.Abs(ts - TtlValue.Now) < 5);

            var newValue = new System.Buffers.ArrayBufferWriter<byte>();
            var skip = new System.Buffers.ArrayBufferWriter<byte>();
            Assert.AreEqual(CompactionFilterDecision.Keep, new TtlCompactionFilter(100, null, () => ts + 50).Filter(1, B("b"), CompactionFilterValueType.Value, raw, newValue, skip));
            Assert.AreEqual(CompactionFilterDecision.Remove, new TtlCompactionFilter(100, null, () => ts + 101).Filter(1, B("b"), CompactionFilterValueType.Value, raw, newValue, skip));
            Assert.AreEqual(CompactionFilterDecision.Keep, new TtlCompactionFilter(0, null, () => ts + 1000000).Filter(1, B("b"), CompactionFilterValueType.Value, raw, newValue, skip));
            db.Database.Put(B("bad"), B("xy"));
            Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => db.Get(B("bad"))).Code);
        }
        using TtlDatabase re = TtlDatabase.Open(DbPath, NewOptions(fs), 100);
        Assert.AreEqual("2", S(re.Get(B("b"))));
    }
}
