using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Trace;
using RocksDb.Managed.Util;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Trace;

/// <summary>Query / block cache / IO tracing and replay (port of the spirit of db/db_test2.cc TraceAndReplay*, trace_replay/*_test.cc).</summary>
[TestClass]
public sealed class TracingTests
{
    private const string TracePath = "/trace";

    private static List<TraceRecord> ReadAll(RocksDatabase db, IFileSystem fs, string path, out ulong headerTs)
    {
        using Replayer r = db.NewDefaultReplayer(TraceReader.OpenFile(fs, path));
        r.Prepare();
        headerTs = r.HeaderTimestamp;
        var list = new List<TraceRecord>();
        while (r.TryNext(out TraceRecord? rec)) list.Add(rec!);
        return list;
    }

    private static void Workload(RocksDatabase db, ColumnFamily cf)
    {
        db.Put(B("a"), B("1"));
        using (WriteBatch wb = db.CreateWriteBatch())
        {
            wb.Put(B("b"), B("2")).Delete(B("a"));
            db.Write(wb);
        }
        cf.Put(B("x"), B("9"));
        _ = db.Get(B("a"));
        _ = db.Get(B("b"));
        _ = cf.Get(B("x"));
        _ = db.MultiGet([B("a"), B("b"), B("c")]);
        using Iterator it = db.NewIterator(new ReadOptions { IterateUpperBound = B("z") });
        it.Seek(B("b"));
        it.SeekForPrev(B("b"));
    }

    [TestMethod]
    public void TraceRecordsAllQueryTypesAndReplaysThemIntoAnotherDb()
    {
        MemoryFileSystem fs = NewFs();
        uint cfId;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            ColumnFamily cf = db.CreateColumnFamily("cf1", new ColumnFamilyOptions());
            cfId = cf.Id;
            db.StartTrace(new TraceOptions(), TracePath);
            Workload(db, cf);
            db.EndTrace();
            RocksDbException e = Assert.ThrowsExactly<RocksDbException>(db.EndTrace);
            Assert.AreEqual(StatusCode.IOError, e.Code);

            List<TraceRecord> recs = ReadAll(db, fs, TracePath, out ulong headerTs);
            Assert.IsTrue(headerTs > 0);
            CollectionAssert.AreEqual(
                new[] { TraceType.Write, TraceType.Write, TraceType.Write, TraceType.Get, TraceType.Get, TraceType.Get, TraceType.MultiGet, TraceType.IteratorSeek, TraceType.IteratorSeekForPrev },
                recs.ConvertAll(r => r.TraceType));
            var get = (GetQueryTraceRecord)recs[5];
            Assert.AreEqual(cfId, get.ColumnFamilyId);
            CollectionAssert.AreEqual(B("x"), get.Key.ToArray());
            Assert.IsNull(get.ReadOptions);
            var mg = (MultiGetQueryTraceRecord)recs[6];
            Assert.AreEqual(3, mg.Keys.Count);
            var seek = (IteratorSeekQueryTraceRecord)recs[7];
            CollectionAssert.AreEqual(B("z"), seek.UpperBound.ToArray());
            Assert.IsTrue(seek.LowerBound.IsEmpty);
        }

        // Replay into a fresh DB with the same column families: the writes are re-applied.
        string other = "/replayed";
        var opts = NewOptions(fs);
        using RocksDatabase target = RocksDatabase.Open(other, opts);
        ColumnFamily tcf = target.CreateColumnFamily("cf1", new ColumnFamilyOptions());
        Assert.AreEqual(cfId, tcf.Id);
        using Replayer replayer = target.NewDefaultReplayer(TraceReader.OpenFile(fs, TracePath));
        replayer.Prepare();
        var results = new List<TraceRecordResult>();
        replayer.Replay(new ReplayOptions { FastForward = 1000 }, (err, res) =>
        {
            Assert.IsNull(err);
            lock (results) results.Add(res!);
        });
        Assert.AreEqual(9, results.Count);
        Assert.IsNull(target.Get(B("a")));
        Assert.AreEqual("2", S(target.Get(B("b"))));
        Assert.AreEqual("9", S(tcf.Get(B("x"))));
        var g = (SingleValueTraceExecutionResult)results[4];
        Assert.AreEqual(StatusCode.Ok, g.Status);
        CollectionAssert.AreEqual(B("2"), g.Value);
        var m = (MultiValuesTraceExecutionResult)results[6];
        CollectionAssert.AreEqual(new[] { StatusCode.NotFound, StatusCode.Ok, StatusCode.NotFound }, m.Statuses.ToArray());
        var s = (IteratorTraceExecutionResult)results[7];
        Assert.IsTrue(s.Valid);
        CollectionAssert.AreEqual(B("b"), s.Key);
        // A replay past the end of the trace is Incomplete; Prepare rewinds.
        Assert.AreEqual(StatusCode.Incomplete, Assert.ThrowsExactly<RocksDbException>(() => replayer.Replay(new ReplayOptions(), null)).Code);
        replayer.Prepare();
        replayer.Replay(new ReplayOptions { NumThreads = 4, FastForward = 1000 }, null);
    }

    [TestMethod]
    public void FilterSamplingAndMaxSizeAreApplied()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        db.StartTrace(new TraceOptions { Filter = TraceFilter.Get | TraceFilter.IteratorSeek }, TracePath);
        Workload(db, db.DefaultColumnFamily);
        db.EndTrace();
        List<TraceRecord> recs = ReadAll(db, fs, TracePath, out _);
        Assert.IsFalse(recs.Exists(r => r.TraceType is TraceType.Get or TraceType.IteratorSeek));
        Assert.IsTrue(recs.Exists(r => r.TraceType == TraceType.IteratorSeekForPrev));

        // Sampling: one of every 3 requests.
        db.StartTrace(new TraceOptions { SamplingFrequency = 3 }, TracePath);
        for (int i = 0; i < 30; i++) db.Put(Key(i), Value(i, 0));
        db.EndTrace();
        Assert.AreEqual(10, ReadAll(db, fs, TracePath, out _).Count);

        // Max size: the header alone already exceeds 1 byte, so nothing but header and footer is written.
        db.StartTrace(new TraceOptions { MaxTraceFileSize = 1 }, TracePath);
        for (int i = 0; i < 30; i++) db.Put(Key(i), Value(i, 0));
        db.EndTrace();
        Assert.AreEqual(0, ReadAll(db, fs, TracePath, out _).Count);
    }

    [TestMethod]
    public void ReadWriteOptionsAreRecordedAndDecoded()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        db.StartTrace(new TraceOptions { IncludeReadWriteOptions = true, PreserveWriteOrder = true }, TracePath);
        db.Put(new WriteOptions { DisableWal = true, NoSlowdown = true }, B("k"), B("v"));
        _ = db.Get(new ReadOptions { FillCache = false, ReadaheadSize = 4096, ReadTier = ReadTier.BlockCacheTier, VerifyChecksums = false }, B("k"));
        db.EndTrace();
        List<TraceRecord> recs = ReadAll(db, fs, TracePath, out _);
        var w = (WriteQueryTraceRecord)recs[0];
        Assert.IsNotNull(w.WriteOptions);
        Assert.IsTrue(w.WriteOptions.DisableWal);
        Assert.IsTrue(w.WriteOptions.NoSlowdown);
        Assert.IsFalse(w.WriteOptions.Sync);
        using (var batch = new WriteBatch(w.WriteBatchRep.Span)) Assert.AreEqual(1, batch.Count);
        ReadOptions ro = ((GetQueryTraceRecord)recs[1]).ReadOptions!;
        Assert.IsFalse(ro.FillCache);
        Assert.IsFalse(ro.VerifyChecksums);
        Assert.AreEqual(4096UL, ro.ReadaheadSize);
        Assert.AreEqual(ReadTier.BlockCacheTier, ro.ReadTier);
        Assert.IsTrue(ro.AutoReadaheadSize);
    }

    [TestMethod]
    public void HeaderFormatMatchesNative()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        db.StartTrace(new TraceOptions(), TracePath);
        db.EndTrace();
        using TraceReader reader = TraceReader.OpenFile(fs, TracePath);
        byte[] header = reader.ReadRecord()!;
        Assert.AreEqual((byte)TraceType.Begin, header[8]);
        string payload = System.Text.Encoding.ASCII.GetString(header, 13, header.Length - 13);
        StringAssert.StartsWith(payload, "feedcafedeadbeef\tTrace Version: 0.2\tRocksDB Version: ");
        StringAssert.EndsWith(payload, "\tFormat: Timestamp OpType Payload\n");
        Assert.AreEqual(Coding.DecodeFixed32(header.AsSpan(9)), (uint)(header.Length - 13));
        byte[] footer = reader.ReadRecord()!;
        Assert.AreEqual((byte)TraceType.End, footer[8]);
        Assert.AreEqual(13, footer.Length);
        Assert.IsNull(reader.ReadRecord());
    }

    [TestMethod]
    public void ReplayStopsAtFirstFailingRecord()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            ColumnFamily cf = db.CreateColumnFamily("cf1", new ColumnFamilyOptions());
            db.StartTrace(new TraceOptions(), TracePath);
            cf.Put(B("x"), B("1"));
            db.EndTrace();
        }
        using RocksDatabase target = RocksDatabase.Open("/t2", NewOptions(fs));
        using Replayer r = target.NewDefaultReplayer(TraceReader.OpenFile(fs, TracePath));
        r.Prepare();
        // The write batch names column family 1, which the target does not have.
        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => r.Replay(new ReplayOptions(), null));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => r.Replay(new ReplayOptions { FastForward = 0 }, null)).Code);
    }

    [TestMethod]
    public void BlockCacheTraceRecordsLookups()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        for (int i = 0; i < 200; i++) db.Put(Key(i), Value(i, 0));
        db.Flush();
        db.StartBlockCacheTrace(new BlockCacheTraceOptions(), TracePath);
        Assert.AreEqual(StatusCode.Busy, Assert.ThrowsExactly<RocksDbException>(() => db.StartBlockCacheTrace(new BlockCacheTraceOptions(), "/other")).Code);
        _ = db.Get(Key(5));
        _ = db.Get(Key(5));
        using (Iterator it = db.NewIterator()) for (it.SeekToFirst(); it.Valid; it.Next()) { }
        db.EndBlockCacheTrace();
        db.EndBlockCacheTrace(); // no-op

        using var reader = new BlockCacheTraceReader(TraceReader.OpenFile(fs, TracePath));
        TraceFileHeader h = reader.ReadHeader();
        Assert.AreEqual((uint)OptionsFile.CurrentRocksDbVersion.Major, h.RocksDbMajorVersion);
        var recs = new List<BlockCacheTraceRecord>();
        while (reader.TryReadAccess(out BlockCacheTraceRecord? r)) recs.Add(r!);
        List<BlockCacheTraceRecord> gets = recs.FindAll(r => r.Caller == TableReaderCaller.UserGet && r.BlockType == TraceType.BlockDataBlock);
        Assert.AreEqual(2, gets.Count);
        Assert.IsFalse(gets[0].IsCacheHit);
        Assert.IsTrue(gets[1].IsCacheHit);
        Assert.AreNotEqual(gets[0].GetId, gets[1].GetId);
        CollectionAssert.AreEqual(Key(5), gets[0].ReferencedKey);
        Assert.AreEqual(16, gets[0].BlockKey.Length);
        Assert.AreEqual("default", gets[0].ColumnFamilyName);
        Assert.AreEqual(0u, gets[0].Level);
        Assert.IsTrue(recs.Exists(r => r.Caller == TableReaderCaller.UserIterator && r.BlockType == TraceType.BlockDataBlock));
    }

    [TestMethod]
    public void BlockCacheTraceSamplingKeepsWholeBlockHistories()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { TableOptions = new BlockBasedTableOptions { BlockSize = 256 } });
        for (int i = 0; i < 2000; i++) db.Put(Key(i), Value(i, 0));
        db.Flush();
        db.StartBlockCacheTrace(new BlockCacheTraceOptions { SamplingFrequency = 4 }, TracePath);
        for (int round = 0; round < 2; round++)
        {
            using Iterator it = db.NewIterator();
            for (it.SeekToFirst(); it.Valid; it.Next()) { }
        }
        db.EndBlockCacheTrace();
        using var reader = new BlockCacheTraceReader(TraceReader.OpenFile(fs, TracePath));
        reader.ReadHeader();
        var perBlock = new Dictionary<string, int>();
        while (reader.TryReadAccess(out BlockCacheTraceRecord? r))
        {
            if (r!.BlockType != TraceType.BlockDataBlock) continue;
            string k = Convert.ToHexString(r.BlockKey);
            perBlock[k] = perBlock.GetValueOrDefault(k) + 1;
            Assert.AreEqual(0UL, Hash.GetSliceRangedNPHash(r.BlockKey, 4));
        }
        Assert.IsTrue(perBlock.Count > 0);
        foreach (int n in perBlock.Values) Assert.AreEqual(2, n);
    }

    [TestMethod]
    public void IoTraceRecordsFileOperations()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        db.StartIOTrace(new TraceOptions(), TracePath);
        for (int i = 0; i < 100; i++) db.Put(Key(i), Value(i, 0));
        db.Flush();
        db.EndIOTrace();
        using var reader = new IoTraceReader(TraceReader.OpenFile(fs, TracePath));
        reader.ReadHeader();
        var ops = new List<IoTraceRecord>();
        while (reader.TryReadIoOp(out IoTraceRecord? r)) ops.Add(r!);
        Assert.IsTrue(ops.Exists(o => o.FileOperation == "NewWritableFile" && o.FileName.EndsWith(".sst", StringComparison.Ordinal)));
        IoTraceRecord append = ops.Find(o => o.FileOperation == "Append" && o.FileName.EndsWith(".log", StringComparison.Ordinal))!;
        Assert.IsNotNull(append);
        Assert.AreEqual(IoTraceOp.Length, append.IoOpData);
        Assert.IsTrue(append.Length > 0);
        Assert.AreEqual("OK", append.IoStatus);
        Assert.IsFalse(ops.Exists(o => o.FileName == "trace"));
    }
}
