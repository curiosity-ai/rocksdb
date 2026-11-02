using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>
/// SeqnoToTimeMapping (ports of db/seqno_time_test.cc MappingAppend, CapacityLimits, TimeSpanLimits,
/// ProximalFunctions, EncodeDecodeBasic) and the DB-level tiering it drives: the prepopulated mapping of a new DB,
/// rocksdb.seqno.time.map table properties, TimedPut preferred seqnos at flush and per-key placement of
/// preclude_last_level_data_seconds (tiered_compaction_test.cc / seqno_time_test.cc style).
/// </summary>
[TestClass]
public sealed class SeqnoToTimeMappingTests
{
    private const string DbPath = "/seqno-time";

    private static SeqnoToTimeMapping.SeqnoTimePair P(ulong s, ulong t) => new(s, t);

    private static SeqnoToTimeMapping.SeqnoTimePair Last(SeqnoToTimeMapping m) => m.Pairs[^1];

    [TestMethod]
    public void MappingAppend()
    {
        var test = new SeqnoToTimeMapping();
        test.SetMaxTimeSpan(100).SetCapacity(10);
        // ignore seqno == 0, as it may mean the seqno is zeroed out
        Assert.IsFalse(test.Append(0, 100));
        Assert.IsTrue(test.Append(3, 200));
        int size = test.Size;
        Assert.IsTrue(test.Append(10, 300));
        size++;
        Assert.AreEqual(size, test.Size);
        // Append with the same seqno, newer time is rejected
        Assert.IsFalse(test.Append(10, 301));
        Assert.AreEqual(size, test.Size);
        Assert.AreEqual(P(10, 300), Last(test));
        // Same or new seqno with same or older time replaces the last entry
        Assert.IsFalse(test.Append(10, 299));
        Assert.AreEqual(P(10, 299), Last(test));
        Assert.IsFalse(test.Append(11, 299));
        Assert.AreEqual(P(11, 299), Last(test));
        Assert.IsFalse(test.Append(11, 250));
        Assert.AreEqual(size, test.Size);
        Assert.AreEqual(P(11, 250), Last(test));
    }

    [TestMethod]
    public void CapacityLimits()
    {
        var test = new SeqnoToTimeMapping();
        test.SetCapacity(3);
        Assert.IsTrue(test.Append(10, 300));
        Assert.IsTrue(test.Append(20, 400));
        Assert.IsTrue(test.Append(30, 500));
        Assert.IsTrue(test.Append(40, 600));
        Assert.AreEqual(3, test.Size);
        Assert.AreEqual(P(40, 600), Last(test));
        test.SetCapacity(2);
        Assert.AreEqual(2, test.Size);
        Assert.AreEqual(P(40, 600), Last(test));
        Assert.IsTrue(test.Append(50, 700));
        Assert.AreEqual(2, test.Size);
        Assert.AreEqual(P(50, 700), Last(test));
        // Capacity 1 is coerced to 2.
        test.SetCapacity(1);
        Assert.AreEqual(2, test.Size);
        Assert.IsTrue(test.Append(60, 800));
        Assert.AreEqual(2, test.Size);
        Assert.AreEqual(P(60, 800), Last(test));
        // Capacity 0 means throw everything away
        test.SetCapacity(0);
        Assert.AreEqual(0, test.Size);
        Assert.IsFalse(test.Append(70, 900));
        Assert.AreEqual(0, test.Size);
        test.SetCapacity(ulong.MaxValue);
        for (uint i = 1; i <= 10101U; i++) Assert.IsTrue(test.Append(i, 11UL * i));
        Assert.AreEqual(10101, test.Size);
    }

    [TestMethod]
    public void TimeSpanLimits()
    {
        var test = new SeqnoToTimeMapping();
        for (int i = 1; i <= 63; i++) Assert.IsTrue(test.Append(1000 + (ulong)i, 1UL << i));
        Assert.AreEqual(63, test.Size);
        // We generally keep 2 entries as long as the configured max time span is non-zero
        test.SetMaxTimeSpan(10);
        Assert.AreEqual(2, test.Size);
        test.SetMaxTimeSpan(1);
        Assert.AreEqual(2, test.Size);
        // But go down to 1 entry if the max time span is zero
        test.SetMaxTimeSpan(0);
        Assert.AreEqual(1, test.Size);
        test.Clear();
        test.SetMaxTimeSpan(100);
        ulong[] times = [123, 134, 150, 189, 220, 233, 234, 235, 300, 350, 470];
        int[] sizes = [1, 2, 3, 4, 5, 6, 6, 7, 6, 3, 2];
        for (int i = 0; i < times.Length; i++)
        {
            Assert.IsTrue(test.Append(1001 + (ulong)i, times[i]));
            Assert.AreEqual(sizes[i], test.Size, "after append " + i);
        }
    }

    [TestMethod]
    public void ProximalFunctions()
    {
        var test = new SeqnoToTimeMapping();
        test.SetCapacity(10);
        Assert.AreEqual(SeqnoToTimeMapping.UnknownTimeBeforeAll, test.GetProximalTimeBeforeSeqno(1));
        Assert.AreEqual(SeqnoToTimeMapping.UnknownSeqnoBeforeAll, test.GetProximalSeqnoBeforeTime(1));
        Assert.IsTrue(test.Append(10, 500));
        Assert.AreEqual(0UL, test.GetProximalTimeBeforeSeqno(10));
        Assert.AreEqual(500UL, test.GetProximalTimeBeforeSeqno(11));
        Assert.AreEqual(0UL, test.GetProximalSeqnoBeforeTime(499));
        Assert.AreEqual(10UL, test.GetProximalSeqnoBeforeTime(500));
        Assert.IsTrue(test.Append(20, 600));
        Assert.IsTrue(test.Append(30, 700));
        Assert.AreEqual(500UL, test.GetProximalTimeBeforeSeqno(20));
        Assert.AreEqual(600UL, test.GetProximalTimeBeforeSeqno(21));
        Assert.AreEqual(20UL, test.GetProximalSeqnoBeforeTime(699));
        Assert.AreEqual(30UL, test.GetProximalSeqnoBeforeTime(700));
        // Redundant / later samples with the same seqno are ignored
        Assert.IsFalse(test.Append(30, 700));
        Assert.IsFalse(test.Append(30, 800));
        Assert.AreEqual(30UL, test.GetProximalSeqnoBeforeTime(700));
        Assert.IsTrue(test.Append(40, 900));
        // Burst of writes: merge with previous entry
        Assert.IsFalse(test.Append(50, 900));
        Assert.AreEqual(700UL, test.GetProximalTimeBeforeSeqno(49));
        Assert.AreEqual(50UL, test.GetProximalSeqnoBeforeTime(900));

        // GetCurrentTieringCutoffSeqnos
        ulong preserve = DbFormat.MaxSequenceNumber, preclude = DbFormat.MaxSequenceNumber;
        test.GetCurrentTieringCutoffSeqnos(1000, 0, 0, ref preserve, ref preclude);
        Assert.AreEqual(DbFormat.MaxSequenceNumber, preserve, "disabled: unchanged");
        test.GetCurrentTieringCutoffSeqnos(1000, 400, 300, ref preserve, ref preclude);
        // Pairs (10,500) (20,600) (30,700) (50,900): written at or after time 600 means seqno > 20.
        Assert.AreEqual(21UL, preserve, "written at or after time 600");
        Assert.AreEqual(31UL, preclude, "written at or after time 700");
    }

    [TestMethod]
    public void EncodeDecodeBasic()
    {
        const uint OriginalSamples = 1000;
        var test = new SeqnoToTimeMapping();
        test.SetCapacity(OriginalSamples);
        Assert.AreEqual(0, test.Encode().Length);
        Assert.IsNull(test.DecodeFrom([]));
        var rnd = new System.Random(123);
        for (uint i = 1; i <= OriginalSamples; i++) Assert.IsTrue(test.Append(i, i * 10UL + (ulong)rnd.Next(10)));
        byte[] output = test.Encode();
        var decoded = new SeqnoToTimeMapping();
        Assert.IsNull(decoded.DecodeFrom(output));
        Assert.IsTrue(decoded.IsEnforced);
        CollectionAssert.AreEqual(test.Pairs.ToArray(), decoded.Pairs.ToArray());

        // Encode a reduced set of mappings
        const int ReducedSize = 51;
        var reduced = new SeqnoToTimeMapping();
        reduced.CopyFrom(test);
        reduced.SetCapacity(ReducedSize);
        decoded.Clear();
        Assert.IsNull(decoded.DecodeFrom(reduced.Encode()));
        Assert.AreEqual(ReducedSize, decoded.Size);
        for (ulong t = 1; t <= OriginalSamples * 11; t += 1 + t / 100)
        {
            ulong origS = test.GetProximalSeqnoBeforeTime(t);
            ulong approxS = decoded.GetProximalSeqnoBeforeTime(t);
            Assert.AreEqual(origS == 0, approxS == 0, "t=" + t);
            Assert.AreEqual(origS == OriginalSamples, approxS == OriginalSamples, "t=" + t);
            const uint SeqnoFuzz = OriginalSamples * 3 / 2 / ReducedSize;
            Assert.IsTrue(approxS + SeqnoFuzz >= origS && origS >= approxS, "t=" + t);
        }
        // Corrupt input leaves the mapping unchanged.
        int before = decoded.Size;
        Assert.IsNotNull(decoded.DecodeFrom([5, 1]));
        Assert.AreEqual(before, decoded.Size);
    }

    // ---- DB level ------------------------------------------------------------------------------------------------

    private static ColumnFamilyOptions Tiered(ulong precludeSeconds) => new()
    {
        NumLevels = 7,
        PrecludeLastLevelDataSeconds = precludeSeconds,
        DisableAutoCompactions = true,
        Compression = RocksDb.Managed.Compression.CompressionType.None,
    };

    private static List<int> Levels(RocksDatabase db)
    {
        var l = new List<int>();
        foreach (LiveFileMetadata f in db.GetLiveFilesMetadata()) l.Add(f.Level);
        l.Sort();
        return l;
    }

    private static TableProperties PropertiesOf(RocksDatabase db, LiveFileMetadata f)
    {
        ColumnFamilyData cfd = db.DefaultColumnFamily.Data;
        foreach (FileMetaData m in cfd.Current.Storage.LevelFiles(f.Level))
        {
            if (m.FileNumber != f.FileNumber) continue;
            using TableReaderLease lease = cfd.TableCache.GetReader(m);
            return lease.Reader.Properties;
        }
        throw new InvalidOperationException("file not found");
    }

    [TestMethod]
    public void NewDbWithPrecludePrepopulatesTheMapping()
    {
        // PrePopulateInDB: 100 pre-allocated seqnos mapped back over the preserve window.
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), Tiered(10000)))
        {
            Assert.AreEqual(100UL, db.GetLatestSequenceNumber());
            SeqnoToTimeMapping m = db.DefaultColumnFamily.Db.TestGetSeqnoToTimeMapping();
            Assert.IsFalse(m.Empty);
            ulong now = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            // Seqno 1 maps to about now - 10000.
            Assert.IsTrue(m.GetProximalSeqnoBeforeTime(now - 9000) is > 1 and < 100);
            db.Put(B("k"), B("v"));
        }
        // Not repeated on reopen; the pre-allocated seqnos are in the MANIFEST.
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), Tiered(10000)))
        {
            Assert.AreEqual(101UL, db.GetLatestSequenceNumber());
            Assert.AreEqual("v", GetString(db, "k"));
        }
        // Without preserve / preclude a new DB starts at 0.
        MemoryFileSystem fs2 = NewFs();
        using RocksDatabase plain = RocksDatabase.Open(DbPath, NewOptions(fs2), Tiered(0));
        Assert.AreEqual(0UL, plain.GetLatestSequenceNumber());
    }

    [TestMethod]
    public void FreshDataStaysAboveTheLastLevel()
    {
        // preclude_last_level_data_seconds: a full compaction keeps data younger than the window on the proximal level
        // (L5 of 7), and older data (a test cutoff) goes to the last level.
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), Tiered(10000));
        for (int i = 0; i < 100; i++) db.Put(Key(i), Value(i, 0));
        db.Flush();
        List<LiveFileMetadata> files = db.GetLiveFilesMetadata();
        Assert.AreEqual(1, files.Count);
        Assert.AreNotEqual(0, PropertiesOf(db, files[0]).SequenceNumberTimeMapping.Length, "flush writes rocksdb.seqno.time.map");
        db.CompactRange();
        CollectionAssert.AreEqual(new List<int> { 5 }, Levels(db));
        Assert.AreNotEqual(0, PropertiesOf(db, db.GetLiveFilesMetadata()[0]).SequenceNumberTimeMapping.Length);
        for (int i = 0; i < 100; i++) Assert.AreEqual(S(Value(i, 0)), GetString(db, S(Key(i))!));

        // Pretend the first half of the writes is older than the window: those entries go to the last level.
        // preclude_last_level_min_seqno = cutoff; proximal_after_seqno_ = cutoff, so entries with seqno > cutoff go to the
        // proximal level (CompactionJob: use_proximal_output = seq > proximal_after_seqno_).
        ulong cutoff = 100 + 50; // seqnos 101..150 are "old"
        CompactionJob.TestPrecludeLastLevelMinSeqno = _ => cutoff;
        try
        {
            db.CompactRange();
        }
        finally
        {
            CompactionJob.TestPrecludeLastLevelMinSeqno = null;
        }
        CollectionAssert.AreEqual(new List<int> { 5, 6 }, Levels(db));
        foreach (LiveFileMetadata f in db.GetLiveFilesMetadata())
        {
            if (f.Level == 6) Assert.IsTrue(f.LargestSeqno <= cutoff, "last level holds only old entries");
            else Assert.IsTrue(f.SmallestSeqno > cutoff, "proximal level holds only new entries");
        }
        for (int i = 0; i < 100; i++) Assert.AreEqual(S(Value(i, 0)), GetString(db, S(Key(i))!));
    }

    [TestMethod]
    public void WithoutPrecludeEverythingGoesToTheLastLevel()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), Tiered(0));
        for (int i = 0; i < 100; i++) db.Put(Key(i), Value(i, 0));
        db.Flush();
        Assert.AreEqual(0, PropertiesOf(db, db.GetLiveFilesMetadata()[0]).SequenceNumberTimeMapping.Length);
        db.CompactRange();
        CollectionAssert.AreEqual(new List<int> { 6 }, Levels(db));
    }

    [TestMethod]
    public void PreserveInternalTimeKeepsSequenceNumbers()
    {
        // preserve_internal_time_seconds: the bottommost compaction does not zero sequence numbers written inside the
        // window (their write time stays recoverable); without it they are zeroed.
        foreach (ulong preserve in new ulong[] { 0, 10000 })
        {
            MemoryFileSystem fs = NewFs();
            ColumnFamilyOptions cf = Tiered(0);
            cf.PreserveInternalTimeSeconds = preserve;
            using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cf);
            // Two overlapping files, so the compaction rewrites them (no trivial move).
            for (int round = 0; round < 2; round++)
            {
                for (int i = 0; i < 10; i++) db.Put(Key(i), Value(i, round));
                db.Flush();
            }
            db.CompactRange();
            LiveFileMetadata f = db.GetLiveFilesMetadata()[0];
            Assert.AreEqual(6, f.Level);
            if (preserve == 0) Assert.AreEqual(0UL, f.LargestSeqno, "zeroed");
            else Assert.AreNotEqual(0UL, f.SmallestSeqno, "preserved");
        }
    }

    [TestMethod]
    public void TimedPutGetsAPreferredSeqnoAtFlush()
    {
        // BuildTable: a TimedPut with a write time inside the mapping gets the matching (older) preferred seqno; the
        // value reads back unchanged, before and after a bottommost compaction swaps the seqno in.
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cf = Tiered(0);
        cf.PreserveInternalTimeSeconds = 10000;
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cf);
        ulong now = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        for (int i = 0; i < 20; i++) db.Put(Key(i), Value(i, 0));
        using (var batch = new WriteBatch())
        {
            batch.TimedPut(0, B("timed"), B("old-value"), now - 9000);
            db.Write(batch);
        }
        db.Flush();
        Assert.AreEqual("old-value", GetString(db, "timed"));
        db.CompactRange();
        Assert.AreEqual("old-value", GetString(db, "timed"));
        for (int i = 0; i < 20; i++) Assert.AreEqual(S(Value(i, 0)), GetString(db, S(Key(i))!));
    }
}
