using System.Buffers;
using System.Globalization;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Db;
using RocksDb.Managed.Db.Blob;
using RocksDb.Managed.Env;
using RocksDb.Managed.Tests.Db;
using RocksDb.Managed.Util;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Blob;

/// <summary>
/// Integrated BlobDB at the DB level. Ports of key cases of db/blob/db_blob_basic_test.cc,
/// db/blob/db_blob_compaction_test.cc and db/blob/db_blob_index_test.cc.
/// </summary>
[TestClass]
public sealed class DbBlobTests
{
    private static ColumnFamilyOptions BlobOptions(ulong minBlobSize = 0) => new()
    {
        EnableBlobFiles = true,
        MinBlobSize = minBlobSize,
        DisableAutoCompactions = true,
    };

    private static RocksDatabase Open(MemoryFileSystem fs, ColumnFamilyOptions cf) => RocksDatabase.Open(DbPath, NewOptions(fs), cf);

    private static List<ulong> BlobFilesOnDisk(IFileSystem fs)
    {
        var list = new List<ulong>();
        foreach (string c in fs.GetChildren(DbPath))
        {
            if (FileNames.TryParseFileName(c, out ulong n, out FileType t) && t == FileType.BlobFile) list.Add(n);
        }
        list.Sort();
        return list;
    }

    private static ulong IntProperty(RocksDatabase db, string p) => ulong.Parse(db.GetProperty(p)!, CultureInfo.InvariantCulture);

    private static string BigValue(int i, string tag) => tag + "-" + i.ToString(CultureInfo.InvariantCulture) + new string((char)('a' + i % 26), 200 + i % 50);

    [TestMethod]
    public void GetBlobAfterFlushAndReopen()
    {
        // db_blob_basic_test.cc GetBlob / MultipleBlobs
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cf = BlobOptions();
        using (RocksDatabase db = Open(fs, cf))
        {
            for (int i = 0; i < 100; i++) db.Put(Key(i), B(BigValue(i, "v")));
            db.Flush();
            Assert.AreEqual(1UL, IntProperty(db, DbProperties.NumBlobFiles));
            Assert.IsTrue(IntProperty(db, DbProperties.LiveBlobFileSize) > 20_000);
            Assert.AreEqual(0UL, IntProperty(db, DbProperties.LiveBlobFileGarbageSize));
            Assert.AreEqual(IntProperty(db, DbProperties.LiveBlobFileSize), IntProperty(db, DbProperties.TotalBlobFileSize));
            for (int i = 0; i < 100; i++) Assert.AreEqual(BigValue(i, "v"), GetString(db, S(Key(i))!));
            StringAssert.StartsWith(db.GetProperty(DbProperties.BlobStats), "Number of blob files: 1\n");
        }
        Assert.AreEqual(1, BlobFilesOnDisk(fs).Count);
        using (RocksDatabase db = Open(fs, cf))
        {
            for (int i = 0; i < 100; i++) Assert.AreEqual(BigValue(i, "v"), GetString(db, S(Key(i))!));
            Assert.AreEqual(1UL, IntProperty(db, DbProperties.NumBlobFiles));
        }
    }

    [TestMethod]
    public async Task AsyncGetMultiGetAndIterators()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = Open(fs, BlobOptions(minBlobSize: 100));
        for (int i = 0; i < 50; i++) db.Put(Key(i), B(i % 2 == 0 ? BigValue(i, "big") : "small" + i));
        db.Flush();
        for (int i = 0; i < 50; i++)
        {
            byte[]? v = await db.GetAsync(Key(i));
            Assert.AreEqual(i % 2 == 0 ? BigValue(i, "big") : "small" + i, S(v));
        }
        // MultiGet (sync and async)
        using (var batch = new MultiGetBatch())
        {
            for (int i = 0; i < 50; i++) batch.Add(Key(i));
            db.MultiGet(batch);
            for (int i = 0; i < 50; i++) Assert.AreEqual(i % 2 == 0 ? BigValue(i, "big") : "small" + i, S(batch.GetValue(i).ToArray()));
        }
        using (var batch = new MultiGetBatch())
        {
            for (int i = 49; i >= 0; i--) batch.Add(Key(i));
            await db.MultiGetAsync(batch);
            for (int i = 0; i < 50; i++) Assert.AreEqual((49 - i) % 2 == 0 ? BigValue(49 - i, "big") : "small" + (49 - i), S(batch.GetValue(i).ToArray()));
        }
        // Forward / backward iteration
        using (Iterator it = db.NewIterator())
        {
            int n = 0;
            for (it.SeekToFirst(); it.Valid; it.Next(), n++)
            {
                Assert.AreEqual(n % 2 == 0 ? BigValue(n, "big") : "small" + n, S(it.Value.ToArray()));
            }
            Assert.AreEqual(50, n);
            for (it.SeekToLast(); it.Valid; it.Prev())
            {
                n--;
                Assert.AreEqual(S(Key(n)), S(it.Key.ToArray()));
                Assert.AreEqual(n % 2 == 0 ? BigValue(n, "big") : "small" + n, S(it.Value.ToArray()));
            }
            Assert.AreEqual(0, n);
        }
        // Async scan
        int count = 0;
        using Iterator scan = db.NewIterator();
        await foreach (KeyValue kv in scan.ScanAsync())
        {
            Assert.AreEqual(count % 2 == 0 ? BigValue(count, "big") : "small" + count, S(kv.Value.ToArray()));
            count++;
        }
        Assert.AreEqual(50, count);
    }

    [TestMethod]
    public void MergeOnTopOfBlob()
    {
        // db_blob_basic_test.cc Properties / MergeBlobWithBase: merge operands applied to a blob base value in Get,
        // iterators and compaction.
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cf = BlobOptions();
        cf.MergeOperator = new StringAppendMergeOperator();
        using RocksDatabase db = Open(fs, cf);
        db.Put(B("k1"), B("base1"));
        db.Put(B("k2"), B("base2"));
        db.Flush();
        db.Merge(B("k1"), B("m1"));
        db.Merge(B("k2"), B("m2"));
        Assert.AreEqual("base1,m1", GetString(db, "k1"));
        using (Iterator it = db.NewIterator())
        {
            it.SeekToFirst();
            Assert.AreEqual("base1,m1", S(it.Value.ToArray()));
            it.SeekToLast();
            Assert.AreEqual("base2,m2", S(it.Value.ToArray()));
        }
        db.Flush();
        db.CompactRange();
        Assert.AreEqual("base1,m1", GetString(db, "k1"));
        Assert.AreEqual("base2,m2", GetString(db, "k2"));
    }

    [TestMethod]
    public void OverwritesProduceGarbageAndGcDeletesOldFiles()
    {
        // db_blob_compaction_test.cc CompactionReadaheadGarbageCollection / db_blob_basic_test GC: relocation of the
        // oldest blob files makes them obsolete.
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cf = BlobOptions();
        cf.EnableBlobGarbageCollection = true;
        cf.BlobGarbageCollectionAgeCutoff = 1.0;
        using RocksDatabase db = Open(fs, cf);
        for (int round = 0; round < 3; round++)
        {
            for (int i = 0; i < 40; i++) db.Put(Key(i), B(BigValue(i, "r" + round)));
            db.Flush();
        }
        Assert.AreEqual(3UL, IntProperty(db, DbProperties.NumBlobFiles));
        Assert.AreEqual(3, BlobFilesOnDisk(fs).Count);
        db.CompactRange();
        // All blobs relocated into one new blob file; the three old ones are gone.
        Assert.AreEqual(1UL, IntProperty(db, DbProperties.NumBlobFiles));
        List<ulong> onDisk = BlobFilesOnDisk(fs);
        Assert.AreEqual(1, onDisk.Count);
        Assert.AreEqual(0UL, IntProperty(db, DbProperties.LiveBlobFileGarbageSize));
        for (int i = 0; i < 40; i++) Assert.AreEqual(BigValue(i, "r2"), GetString(db, S(Key(i))!));
    }

    [TestMethod]
    public void CompactionWithoutGcRecordsGarbage()
    {
        // db_blob_compaction_test.cc: garbage accounting (BlobGarbageMeter) without relocation.
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cf = BlobOptions();
        using RocksDatabase db = Open(fs, cf);
        for (int i = 0; i < 20; i++) db.Put(Key(i), B(BigValue(i, "a")));
        db.Flush();
        for (int i = 0; i < 10; i++) db.Put(Key(i), B(BigValue(i, "b")));
        db.Flush();
        db.CompactRange();
        // file 1: 10 of 20 blobs garbage; file 2: none.
        Assert.AreEqual(2UL, IntProperty(db, DbProperties.NumBlobFiles));
        ulong garbage = IntProperty(db, DbProperties.LiveBlobFileGarbageSize);
        ulong expected = 0;
        for (int i = 0; i < 10; i++) expected += (ulong)(32 + Key(i).Length + BigValue(i, "a").Length);
        Assert.AreEqual(expected, garbage);
        // Overwrite the rest: file 1 becomes all garbage and is dropped.
        for (int i = 10; i < 20; i++) db.Put(Key(i), B(BigValue(i, "c")));
        db.Flush();
        db.CompactRange();
        Assert.AreEqual(2UL, IntProperty(db, DbProperties.NumBlobFiles));
        Assert.AreEqual(2, BlobFilesOnDisk(fs).Count);
        for (int i = 0; i < 20; i++) Assert.AreEqual(BigValue(i, i < 10 ? "b" : "c"), GetString(db, S(Key(i))!));
    }

    [TestMethod]
    public void ManualGcPolicyAndAgeCutoff()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cf = BlobOptions();
        using RocksDatabase db = Open(fs, cf);
        for (int round = 0; round < 4; round++)
        {
            // Overlapping key ranges (no trivial move): round r writes the keys with i % 4 == r.
            for (int i = round; i < 40; i += 4) db.Put(Key(i), B(BigValue(i, "x")));
            db.Flush();
        }
        List<ulong> before = BlobFilesOnDisk(fs);
        Assert.AreEqual(4, before.Count);
        // Force GC of the oldest half (cutoff 0.5: files below the 3rd file are relocated).
        db.CompactRange(new CompactRangeOptions { BlobGarbageCollectionPolicy = BlobGarbageCollectionPolicy.Force, BlobGarbageCollectionAgeCutoff = 0.5 }, null, null);
        List<ulong> after = BlobFilesOnDisk(fs);
        Assert.AreEqual(3, after.Count);
        Assert.IsFalse(after.Contains(before[0]));
        Assert.IsFalse(after.Contains(before[1]));
        Assert.IsTrue(after.Contains(before[2]) && after.Contains(before[3]));
        for (int i = 0; i < 40; i++) Assert.AreEqual(BigValue(i, "x"), GetString(db, S(Key(i))!));
    }

    [TestMethod]
    public void ForceThresholdSchedulesBlobGcCompaction()
    {
        // db_blob_compaction_test.cc / version_set_test.cc ComputeFilesMarkedForForcedBlobGC: once the oldest blob files
        // (age cutoff) reach the garbage ratio threshold, the table files linked to the oldest one are compacted.
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cf = BlobOptions();
        cf.DisableAutoCompactions = false;
        cf.Level0FileNumCompactionTrigger = 100;
        cf.EnableBlobGarbageCollection = true;
        cf.BlobGarbageCollectionAgeCutoff = 0.5;
        cf.BlobGarbageCollectionForceThreshold = 0.4;
        using RocksDatabase db = Open(fs, cf);
        for (int i = 0; i < 100; i++) db.Put(Key(i), B(BigValue(i, "a")));
        db.Flush();
        ulong first = BlobFilesOnDisk(fs)[0];
        for (int i = 0; i < 50; i++) db.Put(Key(i), B(BigValue(i, "b")));
        db.Flush();
        // Merge both files without relocating: the oldest blob file is now about half garbage and its SST is marked.
        db.CompactRange(new CompactRangeOptions { BlobGarbageCollectionPolicy = BlobGarbageCollectionPolicy.Disable }, null, null);
        db.WaitForCompact();
        CollectionAssert.DoesNotContain(BlobFilesOnDisk(fs), first);
        Assert.AreEqual(0UL, IntProperty(db, DbProperties.LiveBlobFileGarbageSize));
        for (int i = 0; i < 100; i++) Assert.AreEqual(BigValue(i, i < 50 ? "b" : "a"), GetString(db, S(Key(i))!));
    }

    private sealed class BlobValueFilter : CompactionFilter
    {
        public int Calls;
        public override string Name => "BlobValueFilter";

        public override CompactionFilterDecision Filter(int level, ReadOnlySpan<byte> key, CompactionFilterValueType valueType,
            ReadOnlySpan<byte> existingValue, IBufferWriter<byte> newValue, IBufferWriter<byte> skipUntil)
        {
            Calls++;
            Assert.AreEqual(CompactionFilterValueType.Value, valueType);
            if (existingValue.StartsWith("drop"u8)) return CompactionFilterDecision.Remove;
            if (existingValue.StartsWith("chg"u8))
            {
                newValue.Write("changed"u8);
                return CompactionFilterDecision.ChangeValue;
            }
            return CompactionFilterDecision.Keep;
        }
    }

    [TestMethod]
    public void CompactionFilterSeesBlobValues()
    {
        // db_blob_compaction_test.cc FilterByKeyLength / FilterByValueLength / FilterByValueChange: the filter sees the
        // blob value (read by the compaction), may remove it or change it (the new value is extracted again if large).
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cf = BlobOptions();
        var filter = new BlobValueFilter();
        cf.CompactionFilter = filter;
        using RocksDatabase db = Open(fs, cf);
        db.Put(B("a"), B("drop-" + new string('x', 100)));
        db.Put(B("b"), B("chg-" + new string('y', 100)));
        db.Put(B("c"), B("keep-" + new string('z', 100)));
        db.Flush();
        db.CompactRange();
        Assert.AreEqual(3, filter.Calls);
        Assert.IsNull(GetString(db, "a"));
        Assert.AreEqual("changed", GetString(db, "b"));
        Assert.AreEqual("keep-" + new string('z', 100), GetString(db, "c"));
    }

    [TestMethod]
    public void RecoveryFlushWritesBlobFiles()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cf = BlobOptions();
        using (RocksDatabase db = Open(fs, cf))
        {
            for (int i = 0; i < 30; i++) db.Put(Key(i), B(BigValue(i, "w")));
        }
        Assert.AreEqual(0, BlobFilesOnDisk(fs).Count);
        using (RocksDatabase db = Open(fs, cf))
        {
            Assert.AreEqual(1UL, IntProperty(db, DbProperties.NumBlobFiles));
            for (int i = 0; i < 30; i++) Assert.AreEqual(BigValue(i, "w"), GetString(db, S(Key(i))!));
        }
    }

    [TestMethod]
    public void BlobFileStartingLevelAndCompressionAndCheckpoint()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cf = BlobOptions();
        cf.BlobFileStartingLevel = 1;
        cf.BlobCompressionType = CompressionType.LZ4;
        using RocksDatabase db = Open(fs, cf);
        for (int i = 0; i < 30; i++)
        {
            db.Put(Key(i), B(BigValue(i, "s")));
            if (i == 15) db.Flush(); // two overlapping L0 files: the compaction is not a trivial move
        }
        db.Put(Key(0), B(BigValue(0, "s")));
        db.Flush();
        Assert.AreEqual(0UL, IntProperty(db, DbProperties.NumBlobFiles)); // L0 output stays inline
        db.CompactRange();
        Assert.AreEqual(1UL, IntProperty(db, DbProperties.NumBlobFiles)); // L1+ output goes to blob files
        for (int i = 0; i < 30; i++) Assert.AreEqual(BigValue(i, "s"), GetString(db, S(Key(i))!));

        List<string> live = db.GetLiveFiles(false, out _);
        ulong blob = BlobFilesOnDisk(fs)[0];
        CollectionAssert.Contains(live, FileNames.BlobFileName("", blob));

        db.CreateCheckpoint("/ckpt");
        Assert.IsTrue(fs.FileExists(FileNames.BlobFileName("/ckpt", blob)));
        using RocksDatabase copy = RocksDatabase.Open("/ckpt", new DbOptions { FileSystem = fs }, cf);
        for (int i = 0; i < 30; i++) Assert.AreEqual(BigValue(i, "s"), GetString(copy, S(Key(i))!));
    }

    [TestMethod]
    public void BlobCacheAndPrepopulate()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cf = BlobOptions();
        cf.BlobCache = Managed.Cache.BlockCache.CreateLru(1 << 20);
        cf.PrepopulateBlobCache = PrepopulateBlobCache.FlushOnly;
        using RocksDatabase db = Open(fs, cf);
        db.Put(B("k"), B("blob-value"));
        db.Flush();
        Assert.IsTrue(IntProperty(db, DbProperties.BlobCacheUsage) > 0);
        Assert.AreEqual((ulong)(1 << 20), IntProperty(db, DbProperties.BlobCacheCapacity));
        // Served from the cache even when no I/O is allowed.
        Assert.AreEqual("blob-value", S(db.Get(new ReadOptions { ReadTier = ReadTier.BlockCacheTier }, B("k"))));

        // Without prepopulation a block-cache-tier read of an uncached blob is Incomplete.
        MemoryFileSystem fs2 = NewFs();
        ColumnFamilyOptions cf2 = BlobOptions();
        cf2.BlobCache = Managed.Cache.BlockCache.CreateLru(1 << 20);
        using RocksDatabase db2 = Open(fs2, cf2);
        db2.Put(B("k"), B("blob-value"));
        db2.Flush();
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db2.Get(new ReadOptions { ReadTier = ReadTier.BlockCacheTier }, B("k")));
        Assert.AreEqual(StatusCode.Incomplete, ex.Code);
        Assert.AreEqual("blob-value", GetString(db2, "k")); // fills the cache
        Assert.AreEqual("blob-value", S(db2.Get(new ReadOptions { ReadTier = ReadTier.BlockCacheTier }, B("k"))));
    }

    [TestMethod]
    public void BlobIndexInMemTableAndCorruptIndexes()
    {
        // db_blob_basic_test.cc GetBlob_CorruptIndex / GetBlob_InlinedTTLIndex / GetBlob_IndexWithInvalidFileNumber
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = Open(fs, BlobOptions());
        using (WriteBatch b = db.CreateWriteBatch())
        {
            b.PutBlobIndex(0, B("corrupt"), [0x1, 0x2]);
            var w = new PooledBufferWriter(16);
            BlobIndex.EncodeInlinedTtl(w, 123, "inlined"u8);
            b.PutBlobIndex(0, B("inlined"), w.WrittenSpan);
            w.Clear();
            BlobIndex.EncodeBlob(w, 999, 1000, 10, CompressionType.None);
            b.PutBlobIndex(0, B("badfile"), w.WrittenSpan);
            w.Dispose();
            db.Write(b);
        }
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => db.Get(B("corrupt"))).Code);
        Assert.AreEqual("inlined", GetString(db, "inlined"));
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.Get(B("badfile")));
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
        StringAssert.Contains(ex.Message, "Invalid blob file number");
    }

    [TestMethod]
    public void SnapshotsSeeOldBlobsAcrossCompaction()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cf = BlobOptions();
        cf.EnableBlobGarbageCollection = true;
        cf.BlobGarbageCollectionAgeCutoff = 1.0;
        using RocksDatabase db = Open(fs, cf);
        db.Put(B("a"), B("old-value"));
        db.Flush();
        using Snapshot snap = db.GetSnapshot();
        db.Put(B("a"), B("new-value"));
        db.Delete(B("a"));
        db.Flush();
        db.CompactRange();
        Assert.IsNull(GetString(db, "a"));
        Assert.AreEqual("old-value", GetString(db, "a", snap));
    }

    [TestMethod]
    public void ConcurrentReadsDuringCompactionWithGc()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cf = BlobOptions();
        cf.EnableBlobGarbageCollection = true;
        cf.BlobGarbageCollectionAgeCutoff = 1.0;
        cf.DisableAutoCompactions = false;
        cf.Level0FileNumCompactionTrigger = 2;
        cf.WriteBufferSize = 64 * 1024;
        using RocksDatabase db = Open(fs, cf);
        using var stop = new CancellationTokenSource();
        var latest = new int[64];
        Exception? failure = null;
        Task reader = Task.Run(() =>
        {
            try
            {
                var rnd = new Random(3);
                while (!stop.IsCancellationRequested)
                {
                    int k = rnd.Next(64);
                    string? v = GetString(db, S(Key(k))!);
                    if (v is not null && !v.StartsWith("r", StringComparison.Ordinal)) throw new InvalidOperationException("bad value " + v);
                    using Iterator it = db.NewIterator();
                    int n = 0;
                    for (it.SeekToFirst(); it.Valid && n < 10; it.Next(), n++) _ = it.Value.Length;
                }
            }
            catch (Exception e)
            {
                failure = e;
            }
        });
        for (int round = 0; round < 40; round++)
        {
            for (int k = 0; k < 64; k++)
            {
                db.Put(Key(k), B(BigValue(k, "r" + round)));
                latest[k] = round;
            }
        }
        db.Flush();
        db.CompactRange();
        stop.Cancel();
        reader.Wait();
        Assert.IsNull(failure, failure?.ToString());
        for (int k = 0; k < 64; k++) Assert.AreEqual(BigValue(k, "r" + latest[k]), GetString(db, S(Key(k))!));
        // Only blob files of the current version remain on disk (obsolete files are purged after the next job).
        db.Put(B("zz"), B("x"));
        db.Flush();
        Assert.AreEqual((int)IntProperty(db, DbProperties.NumBlobFiles), BlobFilesOnDisk(fs).Count);
    }
}
