using System.Globalization;
using System.Text;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Db;
using RocksDb.Managed.Db.Blob;
using RocksDb.Managed.Db.Wide;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;
using RocksDb.Managed.Util;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Blob;

/// <summary>
/// Blob direct write (enable_blob_direct_write). Ports of db/blob/db_blob_direct_write_test.cc (partition spread and
/// custom strategies, close flush with avoid_flush_during_shutdown, generation order of auto flushes, rejected write
/// modes, iterator over mixed values, rotation, boundary values, batches without qualifying values, delete and re-put,
/// snapshot isolation) and db/wide/db_wide_blob_direct_write_test.cc (DirectWriteWideEntityBeforeAndAfterFlush), plus
/// garbage accounting of the flush, GC by compaction, sync writes and OPTIONS persistence.
/// </summary>
[TestClass]
public sealed class BlobDirectWriteTests
{
    private static ColumnFamilyOptions DirectWriteCf(ulong minBlobSize = 32, uint partitions = 1) => new()
    {
        EnableBlobFiles = true,
        EnableBlobDirectWrite = true,
        MinBlobSize = minBlobSize,
        BlobDirectWritePartitions = partitions,
        DisableAutoCompactions = true,
    };

    private static DbOptions DirectWriteDb(IFileSystem fs)
    {
        DbOptions o = NewOptions(fs);
        o.AllowConcurrentMemtableWrite = false;
        return o;
    }

    private static RocksDatabase Open(MemoryFileSystem fs, ColumnFamilyOptions cf) => RocksDatabase.Open(DbPath, DirectWriteDb(fs), cf);

    private static List<ulong> BlobFiles(IFileSystem fs)
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

    /// <summary>Raw values of the memtable/table entries by key and type, read with a raw internal iterator over the WAL batches.</summary>
    private static int CountBlobIndexesInTables(IFileSystem fs)
    {
        int n = 0;
        foreach (string c in fs.GetChildren(DbPath))
        {
            if (!c.EndsWith(".sst", StringComparison.Ordinal)) continue;
            using BlockBasedTableReader reader = BlockBasedTableReader.Open(fs.OpenRandomAccess(DbPath + "/" + c), new TableReaderOptions());
            using BlockBasedTableIterator it = reader.NewIterator();
            for (it.SeekToFirst(); it.Valid; it.Next())
            {
                if ((EntryType)(byte)DbFormat.ExtractFooter(it.Key) == EntryType.BlobIndex) n++;
            }
        }
        return n;
    }

    [TestMethod]
    public void DefaultRoundRobinSpreadsWriteBatchAcrossPartitionsBeforeAndAfterFlush()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cf = DirectWriteCf(32, 4);
        string[] keys = ["blob_key_0", "blob_key_1", "blob_key_2", "blob_key_3", "inline_key"];
        string[] values = [new('a', 256), new('b', 256), new('c', 256), new('d', 256), "tiny"];
        using (RocksDatabase db = Open(fs, cf))
        {
            using (var batch = new WriteBatch())
            {
                for (int i = 0; i < keys.Length; i++) batch.Put(B(keys[i]), B(values[i]));
                batch.Delete(B("deleted_key"));
                db.Write(batch);
            }
            // The WAL/memtable hold blob indexes before any flush: four files, one per partition.
            Assert.AreEqual(4, BlobFiles(fs).Count);
            void Verify()
            {
                for (int i = 0; i < keys.Length; i++) Assert.AreEqual(values[i], GetString(db, keys[i]));
                byte[]?[] mg = db.MultiGet([B(keys[0]), B(keys[1]), B(keys[2]), B(keys[3]), B(keys[4])]);
                for (int i = 0; i < keys.Length; i++) Assert.AreEqual(values[i], S(mg[i]));
            }
            Verify();
            // KeyMayExist cannot return a blob value without I/O.
            for (int i = 0; i < 4; i++)
            {
                var w = new System.Buffers.ArrayBufferWriter<byte>();
                Assert.IsTrue(db.KeyMayExist(null, B(keys[i]), w, out bool found));
                Assert.IsFalse(found);
            }
            db.Flush();
            Verify();
            Assert.AreEqual(4, BlobFiles(fs).Count);
            Assert.AreEqual(4UL, IntProperty(db, DbProperties.NumBlobFiles));
            Assert.AreEqual(4, CountBlobIndexesInTables(fs));
        }
        using (RocksDatabase db = Open(fs, cf))
        {
            for (int i = 0; i < keys.Length; i++) Assert.AreEqual(values[i], GetString(db, keys[i]));
        }
    }

    private sealed class FixedStrategy(uint partition) : BlobDirectWritePartitionStrategy
    {
        public readonly List<(uint N, uint Cf, string Key, int ValueLength)> Calls = new();
        public override string Name => "FixedBlobDirectWritePartitionStrategy";

        public override uint SelectPartition(uint numPartitions, uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            lock (Calls) Calls.Add((numPartitions, columnFamilyId, Encoding.UTF8.GetString(key), value.Length));
            return partition;
        }
    }

    [TestMethod]
    public void CustomPartitionStrategyRoutesWritesAndAppliesModulo()
    {
        foreach (uint raw in new uint[] { 3, 4, 9, uint.MaxValue })
        {
            MemoryFileSystem fs = NewFs();
            var strategy = new FixedStrategy(raw);
            ColumnFamilyOptions cf = DirectWriteCf(32, 4);
            cf.BlobFileSize = 1 << 20;
            cf.BlobDirectWritePartitionStrategy = strategy;
            using RocksDatabase db = Open(fs, cf);
            using (var batch = new WriteBatch())
            {
                for (int i = 0; i < 3; i++) batch.Put(B("k" + i), B(new string((char)('a' + i), 128)));
                batch.Put(B("inline"), B("tiny"));
                db.Write(batch);
            }
            Assert.AreEqual(1, BlobFiles(fs).Count, "raw " + raw);
            db.Flush();
            Assert.AreEqual(1, BlobFiles(fs).Count, "raw " + raw);
            // Called once per separated value, with the original value.
            Assert.AreEqual(3, strategy.Calls.Count);
            Assert.IsTrue(strategy.Calls.TrueForAll(static c => c.N == 4 && c.Cf == 0 && c.ValueLength == 128));
            for (int i = 0; i < 3; i++) Assert.AreEqual(new string((char)('a' + i), 128), GetString(db, "k" + i));
        }
    }

    [TestMethod]
    public void CloseFlushesWhenShutdownFlushIsDisabled()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cf = DirectWriteCf(32, 4);
        DbOptions o = DirectWriteDb(fs);
        o.AvoidFlushDuringShutdown = true;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o, cf))
        {
            for (int i = 0; i < 4; i++) db.Put(B("close_blob_key_" + i), B(new string((char)('a' + i), 128)));
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o, cf))
        {
            for (int i = 0; i < 4; i++) Assert.AreEqual(new string((char)('a' + i), 128), GetString(db, "close_blob_key_" + i));
            Assert.AreEqual(4UL, IntProperty(db, DbProperties.NumBlobFiles));
        }
    }

    [TestMethod]
    public void RejectsUnsupportedConfigurationsAndSanitizes()
    {
        static void AssertRejected(DbOptions o, ColumnFamilyOptions cf, string expected)
        {
            var e = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, o, cf).Dispose());
            Assert.AreEqual(StatusCode.NotSupported, e.Code);
            StringAssert.Contains(e.Message, expected);
        }

        AssertRejected(NewOptions(NewFs()), DirectWriteCf(), "concurrent memtable writes"); // allow_concurrent_memtable_write defaults to true
        DbOptions o = DirectWriteDb(NewFs());
        o.EnablePipelinedWrite = true;
        AssertRejected(o, DirectWriteCf(), "pipelined writes");
        o = DirectWriteDb(NewFs());
        o.UnorderedWrite = true;
        AssertRejected(o, DirectWriteCf(), "unordered writes");
        o = DirectWriteDb(NewFs());
        o.TwoWriteQueues = true;
        AssertRejected(o, DirectWriteCf(), "two write queues");
        ColumnFamilyOptions cf = DirectWriteCf();
        cf.ExperimentalMempurgeThreshold = 1.0;
        AssertRejected(DirectWriteDb(NewFs()), cf, "MemPurge");
        cf = DirectWriteCf();
        cf.Comparator = Comparator.BytewiseWithU64Ts;
        AssertRejected(DirectWriteDb(NewFs()), cf, "user-defined timestamps");

        // Without enable_blob_files the option is sanitized away (values stay inline until flush).
        MemoryFileSystem fs = NewFs();
        cf = DirectWriteCf();
        cf.EnableBlobFiles = false;
        cf.BlobDirectWritePartitions = 0;
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cf);
        db.Put(B("k"), B(new string('v', 100)));
        Assert.AreEqual(0, BlobFiles(fs).Count);
        Assert.IsNull(db.DefaultColumnFamily.Data.BlobDirectWrite);
        Assert.IsFalse(db.DefaultColumnFamily.Data.Options.EnableBlobDirectWrite);
        Assert.AreEqual(1U, db.DefaultColumnFamily.Data.Options.BlobDirectWritePartitions);
    }

    [TestMethod]
    public void IteratorMixedValuesRotationAndBoundaries()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cf = DirectWriteCf(20);
        cf.BlobFileSize = 300; // DirectWriteBlobFileRotationSinglePartition: a new file every couple of blobs
        using RocksDatabase db = Open(fs, cf);
        var expected = new SortedDictionary<string, string>(StringComparer.Ordinal)
        {
            ["a_small"] = "tiny",
            ["b_large"] = new string('B', 80),
            ["below"] = new string('b', 19),
            ["c_small"] = "mini",
            ["d_large"] = new string('D', 80),
            ["exact"] = new string('e', 20),
            ["z_above"] = new string('a', 21),
        };
        foreach ((string k, string v) in expected) db.Put(B(k), B(v));
        for (int i = 0; i < 10; i++) expected["rot" + i] = new string((char)('0' + i), 100);
        for (int i = 0; i < 10; i++) db.Put(B("rot" + i), B(expected["rot" + i]));
        Assert.IsTrue(BlobFiles(fs).Count >= 5, "rotation: " + BlobFiles(fs).Count);

        void Verify(string phase)
        {
            using Iterator it = db.NewIterator();
            var fwd = new StringBuilder();
            foreach ((string k, string v) in expected) fwd.Append(k).Append('=').Append(v).Append(';');
            var got = new StringBuilder();
            for (it.SeekToFirst(); it.Valid; it.Next()) got.Append(S(it.Key.ToArray())).Append('=').Append(S(it.Value.ToArray())).Append(';');
            Assert.AreEqual(fwd.ToString(), got.ToString(), phase);
            got.Clear();
            for (it.SeekToLast(); it.Valid; it.Prev()) got.Insert(0, S(it.Key.ToArray()) + "=" + S(it.Value.ToArray()) + ";");
            Assert.AreEqual(fwd.ToString(), got.ToString(), phase + " reverse");
        }

        Verify("memtable");
        int filesBefore = BlobFiles(fs).Count;
        db.Flush();
        Verify("flushed");
        Assert.AreEqual(filesBefore, BlobFiles(fs).Count);
        // below (19 < 20) and the small values stay inline.
        Assert.AreEqual(14, CountBlobIndexesInTables(fs));
    }

    [TestMethod]
    public void BatchWithoutQualifyingValuesDeleteReputAndSnapshots()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = Open(fs, DirectWriteCf(1024)))
        {
            using (var batch = new WriteBatch())
            {
                batch.Put(B("k1"), B("small_v1")).Put(B("k2"), B("small_v2")).Delete(B("k3")).SingleDelete(B("k4"));
                db.Write(batch);
            }
            Assert.AreEqual(0, BlobFiles(fs).Count);
            db.Flush();
            Assert.AreEqual("small_v1", GetString(db, "k1"));
            Assert.AreEqual(0, BlobFiles(fs).Count);
        }

        fs = NewFs();
        using RocksDatabase db2 = Open(fs, DirectWriteCf());
        string first = new('1', 96), second = new('2', 128);
        db2.Put(B("reput_key"), B(first));
        Assert.AreEqual(first, GetString(db2, "reput_key"));
        db2.Delete(B("reput_key"));
        Assert.IsNull(GetString(db2, "reput_key"));
        db2.Put(B("reput_key"), B(second));
        Assert.AreEqual(second, GetString(db2, "reput_key"));

        db2.Put(B("snap_key"), B(first));
        using Snapshot snap = db2.GetSnapshot();
        db2.Put(B("snap_key"), B(second));
        db2.Put(B("snap_new_key"), B(second));
        Assert.AreEqual(second, GetString(db2, "snap_key"));
        Assert.AreEqual(first, GetString(db2, "snap_key", snap));
        Assert.IsNull(GetString(db2, "snap_new_key", snap));
        db2.Flush();
        Assert.AreEqual(second, GetString(db2, "reput_key"));
        Assert.AreEqual(first, GetString(db2, "snap_key", snap));
        Assert.AreEqual(second, GetString(db2, "snap_key"));
    }

    [TestMethod]
    public void FlushRegistersGarbageOfDroppedVersionsAndCompactionCollectsIt()
    {
        // Fix in 11.2: "blob garbage accounting for blob direct-write flushes so flush-time filtering and overwrite
        // elision correctly register obsolete blob bytes". Overwrites within one memtable leave unreferenced blobs.
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cf = DirectWriteCf(32);
        using RocksDatabase db = Open(fs, cf);
        for (int round = 0; round < 3; round++)
        {
            for (int i = 0; i < 10; i++) db.Put(Key(i), B(round.ToString(CultureInfo.InvariantCulture) + new string('v', 100)));
        }
        db.Flush();
        Assert.AreEqual(1UL, IntProperty(db, DbProperties.NumBlobFiles));
        ulong total = IntProperty(db, DbProperties.LiveBlobFileSize);
        ulong garbage = IntProperty(db, DbProperties.LiveBlobFileGarbageSize);
        // Two of three versions of every key are garbage.
        Assert.IsTrue(garbage * 3 >= (total - BlobLogFormat.HeaderSize - BlobLogFormat.FooterSize) * 2 - 10, garbage + " of " + total);
        for (int i = 0; i < 10; i++) Assert.AreEqual("2" + new string('v', 100), GetString(db, S(Key(i))!));

        // Compaction with forced GC relocates the live blobs; the direct-write file becomes obsolete and is deleted.
        ulong before = BlobFiles(fs)[0];
        db.CompactRange(new CompactRangeOptions
        {
            BlobGarbageCollectionPolicy = BlobGarbageCollectionPolicy.Force,
            BlobGarbageCollectionAgeCutoff = 1.0,
            BottommostLevelCompaction = BottommostLevelCompaction.Force,
        }, null, null);
        Assert.IsFalse(BlobFiles(fs).Contains(before));
        Assert.AreEqual(0UL, IntProperty(db, DbProperties.LiveBlobFileGarbageSize));
        for (int i = 0; i < 10; i++) Assert.AreEqual("2" + new string('v', 100), GetString(db, S(Key(i))!));
    }

    [TestMethod]
    public void AutoFlushPreservesGenerationOrderAndOldestBlobFile()
    {
        // DirectWriteAutoFlushPreservesBlobGenerationOrder: every flush registers the files of its own memtable.
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cf = DirectWriteCf(32);
        cf.WriteBufferSize = 64 << 10;
        cf.MaxWriteBufferNumber = 6;
        // As upstream's test: no automatic compactions. With the default L0 trigger (4) the ~6 flushes start an L0
        // compaction whose input set depends on when its thread runs relative to the next flushes (a late start takes
        // the 5th flush too and leaves a single L0 table), so the L0 table count was timing dependent.
        cf.DisableAutoCompactions = true;
        using RocksDatabase db = Open(fs, cf);
        for (int i = 0; i < 2000; i++)
        {
            db.Put(Key(i), B(new string((char)('a' + i % 26), 120)));
            // Let every auto flush finish before the next memtable fills up: a flush job picks all immutable memtables,
            // so a late flush thread (a loaded machine) would otherwise write several memtables into one table.
            if (IntProperty(db, DbProperties.NumImmutableMemTable) > 0) db.WaitForCompact();
        }
        db.Flush();
        db.WaitForCompact();
        VersionStorageInfo vs = db.DefaultColumnFamily.Data.Current.Storage;
        FileMetaData[] l0 = vs.LevelFilesArray(0);
        Assert.IsTrue(l0.Length > 1, "flushes: " + l0.Length);
        var oldest = new HashSet<ulong>();
        foreach (FileMetaData f in l0)
        {
            Assert.IsNotNull(vs.GetBlobFileMetaData(f.OldestBlobFileNumber), "table " + f.FileNumber);
            oldest.Add(f.OldestBlobFileNumber);
        }
        Assert.AreEqual(l0.Length, oldest.Count);
        Assert.AreEqual((ulong)BlobFiles(fs).Count, IntProperty(db, DbProperties.NumBlobFiles));
        for (int i = 0; i < 2000; i += 37) Assert.AreEqual(new string((char)('a' + i % 26), 120), GetString(db, S(Key(i))!));
    }

    [TestMethod]
    public async Task SyncWritesCompressionAsyncReadsAndOptionsFile()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cf = DirectWriteCf(32, 2);
        cf.BlobCompressionType = CompressionType.LZ4;
        using (RocksDatabase db = Open(fs, cf))
        {
            var sync = new WriteOptions { Sync = true };
            for (int i = 0; i < 20; i++) await db.PutAsync(sync, Key(i), B(new string('z', 200 + i)));
            for (int i = 0; i < 20; i++) Assert.AreEqual(new string('z', 200 + i), S(await db.GetAsync(Key(i))));
            using (Iterator it = db.NewIterator())
            {
                it.SeekToFirst();
                await it.PrepareValueAsync();
                Assert.AreEqual(200, it.Value.Length);
            }
        }
        // OPTIONS persistence of the typed options.
        string optionsFile = fs.GetChildren(DbPath).Where(static n => n.StartsWith("OPTIONS-", StringComparison.Ordinal)).Order(StringComparer.Ordinal).Last();
        string text = Encoding.UTF8.GetString(FileNames.ReadAllBytes(fs, DbPath + "/" + optionsFile));
        StringAssert.Contains(text, "enable_blob_direct_write=true");
        StringAssert.Contains(text, "blob_direct_write_partitions=2");
        StringAssert.Contains(text, "blob_file_writable_file_max_buffer_size=0");
        OptionsFile parsed = OptionsFile.Parse(text, preserveUnknownOptions: false);
        ColumnFamilyOptions back = parsed.ColumnFamilies[0].Options;
        Assert.IsTrue(back.EnableBlobDirectWrite);
        Assert.AreEqual(2U, back.BlobDirectWritePartitions);
        using (RocksDatabase db = Open(fs, cf))
        {
            for (int i = 0; i < 20; i++) Assert.AreEqual(new string('z', 200 + i), GetString(db, S(Key(i))!));
        }
    }

    [TestMethod]
    public void WideEntitiesBeforeAndAfterFlush()
    {
        // db_wide_blob_direct_write_test.cc DirectWriteWideEntityBeforeAndAfterFlush: PutEntity (DB and WriteBatch)
        // writes its large columns to blob files before the flush.
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cf = DirectWriteCf(64);
        WideColumn[] c1 = [new(B(""), B(new string('a', 128))), new(B("ttl"), B("00000001")), new(B("type"), B("cold"))];
        WideColumn[] c2 = [new(B(""), B(new string('b', 160))), new(B("ttl"), B("00000002")), new(B("type"), B("hot"))];
        using (RocksDatabase db = Open(fs, cf))
        {
            db.PutEntity(B("entity_key_a"), c1);
            using (var batch = new WriteBatch())
            {
                batch.PutEntity(B("entity_key_b"), c2);
                db.Write(batch);
            }
            Assert.AreEqual(1, BlobFiles(fs).Count);
            void Verify(string phase)
            {
                Assert.AreEqual(new string('a', 128), GetString(db, "entity_key_a"), phase);
                Assert.AreEqual(new string('b', 160), GetString(db, "entity_key_b"), phase);
                Assert.AreEqual("=" + new string('a', 128) + ";ttl=00000001;type=cold;", Describe(db.GetEntity(B("entity_key_a"))!), phase);
                Assert.AreEqual("=" + new string('b', 160) + ";ttl=00000002;type=hot;", Describe(db.GetEntity(B("entity_key_b"))!), phase);
                using Iterator it = db.NewIterator();
                it.SeekToFirst();
                Assert.AreEqual("=" + new string('a', 128) + ";ttl=00000001;type=cold;", Describe(it.Columns), phase);
                it.Next();
                Assert.AreEqual(new string('b', 160), S(it.Value.ToArray()), phase);
            }
            Verify("memtable");
            db.Flush();
            Assert.AreEqual(1UL, IntProperty(db, DbProperties.NumBlobFiles));
            BlobFileMetaData meta = db.DefaultColumnFamily.Data.Current.Storage.BlobFiles[0];
            Assert.AreEqual(2UL, meta.Addition.TotalBlobCount); // both default columns, nothing else
            Verify("flushed");
        }
        using (RocksDatabase db = Open(fs, cf))
        {
            Assert.AreEqual("=" + new string('b', 160) + ";ttl=00000002;type=hot;", Describe(db.GetEntity(B("entity_key_b"))!));
        }
    }

    private static string Describe(WideColumns c)
    {
        var sb = new StringBuilder();
        for (int i = 0; i < c.Count; i++) sb.Append(Encoding.UTF8.GetString(c.GetName(i))).Append('=').Append(Encoding.UTF8.GetString(c.GetValue(i))).Append(';');
        return sb.ToString();
    }

    [TestMethod]
    public void MergeBatchesAndTransactionsAreNotSeparated()
    {
        // Upstream skips batches with merge operands (MaybeTransformBatchForBlobDirectWrite); their large values are
        // separated by the flush like without direct write.
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cf = DirectWriteCf(32);
        cf.MergeOperator = new RocksDb.Managed.Tests.Db.StringAppendMergeOperator();
        using RocksDatabase db = Open(fs, cf);
        using (var batch = new WriteBatch())
        {
            batch.Put(B("p"), B(new string('p', 100)));
            batch.Merge(B("p"), B("m"));
            db.Write(batch);
        }
        Assert.AreEqual(0, BlobFiles(fs).Count);
        Assert.AreEqual(new string('p', 100) + ",m", GetString(db, "p"));
        db.Flush();
        Assert.AreEqual(new string('p', 100) + ",m", GetString(db, "p"));
    }
}
