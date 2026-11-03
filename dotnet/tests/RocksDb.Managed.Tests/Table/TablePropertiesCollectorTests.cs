using RocksDb.Managed.Table;
using static RocksDb.Managed.Tests.Db.DbTestUtil;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.Tests.Table;

/// <summary>
/// User table properties collectors (include/rocksdb/table_properties.h): invoked by flush, compaction and SstFileWriter,
/// properties readable through GetPropertiesOfAllTables, NeedCompact marking files for compaction. Ports of the
/// relevant parts of db/table_properties_collector_test.cc, db_properties_test.cc and
/// compact_on_deletion_collector_test.cc.
/// </summary>
[TestClass]
public sealed class TablePropertiesCollectorTests
{
    /// <summary>Counts entries by type and blocks; records the context it was created with.</summary>
    private sealed class CountingFactory(bool needCompact = false, bool decline = false) : TablePropertiesCollectorFactory
    {
        public readonly List<TablePropertiesCollectorContext> Contexts = new();
        public override string Name => "CountingFactory";

        public override TablePropertiesCollector? CreateTablePropertiesCollector(TablePropertiesCollectorContext context)
        {
            lock (Contexts) Contexts.Add(context);
            return decline ? null : new Collector(needCompact);
        }

        private sealed class Collector(bool needCompact) : TablePropertiesCollector
        {
            private ulong _puts, _deletes, _rangeDeletes, _blocks, _maxSeq, _lastFileSize;
            private bool _sorted = true;
            private byte[]? _last;
            public override string Name => "Counting";
            public override bool NeedCompact => needCompact;

            public override void AddUserKey(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, TableEntryType type, ulong sequence, ulong fileSize)
            {
                if (type == TableEntryType.Put) _puts++;
                else if (type == TableEntryType.Delete) _deletes++;
                else if (type == TableEntryType.RangeDeletion) _rangeDeletes++;
                if (type != TableEntryType.RangeDeletion)
                {
                    if (_last is not null && key.SequenceCompareTo(_last) < 0) _sorted = false;
                    _last = key.ToArray();
                }
                _maxSeq = Math.Max(_maxSeq, sequence);
                if (fileSize < _lastFileSize) _sorted = false;
                _lastFileSize = fileSize;
            }

            public override void BlockAdd(ulong blockUncompressedBytes, ulong blockCompressedBytesFast, ulong blockCompressedBytesSlow) => _blocks++;

            public override void Finish(IDictionary<string, byte[]> properties)
            {
                properties["test.puts"] = BitConverter.GetBytes(_puts);
                properties["test.deletes"] = BitConverter.GetBytes(_deletes);
                properties["test.range_deletes"] = BitConverter.GetBytes(_rangeDeletes);
                properties["test.blocks"] = BitConverter.GetBytes(_blocks);
                properties["test.max_seq"] = BitConverter.GetBytes(_maxSeq);
                properties["test.sorted"] = [(byte)(_sorted ? 1 : 0)];
            }
        }
    }

    /// <summary>A collector that fails in Finish: its properties are dropped, the table is still written.</summary>
    private sealed class FailingFactory : TablePropertiesCollectorFactory
    {
        public override string Name => "FailingFactory";
        public override TablePropertiesCollector CreateTablePropertiesCollector(TablePropertiesCollectorContext context) => new Failing();

        private sealed class Failing : TablePropertiesCollector
        {
            public override string Name => "Failing";
            public override void AddUserKey(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, TableEntryType type, ulong sequence, ulong fileSize) =>
                throw new InvalidOperationException("add");
            public override void Finish(IDictionary<string, byte[]> properties)
            {
                properties["test.failing"] = [1];
                throw new InvalidOperationException("finish");
            }
        }
    }

    private static ulong U(TableProperties p, string name) => BitConverter.ToUInt64(p.UserCollectedProperties[name]);

    [TestMethod]
    public void FlushAndCompactionRunCollectors()
    {
        var fs = NewFs();
        var counting = new CountingFactory();
        var cfo = new ColumnFamilyOptions
        {
            DisableAutoCompactions = true,
            TablePropertiesCollectorFactories = { counting, new CountingFactory(decline: true), new FailingFactory() },
        };
        cfo.TableOptions.BlockSize = 512;
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        for (int i = 0; i < 500; i++) db.Put(Key(i), Value(i, 0));
        for (int i = 0; i < 500; i += 5) db.Delete(Key(i));
        db.DeleteRange(Key(900), Key(950));
        db.Flush();

        Dictionary<string, TableProperties> all = db.GetPropertiesOfAllTables();
        Assert.AreEqual(1, all.Count);
        TableProperties p = all.Values.Single();
        Assert.AreEqual("[CountingFactory,CountingFactory,FailingFactory]", p.PropertyCollectorsNames);
        Assert.AreEqual(400UL, U(p, "test.puts"));
        Assert.AreEqual(100UL, U(p, "test.deletes"));
        Assert.AreEqual(1UL, U(p, "test.range_deletes"));
        Assert.AreEqual(p.NumDataBlocks, U(p, "test.blocks"));
        Assert.AreEqual(601UL, U(p, "test.max_seq"));
        CollectionAssert.AreEqual(new byte[] { 1 }, p.UserCollectedProperties["test.sorted"]);
        Assert.IsFalse(p.UserCollectedProperties.ContainsKey("test.failing"));
        // Flush context: level 0, num_levels of the column family, the column family id.
        TablePropertiesCollectorContext ctx = counting.Contexts.Single();
        Assert.AreEqual(0, ctx.LevelAtCreation);
        Assert.AreEqual(7, ctx.NumLevels);
        Assert.AreEqual(0u, ctx.ColumnFamilyId);

        // A second overlapping L0 file so CompactRange rewrites (rather than moves) the data.
        db.Put(Key(1), Value(1, 1));
        db.Flush();
        db.CompactRange();
        p = db.GetPropertiesOfAllTables().Values.Single();
        Assert.AreEqual(400UL, U(p, "test.puts"));
        Assert.AreEqual(0UL, U(p, "test.deletes")); // dropped at the bottommost level
        Assert.AreEqual(6, counting.Contexts[^1].LevelAtCreation);
    }

    [TestMethod]
    public void SstFileWriterRunsCollectors()
    {
        var fs = NewFs();
        var counting = new CountingFactory();
        var cfo = new ColumnFamilyOptions { TablePropertiesCollectorFactories = { counting } };
        using (var w = new SstFileWriter(cfo, fs))
        {
            w.Open("/ext.sst");
            for (int i = 0; i < 100; i++) w.Put(Key(i), Value(i, 1));
            w.Finish();
        }
        Assert.AreEqual(-1, counting.Contexts.Single().LevelAtCreation);
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        db.IngestExternalFiles(["/ext.sst"], new IngestExternalFileOptions());
        TableProperties p = db.GetPropertiesOfAllTables().Values.Single();
        Assert.AreEqual(100UL, U(p, "test.puts"));
        Assert.AreEqual("[CountingFactory]", p.PropertyCollectorsNames);
    }

    /// <summary>NeedCompact marks the flushed file; the leveled picker then compacts it although L0 is below its trigger.</summary>
    [TestMethod]
    public void NeedCompactMarksFileForCompaction()
    {
        var fs = NewFs();
        var cfo = new ColumnFamilyOptions
        {
            TablePropertiesCollectorFactories = { new CompactOnDeletionCollectorFactory(100, 10, 0, 0) },
            Level0FileNumCompactionTrigger = 10,
        };
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        for (int i = 0; i < 200; i++) db.Put(Key(i), Value(i, 0));
        db.Flush();
        db.WaitForCompact();
        Assert.IsTrue(db.GetLiveFilesMetadata().All(f => f.Level == 0), "no deletions: stays in L0");
        for (int i = 0; i < 200; i++)
        {
            if (i % 2 == 0) db.Delete(Key(i));
            else db.Put(Key(i), Value(i, 1));
        }
        db.Flush();
        db.WaitForCompact();
        Assert.IsTrue(db.GetLiveFilesMetadata().All(f => f.Level > 0), "the marked file was compacted out of L0");
        Assert.AreEqual("[CompactOnDeletionCollector]", db.GetPropertiesOfAllTables().Values.First().PropertyCollectorsNames);
    }

    /// <summary>Port of compact_on_deletion_collector_test.cc: sliding window and deletion ratio triggers.</summary>
    [TestMethod]
    public void CompactOnDeletionCollectorTriggers()
    {
        static bool Run(ulong window, ulong trigger, double ratio, ulong minFileSize, Func<int, TableEntryType> type, int count, ulong fileSize = 1000)
        {
            TablePropertiesCollector c = new CompactOnDeletionCollectorFactory(window, trigger, ratio, minFileSize)
                .CreateTablePropertiesCollector(new TablePropertiesCollectorContext(0, 0, 7, ulong.MaxValue))!;
            for (int i = 0; i < count; i++) c.AddUserKey("k"u8, default, type(i), 0, fileSize);
            c.Finish(new Dictionary<string, byte[]>());
            return c.NeedCompact;
        }
        // 10 deletions inside a window of 128 entries.
        Assert.IsTrue(Run(128, 10, 0, 0, i => i % 12 == 0 ? TableEntryType.Delete : TableEntryType.Put, 200));
        // Deletions spread thinner than the window allows.
        Assert.IsFalse(Run(128, 10, 0, 0, i => i % 50 == 0 ? TableEntryType.Delete : TableEntryType.Put, 2000));
        // Single deletions and deletions with timestamps count; merges do not.
        Assert.IsTrue(Run(128, 3, 0, 0, i => i < 3 ? TableEntryType.SingleDelete : TableEntryType.Put, 10));
        Assert.IsFalse(Run(128, 3, 0, 0, i => TableEntryType.Merge, 100));
        // Ratio trigger (window disabled).
        Assert.IsTrue(Run(0, 0, 0.5, 0, i => i % 2 == 0 ? TableEntryType.Delete : TableEntryType.Put, 100));
        Assert.IsFalse(Run(0, 0, 0.5, 0, i => i % 3 == 0 ? TableEntryType.Delete : TableEntryType.Put, 100));
        // Minimum file size.
        Assert.IsFalse(Run(128, 1, 0, 2000, i => TableEntryType.Delete, 10));
        Assert.IsFalse(Run(0, 0, 0, 0, i => TableEntryType.Delete, 10), "disabled");
        Assert.AreEqual(TableEntryType.TimedPut, TablePropertiesCollectors.GetEntryType(EntryType.ValuePreferredSeqno));
        Assert.AreEqual(TableEntryType.Other, TablePropertiesCollectors.GetEntryType(EntryType.ColumnFamilyValue));
    }

    /// <summary>Port of compact_for_tiering_collector_test.cc (NotEnabled, TieringDisabled, LastLevelFile, CollectorEnabled, TimedPutEntries).</summary>
    [TestMethod]
    public void CompactForTieringCollectorPort()
    {
        const string Prop = CompactForTieringCollectorFactory.NumEligibleLastLevelEntriesPropertyName;
        // NotEnabled: ratio 0 creates no collector.
        Assert.IsNull(new CompactForTieringCollectorFactory(0).CreateTablePropertiesCollector(new TablePropertiesCollectorContext(1, 1, 6, 50)));
        foreach (double ratio in new[] { 0.0, 0.1, 1.0, 1.5 })
        {
            // TieringDisabled and LastLevelFile: no collector.
            Assert.IsNull(new CompactForTieringCollectorFactory(ratio).CreateTablePropertiesCollector(new TablePropertiesCollectorContext(1, 1, 6, RocksDb.Managed.Db.DbFormat.MaxSequenceNumber)));
            Assert.IsNull(new CompactForTieringCollectorFactory(ratio).CreateTablePropertiesCollector(new TablePropertiesCollectorContext(1, 5, 6, 50)));
        }
        // CollectorEnabled: seqnos 0..99 with threshold 50 -> 50 eligible entries.
        foreach (double ratio in new[] { 0.1, 0.33333333, 0.5, 1.0, 1.5 })
        {
            TablePropertiesCollector c = new CompactForTieringCollectorFactory(ratio).CreateTablePropertiesCollector(new TablePropertiesCollectorContext(1, 1, 6, 50))!;
            Assert.AreEqual(CompactForTieringCollectorFactory.ClassName, c.Name);
            for (ulong i = 0; i < 100; i++)
            {
                c.AddUserKey("hello"u8, "rocksdb"u8, TableEntryType.Put, i, 0);
                Assert.IsFalse(c.NeedCompact);
            }
            var props = new Dictionary<string, byte[]>();
            c.Finish(props);
            Assert.AreEqual("50", System.Text.Encoding.ASCII.GetString(props[Prop]));
            Assert.AreEqual("50", c.GetReadableProperties()[Prop]);
            Assert.AreEqual(ratio <= 0.5, c.NeedCompact, "ratio " + ratio);
        }
        // TimedPutEntries: the packed preferred seqno decides, not the entry's seqno.
        TablePropertiesCollector t = new CompactForTieringCollectorFactory(0.1).CreateTablePropertiesCollector(new TablePropertiesCollectorContext(1, 1, 6, 50))!;
        for (ulong i = 0; i < 100; i++)
        {
            byte[] packed = new byte[7 + 8];
            "rocksdb"u8.CopyTo(packed);
            RocksDb.Managed.Util.Coding.EncodeFixed64(packed.AsSpan(7), i);
            t.AddUserKey("hello"u8, packed, TableEntryType.TimedPut, 0, 0);
        }
        var tp = new Dictionary<string, byte[]>();
        t.Finish(tp);
        Assert.AreEqual("50", System.Text.Encoding.ASCII.GetString(tp[Prop]));
        Assert.IsTrue(t.NeedCompact);
        // No eligible entries: no property.
        TablePropertiesCollector none = new CompactForTieringCollectorFactory(0.5).CreateTablePropertiesCollector(new TablePropertiesCollectorContext(1, 1, 6, 0))!;
        none.AddUserKey("k"u8, "v"u8, TableEntryType.Put, 10, 0);
        var np = new Dictionary<string, byte[]>();
        none.Finish(np);
        Assert.AreEqual(0, np.Count);
        Assert.IsFalse(none.NeedCompact);
    }
}
