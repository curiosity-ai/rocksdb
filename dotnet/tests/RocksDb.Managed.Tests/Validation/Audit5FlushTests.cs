using System.Buffers;
using System.Globalization;
using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;
using RocksDb.Managed.Tests.Db;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #5: what a memtable flush writes, read against FlushJob::WriteLevel0Table (db/flush_job.cc), BuildTable
/// (db/builder.cc), CompactionIterator::NextFromInput, MergeHelper::MergeUntil and MemTableList::IsFlushPending /
/// PickMemtablesToFlush (db/memtable_list.cc). The expected table contents of the merge, single-delete and range-tombstone
/// cases are what native RocksDB writes for the same operations (compat tests Audit5FlushCompatTests).
/// </summary>
[TestClass]
public sealed class Audit5FlushTests
{
    [TestCleanup]
    public void Cleanup() => TestHooks.ClearAll();

    /// <summary>The table files of the DB directory, oldest first, each as "key@seq:Type=value; ... [start,end)@seq; ".</summary>
    private static List<string> Tables(MemoryFileSystem fs)
    {
        var names = new List<string>();
        foreach (string c in fs.GetChildren(DbPath))
        {
            if (c.EndsWith(".sst", StringComparison.Ordinal)) names.Add(c);
        }
        names.Sort(static (a, b) => ulong.Parse(a[..^4], CultureInfo.InvariantCulture).CompareTo(ulong.Parse(b[..^4], CultureInfo.InvariantCulture)));
        var result = new List<string>();
        foreach (string name in names)
        {
            var sb = new StringBuilder();
            using BlockBasedTableReader reader = BlockBasedTableReader.Open(fs.OpenRandomAccess(DbPath + "/" + name), new TableReaderOptions());
            using (BlockBasedTableIterator it = reader.NewIterator())
            {
                for (it.SeekToFirst(); it.Valid; it.Next())
                {
                    DbFormat.UnpackSequenceAndType(DbFormat.ExtractFooter(it.Key), out ulong seq, out EntryType t);
                    sb.Append(Encoding.UTF8.GetString(DbFormat.ExtractUserKey(it.Key))).Append('@').Append(seq).Append(':').Append(t)
                        .Append('=').Append(Encoding.UTF8.GetString(it.Value)).Append("; ");
                }
            }
            using (BlockIter? rd = reader.NewRangeTombstoneIterator())
            {
                if (rd is not null)
                {
                    for (rd.SeekToFirst(); rd.Valid; rd.Next())
                    {
                        DbFormat.UnpackSequenceAndType(DbFormat.ExtractFooter(rd.Key), out ulong seq, out _);
                        sb.Append('[').Append(Encoding.UTF8.GetString(DbFormat.ExtractUserKey(rd.Key))).Append(',')
                            .Append(Encoding.UTF8.GetString(rd.Value)).Append(")@").Append(seq).Append("; ");
                    }
                }
            }
            result.Add(sb.ToString());
        }
        return result;
    }

    private static string Table(MemoryFileSystem fs)
    {
        List<string> tables = Tables(fs);
        Assert.AreEqual(1, tables.Count, string.Join("\n", tables));
        return tables[0];
    }

    private static ColumnFamilyOptions Cf() => new() { DisableAutoCompactions = true };

    private static ColumnFamilyOptions MergeCf() => new() { DisableAutoCompactions = true, MergeOperator = new StringAppendOperator() };

    private static void Merge(RocksDatabase db, string k, string v) => db.Merge(B(k), B(v));

    private static void Put(RocksDatabase db, string k, string v) => db.Put(B(k), B(v));

    // ---------------------------------------------------------------------------------------------------------
    // 1a: the merge helper, single deletes and range tombstones at flush (BuildTable's CompactionIterator)

    [TestMethod]
    public void FlushCollapsesMergeOperands()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), MergeCf());
        // operands only: no base value is visible, so they are partially merged into one operand
        Merge(db, "a", "1"); Merge(db, "a", "2"); Merge(db, "a", "3");
        // a Put base: full merge into a value at the newest operand's sequence number
        Put(db, "b", "0"); Merge(db, "b", "1"); Merge(db, "b", "2");
        // a Delete base: full merge without an existing value
        Put(db, "c", "0"); db.Delete(B("c")); Merge(db, "c", "1"); Merge(db, "c", "2");
        // one operand stays an operand
        Merge(db, "d", "1");
        // operands hidden by a newer Put
        Merge(db, "e", "1"); Merge(db, "e", "2"); Put(db, "e", "3");
        db.Flush();
        Assert.AreEqual("a@3:Merge=1,2,3; b@6:Value=0,1,2; c@10:Value=1,2; d@11:Merge=1; e@14:Value=3; ", Table(fs));
        Assert.AreEqual("1,2,3", GetString(db, "a"));
        Assert.AreEqual("0,1,2", GetString(db, "b"));
        Assert.AreEqual("1,2", GetString(db, "c"));
        Assert.AreEqual("1", GetString(db, "d"));
        Assert.AreEqual("3", GetString(db, "e"));
    }

    [TestMethod]
    public void FlushStopsMergingAtSnapshots()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), MergeCf());
        Merge(db, "a", "1"); Merge(db, "a", "2");
        using Snapshot s1 = db.GetSnapshot();
        Merge(db, "a", "3"); Merge(db, "a", "4");
        Put(db, "b", "0"); Merge(db, "b", "1");
        using Snapshot s2 = db.GetSnapshot();
        Merge(db, "b", "2"); Merge(db, "b", "3");
        db.Flush();
        // The operands of each snapshot stripe are merged on their own; the stripe with the Put becomes a value.
        Assert.AreEqual("a@4:Merge=3,4; a@2:Merge=1,2; b@8:Merge=2,3; b@6:Value=0,1; ", Table(fs));
        Assert.AreEqual("1,2", GetString(db, "a", s1));
        Assert.AreEqual("1,2,3,4", GetString(db, "a", s2));
        Assert.AreEqual("0,1", GetString(db, "b", s2));
        Assert.AreEqual("0,1,2,3", GetString(db, "b"));
    }

    [TestMethod]
    public void FlushDropsEntriesCoveredByRangeTombstones()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), MergeCf());
        Put(db, "a", "1"); Put(db, "b", "1"); Put(db, "c", "1");
        db.DeleteRange(B("a"), B("c"));
        Put(db, "b", "2");
        Put(db, "d", "1");
        using Snapshot s = db.GetSnapshot();
        db.DeleteRange(B("d"), B("e")); // d@6 is visible to the snapshot: kept
        Put(db, "m", "0"); Merge(db, "m", "1");
        db.DeleteRange(B("m"), B("n"));
        Merge(db, "m", "2"); Merge(db, "m", "3"); // merged without the covered base and operand
        db.Flush();
        Assert.AreEqual("b@5:Value=2; c@3:Value=1; d@6:Value=1; m@12:Value=2,3; [a,c)@4; [d,e)@7; [m,n)@10; ", Table(fs));
        Assert.IsNull(GetString(db, "a"));
        Assert.AreEqual("2", GetString(db, "b"));
        Assert.AreEqual("1", GetString(db, "d", s));
        Assert.IsNull(GetString(db, "d"));
        Assert.AreEqual("2,3", GetString(db, "m"));
    }

    [TestMethod]
    public void FlushResolvesSingleDeletes()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), Cf());
        Put(db, "a", "1"); db.SingleDelete(B("a"));            // the pair is dropped
        db.SingleDelete(B("b"));                               // nothing to pair with in the memtable: kept
        Put(db, "c", "1");
        using Snapshot s = db.GetSnapshot();
        db.SingleDelete(B("c"));                               // the Put is visible to the snapshot: both kept
        Put(db, "d", "1"); db.SingleDelete(B("d")); Put(db, "d", "2");
        db.Flush();
        Assert.AreEqual("b@3:SingleDeletion=; c@5:SingleDeletion=; c@4:Value=1; d@8:Value=2; ", Table(fs));
        Assert.IsNull(GetString(db, "a"));
        Assert.IsNull(GetString(db, "c"));
        Assert.AreEqual("1", GetString(db, "c", s));
        Assert.AreEqual("2", GetString(db, "d"));
    }

    [TestMethod]
    [DataRow(true)]
    [DataRow(false)]
    public void FlushChecksTheSingleDeleteContract(bool enforce)
    {
        // CompactionIterator: a SingleDelete followed by a Delete of the same key in the same stripe violates the
        // contract; with enforce_single_del_contracts the flush fails with Corruption. Without, the SingleDelete is
        // dropped and, as upstream, still hides the older versions of its stripe (rule (A) sees its sequence number).
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.EnforceSingleDelContracts = enforce;
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, Cf());
        Put(db, "a", "1"); db.Delete(B("a")); db.SingleDelete(B("a"));
        if (enforce)
        {
            var ex = Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
            Assert.AreEqual(StatusCode.Corruption, ex.Code);
        }
        else
        {
            db.Flush();
            Assert.AreEqual(0, Tables(fs).Count);
            Assert.IsNull(GetString(db, "a"));
        }
    }

    private sealed class FailingOperator : MergeOperator
    {
        public override string Name => "FailingOperator";

        public override bool FullMerge(ReadOnlySpan<byte> key, bool hasExistingValue, ReadOnlySpan<byte> existingValue, in MergeOperands operands,
            IBufferWriter<byte> result) => false;
    }

    [TestMethod]
    public void FlushFailsWhenTheMergeOperatorFails()
    {
        // MergeHelper::MergeUntil returns the operator's failure (op_failure_scope kDefault) and BuildTable fails with
        // it: the flush reports Corruption and the memtable stays. An operand without a base in the memtable is not
        // merged (no full merge away from the bottommost level), so its flush succeeds.
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cf = Cf();
        cf.MergeOperator = new FailingOperator();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cf);
        Merge(db, "a", "1");
        db.Flush();
        Assert.AreEqual("a@1:Merge=1; ", Table(fs));
        Put(db, "b", "0"); Merge(db, "b", "1");
        var ex = Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
        Assert.AreEqual(1, Tables(fs).Count);
    }

    [TestMethod]
    public void FlushOfPlainEntriesIsUnchanged()
    {
        // Puts and deletes only (the default configuration): the copy loop, versions hidden in their stripe dropped.
        MemoryFileSystem fs = NewFs();
        var stats = new Statistics();
        DbOptions o = NewOptions(fs);
        o.Statistics = stats;
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, Cf());
        Put(db, "a", "1"); Put(db, "a", "2"); Put(db, "b", "1"); db.Delete(B("b")); Put(db, "c", "1");
        using Snapshot s = db.GetSnapshot();
        Put(db, "c", "2"); db.Delete(B("d"));
        db.Flush();
        Assert.AreEqual("a@2:Value=2; b@4:Deletion=; c@6:Value=2; c@5:Value=1; d@7:Deletion=; ", Table(fs));
        // 7 entries of 9 key bytes, 5 value bytes read; 5 entries with 3 value bytes written.
        Assert.AreEqual(68UL, stats.GetTickerCount(Ticker.MemtablePayloadBytesAtFlush));
        Assert.AreEqual(20UL, stats.GetTickerCount(Ticker.MemtableGarbageBytesAtFlush));
    }

    [TestMethod]
    public void RecoveryFlushCollapsesMergeOperands()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), MergeCf()))
        {
            Put(db, "a", "0"); Merge(db, "a", "1"); Merge(db, "a", "2"); Merge(db, "b", "1"); Merge(db, "b", "2");
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), MergeCf()))
        {
            Assert.AreEqual("a@3:Value=0,1,2; b@5:Merge=1,2; ", Table(fs));
            Assert.AreEqual("0,1,2", GetString(db, "a"));
            Assert.AreEqual("1,2", GetString(db, "b"));
        }
    }

    [TestMethod]
    public void FlushMergesOntoABlobValue()
    {
        // enable_blob_files: the merge result is a plain value again and goes to a blob file when large enough
        // (CompactionIterator::PrepareOutput -> ExtractLargeValueIfNeeded on the merged value).
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cf = MergeCf();
        cf.EnableBlobFiles = true;
        cf.MinBlobSize = 16;
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cf);
        string big = new('x', 64);
        Put(db, "a", big); Merge(db, "a", "1");
        Merge(db, "b", "1"); Merge(db, "b", "2");
        db.Flush();
        Assert.AreEqual(big + ",1", GetString(db, "a"));
        Assert.AreEqual("1,2", GetString(db, "b"));
        string table = Table(fs);
        StringAssert.StartsWith(table, "a@2:BlobIndex=");
        StringAssert.Contains(table, "b@4:Merge=1,2; ");
        // A later flush merges operands only (the blob base is in an older file and is not read).
        Merge(db, "a", "2"); Merge(db, "a", "3");
        db.Flush();
        Assert.AreEqual(big + ",1,2,3", GetString(db, "a"));
        db.CompactRange();
        Assert.AreEqual(big + ",1,2,3", GetString(db, "a"));
    }

    // ---------------------------------------------------------------------------------------------------------
    // 1b: the compaction filter at flush (BuildTable: compaction_filter_factory with ShouldFilterTableFileCreation)

    /// <summary>Removes values "drop", rewrites "change" to "changed", removes operands "drop"; records what it saw.</summary>
    private sealed class RecordingFilter : CompactionFilter
    {
        public List<string> Seen { get; } = new();

        public override string Name => "RecordingFilter";

        public override CompactionFilterDecision Filter(int level, ReadOnlySpan<byte> key, CompactionFilterValueType valueType,
            ReadOnlySpan<byte> existingValue, IBufferWriter<byte> newValue, IBufferWriter<byte> skipUntil)
        {
            string k = Encoding.UTF8.GetString(key), v = Encoding.UTF8.GetString(existingValue);
            Seen.Add(level + ":" + k + "=" + v + (valueType == CompactionFilterValueType.MergeOperand ? "(op)" : ""));
            if (v == "drop") return CompactionFilterDecision.Remove;
            if (v == "change")
            {
                newValue.Write("changed"u8);
                return CompactionFilterDecision.ChangeValue;
            }
            if (v == "skip")
            {
                skipUntil.Write("m"u8);
                return CompactionFilterDecision.RemoveAndSkipUntil;
            }
            return CompactionFilterDecision.Keep;
        }
    }

    private sealed class FlushFilterFactory(bool forFlush) : CompactionFilterFactory
    {
        public List<CompactionFilterContext> Contexts { get; } = new();
        public List<RecordingFilter> Filters { get; } = new();

        public override string Name => "FlushFilterFactory";

        public override CompactionFilter? CreateCompactionFilter(in CompactionFilterContext context)
        {
            Contexts.Add(context);
            var f = new RecordingFilter();
            Filters.Add(f);
            return f;
        }

        public override bool ShouldFilterTableFileCreation(bool isFlush) => !isFlush || forFlush;
    }

    [TestMethod]
    public void FlushAppliesTheFactoryFilterWhenItAsksForFlushes()
    {
        MemoryFileSystem fs = NewFs();
        var factory = new FlushFilterFactory(true);
        ColumnFamilyOptions cf = MergeCf();
        cf.CompactionFilterFactory = factory;
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cf);
        Put(db, "a", "drop");                       // Remove: becomes a Delete at the value's sequence number
        Put(db, "b", "change");                     // ChangeValue
        Put(db, "c", "old"); Put(db, "c", "keep");  // only the newest version reaches the filter
        Merge(db, "d", "1"); Merge(db, "d", "drop"); Merge(db, "d", "2"); // operands are filtered one by one
        db.Flush();
        Assert.AreEqual("a@1:Deletion=; b@2:Value=changed; c@4:Value=keep; d@7:Merge=1,2; ", Table(fs));
        Assert.AreEqual(1, factory.Contexts.Count);
        Assert.AreEqual(new CompactionFilterContext(IsFullCompaction: false, IsManualCompaction: false, ColumnFamilyId: 0, IsFlush: true), factory.Contexts[0]);
        CollectionAssert.AreEqual(new[] { "0:a=drop", "0:b=change", "0:c=keep", "0:d=2(op)", "0:d=drop(op)", "0:d=1(op)" }, factory.Filters[0].Seen);
        Assert.IsNull(GetString(db, "a"));
        Assert.AreEqual("changed", GetString(db, "b"));
        Assert.AreEqual("1,2", GetString(db, "d"));
    }

    [TestMethod]
    public void FlushFilterRemoveAndSkipUntil()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cf = Cf();
        cf.CompactionFilterFactory = new FlushFilterFactory(true);
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cf);
        Put(db, "a", "skip"); Put(db, "b", "1"); Put(db, "l", "1"); Put(db, "m", "1");
        db.Flush();
        // [a, m) is skipped without writing anything for it (the entries vanish, older data would show through).
        Assert.AreEqual("m@4:Value=1; ", Table(fs));
    }

    [TestMethod]
    public void FlushFilterRespectsSnapshotsForMergeOperands()
    {
        // MergeHelper filters only operands newer than the newest snapshot; values are filtered whatever the snapshots
        // (IgnoreSnapshots() = true is the only supported mode).
        MemoryFileSystem fs = NewFs();
        var factory = new FlushFilterFactory(true);
        ColumnFamilyOptions cf = MergeCf();
        cf.CompactionFilterFactory = factory;
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cf);
        Merge(db, "d", "drop");
        using Snapshot s = db.GetSnapshot();
        Merge(db, "d", "drop");
        Put(db, "v", "drop");
        db.Flush();
        Assert.AreEqual("d@1:Merge=drop; v@3:Deletion=; ", Table(fs));
        Assert.AreEqual("drop", GetString(db, "d", s));
    }

    [TestMethod]
    public void FlushIgnoresFiltersNotMeantForFlushes()
    {
        // The default ShouldFilterTableFileCreation (compactions only) and the plain compaction_filter option leave the
        // flush alone; the compaction applies them.
        MemoryFileSystem fs = NewFs();
        var factory = new FlushFilterFactory(false);
        ColumnFamilyOptions cf = Cf();
        cf.CompactionFilterFactory = factory;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cf))
        {
            Put(db, "a", "drop");
            db.Flush();
            Assert.AreEqual("a@1:Value=drop; ", Table(fs));
            Assert.AreEqual(0, factory.Contexts.Count);
            db.CompactRange(new CompactRangeOptions { BottommostLevelCompaction = BottommostLevelCompaction.Force }, null, null);
            Assert.IsNull(GetString(db, "a"));
        }
        MemoryFileSystem fs2 = NewFs();
        var filter = new RecordingFilter();
        ColumnFamilyOptions cf2 = Cf();
        cf2.CompactionFilter = filter;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs2), cf2))
        {
            Put(db, "a", "drop");
            db.Flush();
            Assert.AreEqual("a@1:Value=drop; ", Table(fs2));
            Assert.AreEqual(0, filter.Seen.Count);
        }
    }

    [TestMethod]
    public void RecoveryFlushAppliesTheFlushFilter()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cf = Cf();
        cf.CompactionFilterFactory = new FlushFilterFactory(true);
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cf))
        {
            Put(db, "a", "change");
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cf))
        {
            Assert.AreEqual("a@1:Value=changed; ", Table(fs));
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // 2: min_write_buffer_number_to_merge (MemTableList::IsFlushPending / PickMemtablesToFlush / FlushRequested)

    private static ColumnFamilyOptions MergeBuffers(int minToMerge, int max) => new()
    {
        DisableAutoCompactions = true,
        MergeOperator = new StringAppendOperator(),
        MinWriteBufferNumberToMerge = minToMerge,
        MaxWriteBufferNumber = max,
    };

    private static void SealAndSettle(RocksDatabase db)
    {
        db.DefaultColumnFamily.Db.SwitchMemtableForTest();
        db.DefaultColumnFamily.Db.WaitForBackgroundWorkForTest();
    }

    [TestMethod]
    public void FlushWaitsForMinWriteBufferNumberToMerge()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), MergeBuffers(3, 5));
        Put(db, "a", "1"); Put(db, "c", "1");
        SealAndSettle(db);
        Put(db, "a", "2"); Put(db, "b", "1");
        SealAndSettle(db);
        Assert.AreEqual(0, Tables(fs).Count);
        Assert.AreEqual("2", db.GetProperty("rocksdb.num-immutable-mem-table"));
        Assert.AreEqual("0", db.GetProperty("rocksdb.mem-table-flush-pending"));
        Merge(db, "c", "2"); Put(db, "d", "1");
        SealAndSettle(db);
        // The three memtables are flushed into one file; a@1 is hidden by a@3 of a newer memtable, c's base value and
        // operand from different memtables are merged.
        Assert.AreEqual("a@3:Value=2; b@4:Value=1; c@5:Value=1,2; d@6:Value=1; ", Table(fs));
        Assert.AreEqual("0", db.GetProperty("rocksdb.num-immutable-mem-table"));
    }

    [TestMethod]
    public void ManualFlushDoesNotWaitForMinWriteBufferNumberToMerge()
    {
        // DBImpl::FlushMemTable: FlushRequested makes the waiting memtable flushable; both are picked together.
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), MergeBuffers(3, 5));
        Put(db, "a", "1");
        SealAndSettle(db);
        Assert.AreEqual(0, Tables(fs).Count);
        Put(db, "a", "2"); Put(db, "b", "1");
        db.Flush();
        Assert.AreEqual("a@2:Value=2; b@3:Value=1; ", Table(fs));
        // A manual flush with an empty memtable still flushes what waits.
        Put(db, "c", "1");
        SealAndSettle(db);
        Assert.AreEqual(1, Tables(fs).Count);
        db.Flush();
        Assert.AreEqual(2, Tables(fs).Count);
        Assert.AreEqual("0", db.GetProperty("rocksdb.num-immutable-mem-table"));
    }

    [TestMethod]
    public void DefaultMinWriteBufferNumberToMergeFlushesEachMemtable()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), MergeBuffers(1, 2));
        Put(db, "a", "1");
        SealAndSettle(db);
        Put(db, "a", "2");
        SealAndSettle(db);
        CollectionAssert.AreEqual(new[] { "a@1:Value=1; ", "a@2:Value=2; " }, Tables(fs));
    }

    [TestMethod]
    public void MinWriteBufferNumberToMergeIsSanitized()
    {
        // SanitizeCfOptions: at most max_write_buffer_number - 1 (a flush always starts before writes stop), and 1 with
        // atomic_flush.
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), MergeBuffers(5, 3)))
        {
            Put(db, "a", "1");
            SealAndSettle(db);
            Assert.AreEqual(0, Tables(fs).Count);
            Put(db, "b", "1");
            SealAndSettle(db);
            Assert.AreEqual(1, Tables(fs).Count);
        }
        MemoryFileSystem fs2 = NewFs();
        DbOptions o = NewOptions(fs2);
        o.AtomicFlush = true;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o, MergeBuffers(3, 5)))
        {
            Put(db, "a", "1");
            SealAndSettle(db);
            Assert.AreEqual(1, Tables(fs2).Count);
        }
    }

    [TestMethod]
    public void WaitingMemtablesAreRecoveredFromTheWal()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), MergeBuffers(3, 5)))
        {
            Put(db, "a", "1"); Merge(db, "a", "2");
            SealAndSettle(db);
            Merge(db, "a", "3");
            SealAndSettle(db);
            Assert.AreEqual(0, Tables(fs).Count);
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), MergeBuffers(3, 5)))
        {
            Assert.AreEqual("1,2,3", GetString(db, "a"));
            Assert.AreEqual("a@3:Value=1,2,3; ", Table(fs));
        }
    }

    [TestMethod]
    public void WalLimitFlushDoesNotWaitForMinWriteBufferNumberToMerge()
    {
        // SwitchWAL (max_total_wal_size, kWalFull) calls FlushRequested: a column family whose memtables wait for
        // min_write_buffer_number_to_merge is flushed so that the oldest WAL can go.
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.CreateMissingColumnFamilies = true;
        o.MaxTotalWalSize = 1;
        using RocksDatabase db = RocksDatabase.Open(DbPath, o,
            [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, MergeBuffers(3, 5)), new ColumnFamilyDescriptor("other", MergeBuffers(3, 5))]);
        Put(db, "a", "1");
        SealAndSettle(db);
        Assert.AreEqual(0, Tables(fs).Count);
        // The next write finds the WALs above max_total_wal_size; the oldest one holds only the waiting memtable.
        db.GetColumnFamily("other").Put(B("x"), B("1"));
        db.DefaultColumnFamily.Db.WaitForBackgroundWorkForTest();
        Assert.AreEqual(1, Tables(fs).Count, "the default column family's WAL data was flushed");
        Assert.AreEqual("1", GetString(db, "a"));
    }

    // ---------------------------------------------------------------------------------------------------------
    // 3: paranoid_file_checks (BuildTable: OutputValidator hash compared after reading the new table back)

    /// <summary>
    /// Flips one byte of the value "corrupt-me" in the new table file before it is validated. The table is written
    /// without checksums and compression, so the blocks still read and only the paranoid hash can notice.
    /// </summary>
    private static void CorruptTheOutputBeforeValidation(MemoryFileSystem fs) => TestHooks.Set("BuildTable:BeforeOutputValidation", () =>
    {
        foreach (string c in fs.GetChildren(DbPath))
        {
            if (!c.EndsWith(".sst", StringComparison.Ordinal)) continue;
            string path = DbPath + "/" + c;
            byte[] data = fs.ReadAllBytes(path);
            int at = data.AsSpan().IndexOf("corrupt-me"u8);
            if (at >= 0) fs.WriteAt(path, at, "C"u8);
        }
    });

    private static ColumnFamilyOptions UncheckedTables(ColumnFamilyOptions cf)
    {
        cf.Compression = RocksDb.Managed.Compression.CompressionType.None;
        cf.TableOptions = new BlockBasedTableOptions { Checksum = ChecksumType.NoChecksum };
        return cf;
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void ParanoidFileChecksReadsTheFlushOutputBack(bool withMerge)
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cf = UncheckedTables(MergeCf());
        cf.ParanoidFileChecks = true;
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cf);
        Put(db, "a", "1");
        Put(db, "b", "corrupt-me");
        if (withMerge) Merge(db, "c", "1");
        CorruptTheOutputBeforeValidation(fs);
        var ex = Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
        StringAssert.Contains(ex.Message, "Paranoid checksums do not match");
        TestHooks.ClearAll();
        Assert.AreEqual(0, Tables(fs).Count, "the output is deleted");
        Assert.AreEqual("corrupt-me", GetString(db, "b"));
    }

    [TestMethod]
    public void ParanoidFileChecksPassesForAGoodFlush()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cf = MergeCf();
        cf.ParanoidFileChecks = true;
        cf.EnableBlobFiles = true;
        cf.MinBlobSize = 8;
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cf);
        Put(db, "a", "1"); Put(db, "a", "2"); Put(db, "big", new string('b', 32)); Merge(db, "m", "1"); Merge(db, "m", "2");
        db.Flush();
        Assert.AreEqual(1, Tables(fs).Count);
        Assert.AreEqual("2", GetString(db, "a"));
        Assert.AreEqual("1,2", GetString(db, "m"));
    }

    [TestMethod]
    public void WithoutParanoidFileChecksTheFlushOutputIsNotReadBack()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), UncheckedTables(Cf()));
        Put(db, "b", "corrupt-me");
        CorruptTheOutputBeforeValidation(fs);
        db.Flush();
        TestHooks.ClearAll();
        Assert.AreEqual(1, Tables(fs).Count);
    }

    // ---------------------------------------------------------------------------------------------------------
    // 3: the MANIFEST size check of VersionSet::Close

    private static List<string> Manifests(MemoryFileSystem fs)
    {
        var list = new List<string>();
        foreach (string c in fs.GetChildren(DbPath))
        {
            if (c.StartsWith("MANIFEST-", StringComparison.Ordinal)) list.Add(c);
        }
        list.Sort(StringComparer.Ordinal);
        return list;
    }

    [TestMethod]
    public void CloseKeepsAManifestOfTheExpectedSize()
    {
        MemoryFileSystem fs = NewFs();
        string manifest;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), Cf()))
        {
            Put(db, "a", "1");
            db.Flush();
            manifest = Manifests(fs)[^1];
        }
        Assert.AreEqual(manifest, Manifests(fs)[^1], "no MANIFEST was written by the close");
        StringAssert.Contains(S(fs.ReadAllBytes(DbPath + "/CURRENT"))!, manifest);
    }

    [TestMethod]
    public void CloseWritesANewManifestWhenTheSizeDiffers()
    {
        // VersionSet::Close: the MANIFEST on disk is longer than what was written to it (e.g. a write the file system
        // kept after reporting a failure): the close logs it and writes a new MANIFEST, which the next open reads.
        MemoryFileSystem fs = NewFs();
        string manifest;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), Cf()))
        {
            Put(db, "a", "1");
            db.Flush();
            manifest = Manifests(fs)[^1];
            TestHooks.Set("VersionSet::Close:AfterClose", () =>
            {
                string path = DbPath + "/" + manifest;
                fs.WriteAt(path, fs.GetFileSize(path), new byte[7]);
            });
        }
        TestHooks.ClearAll();
        string current = S(fs.ReadAllBytes(DbPath + "/CURRENT"))!.Trim();
        Assert.AreNotEqual(manifest, current);
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), Cf()))
        {
            Assert.AreEqual("1", GetString(db, "a"));
        }
    }

    /// <summary>
    /// Whatever the flush merges or drops, every key reads the same before and after it, at the tip and at every
    /// snapshot (random puts, merges, deletes, single deletes on their own keys and range deletions).
    /// </summary>
    [TestMethod]
    [DataRow(11)]
    [DataRow(12)]
    [DataRow(13)]
    public void ReadsAreUnchangedByTheFlush(int seed)
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), MergeCf());
        var rnd = new Random(seed);
        var snapshots = new List<Snapshot>();
        const int keys = 10;
        // Keys 8 and 9 follow the SingleDelete contract: one Put, then one SingleDelete.
        var live = new bool[keys];
        string Name(int k) => "k" + k.ToString("D2", CultureInfo.InvariantCulture);
        List<string?> ReadAll()
        {
            var all = new List<string?>();
            for (int k = 0; k < keys; k++) all.Add(GetString(db, Name(k)));
            foreach (Snapshot s in snapshots)
            {
                for (int k = 0; k < keys; k++) all.Add(GetString(db, Name(k), s));
            }
            return all;
        }
        try
        {
            for (int round = 0; round < 6; round++)
            {
                for (int i = 0; i < 120; i++)
                {
                    int k = rnd.Next(keys);
                    bool singleDeleteKey = k >= 8;
                    string v = "v" + round + "_" + i;
                    int r = rnd.Next(100);
                    if (singleDeleteKey)
                    {
                        if (live[k]) db.SingleDelete(B(Name(k)));
                        else Put(db, Name(k), v);
                        live[k] = !live[k];
                    }
                    else if (r < 30) Put(db, Name(k), v);
                    else if (r < 62) Merge(db, Name(k), v);
                    else if (r < 76) db.Delete(B(Name(k)));
                    else if (r < 82) db.DeleteRange(B(Name(rnd.Next(4))), B(Name(4 + rnd.Next(4))));
                    else if (r < 90 && snapshots.Count < 12) snapshots.Add(db.GetSnapshot());
                    else Put(db, Name(k), v);
                }
                List<string?> before = ReadAll();
                db.Flush();
                CollectionAssert.AreEqual(before, ReadAll(), "round " + round);
            }
        }
        finally
        {
            foreach (Snapshot s in snapshots) s.Dispose();
        }
    }
}
