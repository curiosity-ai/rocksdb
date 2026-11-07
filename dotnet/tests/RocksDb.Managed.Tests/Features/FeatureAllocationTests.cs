using System.Buffers;
using System.Reflection;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Tracing;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Features;

/// <summary>
/// Steady-state allocation bounds and functional coverage of the low-allocation paths of optional features: attribute
/// group reads (sync and async, one consistent view through a reused internal snapshot), MultiScan setup, the
/// cross-column-family iterators' direction switch, block cache / IO tracing records and TryGetFullHistoryTsLow.
/// </summary>
[TestClass]
public sealed class FeatureAllocationTests
{
    private static readonly bool Optimized =
        typeof(RocksDatabase).Assembly.GetCustomAttribute<System.Diagnostics.DebuggableAttribute>() is not { IsJITOptimizerDisabled: true };

    /// <summary>Bytes allocated on this thread by <paramref name="iterations"/> calls of <paramref name="op"/>, after warm-up calls.</summary>
    private static long MeasureAllocated<TState>(TState state, Action<TState> op, int iterations)
    {
        for (int i = 0; i < 200; i++) op(state);
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < iterations; i++) op(state);
        return GC.GetAllocatedBytesForCurrentThread() - before;
    }

    /// <summary>
    /// Bytes per call of a reused-batch MultiGetEntity of one key in a table file: 0 in an optimized build; a Debug build of the library
    /// compiles the batched lookup's async state machine (run synchronously) as a class.
    /// </summary>
    private static long MultiGetCallOverhead(ColumnFamily cf, byte[] key)
    {
        using var batch = new MultiGetBatch();
        batch.Add(key);
        long overhead = MeasureAllocated((cf, batch), static s => s.cf.MultiGetEntity(s.batch), 1000) / 1000;
        if (Optimized) Assert.AreEqual(0L, overhead);
        return overhead;
    }

    private static readonly byte[] s_key3 = Key(3);
    private static readonly byte[] s_key17 = Key(17);
    private static readonly byte[] s_key200 = Key(200);

    private static readonly WideColumn[] s_hotColumns = [WideColumn.Default(B("hot-value")), new WideColumn(B("n"), B("12345678"))];
    private static readonly WideColumn[] s_coldColumns = [new WideColumn(B("city"), B("Paris")), new WideColumn(B("email"), B("a@b.c"))];

    /// <summary>A DB with "hot" and "cold" column families and 500 keys written as two attribute groups (key 7 only in hot).</summary>
    private static (RocksDatabase Db, ColumnFamily Hot, ColumnFamily Cold) OpenAttributeGroups(MemoryFileSystem fs, bool flush)
    {
        RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        ColumnFamily hot = db.CreateColumnFamily("hot", new ColumnFamilyOptions());
        ColumnFamily cold = db.CreateColumnFamily("cold", new ColumnFamilyOptions());
        for (int i = 0; i < 500; i++)
        {
            if (i == 7) hot.PutEntity(Key(i), s_hotColumns);
            else db.PutAttributeGroups(Key(i), [new AttributeGroup(hot, s_hotColumns), new AttributeGroup(cold, s_coldColumns)]);
        }
        if (flush)
        {
            hot.Flush();
            cold.Flush();
        }
        return (db, hot, cold);
    }

    private static void AssertColumns(WideColumn[] expected, WideColumns actual)
    {
        Assert.AreEqual(expected.Length, actual.Count);
        for (int i = 0; i < expected.Length; i++)
        {
            CollectionAssert.AreEqual(expected[i].Name.ToArray(), actual[i].Name.ToArray());
            CollectionAssert.AreEqual(expected[i].Value.ToArray(), actual[i].Value.ToArray());
        }
    }

    // =====================================================================================================
    // Attribute groups
    // =====================================================================================================

    [TestMethod]
    public void AttributeGroupReadsAreAllocationFreeInSteadyState()
    {
        MemoryFileSystem fs = NewFs();
        (RocksDatabase db, ColumnFamily hot, ColumnFamily cold) = OpenAttributeGroups(fs, flush: true);
        using (db)
        {
            long perMultiGet = MultiGetCallOverhead(cold, s_key3);
            PinnableAttributeGroup[] two = [new PinnableAttributeGroup(hot), new PinnableAttributeGroup(cold)];
            PinnableAttributeGroup[] one = [new PinnableAttributeGroup(hot)];
            const int N = 1000;
            // One MultiGet per column family; the internal snapshot of the 2-CF read is reused.
            long a2 = MeasureAllocated((db, two), static s => s.db.GetAttributeGroups(null, s_key3, s.two), N);
            long a1 = MeasureAllocated((db, one), static s => s.db.GetAttributeGroups(null, s_key3, s.one), N);
            using Snapshot snap = db.GetSnapshot();
            var ro = new ReadOptions { Snapshot = snap };
            long aSnap = MeasureAllocated((db, two, ro), static s => s.db.GetAttributeGroups(s.ro, s_key3, s.two), N);
            // Debug builds: the per-MultiGet state machine boxes plus a little slack for Debug-only temporaries.
            long slack = Optimized ? 0 : 16 * N;
            Assert.IsTrue(a2 <= 2 * perMultiGet * N + slack, $"2-CF GetAttributeGroups allocated {a2} B / {N}, MultiGet call {perMultiGet} B");
            Assert.IsTrue(a1 <= perMultiGet * N + slack, $"1-CF GetAttributeGroups allocated {a1} B / {N}");
            Assert.IsTrue(aSnap <= 2 * perMultiGet * N + slack, $"snapshot GetAttributeGroups allocated {aSnap} B / {N}");

            var keys = new ReadOnlyMemory<byte>[16];
            var results = new PinnableAttributeGroup[16][];
            for (int i = 0; i < 16; i++)
            {
                keys[i] = Key(i * 3);
                results[i] = [new PinnableAttributeGroup(hot), new PinnableAttributeGroup(cold)];
            }
            long am = MeasureAllocated((db, keys, results), static s => s.db.MultiGetAttributeGroups(null, s.keys, s.results), 200);
            Assert.IsTrue(am <= 2 * perMultiGet * 200 + slack, $"MultiGetAttributeGroups allocated {am} B / 200");
            if (Optimized)
            {
                Assert.AreEqual(0L, a2);
                Assert.AreEqual(0L, aSnap);
                Assert.AreEqual(0L, am);
            }
            // Only the user's snapshot remains: the internal one is released after every read.
            Assert.AreEqual(1, db.Impl.SnapshotCount);
        }
    }

    [TestMethod]
    public async Task AttributeGroupAsyncReadsMatchSyncReads()
    {
        MemoryFileSystem fs = NewFs();
        (RocksDatabase db, ColumnFamily hot, ColumnFamily cold) = OpenAttributeGroups(fs, flush: true);
        using (db)
        {
            PinnableAttributeGroup[] groups = [new PinnableAttributeGroup(hot), new PinnableAttributeGroup(cold)];
            foreach (int k in (int[])[3, 7])
            {
                await db.GetAttributeGroupsAsync(null, Key(k), groups);
                Assert.AreEqual(MultiGetStatus.Found, groups[0].Status);
                AssertColumns(s_hotColumns, groups[0].Columns);
                if (k == 7)
                {
                    Assert.AreEqual(MultiGetStatus.NotFound, groups[1].Status);
                    Assert.AreEqual(0, groups[1].Columns.Count);
                }
                else
                {
                    Assert.AreEqual(MultiGetStatus.Found, groups[1].Status);
                    AssertColumns(s_coldColumns, groups[1].Columns);
                }
            }
            await db.GetAttributeGroupsAsync(null, B("missing"), groups);
            Assert.AreEqual(MultiGetStatus.NotFound, groups[0].Status);
            Assert.AreEqual(MultiGetStatus.NotFound, groups[1].Status);

            ReadOnlyMemory<byte>[] keys = [Key(7), B("missing"), Key(10), Key(499)];
            var results = new PinnableAttributeGroup[keys.Length][];
            var syncResults = new PinnableAttributeGroup[keys.Length][];
            for (int i = 0; i < keys.Length; i++)
            {
                // Different group orders per key, and a key that reads only one column family.
                results[i] = i == 2 ? [new PinnableAttributeGroup(cold)] : [new PinnableAttributeGroup(cold), new PinnableAttributeGroup(hot)];
                syncResults[i] = i == 2 ? [new PinnableAttributeGroup(cold)] : [new PinnableAttributeGroup(cold), new PinnableAttributeGroup(hot)];
            }
            await db.MultiGetAttributeGroupsAsync(null, keys, results);
            db.MultiGetAttributeGroups(null, keys, syncResults);
            for (int i = 0; i < keys.Length; i++)
            {
                for (int g = 0; g < results[i].Length; g++)
                {
                    Assert.AreEqual(syncResults[i][g].Status, results[i][g].Status, $"key {i} group {g}");
                    Assert.AreEqual(syncResults[i][g].Columns.Count, results[i][g].Columns.Count);
                }
            }
            Assert.AreEqual(MultiGetStatus.NotFound, results[0][0].Status);
            Assert.AreEqual(MultiGetStatus.Found, results[0][1].Status);
            Assert.AreEqual(MultiGetStatus.NotFound, results[1][1].Status);
            AssertColumns(s_coldColumns, results[2][0].Columns);
            AssertColumns(s_hotColumns, results[3][1].Columns);
            Assert.AreEqual(0, db.Impl.SnapshotCount);
        }
    }

    [TestMethod]
    public void AttributeGroupReadsReuseTheInternalSnapshotSafely()
    {
        MemoryFileSystem fs = NewFs();
        (RocksDatabase db, ColumnFamily hot, ColumnFamily cold) = OpenAttributeGroups(fs, flush: false);
        using (db)
        {
            PinnableAttributeGroup[] groups = [new PinnableAttributeGroup(hot), new PinnableAttributeGroup(cold)];
            WideColumn[] newer = [new WideColumn(B("city"), B("Rome"))];
            for (int round = 0; round < 20; round++)
            {
                // A user snapshot taken between reads keeps its own sequence while the internal one is re-armed.
                using Snapshot user = db.GetSnapshot();
                db.GetAttributeGroups(null, Key(1), groups);
                cold.PutEntity(Key(1), round % 2 == 0 ? newer : s_coldColumns);
                db.GetAttributeGroups(null, Key(1), groups);
                AssertColumns(round % 2 == 0 ? newer : s_coldColumns, groups[1].Columns);
                db.GetAttributeGroups(new ReadOptions { Snapshot = user }, Key(1), groups);
                AssertColumns(round % 2 == 0 ? s_coldColumns : newer, groups[1].Columns);
                Assert.AreEqual(1, db.Impl.SnapshotCount);
                if (round % 5 == 4)
                {
                    cold.Flush();
                    cold.CompactRange();
                }
            }
            Assert.AreEqual(0, db.Impl.SnapshotCount);
        }
    }

    // =====================================================================================================
    // MultiScan, multi-CF iterators
    // =====================================================================================================

    [TestMethod]
    public void MultiScanSetupAllocatesOnlyItsHandles()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        for (int i = 0; i < 2000; i++) db.Put(Key(i), Value(i, 0));
        db.Flush();
        db.CompactRange();
        var args = new MultiScanArgs();
        for (int r = 0; r < 4; r++) args.Add(Key(r * 400), Key(r * 400 + 10));
        // Options with bounds: the bounds are ignored (not copied into the scan's iterator).
        var bounded = new ReadOptions { IterateLowerBound = Key(1000), IterateUpperBound = Key(1001) };
        int count = 0;
        using (MultiScan scan = db.NewMultiScan(bounded, args))
        {
            while (scan.MoveNextRange())
            {
                for (; scan.Valid; scan.Next()) count++;
            }
        }
        Assert.AreEqual(40, count);
        long allocated = MeasureAllocated((db, args), static s =>
        {
            using MultiScan scan = s.db.NewMultiScan(s.args);
            while (scan.MoveNextRange())
            {
                for (; scan.Valid; scan.Next()) { }
            }
        }, 200);
        // The MultiScan and Iterator handles (plus, in a Debug build, a few unpooled internals).
        Assert.IsTrue(allocated <= (Optimized ? 200 : 400) * 200, $"MultiScan per scan: {allocated / 200} B");
    }

    [TestMethod]
    public void CoalescingIteratorDirectionSwitchDoesNotAllocate()
    {
        MemoryFileSystem fs = NewFs();
        (RocksDatabase db, ColumnFamily hot, ColumnFamily cold) = OpenAttributeGroups(fs, flush: true);
        using (db)
        {
            using CoalescingIterator it = db.NewCoalescingIterator(null, [hot, cold]);
            it.Seek(Key(100));
            it.Next();
            it.Prev();
            CollectionAssert.AreEqual(Key(100), it.Key.ToArray());
            it.Prev();
            CollectionAssert.AreEqual(Key(99), it.Key.ToArray());
            it.Next();
            CollectionAssert.AreEqual(Key(100), it.Key.ToArray());
            Assert.AreEqual(4, it.Columns.Count);
            long allocated = MeasureAllocated(it, static i =>
            {
                i.Seek(s_key200);
                i.Next();
                i.Prev();
                i.Prev();
                i.Next();
            }, 1000);
            Assert.AreEqual(0L, allocated);
        }
    }

    // =====================================================================================================
    // Tracing
    // =====================================================================================================

    private sealed class CountingTraceWriter : TraceWriter
    {
        public ulong Bytes;
        public override void Write(ReadOnlySpan<byte> data) => Bytes += (ulong)data.Length;
        public override ulong FileSize => 0;
    }

    [TestMethod]
    public void BlockCacheTracedGetsDoNotAllocate()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        for (int i = 0; i < 500; i++) db.Put(Key(i), Value(i, 0));
        db.Flush();
        var writer = new CountingTraceWriter();
        db.StartBlockCacheTrace(new BlockCacheTraceOptions(), writer);
        byte[] buf = new byte[256];
        long allocated = MeasureAllocated((db, buf), static s => s.db.TryGet(s_key17, s.buf, out _), 1000);
        ulong bytes = writer.Bytes;
        db.EndBlockCacheTrace();
        Assert.IsTrue(bytes > 0);
        Assert.AreEqual(0L, allocated, "bytes allocated by 1000 traced Gets");
    }

    [TestMethod]
    public void NestedSynchronousBlockCacheContextsAreKeptApart()
    {
        MemoryFileSystem fs = NewFs();
        var nested = new NestedGetOperator();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { MergeOperator = nested });
        nested.Db = db;
        db.Put(B("inner"), B("v"));
        db.Put(B("outer"), B("base"));
        db.Flush();
        db.Merge(B("outer"), B("op"));
        db.Flush();
        db.StartBlockCacheTrace(new BlockCacheTraceOptions(), TracePath);
        Assert.AreEqual("base,op+v", S(db.Get(B("outer"))));
        db.EndBlockCacheTrace();
        using var reader = new BlockCacheTraceReader(TraceReader.OpenFile(fs, TracePath));
        reader.ReadHeader();
        var keys = new HashSet<string>();
        while (reader.TryReadAccess(out BlockCacheTraceRecord? r))
        {
            if (r!.Caller == TableReaderCaller.UserGet && r.BlockType == TraceType.BlockDataBlock) keys.Add(S(r.ReferencedKey)!);
        }
        CollectionAssert.AreEquivalent(new[] { "outer", "inner" }, keys.ToArray());
    }

    private const string TracePath = "/trace";

    /// <summary>A merge operator whose full merge reads another key (a nested synchronous Get).</summary>
    private sealed class NestedGetOperator : MergeOperator
    {
        public RocksDatabase? Db;
        public override string Name => "NestedGet";

        public override bool FullMerge(ReadOnlySpan<byte> key, bool hasExistingValue, ReadOnlySpan<byte> existingValue, in MergeOperands operands,
            IBufferWriter<byte> result)
        {
            byte[]? inner = Db!.Get(B("inner"));
            result.Write(existingValue);
            for (int i = 0; i < operands.Count; i++)
            {
                result.Write(","u8);
                result.Write(operands[i]);
            }
            result.Write("+"u8);
            result.Write(inner);
            return true;
        }
    }

    [TestMethod]
    public void IoTraceRecordsDoNotAllocate()
    {
        var tracer = new IoTracer();
        var writer = new CountingTraceWriter();
        tracer.StartTrace(new TraceOptions(), writer);
        long start = System.Diagnostics.Stopwatch.GetTimestamp();
        long allocated = MeasureAllocated((tracer, start), static s =>
            s.tracer.WriteIoOp("Read", s.start, null, "/db/000012.sst", IoTraceOp.Length | IoTraceOp.Offset, 4096, 8192, 0), 1000);
        tracer.EndTrace();
        Assert.IsTrue(writer.Bytes > 0);
        Assert.AreEqual(0L, allocated);
    }

    // =====================================================================================================
    // User-defined timestamps
    // =====================================================================================================

    [TestMethod]
    public void TryGetFullHistoryTsLowCopiesWithoutAllocating()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { Comparator = Comparator.BytewiseWithU64Ts });
        var w = new ArrayBufferWriter<byte>(16);
        Assert.IsFalse(db.TryGetFullHistoryTsLow(w));
        Assert.AreEqual(0, w.WrittenCount);
        byte[] low = BitConverter.GetBytes(42UL);
        db.IncreaseFullHistoryTsLow(low);
        Assert.IsTrue(db.TryGetFullHistoryTsLow(w));
        CollectionAssert.AreEqual(low, w.WrittenSpan.ToArray());
        CollectionAssert.AreEqual(db.GetFullHistoryTsLow(), w.WrittenSpan.ToArray());
        long allocated = MeasureAllocated((db, w), static s => { s.w.ResetWrittenCount(); s.db.TryGetFullHistoryTsLow(s.w); }, 1000);
        Assert.AreEqual(0L, allocated);

        using RocksDatabase plain = RocksDatabase.Open(DbPath + "2", NewOptions(fs));
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => plain.TryGetFullHistoryTsLow(w)).Code);
    }
}
