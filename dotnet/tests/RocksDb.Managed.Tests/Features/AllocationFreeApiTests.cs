using System.Buffers;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Features;

/// <summary>
/// Steady-state allocation bounds and functional coverage of the low-allocation read, property and metadata APIs:
/// the pooled legacy MultiGet and the caller-memory MultiGet, reused lazy entity results, numeric properties without
/// string formatting, the cached column family list, live file visiting, span size estimates, histogram data, the
/// lazily materialized HyperClock table and the shared directory listing at open.
/// </summary>
[TestClass]
public sealed class AllocationFreeApiTests
{
    private const int ValueSize = 100;

    private static byte[] Val(int i)
    {
        byte[] v = new byte[ValueSize];
        v.AsSpan().Fill((byte)i);
        Encoding.ASCII.GetBytes(i.ToString("D6", System.Globalization.CultureInfo.InvariantCulture)).CopyTo(v, 0);
        return v;
    }

    /// <summary>A DB with <paramref name="count"/> keys (Key(i) -> Val(i)) flushed to a table file, every block cached.</summary>
    private static RocksDatabase OpenFilled(MemoryFileSystem fs, int count, ColumnFamilyOptions? cfo = null)
    {
        RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo ?? new ColumnFamilyOptions());
        for (int i = 0; i < count; i++) db.Put(Key(i), Val(i));
        db.Flush();
        for (int i = 0; i < count; i++) Assert.IsNotNull(db.Get(Key(i)));
        return db;
    }

    /// <summary>Bytes allocated on this thread by <paramref name="iterations"/> calls of <paramref name="op"/>, after warm-up calls.</summary>
    private static long MeasureAllocated<TState>(TState state, Action<TState> op, int iterations)
    {
        for (int i = 0; i < 200; i++) op(state);
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < iterations; i++) op(state);
        return GC.GetAllocatedBytesForCurrentThread() - before;
    }

    /// <summary>Heap size of a byte[] of <paramref name="length"/> on a 64-bit runtime (header + length, 8-byte aligned).</summary>
    private static long ByteArraySize(int length) => 24 + ((length + 7) & ~7L);

    /// <summary>
    /// Bytes that <paramref name="iterations"/> runs of a reused <see cref="MultiGetBatch"/> over <paramref name="keys"/>
    /// allocate: 0 in an optimized build; a Debug build of the library compiles the batched lookup's async state machine
    /// (run with synchronous: true) as a class, which every MultiGet flavour pays alike.
    /// </summary>
    private static long BatchMultiGetOverhead(RocksDatabase db, IEnumerable<ReadOnlyMemory<byte>> keys, int iterations)
    {
        using var batch = new MultiGetBatch();
        foreach (ReadOnlyMemory<byte> k in keys) batch.Add(k.Span);
        long overhead = MeasureAllocated((db, batch), static s => s.db.MultiGet(s.batch), iterations);
        if (Optimized) Assert.AreEqual(0L, overhead, "reused MultiGetBatch");
        return overhead;
    }

    private static readonly bool Optimized =
        typeof(RocksDatabase).Assembly.GetCustomAttribute<System.Diagnostics.DebuggableAttribute>() is not { IsJITOptimizerDisabled: true };

    private static ReadOnlyMemory<byte>[] KeyMemories(int[] indexes)
    {
        var keys = new ReadOnlyMemory<byte>[indexes.Length];
        for (int i = 0; i < indexes.Length; i++) keys[i] = indexes[i] < 0 ? B("missing" + i) : Key(indexes[i]);
        return keys;
    }

    // =====================================================================================================
    // MultiGet (legacy, pooled batch) and MultiGet into caller memory
    // =====================================================================================================

    [TestMethod]
    public void LegacyMultiGetAllocatesOnlyTheResults()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = OpenFilled(fs, 1000);
        var keys = new List<byte[]>();
        for (int i = 0; i < 16; i++) keys.Add(Key(i * 37 % 1000));
        const int n = 500;
        long allocated = MeasureAllocated((db, keys), static s => s.db.MultiGet(s.keys), n);
        long overhead = BatchMultiGetOverhead(db, keys.Select(static k => new ReadOnlyMemory<byte>(k)), n);
        // Inherent: the result array and one array per value (the lookup itself uses the thread's pooled batch).
        long inherent = ByteArraySize(16 * 8) + 16 * ByteArraySize(ValueSize);
        Assert.IsTrue(allocated <= n * inherent + overhead, "bytes per MultiGet of 16 keys: " + (double)allocated / n + ", inherent " + inherent
            + ", batch overhead " + (double)overhead / n);
    }

    [TestMethod]
    public void LegacyMultiGetPooledBatchIsReusedAcrossSizesAndThreads()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = OpenFilled(fs, 3000);
        // Different sizes on one thread (the pooled batch is cleared between calls; a 2000-key batch is not kept).
        foreach (int size in new[] { 16, 3, 2000, 1, 40, 2000, 16 })
        {
            var keys = new List<byte[]>();
            for (int i = 0; i < size; i++) keys.Add(i % 5 == 4 ? B("missing" + i) : Key((i * 7 + size) % 3000));
            byte[]?[] r = db.MultiGet(keys);
            Assert.AreEqual(size, r.Length);
            for (int i = 0; i < size; i++)
            {
                if (i % 5 == 4) Assert.IsNull(r[i]);
                else CollectionAssert.AreEqual(Val((i * 7 + size) % 3000), r[i]);
            }
        }
        // Concurrent callers each get their own batch.
        Parallel.For(0, 8, t =>
        {
            for (int round = 0; round < 200; round++)
            {
                var keys = new List<byte[]>();
                for (int i = 0; i < 16; i++) keys.Add(Key((t * 131 + round * 17 + i) % 3000));
                byte[]?[] r = db.MultiGet(keys);
                for (int i = 0; i < 16; i++) CollectionAssert.AreEqual(Val((t * 131 + round * 17 + i) % 3000), r[i]);
            }
        });
    }

    [TestMethod]
    public void LegacyMultiGetPerKeyFailureIsThrownAndThePoolStaysUsable()
    {
        MemoryFileSystem fs = NewFs();
        var cfo = new ColumnFamilyOptions { TableOptions = new BlockBasedTableOptions { NoBlockCache = true } };
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        for (int i = 0; i < 100; i++) db.Put(Key(i), Val(i));
        db.Flush();
        var keys = new List<byte[]> { Key(1), Key(2) };
        // No block cache + kBlockCacheTier: the data block cannot be read without I/O.
        var ex = Assert.ThrowsExactly<RocksDbException>(() => db.MultiGet(new ReadOptions { ReadTier = ReadTier.BlockCacheTier }, keys));
        Assert.AreEqual(StatusCode.Incomplete, ex.Code);
        byte[]?[] r = db.MultiGet(keys);
        CollectionAssert.AreEqual(Val(1), r[0]);
        CollectionAssert.AreEqual(Val(2), r[1]);
    }

    [TestMethod]
    public async Task SpanMultiGetFillsCallerMemory()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = OpenFilled(fs, 500);
        db.Put(Key(7), []); // an empty value is found with an empty range
        int[] which = [3, -1, 7, 499, 3, -1, 0];
        ReadOnlyMemory<byte>[] keys = KeyMemories(which);
        var values = new ArrayBufferWriter<byte>();
        values.Write("prefix"u8); // ranges are relative to the bytes this call writes
        var ranges = new Range[keys.Length + 2];
        var statuses = new MultiGetStatus[keys.Length + 2];
        db.MultiGet(null, keys, values, ranges, statuses);
        Verify(values.WrittenMemory[6..]);

        // Async variant (completes synchronously: every block is cached) and a column family receiver.
        values.ResetWrittenCount();
        Array.Clear(ranges);
        Array.Clear(statuses);
        await db.DefaultColumnFamily.MultiGetAsync(null, keys, values, ranges, statuses);
        Verify(values.WrittenMemory);

        // Nothing to look up: nothing written.
        values.ResetWrittenCount();
        db.MultiGet(null, ReadOnlySpan<ReadOnlyMemory<byte>>.Empty, values, [], []);
        Assert.AreEqual(0, values.WrittenCount);

        void Verify(ReadOnlyMemory<byte> written)
        {
            for (int i = 0; i < which.Length; i++)
            {
                if (which[i] < 0)
                {
                    Assert.AreEqual(MultiGetStatus.NotFound, statuses[i]);
                    Assert.AreEqual(0, ranges[i].GetOffsetAndLength(written.Length).Length);
                    continue;
                }
                Assert.AreEqual(MultiGetStatus.Found, statuses[i]);
                byte[] expected = which[i] == 7 ? [] : Val(which[i]);
                CollectionAssert.AreEqual(expected, written.Span[ranges[i]].ToArray(), "key " + i);
            }
        }
    }

    [TestMethod]
    public void SpanMultiGetValidatesOutputsAndReportsPerKeyErrors()
    {
        MemoryFileSystem fs = NewFs();
        var cfo = new ColumnFamilyOptions { TableOptions = new BlockBasedTableOptions { NoBlockCache = true } };
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        db.Put(Key(1), Val(1));
        db.Flush();
        db.Put(Key(2), Val(2)); // memtable: found without I/O
        ReadOnlyMemory<byte>[] keys = [Key(1), Key(2)];
        var values = new ArrayBufferWriter<byte>();
        Assert.AreEqual("valueRanges", Assert.ThrowsExactly<ArgumentException>(() => db.MultiGet(null, keys, values, new Range[1], new MultiGetStatus[2])).ParamName);
        Assert.AreEqual("statuses", Assert.ThrowsExactly<ArgumentException>(() => db.MultiGet(null, keys, values, new Range[2], new MultiGetStatus[1])).ParamName);
        var ranges = new Range[2];
        var statuses = new MultiGetStatus[2];
        db.MultiGet(new ReadOptions { ReadTier = ReadTier.BlockCacheTier }, keys, values, ranges, statuses);
        Assert.AreEqual(MultiGetStatus.Error, statuses[0]);
        Assert.AreEqual(MultiGetStatus.Found, statuses[1]);
        CollectionAssert.AreEqual(Val(2), values.WrittenSpan[ranges[1]].ToArray());
        Assert.AreEqual(0, ranges[0].GetOffsetAndLength(values.WrittenCount).Length);
    }

    [TestMethod]
    public void SpanMultiGetDoesNotAllocateInSteadyState()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = OpenFilled(fs, 1000);
        var idx = new int[16];
        for (int i = 0; i < idx.Length; i++) idx[i] = i % 4 == 3 ? -1 : i * 61 % 1000;
        var state = new SpanMultiGetState(db, KeyMemories(idx));
        long allocated = MeasureAllocated(state, static s =>
        {
            s.Values.ResetWrittenCount();
            s.Db.MultiGet(null, s.Keys, s.Values, s.Ranges, s.Statuses);
        }, 1000);
        long overhead = BatchMultiGetOverhead(db, state.Keys, 1000);
        Assert.AreEqual(overhead, allocated, "bytes allocated by 1000 span MultiGet of 16 keys");
        long asyncAllocated = MeasureAllocated(state, static s =>
        {
            s.Values.ResetWrittenCount();
            ValueTask t = s.Db.MultiGetAsync(null, s.Keys, s.Values, s.Ranges, s.Statuses);
            t.GetAwaiter().GetResult();
        }, 1000);
        if (Optimized) Assert.AreEqual(0L, asyncAllocated, "bytes allocated by 1000 cached MultiGetAsync of 16 keys");
        Assert.AreEqual(MultiGetStatus.Found, state.Statuses[0]);
        CollectionAssert.AreEqual(Val(0), state.Values.WrittenSpan[state.Ranges[0]].ToArray());
    }

    private sealed class SpanMultiGetState(RocksDatabase db, ReadOnlyMemory<byte>[] keys)
    {
        public readonly RocksDatabase Db = db;
        public readonly ReadOnlyMemory<byte>[] Keys = keys;
        public readonly ArrayBufferWriter<byte> Values = new(4096);
        public readonly Range[] Ranges = new Range[keys.Length];
        public readonly MultiGetStatus[] Statuses = new MultiGetStatus[keys.Length];
    }

    [TestMethod]
    public async Task SpanMultiGetAsyncConcurrentCallers()
    {
        string dir = Path.Combine(Path.GetTempPath(), "rocksdb-managed-mgasync-" + Guid.NewGuid().ToString("N"));
        try
        {
            // A real file system and a tiny block cache, so lookups miss the cache and take the async read path.
            var cfo = new ColumnFamilyOptions
            {
                TableOptions = new BlockBasedTableOptions { BlockCache = Managed.Cache.BlockCache.CreateLru(new Managed.Cache.LruCacheOptions { Capacity = 4096 }) },
            };
            using RocksDatabase db = RocksDatabase.Open(dir, new DbOptions { CreateIfMissing = true }, cfo);
            for (int i = 0; i < 2000; i++) db.Put(Key(i), Val(i));
            db.Flush();
            await Task.WhenAll(Enumerable.Range(0, 8).Select(t => Task.Run(async () =>
            {
                var values = new ArrayBufferWriter<byte>();
                var ranges = new Range[16];
                var statuses = new MultiGetStatus[16];
                var idx = new int[16];
                for (int round = 0; round < 50; round++)
                {
                    for (int i = 0; i < 16; i++) idx[i] = (t * 977 + round * 31 + i * 113) % 2000;
                    values.ResetWrittenCount();
                    await db.MultiGetAsync(null, KeyMemories(idx), values, ranges, statuses);
                    for (int i = 0; i < 16; i++)
                    {
                        Assert.AreEqual(MultiGetStatus.Found, statuses[i]);
                        CollectionAssert.AreEqual(Val(idx[i]), values.WrittenSpan[ranges[i]].ToArray());
                    }
                }
            })));
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch (IOException) { }
        }
    }

    // =====================================================================================================
    // Lazy entities: the pin of a reused result is re-armed
    // =====================================================================================================

    private static WideColumn C(string name, string value) => new(B(name), B(value));

    [TestMethod]
    public void TryGetEntityLazyWithReusedResultDoesNotAllocate()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions());
        for (int i = 0; i < 500; i++) db.PutEntity(Key(i), [C("", "d"), C("a", "va"), C("b", "vb")]);
        db.Flush();
        using var lazy = new LazyWideColumns();
        var state = (db.DefaultColumnFamily, Key(42), lazy);
        long allocated = MeasureAllocated(state, static s =>
        {
            if (!s.Item1.TryGetEntityLazy(null, s.Item2, s.lazy) || s.lazy.Count != 3) throw new InvalidOperationException();
        }, 2000);
        Assert.AreEqual(0L, allocated, "bytes allocated by 2000 TryGetEntityLazy into a reused result");
        Assert.IsTrue(lazy.TryGetInlineValue(1, out ReadOnlySpan<byte> va));
        Assert.AreEqual("va", Encoding.UTF8.GetString(va));
    }

    [TestMethod]
    public void ReusedLazyResultsReleaseAndRepinTheirVersion()
    {
        MemoryFileSystem fs = NewFs();
        var cfo = new ColumnFamilyOptions { EnableBlobFiles = true, MinBlobSize = 16, DisableAutoCompactions = true };
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        string big1 = new('x', 64), big2 = new('y', 64);
        db.PutEntity(B("k"), [C("", "d"), C("big", big1)]);
        db.Flush();
        ColumnFamily cf = db.DefaultColumnFamily;
        Assert.AreEqual(1UL, db.GetIntProperty(DbProperties.NumLiveVersions));

        using var lazy = new LazyWideColumns();
        using var batch = new LazyWideColumnsBatch();
        using var keys = new MultiGetBatch();
        keys.Add(B("k")).Add(B("none"));
        for (int round = 0; round < 3; round++)
        {
            string expected = round == 0 ? big1 : big2;
            Assert.IsTrue(cf.TryGetEntityLazy(null, B("k"), lazy));
            cf.MultiGetEntityLazy(null, keys, batch);
            Assert.AreEqual(MultiGetStatus.Found, batch.GetStatus(0));
            Assert.AreEqual(MultiGetStatus.NotFound, batch.GetStatus(1));
            LazyWideColumns entry = batch[0];
            // Rewrite the blob and compact: the results pin the old version (and its blob file) until reset.
            db.PutEntity(B("k"), [C("", "d"), C("big", big2)]);
            db.Flush();
            db.CompactRange(new CompactRangeOptions { BottommostLevelCompaction = BottommostLevelCompaction.Force }, null, null);
            Assert.IsTrue(db.GetIntProperty(DbProperties.NumLiveVersions) > 1);
            Assert.AreEqual(expected, Encoding.UTF8.GetString(lazy.ResolveColumn(lazy.IndexOf("big"u8))));
            Assert.AreEqual(expected, Encoding.UTF8.GetString(entry.ResolveColumn(entry.IndexOf("big"u8))));
            lazy.Reset();
            batch.Reset();
            Assert.AreEqual(1UL, db.GetIntProperty(DbProperties.NumLiveVersions), "round " + round);
            // A reset result (and a batch entry after its batch's reset) is released, not silently re-armed.
            Assert.AreEqual(0, lazy.Count);
            Assert.ThrowsExactly<ArgumentOutOfRangeException>(() => lazy.ResolveColumn(0));
            Assert.AreEqual(0, entry.Count);
        }
        // A miss releases the pin at once (nothing to keep alive).
        Assert.IsFalse(cf.TryGetEntityLazy(null, B("none"), lazy));
        Assert.AreEqual(1UL, db.GetIntProperty(DbProperties.NumLiveVersions));
        // Disposing a result that holds a pin releases it.
        Assert.IsTrue(cf.TryGetEntityLazy(null, B("k"), lazy));
        db.Put(B("other"), B("v"));
        db.Flush();
        Assert.IsTrue(db.GetIntProperty(DbProperties.NumLiveVersions) > 1);
        lazy.Dispose();
        Assert.AreEqual(1UL, db.GetIntProperty(DbProperties.NumLiveVersions));
        Assert.ThrowsExactly<ObjectDisposedException>(() => cf.TryGetEntityLazy(null, B("k"), lazy));
        Assert.AreEqual(1UL, db.GetIntProperty(DbProperties.NumLiveVersions));
    }

    [TestMethod]
    public void GetEntityAllocatesOnlyTheResult()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions());
        db.PutEntity(B("k"), [C("", "d"), C("a", "va"), C("b", "vb")]);
        db.Flush();
        long allocated = MeasureAllocated(db, static d => d.GetEntity(B("k")), 1000);
        // The key array, the WideColumns object with its buffer and slot list (sized once for the 3 columns).
        Assert.IsTrue(allocated / 1000 <= 320, "bytes per GetEntity: " + allocated / 1000);
        WideColumns? c = db.GetEntity(B("k"));
        Assert.AreEqual(3, c!.Count);
    }

    // =====================================================================================================
    // Numeric properties
    // =====================================================================================================

    private static IEnumerable<string> AllPropertyNames()
    {
        foreach (FieldInfo f in typeof(DbProperties).GetFields(BindingFlags.Public | BindingFlags.Static))
        {
            if (f.IsLiteral && f.FieldType == typeof(string) && !f.Name.EndsWith("Prefix", StringComparison.Ordinal)) yield return (string)f.GetRawConstantValue()!;
        }
        yield return DbProperties.NumFilesAtLevel(0);
        yield return DbProperties.NumFilesAtLevel(6);
        yield return DbProperties.NumFilesAtLevel(7); // beyond num_levels
        yield return DbProperties.NumFilesAtLevelPrefix + "x";
        yield return DbProperties.LiveSstFilesSizeAtTemperaturePrefix + "0";
        yield return DbProperties.LiveSstFilesSizeAtTemperaturePrefix + "x";
        yield return DbProperties.CompressionRatioAtLevelPrefix + "0";
        yield return "rocksdb.no-such-property";
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void IntPropertiesMatchTheirStringForm(bool noBlockCache)
    {
        MemoryFileSystem fs = NewFs();
        var cfo = new ColumnFamilyOptions { DisableAutoCompactions = true, TableOptions = new BlockBasedTableOptions { NoBlockCache = noBlockCache } };
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        for (int i = 0; i < 300; i++) db.Put(Key(i), Val(i));
        db.Flush();
        for (int i = 0; i < 100; i++) db.Delete(Key(i));
        using Snapshot snap = db.GetSnapshot();
        int numeric = 0;
        foreach (string p in AllPropertyNames())
        {
            string? s = db.GetProperty(p);
            bool isInt = db.TryGetIntProperty(p, out ulong v);
            bool parses = ulong.TryParse(s, System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out ulong parsed);
            Assert.AreEqual(parses, isInt, p + " = " + s);
            if (isInt)
            {
                Assert.AreEqual(parsed, v, p);
                numeric++;
            }
        }
        Assert.IsTrue(numeric > 40, "numeric properties: " + numeric);
        Assert.AreEqual(noBlockCache, db.GetProperty(DbProperties.BlockCacheCapacity) is null);
        Assert.IsFalse(db.TryGetIntProperty(DbProperties.NumFilesAtLevel(7), out _));
        Assert.IsTrue(db.TryGetIntProperty(DbProperties.NumFilesAtLevel(0), out ulong l0));
        Assert.AreEqual(1UL, l0);
        Assert.IsTrue(db.TryGetIntProperty(DbProperties.NumSnapshots, out ulong snaps));
        Assert.AreEqual(1UL, snaps);
        Assert.IsTrue(db.TryGetAggregatedIntProperty(DbProperties.NumEntriesActiveMemTable, out ulong active));
        Assert.AreEqual(100UL, active);
        Assert.IsFalse(db.TryGetIntProperty(DbProperties.Stats, out _));
    }

    [TestMethod]
    public void IntPropertiesDoNotAllocate()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = OpenFilled(fs, 500);
        db.CreateColumnFamily("second", new ColumnFamilyOptions());
        db.Put(Key(1), Val(1));
        string[] properties =
        [
            DbProperties.EstimateNumKeys, DbProperties.CurSizeAllMemTables, DbProperties.SizeAllMemTables, DbProperties.EstimateLiveDataSize,
            DbProperties.BlockCacheUsage, DbProperties.NumLiveVersions, DbProperties.TotalSstFilesSize, DbProperties.LiveSstFilesSize,
            DbProperties.EstimatePendingCompactionBytes, DbProperties.NumRunningCompactions, DbProperties.EstimateTableReadersMem,
            DbProperties.NumFilesAtLevel(0), DbProperties.MinObsoleteSstNumberToKeep, DbProperties.IsWriteStopped,
        ];
        var perProperty = new StringBuilder();
        long allocated = 0;
        foreach (string p in properties)
        {
            long a = MeasureAllocated((db, p), static s =>
            {
                if (!s.db.TryGetIntProperty(s.p, out _)) throw new InvalidOperationException(s.p);
            }, 200);
            if (a != 0) perProperty.Append(p).Append(": ").Append(a / 200).Append(" B; ");
            allocated += a;
        }
        allocated += MeasureAllocated(db, static d =>
        {
            if (!d.TryGetAggregatedIntProperty(DbProperties.CurSizeAllMemTables, out _)) throw new InvalidOperationException();
        }, 200);
        Assert.AreEqual(0L, allocated, "bytes allocated by numeric property reads: " + perProperty);
    }

    // =====================================================================================================
    // Column family list, live files, approximate sizes, histograms
    // =====================================================================================================

    [TestMethod]
    public void ColumnFamiliesIsACachedImmutableSnapshot()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions());
        ColumnFamily a = db.CreateColumnFamily("a", new ColumnFamilyOptions());
        ColumnFamily b = db.CreateColumnFamily("b", new ColumnFamilyOptions());
        IReadOnlyList<ColumnFamily> first = db.ColumnFamilies;
        Assert.AreSame(first, db.ColumnFamilies);
        CollectionAssert.AreEqual(new[] { "default", "a", "b" }, first.Select(c => c.Name).ToArray());
        Assert.AreSame(a, first[1]);
        Assert.IsInstanceOfType<IList<ColumnFamily>>(first);
        Assert.ThrowsExactly<NotSupportedException>(() => ((IList<ColumnFamily>)first)[0] = b);
        Assert.AreEqual(0L, MeasureAllocated(db, static d => _ = d.ColumnFamilies[0], 1000));

        db.DropColumnFamily(a);
        IReadOnlyList<ColumnFamily> second = db.ColumnFamilies;
        Assert.AreNotSame(first, second);
        CollectionAssert.AreEqual(new[] { "default", "b" }, second.Select(c => c.Name).ToArray());
        Assert.AreEqual(3, first.Count); // an older snapshot is not changed
        ColumnFamily c = db.CreateColumnFamily("c", new ColumnFamilyOptions());
        CollectionAssert.AreEqual(new[] { "default", "b", "c" }, db.ColumnFamilies.Select(x => x.Name).ToArray());
        IReadOnlyList<ColumnFamily> third = db.ColumnFamilies;
        Assert.AreSame(c, third[2]);
        Assert.AreSame(third, db.ColumnFamilies);
    }

    private static RocksDatabase OpenWithFiles(MemoryFileSystem fs)
    {
        var cfo = new ColumnFamilyOptions { DisableAutoCompactions = true };
        RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        ColumnFamily second = db.CreateColumnFamily("second", cfo);
        for (int f = 0; f < 4; f++)
        {
            for (int i = 0; i < 200; i++)
            {
                db.Put(Key(f * 1000 + i), Val(i));
                second.Put(Key(f * 1000 + i), Val(i));
            }
            db.Flush();
            second.Flush();
        }
        return db;
    }

    [TestMethod]
    public void LiveFilesRefillAndVisitMatchTheListApi()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = OpenWithFiles(fs);
        List<LiveFileMetadata> expected = db.GetLiveFilesMetadata();
        Assert.AreEqual(8, expected.Count);
        var reused = new List<LiveFileMetadata> { expected[0], expected[1] }; // stale entries are replaced
        db.GetLiveFilesMetadata(reused);
        Assert.AreEqual(expected.Count, reused.Count);
        var visited = new List<LiveFileMetadata>();
        db.VisitLiveFiles(visited, static (in LiveFileInfo f, List<LiveFileMetadata> list) => list.Add(f.ToMetadata()));
        Assert.AreEqual(expected.Count, visited.Count);
        for (int i = 0; i < expected.Count; i++)
        {
            AssertSameFile(expected[i], reused[i]);
            AssertSameFile(expected[i], visited[i]);
        }
        // Field by field from the ref struct.
        var firstOfSecond = new StrongBox<string>();
        db.VisitLiveFiles(firstOfSecond, static (in LiveFileInfo f, StrongBox<string> box) =>
        {
            if (box.Value is null && f.ColumnFamilyName == "second")
            {
                box.Value = f.FileNumber + "|" + f.Level + "|" + f.Size + "|" + Encoding.UTF8.GetString(f.SmallestUserKey) + "|"
                    + Encoding.UTF8.GetString(f.LargestUserKey) + "|" + f.ColumnFamilyId + "|" + f.Directory + "|" + f.SmallestInternalKey.Length;
            }
        });
        LiveFileMetadata m = expected.First(x => x.ColumnFamilyName == "second");
        Assert.AreEqual(m.FileNumber + "|" + m.Level + "|" + m.Size + "|" + Encoding.UTF8.GetString(m.SmallestKey) + "|" + Encoding.UTF8.GetString(m.LargestKey)
            + "|" + db.GetColumnFamily("second").Id + "|" + m.Directory + "|" + m.SmallestInternalKey.Length, firstOfSecond.Value);
        // An exception from the visitor stops the visit, propagates and releases the pinned version.
        var count = new StrongBox<int>();
        Assert.ThrowsExactly<InvalidOperationException>(() => db.VisitLiveFiles(count, static (in LiveFileInfo f, StrongBox<int> c) =>
        {
            if (++c.Value == 3) throw new InvalidOperationException("stop");
        }));
        Assert.AreEqual(3, count.Value);
        Assert.AreEqual(1UL, db.GetIntProperty(DbProperties.NumLiveVersions));
    }

    private static void AssertSameFile(LiveFileMetadata a, LiveFileMetadata b)
    {
        Assert.AreEqual(a.Name, b.Name);
        Assert.AreEqual(a.Directory, b.Directory);
        Assert.AreEqual(a.FileNumber, b.FileNumber);
        Assert.AreEqual(a.ColumnFamilyName, b.ColumnFamilyName);
        Assert.AreEqual(a.Level, b.Level);
        Assert.AreEqual(a.Size, b.Size);
        CollectionAssert.AreEqual(a.SmallestKey, b.SmallestKey);
        CollectionAssert.AreEqual(a.LargestKey, b.LargestKey);
        Assert.AreEqual(a.SmallestSeqno, b.SmallestSeqno);
        Assert.AreEqual(a.LargestSeqno, b.LargestSeqno);
        Assert.AreEqual(a.EpochNumber, b.EpochNumber);
        Assert.AreEqual(a.Temperature, b.Temperature);
        CollectionAssert.AreEqual(a.FileChecksum, b.FileChecksum);
        Assert.AreEqual(a.FileChecksumFuncName, b.FileChecksumFuncName);
        Assert.AreEqual(a.FileCreationTime, b.FileCreationTime);
        Assert.AreEqual(a.OldestAncestorTime, b.OldestAncestorTime);
        CollectionAssert.AreEqual(a.SmallestInternalKey, b.SmallestInternalKey);
        CollectionAssert.AreEqual(a.LargestInternalKey, b.LargestInternalKey);
        Assert.AreEqual(a.OldestBlobFileNumber, b.OldestBlobFileNumber);
    }

    [TestMethod]
    public void VisitLiveFilesAndRefillAllocateLittle()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = OpenWithFiles(fs);
        var total = new StrongBox<ulong>();
        long allocated = MeasureAllocated((db, total), static s =>
            s.db.VisitLiveFiles(s.total, static (in LiveFileInfo f, StrongBox<ulong> t) => t.Value += f.Size + (ulong)f.SmallestUserKey.Length), 500);
        Assert.AreEqual(0L, allocated, "bytes allocated by 500 VisitLiveFiles over 8 files");
        // The refill overload reuses the list: only the per-file records remain.
        var list = new List<LiveFileMetadata>();
        long refill = MeasureAllocated((db, list), static s => s.db.GetLiveFilesMetadata(s.list), 200);
        long fresh = MeasureAllocated(db, static d => d.GetLiveFilesMetadata(), 200);
        Assert.IsTrue(refill < fresh, "refill " + refill / 200 + " B, new list " + fresh / 200 + " B");
    }

    [TestMethod]
    public void SpanApproximateSizesMatchAndDoNotAllocate()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = OpenWithFiles(fs);
        db.Put(Key(5), Val(5)); // memtable data
        KeyRange[] ranges = [new(Key(0), Key(2500)), new(Key(100), Key(150)), new(B("zz"), B("zzz")), new(Key(3000), Key(0)), new(new byte[600], Key(1))];
        var options = new SizeApproximationOptions { IncludeMemtables = true, IncludeFiles = true };
        foreach (SizeApproximationOptions o in new[] { new SizeApproximationOptions(), options, new SizeApproximationOptions { FilesSizeErrorMargin = 0.5 } })
        {
            ulong[] expected = db.GetApproximateSizes(o, ranges);
            var sizes = new ulong[ranges.Length];
            db.GetApproximateSizes(o, ranges, sizes);
            CollectionAssert.AreEqual(expected, sizes);
        }
        var plain = new ulong[ranges.Length];
        db.GetApproximateSizes(ranges, plain);
        CollectionAssert.AreEqual(db.GetApproximateSizes(ranges), plain);
        Assert.IsTrue(plain[0] > 0);
        Assert.AreEqual(plain[0], db.DefaultColumnFamily.GetApproximateSize(Key(0), Key(2500)));
        Assert.AreEqual("sizes", Assert.ThrowsExactly<ArgumentException>(() => db.GetApproximateSizes(ranges, new ulong[1])).ParamName);
        var none = new SizeApproximationOptions { IncludeFiles = false };
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => db.GetApproximateSizes(none, ranges, plain)).Code);

        var state = (db, few: ranges[..3], sizes: new ulong[3], options, start: Key(0), limit: Key(9));
        long files = MeasureAllocated(state, static s => s.db.GetApproximateSizes(s.few, s.sizes), 200);
        long withMemtables = MeasureAllocated(state, static s => s.db.GetApproximateSizes(s.options, s.few, s.sizes), 200);
        long single = MeasureAllocated(state, static s => _ = s.db.DefaultColumnFamily.GetApproximateSize(s.start, s.limit), 200);
        Assert.AreEqual("0/0/0", files + "/" + withMemtables + "/" + single, "bytes allocated by 200 calls (files / files+memtables / one range)");
    }

    [TestMethod]
    public void HistogramDataDoesNotAllocate()
    {
        var stats = new Statistics();
        for (ulong v = 1; v <= 1000; v++) stats.RecordInHistogram(Histogram.DbGet, v);
        HistogramData d = stats.GetHistogramData(Histogram.DbGet);
        Assert.AreEqual(1000UL, d.Count);
        Assert.AreEqual(500500UL, d.Sum);
        Assert.AreEqual(1000.0, d.Max);
        Assert.IsTrue(d.Median > 400 && d.Median < 600, "median " + d.Median);
        Assert.AreEqual(0L, MeasureAllocated(stats, static s => s.GetHistogramData(Histogram.DbGet), 1000));
        Assert.AreEqual(0UL, stats.GetHistogramData(Histogram.DbWrite).Count);
    }

    // =====================================================================================================
    // Open: lazily materialized HyperClock slots, one listing for the obsolete-file scan and the summary
    // =====================================================================================================

    [TestMethod]
    public void HyperClockTableSlotsAreMaterializedOnFirstUse()
    {
        long before = GC.GetAllocatedBytesForCurrentThread();
        Managed.Cache.BlockCache cache = Managed.Cache.BlockCache.CreateDefault(32L << 20, 4096);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        // The slot arrays (8 B per slot reference) only: no slot object per table position up front.
        Assert.IsTrue(allocated < 256 << 10, "bytes allocated by a 32 MiB HyperClock cache: " + allocated);
        GC.KeepAlive(cache);
    }

    [TestMethod]
    public void OpenSummaryListsTheDbDirectory()
    {
        string dir = Path.Combine(Path.GetTempPath(), "rocksdb-managed-summary-" + Guid.NewGuid().ToString("N"));
        try
        {
            using (RocksDatabase db = RocksDatabase.Open(dir, new DbOptions { CreateIfMissing = true }))
            {
                db.Put(B("a"), B("1"));
                db.Flush();
                db.Put(B("b"), B("2"));
                db.Flush();
            }
            File.WriteAllText(Path.Combine(dir, ".hidden"), "x");
            Directory.CreateDirectory(Path.Combine(dir, "subdir"));
            List<string> children = [.. LocalFileSystem.Instance.GetChildren(dir)];
            CollectionAssert.Contains(children, ".hidden");
            CollectionAssert.Contains(children, "subdir");
            CollectionAssert.Contains(children, "CURRENT");
            foreach (string c in children) Assert.IsFalse(c.Contains('/'), c);
            CollectionAssert.AreEquivalent(Directory.GetFileSystemEntries(dir).Select(Path.GetFileName).ToArray(), children);
            Assert.AreEqual(StatusCode.IOError, Assert.ThrowsExactly<RocksDbException>(() => LocalFileSystem.Instance.GetChildren(Path.Combine(dir, "nope"))).Code);

            using (RocksDatabase db = RocksDatabase.Open(dir, new DbOptions()))
            {
                Assert.AreEqual("2", Encoding.UTF8.GetString(db.Get(B("b"))!));
            }
            string log = File.ReadAllText(Path.Combine(dir, "LOG"));
            StringAssert.Contains(log, "CURRENT file:  CURRENT");
            StringAssert.Contains(log, "IDENTITY file:  IDENTITY");
            StringAssert.Contains(log, "MANIFEST file:  MANIFEST-");
            StringAssert.Contains(log, "SST files in " + dir + " dir, Total Num: 2, files: ");
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch (IOException) { }
        }
    }
}
