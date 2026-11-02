using System.Buffers;
using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Db.Wide;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Features;

/// <summary>
/// The wide-column APIs beyond PutEntity/GetEntity: FilterV4 with a lazy blob resolver and FilterBlobByKey
/// (db/compaction/compaction_iterator.cc, db_wide_basic_test.cc / db_blob_compaction_test.cc), MultiGetEntity, attribute
/// groups (db_wide_basic_test.cc MultiCFMultiGetEntityAsPinnableAttributeGroups, PutEntityAttributeGroups), the coalescing
/// and attribute-group iterators (db/multi_cf_iterator_test.cc) and the lazy entity reads (db/wide/db_lazy_entity_test.cc).
/// </summary>
[TestClass]
public sealed class WideColumnApiTests
{
    private static WideColumn C(string name, string value) => new(B(name), B(value));

    private static string Large(int n, char c) => new(c, n);

    private static string Describe(WideColumns c)
    {
        var sb = new StringBuilder();
        for (int i = 0; i < c.Count; i++) sb.Append(Encoding.UTF8.GetString(c.GetName(i))).Append('=').Append(Encoding.UTF8.GetString(c.GetValue(i))).Append(';');
        return sb.ToString();
    }

    private static ColumnFamilyOptions BlobOptions(ulong minBlobSize) => new()
    {
        EnableBlobFiles = true,
        MinBlobSize = minBlobSize,
        DisableAutoCompactions = true,
        MergeOperator = new RocksDb.Managed.Tests.Db.StringAppendMergeOperator(),
    };

    // =====================================================================================================
    // FilterV4 / FilterBlobByKey
    // =====================================================================================================

    /// <summary>A FilterV4 filter that resolves only the column named "check" and decides on its value.</summary>
    private sealed class LazyFilter(bool supportsV4) : CompactionFilter
    {
        public int EntityCalls;
        public int ResolverCalls;
        public int BlobColumnsSeen;
        public int MergeOperandCalls;
        public override string Name => "LazyFilter";
        public override bool SupportsFilterV4 => supportsV4;

        public override CompactionFilterDecision Filter(int level, ReadOnlySpan<byte> key, CompactionFilterValueType valueType,
            ReadOnlySpan<byte> existingValue, IBufferWriter<byte> newValue, IBufferWriter<byte> skipUntil) => CompactionFilterDecision.Keep;

        public override CompactionFilterDecision FilterV4(int level, ReadOnlySpan<byte> key, CompactionFilterValueType valueType,
            ReadOnlySpan<byte> existingValue, WideColumns? existingColumns, IBufferWriter<byte> newValue, List<WideColumn> newColumns,
            IBufferWriter<byte> skipUntil, WideColumnBlobResolver? blobResolver)
        {
            if (valueType == CompactionFilterValueType.MergeOperand)
            {
                MergeOperandCalls++;
                return existingValue.SequenceEqual("drop"u8) ? CompactionFilterDecision.Remove : CompactionFilterDecision.Keep;
            }
            if (valueType != CompactionFilterValueType.WideColumnEntity) return CompactionFilterDecision.Keep;
            EntityCalls++;
            if (blobResolver is not null) ResolverCalls++;
            for (int i = 0; i < existingColumns!.Count; i++)
            {
                if (blobResolver?.IsBlobColumn(i) == true) BlobColumnsSeen++;
                if (!existingColumns.GetName(i).SequenceEqual("check"u8)) continue;
                ReadOnlySpan<byte> v = blobResolver is not null ? blobResolver.ResolveColumn(i) : existingColumns.GetValue(i);
                // Resolving twice returns the cached value.
                if (blobResolver is not null) Assert.IsTrue(v.SequenceEqual(blobResolver.ResolveColumn(i)));
                if (v.Length > 0 && v[0] == (byte)'r') return CompactionFilterDecision.Remove;
                if (v.Length > 0 && v[0] == (byte)'c')
                {
                    newColumns.Add(C("check", "kept"));
                    return CompactionFilterDecision.ChangeWideColumnEntity;
                }
            }
            return CompactionFilterDecision.Keep;
        }
    }

    [TestMethod]
    public void FilterV4LazyResolverReadsOnlyRequestedBlobColumns()
    {
        foreach (bool v4 in new[] { true, false })
        {
            MemoryFileSystem fs = NewFs();
            ColumnFamilyOptions cf = BlobOptions(32);
            var filter = new LazyFilter(v4);
            cf.CompactionFilter = filter;
            var st = new Statistics();
            DbOptions o = NewOptions(fs);
            o.Statistics = st;
            using RocksDatabase db = RocksDatabase.Open(DbPath, o, cf);
            db.PutEntity(B("a_remove"), [C("check", Large(40, 'r')), C("big", Large(100, 'x'))]);
            db.PutEntity(B("b_change"), [C("check", Large(40, 'c')), C("big", Large(100, 'y'))]);
            db.PutEntity(B("c_keep"), [C("check", Large(40, 'k')), C("big", Large(100, 'z')), C("small", "s")]);
            db.PutEntity(B("d_inline"), [C("check", "r")]); // no blob columns: no resolver
            db.Flush();
            db.CompactRange();
            Assert.AreEqual(4, filter.EntityCalls, "v4=" + v4);
            Assert.AreEqual(v4 ? 3 : 0, filter.ResolverCalls, "v4=" + v4);
            Assert.AreEqual(v4 ? 6 : 0, filter.BlobColumnsSeen, "v4=" + v4);
            Assert.IsNull(db.GetEntity(B("a_remove")));
            Assert.IsNull(db.GetEntity(B("d_inline")));
            Assert.AreEqual("check=kept;", Describe(db.GetEntity(B("b_change"))!));
            Assert.AreEqual("big=" + Large(100, 'z') + ";check=" + Large(40, 'k') + ";small=s;", Describe(db.GetEntity(B("c_keep"))!));
        }
    }

    /// <summary>A filter whose resolver call fails and that swallows the exception: the compaction still fails.</summary>
    private sealed class SwallowingFilter : CompactionFilter
    {
        public override string Name => "Swallowing";
        public override bool SupportsFilterV4 => true;

        public override CompactionFilterDecision Filter(int level, ReadOnlySpan<byte> key, CompactionFilterValueType valueType,
            ReadOnlySpan<byte> existingValue, IBufferWriter<byte> newValue, IBufferWriter<byte> skipUntil) => CompactionFilterDecision.Keep;

        public override CompactionFilterDecision FilterV4(int level, ReadOnlySpan<byte> key, CompactionFilterValueType valueType,
            ReadOnlySpan<byte> existingValue, WideColumns? existingColumns, IBufferWriter<byte> newValue, List<WideColumn> newColumns,
            IBufferWriter<byte> skipUntil, WideColumnBlobResolver? blobResolver)
        {
            if (blobResolver is null) return CompactionFilterDecision.Keep;
            try
            {
                blobResolver.ResolveColumn(blobResolver.ColumnCount); // out of bounds
            }
            catch (RocksDbException)
            {
                // ignored on purpose
            }
            return CompactionFilterDecision.Keep;
        }
    }

    [TestMethod]
    public void FilterV4ResolverErrorFailsCompaction()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cf = BlobOptions(16);
        cf.CompactionFilter = new SwallowingFilter();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cf);
        db.PutEntity(B("k"), [C("a", Large(40, 'a'))]);
        db.Flush();
        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => db.CompactRange());
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        Assert.AreEqual(Large(40, 'a'), Encoding.UTF8.GetString(db.GetEntity(B("k"))!.GetValue(0)));
    }

    /// <summary>FilterBlobByKey decides by key for blob values; undetermined keys reach FilterV4 as plain values.</summary>
    private sealed class ByKeyFilter : CompactionFilter
    {
        public int ByKeyCalls;
        public int ValueCalls;
        public string? SeenValue;
        public override string Name => "ByKey";

        public override CompactionFilterDecision FilterBlobByKey(int level, ReadOnlySpan<byte> key, IBufferWriter<byte> newValue, IBufferWriter<byte> skipUntil)
        {
            ByKeyCalls++;
            if (key.SequenceEqual("drop"u8)) return CompactionFilterDecision.Remove;
            if (key.SequenceEqual("change"u8))
            {
                newValue.Write("changed by key"u8);
                return CompactionFilterDecision.ChangeValue;
            }
            return CompactionFilterDecision.Undetermined;
        }

        public override CompactionFilterDecision Filter(int level, ReadOnlySpan<byte> key, CompactionFilterValueType valueType,
            ReadOnlySpan<byte> existingValue, IBufferWriter<byte> newValue, IBufferWriter<byte> skipUntil)
        {
            ValueCalls++;
            Assert.AreEqual(CompactionFilterValueType.Value, valueType);
            if (key.SequenceEqual("read"u8)) SeenValue = Encoding.UTF8.GetString(existingValue);
            return CompactionFilterDecision.Keep;
        }
    }

    [TestMethod]
    public void FilterBlobByKeyAvoidsBlobReads()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cf = BlobOptions(16);
        var filter = new ByKeyFilter();
        cf.CompactionFilter = filter;
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cf);
        db.Put(B("change"), B(Large(40, 'c')));
        db.Put(B("drop"), B(Large(40, 'd')));
        db.Put(B("read"), B(Large(40, 'r')));
        db.Put(B("small"), B("s")); // inline: no FilterBlobByKey
        db.Flush();
        db.CompactRange();
        Assert.AreEqual(3, filter.ByKeyCalls);
        Assert.AreEqual(2, filter.ValueCalls);
        Assert.AreEqual(Large(40, 'r'), filter.SeenValue);
        Assert.IsNull(GetString(db, "drop"));
        Assert.AreEqual("changed by key", GetString(db, "change"));
        Assert.AreEqual(Large(40, 'r'), GetString(db, "read"));
    }

    [TestMethod]
    public void MergeOperandsReachFilterV4()
    {
        // MergeHelper::FilterMerge calls FilterV4 with kMergeOperand.
        MemoryFileSystem fs = NewFs();
        var filter = new LazyFilter(true);
        var cf = new ColumnFamilyOptions { DisableAutoCompactions = true, MergeOperator = new RocksDb.Managed.Tests.Db.StringAppendMergeOperator(), CompactionFilter = filter };
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cf);
        db.Put(B("m"), B("base"));
        db.Merge(B("m"), B("keep"));
        db.Merge(B("m"), B("drop"));
        db.Flush();
        db.CompactRange();
        Assert.AreEqual(2, filter.MergeOperandCalls);
        Assert.AreEqual("base,keep", GetString(db, "m"));
    }

    // =====================================================================================================
    // MultiGetEntity
    // =====================================================================================================

    [TestMethod]
    public async Task MultiGetEntityReturnsEntitiesAndDefaultColumns()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), BlobOptions(32));
        db.PutEntity(B("e1"), [C("", "d1"), C("x", Large(50, 'x'))]);
        db.Put(B("plain"), B("pv"));
        db.PutEntity(B("e2"), [C("only", "o")]);
        db.Flush();
        db.Merge(B("e1"), B("m"));
        db.PutEntity(B("mem"), [C("a", "1")]);
        using var batch = new MultiGetBatch();
        batch.Add(B("plain")).Add(B("missing")).Add(B("e1")).Add(B("mem")).Add(B("e2"));
        var cols = new WideColumns();
        for (int round = 0; round < 2; round++)
        {
            if (round == 0) db.MultiGetEntity(batch);
            else await db.MultiGetEntityAsync(null, batch);
            Assert.AreEqual(4, batch.FoundCount);
            Assert.IsTrue(batch.TryGetEntity(0, cols));
            Assert.AreEqual("=pv;", Describe(cols));
            Assert.IsFalse(batch.TryGetEntity(1, cols));
            Assert.AreEqual(0, cols.Count);
            Assert.IsTrue(batch.TryGetEntity(2, cols));
            Assert.AreEqual("=d1,m;x=" + Large(50, 'x') + ";", Describe(cols));
            Assert.AreEqual("d1,m", Encoding.UTF8.GetString(batch.GetValue(2)));
            Assert.AreEqual("d1,m", Encoding.UTF8.GetString(batch.GetValueMemory(2).Span));
            Assert.IsTrue(batch.TryGetEntity(3, cols));
            Assert.AreEqual("a=1;", Describe(cols));
            Assert.AreEqual(0, batch.GetValue(4).Length); // no default column
        }
        // A plain MultiGet afterwards on the same batch returns plain values again.
        db.MultiGet(batch);
        Assert.AreEqual("d1,m", Encoding.UTF8.GetString(batch.GetValue(2)));
        Assert.ThrowsExactly<InvalidOperationException>(() => batch.TryGetEntity(2, cols));
    }

    // =====================================================================================================
    // Attribute groups
    // =====================================================================================================

    private static RocksDatabase OpenWithCfs(MemoryFileSystem fs, out ColumnFamily hot, out ColumnFamily cold)
    {
        var o = NewOptions(fs);
        o.CreateMissingColumnFamilies = true;
        RocksDatabase db = RocksDatabase.Open(DbPath, o,
            [new("default", new ColumnFamilyOptions()), new("hot", new ColumnFamilyOptions()), new("cold", BlobOptions(16))]);
        hot = db.GetColumnFamily("hot");
        cold = db.GetColumnFamily("cold");
        return db;
    }

    [TestMethod]
    public void AttributeGroupsPutGetMultiGet()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = OpenWithCfs(fs, out ColumnFamily hot, out ColumnFamily cold);
        db.PutAttributeGroups(B("k1"), [new AttributeGroup(hot, new[] { C("", "h1"), C("n", "1") }), new AttributeGroup(cold, new[] { C("blob", Large(40, 'b')) })]);
        db.PutAttributeGroups(B("k2"), [new AttributeGroup(hot, new[] { C("n", "2") })]);
        db.DefaultColumnFamily.Put(B("k1"), B("default only"));
        Assert.AreEqual("h1", GetString(hot, "k1"));
        Assert.AreEqual(StatusCode.InvalidArgument,
            Assert.ThrowsExactly<RocksDbException>(() => db.PutAttributeGroups(B("x"), ReadOnlySpan<AttributeGroup>.Empty)).Code);

        PinnableAttributeGroup[] groups = [new(hot), new(cold)];
        for (int round = 0; round < 2; round++)
        {
            db.GetAttributeGroups(null, B("k1"), groups);
            Assert.AreEqual(MultiGetStatus.Found, groups[0].Status);
            Assert.AreEqual("=h1;n=1;", Describe(groups[0].Columns));
            Assert.AreEqual(MultiGetStatus.Found, groups[1].Status);
            Assert.AreEqual("blob=" + Large(40, 'b') + ";", Describe(groups[1].Columns));
            hot.Flush();
            cold.Flush();
        }
        db.GetAttributeGroups(null, B("k2"), groups);
        Assert.AreEqual(MultiGetStatus.Found, groups[0].Status);
        Assert.AreEqual(MultiGetStatus.NotFound, groups[1].Status);
        Assert.AreEqual(0, groups[1].Columns.Count);

        PinnableAttributeGroup[][] results = [[new(hot), new(cold)], [new(cold)], [new(hot), new(db.DefaultColumnFamily)]];
        db.MultiGetAttributeGroups(null, [B("k1"), B("k2"), B("k1")], results);
        Assert.AreEqual("=h1;n=1;", Describe(results[0][0].Columns));
        Assert.AreEqual("blob=" + Large(40, 'b') + ";", Describe(results[0][1].Columns));
        Assert.AreEqual(MultiGetStatus.NotFound, results[1][0].Status);
        Assert.AreEqual("=h1;n=1;", Describe(results[2][0].Columns));
        Assert.AreEqual("=default only;", Describe(results[2][1].Columns));
    }

    [TestMethod]
    public void AttributeGroupWriteBatchMatchesPerCfPutEntity()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = OpenWithCfs(fs, out ColumnFamily hot, out ColumnFamily cold);
        using WriteBatch a = new();
        a.PutAttributeGroups(B("k"), [new AttributeGroup(hot, new[] { C("b", "2"), C("a", "1") }), new AttributeGroup(cold, new[] { C("", "c") })]);
        using WriteBatch b = new();
        b.PutEntity(hot.Id, B("k"), [C("a", "1"), C("b", "2")]);
        b.PutEntity(cold.Id, B("k"), [C("", "c")]);
        CollectionAssert.AreEqual(b.Data.ToArray(), a.Data.ToArray());
    }

    // =====================================================================================================
    // Coalescing / attribute group iterators (multi_cf_iterator_test.cc)
    // =====================================================================================================

    [TestMethod]
    public void CoalescingIteratorShadowsColumnsAcrossCfs()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = OpenWithCfs(fs, out ColumnFamily hot, out ColumnFamily cold);
        ColumnFamily def = db.DefaultColumnFamily;
        // multi_cf_iterator_test.cc CoalescingIteratorTest: later CFs shadow earlier ones column by column.
        def.PutEntity(B("key_1"), [C("col_1", "foo"), C("col_2", "baz")]);
        hot.PutEntity(B("key_1"), [C("col_2", "quux"), C("col_3", "bla")]);
        def.Put(B("key_2"), B("def2"));
        cold.Put(B("key_2"), B("cold2"));
        hot.Put(B("key_3"), B("hot3"));
        cold.PutEntity(B("key_4"), [C("x", Large(40, 'x'))]);
        cold.Flush();
        string[] expectedKeys = ["key_1", "key_2", "key_3", "key_4"];
        string[] expectedCols =
        [
            "col_1=foo;col_2=quux;col_3=bla;",
            "=cold2;",
            "=hot3;",
            "x=" + Large(40, 'x') + ";",
        ];
        using CoalescingIterator it = db.NewCoalescingIterator(null, [def, hot, cold]);
        int i = 0;
        for (it.SeekToFirst(); it.Valid; it.Next(), i++)
        {
            Assert.AreEqual(expectedKeys[i], Encoding.UTF8.GetString(it.Key));
            Assert.AreEqual(expectedCols[i], Describe(it.Columns));
        }
        Assert.AreEqual(4, i);
        Assert.AreEqual("", Encoding.UTF8.GetString(Seek(it, "key_1"))); // no default column
        Assert.AreEqual("cold2", Encoding.UTF8.GetString(Seek(it, "key_2")));
        // Reverse, and direction switches in both ways.
        i = 3;
        for (it.SeekToLast(); it.Valid; it.Prev(), i--) Assert.AreEqual(expectedKeys[i], Encoding.UTF8.GetString(it.Key));
        Assert.AreEqual(-1, i);
        it.Seek(B("key_2"));
        it.Prev();
        Assert.AreEqual("key_1", Encoding.UTF8.GetString(it.Key));
        it.Next();
        Assert.AreEqual("key_2", Encoding.UTF8.GetString(it.Key));
        it.SeekForPrev(B("key_25"));
        Assert.AreEqual("key_2", Encoding.UTF8.GetString(it.Key));
        it.Next();
        Assert.AreEqual("key_3", Encoding.UTF8.GetString(it.Key));
        Assert.AreEqual("hot3", Encoding.UTF8.GetString(it.Value));

        static ReadOnlySpan<byte> Seek(CoalescingIterator it, string key)
        {
            it.Seek(B(key));
            return it.Value;
        }
    }

    [TestMethod]
    public void AttributeGroupIteratorAndConsistentView()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = OpenWithCfs(fs, out ColumnFamily hot, out ColumnFamily cold);
        db.PutAttributeGroups(B("a"), [new AttributeGroup(hot, new[] { C("h", "1") }), new AttributeGroup(cold, new[] { C("c", "1") })]);
        db.PutAttributeGroups(B("b"), [new AttributeGroup(cold, new[] { C("c", "2") })]);
        using AttributeGroupIterator it = db.NewAttributeGroupIterator(null, [hot, cold]);
        // Written after the iterator was created: not visible (one implicit snapshot for all column families).
        db.PutAttributeGroups(B("c"), [new AttributeGroup(hot, new[] { C("h", "3") })]);
        it.SeekToFirst();
        Assert.AreEqual("a", Encoding.UTF8.GetString(it.Key));
        Assert.AreEqual(2, it.AttributeGroups.Count);
        Assert.AreEqual("hot", it.AttributeGroups[0].ColumnFamily.Name);
        Assert.AreEqual("h=1;", Describe(it.AttributeGroups[0].Columns));
        Assert.AreEqual("c=1;", Describe(it.AttributeGroups[1].Columns));
        it.Next();
        Assert.AreEqual("b", Encoding.UTF8.GetString(it.Key));
        Assert.AreEqual(1, it.AttributeGroups.Count);
        Assert.AreEqual("cold", it.AttributeGroups[0].ColumnFamily.Name);
        it.Next();
        Assert.IsFalse(it.Valid);

        Assert.AreEqual(StatusCode.InvalidArgument,
            Assert.ThrowsExactly<RocksDbException>(() => db.NewAttributeGroupIterator(null, ReadOnlySpan<ColumnFamily>.Empty)).Code);
    }

    // =====================================================================================================
    // Lazy entity reads (db_lazy_entity_test.cc)
    // =====================================================================================================

    [TestMethod]
    public async Task GetEntityLazyDefersBlobReads()
    {
        MemoryFileSystem fs = NewFs();
        var st = new Statistics();
        DbOptions o = NewOptions(fs);
        o.Statistics = st;
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, BlobOptions(32));
        string big = Large(100, 'b') + "0123456789" + Large(90, 'e');
        db.PutEntity(B("k"), [C("", "default"), C("big", big), C("small", "s"), C("z", Large(50, 'z'))]);
        db.Put(B("plain"), B("pv"));
        db.Flush();
        ColumnFamily cf = db.DefaultColumnFamily;
        using var lazy = new LazyWideColumns();
        Assert.IsTrue(cf.TryGetEntityLazy(null, B("k"), lazy));
        Assert.AreEqual(4, lazy.Count);
        Assert.AreEqual(0UL, st.GetTickerCount(Ticker.BlobDbLazyReadCount));
        int bigIdx = lazy.IndexOf("big"u8);
        Assert.AreEqual(1, bigIdx);
        Assert.IsTrue(lazy.IsReference(bigIdx));
        Assert.IsFalse(lazy.IsReference(0));
        Assert.IsTrue(lazy.TryGetInlineValue(0, out ReadOnlySpan<byte> d));
        Assert.AreEqual("default", Encoding.UTF8.GetString(d));
        Assert.IsTrue(lazy.TryGetInlineValue(2, out ReadOnlySpan<byte> s));
        Assert.AreEqual("s", Encoding.UTF8.GetString(s));
        Assert.IsFalse(lazy.TryGetInlineValue(bigIdx, out _));
        Assert.AreEqual((ulong)big.Length, lazy.GetLogicalSize(bigIdx));
        Assert.AreEqual(-1, lazy.IndexOf("nope"u8));

        // Partial read: only the requested bytes.
        Span<byte> buf = stackalloc byte[10];
        Assert.AreEqual(10, lazy.ReadColumnRange(bigIdx, 100, buf));
        Assert.AreEqual("0123456789", Encoding.UTF8.GetString(buf));
        Assert.AreEqual(1UL, st.GetTickerCount(Ticker.BlobDbLazyPartialReadCount));
        Assert.AreEqual((ulong)(big.Length - 10), st.GetTickerCount(Ticker.BlobDbLazyPartialBytesSaved));
        // Clamped at the end; past the end reads nothing.
        Assert.AreEqual(5, lazy.ReadColumnRange(bigIdx, (ulong)big.Length - 5, buf));
        Assert.AreEqual(0, lazy.ReadColumnRange(bigIdx, (ulong)big.Length + 3, buf));
        // force_verify: whole verified read.
        Assert.AreEqual(10, lazy.ReadColumnRange(bigIdx, 100, buf, forceVerify: true));
        Assert.AreEqual("0123456789", Encoding.UTF8.GetString(buf));
        ulong reads = st.GetTickerCount(Ticker.BlobDbLazyReadCount);
        Assert.AreEqual(big, Encoding.UTF8.GetString(lazy.ResolveColumn(bigIdx)));
        Assert.AreEqual(reads, st.GetTickerCount(Ticker.BlobDbLazyReadCount)); // cached by the verified read
        Assert.AreEqual(Large(50, 'z'), Encoding.UTF8.GetString((await lazy.ResolveColumnAsync(3)).Span));

        // The result keeps the blob files alive across a compaction that rewrites them away.
        using var lazy2 = cf.GetEntityLazy(null, B("k"))!;
        db.SetOptions(new Dictionary<string, string> { ["enable_blob_files"] = "false" });
        db.CompactRange(new CompactRangeOptions { BottommostLevelCompaction = BottommostLevelCompaction.Force }, null, null);
        Assert.AreEqual(Large(50, 'z'), Encoding.UTF8.GetString(lazy2.ResolveColumn(3)));

        // Plain values and missing keys.
        Assert.IsTrue(cf.TryGetEntityLazy(null, B("plain"), lazy));
        Assert.AreEqual(1, lazy.Count);
        Assert.AreEqual("pv", Encoding.UTF8.GetString(lazy.ResolveColumn(0)));
        Assert.IsFalse(cf.TryGetEntityLazy(null, B("missing"), lazy));
        Assert.AreEqual(0, lazy.Count);
        lazy.Dispose();
        Assert.ThrowsExactly<ObjectDisposedException>(() => lazy.ResolveColumn(0));
    }

    [TestMethod]
    public void GetEntityLazyBlockCacheTierAndMerges()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), BlobOptions(32));
        db.PutEntity(B("k"), [C("", Large(40, 'd')), C("x", Large(40, 'x'))]);
        db.Flush();
        ColumnFamily cf = db.DefaultColumnFamily;
        using var lazy = new LazyWideColumns();
        Assert.IsTrue(cf.TryGetEntityLazy(new ReadOptions { ReadTier = ReadTier.BlockCacheTier }, B("k"), lazy));
        Assert.AreEqual(StatusCode.Incomplete, Assert.ThrowsExactly<RocksDbException>(() => lazy.ResolveColumn(1)).Code);
        // A merge operand on top: the entity is resolved eagerly (merged default column, inline columns).
        db.Merge(B("k"), B("m"));
        Assert.IsTrue(cf.TryGetEntityLazy(null, B("k"), lazy));
        Assert.IsFalse(lazy.IsReference(0));
        Assert.IsFalse(lazy.IsReference(1));
        Assert.IsTrue(lazy.TryGetInlineValue(0, out ReadOnlySpan<byte> v));
        Assert.AreEqual(Large(40, 'd') + ",m", Encoding.UTF8.GetString(v));
    }

    [TestMethod]
    public void MultiGetEntityLazySharesOnePin()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), BlobOptions(32));
        for (int i = 0; i < 5; i++) db.PutEntity(Key(i), [C("v", Large(40 + i, (char)('a' + i)))]);
        db.Flush();
        using var keys = new MultiGetBatch();
        keys.Add(Key(0)).Add(B("missing")).Add(Key(4));
        using var result = new LazyWideColumnsBatch();
        db.DefaultColumnFamily.MultiGetEntityLazy(null, keys, result);
        Assert.AreEqual(3, result.Count);
        Assert.AreEqual(MultiGetStatus.Found, result.GetStatus(0));
        Assert.AreEqual(MultiGetStatus.NotFound, result.GetStatus(1));
        Assert.IsTrue(result[2].IsReference(0));
        Assert.AreEqual(Large(44, 'e'), Encoding.UTF8.GetString(result[2].ResolveColumn(0)));
        Assert.AreEqual(Large(40, 'a'), Encoding.UTF8.GetString(result[0].ResolveColumn(0)));
        LazyWideColumns first = result[0];
        result.Reset();
        Assert.AreEqual(0, result.Count);
        Assert.AreEqual(0, first.Count);
    }
}
