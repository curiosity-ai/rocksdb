using System.Buffers;
using System.Globalization;
using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Db.Blob;
using RocksDb.Managed.Db.Wide;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;
using RocksDb.Managed.Util;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Blob;

/// <summary>
/// Wide-column entities with blob-backed columns (version 2 entities). Ports of
/// db/wide/wide_column_serialization_test.cc (V2 cases) and db/wide/db_wide_basic_test.cc (EntityBlobAfterFlush,
/// EntityBlobAfterCompaction, MergeEntityWithBlobColumns, AllBlobColumnsEntity, EmptyEntityWithBlobOptions,
/// EntityBlobReverseIteration, CompactionFilter, CompactionFilterWithBlobGC).
/// </summary>
[TestClass]
public sealed class EntityBlobTests
{
    private static WideColumn C(string name, string value) => new(B(name), B(value));

    private static string Large(int n, char c) => new(c, n);

    private static string Describe(WideColumns c)
    {
        var sb = new StringBuilder();
        for (int i = 0; i < c.Count; i++) sb.Append(Encoding.UTF8.GetString(c.GetName(i))).Append('=').Append(Encoding.UTF8.GetString(c.GetValue(i))).Append(';');
        return sb.ToString();
    }

    private static string Describe(params WideColumn[] cols)
    {
        var sorted = (WideColumn[])cols.Clone();
        WideColumnSerialization.SortColumns(sorted);
        var sb = new StringBuilder();
        foreach (WideColumn c in sorted) sb.Append(Encoding.UTF8.GetString(c.Name.Span)).Append('=').Append(Encoding.UTF8.GetString(c.Value.Span)).Append(';');
        return sb.ToString();
    }

    private static ColumnFamilyOptions BlobOptions(ulong minBlobSize) => new()
    {
        EnableBlobFiles = true,
        MinBlobSize = minBlobSize,
        DisableAutoCompactions = true,
        MergeOperator = new RocksDb.Managed.Tests.Db.StringAppendMergeOperator(),
    };

    private static int CountBlobFiles(IFileSystem fs)
    {
        int n = 0;
        foreach (string c in fs.GetChildren(DbPath))
        {
            if (FileNames.TryParseFileName(c, out _, out FileType t) && t == FileType.BlobFile) n++;
        }
        return n;
    }

    /// <summary>Raw entity values stored in the table files, by user key (newest table wins).</summary>
    private static Dictionary<string, byte[]> RawEntitiesInTables(IFileSystem fs)
    {
        var result = new Dictionary<string, byte[]>();
        foreach (string c in fs.GetChildren(DbPath))
        {
            if (!c.EndsWith(".sst", StringComparison.Ordinal)) continue;
            using BlockBasedTableReader reader = BlockBasedTableReader.Open(fs.OpenRandomAccess(DbPath + "/" + c), new TableReaderOptions());
            using BlockBasedTableIterator it = reader.NewIterator();
            for (it.SeekToFirst(); it.Valid; it.Next())
            {
                ReadOnlySpan<byte> k = it.Key;
                if ((EntryType)(byte)DbFormat.ExtractFooter(k) != EntryType.WideColumnEntity) continue;
                result[Encoding.UTF8.GetString(DbFormat.ExtractUserKey(k))] = it.Value.ToArray();
            }
        }
        return result;
    }

    // ---- serialization (wide_column_serialization_test.cc) ----------------------------------------------------------

    private static byte[] SerializeV2(params (string Name, byte[] Value, bool Blob)[] cols)
    {
        using var list = new EntityColumnList();
        foreach ((string n, byte[] v, bool b) in cols) list.Add(B(n), v, b);
        var w = new PooledBufferWriter(64);
        WideColumnSerialization.SerializeV2(list, w);
        return w.WrittenSpan.ToArray();
    }

    private static byte[] EncodeBlobIndex(ulong file, ulong offset, ulong size)
    {
        var w = new PooledBufferWriter(32);
        BlobIndex.EncodeBlob(w, file, offset, size, RocksDb.Managed.Compression.CompressionType.None);
        return w.WrittenSpan.ToArray();
    }

    [TestMethod]
    public void V2LayoutStructureVerification()
    {
        byte[] e = SerializeV2(("aa", B("val_aa"), false), ("bbb", B("val_bbb"), false));
        byte[] expected =
        [
            2, 2, // version, columns
            2, 2, 5, // name_sizes_bytes, value_sizes_bytes, names_bytes
            0x1, 0x1, // types
            2, 3, // name sizes
            6, 7, // value sizes
            .. "aabbb"u8, .. "val_aaval_bbb"u8,
        ];
        CollectionAssert.AreEqual(expected, e);
        Assert.IsFalse(WideColumnSerialization.HasBlobColumns(e));
        var c = new WideColumns();
        c.SetSerialized(e);
        Assert.AreEqual("aa=val_aa;bbb=val_bbb;", Describe(c));
    }

    [TestMethod]
    public void V2BlobColumnsRoundTripAndRejection()
    {
        byte[] idx = EncodeBlobIndex(7, 100, 50);
        byte[] e = SerializeV2(("", idx, true), ("a", B("inline"), false), ("b", EncodeBlobIndex(9, 3, 4), true));
        Assert.IsTrue(WideColumnSerialization.HasBlobColumns(e));
        // V2BlobColumnRejectsDeserialize: consumers of a fully resolved entity report Corruption.
        var c = new WideColumns();
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => c.SetSerialized(e));
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
        StringAssert.Contains(ex.Message, "Wide column blob reference in a context where it is not expected.");
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => WideColumnSerialization.GetValueOfDefaultColumn(e)).Code);
        var slots = new List<WideColumnSlot>();
        Assert.IsTrue(WideColumnSerialization.DeserializeWithBlobColumns(e, slots));
        Assert.AreEqual(3, slots.Count);
        Assert.IsTrue(slots[0].IsBlobIndex);
        Assert.IsFalse(slots[1].IsBlobIndex);
        CollectionAssert.AreEqual(idx, e.AsSpan(slots[0].ValueOffset, slots[0].ValueLength).ToArray());
        WideColumnSerialization.GetBlobColumnIndexes(e, slots);
        Assert.AreEqual(2, slots.Count);
        Assert.AreEqual(9UL, BlobIndex.Decode(e.AsSpan(slots[1].ValueOffset, slots[1].ValueLength)).FileNumber);

        // ResolveEntityBlobColumns produces the V1 entity with the fetched values.
        var fetcher = new FakeFetcher();
        var output = new PooledBufferWriter(64);
        using var scratch = new PooledBufferWriter(64);
        Assert.IsTrue(WideColumnSerialization.ResolveEntityBlobColumns(e, B("k"), ref fetcher, slots, scratch, output));
        c.SetSerialized(output.WrittenSpan);
        Assert.AreEqual("=blob#7@100;a=inline;b=blob#9@3;", Describe(c));
        output.Clear();
        Assert.IsFalse(WideColumnSerialization.ResolveEntityBlobColumns(SerializeV2(("x", B("y"), false)), B("k"), ref fetcher, slots, scratch, output));

        // Corrupt blob index / empty blob index / unsupported column type.
        byte[] empty = SerializeV2(("a", [], true));
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => WideColumnSerialization.DeserializeWithBlobColumns(empty, slots)).Code);
        byte[] bad = SerializeV2(("a", [0x7f], true));
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => WideColumnSerialization.DeserializeWithBlobColumns(bad, slots)).Code);
        byte[] recursive = (byte[])e.Clone();
        recursive[5] = (byte)EntryType.WideColumnEntity; // DeserializeV2RejectsRecursiveType
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => WideColumnSerialization.HasBlobColumns(recursive)).Code);
        // SerializeV2Errors: out of order.
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => SerializeV2(("b", B("1"), false), ("a", B("2"), false))).Code);
    }

    private struct FakeFetcher : IEntityBlobFetcher
    {
        public void FetchBlob(ReadOnlySpan<byte> userKey, ReadOnlySpan<byte> blobIndex, IBufferWriter<byte> output)
        {
            BlobIndex i = BlobIndex.Decode(blobIndex);
            output.Write(B("blob#" + i.FileNumber.ToString(CultureInfo.InvariantCulture) + "@" + i.Offset.ToString(CultureInfo.InvariantCulture)));
        }
    }

    [TestMethod]
    public void RandomizedV2RoundTrip()
    {
        int seed = Environment.TickCount;
        var rnd = new Random(seed);
        for (int iter = 0; iter < 200; iter++)
        {
            int n = rnd.Next(0, 12);
            var names = new SortedSet<string>(StringComparer.Ordinal);
            while (names.Count < n) names.Add(new string((char)('a' + rnd.Next(26)), rnd.Next(0, 5)) + rnd.Next(1000).ToString(CultureInfo.InvariantCulture));
            var cols = new List<(string, byte[], bool)>();
            foreach (string name in names)
            {
                bool blob = rnd.Next(3) == 0;
                byte[] v = blob ? EncodeBlobIndex((ulong)rnd.Next(1, 1000), (ulong)rnd.Next(100_000), (ulong)rnd.Next(1, 10_000)) : B(new string('v', rnd.Next(0, 300)));
                cols.Add((name, v, blob));
            }
            byte[] e = SerializeV2([.. cols]);
            var slots = new List<WideColumnSlot>();
            bool hasBlob = WideColumnSerialization.DeserializeWithBlobColumns(e, slots);
            Assert.AreEqual(cols.Exists(static c => c.Item3), hasBlob, "seed=" + seed);
            Assert.AreEqual(hasBlob, WideColumnSerialization.HasBlobColumns(e), "seed=" + seed);
            Assert.AreEqual(cols.Count, slots.Count, "seed=" + seed);
            for (int i = 0; i < cols.Count; i++)
            {
                Assert.AreEqual(cols[i].Item1, Encoding.UTF8.GetString(e.AsSpan(slots[i].NameOffset, slots[i].NameLength)), "seed=" + seed);
                CollectionAssert.AreEqual(cols[i].Item2, e.AsSpan(slots[i].ValueOffset, slots[i].ValueLength).ToArray(), "seed=" + seed);
                Assert.AreEqual(cols[i].Item3, slots[i].IsBlobIndex, "seed=" + seed);
            }
        }
    }

    // ---- DB level (db_wide_basic_test.cc) -----------------------------------------------------------------------------

    private static readonly WideColumn[] s_columns1 = [C("", Large(128, 'a')), C("attr1", "small")];
    private static readonly WideColumn[] s_columns2 = [C("attr2", Large(128, 'b')), C("attr3", Large(256, 'c'))];
    private static readonly WideColumn[] s_columns3 = [C("", "small"), C("attr4", Large(256, 'd'))];

    private static void VerifyEntities(RocksDatabase db, string phase)
    {
        Assert.AreEqual(Describe(s_columns1), Describe(db.GetEntity(B("k1"))!), phase);
        Assert.AreEqual(Describe(s_columns2), Describe(db.GetEntity(B("k2"))!), phase);
        Assert.AreEqual(Describe(s_columns3), Describe(db.GetEntity(B("k3"))!), phase);
        Assert.AreEqual(Large(128, 'a'), GetString(db, "k1"), phase);
        Assert.AreEqual("", GetString(db, "k2"), phase);
        Assert.AreEqual("small", GetString(db, "k3"), phase);
        using Iterator it = db.NewIterator();
        var fwd = new StringBuilder();
        for (it.SeekToFirst(); it.Valid; it.Next()) fwd.Append(S(it.Key.ToArray())).Append(':').Append(it.Value.Length).Append('{').Append(Describe(it.Columns)).Append('}');
        Assert.AreEqual("k1:128{" + Describe(s_columns1) + "}k2:0{" + Describe(s_columns2) + "}k3:5{" + Describe(s_columns3) + "}", fwd.ToString(), phase);
        // EntityBlobReverseIteration
        var rev = new StringBuilder();
        for (it.SeekToLast(); it.Valid; it.Prev()) rev.Append(S(it.Key.ToArray())).Append('{').Append(Describe(it.Columns)).Append('}');
        Assert.AreEqual("k3{" + Describe(s_columns3) + "}k2{" + Describe(s_columns2) + "}k1{" + Describe(s_columns1) + "}", rev.ToString(), phase);
        // MultiGet of the default column.
        byte[]?[] mg = db.MultiGet([B("k1"), B("k2"), B("k3")]);
        Assert.AreEqual(Large(128, 'a'), S(mg[0]), phase);
        Assert.AreEqual("", S(mg[1]), phase);
        Assert.AreEqual("small", S(mg[2]), phase);
    }

    [TestMethod]
    public async Task EntityBlobAfterFlushAndCompaction()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cf = BlobOptions(64);
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cf))
        {
            db.PutEntity(B("k1"), s_columns1);
            db.PutEntity(B("k2"), s_columns2);
            db.Flush();
            db.PutEntity(B("k3"), s_columns3);
            VerifyEntities(db, "memtable+L0");
            db.Flush();
            Assert.AreEqual(2, CountBlobFiles(fs));
            Dictionary<string, byte[]> raw = RawEntitiesInTables(fs);
            // Large columns were separated at flush: the stored entities are version 2 with blob-backed columns.
            foreach (string k in new[] { "k1", "k2", "k3" })
            {
                Assert.AreEqual(2, raw[k][0], k);
                Assert.IsTrue(WideColumnSerialization.HasBlobColumns(raw[k]), k);
            }
            VerifyEntities(db, "L0");
            // Async reads resolve blob columns too.
            Assert.AreEqual(Describe(s_columns2), Describe((await db.GetEntityAsync(B("k2")))!));
            Assert.AreEqual(Large(128, 'a'), S(await db.GetAsync(B("k1"))));
            // Oldest blob file numbers are tracked for entity references.
            VersionStorageInfo vs = db.DefaultColumnFamily.Data.Current.Storage;
            for (int l = 0; l < vs.NumLevels; l++) foreach (FileMetaData f in vs.LevelFilesArray(l)) Assert.AreNotEqual(0UL, f.OldestBlobFileNumber);
            db.CompactRange();
            VerifyEntities(db, "compacted");
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cf))
        {
            VerifyEntities(db, "reopened");
        }
    }

    [TestMethod]
    public void AllBlobColumnsAndEmptyEntity()
    {
        // AllBlobColumnsEntity / EmptyEntityWithBlobOptions; empty column values are never separated.
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), BlobOptions(0));
        WideColumn[] all = [C("", "d"), C("a", "1"), C("b", "2")];
        db.PutEntity(B("all"), all);
        db.PutEntity(B("empty"), []);
        db.PutEntity(B("emptyvals"), [C("", ""), C("x", "")]);
        db.Flush();
        Dictionary<string, byte[]> raw = RawEntitiesInTables(fs);
        var slots = new List<WideColumnSlot>();
        WideColumnSerialization.DeserializeWithBlobColumns(raw["all"], slots);
        Assert.IsTrue(slots.TrueForAll(static s => s.IsBlobIndex));
        Assert.AreEqual(1, raw["empty"][0]); // nothing extracted: still version 1
        Assert.AreEqual(1, raw["emptyvals"][0]);
        Assert.AreEqual(Describe(all), Describe(db.GetEntity(B("all"))!));
        Assert.AreEqual("", Describe(db.GetEntity(B("empty"))!));
        Assert.AreEqual("=;x=;", Describe(db.GetEntity(B("emptyvals"))!));
        Assert.AreEqual("d", GetString(db, "all"));
    }

    [TestMethod]
    public void MergeEntityWithBlobColumns()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), BlobOptions(32));
        db.PutEntity(B("m"), [C("", Large(64, 'x')), C("other", Large(64, 'y')), C("tiny", "t")]);
        db.PutEntity(B("nodef"), [C("other", Large(64, 'z'))]);
        db.Flush();
        db.Merge(B("m"), B("op1"));
        db.Merge(B("nodef"), B("op2"));
        string expected = "=" + Large(64, 'x') + ",op1;other=" + Large(64, 'y') + ";tiny=t;";
        Assert.AreEqual(expected, Describe(db.GetEntity(B("m"))!));
        Assert.AreEqual(Large(64, 'x') + ",op1", GetString(db, "m"));
        Assert.AreEqual("=,op2;other=" + Large(64, 'z') + ";", Describe(db.GetEntity(B("nodef"))!));
        using (Iterator it = db.NewIterator())
        {
            it.Seek(B("m"));
            Assert.AreEqual(expected, Describe(it.Columns));
        }
        db.Flush();
        db.CompactRange(); // MergeHelper resolves the entity base value, the output is separated again
        Assert.AreEqual(expected, Describe(db.GetEntity(B("m"))!));
        Assert.AreEqual("=,op2;other=" + Large(64, 'z') + ";", Describe(db.GetEntity(B("nodef"))!));
        Assert.IsTrue(WideColumnSerialization.HasBlobColumns(RawEntitiesInTables(fs)["m"]));
    }

    [TestMethod]
    public void EntityBlobGarbageAndGc()
    {
        // Overwritten entities produce garbage in their blob files (BlobGarbageMeter over entity references); GC with
        // age cutoff 1.0 relocates every blob column and deletes the old blob files.
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cf = BlobOptions(16);
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cf);
        for (int i = 0; i < 20; i++) db.PutEntity(Key(i), [C("", Large(100, 'a')), C("c", Large(50, 'b'))]);
        db.Flush();
        for (int i = 0; i < 10; i++) db.PutEntity(Key(i), [C("", Large(100, 'c')), C("c", Large(50, 'd'))]);
        db.Flush();
        db.CompactRange();
        ulong garbage = ulong.Parse(db.GetProperty(DbProperties.LiveBlobFileGarbageSize)!, CultureInfo.InvariantCulture);
        Assert.IsTrue(garbage > 0, "garbage " + garbage);
        db.SetOptions(new Dictionary<string, string> { ["enable_blob_garbage_collection"] = "true", ["blob_garbage_collection_age_cutoff"] = "1.0" });
        var force = new CompactRangeOptions { BottommostLevelCompaction = BottommostLevelCompaction.Force };
        db.CompactRange(force, null, null);
        Assert.AreEqual(0UL, ulong.Parse(db.GetProperty(DbProperties.LiveBlobFileGarbageSize)!, CultureInfo.InvariantCulture));
        Assert.AreEqual(1, CountBlobFiles(fs));
        for (int i = 0; i < 20; i++)
        {
            string d = i < 10 ? Large(100, 'c') : Large(100, 'a');
            string c = i < 10 ? Large(50, 'd') : Large(50, 'b');
            Assert.AreEqual("=" + d + ";c=" + c + ";", Describe(db.GetEntity(Key(i))!));
        }
        // Without blob files in the output (enable_blob_files=false) GC inlines the relocated columns (version 1).
        db.SetOptions(new Dictionary<string, string> { ["enable_blob_files"] = "false" });
        db.CompactRange(force, null, null);
        Assert.AreEqual(0, CountBlobFiles(fs));
        foreach (byte[] v in RawEntitiesInTables(fs).Values) Assert.AreEqual(1, v[0]);
        Assert.AreEqual("=" + Large(100, 'a') + ";c=" + Large(50, 'b') + ";", Describe(db.GetEntity(Key(15))!));
    }

    private sealed class EntityFilter : CompactionFilter
    {
        public int EntityCalls;
        public string? SeenColumns;
        public override string Name => "EntityFilter";

        public override CompactionFilterDecision Filter(int level, ReadOnlySpan<byte> key, CompactionFilterValueType valueType, ReadOnlySpan<byte> existingValue,
            IBufferWriter<byte> newValue, IBufferWriter<byte> skipUntil) => CompactionFilterDecision.Keep;

        public override CompactionFilterDecision FilterV3(int level, ReadOnlySpan<byte> key, CompactionFilterValueType valueType, ReadOnlySpan<byte> existingValue,
            WideColumns? existingColumns, IBufferWriter<byte> newValue, List<WideColumn> newColumns, IBufferWriter<byte> skipUntil)
        {
            string k = Encoding.UTF8.GetString(key);
            if (valueType == CompactionFilterValueType.WideColumnEntity)
            {
                EntityCalls++;
                if (k == "seen") SeenColumns = Describe(existingColumns!);
            }
            switch (k)
            {
                case "remove": return CompactionFilterDecision.Remove;
                case "to_value":
                    newValue.Write(B("plain now"));
                    return CompactionFilterDecision.ChangeValue;
                case "to_entity": // a plain value turned into an entity (only FilterV3 can do that)
                case "change":
                    newColumns.Add(C("z", Large(80, 'z')));
                    newColumns.Add(C("", "new default"));
                    return CompactionFilterDecision.ChangeWideColumnEntity;
                default: return CompactionFilterDecision.Keep;
            }
        }
    }

    [TestMethod]
    public void CompactionFilterOnEntitiesWithBlobColumns()
    {
        // db_wide_basic_test.cc CompactionFilter / CompactionFilterWithBlobGC: FilterV3 sees entities with resolved
        // blob columns; kRemove, kChangeValue and kChangeWideColumnEntity (large new columns are separated again).
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cf = BlobOptions(32);
        var filter = new EntityFilter();
        cf.CompactionFilter = filter;
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cf);
        WideColumn[] cols = [C("", Large(40, 'd')), C("a", "small"), C("b", Large(60, 'e'))];
        foreach (string k in new[] { "change", "keep", "remove", "seen", "to_value" }) db.PutEntity(B(k), cols);
        db.Put(B("to_entity"), B("plain"));
        db.Flush();
        db.CompactRange();
        Assert.AreEqual(5, filter.EntityCalls);
        Assert.AreEqual(Describe(cols), filter.SeenColumns);
        Assert.IsNull(db.GetEntity(B("remove")));
        Assert.AreEqual("=plain now;", Describe(db.GetEntity(B("to_value"))!));
        string changed = "=new default;z=" + Large(80, 'z') + ";";
        Assert.AreEqual(changed, Describe(db.GetEntity(B("change"))!));
        Assert.AreEqual(changed, Describe(db.GetEntity(B("to_entity"))!));
        Assert.AreEqual(Describe(cols), Describe(db.GetEntity(B("keep"))!));
        Dictionary<string, byte[]> raw = RawEntitiesInTables(fs);
        Assert.IsTrue(WideColumnSerialization.HasBlobColumns(raw["change"]));
        Assert.IsTrue(WideColumnSerialization.HasBlobColumns(raw["keep"]));
    }

    [TestMethod]
    public void DefaultFilterV3KeepsEntities()
    {
        // Upstream's default FilterV3 returns kKeep for entities; a filter overriding only FilterV2 never sees them.
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cf = BlobOptions(32);
        var filter = new RemoveAllFilter();
        cf.CompactionFilter = filter;
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cf);
        db.PutEntity(B("e"), [C("", Large(40, 'd'))]);
        db.Put(B("v"), B("value"));
        db.Flush();
        db.CompactRange();
        Assert.AreEqual(1, filter.Calls);
        Assert.IsNull(GetString(db, "v"));
        Assert.AreEqual(Large(40, 'd'), GetString(db, "e"));
    }

    private sealed class RemoveAllFilter : CompactionFilter
    {
        public int Calls;
        public override string Name => "RemoveAll";

        public override CompactionFilterDecision Filter(int level, ReadOnlySpan<byte> key, CompactionFilterValueType valueType, ReadOnlySpan<byte> existingValue,
            IBufferWriter<byte> newValue, IBufferWriter<byte> skipUntil)
        {
            Calls++;
            return CompactionFilterDecision.Remove;
        }
    }
}
