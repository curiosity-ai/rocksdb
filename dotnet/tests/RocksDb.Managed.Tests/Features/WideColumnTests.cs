using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Db.Wide;
using RocksDb.Managed.Env;
using RocksDb.Managed.Tests.Db;
using RocksDb.Managed.Util;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Features;

/// <summary>
/// Wide columns: serialization (port of db/wide/wide_column_serialization_test.cc key cases) and the DB API (port of
/// db/wide/db_wide_basic_test.cc key cases: PutEntity / GetEntity / iterator columns / merge / compaction).
/// </summary>
[TestClass]
public sealed class WideColumnTests
{
    private static WideColumn C(string name, string value) => new(B(name), B(value));

    private static byte[] Serialize(params WideColumn[] cols)
    {
        var w = new PooledBufferWriter(64);
        WideColumnSerialization.SortColumns(cols);
        WideColumnSerialization.Serialize(cols, w);
        return w.WrittenSpan.ToArray();
    }

    private static string Describe(WideColumns c)
    {
        var sb = new StringBuilder();
        for (int i = 0; i < c.Count; i++) sb.Append(Encoding.UTF8.GetString(c.GetName(i))).Append('=').Append(Encoding.UTF8.GetString(c.GetValue(i))).Append(';');
        return sb.ToString();
    }

    [TestMethod]
    public void SerializeDeserializeV1()
    {
        // wide_column_serialization_test.cc: Construct / SerializeDeserialize
        byte[] e = Serialize(C("foo", "bar"), C("", "baz"), C("hello", "world"));
        // version 1, 3 columns: ("", 3) ("foo", 3) ("hello", 5) then values
        byte[] expected = [1, 3, 0, 3, 3, (byte)'f', (byte)'o', (byte)'o', 3, 5, (byte)'h', (byte)'e', (byte)'l', (byte)'l', (byte)'o', 5,
            .. "baz"u8, .. "bar"u8, .. "world"u8];
        CollectionAssert.AreEqual(expected, e);
        var c = new WideColumns();
        c.SetSerialized(e);
        Assert.AreEqual("=baz;foo=bar;hello=world;", Describe(c));
        Assert.IsTrue(c.HasDefaultColumn);
        Assert.AreEqual("baz", Encoding.UTF8.GetString(c.DefaultColumnValue));
        Assert.IsTrue(c.TryGetValue(B("hello"), out ReadOnlySpan<byte> v));
        Assert.AreEqual("world", Encoding.UTF8.GetString(v));
        Assert.IsFalse(c.TryGetValue(B("nope"), out _));
        Assert.AreEqual("baz", Encoding.UTF8.GetString(WideColumnSerialization.GetValueOfDefaultColumn(e)));
        Assert.AreEqual(0, WideColumnSerialization.GetValueOfDefaultColumn(Serialize(C("a", "1"))).Length);
        CollectionAssert.AreEqual(new byte[] { 1, 0 }, Serialize());
    }

    [TestMethod]
    public void DeserializeV2AndCorruption()
    {
        // V2: version, N, name_sizes_bytes, value_sizes_bytes, names_bytes, types, name sizes, value sizes, names, values
        byte[] v2 = [2, 2, 2, 2, 4, 0x1, 0x1, 1, 3, 1, 2, (byte)'a', (byte)'b', (byte)'c', (byte)'d', (byte)'x', (byte)'y', (byte)'z'];
        // names: "a" (1), "bcd" (3); values: "x" (1), "yz" (2)
        var c = new WideColumns();
        c.SetSerialized(v2);
        Assert.AreEqual("a=x;bcd=yz;", Describe(c));
        byte[] blob = (byte[])v2.Clone();
        blob[6] = (byte)EntryType.BlobIndex;
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => c.SetSerialized(blob)).Code);
        var slots = new List<WideColumnSlot>();
        Assert.AreEqual("Corrupt or unsupported wide column version", WideColumnSerialization.TryDeserialize([3, 0], slots, out _));
        Assert.AreEqual("Error decoding wide column version", WideColumnSerialization.TryDeserialize([], slots, out _));
        Assert.AreEqual("Unexpected trailing data after wide column entity", WideColumnSerialization.TryDeserialize([1, 0, 7], slots, out _));
        Assert.AreEqual("Wide columns out of order", WideColumnSerialization.TryDeserialize([1, 2, 1, (byte)'b', 0, 1, (byte)'a', 0], slots, out _));
        Assert.AreEqual("Error decoding wide column value payload", WideColumnSerialization.TryDeserialize([1, 1, 1, (byte)'a', 5, 1], slots, out _));
        // Duplicate names are rejected on write.
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => Serialize(C("a", "1"), C("a", "2"))).Code);
    }

    private static ColumnFamilyOptions MergeCf() => new() { MergeOperator = new StringAppendMergeOperator() };

    [TestMethod]
    public void PutEntityGetEntityAcrossMemtableFlushAndCompaction()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), MergeCf());
        db.PutEntity(B("e1"), [C("b", "2"), C("", "dflt"), C("a", "1")]);
        db.PutEntity(B("e2"), [C("x", "only")]); // no default column
        db.Put(B("plain"), B("pv"));

        void Check(string phase)
        {
            Assert.AreEqual("dflt", GetString(db, "e1"), phase);
            Assert.AreEqual("", GetString(db, "e2"), phase);
            Assert.AreEqual("=dflt;a=1;b=2;", Describe(db.GetEntity(B("e1"))!), phase);
            Assert.AreEqual("x=only;", Describe(db.GetEntity(B("e2"))!), phase);
            Assert.AreEqual("=pv;", Describe(db.GetEntity(B("plain"))!), phase);
            Assert.IsNull(db.GetEntity(B("missing")), phase);
            using Iterator it = db.NewIterator();
            var seen = new StringBuilder();
            for (it.SeekToFirst(); it.Valid; it.Next())
            {
                seen.Append(Encoding.UTF8.GetString(it.Key)).Append(it.IsEntity ? "[E]" : "[V]").Append(':').Append(Encoding.UTF8.GetString(it.Value))
                    .Append('{').Append(Describe(it.Columns)).Append('}');
            }
            Assert.AreEqual("e1[E]:dflt{=dflt;a=1;b=2;}e2[E]:{x=only;}plain[V]:pv{=pv;}", seen.ToString(), phase);
            // Reverse.
            seen.Clear();
            for (it.SeekToLast(); it.Valid; it.Prev()) seen.Append(Encoding.UTF8.GetString(it.Key)).Append(':').Append(Describe(it.Columns));
            Assert.AreEqual("plain:=pv;e2:x=only;e1:=dflt;a=1;b=2;", seen.ToString(), phase);
        }

        Check("memtable");
        db.Flush();
        Check("L0");
        db.CompactRange();
        Check("compacted");
        // Overwrite an entity with a plain value and vice versa; delete one.
        db.Put(B("e1"), B("now plain"));
        db.PutEntity(B("plain"), [C("c", "3")]);
        db.Delete(B("e2"));
        Assert.AreEqual("=now plain;", Describe(db.GetEntity(B("e1"))!));
        Assert.AreEqual("c=3;", Describe(db.GetEntity(B("plain"))!));
        Assert.IsNull(db.GetEntity(B("e2")));
        db.Flush();
        db.CompactRange();
        Assert.AreEqual("=now plain;", Describe(db.GetEntity(B("e1"))!));
        Assert.AreEqual("c=3;", Describe(db.GetEntity(B("plain"))!));
        Assert.IsNull(db.GetEntity(B("e2")));
    }

    [TestMethod]
    public void MergeOnTopOfEntity()
    {
        // db_wide_basic_test.cc MergePlainKeyValue / MergeEntity: operands apply to the default column.
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), MergeCf());
        db.PutEntity(B("k"), [C("", "base"), C("col", "v")]);
        db.Merge(B("k"), B("m1"));
        db.Merge(B("k"), B("m2"));
        db.PutEntity(B("nodef"), [C("col", "v")]);
        db.Merge(B("nodef"), B("m"));

        void Check(string phase)
        {
            Assert.AreEqual("base,m1,m2", GetString(db, "k"), phase);
            Assert.AreEqual("=base,m1,m2;col=v;", Describe(db.GetEntity(B("k"))!), phase);
            // MergeOperator::FullMergeV3: without a default column the base value is an existing empty value.
            Assert.AreEqual(",m", GetString(db, "nodef"), phase);
            Assert.AreEqual("=,m;col=v;", Describe(db.GetEntity(B("nodef"))!), phase);
            using Iterator it = db.NewIterator();
            it.Seek(B("k"));
            Assert.IsTrue(it.IsEntity, phase);
            Assert.AreEqual("base,m1,m2", Encoding.UTF8.GetString(it.Value), phase);
            Assert.AreEqual("=base,m1,m2;col=v;", Describe(it.Columns), phase);
            it.SeekForPrev(B("k"));
            Assert.AreEqual("=base,m1,m2;col=v;", Describe(it.Columns), phase);
        }

        Check("memtable");
        db.Flush();
        Check("L0");
        db.CompactRange();
        Check("compacted (merged into an entity)");
    }

    [TestMethod]
    public void SnapshotsWriteBatchAndReopen()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            db.PutEntity(B("k"), [C("a", "1")]);
            using Snapshot s = db.GetSnapshot();
            db.PutEntity(B("k"), [C("a", "2")]);
            var cols = new WideColumns();
            Assert.IsTrue(db.TryGetEntity(new ReadOptions { Snapshot = s }, B("k"), cols));
            Assert.AreEqual("a=1;", Describe(cols));
            using WriteBatch b = db.CreateWriteBatch();
            b.PutEntity(B("batch"), [C("z", "26"), C("y", "25")]);
            db.Write(b);
            db.Flush(); // the old version is kept for the snapshot
            Assert.IsTrue(db.TryGetEntity(new ReadOptions { Snapshot = s }, B("k"), cols));
            Assert.AreEqual("a=1;", Describe(cols));
            db.PutEntity(B("wal"), [C("w", "x")]);
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            Assert.AreEqual("a=2;", Describe(db.GetEntity(B("k"))!));
            Assert.AreEqual("y=25;z=26;", Describe(db.GetEntity(B("batch"))!));
            Assert.AreEqual("w=x;", Describe(db.GetEntity(B("wal"))!)); // replayed from the WAL
        }
    }

    [TestMethod]
    public async Task AsyncEntityApi()
    {
        MemoryFileSystem fs = NewFs();
        await using RocksDatabase db = await RocksDatabase.OpenAsync(DbPath, NewOptions(fs));
        await db.PutEntityAsync(B("k"), new[] { C("q", "1") });
        await db.FlushAsync();
        WideColumns? c = await db.GetEntityAsync(B("k"));
        Assert.AreEqual("q=1;", Describe(c!));
        Assert.IsNull(await db.GetEntityAsync(B("none")));
    }
}
