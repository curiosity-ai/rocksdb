using System.Buffers;
using RocksDb.Managed.Db;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using static RocksDb.Managed.CompatTests.Wal.NativeDbHelper;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>
/// Wide-column entities across implementations. librocksdb 11.12's C API has no PutEntity, so native entities are
/// written by handing native a WriteBatch rep holding kTypeWideColumnEntity records (rocksdb_writebatch_create_from +
/// rocksdb_write): native validates, stores, flushes and compacts them like its own; a plain native Get returns the
/// default column.
/// </summary>
[TestClass]
public sealed class WideColumnCompatTests
{
    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);
    private static WideColumn C(string n, string v) => new(U(n), U(v));

    private static string Describe(WideColumns c)
    {
        var sb = new StringBuilder();
        for (int i = 0; i < c.Count; i++) sb.Append(Encoding.UTF8.GetString(c.GetName(i))).Append('=').Append(Encoding.UTF8.GetString(c.GetValue(i))).Append(';');
        return sb.ToString();
    }

    private static readonly WideColumn[][] s_entities =
    [
        [C("", "d0"), C("a", "1"), C("b", "2")],
        [C("x", "no default")],
        [C("", "only default")],
        [],
        [C("zz", new string('v', 3000)), C("", "big"), C("m", "mid")],
    ];

    private static string Expected(int i)
    {
        var sorted = new List<WideColumn>(s_entities[i]);
        sorted.Sort(static (a, b) => a.Name.Span.SequenceCompareTo(b.Name.Span));
        var sb = new StringBuilder();
        foreach (WideColumn c in sorted) sb.Append(Encoding.UTF8.GetString(c.Name.Span)).Append('=').Append(Encoding.UTF8.GetString(c.Value.Span)).Append(';');
        return sb.ToString();
    }

    private static string DefaultOf(int i)
    {
        foreach (WideColumn c in s_entities[i]) if (c.Name.Length == 0) return Encoding.UTF8.GetString(c.Value.Span);
        return "";
    }

    [TestMethod]
    public void NativeStoresManagedEntitiesAndManagedReadsNativeFiles()
    {
        using var dir = new TempDir("wide-n");
        RocksDbSharp.MergeOperator op = CreateConcatMergeOperator();
        using (RocksDbSharp.RocksDb db = Open(dir.Path, new RocksDbSharp.DbOptions(), op))
        {
            using (WriteBatch b = new())
            {
                for (int i = 0; i < s_entities.Length; i++) b.PutEntity(U("e" + i), s_entities[i]);
                b.PutEntity(CfId(db.GetColumnFamily("one")), U("cf"), [C("", "in one"), C("k", "v")]);
                using var nb = new RocksDbSharp.WriteBatch(b.Data.ToArray());
                db.Write(nb);
            }
            for (int i = 0; i < s_entities.Length; i++) Assert.AreEqual(DefaultOf(i), Encoding.UTF8.GetString(db.Get(U("e" + i)) ?? []), "native get " + i);
            db.Merge(U("e0"), U("m1")); // merge on top of an entity (default column)
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            // A second entity version in the WAL only.
            using (WriteBatch b = new())
            {
                b.PutEntity(U("wal"), [C("w", "1")]);
                using var nb = new RocksDbSharp.WriteBatch(b.Data.ToArray());
                db.Write(nb);
            }
            db.CompactRange((byte[]?)null, null);
            Assert.AreEqual("d0,m1", Encoding.UTF8.GetString(db.Get(U("e0"))));
            using RocksDbSharp.Iterator it = db.NewIterator();
            it.Seek(U("e2"));
            Assert.AreEqual("only default", Encoding.UTF8.GetString(it.Value()));
        }
        var cfo = new ColumnFamilyOptions { MergeOperator = new StringAppend() };
        using RocksDatabase mdb = RocksDatabase.Open(dir.Path, new DbOptions(),
            [new("default", cfo), new("one", cfo), new("two", cfo)]);
        Assert.AreEqual("=d0,m1;a=1;b=2;", Describe(mdb.GetEntity(U("e0"))!));
        for (int i = 1; i < s_entities.Length; i++)
        {
            Assert.AreEqual(Expected(i), Describe(mdb.GetEntity(U("e" + i))!), "entity " + i);
            Assert.AreEqual(DefaultOf(i), Encoding.UTF8.GetString(mdb.Get(U("e" + i))!), "get " + i);
        }
        Assert.AreEqual("=in one;k=v;", Describe(mdb.GetColumnFamily("one").GetEntity(U("cf"))!));
        Assert.AreEqual("w=1;", Describe(mdb.GetEntity(U("wal"))!));
        using Iterator mit = mdb.NewIterator();
        int n = 0;
        for (mit.SeekToFirst(); mit.Valid; mit.Next()) if (mit.IsEntity) n++;
        Assert.AreEqual(s_entities.Length + 1, n);
    }

    private sealed class StringAppend : MergeOperator
    {
        public override string Name => "StringAppendOperator";
        public override bool FullMerge(ReadOnlySpan<byte> key, bool hasExistingValue, ReadOnlySpan<byte> existingValue, in MergeOperands operands,
            System.Buffers.IBufferWriter<byte> result)
        {
            bool first = true;
            if (hasExistingValue)
            {
                result.Write(existingValue);
                first = false;
            }
            for (int i = 0; i < operands.Count; i++)
            {
                if (!first) result.Write(","u8);
                result.Write(operands[i]);
                first = false;
            }
            return true;
        }
    }

    [TestMethod]
    public void ManagedEntityFilesReadAndCompactedByNative()
    {
        using var dir = new TempDir("wide-m");
        var cfo = new ColumnFamilyOptions { MergeOperator = new StringAppend() };
        using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions { CreateIfMissing = true, CreateMissingColumnFamilies = true },
                   [new("default", cfo), new("one", cfo), new("two", cfo)]))
        {
            for (int i = 0; i < s_entities.Length; i++) db.PutEntity(U("e" + i), s_entities[i]);
            db.Merge(U("e0"), U("mm"));
            db.Flush();
            db.CompactRange(); // managed compaction merges the operand into the entity
            db.PutEntity(U("wal"), [C("", "dw"), C("c", "3")]);
        }
        RocksDbSharp.MergeOperator op = CreateConcatMergeOperator();
        using (RocksDbSharp.RocksDb db = Open(dir.Path, new RocksDbSharp.DbOptions(), op))
        {
            Assert.AreEqual("d0,mm", Encoding.UTF8.GetString(db.Get(U("e0"))));
            for (int i = 1; i < s_entities.Length; i++) Assert.AreEqual(DefaultOf(i), Encoding.UTF8.GetString(db.Get(U("e" + i)) ?? []), "native get " + i);
            Assert.AreEqual("dw", Encoding.UTF8.GetString(db.Get(U("wal"))));
            db.Merge(U("e2"), U("x"));
            db.CompactRange((byte[]?)null, null); // native rewrites the entities (and merges into e2)
        }
        using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions(), [new("default", cfo), new("one", cfo), new("two", cfo)]))
        {
            Assert.AreEqual("=d0,mm;a=1;b=2;", Describe(db.GetEntity(U("e0"))!));
            Assert.AreEqual("=only default,x;", Describe(db.GetEntity(U("e2"))!));
            Assert.AreEqual(Expected(4), Describe(db.GetEntity(U("e4"))!));
            Assert.AreEqual("=dw;c=3;", Describe(db.GetEntity(U("wal"))!));
        }
    }

    /// <summary>
    /// Attribute groups are per-column-family entities in one write batch (WriteBatch::PutEntity(key, attribute_groups)):
    /// the managed batch rep is accepted by native, native reads each group's default column per column family, and
    /// managed reads the native-stored groups back with GetAttributeGroups, MultiGetAttributeGroups and the
    /// coalescing / attribute-group iterators (after native flushed and compacted them).
    /// </summary>
    [TestMethod]
    public void AttributeGroupsAcrossImplementations()
    {
        using var dir = new TempDir("wide-ag");
        var cfo = new ColumnFamilyOptions { MergeOperator = new StringAppend() };
        using (RocksDatabase mdb = RocksDatabase.Open(dir.Path, new DbOptions { CreateIfMissing = true, CreateMissingColumnFamilies = true },
                   [new("default", cfo), new("one", cfo), new("two", cfo)]))
        {
            // Managed write: groups in "one" and "two".
            mdb.PutAttributeGroups(U("m"), [new AttributeGroup(mdb.GetColumnFamily("one"), new[] { C("", "m1"), C("x", "mx") }),
                new AttributeGroup(mdb.GetColumnFamily("two"), new[] { C("x", "shadow"), C("y", "my") })]);
        }
        RocksDbSharp.MergeOperator op = CreateConcatMergeOperator();
        using (RocksDbSharp.RocksDb db = Open(dir.Path, new RocksDbSharp.DbOptions(), op))
        {
            RocksDbSharp.ColumnFamilyHandle one = db.GetColumnFamily("one"), two = db.GetColumnFamily("two");
            Assert.AreEqual("m1", Encoding.UTF8.GetString(db.Get(U("m"), one)));
            Assert.AreEqual("", Encoding.UTF8.GetString(db.Get(U("m"), two)));
            // A native-applied attribute-group batch (built by the managed WriteBatch, ids from native handles).
            using (WriteBatch b = new())
            {
                b.PutEntity(CfId(one), U("n"), [C("", "n1"), C("a", "1")]);
                b.PutEntity(CfId(two), U("n"), [C("a", "2"), C("b", "3")]);
                using var nb = new RocksDbSharp.WriteBatch(b.Data.ToArray());
                db.Write(nb);
            }
            db.CompactRange((byte[]?)null, null, one); // flushes "one" first
            db.CompactRange((byte[]?)null, null, two);
            Assert.AreEqual("n1", Encoding.UTF8.GetString(db.Get(U("n"), one)));
        }
        using (RocksDatabase mdb = RocksDatabase.Open(dir.Path, new DbOptions(), [new("default", cfo), new("one", cfo), new("two", cfo)]))
        {
            ColumnFamily one = mdb.GetColumnFamily("one"), two = mdb.GetColumnFamily("two");
            PinnableAttributeGroup[] groups = [new(one), new(two), new(mdb.DefaultColumnFamily)];
            mdb.GetAttributeGroups(null, U("n"), groups);
            Assert.AreEqual("=n1;a=1;", Describe(groups[0].Columns));
            Assert.AreEqual("a=2;b=3;", Describe(groups[1].Columns));
            Assert.AreEqual(MultiGetStatus.NotFound, groups[2].Status);
            PinnableAttributeGroup[][] results = [[new(one), new(two)], [new(two)]];
            mdb.MultiGetAttributeGroups(null, [U("m"), U("n")], results);
            Assert.AreEqual("=m1;x=mx;", Describe(results[0][0].Columns));
            Assert.AreEqual("x=shadow;y=my;", Describe(results[0][1].Columns));
            Assert.AreEqual("a=2;b=3;", Describe(results[1][0].Columns));
            using CoalescingIterator it = mdb.NewCoalescingIterator(null, [one, two]);
            it.SeekToFirst();
            Assert.AreEqual("m", Encoding.UTF8.GetString(it.Key));
            Assert.AreEqual("=m1;x=shadow;y=my;", Describe(it.Columns));
            it.Next();
            Assert.AreEqual("=n1;a=2;b=3;", Describe(it.Columns));
            using AttributeGroupIterator ag = mdb.NewAttributeGroupIterator(null, [two, one]);
            ag.SeekToLast();
            Assert.AreEqual("n", Encoding.UTF8.GetString(ag.Key));
            Assert.AreEqual("two", ag.AttributeGroups[0].ColumnFamily.Name);
            Assert.AreEqual("=n1;a=1;", Describe(ag.AttributeGroups[1].Columns));
        }
    }
}
