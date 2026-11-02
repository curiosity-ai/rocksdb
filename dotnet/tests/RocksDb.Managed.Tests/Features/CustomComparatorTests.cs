using System.Globalization;
using System.Text;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Features;

/// <summary>Orders decimal-number keys by numeric value ("2" &lt; "10"); bytes compare as a tie breaker.</summary>
public sealed class NumericComparator : Comparator
{
    public override string Name => "test.NumericComparator";

    public override int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        int c = a.Length.CompareTo(b.Length); // no leading zeros: shorter number is smaller
        return c != 0 ? c : a.SequenceCompareTo(b);
    }
}

/// <summary>Custom comparators end to end (ColumnFamilyOptions.Comparator; VersionEditHandler comparator check).</summary>
[TestClass]
public sealed class CustomComparatorTests
{
    private static ColumnFamilyOptions CfOptions() => CfOptions(7);

    private static ColumnFamilyOptions CfOptions(int numLevels) => new()
    {
        Comparator = new NumericComparator(),
        WriteBufferSize = 64 << 10,
        NumLevels = numLevels,
    };

    private static byte[] N(int i) => B(i.ToString(CultureInfo.InvariantCulture));

    private static List<int> Scan(Iterator it)
    {
        var keys = new List<int>();
        for (it.SeekToFirst(); it.Valid; it.Next()) keys.Add(int.Parse(Encoding.UTF8.GetString(it.Key), CultureInfo.InvariantCulture));
        return keys;
    }

    private static void AssertNumericOrder(List<int> keys, int count)
    {
        Assert.AreEqual(count, keys.Count);
        for (int i = 0; i < keys.Count; i++) Assert.AreEqual(i, keys[i]);
    }

    private const int Count = 3000;

    private static void Fill(RocksDatabase db)
    {
        // Reverse insertion order with values large enough to spill several memtables.
        for (int i = Count - 1; i >= 0; i--) db.Put(N(i), B("v" + i + new string('x', 64)));
    }

    [TestMethod]
    public void FlushCompactIterateReopenWithCustomComparator()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), CfOptions()))
        {
            Fill(db);
            db.Flush();
            using (Iterator it = db.NewIterator()) AssertNumericOrder(Scan(it), Count);
            db.CompactRange();
            using (Iterator it = db.NewIterator())
            {
                AssertNumericOrder(Scan(it), Count);
                it.Seek(B("99"));
                Assert.AreEqual("99", Encoding.UTF8.GetString(it.Key));
                it.Next();
                Assert.AreEqual("100", Encoding.UTF8.GetString(it.Key));
                it.SeekForPrev(B("1000"));
                Assert.AreEqual("1000", Encoding.UTF8.GetString(it.Key));
                it.Prev();
                Assert.AreEqual("999", Encoding.UTF8.GetString(it.Key));
            }
            Assert.AreEqual("v42" + new string('x', 64), GetString(db, "42"));
            db.Put(N(Count), B("tail")); // stays in the WAL
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), CfOptions()))
        {
            using Iterator it = db.NewIterator();
            AssertNumericOrder(Scan(it), Count + 1);
            Assert.AreEqual("tail", GetString(db, Count.ToString(CultureInfo.InvariantCulture)));
            Assert.AreEqual("test.NumericComparator", db.DefaultColumnFamily.Options.ComparatorName);
        }
        ColumnFamilyDescriptor[] cfs = [new("default", CfOptions())];
        using (RocksDatabase ro = RocksDatabase.OpenReadOnly(DbPath, NewOptions(fs), cfs, errorIfWalFileExists: false))
        {
            using Iterator it = ro.NewIterator();
            AssertNumericOrder(Scan(it), Count + 1);
        }
        using (RocksDatabase primary = RocksDatabase.Open(DbPath, NewOptions(fs), CfOptions()))
        using (RocksDatabase secondary = RocksDatabase.OpenAsSecondary(DbPath, "/secondary", NewOptions(fs), cfs))
        {
            primary.Put(N(Count + 1), B("more"));
            primary.Flush();
            secondary.TryCatchUpWithPrimary();
            using Iterator it = secondary.NewIterator();
            AssertNumericOrder(Scan(it), Count + 2);
        }
    }

    [TestMethod]
    public void ComparatorMismatchIsInvalidArgument()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), CfOptions())) db.Put(N(1), B("x"));
        var e = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions()));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        StringAssert.Contains(e.Message, "does not match existing comparator test.NumericComparator");
        // The right name without a comparator object cannot be resolved.
        var named = new ColumnFamilyOptions();
        named.SetOption("comparator", "test.NumericComparator", preserveUnknown: false); // e.g. from an OPTIONS file
        Assert.IsNull(named.Comparator);
        Assert.AreEqual("test.NumericComparator", named.ComparatorName);
        e = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, NewOptions(fs), named));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        // Bytewise DB opened with a custom comparator.
        MemoryFileSystem fs2 = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs2))) db.Put(N(1), B("x"));
        e = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, NewOptions(fs2), CfOptions()));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        StringAssert.Contains(e.Message, "does not match existing comparator leveldb.BytewiseComparator");
    }

    [TestMethod]
    public void CustomComparatorColumnFamily()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.CreateMissingColumnFamilies = true;
        ColumnFamilyDescriptor[] cfs = [new("default", new ColumnFamilyOptions()), new("num", CfOptions())];
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o, cfs))
        {
            ColumnFamily num = db.GetColumnFamily("num");
            for (int i = 20; i >= 0; i--)
            {
                num.Put(N(i), B("n"));
                db.Put(N(i), B("d"));
            }
            num.Flush();
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o, cfs))
        {
            using (Iterator it = db.GetColumnFamily("num").NewIterator()) AssertNumericOrder(Scan(it), 21);
            using Iterator d = db.NewIterator();
            d.SeekToFirst();
            Assert.AreEqual("0", Encoding.UTF8.GetString(d.Key));
            d.Next();
            Assert.AreEqual("1", Encoding.UTF8.GetString(d.Key));
            d.Next();
            Assert.AreEqual("10", Encoding.UTF8.GetString(d.Key)); // bytewise
            ColumnFamily created = db.CreateColumnFamily("num2", CfOptions());
            for (int i = 0; i < 12; i++) created.Put(N(i), B("c"));
            created.Flush();
            using Iterator c = created.NewIterator();
            AssertNumericOrder(Scan(c), 12);
        }
    }

    [TestMethod]
    public void FewerNumLevelsThanDataIsInvalidArgument()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), CfOptions()))
        {
            Fill(db);
            db.CompactRange(); // data lands on the last level (6)
        }
        var e = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, NewOptions(fs), CfOptions(3)));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        StringAssert.Contains(e.Message, "db has more levels than options.num_levels");
        using RocksDatabase ok = RocksDatabase.Open(DbPath, NewOptions(fs), CfOptions());
        using Iterator it = ok.NewIterator();
        AssertNumericOrder(Scan(it), Count);
    }
}
