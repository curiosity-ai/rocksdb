using System.Globalization;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>
/// Custom comparators across implementations: a DB written by native RocksDB with a custom comparator (or a comparator
/// named "rocksdb.ReverseBytewiseComparator") is opened by the managed port with the matching comparator, extended,
/// flushed and compacted, and handed back to native. A mismatching comparator fails on both sides.
/// </summary>
[TestClass]
public sealed class CustomComparatorCompatTests
{
    /// <summary>Managed: decimal keys by numeric value.</summary>
    private sealed class ManagedNumeric : Comparator
    {
        public override string Name => "test.NumericComparator";
        public override int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
        {
            int c = a.Length.CompareTo(b.Length);
            return c != 0 ? c : a.SequenceCompareTo(b);
        }
    }

    /// <summary>Native (rocksdb-sharp) comparator with the same order and a configurable name.</summary>
    private sealed unsafe class NativeComparator(string name, bool reverseBytewise) : RocksDbSharp.Comparator
    {
        public string Name => name;

        public int Compare(IntPtr a, UIntPtr alen, IntPtr b, UIntPtr blen)
        {
            var x = new ReadOnlySpan<byte>((void*)a, (int)alen);
            var y = new ReadOnlySpan<byte>((void*)b, (int)blen);
            if (reverseBytewise) return y.SequenceCompareTo(x);
            int c = x.Length.CompareTo(y.Length);
            return c != 0 ? c : x.SequenceCompareTo(y);
        }
    }

    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    private static RocksDbSharp.RocksDb OpenNative(string path, RocksDbSharp.Comparator? comparator)
    {
        var cf = new RocksDbSharp.ColumnFamilyOptions();
        if (comparator is not null) cf.SetComparator(comparator);
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        s_keepAlive.Add(cf);
        s_keepAlive.Add(o);
        return RocksDbSharp.RocksDb.Open(o, path, new RocksDbSharp.ColumnFamilies(cf));
    }

    private static byte[] N(int i) => Encoding.UTF8.GetBytes(i.ToString(CultureInfo.InvariantCulture));

    private static List<string> NativeScan(RocksDbSharp.RocksDb db)
    {
        var list = new List<string>();
        using RocksDbSharp.Iterator it = db.NewIterator();
        for (it.SeekToFirst(); it.Valid(); it.Next()) list.Add(Encoding.UTF8.GetString(it.Key()));
        return list;
    }

    private static List<string> ManagedScan(RocksDatabase db)
    {
        var list = new List<string>();
        using Iterator it = db.NewIterator();
        for (it.SeekToFirst(); it.Valid; it.Next()) list.Add(Encoding.UTF8.GetString(it.Key));
        return list;
    }

    private static List<string> Expected(int n, bool reverseBytewise)
    {
        var l = new List<string>();
        for (int i = 0; i < n; i++) l.Add(i.ToString(CultureInfo.InvariantCulture));
        if (reverseBytewise) l.Sort(static (a, b) => string.CompareOrdinal(b, a));
        return l;
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void NativeCustomComparatorDbRoundTrip(bool reverseBytewise)
    {
        using var dir = new TempDir("cmp");
        string name = reverseBytewise ? "rocksdb.ReverseBytewiseComparator" : "test.NumericComparator";
        Comparator managedCmp = reverseBytewise ? Comparator.ReverseBytewise : new ManagedNumeric();
        using (RocksDbSharp.RocksDb db = OpenNative(dir.Path, new NativeComparator(name, reverseBytewise)))
        {
            for (int i = 999; i >= 0; i--) db.Put(N(i), N(i * 2));
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            for (int i = 1000; i < 1500; i++) db.Put(N(i), N(i * 2)); // WAL only
        }
        var cfo = new ColumnFamilyOptions { Comparator = managedCmp };
        using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions(), cfo))
        {
            CollectionAssert.AreEqual(Expected(1500, reverseBytewise), ManagedScan(db));
            Assert.AreEqual("246", Encoding.UTF8.GetString(db.Get(N(123))!));
            for (int i = 1500; i < 2000; i++) db.Put(N(i), N(i * 2));
            db.Flush();
            db.CompactRange();
            CollectionAssert.AreEqual(Expected(2000, reverseBytewise), ManagedScan(db));
            for (int i = 2000; i < 2100; i++) db.Put(N(i), N(i * 2)); // WAL only
        }
        using (RocksDbSharp.RocksDb db = OpenNative(dir.Path, new NativeComparator(name, reverseBytewise)))
        {
            CollectionAssert.AreEqual(Expected(2100, reverseBytewise), NativeScan(db));
            Assert.AreEqual("4000", Encoding.UTF8.GetString(db.Get(N(2000))));
            db.CompactRange((byte[]?)null, null);
            CollectionAssert.AreEqual(Expected(2100, reverseBytewise), NativeScan(db));
        }
        using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions(), cfo))
        {
            CollectionAssert.AreEqual(Expected(2100, reverseBytewise), ManagedScan(db));
        }
    }

    [TestMethod]
    public void ComparatorMismatchFailsOnBothSides()
    {
        using var dir = new TempDir("cmpx");
        using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions { CreateIfMissing = true },
                   new ColumnFamilyOptions { Comparator = new ManagedNumeric() }))
        {
            db.Put(N(1), N(1));
            db.Flush();
        }
        // Native with the default (bytewise) comparator.
        var ex = Assert.ThrowsExactly<RocksDbSharp.RocksDbException>(() => OpenNative(dir.Path, null).Dispose());
        StringAssert.Contains(ex.Message, "does not match existing comparator test.NumericComparator");
        // Native with the matching comparator works.
        using (RocksDbSharp.RocksDb db = OpenNative(dir.Path, new NativeComparator("test.NumericComparator", false)))
        {
            Assert.AreEqual("1", Encoding.UTF8.GetString(db.Get(N(1))));
        }
        var e = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(dir.Path, new DbOptions()).Dispose());
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
    }
}
