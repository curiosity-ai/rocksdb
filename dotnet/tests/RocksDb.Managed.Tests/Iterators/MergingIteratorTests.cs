using System.Globalization;
using RocksDb.Managed.Db;
using RocksDb.Managed.Db.Iterators;
using RocksDb.Managed.Table;
using RocksDb.Managed.Tests.Table;
using static RocksDb.Managed.Tests.Iterators.IterTestHelpers;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.Tests.Iterators;

/// <summary>
/// MergingIterator over heterogeneous children (vectors, memtables, tables, level iterators) vs. a sorted union.
/// Port of the randomized merging checks of table/table_test.cc (HarnessTest over MergingIterator) and
/// table/merger_test.cc.
/// </summary>
[TestClass]
public sealed class MergingIteratorTests
{
    private static readonly InternalKeyComparator Icmp = InternalKeyComparator.Bytewise;

    private static string Ik(ReadOnlySpan<byte> ikey) => S(DbFormat.ExtractUserKey(ikey)) + "@" + (DbFormat.ExtractFooter(ikey) >> 8).ToString(CultureInfo.InvariantCulture);

    private static (List<byte[]> All, List<InternalIterator> Children, List<IDisposable> Owned) Build(int seed, int numChildren, int perChild)
    {
        var rnd = new Random(seed);
        var all = new List<byte[]>();
        var children = new List<InternalIterator>();
        var owned = new List<IDisposable>();
        ulong seq = 1;
        for (int c = 0; c < numChildren; c++)
        {
            var entries = new List<(byte[] Key, byte[] Value)>();
            int n = c == 0 ? 0 : rnd.Next(perChild); // child 0 is empty
            for (int i = 0; i < n; i++)
            {
                byte[] k = TableTestUtil.IKey("k" + rnd.Next(200).ToString("D4", CultureInfo.InvariantCulture), seq++, EntryType.Value);
                entries.Add((k, B("v" + seq)));
                all.Add(k);
            }
            entries.Sort((a, b) => Icmp.Compare(a.Key, b.Key));
            switch (c % 3)
            {
                case 0:
                {
                    var ti = new TestInternalIterator();
                    foreach ((byte[] k, byte[] v) in entries)
                    {
                        DbFormat.UnpackSequenceAndType(DbFormat.ExtractFooter(k), out ulong s, out EntryType t);
                        ti.Add(DbFormat.ExtractUserKey(k).ToArray(), t, v, s);
                    }
                    ti.Finish();
                    children.Add(ti);
                    break;
                }
                case 1:
                {
                    var mem = new MemTable(Icmp, new MemTableOptions(), 0);
                    foreach ((byte[] k, byte[] v) in entries)
                    {
                        DbFormat.UnpackSequenceAndType(DbFormat.ExtractFooter(k), out ulong s, out EntryType t);
                        mem.Add(s, t, DbFormat.ExtractUserKey(k), v);
                    }
                    owned.Add(mem);
                    children.Add(new MemTableInternalIterator(mem));
                    break;
                }
                default:
                {
                    BlockBasedTableReader reader = TableTestUtil.Open(TableTestUtil.BuildTable(entries, new TableFormatOptions { BlockSize = 128 }));
                    owned.Add(reader);
                    children.Add(new TableInternalIterator(reader));
                    break;
                }
            }
        }
        all.Sort((a, b) => Icmp.Compare(a, b));
        return (all, children, owned);
    }

    private static int Lower(List<byte[]> all, byte[] target)
    {
        int i = 0;
        while (i < all.Count && Icmp.Compare(all[i], target) < 0) i++;
        return i;
    }

    [TestMethod]
    [DataRow(1)]
    [DataRow(2)]
    [DataRow(3)]
    public void RandomOperationsMatchSortedUnion(int seedOffset)
    {
        int seed = Environment.TickCount ^ seedOffset;
        (List<byte[]> all, List<InternalIterator> children, List<IDisposable> owned) = Build(seed, 7, 80);
        try
        {
            using var it = new MergingIterator(Icmp, System.Runtime.InteropServices.CollectionsMarshal.AsSpan(children));
            var rnd = new Random(seed);
            var seen = new List<string>();
            for (it.SeekToFirst(); it.Valid; it.Next()) seen.Add(Ik(it.Key));
            Assert.AreEqual(string.Join(" ", all.ConvertAll(k => Ik(k))), string.Join(" ", seen), "seed=" + seed);
            seen.Clear();
            for (it.SeekToLast(); it.Valid; it.Prev()) seen.Add(Ik(it.Key));
            seen.Reverse();
            Assert.AreEqual(string.Join(" ", all.ConvertAll(k => Ik(k))), string.Join(" ", seen), "seed=" + seed);

            for (int round = 0; round < 200; round++)
            {
                byte[] target = TableTestUtil.IKey("k" + rnd.Next(210).ToString("D4", CultureInfo.InvariantCulture), (ulong)rnd.Next(1, 600), EntryType.Value);
                int pos;
                if (rnd.Next(2) == 0)
                {
                    it.Seek(target);
                    pos = Lower(all, target);
                }
                else
                {
                    it.SeekForPrev(target);
                    pos = Lower(all, target);
                    if (pos >= all.Count || Icmp.Compare(all[pos], target) != 0) pos--;
                }
                for (int step = 0; step < 10; step++)
                {
                    if (pos < 0 || pos >= all.Count)
                    {
                        Assert.IsFalse(it.Valid, "seed=" + seed);
                        break;
                    }
                    Assert.IsTrue(it.Valid, "seed=" + seed);
                    Assert.AreEqual(Ik(all[pos]), Ik(it.Key), "seed=" + seed);
                    if (rnd.Next(2) == 0)
                    {
                        it.Next();
                        pos++;
                    }
                    else
                    {
                        it.Prev();
                        pos--;
                    }
                }
            }
        }
        finally
        {
            foreach (IDisposable d in owned) d.Dispose();
        }
    }

    [TestMethod]
    public async Task AsyncSeekAndNext()
    {
        int seed = Environment.TickCount;
        (List<byte[]> all, List<InternalIterator> children, List<IDisposable> owned) = Build(seed, 6, 60);
        try
        {
            using var it = new MergingIterator(Icmp, System.Runtime.InteropServices.CollectionsMarshal.AsSpan(children));
            var seen = new List<string>();
            await it.SeekToFirstAsync();
            while (it.Valid)
            {
                seen.Add(Ik(it.Key));
                await it.NextAsync();
            }
            Assert.AreEqual(string.Join(" ", all.ConvertAll(k => Ik(k))), string.Join(" ", seen), "seed=" + seed);
            if (all.Count > 0)
            {
                byte[] mid = all[all.Count / 2];
                await it.SeekAsync(mid);
                Assert.AreEqual(Ik(mid), Ik(it.Key));
            }
        }
        finally
        {
            foreach (IDisposable d in owned) d.Dispose();
        }
    }

    [TestMethod]
    public void EmptyAndErrorIterators()
    {
        using var it = new MergingIterator(Icmp, [EmptyInternalIterator.Instance, new TestInternalIterator()]);
        it.SeekToFirst();
        Assert.IsFalse(it.Valid);
        it.SeekForPrev(TableTestUtil.LookupKey("a"));
        Assert.IsFalse(it.Valid);

        using var err = new MergingIterator(Icmp, [new ErrorInternalIterator(StatusCode.IOError, "boom")]);
        var ex = Assert.ThrowsExactly<RocksDbException>(err.SeekToFirst);
        Assert.AreEqual(StatusCode.IOError, ex.Code);
    }
}
