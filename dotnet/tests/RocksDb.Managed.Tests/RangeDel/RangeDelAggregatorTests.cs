using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Tests.Table;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.Tests.RangeDel;

/// <summary>Ports of db/range_tombstone_fragmenter_test.cc and db/range_del_aggregator_test.cc.</summary>
[TestClass]
public sealed class RangeDelAggregatorTests
{
    private static readonly InternalKeyComparator Icmp = InternalKeyComparator.Bytewise;

    private static FragmentedRangeTombstoneList Frag(params (string Start, string End, ulong Seq)[] tombstones)
    {
        var l = new List<FragmentedRangeTombstoneList.Tombstone>();
        foreach ((string s, string e, ulong q) in tombstones) l.Add(new(Encoding.UTF8.GetBytes(s), Encoding.UTF8.GetBytes(e), q));
        return FragmentedRangeTombstoneList.Build(Comparator.Bytewise, l);
    }

    private static string Dump(FragmentedRangeTombstoneList f)
    {
        var sb = new StringBuilder();
        for (int i = 0; i < f.Count; i++)
        {
            if (i > 0) sb.Append(' ');
            sb.Append(Encoding.UTF8.GetString(f.GetStart(i))).Append('-').Append(Encoding.UTF8.GetString(f.GetEnd(i))).Append(':');
            sb.Append(string.Join(",", f.GetSequences(i).ToArray()));
        }
        return sb.ToString();
    }

    private static void VerifyShouldDelete(RangeDelAggregator agg, params (string Key, ulong Seq, bool Result)[] cases)
    {
        foreach ((string k, ulong s, bool r) in cases)
        {
            Assert.AreEqual(r, agg.ShouldDelete(Encoding.UTF8.GetBytes(k), s), k + "@" + s);
            Assert.AreEqual(r, agg.ShouldDelete(TableTestUtil.IKey(k, s, EntryType.Value)), k + "@" + s + " (internal key)");
        }
        // Again in reverse order (cursor reuse in the other direction).
        for (int i = cases.Length - 1; i >= 0; i--)
        {
            Assert.AreEqual(cases[i].Result, agg.ShouldDelete(Encoding.UTF8.GetBytes(cases[i].Key), cases[i].Seq), cases[i].Key + "@" + cases[i].Seq + " reverse");
        }
    }

    private static void VerifyIsRangeOverlapped(RangeDelAggregator agg, params (string Start, string End, bool Result)[] cases)
    {
        foreach ((string s, string e, bool r) in cases)
        {
            Assert.AreEqual(r, agg.IsRangeOverlapped(Encoding.UTF8.GetBytes(s), Encoding.UTF8.GetBytes(e), false), "[" + s + "," + e + "]");
        }
    }

    // ------------------------------------------------------------------ fragmenter

    [TestMethod]
    public void FragmenterNonOverlapping() => Assert.AreEqual("a-b:10 c-d:5", Dump(Frag(("a", "b", 10), ("c", "d", 5))));

    [TestMethod]
    public void FragmenterOverlapping() => Assert.AreEqual("a-c:10 c-e:15,10 e-g:15", Dump(Frag(("a", "e", 10), ("c", "g", 15))));

    [TestMethod]
    public void FragmenterContiguous() => Assert.AreEqual("a-c:10 c-e:20 e-g:15", Dump(Frag(("a", "c", 10), ("c", "e", 20), ("e", "g", 15))));

    [TestMethod]
    public void FragmenterRepeatedStartAndEndKey() => Assert.AreEqual("a-c:10,7,3", Dump(Frag(("a", "c", 10), ("a", "c", 7), ("a", "c", 3))));

    [TestMethod]
    public void FragmenterRepeatedStartKeyDifferentEndKeys() =>
        Assert.AreEqual("a-c:10,7,3 c-e:10,3 e-g:3", Dump(Frag(("a", "e", 10), ("a", "g", 7 - 4), ("a", "c", 7))));

    [TestMethod]
    public void FragmenterOverlapAndRepeatedStartKey() =>
        Assert.AreEqual("a-c:10 c-e:10,8,6 e-g:8,6 g-i:6 j-l:4,2 l-n:4",
            Dump(Frag(("a", "e", 10), ("c", "g", 8), ("c", "i", 6), ("j", "n", 4), ("j", "l", 2))));

    [TestMethod]
    public void FragmenterUnorderedInputAndEmptyRanges() =>
        Assert.AreEqual("a-c:10 c-e:10,8,6 e-g:8,6 g-i:6 j-l:4,2 l-n:4",
            Dump(Frag(("j", "n", 4), ("c", "i", 6), ("c", "g", 8), ("j", "l", 2), ("a", "e", 10), ("z", "z", 99), ("q", "p", 50))));

    [TestMethod]
    public void MaxCoveringTombstoneSeqnum()
    {
        FragmentedRangeTombstoneList f = Frag(("a", "e", 10), ("c", "g", 8), ("c", "i", 6), ("j", "n", 4), ("j", "l", 2));
        var agg = new RangeDelAggregator(Icmp, 9);
        agg.AddTombstones(f);
        Assert.AreEqual(0UL, agg.MaxCoveringTombstoneSeqnum("_"u8));
        Assert.AreEqual(0UL, agg.MaxCoveringTombstoneSeqnum("a"u8)); // 10 is not visible at 9
        Assert.AreEqual(8UL, agg.MaxCoveringTombstoneSeqnum("c"u8));
        Assert.AreEqual(8UL, agg.MaxCoveringTombstoneSeqnum("f"u8));
        Assert.AreEqual(6UL, agg.MaxCoveringTombstoneSeqnum("h"u8));
        Assert.AreEqual(0UL, agg.MaxCoveringTombstoneSeqnum("i"u8));
        Assert.AreEqual(4UL, agg.MaxCoveringTombstoneSeqnum("k"u8));
        Assert.AreEqual(0UL, agg.MaxCoveringTombstoneSeqnum("n"u8));
        Assert.AreEqual(f.MaxCoveringTombstoneSeqnum("f"u8, 9), agg.MaxCoveringTombstoneSeqnum("f"u8));
    }

    // ------------------------------------------------------------------ aggregator

    [TestMethod]
    public void SingleIterInAggregator()
    {
        var agg = new RangeDelAggregator(Icmp, DbFormat.MaxSequenceNumber);
        agg.AddTombstones(Frag(("a", "e", 10), ("c", "g", 8)));
        VerifyShouldDelete(agg, ("a", 19, false), ("b", 9, true), ("d", 9, true), ("e", 7, true), ("g", 7, false));
        VerifyIsRangeOverlapped(agg, ("", "_", false), ("_", "a", true), ("a", "c", true), ("d", "f", true), ("g", "l", false));
        Assert.IsFalse(agg.IsRangeOverlapped("_"u8, "a"u8, true));
    }

    [TestMethod]
    public void MultipleItersInAggregator()
    {
        var agg = new RangeDelAggregator(Icmp, DbFormat.MaxSequenceNumber);
        agg.AddTombstones(Frag(("a", "e", 10), ("c", "g", 8)));
        agg.AddTombstones(Frag(("a", "b", 20), ("h", "i", 25), ("ii", "j", 15)));
        VerifyShouldDelete(agg, ("a", 19, true), ("b", 19, false), ("b", 9, true), ("d", 9, true), ("e", 7, true), ("g", 7, false),
            ("h", 24, true), ("i", 24, false), ("ii", 14, true), ("j", 14, false));
        VerifyIsRangeOverlapped(agg, ("", "_", false), ("_", "a", true), ("a", "c", true), ("d", "f", true), ("g", "l", true), ("x", "y", false));
    }

    [TestMethod]
    public void MultipleItersInAggregatorWithUpperBound()
    {
        var agg = new RangeDelAggregator(Icmp, 19);
        agg.AddTombstones(Frag(("a", "e", 10), ("c", "g", 8)));
        agg.AddTombstones(Frag(("a", "b", 20), ("h", "i", 25), ("ii", "j", 15)));
        VerifyShouldDelete(agg, ("a", 19, false), ("a", 9, true), ("b", 9, true), ("d", 9, true), ("e", 7, true), ("g", 7, false),
            ("h", 24, false), ("i", 24, false), ("ii", 14, true), ("j", 14, false));
        VerifyIsRangeOverlapped(agg, ("", "_", false), ("_", "a", true), ("a", "c", true), ("d", "f", true), ("g", "l", true), ("x", "y", false));
    }

    [TestMethod]
    public void MultipleTruncatedItersInAggregator()
    {
        (byte[], byte[])[] bounds =
        [
            (TableTestUtil.IKey("a", 4, EntryType.Value), TableTestUtil.IKey("m", DbFormat.MaxSequenceNumber, EntryType.RangeDeletion)),
            (TableTestUtil.IKey("m", 20, EntryType.Value), TableTestUtil.IKey("x", DbFormat.MaxSequenceNumber, EntryType.RangeDeletion)),
            (TableTestUtil.IKey("x", 5, EntryType.Value), TableTestUtil.IKey("zz", 30, EntryType.Value)),
        ];
        var agg = new RangeDelAggregator(Icmp, 19);
        for (int i = 0; i < 3; i++)
        {
            agg.AddTombstones(Frag(("a", "z", 10)), bounds[i].Item1, bounds[i].Item2);
            // Same-level incremental adds (MultipleTruncatedItersInAggregatorSameLevel).
            if (i == 0) VerifyShouldDelete(agg, ("a", 10, false), ("a", 9, false), ("a", 4, true));
            if (i == 1) VerifyShouldDelete(agg, ("m", 10, false), ("m", 9, true));
        }
        VerifyShouldDelete(agg, ("a", 10, false), ("a", 9, false), ("a", 4, true), ("m", 10, false), ("m", 9, true),
            ("x", 10, false), ("x", 9, false), ("x", 5, true), ("z", 9, false));
        VerifyIsRangeOverlapped(agg, ("", "_", false), ("_", "a", true), ("a", "n", true), ("l", "x", true), ("w", "z", true), ("zzz", "zz", false), ("zz", "zzz", false));
    }

    [TestMethod]
    public void TruncationCoversLargestKeyOfFile()
    {
        // File [b@50 .. d@40]: the tombstone [a, z)@60 is truncated to [b@50, d@39 kTypeMaxValid).
        var agg = new RangeDelAggregator(Icmp, DbFormat.MaxSequenceNumber);
        agg.AddTombstones(Frag(("a", "z", 60)), TableTestUtil.IKey("b", 50, EntryType.Value), TableTestUtil.IKey("d", 40, EntryType.Value));
        VerifyShouldDelete(agg, ("a", 1, false), ("b", 55, false), ("b", 50, true), ("c", 1, true), ("d", 40, true), ("d", 39, false), ("e", 1, false));
        // Largest with seqno 0: unchanged bound (exclusive at d@0).
        var agg0 = new RangeDelAggregator(Icmp, DbFormat.MaxSequenceNumber);
        agg0.AddTombstones(Frag(("a", "z", 60)), TableTestUtil.IKey("b", 50, EntryType.Value), TableTestUtil.IKey("d", 0, EntryType.Value));
        VerifyShouldDelete(agg0, ("c", 1, true), ("d", 5, true), ("e", 1, false));
    }

    [TestMethod]
    public void CompactionAggregatorNoSnapshots()
    {
        var agg = RangeDelAggregator.ForCompaction(Icmp, []);
        agg.AddTombstones(Frag(("a", "e", 10), ("c", "g", 8)));
        agg.AddTombstones(Frag(("a", "b", 20), ("h", "i", 25), ("ii", "j", 15)));
        VerifyShouldDelete(agg, ("a", 19, true), ("b", 19, false), ("b", 9, true), ("d", 9, true), ("e", 7, true), ("g", 7, false),
            ("h", 24, true), ("i", 24, false), ("ii", 14, true), ("j", 14, false));
    }

    [TestMethod]
    public void CompactionAggregatorWithSnapshots()
    {
        var agg = RangeDelAggregator.ForCompaction(Icmp, [19, 9]);
        agg.AddTombstones(Frag(("a", "e", 10), ("c", "g", 8)));
        agg.AddTombstones(Frag(("a", "b", 20), ("h", "i", 25), ("ii", "j", 15)));
        VerifyShouldDelete(agg,
            ("a", 19, false), // [10, 19]
            ("a", 9, false),  // [0, 9]
            ("b", 9, false),  // [0, 9]
            ("d", 9, false),  // [0, 9]
            ("d", 7, true),   // [0, 9]
            ("e", 7, true),   // [0, 9]
            ("g", 7, false),  // [0, 9]
            ("h", 24, true),  // [20, kMaxSequenceNumber]
            ("i", 24, false), // [20, kMaxSequenceNumber]
            ("ii", 14, true), // [10, 19]
            ("j", 14, false)); // [10, 19]
    }

    [TestMethod]
    public void EmptyAggregator()
    {
        var agg = new RangeDelAggregator(Icmp, 100);
        agg.AddTombstones(Frag());
        Assert.IsTrue(agg.IsEmpty);
        Assert.IsFalse(agg.ShouldDelete("a"u8, 1));
        Assert.AreEqual(0UL, agg.MaxCoveringTombstoneSeqnum("a"u8));
    }

    [TestMethod]
    public void RandomizedAgainstBruteForce()
    {
        int seed = Environment.TickCount;
        var rnd = new Random(seed);
        for (int round = 0; round < 50; round++)
        {
            var all = new List<(string, string, ulong)>();
            var agg = new RangeDelAggregator(Icmp, (ulong)rnd.Next(1, 120));
            ulong readSeq = (ulong)rnd.Next(1, 120);
            agg = new RangeDelAggregator(Icmp, readSeq);
            for (int src = 0; src < 3; src++)
            {
                var ts = new List<(string, string, ulong)>();
                for (int i = 0; i < rnd.Next(0, 8); i++)
                {
                    int s = rnd.Next(26), e = s + rnd.Next(0, 6);
                    ts.Add((((char)('a' + s)).ToString(), ((char)('a' + Math.Min(25, e))).ToString(), (ulong)rnd.Next(1, 100)));
                }
                all.AddRange(ts);
                agg.AddTombstones(Frag(ts.ToArray()));
            }
            // Mostly sequential queries (forward and backward) to exercise the cursors, plus random ones.
            for (int q = 0; q < 200; q++)
            {
                int ki = q < 80 ? q % 27 : q < 160 ? 26 - q % 27 : rnd.Next(27);
                string k = ((char)('a' + ki)).ToString();
                ulong seq = (ulong)rnd.Next(0, 110);
                bool expected = false;
                ulong maxCover = 0;
                foreach ((string s, string e, ulong t) in all)
                {
                    if (string.CompareOrdinal(s, k) <= 0 && string.CompareOrdinal(k, e) < 0 && t <= readSeq)
                    {
                        if (t > seq) expected = true;
                        if (t > maxCover) maxCover = t;
                    }
                }
                Assert.AreEqual(expected, agg.ShouldDelete(Encoding.UTF8.GetBytes(k), seq), "seed=" + seed + " " + k + "@" + seq);
                Assert.AreEqual(maxCover, agg.MaxCoveringTombstoneSeqnum(Encoding.UTF8.GetBytes(k)), "seed=" + seed + " " + k);
            }
        }
    }

    [TestMethod]
    public void TableTombstonesAreLoadedAndCached()
    {
        var entries = new List<(byte[] Key, byte[] Value)>
        {
            (TableTestUtil.IKey("b", 5, EntryType.Value), "x"u8.ToArray()),
            (TableTestUtil.IKey("a", 10, EntryType.RangeDeletion), "c"u8.ToArray()),
            (TableTestUtil.IKey("b", 12, EntryType.RangeDeletion), "f"u8.ToArray()),
        };
        using RocksDb.Managed.Table.BlockBasedTableReader reader = TableTestUtil.Open(TableTestUtil.BuildTable(entries, RocksDb.Managed.Table.TableFormatOptions.Default));
        FragmentedRangeTombstoneList? f = RangeDelTableTombstones.Get(reader);
        Assert.IsNotNull(f);
        Assert.AreEqual("a-b:10 b-c:12,10 c-f:12", Dump(f));
        Assert.AreSame(f, RangeDelTableTombstones.Get(reader));

        using RocksDb.Managed.Table.BlockBasedTableReader plain = TableTestUtil.Open(TableTestUtil.BuildTable([(TableTestUtil.IKey("a", 1, EntryType.Value), [])], RocksDb.Managed.Table.TableFormatOptions.Default));
        Assert.IsNull(RangeDelTableTombstones.Get(plain));
    }
}
