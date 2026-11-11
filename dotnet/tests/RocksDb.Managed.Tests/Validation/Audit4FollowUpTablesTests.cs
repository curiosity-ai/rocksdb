using System.Globalization;
using RocksDb.Managed.Db;
using RocksDb.Managed.Tests.Table;
using static RocksDb.Managed.Tests.Db.DbTestUtil;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4 follow-up (tables): range tombstones, skip list / arenas, MultiGet and the table readers, filters and
/// block builders, flush scheduling. Each test names the C++ function it pins down.
/// </summary>
[TestClass]
public sealed class Audit4FollowUpTablesTests
{
    // ------------------------------------------------------------------------------------------ range tombstones

    private sealed class RangeDelModel
    {
        public readonly Comparator Ucmp;
        public readonly InternalKeyComparator Icmp;
        public readonly bool Reverse;
        public readonly List<(int Pos, ulong Seq)> Points = [];
        public readonly List<(int Start, int End, ulong Seq)> Tombstones = [];
        public readonly List<File> Files = [];

        public sealed class File
        {
            public int First, Last; // indices into Points (inclusive)
            public byte[] Smallest = [], Largest = [];
            public FragmentedRangeTombstoneList List = null!;
        }

        public RangeDelModel(bool reverse)
        {
            Reverse = reverse;
            Ucmp = reverse ? Comparator.ReverseBytewise : Comparator.Bytewise;
            Icmp = new InternalKeyComparator(Ucmp);
        }

        /// <summary>The user key at position <paramref name="pos"/> of the comparator's order (0..30).</summary>
        public byte[] UserKey(int pos) => B("k" + (Reverse ? 30 - pos : pos).ToString("D2", CultureInfo.InvariantCulture));

        public byte[] IKey(int pos, ulong seq, EntryType type) => TableTestUtil.IKey(UserKey(pos), seq, type);

        /// <summary>The truth: is (pos, seq) covered by a tombstone t with seq &lt; t &lt;= upper?</summary>
        public bool Covered(int pos, ulong seq, ulong upper)
        {
            foreach ((int a, int b, ulong t) in Tombstones)
            {
                if (a <= pos && pos < b && t > seq && t <= upper) return true;
            }
            return false;
        }

        public ulong MaxCovering(int pos, ulong upper)
        {
            ulong best = 0;
            foreach ((int a, int b, ulong t) in Tombstones)
            {
                if (a <= pos && pos < b && t <= upper && t > best) best = t;
            }
            return best;
        }

        public FragmentedRangeTombstoneList Build(List<(int Start, int End, ulong Seq)> tombstones)
        {
            var l = new List<FragmentedRangeTombstoneList.Tombstone>();
            foreach ((int a, int b, ulong t) in tombstones) l.Add(new(UserKey(a), UserKey(b), t));
            return FragmentedRangeTombstoneList.Build(Ucmp, l);
        }

        /// <summary>
        /// Random point entries and tombstones (unique sequence numbers, plus some seqno-0 entries as a bottommost
        /// compaction leaves them), cut into the files of one sorted run the way CompactionOutputs::AddRangeDels
        /// (db/compaction/compaction_outputs.cc:598-900) does: a file gets every tombstone overlapping
        /// [lower_bound, upper_bound], stored untruncated, and its smallest / largest keys are extended by the
        /// tombstones clipped to those bounds.
        /// </summary>
        public void Generate(Random rnd)
        {
            var seqs = new List<ulong>();
            for (ulong s = 1; s <= 120; s++) seqs.Add(s);
            for (int i = seqs.Count - 1; i > 0; i--)
            {
                int j = rnd.Next(i + 1);
                (seqs[i], seqs[j]) = (seqs[j], seqs[i]);
            }
            int next = 0;
            for (int pos = 0; pos < 30; pos++)
            {
                if (rnd.Next(3) == 0) continue;
                int versions = rnd.Next(1, 5);
                for (int v = 0; v < versions; v++) Points.Add((pos, seqs[next++]));
                if (rnd.Next(4) == 0) Points.Add((pos, 0));
            }
            if (Points.Count == 0) Points.Add((7, seqs[next++]));
            Points.Sort(static (x, y) => x.Pos != y.Pos ? x.Pos.CompareTo(y.Pos) : y.Seq.CompareTo(x.Seq));
            int tombstones = rnd.Next(1, 9);
            for (int i = 0; i < tombstones; i++)
            {
                int a = rnd.Next(30), b = a + 1 + rnd.Next(rnd.Next(2) == 0 ? 3 : 30 - a);
                Tombstones.Add((a, Math.Min(b, 30), seqs[next++]));
            }

            // Cut the sorted entries into 1..4 files, possibly between two versions of a user key.
            var cuts = new SortedSet<int> { 0 };
            int files = rnd.Next(1, 5);
            for (int i = 1; i < files; i++) cuts.Add(rnd.Next(Points.Count));
            int[] starts = [.. cuts];
            for (int i = 0; i < starts.Length; i++)
            {
                var f = new File { First = starts[i], Last = (i + 1 < starts.Length ? starts[i + 1] : Points.Count) - 1 };
                (int fp, ulong fs) = Points[f.First];
                (int lp, ulong ls) = Points[f.Last];
                byte[] smallest = IKey(fp, fs, EntryType.Value), largest = IKey(lp, ls, EntryType.Value);
                byte[]? lower = i == 0 ? null : smallest;
                byte[]? upper = null;
                if (i + 1 < starts.Length)
                {
                    int np = Points[starts[i + 1]].Pos;
                    upper = np == lp ? largest : IKey(np, DbFormat.MaxSequenceNumber, EntryType.RangeDeletion);
                }
                var mine = new List<(int, int, ulong)>();
                foreach ((int a, int b, ulong t) in Tombstones)
                {
                    byte[] end = IKey(b, DbFormat.MaxSequenceNumber, EntryType.RangeDeletion);
                    if (lower is not null && Icmp.Compare(end, lower) <= 0) continue;
                    byte[] start = IKey(lower is not null && a < fp ? fp : a, t, EntryType.RangeDeletion);
                    if (upper is not null && Icmp.Compare(upper, start) < 0) continue;
                    if (lower is not null && Icmp.Compare(start, lower) < 0) start = lower;
                    if (upper is not null && Icmp.Compare(upper, end) < 0) end = upper;
                    mine.Add((a, b, t));
                    if (Icmp.Compare(start, smallest) < 0) smallest = start;
                    if (Icmp.Compare(largest, end) < 0) largest = end;
                }
                f.Smallest = smallest;
                f.Largest = largest;
                f.List = Build(mine);
                Files.Add(f);
            }
        }
    }

    /// <summary>
    /// TruncatedRangeDelIterator (db/range_del_aggregator.cc:23-80, start_key / end_key in range_del_aggregator.h:66-78),
    /// ForwardRangeDelIterator / ReverseRangeDelIterator::ShouldDelete (:186-255), StripeRep::InStripe,
    /// CompactionRangeDelAggregator::ShouldDelete (:408-415, the stripe of FragmentedRangeTombstoneIterator::
    /// SplitBySnapshot, range_tombstone_fragmenter.cc:518-542) and FragmentedRangeTombstoneIterator::
    /// MaxCoveringTombstoneSeqnum (:486-493), against a brute-force model: random tombstones, random snapshots and
    /// random file boundaries (also between two versions of one user key, and seqno-0 largest keys). A file's
    /// tombstones answer the truth for the entries of that file and nothing for the entries of its neighbours; all
    /// files together answer the truth for every entry, in any query order.
    /// </summary>
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void RangeTombstonesWithFileBoundariesAndSnapshotsMatchBruteForce(bool reverseComparator)
    {
        int seed = Environment.TickCount;
        var rnd = new Random(seed);
        for (int round = 0; round < 150; round++)
        {
            string ctx = "seed=" + seed + " round=" + round;
            var m = new RangeDelModel(reverseComparator);
            m.Generate(rnd);
            FragmentedRangeTombstoneList whole = m.Build(m.Tombstones);

            // Fragments: sorted, non-overlapping, non-empty, sequence numbers newest first (FragmentTombstones).
            for (int i = 0; i < whole.Count; i++)
            {
                Assert.IsTrue(m.Ucmp.Compare(whole.GetStart(i), whole.GetEnd(i)) < 0, ctx);
                if (i > 0) Assert.IsTrue(m.Ucmp.Compare(whole.GetEnd(i - 1), whole.GetStart(i)) <= 0, ctx);
                ReadOnlySpan<ulong> s = whole.GetSequences(i);
                Assert.IsTrue(s.Length > 0, ctx);
                for (int k = 1; k < s.Length; k++) Assert.IsTrue(s[k - 1] > s[k], ctx);
            }

            ulong[] reads = [DbFormat.MaxSequenceNumber, (ulong)rnd.Next(0, 125), (ulong)rnd.Next(0, 125), (ulong)rnd.Next(0, 125)];
            foreach (ulong read in reads)
            {
                // The untruncated list of a point lookup.
                for (int pos = 0; pos <= 30; pos++)
                {
                    Assert.AreEqual(m.MaxCovering(pos, read), whole.MaxCoveringTombstoneSeqnum(m.UserKey(pos), read), ctx + " max covering " + pos + " read=" + read);
                }

                // One file alone: the truth inside the file, nothing outside.
                for (int fi = 0; fi < m.Files.Count; fi++)
                {
                    RangeDelModel.File f = m.Files[fi];
                    var one = new RangeDelAggregator(m.Icmp, read);
                    one.AddTombstones(f.List, f.Smallest, f.Largest);
                    for (int p = 0; p < m.Points.Count; p++)
                    {
                        (int pos, ulong seq) = m.Points[p];
                        bool expected = p >= f.First && p <= f.Last && m.Covered(pos, seq, read);
                        Assert.AreEqual(expected, one.ShouldDelete(m.UserKey(pos), seq), ctx + " file " + fi + " " + pos + "@" + seq + " read=" + read);
                    }
                }

                // All files: forward, backward and random query order (the per-source cursors).
                var all = new RangeDelAggregator(m.Icmp, read);
                foreach (RangeDelModel.File f in m.Files) all.AddTombstones(f.List, f.Smallest, f.Largest);
                for (int pass = 0; pass < 3; pass++)
                {
                    for (int q = 0; q < m.Points.Count; q++)
                    {
                        int p = pass == 0 ? q : pass == 1 ? m.Points.Count - 1 - q : rnd.Next(m.Points.Count);
                        (int pos, ulong seq) = m.Points[p];
                        Assert.AreEqual(m.Covered(pos, seq, read), all.ShouldDelete(m.IKey(pos, seq, EntryType.Value)), ctx + " all " + pos + "@" + seq + " read=" + read + " pass " + pass);
                    }
                }
            }

            // Compaction: a tombstone only deletes entries of its own snapshot stripe (lower_bound(seq) in reps_).
            var snapshots = new List<ulong>();
            int ns = rnd.Next(0, 5);
            for (int i = 0; i < ns; i++) snapshots.Add((ulong)rnd.Next(0, 125));
            ulong[] sorted = [.. snapshots];
            Array.Sort(sorted);
            RangeDelAggregator comp = RangeDelAggregator.ForCompaction(m.Icmp, snapshots.ToArray());
            foreach (RangeDelModel.File f in m.Files) comp.AddTombstones(f.List, f.Smallest, f.Largest);
            for (int pass = 0; pass < 2; pass++)
            {
                for (int q = 0; q < m.Points.Count; q++)
                {
                    (int pos, ulong seq) = m.Points[pass == 0 ? q : m.Points.Count - 1 - q];
                    ulong stripeUpper = DbFormat.MaxSequenceNumber;
                    foreach (ulong s in sorted)
                    {
                        if (s >= seq)
                        {
                            stripeUpper = s;
                            break;
                        }
                    }
                    Assert.AreEqual(m.Covered(pos, seq, stripeUpper), comp.ShouldDelete(m.UserKey(pos), seq),
                        ctx + " compaction " + pos + "@" + seq + " snapshots=" + string.Join(",", sorted));
                }
            }
        }
    }

    private static RangeDelAggregator OneFileAggregator(ulong tombstoneSeq, byte[] smallest, byte[] largest)
    {
        var icmp = new InternalKeyComparator(Comparator.Bytewise);
        FragmentedRangeTombstoneList list = FragmentedRangeTombstoneList.Build(Comparator.Bytewise,
            [new FragmentedRangeTombstoneList.Tombstone(B("b"), B("x"), tombstoneSeq)]);
        var agg = new RangeDelAggregator(icmp, DbFormat.MaxSequenceNumber);
        agg.AddTombstones(list, smallest, largest);
        return agg;
    }

    /// <summary>
    /// TruncatedRangeDelIterator's constructor (db/range_del_aggregator.cc:23-80) and ForwardRangeDelIterator::ShouldDelete
    /// (:186-212): an entry is covered iff start_key() &lt;= entry &lt; end_key() in internal key order, with
    /// smallest' = (smallest, kTypeMaxValid) and largest' = (largest.seq - 1, kTypeMaxValid), a seqno-0 largest left
    /// unchanged and a range-deletion sentinel left unchanged. The versions of the boundary user keys that belong to the
    /// neighbouring files are not covered, whatever the entry's type.
    /// </summary>
    [TestMethod]
    public void TruncatedTombstoneCoversOnlyTheBoundaryVersionsOfItsFile()
    {
        byte[] d = B("d"), m = B("m");
        // Tombstone [b, x)@30; file [d@20 .. m@7].
        RangeDelAggregator agg = OneFileAggregator(30, TableTestUtil.IKey(d, 20, EntryType.Value), TableTestUtil.IKey(m, 7, EntryType.Value));
        foreach (EntryType type in new[] { EntryType.Deletion, EntryType.Value, EntryType.Merge, EntryType.SingleDeletion })
        {
            string ctx = type.ToString();
            Assert.IsFalse(agg.ShouldDelete(TableTestUtil.IKey(d, 25, type)), ctx);  // before smallest': the previous file's
            Assert.IsFalse(agg.ShouldDelete(TableTestUtil.IKey(d, 21, type)), ctx);
            Assert.IsTrue(agg.ShouldDelete(TableTestUtil.IKey(d, 20, type)), ctx);   // smallest itself (kTypeMaxValid sorts first)
            Assert.IsTrue(agg.ShouldDelete(TableTestUtil.IKey(d, 3, type)), ctx);
            Assert.IsFalse(agg.ShouldDelete(TableTestUtil.IKey(B("c~"), 1, type)), ctx); // user key before the file
            Assert.IsTrue(agg.ShouldDelete(TableTestUtil.IKey(B("g"), 29, type)), ctx);
            Assert.IsFalse(agg.ShouldDelete(TableTestUtil.IKey(B("g"), 30, type)), ctx); // not newer than the entry
            Assert.IsTrue(agg.ShouldDelete(TableTestUtil.IKey(m, 8, type)), ctx);
            Assert.IsTrue(agg.ShouldDelete(TableTestUtil.IKey(m, 7, type)), ctx);    // largest itself: seq - 1 keeps it covered
            Assert.IsFalse(agg.ShouldDelete(TableTestUtil.IKey(m, 6, type)), ctx);   // largest' = (m, 6, kTypeMaxValid) is exclusive
            Assert.IsFalse(agg.ShouldDelete(TableTestUtil.IKey(m, 1, type)), ctx);
            Assert.IsFalse(agg.ShouldDelete(TableTestUtil.IKey(B("n"), 1, type)), ctx);
        }
        // The point lookup (user-key granularity) sees the tombstone for both boundary user keys.
        Assert.AreEqual(30UL, agg.MaxCoveringTombstoneSeqnum(d));
        Assert.AreEqual(30UL, agg.MaxCoveringTombstoneSeqnum(m));
        Assert.AreEqual(0UL, agg.MaxCoveringTombstoneSeqnum(B("c")));
        Assert.AreEqual(0UL, agg.MaxCoveringTombstoneSeqnum(B("n")));

        // Largest with seqno 0: left unchanged, so it is exclusive (the entry equal to it is not covered).
        agg = OneFileAggregator(30, TableTestUtil.IKey(d, 20, EntryType.Value), TableTestUtil.IKey(m, 0, EntryType.Value));
        Assert.IsTrue(agg.ShouldDelete(TableTestUtil.IKey(m, 1, EntryType.Value)));
        Assert.IsFalse(agg.ShouldDelete(TableTestUtil.IKey(m, 0, EntryType.Value)));
        // The same with a deletion as the largest key (type 0 sorts last): parsed < largest_ is false for the key itself.
        agg = OneFileAggregator(30, TableTestUtil.IKey(d, 20, EntryType.Value), TableTestUtil.IKey(m, 0, EntryType.Deletion));
        Assert.IsTrue(agg.ShouldDelete(TableTestUtil.IKey(m, 1, EntryType.Deletion)));
        Assert.IsFalse(agg.ShouldDelete(TableTestUtil.IKey(m, 0, EntryType.Deletion)));

        // Range-deletion sentinel (m, kMaxSequenceNumber, kTypeRangeDeletion): no version of m is covered.
        agg = OneFileAggregator(30, TableTestUtil.IKey(d, 20, EntryType.Value), TableTestUtil.IKey(m, DbFormat.MaxSequenceNumber, EntryType.RangeDeletion));
        Assert.IsTrue(agg.ShouldDelete(TableTestUtil.IKey(B("l~"), 1, EntryType.Value)));
        Assert.IsFalse(agg.ShouldDelete(TableTestUtil.IKey(m, 29, EntryType.Value)));
        Assert.IsFalse(agg.ShouldDelete(TableTestUtil.IKey(m, 0, EntryType.Value)));
        Assert.AreEqual(0UL, agg.MaxCoveringTombstoneSeqnum(m));
    }

    /// <summary>
    /// RangeDelAggregator::StripeRep::IsRangeOverlapped (db/range_del_aggregator.cc:298-336) over a truncated tombstone:
    /// overlap iff (start, kMaxSequenceNumber, 0) &lt; end_key() and start_key() &lt;= (end, exclusive ? kMaxSequenceNumber
    /// : 0, 0). The truncated end (largest.seq - 1, kTypeMaxValid) therefore includes the largest user key, a sentinel
    /// largest and an untruncated end do not; the truncated start includes the smallest user key only for an inclusive
    /// range end.
    /// </summary>
    [TestMethod]
    public void IsRangeOverlappedHonoursTruncatedBoundaries()
    {
        byte[] sm = TableTestUtil.IKey(B("d"), 20, EntryType.Value);
        RangeDelAggregator agg = OneFileAggregator(10, sm, TableTestUtil.IKey(B("m"), 7, EntryType.Value));
        Assert.IsTrue(agg.IsRangeOverlapped(B("m"), B("z"), false));   // starts at the largest user key: covered
        Assert.IsTrue(agg.IsRangeOverlapped(B("m"), B("z"), true));
        Assert.IsFalse(agg.IsRangeOverlapped(B("n"), B("z"), false));  // the tombstone's [n, x) part is outside the file
        Assert.IsFalse(agg.IsRangeOverlapped(B("a"), B("c"), false));  // and so is [b, d)
        Assert.IsTrue(agg.IsRangeOverlapped(B("a"), B("d"), false));
        Assert.IsFalse(agg.IsRangeOverlapped(B("a"), B("d"), true));
        Assert.IsTrue(agg.IsRangeOverlapped(B("e"), B("f"), true));
        Assert.IsTrue(agg.IsRangeOverlapped(B("a"), B("z"), true));

        agg = OneFileAggregator(10, sm, TableTestUtil.IKey(B("m"), DbFormat.MaxSequenceNumber, EntryType.RangeDeletion));
        Assert.IsFalse(agg.IsRangeOverlapped(B("m"), B("z"), false));  // sentinel: the end is exclusive
        Assert.IsTrue(agg.IsRangeOverlapped(B("l"), B("z"), false));

        // Untruncated (memtable): [b, x), end exclusive, start inclusive.
        var icmp = new InternalKeyComparator(Comparator.Bytewise);
        var mem = new RangeDelAggregator(icmp, DbFormat.MaxSequenceNumber);
        mem.AddTombstones(FragmentedRangeTombstoneList.Build(Comparator.Bytewise, [new FragmentedRangeTombstoneList.Tombstone(B("b"), B("x"), 10)]));
        Assert.IsFalse(mem.IsRangeOverlapped(B("x"), B("z"), false));
        Assert.IsTrue(mem.IsRangeOverlapped(B("w"), B("z"), false));
        Assert.IsTrue(mem.IsRangeOverlapped(B("a"), B("b"), false));
        Assert.IsFalse(mem.IsRangeOverlapped(B("a"), B("b"), true));
        // A tombstone newer than the read sequence is invisible.
        var old = new RangeDelAggregator(icmp, 9);
        old.AddTombstones(FragmentedRangeTombstoneList.Build(Comparator.Bytewise, [new FragmentedRangeTombstoneList.Tombstone(B("b"), B("x"), 10)]));
        Assert.IsFalse(old.IsRangeOverlapped(B("c"), B("d"), false));
    }
}
