using System.Globalization;
using RocksDb.Managed.Db;
using RocksDb.Managed.Db.Iterators;
using RocksDb.Managed.Env;
using RocksDb.Managed.Tests.Iterators;
using static RocksDb.Managed.Tests.Db.DbTestUtil;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4, DbIter slice (db/db_iter.cc, db/multi_cf_iterator_impl.h, db/multi_scan.cc + DBIter::ValidateScanOptions).
/// </summary>
[TestClass]
public sealed class Audit4DbIterTests
{
    private const string Path = "/audit4-dbiter";
    private static readonly MergeOperator s_append = new StringAppendOperator();

    // ------------------------------------------------------------------ failing inner iterator

    /// <summary>Forwards to a <see cref="TestInternalIterator"/>; the <see cref="FailAt"/>-th move after arming throws IOError.</summary>
    private sealed class FailingIterator(TestInternalIterator inner) : InternalIterator
    {
        public int Moves;
        public int FailAt; // 0 = never

        private void Move()
        {
            if (++Moves == FailAt) throw new RocksDbException(StatusCode.IOError, "IO error: injected");
        }

        public override bool Valid => inner.Valid;
        public override ReadOnlySpan<byte> Key => inner.Key;
        public override ReadOnlySpan<byte> Value => inner.Value;

        public override void SeekToFirst()
        {
            Move();
            inner.SeekToFirst();
        }

        public override void SeekToLast()
        {
            Move();
            inner.SeekToLast();
        }

        public override void Seek(ReadOnlySpan<byte> target)
        {
            Move();
            inner.Seek(target);
        }

        public override void SeekForPrev(ReadOnlySpan<byte> target)
        {
            Move();
            inner.SeekForPrev(target);
        }

        public override void Next()
        {
            Move();
            inner.Next();
        }

        public override void Prev()
        {
            Move();
            inner.Prev();
        }
    }

    /// <summary>a: put + 2 merges; b: 3 merges on a deletion; c: put; d: 4 puts; e: merge on put; f: put (seqs 0..).</summary>
    private static TestInternalIterator FailureHistory()
    {
        var ii = new TestInternalIterator();
        ii.AddPut("a", "a0");
        ii.AddMerge("a", "a1");
        ii.AddMerge("a", "a2");
        ii.AddPut("b", "b0");
        ii.AddDeletion("b");
        ii.AddMerge("b", "b1");
        ii.AddMerge("b", "b2");
        ii.AddMerge("b", "b3");
        ii.AddPut("c", "c0");
        ii.AddPut("d", "d0");
        ii.AddPut("d", "d1");
        ii.AddPut("d", "d2");
        ii.AddPut("d", "d3");
        ii.AddPut("e", "e0");
        ii.AddMerge("e", "e1");
        ii.AddPut("f", "f0");
        ii.Finish();
        return ii;
    }

    private static void Apply(DbIter it, string op, bool async)
    {
        string name = op, arg = "";
        int sp = op.IndexOf(' ');
        if (sp >= 0)
        {
            name = op[..sp];
            arg = op[(sp + 1)..];
        }
        byte[] target = B(arg);
        if (async)
        {
            ValueTask t = name switch
            {
                "Seek" => it.SeekAsync(target),
                "SeekForPrev" => it.SeekForPrevAsync(target),
                "SeekToFirst" => it.SeekToFirstAsync(),
                "SeekToLast" => it.SeekToLastAsync(),
                "Next" => it.NextAsync(),
                _ => it.PrevAsync(),
            };
            t.AsTask().GetAwaiter().GetResult();
            return;
        }
        switch (name)
        {
            case "Seek": it.Seek(target); break;
            case "SeekForPrev": it.SeekForPrev(target); break;
            case "SeekToFirst": it.SeekToFirst(); break;
            case "SeekToLast": it.SeekToLast(); break;
            case "Next": it.Next(); break;
            default: it.Prev(); break;
        }
    }

    /// <summary>
    /// db/db_iter.cc: every "if (!iter_.status().ok()) { valid_ = false; return false; }" (Next, MergeValuesNewToOld
    /// incl. the step after the base value, ReverseToForward, ReverseToBackward, FindValueForCurrentKey(+UsingSeek),
    /// FindUserKeyBeforeSavedKey). The port reports the inner iterator's error as an exception; the iterator must then be
    /// invalid, never left "valid" on the previous or a half-built position. Each scripted operation is failed at every
    /// one of its inner iterator moves, through the sync and the async API.
    /// </summary>
    [TestMethod]
    [DataRow(8UL, "Seek a", "Next")]                 // onto the merge chain of b (operands, then the deletion)
    [DataRow(8UL, "Seek b", "Next")]                 // leaving a merged entry
    [DataRow(8UL, "Seek c", "Next")]                 // plain value to plain value
    [DataRow(8UL, "", "SeekToFirst")]                // merge with a put base: the step after the base value
    [DataRow(8UL, "", "Seek e")]
    [DataRow(1UL, "Seek c", "Next")]                 // d: reseek (max_sequential_skip_in_iterations = 1)
    [DataRow(8UL, "", "SeekToLast")]
    [DataRow(8UL, "", "SeekForPrev e")]
    [DataRow(8UL, "SeekToLast", "Prev")]
    [DataRow(8UL, "SeekForPrev e", "Prev")]
    [DataRow(8UL, "SeekForPrev c", "Prev")]
    [DataRow(8UL, "SeekForPrev b", "Prev")]
    [DataRow(1UL, "SeekForPrev e", "Prev")]          // d: FindValueForCurrentKeyUsingSeek, FindUserKeyBeforeSavedKey reseek
    [DataRow(1UL, "SeekForPrev c", "Prev")]          // b: UsingSeek merge operands
    [DataRow(8UL, "Seek b", "Prev")]                 // ReverseToBackward from a merged entry
    [DataRow(8UL, "Seek d", "Prev")]
    [DataRow(8UL, "SeekForPrev b", "Next")]          // ReverseToForward
    [DataRow(8UL, "SeekForPrev d", "Next")]
    public void InnerIteratorFailureLeavesTheIteratorInvalid(ulong maxSkip, string prelude, string op)
    {
        foreach (bool async in new[] { false, true })
        {
            // Count the inner iterator moves of the operation.
            int moves;
            {
                var counting = new FailingIterator(FailureHistory());
                using DbIter it = IterTestHelpers.NewDbIter(counting, 100, s_append, null, maxSkip);
                if (prelude.Length != 0) Apply(it, prelude, async);
                int before = counting.Moves;
                Apply(it, op, async);
                moves = counting.Moves - before;
            }
            Assert.IsTrue(moves >= 1, op);
            for (int k = 1; k <= moves; k++)
            {
                var failing = new FailingIterator(FailureHistory());
                using DbIter it = IterTestHelpers.NewDbIter(failing, 100, s_append, null, maxSkip);
                if (prelude.Length != 0)
                {
                    Apply(it, prelude, async);
                    Assert.IsTrue(it.Valid);
                }
                failing.FailAt = failing.Moves + k;
                string ctx = "maxSkip " + maxSkip + " [" + prelude + "] " + op + " failing move " + k + "/" + moves + (async ? " async" : "");
                RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => Apply(it, op, async), ctx);
                Assert.AreEqual(StatusCode.IOError, e.Code, ctx);
                Assert.IsFalse(it.Valid, ctx + ": the iterator must be invalid after the inner iterator failed");
                // A later seek works again (status_ = Status::OK() in Seek).
                failing.FailAt = 0;
                it.Seek(B("c"));
                Assert.IsTrue(it.Valid, ctx);
                Assert.AreEqual("c", IterTestHelpers.S(it.Key), ctx);
                Assert.AreEqual("c0", IterTestHelpers.S(it.Value), ctx);
            }
        }
    }

    // ------------------------------------------------------------------ writes behind an open iterator

    /// <summary>
    /// The intent of db_iter_test.cc's DBIterWithMergeIterTest.InnerMergeIteratorDataRace1..8 at DB level: entries
    /// added to the memtable after the iterator was created (new keys, many new versions, deletions, merges; all with
    /// seqno above the iterator's sequence) show up in its inner memtable iterator at any time. Every positioning call
    /// in both directions must keep returning the view of the iterator's creation (FindNextUserEntry's skip of newer
    /// entries with the reseek to sequence_, FindValueForCurrentKey ending at the first invisible version,
    /// FindUserKeyBeforeSavedKey's reseek, the direction changes).
    /// </summary>
    [TestMethod]
    [DataRow(0UL, 7)]
    [DataRow(1UL, 8)]
    [DataRow(3UL, 9)]
    [DataRow(8UL, 10)]
    public void IteratorKeepsItsViewWhileTheMemtableGrows(ulong maxSkip, int seed)
    {
        var rnd = new Random(seed);
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(Path, NewOptions(fs),
            new ColumnFamilyOptions { MergeOperator = new StringAppendOperator(), MaxSequentialSkipInIterations = maxSkip, DisableAutoCompactions = true });
        var model = new SortedDictionary<string, string>(StringComparer.Ordinal);
        const int keySpace = 40;
        static string K(int i) => "k" + i.ToString("D2", CultureInfo.InvariantCulture);

        void Write(SortedDictionary<string, string>? m, int n)
        {
            for (int i = 0; i < n; i++)
            {
                string k = K(rnd.Next(keySpace));
                string v = "v" + rnd.Next(1000).ToString(CultureInfo.InvariantCulture);
                int r = rnd.Next(10);
                if (r < 5)
                {
                    db.Put(B(k), B(v));
                    if (m is not null) m[k] = v;
                }
                else if (r < 8)
                {
                    db.Merge(B(k), B(v));
                    if (m is not null) m[k] = m.TryGetValue(k, out string? old) ? old + "," + v : v;
                }
                else
                {
                    db.Delete(B(k));
                    m?.Remove(k);
                }
            }
        }

        Write(model, 120);
        db.Flush();
        Write(model, 120); // the rest stays in the memtable the later writes go to
        var keys = new List<string>(model.Keys);

        using Iterator it = db.NewIterator();
        int pos = -1; // index into keys, -1 = invalid
        for (int step = 0; step < 400; step++)
        {
            // Writes the iterator must not see, clustered so that keys get more new versions than max_skip.
            int hot = rnd.Next(keySpace);
            for (int i = rnd.Next(12); i > 0; i--)
            {
                string k = K(Math.Min(keySpace - 1, hot + rnd.Next(3)));
                switch (rnd.Next(3))
                {
                    case 0: db.Put(B(k), B("new")); break;
                    case 1: db.Merge(B(k), B("new")); break;
                    default: db.Delete(B(k)); break;
                }
            }

            int op = pos < 0 ? rnd.Next(4) : rnd.Next(8);
            string target = K(rnd.Next(keySpace)) + (rnd.Next(4) == 0 ? "!" : "");
            string what;
            switch (op)
            {
                case 0:
                    it.Seek(B(target));
                    pos = keys.FindIndex(k => string.CompareOrdinal(k, target) >= 0);
                    what = "Seek " + target;
                    break;
                case 1:
                    it.SeekForPrev(B(target));
                    pos = keys.FindLastIndex(k => string.CompareOrdinal(k, target) <= 0);
                    what = "SeekForPrev " + target;
                    break;
                case 2:
                    it.SeekToFirst();
                    pos = keys.Count == 0 ? -1 : 0;
                    what = "SeekToFirst";
                    break;
                case 3:
                    it.SeekToLast();
                    pos = keys.Count - 1;
                    what = "SeekToLast";
                    break;
                case 4:
                case 5:
                    it.Next();
                    pos = pos + 1 < keys.Count ? pos + 1 : -1;
                    what = "Next";
                    break;
                default:
                    it.Prev();
                    pos--;
                    what = "Prev";
                    break;
            }
            string ctx = "seed " + seed + " step " + step + " " + what;
            Assert.AreEqual(pos >= 0, it.Valid, ctx);
            if (pos >= 0)
            {
                Assert.AreEqual(keys[pos], IterTestHelpers.S(it.Key), ctx);
                Assert.AreEqual(model[keys[pos]], IterTestHelpers.S(it.Value), ctx);
            }
        }
    }

    // ------------------------------------------------------------------ MultiScan

    /// <summary>
    /// DBIter::ValidateScanOptions (db/db_iter.cc), run by upstream's MultiScan through Iterator::Prepare when the
    /// ranges do not mix bounded and unbounded ones (db/multi_scan.cc): "Overlapping ranges at index i" when a range
    /// starts before the previous limit (overlapping or out of order) and "Scan has no upper bound" for several
    /// unbounded ranges; MultiScan::begin() then throws the iterator's InvalidArgument status. The mixed-bounds slow
    /// path skips Prepare, so it is not validated (upstream as well).
    /// </summary>
    [TestMethod]
    public void MultiScanRejectsOverlappingAndUnorderedRanges()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(Path, NewOptions(fs));
        for (int i = 0; i < 30; i++) db.Put(Key(i), Value(i, 0));

        static StatusCode CodeOf(Action a) => Assert.ThrowsExactly<RocksDbException>(a).Code;
        static string MessageOf(Action a) => Assert.ThrowsExactly<RocksDbException>(a).Message;

        // Overlapping.
        Action overlapping = () => db.NewMultiScan(new MultiScanArgs().Add(Key(1), Key(10)).Add(Key(5), Key(12))).Dispose();
        Assert.AreEqual(StatusCode.InvalidArgument, CodeOf(overlapping));
        StringAssert.Contains(MessageOf(overlapping), "Overlapping ranges at index 1");
        // Out of order.
        Assert.AreEqual(StatusCode.InvalidArgument,
            CodeOf(() => db.NewMultiScan(new MultiScanArgs().Add(Key(10), Key(12)).Add(Key(1), Key(3))).Dispose()));
        // Reverse scans take their ranges in the same (ascending) order.
        Assert.AreEqual(StatusCode.InvalidArgument,
            CodeOf(() => db.NewMultiScan(new MultiScanArgs { Reverse = true }.Add(Key(10), Key(12)).Add(Key(1), Key(3))).Dispose()));
        // Several unbounded ranges.
        Action unbounded = () => db.NewMultiScan(new MultiScanArgs().Add(Key(1)).Add(Key(5))).Dispose();
        Assert.AreEqual(StatusCode.InvalidArgument, CodeOf(unbounded));
        StringAssert.Contains(MessageOf(unbounded), "Scan has no upper bound");

        // Still accepted: adjacent ranges (start == previous limit), one unbounded range, mixed bounds.
        Assert.AreEqual(4, Count(db.NewMultiScan(new MultiScanArgs().Add(Key(1), Key(3)).Add(Key(3), Key(5)))));
        Assert.AreEqual(4, Count(db.NewMultiScan(new MultiScanArgs { Reverse = true }.Add(Key(1), Key(3)).Add(Key(3), Key(5)))));
        Assert.AreEqual(2, Count(db.NewMultiScan(new MultiScanArgs().Add(Key(28)))));
        Assert.AreEqual(4, Count(db.NewMultiScan(new MultiScanArgs().Add(Key(28)).Add(Key(1), Key(3)))));

        static int Count(MultiScan scan)
        {
            using (scan)
            {
                int n = 0;
                while (scan.MoveNextRange())
                {
                    for (; scan.Valid; scan.Next()) n++;
                }
                return n;
            }
        }
    }

    // ------------------------------------------------------------------ multi-CF iterators

    /// <summary>
    /// MultiCfIteratorImpl::SeekCommon / AdvanceIterator (db/multi_cf_iterator_impl.h): a child iterator that ends with
    /// a non-OK status makes the multi-CF iterator invalid (considerStatus + heap.clear()). The child failure used
    /// here is DBIter::TooManyInternalKeysSkipped (Incomplete) in the second column family.
    /// </summary>
    [TestMethod]
    public void MultiCfIteratorIsInvalidAfterAChildFailed()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(Path, NewOptions(fs));
        ColumnFamily cf1 = db.CreateColumnFamily("one", new ColumnFamilyOptions());
        ColumnFamily cf2 = db.CreateColumnFamily("two", new ColumnFamilyOptions());
        cf1.Put(B("a"), B("1a"));
        cf1.Put(B("z"), B("1z"));
        cf2.Put(B("a"), B("2a"));
        // After "a", the second column family only has tombstones: more than max_skippable_internal_keys.
        for (int i = 0; i < 20; i++) cf2.Delete(B("b" + i.ToString("D2", CultureInfo.InvariantCulture)));
        cf2.Put(B("z"), B("2z"));
        var ro = new ReadOptions { MaxSkippableInternalKeys = 5 };

        using (CoalescingIterator it = db.NewCoalescingIterator(ro, [cf1, cf2]))
        {
            it.SeekToFirst();
            Assert.IsTrue(it.Valid);
            Assert.AreEqual("a", IterTestHelpers.S(it.Key));
            Assert.AreEqual("2a", IterTestHelpers.S(it.Value));
            Assert.AreEqual(StatusCode.Incomplete, Assert.ThrowsExactly<RocksDbException>(() => it.Next()).Code);
            Assert.IsFalse(it.Valid, "Next: a failed child must invalidate the iterator");

            it.SeekToLast();
            Assert.IsTrue(it.Valid);
            Assert.AreEqual("z", IterTestHelpers.S(it.Key));
            Assert.AreEqual(StatusCode.Incomplete, Assert.ThrowsExactly<RocksDbException>(() => it.Prev()).Code);
            Assert.IsFalse(it.Valid, "Prev: a failed child must invalidate the iterator");

            it.SeekToLast();
            Assert.IsTrue(it.Valid);
            Assert.AreEqual(StatusCode.Incomplete, Assert.ThrowsExactly<RocksDbException>(() => it.Seek(B("b"))).Code);
            Assert.IsFalse(it.Valid, "Seek: a failed child must invalidate the iterator");
        }
        using (AttributeGroupIterator it = db.NewAttributeGroupIterator(ro, [cf1, cf2]))
        {
            it.SeekToFirst();
            Assert.IsTrue(it.Valid);
            Assert.AreEqual(2, it.AttributeGroups.Count);
            Assert.AreEqual(StatusCode.Incomplete, Assert.ThrowsExactly<RocksDbException>(() => it.Next()).Code);
            Assert.IsFalse(it.Valid);
        }
    }

    // ------------------------------------------------------------------ upstream vectors without a managed test

    /// <summary>
    /// Port of DBIteratorTest.DBIteratorTimedPutBasic (db/db_iter_test.cc): kTypeValuePreferredSeqno entries read like
    /// values with the packed write time stripped (ParsePackedValueForValue) in FindNextUserEntry, MergeValuesNewToOld,
    /// FindValueForCurrentKey and FindValueForCurrentKeyUsingSeek (max_sequential_skip_in_iterations = 0 forces it).
    /// </summary>
    [TestMethod]
    [DataRow(8UL)]
    [DataRow(0UL)]
    public void TimedPutBasic(ulong maxSkip)
    {
        static string Packed(string v) => v + "\u0001\u0002\u0003\u0004\u0005\u0006\u0007\u0008"; // value + fixed64 write time
        TestInternalIterator History()
        {
            var ii = new TestInternalIterator();
            ii.Add("a", EntryType.ValuePreferredSeqno, Packed("0"));
            ii.Add("a", EntryType.ValuePreferredSeqno, Packed("1"));
            ii.AddPut("b", "0");
            ii.AddDeletion("b");
            ii.Add("c", EntryType.ValuePreferredSeqno, Packed("3"));
            ii.AddDeletion("c");
            ii.Add("c", EntryType.ValuePreferredSeqno, Packed("4"));
            ii.AddMerge("c", "5");
            ii.Add("d", EntryType.ValuePreferredSeqno, Packed("6"));
            ii.AddMerge("d", "7");
            ii.AddMerge("d", "8");
            ii.Finish();
            return ii;
        }
        var expected = new[] { ("a", "1"), ("c", "4,5"), ("d", "6,7,8") };

        using (DbIter it = IterTestHelpers.NewDbIter(History(), 100, s_append, null, maxSkip))
        {
            it.SeekToFirst();
            foreach ((string k, string v) in expected)
            {
                Assert.IsTrue(it.Valid, k);
                Assert.AreEqual(k, IterTestHelpers.S(it.Key));
                Assert.AreEqual(v, IterTestHelpers.S(it.Value));
                it.Next();
            }
            Assert.IsFalse(it.Valid);
        }
        using (DbIter it = IterTestHelpers.NewDbIter(History(), 100, s_append, null, maxSkip))
        {
            it.SeekToLast();
            for (int i = expected.Length - 1; i >= 0; i--)
            {
                Assert.IsTrue(it.Valid, expected[i].Item1);
                Assert.AreEqual(expected[i].Item1, IterTestHelpers.S(it.Key));
                Assert.AreEqual(expected[i].Item2, IterTestHelpers.S(it.Value));
                it.Prev();
            }
            Assert.IsFalse(it.Valid);
        }
        // An older snapshot (seqno 4): a's second timed put, b deleted, and c's first timed put (before its deletion).
        using (DbIter it = IterTestHelpers.NewDbIter(History(), 4, s_append, null, maxSkip))
        {
            it.SeekForPrev(B("c"));
            Assert.IsTrue(it.Valid);
            Assert.AreEqual("c", IterTestHelpers.S(it.Key));
            Assert.AreEqual("3", IterTestHelpers.S(it.Value));
            it.Prev();
            Assert.IsTrue(it.Valid);
            Assert.AreEqual("a", IterTestHelpers.S(it.Key));
            Assert.AreEqual("1", IterTestHelpers.S(it.Value));
            it.Next();
            Assert.IsTrue(it.Valid);
            Assert.AreEqual("c", IterTestHelpers.S(it.Key));
            Assert.AreEqual("3", IterTestHelpers.S(it.Value));
        }
    }
}
