using RocksDb.Managed.Db;
using RocksDb.Managed.Db.Iterators;
using static RocksDb.Managed.Tests.Iterators.IterTestHelpers;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.Tests.Iterators;

/// <summary>Ports of db/db_iter_test.cc cases over the <see cref="TestInternalIterator"/> harness.</summary>
[TestClass]
public sealed class DbIterTests
{
    private static readonly MergeOperator s_append = new StringAppendOperator();

    private static void AssertAt(DbIter it, string key, string value)
    {
        Assert.IsTrue(it.Valid, "expected " + key);
        Assert.AreEqual(key, S(it.Key));
        Assert.AreEqual(value, S(it.Value));
    }

    [TestMethod]
    public void PrevNext()
    {
        var ii = new TestInternalIterator();
        ii.AddDeletion("a");
        ii.AddDeletion("a");
        ii.AddDeletion("a");
        ii.AddDeletion("a");
        ii.AddPut("a", "val_a");
        ii.AddPut("b", "val_b");
        ii.Finish();
        using DbIter it = NewDbIter(ii, 10, null, null);
        it.SeekToLast();
        AssertAt(it, "b", "val_b");
        it.Prev();
        AssertAt(it, "a", "val_a");
        it.Next();
        AssertAt(it, "b", "val_b");
        it.Next();
        Assert.IsFalse(it.Valid);
    }

    [TestMethod]
    public void SeekToLastWithUpperBound()
    {
        var ii = new TestInternalIterator();
        foreach (string k in new[] { "a", "b", "c", "d", "e", "f" }) ii.AddPut(k, "val_" + k);
        ii.Finish();
        using DbIter it = NewDbIter(ii, 10, null, new ReadOptions { IterateUpperBound = B("d") });
        it.SeekToLast();
        AssertAt(it, "c", "val_c");
        it.Next();
        Assert.IsFalse(it.Valid);
        it.SeekToLast();
        AssertAt(it, "c", "val_c");

        var ii2 = new TestInternalIterator();
        ii2.AddPut("a", "val_a");
        ii2.AddPut("a", "val_a");
        ii2.AddPut("b", "val_b");
        ii2.AddPut("c", "val_c");
        ii2.AddPut("d", "val_d");
        ii2.Finish();
        using DbIter it2 = NewDbIter(ii2, 10, null, new ReadOptions { IterateUpperBound = B("z") });
        it2.SeekToLast();
        AssertAt(it2, "d", "val_d");
        it2.Next();
        Assert.IsFalse(it2.Valid);
    }

    [TestMethod]
    public void UpperBoundStopsForwardScan()
    {
        var ii = new TestInternalIterator();
        foreach (string k in new[] { "a", "b", "c", "d" }) ii.AddPut(k, k);
        ii.Finish();
        using DbIter it = NewDbIter(ii, 10, null, new ReadOptions { IterateUpperBound = B("c") });
        Assert.AreEqual("a=a b=b", Format(ScanForward(it)));
        it.Seek(B("c"));
        Assert.IsFalse(it.Valid);
        it.SeekForPrev(B("z"));
        AssertAt(it, "b", "b");
    }

    [TestMethod]
    public void Empty()
    {
        var ii = new TestInternalIterator();
        ii.Finish();
        using DbIter it = NewDbIter(ii, 0, null, null);
        it.SeekToLast();
        Assert.IsFalse(it.Valid);
        it.SeekToFirst();
        Assert.IsFalse(it.Valid);
        it.Seek(B("x"));
        Assert.IsFalse(it.Valid);
    }

    /// <summary>DBIterator1..4: visibility of puts/deletes/merges at increasing snapshots.</summary>
    [TestMethod]
    [DataRow(1UL, "a=0 b=0")]
    [DataRow(0UL, "a=0")]
    [DataRow(2UL, "a=0")]
    [DataRow(4UL, "a=0,1 b=2")]
    public void Snapshots(ulong sequence, string expected)
    {
        var ii = new TestInternalIterator();
        ii.AddPut("a", "0");
        ii.AddPut("b", "0");
        ii.AddDeletion("b");
        ii.AddMerge("a", "1");
        ii.AddMerge("b", "2");
        ii.Finish();
        using DbIter it = NewDbIter(ii, sequence, s_append, null);
        Assert.AreEqual(expected, Format(ScanForward(it)));
        Assert.AreEqual(expected, Format(ScanReverse(it)));
    }

    /// <summary>DBIterator5/6: merges on top of a put at every snapshot, both directions.</summary>
    [TestMethod]
    public void MergeChainAtEverySnapshot()
    {
        string[] expected =
        [
            "merge_1", "merge_1,merge_2", "merge_1,merge_2,merge_3", "put_1", "put_1,merge_4", "put_1,merge_4,merge_5",
            "put_1,merge_4,merge_5,merge_6",
        ];
        for (ulong seq = 0; seq < 7; seq++)
        {
            foreach (ulong maxSkip in new ulong[] { 8, 1, 0 })
            {
                var ii = new TestInternalIterator();
                ii.AddMerge("a", "merge_1");
                ii.AddMerge("a", "merge_2");
                ii.AddMerge("a", "merge_3");
                ii.AddPut("a", "put_1");
                ii.AddMerge("a", "merge_4");
                ii.AddMerge("a", "merge_5");
                ii.AddMerge("a", "merge_6");
                ii.Finish();
                using DbIter it = NewDbIter(ii, seq, s_append, null, maxSkip);
                it.SeekToLast();
                AssertAt(it, "a", expected[seq]);
                it.Prev();
                Assert.IsFalse(it.Valid);
                it.SeekToFirst();
                AssertAt(it, "a", expected[seq]);
                it.Next();
                Assert.IsFalse(it.Valid);
            }
        }
    }

    /// <summary>DBIterator6: merges on top of a deletion.</summary>
    [TestMethod]
    public void MergeOverDeletion()
    {
        string?[] expected = ["merge_1", "merge_1,merge_2", "merge_1,merge_2,merge_3", null, "merge_4", "merge_4,merge_5", "merge_4,merge_5,merge_6"];
        for (ulong seq = 0; seq < 7; seq++)
        {
            var ii = new TestInternalIterator();
            ii.AddMerge("a", "merge_1");
            ii.AddMerge("a", "merge_2");
            ii.AddMerge("a", "merge_3");
            ii.AddDeletion("a");
            ii.AddMerge("a", "merge_4");
            ii.AddMerge("a", "merge_5");
            ii.AddMerge("a", "merge_6");
            ii.Finish();
            using DbIter it = NewDbIter(ii, seq, s_append, null);
            it.SeekToLast();
            if (expected[seq] is null) Assert.IsFalse(it.Valid);
            else AssertAt(it, "a", expected[seq]!);
            it.SeekToFirst();
            if (expected[seq] is null) Assert.IsFalse(it.Valid);
            else AssertAt(it, "a", expected[seq]!);
        }
    }

    /// <summary>DBIteratorUseSkip: many versions of a key, reverse iteration with reseeks.</summary>
    [TestMethod]
    public void UseSkip()
    {
        for (ulong i = 0; i < 200; i += 7)
        {
            var ii = new TestInternalIterator();
            ii.AddMerge("b", "merge_1");
            ii.AddMerge("a", "merge_2");
            for (int k = 0; k < 200; k++) ii.AddPut("c", k.ToString(System.Globalization.CultureInfo.InvariantCulture));
            ii.Finish();
            using DbIter it = NewDbIter(ii, i + 2, s_append, null);
            it.SeekToLast();
            AssertAt(it, "c", i.ToString(System.Globalization.CultureInfo.InvariantCulture));
            it.Prev();
            AssertAt(it, "b", "merge_1");
            it.Prev();
            AssertAt(it, "a", "merge_2");
            it.Prev();
            Assert.IsFalse(it.Valid);
            Assert.AreEqual("a=merge_2 b=merge_1 c=" + i, Format(ScanForward(it)));
        }

        for (ulong i = 0; i < 200; i += 7)
        {
            var ii = new TestInternalIterator();
            ii.AddMerge("b", "merge_1");
            ii.AddMerge("a", "merge_2");
            for (int k = 0; k < 200; k++) ii.AddDeletion("c");
            ii.AddPut("c", "200");
            ii.Finish();
            using DbIter it = NewDbIter(ii, i + 2, s_append, null);
            it.SeekToLast();
            AssertAt(it, "b", "merge_1");
            it.Prev();
            AssertAt(it, "a", "merge_2");
            Assert.AreEqual("a=merge_2 b=merge_1", Format(ScanForward(it)));
        }
        {
            var ii = new TestInternalIterator();
            ii.AddMerge("b", "merge_1");
            ii.AddMerge("a", "merge_2");
            for (int k = 0; k < 200; k++) ii.AddDeletion("c");
            ii.AddPut("c", "200");
            ii.Finish();
            using DbIter it = NewDbIter(ii, 202, s_append, null);
            it.SeekToLast();
            AssertAt(it, "c", "200");
            it.Prev();
            AssertAt(it, "b", "merge_1");
        }
    }

    /// <summary>DBIteratorSkipInternalKeys: max_skippable_internal_keys yields Incomplete.</summary>
    [TestMethod]
    public void SkipInternalKeysIncomplete()
    {
        var ii = new TestInternalIterator();
        ii.AddPut("a", "val_a");
        ii.AddDeletion("b");
        ii.AddDeletion("b");
        ii.AddDeletion("b");
        ii.AddDeletion("c");
        ii.AddDeletion("c");
        ii.AddPut("d", "val_d");
        ii.Finish();
        using DbIter it = NewDbIter(ii, 10, null, new ReadOptions { MaxSkippableInternalKeys = 2 });
        it.SeekToFirst();
        AssertAt(it, "a", "val_a");
        var ex = Assert.ThrowsExactly<RocksDbException>(it.Next);
        Assert.AreEqual(StatusCode.Incomplete, ex.Code);
        Assert.IsFalse(it.Valid);

        // Within the limit everything works.
        using DbIter it2 = NewDbIter(ii, 10, null, new ReadOptions { MaxSkippableInternalKeys = 10 });
        Assert.AreEqual("a=val_a d=val_d", Format(ScanForward(it2)));
    }

    /// <summary>DBIterator10: direction changes around a seek target.</summary>
    [TestMethod]
    public void DirectionChanges()
    {
        var ii = new TestInternalIterator();
        ii.AddPut("a", "1");
        ii.AddPut("b", "2");
        ii.AddPut("c", "3");
        ii.AddPut("d", "4");
        ii.Finish();
        using DbIter it = NewDbIter(ii, 10, null, null);
        it.Seek(B("c"));
        AssertAt(it, "c", "3");
        it.Prev();
        AssertAt(it, "b", "2");
        it.Next();
        AssertAt(it, "c", "3");
        it.SeekForPrev(B("c"));
        AssertAt(it, "c", "3");
        it.Next();
        AssertAt(it, "d", "4");
        it.Prev();
        AssertAt(it, "c", "3");
    }

    /// <summary>
    /// NextAsync after reverse movement: ReverseToForward's re-seek / skip awaits the inner iterator (no synchronous
    /// moves), with the same results as the sync direction change.
    /// </summary>
    [TestMethod]
    public async Task DirectionChangesAsync()
    {
        var ii = new TestInternalIterator();
        foreach (string k in new[] { "a", "b", "c", "d" })
        {
            ii.AddPut(k, k + "1");
            ii.AddMerge(k, k + "2");
        }
        ii.Finish();
        using DbIter it = NewDbIter(ii, 100, s_append, null);
        await it.SeekAsync(B("c"));
        AssertAt(it, "c", "c1,c2");
        ii.YieldAsync = true;
        await it.PrevAsync();
        AssertAt(it, "b", "b1,b2");
        int stepsBefore = ii.Steps, asyncBefore = ii.AsyncMoves;
        await it.NextAsync();
        AssertAt(it, "c", "c1,c2");
        Assert.IsTrue(ii.AsyncMoves > asyncBefore);
        Assert.AreEqual(ii.Steps - stepsBefore, ii.AsyncMoves - asyncBefore, "every inner move of the direction change was async");
        await it.NextAsync();
        AssertAt(it, "d", "d1,d2");
        await it.PrevAsync();
        AssertAt(it, "c", "c1,c2");
        await it.PrevAsync();
        await it.PrevAsync();
        AssertAt(it, "a", "a1,a2");
        await it.NextAsync();
        AssertAt(it, "b", "b1,b2");
    }

    /// <summary>DBIterator11/12: single deletions hide the key.</summary>
    [TestMethod]
    public void SingleDeletion()
    {
        var ii = new TestInternalIterator();
        ii.AddPut("a", "1");
        ii.AddPut("b", "2");
        ii.AddPut("c", "3");
        ii.AddSingleDeletion("b");
        ii.Finish();
        using DbIter it = NewDbIter(ii, 10, null, null);
        Assert.AreEqual("a=1 c=3", Format(ScanForward(it)));
        Assert.AreEqual("a=1 c=3", Format(ScanReverse(it)));
    }

    /// <summary>DBIterator13/14: reseek after max_sequential_skip_in_iterations versions.</summary>
    [TestMethod]
    public void ReseekAfterSkips()
    {
        string key = "b" + new string('\0', 8);
        var ii = new TestInternalIterator();
        for (int i = 0; i <= 8; i++) ii.AddPut(key, i.ToString(System.Globalization.CultureInfo.InvariantCulture));
        ii.Finish();
        using DbIter it = NewDbIter(ii, 2, null, null, maxSkip: 3);
        it.Seek(B("b"));
        AssertAt(it, key, "2");

        var ii2 = new TestInternalIterator();
        ii2.AddPut("b", "0");
        ii2.AddPut("b", "1");
        ii2.AddPut("b", "2");
        ii2.AddPut("b", "3");
        ii2.AddPut("a", "4");
        ii2.AddPut("a", "5");
        ii2.AddPut("a", "6");
        ii2.AddPut("c", "7");
        ii2.AddPut("c", "8");
        ii2.AddPut("c", "9");
        ii2.Finish();
        using DbIter it2 = NewDbIter(ii2, 4, null, null, maxSkip: 1);
        it2.Seek(B("b"));
        AssertAt(it2, "b", "3");
        it2.SeekToFirst();
        AssertAt(it2, "a", "4");
    }

    /// <summary>DBIterWithMergeIterTest.InnerMergeIterator1/2: DBIter over a MergingIterator.</summary>
    [TestMethod]
    public void OverMergingIterator()
    {
        var i1 = new TestInternalIterator();
        i1.Add("a", EntryType.Value, "1", 3);
        i1.Add("f", EntryType.Value, "2", 5);
        i1.Add("g", EntryType.Value, "3", 7);
        i1.Finish();
        var i2 = new TestInternalIterator();
        i2.Add("a", EntryType.Value, "4", 6);
        i2.Add("b", EntryType.Value, "5", 1);
        i2.Add("c", EntryType.Value, "6", 2);
        i2.Add("d", EntryType.Value, "7", 3);
        i2.Finish();
        var merging = new MergingIterator(InternalKeyComparator.Bytewise, [i1, i2]);
        using DbIter it = NewDbIter(merging, DbFormat.MaxSequenceNumber, null, null, maxSkip: 3);
        Assert.AreEqual("a=4 b=5 c=6 d=7 f=2 g=3", Format(ScanForward(it)));
        it.SeekForPrev(B("g"));
        var back = new List<string>();
        for (; it.Valid; it.Prev()) back.Add(S(it.Key) + "=" + S(it.Value));
        Assert.AreEqual("g=3 f=2 d=7 c=6 b=5 a=4", string.Join(" ", back));
    }

    /// <summary>SeekToFirstLowerBound / PrevLowerBound / SeekLessLowerBound.</summary>
    [TestMethod]
    public void LowerBound()
    {
        for (int i = 0; i < 5; i++)
        {
            var ii = new TestInternalIterator();
            for (int j = 1; j <= 3; j++) ii.AddPut(j.ToString(System.Globalization.CultureInfo.InvariantCulture), "val");
            ii.Finish();
            using DbIter it = NewDbIter(ii, 10, null, new ReadOptions { IterateLowerBound = B(i.ToString(System.Globalization.CultureInfo.InvariantCulture)) });
            it.SeekToFirst();
            if (i == 4) Assert.IsFalse(it.Valid);
            else Assert.AreEqual(i == 0 ? "1" : i.ToString(System.Globalization.CultureInfo.InvariantCulture), S(it.Key));
        }
        {
            var ii = new TestInternalIterator();
            for (int j = 1; j <= 3; j++) ii.AddPut(j.ToString(System.Globalization.CultureInfo.InvariantCulture), "val");
            ii.Finish();
            using DbIter it = NewDbIter(ii, 10, null, new ReadOptions { IterateLowerBound = B("2") });
            it.SeekToLast();
            for (int i = 3; i >= 2; i--)
            {
                Assert.AreEqual(i.ToString(System.Globalization.CultureInfo.InvariantCulture), S(it.Key));
                it.Prev();
            }
            Assert.IsFalse(it.Valid);
            it.Seek(B("1"));
            Assert.AreEqual("2", S(it.Key));
        }
    }

    [TestMethod]
    public void MergeWithoutOperatorThrowsInvalidArgument()
    {
        var ii = new TestInternalIterator();
        ii.AddMerge("a", "x");
        ii.Finish();
        using DbIter it = NewDbIter(ii, 10, null, null);
        var ex = Assert.ThrowsExactly<RocksDbException>(it.SeekToFirst);
        Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
        Assert.IsFalse(it.Valid);
    }

    [TestMethod]
    public void RangeTombstonesHideCoveredKeys()
    {
        var ii = new TestInternalIterator();
        ii.Add("a", EntryType.Value, "a1", 1);
        ii.Add("b", EntryType.Value, "b1", 2);
        ii.Add("c", EntryType.Merge, "c1", 3);
        ii.Add("c", EntryType.Merge, "c2", 10);
        ii.Add("d", EntryType.Value, "d1", 4);
        ii.Add("e", EntryType.Value, "e1", 20);
        ii.Finish();
        // [b, e) @ 5 deletes b, d and the first operand of c.
        var rd = new RangeDelAggregator(InternalKeyComparator.Bytewise, 100);
        rd.AddTombstones(FragmentedRangeTombstoneList.Build(Comparator.Bytewise, [new(B("b"), B("e"), 5)]));
        using DbIter it = NewDbIter(ii, 100, s_append, null, 8, rd);
        Assert.AreEqual("a=a1 c=c2 e=e1", Format(ScanForward(it)));
        Assert.AreEqual("a=a1 c=c2 e=e1", Format(ScanReverse(it)));

        // At a snapshot before the tombstone everything is visible.
        var rd2 = new RangeDelAggregator(InternalKeyComparator.Bytewise, 4);
        rd2.AddTombstones(FragmentedRangeTombstoneList.Build(Comparator.Bytewise, [new(B("b"), B("e"), 5)]));
        ii.SeekToFirst();
        using DbIter it2 = NewDbIter(ii, 4, s_append, null, 8, rd2);
        Assert.AreEqual("a=a1 b=b1 c=c1 d=d1", Format(ScanForward(it2)));

        // IgnoreRangeDeletions
        using DbIter it3 = NewDbIter(ii, 100, s_append, new ReadOptions { IgnoreRangeDeletions = true }, 8, rd);
        Assert.AreEqual("a=a1 b=b1 c=c1,c2 d=d1 e=e1", Format(ScanForward(it3)));
    }

    [TestMethod]
    public async Task AsyncMatchesSync()
    {
        var ii = new TestInternalIterator();
        for (int i = 0; i < 50; i++)
        {
            string k = "k" + (i % 17).ToString("D2", System.Globalization.CultureInfo.InvariantCulture);
            ii.Add(k, (i % 5) switch { 0 => EntryType.Deletion, 1 or 2 => EntryType.Merge, _ => EntryType.Value }, "v" + i);
        }
        ii.Finish();
        using DbIter sync = NewDbIter(ii, 40, s_append, null, 2);
        List<(string, string)> expected = ScanForward(sync);
        var ii2 = new TestInternalIterator();
        for (int i = 0; i < 50; i++)
        {
            string k = "k" + (i % 17).ToString("D2", System.Globalization.CultureInfo.InvariantCulture);
            ii2.Add(k, (i % 5) switch { 0 => EntryType.Deletion, 1 or 2 => EntryType.Merge, _ => EntryType.Value }, "v" + i);
        }
        ii2.Finish();
        using DbIter it = NewDbIter(ii2, 40, s_append, null, 2);
        var got = new List<(string, string)>();
        await it.SeekToFirstAsync();
        while (it.Valid)
        {
            got.Add((S(it.Key), S(it.Value)));
            await it.NextAsync();
        }
        Assert.AreEqual(Format(expected), Format(got));
        await it.SeekAsync(B("k05"));
        Assert.IsTrue(it.Valid);
        Assert.IsTrue(string.CompareOrdinal(S(it.Key), "k05") >= 0);
    }
}
