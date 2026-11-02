using System.Globalization;
using RocksDb.Managed.Db;
using RocksDb.Managed.Db.Iterators;
using static RocksDb.Managed.Tests.Iterators.IterTestHelpers;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.Tests.Iterators;

/// <summary>
/// Further ports of db/db_iter_test.cc (DBIterator1..14, SeekToLastOccurrenceSeq0, SeekToFirstLowerBound,
/// PrevLowerBound, SeekLessLowerBound) over the <see cref="TestInternalIterator"/> harness. Sequence numbers are
/// assigned from 0 in insertion order, as in the upstream TestIterator.
/// </summary>
[TestClass]
public sealed class DbIterUpstreamTests
{
    private static readonly MergeOperator s_append = new StringAppendOperator();

    private static void AssertAt(DbIter it, string key, string value)
    {
        Assert.IsTrue(it.Valid, "expected " + key);
        Assert.AreEqual(key, S(it.Key));
        Assert.AreEqual(value, S(it.Value));
    }

    /// <summary>a: put 0, merge 1; b: put 0, delete, merge 2 (DBIterator1..4 history).</summary>
    private static TestInternalIterator History1to4()
    {
        var ii = new TestInternalIterator();
        ii.AddPut("a", "0");
        ii.AddPut("b", "0");
        ii.AddDeletion("b");
        ii.AddMerge("a", "1");
        ii.AddMerge("b", "2");
        ii.Finish();
        return ii;
    }

    [TestMethod]
    public void DBIterator1to4()
    {
        using (DbIter it = NewDbIter(History1to4(), 1, s_append, null))
        {
            it.SeekToFirst();
            AssertAt(it, "a", "0");
            it.Next();
            Assert.IsTrue(it.Valid);
            Assert.AreEqual("b", S(it.Key));
            it.Next();
            Assert.IsFalse(it.Valid);
        }
        foreach (ulong seq in new ulong[] { 0, 2 })
        {
            using DbIter it = NewDbIter(History1to4(), seq, s_append, null);
            it.SeekToFirst();
            AssertAt(it, "a", "0");
            it.Next();
            Assert.IsFalse(it.Valid, "seq " + seq);
        }
        using (DbIter it = NewDbIter(History1to4(), 4, s_append, null))
        {
            it.SeekToFirst();
            AssertAt(it, "a", "0,1");
            it.Next();
            AssertAt(it, "b", "2");
            it.Next();
            Assert.IsFalse(it.Valid);
        }
    }

    private static TestInternalIterator MergeHistory(bool deleteInsteadOfPut)
    {
        var ii = new TestInternalIterator();
        ii.AddMerge("a", "merge_1");
        ii.AddMerge("a", "merge_2");
        ii.AddMerge("a", "merge_3");
        if (deleteInsteadOfPut) ii.AddDeletion("a");
        else ii.AddPut("a", "put_1");
        ii.AddMerge("a", "merge_4");
        ii.AddMerge("a", "merge_5");
        ii.AddMerge("a", "merge_6");
        ii.Finish();
        return ii;
    }

    [TestMethod]
    [DataRow(false, 0UL, "merge_1")]
    [DataRow(false, 1UL, "merge_1,merge_2")]
    [DataRow(false, 2UL, "merge_1,merge_2,merge_3")]
    [DataRow(false, 3UL, "put_1")]
    [DataRow(false, 4UL, "put_1,merge_4")]
    [DataRow(false, 5UL, "put_1,merge_4,merge_5")]
    [DataRow(false, 6UL, "put_1,merge_4,merge_5,merge_6")]
    [DataRow(true, 0UL, "merge_1")]
    [DataRow(true, 1UL, "merge_1,merge_2")]
    [DataRow(true, 2UL, "merge_1,merge_2,merge_3")]
    [DataRow(true, 3UL, null)]
    [DataRow(true, 4UL, "merge_4")]
    [DataRow(true, 5UL, "merge_4,merge_5")]
    [DataRow(true, 6UL, "merge_4,merge_5,merge_6")]
    public void DBIterator5And6(bool deleteInsteadOfPut, ulong sequence, string? expected)
    {
        // Backward (SeekToLast / Prev) and forward (SeekToFirst) must agree at every snapshot.
        using (DbIter it = NewDbIter(MergeHistory(deleteInsteadOfPut), sequence, s_append, null))
        {
            it.SeekToLast();
            if (expected is null)
            {
                Assert.IsFalse(it.Valid);
            }
            else
            {
                AssertAt(it, "a", expected);
                it.Prev();
                Assert.IsFalse(it.Valid);
            }
        }
        using (DbIter it = NewDbIter(MergeHistory(deleteInsteadOfPut), sequence, s_append, null))
        {
            it.SeekToFirst();
            if (expected is null)
            {
                Assert.IsFalse(it.Valid);
            }
            else
            {
                AssertAt(it, "a", expected);
                it.Next();
                Assert.IsFalse(it.Valid);
            }
        }
    }

    [TestMethod]
    public void DBIterator5PutSingleDeleteMergePrev()
    {
        var ii = new TestInternalIterator();
        ii.AddPut("a", "val_a");
        ii.AddSingleDeletion("a");
        ii.AddMerge("a", "merge_1");
        ii.AddMerge("a", "merge_2");
        ii.AddPut("b", "val_b");
        ii.Finish();
        using DbIter it = NewDbIter(ii, 10, s_append, null);
        it.Seek(B("b"));
        AssertAt(it, "b", "val_b");
        it.Prev();
        AssertAt(it, "a", "merge_1,merge_2");
    }

    [TestMethod]
    public void DBIterator10DirectionSwitches()
    {
        var ii = new TestInternalIterator();
        ii.AddPut("a", "1");
        ii.AddPut("b", "2");
        ii.AddPut("c", "3");
        ii.AddPut("d", "4");
        ii.Finish();
        using DbIter it = NewDbIter(ii, 10, null, null);
        it.Seek(B("c"));
        Assert.IsTrue(it.Valid);
        it.Prev();
        AssertAt(it, "b", "2");
        it.Next();
        AssertAt(it, "c", "3");
        it.SeekForPrev(B("c"));
        Assert.IsTrue(it.Valid);
        it.Next();
        AssertAt(it, "d", "4");
        it.Prev();
        AssertAt(it, "c", "3");
    }

    [TestMethod]
    public void SeekToLastOccurrenceSeq0()
    {
        var ii = new TestInternalIterator();
        ii.Add("a", EntryType.Value, "1", 0);
        ii.Add("b", EntryType.Value, "2", 0);
        ii.Finish();
        using DbIter it = NewDbIter(ii, 10, null, null);
        it.SeekToFirst();
        AssertAt(it, "a", "1");
        it.Next();
        AssertAt(it, "b", "2");
        it.Next();
        Assert.IsFalse(it.Valid);
    }

    [TestMethod]
    public void DBIterator11SingleDeletion()
    {
        var ii = new TestInternalIterator();
        ii.AddPut("a", "0");
        ii.AddPut("b", "0");
        ii.AddSingleDeletion("b");
        ii.AddMerge("a", "1");
        ii.AddMerge("b", "2");
        ii.Finish();
        using DbIter it = NewDbIter(ii, 1, s_append, null);
        it.SeekToFirst();
        AssertAt(it, "a", "0");
        it.Next();
        Assert.IsTrue(it.Valid);
        Assert.AreEqual("b", S(it.Key));
        it.Next();
        Assert.IsFalse(it.Valid);
    }

    [TestMethod]
    public void DBIterator12SingleDeletionReverse()
    {
        var ii = new TestInternalIterator();
        ii.AddPut("a", "1");
        ii.AddPut("b", "2");
        ii.AddPut("c", "3");
        ii.AddSingleDeletion("b");
        ii.Finish();
        using DbIter it = NewDbIter(ii, 10, null, null);
        it.SeekToLast();
        AssertAt(it, "c", "3");
        it.Prev();
        AssertAt(it, "a", "1");
        it.Prev();
        Assert.IsFalse(it.Valid);
    }

    [TestMethod]
    public void DBIterator13ReseekToSnapshot()
    {
        // A 9-byte key overwritten 9 times; with max_sequential_skip_in_iterations = 3 the iterator reseeks to
        // (key, snapshot) instead of stepping over every newer version.
        string key = "b" + new string('\0', 8);
        var ii = new TestInternalIterator();
        for (int i = 0; i <= 8; i++) ii.AddPut(key, i.ToString(CultureInfo.InvariantCulture));
        ii.Finish();
        using DbIter it = NewDbIter(ii, 2, null, null, maxSkip: 3);
        it.Seek(B("b"));
        AssertAt(it, key, "2");
    }

    [TestMethod]
    public void DBIterator14ReseekWithMaxSkipOne()
    {
        var ii = new TestInternalIterator();
        ii.AddPut("b", "0");
        ii.AddPut("b", "1");
        ii.AddPut("b", "2");
        ii.AddPut("b", "3");
        ii.AddPut("a", "4");
        ii.AddPut("a", "5");
        ii.AddPut("a", "6");
        ii.AddPut("c", "7");
        ii.AddPut("c", "8");
        ii.AddPut("c", "9");
        ii.Finish();
        using DbIter it = NewDbIter(ii, 4, null, null, maxSkip: 1);
        it.Seek(B("b"));
        AssertAt(it, "b", "3");
        it.SeekToFirst();
        AssertAt(it, "a", "4");
    }

    private static TestInternalIterator Numbered(int count)
    {
        var ii = new TestInternalIterator();
        for (int j = 1; j <= count; j++) ii.AddPut(j.ToString(CultureInfo.InvariantCulture), "val");
        ii.Finish();
        return ii;
    }

    [TestMethod]
    public void SeekToFirstLowerBound()
    {
        const int kNumKeys = 3;
        for (int i = 0; i < kNumKeys + 2; i++)
        {
            using DbIter it = NewDbIter(Numbered(kNumKeys), 10, null,
                new ReadOptions { IterateLowerBound = B(i.ToString(CultureInfo.InvariantCulture)) });
            it.SeekToFirst();
            if (i == kNumKeys + 1)
            {
                Assert.IsFalse(it.Valid); // lower bound was beyond the last key
            }
            else
            {
                Assert.IsTrue(it.Valid);
                Assert.AreEqual((i == 0 ? 1 : i).ToString(CultureInfo.InvariantCulture), S(it.Key));
            }
        }
    }

    [TestMethod]
    public void PrevLowerBound()
    {
        const int kNumKeys = 3, kLowerBound = 2;
        using DbIter it = NewDbIter(Numbered(kNumKeys), 10, null, new ReadOptions { IterateLowerBound = B("2") });
        it.SeekToLast();
        for (int i = kNumKeys; i >= kLowerBound; --i)
        {
            Assert.IsTrue(it.Valid);
            Assert.AreEqual(i.ToString(CultureInfo.InvariantCulture), S(it.Key));
            it.Prev();
        }
        Assert.IsFalse(it.Valid);
    }

    [TestMethod]
    public void SeekLessLowerBound()
    {
        using DbIter it = NewDbIter(Numbered(3), 10, null, new ReadOptions { IterateLowerBound = B("2") });
        it.Seek(B("1"));
        Assert.IsTrue(it.Valid);
        Assert.AreEqual("2", S(it.Key));
    }

    [TestMethod]
    public void SeekForPrevAboveUpperBoundClampsBelowIt()
    {
        // SetSavedKeyToSeekForPrevTarget: a SeekForPrev target >= iterate_upper_bound lands on the last key < bound.
        using DbIter it = NewDbIter(Numbered(5), 10, null, new ReadOptions { IterateUpperBound = B("4") });
        it.SeekForPrev(B("9"));
        Assert.IsTrue(it.Valid);
        Assert.AreEqual("3", S(it.Key));
        it.SeekForPrev(B("4"));
        Assert.AreEqual("3", S(it.Key));
        it.SeekForPrev(B("2"));
        Assert.AreEqual("2", S(it.Key));
    }
}
