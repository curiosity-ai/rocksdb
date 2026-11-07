using System.Buffers;
using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Tests.Iterators;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.Tests.Db;

/// <summary>Ports of cases from db/compaction/compaction_iterator_test.cc on a vector-backed input.</summary>
[TestClass]
public sealed class CompactionIteratorTests
{
    private static byte[] IKey(string user, ulong seq, EntryType t)
    {
        byte[] u = Encoding.UTF8.GetBytes(user);
        var k = new byte[u.Length + 8];
        DbFormat.AppendInternalKey(k, u, seq, t);
        return k;
    }

    /// <summary>A compaction of one L1 file into L1; bottommost unless <paramref name="bottommost"/> is false (an L2 file overlaps).</summary>
    private static Compaction MakeCompaction(bool bottommost)
    {
        var options = new ColumnFamilyOptions { NumLevels = 3, LevelCompactionDynamicLevelBytes = false, Ttl = 0 };
        var input = new FileMetaData { FileNumber = 1, FileSize = 1, Smallest = IKey("a", 100, EntryType.Value), Largest = IKey("z", 1, EntryType.Value) };
        var files = new FileMetaData[3][];
        files[0] = [];
        files[1] = [input];
        files[2] = bottommost ? [] : [new FileMetaData { FileNumber = 2, FileSize = 1, Smallest = IKey("a", 1, EntryType.Value), Largest = IKey("z", 1, EntryType.Value) }];
        var vs = new VersionStorageInfo(InternalKeyComparator.Bytewise, 3, options, files);
        return new Compaction(vs, options, [new CompactionInputFiles(1, [input])], 1, 1 << 20, 1 << 30, 0, RocksDb.Managed.Compression.CompressionType.None,
            new List<FileMetaData>(), CompactionReason.ManualCompaction, 0, false, true);
    }

    private static List<string> Run(TestInternalIterator input, ulong[] snapshots, Compaction? compaction, MergeOperator? merge = null,
        RangeDelAggregator? rangeDel = null, CompactionFilter? filter = null) =>
        Run(input, snapshots, compaction, merge, rangeDel, filter, null, 0);

    private static List<string> Run(TestInternalIterator input, ulong[] snapshots, Compaction? compaction, MergeOperator? merge,
        RangeDelAggregator? rangeDel, CompactionFilter? filter, SnapshotChecker? checker, ulong jobSnapshot)
    {
        input.Finish();
        InternalKeyComparator icmp = InternalKeyComparator.Bytewise;
        rangeDel ??= RangeDelAggregator.ForCompaction(icmp, snapshots);
        using MergeHelper? mh = merge is null ? null : new MergeHelper(icmp, merge, filter, snapshots.Length == 0 ? 0 : snapshots[^1], 1);
        using var it = new CompactionIterator(input, icmp, mh, snapshots, DbFormat.MaxSequenceNumber, rangeDel, compaction, filter, true, default)
        {
            SnapshotChecker = checker,
            JobSnapshot = jobSnapshot,
        };
        var result = new List<string>();
        input.SeekToFirst();
        for (it.SeekToFirst(); it.Valid; it.Next())
        {
            DbFormat.UnpackSequenceAndType(DbFormat.ExtractFooter(it.Key), out ulong seq, out EntryType t);
            result.Add(Encoding.UTF8.GetString(it.UserKey) + "@" + seq + ":" + TypeName(t) + "=" + Encoding.UTF8.GetString(it.Value));
        }
        return result;
    }

    private static string TypeName(EntryType t) => t switch
    {
        EntryType.Value => "put",
        EntryType.Deletion => "del",
        EntryType.SingleDeletion => "sd",
        EntryType.Merge => "merge",
        _ => t.ToString(),
    };

    private static void AssertOutput(List<string> actual, params string[] expected) =>
        CollectionAssert.AreEqual(expected, actual, string.Join(" | ", actual));

    [TestMethod]
    public void NewerVersionHidesOlderWithoutSnapshots()
    {
        var input = new TestInternalIterator();
        input.Add("a", EntryType.Value, "v1", 1);
        input.Add("a", EntryType.Value, "v2", 2);
        input.Add("b", EntryType.Value, "w", 3);
        AssertOutput(Run(input, [], MakeCompaction(bottommost: false)), "a@2:put=v2", "b@3:put=w");
    }

    [TestMethod]
    public void SnapshotsKeepOneVersionPerStripe()
    {
        var input = new TestInternalIterator();
        for (ulong s = 1; s <= 6; s++) input.Add("a", EntryType.Value, "v" + s, s);
        // Snapshots at 2 and 4: visible versions are 2 (for snap 2), 4 (for snap 4) and 6 (latest).
        AssertOutput(Run(input, [2, 4], MakeCompaction(bottommost: false)), "a@6:put=v6", "a@4:put=v4", "a@2:put=v2");
    }

    [TestMethod]
    public void DeletionDroppedAtBottommostOnly()
    {
        var input = new TestInternalIterator();
        input.Add("a", EntryType.Value, "v", 1);
        input.Add("a", EntryType.Deletion, "", 2);
        input.Add("b", EntryType.Value, "w", 3);
        AssertOutput(Run(input, [], MakeCompaction(bottommost: true)), "b@0:put=w");

        input = new TestInternalIterator();
        input.Add("a", EntryType.Value, "v", 1);
        input.Add("a", EntryType.Deletion, "", 2);
        // An L2 file overlaps "a": the tombstone must stay.
        AssertOutput(Run(input, [], MakeCompaction(bottommost: false)), "a@2:del=");
    }

    [TestMethod]
    public void BottommostDeletionKeptForSnapshot()
    {
        var input = new TestInternalIterator();
        input.Add("a", EntryType.Value, "v", 1);
        input.Add("a", EntryType.Deletion, "", 3);
        // Snapshot 2 still sees v: the deletion and v are both kept; v's seqno is zeroed (visible to the earliest snapshot).
        AssertOutput(Run(input, [2], MakeCompaction(bottommost: true)), "a@3:del=", "a@0:put=v");
    }

    [TestMethod]
    public void SequenceZeroedOnlyBelowEarliestSnapshot()
    {
        var input = new TestInternalIterator();
        input.Add("a", EntryType.Value, "old", 5);
        input.Add("b", EntryType.Value, "new", 10);
        AssertOutput(Run(input, [7], MakeCompaction(bottommost: true)), "a@0:put=old", "b@10:put=new");
    }

    [TestMethod]
    public void SingleDeleteDropsMatchingPut()
    {
        var input = new TestInternalIterator();
        input.Add("a", EntryType.Value, "v", 1);
        input.Add("a", EntryType.SingleDeletion, "", 2);
        input.Add("b", EntryType.Value, "w", 3);
        AssertOutput(Run(input, [], MakeCompaction(bottommost: false)), "b@3:put=w");
    }

    [TestMethod]
    public void SingleDeleteAcrossSnapshotIsKept()
    {
        var input = new TestInternalIterator();
        input.Add("a", EntryType.Value, "v", 1);
        input.Add("a", EntryType.SingleDeletion, "", 3);
        // Snapshot 2 separates the SD from the put: both survive.
        AssertOutput(Run(input, [2], MakeCompaction(bottommost: false)), "a@3:sd=", "a@1:put=v");
    }

    [TestMethod]
    public void SingleDeleteWithoutPutAtBottommostIsDropped()
    {
        var input = new TestInternalIterator();
        input.Add("a", EntryType.SingleDeletion, "", 3);
        input.Add("b", EntryType.Value, "w", 4);
        AssertOutput(Run(input, [], MakeCompaction(bottommost: true)), "b@0:put=w");
        input = new TestInternalIterator();
        input.Add("a", EntryType.SingleDeletion, "", 3);
        AssertOutput(Run(input, [], MakeCompaction(bottommost: false)), "a@3:sd=");
    }

    [TestMethod]
    public void SingleDeleteFollowedByDeleteIsCorruptionWhenEnforced()
    {
        var input = new TestInternalIterator();
        input.Add("a", EntryType.Deletion, "", 1);
        input.Add("a", EntryType.SingleDeletion, "", 2);
        var ex = Assert.ThrowsExactly<RocksDbException>(() => Run(input, [], MakeCompaction(bottommost: false)));
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
    }

    [TestMethod]
    public void MergeFullyMergedAtBottommostAndPartialOtherwise()
    {
        var input = new TestInternalIterator();
        input.Add("a", EntryType.Value, "base", 1);
        input.Add("a", EntryType.Merge, "m1", 2);
        input.Add("a", EntryType.Merge, "m2", 3);
        AssertOutput(Run(input, [], MakeCompaction(bottommost: false), new StringAppendOperator()), "a@3:put=base,m1,m2");

        input = new TestInternalIterator();
        input.Add("a", EntryType.Merge, "m1", 2);
        input.Add("a", EntryType.Merge, "m2", 3);
        // Bottommost: no older base can exist, full merge into a put (seqno zeroed).
        AssertOutput(Run(input, [], MakeCompaction(bottommost: true), new StringAppendOperator()), "a@0:put=m1,m2");

        input = new TestInternalIterator();
        input.Add("a", EntryType.Merge, "m1", 2);
        input.Add("a", EntryType.Merge, "m2", 3);
        // Not bottommost and no base: operands stay operands (partial merge not supported by the operator here).
        List<string> partial = Run(input, [], MakeCompaction(bottommost: false), new StringAppendNoPartialOperator());
        Assert.AreEqual(2, partial.Count, string.Join(" | ", partial));
        Assert.IsTrue(partial[0].StartsWith("a@3:merge", StringComparison.Ordinal));
    }

    [TestMethod]
    public void MergeStopsAtSnapshot()
    {
        var input = new TestInternalIterator();
        input.Add("a", EntryType.Value, "base", 1);
        input.Add("a", EntryType.Merge, "m1", 2);
        input.Add("a", EntryType.Merge, "m2", 3);
        // Snapshot 2 sees base,m1: the merge chain is split at the snapshot.
        AssertOutput(Run(input, [2], MakeCompaction(bottommost: false), new StringAppendOperator()), "a@3:merge=m2", "a@2:put=base,m1");
    }

    [TestMethod]
    public void MergeWithoutOperatorFails()
    {
        var input = new TestInternalIterator();
        input.Add("a", EntryType.Merge, "m1", 2);
        var ex = Assert.ThrowsExactly<RocksDbException>(() => Run(input, [], MakeCompaction(bottommost: false)));
        Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
    }

    [TestMethod]
    public void RangeTombstoneDropsCoveredKeysInSameStripe()
    {
        InternalKeyComparator icmp = InternalKeyComparator.Bytewise;
        var input = new TestInternalIterator();
        input.Add("a", EntryType.Value, "v", 1);
        input.Add("b", EntryType.Value, "v", 2);
        input.Add("c", EntryType.Value, "v", 6);
        input.Add("d", EntryType.Value, "v", 3);
        var rd = RangeDelAggregator.ForCompaction(icmp, [4]);
        rd.AddTombstones(FragmentedRangeTombstoneList.Build(icmp.UserComparator,
            [new FragmentedRangeTombstoneList.Tombstone(Encoding.UTF8.GetBytes("b"), Encoding.UTF8.GetBytes("e"), 5)]));
        // Tombstone@5 covers b@2 and d@3 but they are visible to snapshot 4 (different stripe): kept. c@6 is newer.
        AssertOutput(Run(input, [4], MakeCompaction(bottommost: false), rangeDel: rd), "a@1:put=v", "b@2:put=v", "c@6:put=v", "d@3:put=v");

        input = new TestInternalIterator();
        input.Add("a", EntryType.Value, "v", 1);
        input.Add("b", EntryType.Value, "v", 2);
        input.Add("c", EntryType.Value, "v", 6);
        input.Add("d", EntryType.Value, "v", 3);
        rd = RangeDelAggregator.ForCompaction(icmp, []);
        rd.AddTombstones(FragmentedRangeTombstoneList.Build(icmp.UserComparator,
            [new FragmentedRangeTombstoneList.Tombstone(Encoding.UTF8.GetBytes("b"), Encoding.UTF8.GetBytes("e"), 5)]));
        AssertOutput(Run(input, [], MakeCompaction(bottommost: false), rangeDel: rd), "a@1:put=v", "c@6:put=v");
    }

    private sealed class TestFilter : CompactionFilter
    {
        public override string Name => "TestFilter";

        public override CompactionFilterDecision Filter(int level, ReadOnlySpan<byte> key, CompactionFilterValueType valueType, ReadOnlySpan<byte> existingValue,
            IBufferWriter<byte> newValue, IBufferWriter<byte> skipUntil)
        {
            switch (Encoding.UTF8.GetString(key))
            {
                case "drop": return CompactionFilterDecision.Remove;
                case "change":
                    newValue.Write("new"u8);
                    return CompactionFilterDecision.ChangeValue;
                case "skip":
                    skipUntil.Write("t"u8);
                    return CompactionFilterDecision.RemoveAndSkipUntil;
                default: return CompactionFilterDecision.Keep;
            }
        }
    }

    [TestMethod]
    public void CompactionFilterDecisions()
    {
        var input = new TestInternalIterator();
        input.Add("change", EntryType.Value, "old", 1);
        input.Add("drop", EntryType.Value, "x", 2);
        input.Add("keep", EntryType.Value, "k", 3);
        input.Add("skip", EntryType.Value, "s", 4);
        input.Add("skipped", EntryType.Value, "s2", 5);
        input.Add("tail", EntryType.Value, "t", 6);
        // Not bottommost: the removed key becomes a deletion.
        AssertOutput(Run(input, [], MakeCompaction(bottommost: false), filter: new TestFilter()),
            "change@1:put=new", "drop@2:del=", "keep@3:put=k", "tail@6:put=t");
    }

    /// <summary>
    /// Reports <see cref="_lateSequence"/> as uncommitted on its first query only: the transaction commits while the
    /// compaction is running (a WritePrepared / WriteUnprepared commit racing a background compaction).
    /// </summary>
    private sealed class CommitsDuringCompactionChecker(ulong lateSequence) : SnapshotChecker
    {
        private readonly ulong _lateSequence = lateSequence;
        private bool _reported;

        public override SnapshotCheckerResult CheckInSnapshot(ulong sequence, ulong snapshotSequence)
        {
            if (sequence > snapshotSequence) return SnapshotCheckerResult.NotInSnapshot;
            if (sequence != _lateSequence || _reported) return SnapshotCheckerResult.InSnapshot;
            _reported = true;
            return SnapshotCheckerResult.NotInSnapshot;
        }
    }

    /// <summary>
    /// Regression (TwoWriteQueuesTests.CommitsRacingReaders): a version that was uncommitted when the iterator reached
    /// it is output as is, so the older committed version is kept too. If the commit lands before PrepareOutput, the
    /// uncommitted version must still not get its sequence zeroed (upstream requires current_key_committed_), or
    /// both versions become a@0 and the table builder rejects the out-of-order key.
    /// </summary>
    [TestMethod]
    public void UncommittedVersionCommittedDuringCompactionKeepsItsSequence()
    {
        var input = new TestInternalIterator();
        input.Add("a", EntryType.Value, "v3", 3);
        input.Add("a", EntryType.Value, "v5", 5);
        input.Add("b", EntryType.Value, "w", 6);
        AssertOutput(Run(input, [], MakeCompaction(bottommost: true), null, null, null, new CommitsDuringCompactionChecker(5), 10),
            "a@5:put=v5", "a@0:put=v3", "b@0:put=w");
    }
}
