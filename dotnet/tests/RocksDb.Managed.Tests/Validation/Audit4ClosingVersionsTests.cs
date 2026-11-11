using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit 4, slice "ClosingVersions": VersionBuilder (db/version_builder.cc) against the MANIFEST replay and the live
/// version edits, plus the manual-compaction / column family id items of the remaining list.
/// </summary>
[TestClass]
public sealed class Audit4ClosingVersionsTests
{
    private const string DbPath = "/audit4-closing-versions";

    private static FileMetaData F(ulong number, string smallest, string largest, ulong seq, ulong epoch) => new()
    {
        FileNumber = number,
        FileSize = 1000 + number,
        Smallest = new InternalKey(B(smallest), seq, EntryType.Value).Encoded.ToArray(),
        Largest = new InternalKey(B(largest), seq, EntryType.Value).Encoded.ToArray(),
        SmallestSeqno = seq,
        LargestSeqno = seq,
        EpochNumber = epoch,
    };

    private static VersionEdit BaseEdit() =>
        new() { ComparatorName = "leveldb.BytewiseComparator", PersistUserDefinedTimestamps = true, LogNumber = 0, NextFileNumber = 2, LastSequence = 0 };

    // VersionBuilder::SaveSSTFilesTo (db/version_builder.cc:1794-1815): while a file of the column family has no epoch
    // number (PromoteEpochNumberRequirementIfNeeded fails, kMightMissing), L0 is ordered by NewestFirstBySeqNo only.
    // Epoch numbers present on other L0 files must not decide the order (RecoverEpochNumbers then reassigns all of them
    // from that order).
    [TestMethod]
    public void ReplayOrdersL0BySeqnoWhileSomeFileHasNoEpochNumber()
    {
        var r = new ManifestReplayer(null);
        r.Apply(BaseEdit());
        var e = new VersionEdit { LogNumber = 5, NextFileNumber = 20, LastSequence = 40 };
        e.AddFile(0, F(10, "a", "m", seq: 10, epoch: 7)); // older data, but a (stale) high epoch number
        e.AddFile(0, F(11, "a", "m", seq: 20, epoch: 3));
        e.AddFile(0, F(12, "a", "m", seq: 30, epoch: 5));
        e.AddFile(1, F(9, "a", "m", seq: 1, epoch: FileMetaData.UnknownEpochNumber)); // written before epoch numbers existed
        r.Apply(e);
        RecoveredVersionState state = r.Finish();
        IReadOnlyList<FileMetaData> l0 = state.ColumnFamilies[0].Levels[0];
        CollectionAssert.AreEqual(new ulong[] { 12, 11, 10 }, l0.Select(f => f.FileNumber).ToArray());
    }

    // Counterpart: with every epoch number present the requirement is promoted to kMustPresent and L0 is ordered by
    // NewestFirstByEpochNumber (version_builder.cc:1775-1792, 1814).
    [TestMethod]
    public void ReplayOrdersL0ByEpochNumberWhenAllFilesHaveOne()
    {
        var r = new ManifestReplayer(null);
        r.Apply(BaseEdit());
        var e = new VersionEdit { LogNumber = 5, NextFileNumber = 20, LastSequence = 40 };
        e.AddFile(0, F(10, "a", "m", seq: 10, epoch: 7));
        e.AddFile(0, F(11, "a", "m", seq: 20, epoch: 3));
        e.AddFile(0, F(12, "a", "m", seq: 30, epoch: 5));
        e.AddFile(1, F(9, "a", "m", seq: 1, epoch: 1));
        r.Apply(e);
        IReadOnlyList<FileMetaData> l0 = r.Finish().ColumnFamilies[0].Levels[0];
        CollectionAssert.AreEqual(new ulong[] { 10, 12, 11 }, l0.Select(f => f.FileNumber).ToArray());
    }

    // VersionBuilder::CheckConsistencyDetails (version_builder.cc:829-851): the L0 check follows the version's epoch
    // number requirement, and VersionStorageInfo::HasMissingEpochNumber looks at every level. A version whose L1 file
    // has no epoch number is checked by sequence numbers; its L0 order by seqno is not Corruption.
    [TestMethod]
    public void ConsistencyCheckUsesSeqnoOrderWhenANonL0FileHasNoEpochNumber()
    {
        var icmp = new InternalKeyComparator(BytewiseComparator.Instance);
        FileMetaData[][] levels =
        [
            [F(12, "a", "m", 30, 5), F(11, "a", "m", 20, 3), F(10, "a", "m", 10, 7)],
            [F(9, "a", "m", 1, FileMetaData.UnknownEpochNumber)],
        ];
        VersionStorageInfo.CheckConsistency(icmp, levels, [], forceConsistencyChecks: true);
        // The same L0 order with all epoch numbers present is rejected (kMustPresent, NewestFirstByEpochNumber).
        levels[1][0].EpochNumber = 1;
        var ex = Assert.ThrowsExactly<RocksDbException>(() => VersionStorageInfo.CheckConsistency(icmp, levels, [], forceConsistencyChecks: true));
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
        StringAssert.Contains(ex.Message, "L0 files are not sorted properly: files #11 with epoch number 3, #10 with epoch number 7");
    }

    // VersionBuilder::ApplyFileDeletion (version_builder.cc:1157-1173): deleting a table file from a level it is not on
    // names the level it is on; only an unknown file is "not in the LSM tree".
    [TestMethod]
    public void LiveEditDeletingAFileFromTheWrongLevelNamesItsLevel()
    {
        var icmp = new InternalKeyComparator(BytewiseComparator.Instance);
        var storage = new VersionStorageInfo(icmp, 3, new ColumnFamilyOptions(), [[F(5, "a", "c", 9, 2)], [F(4, "d", "f", 3, 1)], []]);

        var wrongLevel = new VersionEdit();
        wrongLevel.DeleteFile(0, 4);
        var ex = Assert.ThrowsExactly<RocksDbException>(() => storage.ApplyEdit(wrongLevel, 3));
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
        StringAssert.Contains(ex.Message, "VersionBuilder: Cannot delete table file #4 from level 0 since it is on level 1");

        var unknown = new VersionEdit();
        unknown.DeleteFile(1, 77);
        ex = Assert.ThrowsExactly<RocksDbException>(() => storage.ApplyEdit(unknown, 3));
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
        StringAssert.Contains(ex.Message, "VersionBuilder: Cannot delete table file #77 from level 1 since it is not in the LSM tree");

        // Deletions are applied before additions (VersionBuilder::Apply): a trivial move in one edit is fine.
        var move = new VersionEdit();
        move.DeleteFile(0, 5);
        move.AddFile(2, F(5, "a", "c", 9, 2));
        FileMetaData[][] moved = storage.ApplyEdit(move, 3);
        Assert.AreEqual(0, moved[0].Length);
        Assert.AreEqual(5UL, moved[2][0].FileNumber);
    }

    private sealed class UseLocalService : CompactionService
    {
        public int Scheduled;
        public override string Name => "UseLocal";

        public override ValueTask<CompactionServiceScheduleResponse> ScheduleAsync(CompactionServiceJobInfo info, ReadOnlyMemory<byte> input,
            CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref Scheduled);
            return ValueTask.FromResult(new CompactionServiceScheduleResponse("", CompactionServiceJobStatus.UseLocal));
        }
    }

    // DBImpl::CompactFilesImpl (db_impl_compaction_flush.cc:1840-1845): with a compaction_service a CompactFiles call is
    // never a trivial move, even with allow_trivial_move: the service sees the job and the file is rewritten.
    [TestMethod]
    public void CompactFilesWithACompactionServiceIsNotATrivialMove()
    {
        MemoryFileSystem fs = NewFs();
        var svc = new UseLocalService();
        DbOptions o = NewOptions(fs);
        o.CompactionService = svc;
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, new ColumnFamilyOptions { DisableAutoCompactions = true });
        db.Put(B("a"), B("1"));
        db.Put(B("b"), B("2"));
        db.Flush();
        string input = db.GetLiveFilesMetadata().Single().Name;
        db.CompactFiles(new CompactFilesOptions { AllowTrivialMove = true }, [input], 1);
        Assert.AreEqual(1, Volatile.Read(ref svc.Scheduled));
        LiveFileMetadata output = db.GetLiveFilesMetadata().Single();
        Assert.AreEqual(1, output.Level);
        Assert.AreNotEqual(input, output.Name);
        Assert.AreEqual("1", GetString(db, "a"));
    }

    // Without a service the same call moves the file (CompactFilesImpl trivial move branch).
    [TestMethod]
    public void CompactFilesWithoutAServiceTriviallyMovesTheFile()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { DisableAutoCompactions = true });
        db.Put(B("a"), B("1"));
        db.Flush();
        string input = db.GetLiveFilesMetadata().Single().Name;
        db.CompactFiles(new CompactFilesOptions { AllowTrivialMove = true }, [input], 1);
        LiveFileMetadata output = db.GetLiveFilesMetadata().Single();
        Assert.AreEqual(1, output.Level);
        Assert.AreEqual(input, output.Name);
    }

    // VersionSet::LogAndApplyCFHelper / ProcessManifestWrites (version_set.cc:6244-6249, 6774-6778): max_column_family
    // survives a drop, a reopen and a MANIFEST rollover, so the id of a dropped column family is never reused.
    [TestMethod]
    public void DroppedColumnFamilyIdIsNotReusedAfterReopenAndManifestRollover()
    {
        MemoryFileSystem fs = NewFs();
        uint droppedId;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            db.CreateColumnFamily("one", new ColumnFamilyOptions());
            ColumnFamily two = db.CreateColumnFamily("two", new ColumnFamilyOptions());
            droppedId = two.Id;
            db.DropColumnFamily(two);
        }
        ColumnFamilyDescriptor[] cfs = [new("default", new ColumnFamilyOptions()), new("one", new ColumnFamilyOptions())];
        // Every open writes a new MANIFEST from the recovered state: two reopens cover recovery from the edits and from
        // the snapshot.
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfs))
        {
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfs))
        {
            ColumnFamily three = db.CreateColumnFamily("three", new ColumnFamilyOptions());
            Assert.IsTrue(three.Id > droppedId, "id " + three.Id + " reuses or precedes dropped id " + droppedId);
        }
    }
}
