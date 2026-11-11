using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4, slice "LastUnread": db/repair.cc beyond the WAL conversion, the deletion tail of
/// DBImpl::PurgeObsoleteFiles (db/db_impl/db_impl_files.cc) and the start of utilities/backup/backup_engine.cc.
/// </summary>
[TestClass]
public sealed class Audit4LastUnreadTests
{
    private static void DeleteManifests(IFileSystem fs)
    {
        foreach (string f in fs.GetChildren(DbPath))
        {
            if (f.StartsWith("MANIFEST-", StringComparison.Ordinal) || f == "CURRENT") fs.DeleteFile(DbPath + "/" + f);
        }
    }

    private static List<string> TableFiles(IFileSystem fs, string dir)
    {
        var tables = new List<string>();
        foreach (string f in fs.GetChildren(dir))
        {
            if (f.EndsWith(".sst", StringComparison.Ordinal)) tables.Add(f);
        }
        tables.Sort(StringComparer.Ordinal);
        return tables;
    }

    private static ColumnFamilyDescriptor[] DefaultAnd(string name, ColumnFamilyOptions options) =>
        [new("default", new ColumnFamilyOptions()), new(name, options)];

    // Repairer::ScanTable (db/repair.cc): the boundaries come from FileMetaData::UpdateBoundaries in iteration order and
    // UpdateBoundariesForRange with cfd->internal_comparator(), i.e. they are ordered by the comparator of the table's
    // column family, not by the comparator of the default column family.
    [TestMethod]
    public void Repair_TableOfColumnFamilyWithOtherComparator_KeepsItsKeyRange()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.CreateMissingColumnFamilies = true;
        ColumnFamilyDescriptor[] cfs = DefaultAnd("rev", new ColumnFamilyOptions { Comparator = Comparator.ReverseBytewise });
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o, cfs))
        {
            ColumnFamily rev = db.GetColumnFamily("rev");
            for (int i = 0; i < 50; i++) rev.Put(Key(i), Value(i, 0));
            rev.Flush();
        }
        DeleteManifests(fs);

        RocksDatabase.Repair(DbPath, new DbOptions { FileSystem = fs }, cfs, null);

        using RocksDatabase repaired = RocksDatabase.Open(DbPath, new DbOptions { FileSystem = fs }, cfs);
        LiveFileMetadata file = repaired.GetLiveFilesMetadata().Single(f => f.ColumnFamilyName == "rev");
        CollectionAssert.AreEqual(Key(49), file.SmallestKey, "smallest key in reverse order");
        CollectionAssert.AreEqual(Key(0), file.LargestKey, "largest key in reverse order");
        ColumnFamily cf = repaired.GetColumnFamily("rev");
        for (int i = 0; i < 50; i++) Assert.AreEqual(S(Value(i, 0)), GetString(cf, S(Key(i))!), "key " + i);
    }

    // Repairer::ScanTable: a table without a column family id goes to column family 0, but its recorded name stays and
    // "cfd->GetName() != props->column_family_name" then fails with Corruption("inconsistent column family name"), so
    // ExtractMetaData archives it. SstFileWriter without a column family writes such tables (id unknown, name "").
    // The native side of this is Audit4LastUnreadCompatTests.NativeRepairArchivesTableWithoutColumnFamilyName.
    [TestMethod]
    public void Repair_TableWithoutColumnFamilyName_IsArchived()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            db.Put(B("a"), B("1"));
            db.Flush();
        }
        using (var w = new SstFileWriter(new ColumnFamilyOptions(), fs))
        {
            w.Open(DbPath + "/000900.sst");
            w.Put(B("external"), B("x"));
            w.Finish();
        }
        DeleteManifests(fs);

        RocksDatabase.Repair(DbPath, new DbOptions { FileSystem = fs });

        Assert.IsTrue(fs.FileExists(DbPath + "/lost/000900.sst"), "archived");
        Assert.IsFalse(fs.FileExists(DbPath + "/000900.sst"));
        using RocksDatabase repaired = RocksDatabase.Open(DbPath, new DbOptions { FileSystem = fs });
        Assert.AreEqual("1", GetString(repaired, "a"));
        Assert.IsNull(repaired.Get(B("external")));
    }

    // Repairer::ScanTable calls AddColumnFamily (a MANIFEST edit) as soon as the properties name an unknown column
    // family id, before the table is scanned: the column family exists in the repaired DB even when the scan then fails
    // and ExtractMetaData archives the table.
    [TestMethod]
    public void Repair_ColumnFamilyOfTableArchivedByTheScan_StillExists()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.CreateMissingColumnFamilies = true;
        ColumnFamilyDescriptor[] cfs = DefaultAnd("one", new ColumnFamilyOptions());
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o, cfs))
        {
            ColumnFamily one = db.GetColumnFamily("one");
            for (int i = 0; i < 20; i++) one.Put(Key(i), Value(i, 0));
            one.Flush();
        }
        List<string> tables = TableFiles(fs, DbPath);
        Assert.AreEqual(1, tables.Count);
        // Damage the first data block: the table still opens (footer, index, properties), reading its entries fails.
        string path = DbPath + "/" + tables[0];
        byte[] data = fs.ReadAllBytes(path);
        data[3] ^= 0xff;
        using (IWritableFile w = fs.CreateWritable(path)) w.Append(data);
        DeleteManifests(fs);

        RocksDatabase.Repair(DbPath, new DbOptions { FileSystem = fs });

        Assert.IsTrue(fs.FileExists(DbPath + "/lost/" + tables[0]), "the damaged table is archived");
        CollectionAssert.AreEquivalent(new[] { "default", "one" }, RocksDatabase.ListColumnFamilies(DbPath, fs).ToArray());
    }

    // GetDefaultCFOptions (db/repair.cc): RepairDB with column family descriptors needs one for the default column family.
    [TestMethod]
    public void Repair_DescriptorsWithoutDefaultColumnFamily_IsInvalidArgument()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs))) db.Put(B("a"), B("1"));
        var ex = Assert.ThrowsExactly<RocksDbException>(() =>
            RocksDatabase.Repair(DbPath, new DbOptions { FileSystem = fs }, [new ColumnFamilyDescriptor("one", new ColumnFamilyOptions())], new ColumnFamilyOptions()));
        Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
        // Nothing was touched.
        Assert.IsTrue(fs.FileExists(DbPath + "/CURRENT"));
    }

    // Repairer::AddTables -> VersionStorageInfo::RecoverEpochNumbers(cfd): with allow_ingest_behind the first epoch
    // number (kReservedEpochNumberForFileIngestedBehind = 1) is reserved, the oldest L0 file gets 2.
    [TestMethod]
    public void Repair_AllowIngestBehind_ReservesTheFirstEpochNumber()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.AllowIngestBehind = true;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o))
        {
            db.Put(B("a"), B("1"));
            db.Flush();
            db.Put(B("b"), B("2"));
            db.Flush();
        }
        DeleteManifests(fs);

        RocksDatabase.Repair(DbPath, new DbOptions { FileSystem = fs, AllowIngestBehind = true });

        using RocksDatabase repaired = RocksDatabase.Open(DbPath, new DbOptions { FileSystem = fs, AllowIngestBehind = true });
        List<ulong> epochs = repaired.GetLiveFilesMetadata().Select(f => f.EpochNumber).Order().ToList();
        CollectionAssert.AreEqual(new ulong[] { 2, 3 }, epochs);
        Assert.AreEqual("1", GetString(repaired, "a"));
        Assert.AreEqual("2", GetString(repaired, "b"));
    }

    // Repairer::ScanTable -> FileMetaData::UpdateBoundaries(key, value, seq, type): a kTypeBlobIndex entry sets
    // oldest_blob_file_number, which AddTables writes to the MANIFEST.
    [TestMethod]
    public void Repair_TableWithBlobIndexes_RecordsTheOldestBlobFile()
    {
        MemoryFileSystem fs = NewFs();
        var cfo = new ColumnFamilyOptions { EnableBlobFiles = true, MinBlobSize = 16 };
        ColumnFamilyDescriptor[] cfs = [new("default", cfo)];
        ulong expected;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfs))
        {
            db.Put(B("a"), new byte[200]);
            db.Flush();
            expected = db.GetLiveFilesMetadata().Single().OldestBlobFileNumber;
            Assert.AreNotEqual(0UL, expected);
        }
        DeleteManifests(fs);

        RocksDatabase.Repair(DbPath, new DbOptions { FileSystem = fs }, cfs, null);

        using RocksDatabase repaired = RocksDatabase.Open(DbPath, new DbOptions { FileSystem = fs }, cfs);
        Assert.AreEqual(expected, repaired.GetLiveFilesMetadata().Single().OldestBlobFileNumber);
    }

    private sealed class DeletionListener : EventListener
    {
        public readonly List<TableFileDeletionInfo> Deleted = new();

        public override void OnTableFileDeleted(TableFileDeletionInfo info)
        {
            lock (Deleted) Deleted.Add(info);
        }
    }

    // DBImpl::PurgeObsoleteFiles -> DeleteObsoleteFileImpl (db/db_impl/db_impl_files.cc): a table file that only the
    // full scan finds (not live, not a pending output) is deleted like a queued one, including
    // EventHelpers::LogAndNotifyTableFileDeletion (EventListener::OnTableFileDeleted).
    [TestMethod]
    public void FullScan_OrphanTableFile_IsReportedToListeners()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            db.Put(B("a"), B("1"));
            db.Flush();
        }
        string orphan = DbPath + "/000900.sst";
        using (IWritableFile w = fs.CreateWritable(orphan)) w.Append(new byte[100]);
        var listener = new DeletionListener();
        DbOptions o = NewOptions(fs);
        o.Listeners.Add(listener);

        using (RocksDatabase db = RocksDatabase.Open(DbPath, o))
        {
            Assert.AreEqual("1", GetString(db, "a"));
            Assert.IsFalse(fs.FileExists(orphan));
        }

        lock (listener.Deleted)
        {
            TableFileDeletionInfo info = listener.Deleted.Single(d => d.FilePath.EndsWith("/000900.sst", StringComparison.Ordinal));
            Assert.IsNull(info.Status);
            Assert.AreEqual(DbPath, info.DbName);
        }
    }
}
