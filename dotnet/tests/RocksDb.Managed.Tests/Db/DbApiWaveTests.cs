using System.Buffers;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>
/// DB APIs: GetMergeOperands (db/db_merge_operand_test.cc), PromoteL0 / SuggestCompactRange (db/db_test2.cc,
/// db/db_compaction_test.cc), LockWAL (db/db_write_test.cc), Disable/EnableManualCompaction, DeleteFilesInRange,
/// ClipColumnFamily (db/db_with_timestamp... db/db_test2.cc), GetCreationTimeOfOldestFile,
/// GetPropertiesOfTablesInRange (db/db_table_properties_test.cc) and OpenAndTrimHistory.
/// </summary>
[TestClass]
public sealed class DbApiWaveTests
{
    private static RocksDatabase OpenMerge(IFileSystem fs, bool autoCompactions = false) =>
        RocksDatabase.Open(DbPath, NewOptions(fs),
            new ColumnFamilyOptions { MergeOperator = new StringAppendMergeOperator(), DisableAutoCompactions = !autoCompactions });

    private static string[] Strings(MergeOperandsResult r)
    {
        var s = new string[r.Count];
        for (int i = 0; i < r.Count; i++) s[i] = System.Text.Encoding.UTF8.GetString(r[i]);
        return s;
    }

    // ---- GetMergeOperands ---------------------------------------------------------------------------

    [TestMethod]
    public void GetMergeOperandsAcrossMemtableAndFiles()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = OpenMerge(fs);
        var all = new GetMergeOperandsOptions { ExpectedMaxNumberOfOperands = 10 };
        using var r = new MergeOperandsResult();
        // Plain value: one operand.
        db.Put(B("v"), B("x"));
        Assert.IsTrue(db.TryGetMergeOperands(B("v"), all, r));
        CollectionAssert.AreEqual(new[] { "x" }, Strings(r));
        // Not found.
        Assert.IsFalse(db.TryGetMergeOperands(B("nope"), all, r));
        Assert.AreEqual(0, r.Count);
        // Base + operands spread over two files and the memtable.
        db.Put(B("k"), B("a"));
        db.Merge(B("k"), B("b"));
        db.Flush();
        db.Merge(B("k"), B("c"));
        db.Flush();
        db.Merge(B("k"), B("d"));
        db.Merge(B("k"), B("e"));
        Assert.IsTrue(db.TryGetMergeOperands(B("k"), all, r));
        CollectionAssert.AreEqual(new[] { "a", "b", "c", "d", "e" }, Strings(r));
        Assert.AreEqual(5, r.NumberOfOperands);
        Assert.AreEqual("a,b,c,d,e", GetString(db, "k"));
        // Deletion below the operands: operands only.
        db.Delete(B("m"));
        db.Merge(B("m"), B("1"));
        db.Flush();
        db.Merge(B("m"), B("2"));
        Assert.IsTrue(db.TryGetMergeOperands(B("m"), all, r));
        CollectionAssert.AreEqual(new[] { "1", "2" }, Strings(r));
        // Async gives the same answer.
        Assert.IsTrue(db.TryGetMergeOperandsAsync(null, B("k"), all, r).AsTask().GetAwaiter().GetResult());
        CollectionAssert.AreEqual(new[] { "a", "b", "c", "d", "e" }, Strings(r));
    }

    [TestMethod]
    public void GetMergeOperandsLimitAndContinueCallback()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = OpenMerge(fs);
        db.Merge(B("k"), B("1"));
        db.Merge(B("k"), B("2"));
        db.Flush();
        db.Merge(B("k"), B("3"));
        db.Merge(B("k"), B("4"));
        using var r = new MergeOperandsResult();
        // More operands than expected: nothing returned, but the count (Status::Incomplete upstream).
        Assert.IsTrue(db.TryGetMergeOperands(B("k"), new GetMergeOperandsOptions { ExpectedMaxNumberOfOperands = 3 }, r));
        Assert.IsTrue(r.ExceededExpectedMax);
        Assert.AreEqual(4, r.NumberOfOperands);
        Assert.AreEqual(0, r.Count);

        // continue_cb sees operands newest first; stopping after the second keeps the two newest (in the memtable).
        var seen = new List<string>();
        int calls = 0;
        var stopAfterTwo = new GetMergeOperandsOptions
        {
            ExpectedMaxNumberOfOperands = 4,
            ContinueCallback = v =>
            {
                seen.Add(System.Text.Encoding.UTF8.GetString(v));
                return ++calls < 2;
            },
        };
        Assert.IsTrue(db.TryGetMergeOperands(B("k"), stopAfterTwo, r));
        CollectionAssert.AreEqual(new[] { "4", "3" }, seen);
        CollectionAssert.AreEqual(new[] { "3", "4" }, Strings(r));

        // Stopping in the table file (third operand).
        seen.Clear();
        calls = 0;
        var stopAfterThree = new GetMergeOperandsOptions
        {
            ExpectedMaxNumberOfOperands = 4,
            ContinueCallback = v =>
            {
                seen.Add(System.Text.Encoding.UTF8.GetString(v));
                return ++calls < 3;
            },
        };
        Assert.IsTrue(db.TryGetMergeOperands(B("k"), stopAfterThree, r));
        CollectionAssert.AreEqual(new[] { "4", "3", "2" }, seen);
        CollectionAssert.AreEqual(new[] { "2", "3", "4" }, Strings(r));
    }

    // ---- PromoteL0 / SuggestCompactRange ------------------------------------------------------------

    private static int FilesAtLevel(RocksDatabase db, int level)
    {
        int n = 0;
        foreach (LiveFileMetadata f in db.GetLiveFilesMetadata()) if (f.Level == level) n++;
        return n;
    }

    [TestMethod]
    public void PromoteL0MovesNonOverlappingFiles()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = OpenMerge(fs);
        for (int f = 0; f < 3; f++)
        {
            for (int i = 0; i < 10; i++) db.Put(Key(f * 100 + i), Value(i, f));
            db.Flush();
        }
        Assert.AreEqual(3, FilesAtLevel(db, 0));
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => db.PromoteL0(0)).Code);
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => db.PromoteL0(7)).Code);
        db.PromoteL0(2);
        Assert.AreEqual(0, FilesAtLevel(db, 0));
        Assert.AreEqual(3, FilesAtLevel(db, 2));
        for (int f = 0; f < 3; f++) Assert.AreEqual(S(Value(5, f)), GetString(db, S(Key(f * 100 + 5))!));
        // Overlapping L0 files can't be promoted; nor into a non-empty level.
        db.Put(Key(5), B("a"));
        db.Put(Key(7), B("a"));
        db.Flush();
        db.Put(Key(6), B("b"));
        db.Flush();
        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => db.PromoteL0(1));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        db.Put(Key(205), B("c"));
        db.Flush();
        e = Assert.ThrowsExactly<RocksDbException>(() => db.PromoteL0(3));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
    }

    [TestMethod]
    public void SuggestCompactRangeTriggersCompaction()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        // L0 trigger high enough not to compact by itself.
        var cfo = new ColumnFamilyOptions { Level0FileNumCompactionTrigger = 100 };
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, cfo);
        for (int f = 0; f < 4; f++)
        {
            for (int i = 0; i < 20; i++) db.Put(Key(i), Value(i, f));
            db.Flush();
        }
        db.CompactRange(); // everything in the last level
        for (int f = 0; f < 3; f++)
        {
            for (int i = 0; i < 20; i++) db.Put(Key(i), Value(i, 10 + f));
            db.Flush();
        }
        Assert.AreEqual(3, FilesAtLevel(db, 0));
        db.SuggestCompactRange(null, null);
        db.WaitForCompact();
        Assert.AreEqual(0, FilesAtLevel(db, 0));
        Assert.AreEqual(S(Value(3, 12)), GetString(db, S(Key(3))!));
    }

    // ---- LockWAL -------------------------------------------------------------------------------------

    [TestMethod]
    public async Task LockWalStopsWritesUntilUnlocked()
    {
        MemoryFileSystem fs = NewFs();
        await using RocksDatabase db = await RocksDatabase.OpenAsync(DbPath, NewOptions(fs));
        db.Put(B("a"), B("1"));
        ulong seq = db.GetLatestSequenceNumber();
        WalLock outer = db.LockWal();
        WalLock inner = await db.LockWalAsync();
        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => db.Put(new WriteOptions { NoSlowdown = true }, B("b"), B("2")));
        Assert.AreEqual(StatusCode.Incomplete, e.Code);
        Task blocked = Task.Run(() => db.Put(B("c"), B("3")));
        // Checkpoints work (without flushing) while the WAL is locked.
        db.CreateCheckpoint("/cp");
        Assert.IsFalse(blocked.Wait(100));
        Assert.AreEqual(seq, db.GetLatestSequenceNumber());
        inner.Dispose();
        inner.Dispose(); // idempotent
        Assert.IsFalse(blocked.Wait(50));
        outer.Dispose();
        await blocked.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.AreEqual("3", GetString(db, "c"));
        using RocksDatabase cp = RocksDatabase.Open("/cp", NewOptions(fs));
        Assert.AreEqual("1", GetString(cp, "a"));
        Assert.IsNull(GetString(cp, "c"));
    }

    // ---- manual compaction pause --------------------------------------------------------------------

    [TestMethod]
    public void DisableManualCompactionFailsManualCompactions()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = OpenMerge(fs);
        for (int i = 0; i < 10; i++) db.Put(Key(i), Value(i, 0));
        db.Flush();
        db.DisableManualCompaction();
        db.DisableManualCompaction();
        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => db.CompactRange());
        Assert.AreEqual(StatusCode.Incomplete, e.Code);
        Assert.AreEqual(StatusSubCode.ManualCompactionPaused, e.SubCode);
        string file = db.GetLiveFilesMetadata()[0].Name;
        e = Assert.ThrowsExactly<RocksDbException>(() => db.CompactFiles([file], 1));
        Assert.AreEqual(StatusSubCode.ManualCompactionPaused, e.SubCode);
        db.EnableManualCompaction();
        Assert.AreEqual(StatusCode.Incomplete, Assert.ThrowsExactly<RocksDbException>(() => db.CompactRange()).Code);
        db.EnableManualCompaction();
        db.CompactRange();
        Assert.AreEqual(0, FilesAtLevel(db, 0));
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(db.EnableManualCompaction).Code);
    }

    // ---- file-level deletes / clip --------------------------------------------------------------------

    [TestMethod]
    public void DeleteFilesInRangeAndClipColumnFamily()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = OpenMerge(fs);
        for (int f = 0; f < 4; f++)
        {
            for (int i = 0; i < 10; i++) db.Put(Key(f * 10 + i), Value(i, f));
            db.Flush();
        }
        db.PromoteL0(1); // four non-overlapping L1 files: [0,9] [10,19] [20,29] [30,39]
        // Only whole files strictly inside [key10, key29) go: [10,19]; [20,29] ends at the excluded limit's file.
        db.DeleteFilesInRange(Key(10), Key(29), includeEnd: false);
        Assert.AreEqual(3, FilesAtLevel(db, 1));
        Assert.IsNull(GetString(db, S(Key(15))!));
        Assert.IsNotNull(GetString(db, S(Key(25))!));
        db.DeleteFilesInRange(Key(20), Key(29), includeEnd: true);
        Assert.IsNull(GetString(db, S(Key(25))!));

        // Clip to [key05, key35).
        db.Put(Key(36), B("mem"));
        db.ClipColumnFamily(Key(5), Key(35));
        for (int i = 0; i < 40; i++)
        {
            string? v = GetString(db, S(Key(i))!);
            bool kept = i >= 5 && i < 35 && (i < 10 || i >= 30);
            Assert.AreEqual(kept, v is not null, "key " + i);
        }
        Assert.IsNull(GetString(db, S(Key(36))!));
    }

    // ---- properties / creation time -----------------------------------------------------------------

    [TestMethod]
    public void PropertiesOfTablesInRangeAndOldestFileTime()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, new ColumnFamilyOptions { DisableAutoCompactions = true });
        Assert.AreEqual(ulong.MaxValue, db.GetCreationTimeOfOldestFile());
        ulong before = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        for (int f = 0; f < 3; f++)
        {
            for (int i = 0; i < 10; i++) db.Put(Key(f * 10 + i), Value(i, f));
            db.Flush();
        }
        ulong t = db.GetCreationTimeOfOldestFile();
        Assert.IsTrue(t >= before && t <= (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds(), t.ToString(System.Globalization.CultureInfo.InvariantCulture));
        Dictionary<string, TableProperties> props = db.GetPropertiesOfTablesInRange([new KeyRange(Key(12), Key(13))]);
        Assert.AreEqual(1, props.Count);
        foreach (TableProperties p in props.Values) Assert.AreEqual(10UL, p.NumEntries);
        props = db.GetPropertiesOfTablesInRange([new KeyRange(Key(5), Key(15)), new KeyRange(Key(25), Key(99))]);
        Assert.AreEqual(3, props.Count);
        Assert.AreEqual(0, db.GetPropertiesOfTablesInRange([new KeyRange(B("a"), B("b"))]).Count);
        List<Dictionary<string, TableProperties>> byLevel = db.DefaultColumnFamily.GetPropertiesOfTablesByLevel();
        Assert.AreEqual(3, byLevel[0].Count);
    }

    [TestMethod]
    public void GetCreationTimeOfOldestFileNeedsUnlimitedOpenFiles()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.MaxOpenFiles = 100;
        using RocksDatabase db = RocksDatabase.Open(DbPath, o);
        Assert.AreEqual(StatusCode.NotSupported, Assert.ThrowsExactly<RocksDbException>(() => db.GetCreationTimeOfOldestFile()).Code);
    }

    // ---- OpenAndTrimHistory -------------------------------------------------------------------------

    [TestMethod]
    public void OpenAndTrimHistoryDropsNewerVersions()
    {
        MemoryFileSystem fs = NewFs();
        var cfo = new ColumnFamilyOptions { Comparator = Comparator.BytewiseWithU64Ts, DisableAutoCompactions = true };
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo))
        {
            db.Put(B("a"), DbTimestampTests.Ts(10), B("a10"));
            db.Put(B("b"), DbTimestampTests.Ts(15), B("b15"));
            db.Flush();
            db.Put(B("a"), DbTimestampTests.Ts(20), B("a20"));
            db.Put(B("c"), DbTimestampTests.Ts(25), B("c25"));
            db.Delete(B("b"), DbTimestampTests.Ts(30));
            // left in the WAL: flushed by recovery, then trimmed
        }
        DbOptions o = NewOptions(fs);
        o.AvoidFlushDuringRecovery = true;
        var cfs = new[] { new ColumnFamilyDescriptor("default", cfo) };
        Assert.AreEqual(StatusCode.InvalidArgument,
            Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.OpenAndTrimHistory(DbPath, o, cfs, DbTimestampTests.Ts(18))).Code);
        using (RocksDatabase db = RocksDatabase.OpenAndTrimHistory(DbPath, NewOptions(fs), cfs, DbTimestampTests.Ts(18)))
        {
            var max = new ReadOptions { Timestamp = DbTimestampTests.Ts(ulong.MaxValue) };
            Assert.AreEqual("a10", S(db.Get(max, B("a"))));
            Assert.AreEqual("b15", S(db.Get(max, B("b"))));
            Assert.IsNull(db.Get(max, B("c")));
        }
    }

    // ---- export / import ------------------------------------------------------------------------------

    private static void Fill(ColumnFamily cf, int from, int to, int round)
    {
        for (int i = from; i < to; i++) cf.Put(Key(i), Value(i, round));
    }

    [TestMethod]
    public void ExportAndImportColumnFamily()
    {
        MemoryFileSystem fs = NewFs();
        ExportImportFilesMetadata meta;
        ulong srcSeq;
        using (RocksDatabase src = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            ColumnFamily cf = src.CreateColumnFamily("data", new ColumnFamilyOptions { DisableAutoCompactions = true });
            Fill(cf, 0, 50, 0);
            cf.Flush();
            Fill(cf, 25, 75, 1);
            cf.Flush();
            cf.CompactRange(); // one file in the bottommost level
            Fill(cf, 70, 100, 2); // memtable: flushed by the export
            cf.Delete(Key(1));
            meta = cf.ExportColumnFamily("/export");
            srcSeq = src.GetLatestSequenceNumber();
            Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => cf.ExportColumnFamily("/export")).Code);
        }
        Assert.AreEqual(Comparator.Bytewise.Name, meta.DbComparatorName);
        Assert.AreEqual(2, meta.Files.Count);
        Assert.IsFalse(fs.DirectoryExists("/export.tmp"));

        fs.CreateDirectoryIfMissing("/dst");
        using RocksDatabase dst = RocksDatabase.Open("/dst", NewOptions(fs));
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() =>
            dst.CreateColumnFamilyWithImport("bad", new ColumnFamilyOptions { Comparator = Comparator.ReverseBytewise }, new ImportColumnFamilyOptions(), meta)).Code);
        Assert.IsFalse(dst.TryGetColumnFamily("bad", out _));
        ColumnFamily imported = dst.CreateColumnFamilyWithImport("imported", new ColumnFamilyOptions(), new ImportColumnFamilyOptions(), meta);
        Assert.IsTrue(dst.GetLatestSequenceNumber() >= srcSeq - 1, "last sequence covers the imported files");
        for (int i = 0; i < 100; i++)
        {
            string? expected = i == 1 ? null : S(Value(i, i >= 70 ? 2 : i >= 25 ? 1 : 0));
            Assert.AreEqual(expected, GetString(imported, S(Key(i))!), "key " + i);
        }
        // Writes after the import are newer than the imported data.
        imported.Put(Key(3), B("new"));
        imported.Flush();
        imported.CompactRange();
        Assert.AreEqual("new", GetString(imported, S(Key(3))!));
        // The originals are kept (copy).
        foreach (LiveFileMetadata f in meta.Files) Assert.IsTrue(fs.FileExists(f.Directory + "/" + f.Name));
    }

    [TestMethod]
    public void ImportMultipleExportsAndMoveFiles()
    {
        MemoryFileSystem fs = NewFs();
        var metas = new List<ExportImportFilesMetadata>();
        using (RocksDatabase src = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            ColumnFamily a = src.CreateColumnFamily("a", new ColumnFamilyOptions());
            ColumnFamily b = src.CreateColumnFamily("b", new ColumnFamilyOptions());
            ColumnFamily c = src.CreateColumnFamily("c", new ColumnFamilyOptions());
            Fill(a, 0, 10, 0);
            Fill(b, 10, 20, 1);
            Fill(c, 15, 25, 2); // overlaps b
            metas.Add(a.ExportColumnFamily("/ea"));
            metas.Add(b.ExportColumnFamily("/eb"));
            metas.Add(c.ExportColumnFamily("/ec"));
        }
        fs.CreateDirectoryIfMissing("/dst");
        using RocksDatabase dst = RocksDatabase.Open("/dst", NewOptions(fs));
        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() =>
            dst.CreateColumnFamilyWithImport("x", new ColumnFamilyOptions(), new ImportColumnFamilyOptions(), metas));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        StringAssert.Contains(e.Message, "overlapping");
        ColumnFamily merged = dst.CreateColumnFamilyWithImport("x", new ColumnFamilyOptions(), new ImportColumnFamilyOptions { MoveFiles = true },
            [metas[0], metas[1]]);
        for (int i = 0; i < 20; i++) Assert.AreEqual(S(Value(i, i < 10 ? 0 : 1)), GetString(merged, S(Key(i))!));
        // move_files: the originals are deleted.
        foreach (LiveFileMetadata f in metas[0].Files) Assert.IsFalse(fs.FileExists(f.Directory + "/" + f.Name));
        // Survives a reopen (MANIFEST holds the imported files).
        dst.Dispose();
        using RocksDatabase re = RocksDatabase.Open("/dst", NewOptions(fs),
            [new ColumnFamilyDescriptor("default", new ColumnFamilyOptions()), new ColumnFamilyDescriptor("x", new ColumnFamilyOptions())]);
        Assert.AreEqual(S(Value(15, 1)), GetString(re.GetColumnFamily("x"), S(Key(15))!));
    }

    [TestMethod]
    public void ImportWithoutInternalKeysReadsTheFiles()
    {
        MemoryFileSystem fs = NewFs();
        ExportImportFilesMetadata meta;
        using (RocksDatabase src = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            Fill(src.DefaultColumnFamily, 0, 30, 0);
            src.DeleteRange(Key(5), Key(8));
            meta = src.DefaultColumnFamily.ExportColumnFamily("/e");
        }
        // As received through the C API: no internal keys.
        var stripped = new List<LiveFileMetadata>();
        foreach (LiveFileMetadata f in meta.Files) stripped.Add(f with { SmallestInternalKey = [], LargestInternalKey = [], Size = 0 });
        fs.CreateDirectoryIfMissing("/dst");
        using RocksDatabase dst = RocksDatabase.Open("/dst", NewOptions(fs));
        ColumnFamily cf = dst.CreateColumnFamilyWithImport("i", new ColumnFamilyOptions(), new ImportColumnFamilyOptions(),
            new ExportImportFilesMetadata(meta.DbComparatorName, stripped));
        Assert.IsNull(GetString(cf, S(Key(6))!));
        Assert.AreEqual(S(Value(9, 0)), GetString(cf, S(Key(9))!));
    }
    // ---- MultiScan --------------------------------------------------------------------------------------

    private static List<string> Collect(MultiScan ms)
    {
        var all = new List<string>();
        while (ms.MoveNextRange())
        {
            var keys = new List<string>();
            for (; ms.Valid; ms.Next()) keys.Add(System.Text.Encoding.UTF8.GetString(ms.Key));
            all.Add(ms.RangeIndex + ":" + string.Join(",", keys));
        }
        return all;
    }

    [TestMethod]
    public void MultiScanVisitsEveryRange()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = OpenMerge(fs);
        for (int i = 0; i < 100; i++) db.Put(B("k" + i.ToString("D3", System.Globalization.CultureInfo.InvariantCulture)), B("v" + i));
        db.Flush();
        db.Delete(B("k011"));
        db.Put(B("k012"), B("new"));
        db.CompactRange(B("k050"), B("k099"));
        var args = new MultiScanArgs().Add(B("k010"), B("k014")).Add(B("k050"), B("k052")).Add(B("k098"), B("k200"));
        using (MultiScan ms = db.NewMultiScan(args))
        {
            CollectionAssert.AreEqual(new[] { "0:k010,k012,k013", "1:k050,k051", "2:k098,k099" }, Collect(ms));
            Assert.IsFalse(ms.MoveNextRange());
        }
        using (MultiScan ms = db.NewMultiScan(new MultiScanArgs { Reverse = true }.Add(B("k010"), B("k014")).Add(B("k097"), B("k099"))))
        {
            CollectionAssert.AreEqual(new[] { "0:k013,k012,k010", "1:k098,k097" }, Collect(ms));
        }
        // Mixed bounded / unbounded ranges (no prefetch) and the value accessor.
        using (MultiScan ms = db.NewMultiScan(new ReadOptions { IterateUpperBound = B("k001") }, new MultiScanArgs { MaxPrefetchSize = 1, IoCoalesceThreshold = 0 }
                   .Add(B("k012"), B("k013")).Add(B("k097"))))
        {
            Assert.IsTrue(ms.MoveNextRange());
            Assert.AreEqual("new", S(ms.Value.ToArray()));
            Assert.IsTrue(ms.MoveNextRangeAsync().AsTask().GetAwaiter().GetResult());
            int n = 0;
            for (; ms.Valid; ms.NextAsync().AsTask().GetAwaiter().GetResult()) n++;
            Assert.AreEqual(3, n);
            Assert.ThrowsExactly<InvalidOperationException>(() => ms.Next());
        }
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => db.NewMultiScan(new MultiScanArgs())).Code);
        Assert.AreEqual(StatusCode.InvalidArgument,
            Assert.ThrowsExactly<RocksDbException>(() => db.NewMultiScan(new MultiScanArgs().Add(B("b"), B("a")))).Code);
        Assert.AreEqual(StatusCode.InvalidArgument,
            Assert.ThrowsExactly<RocksDbException>(() => db.NewMultiScan(new MultiScanArgs { Reverse = true }.Add(B("a")))).Code);
    }
}
