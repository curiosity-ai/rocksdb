using System.Buffers;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Util;
using RocksDb.Managed.Utilities.Transactions;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>
/// Regression tests for the compaction / version / MANIFEST findings of the second correctness review
/// (write-conflict-boundary snapshots, IgnoreSnapshots, ReFitLevel guards, consistency checks, output validation,
/// oldest ancestor time, MANIFEST auto-tuning, db_paths, obsolete-file scans, round-robin cursors, trivial copy).
/// </summary>
[TestClass]
public sealed class CompactionReviewFixesTests
{
    private static readonly WriteOptions s_wo = new();

    private static readonly CompactRangeOptions s_force = new() { BottommostLevelCompaction = BottommostLevelCompaction.Force };

    // ---------------------------------------------------------------------------------------------------------
    // write-conflict-boundary snapshots (db_impl.cc GetSnapshotForWriteConflictBoundary, snapshot_impl.h)

    /// <summary>
    /// A SingleDelete + Put pair written after a transaction's snapshot must survive compaction so the transaction's
    /// conflict check still sees the write (CompactionIterator earliest_write_conflict_snapshot).
    /// </summary>
    [TestMethod]
    public void SingleDeletePairUnderPessimisticTxnSnapshotSurvivesCompaction()
    {
        MemoryFileSystem fs = NewFs();
        using TransactionDatabase db = TransactionDatabase.Open(DbPath, NewOptions(fs), new TransactionDbOptions(), NoHistoryCf());
        using Transaction txn = db.BeginTransaction(s_wo);
        txn.SetSnapshot();
        db.Put(B("k"), B("v"));
        db.SingleDelete(B("k"));
        PushOutOfMemtables(db.Database);
        var ex = Assert.ThrowsExactly<RocksDbException>(() => txn.Put(B("k"), B("txn")));
        Assert.AreEqual(StatusCode.Busy, ex.Code);
    }

    [TestMethod]
    public void SingleDeletePairUnderOptimisticTxnSnapshotSurvivesCompaction()
    {
        MemoryFileSystem fs = NewFs();
        using OptimisticTransactionDatabase db = OptimisticTransactionDatabase.Open(DbPath, NewOptions(fs), new OptimisticTransactionDbOptions(), NoHistoryCf());
        using Transaction txn = db.BeginTransaction(s_wo);
        txn.SetSnapshot();
        db.Database.Put(B("k"), B("v"));
        db.Database.SingleDelete(B("k"));
        PushOutOfMemtables(db.Database);
        // Optimistic validation only reads memtables (TryAgain without history), but its snapshot is a write-conflict
        // boundary too (TransactionBaseImpl::SetSnapshot): the SingleDelete and the (value-less) Put are kept.
        Assert.AreEqual(2, CountVersions(db.Database, "k"u8));
        txn.Dispose();
        db.Database.CompactRange(s_force, null, null);
        Assert.AreEqual(0, CountVersions(db.Database, "k"u8));
    }

    private static int CountVersions(RocksDatabase db, ReadOnlySpan<byte> key)
    {
        int entries = 0;
        using KeyVersionCollection versions = db.GetAllKeyVersions(null, null, int.MaxValue);
        foreach (KeyVersion v in versions)
        {
            if (v.UserKey.Span.SequenceEqual(key)) entries++;
        }
        return entries;
    }

    /// <summary>A plain snapshot is no write-conflict boundary: the pair is still compacted away (upstream behaviour).</summary>
    [TestMethod]
    public void SingleDeletePairUnderPlainSnapshotIsDropped()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions());
        db.Put(B("a"), B("a"));
        using Snapshot snap = db.GetSnapshot();
        db.Put(B("k"), B("v"));
        db.SingleDelete(B("k"));
        PushOutOfMemtables(db);
        Assert.AreEqual(0, CountVersions(db, "k"u8));
    }

    /// <summary>Flush + CompactRange(Force) to the bottommost level, with no memtable history left.</summary>
    private static void PushOutOfMemtables(RocksDatabase db)
    {
        db.Flush();
        // Push the flushed memtable out of the (1-byte) memtable history: the conflict check must read the table files.
        db.Put(B("other"), B("x"));
        db.Flush();
        db.CompactRange(s_force, null, null);
    }

    /// <summary>Column families with almost no memtable history (max_write_buffer_size_to_maintain = 1).</summary>
    private static ColumnFamilyDescriptor[] NoHistoryCf() =>
        [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions { MaxWriteBufferSizeToMaintain = 1 })];

    // ---------------------------------------------------------------------------------------------------------
    // CompactionFilter::IgnoreSnapshots() = false (compaction_job.cc SetupAndValidateCompactionFilter, builder.cc)

    private sealed class SnapshotAwareFilter : CompactionFilter
    {
        public override string Name => "SnapshotAwareFilter";

        public override bool IgnoreSnapshots => false;

        public override CompactionFilterDecision Filter(int level, ReadOnlySpan<byte> key, CompactionFilterValueType valueType,
            ReadOnlySpan<byte> existingValue, IBufferWriter<byte> newValue, IBufferWriter<byte> skipUntil) => CompactionFilterDecision.Keep;
    }

    private sealed class FlushFilterFactory(bool forFlush) : CompactionFilterFactory
    {
        public override string Name => "FlushFilterFactory";

        public override CompactionFilter? CreateCompactionFilter(in CompactionFilterContext context) => new SnapshotAwareFilter();

        public override bool ShouldFilterTableFileCreation(bool isFlush) => !isFlush || forFlush;
    }

    [TestMethod]
    public void CompactionFilterNotIgnoringSnapshotsFailsCompaction()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { CompactionFilter = new SnapshotAwareFilter() });
        db.Put(B("a"), B("1"));
        db.Flush();
        db.Put(B("b"), B("1"));
        db.Flush();
        var ex = Assert.ThrowsExactly<RocksDbException>(() => db.CompactRange(s_force, null, null));
        Assert.AreEqual(StatusCode.NotSupported, ex.Code);
        StringAssert.Contains(ex.Message, "CompactionFilter::IgnoreSnapshots() = false is not supported anymore.");
    }

    [TestMethod]
    public void CompactionFilterFactoryNotIgnoringSnapshotsFailsCompaction()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { CompactionFilterFactory = new FlushFilterFactory(false) });
        db.Put(B("a"), B("1"));
        db.Flush(); // the factory does not filter flushes
        var ex = Assert.ThrowsExactly<RocksDbException>(() => db.CompactRange(s_force, null, null));
        Assert.AreEqual(StatusCode.NotSupported, ex.Code);
    }

    [TestMethod]
    public void FlushFilterNotIgnoringSnapshotsFailsFlush()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { CompactionFilterFactory = new FlushFilterFactory(true) });
        db.Put(B("a"), B("1"));
        var ex = Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
        Assert.AreEqual(StatusCode.NotSupported, ex.Code);
    }

    // ---------------------------------------------------------------------------------------------------------
    // ReFitLevel / change_level (db_impl_compaction_flush.cc DBImpl::ReFitLevel)

    private static readonly ColumnFamilyOptions s_smallFiles = new()
    {
        DisableAutoCompactions = true,
        TargetFileSizeBase = 4 * 1024,
        Compression = RocksDb.Managed.Compression.CompressionType.None,
    };

    /// <summary>Writes <paramref name="n"/> keys and compacts them into several files of the bottommost level.</summary>
    private static void FillBottommost(RocksDatabase db, int n)
    {
        for (int i = 0; i < n; i++) db.Put(Key(i), Value(i, 0));
        db.Flush();
        db.CompactRange(s_force, null, null);
    }

    private static int[] FilesPerLevel(RocksDatabase db)
    {
        var r = new int[7];
        foreach (LiveFileMetadata f in db.GetLiveFilesMetadata()) r[f.Level]++;
        return r;
    }

    [TestMethod]
    public void ReFitLevelMovingSeveralFilesToL0IsAborted()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), s_smallFiles);
        FillBottommost(db, 500);
        int[] before = FilesPerLevel(db);
        Assert.IsTrue(before[^1] > 1, "files: " + string.Join(",", before));
        var ex = Assert.ThrowsExactly<RocksDbException>(() => db.CompactRange(new CompactRangeOptions { ChangeLevel = true, TargetLevel = 0 }, null, null));
        Assert.AreEqual(StatusCode.Aborted, ex.Code);
        StringAssert.Contains(ex.Message, "Moving more than 1 file from non-L0 to L0 is not allowed");
        CollectionAssert.AreEqual(before, FilesPerLevel(db));
        // Moving them up to L1 is fine.
        db.CompactRange(new CompactRangeOptions { ChangeLevel = true, TargetLevel = 1 }, null, null);
        Assert.AreEqual(before[^1], FilesPerLevel(db)[1]);
        for (int i = 0; i < 500; i++) CollectionAssert.AreEqual(Value(i, 0), db.Get(Key(i)));
    }

    /// <summary>
    /// While the refit's MANIFEST write has released the mutex, a concurrent CompactRange / CompactFiles cannot pick the
    /// moving files: manual compaction is disabled (Incomplete) and the files are registered as being compacted.
    /// </summary>
    [TestMethod]
    public void ReFitLevelBlocksConcurrentManualCompactionOfMovingFiles()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), s_smallFiles);
        FillBottommost(db, 500);
        List<string> files = new();
        foreach (LiveFileMetadata f in db.GetLiveFilesMetadata()) files.Add(f.Name);
        RocksDbException? compactRange = null, compactFiles = null;
        TestHooks.Set("DBImpl::ReFitLevel:PostRegisterCompaction", () =>
        {
            Task.Run(() =>
            {
                try
                {
                    db.CompactRange(s_force, null, null);
                }
                catch (RocksDbException e)
                {
                    compactRange = e;
                }
                try
                {
                    db.CompactFiles(files, 2);
                }
                catch (RocksDbException e)
                {
                    compactFiles = e;
                }
            }).Wait();
        });
        try
        {
            db.CompactRange(new CompactRangeOptions { ChangeLevel = true, TargetLevel = 1 }, null, null);
        }
        finally
        {
            TestHooks.Set("DBImpl::ReFitLevel:PostRegisterCompaction", null);
        }
        Assert.IsNotNull(compactRange);
        Assert.AreEqual(StatusCode.Incomplete, compactRange.Code);
        Assert.IsNotNull(compactFiles);
        Assert.AreEqual(StatusCode.Incomplete, compactFiles.Code);
        Assert.AreEqual(files.Count, FilesPerLevel(db)[1]);
        // Manual compaction works again afterwards and the DB is healthy.
        db.CompactRange(s_force, null, null);
        for (int i = 0; i < 500; i++) CollectionAssert.AreEqual(Value(i, 0), db.Get(Key(i)));
    }

    /// <summary>Refits racing plain CompactRange calls never corrupt the LSM or raise a background error.</summary>
    [TestMethod]
    public void ReFitLevelRacingCompactRangeKeepsDbConsistent()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), s_smallFiles);
        FillBottommost(db, 300);
        using var stop = new CancellationTokenSource();
        int compactions = 0;
        Task racer = Task.Run(() =>
        {
            int round = 1;
            while (!stop.IsCancellationRequested)
            {
                try
                {
                    db.Put(Key(round % 300), Value(round % 300, 1));
                    db.Flush();
                    db.CompactRange(s_force, null, null);
                    compactions++;
                }
                catch (RocksDbException e) when (e.Code is StatusCode.Incomplete or StatusCode.NotSupported)
                {
                    // paused by the refit / levels not empty: expected
                }
                round++;
            }
        });
        try
        {
            for (int i = 0; i < 40; i++)
            {
                try
                {
                    db.CompactRange(new CompactRangeOptions { ChangeLevel = true, TargetLevel = 1 + (i % 3) }, null, null);
                }
                catch (RocksDbException e) when (e.Code is StatusCode.Incomplete or StatusCode.NotSupported)
                {
                    // canceled by the racer's pause / levels not empty
                }
            }
        }
        finally
        {
            stop.Cancel();
            racer.Wait();
        }
        Assert.AreEqual("0", db.GetProperty("rocksdb.background-errors"));
        db.CompactRange(s_force, null, null);
        for (int i = 0; i < 300; i++)
        {
            byte[]? v = db.Get(Key(i));
            Assert.IsNotNull(v, "key " + i);
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // VersionBuilder::CheckConsistency + force_consistency_checks (db/version_builder.cc)

    /// <summary>
    /// Moves every L0 file of the default column family to <paramref name="level"/> with a raw MANIFEST edit (the files
    /// overlap, so the result is an inconsistent LSM unless the consistency checks reject it).
    /// </summary>
    internal static void MoveL0FilesTo(RocksDatabase db, int level)
    {
        ColumnFamilyData cfd = db.DefaultColumnFamily.Data;
        var edit = new VersionEdit { ColumnFamily = cfd.Id };
        foreach (FileMetaData f in cfd.Current.Storage.LevelFiles(0))
        {
            edit.DeleteFile(0, f.FileNumber);
            edit.AddFile(level, f.Clone());
        }
        db.Impl.Versions.LogAndApplyAsync(cfd, edit, synchronous: true, CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <summary>Two overlapping L0 files (keys a..c and b..d).</summary>
    internal static void WriteOverlappingL0Files(RocksDatabase db)
    {
        db.Put(B("a"), B("1"));
        db.Put(B("c"), B("1"));
        db.Flush();
        db.Put(B("b"), B("2"));
        db.Put(B("d"), B("2"));
        db.Flush();
    }

    [TestMethod]
    public void LiveEditWithOverlappingL1FilesFailsConsistencyCheck()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { DisableAutoCompactions = true });
        WriteOverlappingL0Files(db);
        var ex = Assert.ThrowsExactly<RocksDbException>(() => MoveL0FilesTo(db, 1));
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
        StringAssert.Contains(ex.Message, "force_consistency_checks: VersionBuilder: L1 has overlapping ranges");
        // Nothing was written: the DB reopens with its two L0 files.
        db.Dispose();
        using RocksDatabase again = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { DisableAutoCompactions = true });
        Assert.AreEqual("2", GetString(again, "d"));
        Assert.AreEqual(2, CountLevelFiles(again, 0));
    }

    [TestMethod]
    public void ManifestWithOverlappingL1FilesFailsToOpen()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { DisableAutoCompactions = true, ForceConsistencyChecks = false }))
        {
            WriteOverlappingL0Files(db);
            MoveL0FilesTo(db, 1); // accepted without the checks
        }
        var ex = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions()));
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
        StringAssert.Contains(ex.Message, "L1 has overlapping ranges");
        // Like upstream, the checks can be turned off.
        using RocksDatabase unchecked_ = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { DisableAutoCompactions = true, ForceConsistencyChecks = false });
        Assert.AreEqual(2, CountLevelFiles(unchecked_, 1));
    }

    private static int CountLevelFiles(RocksDatabase db, int level)
    {
        int n = 0;
        foreach (LiveFileMetadata f in db.GetLiveFilesMetadata()) n += f.Level == level ? 1 : 0;
        return n;
    }

    [TestMethod]
    public void ConsistencyCheckRejectsUnsortedAndSameEpochOverlappingL0()
    {
        var icmp = new InternalKeyComparator(BytewiseComparator.Instance);
        FileMetaData F(ulong number, string smallest, string largest, ulong epoch, ulong seq) => new()
        {
            FileNumber = number,
            Smallest = new InternalKey(B(smallest), seq, EntryType.Value).Encoded.ToArray(),
            Largest = new InternalKey(B(largest), seq, EntryType.Value).Encoded.ToArray(),
            EpochNumber = epoch,
            SmallestSeqno = seq,
            LargestSeqno = seq,
        };
        // Same epoch, overlapping (e.g. two outputs of one atomic flush that overlap).
        var sameEpoch = new[] { new[] { F(2, "a", "c", 5, 10), F(1, "b", "d", 5, 9) } };
        var ex = Assert.ThrowsExactly<RocksDbException>(() => VersionStorageInfo.CheckConsistency(icmp, sameEpoch, [], forceConsistencyChecks: true));
        StringAssert.Contains(ex.Message, "L0 files of same epoch number but overlapping range");
        // Older epoch first.
        var unsorted = new[] { new[] { F(1, "a", "c", 4, 9), F(2, "x", "z", 5, 10) } };
        ex = Assert.ThrowsExactly<RocksDbException>(() => VersionStorageInfo.CheckConsistency(icmp, unsorted, [], forceConsistencyChecks: true));
        StringAssert.Contains(ex.Message, "L0 files are not sorted properly");
        // Levels 1+ must be sorted by smallest key.
        var l1 = new[] { Array.Empty<FileMetaData>(), new[] { F(2, "m", "n", 1, 1), F(1, "a", "b", 1, 1) } };
        ex = Assert.ThrowsExactly<RocksDbException>(() => VersionStorageInfo.CheckConsistency(icmp, l1, [], forceConsistencyChecks: true));
        StringAssert.Contains(ex.Message, "L1 files are not sorted properly");
        // A sound version and disabled checks pass.
        VersionStorageInfo.CheckConsistency(icmp, new[] { new[] { F(2, "x", "z", 5, 10), F(1, "a", "c", 4, 9) } }, [], forceConsistencyChecks: true);
        VersionStorageInfo.CheckConsistency(icmp, l1, [], forceConsistencyChecks: false);
    }
}
