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

    // ---------------------------------------------------------------------------------------------------------
    // OutputValidator + paranoid_file_checks / verify_output_flags (output_validator.cc, CompactionJob::VerifyOutputFiles)

    /// <summary>A bytewise comparator whose order can be reversed after data was written (a comparator bug).</summary>
    private sealed class FlippableComparator : Comparator
    {
        public volatile bool Reversed;

        public override string Name => "test.Flippable";

        public override int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => Reversed ? b.SequenceCompareTo(a) : a.SequenceCompareTo(b);
    }

    [TestMethod]
    public void CompactionSeesOutOfOrderKeys()
    {
        MemoryFileSystem fs = NewFs();
        var cmp = new FlippableComparator();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { Comparator = cmp, DisableAutoCompactions = true });
        for (int i = 0; i < 20; i++) db.Put(Key(i), Value(i, 0));
        db.Flush();
        for (int i = 10; i < 30; i++) db.Put(Key(i), Value(i, 1));
        db.Flush();
        // The order changes once the compaction is picked: its input files are read in their stored order.
        TestHooks.Set("DBImpl::RunCompaction:Start", () => cmp.Reversed = true);
        RocksDbException ex;
        try
        {
            ex = Assert.ThrowsExactly<RocksDbException>(() => db.CompactRange(s_force, null, null));
        }
        finally
        {
            TestHooks.Set("DBImpl::RunCompaction:Start", null);
            cmp.Reversed = false;
        }
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
        StringAssert.Contains(ex.Message, "Compaction sees out-of-order keys.");
    }

    /// <summary>Fills two overlapping L0 files on a real directory (so the test can corrupt an output on disk).</summary>
    private static void FillTwoL0Files(RocksDatabase db)
    {
        for (int i = 0; i < 300; i++) db.Put(Key(i), Value(i, 0));
        db.Flush();
        for (int i = 150; i < 450; i++) db.Put(Key(i), Value(i, 1));
        db.Flush();
    }

    /// <summary>Flips a byte of the first data block of every table file not in <paramref name="keep"/>.</summary>
    private static void CorruptNewTableFiles(string dir, HashSet<string> keep)
    {
        foreach (string f in Directory.GetFiles(dir, "*.sst"))
        {
            if (keep.Contains(f)) continue;
            using var s = new FileStream(f, FileMode.Open, FileAccess.ReadWrite);
            s.Position = 20;
            int b = s.ReadByte();
            s.Position = 20;
            s.WriteByte((byte)(b ^ 0x5a));
        }
    }

    [TestMethod]
    [DataRow(true, VerifyOutputFlags.None)]
    [DataRow(false, VerifyOutputFlags.Iteration | VerifyOutputFlags.EnableForLocalCompaction)]
    [DataRow(false, VerifyOutputFlags.BlockChecksum | VerifyOutputFlags.EnableForLocalCompaction)]
    [DataRow(false, VerifyOutputFlags.FileChecksum | VerifyOutputFlags.EnableForLocalCompaction)]
    [DataRow(false, VerifyOutputFlags.None)]
    [DataRow(false, VerifyOutputFlags.All & ~VerifyOutputFlags.EnableForLocalCompaction)]
    public void VerifyOutputFilesDetectsCorruptedOutput(bool paranoid, VerifyOutputFlags flags)
    {
        string dir = Path.Combine(Path.GetTempPath(), "rocksdb-managed-verify-output-" + Guid.NewGuid().ToString("N"));
        try
        {
            var dbo = new DbOptions { CreateIfMissing = true, FileChecksumGenFactory = FileChecksumGenCrc32cFactory.Instance };
            var cfo = new ColumnFamilyOptions
            {
                DisableAutoCompactions = true,
                ParanoidFileChecks = paranoid,
                VerifyOutputFlags = flags,
                Compression = RocksDb.Managed.Compression.CompressionType.None,
            };
            using RocksDatabase db = RocksDatabase.Open(dir, dbo, cfo);
            FillTwoL0Files(db);
            var inputs = new HashSet<string>(Directory.GetFiles(dir, "*.sst"));
            TestHooks.Set("CompactionJob::VerifyOutputFiles:Start", () => CorruptNewTableFiles(dir, inputs));
            bool verifies = paranoid || (flags & VerifyOutputFlags.EnableForLocalCompaction) != 0 && (flags & (VerifyOutputFlags.Iteration
                | VerifyOutputFlags.BlockChecksum | VerifyOutputFlags.FileChecksum)) != 0;
            try
            {
                if (verifies)
                {
                    var ex = Assert.ThrowsExactly<RocksDbException>(() => db.CompactRange());
                    Assert.AreEqual(StatusCode.Corruption, ex.Code, ex.Message);
                }
                else
                {
                    db.CompactRange(); // one L0 -> Lmax compaction: the corruption goes unnoticed until the data is read
                }
            }
            finally
            {
                TestHooks.Set("CompactionJob::VerifyOutputFiles:Start", null);
            }
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch (IOException) { }
        }
    }

    /// <summary>Ports of VerifyIterationWithoutParanoidFileChecks / VerifyAllOutputFlagsWithoutParanoidFileChecks.</summary>
    [TestMethod]
    [DataRow(VerifyOutputFlags.Iteration | VerifyOutputFlags.EnableForLocalCompaction)]
    [DataRow(VerifyOutputFlags.BlockChecksum | VerifyOutputFlags.Iteration | VerifyOutputFlags.FileChecksum | VerifyOutputFlags.EnableForLocalCompaction)]
    public void VerifyOutputFlagsWithoutParanoidFileChecks(VerifyOutputFlags flags)
    {
        MemoryFileSystem fs = NewFs();
        DbOptions dbo = NewOptions(fs);
        dbo.FileChecksumGenFactory = FileChecksumGenCrc32cFactory.Instance;
        using RocksDatabase db = RocksDatabase.Open(DbPath, dbo, new ColumnFamilyOptions { DisableAutoCompactions = true, VerifyOutputFlags = flags, TargetFileSizeBase = 4096 });
        FillTwoL0Files(db);
        db.CompactRange(s_force, null, null);
        for (int i = 0; i < 450; i++) CollectionAssert.AreEqual(Value(i, i >= 150 ? 1 : 0), db.Get(Key(i)));
    }

    [TestMethod]
    public void VerifyOutputFlagsPersistedAndMutable()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions());
        db.SetOptions(new Dictionary<string, string> { ["verify_output_flags"] = "1026" });
        Assert.AreEqual(VerifyOutputFlags.Iteration | VerifyOutputFlags.EnableForLocalCompaction, db.DefaultColumnFamily.Options.VerifyOutputFlags);
        var cfo = new ColumnFamilyOptions();
        cfo.SetOption("verify_output_flags", "4294967295", preserveUnknown: false);
        Assert.AreEqual(VerifyOutputFlags.All, cfo.VerifyOutputFlags);
    }

    // ---------------------------------------------------------------------------------------------------------
    // oldest_ancestor_time of compaction outputs (compaction_job.cc OpenCompactionOutputFile / FinishCompactionOutputFile)

    /// <summary>Rewrites the oldest ancestor time of every file at <paramref name="level"/> whose keys are below <paramref name="limit"/>.</summary>
    private static void SetOldestAncestorTime(RocksDatabase db, int level, byte[] limit, ulong time)
    {
        ColumnFamilyData cfd = db.DefaultColumnFamily.Data;
        var edit = new VersionEdit { ColumnFamily = cfd.Id };
        foreach (FileMetaData f in cfd.Current.Storage.LevelFiles(level))
        {
            if (DbFormat.ExtractUserKey(f.Largest).SequenceCompareTo(limit) >= 0) continue;
            FileMetaData c = f.Clone();
            c.OldestAncestorTime = time;
            edit.DeleteFile(level, f.FileNumber);
            edit.AddFile(level, c);
        }
        db.Impl.Versions.LogAndApplyAsync(cfd, edit, synchronous: true, CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <summary>
    /// Each output of a multi-output compaction gets the oldest ancestor time of the inputs overlapping its own key
    /// range, not the whole compaction's minimum: outputs made only of new data are not TTL-expired.
    /// </summary>
    [TestMethod]
    public void MultiOutputCompactionNarrowsOldestAncestorTimePerOutput()
    {
        MemoryFileSystem fs = NewFs();
        var cfo = new ColumnFamilyOptions
        {
            DisableAutoCompactions = true,
            TargetFileSizeBase = 4 * 1024,
            Compression = RocksDb.Managed.Compression.CompressionType.None,
            Ttl = 10_000,
        };
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        ulong now = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        ulong old = now - 100_000; // older than the TTL
        for (int i = 0; i < 500; i++) db.Put(Key(i), Value(i, 0));
        db.Flush();
        db.CompactRange(); // several L6 files over keys 0..499
        SetOldestAncestorTime(db, 6, Key(1000), old); // all files with keys 0..499
        for (int i = 500; i < 1000; i++) db.Put(Key(i), Value(i, 0));
        db.Flush();
        db.CompactRange(s_force, null, null); // rewrites everything into many outputs
        int oldOutputs = 0, newOutputs = 0;
        foreach (LiveFileMetadata f in db.GetLiveFilesMetadata())
        {
            Assert.AreEqual(6, f.Level);
            byte[] largestUser = f.LargestKey;
            byte[] smallestUser = f.SmallestKey;
            if (largestUser.AsSpan().SequenceCompareTo(Key(500)) < 0)
            {
                Assert.AreEqual(old, f.OldestAncestorTime, "file " + f.Name);
                oldOutputs++;
            }
            else if (smallestUser.AsSpan().SequenceCompareTo(Key(500)) >= 0)
            {
                Assert.IsTrue(f.OldestAncestorTime >= now - 60, "file " + f.Name + " time " + f.OldestAncestorTime);
                newOutputs++;
            }
        }
        Assert.IsTrue(oldOutputs > 0 && newOutputs > 0, oldOutputs + " / " + newOutputs);
        // Moved above the last level, only the outputs holding old data are TTL-expired (ComputeExpiredTtlFiles).
        db.CompactRange(new CompactRangeOptions { ChangeLevel = true, TargetLevel = 5 }, null, null);
        int expired = db.DefaultColumnFamily.Data.Current.Storage.ExpiredTtlFiles.Count;
        Assert.IsTrue(expired >= oldOutputs && expired < oldOutputs + newOutputs, "expired " + expired + " old " + oldOutputs + " new " + newOutputs);
    }

    // ---------------------------------------------------------------------------------------------------------
    // MANIFEST size auto-tuning (version_set.cc TuneMaxManifestFileSize, ProcessManifestWrites, WriteCurrentStateToManifest)

    private static ulong ManifestNumber(RocksDatabase db) => db.Impl.Versions.ManifestFileNumber;

    private static long ManifestSize(IFileSystem fs, RocksDatabase db) => fs.GetFileSize(FileNames.DescriptorFileName(DbPath, ManifestNumber(db)));

    /// <summary>Decodes every record of the current MANIFEST.</summary>
    internal static List<VersionEdit> ReadManifest(IFileSystem fs, string dbPath, ulong number)
    {
        var edits = new List<VersionEdit>();
        using ManifestLogReader reader = ManifestLogReader.Open(fs, FileNames.DescriptorFileName(dbPath, number));
        while (reader.TryReadRecord(out ReadOnlySpan<byte> record)) edits.Add(VersionEdit.DecodeFrom(record));
        return edits;
    }

    [TestMethod]
    public void NewManifestRecordsLastCompactedManifestFileSize()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions());
        List<VersionEdit> edits = ReadManifest(fs, DbPath, ManifestNumber(db));
        int index = edits.FindIndex(e => e.LastCompactedManifestFileSize is not null);
        Assert.IsTrue(index > 0, "no kLastCompactedManifestFileSize record");
        // Last record of the snapshot (only the batch's next-file-number edit and the edits after it follow).
        Assert.IsTrue(edits[index].LastCompactedManifestFileSize > 0UL && edits[index].LastCompactedManifestFileSize < (ulong)ManifestSize(fs, db) + 16);
        // max(max_manifest_file_size = 1 GiB, ~6 x a few hundred bytes).
        Assert.AreEqual(1024UL * 1024 * 1024, db.Impl.Versions.TunedMaxManifestFileSize);
    }

    /// <summary>
    /// With a tiny max_manifest_file_size, the MANIFEST is rewritten only once it exceeds the compacted size by
    /// max_manifest_space_amp_pct (0 = on every write).
    /// </summary>
    [TestMethod]
    [DataRow(0)]
    [DataRow(100)]
    [DataRow(500)]
    public void ManifestRotationFollowsSpaceAmp(int pct)
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.MaxManifestFileSize = 1;
        o.MaxManifestSpaceAmpPct = pct;
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, new ColumnFamilyOptions { DisableAutoCompactions = true });
        int rotations = 0;
        for (int i = 0; i < 40; i++)
        {
            ulong before = ManifestNumber(db);
            long size = ManifestSize(fs, db);
            ulong limit = db.Impl.Versions.TunedMaxManifestFileSize;
            db.Put(Key(i), Value(i, 0));
            db.Flush();
            bool rotated = ManifestNumber(db) != before;
            // ProcessManifestWrites: rotate when prev_manifest_file_size >= the tuned limit (background batch).
            Assert.AreEqual((ulong)size >= limit, rotated, "flush " + i + " size " + size + " limit " + limit);
            if (rotated) rotations++;
        }
        if (pct == 0) Assert.AreEqual(40, rotations);
        else Assert.IsTrue(rotations > 0 && rotations < 40, "rotations " + rotations);
    }

    /// <summary>A foreground batch (ingestion) may exceed the limit by 25% before it rotates; a flush does not.</summary>
    [TestMethod]
    public void ForegroundManifestWriteGetsRelaxedLimit()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.MaxManifestSpaceAmpPct = 0;
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, new ColumnFamilyOptions { DisableAutoCompactions = true });
        for (int i = 0; i < 30; i++)
        {
            db.Put(Key(i), Value(i, 0));
            db.Flush();
        }
        long size = ManifestSize(fs, db);
        // size is in [limit, 1.25 * limit) and above the last compacted size.
        db.SetDBOptions(new Dictionary<string, string> { ["max_manifest_file_size"] = (size * 9 / 10).ToString(System.Globalization.CultureInfo.InvariantCulture) });
        Assert.IsTrue(db.Impl.Versions.TunedMaxManifestFileSize <= (ulong)size);
        ulong before = ManifestNumber(db);
        string sst = "/ingest.sst";
        using (var w = new SstFileWriter(new ColumnFamilyOptions(), fs))
        {
            w.Open(sst);
            w.Put(Key(1000), B("x"));
            w.Finish();
        }
        db.IngestExternalFiles([sst], new IngestExternalFileOptions());
        Assert.AreEqual(before, ManifestNumber(db), "the foreground write rotated the MANIFEST");
        db.Put(Key(2000), B("y"));
        db.Flush();
        Assert.AreNotEqual(before, ManifestNumber(db), "the background write did not rotate the MANIFEST");
    }

    [TestMethod]
    public void ManifestSpaceAmpOptionsPersistedAndMutable()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions());
        Assert.AreEqual(500, db.Impl.Options.MaxManifestSpaceAmpPct);
        db.SetDBOptions(new Dictionary<string, string> { ["max_manifest_space_amp_pct"] = "7", ["verify_manifest_content_on_close"] = "true" });
        Assert.AreEqual(7, db.Impl.Options.MaxManifestSpaceAmpPct);
        Assert.IsTrue(db.Impl.Options.VerifyManifestContentOnClose);
    }

    /// <summary>verify_manifest_content_on_close: a corrupt MANIFEST is detected on close and rewritten.</summary>
    [TestMethod]
    public void VerifyManifestContentOnCloseRewritesCorruptManifest()
    {
        string dir = Path.Combine(Path.GetTempPath(), "rocksdb-managed-verify-manifest-" + Guid.NewGuid().ToString("N"));
        try
        {
            var stats = new Statistics();
            var o = new DbOptions { CreateIfMissing = true, VerifyManifestContentOnClose = true, Statistics = stats };
            ulong corrupted;
            using (RocksDatabase db = RocksDatabase.Open(dir, o, new ColumnFamilyOptions()))
            {
                for (int i = 0; i < 5; i++)
                {
                    db.Put(Key(i), Value(i, 0));
                    db.Flush();
                }
                corrupted = db.Impl.Versions.ManifestFileNumber;
                // Flip a byte inside a record payload of the (live) MANIFEST.
                using var s = new FileStream(FileNames.DescriptorFileName(dir, corrupted), FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
                s.Position = s.Length / 2;
                int b = s.ReadByte();
                s.Position = s.Length / 2;
                s.WriteByte((byte)(b ^ 0xff));
            }
            Assert.AreEqual(1UL, stats.GetTickerCount(Ticker.ManifestValidationFailureCount));
            using RocksDatabase again = RocksDatabase.Open(dir, new DbOptions(), new ColumnFamilyOptions());
            Assert.AreNotEqual(corrupted, again.Impl.Versions.ManifestFileNumber);
            for (int i = 0; i < 5; i++) CollectionAssert.AreEqual(Value(i, 0), again.Get(Key(i)));
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch (IOException) { }
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // db_paths placement (LevelCompactionBuilder::GetPathId, UniversalCompactionBuilder::GetPathId, target_path_id)

    [TestMethod]
    public void LevelGetPathIdPlacesLevelsByPathTargetSize()
    {
        var o = new ColumnFamilyOptions { MaxBytesForLevelBase = 256 * 1024, MaxBytesForLevelMultiplier = 10, LevelCompactionDynamicLevelBytes = false };
        var picker = new LevelCompactionPicker(o, new InternalKeyComparator(BytewiseComparator.Instance))
        {
            PathTargetSizes = [500 * 1024, 5 * 1024 * 1024, ulong.MaxValue],
        };
        uint[] expected = [0, 1, 1, 2, 2, 2, 2];
        for (int level = 0; level < expected.Length; level++) Assert.AreEqual(expected[level], picker.GetPathId(level), "level " + level);
        picker.PathTargetSizes = [ulong.MaxValue];
        Assert.AreEqual(0U, picker.GetPathId(6));
    }

    [TestMethod]
    public void UniversalGetPathIdPlacesFilesByPathTargetSize()
    {
        var o = new ColumnFamilyOptions { CompactionStyle = CompactionStyle.Universal };
        var picker = new UniversalCompactionPicker(o, new InternalKeyComparator(BytewiseComparator.Instance)) { PathTargetSizes = [1000, 10_000, ulong.MaxValue] };
        Assert.AreEqual(0U, picker.GetUniversalPathId(500));
        Assert.AreEqual(1U, picker.GetUniversalPathId(900));
        Assert.AreEqual(2U, picker.GetUniversalPathId(20_000));
    }

    /// <summary>Flushes go to db_paths[0], CompactRange(target_path_id) outputs to db_paths[target_path_id].</summary>
    [TestMethod]
    public void CompactRangeTargetPathIdPlacesOutputs()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.DbPaths.Add(("/p0", 1UL << 40));
        o.DbPaths.Add(("/p1", 1UL << 40));
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, new ColumnFamilyOptions { DisableAutoCompactions = true });
        for (int i = 0; i < 100; i++) db.Put(Key(i), Value(i, 0));
        db.Flush();
        foreach (LiveFileMetadata f in db.GetLiveFilesMetadata()) Assert.AreEqual("/p0", f.Directory.TrimEnd('/'));
        db.CompactRange(new CompactRangeOptions { TargetPathId = 1, BottommostLevelCompaction = BottommostLevelCompaction.Force }, null, null);
        Assert.IsTrue(db.GetLiveFilesMetadata().Count > 0);
        foreach (LiveFileMetadata f in db.GetLiveFilesMetadata()) Assert.AreEqual("/p1", f.Directory.TrimEnd('/'));
        Assert.IsFalse(fs.GetChildren("/p0").Any(n => n.EndsWith(".sst", StringComparison.Ordinal)));
        db.Dispose();
        using RocksDatabase again = RocksDatabase.Open(DbPath, o, new ColumnFamilyOptions());
        for (int i = 0; i < 100; i++) CollectionAssert.AreEqual(Value(i, 0), again.Get(Key(i)));
        var ex = Assert.ThrowsExactly<RocksDbException>(() => again.CompactRange(new CompactRangeOptions { TargetPathId = 2 }, null, null));
        Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
    }

    // ---------------------------------------------------------------------------------------------------------
    // periodic full scan of obsolete files (db_impl_files.cc FindObsoleteFiles, delete_obsolete_files_period_micros)

    [TestMethod]
    public void PeriodicFullScanDeletesOrphanTableFiles()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { DisableAutoCompactions = true });
        const string orphan = DbPath + "/009999.sst";
        using (IWritableFile f = fs.CreateWritable(orphan)) f.Append("not a table"u8);
        db.Put(B("a"), B("1"));
        db.Flush(); // default period (6 h): no full scan, the orphan stays
        Assert.IsTrue(fs.FileExists(orphan));
        db.SetDBOptions(new Dictionary<string, string> { ["delete_obsolete_files_period_micros"] = "0" });
        db.Put(B("b"), B("1"));
        db.Flush(); // period 0: every purge scans the directories
        Assert.IsFalse(fs.FileExists(orphan));
        // Live files are kept.
        Assert.AreEqual("1", GetString(db, "a"));
        Assert.AreEqual("1", GetString(db, "b"));
    }

    // ---------------------------------------------------------------------------------------------------------
    // FIFO trivial copy on temperature change (CopyTrivialCompactionFiles, allow_trivial_copy_when_change_temperature)

    private static byte[] ReadAll(IFileSystem fs, string path)
    {
        using ISequentialFile f = fs.OpenSequential(path);
        var ms = new MemoryStream();
        var buf = new byte[4096];
        int n;
        while ((n = f.Read(buf)) > 0) ms.Write(buf, 0, n);
        return ms.ToArray();
    }

    [TestMethod]
    [DataRow(true)]
    [DataRow(false)]
    public void FifoTemperatureChangeTrivialCopy(bool allowTrivialCopy)
    {
        MemoryFileSystem fs = NewFs();
        var cfo = new ColumnFamilyOptions
        {
            CompactionStyle = CompactionStyle.Fifo,
            NumLevels = 1,
            DisableAutoCompactions = true,
        };
        cfo.CompactionOptionsFifo.FileTemperatureAgeThresholds = [new FileTemperatureAge(Temperature.Cold, 0)];
        cfo.CompactionOptionsFifo.AllowTrivialCopyWhenChangeTemperature = allowTrivialCopy;
        cfo.CompactionOptionsFifo.TrivialCopyBufferSize = 100; // several reads per file
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        for (int i = 0; i < 50; i++) db.Put(Key(i), Value(i, 0));
        db.Flush();
        LiveFileMetadata before = db.GetLiveFilesMetadata()[0];
        byte[] original = ReadAll(fs, before.Directory.TrimEnd('/') + "/" + before.Name.TrimStart('/'));
        db.SetOptions(new Dictionary<string, string> { ["disable_auto_compactions"] = "false" });
        db.Put(Key(100), B("x"));
        db.Flush();
        db.WaitForCompact();
        LiveFileMetadata? after = db.GetLiveFilesMetadata().Find(f => f.SmallestKey.AsSpan().SequenceEqual(before.SmallestKey) && f.Temperature == Temperature.Cold);
        Assert.IsNotNull(after, "the old file was not moved to the cold temperature");
        Assert.AreNotEqual(before.FileNumber, after.FileNumber);
        byte[] copy = ReadAll(fs, after.Directory.TrimEnd('/') + "/" + after.Name.TrimStart('/'));
        if (allowTrivialCopy) CollectionAssert.AreEqual(original, copy, "a trivial copy is byte-identical");
        else CollectionAssert.AreNotEqual(original, copy, "a rewrite has a new file number in its properties");
        for (int i = 0; i < 50; i++) CollectionAssert.AreEqual(Value(i, 0), db.Get(Key(i)));
        db.Dispose();
        using RocksDatabase again = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        Assert.AreEqual(Temperature.Cold, again.GetLiveFilesMetadata().Find(f => f.FileNumber == after.FileNumber)!.Temperature);
        CollectionAssert.AreEqual(Value(7, 0), again.Get(Key(7)));
    }
}
