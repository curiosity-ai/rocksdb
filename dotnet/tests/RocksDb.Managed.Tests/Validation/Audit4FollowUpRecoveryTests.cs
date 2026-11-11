using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Tests.Db;
using RocksDb.Managed.Wal;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4 follow-up (recovery): WAL replay of refused batches, WAL truncation after a failed memtable insert, error
/// handling, obsolete files, version application, repair, secondary instances
/// (db/db_impl/db_impl_open.cc, db/db_impl/db_impl_write.cc, db/error_handler.cc, db/db_impl/db_impl_files.cc,
/// db/version_builder.cc, db/repair.cc, db/db_impl/db_impl_secondary.cc, db/memtable_list.cc).
/// </summary>
[TestClass]
public sealed class Audit4FollowUpRecoveryTests
{
    private const string DbPath = "/audit4fr";

    private static MemoryFileSystem NewDbFs()
    {
        var fs = new MemoryFileSystem();
        fs.CreateDirectoryIfMissing(DbPath);
        fs.SyncDirectory("/");
        return fs;
    }

    private static List<ulong> WalNumbers(MemoryFileSystem fs)
    {
        var r = new List<ulong>();
        foreach (string c in fs.GetChildren(DbPath))
        {
            if (FileNames.TryParseFileName(c, out ulong n, out FileType t) && t == FileType.WalFile) r.Add(n);
        }
        r.Sort();
        return r;
    }

    private static int WalRecordCount(MemoryFileSystem fs)
    {
        int records = 0;
        foreach (ulong n in WalNumbers(fs))
        {
            using var reader = new LogReader(fs.OpenSequential(FileNames.LogFileName(DbPath, n)), null, checksum: true, n);
            while (reader.TryReadRecord(out _)) records++;
        }
        return records;
    }

    private static void WriteRefusedBatch(RocksDatabase db)
    {
        var ex = Assert.ThrowsExactly<RocksDbException>(() =>
        {
            using WriteBatch b = db.CreateWriteBatch();
            b.Put(B("x"), B("1")).DeleteRange(B("c"), B("a")).Put(B("y"), B("1"));
            db.Write(b);
        });
        Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
    }

    [TestMethod]
    public void RecoveryContextFollowsTheErrorThatBecomesTheBackgroundError()
    {
        // ErrorHandler::CheckAndSetRecoveryAndBGError -> UpdateRecoveryContext(context, replace_existing_context =
        // bg_err.severity() > bg_error_.severity()): the flush reason of the recovery (kErrorRecoveryRetryFlush for a
        // retryable no-WAL flush error, else kErrorRecovery = flush every column family) belongs to the error that is
        // the background error. The port kept kErrorRecoveryRetryFlush after a hard error replaced the soft one, and
        // switched to it when a soft error arrived while a hard error was set.
        var fs = new RocksDb.Managed.Tests.Db.FaultInjectionFileSystem(NewDbFs());
        using var gate = new ManualResetEventSlim(false);
        TestHooks.Set("RecoverFromRetryableBGIOError:BeforeResume", () => gate.Wait(TimeSpan.FromSeconds(20)));
        try
        {
            using RocksDatabase db = RocksDatabase.Open(DbPath, new DbOptions { CreateIfMissing = true, FileSystem = fs, BgErrorResumeRetryInterval = 20_000 });
            db.Put(B("a"), B("1"));
            fs.FailMatching(".sst", RocksDb.Managed.Tests.Db.FaultOps.Append, 1, RocksDb.Managed.Tests.Db.InjectedFaultKind.Retryable);
            RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
            Assert.AreEqual(ErrorSeverity.SoftError, ex.Severity);
            ErrorHandler handler = db.Impl.ErrorHandler;
            Assert.IsTrue(handler.RetryFlushOnly);
            // A second soft error of the same severity does not replace the context.
            handler.SetBgError(new RocksDbException(StatusCode.IOError, "IO error: injected") { Retryable = true }, BackgroundErrorReason.FlushNoWal, walRelated: false);
            Assert.IsTrue(handler.RetryFlushOnly);
            // A hard error (retryable flush error with a non-empty WAL) becomes the background error: its context
            // (kErrorRecovery) replaces the retry-flush one.
            handler.SetBgError(new RocksDbException(StatusCode.IOError, "IO error: injected") { Retryable = true }, BackgroundErrorReason.Flush, walRelated: false);
            Assert.AreEqual(ErrorSeverity.HardError, db.BackgroundError!.Severity);
            Assert.IsFalse(handler.RetryFlushOnly);
            // A later, less severe no-WAL flush error leaves the hard error's context alone.
            handler.SetBgError(new RocksDbException(StatusCode.IOError, "IO error: injected") { Retryable = true }, BackgroundErrorReason.FlushNoWal, walRelated: false);
            Assert.AreEqual(ErrorSeverity.HardError, db.BackgroundError!.Severity);
            Assert.IsFalse(handler.RetryFlushOnly);
            gate.Set();
            Assert.IsTrue(SpinWait.SpinUntil(() => db.BackgroundError is null, TimeSpan.FromSeconds(20)), "recovery did not finish");
            db.Put(B("b"), B("2"));
            db.Flush();
            Assert.AreEqual("1", GetString(db, "a"));
        }
        finally
        {
            gate.Set();
            TestHooks.ClearAll();
        }
    }

    [TestMethod]
    public void WalWriteErrorIsNotABackgroundErrorWithoutParanoidChecks()
    {
        // S13. DBImpl::WALIOStatusCheck: "if ((paranoid_checks && !io_status.ok() && !IsBusy() && !IsIncomplete()) ||
        // io_status.IsIOFenced()) { ... SetBGError(...) } else { logs_.back().writer->file()->reset_seen_error(); }".
        // Without paranoid_checks a failed WAL write only fails that write. The port always reported it to the error
        // handler, where a retryable error became a hard background error (writes stopped, automatic recovery).
        var fs = new RocksDb.Managed.Tests.Db.FaultInjectionFileSystem(NewDbFs());
        using RocksDatabase db = RocksDatabase.Open(DbPath, new DbOptions { CreateIfMissing = true, FileSystem = fs, ParanoidChecks = false });
        db.Put(B("a"), B("1"));
        fs.FailMatching(".log", RocksDb.Managed.Tests.Db.FaultOps.Append, 1, RocksDb.Managed.Tests.Db.InjectedFaultKind.Retryable);
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.Put(B("lost"), B("x")));
        Assert.AreEqual(StatusCode.IOError, ex.Code);
        Assert.IsNull(db.BackgroundError);
        Assert.AreEqual("0", db.GetProperty(DbProperties.BackgroundErrors));
        db.Put(B("b"), B("2")); // the WAL stays writable (reset_seen_error)
        Assert.AreEqual("2", GetString(db, "b"));
    }

    [TestMethod]
    public void WalWriterRefusesToSyncAfterAFailedSync()
    {
        // S75. WritableFileWriter::seen_error_ (file/writable_file_writer.cc): after a failed Append / Sync, later
        // Append / Sync calls return IOError("Writer has previous error.") until reset_seen_error(). A retried fsync can
        // succeed although the kernel dropped the dirty pages, so without this a WAL whose sync failed could later be
        // reported (and recorded in the MANIFEST) as synced.
        var fs = new RocksDb.Managed.Tests.Db.FaultInjectionFileSystem(NewDbFs());
        string path = FileNames.LogFileName(DbPath, 7);
        using var log = new LogWriter(fs.CreateWritable(path), 7, recycleLogFiles: false);
        log.AddRecord(B("one"));
        fs.FailMatching(".log", RocksDb.Managed.Tests.Db.FaultOps.Sync, 1, RocksDb.Managed.Tests.Db.InjectedFaultKind.IOError);
        Assert.ThrowsExactly<RocksDbException>(() => log.Sync());
        Assert.IsTrue(log.SeenError);
        RocksDbException again = Assert.ThrowsExactly<RocksDbException>(() => log.Sync()); // the injected fault is used up
        Assert.AreEqual(StatusCode.IOError, again.Code);
        StringAssert.Contains(again.Message, "Writer has previous error");
        RocksDbException append = Assert.ThrowsExactly<RocksDbException>(() => log.AddRecord(B("two")));
        StringAssert.Contains(append.Message, "Writer has previous error");
        log.ResetSeenError();
        log.AddRecord(B("two"));
        log.Sync();
    }

    [TestMethod]
    public void AtomicGroupWithAnImpossibleSizeIsCorruption()
    {
        // S28. AtomicGroupReadBuffer::AddEdit (db/version_set.cc): the first edit of a group sizes the replay buffer
        // with remaining_entries + 1. A corrupt count above int.MaxValue made the port throw OverflowException (not a
        // RocksDbException, so callers that handle Corruption did not see it).
        var replayer = new ManifestReplayer(null);
        var edit = new VersionEdit { ColumnFamily = 0 };
        edit.MarkAtomicGroup(uint.MaxValue);
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => replayer.Apply(edit));
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
        var edit2 = new VersionEdit { ColumnFamily = 0 };
        edit2.MarkAtomicGroup(int.MaxValue);
        ex = Assert.ThrowsExactly<RocksDbException>(() => new ManifestReplayer(null).Apply(edit2));
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
    }

    [TestMethod]
    public void RepairOrdersWalDataBySequenceNotAsNewest()
    {
        // Repairer::Run (db/repair.cc): ConvertLogFilesToTables runs before AddTables, each WAL becomes its own tables,
        // and AddTables orders all level-0 tables of a column family by sequence number (RecoverEpochNumbers,
        // NewestFirstBySeqNo). Here a stale WAL (k=old, sequence 1; its data was flushed and overwritten since) and a
        // live WAL (j, sequence 3) are both on disk. The port replayed all WALs into one table after writing the
        // MANIFEST, as the newest data: k came back as "old". Native: Audit4FollowUpRecoveryCompatTests.
        // RepairWithAStaleWalKeepsTheNewerTableDataLikeNative.
        MemoryFileSystem fs = NewDbFs();
        string staleWal;
        byte[] staleBytes;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, new DbOptions { CreateIfMissing = true, FileSystem = fs }))
        {
            db.Put(B("k"), B("old"));
            staleWal = FileNames.LogFileName(DbPath, WalNumbers(fs)[^1]);
            staleBytes = new byte[fs.GetFileSize(staleWal)];
            using (IRandomAccessFile f = fs.OpenRandomAccess(staleWal)) Assert.AreEqual(staleBytes.Length, f.Read(0, staleBytes));
            db.Flush();
            db.Put(B("k"), B("new"));
            db.Flush();
            db.Put(B("j"), B("wal"));
        }
        using (IWritableFile w = fs.CreateWritable(staleWal, truncate: true))
        {
            w.Append(staleBytes);
            w.Sync();
        }
        Assert.IsTrue(WalNumbers(fs).Count >= 2);
        foreach (string c in new List<string>(fs.GetChildren(DbPath)))
        {
            if (c.StartsWith("MANIFEST-", StringComparison.Ordinal) || c == "CURRENT") fs.DeleteFile(DbPath + "/" + c);
        }
        RocksDatabase.Repair(DbPath, new DbOptions { FileSystem = fs });
        using (RocksDatabase db = RocksDatabase.Open(DbPath, new DbOptions { FileSystem = fs }))
        {
            Assert.AreEqual("new", GetString(db, "k"));
            Assert.AreEqual("wal", GetString(db, "j"));
            Assert.AreEqual(3UL, db.GetLatestSequenceNumber());
        }
        // The WALs are archived, like upstream's ArchiveFile.
        Assert.IsTrue(fs.FileExists(DbPath + "/lost/" + staleWal[(staleWal.LastIndexOf('/') + 1)..]));
    }

    [TestMethod]
    public void LiveEditCannotAddATableFileThatIsAlreadyInTheLsmTree()
    {
        // S30. VersionBuilder::ApplyFileAddition (db/version_builder.cc): "Cannot add table file #N to level L since it
        // is already in the LSM tree on level M" (Corruption), for every edit, not only during MANIFEST replay. The
        // port's live path (VersionStorageInfo.ApplyEdit) had no such check: the edit was written to the MANIFEST (which
        // then no longer replays: ManifestReplayer has the check) and the version listed the file on two levels.
        // An edit that also deletes the file (a trivial move) is fine: deletions are applied first.
        MemoryFileSystem fs = NewDbFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, new DbOptions { CreateIfMissing = true, FileSystem = fs }))
        {
            db.Put(B("a"), B("1"));
            db.Flush();
            ColumnFamilyData cfd = db.Impl.GetColumnFamilies()[0];
            FileMetaData file = cfd.Current.Storage.LevelFiles(0)[0];
            var duplicate = new VersionEdit { ColumnFamily = cfd.Id };
            duplicate.AddFile(3, file.Clone());
            RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() =>
                db.Impl.Versions.LogAndApplyAsync(cfd, duplicate, synchronous: true, CancellationToken.None).GetAwaiter().GetResult());
            Assert.AreEqual(StatusCode.Corruption, ex.Code);
            StringAssert.Contains(ex.Message, "Cannot add table file #" + file.FileNumber + " to level 3 since it is already in the LSM tree on level 0");
            Assert.AreEqual(1, cfd.Current.Storage.LevelFiles(0).Length);
            Assert.AreEqual(0, cfd.Current.Storage.LevelFiles(3).Length);

            var move = new VersionEdit { ColumnFamily = cfd.Id };
            move.DeleteFile(0, file.FileNumber);
            move.AddFile(3, file.Clone());
            db.Impl.Versions.LogAndApplyAsync(cfd, move, synchronous: true, CancellationToken.None).GetAwaiter().GetResult();
            Assert.AreEqual(0, cfd.Current.Storage.LevelFiles(0).Length);
            Assert.AreEqual(1, cfd.Current.Storage.LevelFiles(3).Length);
        }
        // The MANIFEST replays: the refused edit was not written.
        using RocksDatabase reopened = RocksDatabase.Open(DbPath, new DbOptions { FileSystem = fs });
        Assert.AreEqual("1", GetString(reopened, "a"));
    }

    private sealed class SuppressingListener : EventListener
    {
        public int Errors;

        public override void OnBackgroundError(BackgroundErrorEventArgs e)
        {
            Interlocked.Increment(ref Errors);
            e.Error = null;
        }
    }

    [TestMethod]
    public void ListenerCannotSuppressARetryableIoError()
    {
        // ErrorHandler::SetBGErrorImpl (db/error_handler.cc), retryable / file-scoped IO error:
        // "CheckAndSetRecoveryAndBGError(bg_err, context); ... EventHelpers::NotifyOnBackgroundError(listeners, reason,
        // &new_bg_io_err, ...)": the background error is set before the listeners run and they get a copy, so a
        // listener that clears the status does not suppress it (only HandleKnownErrors uses the listeners' status:
        // ErrorHandlerTests.ListenerCanSuppressBackgroundError). The port notified first and dropped the error.
        // max_bgerror_resume_count = 0: "severity = Status::Severity::kHardError", no automatic recovery.
        var fs = new FaultInjectionFileSystem(NewDbFs());
        var listener = new SuppressingListener();
        var options = new DbOptions { CreateIfMissing = true, FileSystem = fs, MaxBgErrorResumeCount = 0 };
        options.Listeners.Add(listener);
        using RocksDatabase db = RocksDatabase.Open(DbPath, options);
        db.Put(B("a"), B("1"));
        fs.FailMatching(".sst", FaultOps.Append, 1, InjectedFaultKind.Retryable);
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
        Assert.AreEqual(StatusCode.IOError, ex.Code);
        Assert.AreEqual(1, listener.Errors);
        Assert.IsNotNull(db.BackgroundError);
        Assert.AreEqual(ErrorSeverity.HardError, db.BackgroundError.Severity);
        ex = Assert.ThrowsExactly<RocksDbException>(() => db.Put(B("b"), B("2")));
        Assert.AreEqual(StatusCode.IOError, ex.Code);
        db.Resume();
        Assert.IsNull(db.BackgroundError);
        db.Put(B("b"), B("2"));
        Assert.AreEqual("1", GetString(db, "a"));
    }

    [TestMethod]
    public void SecondaryCatchUpReportsACorruptWalRecord()
    {
        // DBImplSecondary::RecoverLogFiles: "while (reader->ReadRecord(...) && wal_read_status->ok() && status.ok())"
        // and "if (status.ok() && !wal_read_status->ok()) status = *wal_read_status": a corruption reported by the WAL
        // reader (here a checksum mismatch) fails TryCatchUpWithPrimary. The port never looked at the reporter: the
        // dropped records were skipped silently and the catch-up reported success.
        MemoryFileSystem fs = NewDbFs();
        fs.CreateDirectoryIfMissing("/audit4fr-sec");
        using RocksDatabase primary = RocksDatabase.Open(DbPath, new DbOptions { CreateIfMissing = true, FileSystem = fs });
        primary.Put(B("a"), B("1"));
        using RocksDatabase secondary = RocksDatabase.OpenAsSecondary(DbPath, "/audit4fr-sec", new DbOptions { FileSystem = fs },
            [new ColumnFamilyDescriptor("default", new ColumnFamilyOptions())]);
        Assert.AreEqual("1", GetString(secondary, "a"));
        primary.Put(B("b"), B("2222222222"));
        primary.Put(B("c"), B("3"));
        string wal = FileNames.LogFileName(DbPath, WalNumbers(fs)[^1]);
        byte[] bytes = fs.ReadAllBytes(wal);
        int at = bytes.AsSpan().IndexOf(B("2222222222"));
        Assert.IsTrue(at > 0);
        // Payload of the second record, changed in place (the secondary's reader has the file open): its checksum no
        // longer matches.
        fs.WriteAt(wal, at, [(byte)(bytes[at] ^ 0x40)]);
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => secondary.TryCatchUpWithPrimary());
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
        Assert.IsNull(GetString(secondary, "c"));
        // The WAL's status sticks (LogReaderContainer::status_) while the WAL is needed.
        ex = Assert.ThrowsExactly<RocksDbException>(() => secondary.TryCatchUpWithPrimary());
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
        Assert.AreEqual("1", GetString(secondary, "a"));
    }

    [TestMethod]
    public void WalIsCutBackAtCloseAfterAFailedMemtableInsert()
    {
        // DBImpl::WriteImpl: "if (!w.status.ok()) { ... logs_.back().SetAttemptTruncateSize(wal_context.prev_size); }
        // HandleMemTableInsertFailure(w.status);" and LogWriterNumber::ClearWriter (db_impl.h, DB close):
        // "writer->file()->writable_file()->Truncate(attempt_truncate_size)". The record of the refused write is removed
        // from the WAL when the DB closes. The port kept it: the reopened DB had the batch's first entry (x) in its
        // memtable at an unpublished sequence, and the next write (same sequence) made it visible.
        MemoryFileSystem fs = NewDbFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, new DbOptions { CreateIfMissing = true, FileSystem = fs }))
        {
            db.Put(B("a"), B("1"));
            WriteRefusedBatch(db);
            Assert.AreEqual(1UL, db.GetLatestSequenceNumber());
            Assert.AreEqual(2, WalRecordCount(fs));
        }
        Assert.AreEqual(1, WalRecordCount(fs));
        using (RocksDatabase db = RocksDatabase.Open(DbPath, new DbOptions { FileSystem = fs }))
        {
            Assert.AreEqual(1UL, db.GetLatestSequenceNumber());
            db.Put(B("z"), B("1"));
            Assert.IsNull(GetString(db, "x"));
            Assert.IsNull(GetString(db, "y"));
            Assert.AreEqual("1", GetString(db, "a"));
            Assert.AreEqual(2UL, db.GetLatestSequenceNumber());
        }
    }

    [TestMethod]
    public void RefusedBatchLeftInTheWalIsReplayedUpToTheRefusedEntry()
    {
        // The DB did not close (crash): the refused write's record stays in the WAL. DBImpl::InsertLogRecordToMemtable ->
        // WriteBatchInternal::InsertInto sets "*next_seq = inserter.sequence()" even when Iterate fails, the failure is
        // reported (reporter->Corruption(record.size(), process_status)), point-in-time recovery stops there and
        // FinishLogFileProcessing publishes next_sequence - 1: the entries before the refused one are recovered (native
        // does the same: Audit4FollowUpRecoveryCompatTests.MalformedOrRefusedBatchInWalRecoversLikeNative).
        MemoryFileSystem fs = NewDbFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, new DbOptions { CreateIfMissing = true, FileSystem = fs }))
        {
            db.Put(B("a"), B("1"));
        }
        ulong number = WalNumbers(fs)[^1] + 10;
        using (var log = new LogWriter(fs.CreateWritable(FileNames.LogFileName(DbPath, number)), number, recycleLogFiles: false))
        {
            using var b = new WriteBatch();
            b.Put(B("x"), B("1")).DeleteRange(B("c"), B("a")).Put(B("y"), B("1"));
            b.Sequence = 2;
            log.AddRecord(b.Data);
            log.WriteBuffer();
            log.Sync();
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, new DbOptions { FileSystem = fs }))
        {
            Assert.AreEqual("1", GetString(db, "x"));
            Assert.IsNull(GetString(db, "y"));
            Assert.AreEqual("1", GetString(db, "a")); // the refused range tombstone [c, a) was not applied
            Assert.AreEqual(2UL, db.GetLatestSequenceNumber());
            db.Put(B("z"), B("1"));
            Assert.AreEqual(3UL, db.GetLatestSequenceNumber());
        }
        // kAbsoluteConsistency returns the handler's status unchanged (InvalidArgument, not Corruption).
        MemoryFileSystem fs2 = NewDbFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, new DbOptions { CreateIfMissing = true, FileSystem = fs2 }))
        {
            db.Put(B("a"), B("1"));
        }
        number = WalNumbers(fs2)[^1] + 10;
        using (var log = new LogWriter(fs2.CreateWritable(FileNames.LogFileName(DbPath, number)), number, recycleLogFiles: false))
        {
            using var b = new WriteBatch();
            b.Put(B("x"), B("1")).DeleteRange(B("c"), B("a"));
            b.Sequence = 2;
            log.AddRecord(b.Data);
            log.WriteBuffer();
            log.Sync();
        }
        var ex = Assert.ThrowsExactly<RocksDbException>(
            () => RocksDatabase.Open(DbPath, new DbOptions { FileSystem = fs2, WalRecoveryMode = WalRecoveryMode.AbsoluteConsistency }).Dispose());
        Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
    }
}
