using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Utilities.Backup;
using RocksDb.Managed.Utilities.Transactions;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4 closing pass: the suspicions earlier slices could not prove (R2, R16, R20, R24...), each settled against
/// the C++ function cited in the test.
/// </summary>
[TestClass]
public sealed class Audit4ClosingSuspicionsTests
{
    private const string DbPath = "/audit4-suspicions";

    [TestCleanup]
    public void Cleanup() => TestHooks.ClearAll();

    [TestMethod]
    public void MultiGetSoftLimitCountsTheActiveMemtableBeforeTheImmutableOnes()
    {
        // DBImpl::MultiGetImpl (db/db_impl/db_impl_sync_and_async.h:592-598): super_version->mem->MultiGet runs for
        // every key of the batch, then imm->MultiGet for the keys left. MemTable::MultiGet (db/memtable.cc:1952-1966)
        // adds each value it finds and, once the total exceeds value_size_soft_limit, marks every key still in the
        // range Aborted. With "a" in an immutable memtable and "b", "c" in the active one (10 bytes each, limit 15),
        // the active memtable returns b and c (20 > 15) and "a" is Aborted without the immutable memtable being read.
        // The port looked each key up in all memtables in turn: a and b were returned, c Aborted.
        MemoryFileSystem fs = NewFs();
        using var gate = new ManualResetEventSlim(false);
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        try
        {
            db.Put(B("a"), B("0123456789"));
            TestHooks.Set("DBImpl::FlushMemTables:Start", () => gate.Wait());
            db.Flush(new FlushOptions { Wait = false }); // "a" stays in an immutable memtable while the flush is held
            db.Put(B("b"), B("0123456789"));
            db.Put(B("c"), B("0123456789"));
            using var batch = new MultiGetBatch();
            batch.Add(B("a")).Add(B("b")).Add(B("c"));
            db.MultiGet(new ReadOptions { ValueSizeSoftLimit = 15 }, batch);
            Assert.AreEqual(MultiGetStatus.Error, batch.GetStatus(0));
            Assert.AreEqual(StatusCode.Aborted, batch.GetError(0)!.Code);
            Assert.AreEqual(MultiGetStatus.Found, batch.GetStatus(1));
            Assert.AreEqual(MultiGetStatus.Found, batch.GetStatus(2));

            // Not crossed inside the active memtable: the immutable memtable's value crosses it, nothing is left to abort.
            using var two = new MultiGetBatch();
            two.Add(B("a")).Add(B("b")).Add(B("zz"));
            db.MultiGet(new ReadOptions { ValueSizeSoftLimit = 15 }, two);
            Assert.AreEqual(MultiGetStatus.Found, two.GetStatus(0));
            Assert.AreEqual(MultiGetStatus.Found, two.GetStatus(1));
            Assert.AreEqual(StatusCode.Aborted, two.GetError(2)!.Code); // MemTable::MultiGet: every key left in the range
        }
        finally
        {
            TestHooks.ClearAll();
            gate.Set();
        }
    }

    private static string OnlyTableFile(MemoryFileSystem fs, string dir)
    {
        string? found = null;
        foreach (string c in fs.GetChildren(dir))
        {
            if (!c.EndsWith(".sst", StringComparison.Ordinal)) continue;
            Assert.IsNull(found, "one table file expected");
            found = c;
        }
        Assert.IsNotNull(found);
        return found;
    }

    [TestMethod]
    public void RestoreVerifyChecksumDoesNotKeepAFileUnderAnotherName()
    {
        // BackupEngineImpl::InferDBFilesToRetainInRestore, kVerifyChecksum (utilities/backup/backup_engine.cc ~2520):
        // "f_ei == end() || f_ei->second.second->GetDbFileName() != f" -> "Existing file is not present in the backup",
        // the file is not retained. An existing 000NNN.ldb (ParseFileName: a table file with the same number) is
        // therefore deleted and 000NNN.sst restored. The port matched on the number only and kept the .ldb file
        // instead of restoring the .sst.
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        using (BackupEngine be = BackupEngine.Open(new BackupEngineOptions("/backup"), fs))
        {
            db.Put(B("k"), B("v"));
            be.CreateNewBackup(db, flushBeforeBackup: true);
        }
        using BackupEngineReadOnly ro = BackupEngineReadOnly.Open(new BackupEngineOptions("/backup"), fs);
        ro.RestoreDbFromLatestBackup("/r", "/r", new RestoreOptions());
        string sst = OnlyTableFile(fs, "/r");
        string ldb = sst[..^4] + ".ldb";
        fs.RenameFile("/r/" + sst, "/r/" + ldb);
        ro.RestoreDbFromLatestBackup("/r", "/r", new RestoreOptions { Mode = RestoreMode.VerifyChecksum });
        Assert.IsTrue(fs.FileExists("/r/" + sst));
        Assert.IsFalse(fs.FileExists("/r/" + ldb));
        // The same name with the same contents is retained (and still there).
        ro.RestoreDbFromLatestBackup("/r", "/r", new RestoreOptions { Mode = RestoreMode.VerifyChecksum });
        Assert.IsTrue(fs.FileExists("/r/" + sst));
    }

    [TestMethod]
    public void RestoreKeepLatestDbSessionIdFilesMatchesTheExactSharedFileName()
    {
        // InferDBFilesToRetainInRestore, kKeepLatestDbSessionIdFiles (backup_engine.cc ~2478-2511): the existing file is
        // retained only if the backup file is named GenerateSharedFileWithDbSessionIdAndSize(f, size, session id), i.e.
        // shared_checksum/<number>_s<session>_<size>.sst. A backup made with kUseDbSessionId without
        // kFlagIncludeFileSize is named <number>_s<session>.sst: nothing is retained and the file is copied again.
        // The port retained any file with the backup file's size and session id. The existing file is damaged here
        // (same size, same properties) so that a retained file is visible.
        MemoryFileSystem fs = NewFs();
        var o = new BackupEngineOptions("/backup") { ShareFilesWithChecksumNaming = ShareFilesNaming.UseDbSessionId };
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        using (BackupEngine be = BackupEngine.Open(o, fs))
        {
            for (int i = 0; i < 200; i++) db.Put(Key(i), Value(i, 1));
            be.CreateNewBackup(db, flushBeforeBackup: true);
        }
        using BackupEngineReadOnly ro = BackupEngineReadOnly.Open(o, fs);
        ro.RestoreDbFromLatestBackup("/r", "/r", new RestoreOptions());
        string sst = OnlyTableFile(fs, "/r");
        byte[] good = fs.ReadAllBytes("/r/" + sst);
        fs.WriteAt("/r/" + sst, 10, [(byte)(good[10] ^ 0xff)]);
        ro.RestoreDbFromLatestBackup("/r", "/r", new RestoreOptions { Mode = RestoreMode.KeepLatestDbSessionIdFiles });
        CollectionAssert.AreEqual(good, fs.ReadAllBytes("/r/" + sst));

        // With the default naming (session id and size in the name) the existing file is retained as is.
        var withSize = new BackupEngineOptions("/backup2");
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        using (BackupEngine be = BackupEngine.Open(withSize, fs))
        {
            be.CreateNewBackup(db, flushBeforeBackup: true);
        }
        using BackupEngineReadOnly ro2 = BackupEngineReadOnly.Open(withSize, fs);
        ro2.RestoreDbFromLatestBackup("/r2", "/r2", new RestoreOptions());
        sst = OnlyTableFile(fs, "/r2");
        fs.WriteAt("/r2/" + sst, 10, [(byte)(good[10] ^ 0xff)]);
        byte[] damaged = fs.ReadAllBytes("/r2/" + sst);
        ro2.RestoreDbFromLatestBackup("/r2", "/r2", new RestoreOptions { Mode = RestoreMode.KeepLatestDbSessionIdFiles });
        CollectionAssert.AreEqual(damaged, fs.ReadAllBytes("/r2/" + sst));
    }

    [TestMethod]
    public void MultiGetChecksTheDeadlineBeforeEachBatchOf32Keys()
    {
        // DBImpl::MultiGetImpl (db/db_impl/db_impl_sync_and_async.h:563-569): "while (keys_left) { if (deadline &&
        // NowMicros() > deadline) { s = TimedOut; break; } ..." -- the deadline is checked before every batch of
        // MultiGetContext::MAX_BATCH_SIZE keys, and the keys of the batches not started are TimedOut (lines 695-701)
        // even when their values sit in the memtable. The port checked it once, before the first batch. The deadline
        // "passes" here between the first and the second batch (hook before each later batch).
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        for (int i = 0; i < 40; i++) db.Put(Key(i), Value(i, 1));
        var ro = new ReadOptions { Deadline = TimeSpan.FromDays(365 * 200) }; // microseconds since the epoch: far away
        TestHooks.Set("DBImpl::MultiGetImpl:BeforeBatch", () => ro.Deadline = TimeSpan.FromSeconds(1));
        using var batch = new MultiGetBatch();
        for (int i = 39; i >= 0; i--) batch.Add(Key(i)); // batches are cut from the sorted keys
        db.MultiGet(ro, batch);
        for (int i = 0; i < 40; i++)
        {
            int key = 39 - i;
            if (key < 32)
            {
                Assert.AreEqual(MultiGetStatus.Found, batch.GetStatus(i), "key " + key);
            }
            else
            {
                Assert.AreEqual(MultiGetStatus.Error, batch.GetStatus(i), "key " + key);
                Assert.AreEqual(StatusCode.TimedOut, batch.GetError(i)!.Code);
            }
        }
    }

    [TestMethod]
    public void LockLimitFailureAfterWaitingDoesNotRecordWaitingInfo()
    {
        // PointLockManager::AcquireWithTimeout (utilities/transactions/lock/point/point_lock_manager.cc:714-717):
        // "On timeout, persist the lock information ... if (result.IsTimedOut()) txn->SetWaitingTxn(...)". A lock limit
        // failure (Status::LockLimit() = Aborted) after the wait loop records nothing, so GetWaitingTxns keeps
        // reporting no key. The port recorded the key for every failure.
        using TransactionDatabase db = TransactionDatabase.Open(DbPath, NewOptions(NewFs()), new TransactionDbOptions { MaxNumLocks = 1 });
        var wo = new WriteOptions();
        using Transaction holder = db.BeginTransaction(wo);
        holder.Put(B("a"), B("1"));
        using Transaction other = db.BeginTransaction(wo, new TransactionOptions { LockTimeout = TimeSpan.FromMilliseconds(20) });
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => other.Put(B("b"), B("1")));
        Assert.AreEqual(StatusCode.Aborted, ex.Code);
        Assert.AreEqual(StatusSubCode.LockLimit, ex.SubCode);
        IReadOnlyList<ulong> waiting = ((PessimisticTransaction)other).GetWaitingTransactions(out _, out byte[]? key);
        Assert.AreEqual(0, waiting.Count);
        Assert.IsTrue(key is null || key.Length == 0);
    }

    [TestMethod]
    public void WalIteratorWithoutFilesStopsAtAKnownRecoveryGap()
    {
        // WalIteratorImpl::SeekToStartSequence (db/wal_iterator_impl.cc:150-153): "if (files_->size() <=
        // start_file_index) { StopBeforeRecoveryGap(/*wal_number=*/0, kMaxSequenceNumber); return; }" -- with no WAL
        // left to read, a recovery gap at or after the start sequence that was known when the iterator was created
        // ends the run with NotFound instead of a silent end. The port returned without a status.
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        DbImpl impl = db.DefaultColumnFamily.Db;
        using (var none = new TransactionLogIterator(impl, 5, new List<WalFileInfo>(), verifyChecksums: true))
        {
            Assert.IsFalse(none.Valid);
            Assert.IsNull(none.Error);
        }
        impl.WalRecoverySequences.Record(7, 6, 9);
        using var it = new TransactionLogIterator(impl, 5, new List<WalFileInfo>(), verifyChecksums: true);
        Assert.IsFalse(it.Valid);
        Assert.AreEqual(StatusCode.NotFound, it.Error?.Code);
    }

    [TestMethod]
    public void LocalWritableFileRangeSyncLeavesTheLastMebibyteAndRejectsAppendAfterClose()
    {
        // WritableFileWriter::Flush (file/writable_file_writer.cc:502-527): with bytes_per_sync the range sync stops
        // 1 MiB (kBytesNotSyncRange) before the end of the file, aligned down to 4 KiB, and only once that offset is
        // bytes_per_sync past the last synced one. The port synced up to the end of the file.
        string dir = Path.Combine(Path.GetTempPath(), "audit4-suspicions-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var chunk = new byte[256 * 1024];
            var f = (LocalWritableFile)LocalFileSystem.Instance.CreateWritable(dir + "/f", truncate: true, new WritableFileOptions { BytesPerSync = 64 * 1024 });
            using (f)
            {
                f.Append(chunk);
                f.Append(chunk);
                f.Flush();
                Assert.AreEqual(0L, f.LastRangeSyncForTest); // 512 KiB: everything is within the last 1 MiB
                for (int i = 0; i < 3; i++) f.Append(chunk);
                f.Flush();
                Assert.AreEqual(256L * 1024, f.LastRangeSyncForTest); // 1 MiB + 256 KiB written
            }
            // A closed file fails every append the same way, including an empty one (it threw NullReferenceException).
            RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => f.Append(ReadOnlySpan<byte>.Empty));
            Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [TestMethod]
    public void BackupMetaLineWithATrailingSpaceIsAccepted()
    {
        // BackupEngineImpl::BackupMeta::LoadFromFile (utilities/backup/backup_engine.cc:3065): StringSplit(line, ' ')
        // (util/string_util.cc, std::getline) yields no component after a trailing delimiter, so "<file> crc32 <n> "
        // has three components and loads; an empty line has none ("Empty line instead of file entry."). string.Split
        // kept a fourth, empty component and the backup was reported corrupt ("Extra data for entry").
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        using (BackupEngine be = BackupEngine.Open(new BackupEngineOptions("/backup"), fs))
        {
            db.Put(B("k"), B("v"));
            be.CreateNewBackup(db, flushBeforeBackup: true);
        }
        string meta = System.Text.Encoding.ASCII.GetString(fs.ReadAllBytes("/backup/meta/1"));
        Assert.IsTrue(meta.EndsWith('\n'));
        WriteFile(fs, "/backup/meta/1", meta[..^1] + " \n");
        using (BackupEngineReadOnly ro = BackupEngineReadOnly.Open(new BackupEngineOptions("/backup"), fs))
        {
            Assert.AreEqual(0, ro.GetCorruptedBackups().Count);
            Assert.AreEqual(1, ro.GetBackupInfo().Count);
        }
        WriteFile(fs, "/backup/meta/1", meta + "\n");
        using (BackupEngineReadOnly ro = BackupEngineReadOnly.Open(new BackupEngineOptions("/backup"), fs))
        {
            Assert.AreEqual(1, ro.GetCorruptedBackups().Count);
        }
    }

    private static void WriteFile(MemoryFileSystem fs, string path, string text)
    {
        using IWritableFile f = fs.CreateWritable(path);
        f.Append(System.Text.Encoding.ASCII.GetBytes(text));
        f.Sync();
    }

    [TestMethod]
    public void IngestedFileUniqueIdIsNullWhenItCannotBeDerived()
    {
        // ExternalSstFileIngestionJob::GetIngestedFileInfo (db/external_sst_file_ingestion_job.cc:1665-1675) calls
        // GetSstInternalUniqueId without force (table/unique_id.cc:64-84): a missing db id, a zero file number, a
        // missing or malformed session id are NotSupported, "Failed to get SST unique id" is logged and the file gets
        // kNullUniqueId64x2. The port derived an id with the forced fallback for a zero file number / malformed id.
        const string Session = "ABCDEFGHIJ0123456789";
        Assert.IsTrue(ExternalSstFileIngestionJob.IngestedFileUniqueId("db", Session, 0).IsNull);
        Assert.IsTrue(ExternalSstFileIngestionJob.IngestedFileUniqueId("db", "bad", 7).IsNull);
        Assert.IsTrue(ExternalSstFileIngestionJob.IngestedFileUniqueId("", Session, 7).IsNull);
        Assert.IsTrue(ExternalSstFileIngestionJob.IngestedFileUniqueId("db", "", 7).IsNull);
        UniqueId64x2 id = ExternalSstFileIngestionJob.IngestedFileUniqueId("db", Session, 7);
        Assert.IsFalse(id.IsNull);
        RocksDb.Managed.Cache.OffsetableCacheKey.GetSstInternalUniqueId("db", Session, 7, out ulong id0, out ulong id1);
        Assert.AreEqual(new UniqueId64x2(id0, id1), id);
    }

    [TestMethod]
    public void WalPurgeIntervalIsAtLeastOneSecond()
    {
        // WalManager::PurgeObsoleteWALFiles (db/wal_manager.cc:180-184): time_to_check = ttl_enabled ?
        // min(kDefaultIntervalToDeleteObsoleteWAL, max(1, WAL_ttl_seconds / 2)) : kDefaultIntervalToDeleteObsoleteWAL.
        // Without the max a ttl of 1 second gave an interval of 0 (a purge scan on every call).
        Assert.AreEqual(1L, DbImpl.WalPurgeIntervalSeconds(1));
        Assert.AreEqual(1L, DbImpl.WalPurgeIntervalSeconds(3));
        Assert.AreEqual(50L, DbImpl.WalPurgeIntervalSeconds(100));
        Assert.AreEqual(600L, DbImpl.WalPurgeIntervalSeconds(5000));
        Assert.AreEqual(600L, DbImpl.WalPurgeIntervalSeconds(0)); // size limit only
    }

    [TestMethod]
    public void CheckpointUnderLockWalWithActiveBlobDirectWriteFilesIsNotSupported()
    {
        // DBImpl::GetLiveFilesStorageInfo (db/db_filesnapshot.cc): with blob direct write files that only the flush of
        // their memtable registers in the MANIFEST, a capture that cannot flush because the WAL is locked fails with
        // NotSupported("... Retry with WAL unlocked."). The branch had no test.
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.AllowConcurrentMemtableWrite = false;
        var cf = new ColumnFamilyOptions { EnableBlobFiles = true, EnableBlobDirectWrite = true, MinBlobSize = 32, DisableAutoCompactions = true };
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, cf);
        db.Put(B("k"), new byte[200]);
        using (db.LockWal())
        {
            RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.CreateCheckpoint("/cp-locked"));
            Assert.AreEqual(StatusCode.NotSupported, ex.Code);
            StringAssert.Contains(ex.Message, "Retry with WAL unlocked");
        }
        db.CreateCheckpoint("/cp");
        using RocksDatabase copy = RocksDatabase.Open("/cp", new DbOptions { FileSystem = fs, AllowConcurrentMemtableWrite = false }, cf);
        Assert.AreEqual(200, copy.Get(B("k"))!.Length);
    }

    [TestMethod]
    public void RecoveredUnpreparedRollbackRejectsDeleteRange()
    {
        // WriteUnpreparedTxnDB::RollbackRecoveredTransaction (utilities/transactions/write_unprepared_txn_db.cc:60-160):
        // the RollbackWriteBatchBuilder handler does not override DeleteRangeCF, so WriteBatch::Handler's default
        // (include/rocksdb/write_batch.h:302-306) returns InvalidArgument("DeleteRangeCF not implemented"), Iterate
        // fails and so does the recovery. The port ignored the range deletion and went on with a partial rollback.
        var builder = new TransactionDatabase.RecoveredRollbackBuilder(null!, null!, null!, false);
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => builder.DeleteRange(0, B("a"), B("b")));
        Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
    }
}
