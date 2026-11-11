using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Utilities.Transactions;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4 follow-up, slice FollowUpWriteTxn: the write path, the WAL and the transaction layers re-read line by line
/// against write_prepared_txn_db.{h,cc}, db/write_batch.cc (MemTableInserter), db/db_impl/db_impl_write.cc,
/// db/log_reader.cc, db/transaction_log_impl.cc and utilities/transactions/*.
/// </summary>
[TestClass]
public sealed class Audit4FollowUpWriteTxnTests
{
    private static readonly WriteOptions s_wo = new();
    private static readonly WriteOptions s_sync = new() { Sync = true };

    private static TransactionDatabase Open(MemoryFileSystem fs, TransactionWritePolicy policy, bool twoWriteQueues = false, Comparator? comparator = null,
        int commitCacheBits = 10)
    {
        DbOptions o = NewOptions(fs);
        o.TwoWriteQueues = twoWriteQueues;
        var cfo = new ColumnFamilyOptions { DisableAutoCompactions = true };
        if (comparator is not null) cfo.Comparator = comparator;
        var tdo = new TransactionDbOptions
        {
            WritePolicy = policy,
            WpCommitCacheBits = commitCacheBits,
            WpSnapshotCacheBits = 2,
            TransactionLockTimeout = TimeSpan.FromMilliseconds(50),
            DefaultLockTimeout = TimeSpan.FromMilliseconds(50),
        };
        return TransactionDatabase.Open(DbPath, o, tdo, [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, cfo)]);
    }

    private static string Get(TransactionDatabase db, string key) => S(db.Database.Get(B(key))) ?? "NOT_FOUND";

    /// <summary>A simulated power loss: the instance is abandoned (not closed).</summary>
    private static void Crash(MemoryFileSystem fs, TransactionDatabase db)
    {
        db.Database.WaitForCompact();
        fs.SimulateCrash();
    }

    private static List<ulong> WalNumbers(MemoryFileSystem fs)
    {
        var wals = new List<ulong>();
        foreach (string child in fs.GetChildren(DbPath))
        {
            if (FileNames.TryParseFileName(child, out ulong number, out FileType type) && type == FileType.WalFile) wals.Add(number);
        }
        wals.Sort();
        return wals;
    }

    /// <summary>Keys that differ only in ASCII case are equal (a comparator for which different bytes can be equal).</summary>
    private sealed class CaseInsensitiveComparator : Comparator
    {
        public override string Name => "audit4.followup.CaseInsensitiveComparator";

        public override int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
        {
            int n = Math.Min(a.Length, b.Length);
            for (int i = 0; i < n; i++)
            {
                int x = Lower(a[i]), y = Lower(b[i]);
                if (x != y) return x < y ? -1 : 1;
            }
            return a.Length.CompareTo(b.Length);
        }

        private static int Lower(byte c) => c is >= (byte)'A' and <= (byte)'Z' ? c + 32 : c;
    }

    // ---- GAP-8: recovery of prepared transactions ---------------------------------------------------------------

    [TestMethod]
    public void RecoveryOfAFlushedPreparedSectionCountsSubBatchesWithTheColumnFamilyComparator()
    {
        // MemTableInserter::PutCFImpl (db/write_batch.cc): when recovery skips a column family whose data is already in
        // table files, a WritePrepared prepared section is still rebuilt and its sub-batches are counted with
        // IsDuplicateKeySeq -> DuplicateDetector (util/duplicate_detector.h), whose key set is a
        // std::set<Slice, SetComparator> over the column family's comparator (InitWithComp). MarkEndPrepare then records
        // batch_cnt = sequence_ - rebuilding_trx_seq_ + 1 and WritePreparedTxnDB::Initialize adds batch_cnt prepare
        // sequence numbers to the prepared heap. Two keys the comparator considers equal ("K" and "k") are a duplicate:
        // the section has two sub-batches (seq, seq + 1). Counting it as one leaves seq + 1 out of the prepared heap, so
        // after AdvanceMaxEvictedSeq(last_seq) its uncommitted data reads as committed.
        MemoryFileSystem fs = NewFs();
        var cmp = new CaseInsensitiveComparator();
        ulong prepareSeq;
        {
            TransactionDatabase db = Open(fs, TransactionWritePolicy.WritePrepared, comparator: cmp);
            db.Put(s_sync, null, B("k"), B("old"));
            Transaction txn = db.BeginTransaction(s_sync);
            txn.SetName("ci");
            txn.Put(B("K"), B("n1"));
            txn.Put(B("k"), B("n2"));
            txn.Prepare();
            prepareSeq = ((WritePreparedTxn)txn).PrepareSequence;
            Assert.AreEqual("old", Get(db, "k"));
            // The prepared data goes to a table file; the column family's log number passes the prepare WAL, which is
            // kept for its prepared section.
            db.Database.Flush();
            db.Put(s_sync, null, B("z"), B("z1"));
            Crash(fs, db);
        }
        using TransactionDatabase db2 = Open(fs, TransactionWritePolicy.WritePrepared, comparator: cmp);
        WritePreparedTxnDb wp = db2.WritePreparedDb!;
        Assert.IsTrue(wp.IsPrepared(prepareSeq), "first sub-batch");
        Assert.AreEqual("old", Get(db2, "k"), "the second sub-batch of the prepared transaction must stay uncommitted");
        List<Transaction> prepared = db2.GetAllPreparedTransactions();
        Assert.AreEqual(1, prepared.Count);
        prepared[0].Commit();
        Assert.AreEqual("n2", Get(db2, "k"));
        Assert.AreEqual("z1", Get(db2, "z"));
    }

    [TestMethod]
    [DataRow(TransactionWritePolicy.WriteCommitted, false, true)]
    [DataRow(TransactionWritePolicy.WriteCommitted, true, false)]
    [DataRow(TransactionWritePolicy.WritePrepared, false, true)]
    [DataRow(TransactionWritePolicy.WritePrepared, true, false)]
    [DataRow(TransactionWritePolicy.WriteUnprepared, false, true)]
    [DataRow(TransactionWritePolicy.WriteUnprepared, true, false)]
    public void WalOfAPreparedTransactionSurvivesFlushesAndCrashesUntilItIsResolved(TransactionWritePolicy policy, bool twoWriteQueues, bool commit)
    {
        // logs_with_prep_tracker.cc + PrecomputeMinLogNumberToKeep2PC / FindObsoleteFiles (db_impl_files.cc) +
        // DBImpl::RecoverLogFiles (db_impl_open.cc, min_wal_number = MinLogNumberToKeep() with allow_2pc): a WAL holding
        // the prepared section of an unresolved transaction is never deleted, whatever is flushed after it, and every
        // recovery replays it (rebuilding the transaction) although the column family's log number is far beyond it.
        MemoryFileSystem fs = NewFs();
        ulong prepareWal;
        {
            TransactionDatabase db = Open(fs, policy, twoWriteQueues);
            db.Put(s_sync, null, B("base"), B("b0"));
            db.Put(s_sync, null, B("k2"), B("old2"));
            Transaction txn = db.BeginTransaction(s_sync, new TransactionOptions { WriteBatchFlushThreshold = policy == TransactionWritePolicy.WriteUnprepared ? 1 : 0 });
            txn.SetName("survivor");
            txn.Put(B("k1"), B("v1"));
            txn.Put(B("k2"), B("v2"));
            txn.Put(B("k3"), B("v3"));
            txn.Prepare();
            prepareWal = WalNumbers(fs)[^1];
            for (int i = 0; i < 3; i++)
            {
                db.Put(s_sync, null, B("x" + i), B("y" + i));
                db.Database.Flush();
            }
            Assert.IsTrue(WalNumbers(fs).Contains(prepareWal), "prepare WAL deleted while its transaction is prepared");
            Assert.IsTrue(WalNumbers(fs)[^1] > prepareWal);
            Crash(fs, db);
        }
        {
            TransactionDatabase db = Open(fs, policy, twoWriteQueues);
            Assert.AreEqual(1, db.GetAllPreparedTransactions().Count);
            Assert.AreEqual("NOT_FOUND", Get(db, "k1"));
            Assert.AreEqual("old2", Get(db, "k2"));
            Assert.AreEqual("y2", Get(db, "x2"));
            for (int i = 3; i < 5; i++)
            {
                db.Put(s_sync, null, B("x" + i), B("y" + i));
                db.Database.Flush();
            }
            Assert.IsTrue(WalNumbers(fs).Contains(prepareWal), "prepare WAL deleted after the first recovery");
            Crash(fs, db);
        }
        {
            TransactionDatabase db = Open(fs, policy, twoWriteQueues);
            List<Transaction> prepared = db.GetAllPreparedTransactions();
            Assert.AreEqual(1, prepared.Count);
            Assert.AreEqual("survivor", prepared[0].Name);
            Assert.AreEqual("NOT_FOUND", Get(db, "k1"));
            if (commit) prepared[0].Commit();
            else prepared[0].Rollback();
            Assert.AreEqual(commit ? "v1" : "NOT_FOUND", Get(db, "k1"));
            Assert.AreEqual(commit ? "v2" : "old2", Get(db, "k2"));
            db.Put(s_sync, null, B("after"), B("a1"));
            Crash(fs, db);
        }
        {
            // The commit / rollback marker is in a later WAL than the prepared section: both are replayed.
            using TransactionDatabase db = Open(fs, policy, twoWriteQueues);
            Assert.AreEqual(0, db.GetAllPreparedTransactions().Count);
            Assert.AreEqual(commit ? "v1" : "NOT_FOUND", Get(db, "k1"));
            Assert.AreEqual(commit ? "v2" : "old2", Get(db, "k2"));
            Assert.AreEqual(commit ? "v3" : "NOT_FOUND", Get(db, "k3"));
            Assert.AreEqual("a1", Get(db, "after"));
            Assert.AreEqual("y4", Get(db, "x4"));
            // Once the transaction is resolved and its data flushed, the prepare WAL is released.
            db.Put(s_sync, null, B("last"), B("l"));
            db.Database.Flush();
            db.Put(s_sync, null, B("last2"), B("l"));
            db.Database.Flush();
            Assert.IsFalse(WalNumbers(fs).Contains(prepareWal), "prepare WAL kept after its transaction was resolved and flushed");
        }
    }

    // ---- S37: SubBatchCounter and range deletions ------------------------------------------------------------------

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void PlainWriteWithARangeDeletionAtAPointKeyIsOneSubBatch(bool twoWriteQueues)
    {
        // WritePreparedTxnDB::WriteInternal: batch_cnt must be the number of sequence numbers the MemTableInserter
        // consumes ("assert(!seq_per_batch || inserter.sequence() - w->sequence == w->batch_cnt)",
        // WriteBatchInternal::InsertInto). The inserter starts a new sub-batch only when MemTable::Add refuses a
        // duplicate key+seq, and range tombstones are added to the memtable's separate range_del_table_
        // (MemTable::Add, db/memtable.cc), so a DeleteRange whose begin key equals a point key of the same sub-batch
        // is NOT a duplicate. Upstream only accepts range deletions in such a DB through
        // Write(TransactionDBWriteOptimizations{skip_concurrency_control, skip_duplicate_key_check}) with
        // batch_cnt = ONE_BATCH_CNT (include/rocksdb/utilities/transaction_db.h); the port counts the sub-batches
        // itself and must count this batch as one.
        using TransactionDatabase db = Open(NewFs(), TransactionWritePolicy.WritePrepared, twoWriteQueues);
        db.Put(s_wo, null, B("b"), B("b0"));
        ulong before = db.Database.GetLatestSequenceNumber();
        using (var batch = new WriteBatch())
        {
            batch.Put(B("a"), B("a1"));
            batch.DeleteRange(0, B("a"), B("c"));
            db.Database.Write(s_wo, batch);
        }
        // one sub-batch (two write queues: plus the sequence of the commit write on the second queue)
        Assert.AreEqual(before + (twoWriteQueues ? 2UL : 1UL), db.Database.GetLatestSequenceNumber());
        // Same sequence number: the tombstone covers older entries only.
        Assert.AreEqual("a1", Get(db, "a"));
        Assert.AreEqual("NOT_FOUND", Get(db, "b"));

        // Two range deletions with the same begin key are duplicates of each other in the range tombstone table.
        db.Put(s_wo, null, B("d"), B("d0"));
        db.Put(s_wo, null, B("e"), B("e0"));
        ulong mid = db.Database.GetLatestSequenceNumber();
        using (var batch = new WriteBatch())
        {
            batch.DeleteRange(0, B("d"), B("dz"));
            batch.DeleteRange(0, B("d"), B("f"));
            db.Database.Write(s_wo, batch);
        }
        Assert.AreEqual(mid + (twoWriteQueues ? 3UL : 2UL), db.Database.GetLatestSequenceNumber());
        Assert.AreEqual("NOT_FOUND", Get(db, "d"));
        Assert.AreEqual("NOT_FOUND", Get(db, "e"));
    }

    // ---- OPEN-2: commit with a pending SetSnapshotOnNextOperation request -------------------------------------------

    [TestMethod]
    public void PreparedCommitWithPendingSnapshotRequestNeedsACommitTimestamp()
    {
        // WriteCommittedTxn::CommitInternal (pessimistic_transaction.cc): "if (snapshot_needed_) { if
        // (commit_timestamp_ == kMaxTxnTimestamp) { s = Status::InvalidArgument("Must set transaction commit
        // timestamp"); return s; } ... }", and PessimisticTransaction::Commit then restores txn_state_ = PREPARED.
        using TransactionDatabase db = Open(NewFs(), TransactionWritePolicy.WriteCommitted);
        using Transaction txn = db.BeginTransaction(s_wo);
        txn.SetName("pending-snapshot");
        txn.Put(B("k"), B("v"));
        txn.Prepare();
        txn.SetSnapshotOnNextOperation();
        var e = Assert.ThrowsExactly<RocksDbException>(txn.Commit);
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code, e.Message);
        StringAssert.Contains(e.Message, "Must set transaction commit timestamp");
        Assert.AreEqual(TransactionState.Prepared, txn.State);
        Assert.AreEqual("NOT_FOUND", Get(db, "k"));
        // ClearSnapshot drops the request (snapshot_needed_ = false): the commit goes through.
        txn.ClearSnapshot();
        txn.Commit();
        Assert.AreEqual("v", Get(db, "k"));
    }

    [TestMethod]
    [DataRow(TransactionWritePolicy.WritePrepared)]
    [DataRow(TransactionWritePolicy.WriteUnprepared)]
    public void WritePreparedCommitIgnoresAPendingSnapshotRequest(TransactionWritePolicy policy)
    {
        // WritePreparedTxn::CommitWithoutPrepareInternal / CommitInternal and the WriteUnprepared ones
        // (write_prepared_txn.cc, write_unprepared_txn.cc) never read snapshot_needed_: only WriteCommittedTxn creates
        // the requested snapshot at commit, so only it can fail with "Must set transaction commit timestamp".
        using TransactionDatabase db = Open(NewFs(), policy);
        using (Transaction txn = db.BeginTransaction(s_wo))
        {
            txn.SetSnapshotOnNextOperation();
            Assert.IsNull(txn.Get(B("k")));
            txn.Commit();
        }
        using (Transaction txn = db.BeginTransaction(s_wo))
        {
            txn.SetName("p");
            txn.Put(B("k"), B("v"));
            txn.Prepare();
            txn.SetSnapshotOnNextOperation();
            txn.Commit();
        }
        Assert.AreEqual("v", Get(db, "k"));
    }

    // ---- S38: rollback of a recovered unprepared transaction --------------------------------------------------------

    [TestMethod]
    public void RecoveredUnpreparedRollbackRestoresEqualKeysOnce()
    {
        // WriteUnpreparedTxnDB::RollbackRecoveredTransaction (write_unprepared_txn_db.cc): its RollbackWriteBatchBuilder
        // keeps the keys already restored in a std::set<Slice, SetComparator> per column family ("keys_", built from the
        // column family comparators), so two keys of an unprepared batch that the comparator considers equal produce
        // ONE entry of the rollback batch, which is written with batch_cnt = 1.
        MemoryFileSystem fs = NewFs();
        var cmp = new CaseInsensitiveComparator();
        {
            TransactionDatabase db = Open(fs, TransactionWritePolicy.WriteUnprepared, comparator: cmp);
            db.Put(s_sync, null, B("k"), B("old"));
            // The third write finds the batch [K, k] above the threshold and writes it as one unprepared batch.
            Transaction txn = db.BeginTransaction(s_sync, new TransactionOptions { WriteBatchFlushThreshold = 20 });
            txn.SetName("unprepared");
            txn.Put(B("K"), B("n1"));
            txn.Put(B("k"), B("n2"));
            txn.Put(B("z"), B("z1"));
            Assert.AreEqual(1, ((WriteUnpreparedTxn)txn).UnpreparedSequenceNumbers.Count);
            Assert.AreEqual("old", Get(db, "k"));
            Crash(fs, db);
        }
        using TransactionDatabase db2 = Open(fs, TransactionWritePolicy.WriteUnprepared, comparator: cmp);
        Assert.AreEqual(0, db2.GetAllPreparedTransactions().Count);
        Assert.AreEqual("old", Get(db2, "k"));
        Assert.AreEqual("old", Get(db2, "K"));
        Assert.AreEqual("NOT_FOUND", Get(db2, "z"));
        db2.Put(s_wo, null, B("k"), B("new"));
        Assert.AreEqual("new", Get(db2, "K"));
    }

    // ---- S47: waiting transactions of a lock request that does not wait -------------------------------------------

    [TestMethod]
    public void NoWaitLockTimeoutRecordsTheBlockingTransactions()
    {
        // PointLockManager::AcquireWithTimeout (lock/point/point_lock_manager.cc): with timeout == 0 the retry loop is
        // skipped ("if (!result.ok() && timeout != 0)"), but the tail still runs: "On timeout, persist the lock
        // information so we can debug the contention: if (result.IsTimedOut()) txn->SetWaitingTxn(wait_ids,
        // column_family_id, &key, true)". GetWaitingTxns then names the lock holders and the key.
        using TransactionDatabase db = Open(NewFs(), TransactionWritePolicy.WriteCommitted);
        using Transaction holder = db.BeginTransaction(s_wo);
        holder.Put(B("k"), B("v1"));
        using Transaction blocked = db.BeginTransaction(s_wo, new TransactionOptions { LockTimeout = TimeSpan.Zero });
        var e = Assert.ThrowsExactly<RocksDbException>(() => blocked.Put(B("k"), B("v2")));
        Assert.AreEqual(StatusCode.TimedOut, e.Code, e.Message);
        IReadOnlyList<ulong> waiting = ((PessimisticTransaction)blocked).GetWaitingTransactions(out uint cf, out byte[]? key);
        CollectionAssert.AreEqual(new[] { holder.Id }, waiting.ToArray());
        Assert.AreEqual(0u, cf);
        Assert.AreEqual("k", S(key));
    }

    // ---- S64: full_history_ts_low ---------------------------------------------------------------------------------

    [TestMethod]
    public void RecoveryNeverLowersFullHistoryTsLow()
    {
        // DBImpl::IncreaseFullHistoryTsLowImpl (db_impl_compaction_flush.cc) checks "ts_low < current" and then calls
        // LogAndApply, which releases the DB mutex while the MANIFEST is written: a second caller with a lower timestamp
        // can pass the check meanwhile, so the MANIFEST may hold a lower full_history_ts_low after a higher one (the
        // second caller then gets TryAgain). Both the live state and recovery (VersionEditHandler::
        // ExtractInfoFromVersionEdit) go through ColumnFamilyData::SetFullHistoryTsLow, which only ever increases
        // (db/column_family.h). The edits are logged directly here to get that MANIFEST deterministically.
        static byte[] Ts(ulong t)
        {
            byte[] b = new byte[8];
            System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(b, t);
            return b;
        }
        MemoryFileSystem fs = NewFs();
        var cfo = new ColumnFamilyOptions { Comparator = Comparator.BytewiseWithU64Ts };
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, cfo)]))
        {
            db.IncreaseFullHistoryTsLow(Ts(10));
            ColumnFamilyData cfd = db.DefaultColumnFamily.Data;
            db.Impl.Versions.LogAndApplyAsync(cfd, new VersionEdit { ColumnFamily = cfd.Id, FullHistoryTsLow = Ts(5) }, synchronous: true, CancellationToken.None)
                .AsTask().GetAwaiter().GetResult();
            CollectionAssert.AreEqual(Ts(10), db.GetFullHistoryTsLow());
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, cfo)]))
        {
            CollectionAssert.AreEqual(Ts(10), db.GetFullHistoryTsLow());
        }
    }

    // ---- S48: VerifyCFOptions ---------------------------------------------------------------------------------------

    /// <summary>A comparator with 4-byte timestamps (a transaction timestamp is 8 bytes).</summary>
    private sealed class FourByteTimestampComparator : Comparator
    {
        public override string Name => "audit4.followup.FourByteTs";
        public override int TimestampSize => 4;
        public override int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => a.SequenceCompareTo(b);
    }

    [TestMethod]
    public void TransactionDbRefusesColumnFamiliesWhoseTimestampIsNotEightBytes()
    {
        // PessimisticTransactionDB::VerifyCFOptions (pessimistic_transaction_db.cc): ts_sz != sizeof(TxnTimestamp) ->
        // InvalidArgument "Timestamp of transaction must have 8 bytes. CF comparator ... timestamp size is 4 bytes",
        // checked by Initialize (at open) and by CreateColumnFamily / CreateColumnFamilies / CreateColumnFamilyWithImport
        // before the column family is created.
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.CreateMissingColumnFamilies = true;
        var e = Assert.ThrowsExactly<RocksDbException>(() => TransactionDatabase.Open(DbPath, o, new TransactionDbOptions(),
        [
            new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions()),
            new ColumnFamilyDescriptor("ts4", new ColumnFamilyOptions { Comparator = new FourByteTimestampComparator() }),
        ]));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code, e.Message);
        StringAssert.Contains(e.Message, "Timestamp of transaction must have 8 bytes");

        using TransactionDatabase db = Open(NewFs(), TransactionWritePolicy.WriteCommitted);
        e = Assert.ThrowsExactly<RocksDbException>(
            () => db.Database.CreateColumnFamily("ts4", new ColumnFamilyOptions { Comparator = new FourByteTimestampComparator() }));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code, e.Message);
        // Eight-byte timestamps are fine for WriteCommitted.
        ColumnFamily ok = db.Database.CreateColumnFamily("ts8", new ColumnFamilyOptions { Comparator = Comparator.BytewiseWithU64Ts });
        Assert.AreEqual("ts8", ok.Name);
    }

    [TestMethod]
    [DataRow(TransactionWritePolicy.WritePrepared)]
    [DataRow(TransactionWritePolicy.WriteUnprepared)]
    public void WritePreparedDbRefusesTimestampColumnFamiliesCreatedAfterOpen(TransactionWritePolicy policy)
    {
        // PessimisticTransactionDB::CreateColumnFamily -> VerifyCFOptions: "if (txn_db_options_.write_policy !=
        // WRITE_COMMITTED) return Status::NotSupported("Only WriteCommittedTxn supports timestamp")", also for column
        // families created after the open.
        using TransactionDatabase db = Open(NewFs(), policy);
        var e = Assert.ThrowsExactly<RocksDbException>(
            () => db.Database.CreateColumnFamily("ts", new ColumnFamilyOptions { Comparator = Comparator.BytewiseWithU64Ts }));
        Assert.AreEqual(StatusCode.NotSupported, e.Code, e.Message);
        ColumnFamily plain = db.Database.CreateColumnFamily("plain", new ColumnFamilyOptions());
        plain.Put(B("k"), B("v"));
        Assert.AreEqual("v", S(plain.Get(B("k"))));
    }
}
