using RocksDb.Managed.Db;
using RocksDb.Managed.Utilities.Transactions;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4, transactions slice: regression tests for divergences from utilities/transactions/*.cc found by reading
/// the port side by side with the C++ sources, plus coverage of branches that had no test.
/// </summary>
[TestClass]
public sealed class Audit4TransactionsTests
{
    private const string DbPath = "/audit4txn";
    private static readonly WriteOptions s_wo = new();
    private static readonly TimeSpan s_short = TimeSpan.FromMilliseconds(20);

    private static TransactionDatabase Open(TransactionDbOptions? options = null)
    {
        options ??= new TransactionDbOptions();
        options.TransactionLockTimeout = s_short;
        options.DefaultLockTimeout = s_short;
        return TransactionDatabase.Open(DbPath, NewOptions(NewFs()), options);
    }

    private static void AssertFails(StatusCode code, StatusSubCode sub, Action a)
    {
        var e = Assert.ThrowsExactly<RocksDbException>(a);
        Assert.AreEqual(code, e.Code, e.Message);
        Assert.AreEqual(sub, e.SubCode, e.Message);
    }

    // ---- findings --------------------------------------------------------------------------------------------

    [TestMethod]
    public void SetSnapshotOnNextOperationIsNotTriggeredByReads()
    {
        // TransactionBaseImpl::SetSnapshotIfNeeded is only called from TryLock (PessimisticTransaction::TryLock,
        // utilities/transactions/pessimistic_transaction.cc; OptimisticTransaction::TryLock): Get / MultiGet / GetIterator
        // (transaction_base.cc) never take the pending snapshot. transaction_test.cc
        // SetSnapshotOnNextOperationWithNotification: "A Get does not generate the snapshot". The port took it at the
        // first read, so a key written by someone else between that read and the transaction's first write conflicted.
        using TransactionDatabase db = Open();
        using Transaction txn = db.BeginTransaction(s_wo);
        txn.SetSnapshotOnNextOperation();
        Assert.IsNull(txn.Snapshot);
        db.Put(B("B"), B("1"));

        Assert.AreEqual("1", S(txn.Get(B("B"))));
        Assert.IsNull(txn.Snapshot, "Get");
        using (TransactionIterator it = txn.NewIterator(null))
        {
            it.SeekToFirst();
            Assert.IsTrue(it.Valid);
        }
        Assert.IsNull(txn.Snapshot, "NewIterator");
        using (var multi = new MultiGetBatch())
        {
            multi.Add(B("B"));
            txn.MultiGet(null, multi);
            Assert.IsTrue(multi.TryGetValue(0, out _));
        }
        Assert.IsNull(txn.Snapshot, "MultiGet");
        var columns = new WideColumns();
        Assert.IsTrue(txn.TryGetEntity(null, B("B"), columns));
        Assert.IsNull(txn.Snapshot, "GetEntity");

        // Written after the reads but before the transaction's first write: the snapshot is taken by the write, so
        // this is not a conflict.
        db.Put(B("A"), B("a2"));
        txn.Put(B("A"), B("a3"));
        Assert.IsNotNull(txn.Snapshot);
        txn.Commit();
        Assert.AreEqual("a3", GetString(db.Database, "A"));
    }

    [TestMethod]
    public void OptimisticSetSnapshotOnNextOperationIsNotTriggeredByReads()
    {
        // OptimisticTransaction::TryLock (optimistic_transaction.cc) is the only caller of SetSnapshotIfNeeded: the
        // tracked sequence of a key is the snapshot taken at the first tracked operation, not at an earlier Get.
        using OptimisticTransactionDatabase db = OptimisticTransactionDatabase.Open(DbPath, NewOptions(NewFs()));
        using Transaction txn = db.BeginTransaction(s_wo);
        txn.SetSnapshotOnNextOperation();
        db.Database.Put(B("B"), B("1"));
        Assert.AreEqual("1", S(txn.Get(B("B"))));
        Assert.IsNull(txn.Snapshot);
        db.Database.Put(B("A"), B("a2"));
        Assert.AreEqual("a2", S(txn.GetForUpdate(B("A"))));
        Assert.IsNotNull(txn.Snapshot);
        txn.Put(B("A"), B("a3"));
        txn.Commit(); // A was written before the snapshot: no conflict
        Assert.AreEqual("a3", GetString(db.Database, "A"));
    }

    [TestMethod]
    public void UndoGetForUpdateOfAKeyOnlyWrittenInTheSavePointIsIgnored()
    {
        // PointLockTracker::Untrack (lock/point/point_lock_tracker.cc): a read-only untrack of a key with num_reads == 0
        // (and writes) returns NOT_TRACKED, so TransactionBaseImpl::UndoGetForUpdate (transaction_base.cc: "If there is no
        // GetForUpdate of the key in this save point, then cannot untrack from the global lock tracker") leaves the
        // global tracker alone. The port returned UNTRACKED and dropped the read tracked before the save point, so the
        // rollback to the save point untracked the key and released the lock taken by the earlier GetForUpdate.
        using TransactionDatabase db = Open();
        using Transaction txn = db.BeginTransaction(s_wo);
        Assert.IsNull(txn.GetForUpdate(B("k")));
        txn.SetSavePoint();
        txn.Put(B("k"), B("v"));
        txn.UndoGetForUpdate(B("k"));
        Assert.IsTrue(txn.RollbackToSavePoint());
        Assert.AreEqual(1, txn.NumKeys);
        using (Transaction other = db.BeginTransaction(s_wo))
        {
            AssertFails(StatusCode.TimedOut, StatusSubCode.LockTimeout, () => other.Put(B("k"), B("x")));
        }
        // Without a save point the GetForUpdate can be undone, which releases the lock.
        txn.UndoGetForUpdate(B("k"));
        Assert.AreEqual(0, txn.NumKeys);
        using (Transaction other = db.BeginTransaction(s_wo))
        {
            other.Put(B("k"), B("x"));
            other.Commit();
        }
    }

    [TestMethod]
    public void DirectWritesLockKeysEvenWithSkipConcurrencyControl()
    {
        // PessimisticTransactionDB::Put / Delete / SingleDelete / Merge (pessimistic_transaction_db.cc) always go through
        // an internal transaction and *Untracked (the key is locked); only Write(batch) consults
        // TransactionDBOptions::skip_concurrency_control (WriteCommittedTxnDB::Write). The port wrote directly, ignoring
        // the lock another transaction holds on the key.
        using TransactionDatabase db = Open(new TransactionDbOptions { SkipConcurrencyControl = true });
        using Transaction txn = db.BeginTransaction(s_wo);
        txn.Put(B("k"), B("txn"));
        AssertFails(StatusCode.TimedOut, StatusSubCode.LockTimeout, () => db.Put(B("k"), B("direct")));
        AssertFails(StatusCode.TimedOut, StatusSubCode.LockTimeout, () => db.Delete(B("k")));
        AssertFails(StatusCode.TimedOut, StatusSubCode.LockTimeout, () => db.PutAsync(B("k"), B("direct")).AsTask().GetAwaiter().GetResult());
        db.Put(B("other"), B("1")); // an unlocked key is written
        // Write(batch) skips concurrency control, as upstream.
        using (var batch = new WriteBatch())
        {
            batch.Put(B("k"), B("batch"));
            db.Write(s_wo, batch);
        }
        Assert.AreEqual("batch", GetString(db.Database, "k"));
        txn.Commit();
        Assert.AreEqual("txn", GetString(db.Database, "k"));
        Assert.AreEqual("1", GetString(db.Database, "other"));
    }

    [TestMethod]
    [DataRow(TransactionWritePolicy.WritePrepared)]
    [DataRow(TransactionWritePolicy.WriteUnprepared)]
    public void OpenRejectsTimestampedColumnFamiliesForNonWriteCommittedPolicies(TransactionWritePolicy policy)
    {
        // PessimisticTransactionDB::Initialize -> VerifyCFOptions (pessimistic_transaction_db.cc): a column family whose
        // comparator has a timestamp fails TransactionDB::Open with NotSupported("Only WriteCommittedTxn supports
        // timestamp") unless the write policy is WRITE_COMMITTED. The port opened the DB and only failed later, at the
        // first transaction operation on that column family.
        var options = new TransactionDbOptions { WritePolicy = policy, WpCommitCacheBits = 10 };
        DbOptions dbOptions = NewOptions(NewFs());
        dbOptions.CreateMissingColumnFamilies = true;
        var e = Assert.ThrowsExactly<RocksDbException>(() => TransactionDatabase.Open(DbPath, dbOptions, options,
            [
                new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions()),
                new ColumnFamilyDescriptor("ts", new ColumnFamilyOptions { Comparator = Comparator.BytewiseWithU64Ts }),
            ]));
        Assert.AreEqual(StatusCode.NotSupported, e.Code, e.Message);
    }

    [TestMethod]
    public void ReusedTransactionTakesTheNewMaxWriteBatchSize()
    {
        // PessimisticTransaction::Initialize (pessimistic_transaction.cc), also run by Reinitialize for
        // BeginTransaction(..., old_txn): write_batch_.SetMaxBytes(txn_options.max_write_batch_size). The port fixed the
        // limit when the transaction object was created, so a reused transaction kept the previous limit.
        using TransactionDatabase db = Open();
        byte[] big = new byte[400];
        Transaction txn = db.BeginTransaction(s_wo, new TransactionOptions());
        try
        {
            txn.Put(B("a"), big);
            txn.Rollback();
            Assert.AreSame(txn, db.BeginTransaction(s_wo, new TransactionOptions { MaxWriteBatchSize = 100 }, txn));
            txn.Put(B("small"), B("1"));
            AssertFails(StatusCode.Aborted, StatusSubCode.MemoryLimit, () => txn.Put(B("a"), big));
            txn.Commit(); // the write that did not fit is not part of the batch
            Assert.AreEqual("1", GetString(db.Database, "small"));
            Assert.IsNull(GetString(db.Database, "a"));
            // ... and loses it again when reused without a limit.
            Assert.AreSame(txn, db.BeginTransaction(s_wo, new TransactionOptions(), txn));
            txn.Put(B("a"), big);
            txn.Commit();
            Assert.AreEqual(400, db.Database.Get(B("a"))!.Length);
        }
        finally
        {
            txn.Dispose();
        }
    }

    [TestMethod]
    [Ignore("audit4: needs timestamped snapshots (DBImpl::CreateTimestampedSnapshot / SnapshotCreationCallback), not ported")]
    public void CommitWithPendingSnapshotRequestNeedsACommitTimestamp()
    {
        // WriteCommittedTxn::CommitWithoutPrepareInternal / CommitInternal (pessimistic_transaction.cc): "if
        // (snapshot_needed_) { if (commit_timestamp_ == kMaxTxnTimestamp) return Status::InvalidArgument("Must set
        // transaction commit timestamp"); else post_mem_cb = &snapshot_creation_cb; }" -- a snapshot requested with
        // SetSnapshotOnNextOperation and still pending at Commit (no write / GetForUpdate took it) is created by the
        // commit as a timestamped snapshot, which requires a commit timestamp. The port commits and takes no snapshot.
        using TransactionDatabase db = Open();
        using Transaction txn = db.BeginTransaction(s_wo);
        txn.SetSnapshotOnNextOperation();
        Assert.IsNull(txn.Get(B("k")));
        var e = Assert.ThrowsExactly<RocksDbException>(txn.Commit);
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code, e.Message);
    }

    // ---- coverage of untested branches -------------------------------------------------------------------------

    [TestMethod]
    public void LockLimitIsAbortedAndLocksAreCountedPerKey()
    {
        // PointLockManager::AcquireLocked (lock/point/point_lock_manager.cc): a new key beyond max_num_locks fails with
        // Status::LockLimit() = Aborted(kLockLimit); re-locking a key the transaction already holds does not count, and
        // UnLockKey gives the slot back (transaction_test.cc LockLimitTest).
        using TransactionDatabase db = Open(new TransactionDbOptions { MaxNumLocks = 2 });
        var noWait = new TransactionOptions { LockTimeout = TimeSpan.Zero };
        using Transaction txn = db.BeginTransaction(s_wo, noWait);
        txn.Put(B("a"), B("1"));
        txn.Put(B("b"), B("1"));
        txn.Put(B("a"), B("2")); // already locked by this transaction
        AssertFails(StatusCode.Aborted, StatusSubCode.LockLimit, () => txn.Put(B("c"), B("1")));
        using Transaction other = db.BeginTransaction(s_wo, noWait);
        AssertFails(StatusCode.Aborted, StatusSubCode.LockLimit, () => other.GetForUpdate(B("d")));
        // A held key is reported as a lock timeout, not as the lock limit.
        AssertFails(StatusCode.TimedOut, StatusSubCode.LockTimeout, () => other.Put(B("a"), B("x")));
        txn.Commit();
        other.Put(B("c"), B("1"));
        other.Put(B("d"), B("1"));
        AssertFails(StatusCode.Aborted, StatusSubCode.LockLimit, () => other.Put(B("e"), B("1")));
        other.Commit();
        Assert.AreEqual("2", GetString(db.Database, "a"));
        Assert.AreEqual("1", GetString(db.Database, "d"));
        Assert.IsNull(GetString(db.Database, "e"));
    }

    [TestMethod]
    public void PopSavePointHandsItsLocksToTheSavePointBelow()
    {
        // TransactionBaseImpl::PopSavePoint (transaction_base.cc): "If there is another savepoint A below the current
        // savepoint B, then A needs to inherit tracked_keys in B so that if we rollback to savepoint A, we remember to
        // unlock keys in B. If there is no other savepoint below, then we can safely discard savepoint info."
        using TransactionDatabase db = Open();
        using Transaction txn = db.BeginTransaction(s_wo);
        Assert.IsFalse(txn.PopSavePoint());
        txn.SetSavePoint(); // A
        txn.Put(B("a"), B("1"));
        txn.SetSavePoint(); // B
        txn.Put(B("b"), B("1"));
        Assert.IsTrue(txn.PopSavePoint()); // B is gone, its write stays
        Assert.AreEqual("1", S(txn.Get(B("b"))));
        Assert.AreEqual(2, txn.NumKeys);
        Assert.IsTrue(txn.RollbackToSavePoint()); // to A: both writes are undone and both keys unlocked
        Assert.AreEqual(0, txn.NumKeys);
        Assert.IsNull(txn.Get(B("a")));
        Assert.IsNull(txn.Get(B("b")));
        using (Transaction other = db.BeginTransaction(s_wo))
        {
            other.Put(B("a"), B("x"));
            other.Put(B("b"), B("x"));
            other.Rollback();
        }
        // The only save point popped: its key stays written, tracked and locked.
        txn.SetSavePoint();
        txn.Put(B("c"), B("1"));
        Assert.IsTrue(txn.PopSavePoint());
        Assert.IsFalse(txn.RollbackToSavePoint());
        Assert.AreEqual(1, txn.NumKeys);
        using (Transaction other = db.BeginTransaction(s_wo))
        {
            AssertFails(StatusCode.TimedOut, StatusSubCode.LockTimeout, () => other.Put(B("c"), B("x")));
        }
        txn.Commit();
        Assert.AreEqual("1", GetString(db.Database, "c"));
    }
}
