using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Utilities.Transactions;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4, slice WritePrepared: WritePrepared / WriteUnprepared transactions, the commit cache bookkeeping and the
/// two write queues, checked against utilities/transactions/write_prepared_txn*.cc and write_unprepared_txn*.cc.
/// </summary>
[TestClass]
public sealed class Audit4WritePreparedTests
{
    private static readonly WriteOptions s_wo = new();

    private static TransactionDatabase Open(MemoryFileSystem fs, TransactionWritePolicy policy, bool twoWriteQueues = false, int commitCacheBits = 10,
        Comparator? comparator = null)
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

    private static string TxnGet(Transaction t, string key) => S(t.Get(B(key))) ?? "NOT_FOUND";

    private static string Scan(Iterator it)
    {
        var parts = new List<string>();
        for (it.SeekToFirst(); it.Valid; it.Next()) parts.Add(S(it.Key.ToArray()) + "=" + S(it.Value.ToArray()));
        return string.Join(",", parts);
    }

    // ---- findings ------------------------------------------------------------------------------------------

    [TestMethod]
    public void WriteUnpreparedRollbackToSavePointRestoresUntrackedKeys()
    {
        // WriteUnpreparedTxn::WriteRollbackKeys (write_unprepared_txn.cc) always restores untracked_keys_ after the
        // tracked ones, also when called by RollbackToSavePointInternal. With skip_concurrency_control
        // PessimisticTransaction::TryLock returns before TrackKey, so every key of the transaction is untracked and the
        // save point's new_locks_ is empty: the unprepared batches written after the save point can only be undone
        // through untracked_keys_ (each key gets the value visible at the save point's snapshot with its unprep_seqs).
        using TransactionDatabase db = Open(NewFs(), TransactionWritePolicy.WriteUnprepared);
        db.Put(B("x"), B("x0"));
        using Transaction txn = db.BeginTransaction(s_wo, new TransactionOptions { WriteBatchFlushThreshold = 1, SkipConcurrencyControl = true });
        txn.SetName("sp");
        txn.Put(B("a"), B("a1"));
        txn.SetSavePoint();
        txn.Put(B("x"), B("x1")); // flushes [a]: the save point becomes a flushed save point
        txn.Put(B("b"), B("b1")); // flushes [x]
        txn.Put(B("c"), B("c1")); // flushes [b]
        Assert.AreEqual(3, ((WriteUnpreparedTxn)txn).UnpreparedSequenceNumbers.Count);
        Assert.AreEqual("x1", TxnGet(txn, "x"));
        Assert.IsTrue(txn.RollbackToSavePoint());
        Assert.AreEqual("a1", TxnGet(txn, "a"));
        Assert.AreEqual("x0", TxnGet(txn, "x"));
        Assert.AreEqual("NOT_FOUND", TxnGet(txn, "b"));
        Assert.AreEqual("NOT_FOUND", TxnGet(txn, "c"));
        txn.Commit();
        Assert.AreEqual("a1", Get(db, "a"));
        Assert.AreEqual("x0", Get(db, "x"));
        Assert.AreEqual("NOT_FOUND", Get(db, "b"));
        Assert.AreEqual("NOT_FOUND", Get(db, "c"));
    }

    [TestMethod]
    public void WriteUnpreparedIteratorAfterFlushWithTwoWriteQueues()
    {
        // WriteUnpreparedTxn::HandleWrite: without a snapshot largest_validated_seq_ = GetLastPublishedSequence(), and
        // WriteUnpreparedTxnDB::NewIterator refuses the iterator only if largest_validated_seq_ > snapshot seq. With
        // two_write_queues the unprepared batch written by the main queue advances LastSequence but is not published,
        // so the iterator's own snapshot (taken at the published sequence) is not older than the validated writes.
        using TransactionDatabase db = Open(NewFs(), TransactionWritePolicy.WriteUnprepared, twoWriteQueues: true);
        db.Put(B("k0"), B("v0"));
        using Transaction txn = db.BeginTransaction(s_wo, new TransactionOptions { WriteBatchFlushThreshold = 1 });
        txn.SetName("it");
        txn.Put(B("a"), B("a1"));
        txn.Put(B("b"), B("b1")); // flushes [a] as an unprepared batch
        Assert.AreEqual(1, ((WriteUnpreparedTxn)txn).UnpreparedSequenceNumbers.Count);
        DbImpl impl = db.Database.Impl;
        Assert.IsTrue(impl.Versions.LastSequence > impl.LastPublishedSequence, "the unprepared batch is not published");
        using (TransactionIterator it = txn.NewIterator(new ReadOptions()))
        {
            var parts = new List<string>();
            for (it.SeekToFirst(); it.Valid; it.Next()) parts.Add(S(it.Key.ToArray()) + "=" + S(it.Value.ToArray()));
            Assert.AreEqual("a=a1,b=b1,k0=v0", string.Join(",", parts));
        }
        txn.Commit();
        using Iterator check = db.Database.NewIterator();
        Assert.AreEqual("a=a1,b=b1,k0=v0", Scan(check));
    }

    /// <summary>Keys that differ only in ASCII case are equal (a comparator for which different bytes can be equal).</summary>
    private sealed class CaseInsensitiveComparator : Comparator
    {
        public override string Name => "audit4.CaseInsensitiveComparator";

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

    [TestMethod]
    public void WritePreparedRollbackDeduplicatesKeysWithTheColumnFamilyComparator()
    {
        // WritePreparedTxn::RollbackInternal's RollbackWriteBatchBuilder keeps the keys it already restored in a
        // std::set<Slice, SetComparator> built from the column family's comparator, so two keys of the prepared batch
        // that the comparator considers equal produce ONE entry of the rollback batch, which is written with
        // batch_cnt = ONE_BATCH. Two entries for the same user key would need two sub-batches (sequence numbers).
        using TransactionDatabase db = Open(NewFs(), TransactionWritePolicy.WritePrepared, comparator: new CaseInsensitiveComparator());
        db.Put(B("k"), B("old"));
        using Transaction txn = db.BeginTransaction(s_wo);
        txn.SetName("ci");
        txn.Put(B("K"), B("n1"));
        txn.Put(B("k"), B("n2"));
        ulong before = db.Database.GetLatestSequenceNumber();
        txn.Prepare();
        // The prepared batch has a duplicate key: two sub-batches.
        Assert.AreEqual(before + 2, db.Database.GetLatestSequenceNumber());
        txn.Rollback();
        // One rollback batch of one sub-batch.
        Assert.AreEqual(before + 3, db.Database.GetLatestSequenceNumber());
        Assert.AreEqual("old", Get(db, "k"));
        Assert.AreEqual("old", Get(db, "K"));
        // The sequence numbers stay consistent: later writes are visible.
        db.Put(B("k"), B("newer"));
        Assert.AreEqual("newer", Get(db, "K"));
    }

    // ---- extra coverage ------------------------------------------------------------------------------------

    [TestMethod]
    public void IsInSnapshotUsesDelayedPreparedCommitsAfterEviction()
    {
        // WritePreparedTxnDB::IsInSnapshot / AddCommitted (write_prepared_txn_db.cc): a delayed prepared entry whose
        // commit entry was evicted from the commit cache before RemovePrepared is answered from
        // delayed_prepared_commits_; once it is removed and nothing overlaps, a snapshot at or below max_evicted_seq
        // with no old_commit_map_ entry reports snap_released.
        using TransactionDatabase db = Open(NewFs(), TransactionWritePolicy.WritePrepared, commitCacheBits: 1);
        WritePreparedTxnDb wp = db.WritePreparedDb!;
        const ulong min = ReadCallback.MinUnCommittedSeq;
        bool released = false;
        wp.AddPrepared(10, locked: false);
        wp.AdvanceMaxEvictedSeq(wp.MaxEvictedSeq, 20);
        Assert.AreEqual(1, wp.DelayedPreparedCount);
        Assert.IsFalse(wp.IsInSnapshot(10, 30, min, ref released)); // delayed prepared, not committed
        wp.AddCommitted(10, 25);
        Assert.IsTrue(wp.IsInSnapshot(10, 30, min, ref released));  // commit cache hit
        Assert.IsFalse(wp.IsInSnapshot(10, 24, min, ref released));
        // 12 shares the slot of 10 (cache of 2 entries): <10, 25> is evicted, max_evicted_seq advances to 25 (the
        // evicted commit is not published) and delayed_prepared_commits_[10] = 25.
        wp.AddCommitted(12, 26);
        Assert.IsFalse(wp.TryGetCommitEntry(10, out _));
        Assert.AreEqual(25UL, wp.MaxEvictedSeq);
        Assert.IsTrue(wp.IsInSnapshot(10, 30, min, ref released));
        Assert.IsFalse(wp.IsInSnapshot(10, 24, min, ref released));
        Assert.IsFalse(released);
        // min_uncommitted short-cut: below it everything is committed.
        Assert.IsTrue(wp.IsInSnapshot(10, 24, 11, ref released));
        wp.RemovePrepared(10, 1);
        Assert.AreEqual(0, wp.DelayedPreparedCount);
        Assert.IsTrue(wp.IsInSnapshot(10, 30, min, ref released)); // max_evicted_seq < snapshot: committed
        Assert.IsFalse(released);
        // snapshot <= max_evicted_seq and the old commit map is empty: the snapshot must have been released.
        Assert.IsTrue(wp.IsInSnapshot(10, 24, min, ref released));
        Assert.IsTrue(released);
    }

    [TestMethod]
    public void UnbackedReadBehindMaxEvictedSeqIsTryAgain()
    {
        // WritePreparedTxnDB::ValidateSnapshot / Get: a read that is not backed by a DB snapshot is invalid once
        // max_evicted_seq reached its read sequence (TryAgain, TXN_GET_TRY_AGAIN); a snapshot-backed read never is.
        using TransactionDatabase db = Open(NewFs(), TransactionWritePolicy.WritePrepared, commitCacheBits: 1);
        WritePreparedTxnDb wp = db.WritePreparedDb!;
        ReadCallback cb = wp.BeginRead(null);
        cb.Refresh(5); // DBImpl::GetImpl: the read sequence, assigned after the super version was referenced
        Assert.IsTrue(wp.ValidateSnapshot(5, SnapshotBackup.UnbackedByDbSnapshot));
        wp.AdvanceMaxEvictedSeq(wp.MaxEvictedSeq, 5);
        Assert.IsFalse(wp.ValidateSnapshot(5, SnapshotBackup.UnbackedByDbSnapshot));
        Assert.IsTrue(wp.ValidateSnapshot(6, SnapshotBackup.UnbackedByDbSnapshot));
        Assert.IsTrue(wp.ValidateSnapshot(0, SnapshotBackup.UnbackedByDbSnapshot)); // snap_seq == 0 is always valid
        Assert.IsTrue(wp.ValidateSnapshot(5, SnapshotBackup.BackedByDbSnapshot));
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => wp.EndRead(cb, validate: true));
        Assert.AreEqual(StatusCode.TryAgain, ex.Code);
    }

    [TestMethod]
    public void EmptyCommitWithoutPrepareConsumesNoSequenceNumber()
    {
        // WritePreparedTxnDB::WriteInternal: "if (batch->Count() == 0) return Status::OK()" -- otherwise the one
        // sequence per batch logic breaks. Holds for an empty transaction and an empty batch written through the DB,
        // with one and with two write queues.
        foreach (bool twoWriteQueues in new[] { false, true })
        {
            using TransactionDatabase db = Open(NewFs(), TransactionWritePolicy.WritePrepared, twoWriteQueues);
            db.Put(B("a"), B("1"));
            DbImpl impl = db.Database.Impl;
            ulong allocated = impl.Versions.LastAllocatedSequence;
            ulong published = impl.LastPublishedSequence;
            using (Transaction txn = db.BeginTransaction(s_wo)) txn.Commit();
            using (var empty = new WriteBatch()) db.Database.Write(empty);
            Assert.AreEqual(allocated, impl.Versions.LastAllocatedSequence);
            Assert.AreEqual(published, impl.LastPublishedSequence);
            Assert.AreEqual("1", Get(db, "a"));
        }
    }
}
