using System.Buffers;
using RocksDb.Managed.Db;
using RocksDb.Managed.Tests.Db;
using RocksDb.Managed.Utilities.Transactions;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #14: regression tests for the second review pass over the areas audit #13 left unread (options, open, read
/// path lifetimes, transactions), each compared with the C++ sources (docs/validation.md "Audit #14").
/// </summary>
[TestClass]
public sealed class Audit14ReviewTests
{
    private static readonly WriteOptions s_wo = new();

    [TestMethod]
    public void RolledBackExpirableTransactionStaysKnownToTheLockManager()
    {
        // pessimistic_transaction.cc: only the destructor (and Reinitialize) remove a transaction from
        // expirable_transactions_map_; Rollback from STARTED calls Clear() and the transaction stays usable with the
        // same id and expiration. A transaction missing from the map reads as "gone, steal", without the
        // STARTED -> LOCKS_STOLEN exchange that fails while the owner commits and that tells the owner it lost its locks.
        var options = new TransactionDbOptions { TransactionLockTimeout = TimeSpan.FromMilliseconds(20), DefaultLockTimeout = TimeSpan.FromMilliseconds(20) };
        using TransactionDatabase db = TransactionDatabase.Open("/audit14txn", NewOptions(NewFs()), options);
        using Transaction t1 = db.BeginTransaction(s_wo, new TransactionOptions { Expiration = TimeSpan.FromMilliseconds(150) });
        t1.Put(B("a"), B("1"));
        t1.Rollback();
        t1.Put(B("k"), B("1"));
        SpinWait.SpinUntil(() => false, 300); // past the expiration
        using (Transaction t2 = db.BeginTransaction(s_wo))
        {
            t2.Put(B("k"), B("2")); // steals the expired lock
            t2.Commit();
        }
        Assert.AreEqual(TransactionState.LocksStolen, t1.State);
        var e = Assert.ThrowsExactly<RocksDbException>(t1.Commit);
        Assert.AreEqual(StatusCode.Expired, e.Code, e.Message);
        Assert.AreEqual("2", GetString(db.Database, "k"));
    }

    /// <summary>Appends its operands; while merging "outer" it reads another key that itself needs a merge.</summary>
    private sealed class ReadingMergeOperator : MergeOperator
    {
        public RocksDatabase? Database;

        public override string Name => "ReadingAppend";

        public override bool FullMerge(ReadOnlySpan<byte> key, bool hasExistingValue, ReadOnlySpan<byte> existingValue, in MergeOperands operands,
            IBufferWriter<byte> result)
        {
            if (key.SequenceEqual("outer"u8)) _ = Database!.Get(B("inner")); // a nested merge on this thread
            if (hasExistingValue) result.Write(existingValue);
            for (int i = 0; i < operands.Count; i++)
            {
                result.Write("|"u8);
                result.Write(operands[i]);
            }
            return true;
        }
    }

    [TestMethod]
    public void MergeOperatorThatReadsTheDbKeepsItsOperands()
    {
        // The MergeContext of a read is a stack object upstream. The port resolved every merge through one per-thread
        // operand buffer without taking it out of its slot: a merge operator that read the DB (a nested merge on the
        // same thread) cleared and refilled the buffer the outer operands pointed into.
        var op = new ReadingMergeOperator();
        var cfo = new ColumnFamilyOptions { MergeOperator = op };
        using RocksDatabase db = RocksDatabase.Open("/audit14merge", NewOptions(NewFs()),
            [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, cfo)]);
        op.Database = db;
        for (int i = 0; i < 4; i++) db.Merge(B("outer"), B("o" + i));
        // Long operands: the nested merge grows the buffer (the old arrays go back to the pool).
        for (int i = 0; i < 6; i++) db.Merge(B("inner"), B(new string((char)('A' + i), 2000)));
        Assert.AreEqual("|o0|o1|o2|o3", GetString(db, "outer"));
    }

    [TestMethod]
    public void ReverseIteratorDoesNotKeepAPointerIntoAnInPlaceUpdatedValue()
    {
        // MemTableIterator: value_pinned_(!inplace_update_support). With in-place updates a later Put rewrites the
        // length and the bytes where the value lies; the port reported memtable values as pinned whatever the option,
        // so a reverse iterator's saved value showed a mix of the old and the new bytes.
        var cfo = new ColumnFamilyOptions { InplaceUpdateSupport = true };
        DbOptions o = NewOptions(NewFs());
        o.AllowConcurrentMemtableWrite = false;
        using RocksDatabase db = RocksDatabase.Open("/audit14inplace", o, [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, cfo)]);
        db.Put(B("k"), B("AAAAAAAAAA"));
        using Iterator it = db.NewIterator(new ReadOptions());
        it.SeekToLast();
        Assert.IsTrue(it.Valid);
        db.Put(B("k"), B("bb")); // updates the value in place
        Assert.AreEqual("AAAAAAAAAA", S(it.Value.ToArray()));
    }

    [TestMethod]
    [DataRow(TransactionWritePolicy.WritePrepared)]
    [DataRow(TransactionWritePolicy.WriteUnprepared)]
    public void TransactionMultiGetReadsAllKeysAtOneView(TransactionWritePolicy policy)
    {
        // WritePreparedTxn::MultiGet / WriteUnpreparedTxn::MultiGet -> DBImpl::MultiGetWithCallback: one super version,
        // one sequence, the callback refreshed once. The port read key by key, each at its own sequence, so a commit
        // between two keys showed in the second key only.
        using TransactionDatabase db = TransactionDatabase.Open("/audit14multiget", NewOptions(NewFs()), new TransactionDbOptions { WritePolicy = policy });
        db.Database.Put(B("a"), B("old"));
        db.Database.Put(B("b"), B("old"));
        using Transaction txn = db.BeginTransaction(s_wo);
        int reads = 0;
        TestHooks.Set("DBImpl::GetImpl:AfterSuperVersion", () =>
        {
            // Before the second key is read: another writer commits both keys.
            if (Interlocked.Increment(ref reads) != 2) return;
            using WriteBatch wb = new();
            wb.Put(B("a"), B("new"));
            wb.Put(B("b"), B("new"));
            db.Database.Write(s_wo, wb);
        });
        try
        {
            using var batch = new MultiGetBatch();
            batch.Add(B("a"));
            batch.Add(B("b"));
            txn.MultiGet(null, batch);
            Assert.IsTrue(batch.TryGetValue(0, out ReadOnlySpan<byte> a));
            string sa = S(a.ToArray())!;
            Assert.IsTrue(batch.TryGetValue(1, out ReadOnlySpan<byte> b));
            Assert.AreEqual(sa, S(b.ToArray()), "the two keys were read at different views");
            Assert.IsTrue(Volatile.Read(ref reads) >= 2, "the hook did not run");
        }
        finally
        {
            TestHooks.ClearAll();
        }
        Assert.AreEqual("new", GetString(db.Database, "a"));
    }

    [TestMethod]
    [Timeout(60_000, CooperativeCancellation = true)]
    public void EnablingParanoidFileChecksDuringACompactionDoesNotFailIt()
    {
        // CompactionJob captures paranoid_file_checks in its constructor and reads compaction->mutable_cf_options(), a
        // copy taken at pick time. The port read the live options again at the end: after a SetOptions in between, the
        // outputs written without a key/value hash were compared with a freshly computed one - "Key-value checksum of
        // compaction output doesn't match", a Corruption that stops the DB.
        using RocksDatabase db = RocksDatabase.Open("/audit14paranoid", NewOptions(NewFs()));
        for (int f = 0; f < 3; f++)
        {
            for (int i = 0; i < 200; i++) db.Put(B("key" + i.ToString("D4")), B("v" + f));
            db.Flush();
        }
        int hooked = 0;
        TestHooks.Set("CompactionJob::VerifyOutputFiles:Start", () =>
        {
            if (Interlocked.Exchange(ref hooked, 1) == 0)
                db.DefaultColumnFamily.SetOptions([new KeyValuePair<string, string>("paranoid_file_checks", "true")]);
        });
        try
        {
            db.CompactRange(null, null);
        }
        finally
        {
            TestHooks.ClearAll();
        }
        Assert.AreEqual(1, hooked, "the compaction wrote no output");
        Assert.IsNull(db.BackgroundError);
        Assert.AreEqual("v2", GetString(db, "key0007"));
    }

    private sealed class CountingListener : EventListener
    {
        public int BackgroundErrors;
        public override void OnBackgroundError(BackgroundErrorEventArgs e) => Interlocked.Increment(ref BackgroundErrors);
    }

    [TestMethod]
    public void ListenersHearAboutBackgroundErrorsThatDoNotStopTheDb()
    {
        // ErrorHandler::SetBGError: "if (!new_bg_err.ok()) NotifyOnBackgroundError" - a status classified kNoError
        // (here: a flush error with paranoid_checks off) is still not OK, so the listeners are told; it does not become
        // the background error. The port returned before notifying.
        var fs = new FaultInjectionFileSystem(NewFs());
        var listener = new CountingListener();
        var o = new DbOptions { CreateIfMissing = true, FileSystem = fs, ParanoidChecks = false };
        o.Listeners.Add(listener);
        using RocksDatabase db = RocksDatabase.Open("/audit14listener", o);
        db.Put(B("a"), B("1"));
        fs.FailMatching(".sst", FaultOps.Append, 1, InjectedFaultKind.IOError);
        try { db.Flush(); } catch (RocksDbException) { }
        db.Impl.WaitForBackgroundWorkForTest();
        Assert.IsTrue(listener.BackgroundErrors >= 1, "OnBackgroundError was not raised");
        Assert.IsNull(db.BackgroundError);
    }

    [TestMethod]
    [Timeout(120_000, CooperativeCancellation = true)]
    public void NewColumnFamilyIsNeverVisibleWithoutItsSuperVersion()
    {
        // DBImpl::CreateColumnFamilyImpl holds the mutex across LogAndApply and the super version install. The port
        // installed the memtable and the super version in a later critical section, so a reader that found the new
        // column family in db.ColumnFamilies in between dereferenced a null super version.
        using RocksDatabase db = RocksDatabase.Open("/audit14cf", NewOptions(NewFs()));
        var failures = new System.Collections.Concurrent.ConcurrentQueue<string>();
        int stop = 0;
        var readers = new Thread[3];
        for (int t = 0; t < readers.Length; t++)
        {
            readers[t] = new Thread(() =>
            {
                while (Volatile.Read(ref stop) == 0)
                {
                    try
                    {
                        foreach (ColumnFamily cf in db.ColumnFamilies) _ = cf.Get(B("x"));
                        _ = db.GetLiveFilesMetadata();
                    }
                    catch (RocksDbException e) when (e.Code == StatusCode.ColumnFamilyDropped)
                    {
                    }
                    catch (Exception e)
                    {
                        failures.Enqueue(e.GetType().Name + ": " + e.Message);
                        return;
                    }
                }
            }) { IsBackground = true };
            readers[t].Start();
        }
        for (int i = 0; i < 300 && failures.IsEmpty; i++)
        {
            ColumnFamily cf = db.CreateColumnFamily("cf" + i, new ColumnFamilyOptions());
            db.DropColumnFamily(cf);
        }
        Volatile.Write(ref stop, 1);
        foreach (Thread r in readers) Assert.IsTrue(r.Join(30_000));
        Assert.IsTrue(failures.IsEmpty, string.Join(" | ", failures.Take(3)));
    }
}
