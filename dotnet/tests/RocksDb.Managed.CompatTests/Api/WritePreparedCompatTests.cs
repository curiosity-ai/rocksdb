using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Env;
using RocksDb.Managed.Utilities.Transactions;
using RocksDb.Managed.Wal;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>
/// WritePrepared / WriteUnprepared TransactionDB compatibility with native librocksdb 11.12 (policy selected through
/// rocksdb_transactiondb_options_set_write_policy): committed, rolled back and prepared transactions (including
/// duplicate-key sub-batches and WriteUnprepared transactions with several unprepared batches) written by native
/// RocksDB recover identically in the managed port and vice versa; the WAL records of the same operations are
/// byte-identical; a DB whose WAL holds prepared sections of one policy is refused by the other policies on both sides.
/// Each case also runs with two_write_queues on both sides (rocksdb_options_set_two_write_queues), where commits without
/// prepare and rollbacks take a second write on the WAL-only queue (a rollback adds a PutLogData("") + Noop record).
/// </summary>
[TestClass]
public sealed class WritePreparedCompatTests
{
    private static readonly WriteOptions s_sync = new() { Sync = true };

    private static byte[] B(string s) => Encoding.UTF8.GetBytes(s);
    private static string? S(byte[]? b) => b is null ? null : Encoding.UTF8.GetString(b);

    private static DbOptions ManagedOptions() => new() { CreateIfMissing = true };

    private static TransactionDatabase OpenManaged(string path, TransactionWritePolicy policy) => OpenManaged(path, policy, twoWriteQueues: false);

    private static TransactionDatabase OpenManaged(string path, TransactionWritePolicy policy, bool twoWriteQueues)
    {
        DbOptions o = ManagedOptions();
        o.TwoWriteQueues = twoWriteQueues;
        return TransactionDatabase.Open(path, o, new TransactionDbOptions { WritePolicy = policy, WpCommitCacheBits = 12 });
    }

    [TestMethod]
    [DataRow(TransactionWritePolicy.WritePrepared, false)]
    [DataRow(TransactionWritePolicy.WriteUnprepared, false)]
    [DataRow(TransactionWritePolicy.WritePrepared, true)]
    [DataRow(TransactionWritePolicy.WriteUnprepared, true)]
    public void NativeTransactionsRecoverInManaged(TransactionWritePolicy policy, bool twoWriteQueues)
    {
        using var dir = new TempDir("wp-n2m");
        using (var native = new NativeTxnDb(dir.Path, policy, twoWriteQueues))
        {
            native.Put("plain", "p");
            native.Put("dup", "d0");
            // committed without prepare
            IntPtr t0 = native.Begin();
            NativeTxnDb.Put(t0, "t0", "v0");
            NativeTxnDb.Commit(t0);
            NativeTxnDb.Destroy(t0);
            // 2PC committed, with a duplicate key (two sub-batches)
            IntPtr t1 = native.Begin();
            NativeTxnDb.SetName(t1, "xid-committed");
            NativeTxnDb.Put(t1, "t1", "v1");
            NativeTxnDb.Delete(t1, "plain");
            NativeTxnDb.Put(t1, "t1", "v1b");
            NativeTxnDb.Prepare(t1);
            native.Put("between", "b");
            NativeTxnDb.Commit(t1);
            NativeTxnDb.Destroy(t1);
            // 2PC rolled back (restores "dup")
            IntPtr t2 = native.Begin();
            NativeTxnDb.SetName(t2, "xid-rolledback");
            NativeTxnDb.Put(t2, "t2", "v2");
            NativeTxnDb.Put(t2, "dup", "d1");
            NativeTxnDb.Prepare(t2);
            NativeTxnDb.Rollback(t2);
            NativeTxnDb.Destroy(t2);
            // left prepared when the DB closes (WriteUnprepared: written as several unprepared batches first)
            IntPtr t3 = native.Begin(policy == TransactionWritePolicy.WriteUnprepared ? 1 : 0);
            NativeTxnDb.SetName(t3, "xid-prepared");
            NativeTxnDb.Put(t3, "t3", "v3");
            NativeTxnDb.Put(t3, "t3b", "v3b");
            NativeTxnDb.Put(t3, "t3c", "v3c");
            NativeTxnDb.Put(t3, "t3", "v3-final");
            Assert.AreEqual("v3-final", native.TxnGet(t3, "t3"));
            NativeTxnDb.Prepare(t3);
            Assert.IsNull(native.Get("t3"));
        }

        using (TransactionDatabase db = OpenManaged(dir.Path, policy, twoWriteQueues))
        {
            Assert.AreEqual("v0", S(db.Database.Get(B("t0"))));
            Assert.AreEqual("v1b", S(db.Database.Get(B("t1"))));
            Assert.AreEqual("b", S(db.Database.Get(B("between"))));
            Assert.AreEqual("d0", S(db.Database.Get(B("dup"))));
            Assert.IsNull(db.Database.Get(B("plain")));
            Assert.IsNull(db.Database.Get(B("t2")));
            // prepared data is in the DB (memtables / recovery flush) but invisible
            Assert.IsNull(db.Database.Get(B("t3")));
            Assert.IsNull(db.Database.Get(B("t3b")));
            List<Transaction> prepared = db.GetAllPreparedTransactions();
            Assert.AreEqual(1, prepared.Count);
            Assert.AreEqual("xid-prepared", prepared[0].Name);
            prepared[0].Commit();
            Assert.AreEqual("v3-final", S(db.Database.Get(B("t3"))));
            Assert.AreEqual("v3b", S(db.Database.Get(B("t3b"))));
            Assert.AreEqual("v3c", S(db.Database.Get(B("t3c"))));
            // leave another prepared transaction for native to recover
            using Transaction m = db.BeginTransaction(s_sync, new TransactionOptions { WriteBatchFlushThreshold = policy == TransactionWritePolicy.WriteUnprepared ? 1 : 0 });
            m.SetName("managed-prepared");
            m.Put(B("m1"), B("mv1"));
            m.Put(B("m2"), B("mv2"));
            m.Put(B("m1"), B("mv1b"));
            m.Prepare();
        }

        using (var native = new NativeTxnDb(dir.Path, policy, twoWriteQueues))
        {
            Assert.AreEqual("v3-final", native.Get("t3"));
            Assert.AreEqual("v3b", native.Get("t3b"));
            Assert.IsNull(native.Get("m1"));
            List<IntPtr> prepared = native.GetPreparedTransactions();
            Assert.AreEqual(1, prepared.Count);
            Assert.AreEqual("managed-prepared", NativeTxnDb.GetName(prepared[0]));
            NativeTxnDb.Commit(prepared[0]);
            NativeTxnDb.Destroy(prepared[0]);
            Assert.AreEqual("mv1b", native.Get("m1"));
            Assert.AreEqual("mv2", native.Get("m2"));
        }

        using (TransactionDatabase db = OpenManaged(dir.Path, policy, twoWriteQueues))
        {
            Assert.AreEqual(0, db.GetAllPreparedTransactions().Count);
            Assert.AreEqual("mv1b", S(db.Database.Get(B("m1"))));
            Assert.AreEqual("v3-final", S(db.Database.Get(B("t3"))));
        }
    }

    [TestMethod]
    [DataRow(TransactionWritePolicy.WritePrepared, false)]
    [DataRow(TransactionWritePolicy.WriteUnprepared, false)]
    [DataRow(TransactionWritePolicy.WritePrepared, true)]
    [DataRow(TransactionWritePolicy.WriteUnprepared, true)]
    public void ManagedTransactionsRecoverInNative(TransactionWritePolicy policy, bool twoWriteQueues)
    {
        using var dir = new TempDir("wp-m2n");
        long threshold = policy == TransactionWritePolicy.WriteUnprepared ? 1 : 0;
        using (TransactionDatabase db = OpenManaged(dir.Path, policy, twoWriteQueues))
        {
            db.Put(s_sync, null, B("plain"), B("p"));
            db.Put(s_sync, null, B("dup"), B("d0"));
            using (Transaction t0 = db.BeginTransaction(s_sync))
            {
                t0.Put(B("t0"), B("v0"));
                t0.Put(B("t0"), B("v0b"));
                t0.Commit();
            }
            using (Transaction t1 = db.BeginTransaction(s_sync, new TransactionOptions { WriteBatchFlushThreshold = threshold }))
            {
                t1.SetName("xid-committed");
                t1.Put(B("t1"), B("v1"));
                t1.Delete(B("plain"));
                t1.Prepare();
                db.Put(s_sync, null, B("between"), B("b"));
                t1.Commit();
            }
            using (Transaction t2 = db.BeginTransaction(s_sync, new TransactionOptions { WriteBatchFlushThreshold = threshold }))
            {
                t2.SetName("xid-rolledback");
                t2.Put(B("t2"), B("v2"));
                t2.Put(B("dup"), B("d1"));
                t2.Prepare();
                t2.Rollback();
            }
            using (Db.WriteBatch b = new())
            {
                b.Put(B("wb"), B("1")).Put(B("wb"), B("2"));
                db.Write(s_sync, b);
            }
            Transaction t3 = db.BeginTransaction(s_sync, new TransactionOptions { WriteBatchFlushThreshold = threshold });
            t3.SetName("xid-prepared");
            t3.Put(B("t3"), B("v3"));
            t3.Put(B("t3b"), B("v3b"));
            t3.Put(B("t3"), B("v3-final"));
            t3.Prepare();
            // closed with t3 prepared
        }

        using (var native = new NativeTxnDb(dir.Path, policy, twoWriteQueues))
        {
            Assert.AreEqual("v0b", native.Get("t0"));
            Assert.AreEqual("v1", native.Get("t1"));
            Assert.AreEqual("b", native.Get("between"));
            Assert.AreEqual("d0", native.Get("dup"));
            Assert.AreEqual("2", native.Get("wb"));
            Assert.IsNull(native.Get("plain"));
            Assert.IsNull(native.Get("t2"));
            Assert.IsNull(native.Get("t3"));
            List<IntPtr> prepared = native.GetPreparedTransactions();
            Assert.AreEqual(1, prepared.Count);
            Assert.AreEqual("xid-prepared", NativeTxnDb.GetName(prepared[0]));
            NativeTxnDb.Commit(prepared[0]);
            NativeTxnDb.Destroy(prepared[0]);
            Assert.AreEqual("v3-final", native.Get("t3"));
            // and a native prepared transaction for the managed side
            IntPtr n = native.Begin(threshold);
            NativeTxnDb.SetName(n, "native-prepared");
            NativeTxnDb.Put(n, "n1", "nv1");
            NativeTxnDb.Put(n, "n2", "nv2");
            NativeTxnDb.Prepare(n);
        }

        using (TransactionDatabase db = OpenManaged(dir.Path, policy, twoWriteQueues))
        {
            Assert.AreEqual("v3-final", S(db.Database.Get(B("t3"))));
            Assert.IsNull(db.Database.Get(B("n1")));
            Transaction n = db.GetAllPreparedTransactions().Single();
            Assert.AreEqual("native-prepared", n.Name);
            n.Rollback();
            Assert.IsNull(db.Database.Get(B("n1")));
            Assert.IsNull(db.Database.Get(B("n2")));
        }
        using (var native = new NativeTxnDb(dir.Path, policy, twoWriteQueues))
        {
            Assert.AreEqual(0, native.GetPreparedTransactions().Count);
            Assert.IsNull(native.Get("n1"));
            Assert.AreEqual("v3-final", native.Get("t3"));
        }
    }

    [TestMethod]
    [DataRow(TransactionWritePolicy.WritePrepared, false)]
    [DataRow(TransactionWritePolicy.WriteUnprepared, false)]
    [DataRow(TransactionWritePolicy.WritePrepared, true)]
    [DataRow(TransactionWritePolicy.WriteUnprepared, true)]
    public void WalRecordsAreByteIdentical(TransactionWritePolicy policy, bool twoWriteQueues)
    {
        using var nativeDir = new TempDir("wp-wal-n");
        using var managedDir = new TempDir("wp-wal-m");
        long threshold = policy == TransactionWritePolicy.WriteUnprepared ? 1 : 0;
        using (var native = new NativeTxnDb(nativeDir.Path, policy, twoWriteQueues))
        {
            native.Put("p", "1");
            IntPtr t = native.Begin(threshold);
            NativeTxnDb.SetName(t, "xid");
            NativeTxnDb.Put(t, "k1", "v1");
            NativeTxnDb.Delete(t, "k2");
            NativeTxnDb.Put(t, "k1", "v1b");
            NativeTxnDb.Prepare(t);
            NativeTxnDb.Commit(t);
            NativeTxnDb.Destroy(t);
            IntPtr r = native.Begin(threshold);
            NativeTxnDb.SetName(r, "xid2");
            // One key: the WriteUnprepared rollback batch lists the locked keys in the lock tracker's (hash) order,
            // which is not part of the format.
            NativeTxnDb.Put(r, "p", "2");
            NativeTxnDb.Prepare(r);
            NativeTxnDb.Rollback(r);
            NativeTxnDb.Destroy(r);
            IntPtr c = native.Begin();
            NativeTxnDb.Put(c, "k4", "v4");
            NativeTxnDb.Put(c, "k4", "v4b");
            NativeTxnDb.Commit(c);
            NativeTxnDb.Destroy(c);
        }
        using (TransactionDatabase db = OpenManaged(managedDir.Path, policy, twoWriteQueues))
        {
            db.Put(s_sync, null, B("p"), B("1"));
            using (Transaction t = db.BeginTransaction(s_sync, new TransactionOptions { WriteBatchFlushThreshold = threshold }))
            {
                t.SetName("xid");
                t.Put(B("k1"), B("v1"));
                t.Delete(B("k2"));
                t.Put(B("k1"), B("v1b"));
                t.Prepare();
                t.Commit();
            }
            using (Transaction r = db.BeginTransaction(s_sync, new TransactionOptions { WriteBatchFlushThreshold = threshold }))
            {
                r.SetName("xid2");
                r.Put(B("p"), B("2"));
                r.Prepare();
                r.Rollback();
            }
            using (Transaction c = db.BeginTransaction(s_sync))
            {
                c.Put(B("k4"), B("v4"));
                c.Put(B("k4"), B("v4b"));
                c.Commit();
            }
        }
        List<string> nativeRecords = ReadWalRecords(nativeDir.Path);
        List<string> managedRecords = ReadWalRecords(managedDir.Path);
        Assert.IsTrue(nativeRecords.Count >= 6, string.Join("\n", nativeRecords));
        CollectionAssert.AreEqual(nativeRecords, managedRecords, "native:\n" + string.Join("\n", nativeRecords) + "\nmanaged:\n" + string.Join("\n", managedRecords));
    }

    /// <summary>Hex of the WAL records of every WAL, in order.</summary>
    private static List<string> ReadWalRecords(string dir)
    {
        var wals = new List<string>(Directory.GetFiles(dir, "*.log"));
        wals.Sort(StringComparer.Ordinal);
        var result = new List<string>();
        foreach (string wal in wals)
        {
            ulong number = ulong.Parse(Path.GetFileNameWithoutExtension(wal), System.Globalization.CultureInfo.InvariantCulture);
            using var reader = new LogReader(LocalFileSystem.Instance.OpenSequential(wal), null, checksum: true, number);
            while (reader.TryReadRecord(out ReadOnlySpan<byte> record)) result.Add(Convert.ToHexString(record));
        }
        return result;
    }

    [TestMethod]
    public void OtherWritePoliciesRefusePreparedSections()
    {
        // native WritePrepared prepared section -> managed WriteCommitted / plain allow_2pc refuse (NotSupported)
        using (var dir = new TempDir("wp-policy-n"))
        {
            using (var native = new NativeTxnDb(dir.Path, TransactionWritePolicy.WritePrepared))
            {
                IntPtr t = native.Begin();
                NativeTxnDb.SetName(t, "x");
                NativeTxnDb.Put(t, "k", "v");
                NativeTxnDb.Prepare(t);
            }
            var e = Assert.ThrowsExactly<RocksDbException>(() => TransactionDatabase.Open(dir.Path, ManagedOptions()).Dispose());
            Assert.AreEqual(StatusCode.NotSupported, e.Code);
            e = Assert.ThrowsExactly<RocksDbException>(() => OpenManaged(dir.Path, TransactionWritePolicy.WriteCommitted).Dispose());
            Assert.AreEqual(StatusCode.NotSupported, e.Code);
            // WriteUnprepared reads WritePrepared sections (a superset)
            using (TransactionDatabase db = OpenManaged(dir.Path, TransactionWritePolicy.WriteUnprepared))
            {
                db.GetAllPreparedTransactions().Single().Commit();
                Assert.AreEqual("v", S(db.Database.Get(B("k"))));
            }
        }
        // managed WriteUnprepared unprepared + prepared sections -> native WritePrepared refuses
        using (var dir = new TempDir("wp-policy-m"))
        {
            using (TransactionDatabase db = OpenManaged(dir.Path, TransactionWritePolicy.WriteUnprepared))
            {
                Transaction t = db.BeginTransaction(s_sync, new TransactionOptions { WriteBatchFlushThreshold = 1 });
                t.SetName("x");
                t.Put(B("a"), B("1"));
                t.Put(B("b"), B("2"));
                t.Prepare();
            }
            Assert.ThrowsExactly<RocksDbSharp.RocksDbException>(() => new NativeTxnDb(dir.Path, TransactionWritePolicy.WritePrepared).Dispose());
            Assert.ThrowsExactly<RocksDbSharp.RocksDbException>(() => new NativeTxnDb(dir.Path, TransactionWritePolicy.WriteCommitted).Dispose());
            using (var native = new NativeTxnDb(dir.Path, TransactionWritePolicy.WriteUnprepared))
            {
                List<IntPtr> prepared = native.GetPreparedTransactions();
                Assert.AreEqual(1, prepared.Count);
                NativeTxnDb.Commit(prepared[0]);
                NativeTxnDb.Destroy(prepared[0]);
                Assert.AreEqual("2", native.Get("b"));
            }
        }
    }
}
