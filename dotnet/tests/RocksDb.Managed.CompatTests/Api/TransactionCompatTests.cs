using System.Runtime.InteropServices;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Env;
using RocksDb.Managed.Utilities.Transactions;
using RocksDb.Managed.Wal;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>
/// Thin wrapper over the native TransactionDB C API (rocksdb_transactiondb_* / rocksdb_transaction_*), WriteCommitted.
/// </summary>
internal sealed class NativeTxnDb : IDisposable
{
    private static readonly RocksDbSharp.Native N = RocksDbSharp.Native.Instance;
    private readonly RocksDbSharp.DbOptions _options;
    private readonly IntPtr _txnDbOptions;
    private readonly RocksDbSharp.WriteOptions _wo = new RocksDbSharp.WriteOptions().SetSync(true);
    private readonly RocksDbSharp.ReadOptions _ro = new();
    public IntPtr Handle { get; private set; }

    public NativeTxnDb(string path) : this(path, TransactionWritePolicy.WriteCommitted)
    {
    }

    /// <summary>Opens a TransactionDB with <paramref name="policy"/> (rocksdb_transactiondb_options_set_write_policy).</summary>
    public NativeTxnDb(string path, TransactionWritePolicy policy)
    {
        _options = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        _txnDbOptions = N.rocksdb_transactiondb_options_create();
        // TxnDBWritePolicy: WRITE_COMMITTED = 0, WRITE_PREPARED = 1, WRITE_UNPREPARED = 2 (same values as the managed enum)
        N.rocksdb_transactiondb_options_set_write_policy(_txnDbOptions, (int)policy);
        try
        {
            Handle = N.rocksdb_transactiondb_open(_options.Handle, _txnDbOptions, path);
        }
        catch
        {
            N.rocksdb_transactiondb_options_destroy(_txnDbOptions);
            throw;
        }
    }

    public IntPtr Begin() => Begin(0);

    /// <summary>A transaction with write_batch_flush_threshold (WriteUnprepared; 0 = the DB default).</summary>
    public IntPtr Begin(long writeBatchFlushThreshold)
    {
        IntPtr o = N.rocksdb_transaction_options_create();
        try
        {
            if (writeBatchFlushThreshold != 0) N.rocksdb_transaction_options_set_write_batch_flush_threshold(o, writeBatchFlushThreshold);
            return N.rocksdb_transaction_begin(Handle, _wo.Handle, o, IntPtr.Zero);
        }
        finally { N.rocksdb_transaction_options_destroy(o); }
    }

    public static void Merge(IntPtr txn, string k, string v)
    {
        byte[] kb = Encoding.UTF8.GetBytes(k), vb = Encoding.UTF8.GetBytes(v);
        N.rocksdb_transaction_merge(txn, kb, (UIntPtr)kb.Length, vb, (UIntPtr)vb.Length);
    }

    /// <summary>rocksdb_transaction_get: the transaction's view of the key.</summary>
    public string? TxnGet(IntPtr txn, string k)
    {
        byte[] kb = Encoding.UTF8.GetBytes(k);
        IntPtr v = N.rocksdb_transaction_get(txn, _ro.Handle, kb, (UIntPtr)kb.Length, out UIntPtr vlen);
        if (v == IntPtr.Zero) return null;
        try { return Marshal.PtrToStringUTF8(v, (int)vlen); }
        finally { N.rocksdb_free(v); }
    }

    public static void SetName(IntPtr txn, string name)
    {
        byte[] b = Encoding.UTF8.GetBytes(name);
        IntPtr p = Marshal.AllocHGlobal(b.Length);
        try
        {
            Marshal.Copy(b, 0, p, b.Length);
            N.rocksdb_transaction_set_name(txn, p, (UIntPtr)b.Length);
        }
        finally { Marshal.FreeHGlobal(p); }
    }

    public static void Put(IntPtr txn, string k, string v)
    {
        byte[] kb = Encoding.UTF8.GetBytes(k), vb = Encoding.UTF8.GetBytes(v);
        N.rocksdb_transaction_put(txn, kb, (UIntPtr)kb.Length, vb, (UIntPtr)vb.Length);
    }

    public static void Delete(IntPtr txn, string k)
    {
        byte[] kb = Encoding.UTF8.GetBytes(k);
        N.rocksdb_transaction_delete(txn, kb, (UIntPtr)kb.Length);
    }

    public static void Prepare(IntPtr txn) => N.rocksdb_transaction_prepare(txn);
    public static void Commit(IntPtr txn) => N.rocksdb_transaction_commit(txn);
    public static void Rollback(IntPtr txn) => N.rocksdb_transaction_rollback(txn);
    public static void Destroy(IntPtr txn) => N.rocksdb_transaction_destroy(txn);

    public static string GetName(IntPtr txn)
    {
        IntPtr p = N.rocksdb_transaction_get_name(txn, out UIntPtr len);
        try { return Marshal.PtrToStringUTF8(p, (int)len); }
        finally { N.rocksdb_free(p); }
    }

    public string? Get(string k)
    {
        byte[] kb = Encoding.UTF8.GetBytes(k);
        IntPtr v = N.rocksdb_transactiondb_get(Handle, _ro.Handle, kb, (UIntPtr)kb.Length, out UIntPtr vlen);
        if (v == IntPtr.Zero) return null;
        try { return Marshal.PtrToStringUTF8(v, (int)vlen); }
        finally { N.rocksdb_free(v); }
    }

    public void Put(string k, string v)
    {
        byte[] kb = Encoding.UTF8.GetBytes(k), vb = Encoding.UTF8.GetBytes(v);
        N.rocksdb_transactiondb_put(Handle, _wo.Handle, kb, (UIntPtr)kb.Length, vb, (UIntPtr)vb.Length);
    }

    /// <summary>rocksdb_transactiondb_get_prepared_transactions: the prepared transactions (handles owned by the caller).</summary>
    public List<IntPtr> GetPreparedTransactions()
    {
        IntPtr cnt = Marshal.AllocHGlobal(IntPtr.Size);
        try
        {
            IntPtr arr = N.rocksdb_transactiondb_get_prepared_transactions(Handle, cnt);
            int n = (int)Marshal.ReadIntPtr(cnt);
            var list = new List<IntPtr>(n);
            for (int i = 0; i < n; i++) list.Add(Marshal.ReadIntPtr(arr, i * IntPtr.Size));
            if (arr != IntPtr.Zero) N.rocksdb_free(arr);
            return list;
        }
        finally { Marshal.FreeHGlobal(cnt); }
    }

    /// <summary>Closes the DB. Transactions still alive (e.g. prepared ones) are deleted by the TransactionDB, not rolled back.</summary>
    public void Dispose()
    {
        if (Handle == IntPtr.Zero) return;
        N.rocksdb_transactiondb_close(Handle);
        Handle = IntPtr.Zero;
        N.rocksdb_transactiondb_options_destroy(_txnDbOptions);
    }
}

/// <summary>
/// TransactionDB WAL compatibility (WriteCommitted): prepared, committed and rolled back 2PC transactions written by
/// native RocksDB recover in the managed port and vice versa; prepare/commit WAL records are byte-identical.
/// </summary>
[TestClass]
public sealed class TransactionCompatTests
{
    private static readonly WriteOptions s_sync = new() { Sync = true };

    private static byte[] B(string s) => Encoding.UTF8.GetBytes(s);
    private static string? S(byte[]? b) => b is null ? null : Encoding.UTF8.GetString(b);

    private static DbOptions ManagedOptions() => new() { CreateIfMissing = true };

    [TestMethod]
    public void NativeTwoPhaseCommitRecoversInManaged()
    {
        using var dir = new TempDir("txn-n2m");
        using (var native = new NativeTxnDb(dir.Path))
        {
            native.Put("plain", "p");
            // committed without prepare
            IntPtr t0 = native.Begin();
            NativeTxnDb.Put(t0, "t0", "v0");
            NativeTxnDb.Commit(t0);
            NativeTxnDb.Destroy(t0);
            // 2PC committed
            IntPtr t1 = native.Begin();
            NativeTxnDb.SetName(t1, "xid-committed");
            NativeTxnDb.Put(t1, "t1", "v1");
            NativeTxnDb.Delete(t1, "plain");
            NativeTxnDb.Prepare(t1);
            native.Put("between", "b");
            NativeTxnDb.Commit(t1);
            NativeTxnDb.Destroy(t1);
            // 2PC rolled back
            IntPtr t2 = native.Begin();
            NativeTxnDb.SetName(t2, "xid-rolledback");
            NativeTxnDb.Put(t2, "t2", "v2");
            NativeTxnDb.Prepare(t2);
            NativeTxnDb.Rollback(t2);
            NativeTxnDb.Destroy(t2);
            // 2PC left prepared when the DB closes
            IntPtr t3 = native.Begin();
            NativeTxnDb.SetName(t3, "xid-prepared");
            NativeTxnDb.Put(t3, "t3", "v3");
            NativeTxnDb.Put(t3, "t3b", "v3b");
            NativeTxnDb.Prepare(t3);
        }

        using (TransactionDatabase db = TransactionDatabase.Open(dir.Path, ManagedOptions()))
        {
            Assert.AreEqual("v0", S(db.Database.Get(B("t0"))));
            Assert.AreEqual("v1", S(db.Database.Get(B("t1"))));
            Assert.AreEqual("b", S(db.Database.Get(B("between"))));
            Assert.IsNull(db.Database.Get(B("plain")));
            Assert.IsNull(db.Database.Get(B("t2")));
            Assert.IsNull(db.Database.Get(B("t3")));
            List<Transaction> prepared = db.GetAllPreparedTransactions();
            Assert.AreEqual(1, prepared.Count);
            Assert.AreEqual("xid-prepared", prepared[0].Name);
            Assert.AreEqual(2, prepared[0].Count);
            prepared[0].Commit();
            Assert.AreEqual("v3", S(db.Database.Get(B("t3"))));
            // leave another prepared transaction for native to recover
            using Transaction m = db.BeginTransaction(s_sync);
            m.SetName("managed-prepared");
            m.Put(B("m1"), B("mv1"));
            m.Prepare();
        }

        using (var native = new NativeTxnDb(dir.Path))
        {
            Assert.AreEqual("v3", native.Get("t3"));
            Assert.AreEqual("v3b", native.Get("t3b"));
            Assert.IsNull(native.Get("m1"));
            List<IntPtr> prepared = native.GetPreparedTransactions();
            Assert.AreEqual(1, prepared.Count);
            Assert.AreEqual("managed-prepared", NativeTxnDb.GetName(prepared[0]));
            NativeTxnDb.Commit(prepared[0]);
            NativeTxnDb.Destroy(prepared[0]);
            Assert.AreEqual("mv1", native.Get("m1"));
        }
    }

    [TestMethod]
    public void ManagedTwoPhaseCommitRecoversInNative()
    {
        using var dir = new TempDir("txn-m2n");
        using (TransactionDatabase db = TransactionDatabase.Open(dir.Path, ManagedOptions()))
        {
            db.Put(s_sync, null, B("plain"), B("p"));
            using (Transaction t0 = db.BeginTransaction(s_sync))
            {
                t0.Put(B("t0"), B("v0"));
                t0.Commit();
            }
            using (Transaction t1 = db.BeginTransaction(s_sync))
            {
                t1.SetName("xid-committed");
                t1.Put(B("t1"), B("v1"));
                t1.Delete(B("plain"));
                t1.Prepare();
                db.Put(s_sync, null, B("between"), B("b"));
                t1.Commit();
            }
            using (Transaction t2 = db.BeginTransaction(s_sync))
            {
                t2.SetName("xid-rolledback");
                t2.Put(B("t2"), B("v2"));
                t2.Prepare();
                t2.Rollback();
            }
            Transaction t3 = db.BeginTransaction(s_sync);
            t3.SetName("xid-prepared");
            t3.Put(B("t3"), B("v3"));
            t3.Prepare();
            // closed with t3 prepared
        }

        using (var native = new NativeTxnDb(dir.Path))
        {
            Assert.AreEqual("v0", native.Get("t0"));
            Assert.AreEqual("v1", native.Get("t1"));
            Assert.AreEqual("b", native.Get("between"));
            Assert.IsNull(native.Get("plain"));
            Assert.IsNull(native.Get("t2"));
            Assert.IsNull(native.Get("t3"));
            List<IntPtr> prepared = native.GetPreparedTransactions();
            Assert.AreEqual(1, prepared.Count);
            Assert.AreEqual("xid-prepared", NativeTxnDb.GetName(prepared[0]));
            NativeTxnDb.Rollback(prepared[0]);
            NativeTxnDb.Destroy(prepared[0]);
            native.Put("after", "native");
        }

        using (TransactionDatabase db = TransactionDatabase.Open(dir.Path, ManagedOptions()))
        {
            Assert.AreEqual(0, db.GetAllPreparedTransactions().Count);
            Assert.IsNull(db.Database.Get(B("t3")));
            Assert.AreEqual("native", S(db.Database.Get(B("after"))));
            Assert.AreEqual("v1", S(db.Database.Get(B("t1"))));
        }
    }

    [TestMethod]
    public void PrepareAndCommitWalRecordsAreByteIdentical()
    {
        using var nativeDir = new TempDir("txn-wal-n");
        using var managedDir = new TempDir("txn-wal-m");
        using (var native = new NativeTxnDb(nativeDir.Path))
        {
            IntPtr t = native.Begin();
            NativeTxnDb.SetName(t, "xid");
            NativeTxnDb.Put(t, "k1", "v1");
            NativeTxnDb.Delete(t, "k2");
            NativeTxnDb.Prepare(t);
            NativeTxnDb.Commit(t);
            NativeTxnDb.Destroy(t);
            IntPtr r = native.Begin();
            NativeTxnDb.SetName(r, "xid2");
            NativeTxnDb.Put(r, "k3", "v3");
            NativeTxnDb.Prepare(r);
            NativeTxnDb.Rollback(r);
            NativeTxnDb.Destroy(r);
            IntPtr c = native.Begin();
            NativeTxnDb.Put(c, "k4", "v4");
            NativeTxnDb.Commit(c);
            NativeTxnDb.Destroy(c);
        }
        using (TransactionDatabase db = TransactionDatabase.Open(managedDir.Path, ManagedOptions()))
        {
            using (Transaction t = db.BeginTransaction(s_sync))
            {
                t.SetName("xid");
                t.Put(B("k1"), B("v1"));
                t.Delete(B("k2"));
                t.Prepare();
                t.Commit();
            }
            using (Transaction r = db.BeginTransaction(s_sync))
            {
                r.SetName("xid2");
                r.Put(B("k3"), B("v3"));
                r.Prepare();
                r.Rollback();
            }
            using (Transaction c = db.BeginTransaction(s_sync))
            {
                c.Put(B("k4"), B("v4"));
                c.Commit();
            }
        }
        List<string> nativeRecords = ReadLastWalRecords(nativeDir.Path);
        List<string> managedRecords = ReadLastWalRecords(managedDir.Path);
        Assert.AreEqual(5, nativeRecords.Count, string.Join("\n", nativeRecords));
        CollectionAssert.AreEqual(nativeRecords, managedRecords);
    }

    /// <summary>Hex of every WAL record of the newest WAL that holds records.</summary>
    private static List<string> ReadLastWalRecords(string dir)
    {
        var wals = new List<string>(Directory.GetFiles(dir, "*.log"));
        wals.Sort(StringComparer.Ordinal);
        var result = new List<string>();
        foreach (string wal in wals)
        {
            ulong number = ulong.Parse(Path.GetFileNameWithoutExtension(wal), System.Globalization.CultureInfo.InvariantCulture);
            using var reader = new LogReader(LocalFileSystem.Instance.OpenSequential(wal), null, checksum: true, number);
            var records = new List<string>();
            while (reader.TryReadRecord(out ReadOnlySpan<byte> record)) records.Add(Convert.ToHexString(record));
            if (records.Count > 0) result = records;
        }
        return result;
    }

    [TestMethod]
    public void ManagedOptimisticCommitsReadableByNative()
    {
        using var dir = new TempDir("txn-occ");
        using (OptimisticTransactionDatabase db = OptimisticTransactionDatabase.Open(dir.Path, ManagedOptions()))
        {
            using Transaction t = db.BeginTransaction(s_sync, new OptimisticTransactionOptions { SetSnapshot = true });
            t.Put(B("a"), B("1"));
            t.Put(B("b"), B("2"));
            t.Delete(B("a"));
            t.Commit();
        }
        using (var native = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions(), dir.Path))
        {
            Assert.IsNull(native.Get(B("a")));
            Assert.AreEqual("2", S(native.Get(B("b"))));
        }
    }
}
