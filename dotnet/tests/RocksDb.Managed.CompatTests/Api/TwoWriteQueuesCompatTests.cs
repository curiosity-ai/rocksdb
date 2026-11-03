using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Env;
using RocksDb.Managed.Utilities.Transactions;
using RocksDb.Managed.Wal;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>
/// two_write_queues and commit-time write batches against native librocksdb 11.12:
/// <list type="bullet">
/// <item>WriteCommitted with two write queues (2PC prepares on the WAL-only queue): WALs recover both ways and their
/// records are byte-identical (the WritePrepared / WriteUnprepared cases are the two-queue rows of
/// <see cref="WritePreparedCompatTests"/>).</item>
/// <item>Commit-time batches written by the managed port (WriteCommitted; WritePrepared / WriteUnprepared with
/// use_only_the_last_commit_time_batch_for_recovery) are recovered by native RocksDB. The C API has no
/// GetCommitTimeWriteBatch, so native cannot produce such WALs; the managed side reads its own and native's
/// commit-marker records through the same WAL replay.</item>
/// </list>
/// </summary>
[TestClass]
public sealed class TwoWriteQueuesCompatTests
{
    private static readonly WriteOptions s_sync = new() { Sync = true };

    private static byte[] B(string s) => Encoding.UTF8.GetBytes(s);
    private static string? S(byte[]? b) => b is null ? null : Encoding.UTF8.GetString(b);

    private static TransactionDatabase OpenManaged(string path, TransactionWritePolicy policy, bool twoWriteQueues) =>
        TransactionDatabase.Open(path, new DbOptions { CreateIfMissing = true, TwoWriteQueues = twoWriteQueues },
            new TransactionDbOptions { WritePolicy = policy, WpCommitCacheBits = 12 });

    [TestMethod]
    public void WriteCommittedTwoQueuesRecoverBothWays()
    {
        using var dir = new TempDir("twq-wc");
        using (var native = new NativeTxnDb(dir.Path, TransactionWritePolicy.WriteCommitted, twoWriteQueues: true))
        {
            native.Put("plain", "p");
            IntPtr t1 = native.Begin();
            NativeTxnDb.SetName(t1, "n-committed");
            NativeTxnDb.Put(t1, "n1", "v1");
            NativeTxnDb.Prepare(t1);
            native.Put("between", "b");
            NativeTxnDb.Commit(t1);
            NativeTxnDb.Destroy(t1);
            IntPtr t2 = native.Begin();
            NativeTxnDb.SetName(t2, "n-prepared");
            NativeTxnDb.Put(t2, "n2", "v2");
            NativeTxnDb.Prepare(t2);
        }
        using (TransactionDatabase db = OpenManaged(dir.Path, TransactionWritePolicy.WriteCommitted, twoWriteQueues: true))
        {
            Assert.AreEqual("v1", S(db.Database.Get(B("n1"))));
            Assert.AreEqual("b", S(db.Database.Get(B("between"))));
            Assert.IsNull(db.Database.Get(B("n2")));
            Transaction p = db.GetAllPreparedTransactions().Single();
            Assert.AreEqual("n-prepared", p.Name);
            p.Commit();
            Assert.AreEqual("v2", S(db.Database.Get(B("n2"))));
            Transaction m = db.BeginTransaction(s_sync);
            m.SetName("m-prepared");
            m.Put(B("m1"), B("mv1"));
            m.Prepare();
        }
        using (var native = new NativeTxnDb(dir.Path, TransactionWritePolicy.WriteCommitted, twoWriteQueues: true))
        {
            Assert.AreEqual("v2", native.Get("n2"));
            Assert.IsNull(native.Get("m1"));
            List<IntPtr> prepared = native.GetPreparedTransactions();
            Assert.AreEqual(1, prepared.Count);
            Assert.AreEqual("m-prepared", NativeTxnDb.GetName(prepared[0]));
            NativeTxnDb.Commit(prepared[0]);
            NativeTxnDb.Destroy(prepared[0]);
            Assert.AreEqual("mv1", native.Get("m1"));
        }
    }

    [TestMethod]
    public void WriteCommittedTwoQueuesWalRecordsAreByteIdentical()
    {
        using var nativeDir = new TempDir("twq-wc-wal-n");
        using var managedDir = new TempDir("twq-wc-wal-m");
        using (var native = new NativeTxnDb(nativeDir.Path, TransactionWritePolicy.WriteCommitted, twoWriteQueues: true))
        {
            native.Put("p", "1");
            IntPtr t = native.Begin();
            NativeTxnDb.SetName(t, "xid");
            NativeTxnDb.Put(t, "k1", "v1");
            NativeTxnDb.Delete(t, "k2");
            NativeTxnDb.Prepare(t);
            NativeTxnDb.Commit(t);
            NativeTxnDb.Destroy(t);
            IntPtr r = native.Begin();
            NativeTxnDb.SetName(r, "xid2");
            NativeTxnDb.Put(r, "p", "2");
            NativeTxnDb.Prepare(r);
            NativeTxnDb.Rollback(r);
            NativeTxnDb.Destroy(r);
        }
        using (TransactionDatabase db = OpenManaged(managedDir.Path, TransactionWritePolicy.WriteCommitted, twoWriteQueues: true))
        {
            db.Put(s_sync, null, B("p"), B("1"));
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
                r.Put(B("p"), B("2"));
                r.Prepare();
                r.Rollback();
            }
        }
        List<string> nativeRecords = ReadWalRecords(nativeDir.Path);
        List<string> managedRecords = ReadWalRecords(managedDir.Path);
        Assert.IsTrue(nativeRecords.Count >= 5, string.Join("\n", nativeRecords));
        CollectionAssert.AreEqual(nativeRecords, managedRecords, "native:\n" + string.Join("\n", nativeRecords) + "\nmanaged:\n" + string.Join("\n", managedRecords));
    }

    [TestMethod]
    [DataRow(TransactionWritePolicy.WriteCommitted, false)]
    [DataRow(TransactionWritePolicy.WriteCommitted, true)]
    [DataRow(TransactionWritePolicy.WritePrepared, false)]
    [DataRow(TransactionWritePolicy.WritePrepared, true)]
    [DataRow(TransactionWritePolicy.WriteUnprepared, false)]
    [DataRow(TransactionWritePolicy.WriteUnprepared, true)]
    public void ManagedCommitTimeBatchesRecoverInNative(TransactionWritePolicy policy, bool twoWriteQueues)
    {
        using var dir = new TempDir("twq-ctb");
        var options = new TransactionOptions
        {
            UseOnlyTheLastCommitTimeBatchForRecovery = policy != TransactionWritePolicy.WriteCommitted,
            WriteBatchFlushThreshold = policy == TransactionWritePolicy.WriteUnprepared ? 1 : 0,
        };
        using (TransactionDatabase db = OpenManaged(dir.Path, policy, twoWriteQueues))
        {
            for (int i = 1; i <= 2; i++)
            {
                using Transaction t = db.BeginTransaction(s_sync, options);
                t.SetName("ct" + i);
                t.Put(B("data" + i), B("v" + i));
                t.Put(B("more" + i), B("m" + i));
                t.Prepare();
                t.GetCommitTimeWriteBatch().Put(B("pos"), B(i.ToString(System.Globalization.CultureInfo.InvariantCulture)));
                t.Commit();
            }
            Assert.AreEqual("v2", S(db.Database.Get(B("data2"))));
            // WriteCommitted inserts the commit-time batch at commit; WritePrepared / WriteUnprepared only at the next
            // memtable switch (or recovery).
            Assert.AreEqual(policy == TransactionWritePolicy.WriteCommitted ? "2" : null, S(db.Database.Get(B("pos"))));
        }
        using (var native = new NativeTxnDb(dir.Path, policy, twoWriteQueues))
        {
            Assert.AreEqual("v1", native.Get("data1"));
            Assert.AreEqual("m2", native.Get("more2"));
            Assert.AreEqual("2", native.Get("pos"));
            Assert.AreEqual(0, native.GetPreparedTransactions().Count);
        }
        using (TransactionDatabase db = OpenManaged(dir.Path, policy, twoWriteQueues))
        {
            Assert.AreEqual("2", S(db.Database.Get(B("pos"))));
            Assert.AreEqual("m1", S(db.Database.Get(B("more1"))));
        }
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
}
