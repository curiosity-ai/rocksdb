using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Utilities.Transactions;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>
/// WriteCommitted transactions on a column family with user-defined timestamps across implementations: the native side
/// uses rocksdb_transaction_put (placeholder timestamps), rocksdb_transaction_set_commit_timestamp and 2PC
/// (rocksdb_transaction_prepare: the commit marker is a kTypeCommitXIDAndTimestamp record whose timestamp recovery assigns
/// to the prepared batch). Committed data left in the WAL is recovered by the other implementation in both directions.
/// </summary>
public sealed unsafe partial class TimestampCompatTests
{
    private sealed class NativeTsTxnDb : IDisposable
    {
        private readonly RocksDbSharp.DbOptions _options;
        private readonly IntPtr _txnDbOptions;
        private readonly RocksDbSharp.WriteOptions _wo = new RocksDbSharp.WriteOptions().SetSync(true);
        public IntPtr Handle { get; private set; }

        public NativeTsTxnDb(string path)
        {
            _options = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
            N.rocksdb_options_set_comparator(_options.Handle, s_comparator.Value);
            N.rocksdb_options_set_avoid_flush_during_recovery(_options.Handle, 1);
            _txnDbOptions = N.rocksdb_transactiondb_options_create();
            Handle = N.rocksdb_transactiondb_open(_options.Handle, _txnDbOptions, path);
            Assert.AreNotEqual(IntPtr.Zero, Handle);
        }

        /// <summary>Writes <paramref name="kv"/> in one transaction committed at <paramref name="commitTs"/> (2PC when named).</summary>
        public void Commit(IEnumerable<(string Key, string? Value)> kv, ulong commitTs, string? name)
        {
            IntPtr o = N.rocksdb_transaction_options_create();
            N.rocksdb_transaction_options_set_skip_prepare(o, name is null ? (byte)1 : (byte)0);
            IntPtr txn = N.rocksdb_transaction_begin(Handle, _wo.Handle, o, IntPtr.Zero);
            N.rocksdb_transaction_options_destroy(o);
            try
            {
                if (name is not null)
                {
                    byte[] nb = B(name);
                    fixed (byte* p = nb) N.rocksdb_transaction_set_name(txn, (IntPtr)p, (UIntPtr)nb.Length);
                }
                foreach ((string k, string? v) in kv)
                {
                    byte[] kb = B(k);
                    if (v is null) N.rocksdb_transaction_delete(txn, kb, (UIntPtr)kb.Length);
                    else
                    {
                        byte[] vb = B(v);
                        N.rocksdb_transaction_put(txn, kb, (UIntPtr)kb.Length, vb, (UIntPtr)vb.Length);
                    }
                }
                if (name is not null) N.rocksdb_transaction_prepare(txn);
                N.rocksdb_transaction_set_commit_timestamp(txn, commitTs);
                N.rocksdb_transaction_commit(txn);
            }
            finally
            {
                N.rocksdb_transaction_destroy(txn);
            }
        }

        public string? Get(string key, ulong readTs)
        {
            var ro = new RocksDbSharp.ReadOptions();
            IntPtr ts = Marshal.AllocHGlobal(8);
            try
            {
                Ts(readTs).CopyTo(new Span<byte>((void*)ts, 8));
                N.rocksdb_readoptions_set_timestamp(ro.Handle, ts, (UIntPtr)8);
                byte[] kb = B(key);
                IntPtr v = N.rocksdb_transactiondb_get(Handle, ro.Handle, kb, (UIntPtr)kb.Length, out UIntPtr vlen);
                if (v == IntPtr.Zero) return null;
                try { return Marshal.PtrToStringUTF8(v, (int)vlen); }
                finally { N.rocksdb_free(v); }
            }
            finally
            {
                Marshal.FreeHGlobal(ts);
            }
        }

        public void Dispose()
        {
            if (Handle == IntPtr.Zero) return;
            N.rocksdb_transactiondb_close(Handle);
            Handle = IntPtr.Zero;
            N.rocksdb_transactiondb_options_destroy(_txnDbOptions);
        }
    }

    private static TransactionDatabase OpenManagedTsTxnDb(string path) =>
        TransactionDatabase.Open(path, new DbOptions { CreateIfMissing = true, AvoidFlushDuringRecovery = true }, new TransactionDbOptions(),
            [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions { Comparator = Comparator.BytewiseWithU64Ts })]);

    private static void ManagedCommit(TransactionDatabase db, IEnumerable<(string Key, string? Value)> kv, ulong commitTs, string? name)
    {
        using Transaction txn = db.BeginTransaction(new WriteOptions { Sync = true }, new TransactionOptions { SkipPrepare = name is null });
        if (name is not null) txn.SetName(name);
        foreach ((string k, string? v) in kv)
        {
            if (v is null) txn.Delete(B(k));
            else txn.Put(B(k), B(v));
        }
        if (name is not null) txn.Prepare();
        txn.SetCommitTimestamp(commitTs);
        txn.Commit();
    }

    private static string? ManagedTxnDbGet(TransactionDatabase db, string key, ulong readTs, out ulong foundTs)
    {
        var v = new ArrayBufferWriter<byte>();
        var t = new ArrayBufferWriter<byte>();
        bool found = db.Database.TryGet(new ReadOptions { Timestamp = Ts(readTs) }, B(key), v, t);
        foundTs = t.WrittenCount == 8 ? BinaryPrimitives.ReadUInt64LittleEndian(t.WrittenSpan) : ulong.MaxValue;
        return found ? System.Text.Encoding.UTF8.GetString(v.WrittenSpan) : null;
    }

    private static (string, string?)[] Round(int round, int keys) =>
        Enumerable.Range(0, keys).Select(i => (Key(i), i % 7 == round % 7 ? (string?)null : "r" + round + "-" + i)).ToArray();

    [TestMethod]
    public void TimestampedTransactionsRecoverBothWays()
    {
        using var dir = new TempDir("udt-txn");
        const int Keys = 40;
        // Native: one plain commit and one 2PC commit per round, all left in the WAL.
        using (var native = new NativeTsTxnDb(dir.Path))
        {
            native.Commit(Round(0, Keys), 10, null);
            native.Commit(Round(1, Keys), 20, "n1");
            native.Commit(Round(2, Keys), 30, null);
        }
        using (TransactionDatabase db = OpenManagedTsTxnDb(dir.Path))
        {
            for (int r = 0; r < 3; r++)
            {
                ulong ts = (ulong)(r + 1) * 10;
                foreach ((string k, string? v) in Round(r, Keys))
                {
                    string? got = ManagedTxnDbGet(db, k, ts, out ulong foundTs);
                    Assert.AreEqual(v, got, k + "@" + ts);
                    Assert.AreEqual(ts, foundTs, k + "@" + ts);
                }
            }
            Assert.IsNull(ManagedTxnDbGet(db, Key(1), 9, out _));
            // Managed commits on top (plain and 2PC), left in the WAL for native.
            ManagedCommit(db, Round(3, Keys), 40, null);
            ManagedCommit(db, Round(4, Keys), 50, "m1");
        }
        using (var native = new NativeTsTxnDb(dir.Path))
        {
            for (int r = 0; r < 5; r++)
            {
                ulong ts = (ulong)(r + 1) * 10;
                foreach ((string k, string? v) in Round(r, Keys)) Assert.AreEqual(v, native.Get(k, ts), k + "@" + ts);
            }
            Assert.IsNull(native.Get(Key(1), 9));
        }
    }
}
