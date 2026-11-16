using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
using RocksDb.Managed.CompatTests.Api;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Utilities.Backup;
using RocksDb.Managed.Utilities.Transactions;

namespace RocksDb.Managed.CompatTests.Audit7;

/// <summary>
/// Audit #7, utilities under failure conditions, side by side with native RocksDB 11.12: what native does on the same
/// inputs the managed unit tests (Validation/Audit7UtilitiesFailuresTests.cs) feed the port.
/// </summary>
[TestClass]
public sealed unsafe class Audit7UtilitiesFailuresCompatTests
{
    private static readonly RocksDbSharp.Native N = RocksDbSharp.Native.Instance;

    // A native comparator with 8-byte timestamps named like BytewiseComparatorWithU64Ts (as in TimestampCompatTests).
    private static readonly RocksDbSharp.DestructorDelegate s_destructor = static _ => { };
    private static readonly RocksDbSharp.CompareDelegate s_compare = static (_, a, alen, b, blen) =>
        Comparator.BytewiseWithU64Ts.Compare(new ReadOnlySpan<byte>((void*)a, (int)alen), new ReadOnlySpan<byte>((void*)b, (int)blen));
    private static readonly RocksDbSharp.CompareTsDelegate s_compareTs = static (_, a, alen, b, blen) =>
        Comparator.BytewiseWithU64Ts.CompareTimestamp(new ReadOnlySpan<byte>((void*)a, (int)alen), new ReadOnlySpan<byte>((void*)b, (int)blen));
    private static readonly RocksDbSharp.CompareWithoutTsDelegate s_compareWithoutTs = static (_, a, alen, aHasTs, b, blen, bHasTs) =>
        Comparator.BytewiseWithU64Ts.CompareWithoutTimestamp(new ReadOnlySpan<byte>((void*)a, (int)alen), aHasTs != 0,
            new ReadOnlySpan<byte>((void*)b, (int)blen), bHasTs != 0);
    private static readonly IntPtr s_name = Marshal.StringToHGlobalAnsi("leveldb.BytewiseComparator.u64ts");
    private static readonly RocksDbSharp.NameDelegate s_nameDelegate = static _ => s_name;
    private static readonly Lazy<IntPtr> s_comparator = new(static () =>
        N.rocksdb_comparator_with_ts_create(IntPtr.Zero, s_destructor, s_compare, s_compareTs, s_compareWithoutTs, s_nameDelegate, (UIntPtr)8));

    private static string? TakeError(IntPtr err)
    {
        if (err == IntPtr.Zero) return null;
        string msg = Marshal.PtrToStringAnsi(err) ?? "";
        N.rocksdb_free(err);
        return msg;
    }

    /// <summary>
    /// pessimistic_transaction_db.h FailIfCfEnablesTs / FailIfBatchHasTs: on a TransactionDB, Put on a column family
    /// with timestamps and Write of a batch holding a timestamped key are NotSupported natively; the port matches.
    /// </summary>
    [TestMethod]
    public void TransactionDbDirectWritesWithTimestampsAreNotSupported()
    {
        using var dir = new TempDir("a7-txn-ts");
        var options = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        N.rocksdb_options_set_comparator(options.Handle, s_comparator.Value);
        IntPtr txnDbOptions = N.rocksdb_transactiondb_options_create();
        IntPtr txnDb = N.rocksdb_transactiondb_open(options.Handle, txnDbOptions, Path.Combine(dir.Path, "native"));
        var wo = new RocksDbSharp.WriteOptions();
        string? putError, writeError;
        try
        {
            byte[] k = U("k"), v = U("v"), ts = new byte[8];
            N.rocksdb_transactiondb_put(txnDb, wo.Handle, k, (UIntPtr)k.Length, v, (UIntPtr)v.Length, out IntPtr err);
            putError = TakeError(err);
            IntPtr baseDb = N.rocksdb_transactiondb_get_base_db(txnDb);
            IntPtr cf = N.rocksdb_get_default_column_family_handle(baseDb);
            IntPtr batch = N.rocksdb_writebatch_create();
            fixed (byte* pts = ts) N.rocksdb_writebatch_put_cf_with_ts(batch, cf, k, (UIntPtr)k.Length, (IntPtr)pts, (UIntPtr)8, v, (UIntPtr)v.Length);
            N.rocksdb_transactiondb_write(txnDb, wo.Handle, batch, out IntPtr err2); // a fresh variable: SaveError frees a non-null *errptr
            writeError = TakeError(err2);
            N.rocksdb_writebatch_destroy(batch);
        }
        finally
        {
            N.rocksdb_transactiondb_close(txnDb);
            N.rocksdb_transactiondb_options_destroy(txnDbOptions);
        }
        StringAssert.StartsWith(putError, "Not implemented: Write operation with user timestamp must go through the transaction API");
        StringAssert.StartsWith(writeError, "Not implemented: Writes with timestamp must go through transaction API");

        using TransactionDatabase db = TransactionDatabase.Open(Path.Combine(dir.Path, "managed"), new DbOptions { CreateIfMissing = true },
            new TransactionDbOptions(), [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions { Comparator = Comparator.BytewiseWithU64Ts })]);
        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => db.Put(U("k"), U("v")));
        Assert.AreEqual(StatusCode.NotSupported, e.Code, e.Message);
        using var mb = new RocksDb.Managed.Db.WriteBatch();
        mb.Put(db.Database.DefaultColumnFamily, U("k"), new byte[8], U("v"));
        e = Assert.ThrowsExactly<RocksDbException>(() => db.Write(null, mb));
        Assert.AreEqual(StatusCode.NotSupported, e.Code, e.Message);
    }

    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);

    private static string? Str(byte[]? b) => b is null ? null : Encoding.UTF8.GetString(b);

    private static bool Chattr(string flag, string path)
    {
        try
        {
            using var p = Process.Start(new ProcessStartInfo("chattr", flag + " " + path) { RedirectStandardError = true, RedirectStandardOutput = true });
            if (p is null) return false;
            p.WaitForExit();
            return p.ExitCode == 0;
        }
        catch (System.ComponentModel.Win32Exception)
        {
            return false;
        }
    }

    /// <summary>
    /// BackupEngineImpl::RestoreDBFromBackup -> DeleteChildren (utilities/backup/backup_engine.cc): "db_fs_->DeleteFile(
    /// dir + "/" + f, io_options_, nullptr).PermitUncheckedError();  // ignore errors". A WAL of an earlier DB left in
    /// the destination that cannot be deleted (made immutable with chattr +i) does not fail the restore, and the next
    /// open replays it into the restored DB -- natively and in the port alike.
    /// </summary>
    [TestMethod]
    public void RestoreOverAnUndeletableStaleWalReplaysItNativelyAndInThePort()
    {
        using var dir = new TempDir("a7-stale-wal");
        string src = Path.Combine(dir.Path, "src"), backupDir = Path.Combine(dir.Path, "backup");
        using (var db = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions().SetCreateIfMissing(true), src))
        {
            for (int i = 0; i < 50; i++) db.Put(U("k" + i), U("v" + i));
            using var nbe = new NativeBackupEngine(backupDir);
            nbe.CreateNewBackup(db, flush: true);
        }

        foreach (bool native in new[] { true, false })
        {
            string dest = Path.Combine(dir.Path, native ? "rn" : "rm");
            using (var other = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions().SetCreateIfMissing(true), dest))
            {
                for (int i = 0; i < 20; i++)
                {
                    other.Put(U("filler"), U(i.ToString()));
                    other.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
                }
                other.Put(U("k0"), U("stale"));
                other.Put(U("only-in-stale-wal"), U("x"));
            }
            string? wal = null;
            foreach (string f in Directory.EnumerateFiles(dest, "*.log")) wal = f;
            Assert.IsNotNull(wal);
            if (!Chattr("+i", wal)) Assert.Inconclusive("chattr +i is not available (needs root and ext2/3/4 or xfs)");
            try
            {
                if (native)
                {
                    using var nbe = new NativeBackupEngine(backupDir);
                    nbe.RestoreLatest(dest);
                }
                else
                {
                    using BackupEngineReadOnly be = BackupEngineReadOnly.Open(new BackupEngineOptions(backupDir));
                    be.RestoreDbFromLatestBackup(dest, dest, new RestoreOptions());
                }
                Assert.IsTrue(File.Exists(wal), "the stale WAL survived the restore");
                using var r = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions(), dest);
                Assert.AreEqual("x", Str(r.Get(U("only-in-stale-wal"))), (native ? "native" : "managed") + " restore: the stale WAL is replayed");
                Assert.AreEqual("stale", Str(r.Get(U("k0"))), native ? "native" : "managed");
            }
            finally
            {
                Chattr("-i", wal);
            }
        }
    }
}
