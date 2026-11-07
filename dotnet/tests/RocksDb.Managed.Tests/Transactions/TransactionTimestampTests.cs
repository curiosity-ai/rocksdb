using System.Buffers;
using System.Buffers.Binary;
using RocksDb.Managed.Env;
using RocksDb.Managed.Utilities.Transactions;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Transactions;

/// <summary>
/// User-defined timestamps in WriteCommitted transactions and WriteBatchWithIndex. Ports of key cases of
/// utilities/transactions/write_committed_transaction_ts_test.cc (SanityChecks, ReOpenWithTimestamp, RecoverFromWal,
/// GetForUpdate, BlindWrite, RefineReadTimestamp, CheckKeysForConflicts) and the timestamp cases of
/// utilities/write_batch_with_index/write_batch_with_index_test.cc.
/// </summary>
[TestClass]
public sealed class TransactionTimestampTests
{
    private static readonly WriteOptions s_wo = new();

    private static byte[] Ts(ulong t)
    {
        byte[] b = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(b, t);
        return b;
    }

    private static ReadOptions At(ulong ts) => new() { Timestamp = Ts(ts) };

    private static TransactionDatabase Open(IFileSystem fs, TransactionDbOptions? txnOptions = null) =>
        TransactionDatabase.Open(DbPath, NewOptions(fs), txnOptions ?? new TransactionDbOptions { TransactionLockTimeout = TimeSpan.FromMilliseconds(50) },
            [
                new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions()),
                new ColumnFamilyDescriptor("test_cf", new ColumnFamilyOptions { Comparator = Comparator.BytewiseWithU64Ts }),
            ]);

    private static TransactionDatabase OpenFresh(IFileSystem fs, TransactionDbOptions? txnOptions = null)
    {
        DbOptions o = NewOptions(fs);
        o.CreateMissingColumnFamilies = true;
        return TransactionDatabase.Open(DbPath, o, txnOptions ?? new TransactionDbOptions { TransactionLockTimeout = TimeSpan.FromMilliseconds(50) },
            [
                new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions()),
                new ColumnFamilyDescriptor("test_cf", new ColumnFamilyOptions { Comparator = Comparator.BytewiseWithU64Ts }),
            ]);
    }

    private static ColumnFamily Cf(TransactionDatabase db) => db.Database.GetColumnFamily("test_cf");

    /// <summary>(value, ts) of <paramref name="key"/> at <paramref name="readTs"/> straight from the DB.</summary>
    private static (string? Value, ulong Ts) GetAt(TransactionDatabase db, string key, ulong readTs)
    {
        var v = new ArrayBufferWriter<byte>();
        var t = new ArrayBufferWriter<byte>();
        bool found = Cf(db).TryGet(At(readTs), B(key), v, t);
        return (found ? S(v.WrittenSpan.ToArray()) : null, t.WrittenCount == 8 ? BinaryPrimitives.ReadUInt64LittleEndian(t.WrittenSpan) : ulong.MaxValue);
    }

    private static void AssertFails(StatusCode code, Action a)
    {
        var e = Assert.ThrowsExactly<RocksDbException>(a);
        Assert.AreEqual(code, e.Code, e.Message);
    }

    [TestMethod]
    public void CommitRequiresCommitTimestamp()
    {
        MemoryFileSystem fs = NewFs();
        using TransactionDatabase db = OpenFresh(fs);
        using (Transaction txn = db.BeginTransaction(s_wo))
        {
            txn.Put(Cf(db), B("foo"), B("value"));
            AssertFails(StatusCode.InvalidArgument, txn.Commit);
            txn.SetCommitTimestamp(20);
            txn.Commit();
        }
        Assert.AreEqual(("value", 20UL), GetAt(db, "foo", 20));
        Assert.AreEqual((null, ulong.MaxValue), GetAt(db, "foo", 19));
        // A transaction writing only to column families without timestamps needs no commit timestamp.
        using (Transaction txn = db.BeginTransaction(s_wo))
        {
            txn.Put(B("plain"), B("x"));
            txn.Commit();
        }
        Assert.AreEqual("x", GetString(db.Database, "plain"));
        // Reads of a column family with timestamps need a read timestamp.
        using (Transaction txn = db.BeginTransaction(s_wo))
        {
            AssertFails(StatusCode.InvalidArgument, () => txn.Get(null, Cf(db), B("foo")));
            Assert.AreEqual("value", S(txn.Get(At(25), Cf(db), B("foo"))));
            txn.Put(Cf(db), B("foo"), B("mine"));
            Assert.AreEqual("mine", S(txn.Get(At(25), Cf(db), B("foo"))));
            txn.Delete(Cf(db), B("other"));
            txn.Merge(B("plain"), B("ignored-without-merge-operator-is-not-read"));
            txn.Rollback();
        }
    }

    [TestMethod]
    public void PreparedCommitWithTimestampAndRecovery()
    {
        MemoryFileSystem fs = NewFs();
        using (TransactionDatabase db = OpenFresh(fs))
        {
            Transaction txn = db.BeginTransaction(s_wo, new TransactionOptions { SkipPrepare = false });
            txn.SetName("txn1");
            txn.Put(Cf(db), B("foo"), B("v1"));
            txn.Put(Cf(db), B("bar"), B("v2"));
            txn.Delete(Cf(db), B("baz"));
            txn.Prepare();
            AssertFails(StatusCode.InvalidArgument, txn.Commit);
            txn.SetCommitTimestamp(23);
            txn.Commit();
            txn.Dispose();
            Assert.AreEqual(("v1", 23UL), GetAt(db, "foo", 30));

            // A second prepared transaction is committed, then the DB is reopened without a flush: the WAL replays the
            // prepared section and applies the commit timestamp of the commit marker (MarkCommitWithTimestamp).
            Transaction txn2 = db.BeginTransaction(s_wo, new TransactionOptions { SkipPrepare = false });
            txn2.SetName("txn2");
            txn2.Put(Cf(db), B("foo"), B("v3"));
            txn2.Prepare();
            txn2.SetCommitTimestamp(40);
            txn2.Commit();
            txn2.Dispose();
            using Transaction txn3 = db.BeginTransaction(s_wo);
            txn3.Put(Cf(db), B("qux"), B("v4"));
            txn3.SetCommitTimestamp(41);
            txn3.Commit();
        }
        using (TransactionDatabase db = Open(fs))
        {
            Assert.AreEqual(("v1", 23UL), GetAt(db, "foo", 30));
            Assert.AreEqual(("v3", 40UL), GetAt(db, "foo", 40));
            Assert.AreEqual(("v2", 23UL), GetAt(db, "bar", 100));
            Assert.AreEqual(("v4", 41UL), GetAt(db, "qux", 41));
            Assert.AreEqual((null, ulong.MaxValue), GetAt(db, "qux", 40));
        }
    }

    [TestMethod]
    public void GetForUpdateValidatesReadTimestamp()
    {
        MemoryFileSystem fs = NewFs();
        using TransactionDatabase db = OpenFresh(fs);
        using (Transaction txn1 = db.BeginTransaction(s_wo))
        {
            // Blind writes need no read timestamp.
            txn1.Put(Cf(db), B("key"), B("value1"));
            txn1.Put(Cf(db), B("foo"), B("value1"));
            txn1.SetCommitTimestamp(24);
            txn1.Commit();
        }
        // Validation fails: timestamp in the DB (24) > read timestamp (23).
        using (Transaction txn0 = db.BeginTransaction(s_wo))
        {
            txn0.SetReadTimestampForValidation(23);
            AssertFails(StatusCode.Busy, () => txn0.GetForUpdate(null, Cf(db), B("key"), exclusive: true));
            txn0.Rollback();
        }
        // Validation passes: 24 < 25.
        using (Transaction txn2 = db.BeginTransaction(s_wo))
        {
            txn2.SetReadTimestampForValidation(25);
            Assert.AreEqual("value1", S(txn2.GetForUpdate(null, Cf(db), B("key"), exclusive: true)));
            // A different read timestamp in ReadOptions is invalid.
            AssertFails(StatusCode.InvalidArgument, () => txn2.GetForUpdate(At(24), Cf(db), B("foo"), exclusive: true));
            Assert.AreEqual("value1", S(txn2.GetForUpdate(At(25), Cf(db), B("foo"), exclusive: true)));
            // The commit timestamp must be above the read timestamp; the read timestamp cannot decrease.
            AssertFails(StatusCode.InvalidArgument, () => txn2.SetCommitTimestamp(25));
            AssertFails(StatusCode.InvalidArgument, () => txn2.SetReadTimestampForValidation(20));
            txn2.Put(Cf(db), B("key"), B("value2"));
            txn2.SetCommitTimestamp(26);
            txn2.Commit();
        }
        Assert.AreEqual(("value2", 26UL), GetAt(db, "key", 26));
        // No read timestamp: GetForUpdate with validation is invalid.
        using (Transaction txn4 = db.BeginTransaction(s_wo))
        {
            AssertFails(StatusCode.InvalidArgument, () => txn4.GetForUpdate(null, Cf(db), B("key"), exclusive: true));
            AssertFails(StatusCode.InvalidArgument, () => txn4.GetForUpdate(At(27), Cf(db), B("key"), exclusive: true));
            txn4.Rollback();
        }
        // Writes after setting a read timestamp are validated as well (RefineReadTimestamp / CheckKeysForConflicts).
        using (Transaction txn5 = db.BeginTransaction(s_wo))
        {
            txn5.SetReadTimestampForValidation(25);
            AssertFails(StatusCode.Busy, () => txn5.Put(Cf(db), B("key"), B("x")));
            txn5.SetReadTimestampForValidation(30);
            txn5.Put(Cf(db), B("key"), B("x"));
            txn5.SetCommitTimestamp(31);
            txn5.Commit();
        }
        Assert.AreEqual(("x", 31UL), GetAt(db, "key", 40));
    }

    [TestMethod]
    public void UdtValidationDisabled()
    {
        MemoryFileSystem fs = NewFs();
        using TransactionDatabase db = OpenFresh(fs, new TransactionDbOptions { EnableUdtValidation = false });
        using (Transaction txn1 = db.BeginTransaction(s_wo))
        {
            txn1.Put(Cf(db), B("key"), B("value1"));
            txn1.SetCommitTimestamp(24);
            txn1.Commit();
        }
        using Transaction txn = db.BeginTransaction(s_wo);
        txn.SetReadTimestampForValidation(23);
        // read_timestamp is set but timestamp validation is disabled for the DB.
        AssertFails(StatusCode.InvalidArgument, () => txn.GetForUpdate(null, Cf(db), B("key"), exclusive: true));
    }

    [TestMethod]
    public void IteratorOverBatchAndTimestampedDb()
    {
        MemoryFileSystem fs = NewFs();
        using TransactionDatabase db = OpenFresh(fs);
        using (Transaction t = db.BeginTransaction(s_wo))
        {
            t.Put(Cf(db), B("a"), B("a1"));
            t.Put(Cf(db), B("c"), B("c1"));
            t.SetCommitTimestamp(10);
            t.Commit();
        }
        db.Database.Flush();
        using Transaction txn = db.BeginTransaction(s_wo);
        txn.Put(Cf(db), B("b"), B("b-txn"));
        txn.Delete(Cf(db), B("c"));
        using TransactionIterator it = txn.NewIterator(Cf(db), At(20));
        var seen = new List<string>();
        for (it.SeekToFirst(); it.Valid; it.Next())
        {
            ulong ts = it.Timestamp.Length == 8 ? BinaryPrimitives.ReadUInt64LittleEndian(it.Timestamp) : 0;
            seen.Add(S(it.Key.ToArray()) + "=" + S(it.Value.ToArray()) + "@" + ts);
        }
        CollectionAssert.AreEqual(new[] { "a=a1@10", "b=b-txn@0" }, seen);
        txn.Rollback();
    }

    /// <summary>
    /// Upstream parity: OptimisticTransaction has no timestamp support. SetCommitTimestamp / SetReadTimestampForValidation
    /// are rocksdb::Transaction's defaults (NotSupported "timestamp not supported", include/rocksdb/utilities/transaction.h);
    /// writes to a column family with timestamps are buffered with placeholder timestamps, so the commit fails in
    /// DBImpl::WriteImpl (InvalidArgument "write batch must have timestamp(s) set", db/db_impl/db_impl_write.cc).
    /// Transactions that only read such a column family (with a read timestamp) or write elsewhere commit normally.
    /// </summary>
    [TestMethod]
    public void OptimisticTransactionsCannotCommitTimestampedWrites()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.CreateMissingColumnFamilies = true;
        using OptimisticTransactionDatabase db = OptimisticTransactionDatabase.Open(DbPath, o, new OptimisticTransactionDbOptions(),
            [
                new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions()),
                new ColumnFamilyDescriptor("test_cf", new ColumnFamilyOptions { Comparator = Comparator.BytewiseWithU64Ts }),
            ]);
        ColumnFamily cf = db.Database.GetColumnFamily("test_cf");
        cf.Put(B("r"), Ts(5), B("db-r"));
        using (Transaction txn = db.BeginTransaction(s_wo))
        {
            var e = Assert.ThrowsExactly<RocksDbException>(() => txn.SetCommitTimestamp(1));
            Assert.AreEqual(StatusCode.NotSupported, e.Code);
            StringAssert.Contains(e.Message, "timestamp not supported");
            AssertFails(StatusCode.NotSupported, () => txn.SetReadTimestampForValidation(1));
            txn.Put(cf, B("k"), B("v"));
            Assert.AreEqual("v", S(txn.Get(At(100), cf, B("k")))); // read-your-own-writes through the index
            e = Assert.ThrowsExactly<RocksDbException>(txn.Commit);
            Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
            StringAssert.Contains(e.Message, "write batch must have timestamp(s) set");
        }
        var v = new ArrayBufferWriter<byte>();
        Assert.IsFalse(cf.TryGet(At(ulong.MaxValue), B("k"), v));
        using (Transaction txn = db.BeginTransaction(s_wo))
        {
            // Read-only use of the timestamped column family (GetForUpdate tracks the key) plus writes elsewhere.
            Assert.AreEqual("db-r", S(txn.GetForUpdate(At(10), cf, B("r"), exclusive: true)));
            txn.Put(B("plain"), B("p"));
            txn.Commit();
        }
        Assert.AreEqual("p", S(db.Database.Get(B("plain"))));
    }

    /// <summary>
    /// write_committed_transaction_ts_test.cc GetForUpdate do_validate = false: the key is locked but not validated, so a
    /// newer committed version does not fail the read; a read timestamp or snapshot is not defined without validation.
    /// </summary>
    [TestMethod]
    public void GetForUpdateWithoutValidation()
    {
        MemoryFileSystem fs = NewFs();
        using TransactionDatabase db = OpenFresh(fs);
        ColumnFamily cf = Cf(db);
        cf.Put(B("k"), Ts(20), B("v20"));
        using (Transaction txn = db.BeginTransaction(s_wo))
        {
            // Without validation and without a read timestamp the newest version is read and locked.
            Assert.AreEqual("v20", S(txn.GetForUpdate(null, cf, B("k"), exclusive: true, doValidate: false)));
            txn.Put(cf, B("k"), B("v30"));
            txn.SetCommitTimestamp(30);
            txn.Commit();
        }
        Assert.AreEqual(("v30", 30UL), GetAt(db, "k", 30));
        using (Transaction txn = db.BeginTransaction(s_wo))
        {
            txn.SetReadTimestampForValidation(25);
            // With validation the version at 30 > read timestamp 25 is a conflict...
            AssertFails(StatusCode.Busy, () => txn.GetForUpdate(null, cf, B("k"), exclusive: true));
            // ...and without validation a read timestamp is not defined.
            var e = Assert.ThrowsExactly<RocksDbException>(() => txn.GetForUpdate(null, cf, B("k"), exclusive: true, doValidate: false));
            Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
            StringAssert.Contains(e.Message, "If do_validate is false then GetForUpdate with read_timestamp is not defined.");
        }
        using (Transaction txn = db.BeginTransaction(s_wo))
        using (Snapshot snap = db.Database.GetSnapshot())
        {
            var e = Assert.ThrowsExactly<RocksDbException>(() => txn.GetForUpdate(new ReadOptions { Snapshot = snap }, db.Database.DefaultColumnFamily, B("x"),
                exclusive: true, doValidate: false));
            Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
            StringAssert.Contains(e.Message, "If do_validate is false then GetForUpdate with snapshot is not defined.");
        }
        // A shared lock without validation on a missing key; the commit has nothing to apply.
        using (Transaction txn = db.BeginTransaction(s_wo))
        {
            var w = new ArrayBufferWriter<byte>();
            Assert.IsFalse(txn.TryGetForUpdate(null, db.Database.DefaultColumnFamily, B("other"), w, exclusive: false, doValidate: false));
            txn.Commit();
        }
    }

    /// <summary>
    /// write_committed_transaction_ts_test.cc WritesBypassTransactionAPIs: timestamped writes made directly to the
    /// transaction's WriteBatch get the commit timestamp, with and without write_batch_track_timestamp_size.
    /// </summary>
    [TestMethod]
    public void WritesBypassTransactionApis()
    {
        MemoryFileSystem fs = NewFs();
        using TransactionDatabase db = OpenFresh(fs);
        ColumnFamily cf = Cf(db);
        foreach (bool track in new[] { true, false })
        {
            string tag = track ? "t" : "u";
            using Transaction txn = db.BeginTransaction(s_wo, new TransactionOptions { WriteBatchTrackTimestampSize = track });
            txn.Put(B("plain" + tag), B("p"));
            // Placeholder (zero) timestamp, replaced at commit.
            txn.WriteBatch.Put(cf.Id, B("foobarbaz" + tag), Ts(0), B("baz_val"));
            txn.WriteBatch.Put(db.Database.DefaultColumnFamily.Id, B("direct" + tag), B("d"));
            txn.SetName("txn" + tag);
            txn.SetCommitTimestamp(2);
            txn.Prepare();
            txn.Commit();
            Assert.AreEqual(("baz_val", 2UL), GetAt(db, "foobarbaz" + tag, 2), tag);
            Assert.AreEqual("d", S(db.Database.Get(B("direct" + tag))), tag);
        }
        db.Database.GetColumnFamily("test_cf").Flush();
        Assert.AreEqual(("baz_val", 2UL), GetAt(db, "foobarbazt", 3));
        Assert.AreEqual(("baz_val", 2UL), GetAt(db, "foobarbazu", 3));
    }

    [TestMethod]
    public void WriteBatchWithIndexOnTimestampedColumnFamily()
    {
        MemoryFileSystem fs = NewFs();
        using TransactionDatabase tdb = OpenFresh(fs);
        RocksDatabase db = tdb.Database;
        ColumnFamily cf = db.GetColumnFamily("test_cf");
        cf.Put(B("x"), Ts(5), B("db-x"));
        cf.Put(B("y"), Ts(5), B("db-y"));
        using var wbwi = new WriteBatchWithIndex(Comparator.Bytewise, 0, true, 0);
        wbwi.Put(cf, B("x"), B("batch-x"));
        wbwi.Put(cf, B("x"), B("batch-x2")); // overwrite_key: one index entry
        wbwi.Delete(cf, B("y"));
        // The batch keys carry a placeholder timestamp; the index and lookups use the key without it.
        Assert.AreEqual("batch-x2", S(wbwi.GetFromBatch(cf, B("x"))));
        AssertFails(StatusCode.InvalidArgument, () => wbwi.GetFromBatchAndDB(db, null, cf, B("x")));
        Assert.AreEqual("batch-x2", S(wbwi.GetFromBatchAndDB(db, At(10), cf, B("x"))));
        Assert.IsNull(wbwi.GetFromBatchAndDB(db, At(10), cf, B("y")));
        cf.Put(B("z"), Ts(5), B("db-z"));
        Assert.AreEqual("db-z", S(wbwi.GetFromBatchAndDB(db, At(10), cf, B("z"))));
        {
            WriteBatchWithIndexIterator it = wbwi.NewIterator(cf);
            it.SeekToFirst();
            Assert.AreEqual("x", S(it.Key.ToArray()));
            it.Next();
            Assert.AreEqual("y", S(it.Key.ToArray()));
        }
        // Assign a timestamp and write the batch.
        wbwi.WriteBatch.UpdateTimestamps(Ts(7), id => id == cf.Id ? 8 : 0);
        db.Write(wbwi);
        Assert.AreEqual("batch-x2", S(cf.Get(At(7), B("x"))));
        Assert.AreEqual("db-x", S(cf.Get(At(6), B("x"))));
        Assert.IsNull(cf.Get(At(7), B("y")));
    }
}
