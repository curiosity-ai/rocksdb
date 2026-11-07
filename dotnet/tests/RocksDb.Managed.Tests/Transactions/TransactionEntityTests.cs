using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Utilities.Transactions;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Transactions;

/// <summary>
/// Wide-column entities in transactions and WriteBatchWithIndex: ports of the entity cases of
/// utilities/transactions/transaction_test.cc (GetEntity / MultiGetEntity / GetEntityForUpdate / PutEntity,
/// CoalescingIterator / AttributeGroupIterator) and utilities/write_batch_with_index/write_batch_with_index_test.cc
/// (GetEntityFromBatchAndDB / MultiGetEntityFromBatchAndDB), for every write policy.
/// </summary>
[TestClass]
public class TransactionEntityTests
{
    protected virtual TransactionWritePolicy Policy => TransactionWritePolicy.WriteCommitted;

    private static readonly WriteOptions s_wo = new();
    private static readonly TimeSpan s_short = TimeSpan.FromMilliseconds(50);

    private TransactionDatabase Open(MemoryFileSystem fs, ColumnFamilyOptions? cfo = null) =>
        TransactionDatabase.Open(DbPath, NewOptions(fs),
            new TransactionDbOptions { WritePolicy = Policy, WpCommitCacheBits = 10, TransactionLockTimeout = s_short, DefaultLockTimeout = s_short },
            [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, cfo ?? new ColumnFamilyOptions { MergeOperator = MergeOperator.StringAppend(",") })]);

    private static WideColumn C(string name, string value) => new(B(name), B(value));

    /// <summary>"name=value;" per column, in column order (empty name = default column).</summary>
    private static string Describe(WideColumns c)
    {
        var sb = new StringBuilder();
        for (int i = 0; i < c.Count; i++) sb.Append(Encoding.UTF8.GetString(c.GetName(i))).Append('=').Append(Encoding.UTF8.GetString(c.GetValue(i))).Append(';');
        return sb.ToString();
    }

    private static string? Entity(Transaction t, string key)
    {
        var c = new WideColumns();
        return t.TryGetEntity(null, B(key), c) ? Describe(c) : null;
    }

    private static void AssertFails(StatusCode code, Action a, StatusSubCode sub = StatusSubCode.None)
    {
        var e = Assert.ThrowsExactly<RocksDbException>(a);
        Assert.AreEqual(code, e.Code, e.Message);
        if (sub != StatusSubCode.None) Assert.AreEqual(sub, e.SubCode, e.Message);
    }

    /// <summary>The DB contents the read tests start from.</summary>
    private static void Seed(TransactionDatabase db)
    {
        db.Database.PutEntity(B("entity"), [C("", "d"), C("b", "2"), C("a", "1")]);
        db.Database.PutEntity(B("nodefault"), [C("x", "9")]);
        db.Database.Put(B("plain"), B("p"));
        db.Database.Put(B("deleted-in-txn"), B("old"));
    }

    [TestMethod]
    public void PutEntityAndGetEntity()
    {
        using TransactionDatabase db = Open(NewFs());
        Seed(db);
        using Transaction txn = db.BeginTransaction(s_wo);
        // DB-only keys: entity, plain value as a default column, missing
        Assert.AreEqual("=d;a=1;b=2;", Entity(txn, "entity"));
        Assert.AreEqual("=p;", Entity(txn, "plain"));
        Assert.IsNull(Entity(txn, "missing"));
        // The transaction's own writes win
        txn.PutEntity(B("new"), [C("q", "1"), C("", "nd")]);
        txn.Put(B("entity"), B("now plain"));
        txn.Delete(B("deleted-in-txn"));
        Assert.AreEqual("=nd;q=1;", Entity(txn, "new"));
        Assert.AreEqual("=now plain;", Entity(txn, "entity"));
        Assert.IsNull(Entity(txn, "deleted-in-txn"));
        Assert.AreEqual("nd", S(txn.Get(B("new")))); // a plain read returns the default column
        // Not visible outside before commit
        Assert.IsNull(db.Database.GetEntity(B("new")));
        txn.Commit();
        WideColumns? committed = db.Database.GetEntity(B("new"));
        Assert.IsNotNull(committed);
        Assert.AreEqual("=nd;q=1;", Describe(committed));
        Assert.AreEqual("now plain", GetString(db.Database, "entity"));
    }

    [TestMethod]
    public void MergeOnTopOfDbEntityKeepsOtherColumns()
    {
        using TransactionDatabase db = Open(NewFs());
        Seed(db);
        using Transaction txn = db.BeginTransaction(s_wo);
        txn.Merge(B("entity"), B("m1"));
        txn.Merge(B("entity"), B("m2"));
        txn.Merge(B("nodefault"), B("m"));
        txn.Merge(B("plain"), B("m"));
        txn.Merge(B("missing"), B("m"));
        Assert.AreEqual("=d,m1,m2;a=1;b=2;", Entity(txn, "entity"));
        Assert.AreEqual("=,m;x=9;", Entity(txn, "nodefault")); // an empty default column is the base (kWideBaseValue)
        Assert.AreEqual("=p,m;", Entity(txn, "plain"));
        Assert.AreEqual("=m;", Entity(txn, "missing"));
        // Merges on top of the transaction's own PutEntity / Delete
        txn.PutEntity(B("own"), [C("", "o"), C("k", "v")]);
        txn.Merge(B("own"), B("x"));
        txn.Delete(B("gone"));
        txn.Merge(B("gone"), B("y"));
        Assert.AreEqual("=o,x;k=v;", Entity(txn, "own"));
        Assert.AreEqual("=y;", Entity(txn, "gone"));
        txn.Commit();
        Assert.AreEqual("=d,m1,m2;a=1;b=2;", Describe(db.Database.GetEntity(B("entity"))!));
    }

    [TestMethod]
    public void MergeWithoutMergeOperatorFails()
    {
        using TransactionDatabase db = Open(NewFs(), new ColumnFamilyOptions());
        db.Database.PutEntity(B("e"), [C("", "d")]);
        using Transaction txn = db.BeginTransaction(s_wo);
        txn.Merge(B("e"), B("x"));
        AssertFails(StatusCode.InvalidArgument, () => Entity(txn, "e"));
    }

    [TestMethod]
    public async Task AsyncEntityApis()
    {
        using TransactionDatabase db = Open(NewFs());
        Seed(db);
        using Transaction txn = db.BeginTransaction(s_wo);
        await txn.PutEntityAsync(B("async"), new WideColumn[] { C("", "v"), C("c", "1") });
        var c = new WideColumns();
        Assert.IsTrue(await txn.TryGetEntityAsync(null, B("async"), c));
        Assert.AreEqual("=v;c=1;", Describe(c));
        Assert.IsTrue(await txn.TryGetEntityAsync(null, B("entity"), c));
        Assert.AreEqual("=d;a=1;b=2;", Describe(c));
        Assert.IsFalse(await txn.TryGetEntityAsync(null, B("missing"), c));
        Assert.AreEqual(0, c.Count);
        Assert.IsTrue(await txn.TryGetEntityForUpdateAsync(null, null!, B("plain"), c, exclusive: true));
        Assert.AreEqual("=p;", Describe(c));
        await txn.CommitAsync();
        Assert.AreEqual("=v;c=1;", Describe(db.Database.GetEntity(B("async"))!));
    }

    [TestMethod]
    public void GetEntityForUpdateLocksAndValidates()
    {
        using TransactionDatabase db = Open(NewFs());
        Seed(db);
        using Transaction t1 = db.BeginTransaction(s_wo);
        using Transaction t2 = db.BeginTransaction(s_wo);
        var c = new WideColumns();
        Assert.IsTrue(t1.TryGetEntityForUpdate(null, B("entity"), c));
        Assert.AreEqual("=d;a=1;b=2;", Describe(c));
        // The key is locked by t1
        AssertFails(StatusCode.TimedOut, () => t2.PutEntity(B("entity"), [C("", "t2")]), StatusSubCode.LockTimeout);
        AssertFails(StatusCode.TimedOut, () => t2.TryGetEntityForUpdate(null, B("entity"), c), StatusSubCode.LockTimeout);
        t1.Rollback();
        // Snapshot validation: written after t2's snapshot
        t2.SetSnapshot();
        db.Database.PutEntity(B("entity"), [C("", "later")]);
        AssertFails(StatusCode.Busy, () => t2.TryGetEntityForUpdate(null, B("entity"), c));
        // No validation with a snapshot is undefined
        using Snapshot snap = db.Database.GetSnapshot();
        AssertFails(StatusCode.InvalidArgument,
            () => t2.TryGetEntityForUpdate(new ReadOptions { Snapshot = snap }, null!, B("plain"), c, exclusive: true, doValidate: false));
        // Without validation the key is still locked
        Assert.IsTrue(t2.TryGetEntityForUpdate(null, null!, B("plain"), c, exclusive: false, doValidate: false));
        Assert.AreEqual("=p;", Describe(c));
    }

    [TestMethod]
    public void MultiGetEntityMixesBatchAndDb()
    {
        using TransactionDatabase db = Open(NewFs());
        Seed(db);
        using Transaction txn = db.BeginTransaction(s_wo);
        txn.PutEntity(B("new"), [C("n", "1")]);
        txn.Delete(B("deleted-in-txn"));
        txn.Merge(B("entity"), B("m"));
        txn.Put(B("plain"), B("p2"));
        using var batch = new MultiGetBatch(8);
        string[] keys = ["entity", "new", "deleted-in-txn", "plain", "missing", "nodefault"];
        foreach (string k in keys) batch.Add(B(k));
        txn.MultiGetEntity(null, batch);
        string?[] expected = ["=d,m;a=1;b=2;", "n=1;", null, "=p2;", null, "x=9;"];
        var c = new WideColumns();
        for (int i = 0; i < keys.Length; i++)
        {
            Assert.AreEqual(expected[i] is null ? MultiGetStatus.NotFound : MultiGetStatus.Found, batch.GetStatus(i), keys[i]);
            Assert.AreEqual(expected[i] is not null, batch.TryGetEntity(i, c), keys[i]);
            if (expected[i] is not null) Assert.AreEqual(expected[i], Describe(c), keys[i]);
        }
        // The default column through the value accessors
        Assert.AreEqual("d,m", S(batch.GetValue(0).ToArray()));
        // A plain MultiGet afterwards on the same batch is a value run again
        using var wbwi = new WriteBatchWithIndex();
        wbwi.MultiGetFromBatchAndDB(db.Database, batch);
        Assert.AreEqual("d", S(batch.GetValue(0).ToArray()));
    }

    [TestMethod]
    public void TimestampArgumentsAreChecked()
    {
        using TransactionDatabase db = Open(NewFs());
        using Transaction txn = db.BeginTransaction(s_wo);
        var ts = new byte[8];
        AssertFails(StatusCode.InvalidArgument, () => txn.TryGetEntity(new ReadOptions { Timestamp = ts }, B("k"), new WideColumns()));
        using var batch = new MultiGetBatch(1);
        batch.Add(B("k"));
        txn.MultiGetEntity(new ReadOptions { Timestamp = ts }, batch);
        Assert.AreEqual(MultiGetStatus.Error, batch.GetStatus(0));
        Assert.AreEqual(StatusCode.InvalidArgument, batch.GetError(0)!.Code);
    }

    [TestMethod]
    public void CoalescingAndAttributeGroupIterators()
    {
        using TransactionDatabase db = Open(NewFs());
        ColumnFamily cf1 = db.Database.CreateColumnFamily("cf1", new ColumnFamilyOptions());
        ColumnFamily cf2 = db.Database.CreateColumnFamily("cf2", new ColumnFamilyOptions());
        cf1.PutEntity(B("a"), [C("x", "1"), C("y", "1")]);
        cf2.PutEntity(B("a"), [C("y", "2"), C("z", "2")]);
        cf1.Put(B("b"), B("plain1"));
        cf2.PutEntity(B("c"), [C("w", "c2")]);
        using Transaction txn = db.BeginTransaction(s_wo);
        if (Policy != TransactionWritePolicy.WriteCommitted)
        {
            AssertFails(StatusCode.NotSupported, () => txn.GetCoalescingIterator(null, [cf1, cf2]).Dispose());
            AssertFails(StatusCode.NotSupported, () => txn.GetAttributeGroupIterator(null, [cf1, cf2]).Dispose());
            return;
        }
        // The transaction's writes are part of the view
        txn.PutEntity(cf2, B("b"), [C("", "txn2"), C("t", "t")]);
        txn.Delete(cf2, B("c"));
        txn.Put(cf1, B("d"), B("txn1"));

        using (TransactionCoalescingIterator it = txn.GetCoalescingIterator(null, [cf1, cf2]))
        {
            var seen = new List<string>();
            for (it.SeekToFirst(); it.Valid; it.Next()) seen.Add(S(it.Key.ToArray()) + ":" + Describe(it.Columns));
            CollectionAssert.AreEqual(new[] { "a:x=1;y=2;z=2;", "b:=txn2;t=t;", "d:=txn1;" }, seen);
            seen.Clear();
            for (it.SeekToLast(); it.Valid; it.Prev()) seen.Add(S(it.Key.ToArray())!);
            CollectionAssert.AreEqual(new[] { "d", "b", "a" }, seen);
            it.Seek(B("b"));
            Assert.AreEqual("txn2", S(it.Value.ToArray()));
            it.Prev();
            Assert.AreEqual("a", S(it.Key.ToArray()));
            it.Next();
            Assert.AreEqual("b", S(it.Key.ToArray()));
        }
        using (TransactionAttributeGroupIterator it = txn.GetAttributeGroupIterator(null, [cf1, cf2]))
        {
            it.Seek(B("b"));
            Assert.IsTrue(it.Valid);
            IReadOnlyList<IteratorAttributeGroup> groups = it.AttributeGroups;
            Assert.AreEqual(2, groups.Count);
            Assert.AreEqual(cf1.Id, groups[0].ColumnFamily.Id);
            Assert.AreEqual("=plain1;", Describe(groups[0].Columns));
            Assert.AreEqual("=txn2;t=t;", Describe(groups[1].Columns));
            it.Next();
            Assert.AreEqual("d", S(it.Key.ToArray()));
            Assert.AreEqual(1, it.AttributeGroups.Count);
        }
        AssertFails(StatusCode.InvalidArgument, () => txn.GetCoalescingIterator(null, []).Dispose());
        ColumnFamily rev = db.Database.CreateColumnFamily("rev", new ColumnFamilyOptions { Comparator = Comparator.ReverseBytewise });
        AssertFails(StatusCode.InvalidArgument, () => txn.GetAttributeGroupIterator(null, [cf1, rev]).Dispose());
    }

    [TestMethod]
    public void PutEntityUntrackedSkipsConflictChecks()
    {
        using TransactionDatabase db = Open(NewFs());
        using Transaction t1 = db.BeginTransaction(s_wo);
        t1.SetSnapshot();
        db.Database.Put(B("k"), B("outside"));
        // A tracked write would fail snapshot validation (Busy); untracked writes do not validate.
        AssertFails(StatusCode.Busy, () => t1.PutEntity(B("k"), [C("", "x")]));
        t1.PutEntityUntracked(B("k"), [C("", "untracked")]);
        t1.Commit();
        Assert.AreEqual("untracked", GetString(db.Database, "k"));
    }

    [TestMethod]
    public void WriteUnpreparedReadsItsFlushedEntities()
    {
        if (Policy != TransactionWritePolicy.WriteUnprepared) return;
        using TransactionDatabase db = Open(NewFs());
        Seed(db);
        using Transaction txn = db.BeginTransaction(s_wo, new TransactionOptions { WriteBatchFlushThreshold = 1 });
        txn.SetName("wup");
        txn.PutEntity(B("u1"), [C("", "1"), C("c", "x")]);
        txn.Put(B("u2"), B("2")); // flushes u1 to the DB as an unprepared batch
        txn.Merge(B("u1"), B("m"));
        txn.Put(B("u3"), B("3")); // flushes the merge
        Assert.AreEqual("=1,m;c=x;", Entity(txn, "u1"));
        using var batch = new MultiGetBatch(2);
        batch.Add(B("u1"));
        batch.Add(B("entity"));
        txn.MultiGetEntity(null, batch);
        var c = new WideColumns();
        Assert.IsTrue(batch.TryGetEntity(0, c));
        Assert.AreEqual("=1,m;c=x;", Describe(c));
        Assert.IsTrue(batch.TryGetEntity(1, c));
        Assert.AreEqual("=d;a=1;b=2;", Describe(c));
        // Others do not see the unprepared data
        Assert.IsNull(db.Database.GetEntity(B("u1")));
        txn.Prepare();
        txn.Commit();
        Assert.AreEqual("=1,m;c=x;", Describe(db.Database.GetEntity(B("u1"))!));
    }

    [TestMethod]
    public void OptimisticTransactionEntities()
    {
        using OptimisticTransactionDatabase db = OptimisticTransactionDatabase.Open(DbPath, NewOptions(NewFs()));
        db.Database.PutEntity(B("e"), [C("", "base"), C("c", "1")]);
        using Transaction t1 = db.BeginTransaction(s_wo);
        using Transaction t2 = db.BeginTransaction(s_wo);
        var c = new WideColumns();
        Assert.IsTrue(t1.TryGetEntityForUpdate(null, B("e"), c));
        Assert.AreEqual("=base;c=1;", Describe(c));
        t2.PutEntity(B("e"), [C("", "t2")]);
        t2.Commit();
        t1.PutEntity(B("e"), [C("", "t1")]);
        AssertFails(StatusCode.Busy, t1.Commit);
        Assert.AreEqual("=t2;", Describe(db.Database.GetEntity(B("e"))!));
        using Transaction t3 = db.BeginTransaction(s_wo);
        using TransactionCoalescingIterator it = t3.GetCoalescingIterator(null, [db.Database.DefaultColumnFamily]);
        it.SeekToFirst();
        Assert.AreEqual("e", S(it.Key.ToArray()));
    }

    // ---- WriteBatchWithIndex ------------------------------------------------------------------------------

    [TestMethod]
    public void WriteBatchWithIndexGetEntityFromBatchAndDb()
    {
        using TransactionDatabase txnDb = Open(NewFs());
        RocksDatabase db = txnDb.Database;
        Seed(txnDb);
        using var wbwi = new WriteBatchWithIndex(Comparator.Bytewise, 0, overwriteKey: true, 0);
        wbwi.PutEntity(B("new"), [C("", "n"), C("k", "v")]);
        wbwi.Merge(B("entity"), B("w"));
        wbwi.Delete(B("plain"));
        var c = new WideColumns();
        Assert.IsTrue(wbwi.TryGetEntityFromBatchAndDB(db, null, null, B("new"), c));
        Assert.AreEqual("=n;k=v;", Describe(c));
        Assert.IsTrue(wbwi.TryGetEntityFromBatchAndDB(db, null, null, B("entity"), c));
        Assert.AreEqual("=d,w;a=1;b=2;", Describe(c));
        Assert.IsFalse(wbwi.TryGetEntityFromBatchAndDB(db, null, null, B("plain"), c));
        Assert.AreEqual(0, c.Count);
        Assert.AreEqual("x=9;", Describe(wbwi.GetEntityFromBatchAndDB(db, null, null, B("nodefault"))!));
        Assert.IsNull(wbwi.GetEntityFromBatchAndDB(db, null, null, B("missing")));
        // Snapshot reads of the DB side
        using Snapshot snap = db.GetSnapshot();
        db.PutEntity(B("later"), [C("", "l")]);
        Assert.IsFalse(wbwi.TryGetEntityFromBatchAndDB(db, new ReadOptions { Snapshot = snap }, null, B("later"), c));
        Assert.IsTrue(wbwi.TryGetEntityFromBatchAndDB(db, B("later"), c));

        using var batch = new MultiGetBatch(4);
        foreach (string k in new[] { "new", "entity", "plain", "nodefault", "missing" }) batch.Add(B(k));
        wbwi.MultiGetEntityFromBatchAndDB(db, batch);
        string?[] expected = ["=n;k=v;", "=d,w;a=1;b=2;", null, "x=9;", null];
        for (int i = 0; i < expected.Length; i++)
        {
            Assert.AreEqual(expected[i] is not null, batch.TryGetEntity(i, c), i.ToString());
            if (expected[i] is not null) Assert.AreEqual(expected[i], Describe(c));
        }
    }
}

/// <summary>The entity cases with the WritePrepared policy.</summary>
[TestClass]
public sealed class WritePreparedTransactionEntityTests : TransactionEntityTests
{
    protected override TransactionWritePolicy Policy => TransactionWritePolicy.WritePrepared;
}

/// <summary>The entity cases with the WriteUnprepared policy.</summary>
[TestClass]
public sealed class WriteUnpreparedTransactionEntityTests : TransactionEntityTests
{
    protected override TransactionWritePolicy Policy => TransactionWritePolicy.WriteUnprepared;
}
