using System.Buffers;
using RocksDb.Managed;
using RocksDb.Managed.Utilities.Transactions;
using static QuickStart.Show;

namespace QuickStart;

/// <summary>Optimistic and pessimistic transactions, two-phase commit and WriteBatchWithIndex.</summary>
internal static class Transactions
{
    public static async Task Optimistic(string path)
    {
        // <snippet:optimistic-transactions>
        using OptimisticTransactionDatabase txnDb = OptimisticTransactionDatabase.Open(path, new DbOptions { CreateIfMissing = true });
        RocksDatabase db = txnDb.Database;                 // plain reads/writes outside transactions
        db.Put("balance"u8, "100"u8);

        using Transaction txn = txnDb.BeginTransaction(new WriteOptions(), new OptimisticTransactionOptions { SetSnapshot = true });
        byte[]? balance = txn.GetForUpdate("balance"u8);   // tracked: commit fails if someone else wrote it since
        txn.Put("balance"u8, "90"u8);
        db.Put("balance"u8, "50"u8);                       // concurrent write outside the transaction

        try
        {
            await txn.CommitAsync();
        }
        catch (RocksDbException e) when (e.Code == StatusCode.Busy)
        {
            Console.WriteLine("conflict, retry the transaction");   // optimistic validation failed
        }
        // </snippet:optimistic-transactions>
        Line($"read {Utf8(balance)}, state={txn.State}, balance now {Utf8(db.Get("balance"u8))}");
        Check(Utf8(db.Get("balance"u8)) == "50", "conflicting commit rejected");
    }

    public static Task Pessimistic(string path)
    {
        // <snippet:pessimistic-transactions>
        using TransactionDatabase txnDb = TransactionDatabase.Open(path, new DbOptions { CreateIfMissing = true },
            new TransactionDbOptions { TransactionLockTimeout = TimeSpan.FromMilliseconds(100) });

        using (Transaction t1 = txnDb.BeginTransaction(new WriteOptions()))
        using (Transaction t2 = txnDb.BeginTransaction(new WriteOptions(), new TransactionOptions { DeadlockDetect = true }))
        {
            t1.Put("k"u8, "from t1"u8);                     // takes an exclusive lock on "k"
            try
            {
                t2.Put("k"u8, "from t2"u8);                 // waits for the lock, then times out
            }
            catch (RocksDbException e) when (e.Code == StatusCode.TimedOut && e.SubCode == StatusSubCode.LockTimeout)
            {
                Console.WriteLine("lock timeout");
            }
            t1.Commit();
            t2.Rollback();
        }
        // </snippet:pessimistic-transactions>
        Line($"k = {Utf8(txnDb.Database.Get("k"u8))}");
        Check(Utf8(txnDb.Database.Get("k"u8)) == "from t1", "t1 committed");
        return Task.CompletedTask;
    }

    public static Task RangeLocks(string path)
    {
        // <snippet:range-locks>
        var rangeLocks = new RangeLockManager();             // keep it to inspect and tune the locks
        using TransactionDatabase txnDb = TransactionDatabase.Open(path, new DbOptions { CreateIfMissing = true },
            new TransactionDbOptions { RangeLockManager = rangeLocks, TransactionLockTimeout = TimeSpan.FromMilliseconds(100) });

        using Transaction t1 = txnDb.BeginTransaction(new WriteOptions());
        using Transaction t2 = txnDb.BeginTransaction(new WriteOptions());
        // Lock every key starting with "user:1" (a prefix range: the infinite suffix covers all extensions).
        t1.GetRangeLock(null, new RangeLockEndpoint("user:1"u8.ToArray()), new RangeLockEndpoint("user:1"u8.ToArray(), infSuffix: true));
        t1.Put("user:1:name"u8, "Ada"u8);                    // inside t1's range: no extra lock wait
        try
        {
            t2.Put("user:1:email"u8, "x"u8);                 // point locks are single-key ranges: conflicts
        }
        catch (RocksDbException e) when (e.Code == StatusCode.TimedOut)
        {
            Console.WriteLine("locked by t1's range");
        }
        foreach (RangeLockInfo l in rangeLocks.GetRangeLockStatusData())
            Console.WriteLine($"{l.Start} .. {l.End} held by {string.Join(",", l.TransactionIds)}");
        t1.Commit();                                          // releases the range
        t2.Put("user:1:email"u8, "ada@example.com"u8);
        t2.Commit();
        // </snippet:range-locks>
        Line($"user:1:email = {Utf8(txnDb.Database.Get("user:1:email"u8))}");
        Check(Utf8(txnDb.Database.Get("user:1:name"u8)) == "Ada" && rangeLocks.GetRangeLockStatusData().Count == 0, "range locks");
        return Task.CompletedTask;
    }

    public static Task TransactionEntities(string path)
    {
        using TransactionDatabase txnDb = TransactionDatabase.Open(path, new DbOptions { CreateIfMissing = true });
        txnDb.Database.PutEntity("user:2"u8, [new WideColumn("name"u8.ToArray(), "Grace"u8.ToArray())]);

        // <snippet:transaction-entities>
        using Transaction txn = txnDb.BeginTransaction(new WriteOptions());
        var columns = new WideColumns();
        txn.TryGetEntityForUpdate(null, "user:2"u8, columns);   // locks the key, reads its columns
        txn.PutEntity("user:2"u8, [new WideColumn("name"u8.ToArray(), "Grace"u8.ToArray()),
                                   new WideColumn("lang"u8.ToArray(), "COBOL"u8.ToArray())]);
        txn.TryGetEntity(null, "user:2"u8, columns);            // sees its own uncommitted entity: lang, name
        txn.Commit();
        // </snippet:transaction-entities>
        Line($"user:2 has {columns.Count} columns");
        Check(columns.Count == 2 && txnDb.Database.GetEntity("user:2"u8)!.Count == 2, "transaction entities");
        return Task.CompletedTask;
    }

    public static Task TwoPhaseCommit(string path)
    {
        var dbOptions = new DbOptions { CreateIfMissing = true };
        // <snippet:two-phase-commit>
        using (TransactionDatabase txnDb = TransactionDatabase.Open(path, dbOptions))
        {
            Transaction txn = txnDb.BeginTransaction(new WriteOptions { Sync = true });
            txn.SetName("xid-42");                          // 2PC transactions are named
            txn.Put("order:42"u8, "paid"u8);
            txn.Prepare();                                  // prepare record is in the WAL (native-identical format)
            // ... process stops before Commit ...
        }

        // On reopen, prepared transactions are recovered (also from WALs written by native RocksDB).
        using (TransactionDatabase txnDb = TransactionDatabase.Open(path, dbOptions))
        {
            foreach (Transaction prepared in txnDb.GetAllPreparedTransactions())
            {
                Console.WriteLine($"{prepared.Name}: {prepared.State}");   // xid-42: Prepared
                prepared.Commit();                                          // or Rollback()
                prepared.Dispose();
            }
            Console.WriteLine(txnDb.Database.Get("order:42"u8) is not null);   // True
        }
        // </snippet:two-phase-commit>
        using TransactionDatabase check = TransactionDatabase.Open(path, dbOptions);
        Line($"order:42 = {Utf8(check.Database.Get("order:42"u8))}");
        Check(Utf8(check.Database.Get("order:42"u8)) == "paid", "prepared txn committed after reopen");
        return Task.CompletedTask;
    }

    public static Task WriteBatchWithIndexExample(string path)
    {
        using RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true });
        db.Put("a"u8, "db"u8);
        db.Put("c"u8, "db"u8);

        // <snippet:write-batch-with-index>
        using var wbwi = new WriteBatchWithIndex();
        wbwi.Put("b"u8, "batch"u8).Delete("c"u8);

        // Read your own uncommitted writes, falling back to the DB.
        byte[]? b = wbwi.GetFromBatchAndDB(db, "b"u8);              // "batch"
        byte[]? c = wbwi.GetFromBatchAndDB(db, "c"u8);              // null: deleted in the batch

        // Iterate the DB merged with the batch (the merged iterator owns and disposes the base iterator).
        using (BaseDeltaIterator it = wbwi.NewIteratorWithBase(db.NewIterator()))
            for (it.SeekToFirst(); it.Valid; it.Next())
                Console.WriteLine(System.Text.Encoding.UTF8.GetString(it.Key));   // a, b

        db.Write(wbwi);                                              // apply atomically
        // </snippet:write-batch-with-index>
        Line($"b={Utf8(b)} c={Utf8(c)} after write: c={Utf8(db.Get("c"u8))}");
        Check(Utf8(b) == "batch" && c is null && db.Get("c"u8) is null, "wbwi");
        return Task.CompletedTask;
    }
}
