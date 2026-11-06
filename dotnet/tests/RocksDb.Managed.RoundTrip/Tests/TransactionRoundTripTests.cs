using System.Globalization;
using System.Text;
using RocksDb.Managed.Compression;
using RocksDb.Managed.RoundTrip.Engines;
using RocksDb.Managed.RoundTrip.Harness;
using RocksDb.Managed.RoundTrip.Protocol;

namespace RocksDb.Managed.RoundTrip.Tests;

/// <summary>
/// Two-phase commit across engines: a pessimistic TransactionDB (WriteCommitted) alternately opened by native
/// (rocksdb_transactiondb_*) and the managed <see cref="Utilities.Transactions.TransactionDatabase"/>. Every round
/// checks that exactly the transactions the previous engine left prepared were recovered as prepared (and their
/// writes are invisible), commits or rolls back some of them, mixes plain writes, flushes and compactions with new
/// transactions (multi-CF puts, merges and deletes), prepares some, flushes between prepare and commit (the prepared
/// section's WAL must outlive the flush), and ends either cleanly with transactions still prepared or by killing the
/// process. The model applies a transaction's writes only at commit.
/// </summary>
[TestClass]
public sealed class TransactionRoundTripTests : RoundTripTestBase
{
    private static EngineKind Other(EngineKind e) => e == EngineKind.Native ? EngineKind.Managed : EngineKind.Native;

    [TestMethod]
    [DataRow(EngineKind.Native, false, 64UL << 20)]
    [DataRow(EngineKind.Managed, false, 128UL << 10)]
    [DataRow(EngineKind.Native, true, 96UL << 10)]
    [DataRow(EngineKind.Managed, true, 64UL << 20)]
    public async Task PreparedTransactions_SurviveEngineSwitch(EngineKind first, bool walOptions, ulong writeBuffer)
    {
        DbSpec spec = DbSpec.Standard(CompressionType.LZ4, 6, FilterKind.Bloom, writeBuffer, false);
        spec.ColumnFamilies.RemoveAll(c => c.Name == "late");
        spec.TransactionDb = true;
        if (walOptions)
        {
            spec.Db.WalCompression = CompressionType.Zstd;
            spec.Db.RecycleLogFileNum = 2;
            spec.Db.TrackAndVerifyWalsInManifest = true;
            spec.Db.MaxTotalWalSize = 128 << 10;
        }
        RoundTripHarness h = CreateHarness(spec, $"{first}-{(walOptions ? "wal-options" : "defaults")}-{writeBuffer >> 10}K");
        var gen = new GeneratorOptions
        {
            // TransactionDB::Write rejects DeleteRange and SingleDelete on non-default CFs (LockBatch handler).
            Ops = 300, KeySpace = 800, AllowSnapshots = false, AllowColumnFamilyChanges = false, AllowRangeDelete = false, AllowSingleDelete = false,
        };
        int txnCounter = 0;
        EngineKind engine = first;
        int rounds = LongRuns ? 12 : 6;
        for (int round = 0; round < rounds; round++)
        {
            ScriptBuilder b = h.NewScript($"{engine}-txn-{round}");
            b.Add(Op.TxnCheckPrepared(new List<string>(h.Model.PreparedTransactions)));
            Random rnd = h.Random;
            // Resolve about half of the recovered prepared transactions.
            foreach (string name in new List<string>(h.Model.PreparedTransactions))
            {
                int pick = rnd.Next(4);
                if (pick == 0) b.Add(Op.TxnCommit(name));
                else if (pick == 1) b.Add(Op.TxnRollback(name));
            }
            OpGenerator.Generate(b, gen);
            int newTxns = 2 + rnd.Next(3);
            var preparedHere = new List<string>();
            for (int t = 0; t < newTxns; t++)
            {
                string name = "x" + (++txnCounter).ToString(CultureInfo.InvariantCulture);
                b.Add(Op.TxnBegin(name));
                int n = 1 + rnd.Next(12);
                for (int j = 0; j < n; j++)
                {
                    byte[] key = Encoding.ASCII.GetBytes($"txn/{name}/{j % 5}");
                    switch (rnd.Next(5))
                    {
                        case 0: b.Add(Op.Merge("counters", key, ReferenceModel.U64((ulong)rnd.Next(100)))); break;
                        case 1: b.Add(Op.Merge("default", key, Encoding.ASCII.GetBytes("t" + j))); break;
                        case 2: b.Add(Op.Delete("snappy", key)); break;
                        default: b.Add(Op.Put(rnd.Next(2) == 0 ? "default" : "blobs", key, OpGenerator.RandomBytes(rnd, rnd.Next(0, 3000)))); break;
                    }
                }
                b.Add(Op.TxnPrepare(name));
                preparedHere.Add(name);
            }
            // Flush between prepare and commit: the memtables no longer need the WAL, the prepared sections still do.
            if (rnd.Next(2) == 0) b.Add(Op.FlushAll());
            OpGenerator.Generate(b, new GeneratorOptions { Ops = 80, KeySpace = 800, AllowSnapshots = false, AllowColumnFamilyChanges = false, AllowFlush = false, AllowRangeDelete = false, AllowSingleDelete = false });
            foreach (string name in preparedHere)
            {
                if (rnd.Next(3) == 0) b.Add(Op.TxnCommit(name));
            }
            b.Add(Op.TxnCheckPrepared(new List<string>(h.Model.PreparedTransactions))).Verify();
            if (round % 2 == 1) b.Add(Op.FlushWal(true)).Add(Op.Abandon());
            else b.Dump();
            await h.RunAsync(engine, b);
            engine = Other(engine);
        }
        // Both engines: the prepared set, then resolve everything and verify.
        await h.RunAsync(engine, h.NewScript($"{engine}-check").Add(Op.TxnCheckPrepared(new List<string>(h.Model.PreparedTransactions))));
        engine = Other(engine);
        ScriptBuilder fin = h.NewScript($"{engine}-resolve-all");
        foreach (string name in new List<string>(h.Model.PreparedTransactions)) fin.Add(Op.TxnCommit(name));
        fin.Add(Op.TxnCheckPrepared([])).Add(Op.Compact("default")).Verify().Dump();
        await h.RunAsync(engine, fin);
        await h.RunAsync(Other(engine), h.NewScript($"{Other(engine)}-final").Add(Op.TxnCheckPrepared([])));
    }
}
