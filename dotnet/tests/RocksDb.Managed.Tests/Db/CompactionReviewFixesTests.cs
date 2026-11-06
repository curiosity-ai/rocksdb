using System.Buffers;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Utilities.Transactions;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>
/// Regression tests for the compaction / version / MANIFEST findings of the second correctness review
/// (write-conflict-boundary snapshots, IgnoreSnapshots, ReFitLevel guards, consistency checks, output validation,
/// oldest ancestor time, MANIFEST auto-tuning, db_paths, obsolete-file scans, round-robin cursors, trivial copy).
/// </summary>
[TestClass]
public sealed class CompactionReviewFixesTests
{
    private static readonly WriteOptions s_wo = new();

    private static readonly CompactRangeOptions s_force = new() { BottommostLevelCompaction = BottommostLevelCompaction.Force };

    // ---------------------------------------------------------------------------------------------------------
    // write-conflict-boundary snapshots (db_impl.cc GetSnapshotForWriteConflictBoundary, snapshot_impl.h)

    /// <summary>
    /// A SingleDelete + Put pair written after a transaction's snapshot must survive compaction so the transaction's
    /// conflict check still sees the write (CompactionIterator earliest_write_conflict_snapshot).
    /// </summary>
    [TestMethod]
    public void SingleDeletePairUnderPessimisticTxnSnapshotSurvivesCompaction()
    {
        MemoryFileSystem fs = NewFs();
        using TransactionDatabase db = TransactionDatabase.Open(DbPath, NewOptions(fs), new TransactionDbOptions(), NoHistoryCf());
        using Transaction txn = db.BeginTransaction(s_wo);
        txn.SetSnapshot();
        db.Put(B("k"), B("v"));
        db.SingleDelete(B("k"));
        PushOutOfMemtables(db.Database);
        var ex = Assert.ThrowsExactly<RocksDbException>(() => txn.Put(B("k"), B("txn")));
        Assert.AreEqual(StatusCode.Busy, ex.Code);
    }

    [TestMethod]
    public void SingleDeletePairUnderOptimisticTxnSnapshotSurvivesCompaction()
    {
        MemoryFileSystem fs = NewFs();
        using OptimisticTransactionDatabase db = OptimisticTransactionDatabase.Open(DbPath, NewOptions(fs), new OptimisticTransactionDbOptions(), NoHistoryCf());
        using Transaction txn = db.BeginTransaction(s_wo);
        txn.SetSnapshot();
        db.Database.Put(B("k"), B("v"));
        db.Database.SingleDelete(B("k"));
        PushOutOfMemtables(db.Database);
        // Optimistic validation only reads memtables (TryAgain without history), but its snapshot is a write-conflict
        // boundary too (TransactionBaseImpl::SetSnapshot): the SingleDelete and the (value-less) Put are kept.
        Assert.AreEqual(2, CountVersions(db.Database, "k"u8));
        txn.Dispose();
        db.Database.CompactRange(s_force, null, null);
        Assert.AreEqual(0, CountVersions(db.Database, "k"u8));
    }

    private static int CountVersions(RocksDatabase db, ReadOnlySpan<byte> key)
    {
        int entries = 0;
        using KeyVersionCollection versions = db.GetAllKeyVersions(null, null, int.MaxValue);
        foreach (KeyVersion v in versions)
        {
            if (v.UserKey.Span.SequenceEqual(key)) entries++;
        }
        return entries;
    }

    /// <summary>A plain snapshot is no write-conflict boundary: the pair is still compacted away (upstream behaviour).</summary>
    [TestMethod]
    public void SingleDeletePairUnderPlainSnapshotIsDropped()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions());
        db.Put(B("a"), B("a"));
        using Snapshot snap = db.GetSnapshot();
        db.Put(B("k"), B("v"));
        db.SingleDelete(B("k"));
        PushOutOfMemtables(db);
        Assert.AreEqual(0, CountVersions(db, "k"u8));
    }

    /// <summary>Flush + CompactRange(Force) to the bottommost level, with no memtable history left.</summary>
    private static void PushOutOfMemtables(RocksDatabase db)
    {
        db.Flush();
        // Push the flushed memtable out of the (1-byte) memtable history: the conflict check must read the table files.
        db.Put(B("other"), B("x"));
        db.Flush();
        db.CompactRange(s_force, null, null);
    }

    /// <summary>Column families with almost no memtable history (max_write_buffer_size_to_maintain = 1).</summary>
    private static ColumnFamilyDescriptor[] NoHistoryCf() =>
        [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions { MaxWriteBufferSizeToMaintain = 1 })];

    // ---------------------------------------------------------------------------------------------------------
    // CompactionFilter::IgnoreSnapshots() = false (compaction_job.cc SetupAndValidateCompactionFilter, builder.cc)

    private sealed class SnapshotAwareFilter : CompactionFilter
    {
        public override string Name => "SnapshotAwareFilter";

        public override bool IgnoreSnapshots => false;

        public override CompactionFilterDecision Filter(int level, ReadOnlySpan<byte> key, CompactionFilterValueType valueType,
            ReadOnlySpan<byte> existingValue, IBufferWriter<byte> newValue, IBufferWriter<byte> skipUntil) => CompactionFilterDecision.Keep;
    }

    private sealed class FlushFilterFactory(bool forFlush) : CompactionFilterFactory
    {
        public override string Name => "FlushFilterFactory";

        public override CompactionFilter? CreateCompactionFilter(in CompactionFilterContext context) => new SnapshotAwareFilter();

        public override bool ShouldFilterTableFileCreation(bool isFlush) => !isFlush || forFlush;
    }

    [TestMethod]
    public void CompactionFilterNotIgnoringSnapshotsFailsCompaction()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { CompactionFilter = new SnapshotAwareFilter() });
        db.Put(B("a"), B("1"));
        db.Flush();
        db.Put(B("b"), B("1"));
        db.Flush();
        var ex = Assert.ThrowsExactly<RocksDbException>(() => db.CompactRange(s_force, null, null));
        Assert.AreEqual(StatusCode.NotSupported, ex.Code);
        StringAssert.Contains(ex.Message, "CompactionFilter::IgnoreSnapshots() = false is not supported anymore.");
    }

    [TestMethod]
    public void CompactionFilterFactoryNotIgnoringSnapshotsFailsCompaction()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { CompactionFilterFactory = new FlushFilterFactory(false) });
        db.Put(B("a"), B("1"));
        db.Flush(); // the factory does not filter flushes
        var ex = Assert.ThrowsExactly<RocksDbException>(() => db.CompactRange(s_force, null, null));
        Assert.AreEqual(StatusCode.NotSupported, ex.Code);
    }

    [TestMethod]
    public void FlushFilterNotIgnoringSnapshotsFailsFlush()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { CompactionFilterFactory = new FlushFilterFactory(true) });
        db.Put(B("a"), B("1"));
        var ex = Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
        Assert.AreEqual(StatusCode.NotSupported, ex.Code);
    }
}
