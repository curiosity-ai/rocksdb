using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Utilities.Transactions;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4 follow-up, slice "FollowUpLocks": the range lock manager (range_tree_lock_manager.cc + lib/locktree), a
/// write-group hazard with malformed raw batches, and write-prepared data across flush / compaction.
/// </summary>
[TestClass]
public sealed class Audit4FollowUpLocksTests
{
    private static readonly WriteOptions s_wo = new();

    // ---- range lock manager: randomized model test ------------------------------------------------------------------

    /// <summary>A lock of the brute-force model: a closed endpoint range, its mode and its owners.</summary>
    private sealed class ModelLock
    {
        public int[] Left = null!;
        public int[] Right = null!;
        public bool Shared;
        public HashSet<int> Owners = new();
    }

    /// <summary>
    /// An endpoint as a sequence of symbols: the key bytes, then -1 (plain endpoint: before every longer key) or 256
    /// (infinite suffix: after every key with this prefix). Plain lexicographic order of these sequences is the order
    /// RangeTreeLockManager::CompareDbtEndpoints defines for a bytewise comparator.
    /// </summary>
    private static int[] Sym(string key, bool inf)
    {
        int[] s = new int[key.Length + 1];
        for (int i = 0; i < key.Length; i++) s[i] = key[i];
        s[^1] = inf ? 256 : -1;
        return s;
    }

    private static int CmpSym(int[] a, int[] b)
    {
        for (int i = 0; i < Math.Min(a.Length, b.Length); i++)
        {
            if (a[i] != b[i]) return a[i].CompareTo(b[i]);
        }
        return a.Length.CompareTo(b.Length);
    }

    /// <summary>
    /// The conflict rule of locktree::acquire_lock_consolidated (locktree.cc:419), on a plain list: a shared request with
    /// exactly the endpoints of the single overlapping shared lock joins it; otherwise any overlapping lock with another
    /// owner refuses the request; otherwise the transaction's own overlapping locks are replaced by their union.
    /// </summary>
    private static bool ModelAcquire(List<ModelLock> locks, int txn, int[] left, int[] right, bool write)
    {
        var overlaps = new List<ModelLock>();
        foreach (ModelLock l in locks)
        {
            if (CmpSym(l.Right, left) >= 0 && CmpSym(l.Left, right) <= 0) overlaps.Add(l);
        }
        if (!write && overlaps.Count == 1 && overlaps[0].Shared && CmpSym(overlaps[0].Left, left) == 0 && CmpSym(overlaps[0].Right, right) == 0)
        {
            overlaps[0].Owners.Add(txn);
            return true;
        }
        foreach (ModelLock l in overlaps)
        {
            if (l.Owners.Count != 1 || !l.Owners.Contains(txn)) return false;
        }
        bool shared = !write;
        foreach (ModelLock l in overlaps)
        {
            if (CmpSym(l.Left, left) < 0) left = l.Left;
            if (CmpSym(l.Right, right) > 0) right = l.Right;
            shared &= l.Shared;
            locks.Remove(l);
        }
        locks.Add(new ModelLock { Left = left, Right = right, Shared = shared, Owners = { txn } });
        return true;
    }

    private static void ModelReleaseAll(List<ModelLock> locks, int txn)
    {
        for (int i = locks.Count - 1; i >= 0; i--)
        {
            if (locks[i].Owners.Remove(txn) && locks[i].Owners.Count == 0) locks.RemoveAt(i);
        }
    }

    private static string ShowSym(int[] s)
    {
        var sb = new System.Text.StringBuilder();
        for (int i = 0; i < s.Length - 1; i++) sb.Append((char)s[i]);
        return sb.Append(s[^1] == 256 ? "+" : "").ToString();
    }

    /// <summary>
    /// Random exclusive range locks (GetRangeLock, with infinite-suffix endpoints on keys that are prefixes of each
    /// other), exclusive and shared point locks (GetForUpdate) and commits / rollbacks from four transactions with lock
    /// timeout 0, against a brute-force model of locktree::acquire_lock_consolidated / release_locks. Every request
    /// must be granted or refused (TimedOut / LockTimeout: lock_request::wait with a zero wait) exactly as the model
    /// says; the model's lock list must match GetRangeLockStatusData once the tree left the single-transaction mode.
    /// </summary>
    [TestMethod]
    public void RangeLockManager_RandomRequestsMatchBruteForceModel()
    {
        string[] keys = ["a", "ab", "abc", "abd", "b", "ba", "c", "ca", "cab", "d"];
        for (int seed = 1; seed <= 12; seed++)
        {
            var rnd = new Random(seed);
            MemoryFileSystem fs = NewFs();
            var mgr = new RangeLockManager();
            using TransactionDatabase db = TransactionDatabase.Open(DbPath, NewOptions(fs),
                new TransactionDbOptions { RangeLockManager = mgr, TransactionLockTimeout = TimeSpan.Zero },
                [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions())]);
            ColumnFamily cf = db.Database.DefaultColumnFamily;
            var locks = new List<ModelLock>();
            var txns = new Transaction[4];
            for (int i = 0; i < txns.Length; i++) txns[i] = db.BeginTransaction(s_wo, new TransactionOptions { LockTimeout = TimeSpan.Zero });
            try
            {
                for (int step = 0; step < 400; step++)
                {
                    int t = rnd.Next(txns.Length);
                    string what;
                    bool expected;
                    Action request;
                    int op = rnd.Next(20);
                    if (op == 0)
                    {
                        // Commit or roll back: every lock of the transaction is released (RangeLockList::ReleaseLocks).
                        if (rnd.Next(2) == 0) txns[t].Commit();
                        else txns[t].Rollback();
                        txns[t].Dispose();
                        txns[t] = db.BeginTransaction(s_wo, new TransactionOptions { LockTimeout = TimeSpan.Zero });
                        ModelReleaseAll(locks, t);
                        continue;
                    }
                    if (op < 9)
                    {
                        string k1 = keys[rnd.Next(keys.Length)], k2 = keys[rnd.Next(keys.Length)];
                        bool i1 = rnd.Next(3) == 0, i2 = rnd.Next(3) == 0;
                        if (CmpSym(Sym(k1, i1), Sym(k2, i2)) > 0)
                        {
                            (k1, k2) = (k2, k1);
                            (i1, i2) = (i2, i1);
                        }
                        what = $"range [{k1}{(i1 ? "+" : "")}, {k2}{(i2 ? "+" : "")}]";
                        expected = ModelAcquire(locks, t, Sym(k1, i1), Sym(k2, i2), write: true);
                        request = () => txns[t].GetRangeLock(cf, new RangeLockEndpoint(B(k1), i1), new RangeLockEndpoint(B(k2), i2));
                    }
                    else
                    {
                        string k = keys[rnd.Next(keys.Length)];
                        bool exclusive = rnd.Next(2) == 0;
                        what = $"point {k} {(exclusive ? "X" : "S")}";
                        expected = ModelAcquire(locks, t, Sym(k, false), Sym(k, false), exclusive);
                        request = () => txns[t].GetForUpdate(null, cf, B(k), exclusive);
                    }
                    string ctx = $"seed {seed} step {step} txn {t} {what}";
                    if (expected)
                    {
                        request();
                    }
                    else
                    {
                        var e = Assert.ThrowsExactly<RocksDbException>(request, ctx);
                        Assert.AreEqual(StatusCode.TimedOut, e.Code, ctx);
                        Assert.AreEqual(StatusSubCode.LockTimeout, e.SubCode, ctx);
                    }

                    // locktree::dump_locks reports the tree's consolidated locks once several transactions hold locks
                    // (in the single-transaction mode it reports the unmerged buffer: skipped).
                    var owners = new HashSet<int>();
                    foreach (ModelLock l in locks) owners.UnionWith(l.Owners);
                    if (owners.Count >= 2)
                    {
                        var expectedDump = new List<string>();
                        locks.Sort((x, y) => CmpSym(x.Left, y.Left));
                        foreach (ModelLock l in locks)
                        {
                            var ids = new List<ulong>();
                            foreach (int o in l.Owners) ids.Add(txns[o].Id);
                            ids.Sort();
                            expectedDump.Add($"[{ShowSym(l.Left)},{ShowSym(l.Right)}] {(l.Shared ? "S" : "X")} {string.Join(",", ids)}");
                        }
                        var actualDump = new List<string>();
                        foreach (RangeLockInfo info in mgr.GetRangeLockStatusData())
                        {
                            var ids = new List<ulong>(info.TransactionIds);
                            ids.Sort();
                            actualDump.Add($"[{S(info.Start.Key.ToArray())}{(info.Start.InfSuffix ? "+" : "")},{S(info.End.Key.ToArray())}{(info.End.InfSuffix ? "+" : "")}] " +
                                           $"{(info.Exclusive ? "X" : "S")} {string.Join(",", ids)}");
                        }
                        Assert.AreEqual(string.Join(" | ", expectedDump), string.Join(" | ", actualDump), ctx);
                    }
                }
                foreach (Transaction t in txns) t.Rollback();
                Assert.AreEqual(0, mgr.GetRangeLockStatusData().Count, $"seed {seed}: locks left after every transaction ended");
                Assert.AreEqual(0UL, mgr.GetStatus().CurrentLockMemory, $"seed {seed}: lock memory left after every transaction ended");
            }
            finally
            {
                foreach (Transaction t in txns) t.Dispose();
            }
        }
    }

    // ---- F1: a malformed raw batch in a parallel write group -------------------------------------------------------

    /// <summary>
    /// WriteBatch::ComputeContentFlags (db/write_batch.cc:295) ignores the status of the classifying Iterate
    /// ("Should we handle status here?" ... PermitUncheckedError), so DBImpl::WriteImpl's parallel check
    /// (db_impl_write.cc:1309, writer->batch->HasMerge()) never fails: a malformed batch reaches the memtable insert, whose
    /// Corruption is a fatal background error (WriteThread::CompleteParallelMemTableWriter keeps the status in the group,
    /// db_impl_write.cc:1164 HandleMemTableInsertFailure). The port threw the Corruption from HasMerge after the group's
    /// WAL record was written: the group failed without a background error, so the DB kept accepting writes on top of a
    /// WAL holding a malformed record. With a single writer (serial insert) the port already matched upstream.
    /// </summary>
    [TestMethod]
    [Timeout(60_000, CooperativeCancellation = true)]
    [DataRow(false)]
    [DataRow(true)]
    public async Task MalformedRawBatchStopsWritesAlsoInAParallelGroup(bool groupOfTwo)
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.AllowConcurrentMemtableWrite = true;
        using RocksDatabase db = RocksDatabase.Open(DbPath, o);
        db.Put(B("a"), B("1"));
        // Header (sequence 0, count 1), kTypeValue, key length 5 but only two key bytes.
        byte[] rep = new byte[12 + 4];
        rep[8] = 1;
        rep[12] = 1;
        rep[13] = 5;
        rep[14] = (byte)'k';
        rep[15] = (byte)'k';
        using var bad = new WriteBatch();
        bad.SetContents(rep);
        using var good = new WriteBatch();
        good.Put(B("b"), B("2"));
        var wo = new WriteOptions { IgnoreMissingColumnFamilies = true }; // no column family pre-validation of the batch

        RocksDbException? badError = null;
        if (groupOfTwo)
        {
            // Hold write leadership, queue both writers (WriteAsync links the writer before it first suspends), then
            // release: the first queued writer leads a group of two.
            WriteThread.Writer hold = await db.Impl.EnterUnbatchedAsync(synchronous: true, CancellationToken.None);
            Task goodWrite, badWrite;
            try
            {
                goodWrite = db.WriteAsync(wo, good).AsTask();
                badWrite = db.WriteAsync(wo, bad).AsTask();
                Assert.IsFalse(goodWrite.IsCompleted || badWrite.IsCompleted);
            }
            finally
            {
                db.Impl.ExitUnbatched(hold, synchronous: true);
            }
            try { await badWrite; }
            catch (RocksDbException e) { badError = e; }
            try { await goodWrite; }
            catch (RocksDbException) { } // the group's status; upstream gives it to the writer that finished last
        }
        else
        {
            try { db.Write(wo, bad); }
            catch (RocksDbException e) { badError = e; }
        }
        Assert.IsNotNull(badError, "the malformed batch was accepted");
        Assert.AreEqual(StatusCode.Corruption, badError.Code, badError.Message);

        // HandleMemTableInsertFailure: the memtables have diverged from the WAL, further writes are refused.
        var stopped = Assert.ThrowsExactly<RocksDbException>(() => db.Put(B("c"), B("3")));
        Assert.AreEqual(StatusCode.Corruption, stopped.Code, stopped.Message);
        Assert.AreEqual("1", GetString(db, "a"));
        // The group's sequence numbers were not published (no partial batch write).
        Assert.IsNull(GetString(db, "c"));
    }

    // ---- write-prepared data across flush and full compaction -------------------------------------------------------

    private static string Dump(TransactionDatabase db, Snapshot? snapshot)
    {
        var sb = new System.Text.StringBuilder();
        using Iterator it = db.Database.NewIterator(new ReadOptions { Snapshot = snapshot });
        for (it.SeekToFirst(); it.Valid; it.Next()) sb.Append(S(it.Key.ToArray())).Append('=').Append(S(it.Value.ToArray())).Append(' ');
        // Point lookups must agree with the scan.
        var sb2 = new System.Text.StringBuilder();
        foreach (string k in new[] { "a", "b", "c", "d", "e" })
        {
            byte[]? v = db.Database.Get(new ReadOptions { Snapshot = snapshot }, B(k));
            if (v is not null) sb2.Append(k).Append('=').Append(S(v)).Append(' ');
        }
        Assert.AreEqual(sb.ToString(), sb2.ToString(), "Get and the iterator disagree");
        return sb.ToString().TrimEnd();
    }

    /// <summary>
    /// A prepared (uncommitted) WritePrepared transaction that overwrites and deletes committed keys, across a flush and
    /// full compactions. CompactionIterator::NextFromInput asks the snapshot checker whether each version is committed
    /// (compaction_iterator.cc:837 / :860 current_key_committed_ = KeyCommitted(seq), i.e.
    /// CheckInSnapshot(seq, job_snapshot) == kInSnapshot) and outputs an uncommitted version as it is (:870
    /// kCurrentKeyUncommitted) without letting it shadow, or be merged with, the committed versions below it;
    /// findEarliestVisibleSnapshot (:2043) uses CheckInSnapshot per snapshot (kInSnapshot: visible; kNotInSnapshot: try
    /// the next one; kSnapshotReleased: remember and skip), so the version an older snapshot reads is kept as well. Readers
    /// and the older snapshot must therefore see the committed values until the commit, and the transaction's values
    /// (commit) or the committed ones again (rollback) afterwards, also after further full compactions.
    /// </summary>
    [TestMethod]
    [DataRow(true)]
    [DataRow(false)]
    public void WritePreparedUncommittedDataSurvivesFlushAndCompaction(bool commit)
    {
        MemoryFileSystem fs = NewFs();
        var tdo = new TransactionDbOptions { WritePolicy = TransactionWritePolicy.WritePrepared };
        using TransactionDatabase db = TransactionDatabase.Open(DbPath, NewOptions(fs), tdo,
            [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions { DisableAutoCompactions = true })]);
        var force = new CompactRangeOptions { BottommostLevelCompaction = BottommostLevelCompaction.Force };
        db.Put(B("a"), B("a1"));
        db.Put(B("b"), B("b1"));
        db.Put(B("c"), B("c1"));
        db.Put(B("d"), B("d1"));
        using Snapshot old = db.Database.GetSnapshot();
        db.Put(B("a"), B("a2"));
        db.Delete(B("d"));
        const string oldView = "a=a1 b=b1 c=c1 d=d1";
        const string committedView = "a=a2 b=b1 c=c1";

        using Transaction txn = db.BeginTransaction(s_wo);
        txn.SetName("prepared");
        txn.Put(B("a"), B("aT"));
        txn.Delete(B("b"));
        txn.Put(B("d"), B("dT"));
        txn.Put(B("e"), B("eT"));
        txn.Prepare();
        Assert.AreEqual(committedView, Dump(db, null), "after prepare");

        db.Database.Flush();
        Assert.AreEqual(committedView, Dump(db, null), "after flush");
        Assert.AreEqual(oldView, Dump(db, old), "old snapshot after flush");

        db.Database.CompactRange(force, null, null);
        Assert.AreEqual(committedView, Dump(db, null), "after full compaction");
        Assert.AreEqual(oldView, Dump(db, old), "old snapshot after full compaction");
        using Snapshot beforeEnd = db.Database.GetSnapshot();

        string finalView;
        if (commit)
        {
            txn.Commit();
            finalView = "a=aT c=c1 d=dT e=eT";
        }
        else
        {
            txn.Rollback();
            finalView = committedView;
        }
        for (int round = 0; round < 2; round++)
        {
            Assert.AreEqual(finalView, Dump(db, null), $"after {(commit ? "commit" : "rollback")}, round {round}");
            Assert.AreEqual(committedView, Dump(db, beforeEnd), $"snapshot taken while prepared, round {round}");
            Assert.AreEqual(oldView, Dump(db, old), $"old snapshot, round {round}");
            db.Database.Flush();
            db.Database.CompactRange(force, null, null);
        }
    }
}
