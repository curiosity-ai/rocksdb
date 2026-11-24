using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Tests.Db;
using RocksDb.Managed.Utilities.Backup;
using RocksDb.Managed.Utilities.Transactions;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #13, third pass: synchronous calls must not wait for work that needs a .NET thread-pool thread (the caller's
/// thread may be the pool's last one). Each test runs the synchronous call on dedicated threads while every pool worker
/// is blocked, with a deadline.
/// </summary>
[TestClass]
[DoNotParallelize]
public sealed class Audit13ThreadPoolTests
{
    private static readonly WriteOptions s_wo = new();

    /// <summary>Caps the .NET thread pool at the processor count and blocks every worker until disposed.</summary>
    private sealed class DeadThreadPool : IDisposable
    {
        private readonly ManualResetEventSlim _gate = new(false);
        private readonly int _maxWorkers, _maxIo, _minWorkers, _minIo;

        public DeadThreadPool()
        {
            ThreadPool.GetMaxThreads(out _maxWorkers, out _maxIo);
            ThreadPool.GetMinThreads(out _minWorkers, out _minIo);
            int cap = Environment.ProcessorCount;
            Assert.IsTrue(ThreadPool.SetMinThreads(Math.Min(_minWorkers, cap), _minIo));
            Assert.IsTrue(ThreadPool.SetMaxThreads(cap, _maxIo), "SetMaxThreads");
            for (int i = 0; i < cap; i++) ThreadPool.UnsafeQueueUserWorkItem(static g => ((ManualResetEventSlim)g!).Wait(), _gate);
            var probe = new ManualResetEventSlim(false); // not disposed: the probe runs once the pool is released
            ThreadPool.UnsafeQueueUserWorkItem(static p => ((ManualResetEventSlim)p!).Set(), probe);
            Assert.IsFalse(probe.Wait(500), "the thread pool still runs work items");
        }

        public void Dispose()
        {
            _gate.Set();
            ThreadPool.SetMaxThreads(_maxWorkers, _maxIo);
            ThreadPool.SetMinThreads(_minWorkers, _minIo);
        }
    }

    /// <summary>Runs <paramref name="body"/> on a dedicated thread; false if it did not finish within the deadline.</summary>
    private static bool RunOnThread(Action body, int deadlineMs, out Exception? error)
    {
        Exception? caught = null;
        var t = new Thread(() =>
        {
            try { body(); }
            catch (Exception e) { caught = e; }
        }) { IsBackground = true };
        t.Start();
        bool done = t.Join(deadlineMs);
        error = caught;
        return done;
    }

    [TestMethod]
    public void DisposeDuringAutoRecoveryDoesNotNeedThePool()
    {
        // ErrorHandler: upstream's recovery runs on recovery_thread_, which the close joins. As a thread-pool task the
        // recovery could not observe the shutdown (nor continue after its retry wait) without a pool thread, and a
        // synchronous Dispose blocked until it had.
        var fs = new FaultInjectionFileSystem(NewFs());
        var options = new DbOptions { CreateIfMissing = true, FileSystem = fs, BgErrorResumeRetryInterval = 3_600_000_000UL };
        RocksDatabase db = RocksDatabase.Open("/audit13pool-recovery", options);
        using var waiting = new ManualResetEventSlim();
        TestHooks.Set("RecoverFromRetryableBGIOError:BeforeWait", waiting.Set);
        try
        {
            db.Put(B("a"), B("1"));
            fs.FailMatching(".sst", FaultOps.Append, 1000, InjectedFaultKind.Retryable);
            Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
            Assert.IsTrue(waiting.Wait(30_000), "the recovery did not reach its retry wait");
            bool done;
            Exception? error;
            using (new DeadThreadPool()) done = RunOnThread(db.Dispose, 30_000, out error);
            Assert.IsTrue(done, "Dispose waited for thread-pool work");
            Assert.IsNull(error, error?.ToString());
        }
        finally
        {
            TestHooks.ClearAll();
        }
    }

    [TestMethod]
    public void SynchronousParallelBackupDoesNotNeedThePool()
    {
        // backup_engine.cc copies with its own threads; the port's workers were thread-pool tasks the caller blocked on.
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open("/audit13pool-backup", NewOptions(fs));
        for (int f = 0; f < 6; f++)
        {
            for (int i = 0; i < 50; i++) db.Put(B("k" + f + "-" + i), B("v" + i));
            db.Flush();
        }
        using BackupEngine be = BackupEngine.Open(new BackupEngineOptions("/audit13pool-backup-dir") { MaxBackgroundOperations = 4 }, fs);
        bool done;
        Exception? error;
        using (new DeadThreadPool())
        {
            done = RunOnThread(() =>
            {
                be.CreateNewBackup(db, flushBeforeBackup: false);
                be.VerifyBackup(1, verifyWithChecksum: true);
                be.RestoreDbFromLatestBackup("/audit13pool-restored", "/audit13pool-restored", new RestoreOptions());
            }, 60_000, out error);
        }
        Assert.IsTrue(done, "the synchronous backup waited for thread-pool work");
        Assert.IsNull(error, error?.ToString());
        using RocksDatabase restored = RocksDatabase.Open("/audit13pool-restored", new DbOptions { FileSystem = fs });
        Assert.AreEqual("v7", GetString(restored, "k3-7"));
    }

    [TestMethod]
    public void OptimisticParallelValidationMixedCommitsDoNotNeedThePool()
    {
        // CommitWithParallelValidate holds bucket locks across the write. An asynchronous committer that got its bucket
        // (or finished its write) through a thread-pool continuation kept synchronous committers, blocked on the same
        // bucket, waiting for a pool thread.
        MemoryFileSystem fs = NewFs();
        using OptimisticTransactionDatabase odb = OptimisticTransactionDatabase.Open("/audit13pool-occ", NewOptions(fs),
            new OptimisticTransactionDbOptions { ValidatePolicy = OccValidationPolicy.ValidateParallel },
            [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions())]);
        odb.Database.Put(B("hot"), B("0"));
        using (Transaction first = odb.BeginTransaction(s_wo)) // the DB has a synchronous committer from here on
        {
            first.Put(B("warm"), B("1"));
            first.Commit();
        }
        int committed = 0;
        var errors = new System.Collections.Concurrent.ConcurrentQueue<Exception>();
        bool done = true;
        using (new DeadThreadPool())
        {
            var threads = new Thread[4];
            for (int t = 0; t < threads.Length; t++)
            {
                bool useAsync = t == 0;
                threads[t] = new Thread(() =>
                {
                    try
                    {
                        for (int i = 0; i < 200; i++)
                        {
                            using Transaction txn = odb.BeginTransaction(s_wo);
                            txn.Put(B("hot"), B(i.ToString()));
                            try
                            {
                                if (useAsync) txn.CommitAsync().AsTask().GetAwaiter().GetResult();
                                else txn.Commit();
                                Interlocked.Increment(ref committed);
                            }
                            catch (RocksDbException e) when (e.Code is StatusCode.Busy or StatusCode.TryAgain)
                            {
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        errors.Enqueue(e);
                    }
                }) { IsBackground = true };
                threads[t].Start();
            }
            foreach (Thread t in threads) done &= t.Join(60_000);
        }
        Assert.IsTrue(done, "a commit waited for thread-pool work");
        Assert.IsTrue(errors.IsEmpty, errors.TryPeek(out Exception? first1) ? first1.ToString() : "");
        Assert.IsTrue(committed > 0);
    }
}
