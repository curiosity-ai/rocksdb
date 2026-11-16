using System.Globalization;
using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #8 (the asynchronous implementation), area "AsyncBackgroundIo": the file layer (IFileSystem async members,
/// LocalFileSystem's RWF_NOWAIT fast paths, FileSyncWorkers, MemoryFileSystem / MirrorFileSystem, GenericRateLimiter),
/// background work and the async control APIs (FlushAsync, CompactRangeAsync, WaitForCompactAsync, CompactFilesAsync,
/// OpenAsync, DisposeAsync, ResumeAsync, LogAndApplyAsync). Reference: the synchronous managed path of the same
/// operation and the C++ semantics cited per test.
/// </summary>
[TestClass]
public sealed class Audit8AsyncBackgroundIoTests
{
    private static readonly TimeSpan Deadline = TimeSpan.FromSeconds(20);

    [TestCleanup]
    public void Cleanup() => TestHooks.ClearAll();

    // ---------------------------------------------------------------------------------------------------------
    // Test file system: every async member really completes asynchronously (Task.Yield) and honours its token, and
    // the calls to the async and sync members are counted.

    private sealed class YieldingFileSystem(MemoryFileSystem inner) : IFileSystem
    {
        public MemoryFileSystem Inner { get; } = inner;
        private long _asyncCalls;
        public long AsyncCalls => Interlocked.Read(ref _asyncCalls);
        public bool Yield { get; set; } = true;

        /// <summary>Called (on the calling thread, before the yield) for each async member with its operation and path.</summary>
        public Action<string, string, CancellationToken>? OnAsync { get; set; }

        /// <summary>Stack traces of the async calls (when not null).</summary>
        public System.Collections.Concurrent.ConcurrentQueue<string>? AsyncCallers { get; set; }

        internal async ValueTask BeforeAsync(string op, string path, CancellationToken ct)
        {
            Interlocked.Increment(ref _asyncCalls);
            AsyncCallers?.Enqueue(op + " " + path + "\n" + Environment.StackTrace);
            OnAsync?.Invoke(op, path, ct);
            if (Yield) await Task.Yield();
            ct.ThrowIfCancellationRequested();
        }

        public IWritableFile CreateWritable(string path, bool truncate = true) => new W(this, Inner.CreateWritable(path, truncate), path);
        public IWritableFile CreateWritable(string path, bool truncate, WritableFileOptions options) => new W(this, Inner.CreateWritable(path, truncate, options), path);
        public IWritableFile ReopenWritable(string path, WritableFileOptions options) => new W(this, Inner.ReopenWritable(path, options), path);
        public IRandomAccessFile OpenRandomAccess(string path) => new R(this, Inner.OpenRandomAccess(path), path);
        public IRandomAccessFile OpenRandomAccess(string path, RandomAccessFileOptions options) => new R(this, Inner.OpenRandomAccess(path, options), path);
        public ISequentialFile OpenSequential(string path) => new S(this, Inner.OpenSequential(path), path);
        public ISequentialFile OpenSequential(string path, SequentialFileOptions options) => new S(this, Inner.OpenSequential(path, options), path);
        public bool FileExists(string path) => Inner.FileExists(path);
        public bool DirectoryExists(string path) => Inner.DirectoryExists(path);
        public long GetFileSize(string path) => Inner.GetFileSize(path);
        public void DeleteFile(string path) => Inner.DeleteFile(path);
        public void RenameFile(string source, string target) => Inner.RenameFile(source, target);
        public void CreateDirectoryIfMissing(string path) => Inner.CreateDirectoryIfMissing(path);
        public void DeleteDirectory(string path) => Inner.DeleteDirectory(path);
        public IEnumerable<string> GetChildren(string directory) => Inner.GetChildren(directory);
        public IDisposable LockFile(string path) => Inner.LockFile(path);
        public void SyncDirectory(string path) => Inner.SyncDirectory(path);
        public void LinkFile(string source, string target) => Inner.LinkFile(source, target);
        public DateTimeOffset GetFileModificationTime(string path) => Inner.GetFileModificationTime(path);
        public long GetFreeSpace(string path) => Inner.GetFreeSpace(path);

        private sealed class W(YieldingFileSystem fs, IWritableFile inner, string path) : IWritableFile
        {
            public long Size => inner.Size;
            public void Append(ReadOnlySpan<byte> data) => inner.Append(data);
            public void Flush() => inner.Flush();
            public void Sync() => inner.Sync();
            public void Dispose() => inner.Dispose();

            public async ValueTask AppendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
            {
                await fs.BeforeAsync("AppendAsync", path, cancellationToken);
                inner.Append(data.Span);
            }

            public async ValueTask FlushAsync(CancellationToken cancellationToken = default)
            {
                await fs.BeforeAsync("FlushAsync", path, cancellationToken);
                inner.Flush();
            }

            public async ValueTask SyncAsync(CancellationToken cancellationToken = default)
            {
                await fs.BeforeAsync("SyncAsync", path, cancellationToken);
                inner.Sync();
            }

            public async ValueTask DisposeAsync()
            {
                await fs.BeforeAsync("DisposeAsync", path, CancellationToken.None);
                inner.Dispose();
            }
        }

        private sealed class R(YieldingFileSystem fs, IRandomAccessFile inner, string path) : IRandomAccessFile
        {
            public long Length => inner.Length;
            public int Read(long offset, Span<byte> destination) => inner.Read(offset, destination);

            public async ValueTask<int> ReadAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken = default)
            {
                await fs.BeforeAsync("ReadAsync", path, cancellationToken);
                return inner.Read(offset, destination.Span);
            }

            public void Dispose() => inner.Dispose();
        }

        private sealed class S(YieldingFileSystem fs, ISequentialFile inner, string path) : ISequentialFile
        {
            public int Read(Span<byte> destination) => inner.Read(destination);
            public void Skip(long bytes) => inner.Skip(bytes);

            public async ValueTask<int> ReadAsync(Memory<byte> destination, CancellationToken cancellationToken = default)
            {
                await fs.BeforeAsync("SequentialReadAsync", path, cancellationToken);
                return inner.Read(destination.Span);
            }

            public void Dispose() => inner.Dispose();
        }
    }

    private static T WithDeadline<T>(Task<T> t, string what)
    {
        if (!t.Wait(Deadline)) Assert.Fail(what + " made no progress within " + Deadline);
        return t.Result;
    }

    private static void WithDeadline(Task t, string what)
    {
        if (!t.Wait(Deadline)) Assert.Fail(what + " made no progress within " + Deadline);
        t.GetAwaiter().GetResult();
    }

    /// <summary>Runs <paramref name="body"/> on a dedicated (non thread-pool) thread and waits for it with the deadline.</summary>
    private static void OnDedicatedThread(Action body, string what)
    {
        Exception? error = null;
        var t = new Thread(() =>
        {
            try { body(); }
            catch (Exception e) { error = e; }
        }) { IsBackground = true, Name = "audit8-sync" };
        t.Start();
        if (!t.Join(Deadline)) Assert.Fail(what + " made no progress within " + Deadline);
        if (error is not null) System.Runtime.ExceptionServices.ExceptionDispatchInfo.Throw(error);
    }

    private static List<string> DbFiles(IFileSystem fs)
    {
        var names = new List<string>();
        foreach (string c in fs.GetChildren(DbPath))
        {
            if (c == "LOG" || c.StartsWith("LOG.old", StringComparison.Ordinal)) continue;
            names.Add(c);
        }
        names.Sort(StringComparer.Ordinal);
        return names;
    }

    private static string Dump(RocksDatabase db)
    {
        var sb = new StringBuilder();
        using Iterator it = db.NewIterator();
        for (it.SeekToFirst(); it.Valid; it.Next())
        {
            sb.Append(Encoding.UTF8.GetString(it.Key)).Append('=').Append(Encoding.UTF8.GetString(it.Value)).Append(';');
        }
        return sb.ToString();
    }

    // ---------------------------------------------------------------------------------------------------------
    // 1. The sync APIs use the sync file members only (architecture.md "Async I/O fast paths").

    /// <summary>
    /// Pins: Open, Put (sync WAL), Flush, CompactRange, CreateColumnFamily / DropColumnFamily, WaitForCompact and Dispose
    /// call no async member of the file system (with synchronous: true every wait completes inline), also from the
    /// background flushes and compactions they trigger.
    /// </summary>
    [TestMethod]
    public void SyncApisNeverCallAsyncFileMembers()
    {
        var fs = new YieldingFileSystem(NewFs()) { AsyncCallers = new() };
        OnDedicatedThread(() =>
        {
            DbOptions o = NewOptions(fs);
            using (RocksDatabase db = RocksDatabase.Open(DbPath, o, new ColumnFamilyOptions { WriteBufferSize = 64 << 10, Level0FileNumCompactionTrigger = 2 }))
            {
                for (int round = 0; round < 4; round++)
                {
                    for (int i = 0; i < 300; i++) db.Put(new WriteOptions { Sync = i % 50 == 0 }, Key(i), Value(i, round));
                    db.Flush();
                }
                db.CompactRange();
                ColumnFamily cf = db.CreateColumnFamily("cf1", new ColumnFamilyOptions());
                cf.Put(B("a"), B("b"));
                cf.Flush();
                db.DropColumnFamily(cf);
                db.WaitForCompact();
            }
            using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
            {
                Assert.AreEqual("value-1-3" + new string('x', 1), GetString(db, "key000001"));
            }
        }, "sync program");
        Assert.AreEqual(0, fs.AsyncCalls, string.Join("\n----\n", fs.AsyncCallers!.Take(3)));
    }

    // ---------------------------------------------------------------------------------------------------------
    // 2. Sync/async parity of the control APIs on a file system whose async members really go async.

    private static ColumnFamilyOptions ParityCf() => new() { WriteBufferSize = 64 << 10, DisableAutoCompactions = true };

    private static void SyncProgram(IFileSystem fs, int seed)
    {
        var rnd = new Random(seed);
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), ParityCf()))
        {
            for (int round = 0; round < 5; round++)
            {
                int n = 100 + rnd.Next(200);
                for (int i = 0; i < n; i++) db.Put(new WriteOptions { Sync = i % 37 == 0 }, Key(rnd.Next(500)), Value(i, round));
                if (rnd.Next(2) == 0) db.Delete(Key(rnd.Next(500)));
                db.Flush();
                if (round == 2) db.CompactRange(Key(100), Key(300));
            }
            ColumnFamily cf = db.CreateColumnFamily("cf1", ParityCf());
            cf.Put(B("a"), B("b"));
            cf.Flush();
            db.DropColumnFamily(cf);
            db.CompactRange();
            db.WaitForCompact();
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), ParityCf()))
        {
            db.Put(B("after-reopen"), B("x"));
        }
    }

    private static async Task AsyncProgram(IFileSystem fs, int seed)
    {
        var rnd = new Random(seed);
        await using (RocksDatabase db = await RocksDatabase.OpenAsync(DbPath, NewOptions(fs), ParityCf()))
        {
            for (int round = 0; round < 5; round++)
            {
                int n = 100 + rnd.Next(200);
                for (int i = 0; i < n; i++) await db.PutAsync(new WriteOptions { Sync = i % 37 == 0 }, Key(rnd.Next(500)), Value(i, round));
                if (rnd.Next(2) == 0) await db.DeleteAsync(Key(rnd.Next(500)));
                await db.FlushAsync();
                if (round == 2) await db.CompactRangeAsync(Key(100), Key(300));
            }
            ColumnFamily cf = await db.CreateColumnFamilyAsync("cf1", ParityCf());
            await cf.PutAsync(B("a"), B("b"));
            await cf.FlushAsync();
            await db.DropColumnFamilyAsync(cf);
            await db.CompactRangeAsync();
            await db.WaitForCompactAsync();
        }
        await using (RocksDatabase db = await RocksDatabase.OpenAsync(DbPath, NewOptions(fs), ParityCf()))
        {
            await db.PutAsync(B("after-reopen"), B("x"));
        }
    }

    /// <summary>
    /// Pins sync/async parity of Open / Put / Flush / CompactRange / CreateColumnFamily / DropColumnFamily /
    /// WaitForCompact / Dispose on a file system whose async members complete asynchronously: the same seeded program
    /// leaves the same files (same numbers) and the same data.
    /// </summary>
    [TestMethod]
    public void SyncAndAsyncControlApisLeaveTheSameDb()
    {
        int seed = Environment.TickCount;
        Console.WriteLine("seed=" + seed.ToString(CultureInfo.InvariantCulture));
        var syncFs = new YieldingFileSystem(NewFs());
        OnDedicatedThread(() => SyncProgram(syncFs, seed), "sync program");
        var asyncFs = new YieldingFileSystem(NewFs());
        WithDeadline(Task.Run(() => AsyncProgram(asyncFs, seed)), "async program");
        Assert.AreEqual(0, syncFs.AsyncCalls);
        Assert.IsTrue(asyncFs.AsyncCalls > 0);
        CollectionAssert.AreEqual(DbFiles(syncFs), DbFiles(asyncFs), string.Join(",", DbFiles(syncFs)) + " vs " + string.Join(",", DbFiles(asyncFs)));
        string a, b;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(syncFs), ParityCf())) a = Dump(db);
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(asyncFs), ParityCf())) b = Dump(db);
        Assert.AreEqual(a, b);
    }

    // ---------------------------------------------------------------------------------------------------------
    // 3. Thread pool starvation: the sync API must not wait for .NET thread-pool work.

    /// <summary>
    /// Caps the .NET thread pool at the processor count and blocks every worker until the returned scope is disposed:
    /// no thread-pool work item runs meanwhile (the pool cannot inject threads above the cap).
    /// </summary>
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
            for (int i = 0; i < cap; i++)
            {
                ThreadPool.UnsafeQueueUserWorkItem(static g => ((ManualResetEventSlim)g!).Wait(), _gate);
            }
            // Saturated once a probe work item no longer runs (other work of the process may hold some workers).
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

    /// <summary>
    /// Pins architecture.md "the sync API never waits on thread-pool work": with every .NET thread-pool worker blocked,
    /// Open, Put (synced), Flush, CompactRange (+ automatic compactions), CreateColumnFamily, WaitForCompact, Get,
    /// iteration and Dispose complete on a dedicated thread (flushes and compactions use the rocksdb HIGH / LOW threads).
    /// </summary>
    [TestMethod]
    [DoNotParallelize]
    public void SyncApisProgressWithThreadPoolExhausted()
    {
        var fs = new YieldingFileSystem(NewFs());
        DbOptions o = NewOptions(fs);
        using (new DeadThreadPool())
        {
            OnDedicatedThread(() =>
            {
                using (RocksDatabase db = RocksDatabase.Open(DbPath, o, new ColumnFamilyOptions { WriteBufferSize = 64 << 10, Level0FileNumCompactionTrigger = 2 }))
                {
                    for (int round = 0; round < 4; round++)
                    {
                        for (int i = 0; i < 400; i++) db.Put(new WriteOptions { Sync = i % 100 == 0 }, Key(i), Value(i, round));
                        db.Flush();
                    }
                    db.CompactRange();
                    ColumnFamily cf = db.CreateColumnFamily("cf1", new ColumnFamilyOptions());
                    cf.Put(B("a"), B("b"));
                    cf.Flush();
                    db.WaitForCompact();
                    Assert.AreEqual("b", GetString(cf, "a"));
                    Assert.IsTrue(Dump(db).Length > 0);
                }
                using (RocksDatabase db = RocksDatabase.Open(DbPath, o, [new("default", new ColumnFamilyOptions()), new("cf1", new ColumnFamilyOptions())]))
                {
                    Assert.AreEqual("value-1-3x", GetString(db, "key000001"));
                }
            }, "sync program with the thread pool exhausted");
        }
        Assert.AreEqual(0, fs.AsyncCalls);
    }

    /// <summary>
    /// Pins that Dispose does not wait for .NET thread-pool work (DBImpl::CloseHelper joins only the DB's own threads): with
    /// every pool worker blocked, closing a DB with its default private SstFileManager and info LOG returns at once. Before
    /// the fix the DeleteScheduler waited up to 30 s for its worker task and the info LOG up to 5 s for its writer task,
    /// both of which need a pool thread to observe the close.
    /// </summary>
    [TestMethod]
    [DoNotParallelize]
    public void DisposeDoesNotWaitForThreadPoolWork()
    {
        var fs = new YieldingFileSystem(NewFs());
        RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        db.Put(B("k"), B("v"));
        TimeSpan elapsed = default;
        using (new DeadThreadPool())
        {
            OnDedicatedThread(() =>
            {
                var sw = System.Diagnostics.Stopwatch.StartNew();
                db.Dispose();
                elapsed = sw.Elapsed;
            }, "Dispose with the thread pool exhausted");
        }
        Assert.IsTrue(elapsed < TimeSpan.FromSeconds(3), "Dispose took " + elapsed);
        using RocksDatabase reopened = RocksDatabase.Open(DbPath, NewOptions(fs));
        Assert.AreEqual("v", GetString(reopened, "k"));
    }
}
