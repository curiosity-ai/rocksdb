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
        if (!((IAsyncResult)t).AsyncWaitHandle.WaitOne(Deadline)) Assert.Fail(what + " made no progress within " + Deadline);
        return t.GetAwaiter().GetResult();
    }

    private static void WithDeadline(Task t, string what)
    {
        if (!((IAsyncResult)t).AsyncWaitHandle.WaitOne(Deadline)) Assert.Fail(what + " made no progress within " + Deadline);
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

        public DeadThreadPool() : this(block: true)
        {
        }

        /// <param name="block">false: only cap the pool at the processor count (no thread injection above it).</param>
        public DeadThreadPool(bool block)
        {
            ThreadPool.GetMaxThreads(out _maxWorkers, out _maxIo);
            ThreadPool.GetMinThreads(out _minWorkers, out _minIo);
            int cap = Environment.ProcessorCount;
            Assert.IsTrue(ThreadPool.SetMinThreads(Math.Min(_minWorkers, cap), _minIo));
            Assert.IsTrue(ThreadPool.SetMaxThreads(cap, _maxIo), "SetMaxThreads");
            if (!block) return;
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

    /// <summary>
    /// A compaction (on its LOW thread, or a sync CompactRange on the caller's thread) whose input files have no
    /// readahead(2) (IRandomAccessFile.Prefetch false: macOS, in-memory and custom file systems) reads them through
    /// AsyncPrefetchBuffer, which blocks (ManualResetEventSlim) until IRandomAccessFile.ReadAsync of the next chunk
    /// completes. When that completion runs on the .NET thread pool (here a file system whose ReadAsync yields; on macOS
    /// LocalFileSystem's RandomAccess.ReadAsync runs the pread on a pool thread) and every pool worker is blocked, the sync
    /// CompactRange never finishes. Native's FilePrefetchBuffer async_io polls io_uring from the reading thread.
    /// </summary>
    [TestMethod]
    [DoNotParallelize]
    [Ignore("audit #8 open: compaction input prefetch (AsyncPrefetchBuffer) blocks its compaction thread on a ReadAsync continuation that needs a .NET thread-pool thread")]
    public void CompactionPrefetchProgressesWithThreadPoolExhausted()
    {
        var fs = new YieldingFileSystem(NewFs()); // no readahead(2), ReadAsync continues on the thread pool
        DbOptions o = NewOptions(fs);
        o.CompactionReadaheadSize = 64 << 10;
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, new ColumnFamilyOptions { DisableAutoCompactions = true });
        var rnd = new Random(1);
        for (int f = 0; f < 3; f++)
        {
            for (int i = 0; i < 2000; i++)
            {
                byte[] v = new byte[400];
                rnd.NextBytes(v);
                db.Put(Key(i * 3 + f), v);
            }
            db.Flush();
        }
        using (new DeadThreadPool())
        {
            OnDedicatedThread(db.CompactRange, "CompactRange with the thread pool exhausted");
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // 4. LocalFileSystem async fast paths: same bytes and IOStats as the sync members.

    private static readonly System.Reflection.FieldInfo s_noWaitWriteUnavailable =
        typeof(LocalFileSystem).Assembly.GetType("RocksDb.Managed.Env.UnixNative")!
            .GetField("s_noWaitWriteUnavailable", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;

    private static ulong BytesWrittenBy(Action<IWritableFile> write, string path)
    {
        PerfLevel before = PerfContext.Level;
        PerfContext.Level = PerfLevel.EnableCount;
        try
        {
            using IWritableFile f = LocalFileSystem.Instance.CreateWritable(path, truncate: true,
                new WritableFileOptions { InitialBufferSize = 4096, MaxBufferSize = 4096 });
            IOStatsContext.Current.Reset();
            write(f);
            return IOStatsContext.Current[IOStatsMetric.BytesWritten];
        }
        finally
        {
            PerfContext.Level = before;
        }
    }

    /// <summary>
    /// Pins IOSTATS bytes_written (WritableFileWriter: IOSTATS_ADD(bytes_written, ...) once per write) for the async
    /// append / flush of LocalFileSystem on both of its synchronous-completion paths: pwritev2(RWF_NOWAIT) and the inline
    /// small buffered write used where RWF_NOWAIT writes are unsupported (e.g. ext4). The async calls complete inline here,
    /// so the counters are on this thread, and they must equal the sync members' counts.
    /// </summary>
    [TestMethod]
    [DoNotParallelize]
    public void AsyncLocalWritesCountBytesWrittenLikeSync()
    {
        string dir = Path.Combine(Path.GetTempPath(), "audit8-iostats-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        object saved = s_noWaitWriteUnavailable.GetValue(null)!;
        try
        {
            byte[] small = new byte[1000];
            byte[] large = new byte[10000];
            ulong sync = BytesWrittenBy(f => { f.Append(small); f.Flush(); f.Append(large); f.Flush(); }, Path.Combine(dir, "sync"));
            Assert.AreEqual((ulong)(small.Length + large.Length), sync);
            foreach (bool noWaitUnavailable in new[] { false, true })
            {
                s_noWaitWriteUnavailable.SetValue(null, noWaitUnavailable || (bool)saved);
                // Probe: the first RWF_NOWAIT write tells whether the file system supports it (ext4 does not: EINVAL).
                BytesWrittenBy(f => { f.Append(small); f.FlushAsync().AsTask().GetAwaiter().GetResult(); }, Path.Combine(dir, "probe"));
                if (!noWaitUnavailable && (bool)s_noWaitWriteUnavailable.GetValue(null)!)
                {
                    Console.WriteLine("RWF_NOWAIT writes are not supported here: only the inline path is checked");
                    continue;
                }
                ulong async = BytesWrittenBy(f =>
                {
                    ValueTask t = f.AppendAsync(small);
                    Assert.IsTrue(t.IsCompleted);
                    t.GetAwaiter().GetResult();
                    t = f.FlushAsync();
                    Assert.IsTrue(t.IsCompleted, "the buffered flush completes inline");
                    t.GetAwaiter().GetResult();
                    t = f.AppendAsync(large);
                    Assert.IsTrue(t.IsCompleted, "the write-through completes inline");
                    t.GetAwaiter().GetResult();
                    f.FlushAsync().GetAwaiter().GetResult();
                }, Path.Combine(dir, "async" + noWaitUnavailable));
                Assert.AreEqual(sync, async, "RWF_NOWAIT unavailable: " + (noWaitUnavailable || (bool)saved));
            }
        }
        finally
        {
            s_noWaitWriteUnavailable.SetValue(null, saved);
            Directory.Delete(dir, recursive: true);
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // 5. Cancellation of the async control APIs never leaves a half-applied operation.

    /// <summary>
    /// A token that is cancelled when the file system starts the async operation <paramref name="op"/> on a file whose
    /// name contains <paramref name="fragment"/> (the call already began and the operation has not completed).
    /// </summary>
    private static CancellationTokenSource CancelAt(YieldingFileSystem fs, string op, string fragment)
    {
        var cts = new CancellationTokenSource();
        fs.OnAsync = (o, path, ct) =>
        {
            if (o == op && path.Contains(fragment, StringComparison.Ordinal) && ct == cts.Token) cts.Cancel();
        };
        return cts;
    }

    /// <summary>Opens the DB with every column family its MANIFEST lists.</summary>
    private static async Task<RocksDatabase> OpenAllAsync(IFileSystem fs)
    {
        var descriptors = new List<ColumnFamilyDescriptor>();
        foreach (string n in RocksDatabase.ListColumnFamilies(DbPath, fs)) descriptors.Add(new ColumnFamilyDescriptor(n, new ColumnFamilyOptions()));
        return await RocksDatabase.OpenAsync(DbPath, NewOptions(fs), descriptors);
    }

    /// <summary>
    /// Pins that CreateColumnFamilyAsync cancelled while its MANIFEST record or its OPTIONS file is being written leaves
    /// the DB and the caller in agreement (native's CreateColumnFamily has no cancellation: once the record is written the
    /// creation completes): either OperationCanceledException and no column family in the DB, in the MANIFEST or after a
    /// reopen, or the column family.
    /// </summary>
    [TestMethod]
    public void CreateColumnFamilyAsyncCancelledMidWriteIsAtomic()
    {
        foreach ((string op, string file) in new[] { ("AppendAsync", "MANIFEST"), ("SyncAsync", "MANIFEST"), ("AppendAsync", "OPTIONS"), ("SyncAsync", "OPTIONS") })
        {
            var fs = new YieldingFileSystem(NewFs());
            WithDeadline(Task.Run(async () =>
            {
                await using (RocksDatabase db = await RocksDatabase.OpenAsync(DbPath, NewOptions(fs)))
                {
                    await db.PutAsync(B("k"), B("v"));
                    using CancellationTokenSource cts = CancelAt(fs, op, file);
                    bool created;
                    try
                    {
                        await db.CreateColumnFamilyAsync("cf1", new ColumnFamilyOptions(), cts.Token);
                        created = true;
                    }
                    catch (OperationCanceledException)
                    {
                        created = false;
                    }
                    fs.OnAsync = null;
                    Assert.AreEqual(created, db.ColumnFamilies.Any(c => c.Name == "cf1"), op + " " + file + ": the DB's column families");
                    Assert.AreEqual(created, RocksDatabase.ListColumnFamilies(DbPath, fs).Contains("cf1"), op + " " + file + ": the MANIFEST");
                    // The DB stays usable.
                    await db.PutAsync(B("k2"), B("v2"));
                    ColumnFamily cf2 = await db.CreateColumnFamilyAsync("cf2", new ColumnFamilyOptions());
                    await cf2.PutAsync(B("a"), B("b"));
                }
                await using RocksDatabase reopened = await OpenAllAsync(fs);
                Assert.AreEqual("v2", GetString(reopened, "k2"));
            }), "cancelled CreateColumnFamilyAsync at " + op + " " + file);
        }
    }

    /// <summary>
    /// Pins the same for DropColumnFamilyAsync (cancelled during its MANIFEST record or OPTIONS file) and
    /// SetOptionsAsync (cancelled during the OPTIONS file): OperationCanceledException means nothing changed, otherwise
    /// the change is complete; the column family list of the DB, of the MANIFEST and the live options agree.
    /// </summary>
    [TestMethod]
    public void DropColumnFamilyAndSetOptionsAsyncCancelledMidWriteAreAtomic()
    {
        foreach ((string op, string file) in new[] { ("AppendAsync", "MANIFEST"), ("SyncAsync", "MANIFEST"), ("AppendAsync", "OPTIONS"), ("SyncAsync", "OPTIONS") })
        {
            var fs = new YieldingFileSystem(NewFs());
            WithDeadline(Task.Run(async () =>
            {
                await using RocksDatabase db = await RocksDatabase.OpenAsync(DbPath, NewOptions(fs));
                ColumnFamily cf1 = await db.CreateColumnFamilyAsync("cf1", new ColumnFamilyOptions());
                using (CancellationTokenSource cts = CancelAt(fs, op, file))
                {
                    bool dropped;
                    try
                    {
                        await db.DropColumnFamilyAsync(cf1, cts.Token);
                        dropped = true;
                    }
                    catch (OperationCanceledException)
                    {
                        dropped = false;
                    }
                    fs.OnAsync = null;
                    Assert.AreEqual(dropped, cf1.IsDropped, op + " " + file + ": the handle");
                    Assert.AreEqual(!dropped, RocksDatabase.ListColumnFamilies(DbPath, fs).Contains("cf1"), op + " " + file + ": the MANIFEST");
                }
                if (file != "OPTIONS") return;
                ulong before = db.GetColumnFamily("default").Options.WriteBufferSize;
                using (CancellationTokenSource cts = CancelAt(fs, op, file))
                {
                    bool applied;
                    try
                    {
                        await db.SetOptionsAsync([new("write_buffer_size", "1048576")], cts.Token);
                        applied = true;
                    }
                    catch (OperationCanceledException)
                    {
                        applied = false;
                    }
                    fs.OnAsync = null;
                    Assert.AreEqual(applied ? 1048576UL : before, db.GetColumnFamily("default").Options.WriteBufferSize, op + " " + file + ": SetOptions");
                }
            }), "cancelled DropColumnFamilyAsync / SetOptionsAsync at " + op + " " + file);
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // 6. Mixed sync and async callers.

    /// <summary>
    /// Sync writers on .NET thread-pool threads behind an async write leader: the leader's WAL append completes
    /// asynchronously, so its continuation needs a pool thread, while every pool thread is a sync follower blocked until
    /// that leader finishes the group. With the pool capped at the processor count (no thread injection) this deadlocks;
    /// with the default pool it stalls until the pool injects a thread (hill climbing, about one per 0.5 s per blocked
    /// round). Native has no async writers; the managed sync API is documented not to wait on thread-pool work.
    /// </summary>
    [TestMethod]
    [DoNotParallelize]
    [Ignore("audit #8 open: a sync writer that follows an async write leader waits for the leader's thread-pool continuation (WAL I/O that completes asynchronously, a stall wait); sync writers occupying the pool deadlock a capped pool and stall the default one until it injects threads")]
    public void SyncWritersOnPoolThreadsBehindAnAsyncLeaderProgress()
    {
        var fs = new YieldingFileSystem(NewFs());
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        int n = Environment.ProcessorCount;
        Task all;
        using (new DeadThreadPool(block: false))
        {
            var tasks = new List<Task>();
            for (int t = 0; t < n; t++)
            {
                int id = t;
                tasks.Add(Task.Factory.StartNew(() =>
                {
                    for (int i = 0; i < 2000; i++) db.Put(Key(id * 100_000 + i), B("s"));
                }, CancellationToken.None, TaskCreationOptions.None, TaskScheduler.Default));
            }
            var asyncDone = new TaskCompletionSource();
            var writer = new Thread(() =>
            {
                try
                {
                    for (int i = 0; i < 2000; i++) db.PutAsync(Key(900_000 + i), B("a")).AsTask().GetAwaiter().GetResult();
                    asyncDone.SetResult();
                }
                catch (Exception e)
                {
                    asyncDone.SetException(e);
                }
            }) { IsBackground = true };
            writer.Start();
            tasks.Add(asyncDone.Task);
            all = Task.WhenAll(tasks);
            bool finished = all.Wait(Deadline);
            if (!finished) Assert.Fail("sync writers on pool threads and an async writer made no progress within " + Deadline);
        }
        all.GetAwaiter().GetResult();
    }

    private static async Task ExpectCanceled(ValueTask t, CancellationTokenSource cts, string what)
    {
        await Task.Delay(50);
        Assert.IsFalse(t.IsCompleted, what + " completed before the cancellation");
        cts.Cancel();
        try
        {
            await t.AsTask().WaitAsync(Deadline);
            Assert.Fail(what + " was not cancelled");
        }
        catch (OperationCanceledException)
        {
        }
    }

    /// <summary>
    /// Pins cancellation of the waits of FlushAsync (DBImpl::WaitForFlushMemTables), CompactRangeAsync (RunManualCompaction
    /// waiting for a conflicting compaction) and WaitForCompactAsync: each ends with OperationCanceledException and leaves
    /// no stranded waiter, manual compaction or file marked being compacted. The flush it requested still runs, the
    /// running compaction completes, and later sync and async Flush / CompactRange / WaitForCompact and Dispose work.
    /// </summary>
    [TestMethod]
    public void ControlApiWaitsCancelledLeaveNoStrandedWork()
    {
        var fs = new YieldingFileSystem(NewFs());
        using var compactionGate = new ManualResetEventSlim(false);
        using var flushGate = new ManualResetEventSlim(true);
        int compactions = 0;
        TestHooks.Set("DBImpl::RunCompaction:Start", () =>
        {
            if (Interlocked.Increment(ref compactions) == 1) compactionGate.Wait(Deadline);
        });
        TestHooks.Set("DBImpl::FlushMemTables:Start", () => flushGate.Wait(Deadline));
        WithDeadline(Task.Run(async () =>
        {
            await using RocksDatabase db = await RocksDatabase.OpenAsync(DbPath, NewOptions(fs), new ColumnFamilyOptions { DisableAutoCompactions = true });
            for (int f = 0; f < 3; f++)
            {
                for (int i = 0; i < 50; i++) await db.PutAsync(Key(i), Value(i, f));
                await db.FlushAsync();
            }
            // A manual compaction that blocks on its compaction thread.
            ValueTask first = db.CompactRangeAsync();
            while (Volatile.Read(ref compactions) == 0) await Task.Delay(1);
            // A second CompactRange conflicts with it and waits.
            using (var cts = new CancellationTokenSource()) await ExpectCanceled(db.CompactRangeAsync(Key(0), Key(10), cts.Token), cts, "CompactRangeAsync");
            using (var cts = new CancellationTokenSource()) await ExpectCanceled(db.WaitForCompactAsync(cts.Token), cts, "WaitForCompactAsync");
            // A flush that blocks on its flush thread.
            flushGate.Reset();
            await db.PutAsync(B("flushed"), B("1"));
            using (var cts = new CancellationTokenSource()) await ExpectCanceled(db.FlushAsync(cts.Token), cts, "FlushAsync");
            flushGate.Set();
            compactionGate.Set();
            await first.AsTask().WaitAsync(Deadline);
            await db.WaitForCompactAsync().AsTask().WaitAsync(Deadline);
            // The cancelled flush's request still completed.
            Assert.AreEqual("0", db.GetProperty("rocksdb.num-immutable-mem-table"));
            Assert.AreEqual("0", db.GetProperty("rocksdb.num-running-compactions"));
            await db.PutAsync(B("after"), B("2"));
            await db.FlushAsync();
            await db.CompactRangeAsync();
            await Task.Run(() =>
            {
                db.Put(B("sync"), B("3"));
                db.Flush();
                db.CompactRange();
                db.WaitForCompact();
            });
            // Every CompactRange ran: nothing is left in L0 (non-overlapping files are moved to the last level).
            foreach (LiveFileMetadata f in db.GetLiveFilesMetadata()) Assert.AreEqual(6, f.Level, f.Name);
        }), "cancelled control API waits");
        using RocksDatabase reopened = RocksDatabase.Open(DbPath, NewOptions(fs));
        Assert.AreEqual("1", GetString(reopened, "flushed"));
        Assert.AreEqual("3", GetString(reopened, "sync"));
        Assert.AreEqual("value-49-2" + new string('x', 49), GetString(reopened, "key000049"));
    }

    /// <summary>
    /// Pins concurrent MANIFEST writers mixing sync callers (dedicated threads) and async callers whose MANIFEST I/O
    /// completes asynchronously (VersionSet::LogAndApply's manifest_writers_ queue): column family creates / drops,
    /// flush installs and automatic compactions interleave, every caller gets its own outcome, the reopened MANIFEST
    /// lists exactly the column families the callers created and did not drop, every acknowledged write is readable,
    /// and no background task faults unobserved (TaskScheduler.UnobservedTaskException).
    /// </summary>
    [TestMethod]
    public void ConcurrentSyncAndAsyncManifestWritersAgree()
    {
        int unobserved = 0;
        EventHandler<UnobservedTaskExceptionEventArgs> handler = (_, e) =>
        {
            Interlocked.Increment(ref unobserved);
            Console.WriteLine("unobserved: " + e.Exception);
        };
        TaskScheduler.UnobservedTaskException += handler;
        try
        {
            var fs = new YieldingFileSystem(NewFs());
            var expected = new System.Collections.Concurrent.ConcurrentDictionary<string, bool>();
            RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { WriteBufferSize = 32 << 10, Level0FileNumCompactionTrigger = 2 });
            try
            {
                var workers = new List<Task>();
                for (int w = 0; w < 3; w++)
                {
                    int id = w;
                    workers.Add(Task.Run(async () =>
                    {
                        for (int i = 0; i < 15; i++)
                        {
                            string name = "a" + id + "-" + i;
                            ColumnFamily cf = await db.CreateColumnFamilyAsync(name, new ColumnFamilyOptions());
                            await cf.PutAsync(B("k"), B(name));
                            await db.PutAsync(Key(id * 1000 + i), Value(i, id));
                            if (i % 3 == 0) await db.FlushAsync();
                            if (i % 2 == 0) await db.DropColumnFamilyAsync(cf);
                            else expected[name] = true;
                        }
                    }));
                }
                for (int w = 0; w < 2; w++)
                {
                    int id = w;
                    var done = new TaskCompletionSource();
                    var t = new Thread(() =>
                    {
                        try
                        {
                            for (int i = 0; i < 15; i++)
                            {
                                string name = "s" + id + "-" + i;
                                ColumnFamily cf = db.CreateColumnFamily(name, new ColumnFamilyOptions());
                                cf.Put(B("k"), B(name));
                                db.Put(Key(10_000 + id * 1000 + i), Value(i, id));
                                if (i % 3 == 1) db.Flush();
                                if (i % 2 == 1) db.DropColumnFamily(cf);
                                else expected[name] = true;
                            }
                            done.SetResult();
                        }
                        catch (Exception e)
                        {
                            done.SetException(e);
                        }
                    }) { IsBackground = true };
                    t.Start();
                    workers.Add(done.Task);
                }
                WithDeadline(Task.WhenAll(workers), "concurrent MANIFEST writers");
                WithDeadline(db.WaitForCompactAsync().AsTask(), "WaitForCompactAsync");
            }
            finally
            {
                WithDeadline(db.DisposeAsync().AsTask(), "DisposeAsync");
            }
            List<string> names = RocksDatabase.ListColumnFamilies(DbPath, fs);
            names.Remove("default");
            var want = new List<string>(expected.Keys);
            names.Sort(StringComparer.Ordinal);
            want.Sort(StringComparer.Ordinal);
            CollectionAssert.AreEqual(want, names);
            var descriptors = new List<ColumnFamilyDescriptor> { new("default", new ColumnFamilyOptions()) };
            foreach (string n in want) descriptors.Add(new ColumnFamilyDescriptor(n, new ColumnFamilyOptions()));
            using (RocksDatabase reopened = RocksDatabase.Open(DbPath, NewOptions(fs), descriptors))
            {
                foreach (string n in want) Assert.AreEqual(n, GetString(reopened.GetColumnFamily(n), "k"));
                for (int id = 0; id < 3; id++)
                {
                    for (int i = 0; i < 15; i++) Assert.IsNotNull(reopened.Get(Key(id * 1000 + i)));
                }
                for (int id = 0; id < 2; id++)
                {
                    for (int i = 0; i < 15; i++) Assert.IsNotNull(reopened.Get(Key(10_000 + id * 1000 + i)));
                }
            }
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            Assert.AreEqual(0, Volatile.Read(ref unobserved));
        }
        finally
        {
            TaskScheduler.UnobservedTaskException -= handler;
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // 7. OpenAsync / DisposeAsync parity with Open / Dispose.

    /// <summary>
    /// Pins OpenAsync's failure parity with Open (DBImpl::Open: the LOCK is released when the open fails): a corrupt
    /// CURRENT fails both with the same code, and after either failure (and after an OpenAsync cancelled while it writes
    /// its new MANIFEST) the LOCK is free and the DB opens with all acknowledged data.
    /// </summary>
    [TestMethod]
    public void OpenAsyncFailuresMatchOpenAndReleaseTheLock()
    {
        var fs = new YieldingFileSystem(NewFs());
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            db.Put(B("k"), B("v"));
        }
        // Cancelled while the open writes its OPTIONS file (its MANIFEST writes are not cancellable; the MANIFEST and WAL
        // are read with the blocking members).
        foreach ((string op, string file) in new[] { ("AppendAsync", "OPTIONS"), ("SyncAsync", "OPTIONS") })
        {
            using CancellationTokenSource cts = CancelAt(fs, op, file);
            WithDeadline(Task.Run(async () =>
            {
                try
                {
                    await using RocksDatabase db = await RocksDatabase.OpenAsync(DbPath, NewOptions(fs), cts.Token);
                    Assert.Fail("OpenAsync was not cancelled at " + op + " " + file);
                }
                catch (OperationCanceledException)
                {
                }
            }), "cancelled OpenAsync");
            fs.OnAsync = null;
            using RocksDatabase reopened = RocksDatabase.Open(DbPath, NewOptions(fs));
            Assert.AreEqual("v", GetString(reopened, "k"));
        }
        // A corrupt CURRENT: same error from both, LOCK released.
        byte[] current = ReadAll(fs, DbPath + "/CURRENT");
        WriteAll(fs, DbPath + "/CURRENT", B("MANIFEST-999999\n"));
        var syncError = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, NewOptions(fs)));
        RocksDbException? asyncError = null;
        try
        {
            WithDeadline(RocksDatabase.OpenAsync(DbPath, NewOptions(fs)).AsTask(), "OpenAsync");
        }
        catch (RocksDbException e)
        {
            asyncError = e;
        }
        Assert.IsNotNull(asyncError);
        Assert.AreEqual(syncError.Code, asyncError.Code);
        Assert.AreEqual(syncError.SubCode, asyncError.SubCode);
        Assert.AreEqual(syncError.Message, asyncError.Message);
        WriteAll(fs, DbPath + "/CURRENT", current);
        using (RocksDatabase db = WithDeadline(RocksDatabase.OpenAsync(DbPath, NewOptions(fs)).AsTask(), "OpenAsync"))
        {
            Assert.AreEqual("v", GetString(db, "k"));
        }
    }

    /// <summary>
    /// Pins DisposeAsync racing in-flight async control calls (DBImpl::CloseHelper: CancelAllBackgroundWork, then wait for
    /// the background jobs): a running CompactRangeAsync, a FlushAsync waiting for its flush and a WaitForCompactAsync each
    /// end (success, ObjectDisposedException or ShutdownInProgress), never hang, DisposeAsync completes once the blocked
    /// jobs finish, and the LOCK is released (the DB reopens with the flushed data).
    /// </summary>
    [TestMethod]
    public void DisposeAsyncRacingControlCallsReleasesEveryone()
    {
        var fs = new YieldingFileSystem(NewFs());
        using var compactionGate = new ManualResetEventSlim(false);
        using var flushGate = new ManualResetEventSlim(true);
        int compactions = 0;
        TestHooks.Set("DBImpl::RunCompaction:Start", () =>
        {
            if (Interlocked.Increment(ref compactions) == 1) compactionGate.Wait(Deadline);
        });
        TestHooks.Set("DBImpl::FlushMemTables:Start", () => flushGate.Wait(Deadline));
        WithDeadline(Task.Run(async () =>
        {
            RocksDatabase db = await RocksDatabase.OpenAsync(DbPath, NewOptions(fs), new ColumnFamilyOptions { DisableAutoCompactions = true });
            for (int f = 0; f < 3; f++)
            {
                for (int i = 0; i < 50; i++) await db.PutAsync(Key(i), Value(i, f));
                await db.FlushAsync();
            }
            Task compaction = db.CompactRangeAsync().AsTask();
            while (Volatile.Read(ref compactions) == 0) await Task.Delay(1);
            flushGate.Reset();
            await db.PutAsync(B("late"), B("1"));
            Task flush = db.FlushAsync().AsTask();
            Task wait = db.WaitForCompactAsync().AsTask();
            Task dispose = db.DisposeAsync().AsTask();
            await Task.Delay(50);
            Assert.IsFalse(dispose.IsCompleted, "close waits for the running jobs");
            flushGate.Set();
            compactionGate.Set();
            await dispose.WaitAsync(Deadline);
            foreach ((Task t, string what) in new[] { (compaction, "CompactRangeAsync"), (flush, "FlushAsync"), (wait, "WaitForCompactAsync") })
            {
                try
                {
                    await t.WaitAsync(Deadline);
                }
                catch (ObjectDisposedException)
                {
                }
                catch (RocksDbException e) when (e.Code is StatusCode.ShutdownInProgress or StatusCode.Incomplete)
                {
                }
                catch (TimeoutException)
                {
                    Assert.Fail(what + " was not released by the close");
                }
            }
        }), "DisposeAsync racing control calls");
        using RocksDatabase reopened = RocksDatabase.Open(DbPath, NewOptions(fs));
        Assert.AreEqual("value-49-2" + new string('x', 49), GetString(reopened, "key000049"));
        Assert.AreEqual("1", GetString(reopened, "late"));
    }

    private static byte[] ReadAll(IFileSystem fs, string path)
    {
        using IRandomAccessFile f = fs.OpenRandomAccess(path);
        byte[] b = new byte[f.Length];
        Assert.AreEqual(b.Length, f.Read(0, b));
        return b;
    }

    private static void WriteAll(IFileSystem fs, string path, byte[] data)
    {
        using IWritableFile f = fs.CreateWritable(path, truncate: true);
        f.Append(data);
        f.Sync();
    }

    // ---------------------------------------------------------------------------------------------------------
    // 8. FileSyncWorkers and the rate limiter.

    /// <summary>
    /// Pins FileSyncWorkers (LocalFileSystem.OffloadAsyncSync): many concurrent SyncAsync calls on several files and DBs
    /// each complete exactly once (pooled IValueTaskSource requests, process-wide workers shared by the DBs), a sync of a
    /// closed file fails like the sync member, and synced PutAsync writes on two DBs sharing the workers are durable.
    /// </summary>
    [TestMethod]
    public void OffloadedAsyncSyncsCompleteOncePerCall()
    {
        string dir = Path.Combine(Path.GetTempPath(), "audit8-fsync-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var fs = new LocalFileSystem { OffloadAsyncSync = true };
            int completed = 0;
            var tasks = new List<Task>();
            for (int f = 0; f < 8; f++)
            {
                string path = Path.Combine(dir, "f" + f);
                tasks.Add(Task.Run(async () =>
                {
                    await using IWritableFile file = fs.CreateWritable(path, truncate: true);
                    for (int i = 0; i < 100; i++)
                    {
                        await file.AppendAsync(new byte[100]);
                        await file.SyncAsync();
                        Interlocked.Increment(ref completed);
                    }
                }));
            }
            WithDeadline(Task.WhenAll(tasks), "offloaded syncs");
            Assert.AreEqual(800, completed);
            for (int f = 0; f < 8; f++) Assert.AreEqual(10_000, new FileInfo(Path.Combine(dir, "f" + f)).Length);
            // A sync of a closed file fails like Sync (InvalidArgument "file already closed").
            IWritableFile closed = fs.CreateWritable(Path.Combine(dir, "closed"), truncate: true);
            closed.Dispose();
            var syncError = Assert.ThrowsExactly<RocksDbException>(closed.Sync);
            var asyncError = Assert.ThrowsExactly<RocksDbException>(() => closed.SyncAsync().AsTask().GetAwaiter().GetResult());
            Assert.AreEqual(syncError.Code, asyncError.Code);
            // Two DBs sharing the process-wide sync workers.
            WithDeadline(Task.Run(async () =>
            {
                var o1 = new DbOptions { CreateIfMissing = true, FileSystem = fs };
                var o2 = new DbOptions { CreateIfMissing = true, FileSystem = fs };
                await using RocksDatabase a = await RocksDatabase.OpenAsync(Path.Combine(dir, "a"), o1);
                await using RocksDatabase b = await RocksDatabase.OpenAsync(Path.Combine(dir, "b"), o2);
                var writes = new List<Task>();
                for (int w = 0; w < 4; w++)
                {
                    int id = w;
                    writes.Add(Task.Run(async () =>
                    {
                        for (int i = 0; i < 50; i++)
                        {
                            await a.PutAsync(new WriteOptions { Sync = true }, Key(id * 100 + i), B("a"));
                            await b.PutAsync(new WriteOptions { Sync = true }, Key(id * 100 + i), B("b"));
                        }
                    }));
                }
                await Task.WhenAll(writes);
            }), "synced PutAsync on two DBs");
            using (RocksDatabase a = RocksDatabase.Open(Path.Combine(dir, "a"), new DbOptions { FileSystem = fs }))
            using (RocksDatabase b = RocksDatabase.Open(Path.Combine(dir, "b"), new DbOptions { FileSystem = fs }))
            {
                for (int i = 0; i < 400; i += 7)
                {
                    int k = i / 100 * 100 + i % 50;
                    Assert.AreEqual("a", GetString(a, S(Key(k))!));
                    Assert.AreEqual("b", GetString(b, S(Key(k))!));
                }
            }
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    /// <summary>
    /// Pins GenericRateLimiter waits (util/rate_limiter.cc RequestImpl): an async waiter cancelled while queued ends with
    /// OperationCanceledException and does not block the queue forever (its remaining bytes are still charged, which
    /// is the documented behaviour); a sync waiter queued behind it is granted; Dispose releases a pending async waiter.
    /// </summary>
    [TestMethod]
    public void RateLimiterCancelledAsyncWaiterDoesNotStrandOthers()
    {
        using var limiter = new GenericRateLimiter(10_000, 10_000, 10, RateLimiterMode.WritesOnly, autoTuned: false, 0); // 100 B per 10 ms
        limiter.Request(100, IoPriority.Low); // drain the bucket
        using var cts = new CancellationTokenSource();
        ValueTask cancelled = limiter.RequestAsync(100, IoPriority.Low, cts.Token);
        Assert.IsFalse(cancelled.IsCompleted);
        cts.Cancel();
        Assert.ThrowsExactly<TaskCanceledException>(() => cancelled.AsTask().GetAwaiter().GetResult());
        OnDedicatedThread(() => limiter.Request(100, IoPriority.High), "a sync request behind a cancelled one");
        ValueTask pending = limiter.RequestAsync(100, IoPriority.Low, CancellationToken.None);
        limiter.Dispose();
        WithDeadline(pending.AsTask(), "a pending request after Dispose");
    }
}
