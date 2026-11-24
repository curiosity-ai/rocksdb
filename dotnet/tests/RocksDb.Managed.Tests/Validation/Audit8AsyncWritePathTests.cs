using System.Collections.Concurrent;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Tests.Db;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #8, the asynchronous write path: the async primitives (Util/SyncValueTask, OwnedValueTaskMethodBuilder,
/// AsyncBroadcastSignal), the write thread's pooled waiters and the group commit of every write mode
/// (db/write_thread.cc, db/db_impl/db_impl_write.cc), checked against the synchronous API of the same code.
/// <list type="bullet">
/// <item>A synchronous API call never uses an asynchronous file member, also when its writer leads a group of
/// asynchronous writers, switches memtables or WALs, or waits for a stall (the one-implementation convention: with
/// <c>synchronous: true</c> every await completes inline).</item>
/// <item>The same program gives the same results, sequence numbers and errors through the sync and the async API.</item>
/// </list>
/// </summary>
[TestClass]
public sealed class Audit8AsyncWritePathTests
{
    private const string DbPath = "/audit8asyncwrite";

    [TestCleanup]
    public void Cleanup() => TestHooks.ClearAll();

    /// <summary>The write modes of DBImpl::WriteImpl.</summary>
    public enum WriteMode
    {
        Default,
        SerialInserts,
        Pipelined,
        Unordered,
        TwoWriteQueues,
    }

    private static DbOptions ModeOptions(IFileSystem fs, WriteMode mode)
    {
        DbOptions o = NewOptions(fs);
        o.EnablePipelinedWrite = mode == WriteMode.Pipelined;
        o.UnorderedWrite = mode == WriteMode.Unordered;
        o.TwoWriteQueues = mode == WriteMode.TwoWriteQueues;
        o.AllowConcurrentMemtableWrite = mode != WriteMode.SerialInserts;
        return o;
    }

    // ---- a file system whose asynchronous members really complete asynchronously ------------------------------

    /// <summary>
    /// Forwards to a <see cref="MemoryFileSystem"/>. Every asynchronous member (AppendAsync, FlushAsync, SyncAsync,
    /// DisposeAsync, ReadAsync) yields to the thread pool before it does its work, so that an async path really goes
    /// async; on a thread inside a synchronous API call (<see cref="Sync"/>) it records a violation instead and completes
    /// inline (SyncValueTask's fallback would assert in a debug build).
    /// </summary>
    private sealed class ForcedAsyncFileSystem(MemoryFileSystem inner) : IFileSystem
    {
        [ThreadStatic] private static bool t_inSyncCall;

        public readonly ConcurrentQueue<string> Violations = new();
        public int AsyncCompletions;

        /// <summary>Runs <paramref name="call"/> as a synchronous API call: it must not reach an asynchronous file member.</summary>
        public static void Sync(Action call)
        {
            bool outer = t_inSyncCall;
            t_inSyncCall = true;
            try { call(); }
            finally { t_inSyncCall = outer; }
        }

        public static T Sync<T>(Func<T> call)
        {
            bool outer = t_inSyncCall;
            t_inSyncCall = true;
            try { return call(); }
            finally { t_inSyncCall = outer; }
        }

        /// <summary>True when the member must complete inline (a violation was recorded).</summary>
        private bool InSyncCall(string member, string path)
        {
            if (!t_inSyncCall) return false;
            Violations.Enqueue(member + " of " + path + " in a synchronous API call:\n" + Environment.StackTrace);
            return true;
        }

        private async ValueTask GoAsync()
        {
            // To the thread pool, whatever the caller's synchronization context (Task.Yield would post to it).
            await Task.Run(static () => { }).ConfigureAwait(false);
            Interlocked.Increment(ref AsyncCompletions);
        }

        private sealed class Writable(ForcedAsyncFileSystem fs, IWritableFile file, string path) : IWritableFile, ITruncatableWritableFile
        {
            public long Size => file.Size;
            public void Append(ReadOnlySpan<byte> data) => file.Append(data);
            public void Flush() => file.Flush();
            public void Sync() => file.Sync();
            public void Dispose() => file.Dispose();
            public void Truncate(long size) => ((ITruncatableWritableFile)file).Truncate(size);

            public async ValueTask AppendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
            {
                if (fs.InSyncCall("AppendAsync", path))
                {
                    file.Append(data.Span);
                    return;
                }
                await fs.GoAsync().ConfigureAwait(false);
                await file.AppendAsync(data, cancellationToken).ConfigureAwait(false);
            }

            public async ValueTask FlushAsync(CancellationToken cancellationToken = default)
            {
                if (fs.InSyncCall("FlushAsync", path))
                {
                    file.Flush();
                    return;
                }
                await fs.GoAsync().ConfigureAwait(false);
                await file.FlushAsync(cancellationToken).ConfigureAwait(false);
            }

            public async ValueTask SyncAsync(CancellationToken cancellationToken = default)
            {
                if (fs.InSyncCall("SyncAsync", path))
                {
                    file.Sync();
                    return;
                }
                await fs.GoAsync().ConfigureAwait(false);
                await file.SyncAsync(cancellationToken).ConfigureAwait(false);
            }

            public async ValueTask DisposeAsync()
            {
                if (fs.InSyncCall("DisposeAsync", path))
                {
                    file.Dispose();
                    return;
                }
                await fs.GoAsync().ConfigureAwait(false);
                await file.DisposeAsync().ConfigureAwait(false);
            }
        }

        private sealed class RandomAccess(ForcedAsyncFileSystem fs, IRandomAccessFile file, string path) : IRandomAccessFile
        {
            public long Length => file.Length;
            public int Read(long offset, Span<byte> destination) => file.Read(offset, destination);
            public void Dispose() => file.Dispose();

            public async ValueTask<int> ReadAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken = default)
            {
                if (fs.InSyncCall("ReadAsync", path)) return file.Read(offset, destination.Span);
                await fs.GoAsync().ConfigureAwait(false);
                return await file.ReadAsync(offset, destination, cancellationToken).ConfigureAwait(false);
            }
        }

        private sealed class Sequential(ForcedAsyncFileSystem fs, ISequentialFile file, string path) : ISequentialFile
        {
            public int Read(Span<byte> destination) => file.Read(destination);
            public void Skip(long bytes) => file.Skip(bytes);
            public void Dispose() => file.Dispose();

            public async ValueTask<int> ReadAsync(Memory<byte> destination, CancellationToken cancellationToken = default)
            {
                if (fs.InSyncCall("ReadAsync", path)) return file.Read(destination.Span);
                await fs.GoAsync().ConfigureAwait(false);
                return await file.ReadAsync(destination, cancellationToken).ConfigureAwait(false);
            }
        }

        public IWritableFile CreateWritable(string path, bool truncate = true) => new Writable(this, inner.CreateWritable(path, truncate), path);
        public IWritableFile CreateWritable(string path, bool truncate, WritableFileOptions options) => new Writable(this, inner.CreateWritable(path, truncate, options), path);
        public IWritableFile ReopenWritable(string path, WritableFileOptions options) => new Writable(this, inner.ReopenWritable(path, options), path);
        public IRandomAccessFile OpenRandomAccess(string path) => new RandomAccess(this, inner.OpenRandomAccess(path), path);
        public IRandomAccessFile OpenRandomAccess(string path, RandomAccessFileOptions options) => new RandomAccess(this, inner.OpenRandomAccess(path, options), path);
        public ISequentialFile OpenSequential(string path) => new Sequential(this, inner.OpenSequential(path), path);
        public ISequentialFile OpenSequential(string path, SequentialFileOptions options) => new Sequential(this, inner.OpenSequential(path, options), path);
        public bool FileExists(string path) => inner.FileExists(path);
        public long GetFileSize(string path) => inner.GetFileSize(path);
        public void DeleteFile(string path) => inner.DeleteFile(path);
        public void RenameFile(string source, string target) => inner.RenameFile(source, target);
        public void CreateDirectoryIfMissing(string path) => inner.CreateDirectoryIfMissing(path);
        public IEnumerable<string> GetChildren(string directory) => inner.GetChildren(directory);
        public IDisposable LockFile(string path) => inner.LockFile(path);
        public void SyncDirectory(string path) => inner.SyncDirectory(path);
        public bool DirectoryExists(string path) => inner.DirectoryExists(path);
        public void DeleteDirectory(string path) => inner.DeleteDirectory(path);
        public void LinkFile(string source, string target) => inner.LinkFile(source, target);
        public DateTimeOffset GetFileModificationTime(string path) => inner.GetFileModificationTime(path);
        public long GetFreeSpace(string path) => inner.GetFreeSpace(path);
        public void WriteAt(string path, long offset, ReadOnlySpan<byte> data) => inner.WriteAt(path, offset, data);
    }

    private static void AssertNoViolations(ForcedAsyncFileSystem fs)
    {
        if (fs.Violations.TryPeek(out string? first))
            Assert.Fail(fs.Violations.Count + " asynchronous file member call(s) in synchronous API calls; the first:\n" + first);
    }

    // ---- synchronous API calls stay synchronous ----------------------------------------------------------------

    /// <summary>
    /// Pins the one-implementation convention for the write path: synchronous writes (Put / Delete / Merge / Write /
    /// WriteWithCallback, synced or not), FlushWAL / SyncWAL, LockWAL, Flush, CompactRange, column family creation and
    /// drop, and the close, run concurrently with asynchronous writers on a file system whose async members really
    /// complete asynchronously, and never reach an async file member: a synchronous writer that leads a group of
    /// asynchronous writers does the group's WAL I/O, memtable switches (small write buffers), WAL switches
    /// (max_total_wal_size with two column families) and stall waits synchronously. Every acknowledged write is in the
    /// DB after a reopen.
    /// </summary>
    [TestMethod]
    [DataRow(WriteMode.Default)]
    [DataRow(WriteMode.SerialInserts)]
    [DataRow(WriteMode.Pipelined)]
    [DataRow(WriteMode.Unordered)]
    [DataRow(WriteMode.TwoWriteQueues)]
    [Timeout(60_000, CooperativeCancellation = true)]
    public async Task SyncCallsNeverUseAsyncFileMembersWhileMixedWithAsyncWriters(WriteMode mode)
    {
        var fs = new ForcedAsyncFileSystem(NewFs());
        DbOptions o = ModeOptions(fs, mode);
        o.MaxTotalWalSize = 256 << 10;
        o.DelayedWriteRate = 64 << 20;
        var cfo = new ColumnFamilyOptions { WriteBufferSize = 64 << 10, Level0SlowdownWritesTrigger = 4, Level0StopWritesTrigger = 64, MergeOperator = new StringAppendMergeOperator() };
        o.CreateMissingColumnFamilies = true;
        var descriptors = new List<ColumnFamilyDescriptor>
        {
            new(ColumnFamilyDescriptor.DefaultColumnFamilyName, cfo),
            new("second", cfo),
        };
        var acknowledged = new ConcurrentDictionary<string, string>();
        RocksDatabase db = ForcedAsyncFileSystem.Sync(() => RocksDatabase.Open(DbPath, o, descriptors));
        try
        {
            ColumnFamily second = db.GetColumnFamily("second");
            using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            var asyncWriters = new List<Task>();
            for (int t = 0; t < 4; t++)
            {
                int id = t;
                asyncWriters.Add(Task.Run(async () =>
                {
                    var rnd = new Random(1000 + id);
                    for (int i = 0; !stop.IsCancellationRequested; i++)
                    {
                        string k = "a" + id + "-" + i;
                        string v = new string((char)('a' + i % 26), rnd.Next(1, 300));
                        var wo = new WriteOptions { Sync = rnd.Next(4) == 0 };
                        if (rnd.Next(3) == 0)
                        {
                            await second.PutAsync(wo, B(k), B(v));
                            acknowledged["second:" + k] = v;
                        }
                        else
                        {
                            await db.PutAsync(wo, B(k), B(v));
                            acknowledged[k] = v;
                        }
                    }
                }));
            }
            var syncWriters = new List<Thread>();
            var syncErrors = new ConcurrentQueue<Exception>();
            for (int t = 0; t < 2; t++)
            {
                int id = t;
                var thread = new Thread(() =>
                {
                    try
                    {
                        var rnd = new Random(2000 + id);
                        for (int i = 0; !stop.IsCancellationRequested; i++)
                        {
                            string k = "s" + id + "-" + i;
                            string v = new string((char)('A' + i % 26), rnd.Next(1, 300));
                            var wo = new WriteOptions { Sync = rnd.Next(4) == 0 };
                            int op = rnd.Next(100);
                            ForcedAsyncFileSystem.Sync(() =>
                            {
                                if (op < 50) db.Put(wo, B(k), B(v));
                                else if (op < 60) second.Put(wo, B(k), B(v));
                                else if (op < 70)
                                {
                                    using var batch = new WriteBatch();
                                    batch.Put(B(k), B(v)).Merge(B("merged" + id), B("x")).Delete(B(k + "-gone"));
                                    db.Write(wo, batch);
                                }
                                else if (op < 78)
                                {
                                    using var batch = new WriteBatch();
                                    batch.Put(B(k), B(v));
                                    db.WriteWithCallback(wo, batch, new NoOpUserCallback());
                                }
                                else if (op < 84) db.Put(wo, B(k), B(v)); // the same, more often
                                else if (op < 88) db.FlushWal(sync: rnd.Next(2) == 0);
                                else if (op < 90)
                                {
                                    db.Put(wo, B(k), B(v));
                                    db.Flush();
                                }
                                else if (op < 91)
                                {
                                    db.Put(wo, B(k), B(v));
                                    db.CompactRange();
                                }
                                else if (op < 93)
                                {
                                    using (WalLock l = db.LockWal()) db.FlushWal(sync: true);
                                    db.Put(wo, B(k), B(v));
                                }
                                else if (op < 94)
                                {
                                    ColumnFamily extra = db.CreateColumnFamily("extra" + id + "-" + i, cfo);
                                    extra.Put(B("k"), B("v"));
                                    db.DropColumnFamily(extra);
                                    db.Put(wo, B(k), B(v));
                                }
                                else db.Put(wo, B(k), B(v));
                            });
                            if (op is >= 50 and < 60) acknowledged["second:" + k] = v;
                            else if (op is < 84 or >= 88) acknowledged[k] = v;
                        }
                    }
                    catch (Exception e)
                    {
                        syncErrors.Enqueue(e);
                    }
                });
                thread.Start();
                syncWriters.Add(thread);
            }
            await Task.WhenAll(asyncWriters).WaitAsync(TimeSpan.FromSeconds(40));
            foreach (Thread thread in syncWriters) Assert.IsTrue(thread.Join(TimeSpan.FromSeconds(40)), "a synchronous writer hangs");
            Assert.IsTrue(syncErrors.IsEmpty, syncErrors.TryPeek(out Exception? se) ? se.ToString() : null);
            // Audit #12: once the DB has synchronous writers, asynchronous leaders do their WAL I/O synchronously too (see
            // Audit12SyncAsyncWriteTests), so the asynchronous writers may never reach an asynchronous file member here.
            Assert.IsFalse(acknowledged.IsEmpty, "no write was acknowledged");
            AssertNoViolations(fs);
        }
        finally
        {
            ForcedAsyncFileSystem.Sync(db.Dispose);
        }
        AssertNoViolations(fs);
        using RocksDatabase reopened = RocksDatabase.Open(DbPath, o, descriptors);
        ColumnFamily second2 = reopened.GetColumnFamily("second");
        foreach ((string key, string value) in acknowledged)
        {
            string? got = key.StartsWith("second:", StringComparison.Ordinal) ? GetString(second2, key["second:".Length..]) : GetString(reopened, key);
            Assert.AreEqual(value, got, key);
        }
    }

    // ---- sync / async parity -----------------------------------------------------------------------------------

    /// <summary>Counts the callbacks of a WriteWithCallback write.</summary>
    private sealed class CountingCallback : UserWriteCallback
    {
        public int Enqueued;
        public int WalWritten;
        public override void OnWriteEnqueued() => Interlocked.Increment(ref Enqueued);
        public override void OnWalWriteFinish() => Interlocked.Increment(ref WalWritten);
    }

    private static string Outcome(Exception? e) => e switch
    {
        null => "ok",
        RocksDbException r => "RocksDbException:" + r.Code,
        _ => e.GetType().Name,
    };

    /// <summary>
    /// Runs a seeded program of single writes (Put / Delete / SingleDelete / DeleteRange / Merge, default and second
    /// column family), batches, WriteWithCallback, DisableWal writes, invalid writes (sync with disableWAL; Merge into a
    /// column family without merge operator; a write to a dropped column family) and flushes, through the synchronous or
    /// the asynchronous API; returns the outcome and the latest sequence number after each step, the callback counts,
    /// the write tickers, and the contents of every column family.
    /// </summary>
    private static async Task<List<string>> RunProgramAsync(int seed, WriteMode mode, bool useAsync)
    {
        var fs = new ForcedAsyncFileSystem(NewFs());
        DbOptions o = ModeOptions(fs, mode);
        o.Statistics = new Statistics();
        o.CreateMissingColumnFamilies = true;
        // No automatic compactions: background work would make the outcome timing dependent (a compaction that finds a
        // broken SingleDelete contract stops the DB at some point of the program).
        var withMerge = new ColumnFamilyOptions { MergeOperator = new StringAppendMergeOperator(), DisableAutoCompactions = true };
        var descriptors = new List<ColumnFamilyDescriptor>
        {
            new(ColumnFamilyDescriptor.DefaultColumnFamilyName, withMerge),
            new("second", withMerge),
            new("nomerge", new ColumnFamilyOptions { DisableAutoCompactions = true }),
        };
        var log = new List<string>();
        var rnd = new Random(seed);
        RocksDatabase db = useAsync ? await RocksDatabase.OpenAsync(DbPath, o, descriptors) : RocksDatabase.Open(DbPath, o, descriptors);
        try
        {
            ColumnFamily second = db.GetColumnFamily("second");
            ColumnFamily nomerge = db.GetColumnFamily("nomerge");
            ColumnFamily dropped = useAsync ? await db.CreateColumnFamilyAsync("dropped", withMerge) : db.CreateColumnFamily("dropped", withMerge);
            if (useAsync) await db.DropColumnFamilyAsync(dropped);
            else db.DropColumnFamily(dropped);
            var callback = new CountingCallback();
            for (int step = 0; step < 400; step++)
            {
                int op = rnd.Next(100);
                byte[] k = B("k" + rnd.Next(60).ToString("D3", System.Globalization.CultureInfo.InvariantCulture));
                byte[] k2 = B("k" + rnd.Next(60).ToString("D3", System.Globalization.CultureInfo.InvariantCulture));
                byte[] v = B("v" + step + new string('x', rnd.Next(0, 200)));
                ColumnFamily cf = rnd.Next(4) == 0 ? second : db.DefaultColumnFamily;
                var wo = new WriteOptions { Sync = rnd.Next(5) == 0, DisableWal = rnd.Next(10) == 0 };
                if (wo.Sync && wo.DisableWal && rnd.Next(2) == 0) wo.Sync = false;
                Exception? error = null;
                try
                {
                    if (op < 35)
                    {
                        if (useAsync) await cf.PutAsync(wo, k, v);
                        else cf.Put(wo, k, v);
                    }
                    else if (op < 45)
                    {
                        if (useAsync) await cf.DeleteAsync(wo, k);
                        else cf.Delete(wo, k);
                    }
                    else if (op < 50)
                    {
                        if (useAsync) await cf.SingleDeleteAsync(wo, k);
                        else cf.SingleDelete(wo, k);
                    }
                    else if (op < 55)
                    {
                        byte[] lo = string.CompareOrdinal(S(k), S(k2)) <= 0 ? k : k2;
                        byte[] hi = lo == k ? k2 : k;
                        if (useAsync) await cf.DeleteRangeAsync(wo, lo, hi);
                        else cf.DeleteRange(wo, lo, hi);
                    }
                    else if (op < 65)
                    {
                        ColumnFamily target = rnd.Next(8) == 0 ? nomerge : cf;
                        if (useAsync) await target.MergeAsync(wo, k, v);
                        else target.Merge(wo, k, v);
                    }
                    else if (op < 80)
                    {
                        using var batch = new WriteBatch();
                        int n = rnd.Next(1, 6);
                        for (int i = 0; i < n; i++)
                        {
                            byte[] bk = B("b" + rnd.Next(40));
                            switch (rnd.Next(3))
                            {
                                case 0: batch.Put(second, bk, v); break;
                                case 1: batch.Delete(bk); break;
                                default: batch.Merge(bk, v); break;
                            }
                        }
                        if (rnd.Next(10) == 0) batch.Put(dropped, k, v);
                        if (useAsync) await db.WriteAsync(wo, batch);
                        else db.Write(wo, batch);
                    }
                    else if (op < 90)
                    {
                        using var batch = new WriteBatch();
                        batch.Put(k, v);
                        if (useAsync) await db.WriteWithCallbackAsync(wo, batch, callback);
                        else db.WriteWithCallback(wo, batch, callback);
                    }
                    else if (op < 93)
                    {
                        if (useAsync) await db.FlushAsync();
                        else db.Flush();
                    }
                    else if (op < 96)
                    {
                        if (useAsync) await db.FlushWalAsync(sync: rnd.Next(2) == 0);
                        else db.FlushWal(sync: rnd.Next(2) == 0);
                    }
                    else
                    {
                        // A write that cannot be retried by the sync API under a different shape: an empty batch.
                        using var batch = new WriteBatch();
                        if (useAsync) await db.WriteAsync(wo, batch);
                        else db.Write(wo, batch);
                    }
                }
                catch (Exception e)
                {
                    error = e;
                }
                log.Add(step + " op " + op + ": " + Outcome(error) + " seq " + db.GetLatestSequenceNumber());
            }
            log.Add("callbacks " + callback.Enqueued + "/" + callback.WalWritten);
            Statistics stats = o.Statistics;
            foreach (Ticker t in new[] { Ticker.NumberKeysWritten, Ticker.BytesWritten, Ticker.WriteDoneBySelf, Ticker.WriteDoneByOther, Ticker.WriteWithWal, Ticker.WalFileBytes, Ticker.WalFileSynced })
                log.Add(t + " " + stats.GetTickerCount(t));
            foreach (ColumnFamily c in new[] { db.DefaultColumnFamily, second, nomerge })
            {
                using Iterator it = c.NewIterator();
                for (it.SeekToFirst(); it.Valid; it.Next()) log.Add(c.Name + " " + S(it.Key.ToArray()) + "=" + S(it.Value.ToArray()));
            }
        }
        finally
        {
            if (useAsync) await db.DisposeAsync();
            else db.Dispose();
        }
        if (!useAsync) AssertNoViolations(fs);
        return log;
    }

    /// <summary>
    /// Sync / async parity of the write API (the reference for an async call is the sync path of the same code): the
    /// same seeded program gives the same outcome (success or exception type and status code), the same sequence numbers
    /// after every step, the same WriteWithCallback notifications, the same write tickers and the same contents, in every
    /// write mode, with a file system whose async members complete asynchronously.
    /// </summary>
    [TestMethod]
    [DataRow(WriteMode.Default)]
    [DataRow(WriteMode.SerialInserts)]
    [DataRow(WriteMode.Pipelined)]
    [DataRow(WriteMode.Unordered)]
    [DataRow(WriteMode.TwoWriteQueues)]
    [Timeout(60_000, CooperativeCancellation = true)]
    public async Task SyncAndAsyncApiGiveTheSameResults(WriteMode mode)
    {
        int seed = int.TryParse(Environment.GetEnvironmentVariable("AUDIT8_SEED"), out int fixedSeed) ? fixedSeed : Environment.TickCount;
        Console.WriteLine("seed " + seed);
        List<string> sync = await RunProgramAsync(seed, mode, useAsync: false);
        List<string> async = await RunProgramAsync(seed, mode, useAsync: true);
        if (Environment.GetEnvironmentVariable("AUDIT8_DUMP") is { } dump)
        {
            File.WriteAllLines(dump + ".sync.txt", sync);
            File.WriteAllLines(dump + ".async.txt", async);
        }
        for (int i = 0; i < Math.Min(sync.Count, async.Count); i++)
        {
            Assert.AreEqual(sync[i], async[i], "seed " + seed + ", line " + i);
        }
        Assert.AreEqual(sync.Count, async.Count, "seed " + seed);
    }

    // ---- groups of sync and async writers ---------------------------------------------------------------------

    private const string JoinWaitHook = "WriteThread::JoinBatchGroup:Wait";
    private const string EncodedHook = "MemTable::Add:Encoded";
    private static readonly WriteOptions s_protected = new() { ProtectionBytesPerKey = 8 };

    /// <summary>Holds its write, as the leader of its write group, right after the group's WAL record is written.</summary>
    private sealed class HoldAfterWal : UserWriteCallback, IDisposable
    {
        public readonly ManualResetEventSlim InWal = new();
        public readonly ManualResetEventSlim Release = new();
        public override void OnWriteEnqueued() { }

        public override void OnWalWriteFinish()
        {
            InWal.Set();
            Release.Wait();
        }

        public void Dispose()
        {
            InWal.Dispose();
            Release.Dispose();
        }
    }

    /// <summary>
    /// Queues the writers <paramref name="keys"/> (asynchronous where <paramref name="async"/> says so, else on their
    /// own thread) behind a synchronous write held as group leader after its WAL write, in that order, so that they form
    /// the next write group (the first one leads it); then releases the held write. Returns the outcome of each writer
    /// (null when acknowledged) and the held write's.
    /// </summary>
    private static (Exception?[] Errors, Exception? HeldError) RunGroupBehindHeldLeader(RocksDatabase db, string[] keys, bool[] async, WriteOptions options) =>
        RunGroupBehindHeldLeader(db, keys, async, options, callbacks: null);

    /// <summary>
    /// <see cref="RunGroupBehindHeldLeader(RocksDatabase, string[], bool[], WriteOptions)"/>; writer i writes with
    /// WriteWithCallback(Async) and <paramref name="callbacks"/>[i] when that is not null.
    /// </summary>
    private static (Exception?[] Errors, Exception? HeldError) RunGroupBehindHeldLeader(RocksDatabase db, string[] keys, bool[] async, WriteOptions options,
        UserWriteCallback?[]? callbacks)
    {
        using var queued = new SemaphoreSlim(0);
        using var hold = new HoldAfterWal();
        Exception? heldError = null;
        var held = new Thread(() =>
        {
            try
            {
                using var batch = new WriteBatch();
                batch.Put(B("held"), B("h"));
                db.WriteWithCallback(options, batch, hold);
            }
            catch (Exception e)
            {
                heldError = e;
            }
        });
        held.Start();
        Assert.IsTrue(hold.InWal.Wait(TimeSpan.FromSeconds(20)), "the held write did not reach its WAL write");
        TestHooks.Set(JoinWaitHook, () => queued.Release());
        var errors = new Exception?[keys.Length];
        var waits = new List<Func<bool>>();
        for (int i = 0; i < keys.Length; i++)
        {
            int n = i;
            if (async[i])
            {
                Task t = WriteOneAsync(db, options, keys[n], "v" + n, callbacks?[n]);
                // An asynchronous writer resumes on the thread pool, never inline on the thread of the (synchronous)
                // leader that completed it (RunContinuationsAsynchronously), nor on a held synchronous writer.
                Task<bool> onPool = t.ContinueWith(static _ => Thread.CurrentThread.IsThreadPoolThread, CancellationToken.None,
                    TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
                waits.Add(() =>
                {
                    if (!((IAsyncResult)t).AsyncWaitHandle.WaitOne(TimeSpan.FromSeconds(20))) return false;
                    Assert.IsTrue(onPool.Wait(TimeSpan.FromSeconds(20)) && onPool.Result, keys[n] + " completed on a synchronous writer's thread");
                    errors[n] = t.Exception?.InnerException;
                    if (t.Exception is { InnerExceptions.Count: not 1 } ae) errors[n] = ae;
                    return true;
                });
            }
            else
            {
                var thread = new Thread(() =>
                {
                    try { WriteOne(db, options, keys[n], "v" + n, callbacks?[n]); }
                    catch (Exception e) { errors[n] = e; }
                });
                thread.Start();
                waits.Add(() => thread.Join(TimeSpan.FromSeconds(20)));
            }
            Assert.IsTrue(queued.Wait(TimeSpan.FromSeconds(20)), keys[n] + " did not queue");
        }
        TestHooks.Set(JoinWaitHook, null);
        hold.Release.Set();
        Assert.IsTrue(held.Join(TimeSpan.FromSeconds(20)), "the held write hangs");
        for (int i = 0; i < waits.Count; i++) Assert.IsTrue(waits[i](), keys[i] + " hangs (stranded write group)");
        return (errors, heldError);
    }

    private static void WriteOne(RocksDatabase db, WriteOptions options, string key, string value, UserWriteCallback? callback)
    {
        if (callback is null)
        {
            db.Put(options, B(key), B(value));
            return;
        }
        using var batch = new WriteBatch();
        batch.Put(B(key), B(value));
        db.WriteWithCallback(options, batch, callback);
    }

    private static async Task WriteOneAsync(RocksDatabase db, WriteOptions options, string key, string value, UserWriteCallback? callback)
    {
        if (callback is null)
        {
            await db.PutAsync(options, B(key), B(value));
            return;
        }
        using var batch = new WriteBatch();
        batch.Put(B(key), B(value));
        await db.WriteWithCallbackAsync(options, batch, callback);
    }

    /// <summary>
    /// A write group mixing synchronous and asynchronous writers, led by either kind, whose memtable insert fails (the
    /// entry of one writer is corrupted on its way into the memtable, caught by its protection bytes): every writer of
    /// the group, sync or async, leader or follower, gets the insert's Corruption exactly once as a RocksDbException (no
    /// AggregateException, no other type), none hangs, and the DB is stopped, as for an all-sync group
    /// (Audit4WritePathRaceTests.FailedInsertFailsEveryWriterOfItsGroup; DBImpl::WriteImpl write_group.status).
    /// </summary>
    [TestMethod]
    [DataRow(WriteMode.Default, "async,sync,async", 0)]
    [DataRow(WriteMode.Default, "sync,async,async", 2)]
    [DataRow(WriteMode.SerialInserts, "async,sync,sync", 1)]
    [DataRow(WriteMode.SerialInserts, "sync,async,sync", 0)]
    [DataRow(WriteMode.Pipelined, "async,async,sync", 2)]
    [DataRow(WriteMode.Pipelined, "sync,sync,async", 1)]
    [DataRow(WriteMode.TwoWriteQueues, "async,sync,async", 1)]
    [Timeout(60_000, CooperativeCancellation = true)]
    public void FailedInsertOfMixedGroupFailsEveryWriterOnce(WriteMode mode, string kinds, int bad)
    {
        bool[] async = Array.ConvertAll(kinds.Split(','), k => k == "async");
        string[] keys = ["g0", "g1", "g2"];
        byte[] badKey = B(keys[bad]);
        int corrupted = 0;
        TestHooks.SetSpan(EncodedHook, data =>
        {
            if (data.Slice(1, data[0] - 8).SequenceEqual(badKey) && Interlocked.Exchange(ref corrupted, 1) == 0) data[1 + data[0] + 1]++;
        });
        using RocksDatabase db = RocksDatabase.Open(DbPath, ModeOptions(NewFs(), mode));
        db.Put(s_protected, B("before"), B("1"));
        (Exception?[] errors, Exception? heldError) = RunGroupBehindHeldLeader(db, keys, async, s_protected);
        // Pipelined: the held writer leads its memtable stage once its WAL write is released. If the group behind it
        // finished its WAL stage by then, the held writer's memtable group includes those writers, and the failed insert
        // is the status of that whole group (write_group.status), the held writer included. Which of the two happens is
        // a matter of timing (the test failed about once in fifteen runs when it insisted on the first).
        bool heldSharedTheGroup = mode == WriteMode.Pipelined && heldError is RocksDbException { Code: StatusCode.Corruption };
        if (!heldSharedTheGroup) Assert.IsNull(heldError, heldError?.ToString());
        Assert.AreEqual(1, corrupted);
        for (int i = 0; i < keys.Length; i++)
        {
            Assert.IsInstanceOfType<RocksDbException>(errors[i], keys[i] + " (" + kinds + "): " + (errors[i]?.ToString() ?? "acknowledged"));
            Assert.AreEqual(StatusCode.Corruption, ((RocksDbException)errors[i]!).Code, keys[i]);
            Assert.IsNull(db.Get(B(keys[i])), keys[i]);
        }
        // Each writer got its own exception object (ThrowWriteError).
        Assert.AreNotSame(errors[0], errors[1]);
        Assert.AreNotSame(errors[1], errors[2]);
        Assert.AreEqual(StatusCode.Corruption, db.BackgroundError?.Code);
        Assert.AreEqual("h", GetString(db, "held"));
    }

    /// <summary>
    /// A group mixing synchronous and asynchronous writers, led by either kind, in every write mode: every writer is
    /// acknowledged and readable once it returns, and the group consumes consecutive sequence numbers, as an all-sync
    /// group does.
    /// </summary>
    [TestMethod]
    [DataRow(WriteMode.Default, "async,sync,async,sync")]
    [DataRow(WriteMode.Default, "sync,async,sync,async")]
    [DataRow(WriteMode.SerialInserts, "async,sync,async,sync")]
    [DataRow(WriteMode.Pipelined, "async,sync,sync,async")]
    [DataRow(WriteMode.Pipelined, "sync,async,async,sync")]
    [DataRow(WriteMode.Unordered, "async,sync,async,sync")]
    [DataRow(WriteMode.Unordered, "sync,async,sync,async")]
    [DataRow(WriteMode.TwoWriteQueues, "async,async,sync,sync")]
    [Timeout(60_000, CooperativeCancellation = true)]
    public void MixedGroupIsCommitted(WriteMode mode, string kinds)
    {
        bool[] async = Array.ConvertAll(kinds.Split(','), k => k == "async");
        string[] keys = new string[async.Length];
        for (int i = 0; i < keys.Length; i++) keys[i] = "m" + i;
        using RocksDatabase db = RocksDatabase.Open(DbPath, ModeOptions(NewFs(), mode));
        db.Put(B("before"), B("1"));
        ulong before = db.GetLatestSequenceNumber();
        (Exception?[] errors, Exception? heldError) = RunGroupBehindHeldLeader(db, keys, async, new WriteOptions());
        Assert.IsNull(heldError, heldError?.ToString());
        for (int i = 0; i < keys.Length; i++)
        {
            Assert.IsNull(errors[i], keys[i] + ": " + errors[i]);
            Assert.AreEqual("v" + i, GetString(db, keys[i]), keys[i]);
        }
        Assert.AreEqual(before + 1 + (ulong)keys.Length, db.GetLatestSequenceNumber());
    }

    // ---- cancellation of a delayed leader ---------------------------------------------------------------------------

    /// <summary>
    /// The delay phase of DBImpl::DelayWrite (writes delayed by the L0 slowdown trigger, not stopped): an asynchronous
    /// write that leads a group whose delay is due and whose CancellationToken is canceled throws
    /// OperationCanceledException (not TaskCanceledException wrapped in a RocksDbException, not an IOError), is not
    /// written, and the asynchronous and synchronous followers of its group (which did not ask for the cancellation) are
    /// written once their own delay is over. Audit #5 pinned the stop phase (CanceledStalledLeaderDoesNotFailTheFollowersOfItsGroup).
    /// </summary>
    [TestMethod]
    [DataRow(WriteMode.Default)]
    [DataRow(WriteMode.Pipelined)]
    [DataRow(WriteMode.Unordered)]
    [DataRow(WriteMode.TwoWriteQueues)]
    [Timeout(60_000, CooperativeCancellation = true)]
    public void CanceledDelayedLeaderFailsAloneWithOperationCanceled(WriteMode mode)
    {
        MemoryFileSystem mfs = NewFs();
        DbOptions o = ModeOptions(mfs, mode);
        o.DelayedWriteRate = 64 * 1024;
        // Two L0 files delay writes (RecalculateWriteStallConditions; the L0 triggers apply with auto compactions only):
        // the compaction that would relieve them is held at its start until the end of the test.
        var cfo = new ColumnFamilyOptions { Level0FileNumCompactionTrigger = 2, Level0SlowdownWritesTrigger = 2, Level0StopWritesTrigger = 100 };
        using var releaseCompaction = new ManualResetEventSlim();
        TestHooks.Set("DBImpl::RunCompaction:Start", () => releaseCompaction.Wait());
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, cfo);
        try
        {
            RunCanceledDelayedLeader(db);
        }
        finally
        {
            releaseCompaction.Set();
        }
    }

    private static void RunCanceledDelayedLeader(RocksDatabase db)
    {
        for (int i = 0; i < 2; i++)
        {
            db.Put(B("l0-" + i), B("x"));
            db.Flush();
        }
        using var queued = new SemaphoreSlim(0);
        using var hold = new HoldAfterWal();
        using var cts = new CancellationTokenSource();
        var held = new Thread(() =>
        {
            using var batch = new WriteBatch();
            batch.Put(B("held"), B("h"));
            db.WriteWithCallback(null, batch, hold);
        });
        held.Start();
        Assert.IsTrue(hold.InWal.Wait(TimeSpan.FromSeconds(20)));
        TestHooks.Set(JoinWaitHook, () => queued.Release());
        // The group: the canceled leader (16 KiB: a delay of about 250 ms at 64 KiB/s), an async and a sync follower.
        Task leader = db.PutAsync(B("leader"), new byte[16 * 1024], cts.Token).AsTask();
        Assert.IsTrue(queued.Wait(TimeSpan.FromSeconds(20)));
        Task follower = db.PutAsync(B("follower"), B("f")).AsTask();
        Assert.IsTrue(queued.Wait(TimeSpan.FromSeconds(20)));
        Exception? syncError = null;
        var syncFollower = new Thread(() =>
        {
            try { db.Put(B("sync"), B("s")); }
            catch (Exception e) { syncError = e; }
        });
        syncFollower.Start();
        Assert.IsTrue(queued.Wait(TimeSpan.FromSeconds(20)));
        TestHooks.Set(JoinWaitHook, null);
        cts.Cancel();
        hold.Release.Set();
        Assert.IsTrue(held.Join(TimeSpan.FromSeconds(20)));
        Assert.IsTrue(((IAsyncResult)leader).AsyncWaitHandle.WaitOne(TimeSpan.FromSeconds(20)), "the canceled leader hangs");
        Assert.IsTrue(leader.IsCanceled, leader.Exception?.ToString() ?? "the canceled leader was written");
        Assert.IsTrue(((IAsyncResult)follower).AsyncWaitHandle.WaitOne(TimeSpan.FromSeconds(20)), "the follower hangs");
        Assert.IsNull(follower.Exception, follower.Exception?.ToString());
        Assert.IsTrue(syncFollower.Join(TimeSpan.FromSeconds(20)), "the sync follower hangs");
        Assert.IsNull(syncError, syncError?.ToString());
        Assert.IsNull(db.Get(B("leader")));
        Assert.AreEqual("f", GetString(db, "follower"));
        Assert.AreEqual("s", GetString(db, "sync"));
        Assert.IsNull(db.BackgroundError);
    }

    // ---- a throwing UserWriteCallback ----------------------------------------------------------------------------

    /// <summary>The callback's own exception type, so that the test can tell it from anything the engine throws.</summary>
    private sealed class CallbackFailure(string message) : Exception(message);

    /// <summary>Throws from <see cref="OnWriteEnqueued"/> or from <see cref="OnWalWriteFinish"/>.</summary>
    private sealed class ThrowingCallback(bool onEnqueued) : UserWriteCallback
    {
        public override void OnWriteEnqueued()
        {
            if (onEnqueued) throw new CallbackFailure("OnWriteEnqueued failed");
        }

        public override void OnWalWriteFinish()
        {
            if (!onEnqueued) throw new CallbackFailure("OnWalWriteFinish failed");
        }
    }

    public static IEnumerable<object[]> ThrowingCallbackCases()
    {
        foreach (WriteMode mode in new[] { WriteMode.Default, WriteMode.SerialInserts, WriteMode.Pipelined, WriteMode.Unordered, WriteMode.TwoWriteQueues })
        {
            foreach (bool onEnqueued in new[] { true, false })
            {
                foreach (bool leader in new[] { true, false })
                {
                    foreach (bool async in new[] { true, false }) yield return [mode, onEnqueued, leader, async];
                }
            }
        }
    }

    /// <summary>
    /// A UserWriteCallback that throws fails safely (upstream: undefined behaviour; write_thread.cc JoinBatchGroup calls
    /// CheckWriteEnqueuedCallback after LinkOne without a try/catch). The throwing write (sync or async, leading its
    /// write group or following an asynchronous / synchronous writer of the other kind) gets the callback's exception
    /// unchanged; the other writer of its group is acknowledged and readable; the write queue is not stranded; the DB has
    /// a hard background error, so later writes (sync and async) fail with it until Resume(), after which writes work;
    /// and the DB closes. The port leaked the exception out of JoinBatchGroup while the writer was linked (or leading):
    /// every later write and the close waited forever; an OnWalWriteFinish that threw failed every writer of the group
    /// after its WAL record was written.
    /// </summary>
    [TestMethod]
    [DynamicData(nameof(ThrowingCallbackCases))]
    [Timeout(60_000, CooperativeCancellation = true)]
    public void ThrowingUserWriteCallbackFailsSafely(WriteMode mode, bool onEnqueued, bool leader, bool async)
    {
        RocksDatabase db = RocksDatabase.Open(DbPath, ModeOptions(NewFs(), mode));
        try
        {
            db.Put(B("before"), B("1"));
            // The throwing write leads the next group (first behind the held write) or follows a writer of the other kind.
            string[] keys = leader ? ["thrower", "other"] : ["other", "thrower"];
            int thrower = leader ? 0 : 1;
            bool[] kinds = new bool[2];
            kinds[thrower] = async;
            kinds[1 - thrower] = !async;
            var callbacks = new UserWriteCallback?[2];
            callbacks[thrower] = new ThrowingCallback(onEnqueued);
            callbacks[1 - thrower] = new NoOpUserCallback();
            (Exception?[] errors, Exception? heldError) = RunGroupBehindHeldLeader(db, keys, kinds, new WriteOptions(), callbacks);
            Assert.IsNull(heldError, heldError?.ToString());
            Assert.IsInstanceOfType<CallbackFailure>(errors[thrower], errors[thrower]?.ToString() ?? "the throwing write succeeded");
            Assert.AreEqual(onEnqueued ? "OnWriteEnqueued failed" : "OnWalWriteFinish failed", errors[thrower]!.Message);
            Assert.IsNull(errors[1 - thrower], errors[1 - thrower]?.ToString());
            Assert.AreEqual("v" + (1 - thrower), GetString(db, keys[1 - thrower]));
            Assert.AreEqual("h", GetString(db, "held"));

            // Writes stop with the hard background error until Resume.
            RocksDbException? bg = db.BackgroundError;
            Assert.IsNotNull(bg);
            Assert.AreEqual(StatusCode.Aborted, bg.Code);
            Assert.AreEqual(ErrorSeverity.HardError, bg.Severity);
            var syncLater = Assert.ThrowsExactly<RocksDbException>(() => db.Put(B("later"), B("x")));
            Assert.AreEqual(StatusCode.Aborted, syncLater.Code);
            Task asyncLater = db.PutAsync(B("later"), B("y")).AsTask();
            Assert.IsTrue(((IAsyncResult)asyncLater).AsyncWaitHandle.WaitOne(TimeSpan.FromSeconds(20)), "a later write hangs");
            Assert.IsInstanceOfType<RocksDbException>(asyncLater.Exception?.InnerException, asyncLater.Exception?.ToString() ?? "a later write succeeded");
            Assert.AreEqual(StatusCode.Aborted, ((RocksDbException)asyncLater.Exception!.InnerException!).Code);
            Assert.IsNull(db.Get(B("later")));

            db.Resume();
            Assert.IsNull(db.BackgroundError);
            db.Put(B("after"), B("a"));
            Task asyncAfter = db.PutAsync(B("after2"), B("b")).AsTask();
            Assert.IsTrue(asyncAfter.Wait(TimeSpan.FromSeconds(20)), "a write after Resume hangs");
            Assert.AreEqual("a", GetString(db, "after"));
            Assert.AreEqual("b", GetString(db, "after2"));
        }
        finally
        {
            // Close needs the write queue: a stranded queue must fail the test, not hang the run.
            Task close = Task.Run(db.Dispose);
            Assert.IsTrue(close.Wait(TimeSpan.FromSeconds(20)), "close waits for a stranded write queue");
        }
    }

    // ---- a single-threaded synchronization context ---------------------------------------------------------------

    /// <summary>Runs posted callbacks on one dedicated thread (a UI-like context).</summary>
    private sealed class SingleThreadContext : SynchronizationContext, IDisposable
    {
        private readonly BlockingCollection<(SendOrPostCallback, object?)> _queue = new();
        public readonly Thread Thread;

        public SingleThreadContext()
        {
            Thread = new Thread(() =>
            {
                SetSynchronizationContext(this);
                foreach ((SendOrPostCallback d, object? s) in _queue.GetConsumingEnumerable()) d(s);
            }) { IsBackground = true };
            Thread.Start();
        }

        public override void Post(SendOrPostCallback d, object? state) => _queue.Add((d, state));

        public override void Send(SendOrPostCallback d, object? state) => throw new NotSupportedException();

        public void Dispose() => _queue.CompleteAdding();
    }

    /// <summary>
    /// No sync-over-async that needs the caller's context: on a single-threaded synchronization context (whose thread is
    /// the only one that can run the caller's continuations), many concurrent asynchronous writes awaited with the
    /// context captured, interleaved with synchronous writes, flushes and FlushWAL on that same thread (which then block
    /// it while asynchronous leaders on the thread pool commit their groups), all complete; every engine-internal await
    /// resumes off the context (ConfigureAwait(false)), on a file system whose async members really complete
    /// asynchronously.
    /// </summary>
    [TestMethod]
    [DataRow(WriteMode.Default)]
    [DataRow(WriteMode.Pipelined)]
    [DataRow(WriteMode.Unordered)]
    [DataRow(WriteMode.TwoWriteQueues)]
    [Timeout(60_000, CooperativeCancellation = true)]
    public void WritesOnASingleThreadedContextDoNotDeadlock(WriteMode mode)
    {
        var fs = new ForcedAsyncFileSystem(NewFs());
        DbOptions o = ModeOptions(fs, mode);
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, new ColumnFamilyOptions { WriteBufferSize = 32 << 10 });
        using var context = new SingleThreadContext();
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        context.Post(_ => RunAsync().ContinueWith(t =>
        {
            if (t.IsFaulted) done.TrySetException(t.Exception!.InnerExceptions);
            else done.TrySetResult();
        }, TaskScheduler.Default), null);
        Assert.IsTrue(done.Task.Wait(TimeSpan.FromSeconds(40)), "writes on a single-threaded context deadlock");
        for (int i = 0; i < 400; i++) Assert.AreEqual("v" + i, GetString(db, "c" + i), "c" + i);
        AssertNoViolations(fs);

        async Task RunAsync()
        {
            var pending = new List<Task>();
            for (int i = 0; i < 400; i++)
            {
                var wo = new WriteOptions { Sync = i % 7 == 0 };
                if (i % 5 == 4)
                {
                    // A synchronous call on the context thread while asynchronous writes are in flight.
                    ForcedAsyncFileSystem.Sync(() =>
                    {
                        db.Put(wo, B("c" + i), B("v" + i));
                        if (i % 50 == 9) db.Flush();
                        if (i % 50 == 29) db.FlushWal(sync: true);
                    });
                }
                else
                {
                    pending.Add(AwaitOnContext(db.PutAsync(wo, B("c" + i), B("v" + i))));
                }
            }
            await Task.WhenAll(pending);
        }

        async Task AwaitOnContext(ValueTask write)
        {
            await write; // the continuation is posted to the context
            Assert.AreSame(context, SynchronizationContext.Current);
        }
    }

    // ---- close while writes are stalled by the write buffer manager -----------------------------------------------

    private const long Mib = 1024 * 1024;
    private const string WbmStallWaitHook = "DBImpl::WriteBufferManagerStallWrites:Wait";

    /// <summary>
    /// Close while writes are stalled by a shared write buffer manager whose memory is held elsewhere (another DB, a
    /// reservation). DBImpl::DelayWrite stops waiting once shutting_down_ is set, and the port's close announces itself
    /// before it enters the write queue, so that a stalled leader does not keep it out (Audit5WritePathTests
    /// .CloseReleasesAWriteStalledByStoppedWrites). The write buffer manager stall (WriteBufferManagerStallWrites) must
    /// behave the same: close returns, the stalled write and the one queued behind it fail with ObjectDisposedException
    /// and are not written. The port's stall loop only looked at the disposed / shutting-down flags, which close sets
    /// once it leads the write queue: close released the first stalled write (RemoveDBFromQueue signals it), the write
    /// queued behind it took the leadership and stalled again, and close waited for it until the memory was freed.
    /// </summary>
    [TestMethod]
    [DataRow(WriteBufferFlushPolicy.FlushOldest, false)]
    [DataRow(WriteBufferFlushPolicy.FlushOldest, true)]
    [DataRow(WriteBufferFlushPolicy.FlushLargestAcrossDbs, false)]
    [DataRow(WriteBufferFlushPolicy.FlushLargestAcrossDbs, true)]
    [Timeout(60_000, CooperativeCancellation = true)]
    public void CloseReleasesWritesStalledByTheWriteBufferManager(WriteBufferFlushPolicy policy, bool syncWriters)
    {
        MemoryFileSystem mfs = NewFs();
        var wbm = new WriteBufferManager(64 * Mib, null, allowStall: true, policy);
        DbOptions o = NewOptions(mfs);
        o.WriteBufferManager = wbm;
        using var stalls = new SemaphoreSlim(0);
        using var queued = new SemaphoreSlim(0);
        RocksDatabase db = RocksDatabase.Open(DbPath, o);
        Thread? closer = null;
        bool reserved = false;
        var writes = new List<Func<Exception?>>();
        try
        {
            db.Put(B("a"), B("1"));
            wbm.ReserveMem(128 * Mib); // memory held elsewhere: every write group stalls
            reserved = true;
            TestHooks.Set(WbmStallWaitHook, () => stalls.Release());
            TestHooks.Set(JoinWaitHook, () => queued.Release());
            foreach (string key in new[] { "stalled", "behind" })
            {
                if (syncWriters)
                {
                    Exception? error = null;
                    var t = new Thread(() =>
                    {
                        try { db.Put(B(key), B("x")); }
                        catch (Exception e) { error = e; }
                    });
                    t.Start();
                    writes.Add(() => t.Join(TimeSpan.FromSeconds(20)) ? error : new TimeoutException(key + " hangs"));
                }
                else
                {
                    Task task = db.PutAsync(B(key), B("x")).AsTask();
                    writes.Add(() =>
                    {
                        if (!((IAsyncResult)task).AsyncWaitHandle.WaitOne(TimeSpan.FromSeconds(20))) return new TimeoutException(key + " hangs");
                        return task.Exception?.InnerException;
                    });
                }
                Assert.IsTrue((key == "stalled" ? stalls : queued).Wait(TimeSpan.FromSeconds(20)), key + " did not reach its wait");
            }
            closer = new Thread(db.Dispose);
            closer.Start();
            Assert.IsTrue(closer.Join(TimeSpan.FromSeconds(10)), "close waits for the write buffer manager stall to end");
            foreach (Func<Exception?> w in writes)
            {
                Exception? e = w();
                Assert.IsInstanceOfType<ObjectDisposedException>(e, e?.ToString() ?? "acknowledged during the close");
            }
        }
        finally
        {
            if (reserved) wbm.FreeMem(128 * Mib);
            closer?.Join();
            foreach (Func<Exception?> w in writes) w();
            db.Dispose();
        }
        TestHooks.ClearAll();
        using RocksDatabase reopened = RocksDatabase.Open(DbPath, NewOptions(mfs));
        Assert.AreEqual("1", GetString(reopened, "a"));
        Assert.IsNull(reopened.Get(B("stalled")));
        Assert.IsNull(reopened.Get(B("behind")));
    }

    private sealed class NoOpUserCallback : UserWriteCallback
    {
        public override void OnWriteEnqueued() { }
        public override void OnWalWriteFinish() { }
    }
}
