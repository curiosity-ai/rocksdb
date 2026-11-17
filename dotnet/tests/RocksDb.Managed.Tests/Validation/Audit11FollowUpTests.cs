using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #11 follow-ups decided by the maintainers:
/// <list type="bullet">
/// <item>A: GetUpdatesSince ends a run that stopped below the DB's last sequence with TryAgain like native
/// (WalIteratorImpl::NextImpl, db/wal_iterator_impl.cc), keeps following WAL rotations, and lets a later Next retry;
/// a damaged record that keeps a tail below the last sequence is reported (SkippedCorruptBytes) and ends
/// TailUpdatesAsync with NotFound instead of a silent stall.</item>
/// <item>B: compaction input readahead (and the implicit auto readahead) without IRandomAccessFile.Prefetch reads
/// synchronously on the reading thread (BlockPrefetcher: num_buffers = async_io ? 2 : 1), so a compaction thread or a
/// sync CompactRange never waits for a ReadAsync continuation on the .NET thread pool.</item>
/// </list>
/// </summary>
[TestClass]
public sealed class Audit11FollowUpTests
{
    /// <summary>Status::TryAgain("Create a new iterator to fetch the new tail.").ToString() (db/wal_iterator_impl.cc).</summary>
    private const string TryAgainMessage = "Operation failed. Try again.: Create a new iterator to fetch the new tail.";

    private static readonly TimeSpan Deadline = TimeSpan.FromSeconds(20);
    private static readonly WriteOptions NoWal = new() { DisableWal = true };

    private static StatusCode CodeOf(Action action)
    {
        try
        {
            action();
        }
        catch (RocksDbException e)
        {
            return e.Code;
        }
        return StatusCode.Ok;
    }

    private static List<ulong> ReadAll(TransactionLogIterator it)
    {
        var seqs = new List<ulong>();
        for (; it.Valid; it.Next()) seqs.Add(it.Current.Sequence);
        return seqs;
    }

    private sealed class TempDirectory : IDisposable
    {
        public string Path { get; } = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "a11fu-" + Guid.NewGuid().ToString("N"));

        public void Dispose()
        {
            try
            {
                if (Directory.Exists(Path)) Directory.Delete(Path, true);
            }
            catch (IOException)
            {
            }
        }
    }

    /// <summary>
    /// A local DB with keys 1..<paramref name="good"/> in the live WAL followed by one more batch whose record payload is
    /// damaged in place (checksum mismatch), while the DB keeps running: the DB's last sequence is good + 1.
    /// </summary>
    private static RocksDatabase OpenWithDamagedTailRecord(string path, int good)
    {
        RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true });
        for (int i = 1; i <= good; i++) db.Put(Key(i), Value(i, 0));
        WalFileInfo wal = db.GetSortedWalFiles()[^1];
        string walPath = path + wal.PathName;
        long before = new FileInfo(walPath).Length;
        db.Put(Key(good + 1), Value(good + 1, 0));
        long after = new FileInfo(walPath).Length;
        Assert.IsTrue(after > before + 20, "the last batch was appended to " + walPath);
        using (var f = new FileStream(walPath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete))
        {
            // 7-byte record header, 12-byte batch header, then the record: flip a byte of the key.
            f.Position = before + 7 + 12 + 3;
            int b = f.ReadByte();
            f.Position = before + 7 + 12 + 3;
            f.WriteByte((byte)(b ^ 0x40));
        }
        Assert.AreEqual((ulong)good + 1, db.GetLatestSequenceNumber());
        return db;
    }

    // ---------------------------------------------------------------------------------------------------------
    // A. GetUpdatesSince: TryAgain like native.

    /// <summary>
    /// WalIteratorImpl::NextImpl: "current_last_seq_ == versions_-&gt;LastSequence()" ends with OK (caught up), anything
    /// else (here a write with disableWAL at the end of the log) with TryAgain("Create a new iterator to fetch the new
    /// tail."). The managed iterator ended with OK in both cases. Unlike native, Next retries after TryAgain (still
    /// TryAgain while nothing new is readable); a later logged write is a gap (NotFound, WalIteratorImpl::
    /// UpdateCurrentWriteBatch), which ends the run for good.
    /// </summary>
    [TestMethod]
    public async Task IteratorStoppedBelowLastSequenceEndsWithTryAgain()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        db.Put(B("a"), B("1"));
        using (TransactionLogIterator caughtUp = db.GetUpdatesSince(1))
        {
            CollectionAssert.AreEqual(new ulong[] { 1 }, ReadAll(caughtUp));
            Assert.IsNull(caughtUp.Error, "caught up is OK");
        }

        db.Put(NoWal, B("b"), B("2"));
        using TransactionLogIterator it = db.GetUpdatesSince(1);
        CollectionAssert.AreEqual(new ulong[] { 1 }, ReadAll(it));
        Assert.IsNotNull(it.Error);
        Assert.AreEqual(StatusCode.TryAgain, it.Error.Code);
        Assert.AreEqual(TryAgainMessage, it.Error.Message);
        Assert.AreEqual(0, it.SkippedCorruptBytes, "unlogged writes drop no WAL data");
        var thrown = Assert.ThrowsExactly<RocksDbException>(it.ThrowIfError);
        Assert.AreEqual(StatusCode.TryAgain, thrown.Code);
        Assert.AreEqual(2UL, it.NextSequence);

        // Retrying finds nothing new: TryAgain again (native: the iterator is spent, Next does nothing).
        it.Next();
        Assert.IsFalse(it.Valid);
        Assert.AreEqual(StatusCode.TryAgain, it.Error?.Code);

        // The enumerator lists what is readable and does not throw for TryAgain.
        using (TransactionLogIterator e = db.GetUpdatesSince(1))
        {
            int n = 0;
            await foreach (WalBatch _ in e) n++;
            Assert.AreEqual(1, n);
            Assert.AreEqual(StatusCode.TryAgain, e.Error?.Code);
        }

        // A logged write after the unlogged one: the next batch does not follow on, the run ends with NotFound.
        db.Put(B("c"), B("3"));
        it.Next();
        Assert.IsFalse(it.Valid);
        Assert.AreEqual(StatusCode.NotFound, it.Error?.Code);
        it.Next();
        Assert.AreEqual(StatusCode.NotFound, it.Error?.Code, "a gap ends the run for good");
    }

    /// <summary>
    /// The start sequence is published but not in the WAL (WalIteratorImpl::NextImpl with !started_: SeekToStartSequence
    /// finds nothing, then the read loop ends with TryAgain). The constructor's seek alone reports no error, like native.
    /// </summary>
    [TestMethod]
    public void UnstartedIteratorNextIsTryAgain()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        db.Put(B("a"), B("1"));
        db.Put(NoWal, B("b"), B("2"));
        using TransactionLogIterator it = db.GetUpdatesSince(2);
        Assert.IsFalse(it.Valid);
        Assert.IsNull(it.Error);
        it.Next();
        Assert.IsFalse(it.Valid);
        Assert.AreEqual(StatusCode.TryAgain, it.Error?.Code);
        Assert.AreEqual(TryAgainMessage, it.Error?.Message);
    }

    /// <summary>
    /// manual_wal_flush: a published write whose record is still in the WAL writer's buffer (DBImpl::FlushWAL writes it).
    /// The iterator ends with TryAgain meanwhile; Next after FlushWal reads it (managed: a TryAgain run is resumable).
    /// </summary>
    [TestMethod]
    public void TryAgainRunResumesOnNextOnceManualWalFlushWritesTheRecord()
    {
        // LocalFileSystem: its writable file buffers what FlushWal has not written (MemoryFileSystem appends directly).
        using var dir = new TempDirectory();
        using RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions { CreateIfMissing = true, ManualWalFlush = true });
        db.Put(B("a"), B("1"));
        db.FlushWal(sync: false);
        using TransactionLogIterator it = db.GetUpdatesSince(1);
        CollectionAssert.AreEqual(new ulong[] { 1 }, ReadAll(it));
        Assert.IsNull(it.Error);

        db.Put(B("b"), B("2"));
        it.Next();
        Assert.IsFalse(it.Valid);
        Assert.AreEqual(StatusCode.TryAgain, it.Error?.Code, "published, still buffered");
        db.FlushWal(sync: false);
        it.Next();
        Assert.IsTrue(it.Valid, it.Error?.Message);
        Assert.AreEqual(2UL, it.Current.Sequence);
        Assert.IsNull(it.Error);
    }

    /// <summary>
    /// A damaged live WAL record below the DB's last sequence: the log reader drops it (log::Reader "checksum mismatch",
    /// reported to the iterator's LogReporter, which upstream only logs) and the iterator ends with TryAgain. The managed
    /// iterator tells this apart from unlogged writes through SkippedCorruptBytes.
    /// </summary>
    [TestMethod]
    public void DamagedTailRecordIsTryAgainWithSkippedCorruptBytes()
    {
        using var dir = new TempDirectory();
        using RocksDatabase db = OpenWithDamagedTailRecord(dir.Path, good: 3);
        using TransactionLogIterator it = db.GetUpdatesSince(1);
        CollectionAssert.AreEqual(new ulong[] { 1, 2, 3 }, ReadAll(it));
        Assert.AreEqual(StatusCode.TryAgain, it.Error?.Code);
        Assert.IsTrue(it.SkippedCorruptBytes > 0);
        StringAssert.Contains(it.SkippedCorruptionReason, "checksum mismatch");
        Assert.AreEqual(db.GetSortedWalFiles()[^1].LogNumber, it.CurrentWalNumber);
    }

    /// <summary>
    /// TailUpdatesAsync (MoveNextTailingAsync) used to wait forever behind a damaged record below the last sequence. It
    /// now ends with NotFound once a second read finds the same damage at the same position.
    /// </summary>
    [TestMethod]
    public async Task TailUpdatesEndsWithNotFoundAtADamagedTailRecord()
    {
        using var dir = new TempDirectory();
        await using RocksDatabase db = OpenWithDamagedTailRecord(dir.Path, good: 3);
        using var cts = new CancellationTokenSource(Deadline);
        var seqs = new List<ulong>();
        RocksDbException? error = null;
        try
        {
            await foreach (WalBatch b in db.TailUpdatesAsync(1, cts.Token)) seqs.Add(b.Sequence);
        }
        catch (RocksDbException e)
        {
            error = e;
        }
        CollectionAssert.AreEqual(new ulong[] { 1, 2, 3 }, seqs);
        Assert.IsNotNull(error, "the tail ended without an error");
        Assert.AreEqual(StatusCode.NotFound, error.Code);
        StringAssert.Contains(error.Message, "sequence 4 is damaged");
    }

    /// <summary>
    /// TailUpdatesAsync waits out sequence numbers that are not in the WAL at the end of the log (TryAgain) instead of
    /// ending - disableWAL writes and an ingested file - and continues with data a manual WAL flush writes later.
    /// </summary>
    [TestMethod]
    public async Task TailUpdatesWaitsOutUnloggedWritesAndContinuesAfterAManualWalFlush()
    {
        using var dir = new TempDirectory();
        await using RocksDatabase db = await RocksDatabase.OpenAsync(dir.Path, new DbOptions { CreateIfMissing = true, ManualWalFlush = true });
        db.Put(B("a"), B("1"));
        db.FlushWal(sync: false);
        using var cts = new CancellationTokenSource(Deadline);
        await using IAsyncEnumerator<WalBatch> tail = db.TailUpdatesAsync(1, cts.Token).GetAsyncEnumerator(cts.Token);
        Assert.IsTrue(await tail.MoveNextAsync());
        Assert.AreEqual(1UL, tail.Current.Sequence);

        // Unlogged write, then an ingested file that overlaps the memtable (it takes a sequence number), then a buffered
        // record: the tail keeps waiting.
        db.Put(NoWal, B("b"), B("2"));
        string sst = Path.Combine(dir.Path, "ext.sst");
        using (var w = new SstFileWriter(new ColumnFamilyOptions()))
        {
            w.Open(sst);
            w.Put(B("a"), B("ingested"));
            w.Finish();
        }
        db.IngestExternalFiles([sst], new IngestExternalFileOptions());
        Assert.AreEqual(3UL, db.GetLatestSequenceNumber(), "precondition: the ingestion took sequence 3");
        db.Put(B("c"), B("4"));
        ValueTask<bool> next = tail.MoveNextAsync();
        await Task.Delay(100);
        Assert.IsFalse(next.IsCompleted, "the tail must wait, not end, while the WAL is behind the DB");

        // FlushWAL writes the buffered record: it follows a gap (2, 3), which ends the tail with NotFound.
        db.FlushWal(sync: false);
        var e = await Assert.ThrowsExactlyAsync<RocksDbException>(async () => await next);
        Assert.AreEqual(StatusCode.NotFound, e.Code);
        StringAssert.Contains(e.Message, "Gap in sequence numbers");
    }

    // ---------------------------------------------------------------------------------------------------------
    // B. Readahead without Prefetch support does not wait on the .NET thread pool.

    /// <summary>A MemoryFileSystem whose ReadAsync completes on the .NET thread pool (like RandomAccess.ReadAsync on
    /// macOS) and is counted; no Prefetch support (IRandomAccessFile.Prefetch default).</summary>
    private sealed class PoolReadFileSystem(MemoryFileSystem inner) : IFileSystem
    {
        private long _readAsyncCalls;
        public long ReadAsyncCalls => Interlocked.Read(ref _readAsyncCalls);

        public IWritableFile CreateWritable(string path, bool truncate = true) => inner.CreateWritable(path, truncate);
        public IWritableFile CreateWritable(string path, bool truncate, WritableFileOptions options) => inner.CreateWritable(path, truncate, options);
        public IWritableFile ReopenWritable(string path, WritableFileOptions options) => inner.ReopenWritable(path, options);
        public IRandomAccessFile OpenRandomAccess(string path) => new R(this, inner.OpenRandomAccess(path));
        public IRandomAccessFile OpenRandomAccess(string path, RandomAccessFileOptions options) => new R(this, inner.OpenRandomAccess(path, options));
        public ISequentialFile OpenSequential(string path) => inner.OpenSequential(path);
        public ISequentialFile OpenSequential(string path, SequentialFileOptions options) => inner.OpenSequential(path, options);
        public bool FileExists(string path) => inner.FileExists(path);
        public bool DirectoryExists(string path) => inner.DirectoryExists(path);
        public long GetFileSize(string path) => inner.GetFileSize(path);
        public void DeleteFile(string path) => inner.DeleteFile(path);
        public void RenameFile(string source, string target) => inner.RenameFile(source, target);
        public void CreateDirectoryIfMissing(string path) => inner.CreateDirectoryIfMissing(path);
        public void DeleteDirectory(string path) => inner.DeleteDirectory(path);
        public IEnumerable<string> GetChildren(string directory) => inner.GetChildren(directory);
        public IDisposable LockFile(string path) => inner.LockFile(path);
        public void SyncDirectory(string path) => inner.SyncDirectory(path);
        public void LinkFile(string source, string target) => inner.LinkFile(source, target);
        public DateTimeOffset GetFileModificationTime(string path) => inner.GetFileModificationTime(path);

        private sealed class R(PoolReadFileSystem fs, IRandomAccessFile file) : IRandomAccessFile
        {
            public long Length => file.Length;
            public int Read(long offset, Span<byte> destination) => file.Read(offset, destination);

            public ValueTask<int> ReadAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken = default)
            {
                Interlocked.Increment(ref fs._readAsyncCalls);
                return new ValueTask<int>(Task.Run(() => file.Read(offset, destination.Span), cancellationToken));
            }

            public void Dispose() => file.Dispose();
        }
    }

    /// <summary>
    /// Caps the .NET thread pool at the processor count and blocks every worker until disposed (as
    /// Audit8AsyncBackgroundIoTests.DeadThreadPool): no thread-pool work item runs meanwhile.
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

    /// <summary>Runs <paramref name="body"/> on a dedicated (non thread-pool) thread and waits for it with the deadline.</summary>
    private static void OnDedicatedThread(Action body, string what)
    {
        Exception? error = null;
        var t = new Thread(() =>
        {
            try { body(); }
            catch (Exception e) { error = e; }
        }) { IsBackground = true, Name = "audit11-sync" };
        t.Start();
        if (!t.Join(Deadline)) Assert.Fail(what + " made no progress within " + Deadline);
        if (error is not null) System.Runtime.ExceptionServices.ExceptionDispatchInfo.Throw(error);
    }

    private static void FillFile(RocksDatabase db, Random rnd, int file)
    {
        for (int i = 0; i < 1500; i++)
        {
            byte[] v = new byte[400];
            rnd.NextBytes(v);
            db.Put(Key(i * 3 + file), v);
        }
        db.Flush();
    }

    /// <summary>
    /// BlockPrefetcher::PrefetchIfNeeded (table/block_based/block_prefetcher.cc): without FS prefetch support a
    /// compaction reads its inputs through a FilePrefetchBuffer with num_buffers = is_async_io_prefetch ? 2 : 1, and
    /// compaction reads without async_io - synchronous reads on the compaction's own thread. Here every ReadAsync of the
    /// file system needs a pool thread and the pool is dead: an automatic compaction (LOW thread) with
    /// paranoid_file_checks (VerifyOutputFiles reads the output back with the implicit auto readahead), a sync
    /// CompactRange and a sync full scan (auto readahead) complete, and none of them calls ReadAsync. Before the fix the
    /// compaction input buffer prefetched the next chunk with ReadAsync and blocked until its continuation ran.
    /// </summary>
    [TestMethod]
    [DoNotParallelize]
    public void CompactionAndSyncScansDoNotWaitForPoolReadsWithoutFilePrefetch()
    {
        var fs = new PoolReadFileSystem(NewFs());
        DbOptions o = NewOptions(fs);
        o.CompactionReadaheadSize = 64 << 10;
        var cf = new ColumnFamilyOptions { Level0FileNumCompactionTrigger = 3, ParanoidFileChecks = true };
        using (new DeadThreadPool())
        {
            OnDedicatedThread(() =>
            {
                using RocksDatabase db = RocksDatabase.Open(DbPath, o, cf);
                var rnd = new Random(11);
                for (int f = 0; f < 3; f++) FillFile(db, rnd, f);
                db.WaitForCompact();
                Assert.AreEqual(0, db.GetLiveFilesMetadata().Count(m => m.Level == 0), "the automatic compaction ran");
                FillFile(db, rnd, 1);
                db.CompactRange();
                int n = 0;
                using (Iterator it = db.NewIterator(new ReadOptions { FillCache = false }))
                {
                    for (it.SeekToFirst(); it.Valid; it.Next()) n++;
                }
                Assert.AreEqual(4500, n);
            }, "compaction and scans with the thread pool exhausted");
        }
        Assert.AreEqual(0, fs.ReadAsyncCalls, "a sync read path called ReadAsync");
    }
}
