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

    // ---------------------------------------------------------------------------------------------------------
    // C. Follow-up: WAL data the tailing reader cannot get past although the writer completed it.

    /// <summary>A physical record of a WAL file: offset, header size, payload length, log number (recyclable only).</summary>
    internal readonly record struct WalRecord(long Offset, int HeaderSize, int Length, uint LogNumber);

    /// <summary>The physical records of a WAL (legacy and recyclable headers, log::Writer block layout).</summary>
    internal static List<WalRecord> WalRecords(byte[] log)
    {
        const int Block = 32768;
        var list = new List<WalRecord>();
        long off = 0;
        while (off + 7 <= log.Length)
        {
            int inBlock = (int)(off % Block);
            int type = log[off + 6];
            int length = log[off + 4] | (log[off + 5] << 8);
            bool recyclable = type is >= 5 and <= 8 or 11 or 131;
            int header = recyclable ? 11 : 7;
            if (Block - inBlock < header)
            {
                off += Block - inBlock;
                continue;
            }
            if (type == 0 && length == 0) break;
            uint logNumber = recyclable && off + 11 <= log.Length ? System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(log.AsSpan((int)off + 7)) : 0;
            list.Add(new WalRecord(off, header, length, logNumber));
            off += header + length;
        }
        return list;
    }

    /// <summary>The batch records of this incarnation of a WAL (a recycled WAL's old records carry another log number).</summary>
    internal static List<WalRecord> LiveRecords(string walPath, ulong logNumber)
    {
        byte[] bytes;
        using (var f = new FileStream(walPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete))
        {
            bytes = new byte[f.Length];
            f.ReadExactly(bytes);
        }
        var live = new List<WalRecord>();
        List<WalRecord> all = WalRecords(bytes);
        bool recycled = all.Count > 0 && all[0].HeaderSize == 11;
        foreach (WalRecord r in all)
        {
            if (recycled ? r.HeaderSize == 11 && r.LogNumber == unchecked((uint)logNumber) : r.HeaderSize == 7) live.Add(r);
            else break;
        }
        return live;
    }

    /// <summary>Flips a payload byte of a record in place (past the 12-byte batch header): checksum mismatch.</summary>
    internal static void FlipPayloadByte(string walPath, WalRecord r)
    {
        using var f = new FileStream(walPath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete);
        f.Position = r.Offset + r.HeaderSize + 12 + 3;
        int b = f.ReadByte();
        f.Position = r.Offset + r.HeaderSize + 12 + 3;
        f.WriteByte((byte)(b ^ 0x40));
    }

    /// <summary>Makes a record's length field claim <paramref name="extra"/> more bytes than the file holds after it.</summary>
    internal static void GrowRecordLength(string walPath, WalRecord r, int extra)
    {
        Assert.IsTrue(r.Offset % 32768 + r.HeaderSize + r.Length + extra < 32768, "the grown record stays inside its block");
        using var f = new FileStream(walPath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete);
        int length = r.Length + extra;
        f.Position = r.Offset + 4;
        f.WriteByte((byte)length);
        f.WriteByte((byte)(length >> 8));
    }

    private static string CurrentWalPath(RocksDatabase db, string path) => Path.Combine(path, FileNames.LogFileName(db.CurrentWalNumber));

    /// <summary>
    /// A DB whose current WAL is a recycled file (recycle_log_file_num = 2, two flushed rounds of
    /// <paramref name="oldPuts"/> writes each): its bytes past the live end are records of the file's previous use.
    /// </summary>
    internal static RocksDatabase OpenWithRecycledWal(string path, int oldPuts)
    {
        RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true, RecycleLogFileNum = 2 });
        for (int round = 0; round < 3; round++)
        {
            for (int i = 0; i < oldPuts; i++) db.Put(B("old" + i), B(new string('o', 100)));
            db.Flush();
            db.WaitForCompact();
        }
        db.Put(B("first"), B("live"));
        var recs = WalRecords(File.ReadAllBytes(CurrentWalPath(db, path)));
        Assert.AreEqual(7 + 4, recs[0].HeaderSize, "precondition: the current WAL has recyclable records");
        Assert.IsTrue(recs.Count > 1 && recs[1].LogNumber != unchecked((uint)db.CurrentWalNumber), "precondition: old records follow the live end");
        return db;
    }

    /// <summary>
    /// log::FragmentBufferedReader::ReadRecord (db/log_reader.cc): "case kBadRecordChecksum: if (recycled_) {
    /// fragments_.clear(); return false; }" - a damaged record of a recycled WAL ends the log without a report, so the
    /// records written after it are never read and native's iterator ends with TryAgain. The managed iterator compares
    /// where the reader stopped with what the WAL writer completed (DbImpl.WalWrittenEnd) and counts the rest as damaged
    /// (SkippedCorruptBytes was 0 before).
    /// </summary>
    [TestMethod]
    public void DamagedRecordOfARecycledWalIsCountedAsSkipped()
    {
        using var dir = new TempDirectory();
        using RocksDatabase db = OpenWithRecycledWal(dir.Path, 20);
        ulong first = db.GetLatestSequenceNumber();
        for (int i = 0; i < 5; i++) db.Put(Key(i), Value(i, 0));
        string wal = CurrentWalPath(db, dir.Path);
        List<WalRecord> live = LiveRecords(wal, db.CurrentWalNumber);
        Assert.IsTrue(live.Count >= 6);
        FlipPayloadByte(wal, live[^3]); // Key(2): the batches of Key(0) .. Key(4) are the last five records
        using TransactionLogIterator it = db.GetUpdatesSince(first);
        List<ulong> seqs = ReadAll(it);
        string what = "read " + string.Join(",", seqs) + " from " + first + "; records " + string.Join(" ", live) + "; " + it.Error?.Message;
        CollectionAssert.AreEqual(new[] { first, first + 1, first + 2 }, seqs, what);
        Assert.AreEqual(StatusCode.TryAgain, it.Error?.Code);
        Assert.IsTrue(it.SkippedCorruptBytes > 0, "the records behind the damaged one are not reported");
        StringAssert.Contains(it.SkippedCorruptionReason, "recycled");
    }

    /// <summary>
    /// The legitimate end of a recycled WAL - old records after the live end (log::Reader kOldRecord: "Treat a record
    /// from a previous instance of the log as EOF") - and unlogged writes after it: caught up / TryAgain without damage.
    /// </summary>
    [TestMethod]
    public void RecycledWalEndIsNotDamage()
    {
        using var dir = new TempDirectory();
        using RocksDatabase db = OpenWithRecycledWal(dir.Path, 20);
        ulong first = db.GetLatestSequenceNumber();
        db.Put(Key(1), Value(1, 0));
        using TransactionLogIterator it = db.GetUpdatesSince(first);
        CollectionAssert.AreEqual(new[] { first, first + 1 }, ReadAll(it));
        Assert.IsNull(it.Error);
        Assert.AreEqual(0, it.SkippedCorruptBytes);
        // disableWAL is refused with recycle_log_file_num; an ingested file that overlaps the memtable takes a sequence
        // number without a WAL record (and switches the WAL, which may recycle a file again).
        string sst = Path.Combine(dir.Path, "ext.sst");
        using (var w = new SstFileWriter(new ColumnFamilyOptions()))
        {
            w.Open(sst);
            w.Put(Key(1), B("ingested"));
            w.Finish();
        }
        db.IngestExternalFiles([sst], new IngestExternalFileOptions());
        Assert.AreEqual(first + 2, db.GetLatestSequenceNumber());
        it.Next();
        Assert.AreEqual(StatusCode.TryAgain, it.Error?.Code);
        Assert.AreEqual(0, it.SkippedCorruptBytes, it.SkippedCorruptionReason);
    }

    /// <summary>
    /// log::FragmentBufferedReader::TryReadFragment: "while (header_size + length &gt; buffer_.size()) { ... TryReadMore
    /// ... return false; }" - a record whose length reaches past the end of the data is an incomplete record to wait for
    /// (log::Reader reports kBadRecordLen at eof_ only in kAbsoluteConsistency / kPointInTimeRecovery, as "truncated
    /// record body"). In the live WAL, past what the writer completed, it is damage. Recovery is unchanged.
    /// </summary>
    [TestMethod]
    public void RecordLengthPastTheWrittenEndIsCountedAsSkipped()
    {
        using var dir = new TempDirectory();
        using RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions { CreateIfMissing = true });
        for (int i = 0; i < 4; i++) db.Put(Key(i), Value(i, 0));
        string wal = CurrentWalPath(db, dir.Path);
        GrowRecordLength(wal, LiveRecords(wal, db.CurrentWalNumber)[3], 200);
        using TransactionLogIterator it = db.GetUpdatesSince(1);
        CollectionAssert.AreEqual(new ulong[] { 1, 2, 3 }, ReadAll(it));
        Assert.AreEqual(StatusCode.TryAgain, it.Error?.Code);
        Assert.IsTrue(it.SkippedCorruptBytes > 0);
        StringAssert.Contains(it.SkippedCorruptionReason, "cannot be read past offset");
    }

    /// <summary>
    /// The same damage in a WAL that a switch closed (a second column family's flush keeps it alive) while the newer WAL
    /// holds nothing yet: the iterator leaves the closed WAL short of its written end and reports it.
    /// </summary>
    [TestMethod]
    public void RecordLengthPastTheEndOfAClosedWalIsCountedAsSkipped()
    {
        using var dir = new TempDirectory();
        using RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions { CreateIfMissing = true });
        ColumnFamily other = db.CreateColumnFamily("other", new ColumnFamilyOptions());
        for (int i = 0; i < 4; i++) db.Put(Key(i), Value(i, 0));
        string wal = CurrentWalPath(db, dir.Path);
        ulong closed = db.CurrentWalNumber;
        other.Put(B("x"), B("y"));
        other.Flush();
        Assert.AreNotEqual(closed, db.CurrentWalNumber, "precondition: the flush switched the WAL");
        // The CreateColumnFamily edit and the batches: the 4th batch is the last record but one (other's put).
        List<WalRecord> live = LiveRecords(wal, closed);
        GrowRecordLength(wal, live[^2], 200);
        using TransactionLogIterator it = db.GetUpdatesSince(1);
        List<ulong> seqs = ReadAll(it);
        Assert.AreEqual(StatusCode.TryAgain, it.Error?.Code, string.Join(",", seqs));
        Assert.IsTrue(it.SkippedCorruptBytes > 0);
        StringAssert.Contains(it.SkippedCorruptionReason, "WAL #" + closed);
    }

    /// <summary>
    /// A writer in the middle of a record on the active WAL: with manual_wal_flush a batch larger than the writer's
    /// buffer is partly on disk until FlushWAL. Not damage: the writer has not completed it.
    /// </summary>
    [TestMethod]
    public void RecordPartlyWrittenByTheActiveWalWriterIsNotDamage()
    {
        using var dir = new TempDirectory();
        using RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions { CreateIfMissing = true, ManualWalFlush = true, WritableFileMaxBufferSize = 64 << 10 });
        db.Put(B("a"), B("1"));
        db.FlushWal(sync: false);
        string wal = CurrentWalPath(db, dir.Path);
        long before = new FileInfo(wal).Length;
        db.Put(B("big"), new byte[300 << 10]);
        long partial = new FileInfo(wal).Length;
        Assert.IsTrue(partial > before && partial < before + (300 << 10), "precondition: part of the record is on disk (" + before + " -> " + partial + ")");
        using TransactionLogIterator it = db.GetUpdatesSince(1);
        CollectionAssert.AreEqual(new ulong[] { 1 }, ReadAll(it));
        Assert.AreEqual(StatusCode.TryAgain, it.Error?.Code);
        Assert.AreEqual(0, it.SkippedCorruptBytes, it.SkippedCorruptionReason);
        db.FlushWal(sync: false);
        it.Next();
        Assert.IsTrue(it.Valid, it.Error?.Message);
        Assert.AreEqual(2UL, it.Current.Sequence);
    }
}
