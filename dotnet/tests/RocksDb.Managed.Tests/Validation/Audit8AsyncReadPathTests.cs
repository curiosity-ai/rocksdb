using System.Buffers;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table.Filters;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #8, asynchronous read path (GetAsync / GetPooledAsync / TryGetAsync / GetEntityAsync, MultiGetAsync /
/// MultiGetEntityAsync, the async iterator moves and ScanAsync): sync/async parity on a randomized DB read through a file
/// system whose async reads really complete asynchronously, plus lifecycle and cancellation checks.
/// The reference for an async call is the synchronous managed call (one implementation for both), and below it the C++
/// semantics of DBImpl::GetImpl / MultiGetImpl / DBIter.
/// </summary>
[TestClass]
public sealed class Audit8AsyncReadPathTests
{
    private const string DbPath = "/audit8-async-read";

    // ------------------------------------------------------------------ file system with truly asynchronous reads

    /// <summary>
    /// A <see cref="MemoryFileSystem"/> whose random-access <c>ReadAsync</c> completes on a thread-pool thread (after a
    /// yield, sometimes a short delay so that concurrent reads complete out of order), or waits for <see cref="Gate"/>.
    /// Synchronous reads and everything else go to the memory file system.
    /// </summary>
    internal sealed class AsyncReadFileSystem(MemoryFileSystem inner) : IFileSystem
    {
        private long _asyncReads;
        private int _seed = 1;

        public MemoryFileSystem Inner { get; } = inner;

        /// <summary>When false, ReadAsync completes synchronously (the memory file system's behaviour).</summary>
        public bool Asynchronous { get; set; } = true;

        /// <summary>When set, every async read waits for this task before it reads (and completes).</summary>
        public volatile TaskCompletionSource? Gate;

        /// <summary>Signalled when an async read starts waiting on <see cref="Gate"/>.</summary>
        public volatile TaskCompletionSource? GateReached;

        /// <summary>Async reads that completed asynchronously.</summary>
        public long AsyncReads => Interlocked.Read(ref _asyncReads);

        /// <summary>When set, an async read whose token was cancelled while it waited fails with OperationCanceledException.</summary>
        public bool HonorCancellation { get; set; }

        private async ValueTask<int> ReadSlowAsync(IRandomAccessFile file, long offset, Memory<byte> destination, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _asyncReads);
            TaskCompletionSource? gate = Gate;
            if (gate is not null)
            {
                GateReached?.TrySetResult();
                await gate.Task.ConfigureAwait(false);
            }
            else if ((Interlocked.Increment(ref _seed) & 63) == 0)
            {
                await Task.Delay(1).ConfigureAwait(false);
            }
            else
            {
                await Task.Yield();
            }
            if (HonorCancellation) cancellationToken.ThrowIfCancellationRequested();
            return file.Read(offset, destination.Span);
        }

        private sealed class AsyncFile(AsyncReadFileSystem fs, IRandomAccessFile file) : IRandomAccessFile
        {
            public long Length => file.Length;
            public int Read(long offset, Span<byte> destination) => file.Read(offset, destination);

            public ValueTask<int> ReadAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken = default) =>
                fs.Asynchronous ? fs.ReadSlowAsync(file, offset, destination, cancellationToken) : file.ReadAsync(offset, destination, cancellationToken);

            public void Dispose() => file.Dispose();
        }

        public IWritableFile CreateWritable(string path, bool truncate = true) => Inner.CreateWritable(path, truncate);
        public IWritableFile CreateWritable(string path, bool truncate, WritableFileOptions options) => Inner.CreateWritable(path, truncate, options);
        public IWritableFile ReopenWritable(string path, WritableFileOptions options) => Inner.ReopenWritable(path, options);
        public IRandomAccessFile OpenRandomAccess(string path) => new AsyncFile(this, Inner.OpenRandomAccess(path));
        public IRandomAccessFile OpenRandomAccess(string path, RandomAccessFileOptions options) => new AsyncFile(this, Inner.OpenRandomAccess(path, options));
        public ISequentialFile OpenSequential(string path) => Inner.OpenSequential(path);
        public ISequentialFile OpenSequential(string path, SequentialFileOptions options) => Inner.OpenSequential(path, options);
        public bool FileExists(string path) => Inner.FileExists(path);
        public bool DirectoryExists(string path) => Inner.DirectoryExists(path);
        public long GetFileSize(string path) => Inner.GetFileSize(path);
        public void DeleteFile(string path) => Inner.DeleteFile(path);
        public void RenameFile(string source, string target) => Inner.RenameFile(source, target);
        public void CreateDirectoryIfMissing(string path) => Inner.CreateDirectoryIfMissing(path);
        public void DeleteDirectory(string path) => Inner.DeleteDirectory(path);
        public IEnumerable<string> GetChildren(string directory) => Inner.GetChildren(directory);
        public void LinkFile(string source, string target) => Inner.LinkFile(source, target);
        public long GetFreeSpace(string path) => Inner.GetFreeSpace(path);
        public void WriteAt(string path, long offset, ReadOnlySpan<byte> data) => Inner.WriteAt(path, offset, data);
        public DateTimeOffset GetFileModificationTime(string path) => Inner.GetFileModificationTime(path);
        public IDisposable LockFile(string path) => Inner.LockFile(path);
        public void SyncDirectory(string path) => Inner.SyncDirectory(path);
    }

    // ------------------------------------------------------------------ randomized DB

    private const int KeySpace = 1200;

    private static byte[] K(int i) => B("k" + i.ToString("D5", System.Globalization.CultureInfo.InvariantCulture));

    private static ColumnFamilyOptions CfOptions(bool partitioned, bool readahead, Managed.Cache.BlockCache cache) => new()
    {
        DisableAutoCompactions = true,
        MergeOperator = MergeOperator.StringAppend(","),
        PrefixExtractor = SliceTransform.CreateFixedPrefix(4),
        EnableBlobFiles = true,
        MinBlobSize = 120,
        WriteBufferSize = 4 << 20,
        TableOptions = new BlockBasedTableOptions
        {
            BlockSize = 512,
            BlockCache = cache,
            FilterPolicy = new BloomFilterPolicy(10),
            IndexType = partitioned ? IndexType.TwoLevelIndexSearch : IndexType.BinarySearch,
            PartitionFilters = partitioned,
            MetadataBlockSize = 256,
            CacheIndexAndFilterBlocks = partitioned,
            MaxAutoReadaheadSize = readahead ? 64UL * 1024 : 0,
        },
    };

    private sealed class TestDb : IDisposable
    {
        public required RocksDatabase Db;
        public required Snapshot Snapshot;
        public required Statistics Stats;
        public required Managed.Cache.BlockCache Cache;

        public void Dispose()
        {
            Snapshot.Dispose();
            Db.Dispose();
        }
    }

    /// <summary>
    /// A DB with data in the memtable, two L0 files, L3 and L5 (CompactFiles), merges, deletes, range deletions, wide-column
    /// entities (some with blob columns) and blob values; the snapshot is taken between the rounds.
    /// </summary>
    private static TestDb BuildDb(int seed, IFileSystem fs, bool partitioned, bool readahead = false)
    {
        var rnd = new Random(seed);
        var stats = new Statistics();
        Managed.Cache.BlockCache cache = Managed.Cache.BlockCache.CreateLru(8 << 20);
        RocksDatabase db = RocksDatabase.Open(DbPath, new DbOptions { CreateIfMissing = true, FileSystem = fs, Statistics = stats },
            [new ColumnFamilyDescriptor("default", CfOptions(partitioned, readahead, cache))]);
        ColumnFamily cf = db.DefaultColumnFamily;
        Snapshot? snap = null;
        int[] rounds = [1500, 900, 500, 400, 300];
        for (int r = 0; r < rounds.Length; r++)
        {
            for (int n = 0; n < rounds[r]; n++)
            {
                int k = rnd.Next(KeySpace);
                int op = rnd.Next(100);
                if (op < 45)
                {
                    int len = rnd.Next(100) < 25 ? 120 + rnd.Next(200) : 1 + rnd.Next(60);
                    byte[] v = new byte[len];
                    rnd.NextBytes(v);
                    cf.Put(K(k), v);
                }
                else if (op < 62)
                {
                    cf.Merge(K(k), B("m" + r + "." + n));
                }
                else if (op < 72)
                {
                    cf.Delete(K(k));
                }
                else if (op < 75)
                {
                    int span = 1 + rnd.Next(25);
                    cf.DeleteRange(K(k), K(Math.Min(KeySpace, k + span)));
                }
                else
                {
                    int cols = 1 + rnd.Next(4);
                    var columns = new WideColumn[cols];
                    for (int c = 0; c < cols; c++)
                    {
                        byte[] cv = new byte[rnd.Next(100) < 20 ? 150 + rnd.Next(100) : rnd.Next(40)];
                        rnd.NextBytes(cv);
                        // Column names are sorted; the first one may be the default column.
                        byte[] name = c == 0 && rnd.Next(2) == 0 ? [] : B("c" + c);
                        columns[c] = new WideColumn(name, cv);
                    }
                    cf.PutEntity(K(k), columns);
                }
            }
            if (r == 2) snap = db.GetSnapshot();
            if (r < 4) db.Flush();
            if (r < 2)
            {
                var l0 = new List<ulong>();
                foreach (LiveFileMetadata f in db.GetLiveFilesMetadata())
                {
                    if (f.Level == 0) l0.Add(f.FileNumber);
                }
                cf.CompactFiles(new CompactFilesOptions(), l0, r == 0 ? 5 : 3);
            }
        }
        return new TestDb { Db = db, Snapshot = snap!, Stats = stats, Cache = cache };
    }

    // ------------------------------------------------------------------ read options under test

    private static ReadOptions?[] ReadOptionVariants(Snapshot snap) =>
    [
        null,
        new ReadOptions { Snapshot = snap },
        new ReadOptions { FillCache = false, VerifyChecksums = false },
        new ReadOptions { ReadaheadSize = 4096, AsyncIo = true },
        new ReadOptions { ReadaheadSize = 2048 },
        new ReadOptions { AsyncIo = true, Snapshot = snap },
        new ReadOptions { TotalOrderSeek = true },
        new ReadOptions { PrefixSameAsStart = true },
        new ReadOptions { AutoPrefixMode = true, IterateUpperBound = K(700) },
        new ReadOptions { IgnoreRangeDeletions = true },
        new ReadOptions { PinData = true },
        new ReadOptions { ReadTier = ReadTier.BlockCacheTier },
        new ReadOptions { IterateLowerBound = K(150), IterateUpperBound = K(930), TotalOrderSeek = true },
    ];

    private static string Describe(ReadOptions? o) =>
        o is null ? "null" : "snap=" + (o.Snapshot is not null) + " fill=" + o.FillCache + " ra=" + o.ReadaheadSize + " aio=" + o.AsyncIo + " tos=" +
            o.TotalOrderSeek + " psas=" + o.PrefixSameAsStart + " apm=" + o.AutoPrefixMode + " ird=" + o.IgnoreRangeDeletions + " pin=" + o.PinData +
            " tier=" + o.ReadTier + " lb=" + (o.IterateLowerBound is not null) + " ub=" + (o.IterateUpperBound is not null);

    // ------------------------------------------------------------------ outcome capture

    /// <summary>A comparable outcome: the result bytes, or the exception type and status code.</summary>
    private static string Outcome(Func<string> read)
    {
        try
        {
            return read();
        }
        catch (RocksDbException e)
        {
            return "RocksDbException:" + e.Code;
        }
        catch (Exception e)
        {
            return "EXCEPTION:" + e.GetType().Name;
        }
    }

    private static string Hex(ReadOnlySpan<byte> b) => Convert.ToHexString(b);

    private static T Run<T>(ValueTask<T> t)
    {
        Task<T> task = t.AsTask();
        if (!((IAsyncResult)task).AsyncWaitHandle.WaitOne(TimeSpan.FromSeconds(20))) Assert.Fail("async read did not complete within 20 s (deadlock?)");
        return task.GetAwaiter().GetResult();
    }

    private static void Run(ValueTask t)
    {
        Task task = t.AsTask();
        if (!((IAsyncResult)task).AsyncWaitHandle.WaitOne(TimeSpan.FromSeconds(20))) Assert.Fail("async read did not complete within 20 s (deadlock?)");
        task.GetAwaiter().GetResult();
    }

    private static string GetSync(ColumnFamily cf, ReadOptions? o, byte[] key) => Outcome(() => cf.Get(o, key) is { } v ? Hex(v) : "null");
    private static string GetAsyncOutcome(ColumnFamily cf, ReadOptions? o, byte[] key) => Outcome(() => Run(cf.GetAsync(o, key)) is { } v ? Hex(v) : "null");

    private static string GetPooledSync(ColumnFamily cf, ReadOptions? o, byte[] key) => Outcome(() =>
    {
        using PooledValue? v = cf.GetPooled(o, key);
        return v is null ? "null" : Hex(v.Span);
    });

    private static string GetPooledAsyncOutcome(ColumnFamily cf, ReadOptions? o, byte[] key) => Outcome(() =>
    {
        using PooledValue? v = Run(cf.GetPooledAsync(o, key));
        return v is null ? "null" : Hex(v.Span);
    });

    private static string TryGetAsyncOutcome(ColumnFamily cf, ReadOptions? o, byte[] key) => Outcome(() =>
    {
        var w = new ArrayBufferWriter<byte>();
        return Run(cf.TryGetAsync(o, key, w)) ? Hex(w.WrittenSpan) : "null";
    });

    private static string EntitySync(ColumnFamily cf, ReadOptions? o, byte[] key) => Outcome(() =>
    {
        var c = new WideColumns();
        return cf.TryGetEntity(o, key, c) ? Hex(c.Serialized) : "null";
    });

    private static string EntityAsync(ColumnFamily cf, ReadOptions? o, byte[] key) => Outcome(() =>
    {
        var c = new WideColumns();
        return Run(cf.TryGetEntityAsync(o, key, c)) ? Hex(c.Serialized) : "null";
    });

    private static string BatchOutcome(MultiGetBatch batch, bool entity)
    {
        var sb = new System.Text.StringBuilder();
        var cols = new WideColumns();
        for (int i = 0; i < batch.Count; i++)
        {
            switch (batch.GetStatus(i))
            {
                case MultiGetStatus.Found:
                    sb.Append(entity && batch.TryGetEntity(i, cols) ? Hex(cols.Serialized) : Hex(batch.GetValue(i)));
                    break;
                case MultiGetStatus.Error:
                    sb.Append("E:").Append(batch.GetError(i)!.Code);
                    break;
                default:
                    sb.Append("nf");
                    break;
            }
            sb.Append('|');
        }
        return sb.ToString();
    }

    private static string MultiGetOutcome(ColumnFamily cf, ReadOptions? o, MultiGetBatch batch, bool entity, bool async) => Outcome(() =>
    {
        if (entity)
        {
            if (async) Run(cf.MultiGetEntityAsync(o, batch));
            else cf.MultiGetEntity(o, batch);
        }
        else
        {
            if (async) Run(cf.MultiGetAsync(o, batch));
            else cf.MultiGet(o, batch);
        }
        return BatchOutcome(batch, entity);
    });

    // ------------------------------------------------------------------ point lookups

    /// <summary>
    /// Pins: Get / GetPooled / TryGet / GetEntity and MultiGet / MultiGetEntity return the same values, per-key statuses and
    /// errors (type and code) through the async API as through the sync API, for every read option variant, on a DB with
    /// memtable + L0 + L3 + L5 data, merges, deletes, range deletions, entities and blob values, read through a file
    /// system whose async reads complete on other threads (partitioned and plain indexes).
    /// </summary>
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void PointLookupsMatchSyncThroughAsyncFileSystem(bool partitioned)
    {
        int seed = Environment.TickCount;
        string trace = "seed=" + seed + " partitioned=" + partitioned;
        var fs = new AsyncReadFileSystem(NewFs());
        using TestDb t = BuildDb(seed, fs, partitioned);
        ColumnFamily cf = t.Db.DefaultColumnFamily;
        var rnd = new Random(seed + 1);
        long asyncBefore = fs.AsyncReads;
        foreach (ReadOptions? o in ReadOptionVariants(t.Snapshot))
        {
            string what = trace + " ro=" + Describe(o);
            for (int i = 0; i < 120; i++)
            {
                byte[] key = K(rnd.Next(KeySpace + 20));
                // Empty the cache now and then so that the async lookups read from the files.
                if (i % 40 == 0) t.Cache.EraseUnreferencedEntries();
                string a = GetAsyncOutcome(cf, o, key);
                string s = GetSync(cf, o, key);
                Assert.AreEqual(s, a, what + " Get " + S(key));
                if (i % 40 == 0) t.Cache.EraseUnreferencedEntries();
                Assert.AreEqual(GetPooledSync(cf, o, key), GetPooledAsyncOutcome(cf, o, key), what + " GetPooled " + S(key));
                Assert.AreEqual(s, TryGetAsyncOutcome(cf, o, key), what + " TryGetAsync " + S(key));
                if (i % 3 == 0) t.Cache.EraseUnreferencedEntries();
                string ea = EntityAsync(cf, o, key);
                Assert.AreEqual(EntitySync(cf, o, key), ea, what + " GetEntity " + S(key));
            }
            using var batchA = new MultiGetBatch();
            using var batchS = new MultiGetBatch();
            for (int round = 0; round < 25; round++)
            {
                batchA.Clear();
                batchS.Clear();
                int n = 1 + rnd.Next(40);
                for (int j = 0; j < n; j++)
                {
                    byte[] key = K(rnd.Next(KeySpace + 20));
                    batchA.Add(key);
                    batchS.Add(key);
                }
                bool entity = round % 2 == 1;
                if (round % 5 == 0) t.Cache.EraseUnreferencedEntries();
                string a = MultiGetOutcome(cf, o, batchA, entity, async: true);
                if (round % 5 == 0) t.Cache.EraseUnreferencedEntries();
                string s = MultiGetOutcome(cf, o, batchS, entity, async: false);
                Assert.AreEqual(s, a, what + " MultiGet" + (entity ? "Entity" : "") + " round " + round);
            }
        }
        Assert.IsTrue(fs.AsyncReads > asyncBefore + 100, "the async paths read through the async file system: " + (fs.AsyncReads - asyncBefore));
    }

    // ------------------------------------------------------------------ iterators

    private static string IterState(Iterator it)
    {
        if (!it.Valid) return "invalid";
        return Hex(it.Key) + "=" + Hex(it.Value) + (it.IsEntity ? " E" + Hex(it.Columns.Serialized) : "");
    }

    private enum Move { First, Last, Seek, SeekForPrev, Next, Prev, Refresh }

    private static void Apply(Iterator it, Move m, byte[] target, bool async)
    {
        switch (m)
        {
            case Move.First: if (async) Run(it.SeekToFirstAsync()); else it.SeekToFirst(); break;
            case Move.Last: if (async) Run(it.SeekToLastAsync()); else it.SeekToLast(); break;
            case Move.Seek: if (async) Run(it.SeekAsync(target)); else it.Seek(target); break;
            case Move.SeekForPrev: if (async) Run(it.SeekForPrevAsync(target)); else it.SeekForPrev(target); break;
            case Move.Next: if (async) Run(it.NextAsync()); else it.Next(); break;
            case Move.Prev: if (async) Run(it.PrevAsync()); else it.Prev(); break;
            default: if (async) Run(it.RefreshAsync()); else it.Refresh(); break;
        }
    }

    private static string MoveOutcome(Iterator it, Move m, byte[] target, bool async)
    {
        try
        {
            Apply(it, m, target, async);
            if (it.Valid && async) Run(it.PrepareValueAsync());
            return IterState(it);
        }
        catch (RocksDbException e)
        {
            return "RocksDbException:" + e.Code;
        }
        catch (Exception e)
        {
            return "EXCEPTION:" + e.GetType().Name + ":" + e.Message;
        }
    }

    /// <summary>
    /// Pins: an iterator moved with the async API (SeekAsync / SeekForPrevAsync / SeekToFirstAsync / SeekToLastAsync /
    /// NextAsync / PrevAsync / RefreshAsync), and one mixing sync and async moves at random (Seek then NextAsync, NextAsync
    /// then Prev, ...), stay on the same entry (key, value, columns) or fail with the same status as an iterator moved with
    /// the sync API, for every read option variant (snapshot, bounds, prefix seek, readahead with and without async_io,
    /// pin_data, kBlockCacheTier), with the async file reads completing on other threads.
    /// </summary>
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void IteratorMovesMatchSyncThroughAsyncFileSystem(bool partitioned)
    {
        int seed = Environment.TickCount;
        string trace = "seed=" + seed + " partitioned=" + partitioned;
        var fs = new AsyncReadFileSystem(NewFs());
        using TestDb t = BuildDb(seed, fs, partitioned, readahead: true);
        ColumnFamily cf = t.Db.DefaultColumnFamily;
        var rnd = new Random(seed + 2);
        long asyncBefore = fs.AsyncReads;
        foreach (ReadOptions? o in ReadOptionVariants(t.Snapshot))
        {
            string what = trace + " ro=" + Describe(o);
            using Iterator sync = cf.NewIterator(o);
            using Iterator async = Run(cf.NewIteratorAsync(o));
            using Iterator mixed = cf.NewIterator(o);
            string last = "invalid";
            for (int step = 0; step < 220; step++)
            {
                Move m;
                int r = rnd.Next(100);
                if (last != "invalid" && !last.StartsWith("RocksDbException", StringComparison.Ordinal) && r < 70) m = r < 45 ? Move.Next : Move.Prev;
                else m = (Move)rnd.Next(4);
                if (rnd.Next(150) == 0) m = Move.Refresh;
                byte[] target = K(rnd.Next(KeySpace + 20));
                if (step % 37 == 0) t.Cache.EraseUnreferencedEntries();
                string a = MoveOutcome(async, m, target, async: true);
                string x = MoveOutcome(mixed, m, target, async: rnd.Next(2) == 0);
                string s = MoveOutcome(sync, m, target, async: false);
                Assert.AreEqual(s, a, what + " step " + step + " " + m + " " + S(target) + " (async)");
                Assert.AreEqual(s, x, what + " step " + step + " " + m + " " + S(target) + " (mixed)");
                last = s;
                if (s.StartsWith("RocksDbException", StringComparison.Ordinal) || s.StartsWith("EXCEPTION", StringComparison.Ordinal))
                {
                    // A failed move leaves the iterators invalid; start over with a seek.
                    last = "invalid";
                }
            }
        }
        Assert.IsTrue(fs.AsyncReads > asyncBefore + 100, "the async paths read through the async file system: " + (fs.AsyncReads - asyncBefore));
    }

    /// <summary>
    /// Pins: <see cref="Iterator.ScanAsync(ReadOnlyMemory{byte}?, ReadOnlyMemory{byte}?)"/> yields exactly the entries the
    /// sync <see cref="Iterator.Range(ReadOnlySpan{byte}, ReadOnlySpan{byte})"/> enumerator yields, for random ranges and
    /// read options.
    /// </summary>
    [TestMethod]
    public void ScanAsyncMatchesRange()
    {
        int seed = Environment.TickCount;
        var fs = new AsyncReadFileSystem(NewFs());
        using TestDb t = BuildDb(seed, fs, partitioned: seed % 2 == 0, readahead: true);
        ColumnFamily cf = t.Db.DefaultColumnFamily;
        var rnd = new Random(seed + 3);
        foreach (ReadOptions? o in ReadOptionVariants(t.Snapshot))
        {
            if (o?.ReadTier == ReadTier.BlockCacheTier) continue;
            string what = "seed=" + seed + " ro=" + Describe(o);
            for (int round = 0; round < 4; round++)
            {
                int a = rnd.Next(KeySpace), b = a + rnd.Next(400);
                byte[] start = K(a), end = K(b);
                var expected = new List<string>();
                using (Iterator it = cf.NewIterator(o))
                {
                    foreach (KeyValueRef kv in it.Range(start, end)) expected.Add(Hex(kv.Key) + "=" + Hex(kv.Value));
                }
                t.Cache.EraseUnreferencedEntries();
                var actual = new List<string>();
                using (Iterator it = cf.NewIterator(o))
                {
                    Run(Collect(it.ScanAsync(start, end), actual));
                }
                CollectionAssert.AreEqual(expected, actual, what + " [" + a + ", " + b + ")");
            }
        }
    }

    private static async ValueTask Collect(IAsyncEnumerable<KeyValue> scan, List<string> into)
    {
        await foreach (KeyValue kv in scan.ConfigureAwait(false)) into.Add(Hex(kv.KeySpan) + "=" + Hex(kv.ValueSpan));
    }

    // ------------------------------------------------------------------ statistics parity

    private static ulong[] Tickers(Statistics st)
    {
        var t = new ulong[StatisticsNames.TickerCount];
        for (int i = 0; i < t.Length; i++) t[i] = st.GetTickerCount((Ticker)i);
        return t;
    }

    /// <summary>The tickers that changed while <paramref name="work"/> ran, from an emptied block cache.</summary>
    private static SortedDictionary<string, ulong> TickerDelta(TestDb t, Action work)
    {
        t.Cache.EraseUnreferencedEntries();
        ulong[] before = Tickers(t.Stats);
        work();
        ulong[] after = Tickers(t.Stats);
        var d = new SortedDictionary<string, ulong>();
        for (int i = 0; i < before.Length; i++)
        {
            // The per-processor super version slots (NUMBER_SUPERVERSION_*) depend on the threads a read runs on: an async
            // read holds its slot across an await, so another read on that processor takes a plain reference.
            if ((Ticker)i is Ticker.NumberSuperversionAcquires or Ticker.NumberSuperversionReleases or Ticker.NumberSuperversionCleanups) continue;
            if (after[i] != before[i]) d[((Ticker)i).ToString()] = after[i] - before[i];
        }
        return d;
    }

    private static string Format(SortedDictionary<string, ulong> d)
    {
        var sb = new System.Text.StringBuilder();
        foreach (KeyValuePair<string, ulong> kv in d) sb.Append(kv.Key).Append('=').Append(kv.Value).Append(' ');
        return sb.ToString();
    }

    private static void AssertSameTickers(TestDb t, string what, Action sync, Action async)
    {
        SortedDictionary<string, ulong> s = TickerDelta(t, sync);
        SortedDictionary<string, ulong> a = TickerDelta(t, async);
        Assert.AreEqual(Format(s), Format(a), what);
    }

    /// <summary>
    /// Pins: the async point lookups and MultiGet record the same tickers as the sync ones (block cache hits / misses /
    /// adds per block kind, bytes read from the files, memtable hits, GET_HIT_L*, keys and bytes read, blob reads) when
    /// they start from the same cache state (readahead off: the documented readahead deviation of async moves is not
    /// what this compares).
    /// </summary>
    [TestMethod]
    public void PointLookupStatisticsMatchSync()
    {
        int seed = Environment.TickCount;
        var fs = new AsyncReadFileSystem(NewFs());
        using TestDb t = BuildDb(seed, fs, partitioned: seed % 2 == 0);
        ColumnFamily cf = t.Db.DefaultColumnFamily;
        var keys = new byte[200][];
        var rnd = new Random(seed);
        for (int i = 0; i < keys.Length; i++) keys[i] = K(rnd.Next(KeySpace));
        // Warm up: every table open, so the comparison does not include table opens.
        foreach (byte[] k in keys) cf.Get(k);
        string what = "seed=" + seed;
        AssertSameTickers(t, what + " Get", () => { foreach (byte[] k in keys) cf.Get(k); },
            () => { foreach (byte[] k in keys) Run(cf.GetAsync(k)); });
        var snapOpts = new ReadOptions { Snapshot = t.Snapshot };
        AssertSameTickers(t, what + " Get(snapshot)", () => { foreach (byte[] k in keys) cf.Get(snapOpts, k); },
            () => { foreach (byte[] k in keys) Run(cf.GetAsync(snapOpts, k)); });
        AssertSameTickers(t, what + " GetEntity", () => { foreach (byte[] k in keys) cf.TryGetEntity(null, k, new WideColumns()); },
            () => { foreach (byte[] k in keys) Run(cf.TryGetEntityAsync(null, k, new WideColumns())); });
        using var batch = new MultiGetBatch();
        foreach (byte[] k in keys) batch.Add(k);
        AssertSameTickers(t, what + " MultiGet", () => cf.MultiGet(batch), () => Run(cf.MultiGetAsync(batch)));
        AssertSameTickers(t, what + " MultiGetEntity", () => cf.MultiGetEntity(null, batch), () => Run(cf.MultiGetEntityAsync(null, batch)));
    }

    /// <summary>
    /// Pins: iterator moves through the async API record the same tickers as the sync moves (NUMBER_DB_SEEK / NEXT / PREV
    /// and their FOUND counts, ITER_BYTES_READ, block cache and file read counters), and an async scan
    /// (<see cref="Iterator.ScanAsync(ReadOnlyMemory{byte}?, ReadOnlyMemory{byte}?)"/>) the same as the sync range
    /// enumerator.
    /// </summary>
    [TestMethod]
    public void IteratorStatisticsMatchSync()
    {
        int seed = Environment.TickCount;
        var fs = new AsyncReadFileSystem(NewFs());
        using TestDb t = BuildDb(seed, fs, partitioned: seed % 2 == 0);
        ColumnFamily cf = t.Db.DefaultColumnFamily;
        using (Iterator warm = cf.NewIterator(null))
        {
            for (warm.SeekToFirst(); warm.Valid; warm.Next()) { }
        }
        string what = "seed=" + seed;
        byte[] start = K(200), end = K(600);
        AssertSameTickers(t, what + " moves",
            () =>
            {
                using Iterator it = cf.NewIterator(null);
                it.Seek(start);
                for (int i = 0; i < 50 && it.Valid; i++) it.Next();
                for (int i = 0; i < 30 && it.Valid; i++) it.Prev();
                it.SeekForPrev(end);
                for (int i = 0; i < 30 && it.Valid; i++) it.Prev();
            },
            () =>
            {
                using Iterator it = cf.NewIterator(null);
                Run(it.SeekAsync(start));
                for (int i = 0; i < 50 && it.Valid; i++) Run(it.NextAsync());
                for (int i = 0; i < 30 && it.Valid; i++) Run(it.PrevAsync());
                Run(it.SeekForPrevAsync(end));
                for (int i = 0; i < 30 && it.Valid; i++) Run(it.PrevAsync());
            });
        AssertSameTickers(t, what + " scan",
            () =>
            {
                using Iterator it = cf.NewIterator(null);
                foreach (KeyValueRef kv in it.Range(start, end)) { }
            },
            () =>
            {
                using Iterator it = cf.NewIterator(null);
                Run(Collect(it.ScanAsync(start, end), new List<string>()));
            });
    }

    // ------------------------------------------------------------------ lifecycle: in-flight reads

    /// <summary>A small DB of 400 keys in one L5 file (100-byte values, 512-byte blocks), no cache hits once erased.</summary>
    private static TestDb BuildPlainDb(AsyncReadFileSystem fs)
    {
        var stats = new Statistics();
        Managed.Cache.BlockCache cache = Managed.Cache.BlockCache.CreateLru(8 << 20);
        ColumnFamilyOptions cfo = CfOptions(partitioned: false, readahead: false, cache);
        cfo.EnableBlobFiles = false;
        RocksDatabase db = RocksDatabase.Open(DbPath, new DbOptions { CreateIfMissing = true, FileSystem = fs, Statistics = stats },
            [new ColumnFamilyDescriptor("default", cfo)]);
        for (int i = 0; i < 400; i++) db.Put(K(i), B(new string((char)('a' + i % 26), 100)));
        db.Flush();
        db.CompactRange();
        return new TestDb { Db = db, Snapshot = db.GetSnapshot(), Stats = stats, Cache = cache };
    }

    private static void WaitReached(TaskCompletionSource reached) =>
        Assert.IsTrue(reached.Task.Wait(TimeSpan.FromSeconds(20)), "the async read did not start");

    /// <summary>
    /// Pins: disposing an iterator while an async move is waiting for its file read does not let that move run on the
    /// iterator stack once it has gone back to the column family's pool (where the next NewIterator rents it): the
    /// other iterator keeps its position, the pending move fails with ObjectDisposedException (or completes) and nothing
    /// stays pinned in the block cache.
    /// </summary>
    [TestMethod]
    public void DisposeDuringAsyncMoveDoesNotCorruptThePooledIterator()
    {
        var fs = new AsyncReadFileSystem(NewFs());
        using TestDb t = BuildPlainDb(fs);
        ColumnFamily cf = t.Db.DefaultColumnFamily;
        string expected;
        using (Iterator check = cf.NewIterator(null))
        {
            check.Seek(K(300));
            check.Next();
            expected = S(check.Key.ToArray())!;
        }
        t.Cache.EraseUnreferencedEntries();
        Iterator it1 = cf.NewIterator(null);
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var reached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        fs.GateReached = reached;
        fs.Gate = gate;
        Task pending = it1.SeekAsync(K(10)).AsTask();
        WaitReached(reached);
        fs.Gate = null;
        it1.Dispose();
        // The next iterator of the column family rents the core it1 returned to the pool.
        using Iterator it2 = cf.NewIterator(null);
        it2.Seek(K(300));
        gate.SetResult();
        bool done = ((IAsyncResult)pending).AsyncWaitHandle.WaitOne(TimeSpan.FromSeconds(20));
        Assert.IsTrue(done, "the pending move completed");
        if (pending.IsFaulted) Assert.IsInstanceOfType<ObjectDisposedException>(pending.Exception!.InnerException);
        Assert.IsTrue(it2.Valid);
        Assert.AreEqual("k00300", S(it2.Key.ToArray()));
        it2.Next();
        Assert.AreEqual(expected, S(it2.Key.ToArray()));
        it2.Dispose();
        t.Cache.EraseUnreferencedEntries();
        Assert.AreEqual(0, t.Cache.PinnedUsage, "nothing stays pinned once every iterator is disposed");
    }

    // ------------------------------------------------------------------ ScanAsync lifecycle

    private static List<string> SyncRange(ColumnFamily cf, ReadOptions? o, byte[] start, byte[] end)
    {
        var expected = new List<string>();
        using Iterator it = cf.NewIterator(o);
        foreach (KeyValueRef kv in it.Range(start, end)) expected.Add(Hex(kv.Key) + "=" + Hex(kv.Value));
        return expected;
    }

    /// <summary>
    /// Pins: ScanAsync on a tailing iterator (ReadOptions.Tailing) sees writes made after the iterator was created, also
    /// once a flush installed a new super version, like a Seek / Next of the sync range enumerator on it (the scan used to
    /// move the DB iterator directly, skipping the tailing renewal, and returned the stale state).
    /// </summary>
    [TestMethod]
    public void TailingScanAsyncSeesWritesAfterFlush()
    {
        var fs = new AsyncReadFileSystem(NewFs());
        using TestDb t = BuildPlainDb(fs);
        ColumnFamily cf = t.Db.DefaultColumnFamily;
        var tailing = new ReadOptions { Tailing = true, TotalOrderSeek = true };
        using Iterator sync = cf.NewIterator(tailing);
        using Iterator async = cf.NewIterator(tailing);
        sync.SeekToFirst();
        Run(async.SeekToFirstAsync());
        cf.Put(K(500), B("new"));
        t.Db.Flush();
        // Written to the memtable the flush installed: not in the super version the iterators were built on.
        cf.Put(K(10), B("changed"));
        cf.Put(K(600), B("newer"));
        var expected = new List<string>();
        foreach (KeyValueRef kv in sync.Range(K(0), K(1000))) expected.Add(Hex(kv.Key) + "=" + Hex(kv.Value));
        var actual = new List<string>();
        Run(Collect(async.ScanAsync(K(0), K(1000)), actual));
        Assert.AreEqual(402, expected.Count);
        Assert.AreEqual(Hex(K(10)) + "=" + Hex(B("changed")), expected[10]);
        CollectionAssert.AreEqual(expected, actual);
    }

    /// <summary>
    /// Pins the ScanAsync enumerator contract: breaking out early leaves the iterator usable for the next scan, DisposeAsync
    /// twice is a no-op, MoveNextAsync after DisposeAsync returns false, a second GetAsyncEnumerator throws
    /// InvalidOperationException, a token from WithCancellation cancels at the next MoveNextAsync with
    /// OperationCanceledException (also when cancelled mid-scan), and a scan after a cancelled one returns everything.
    /// </summary>
    [TestMethod]
    public void ScanAsyncEnumeratorLifecycle()
    {
        var fs = new AsyncReadFileSystem(NewFs());
        using TestDb t = BuildPlainDb(fs);
        ColumnFamily cf = t.Db.DefaultColumnFamily;
        List<string> expected = SyncRange(cf, null, K(100), K(200));
        using Iterator it = cf.NewIterator(null);
        Run(LifecycleAsync(it, expected, t));
    }

    private static async ValueTask LifecycleAsync(Iterator it, List<string> expected, TestDb t)
    {
        // Break out early.
        int n = 0;
        await foreach (KeyValue kv in it.ScanAsync(K(100), K(200)).ConfigureAwait(false))
        {
            if (++n == 5) break;
        }
        // GetAsyncEnumerator twice, DisposeAsync twice, MoveNextAsync after dispose.
        IAsyncEnumerable<KeyValue> scan = it.ScanAsync(K(100), K(200));
        IAsyncEnumerator<KeyValue> e = scan.GetAsyncEnumerator();
        Assert.ThrowsExactly<InvalidOperationException>(() => scan.GetAsyncEnumerator());
        Assert.IsTrue(await e.MoveNextAsync().ConfigureAwait(false));
        Assert.AreEqual(expected[0], Hex(e.Current.KeySpan) + "=" + Hex(e.Current.ValueSpan));
        await e.DisposeAsync().ConfigureAwait(false);
        await e.DisposeAsync().ConfigureAwait(false);
        Assert.IsFalse(await e.MoveNextAsync().ConfigureAwait(false));
        // Cancelled before the first MoveNextAsync.
        using (var cts = new CancellationTokenSource())
        {
            cts.Cancel();
            await Assert.ThrowsExactlyAsync<OperationCanceledException>(async () =>
            {
                await foreach (KeyValue kv in it.ScanAsync(K(100), K(200)).WithCancellation(cts.Token).ConfigureAwait(false)) { }
            }).ConfigureAwait(false);
        }
        // Cancelled mid-scan, with reads going to the file.
        t.Cache.EraseUnreferencedEntries();
        using (var cts = new CancellationTokenSource())
        {
            int seen = 0;
            await Assert.ThrowsExactlyAsync<OperationCanceledException>(async () =>
            {
                await foreach (KeyValue kv in it.ScanAsync(K(100), K(200)).WithCancellation(cts.Token).ConfigureAwait(false))
                {
                    if (++seen == 10) cts.Cancel();
                }
            }).ConfigureAwait(false);
            Assert.AreEqual(10, seen);
        }
        var all = new List<string>();
        await Collect(it.ScanAsync(K(100), K(200)), all).ConfigureAwait(false);
        CollectionAssert.AreEqual(expected, all);
    }

    // ------------------------------------------------------------------ cancellation and errors

    /// <summary>
    /// Pins: a point read, MultiGet (batch and caller-buffer forms), MultiGetEntity and an iterator move cancelled while
    /// their file read waits fail with OperationCanceledException (not wrapped), leave no block pinned in the cache and no
    /// pooled object in use (the same reads succeed right after and return what the sync API returns).
    /// </summary>
    [TestMethod]
    public void CancellationWhileAReadWaitsSurfacesOperationCanceled()
    {
        var fs = new AsyncReadFileSystem(NewFs()) { HonorCancellation = true };
        using TestDb t = BuildPlainDb(fs);
        ColumnFamily cf = t.Db.DefaultColumnFamily;
        byte[][] keys = [K(3), K(150), K(399), K(1000)];
        var ops = new (string Name, Func<CancellationToken, Task> Op)[]
        {
            ("GetAsync", ct => cf.GetAsync(null, K(150), ct).AsTask()),
            ("GetPooledAsync", async ct => { using PooledValue? v = await cf.GetPooledAsync(null, K(150), ct).ConfigureAwait(false); }),
            ("TryGetAsync", ct => cf.TryGetAsync(null, K(150), new ArrayBufferWriter<byte>(), ct).AsTask()),
            ("GetEntityAsync", ct => cf.TryGetEntityAsync(null, K(150), new WideColumns(), ct).AsTask()),
            ("MultiGetAsync", async ct =>
            {
                using var b = new MultiGetBatch();
                foreach (byte[] k in keys) b.Add(k);
                await cf.MultiGetAsync(null, b, ct).ConfigureAwait(false);
            }),
            ("MultiGetEntityAsync", async ct =>
            {
                using var b = new MultiGetBatch();
                foreach (byte[] k in keys) b.Add(k);
                await cf.MultiGetEntityAsync(null, b, ct).ConfigureAwait(false);
            }),
            ("MultiGetAsync(buffers)", ct => cf.MultiGetAsync(null, new ReadOnlyMemory<byte>[] { K(3), K(150), K(399) }, new ArrayBufferWriter<byte>(),
                new Range[3], new MultiGetStatus[3], ct).AsTask()),
            ("SeekAsync", async ct =>
            {
                using Iterator it = cf.NewIterator(null);
                await it.SeekAsync(K(150), ct).ConfigureAwait(false);
            }),
        };
        foreach ((string name, Func<CancellationToken, Task> op) in ops)
        {
            t.Cache.EraseUnreferencedEntries();
            using var cts = new CancellationTokenSource();
            var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var reached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            fs.GateReached = reached;
            fs.Gate = gate;
            Task task = op(cts.Token);
            WaitReached(reached);
            fs.Gate = null;
            cts.Cancel();
            gate.SetResult();
            Assert.IsTrue(((IAsyncResult)task).AsyncWaitHandle.WaitOne(TimeSpan.FromSeconds(20)), name + " completed");
            Assert.IsTrue(task.IsCanceled || task.Exception?.InnerException is OperationCanceledException,
                name + ": " + task.Status + " " + task.Exception?.InnerException);
            t.Cache.EraseUnreferencedEntries();
            Assert.AreEqual(0, t.Cache.PinnedUsage, name + ": nothing pinned after the cancelled read");
            Task again = op(CancellationToken.None);
            Assert.IsTrue(((IAsyncResult)again).AsyncWaitHandle.WaitOne(TimeSpan.FromSeconds(20)), name + " ran again");
            again.GetAwaiter().GetResult();
        }
        Assert.AreEqual(Hex(cf.Get(K(150))!), Hex(Run(cf.GetAsync(K(150)))!));
    }

    /// <summary>
    /// Pins: a corrupted data block fails Get / MultiGet / iterator moves with RocksDbException(Corruption) through the
    /// async API as through the sync API (same type and status code, never an AggregateException), with the async reads
    /// completing on other threads.
    /// </summary>
    [TestMethod]
    public void CorruptionSurfacesWithTheSameStatusOnAsyncPaths()
    {
        var fs = new AsyncReadFileSystem(NewFs());
        using TestDb t = BuildPlainDb(fs);
        ColumnFamily cf = t.Db.DefaultColumnFamily;
        string sst = "";
        foreach (string f in fs.GetChildren(DbPath))
        {
            if (f.EndsWith(".sst", StringComparison.Ordinal)) sst = DbPath + "/" + Path.GetFileName(f);
        }
        Assert.AreNotEqual("", sst);
        // Flip bytes inside the first data block (blocks start at offset 0).
        fs.WriteAt(sst, 40, B("XXXXXXXX"));
        t.Cache.EraseUnreferencedEntries();
        byte[] key = K(0);
        string s = GetSync(cf, null, key);
        Assert.AreEqual("RocksDbException:" + StatusCode.Corruption, s);
        t.Cache.EraseUnreferencedEntries();
        Assert.AreEqual(s, GetAsyncOutcome(cf, null, key));
        Assert.AreEqual(s, GetPooledAsyncOutcome(cf, null, key));
        Assert.AreEqual(s, EntityAsync(cf, null, key));
        using var bs = new MultiGetBatch();
        using var ba = new MultiGetBatch();
        bs.Add(key).Add(K(399));
        ba.Add(key).Add(K(399));
        Assert.AreEqual(MultiGetOutcome(cf, null, bs, false, async: false), MultiGetOutcome(cf, null, ba, false, async: true));
        using Iterator si = cf.NewIterator(null);
        using Iterator ai = cf.NewIterator(null);
        string se = MoveOutcome(si, Move.First, key, async: false);
        Assert.AreEqual("RocksDbException:" + StatusCode.Corruption, se);
        Assert.AreEqual(se, MoveOutcome(ai, Move.First, key, async: true));
        Assert.IsTrue(fs.AsyncReads > 0);
    }

    // ------------------------------------------------------------------ concurrency

    /// <summary>
    /// Pins: many concurrent async readers (GetAsync, GetPooledAsync, MultiGetAsync, ScanAsync on their own iterators)
    /// whose file reads complete on other threads and out of order get the sync results; pooled contexts, batches and
    /// values are not shared between them.
    /// </summary>
    [TestMethod]
    public void ConcurrentAsyncReadersGetTheSyncResults()
    {
        int seed = Environment.TickCount;
        var fs = new AsyncReadFileSystem(NewFs());
        using TestDb t = BuildDb(seed, fs, partitioned: seed % 2 == 0);
        ColumnFamily cf = t.Db.DefaultColumnFamily;
        var expected = new string[KeySpace];
        for (int i = 0; i < KeySpace; i++) expected[i] = GetSync(cf, null, K(i));
        List<string> scanExpected = SyncRange(cf, null, K(100), K(400));
        var tasks = new List<Task>();
        for (int w = 0; w < 16; w++)
        {
            int worker = w;
            tasks.Add(Task.Run(async () =>
            {
                var rnd = new Random(seed + worker);
                using var batch = new MultiGetBatch();
                for (int round = 0; round < 60; round++)
                {
                    if (rnd.Next(20) == 0) t.Cache.EraseUnreferencedEntries();
                    int k = rnd.Next(KeySpace);
                    switch (worker % 4)
                    {
                        case 0:
                            byte[]? v = await cf.GetAsync(K(k)).ConfigureAwait(false);
                            Assert.AreEqual(expected[k], v is null ? "null" : Hex(v), "seed=" + seed + " GetAsync " + k);
                            break;
                        case 1:
                            using (PooledValue? pv = await cf.GetPooledAsync(K(k)).ConfigureAwait(false))
                            {
                                Assert.AreEqual(expected[k], pv is null ? "null" : Hex(pv.Span), "seed=" + seed + " GetPooledAsync " + k);
                            }
                            break;
                        case 2:
                            batch.Clear();
                            for (int j = 0; j < 8; j++) batch.Add(K((k + j * 37) % KeySpace));
                            await cf.MultiGetAsync(batch).ConfigureAwait(false);
                            for (int j = 0; j < 8; j++)
                            {
                                int kk = (k + j * 37) % KeySpace;
                                string got = batch.GetStatus(j) == MultiGetStatus.Found ? Hex(batch.GetValue(j)) : "null";
                                Assert.AreEqual(expected[kk], got, "seed=" + seed + " MultiGetAsync " + kk);
                            }
                            break;
                        default:
                            if (round % 10 != 0) break;
                            var got2 = new List<string>();
                            using (Iterator it = await cf.NewIteratorAsync(null).ConfigureAwait(false))
                            {
                                await Collect(it.ScanAsync(K(100), K(400)), got2).ConfigureAwait(false);
                            }
                            CollectionAssert.AreEqual(scanExpected, got2, "seed=" + seed + " ScanAsync");
                            break;
                    }
                }
            }));
        }
        Assert.IsTrue(Task.WaitAll(tasks.ToArray(), TimeSpan.FromSeconds(60)), "readers finished");
    }
}
