using System.Buffers;
using RocksDb.Managed.Cache;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Env;
using RocksDb.Managed.Tests.Db;

namespace RocksDb.Managed.Tests.Cache;

/// <summary>
/// The table reader's use of the cache tiers: batched MultiGet secondary lookups (one WaitAll per batch, as
/// BlockBasedTable::MultiGet), compressed admission to a three-queue tiered cache (PutDataBlockToCache passing the
/// compressed block to Cache::Insert) and async persistent cache reads.
/// </summary>
[TestClass]
public sealed class CacheTierReadPathTests
{
    /// <summary>
    /// A non-volatile tier stand-in: keeps saved values (uncompressed or compressed), lookups without wait stay pending
    /// until waited for. Records the order of lookups and waits and the InsertSaved calls.
    /// </summary>
    private sealed class NvmTier : SecondaryCache
    {
        private readonly Dictionary<CacheKey, (byte[] Data, CompressionType Type)> _data = new();
        private int _lookups, _pendingLookups, _waitAllCalls, _waitAllHandles, _lookupsAtFirstWait = -1, _insertSaved, _compressedInserts;

        public override string Name => "NvmTier";

        public int Lookups => Volatile.Read(ref _lookups);
        public int PendingLookups => Volatile.Read(ref _pendingLookups);
        public int WaitAllCalls => Volatile.Read(ref _waitAllCalls);
        public int WaitAllHandles => Volatile.Read(ref _waitAllHandles);
        public int LookupsAtFirstWait => Volatile.Read(ref _lookupsAtFirstWait);
        public int InsertSavedCalls => Volatile.Read(ref _insertSaved);
        public int CompressedInserts => Volatile.Read(ref _compressedInserts);

        public void ResetCounters()
        {
            _lookups = _pendingLookups = _waitAllCalls = _waitAllHandles = _insertSaved = _compressedInserts = 0;
            _lookupsAtFirstWait = -1;
        }

        public (byte[] Data, CompressionType Type) Get(in CacheKey key)
        {
            lock (_data) return _data[key];
        }

        private void FirstWait() => Interlocked.CompareExchange(ref _lookupsAtFirstWait, Volatile.Read(ref _lookups), -1);

        public override void Insert(ReadOnlySpan<byte> key, in CacheValue value, CacheItemHelper helper, bool forceInsert)
        {
            byte[] saved = new byte[helper.GetSaveSize(value)];
            helper.SaveTo(value, saved);
            lock (_data) _data[CacheKey.Read(key)] = (saved, CompressionType.None);
        }

        public override void InsertSaved(ReadOnlySpan<byte> key, ReadOnlySpan<byte> saved, CompressionType type, CacheTier source)
        {
            Interlocked.Increment(ref _insertSaved);
            if (type != CompressionType.None) Interlocked.Increment(ref _compressedInserts);
            lock (_data) _data[CacheKey.Read(key)] = (saved.ToArray(), type);
        }

        private sealed class Pending(NvmTier owner, byte[] data, CompressionType type, CacheItemHelper helper, object? ctx) : SecondaryCacheResultHandle
        {
            private volatile bool _ready;
            private CacheValue _value;
            private long _size;
            private bool _has;

            public void Complete()
            {
                if (_ready) return;
                _has = helper.TryCreate(data, type, CacheTier.NonVolatileBlockTier, ctx, out _value, out _size);
                _ready = true;
            }

            public override bool IsReady => _ready;

            public override void Wait()
            {
                owner.FirstWait();
                Complete();
            }

            public override async ValueTask WaitAsync(CancellationToken cancellationToken)
            {
                owner.FirstWait();
                await Task.Yield();
                Complete();
            }

            public override bool HasValue => _has;
            public override CacheValue Value => _value;
            public override long Size => _size;
        }

        public override SecondaryCacheResultHandle? Lookup(ReadOnlySpan<byte> key, CacheItemHelper helper, object? createContext, bool wait,
            bool adviseErase, Statistics? stats, out bool keptInSecondaryCache)
        {
            Interlocked.Increment(ref _lookups);
            keptInSecondaryCache = true;
            (byte[] Data, CompressionType Type) d;
            lock (_data)
            {
                if (!_data.TryGetValue(CacheKey.Read(key), out d)) return null;
            }
            var p = new Pending(this, d.Data, d.Type, helper, createContext);
            if (wait) p.Complete();
            else Interlocked.Increment(ref _pendingLookups);
            return p;
        }

        public override void WaitAll(ReadOnlySpan<SecondaryCacheResultHandle> handles)
        {
            Interlocked.Increment(ref _waitAllCalls);
            Interlocked.Add(ref _waitAllHandles, handles.Length);
            base.WaitAll(handles);
        }

        public override bool SupportForceErase => false;

        public override void Erase(ReadOnlySpan<byte> key) { }
    }

    private static ColumnFamilyOptions CfOptions(BlockCache cache, CompressionType compression, PersistentCache? pc = null) => new()
    {
        TableOptions = new BlockBasedTableOptions { BlockCache = cache, BlockSize = 1024, PersistentCache = pc },
        Compression = compression,
    };

    private const int N = 2000;

    private static void Fill(RocksDatabase db)
    {
        for (int i = 0; i < N; i++) db.Put(DbTestUtil.Key(i), DbTestUtil.Value(i, 1));
        db.Flush();
        db.CompactRange(null, null);
    }

    private static void ReadAll(RocksDatabase db)
    {
        for (int i = 0; i < N; i++) CollectionAssert.AreEqual(DbTestUtil.Value(i, 1), db.Get(DbTestUtil.Key(i)), "key " + i);
    }

    /// <summary>Fills a DB, then puts every data block into <paramref name="nvm"/> with a cache dump / load.</summary>
    private static int FillAndWarm(MemoryFileSystem fs, NvmTier nvm)
    {
        BlockCache big = BlockCache.CreateLru(new LruCacheOptions { Capacity = 64L << 20, NumShardBits = 0 });
        using (RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, DbTestUtil.NewOptions(fs), CfOptions(big, CompressionType.None)))
        {
            Fill(db);
            ReadAll(db);
            using CacheDumper d = CacheDump.NewDefaultDumper(new CacheDumpOptions(), big, CacheDump.NewToFileWriter(fs, "/dump"));
            d.DumpCacheEntriesToWriter();
        }
        using (CacheDumpedLoader l = CacheDump.NewDefaultLoader(new CacheDumpOptions(), null, nvm, CacheDump.NewFromFileReader(fs, "/dump")))
            l.RestoreCacheEntriesToSecondaryCache();
        int blocks = nvm.InsertSavedCalls;
        nvm.ResetCounters();
        return blocks;
    }

    private static MultiGetBatch EveryKey(int step)
    {
        var batch = new MultiGetBatch();
        for (int i = 0; i < N; i += step) batch.Add(DbTestUtil.Key(i));
        return batch;
    }

    private static void CheckBatch(MultiGetBatch batch, int step)
    {
        for (int j = 0, i = 0; i < N; i += step, j++)
        {
            Assert.AreEqual(MultiGetStatus.Found, batch.GetStatus(j), "key " + i);
            CollectionAssert.AreEqual(DbTestUtil.Value(i, 1), batch.GetValue(j).ToArray(), "key " + i);
        }
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task MultiGetWaitsOnceForAllSecondaryLookups(bool async)
    {
        MemoryFileSystem fs = DbTestUtil.NewFs();
        var nvm = new NvmTier();
        int blocks = FillAndWarm(fs, nvm);
        Assert.IsTrue(blocks > 50, "blocks " + blocks);
        var stats = new Statistics();
        BlockCache cache = BlockCache.WithSecondaryCache(BlockCache.CreateLru(new LruCacheOptions { Capacity = 64L << 20, NumShardBits = 0 }), nvm);
        DbOptions o = DbTestUtil.NewOptions(fs);
        o.Statistics = stats;
        using RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, o, CfOptions(cache, CompressionType.None));
        // Every key: consecutive keys share blocks, each block is looked up once.
        using MultiGetBatch batch = EveryKey(1);
        if (async) await db.MultiGetAsync(batch);
        else db.MultiGet(batch);
        CheckBatch(batch, 1);
        // One secondary lookup per data block, all started before the first wait (one WaitAll for the batch).
        Assert.AreEqual(blocks, nvm.Lookups);
        Assert.AreEqual(blocks, nvm.PendingLookups);
        Assert.AreEqual(blocks, nvm.LookupsAtFirstWait);
        if (!async)
        {
            Assert.AreEqual(1, nvm.WaitAllCalls);
            Assert.AreEqual(blocks, nvm.WaitAllHandles);
        }
        Assert.AreEqual((ulong)blocks, stats.GetTickerCount(Ticker.SecondaryCacheDataHits));
        Assert.AreEqual(0UL, stats.GetTickerCount(Ticker.BlockCacheDataMiss));

        // Now in the primary cache: a second batch finds everything there without touching the secondary.
        nvm.ResetCounters();
        using MultiGetBatch again = EveryKey(3);
        if (async) await db.MultiGetAsync(again);
        else db.MultiGet(again);
        CheckBatch(again, 3);
        Assert.AreEqual(0, nvm.Lookups);
        Assert.AreEqual(0, nvm.WaitAllCalls);
    }

    [TestMethod]
    public async Task MultiGetReadsBlocksMissingFromEveryTier()
    {
        // The secondary holds nothing: lookups miss, the blocks are read from the file (counted as misses once).
        MemoryFileSystem fs = DbTestUtil.NewFs();
        var nvm = new NvmTier();
        var stats = new Statistics();
        BlockCache cache = BlockCache.WithSecondaryCache(BlockCache.CreateLru(new LruCacheOptions { Capacity = 64L << 20, NumShardBits = 0 }), nvm);
        DbOptions o = DbTestUtil.NewOptions(fs);
        o.Statistics = stats;
        using RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, o, CfOptions(cache, CompressionType.None));
        Fill(db);
        ulong missesBefore = stats.GetTickerCount(Ticker.BlockCacheDataMiss);
        using MultiGetBatch batch = EveryKey(2);
        await db.MultiGetAsync(batch);
        CheckBatch(batch, 2);
        ulong misses = stats.GetTickerCount(Ticker.BlockCacheDataMiss) - missesBefore;
        Assert.IsTrue(misses > 50);
        Assert.AreEqual((int)misses, nvm.Lookups, "one lookup per block, no second lookup before the read");
        Assert.AreEqual(0UL, stats.GetTickerCount(Ticker.SecondaryCacheHits));
        // Keys missing from the table, mixed with present ones, in a synchronous batch.
        using var mixed = new MultiGetBatch();
        for (int i = 0; i < N; i += 97)
        {
            mixed.Add(DbTestUtil.Key(i));
            mixed.Add(DbTestUtil.B("key" + i.ToString("D6", System.Globalization.CultureInfo.InvariantCulture) + "x"));
        }
        db.MultiGet(mixed);
        for (int j = 0; j < mixed.Count; j++) Assert.AreEqual(j % 2 == 0 ? MultiGetStatus.Found : MultiGetStatus.NotFound, mixed.GetStatus(j));
    }

    [TestMethod]
    public async Task ConcurrentMultiGetsThroughAPendingSecondaryTier()
    {
        MemoryFileSystem fs = DbTestUtil.NewFs();
        var nvm = new NvmTier();
        FillAndWarm(fs, nvm);
        // A small primary: blocks keep cycling between the primary and the (never erasing) secondary.
        BlockCache cache = BlockCache.WithSecondaryCache(BlockCache.CreateLru(new LruCacheOptions { Capacity = 48 * 1024, NumShardBits = 1 }), nvm);
        using RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, DbTestUtil.NewOptions(fs), CfOptions(cache, CompressionType.None));
        var tasks = new Task[4];
        for (int t = 0; t < tasks.Length; t++)
        {
            int seed = t;
            tasks[t] = Task.Run(async () =>
            {
                var rnd = new Random(seed);
                for (int round = 0; round < 30; round++)
                {
                    using var batch = new MultiGetBatch();
                    var expected = new List<int>();
                    int start = rnd.Next(N - 200);
                    for (int i = start; i < start + 200; i += 1 + rnd.Next(5))
                    {
                        batch.Add(DbTestUtil.Key(i));
                        expected.Add(i);
                    }
                    if (round % 2 == 0) await db.MultiGetAsync(batch);
                    else db.MultiGet(batch);
                    for (int j = 0; j < expected.Count; j++) CollectionAssert.AreEqual(DbTestUtil.Value(expected[j], 1), batch.GetValue(j).ToArray());
                }
            });
        }
        await Task.WhenAll(tasks);
        Assert.IsTrue(nvm.Lookups > 0);
    }

    // ---- compressed admission (kAdmPolicyThreeQueue) ---------------------------------------------------------

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task ThreeQueueTierReceivesCompressedBlocksReadFromFiles(bool async)
    {
        MemoryFileSystem fs = DbTestUtil.NewFs();
        var nvm = new NvmTier();
        var stats = new Statistics();
        BlockCache cache = BlockCache.CreateTiered(new TieredCacheOptions
        {
            TotalCapacity = 64L << 20, CompressedSecondaryRatio = 0.25, NvmSecondaryCache = nvm,
            LruCacheOptions = new LruCacheOptions { NumShardBits = 0 },
        })!;
        DbOptions o = DbTestUtil.NewOptions(fs);
        o.Statistics = stats;
        using RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, o, CfOptions(cache, CompressionType.LZ4));
        Fill(db);
        nvm.ResetCounters();
        if (async)
        {
            for (int i = 0; i < N; i++) CollectionAssert.AreEqual(DbTestUtil.Value(i, 1), await db.GetAsync(DbTestUtil.Key(i)));
        }
        else
        {
            ReadAll(db);
        }
        ulong dataMisses = stats.GetTickerCount(Ticker.BlockCacheDataMiss);
        Assert.IsTrue(dataMisses > 20);
        // Every data block read from the file was offered to the NVM tier compressed (LZ4 payload, no trailer).
        Assert.AreEqual((int)dataMisses, nvm.CompressedInserts);
        int checkedBlocks = 0;
        cache.ApplyToAllEntries(nvm, (NvmTier n, in CacheKey key, in CacheValue value, long charge, CacheItemHelper? helper) =>
        {
            if (helper?.Role != CacheEntryRole.DataBlock) return;
            (byte[] data, CompressionType type) = n.Get(key);
            Assert.AreEqual(CompressionType.LZ4, type);
            var w = new Managed.Util.PooledBufferWriter(4096);
            BlockCompression.Decompress(type, data, 6, w);
            byte[] saved = new byte[helper.GetSaveSize(value)];
            helper.SaveTo(value, saved);
            CollectionAssert.AreEqual(saved, w.WrittenSpan.ToArray());
            w.Dispose();
            checkedBlocks++;
        });
        Assert.IsTrue(checkedBlocks > 20);
    }

    [TestMethod]
    public void NonThreeQueueCachesAreNotOfferedCompressedBlocks()
    {
        MemoryFileSystem fs = DbTestUtil.NewFs();
        var sec = new NvmTier();
        BlockCache cache = BlockCache.WithSecondaryCache(BlockCache.CreateLru(new LruCacheOptions { Capacity = 64L << 20, NumShardBits = 0 }), sec);
        using RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, DbTestUtil.NewOptions(fs), CfOptions(cache, CompressionType.LZ4));
        Fill(db);
        ReadAll(db);
        Assert.AreEqual(0, sec.InsertSavedCalls);
    }

    // ---- async persistent cache reads ------------------------------------------------------------------------

    /// <summary>Counts which lookup flavour the table reader uses.</summary>
    private sealed class CountingPersistentCache(PersistentCache inner) : PersistentCache
    {
        public int SyncLookups, AsyncLookups;

        public override bool Insert(ReadOnlySpan<byte> key, ReadOnlySpan<byte> data) => inner.Insert(key, data);

        public override bool TryLookup(ReadOnlySpan<byte> key, out byte[] data, out int length)
        {
            Interlocked.Increment(ref SyncLookups);
            return inner.TryLookup(key, out data, out length);
        }

        public override ValueTask<PersistentCacheLookupResult> LookupAsync(ReadOnlyMemory<byte> key, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref AsyncLookups);
            return inner.LookupAsync(key, cancellationToken);
        }

        public override bool IsCompressed => inner.IsCompressed;
        public override IReadOnlyList<IReadOnlyDictionary<string, double>> Stats() => inner.Stats();
        public override string GetPrintableOptions() => inner.GetPrintableOptions();
    }

    [TestMethod]
    [DataRow(true)]
    [DataRow(false)]
    public async Task AsyncReadsUseAsyncPersistentCacheLookups(bool compressedTier)
    {
        string dir = Path.Combine(Path.GetTempPath(), "pcache-" + Guid.NewGuid().ToString("N"));
        var tier = new BlockCacheTier(new PersistentCacheConfig(LocalFileSystem.Instance, dir, 16L << 20, writeBufferSize: 16 * 1024)
        {
            CacheFileSize = 256 * 1024, IsCompressed = compressedTier,
        });
        try
        {
            tier.Open();
            var pc = new CountingPersistentCache(tier);
            MemoryFileSystem fs = DbTestUtil.NewFs();
            var stats = new Statistics();
            DbOptions o = DbTestUtil.NewOptions(fs);
            o.Statistics = stats;
            BlockCache cache = BlockCache.CreateLru(new LruCacheOptions { Capacity = 16 * 1024, NumShardBits = 0 });
            using RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, o, CfOptions(cache, CompressionType.LZ4, pc));
            Fill(db);
            for (int i = 0; i < N; i++) CollectionAssert.AreEqual(DbTestUtil.Value(i, 1), await db.GetAsync(DbTestUtil.Key(i)));
            tier.Flush(); // the inserted pages are now in closed cache files (read from disk) or write buffers
            int syncBefore = pc.SyncLookups;
            for (int pass = 0; pass < 2; pass++)
            {
                for (int i = 0; i < N; i++) CollectionAssert.AreEqual(DbTestUtil.Value(i, 1), await db.GetAsync(DbTestUtil.Key(i)));
                using MultiGetBatch batch = EveryKey(5);
                await db.MultiGetAsync(batch);
                CheckBatch(batch, 5);
            }
            Assert.AreEqual(syncBefore, pc.SyncLookups, "async reads never look the persistent cache up synchronously");
            Assert.IsTrue(pc.AsyncLookups > 0);
            Assert.IsTrue(stats.GetTickerCount(Ticker.PersistentCacheHit) > 0, "persistent cache hits");
            Assert.IsTrue(tier.Stats()[^1]["persistentcache.blockcachetier.bytes_read"] > 0);
            Assert.AreEqual(0.0, tier.Stats()[^1]["persistentcache.blockcachetier.cache_errors"]);
            // Synchronous reads still use the synchronous lookup.
            ReadAll(db);
            Assert.IsTrue(pc.SyncLookups > syncBefore);
        }
        finally
        {
            tier.Close();
            if (Directory.Exists(dir)) Directory.Delete(dir, true);
        }
    }

    [TestMethod]
    public async Task BlockCacheTierLookupAsyncReadsBuffersFilesAndNextTier()
    {
        string dir = Path.Combine(Path.GetTempPath(), "pcache-" + Guid.NewGuid().ToString("N"));
        var tier = new BlockCacheTier(new PersistentCacheConfig(LocalFileSystem.Instance, dir, 4L << 20, writeBufferSize: 16 * 1024)
        {
            CacheFileSize = 256 * 1024, PipelineWrites = false,
        });
        var next = new VolatileCacheTier();
        tier.NextTier = next;
        try
        {
            tier.Open();
            static byte[] KeyOf(int i)
            {
                byte[] k = new byte[CacheKey.Size];
                new CacheKey(5, (ulong)i).WriteTo(k);
                return k;
            }
            static byte[] PageOf(int i)
            {
                byte[] b = new byte[3000 + i];
                new Random(i).NextBytes(b);
                return b;
            }
            for (int i = 0; i < 300; i++)
            {
                if (!tier.Insert(KeyOf(i), PageOf(i)))
                {
                    tier.Flush(); // TryAgain: wait for the dispatched writes
                    Assert.IsTrue(tier.Insert(KeyOf(i), PageOf(i)));
                }
            }
            tier.Flush();
            Assert.IsTrue(tier.FileCount > 2, "closed files are read from disk, the last one from its buffers");
            next.Insert(KeyOf(9999), PageOf(7));
            var lookups = new Task<PersistentCacheLookupResult>[301];
            for (int i = 0; i < 300; i++) lookups[i] = tier.LookupAsync(KeyOf(i), CancellationToken.None).AsTask();
            lookups[300] = tier.LookupAsync(KeyOf(9999), CancellationToken.None).AsTask();
            await Task.WhenAll(lookups);
            for (int i = 0; i < 300; i++)
            {
                PersistentCacheLookupResult r = lookups[i].Result;
                Assert.IsTrue(r.Found, "key " + i);
                CollectionAssert.AreEqual(PageOf(i), r.Data.AsSpan(0, r.Length).ToArray());
                ArrayPool<byte>.Shared.Return(r.Data);
            }
            PersistentCacheLookupResult fromNext = lookups[300].Result;
            Assert.IsTrue(fromNext.Found, "a miss goes to the next tier");
            CollectionAssert.AreEqual(PageOf(7), fromNext.Data.AsSpan(0, fromNext.Length).ToArray());
            ArrayPool<byte>.Shared.Return(fromNext.Data);
            Assert.IsFalse((await tier.LookupAsync(KeyOf(12345), CancellationToken.None)).Found);
            Assert.AreEqual(300.0, tier.Stats()[^1]["persistentcache.blockcachetier.cache_hits"]);
        }
        finally
        {
            tier.Close();
            if (Directory.Exists(dir)) Directory.Delete(dir, true);
        }
    }
}
