using RocksDb.Managed.Cache;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Table;

/// <summary>
/// ReadOptions::auto_readahead_size (BlockBasedTableIterator::BlockCacheLookupForReadAheadSize,
/// FilePrefetchBuffer::ReadAheadSizeTuning). Ports of file/prefetch_test.cc: PrefetchTrimReadaheadTestParam
/// (PrefixSameAsStart, SeekToFirstAfterSeekIgnoresStalePrefix{,AsyncIO}, IterateUpperBoundAtEndOfIndex),
/// PrefetchTest.PrefetchWithBlockLookupAutoTuneTest and PrefetchWithBlockLookupAutoTuneWithPrev. Their MockFS without
/// prefetch support is <see cref="MemoryFileSystem"/>, whose files have no Prefetch: the implicit auto readahead uses the
/// readahead buffer, as upstream with support_prefetch = false.
/// </summary>
[TestClass]
public sealed class ReadaheadTrimmingTests
{
    private const string KPrefix = "a_prefix_";

    private sealed class Db : IDisposable
    {
        public RocksDatabase Database = null!;
        public Statistics Stats = null!;
        public LruCache Cache = null!;
        public void Dispose() => Database.Dispose();
    }

    /// <summary>
    /// PrefetchTrimReadaheadTestParam::SetGenericOptions / SetBlockBasedTableOptions: a 9-byte prefix extractor, one key
    /// per data block (block_size = 1 with the by-size flush policy), everything in one file.
    /// </summary>
    private static Db OpenTrimDb(IndexShorteningMode shortening, bool prefixExtractor = true)
    {
        var cfo = new ColumnFamilyOptions
        {
            DisableAutoCompactions = true,
            WriteBufferSize = 1L << 30,
            PrefixExtractor = prefixExtractor ? SliceTransform.CreateFixedPrefix(KPrefix.Length) : null,
        };
        var cache = new LruCache(new LruCacheOptions { Capacity = 8L << 20 });
        cfo.TableOptions.BlockCache = cache;
        cfo.TableOptions.IndexShortening = shortening;
        cfo.TableOptions.BlockSize = 1;
        cfo.TableOptions.FlushBlockPolicy = FlushBlockPolicyType.BySize;
        var stats = new Statistics();
        DbOptions o = NewOptions(NewFs());
        o.Statistics = stats;
        return new Db { Database = RocksDatabase.Open(DbPath, o, cfo), Stats = stats, Cache = cache };
    }

    private static string RandomString(Random rnd, int n)
    {
        var c = new char[n];
        for (int i = 0; i < n; i++) c[i] = (char)(' ' + rnd.Next(95));
        return new string(c);
    }

    private static string DiffPrefix(int i) => (char)('c' + i) + KPrefix[1..];

    [TestMethod]
    [DataRow(IndexShorteningMode.NoShortening, false)]
    [DataRow(IndexShorteningMode.NoShortening, true)]
    [DataRow(IndexShorteningMode.ShortenSeparators, false)]
    [DataRow(IndexShorteningMode.ShortenSeparators, true)]
    [DataRow(IndexShorteningMode.ShortenSeparatorsAndSuccessor, false)]
    [DataRow(IndexShorteningMode.ShortenSeparatorsAndSuccessor, true)]
    public void PrefixSameAsStart(IndexShorteningMode shortening, bool autoReadaheadSize)
    {
        using Db d = OpenTrimDb(shortening);
        var rnd = new Random(309);
        using (Managed.Db.WriteBatch batch = d.Database.CreateWriteBatch())
        {
            for (int i = 0; i < 10; i++) batch.Put(B(KPrefix + i), B(RandomString(rnd, 100)));
            for (int i = 0; i < 10; i++) batch.Put(B(DiffPrefix(i) + i), B(RandomString(rnd, 100)));
            d.Database.Write(batch);
        }
        d.Database.Flush();

        // A large readahead size wastes reads past the prefix without trimming.
        var ro = new ReadOptions { PrefixSameAsStart = true, AutoReadaheadSize = autoReadaheadSize, ReadaheadSize = 1UL << 30 };
        d.Stats.Reset();
        int n = 0;
        using (Iterator it = d.Database.NewIterator(ro))
        {
            for (it.Seek(B(KPrefix)); it.Valid; it.Next()) n++;
        }
        Assert.AreEqual(10, n);
        ulong trimmed = d.Stats.GetTickerCount(Ticker.ReadaheadTrimmed);
        ulong bytes = d.Stats.GetTickerCount(Ticker.NonLastLevelReadBytes) + d.Stats.GetTickerCount(Ticker.LastLevelReadBytes);
        if (autoReadaheadSize)
        {
            Assert.IsTrue(trimmed > 0, "trimmed " + trimmed);
            // Ten blocks of one ~120-byte entry: nothing of the other prefixes' blocks is read.
            Assert.IsTrue(bytes < 2000, "bytes read " + bytes);
        }
        else
        {
            Assert.AreEqual(0UL, trimmed);
        }
    }

    [TestMethod]
    [DataRow(IndexShorteningMode.NoShortening, false, false)]
    [DataRow(IndexShorteningMode.NoShortening, true, false)]
    [DataRow(IndexShorteningMode.ShortenSeparators, true, false)]
    [DataRow(IndexShorteningMode.ShortenSeparatorsAndSuccessor, true, false)]
    [DataRow(IndexShorteningMode.NoShortening, false, true)]
    [DataRow(IndexShorteningMode.NoShortening, true, true)]
    [DataRow(IndexShorteningMode.ShortenSeparators, true, true)]
    [DataRow(IndexShorteningMode.ShortenSeparatorsAndSuccessor, true, true)]
    public void SeekToFirstAfterSeekIgnoresStalePrefix(IndexShorteningMode shortening, bool autoReadaheadSize, bool asyncIo)
    {
        using Db d = OpenTrimDb(shortening);
        var rnd = new Random(309);
        using (Managed.Db.WriteBatch batch = d.Database.CreateWriteBatch())
        {
            for (int i = 0; i < 40; i++) batch.Put(B(KPrefix + S(Key(i))), B(RandomString(rnd, 100)));
            // Keys under other prefixes, so that a trim against another prefix than "a_prefix_" has somewhere to stop.
            for (int i = 0; i < 10; i++) batch.Put(B(DiffPrefix(i) + S(Key(i))), B(RandomString(rnd, 100)));
            d.Database.Write(batch);
        }
        d.Database.Flush();
        Assert.AreEqual("1", d.Database.GetProperty("rocksdb.num-files-at-level0"));

        // Small enough that the scan exhausts the buffer and re-prefetches.
        var ro = new ReadOptions { PrefixSameAsStart = true, AutoReadaheadSize = autoReadaheadSize, AsyncIo = asyncIo, ReadaheadSize = 1024 };
        static int CountFromSeekToFirst(Iterator it)
        {
            int n = 0;
            for (it.SeekToFirst(); it.Valid; it.Next())
            {
                Assert.AreEqual(KPrefix, S(it.Key.ToArray())![..KPrefix.Length]);
                n++;
            }
            return n;
        }

        // Control: SeekToFirst on an iterator with nothing stale, cold cache.
        d.Cache.EraseUnreferencedEntries();
        d.Stats.Reset();
        int control;
        using (Iterator it = d.Database.NewIterator(ro)) control = CountFromSeekToFirst(it);
        ulong trimmedControl = d.Stats.GetTickerCount(Ticker.ReadaheadTrimmed);

        // The same scan from an equally cold cache after a Seek past the last key, which sets the trimming prefix.
        d.Cache.EraseUnreferencedEntries();
        d.Stats.Reset();
        int afterSeek;
        using (Iterator it = d.Database.NewIterator(ro))
        {
            it.Seek(B('z' + KPrefix[1..]));
            Assert.IsFalse(it.Valid);
            Assert.AreEqual(0UL, d.Stats.GetTickerCount(Ticker.BlockCacheDataMiss), "the Seek reads no data block");
            Assert.AreEqual(0UL, d.Stats.GetTickerCount(Ticker.ReadaheadTrimmed));
            afterSeek = CountFromSeekToFirst(it);
        }
        ulong trimmedAfterSeek = d.Stats.GetTickerCount(Ticker.ReadaheadTrimmed);

        Assert.AreEqual(40, control);
        if (autoReadaheadSize) Assert.IsTrue(trimmedControl > 0);
        else Assert.AreEqual(0UL, trimmedControl);
        Assert.AreEqual(control, afterSeek);
        Assert.AreEqual(trimmedControl, trimmedAfterSeek);
    }

    [TestMethod]
    [DataRow(IndexShorteningMode.NoShortening, false)]
    [DataRow(IndexShorteningMode.NoShortening, true)]
    [DataRow(IndexShorteningMode.ShortenSeparatorsAndSuccessor, true)]
    public void IterateUpperBoundAtEndOfIndex(IndexShorteningMode shortening, bool autoReadaheadSize)
    {
        using Db d = OpenTrimDb(shortening, prefixExtractor: false);
        var rnd = new Random(309);
        for (int i = 0; i < 64; i++) d.Database.Put(B("key" + i), B(RandomString(rnd, 100)));
        d.Database.Flush();
        var ro = new ReadOptions { AsyncIo = true, AutoReadaheadSize = autoReadaheadSize, ReadaheadSize = 1UL << 20, IterateUpperBound = B("keyz") };
        d.Stats.Reset();
        int n = 0;
        using (Iterator it = d.Database.NewIterator(ro))
        {
            for (it.Seek(B("key0")); it.Valid; it.Next()) n++;
        }
        Assert.AreEqual(64, n);
        ulong trimmed = d.Stats.GetTickerCount(Ticker.ReadaheadTrimmed);
        if (autoReadaheadSize) Assert.IsTrue(trimmed > 0);
        else Assert.AreEqual(0UL, trimmed);
    }

    // ------------------------------------------------------------------------------------------------------------
    // PrefetchTest.PrefetchWithBlockLookupAutoTuneTest / PrefetchWithBlockLookupAutoTuneWithPrev: scans with the lookup
    // return what scans without it return, with part of the blocks cached.

    private const string MyPrefix = "my_key_";

    private static Db OpenAutoTuneDb(ulong numFileReadsForAutoReadahead)
    {
        var cfo = new ColumnFamilyOptions { DisableAutoCompactions = true, PrefixExtractor = SliceTransform.CreateFixedPrefix(MyPrefix.Length) };
        var cache = new LruCache(new LruCacheOptions { Capacity = 1L << 20, NumShardBits = 2 });
        cfo.TableOptions.BlockCache = cache;
        cfo.TableOptions.NumFileReadsForAutoReadahead = numFileReadsForAutoReadahead;
        var stats = new Statistics();
        DbOptions o = NewOptions(NewFs());
        o.Statistics = stats;
        var db = RocksDatabase.Open(DbPath, o, cfo);
        // Keys "my_key_a" .. "my_key_aaaaaaaaaa" to "my_key_zzzzzzzzzz", 1000-byte values, compacted.
        var rnd = new Random(309);
        using (Managed.Db.WriteBatch batch = db.CreateWriteBatch())
        {
            for (int i = 0; i < 26; i++)
            {
                string key = MyPrefix;
                for (int j = 0; j < 10; j++)
                {
                    key += (char)('a' + i);
                    batch.Put(B(key), B(RandomString(rnd, 1000)));
                }
            }
            db.Write(batch);
        }
        db.CompactRange(null, null);
        return new Db { Database = db, Stats = stats, Cache = cache };
    }

    private enum Moves { Sync, AsyncNext, Async }

    /// <summary>
    /// PrefetchTest::VerifyScan: both iterators move in lockstep, the tuned one first, so it is the one that misses the
    /// blocks and reads them.
    /// </summary>
    private static void VerifyScan(RocksDatabase db, ReadOptions ro, ReadOptions cmp, string seekKey, string? upperBound, bool prefixSameAsStart, Moves moves)
    {
        ro.IterateUpperBound = cmp.IterateUpperBound = upperBound is null ? (ReadOnlyMemory<byte>?)null : B(upperBound);
        ro.PrefixSameAsStart = cmp.PrefixSameAsStart = prefixSameAsStart;
        using Iterator it = db.NewIterator(ro);
        using Iterator cmpIt = db.NewIterator(cmp);
        if (moves == Moves.Async) it.SeekAsync(B(seekKey)).AsTask().GetAwaiter().GetResult();
        else it.Seek(B(seekKey));
        cmpIt.Seek(B(seekKey));
        int n = 0;
        while (it.Valid && cmpIt.Valid)
        {
            CollectionAssert.AreEqual(cmpIt.Key.ToArray(), it.Key.ToArray());
            n++;
            if (moves == Moves.Sync) it.Next();
            else it.NextAsync().AsTask().GetAwaiter().GetResult();
            cmpIt.Next();
        }
        string ctx = "seek " + seekKey + " ub " + upperBound + " prefix_same_as_start " + prefixSameAsStart + " moves " + moves;
        Assert.IsFalse(it.Valid, ctx);
        Assert.IsFalse(cmpIt.Valid, ctx);
        Assert.IsTrue(n > 0, ctx);
    }

    [TestMethod]
    [DataRow(false, false)]
    [DataRow(true, false)]
    [DataRow(false, true)]
    [DataRow(true, true)]
    public void PrefetchWithBlockLookupAutoTune(bool explicitReadahead, bool asyncIo)
    {
        foreach (Moves moves in new[] { Moves.Sync, Moves.AsyncNext, Moves.Async })
        {
            for (ulong numFileReads = 0; numFileReads < 3; numFileReads++)
            {
                using Db d = OpenAutoTuneDb(numFileReads);
                // Warm up the cache.
                using (Iterator it = d.Database.NewIterator(new ReadOptions()))
                {
                    foreach (string k in new[] { "bbb", "ccccccccc", "ddd", "ddddddd", "e", "eeeee", "eeeeeeeee" })
                    {
                        it.Seek(B(MyPrefix + k));
                        Assert.IsTrue(it.Valid);
                    }
                }
                var ro = new ReadOptions { AutoReadaheadSize = true, AsyncIo = asyncIo };
                var cmp = new ReadOptions { AutoReadaheadSize = false };
                if (explicitReadahead) ro.ReadaheadSize = cmp.ReadaheadSize = 32768;
                d.Stats.Reset();
                VerifyScan(d.Database, ro, cmp, MyPrefix + "aaa", MyPrefix + "uuu", false, moves);
                VerifyScan(d.Database, ro, cmp, MyPrefix + "v", MyPrefix + "y", false, moves);
                VerifyScan(d.Database, ro, cmp, MyPrefix + "aaa", null, true, moves);
                // The synchronous moves read through the readahead buffer: it was trimmed (to the bound, to the prefix, or
                // past the cached blocks). The async moves read no readahead buffer; NextAsync after a synchronous Seek
                // continues through the buffer only once the Seek's own read used it (the lookup's handles are queued).
                if (moves == Moves.Sync || (moves == Moves.AsyncNext && (explicitReadahead || numFileReads == 0)))
                    Assert.IsTrue(d.Stats.GetTickerCount(Ticker.ReadaheadTrimmed) > 0, "moves " + moves + " num_file_reads " + numFileReads);
            }
        }
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void PrefetchWithBlockLookupAutoTuneWithPrev(bool asyncMoves)
    {
        using Db d = OpenAutoTuneDb(2);
        foreach ((string? ub, bool prefixSameAsStart) in new[] { ((string?)MyPrefix + "uuu", false), (null, true) })
        {
            var ro = new ReadOptions { AutoReadaheadSize = true, IterateUpperBound = ub is null ? (ReadOnlyMemory<byte>?)null : B(ub), PrefixSameAsStart = prefixSameAsStart };
            var cmp = new ReadOptions { AutoReadaheadSize = false, IterateUpperBound = ro.IterateUpperBound, PrefixSameAsStart = prefixSameAsStart };
            using Iterator it = d.Database.NewIterator(ro);
            using Iterator cmpIt = d.Database.NewIterator(cmp);
            byte[] seekKey = B(MyPrefix + "bbb");

            // Seek, then enough Next calls that the lookup has moved the index ahead of the current block.
            cmpIt.Seek(seekKey);
            it.Seek(seekKey);
            for (int i = 0; i < 25 && cmpIt.Valid; i++)
            {
                Assert.IsTrue(it.Valid);
                CollectionAssert.AreEqual(cmpIt.Key.ToArray(), it.Key.ToArray());
                cmpIt.Next();
                it.Next();
            }
            // Prev reseeks the index to the current block.
            for (int i = 0; i < 12; i++)
            {
                Assert.IsTrue(cmpIt.Valid);
                Assert.IsTrue(it.Valid);
                CollectionAssert.AreEqual(cmpIt.Key.ToArray(), it.Key.ToArray());
                cmpIt.Prev();
                if (asyncMoves) it.PrevAsync().AsTask().GetAwaiter().GetResult();
                else it.Prev();
            }
            Assert.IsTrue(it.Valid);
            CollectionAssert.AreEqual(cmpIt.Key.ToArray(), it.Key.ToArray());

            // A reseek follows as usual (the lookup stays off for this iterator's files: direction_ is backward).
            cmpIt.Seek(seekKey);
            it.Seek(seekKey);
            Assert.IsTrue(it.Valid);
            CollectionAssert.AreEqual(cmpIt.Key.ToArray(), it.Key.ToArray());
            while (cmpIt.Valid)
            {
                Assert.IsTrue(it.Valid);
                CollectionAssert.AreEqual(cmpIt.Key.ToArray(), it.Key.ToArray());
                cmpIt.Next();
                it.Next();
            }
            Assert.IsFalse(it.Valid);
        }
    }

    /// <summary>
    /// The lookup leaves out the cached blocks at the end of a window: after a scan of the second half of the keys, a scan
    /// of all of them reads only the first half, where a scan without auto_readahead_size reads its 1 MiB readahead
    /// window from the first block it misses, the cached second half included. Neither tuned scan reads past its bound.
    /// </summary>
    [TestMethod]
    public void CachedBlocksAtTheEndOfTheWindowAreNotRead()
    {
        (ulong Miss, ulong Hit, ulong Bytes)[] Run(bool autoReadaheadSize)
        {
            using Db d = OpenAutoTuneDb(2);
            d.Cache.EraseUnreferencedEntries();
            var result = new (ulong, ulong, ulong)[2];
            string[] starts = ["m", "a"];
            int[] rows = [130, 250];
            for (int i = 0; i < 2; i++)
            {
                d.Stats.Reset();
                var ro = new ReadOptions { ReadaheadSize = 1UL << 20, AutoReadaheadSize = autoReadaheadSize, IterateUpperBound = B(MyPrefix + "z") };
                int n = 0;
                using (Iterator it = d.Database.NewIterator(ro))
                {
                    for (it.Seek(B(MyPrefix + starts[i])); it.Valid; it.Next()) n++;
                }
                Assert.AreEqual(rows[i], n);
                result[i] = (d.Stats.GetTickerCount(Ticker.BlockCacheDataMiss), d.Stats.GetTickerCount(Ticker.BlockCacheDataHit),
                    d.Stats.GetTickerCount(Ticker.NonLastLevelReadBytes) + d.Stats.GetTickerCount(Ticker.LastLevelReadBytes));
            }
            return result;
        }
        (ulong Miss, ulong Hit, ulong Bytes)[] tuned = Run(true);
        (ulong Miss, ulong Hit, ulong Bytes)[] untuned = Run(false);
        string ctx = "tuned " + string.Join(", ", tuned) + " untuned " + string.Join(", ", untuned);
        // The same blocks are missed and hit either way: the lookup counts each block once.
        for (int i = 0; i < 2; i++)
        {
            Assert.AreEqual(untuned[i].Miss, tuned[i].Miss, ctx);
            Assert.AreEqual(untuned[i].Hit, tuned[i].Hit, ctx);
        }
        // Untuned: the first scan reads from "m" to the end of the file, the second the whole file. Tuned: "m" to "z",
        // then "a" to "m".
        Assert.IsTrue(untuned[1].Bytes > untuned[0].Bytes * 3 / 2, ctx);
        Assert.IsTrue(tuned[0].Bytes < untuned[0].Bytes, ctx);
        Assert.IsTrue(tuned[1].Bytes < untuned[1].Bytes * 6 / 10, ctx);
        Assert.IsTrue(tuned[0].Bytes + tuned[1].Bytes < untuned[1].Bytes, ctx);
    }

    /// <summary>
    /// Random iterator programs (seeks in both directions, Next / Prev, their async forms, reseeks into the current
    /// block) over several sorted runs and a block cache small enough to evict: an iterator with auto_readahead_size
    /// returns exactly what one without it returns. Seed: READAHEAD_TRIM_SEED.
    /// </summary>
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void RandomProgramsMatchUntunedIterators(bool asyncIo)
    {
        int seed = int.TryParse(Environment.GetEnvironmentVariable("READAHEAD_TRIM_SEED"), out int sd) ? sd : Environment.TickCount;
        var rnd = new Random(seed);
        string ctx0 = "seed=" + seed + " asyncIo=" + asyncIo;
        var cfo = new ColumnFamilyOptions { DisableAutoCompactions = true, PrefixExtractor = SliceTransform.CreateFixedPrefix(4) };
        var cache = new LruCache(new LruCacheOptions { Capacity = 64L << 10 });
        cfo.TableOptions.BlockCache = cache;
        cfo.TableOptions.BlockSize = 256;
        cfo.TableOptions.NumFileReadsForAutoReadahead = (ulong)rnd.Next(3);
        var stats = new Statistics();
        DbOptions options = NewOptions(NewFs());
        options.Statistics = stats;
        using var db = RocksDatabase.Open(DbPath, options, cfo);
        string RandomKey() => "p" + rnd.Next(12).ToString("D3", System.Globalization.CultureInfo.InvariantCulture) + rnd.Next(400).ToString("D4", System.Globalization.CultureInfo.InvariantCulture);
        for (int run = 0; run < 4; run++)
        {
            for (int i = 0; i < 1500; i++)
            {
                if (rnd.Next(10) == 0) db.Delete(B(RandomKey()));
                else db.Put(B(RandomKey()), B(RandomString(rnd, rnd.Next(10, 60))));
            }
            db.Flush();
            if (run == 1) db.CompactRange(null, null);
        }
        for (int program = 0; program < 40; program++)
        {
            string? ub = rnd.Next(3) == 0 ? null : RandomKey();
            bool prefixSameAsStart = rnd.Next(3) == 0;
            ulong readahead = rnd.Next(3) switch { 0 => 0UL, 1 => 2048UL, _ => 1UL << 20 };
            var ro = new ReadOptions { AutoReadaheadSize = true, AsyncIo = asyncIo, ReadaheadSize = readahead, PrefixSameAsStart = prefixSameAsStart, IterateUpperBound = ub is null ? (ReadOnlyMemory<byte>?)null : B(ub) };
            var cmp = new ReadOptions { AutoReadaheadSize = false, ReadaheadSize = readahead, PrefixSameAsStart = prefixSameAsStart, IterateUpperBound = ro.IterateUpperBound };
            using Iterator it = db.NewIterator(ro);
            using Iterator cmpIt = db.NewIterator(cmp);
            string ctx = ctx0 + " program " + program + " ub " + ub + " psas " + prefixSameAsStart + " ra " + readahead;
            for (int op = 0; op < 120; op++)
            {
                int r = rnd.Next(100);
                string what;
                if (r < 8 || !cmpIt.Valid)
                {
                    int kind = rnd.Next(5);
                    byte[] k = B(RandomKey());
                    what = kind + ":" + S(k);
                    switch (kind)
                    {
                        case 0: it.Seek(k); cmpIt.Seek(k); break;
                        case 1: it.SeekAsync(k).AsTask().GetAwaiter().GetResult(); cmpIt.Seek(k); break;
                        case 2: it.SeekForPrev(k); cmpIt.SeekForPrev(k); break;
                        case 3: it.SeekToFirst(); cmpIt.SeekToFirst(); break;
                        default: it.SeekToLast(); cmpIt.SeekToLast(); break;
                    }
                }
                else if (r < 75)
                {
                    what = "next";
                    if (rnd.Next(4) == 0) it.NextAsync().AsTask().GetAwaiter().GetResult();
                    else it.Next();
                    cmpIt.Next();
                }
                else if (r < 90)
                {
                    what = "prev";
                    if (rnd.Next(4) == 0) it.PrevAsync().AsTask().GetAwaiter().GetResult();
                    else it.Prev();
                    cmpIt.Prev();
                }
                else
                {
                    // A reseek to the current key (lands in the current block).
                    byte[] k = cmpIt.Key.ToArray();
                    what = "reseek:" + S(k);
                    it.Seek(k);
                    cmpIt.Seek(k);
                }
                Assert.AreEqual(cmpIt.Valid, it.Valid, ctx + " op " + op + " " + what);
                if (cmpIt.Valid)
                {
                    CollectionAssert.AreEqual(cmpIt.Key.ToArray(), it.Key.ToArray(), ctx + " op " + op + " " + what);
                    CollectionAssert.AreEqual(cmpIt.Value.ToArray(), it.Value.ToArray(), ctx + " op " + op + " " + what);
                }
            }
        }
        Assert.IsTrue(stats.GetTickerCount(Ticker.ReadaheadTrimmed) > 0, ctx0 + ": the programs trimmed readahead");
    }
}
