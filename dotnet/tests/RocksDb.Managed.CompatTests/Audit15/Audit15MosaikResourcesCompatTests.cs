using System.Buffers.Binary;
using System.Globalization;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Memory;
using RocksDb.Managed.TestShared;

namespace RocksDb.Managed.CompatTests.Audit15;

/// <summary>
/// Audit #15, memory, resources and background scheduling at Mosaik's sizes: the same Mosaik-shaped write and read
/// volume on <see cref="MosaikProfile"/> and <see cref="NativeMosaikProfile"/>, in both resource branches, comparing
/// flushes, WAL files, write buffer manager usage, memtable sizes and block cache usage. Flushes are waited for between
/// steps (WaitForCompact on both), so the comparison is of the engines' decisions, not of their timing.
/// </summary>
[TestClass]
public sealed class Audit15MosaikResourcesCompatTests
{
    private static readonly RocksDbSharp.Native N = RocksDbSharp.Native.Instance;

    public TestContext TestContext { get; set; } = null!;

    /// <summary>One Mosaik database per engine, opened with the two profiles.</summary>
    private sealed class Pair : IDisposable
    {
        public readonly TempDir Dir;
        public readonly MosaikProfile ManagedProfile;
        public readonly NativeMosaikProfile NativeProfile;
        public readonly RocksDatabase M;
        public readonly RocksDbSharp.RocksDb Native;
        public readonly Dictionary<string, ColumnFamily> MCf;

        public Pair(string name, bool lowMemoryMode, int processorCount, long cacheBytes, long writeBufferBytes, Action<MosaikProfile, NativeMosaikProfile>? adjust = null)
        {
            Dir = new TempDir(name);
            ManagedProfile = new MosaikProfile(Path.Combine(Dir.Path, "mwal"), lowMemoryMode, processorCount, cacheBytes: cacheBytes, writeBufferBytes: writeBufferBytes);
            NativeProfile = new NativeMosaikProfile(Path.Combine(Dir.Path, "nwal"), lowMemoryMode, processorCount, cacheBytes: cacheBytes, writeBufferBytes: writeBufferBytes);
            adjust?.Invoke(ManagedProfile, NativeProfile);
            M = ManagedProfile.Open(Path.Combine(Dir.Path, "m"));
            Native = NativeProfile.Open(Path.Combine(Dir.Path, "n"));
            MCf = MosaikProfile.Handles(M);
        }

        public void Put(string family, byte[] key, byte[] value)
        {
            MCf[family].Put(key, value);
            Native.Put(key, value, Native.GetColumnFamily(family));
        }

        public void WaitForBackgroundWork()
        {
            M.WaitForCompact();
            IntPtr o = N.rocksdb_wait_for_compact_options_create();
            try
            {
                N.rocksdb_wait_for_compact(Native.Handle, o, out IntPtr err);
                if (err != IntPtr.Zero) Assert.Fail("native WaitForCompact failed");
            }
            finally
            {
                N.rocksdb_wait_for_compact_options_destroy(o);
            }
        }

        public long ManagedFlushes => (long)ManagedProfile.DbOptions.Statistics!.GetHistogramData(Histogram.FlushTime).Count;
        public long NativeFlushes => HistogramCount(NativeProfile.DbOptions.GetStatisticsString(), "rocksdb.db.flush.micros");
        public long ManagedWbmUsage => ManagedProfile.WriteBufferManager.MemoryUsage;
        public long NativeWbmUsage => (long)(ulong)N.rocksdb_write_buffer_manager_memory_usage(NativeProfile.WriteBufferManager);
        public long ManagedProperty(string family, string property) => long.Parse(MCf[family].GetProperty(property)!, CultureInfo.InvariantCulture);
        public long NativeProperty(string family, string property) => long.Parse(Native.GetProperty(property, Native.GetColumnFamily(family)), CultureInfo.InvariantCulture);

        public void Dispose()
        {
            M.Dispose();
            Native.Dispose();
            Dir.Dispose();
        }
    }

    /// <summary>COUNT of a histogram in Statistics::ToString text (the same format on both engines).</summary>
    private static long HistogramCount(string stats, string name)
    {
        int at = stats.IndexOf(name + " ", StringComparison.Ordinal);
        Assert.IsTrue(at >= 0, name);
        int c = stats.IndexOf("COUNT : ", at, StringComparison.Ordinal) + "COUNT : ".Length;
        int e = stats.IndexOf(' ', c);
        return long.Parse(stats.AsSpan(c, e - c), CultureInfo.InvariantCulture);
    }

    /// <summary>COUNT of a ticker in Statistics::ToString text.</summary>
    private static long TickerCount(string stats, string name)
    {
        int at = stats.IndexOf(name + " COUNT : ", StringComparison.Ordinal);
        Assert.IsTrue(at >= 0, name);
        int c = at + name.Length + " COUNT : ".Length;
        int e = c;
        while (e < stats.Length && char.IsAsciiDigit(stats[e])) e++;
        return long.Parse(stats.AsSpan(c, e - c), CultureInfo.InvariantCulture);
    }

    private static int WalFiles(string dir) => Directory.Exists(dir) ? Directory.GetFiles(dir, "*.log").Length : 0;

    private static byte[] Key(ulong id, byte kind)
    {
        var k = new byte[16];
        BinaryPrimitives.WriteUInt64LittleEndian(k, id);
        k[8] = kind;
        return k;
    }

    /// <summary>A node-content-like value: half pseudo-random, half repeated bytes (about 2:1 under LZ4).</summary>
    private static byte[] Value(ulong id, int length)
    {
        var v = new byte[length];
        ulong x = id * 0x9E3779B97F4A7C15UL + 1;
        for (int i = 0; i < length / 2; i++)
        {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            v[i] = (byte)x;
        }
        for (int i = length / 2; i < length; i++) v[i] = (byte)(i % 7);
        return v;
    }

    /// <summary>"# entries" of a column family's aggregated table properties (the same text on both engines).</summary>
    private static long TableEntries(string? aggregated)
    {
        System.Text.RegularExpressions.Match m = System.Text.RegularExpressions.Regex.Match(aggregated ?? "", "(?:^|; )# entries=(\\d+)");
        return m.Success ? long.Parse(m.Groups[1].Value, CultureInfo.InvariantCulture) : 0;
    }

    // ---------------------------------------------------------------------------------------------------------------
    // 1. Low-resource branch: max_total_wal_size 1 MiB over 31 column families
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Mosaik's low-resource branch caps the WALs at 1 MiB (max_total_wal_size) over 31 column families. Each time the
    /// alive WALs pass 1 MiB, DBImpl::SwitchWAL (db_impl_write.cc) switches every column family whose log number is at
    /// most the oldest alive WAL, empty ones included, and flushes it (FlushReason kWalFull). With the flushes waited for
    /// between steps, both engines take the same decisions: every family (the default one too) is flushed once per MiB
    /// of WAL, 31 flush jobs and MANIFEST records for about three table files. Under a sustained write load the engines
    /// leave that cycle at different times (it ends only when some other memtable switch creates a WAL while the idle
    /// families have nothing unflushed), see the audit #15 report.
    /// </summary>
    [TestMethod]
    public void LowResourceWalRoundsFlushEveryFamilyLikeNative()
    {
        using var p = new Pair("a15-walrounds", lowMemoryMode: true, processorCount: 2, cacheBytes: 8L << 20, writeBufferBytes: 32L << 20);
        int families = MosaikProfile.Families.Count + 1; // + default
        var managed = new List<string>();
        var native = new List<string>();
        ulong id = 0;
        for (int step = 0; step < 48; step++)
        {
            // 64 KiB of node content plus the node and edge records written with it.
            for (int i = 0; i < 64; i++, id++)
            {
                p.Put("content", Key(id, 1), Value(id, 1024));
                if (i % 4 == 0) p.Put("nodes", Key(id, 2), Value(id, 64));
                if (i % 4 == 1) p.Put("edges", Key(id, 3), Value(id, 96));
            }
            p.WaitForBackgroundWork();
            managed.Add(p.ManagedFlushes + " flushes, " + WalFiles(p.ManagedProfile.DbOptions.WalDir!) + " WALs, "
                + TableEntries(p.MCf["content"].GetProperty("rocksdb.aggregated-table-properties")) + " content entries in tables");
            native.Add(p.NativeFlushes + " flushes, " + WalFiles(Path.Combine(p.Dir.Path, "nwal")) + " WALs, "
                + TableEntries(p.Native.GetProperty("rocksdb.aggregated-table-properties", p.Native.GetColumnFamily("content"))) + " content entries in tables");
        }
        string trace = string.Join("\n", managed) + "\n-- native --\n" + string.Join("\n", native);
        CollectionAssert.AreEqual(native, managed, trace);
        // 3 MiB of WAL: three rounds, each flushing every family.
        Assert.AreEqual(3L * families, p.NativeFlushes, trace);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // 2. Normal branch: the content family's 128 MiB arena block under the shared write buffer manager
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// The content family (arena_block_size 128 MiB, memtable whole-key bloom of 0.02 x write_buffer_size) allocates a
    /// whole 128 MiB arena block when each of its memtables is created: the 1.28 MiB bloom (DynamicBloom over the
    /// memtable's ConcurrentArena, memtable.cc) is above the shard limit and below a quarter block, so
    /// Arena::AllocateFallback takes a regular block (memory/arena.cc), and the AllocTracker reserves it in the write
    /// buffer manager. An idle Mosaik database in the normal branch therefore charges about 134.5 MiB (that block plus
    /// five 1.28 MiB blooms of the other point-lookup families). Mosaik's manager is available memory / 20, at least
    /// 64 MiB: below about 154 MiB (about 3 GB available) the mutable usage stays above 7/8 of the limit and
    /// WriteBufferManager::ShouldFlush is true on every write, so HandleWriteBufferManagerFlush (db_impl_write.cc)
    /// flushes the oldest non-empty memtable on each write. Both engines do this; with a larger manager neither does.
    /// </summary>
    [TestMethod]
    [DataRow(100L, true)]
    [DataRow(256L, false)]
    public void ContentArenaBlockKeepsSmallWriteBufferManagersFlushingLikeNative(long writeBufferMiB, bool expectFlushPerWrite)
    {
        using var p = new Pair("a15-wbm", lowMemoryMode: false, processorCount: 8, cacheBytes: 32L << 20, writeBufferBytes: writeBufferMiB << 20);
        long nOpen = p.NativeWbmUsage, mOpen = p.ManagedWbmUsage;
        string ctx = "native open=" + nOpen + " managed open=" + mOpen;
        Assert.IsTrue(nOpen > 128L << 20, ctx);
        Assert.AreEqual(nOpen, mOpen, 64 * 1024, ctx);
        const int writes = 20;
        for (int i = 0; i < writes; i++)
        {
            p.Put("nodes", Key((ulong)i, 2), Value((ulong)i, 64));
            p.WaitForBackgroundWork();
        }
        ctx += " native flushes=" + p.NativeFlushes + " managed flushes=" + p.ManagedFlushes + " native usage=" + p.NativeWbmUsage + " managed usage=" + p.ManagedWbmUsage;
        Assert.AreEqual(p.NativeFlushes, p.ManagedFlushes, ctx);
        // The first write finds every memtable empty (nothing to flush); each later one flushes the previous write.
        Assert.AreEqual(expectFlushPerWrite ? writes - 1 : 0, p.NativeFlushes, ctx);
        Assert.AreEqual(p.NativeWbmUsage, p.ManagedWbmUsage, 64 * 1024, ctx);
    }

    /// <summary>
    /// The content memtable at Mosaik's sizes (write_buffer_size 64 MiB, arena block 128 MiB): MemTable::ShouldFlushNow
    /// (memtable.cc) never allows a second block (128 + 128 MiB is not below 64 + 0.6 x 128 MiB), and the first block
    /// alone stays below 64 + 0.6 x 128 MiB, so the memtable is full only when its block has less than a quarter
    /// (32 MiB) left: about 96 MiB of arena, 1.5 x write_buffer_size. Both engines cut the memtable at the same entry
    /// (within the arena accounting differences: malloc_usable_size, shard blocks), report the same memtable sizes while
    /// it fills, and charge the write buffer manager the same once the flush is done.
    /// </summary>
    [TestMethod]
    public void ContentMemtableFlushesAtThreeQuartersOfItsArenaBlockLikeNative()
    {
        try
        {
            using var p = new Pair("a15-content", lowMemoryMode: false, processorCount: 8, cacheBytes: 32L << 20, writeBufferBytes: 1L << 30);
            long idle = p.NativeWbmUsage;
            var trace = new StringBuilder();
            ulong id = 0;
            long written = 0;
            while (p.NativeFlushes == 0 && written < (120L << 20))
            {
                for (int i = 0; i < 4096; i++, id++)
                {
                    byte[] v = Value(id, 1000 + (int)(id % 1000));
                    p.Put("content", Key(id, 1), v);
                    written += v.Length + 16;
                }
                long nCur = p.NativeProperty("content", "rocksdb.cur-size-active-mem-table");
                long mCur = p.ManagedProperty("content", "rocksdb.cur-size-active-mem-table");
                trace.Append(written >> 20).Append(" MiB: native cur-size-active ").Append(nCur).Append(", managed ").Append(mCur).Append('\n');
                if (p.NativeFlushes == 0 && p.ManagedFlushes == 0) Assert.AreEqual(nCur, mCur, 256 * 1024, trace.ToString());
            }
            p.WaitForBackgroundWork();
            long nEntries = TableEntries(p.Native.GetProperty("rocksdb.aggregated-table-properties", p.Native.GetColumnFamily("content")));
            long mEntries = TableEntries(p.MCf["content"].GetProperty("rocksdb.aggregated-table-properties"));
            trace.Append("flushed entries: native ").Append(nEntries).Append(", managed ").Append(mEntries)
                .Append("; flushes native ").Append(p.NativeFlushes).Append(", managed ").Append(p.ManagedFlushes)
                .Append("; manager idle ").Append(idle).Append(", native ").Append(p.NativeWbmUsage).Append(", managed ").Append(p.ManagedWbmUsage);
            string t = trace.ToString();
            TestContext.WriteLine(t);
            Assert.AreEqual(1, p.NativeFlushes, t);
            Assert.AreEqual(p.NativeFlushes, p.ManagedFlushes, t);
            Assert.IsTrue(nEntries > 0 && Math.Abs(nEntries - mEntries) <= nEntries / 200, t);
            // The flushed keys and values fill 70-75% of the block (the rest of the 96 MiB: skip list nodes and the bloom).
            long flushedBytes = 0;
            for (long i = 0; i < nEntries; i++) flushedBytes += 16 + 1000 + i % 1000;
            Assert.IsTrue(flushedBytes > (128L << 20) * 70 / 100 && flushedBytes < (128L << 20) * 75 / 100, t + "; flushed bytes " + flushedBytes);
            Assert.AreEqual(p.NativeWbmUsage, p.ManagedWbmUsage, 1L << 20, t);
        }
        finally
        {
            ArenaSlabPool.Trim();
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // 3. The shared LRU cache under RocksDbBase.ShrinkSharedCacheCapacity / RestoreSharedCacheCapacity
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Mosaik's shared LRU cache (NewLRUCache(capacity): default shard bits, no strict limit, high-priority pool 0.5)
    /// holds partitioned index and filter blocks at high priority with L0 partitions and top levels pinned. Under
    /// SetCapacity to a quarter (ShrinkSharedCacheCapacity under container memory pressure) and back, with reads going
    /// on, usage and pinned usage follow native's, the cache stays within the shrunk capacity, and the data block misses
    /// are the same.
    /// </summary>
    [TestMethod]
    public void SharedCacheShrinkAndRestoreReportLikeNative()
    {
        const long capacity = 32L << 20;
        using var p = new Pair("a15-cache", lowMemoryMode: false, processorCount: 8, cacheBytes: capacity, writeBufferBytes: 1L << 30);
        string[] families = ["nodes", "nodeversion", "simple-text-index-keys", "edges"];
        const int perFlush = 12000;
        for (int f = 0; f < families.Length; f++)
        {
            for (int flush = 0; flush < 3; flush++)
            {
                for (int i = 0; i < perFlush; i++)
                {
                    ulong id = (ulong)(flush * perFlush + i);
                    p.Put(families[f], Key(id * 7 + (ulong)f, (byte)flush), Value(id, 100 + (int)(id % 300)));
                }
                p.MCf[families[f]].Flush();
                N.rocksdb_flush_cf(p.Native.Handle, new RocksDbSharp.FlushOptions().SetWaitForFlush(true).Handle, p.Native.GetColumnFamily(families[f]).Handle, out IntPtr err);
                Assert.AreEqual(IntPtr.Zero, err);
            }
        }
        // edges: compacted to the bottom level (no pinned L0 partitions there).
        p.MCf["edges"].CompactRange(null, null);
        p.Native.CompactRange((byte[]?)null, null, p.Native.GetColumnFamily("edges"));
        p.WaitForBackgroundWork();
        var trace = new StringBuilder();
        void Read(int stride)
        {
            var buf = new byte[512];
            foreach (string f in families)
            {
                ColumnFamily mcf = p.MCf[f];
                RocksDbSharp.ColumnFamilyHandle ncf = p.Native.GetColumnFamily(f);
                int fi = Array.IndexOf(families, f);
                for (int flush = 0; flush < 3; flush++)
                {
                    for (int i = 0; i < perFlush; i += stride)
                    {
                        ulong id = (ulong)(flush * perFlush + i);
                        byte[] k = Key(id * 7 + (ulong)fi, (byte)flush);
                        Assert.IsTrue(mcf.TryGet(k, buf, out _));
                        Assert.IsNotNull(p.Native.Get(k, ncf));
                    }
                }
            }
        }
        (long Usage, long Pinned, long NUsage, long NPinned) Snap(string what)
        {
            long mu = p.ManagedProfile.Cache.Usage, mp = p.ManagedProfile.Cache.PinnedUsage;
            long nu = (long)p.NativeProfile.Cache.GetUsage(), np = (long)p.NativeProfile.Cache.GetPinnedUsage();
            trace.Append(what).Append(": usage native ").Append(nu).Append(" managed ").Append(mu)
                .Append(", pinned native ").Append(np).Append(" managed ").Append(mp).Append('\n');
            string ns = p.NativeProfile.DbOptions.GetStatisticsString();
            Statistics ms = p.ManagedProfile.DbOptions.Statistics!;
            foreach ((string name, Ticker add, Ticker bytes) in new[] { ("data", Ticker.BlockCacheDataAdd, Ticker.BlockCacheDataBytesInsert), ("index", Ticker.BlockCacheIndexAdd, Ticker.BlockCacheIndexBytesInsert), ("filter", Ticker.BlockCacheFilterAdd, Ticker.BlockCacheFilterBytesInsert) })
            {
                trace.Append("  ").Append(name).Append(" adds native ").Append(TickerCount(ns, "rocksdb.block.cache." + name + ".add")).Append(" bytes ").Append(TickerCount(ns, "rocksdb.block.cache." + name + ".bytes.insert"))
                    .Append("; managed ").Append(ms.GetTickerCount(add)).Append(" bytes ").Append(ms.GetTickerCount(bytes)).Append('\n');
            }
            return (mu, mp, nu, np);
        }
        Snap("open");
        Read(5);
        var full = Snap("read");
        p.ManagedProfile.Cache.SetCapacity(capacity / 4);
        p.NativeProfile.Cache.SetCapacity((ulong)(capacity / 4));
        var shrunk = Snap("shrunk");
        Read(3);
        var shrunkRead = Snap("read while shrunk");
        p.ManagedProfile.Cache.SetCapacity(capacity);
        p.NativeProfile.Cache.SetCapacity((ulong)capacity);
        Read(2);
        var restored = Snap("restored, read");
        string t = trace.ToString();
        TestContext.WriteLine(t);
        foreach (var s in new[] { full, shrunk, shrunkRead, restored })
        {
            // Data blocks are charged their buffer (managed: the pool's size class; native: malloc_usable_size), about
            // 1% apart for 4 KiB blocks; index and filter partitions up to 10% apart, a small share of the cache.
            Assert.AreEqual(s.NUsage, s.Usage, s.NUsage / 50 + (64 << 10), t);
            Assert.AreEqual(s.NPinned, s.Pinned, s.NPinned / 10 + (16 << 10), t);
            Assert.IsTrue(s.Pinned > 0, t);
        }
        // SetCapacity evicts down to the new capacity at once, and reads while shrunk keep it there; the pinned L0 and
        // top-level partitions stay.
        Assert.IsTrue(shrunk.Usage <= capacity / 4 && shrunkRead.Usage <= capacity / 4 + (64 << 10), t);
        Assert.IsTrue(full.Usage >= capacity * 9 / 10 && restored.Usage >= capacity * 8 / 10, t);
        string ns = p.NativeProfile.DbOptions.GetStatisticsString();
        long nAdds = TickerCount(ns, "rocksdb.block.cache.data.add");
        long mAdds = (long)p.ManagedProfile.DbOptions.Statistics!.GetTickerCount(Ticker.BlockCacheDataAdd);
        Assert.AreEqual(nAdds, mAdds, nAdds / 50, t);
    }
}
