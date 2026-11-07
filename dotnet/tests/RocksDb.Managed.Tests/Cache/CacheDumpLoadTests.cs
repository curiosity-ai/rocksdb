using System.Buffers;
using System.Text;
using RocksDb.Managed.Cache;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table.Filters;
using RocksDb.Managed.Tests.Db;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Tests.Cache;

/// <summary>
/// Cache dump / load (utilities/cache_dump_load_impl.cc) and Cache::ApplyToAllEntries. The DB tests port
/// DBSecondaryCacheTest.LRUCacheDumpLoadBasic / LRUCacheDumpLoadWithFilter (cache/lru_cache_test.cc); the format test
/// encodes the expected dump bytes independently from the port (the native C API cannot produce or read dumps).
/// </summary>
[TestClass]
public sealed class CacheDumpLoadTests
{
    private static readonly BytesCacheItemHelper s_data = new(CacheEntryRole.DataBlock);
    private static readonly BytesCacheItemHelper s_index = new(CacheEntryRole.IndexBlock);
    private static readonly BytesCacheItemHelper s_misc = new(CacheEntryRole.Misc);

    /// <summary>A clock that always reads the same time.</summary>
    private sealed class FixedClock(DateTimeOffset now) : TimeProvider
    {
        public DateTimeOffset Now { get; set; } = now;

        public override DateTimeOffset GetUtcNow() => Now;
    }

    /// <summary>
    /// TestSecondaryCache (lru_cache_test.cc): keeps saved values, counts inserts and lookups, creates values with the
    /// lookup's helper; never erases.
    /// </summary>
    private sealed class TestSecondaryCache : SecondaryCache
    {
        private readonly Dictionary<CacheKey, byte[]> _data = new();
        private int _inserts, _lookups, _hits;

        public override string Name => "TestSecondaryCache";

        public int Inserts => Volatile.Read(ref _inserts);

        public int Lookups => Volatile.Read(ref _lookups);

        public int Hits => Volatile.Read(ref _hits);

        public int Count
        {
            get
            {
                lock (_data) return _data.Count;
            }
        }

        public bool Contains(in CacheKey key)
        {
            lock (_data) return _data.ContainsKey(key);
        }

        public override void Insert(ReadOnlySpan<byte> key, in CacheValue value, CacheItemHelper helper, bool forceInsert)
        {
            Interlocked.Increment(ref _inserts);
            byte[] saved = new byte[helper.GetSaveSize(value)];
            helper.SaveTo(value, saved);
            lock (_data) _data[CacheKey.Read(key)] = saved;
        }

        public override void InsertSaved(ReadOnlySpan<byte> key, ReadOnlySpan<byte> saved, CompressionType type, CacheTier source)
        {
            Assert.AreEqual(CompressionType.None, type);
            Interlocked.Increment(ref _inserts);
            lock (_data) _data[CacheKey.Read(key)] = saved.ToArray();
        }

        public override SecondaryCacheResultHandle? Lookup(ReadOnlySpan<byte> key, CacheItemHelper helper, object? createContext, bool wait,
            bool adviseErase, Statistics? stats, out bool keptInSecondaryCache)
        {
            Interlocked.Increment(ref _lookups);
            keptInSecondaryCache = true;
            byte[]? d;
            lock (_data)
            {
                if (!_data.TryGetValue(CacheKey.Read(key), out d)) return null;
            }
            if (!helper.TryCreate(d, CompressionType.None, CacheTier.VolatileTier, createContext, out CacheValue v, out long charge)) return null;
            Interlocked.Increment(ref _hits);
            return new ReadySecondaryCacheResultHandle(v, charge);
        }

        public override bool SupportForceErase => false;

        public override void Erase(ReadOnlySpan<byte> key) { }
    }

    private static CacheValue Value(CacheItemHelper helper, byte[] payload)
    {
        byte[] buf = ArrayPool<byte>.Shared.Rent(Math.Max(1, payload.Length));
        payload.CopyTo(buf, 0);
        return CacheValue.FromPooledBuffer(buf, payload.Length).WithHelper(helper);
    }

    private static byte[] Payload(int seed, int size)
    {
        byte[] b = new byte[size];
        for (int i = 0; i < size; i++) b[i] = (byte)(seed * 31 + i);
        return b;
    }

    private static BlockCache NewCache(bool hyperClock, long capacity) => hyperClock
        ? BlockCache.CreateHyperClock(new HyperClockCacheOptions { Capacity = capacity, EstimatedEntryCharge = 4096, NumShardBits = 2 })
        : BlockCache.CreateLru(new LruCacheOptions { Capacity = capacity, NumShardBits = 2 });

    // ---- ApplyToAllEntries --------------------------------------------------------------------------------------

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void ApplyToAllEntriesVisitsEveryEntryOnce(bool hyperClock)
    {
        BlockCache cache = NewCache(hyperClock, 64L << 20);
        const int n = 3000;
        for (int i = 0; i < n; i++) Assert.IsTrue(cache.Insert(new CacheKey(1, (ulong)i), Value(i % 2 == 0 ? s_data : s_index, Payload(i, 100)), 100, CachePriority.Low));
        // A pinned entry and an entry without helper are visited too.
        Assert.IsTrue(cache.TryLookup(new CacheKey(1, 7), out CachedBlock pinned));
        Assert.IsTrue(cache.Insert(new CacheKey(2, 0), CacheValue.FromBuffer(new byte[10], 10), 10, CachePriority.Low));
        var seen = new Dictionary<CacheKey, long>();
        // A small average_entries_per_lock exercises the rotation between shards and the per-shard resume positions.
        cache.ApplyToAllEntries(seen, static (Dictionary<CacheKey, long> s, in CacheKey key, in CacheValue value, long charge, CacheItemHelper? helper) =>
        {
            s.Add(key, charge);
            if (key.FileNumEtc64 == 1)
            {
                Assert.AreSame((int)key.OffsetEtc64 % 2 == 0 ? s_data : s_index, helper);
                CollectionAssert.AreEqual(Payload((int)key.OffsetEtc64, 100), value.Buffer.AsSpan(0, value.Length).ToArray());
            }
            else
            {
                Assert.IsNull(helper);
            }
        }, 3);
        Assert.AreEqual(n + 1, seen.Count);
        // LRUHandle::GetCharge excludes the per-entry metadata charge; the clock cache charges its table up front, so
        // its total charge is the entry's charge too.
        Assert.AreEqual(100, seen[new CacheKey(1, 5)]);
        pinned.Dispose();
        Assert.AreEqual(n + 1, cache.Count, "visiting changes nothing");
        // The default overload and an erased entry.
        Assert.IsTrue(cache.Erase(new CacheKey(1, 5)));
        var visited = new StrongBox();
        cache.ApplyToAllEntries(visited, static (StrongBox c, in CacheKey key, in CacheValue value, long charge, CacheItemHelper? helper) =>
        {
            c.Value++;
            Assert.IsFalse(key == new CacheKey(1, 5));
        });
        Assert.AreEqual(n, visited.Value);
        Assert.AreEqual(n, cache.Count);
    }

    private sealed class StrongBox
    {
        public int Value;
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void ApplyToAllEntriesUnderConcurrentChanges(bool hyperClock)
    {
        // Entries 0..999 stay; writers insert / erase / look up other keys and evict under a small capacity. Every
        // visited value must be intact and every stable entry must be visited.
        BlockCache cache = NewCache(hyperClock, 8L << 20);
        for (int i = 0; i < 1000; i++) Assert.IsTrue(cache.Insert(new CacheKey(1, (ulong)i), Value(s_data, Payload(i, 64)), 64, CachePriority.High));
        var pins = new CachedBlock[1000];
        for (int i = 0; i < 1000; i++) Assert.IsTrue(cache.TryLookup(new CacheKey(1, (ulong)i), out pins[i]));
        using var stop = new CancellationTokenSource();
        Task[] writers = new Task[3];
        for (int t = 0; t < writers.Length; t++)
        {
            int seed = t;
            writers[t] = Task.Run(() =>
            {
                var rnd = new Random(seed);
                while (!stop.IsCancellationRequested)
                {
                    int k = rnd.Next(100_000);
                    var key = new CacheKey(9, (ulong)k);
                    switch (rnd.Next(3))
                    {
                        case 0: cache.Insert(key, Value(s_data, Payload(k, 4000)), 4000, CachePriority.Low); break;
                        case 1: cache.Erase(key); break;
                        default:
                            if (cache.TryLookup(key, out CachedBlock h))
                            {
                                Assert.AreEqual((byte)(k * 31), h.Span[0]);
                                h.Dispose();
                            }
                            break;
                    }
                }
            });
        }
        try
        {
            for (int round = 0; round < 20; round++)
            {
                var stable = new HashSet<ulong>();
                cache.ApplyToAllEntries(stable, static (HashSet<ulong> s, in CacheKey key, in CacheValue value, long charge, CacheItemHelper? helper) =>
                {
                    int k = (int)key.OffsetEtc64;
                    ReadOnlySpan<byte> v = value.Buffer.AsSpan(0, value.Length);
                    Assert.AreEqual((byte)(k * 31), v[0]);
                    Assert.AreEqual((byte)(k * 31 + v.Length - 1), v[^1]);
                    if (key.FileNumEtc64 == 1) s.Add(key.OffsetEtc64);
                }, 16);
                Assert.AreEqual(1000, stable.Count, "round " + round);
            }
        }
        finally
        {
            stop.Cancel();
            Task.WaitAll(writers);
            for (int i = 0; i < pins.Length; i++) pins[i].Dispose();
        }
    }

    [TestMethod]
    public void ApplyToAllEntriesThroughWrappersAndUnsupportedCaches()
    {
        BlockCache lru = BlockCache.CreateLru(new LruCacheOptions { Capacity = 1 << 20, NumShardBits = 0 });
        BlockCache adapter = BlockCache.WithSecondaryCache(lru, new TestSecondaryCache());
        SimCache sim = BlockCache.CreateSimCache(lru, 1 << 20, 0);
        lru.Insert(new CacheKey(3, 3), Value(s_data, Payload(3, 8)), 8, CachePriority.Low);
        foreach (BlockCache c in new[] { adapter, sim })
        {
            var n = new StrongBox();
            c.ApplyToAllEntries(n, static (StrongBox b, in CacheKey key, in CacheValue value, long charge, CacheItemHelper? helper) => b.Value++);
            Assert.AreEqual(1, n.Value, c.Name);
        }
    }

    // ---- dump format ----------------------------------------------------------------------------------------

    /// <summary>The upstream encoding of one dump unit with the file writer's length prefixes (independent encoder).</summary>
    private static void ExpectedUnit(List<byte> file, uint seq, ulong ts, byte type, byte[] key, byte[] value)
    {
        var unit = new List<byte>();
        unit.AddRange(BitConverter.GetBytes(ts)); // the test host is little-endian (asserted below)
        unit.Add(type);
        unit.Add((byte)key.Length); // varint32, small
        unit.AddRange(key);
        unit.AddRange(BitConverter.GetBytes((uint)value.Length));
        unit.AddRange(BitConverter.GetBytes(Crc32C.Value(value)));
        AddVarint(unit, (uint)value.Length);
        unit.AddRange(value);
        byte[] u = unit.ToArray();
        file.AddRange(BitConverter.GetBytes(16u));
        file.AddRange(BitConverter.GetBytes(seq));
        file.AddRange(BitConverter.GetBytes(Crc32C.Value(u)));
        file.AddRange(BitConverter.GetBytes((ulong)u.Length));
        file.AddRange(BitConverter.GetBytes((uint)u.Length));
        file.AddRange(u);
    }

    private static void AddVarint(List<byte> dst, uint v)
    {
        while (v >= 0x80)
        {
            dst.Add((byte)(v | 0x80));
            v >>= 7;
        }
        dst.Add((byte)v);
    }

    [TestMethod]
    public void DumpFileMatchesTheUpstreamFormat()
    {
        Assert.IsTrue(BitConverter.IsLittleEndian);
        var fs = new MemoryFileSystem();
        var now = new DateTimeOffset(2026, 11, 5, 12, 0, 0, TimeSpan.Zero);
        ulong ts = (ulong)((now - DateTimeOffset.UnixEpoch).Ticks / 10);
        var opts = new CacheDumpOptions { Clock = new FixedClock(now) };
        // One shard: entries are visited in bucket order, so use a single dumpable entry plus non-dumpable ones.
        BlockCache cache = BlockCache.CreateLru(new LruCacheOptions { Capacity = 1 << 20, NumShardBits = 0 });
        byte[] block = Payload(5, 300);
        var key = new CacheKey(0x1122334455667788, 0x99);
        cache.Insert(key, Value(s_data, block), block.Length, CachePriority.Low);
        cache.Insert(new CacheKey(1, 1), Value(s_misc, Payload(1, 10)), 10, CachePriority.Low);          // role not dumped
        cache.Insert(new CacheKey(1, 2), CacheValue.FromBuffer(new byte[4], 4), 4, CachePriority.Low);    // no helper
        cache.Insert(new CacheKey(1, 3), Value(s_data.WithoutSecondaryCompat, Payload(3, 4)), 4, CachePriority.Low); // no saveto_cb
        using (CacheDumper dumper = CacheDump.NewDefaultDumper(opts, cache, CacheDump.NewToFileWriter(fs, "/dump")))
        {
            dumper.DumpCacheEntriesToWriter();
        }

        string header = "feedcafedeadbeef\tCache dump format version: 0.1\tRocksDB Version: 11.14\tFormat: dump_unit_metadata " +
                        "<sequence_number, dump_unit_checksum, dump_unit_size>, dump_unit <timestamp, key, block_type, block_size, " +
                        "block_data, block_checksum> cache_value\n";
        var expected = new List<byte>();
        ExpectedUnit(expected, 0, ts, 1, Encoding.ASCII.GetBytes("header"), Encoding.ASCII.GetBytes(header));
        byte[] k = new byte[16];
        key.WriteTo(k);
        CollectionAssert.AreEqual(new byte[] { 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0x99, 0, 0, 0, 0, 0, 0, 0 }, k);
        ExpectedUnit(expected, 1, ts, 3, k, block);
        ExpectedUnit(expected, 2, ts, 2, Encoding.ASCII.GetBytes("footer"), Encoding.ASCII.GetBytes("cache dump completed"));
        CollectionAssert.AreEqual(expected.ToArray(), ReadFile(fs, "/dump"));

        // A dump written by this independent encoder loads (what a native dump with the same content would be).
        fs.DeleteFile("/dump");
        WriteFile(fs, "/dump", expected.ToArray());
        var sec = new TestSecondaryCache();
        using (CacheDumpedLoader loader = CacheDump.NewDefaultLoader(opts, null, sec, CacheDump.NewFromFileReader(fs, "/dump")))
        {
            loader.RestoreCacheEntriesToSecondaryCache();
        }
        Assert.AreEqual(1, sec.Inserts);
        Assert.IsTrue(sec.Contains(key));
    }

    private static byte[] ReadFile(IFileSystem fs, string path)
    {
        using IRandomAccessFile f = fs.OpenRandomAccess(path);
        byte[] b = new byte[f.Length];
        Assert.AreEqual(b.Length, f.Read(0, b));
        return b;
    }

    private static void WriteFile(IFileSystem fs, string path, byte[] data)
    {
        using IWritableFile f = fs.CreateWritable(path);
        f.Append(data);
        f.Sync();
    }

    private static byte[] DumpOf(BlockCache cache, CacheDumpOptions opts, out int units)
    {
        var fs = new MemoryFileSystem();
        using (CacheDumper d = CacheDump.NewDefaultDumper(opts, cache, CacheDump.NewToFileWriter(fs, "/d"))) d.DumpCacheEntriesToWriter();
        byte[] file = ReadFile(fs, "/d");
        units = 0;
        for (int off = 0; off < file.Length; units++)
        {
            off += 4 + 16; // metadata
            off += 4 + (int)BitConverter.ToUInt32(file, off);
        }
        return file;
    }

    [TestMethod]
    public void DumpLimitsMaxSizeAndDeadline()
    {
        BlockCache cache = BlockCache.CreateLru(new LruCacheOptions { Capacity = 1 << 20, NumShardBits = 0 });
        for (int i = 0; i < 10; i++) cache.Insert(new CacheKey(1, (ulong)i), Value(s_data, Payload(i, 100)), 100, CachePriority.Low);
        var clock = new FixedClock(DateTimeOffset.UnixEpoch.AddDays(1));
        DumpOf(cache, new CacheDumpOptions { Clock = clock }, out int all);
        Assert.AreEqual(12, all); // header + 10 blocks + footer
        // max_size_bytes is checked before each block: dumping stops once more than 250 bytes were written (3 blocks).
        DumpOf(cache, new CacheDumpOptions { Clock = clock, MaxSizeBytes = 250 }, out int limited);
        Assert.AreEqual(2 + 3, limited);
        // Entries visited at or after the deadline are skipped; header and footer are still written.
        DumpOf(cache, new CacheDumpOptions { Clock = clock, Deadline = clock.Now }, out int none);
        Assert.AreEqual(2, none);
        DumpOf(cache, new CacheDumpOptions { Clock = clock, Deadline = clock.Now.AddTicks(10) }, out int before);
        Assert.AreEqual(12, before);
    }

    [TestMethod]
    public void LoaderReportsCorruptionAndInvalidArguments()
    {
        BlockCache cache = BlockCache.CreateLru(new LruCacheOptions { Capacity = 1 << 20, NumShardBits = 0 });
        for (int i = 0; i < 4; i++) cache.Insert(new CacheKey(1, (ulong)i), Value(s_data, Payload(i, 100)), 100, CachePriority.Low);
        var opts = new CacheDumpOptions();
        byte[] good = DumpOf(cache, opts, out _);

        static RocksDbException Load(byte[] file, SecondaryCache sec)
        {
            var fs = new MemoryFileSystem();
            WriteFile(fs, "/d", file);
            using CacheDumpedLoader l = CacheDump.NewDefaultLoader(new CacheDumpOptions(), null, sec, CacheDump.NewFromFileReader(fs, "/d"));
            return Assert.ThrowsExactly<RocksDbException>(l.RestoreCacheEntriesToSecondaryCache);
        }

        // Truncated: no footer (the reader's short read).
        var sec = new TestSecondaryCache();
        RocksDbException ex = Load(good.AsSpan(0, good.Length - 10).ToArray(), sec);
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
        StringAssert.Contains(ex.Message, "Corrupted cache dump file.");
        Assert.AreEqual(4, sec.Inserts, "the blocks before the damage were loaded");
        // A flipped byte in the header unit / in a block unit.
        byte[] bad = (byte[])good.Clone();
        bad[4 + 16 + 4 + 20] ^= 1;
        ex = Load(bad, new TestSecondaryCache());
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
        StringAssert.Contains(ex.Message, "Read header unit corrupted!");
        bad = (byte[])good.Clone();
        bad[^60] ^= 1; // inside the last block unit
        ex = Load(bad, new TestSecondaryCache());
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
        // A metadata size that does not match the packet.
        bad = (byte[])good.Clone();
        bad[4 + 8] ^= 1;
        ex = Load(bad, new TestSecondaryCache());
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
        StringAssert.Contains(ex.Message, "does not match the size stored in metadata");
        // Empty file.
        Assert.AreEqual(StatusCode.Corruption, Load([], new TestSecondaryCache()).Code);

        // Arguments (Status::InvalidArgument upstream).
        var fs2 = new MemoryFileSystem();
        using (CacheDumper d = CacheDump.NewDefaultDumper(opts, null!, CacheDump.NewToFileWriter(fs2, "/x")))
            Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(d.DumpCacheEntriesToWriter).Code);
        using (CacheDumper d = CacheDump.NewDefaultDumper(opts, cache, null!))
            Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(d.DumpCacheEntriesToWriter).Code);
        using (CacheDumpedLoader l = CacheDump.NewDefaultLoader(opts, null, null!, CacheDump.NewFromFileReader(fs2, "/x")))
            Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(l.RestoreCacheEntriesToSecondaryCache).Code);
        // The abstract defaults are NotSupported.
        var plain = new PlainDumper();
        Assert.AreEqual(StatusCode.NotSupported, Assert.ThrowsExactly<RocksDbException>(plain.DumpCacheEntriesToWriter).Code);
        Assert.AreEqual(StatusCode.NotSupported, Assert.ThrowsExactly<RocksDbException>(() => plain.SetDumpFilter([])).Code);
    }

    private sealed class PlainDumper : CacheDumper;

    /// <summary>A writer that fails after <c>failAfter</c> packets (a broken transfer).</summary>
    private sealed class FailingWriter(int failAfter) : CacheDumpWriter
    {
        private int _packets;

        public override void WriteMetadata(ReadOnlySpan<byte> metadata) { }

        public override void WritePacket(ReadOnlySpan<byte> data)
        {
            if (++_packets > failAfter) throw RocksDbException.IOError("injected");
        }

        public override void Close() { }
    }

    [TestMethod]
    public void DumpReportsWriterErrors()
    {
        BlockCache cache = BlockCache.CreateLru(new LruCacheOptions { Capacity = 1 << 20, NumShardBits = 0 });
        for (int i = 0; i < 4; i++) cache.Insert(new CacheKey(1, (ulong)i), Value(s_data, Payload(i, 100)), 100, CachePriority.Low);
        foreach (int failAfter in new[] { 0, 2, 5 })
        {
            using CacheDumper d = CacheDump.NewDefaultDumper(new CacheDumpOptions(), cache, new FailingWriter(failAfter));
            Assert.AreEqual(StatusCode.IOError, Assert.ThrowsExactly<RocksDbException>(d.DumpCacheEntriesToWriter).Code, "fail after " + failAfter);
        }
    }

    // ---- DB integration (lru_cache_test.cc DBSecondaryCacheTest) ------------------------------------------------

    private static ColumnFamilyOptions CfOptions(BlockCache cache, bool cacheIndexAndFilter) => new()
    {
        TableOptions = new BlockBasedTableOptions
        {
            BlockCache = cache, BlockSize = 4 * 1024, CacheIndexAndFilterBlocks = cacheIndexAndFilter,
            FilterPolicy = cacheIndexAndFilter ? new BloomFilterPolicy(10) : null,
        },
    };

    private static byte[] DumpValue(char c) => Encoding.ASCII.GetBytes(new string(c, 1000));

    private static void FillAndCompact(RocksDatabase db, char c)
    {
        for (int i = 0; i < 256; i++) db.Put(DbTestUtil.Key(i), DumpValue(c));
        db.Flush();
        db.CompactRange(null, null);
    }

    private static void ReadAll(RocksDatabase db, char c)
    {
        for (int i = 0; i < 256; i++) CollectionAssert.AreEqual(DumpValue(c), db.Get(DbTestUtil.Key(i)), "key " + i);
    }

    /// <summary>The entries of <paramref name="cache"/> a dump would contain, by role.</summary>
    private static Dictionary<CacheEntryRole, int> DumpableEntries(BlockCache cache)
    {
        var roles = new Dictionary<CacheEntryRole, int>();
        cache.ApplyToAllEntries(roles, static (Dictionary<CacheEntryRole, int> r, in CacheKey key, in CacheValue value, long charge, CacheItemHelper? helper) =>
        {
            if (helper is { IsSecondaryCacheCompatible: true } && helper.Role is CacheEntryRole.DataBlock or CacheEntryRole.IndexBlock
                    or CacheEntryRole.FilterBlock or CacheEntryRole.FilterMetaBlock)
                r[helper.Role] = r.GetValueOrDefault(helper.Role) + 1;
        });
        return roles;
    }

    private static int Sum(Dictionary<CacheEntryRole, int> d)
    {
        int s = 0;
        foreach (int v in d.Values) s += v;
        return s;
    }

    [TestMethod]
    [DataRow(false, false)]
    [DataRow(true, false)]
    [DataRow(false, true)]
    public void DumpLoadBasic(bool hyperClock, bool cacheIndexAndFilter)
    {
        MemoryFileSystem fs = DbTestUtil.NewFs();
        BlockCache cache = hyperClock
            ? BlockCache.CreateHyperClock(new HyperClockCacheOptions { Capacity = 1 << 20, EstimatedEntryCharge = 4096, NumShardBits = 0 })
            : BlockCache.CreateLru(new LruCacheOptions { Capacity = 1 << 20, NumShardBits = 0 });
        var opts = new CacheDumpOptions();
        Dictionary<CacheEntryRole, int> dumped;
        using (RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, DbTestUtil.NewOptions(fs), CfOptions(cache, cacheIndexAndFilter)))
        {
            FillAndCompact(db, 'a');
            // Do the reads for all the key-value pairs, so all the blocks are in the cache.
            ReadAll(db, 'a');
            dumped = DumpableEntries(cache);
            Assert.IsTrue(dumped[CacheEntryRole.DataBlock] >= 60, "data blocks " + dumped[CacheEntryRole.DataBlock]);
            if (cacheIndexAndFilter)
            {
                Assert.AreEqual(1, dumped[CacheEntryRole.IndexBlock]);
                Assert.AreEqual(1, dumped[CacheEntryRole.FilterBlock]);
            }
            using CacheDumper dumper = CacheDump.NewDefaultDumper(opts, cache, CacheDump.NewToFileWriter(fs, DbTestUtil.DbPath + "/cache_dump"));
            dumper.SetDumpFilter([db]);
            dumper.DumpCacheEntriesToWriter();
        }

        // A new, empty cache with a secondary cache: load the dump before the DB is reopened.
        var secondary = new TestSecondaryCache();
        BlockCache cache2 = BlockCache.CreateLru(new LruCacheOptions { Capacity = 1 << 20, NumShardBits = 0, SecondaryCache = secondary });
        using (CacheDumpedLoader loader = CacheDump.NewDefaultLoader(opts, null, secondary, CacheDump.NewFromFileReader(fs, DbTestUtil.DbPath + "/cache_dump")))
        {
            loader.RestoreCacheEntriesToSecondaryCache();
        }
        Assert.AreEqual(Sum(dumped), secondary.Inserts);
        Assert.AreEqual(0, secondary.Lookups);

        var stats = new Statistics();
        DbOptions o = DbTestUtil.NewOptions(fs);
        o.Statistics = stats;
        using (RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, o, CfOptions(cache2, cacheIndexAndFilter)))
        {
            ReadAll(db, 'a');
            // Every block comes from the secondary cache: no block cache miss, nothing inserted into the secondary.
            Assert.AreEqual(Sum(dumped), secondary.Inserts);
            Assert.AreEqual(Sum(dumped), secondary.Hits);
            Assert.AreEqual(0UL, stats.GetTickerCount(Ticker.BlockCacheMiss));
            Assert.AreEqual((ulong)dumped[CacheEntryRole.DataBlock], stats.GetTickerCount(Ticker.SecondaryCacheDataHits));
            if (cacheIndexAndFilter)
            {
                Assert.AreEqual(1UL, stats.GetTickerCount(Ticker.SecondaryCacheIndexHits));
                Assert.AreEqual(1UL, stats.GetTickerCount(Ticker.SecondaryCacheFilterHits));
            }
        }
    }

    [TestMethod]
    public void DumpLoadWithFilter()
    {
        MemoryFileSystem fs = DbTestUtil.NewFs();
        fs.CreateDirectoryIfMissing("/db2");
        BlockCache cache = BlockCache.CreateLru(new LruCacheOptions { Capacity = 1 << 20, NumShardBits = 0 });
        var opts = new CacheDumpOptions();
        Dictionary<CacheEntryRole, int> db1Blocks;
        using (RocksDatabase db1 = RocksDatabase.Open(DbTestUtil.DbPath, DbTestUtil.NewOptions(fs), CfOptions(cache, false)))
        using (RocksDatabase db2 = RocksDatabase.Open("/db2", DbTestUtil.NewOptions(fs), CfOptions(cache, false)))
        {
            FillAndCompact(db1, 'a');
            FillAndCompact(db2, 'b');
            ReadAll(db1, 'a');
            db1Blocks = DumpableEntries(cache);
            ReadAll(db2, 'b');
            int both = Sum(DumpableEntries(cache));
            Assert.AreEqual(2 * Sum(db1Blocks), both, "both DBs' blocks are in the shared cache");
            using CacheDumper dumper = CacheDump.NewDefaultDumper(opts, cache, CacheDump.NewToFileWriter(fs, DbTestUtil.DbPath + "/cache_dump"));
            dumper.SetDumpFilter([db1]);
            dumper.DumpCacheEntriesToWriter();
        }

        var secondary = new TestSecondaryCache();
        BlockCache cache2 = BlockCache.CreateLru(new LruCacheOptions { Capacity = 1 << 20, NumShardBits = 0, SecondaryCache = secondary });
        using (CacheDumpedLoader loader = CacheDump.NewDefaultLoader(opts, null, secondary, CacheDump.NewFromFileReader(fs, DbTestUtil.DbPath + "/cache_dump")))
        {
            loader.RestoreCacheEntriesToSecondaryCache();
        }
        // Only db1's blocks were dumped.
        Assert.AreEqual(Sum(db1Blocks), secondary.Inserts);

        var stats = new Statistics();
        DbOptions o = DbTestUtil.NewOptions(fs);
        o.Statistics = stats;
        using (RocksDatabase db1 = RocksDatabase.Open(DbTestUtil.DbPath, o, CfOptions(cache2, false)))
        {
            ReadAll(db1, 'a');
            Assert.AreEqual(0UL, stats.GetTickerCount(Ticker.BlockCacheMiss));
            Assert.AreEqual(Sum(db1Blocks), secondary.Hits);
        }
        var stats2 = new Statistics();
        DbOptions o2 = DbTestUtil.NewOptions(fs);
        o2.Statistics = stats2;
        using (RocksDatabase db2 = RocksDatabase.Open("/db2", o2, CfOptions(cache2, false)))
        {
            ReadAll(db2, 'b');
            Assert.AreEqual(0UL, stats2.GetTickerCount(Ticker.SecondaryCacheHits), "db2's blocks were filtered out");
            Assert.AreEqual((ulong)Sum(db1Blocks), stats2.GetTickerCount(Ticker.BlockCacheMiss));
        }
    }

    [TestMethod]
    public void DumpWhileReadersRun()
    {
        // The dump iterates the cache while other threads read (and evict from a small cache): the dump stays valid.
        MemoryFileSystem fs = DbTestUtil.NewFs();
        BlockCache cache = BlockCache.CreateHyperClock(new HyperClockCacheOptions { Capacity = 160 * 1024, EstimatedEntryCharge = 4096, NumShardBits = 1 });
        using RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, DbTestUtil.NewOptions(fs), CfOptions(cache, false));
        FillAndCompact(db, 'a');
        ReadAll(db, 'a');
        using var stop = new CancellationTokenSource();
        Task[] readers = new Task[3];
        for (int t = 0; t < readers.Length; t++)
        {
            int seed = t;
            readers[t] = Task.Run(() =>
            {
                var rnd = new Random(seed);
                while (!stop.IsCancellationRequested)
                {
                    int i = rnd.Next(256);
                    CollectionAssert.AreEqual(DumpValue('a'), db.Get(DbTestUtil.Key(i)));
                }
            });
        }
        try
        {
            for (int round = 0; round < 10; round++)
            {
                string path = DbTestUtil.DbPath + "/dump" + round;
                using (CacheDumper dumper = CacheDump.NewDefaultDumper(new CacheDumpOptions(), cache, CacheDump.NewToFileWriter(fs, path)))
                {
                    dumper.SetDumpFilter([db]);
                    dumper.DumpCacheEntriesToWriter();
                }
                var sec = new TestSecondaryCache();
                using CacheDumpedLoader loader = CacheDump.NewDefaultLoader(new CacheDumpOptions(), null, sec, CacheDump.NewFromFileReader(fs, path));
                loader.RestoreCacheEntriesToSecondaryCache();
                Assert.IsTrue(sec.Inserts > 0);
            }
        }
        finally
        {
            stop.Cancel();
            Task.WaitAll(readers);
        }
    }
}
