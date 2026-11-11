using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using RocksDb.Managed.Cache;
using RocksDb.Managed.Monitoring;

namespace RocksDb.Managed.CompatTests.Audit4;

/// <summary>
/// Audit #4: the compression dictionary block in the block cache, compared with native librocksdb. The same
/// deterministic workload (ZSTD with a dictionary; flushes, point lookups, MultiGet, scans, a full compaction) runs on
/// both engines and the BLOCK_CACHE_COMPRESSION_DICT_* tickers are compared.
/// </summary>
[TestClass]
public sealed class Audit4CompressionDictCacheCompatTests
{
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    private const string Miss = "rocksdb.block.cache.compression.dict.miss";
    private const string Hit = "rocksdb.block.cache.compression.dict.hit";
    private const string Add = "rocksdb.block.cache.compression.dict.add";
    private const string BytesInsert = "rocksdb.block.cache.compression.dict.bytes.insert";
    private const string AddRedundant = "rocksdb.block.cache.compression.dict.add.redundant";

    private static byte[] K(int i) => Encoding.ASCII.GetBytes("key" + i.ToString("D5", CultureInfo.InvariantCulture));

    private static byte[] V(int i, int gen) => Encoding.ASCII.GetBytes(
        "{\"id\":" + i + ",\"gen\":" + gen + ",\"name\":\"customer-" + (i % 97) + "\",\"email\":\"user" + (i % 89) + "@example.com\",\"country\":\""
        + (i % 3 == 0 ? "Portugal" : i % 3 == 1 ? "Germany" : "Brazil") + "\",\"active\":" + (i % 2 == 0 ? "true" : "false") + "}");

    private static Dictionary<string, ulong> Tickers(string statistics)
    {
        var d = new Dictionary<string, ulong>(StringComparer.Ordinal);
        foreach (Match m in Regex.Matches(statistics, "^(\\S+) COUNT : (\\d+)$", RegexOptions.Multiline))
            d[m.Groups[1].Value] = ulong.Parse(m.Groups[2].Value, CultureInfo.InvariantCulture);
        return d;
    }

    /// <summary>cache_index_and_filter_blocks, pin_l0_filter_and_index_blocks_in_cache, metadata_cache_options.unpartitioned_pinning.</summary>
    private sealed record Mode(bool CacheMeta, bool PinL0, PinningTier Unpartitioned)
    {
        public override string ToString() => "cache_index_and_filter_blocks=" + CacheMeta + " pin_l0=" + PinL0 + " unpartitioned_pinning=" + Unpartitioned;
    }

    private static readonly List<byte[]> s_multiGetKeys = MakeMultiGetKeys();

    private static List<byte[]> MakeMultiGetKeys()
    {
        var keys = new List<byte[]>();
        for (int i = 0; i < 600; i += 37) keys.Add(K(i));
        keys.Add(K(9000)); // absent
        return keys;
    }

    // Each step returns the statistics text after it, so that a difference is attributed to a step.
    private static List<(string Step, Dictionary<string, ulong> Tickers)> NativeWorkload(string path, Mode mode)
    {
        RocksDbSharp.Native n = RocksDbSharp.Native.Instance;
        var bbto = new RocksDbSharp.BlockBasedTableOptions().SetBlockSize(512UL).SetBlockCache(RocksDbSharp.Cache.CreateLru(64UL << 20));
        n.rocksdb_block_based_options_set_cache_index_and_filter_blocks(bbto.Handle, RocksDbSharp.Native.MarshalBool(mode.CacheMeta));
        n.rocksdb_block_based_options_set_pin_l0_filter_and_index_blocks_in_cache(bbto.Handle, RocksDbSharp.Native.MarshalBool(mode.PinL0));
        n.rocksdb_block_based_options_set_unpartitioned_pinning_tier(bbto.Handle, (int)mode.Unpartitioned);
        var no = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).EnableStatistics();
        no.SetCompression(RocksDbSharp.Compression.Zstd);
        n.rocksdb_options_set_compression_options(no.Handle, -14, 32767, 0, 4096); // window_bits, level, strategy, max_dict_bytes
        no.SetDisableAutoCompactions(1);
        no.SetBlockBasedTableFactory(bbto);
        s_keepAlive.Add(no);
        s_keepAlive.Add(bbto);
        var flush = new RocksDbSharp.FlushOptions().SetWaitForFlush(true);
        var steps = new List<(string, Dictionary<string, ulong>)>();
        void Step(string name) => steps.Add((name, Tickers(no.GetStatisticsString())));
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(no, path))
        {
            for (int i = 0; i < 600; i++) db.Put(K(i), V(i, 0));
            db.Flush(flush);
            Step("flush 1");
            for (int i = 0; i < 20; i++) db.Get(K(i * 29));
            Step("gets on L0");
            db.MultiGet(s_multiGetKeys.ToArray());
            Step("multiget on L0");
            using (RocksDbSharp.Iterator it = db.NewIterator())
            {
                it.SeekToFirst();
                for (int i = 0; i < 150 && it.Valid(); i++) it.Next();
                it.Seek(K(400));
                for (int i = 0; i < 30 && it.Valid(); i++) it.Next();
            }
            Step("scan on L0");
            for (int i = 0; i < 300; i++) db.Put(K(i * 2), V(i * 2, 1));
            db.Flush(flush);
            Step("flush 2");
            for (int i = 0; i < 20; i++) db.Get(K(i * 31));
            db.MultiGet(s_multiGetKeys.ToArray());
            Step("reads on two L0 files");
            db.CompactRange(Array.Empty<byte>(), null);
            Step("compaction");
            for (int i = 0; i < 20; i++) db.Get(K(i * 23));
            Step("gets after compaction");
            db.MultiGet(s_multiGetKeys.ToArray());
            Step("multiget after compaction");
            using (RocksDbSharp.Iterator it = db.NewIterator())
            {
                int count = 0;
                for (it.SeekToFirst(); it.Valid(); it.Next()) count++;
                Assert.AreEqual(600, count);
            }
            Step("scan after compaction");
        }
        return steps;
    }

    private static List<(string Step, Dictionary<string, ulong> Tickers)> ManagedWorkload(string path, Mode mode)
    {
        var stats = new Statistics();
        var cf = new ColumnFamilyOptions { Compression = RocksDb.Managed.Compression.CompressionType.Zstd, DisableAutoCompactions = true };
        cf.CompressionOpts.MaxDictBytes = 4096;
        cf.TableOptions.BlockSize = 512;
        cf.TableOptions.BlockCache = BlockCache.CreateLru(64L << 20);
        cf.TableOptions.CacheIndexAndFilterBlocks = mode.CacheMeta;
        cf.TableOptions.PinL0FilterAndIndexBlocksInCache = mode.PinL0;
        cf.TableOptions.MetadataCacheOptions.UnpartitionedPinning = mode.Unpartitioned;
        var steps = new List<(string, Dictionary<string, ulong>)>();
        void Step(string name) => steps.Add((name, Tickers(stats.ToString())));
        using (RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true, Statistics = stats }, cf))
        {
            for (int i = 0; i < 600; i++) db.Put(K(i), V(i, 0));
            db.Flush();
            Step("flush 1");
            for (int i = 0; i < 20; i++) db.Get(K(i * 29));
            Step("gets on L0");
            db.MultiGet(s_multiGetKeys);
            Step("multiget on L0");
            using (Iterator it = db.NewIterator())
            {
                it.SeekToFirst();
                for (int i = 0; i < 150 && it.Valid; i++) it.Next();
                it.Seek(K(400));
                for (int i = 0; i < 30 && it.Valid; i++) it.Next();
            }
            Step("scan on L0");
            for (int i = 0; i < 300; i++) db.Put(K(i * 2), V(i * 2, 1));
            db.Flush();
            Step("flush 2");
            for (int i = 0; i < 20; i++) db.Get(K(i * 31));
            db.MultiGet(s_multiGetKeys);
            Step("reads on two L0 files");
            db.CompactRange(null, null);
            Step("compaction");
            for (int i = 0; i < 20; i++) db.Get(K(i * 23));
            Step("gets after compaction");
            db.MultiGet(s_multiGetKeys);
            Step("multiget after compaction");
            using (Iterator it = db.NewIterator())
            {
                int count = 0;
                for (it.SeekToFirst(); it.Valid; it.Next()) count++;
                Assert.AreEqual(600, count);
            }
            Step("scan after compaction");
        }
        return steps;
    }

    private static void Compare(Mode mode)
    {
        string root = Path.Combine(Path.GetTempPath(), "audit4-dict-cache-compat-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        try
        {
            List<(string Step, Dictionary<string, ulong> Tickers)> native = NativeWorkload(Path.Combine(root, "native"), mode);
            List<(string Step, Dictionary<string, ulong> Tickers)> managed = ManagedWorkload(Path.Combine(root, "managed"), mode);
            Assert.AreEqual(native.Count, managed.Count);
            var differences = new StringBuilder();
            for (int i = 0; i < native.Count; i++)
            {
                foreach (string ticker in new[] { Miss, Hit, Add, AddRedundant })
                {
                    if (native[i].Tickers[ticker] != managed[i].Tickers[ticker])
                        differences.Append("\n  after ").Append(native[i].Step).Append(": ").Append(ticker).Append(" native=")
                            .Append(native[i].Tickers[ticker]).Append(" managed=").Append(managed[i].Tickers[ticker]);
                }
            }
            Assert.AreEqual("", differences.ToString(), mode.ToString());
            for (int i = 0; i < native.Count; i++)
            {
                string what = mode + ", after " + native[i].Step + ": ";
                // The charge of a dictionary is engine-specific (allocator sizes, the digested ZSTD dictionary), so the
                // bytes are compared per insert: zero together, and within a factor of two of each other.
                ulong nb = native[i].Tickers[BytesInsert], mb = managed[i].Tickers[BytesInsert];
                Assert.AreEqual(nb == 0, mb == 0, what + BytesInsert);
                if (nb != 0) Assert.IsTrue(mb * 2 >= nb && mb <= nb * 2, what + BytesInsert + " native=" + nb + " managed=" + mb);
            }
            ulong expectedAdds = mode.CacheMeta ? 1UL : 0UL;
            Assert.IsTrue(native[0].Tickers[Add] >= expectedAdds, "the workload exercises the dictionary: " + mode);
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, true);
        }
    }

    /// <summary>
    /// UncompressionDictReader::Create with use_cache and no pin, GetOrReadUncompressionDictionary per data block
    /// (BlockBasedTable::NewDataBlockIterator) and once per file in BlockBasedTable::MultiGet.
    /// </summary>
    [TestMethod]
    public void UnpinnedDictionaryTickersMatchNative() => Compare(new Mode(true, false, PinningTier.Fallback));

    /// <summary>
    /// pin_unpartitioned in BlockBasedTable::PrefetchIndexAndFilterBlocks with pin_l0_filter_and_index_blocks_in_cache:
    /// flushed L0 files pin the dictionary, the compaction output on a lower level does not.
    /// </summary>
    [TestMethod]
    public void L0PinnedDictionaryTickersMatchNative() => Compare(new Mode(true, true, PinningTier.Fallback));

    /// <summary>metadata_cache_options.unpartitioned_pinning = kAll: every table pins its dictionary.</summary>
    [TestMethod]
    public void AlwaysPinnedDictionaryTickersMatchNative() => Compare(new Mode(true, false, PinningTier.All));

    /// <summary>metadata_cache_options.unpartitioned_pinning = kNone overrides pin_l0_filter_and_index_blocks_in_cache.</summary>
    [TestMethod]
    public void NeverPinnedDictionaryTickersMatchNative() => Compare(new Mode(true, true, PinningTier.None));

    /// <summary>UncompressionDictReader::Create with use_cache = false: the table reader owns the dictionary, no tickers.</summary>
    [TestMethod]
    public void OwnedDictionaryRecordsNothingLikeNative() => Compare(new Mode(false, false, PinningTier.Fallback));
}
