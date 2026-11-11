using RocksDb.Managed.Cache;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4, slice "ClosingPersistent": persistent cache tiers (utilities/persistent_cache), row cache, cache dump /
/// load, column family lifecycle and the close path, read against the C++ sources.
/// </summary>
[TestClass]
public sealed class Audit4ClosingPersistentTests
{
    private static PersistentCacheConfig NewConfig(MemoryFileSystem fs, bool pipelineWrites) =>
        new(fs, "/pcache", 4L << 20, writeBufferSize: 16 * 1024) { CacheFileSize = 256 * 1024, PipelineWrites = pipelineWrites };

    private static byte[] Fill(int size, byte b)
    {
        byte[] data = new byte[size];
        data.AsSpan().Fill(b);
        return data;
    }

    private static byte[]? Lookup(PersistentCache cache, byte[] key)
    {
        if (!cache.TryLookup(key, out byte[] data, out int length)) return null;
        byte[] copy = data.AsSpan(0, length).ToArray();
        RocksArrayPool<byte>.Shared.Return(data);
        return copy;
    }

    [TestMethod]
    public void BlockCacheTierAcceptsKeysOfAnyLength()
    {
        // BlockCacheTier::InsertImpl / Lookup / Erase key the index by the Slice's bytes (BlockInfo::key_ is a
        // std::string, block_cache_tier_metadata.cc); upstream's persistent_cache_test uses 8-byte keys. The port only
        // accepted 16-byte block cache keys (InvalidArgument otherwise).
        MemoryFileSystem fs = NewFs();
        using var tier = new BlockCacheTier(NewConfig(fs, pipelineWrites: false));
        tier.Open();
        byte[] shortKey = B("k1"), longKey = Fill(40, (byte)'L'), other = B("k2");
        Assert.IsTrue(tier.Insert(shortKey, Fill(100, 1)));
        Assert.IsTrue(tier.Insert(longKey, Fill(200, 2)));
        // A duplicate insert is OK and keeps the first value (metadata_.Lookup hit in InsertImpl).
        Assert.IsTrue(tier.Insert(shortKey, Fill(50, 9)));
        CollectionAssert.AreEqual(Fill(100, 1), Lookup(tier, shortKey));
        CollectionAssert.AreEqual(Fill(200, 2), Lookup(tier, longKey));
        Assert.IsNull(Lookup(tier, other));
        Assert.IsTrue(tier.Erase(shortKey));
        Assert.IsNull(Lookup(tier, shortKey));
        CollectionAssert.AreEqual(Fill(200, 2), Lookup(tier, longKey));
    }

    [TestMethod]
    public void VolatileTierEvictsPagesWithShortKeysIntoTheBlockCacheTier()
    {
        // VolatileCacheTier::Evict pushes the evicted page to next_tier()->Insert with the page's own key, whatever its
        // length, and VolatileCacheTier::Lookup falls through to the next tier (volatile_tier_impl.cc). The port threw
        // InvalidArgument from the insert that triggered the eviction.
        MemoryFileSystem fs = NewFs();
        using var tiered = new PersistentTieredCache();
        tiered.AddTier(new VolatileCacheTier(isCompressed: true, maxSize: 100));
        tiered.AddTier(new BlockCacheTier(NewConfig(fs, pipelineWrites: false)));
        tiered.Open();
        Assert.IsTrue(tiered.Insert(B("a"), Fill(60, 1)));
        Assert.IsTrue(tiered.Insert(B("b"), Fill(60, 2))); // evicts "a" into the block cache tier
        CollectionAssert.AreEqual(Fill(60, 2), Lookup(tiered, B("b")));
        CollectionAssert.AreEqual(Fill(60, 1), Lookup(tiered, B("a")));
    }

    [TestMethod]
    public void PipelinedInsertIsDroppedWhenKeyPlusDataReachTheBacklogLimit()
    {
        // BoundedQueue::Push (persistent_cache_util.h): the op is discarded when size_ + t.Size() >= max_size_, with
        // InsertOp::Size() = data_.size() + key_.size() (block_cache_tier.h). The port compared the data size only,
        // with '>'.
        byte[] key = Fill(16, 7);
        foreach ((int limit, bool accepted) in new[] { (16 + 100, false), (16 + 100 + 1, true) })
        {
            MemoryFileSystem fs = NewFs();
            PersistentCacheConfig config = NewConfig(fs, pipelineWrites: true);
            config.MaxWritePipelineBacklogSize = limit;
            using var tier = new BlockCacheTier(config);
            tier.Open();
            Assert.AreEqual(accepted, tier.Insert(key, Fill(100, 3)), "limit " + limit);
            tier.Flush();
            byte[]? found = Lookup(tier, key);
            if (accepted) CollectionAssert.AreEqual(Fill(100, 3), found);
            else Assert.IsNull(found, "limit " + limit);
        }
    }

    [TestMethod]
    public void CloseDeletesUnreferencedTableFiles()
    {
        // DBImpl::CloseHelper (db_impl.cc): "if (opened_successfully_ && !read_only_) FindObsoleteFiles(&job_context,
        // true); ... PurgeObsoleteFiles" on every close -- a full scan, so a table file no version references (and whose
        // number is below min_pending_output) is gone after the close. The port only purged at close when a dropped
        // column family was released.
        const string path = "/audit4close";
        MemoryFileSystem fs = NewFs();
        string stray;
        using (RocksDatabase db = RocksDatabase.Open(path, NewOptions(fs)))
        {
            db.Put(B("a"), B("1"));
            db.Flush(); // the purge after the flush runs the first periodic full scan
            db.Put(B("b"), B("2"));
            db.Flush();
            var used = new HashSet<ulong>();
            ulong max = 0;
            foreach (string child in fs.GetChildren(path))
            {
                if (!RocksDb.Managed.Db.FileNames.TryParseFileName(Path.GetFileName(child), out ulong number, out _)) continue;
                used.Add(number);
                max = Math.Max(max, number);
            }
            ulong free = 0;
            for (ulong n = max - 1; n >= 1 && free == 0; n--)
            {
                if (!used.Contains(n)) free = n;
            }
            Assert.AreNotEqual(0UL, free, "an unused file number below the newest file");
            stray = path + "/" + free.ToString("D6", System.Globalization.CultureInfo.InvariantCulture) + ".sst";
            using (IWritableFile f = fs.CreateWritable(stray)) f.Append(B("not a table"));
            Assert.IsTrue(fs.FileExists(stray));
        }
        Assert.IsFalse(fs.FileExists(stray), "unreferenced table file left behind by the close");
        using RocksDatabase again = RocksDatabase.Open(path, NewOptions(fs));
        Assert.AreEqual("1", GetString(again, "a"));
        Assert.AreEqual("2", GetString(again, "b"));
    }

    [TestMethod]
    public void ReadOnlyCloseKeepsUnreferencedFiles()
    {
        // CloseHelper purges only "if (opened_successfully_ && !read_only_)": a read-only instance never deletes files.
        const string path = "/audit4closero";
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(path, NewOptions(fs)))
        {
            db.Put(B("a"), B("1"));
            db.Flush();
        }
        string stray = path + "/000001.sst";
        using (IWritableFile f = fs.CreateWritable(stray)) f.Append(B("not a table"));
        using (RocksDatabase ro = RocksDatabase.OpenReadOnly(path, new DbOptions { FileSystem = fs }))
        {
            Assert.AreEqual("1", GetString(ro, "a"));
        }
        Assert.IsTrue(fs.FileExists(stray));
    }
}
