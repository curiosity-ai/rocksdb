using System.Globalization;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Db;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;
using RocksDb.Managed.Monitoring;
using RocksDb.Managed.Table;
using RocksDb.Managed.Table.TrieIndex;

namespace RocksDb.Managed.CompatTests.Audit4;

/// <summary>Audit #4, slice ClosingTools: differential checks against native librocksdb.</summary>
[TestClass]
public sealed class Audit4ClosingToolsCompatTests
{
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    private static byte[] B(string s) => Encoding.ASCII.GetBytes(s);

    private static byte[] K(int i) => B("key" + i.ToString("D5", CultureInfo.InvariantCulture));

    // ---- LOUDS trie layout (utilities/trie_index/louds_trie.cc LoudsTrieBuilder::Finish / SerializeAll) ---------

    private const string TrieBlockName = UserDefinedIndexFactory.MetaBlockPrefix + TrieIndexFactory.ClassName;

    private static RocksDbSharp.ColumnFamilyOptions NativeTrie(int blockSize)
    {
        RocksDbSharp.Native n = RocksDbSharp.Native.Instance;
        var bbto = new RocksDbSharp.BlockBasedTableOptions().SetFormatVersion(7).SetBlockSize((ulong)blockSize);
        IntPtr s = Marshal.StringToHGlobalAnsi("trie_index");
        try
        {
            n.rocksdb_block_based_options_set_user_defined_index_factory_from_string(bbto.Handle, s, (UIntPtr)"trie_index".Length);
        }
        finally
        {
            Marshal.FreeHGlobal(s);
        }
        var cf = new RocksDbSharp.ColumnFamilyOptions();
        cf.SetBlockBasedTableFactory(bbto);
        cf.SetCompression(RocksDbSharp.Compression.No);
        s_keepAlive.Add(bbto);
        s_keepAlive.Add(cf);
        return cf;
    }

    private static byte[] TrieBlock(string sst)
    {
        byte[] file = File.ReadAllBytes(sst);
        using var reader = BlockBasedTableReader.Open(RocksDb.Managed.Env.LocalFileSystem.Instance.OpenRandomAccess(sst), new TableReaderOptions());
        BlockHandle h = reader.MetaBlocks[TrieBlockName];
        return file.AsSpan((int)h.Offset, (int)h.Size).ToArray();
    }

    private sealed class BytewiseComparer : IComparer<byte[]>
    {
        public int Compare(byte[]? a, byte[]? b) => a.AsSpan().SequenceCompareTo(b);
    }

    /// <summary>Key sets that drive the layout branches the string-keyed compat tests do not reach.</summary>
    private static IEnumerable<(string Name, int BlockSize, List<byte[]> Keys)> TrieShapes()
    {
        var cmp = new BytewiseComparer();
        var rnd = new Random(4711);
        // (a) Binary keys of 1..5 random bytes: dense levels with a fanout up to 256, labels 0x00 and 0xff, prefix keys
        // in the dense and in the sparse part.
        var set = new SortedSet<byte[]>(cmp);
        while (set.Count < 4000)
        {
            var k = new byte[1 + rnd.Next(5)];
            rnd.NextBytes(k);
            if (rnd.Next(4) == 0) k[^1] = (byte)(rnd.Next(2) == 0 ? 0x00 : 0xff);
            set.Add(k);
        }
        yield return ("binary", 64, new List<byte[]>(set));
        // (b) All 256 one-byte keys and all their 0x00 / 0xff extensions: one full dense root, every node a prefix key.
        set = new SortedSet<byte[]>(cmp);
        for (int i = 0; i < 256; i++)
        {
            set.Add([(byte)i]);
            set.Add([(byte)i, 0x00]);
            set.Add([(byte)i, 0xff]);
            set.Add([(byte)i, 0xff, 0xff]);
        }
        yield return ("full-root", 16, new List<byte[]>(set));
        // (c) A single long key and two keys sharing a 300-byte prefix: fanout-1 chains from the root.
        yield return ("one-long", 64, new List<byte[]> { B(new string('q', 500)) });
        yield return ("two-long", 16, new List<byte[]> { B(new string('q', 300) + "a" + new string('r', 40)), B(new string('q', 300) + "b" + new string('s', 70)) });
        // (d) Many chains of different lengths (8..120 bytes) below distinct two-byte heads: the chain cost / benefit
        // filter (10% budget, longest first). The lengths are all different so the unstable native sort has no ties.
        set = new SortedSet<byte[]>(cmp);
        for (int i = 0; i < 110; i++)
        {
            string head = ((char)('A' + i / 26)).ToString() + (char)('a' + i % 26);
            set.Add(B(head + new string((char)('0' + i % 10), 8 + i)));
            set.Add(B(head + "~"));
        }
        yield return ("chains", 16, new List<byte[]>(set));
        // (e) Nested prefix keys: a, aa, aaa, ... and their siblings.
        set = new SortedSet<byte[]>(cmp);
        for (int i = 1; i <= 60; i++)
        {
            set.Add(B(new string('a', i)));
            set.Add(B(new string('a', i) + "b"));
        }
        yield return ("nested-prefix", 16, new List<byte[]>(set));
    }

    /// <summary>
    /// LoudsTrieBuilder::Finish / SerializeAll: for binary keys (dense levels of fanout 256, labels 0x00 / 0xff),
    /// a full dense root with prefix keys everywhere, root chains, the chain budget filter and nested prefix keys, the
    /// trie block written by the managed SstFileWriter is byte-identical to native's, and the managed reader resolves
    /// every key of the native file through the native trie (Seek, SeekForPrev, both scan directions).
    /// </summary>
    [TestMethod]
    public void TrieLayoutBranches_ByteIdenticalToNative_AndReadThroughTheTrie()
    {
        foreach ((string name, int blockSize, List<byte[]> keys) in TrieShapes())
        {
            using var dir = new TempDir("a4-trie");
            string nativePath = Path.Combine(dir.Path, "native.sst"), managedPath = Path.Combine(dir.Path, "managed.sst");
            using (var w = new RocksDbSharp.SstFileWriter(new RocksDbSharp.EnvOptions(), NativeTrie(blockSize)))
            {
                w.Open(nativePath);
                for (int i = 0; i < keys.Count; i++) w.Put(keys[i], B("v" + i));
                w.Finish();
            }
            var cf = new ColumnFamilyOptions { Compression = RocksDb.Managed.Compression.CompressionType.None };
            cf.TableOptions.BlockSize = (ulong)blockSize;
            cf.TableOptions.FormatVersion = 7;
            cf.TableOptions.UserDefinedIndexFactory = TrieIndexFactory.Instance;
            cf.TableOptions.IndexMode = BlockBasedIndexMode.StandardDefault;
            using (var w = new SstFileWriter(cf))
            {
                w.Open(managedPath);
                for (int i = 0; i < keys.Count; i++) w.Put(keys[i], B("v" + i));
                w.Finish();
            }
            byte[] nt = TrieBlock(nativePath), mt = TrieBlock(managedPath);
            CollectionAssert.AreEqual(nt, mt, "trie block of shape " + name);

            // The native file read through its trie by the managed reader.
            var rcf = new ColumnFamilyOptions();
            rcf.TableOptions.UserDefinedIndexFactory = TrieIndexFactory.Instance;
            rcf.TableOptions.IndexMode = BlockBasedIndexMode.CustomDefault;
            using var reader = new SstFileReader(rcf);
            reader.Open(nativePath);
            using Iterator it = reader.NewIterator(new ReadOptions { ReadIndex = ReadIndex.PreferCustom });
            int n = 0;
            for (it.SeekToFirst(); it.Valid; it.Next()) Assert.IsTrue(it.Key.SequenceEqual(keys[n++]), name + " forward " + n);
            Assert.AreEqual(keys.Count, n, name);
            for (it.SeekToLast(); it.Valid; it.Prev()) Assert.IsTrue(it.Key.SequenceEqual(keys[--n]), name + " backward " + n);
            Assert.AreEqual(0, n, name);
            var cmp = new BytewiseComparer();
            var rnd = new Random(99);
            for (int probe = 0; probe < 600; probe++)
            {
                byte[] t = (byte[])keys[rnd.Next(keys.Count)].Clone();
                switch (rnd.Next(5))
                {
                    case 0: break;
                    case 1: t = t.AsSpan(0, rnd.Next(t.Length + 1)).ToArray(); break;
                    case 2: t = [.. t, (byte)rnd.Next(256)]; break;
                    case 3: t[rnd.Next(t.Length)] = (byte)rnd.Next(256); break;
                    default: t[^1] = (byte)(rnd.Next(2) == 0 ? 0x00 : 0xff); break;
                }
                int lb = keys.BinarySearch(t, cmp);
                int ge = lb >= 0 ? lb : ~lb;
                it.Seek(t);
                Assert.AreEqual(ge < keys.Count, it.Valid, name + " Seek " + Convert.ToHexString(t));
                if (it.Valid) Assert.IsTrue(it.Key.SequenceEqual(keys[ge]), name + " Seek " + Convert.ToHexString(t));
                int le = lb >= 0 ? lb : ~lb - 1;
                it.SeekForPrev(t);
                Assert.AreEqual(le >= 0, it.Valid, name + " SeekForPrev " + Convert.ToHexString(t));
                if (it.Valid) Assert.IsTrue(it.Key.SequenceEqual(keys[le]), name + " SeekForPrev " + Convert.ToHexString(t));
            }
        }
    }

    // ---- ticker call sites ------------------------------------------------------------------------------------

    private static Dictionary<string, ulong> Tickers(string statistics)
    {
        var d = new Dictionary<string, ulong>(StringComparer.Ordinal);
        foreach (Match m in Regex.Matches(statistics, "^(\\S+) COUNT : (\\d+)$", RegexOptions.Multiline))
            d[m.Groups[1].Value] = ulong.Parse(m.Groups[2].Value, CultureInfo.InvariantCulture);
        return d;
    }

    private static byte[] V(int i, int gen) => B("value" + gen + "-" + i.ToString("D5", CultureInfo.InvariantCulture) + new string('x', 80));

    private static string NativeTickerWorkload(string path)
    {
        var bbto = new RocksDbSharp.BlockBasedTableOptions().SetBlockSize(1024UL);
        var no = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).EnableStatistics();
        no.SetCompression(RocksDbSharp.Compression.No);
        no.SetDisableAutoCompactions(1);
        no.SetBlockBasedTableFactory(bbto);
        s_keepAlive.Add(no);
        s_keepAlive.Add(bbto);
        var flush = new RocksDbSharp.FlushOptions().SetWaitForFlush(true);
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(no, path))
        {
            for (int i = 0; i < 200; i++) db.Put(K(i), V(i, 0));
            using (var wb = new RocksDbSharp.WriteBatch())
            {
                for (int i = 200; i < 210; i++) wb.Put(K(i), V(i, 0));
                wb.Delete(K(3));
                wb.Delete(K(4));
                db.Write(wb);
            }
            for (int i = 0; i < 5; i++) db.Get(K(i));          // memtable: 3 hits, 2 tombstones
            db.Get(K(900));                                     // memtable miss, no file
            db.Flush(flush);
            for (int i = 0; i < 50; i++) db.Get(K(i * 4));     // L0
            for (int i = 0; i < 10; i++) db.Get(K(1000 + i));  // misses
            for (int i = 0; i < 100; i++) db.Put(K(i * 2), V(i * 2, 1));
            for (int i = 0; i < 20; i++) db.Remove(K(i * 10 + 1));
            db.Flush(flush);
            db.Put(K(500), V(500, 2));
            db.Get(K(500));
            db.MultiGet([K(0), K(1), K(7), K(500), K(901), K(11)]);
            using (RocksDbSharp.Iterator it = db.NewIterator())
            {
                it.SeekToFirst();
                for (int i = 0; i < 20 && it.Valid(); i++) it.Next();
                it.SeekToLast();
                for (int i = 0; i < 5 && it.Valid(); i++) it.Prev();
                it.Seek(K(100));
                it.Next();
                it.SeekForPrev(K(150));
            }
            db.Flush(flush);
            db.CompactRange(Array.Empty<byte>(), null);
            for (int i = 0; i < 30; i++) db.Get(K(i * 5));
            db.Put(K(600), V(600, 3), writeOptions: new RocksDbSharp.WriteOptions().SetSync(true));
            return no.GetStatisticsString();
        }
    }

    private static string ManagedTickerWorkload(string path)
    {
        var stats = new Statistics();
        var cf = new ColumnFamilyOptions { Compression = RocksDb.Managed.Compression.CompressionType.None, DisableAutoCompactions = true };
        cf.TableOptions.BlockSize = 1024;
        using (RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true, Statistics = stats }, cf))
        {
            for (int i = 0; i < 200; i++) db.Put(K(i), V(i, 0));
            using (WriteBatch wb = db.CreateWriteBatch())
            {
                for (int i = 200; i < 210; i++) wb.Put(K(i), V(i, 0));
                wb.Delete(K(3));
                wb.Delete(K(4));
                db.Write(wb);
            }
            for (int i = 0; i < 5; i++) db.Get(K(i));
            db.Get(K(900));
            db.Flush();
            for (int i = 0; i < 50; i++) db.Get(K(i * 4));
            for (int i = 0; i < 10; i++) db.Get(K(1000 + i));
            for (int i = 0; i < 100; i++) db.Put(K(i * 2), V(i * 2, 1));
            for (int i = 0; i < 20; i++) db.Delete(K(i * 10 + 1));
            db.Flush();
            db.Put(K(500), V(500, 2));
            db.Get(K(500));
            db.MultiGet([K(0), K(1), K(7), K(500), K(901), K(11)]);
            using (Iterator it = db.NewIterator())
            {
                it.SeekToFirst();
                for (int i = 0; i < 20 && it.Valid; i++) it.Next();
                it.SeekToLast();
                for (int i = 0; i < 5 && it.Valid; i++) it.Prev();
                it.Seek(K(100));
                it.Next();
                it.SeekForPrev(K(150));
            }
            db.Flush();
            db.CompactRange(null, null);
            for (int i = 0; i < 30; i++) db.Get(K(i * 5));
            db.Put(new WriteOptions { Sync = true }, K(600), V(600, 3));
            return stats.ToString();
        }
    }

    /// <summary>
    /// Tickers that cannot be compared exactly, each with its reason (the recorded ones are still checked to be within
    /// 25% of native by <see cref="FormerlyMissingTickersMatchNativeOrAreExcluded"/>).
    /// </summary>
    private static readonly HashSet<string> s_notComparable = new(StringComparer.Ordinal)
    {
        // The cache charge: native adds malloc_usable_size of the block and its metadata.
        "rocksdb.block.cache.bytes.write", "rocksdb.block.cache.bytes.read", "rocksdb.block.cache.data.bytes.insert",
        // CompactionJob::UpdateSubcompactionJobStatsIncrementally: CPU time of the compaction threads
        // (clock->CPUMicros(), clock_gettime(CLOCK_THREAD_CPUTIME_ID)); recorded by both, a measurement.
        "rocksdb.compaction.total.time.cpu_micros",
    };

    /// <summary>
    /// Tickers native increments in the workload that the port does not (the explicit exclusion list of
    /// <see cref="FormerlyMissingTickersMatchNativeOrAreExcluded"/>, which fails when one of them starts being recorded).
    /// </summary>
    private static readonly string[] s_notIncremented =
    [
    ];

    /// <summary>Formerly missing tickers that now have the same value as native for the workload.</summary>
    private static readonly string[] s_implementedExact =
    [
        "rocksdb.bytes.compression_bypassed", "rocksdb.number.block_compression_bypassed", // BlockBasedTableBuilder::CompressAndVerifyBlock
        "rocksdb.files.deleted.immediately", // DeleteScheduler::DeleteFileImmediately
        "rocksdb.memtable.payload.bytes.at.flush", "rocksdb.memtable.garbage.bytes.at.flush", // FlushJob::WriteLevel0Table
        "rocksdb.block.checksum.compute.count", // BlockFetcher::ProcessTrailerIfPresent
        "rocksdb.non.last.level.seek.data", "rocksdb.non.last.level.seek.data.useful.no.filter", // BlockBasedTableIterator::InitDataBlock / value()
        "rocksdb.last.level.read.bytes", "rocksdb.last.level.read.count", "rocksdb.non.last.level.read.bytes", "rocksdb.non.last.level.read.count", // RecordIOStats
        "rocksdb.compact.write.bytes", // CompactionJob::RecordCompactionIOStats, IOSTATS(bytes_written): the outputs
        "rocksdb.flush.write.bytes", // FlushJob::RecordFlushIOStats, IOSTATS(bytes_written): the table file + the install's MANIFEST record
        "rocksdb.compact.read.bytes", // CompactionJob::RecordCompactionIOStats, IOSTATS(bytes_read): input blocks + the output's open
    ];

    /// <summary>
    /// Formerly missing tickers that are recorded at the upstream call site but whose value cannot equal native's:
    /// the block cache byte tickers count the cache charge (native: malloc_usable_size of the block plus metadata).
    /// </summary>
    private static readonly string[] s_implementedNotComparable =
    [
        "rocksdb.block.cache.bytes.read", "rocksdb.block.cache.data.bytes.insert", // BlockBasedTable::UpdateCacheHitMetrics / UpdateCacheInsertionMetrics
    ];

    /// <summary>
    /// Recorded at the corresponding points (ColumnFamilyData::GetThreadLocalSuperVersion finding its cached reference
    /// obsolete, DBImpl::CleanupSuperVersion from ReturnAndCleanupSuperVersion) but not comparable: upstream caches one
    /// super version reference per thread and routes every GetReferencedSuperVersion caller (iterators, CompactRange)
    /// through it, the port caches one per processor slot for Get / MultiGet only, so the counts depend on the
    /// processor the thread runs on. Only checked to be recorded.
    /// </summary>
    private static readonly string[] s_implementedSlotDependent =
    [
        "rocksdb.number.superversion_acquires",
        // A measurement (thread CPU time of the compaction, Env/ThreadCpuClock.cs): only checked to be recorded.
        "rocksdb.compaction.total.time.cpu_micros",
    ];

    /// <summary>
    /// The RecordTick call sites of the write path (DBImpl::WriteImpl, WriteToWAL), the read path (DBImpl::GetImpl,
    /// MultiGet, MemTable::Get, Version::Get, BlockBasedTable block cache accounting), DBIter, the flush job and the
    /// compaction job: one deterministic single-threaded workload gives the same value for every ticker on both sides.
    /// </summary>
    [TestMethod]
    public void TickerValuesOfADeterministicWorkloadMatchNative()
    {
        using var nativeDir = new TempDir("a4-tick-n");
        using var managedDir = new TempDir("a4-tick-m");
        Dictionary<string, ulong> nt = Tickers(NativeTickerWorkload(nativeDir.Path));
        Dictionary<string, ulong> mt = Tickers(ManagedTickerWorkload(managedDir.Path));
        var diff = new StringBuilder();
        int compared = 0;
        foreach (KeyValuePair<string, ulong> kv in nt)
        {
            if (s_notComparable.Contains(kv.Key) || Array.IndexOf(s_notIncremented, kv.Key) >= 0) continue;
            if (kv.Key.StartsWith("rocksdb.number.superversion_", StringComparison.Ordinal)) continue; // s_implementedSlotDependent
            if (!mt.TryGetValue(kv.Key, out ulong m)) continue; // a ticker newer / older than the package
            compared++;
            if (m != kv.Value) diff.Append(kv.Key).Append(": native ").Append(kv.Value).Append(", managed ").Append(m).Append('\n');
        }
        Assert.IsTrue(compared > 150, compared.ToString(CultureInfo.InvariantCulture));
        // The workload must reach the call sites it is meant to pin down.
        foreach (string name in new[]
                 {
                     "rocksdb.bytes.written", "rocksdb.bytes.read", "rocksdb.number.keys.written", "rocksdb.number.keys.read", "rocksdb.wal.synced",
                     "rocksdb.wal.bytes", "rocksdb.write.self", "rocksdb.write.wal", "rocksdb.block.cache.hit", "rocksdb.block.cache.miss",
                     "rocksdb.block.cache.add", "rocksdb.block.cache.data.hit", "rocksdb.block.cache.data.miss", "rocksdb.block.cache.data.add",
                     "rocksdb.memtable.hit", "rocksdb.memtable.miss", "rocksdb.l0.hit", "rocksdb.l2andup.hit", "rocksdb.number.db.seek",
                     "rocksdb.number.db.next", "rocksdb.number.db.prev", "rocksdb.db.iter.bytes.read", "rocksdb.number.iter.skip",
                     "rocksdb.compaction.key.drop.new", "rocksdb.compaction.key.drop.obsolete", "rocksdb.no.file.opens",
                     "rocksdb.number.multiget.get", "rocksdb.number.multiget.keys.read", "rocksdb.number.multiget.keys.found", "rocksdb.number.multiget.bytes.read",
                 })
        {
            Assert.IsTrue(nt[name] > 0, name + " is not exercised");
        }
        Assert.AreEqual("", diff.ToString());
    }

    /// <summary>
    /// The tickers the first ticker comparison found missing in the port: each one is now either equal to native
    /// (<see cref="s_implementedExact"/>), recorded but not comparable (<see cref="s_implementedNotComparable"/>, within
    /// 25% of native) or explicitly excluded with its reason (<see cref="s_notIncremented"/>).
    /// </summary>
    [TestMethod]
    public void FormerlyMissingTickersMatchNativeOrAreExcluded()
    {
        using var nativeDir = new TempDir("a4-tick0-n");
        using var managedDir = new TempDir("a4-tick0-m");
        Dictionary<string, ulong> nt = Tickers(NativeTickerWorkload(nativeDir.Path));
        Dictionary<string, ulong> mt = Tickers(ManagedTickerWorkload(managedDir.Path));
        var diff = new StringBuilder();
        foreach (string name in s_implementedExact)
        {
            Assert.IsTrue(nt[name] > 0, name + " is not exercised");
            if (mt[name] != nt[name]) diff.Append(name).Append(": native ").Append(nt[name]).Append(", managed ").Append(mt[name]).Append('\n');
        }
        foreach (string name in s_implementedNotComparable)
        {
            Assert.IsTrue(nt[name] > 0, name + " is not exercised");
            double ratio = mt[name] / (double)nt[name];
            if (ratio < 0.75 || ratio > 1.25) diff.Append(name).Append(": native ").Append(nt[name]).Append(", managed ").Append(mt[name]).Append('\n');
        }
        foreach (string name in s_implementedSlotDependent)
        {
            Assert.IsTrue(nt[name] > 0, name + " is not exercised");
            if (mt[name] == 0) diff.Append(name).Append(" is not recorded\n");
        }
        foreach (string name in s_notIncremented)
        {
            Assert.IsTrue(nt[name] > 0, name + " is not exercised");
            if (mt[name] != 0) diff.Append(name).Append(" is recorded now (").Append(mt[name]).Append("): compare it with native\n");
        }
        Assert.AreEqual("", diff.ToString());
    }
}
