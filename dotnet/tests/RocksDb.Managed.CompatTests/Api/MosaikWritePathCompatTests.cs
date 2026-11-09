using System.Buffers.Binary;
using RocksDb.Managed.Cache;
using RocksDb.Managed.Compression;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Db;
using RocksDb.Managed.Memtable;
using RocksDb.Managed.Table.Filters;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>
/// The write path as Mosaik (Mosaik/Graph/src/Graph/Storage) drives it, replayed on native RocksDB (rocksdb-sharp) and
/// on the managed port with Mosaik's option values, and the results compared: every column family, snapshot views,
/// the latest sequence number, a replica fed from the other engine's WAL, and each engine opening the other's files.
/// <para>
/// Mosaik's options (RocksDbBase / DBOptionsExt / RocksDbStorage / SafeRocksDb): one shared LRU block cache, two-level
/// index with partitioned filters cached and pinned; point-lookup families with a hash data-block index, memtable prefix
/// bloom and whole-key filtering; fixed 8-byte prefix families with a bloom filter and optimize_filters_for_hits; the
/// replica change tracker with a custom ulong comparator (first 8 bytes, little endian), a 16-byte prefix extractor
/// longer than its 8-byte keys and a prefix-only bloom; a shared write buffer manager without stalls, a separate WAL dir
/// with the archive kept (wal_ttl 100 years), max_total_wal_size 1 MB (low-resource mode), SkipAnyCorruptedRecords
/// recovery. Writes use the default WriteOptions (no sync, WAL on), as every Mosaik call does.
/// </para>
/// <para>
/// Write patterns (file:line in Mosaik/Graph/src/Graph/Storage): node writes as one batch over nodes/types/nodeversion/
/// edges (RocksDbStorage.cs:2405-2510); blob rewrites deleting the old 32 KB pages and putting the new ones in the same
/// batch (RocksDbBlobStorage.cs:92-136, 220-230); queue Put/Remove/re-prioritise (RocksDbIndexStorage.FileQueue.cs:304,
/// 323, 677-687); ClearFileQueue's DeleteRange [0^24, FF^24) (FileQueue.cs:649-654); ClearDeprecatedChangeLog's
/// DeleteFilesInRange + DeleteRange [0^8, FF^8) + CompactRange (RocksDbStorage.cs:1690-1704); the change tracker's
/// Put / Remove of acknowledged versions / CompactRange(0, ack) (RocksDbBase.cs:290-330, 395-414); the replica applying
/// the primary's serialized batches (ReplicationConsumer.IngestChunk).
/// </para>
/// </summary>
[TestClass]
public sealed class MosaikWritePathCompatTests
{
    // ---- column families --------------------------------------------------------------------------------------------

    private enum Kind { Plain, PointLookup, PointLookupLz4, Prefix8, Prefix8Bloom, ChangeTracker }

    private static readonly (string Name, Kind Kind)[] s_families =
    [
        ("default", Kind.Plain),
        ("nodes", Kind.Plain),
        ("types", Kind.Plain),
        ("edges", Kind.Plain),
        ("content", Kind.PointLookupLz4),
        ("nodeversion", Kind.PointLookup),
        ("changelog", Kind.Plain),
        ("graphsettings", Kind.Plain),
        ("blob-length", Kind.Plain),
        ("blob-pages", Kind.Plain),
        ("data-vectors", Kind.Prefix8),
        ("to-index-queue", Kind.Prefix8Bloom),
        ("file-extraction-queue", Kind.Plain),
        ("replica-change-tracker", Kind.ChangeTracker),
    ];

    private const int Nodes = 1, Types = 2, Edges = 3, Content = 4, NodeVersion = 5, ChangeLog = 6, Settings = 7,
        BlobLength = 8, BlobPages = 9, DataVectors = 10, ToIndexQueue = 11, FileQueue = 12, ChangeTracker = 13;

    private const string ULongComparatorName = "Mosaik.GraphDB.ulong_Comparator";
    private const ulong WalTtlNever = 100UL * 365 * 24 * 3600;
    private const int BlobPageSize = 32768;

    /// <summary>Mosaik's ulong_Comparator: the first 8 bytes as a little-endian ulong.</summary>
    private sealed class ManagedULongComparator : Comparator
    {
        public static readonly ManagedULongComparator Instance = new();
        public override string Name => ULongComparatorName;
        public override int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) =>
            BinaryPrimitives.ReadUInt64LittleEndian(a).CompareTo(BinaryPrimitives.ReadUInt64LittleEndian(b));
    }

    private sealed unsafe class NativeULongComparator : RocksDbSharp.Comparator
    {
        public static readonly NativeULongComparator Instance = new();
        public string Name => ULongComparatorName;
        public int Compare(IntPtr a, UIntPtr alen, IntPtr b, UIntPtr blen) =>
            BinaryPrimitives.ReadUInt64LittleEndian(new ReadOnlySpan<byte>((void*)a, (int)alen))
                .CompareTo(BinaryPrimitives.ReadUInt64LittleEndian(new ReadOnlySpan<byte>((void*)b, (int)blen)));
    }

    // ---- engines ----------------------------------------------------------------------------------------------------

    private readonly record struct Op(bool IsDelete, int Cf, byte[] Key, byte[]? Value);

    private interface IEngine : IDisposable
    {
        void Put(int cf, byte[] key, byte[] value);
        void Remove(int cf, byte[] key);
        void Write(List<Op> batch);
        void WriteRaw(byte[] rep);
        void DeleteRange(int cf, byte[] begin, byte[] end);
        void DeleteFilesInRange(int cf, byte[] begin, byte[] end);
        void Flush(int cf);
        void Compact(int cf, byte[]? begin, byte[]? end);
        byte[]? Get(int cf, byte[] key, int snapshot);
        List<KeyValuePair<byte[], byte[]>> Dump(int cf, int snapshot);
        int TakeSnapshot();
        void ReleaseSnapshot(int id);
        ulong LatestSequence { get; }
        List<byte[]> BatchesSince(ulong sequence);
    }

    private sealed class ManagedEngine : IEngine
    {
        private readonly RocksDatabase _db;
        private readonly ColumnFamily[] _cfs;
        private readonly Dictionary<int, Snapshot> _snapshots = new();
        private int _nextSnapshot;

        public ManagedEngine(string path, string walDir, WriteBufferManager wbm, BlockCache cache)
        {
            var descriptors = new List<ColumnFamilyDescriptor>();
            foreach ((string name, Kind kind) in s_families) descriptors.Add(new ColumnFamilyDescriptor(name, ManagedFamilyOptions(kind, cache)));
            _db = RocksDatabase.Open(path, ManagedDbOptions(walDir, wbm), descriptors);
            _cfs = new ColumnFamily[s_families.Length];
            for (int i = 0; i < s_families.Length; i++) _cfs[i] = _db.GetColumnFamily(s_families[i].Name);
        }

        public void Put(int cf, byte[] key, byte[] value) => _cfs[cf].Put(key, value);
        public void Remove(int cf, byte[] key) => _cfs[cf].Delete(key);

        public void Write(List<Op> batch)
        {
            using WriteBatch b = _db.CreateWriteBatch();
            foreach (Op op in batch)
            {
                if (op.IsDelete) b.Delete(_cfs[op.Cf], op.Key);
                else b.Put(_cfs[op.Cf], op.Key, op.Value);
            }
            Assert.AreEqual(batch.Count, b.Count);
            _db.Write(b);
        }

        public void WriteRaw(byte[] rep)
        {
            using var b = new WriteBatch(rep);
            _db.Write(b);
        }

        public void DeleteRange(int cf, byte[] begin, byte[] end) => _cfs[cf].DeleteRange(new WriteOptions(), begin, end);
        public void DeleteFilesInRange(int cf, byte[] begin, byte[] end) => _cfs[cf].DeleteFilesInRange(begin, end, includeEnd: true);
        public void Flush(int cf) => _cfs[cf].Flush();
        public void Compact(int cf, byte[]? begin, byte[]? end) => _cfs[cf].CompactRange(begin, end);

        public byte[]? Get(int cf, byte[] key, int snapshot) =>
            snapshot < 0 ? _cfs[cf].Get(key) : _cfs[cf].Get(new ReadOptions { Snapshot = _snapshots[snapshot] }, key);

        public List<KeyValuePair<byte[], byte[]>> Dump(int cf, int snapshot)
        {
            var list = new List<KeyValuePair<byte[], byte[]>>();
            using Iterator it = _cfs[cf].NewIterator(snapshot < 0 ? null : new ReadOptions { Snapshot = _snapshots[snapshot] });
            for (it.SeekToFirst(); it.Valid; it.Next()) list.Add(new(it.Key.ToArray(), it.Value.ToArray()));
            return list;
        }

        public int TakeSnapshot()
        {
            _snapshots[_nextSnapshot] = _db.GetSnapshot();
            return _nextSnapshot++;
        }

        public void ReleaseSnapshot(int id)
        {
            _snapshots[id].Dispose();
            _snapshots.Remove(id);
        }

        public ulong LatestSequence => _db.GetLatestSequenceNumber();

        public List<byte[]> BatchesSince(ulong sequence)
        {
            var list = new List<byte[]>();
            using TransactionLogIterator it = _db.GetUpdatesSince(sequence);
            for (; it.Valid; it.Next()) list.Add(it.Current.Span.ToArray());
            it.ThrowIfError();
            return list;
        }

        public void Dispose()
        {
            foreach (Snapshot s in _snapshots.Values) s.Dispose();
            _snapshots.Clear();
            _db.Dispose();
        }
    }

    private sealed class NativeEngine : IEngine
    {
        private readonly RocksDbSharp.RocksDb _db;
        private readonly RocksDbSharp.ColumnFamilyHandle[] _cfs;
        private readonly Dictionary<int, RocksDbSharp.Snapshot> _snapshots = new();
        private readonly RocksDbSharp.WriteOptions _writeOptions = new();
        private readonly RocksDbSharp.FlushOptions _flushOptions = new RocksDbSharp.FlushOptions().SetWaitForFlush(true);
        private int _nextSnapshot;

        public NativeEngine(string path, string walDir, IntPtr wbm, RocksDbSharp.Cache cache)
        {
            RocksDbSharp.ColumnFamilies? families = null;
            foreach ((string name, Kind kind) in s_families)
            {
                if (families is null) families = new RocksDbSharp.ColumnFamilies(NativeFamilyOptions(kind, cache));
                else families.Add(name, NativeFamilyOptions(kind, cache));
            }
            _db = RocksDbSharp.RocksDb.Open(NativeDbOptions(walDir, wbm), path, families);
            _cfs = new RocksDbSharp.ColumnFamilyHandle[s_families.Length];
            for (int i = 0; i < s_families.Length; i++) _cfs[i] = _db.GetColumnFamily(s_families[i].Name);
        }

        public void Put(int cf, byte[] key, byte[] value) => _db.Put(key, value, _cfs[cf]);
        public void Remove(int cf, byte[] key) => _db.Remove(key, _cfs[cf]);

        public void Write(List<Op> batch)
        {
            using var b = new RocksDbSharp.WriteBatch();
            foreach (Op op in batch)
            {
                if (op.IsDelete) b.Delete(op.Key, _cfs[op.Cf]);
                else b.Put(op.Key, op.Value, _cfs[op.Cf]);
            }
            Assert.AreEqual(batch.Count, b.Count());
            _db.Write(b);
        }

        public void WriteRaw(byte[] rep)
        {
            using var b = new RocksDbSharp.WriteBatch(rep);
            _db.Write(b);
        }

        public void DeleteRange(int cf, byte[] begin, byte[] end)
        {
            // Exactly Mosaik's call (RocksDbStorage.cs:1699, RocksDbIndexStorage.FileQueue.cs:654)
            RocksDbSharp.Native.Instance.rocksdb_delete_range_cf(_db.Handle, _writeOptions.Handle, _cfs[cf].Handle, begin, (nuint)begin.Length,
                end, (nuint)end.Length, out IntPtr err);
            if (err != IntPtr.Zero) throw new RocksDbSharp.RocksDbException(err);
        }

        public void DeleteFilesInRange(int cf, byte[] begin, byte[] end)
        {
            RocksDbSharp.Native.Instance.rocksdb_delete_file_in_range_cf(_db.Handle, _cfs[cf].Handle, begin, (nuint)begin.Length, end, (nuint)end.Length,
                out IntPtr err);
            if (err != IntPtr.Zero) throw new RocksDbSharp.RocksDbException(err);
        }

        public void Flush(int cf)
        {
            RocksDbSharp.Native.Instance.rocksdb_flush_cf(_db.Handle, _flushOptions.Handle, _cfs[cf].Handle, out IntPtr err);
            if (err != IntPtr.Zero) throw new RocksDbSharp.RocksDbException(err);
        }

        public void Compact(int cf, byte[]? begin, byte[]? end) => _db.CompactRange(begin, end, _cfs[cf]);

        public byte[]? Get(int cf, byte[] key, int snapshot) =>
            _db.Get(key, _cfs[cf], snapshot < 0 ? null : new RocksDbSharp.ReadOptions().SetSnapshot(_snapshots[snapshot]));

        public List<KeyValuePair<byte[], byte[]>> Dump(int cf, int snapshot)
        {
            var list = new List<KeyValuePair<byte[], byte[]>>();
            using RocksDbSharp.Iterator it = _db.NewIterator(_cfs[cf], snapshot < 0 ? null : new RocksDbSharp.ReadOptions().SetSnapshot(_snapshots[snapshot]));
            for (it.SeekToFirst(); it.Valid(); it.Next()) list.Add(new(it.Key(), it.Value()));
            return list;
        }

        public int TakeSnapshot()
        {
            _snapshots[_nextSnapshot] = _db.CreateSnapshot();
            return _nextSnapshot++;
        }

        public void ReleaseSnapshot(int id)
        {
            _snapshots[id].Dispose();
            _snapshots.Remove(id);
        }

        public ulong LatestSequence => _db.GetLatestSequenceNumber();

        public List<byte[]> BatchesSince(ulong sequence)
        {
            var list = new List<byte[]>();
            using RocksDbSharp.TransactionLogIterator it = _db.GetUpdatesSince(sequence);
            for (; it.Valid(); it.Next())
            {
                using RocksDbSharp.WriteBatch b = it.GetBatch(out _);
                list.Add(b.ToBytes());
            }
            return list;
        }

        public void Dispose()
        {
            foreach (RocksDbSharp.Snapshot s in _snapshots.Values) s.Dispose();
            _snapshots.Clear();
            _db.Dispose();
        }
    }

    // ---- Mosaik's options -------------------------------------------------------------------------------------------

    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    private static DbOptions ManagedDbOptions(string walDir, WriteBufferManager wbm) => new()
    {
        CreateIfMissing = true,
        CreateMissingColumnFamilies = true,
        WalDir = walDir,
        WriteBufferManager = wbm,
        MaxOpenFiles = 8,
        MaxBackgroundJobs = 2,
        BytesPerSync = 1_048_576,
        WalBytesPerSync = 1_048_576,
        WalTtlSeconds = WalTtlNever,
        WalSizeLimitMb = 0,
        MaxTotalWalSize = 1UL * 1024 * 1024,
        SkipStatsUpdateOnDbOpen = true,
        WalRecoveryMode = WalRecoveryMode.SkipAnyCorruptedRecords,
    };

    private static RocksDbSharp.DbOptions NativeDbOptions(string walDir, IntPtr wbm)
    {
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetCreateMissingColumnFamilies(true).SetWalDir(walDir);
        RocksDbSharp.Native.Instance.rocksdb_options_set_write_buffer_manager(o.Handle, wbm);
        o.SetMaxOpenFiles(8);
        RocksDbSharp.Native.Instance.rocksdb_options_set_max_background_jobs(o.Handle, 2);
        o.SetBytesPerSync(1_048_576);
        RocksDbSharp.Native.Instance.rocksdb_options_set_wal_bytes_per_sync(o.Handle, 1_048_576);
        o.SetWalTtlSeconds(WalTtlNever);
        o.SetWalSizeLimitMB(0);
        o.SetMaxTotalWalSize(1UL * 1024 * 1024);
        o.SkipStatsUpdateOnOpen(true);
        o.SetWalRecoveryMode(RocksDbSharp.Recovery.SkipAnyCorruptedRecords);
        s_keepAlive.Add(o);
        return o;
    }

    private static BlockBasedTableOptions ManagedTableOptions(BlockCache cache, bool bloom) => new()
    {
        BlockCache = cache,
        CacheIndexAndFilterBlocks = true,
        CacheIndexAndFilterBlocksWithHighPriority = true,
        PinL0FilterAndIndexBlocksInCache = true,
        IndexType = IndexType.TwoLevelIndexSearch,
        PartitionFilters = true,
        PinTopLevelIndexAndFilter = true,
        FilterPolicy = bloom ? new BloomFilterPolicy(10) : null,
    };

    private static ColumnFamilyOptions ManagedFamilyOptions(Kind kind, BlockCache cache)
    {
        var o = new ColumnFamilyOptions();
        switch (kind)
        {
            case Kind.Plain:
                o.TableOptions = ManagedTableOptions(cache, bloom: false);
                break;
            case Kind.PointLookup:
            case Kind.PointLookupLz4:
                o.TableOptions = ManagedTableOptions(cache, bloom: true);
                o.TableOptions.DataBlockIndexType = DataBlockIndexType.BinaryAndHash;
                o.TableOptions.DataBlockHashTableUtilRatio = 0.75;
                o.MemtablePrefixBloomSizeRatio = 0.02;
                o.MemtableWholeKeyFiltering = true;
                if (kind == Kind.PointLookupLz4)
                {
                    o.Compression = CompressionType.LZ4;
                    o.ArenaBlockSize = 1UL * 1024 * 1024;
                }
                break;
            case Kind.Prefix8:
                // Mosaik's SetAdviseRandomOnOpen(false) on these family options sets a DB option: a no-op there
                o.TableOptions = ManagedTableOptions(cache, bloom: false);
                o.PrefixExtractor = SliceTransform.CreateFixedPrefix(8);
                break;
            case Kind.Prefix8Bloom:
                o.TableOptions = ManagedTableOptions(cache, bloom: true);
                o.PrefixExtractor = SliceTransform.CreateFixedPrefix(8);
                o.OptimizeFiltersForHits = true;
                break;
            case Kind.ChangeTracker:
                o.TableOptions = ManagedTableOptions(cache, bloom: true);
                o.TableOptions.WholeKeyFiltering = false;
                o.PrefixExtractor = SliceTransform.CreateFixedPrefix(16);
                o.OptimizeFiltersForHits = true;
                o.Comparator = ManagedULongComparator.Instance;
                break;
        }
        return o;
    }

    private static RocksDbSharp.BlockBasedTableOptions NativeTableOptions(RocksDbSharp.Cache cache, bool bloom)
    {
        var t = new RocksDbSharp.BlockBasedTableOptions().SetBlockCache(cache);
        var n = RocksDbSharp.Native.Instance;
        byte on = RocksDbSharp.Native.MarshalBool(true);
        n.rocksdb_block_based_options_set_cache_index_and_filter_blocks(t.Handle, on);
        n.rocksdb_block_based_options_set_cache_index_and_filter_blocks_with_high_priority(t.Handle, on);
        n.rocksdb_block_based_options_set_pin_l0_filter_and_index_blocks_in_cache(t.Handle, on);
        n.rocksdb_block_based_options_set_index_type(t.Handle, RocksDbSharp.BlockBasedTableIndexType.TwoLevelIndex);
        n.rocksdb_block_based_options_set_partition_filters(t.Handle, on);
        n.rocksdb_block_based_options_set_pin_top_level_index_and_filter(t.Handle, on);
        if (bloom) t.SetFilterPolicy(RocksDbSharp.BloomFilterPolicy.Create(10, false));
        s_keepAlive.Add(t);
        return t;
    }

    private static RocksDbSharp.ColumnFamilyOptions NativeFamilyOptions(Kind kind, RocksDbSharp.Cache cache)
    {
        var o = new RocksDbSharp.ColumnFamilyOptions();
        var n = RocksDbSharp.Native.Instance;
        switch (kind)
        {
            case Kind.Plain:
                o.SetBlockBasedTableFactory(NativeTableOptions(cache, bloom: false));
                break;
            case Kind.PointLookup:
            case Kind.PointLookupLz4:
                RocksDbSharp.BlockBasedTableOptions t = NativeTableOptions(cache, bloom: true);
                n.rocksdb_block_based_options_set_data_block_index_type(t.Handle, (int)RocksDbSharp.BlockBasedTableDataBlockIndexType.BinarySearchAndHash);
                n.rocksdb_block_based_options_set_data_block_hash_table_util_ratio(t.Handle, 0.75);
                o.SetBlockBasedTableFactory(t).SetMemtablePrefixBloomSizeRatio(0.02);
                n.rocksdb_options_set_memtable_whole_key_filtering(o.Handle, RocksDbSharp.Native.MarshalBool(true));
                if (kind == Kind.PointLookupLz4) o.SetCompression(RocksDbSharp.Compression.Lz4).SetArenaBlockSize(1UL * 1024 * 1024);
                break;
            case Kind.Prefix8:
                o.SetBlockBasedTableFactory(NativeTableOptions(cache, bloom: false)).SetPrefixExtractor(RocksDbSharp.SliceTransform.CreateFixedPrefix(8))
                    .SetAdviseRandomOnOpen(false);
                break;
            case Kind.Prefix8Bloom:
                o.SetPrefixExtractor(RocksDbSharp.SliceTransform.CreateFixedPrefix(8)).SetBlockBasedTableFactory(NativeTableOptions(cache, bloom: true))
                    .SetOptimizeFiltersForHits(1);
                break;
            case Kind.ChangeTracker:
                RocksDbSharp.BlockBasedTableOptions ct = NativeTableOptions(cache, bloom: true).SetWholeKeyFiltering(false);
                o.SetPrefixExtractor(RocksDbSharp.SliceTransform.CreateFixedPrefix(16)).SetBlockBasedTableFactory(ct).SetOptimizeFiltersForHits(1);
                o.SetComparator(NativeULongComparator.Instance);
                break;
        }
        s_keepAlive.Add(o);
        return o;
    }

    // ---- the workload -----------------------------------------------------------------------------------------------

    /// <summary>
    /// Generates Mosaik-shaped operations from a seed and applies each to every engine. The model tracks what the
    /// application itself tracks (existing blob lengths, queued keys, change-tracker versions) so both engines get
    /// byte-identical operations.
    /// </summary>
    private sealed class Workload(int seed, IEngine[] engines)
    {
        private readonly Random _r = new(seed);
        private readonly Dictionary<int, long> _blobLengths = new();
        private readonly List<byte[]> _queued = new();
        private readonly List<byte[]> _fileQueue = new();
        private readonly List<byte[]> _vectors = new();
        private readonly List<int> _snapshots = new();
        private readonly HashSet<int> _changeLogFilesDropped = new();
        private ulong _trackerVersion, _trackerAck, _trackerCompact = 50;
        private long _changeLogVersion;

        public IEngine[] Engines { get; set; } = engines;
        public IReadOnlyList<int> Snapshots => _snapshots;

        /// <summary>
        /// Snapshots older than a DeleteFilesInRange on the changelog: that call drops whole L1+ files regardless of
        /// snapshots (documented upstream), and which entries sit in such files depends on each engine's background
        /// flush / compaction timing, so the changelog seen through these snapshots legitimately differs.
        /// </summary>
        public bool ChangeLogFilesDroppedUnder(int snapshot) => _changeLogFilesDropped.Contains(snapshot);

        public void ForgetSnapshots()
        {
            _snapshots.Clear();
            _changeLogFilesDropped.Clear();
        }

        private byte[] Bytes(int len)
        {
            var b = new byte[len];
            _r.NextBytes(b);
            return b;
        }

        private static byte[] Uid(int i)
        {
            var b = new byte[16];
            BinaryPrimitives.WriteUInt64LittleEndian(b, (ulong)i * 0x9E3779B97F4A7C15UL);
            BinaryPrimitives.WriteUInt64LittleEndian(b.AsSpan(8), (ulong)i);
            return b;
        }

        private static byte[] SubKey(byte[] uid, int n)
        {
            var k = new byte[20];
            uid.CopyTo(k, 0);
            BinaryPrimitives.WriteInt32BigEndian(k.AsSpan(16), n);
            return k;
        }

        private static byte[] Le8(ulong v)
        {
            var b = new byte[8];
            BinaryPrimitives.WriteUInt64LittleEndian(b, v);
            return b;
        }

        private static byte[] Be8(long v)
        {
            var b = new byte[8];
            BinaryPrimitives.WriteInt64BigEndian(b, v);
            return b;
        }

        private void Each(Action<IEngine> a)
        {
            foreach (IEngine e in Engines) a(e);
        }

        private void NodeWrite()
        {
            byte[] uid = Uid(_r.Next(300));
            var batch = new List<Op>
            {
                new(false, Nodes, uid, Bytes(_r.Next(20, 400))),
                new(false, Types, uid, BitConverter.GetBytes(_r.Next(1, 40))),
                new(false, NodeVersion, uid, BitConverter.GetBytes(_r.NextInt64())),
            };
            int edges = _r.Next(0, 4);
            for (int i = 0; i < edges; i++) batch.Add(new(false, Edges, SubKey(uid, _r.Next(5)), Bytes(_r.Next(8, 200))));
            Each(e => e.Write(batch));
            byte[] content = Bytes(_r.Next(50, 3000));
            Each(e => e.Put(Content, uid, content));
            // NextChangeLogVersion's reservation (RocksDbStorage.cs:1654) and a changelog entry
            _changeLogVersion++;
            byte[] reserved = BitConverter.GetBytes(_changeLogVersion + 1000);
            Each(e => e.Put(Settings, "changelog-version"u8.ToArray(), reserved));
            byte[] log = Bytes(24);
            byte[] logKey = Be8(_changeLogVersion);
            Each(e => e.Put(ChangeLog, logKey, log));
        }

        private void NodeDelete()
        {
            byte[] uid = Uid(_r.Next(300));
            var batch = new List<Op> { new(true, Types, uid, null), new(true, Nodes, uid, null), new(true, NodeVersion, uid, null) };
            for (int i = 0; i < 5; i++) batch.Add(new(true, Edges, SubKey(uid, i), null));
            Each(e => e.Write(batch));
            Each(e => e.Remove(Content, uid));
        }

        private static int Pages(long length) => (int)((length + BlobPageSize - 1) / BlobPageSize);

        private void BlobStore()
        {
            int id = _r.Next(25);
            byte[] uid = Uid(10_000 + id);
            long length = _r.Next(4) == 0 ? 0 : _r.Next(1, 5 * BlobPageSize + 100);
            var batch = new List<Op> { new(false, BlobLength, uid, BitConverter.GetBytes(length)) };
            if (_blobLengths.TryGetValue(id, out long existing) && existing > 0)
            {
                for (int p = 0; p < Pages(existing); p++) batch.Add(new(true, BlobPages, SubKey(uid, p), null));
            }
            for (int p = 0; p < Pages(length); p++)
            {
                int size = (int)Math.Min(BlobPageSize, length - (long)p * BlobPageSize);
                batch.Add(new(false, BlobPages, SubKey(uid, p), Bytes(size)));
            }
            _blobLengths[id] = length;
            Each(e => e.Write(batch));
        }

        private void BlobDelete()
        {
            int id = _r.Next(25);
            if (!_blobLengths.TryGetValue(id, out long length)) return;
            byte[] uid = Uid(10_000 + id);
            var batch = new List<Op> { new(true, BlobLength, uid, null) };
            for (int p = 0; p < Pages(length); p++) batch.Add(new(true, BlobPages, SubKey(uid, p), null));
            _blobLengths.Remove(id);
            Each(e => e.Write(batch));
        }

        private void Queue()
        {
            if (_queued.Count > 0 && _r.Next(3) == 0)
            {
                int i = _r.Next(_queued.Count);
                byte[] k = _queued[i];
                _queued.RemoveAt(i);
                Each(e => e.Remove(ToIndexQueue, k));
            }
            else
            {
                byte[] k = new byte[16];
                BinaryPrimitives.WriteUInt64LittleEndian(k, (ulong)_r.Next(8));
                BinaryPrimitives.WriteUInt64LittleEndian(k.AsSpan(8), (ulong)_r.Next(200));
                _queued.Add(k);
                Each(e => e.Put(ToIndexQueue, k, []));
            }
        }

        private byte[] FileQueueKey()
        {
            byte[] k = Bytes(24);
            if (_r.Next(40) == 0) Array.Fill(k, (byte)0xFF); // the one key ClearFileQueue's exclusive end leaves behind
            else if (_r.Next(40) == 0) Array.Fill(k, (byte)0x00); // its inclusive begin
            return k;
        }

        private void FileQueueOp()
        {
            int what = _r.Next(10);
            if (what < 5 || _fileQueue.Count == 0)
            {
                byte[] k = FileQueueKey();
                byte[] v = Bytes(28);
                _fileQueue.Add(k);
                Each(e => e.Put(FileQueue, k, v));
            }
            else if (what < 8)
            {
                int i = _r.Next(_fileQueue.Count);
                byte[] k = _fileQueue[i];
                _fileQueue.RemoveAt(i);
                Each(e => e.Remove(FileQueue, k));
            }
            else
            {
                // ReEnqueueEntryWithRecomputedPriority: Remove, then Put under the new key
                int i = _r.Next(_fileQueue.Count);
                byte[] old = _fileQueue[i];
                byte[] k = FileQueueKey();
                byte[] v = Bytes(28);
                _fileQueue[i] = k;
                Each(e => e.Remove(FileQueue, old));
                Each(e => e.Put(FileQueue, k, v));
            }
        }

        private void ClearFileQueue()
        {
            _fileQueue.Clear();
            byte[] begin = new byte[24];
            byte[] end = new byte[24];
            Array.Fill(end, (byte)0xFF);
            Each(e => e.DeleteRange(FileQueue, begin, end));
        }

        private void ClearDeprecatedChangeLog()
        {
            byte[] begin = new byte[8];
            byte[] end = new byte[8];
            Array.Fill(end, (byte)0xFF);
            Each(e => e.DeleteFilesInRange(ChangeLog, begin, end));
            foreach (int s in _snapshots) _changeLogFilesDropped.Add(s);
            Each(e => e.DeleteRange(ChangeLog, begin, end));
            Each(e => e.Compact(ChangeLog, null, null));
        }

        private void Vectors()
        {
            if (_vectors.Count > 0 && _r.Next(4) == 0)
            {
                int i = _r.Next(_vectors.Count);
                byte[] k = _vectors[i];
                _vectors.RemoveAt(i);
                Each(e => e.Remove(DataVectors, k));
            }
            else
            {
                byte[] k = new byte[16];
                BinaryPrimitives.WriteUInt64LittleEndian(k, (ulong)_r.Next(50));
                BinaryPrimitives.WriteUInt64LittleEndian(k.AsSpan(8), (ulong)_r.Next(10));
                byte[] v = Bytes(_r.Next(64, 1600));
                _vectors.Add(k);
                Each(e => e.Put(DataVectors, k, v));
            }
        }

        private void TrackChange()
        {
            // RocksDbBase.TrackChangesHandler: the version under graphsettings, then the change under its 8-byte key
            ulong version = ++_trackerVersion;
            byte[] v = Le8(version);
            Each(e => e.Put(Settings, "global-primary-to-replica-version"u8.ToArray(), v));
            byte[] data = Bytes(_r.Next(1, 600));
            Each(e => e.Put(ChangeTracker, Le8(version), data));
        }

        private void AckChanges()
        {
            // RocksDbBase.ClearReplicationChangesAlreadyReplicated
            ulong lowest = _trackerAck + (ulong)_r.Next(0, (int)Math.Min(40, _trackerVersion - _trackerAck + 1));
            for (ulong version = _trackerAck; version < lowest; version++)
            {
                byte[] k = Le8(version);
                Each(e => e.Remove(ChangeTracker, k));
            }
            byte[] ack = Le8(lowest);
            Each(e => e.Put(Settings, "global-primary-to-replica-version-ack"u8.ToArray(), ack));
            _trackerAck = lowest;
            if (_trackerAck > _trackerCompact)
            {
                byte[] from = Le8(0);
                byte[] to = Le8(_trackerAck);
                Each(e => e.Compact(ChangeTracker, from, to));
                _trackerCompact = _trackerAck + 50;
            }
        }

        private void Maintenance()
        {
            int cf = _r.Next(s_families.Length);
            switch (_r.Next(4))
            {
                case 0:
                case 1:
                    Each(e => e.Flush(cf));
                    break;
                case 2:
                    Each(e => e.Compact(cf, null, null));
                    break;
                default:
                    if (_snapshots.Count < 3)
                    {
                        int id = -1;
                        foreach (IEngine e in Engines)
                        {
                            int got = e.TakeSnapshot();
                            Assert.IsTrue(id < 0 || id == got);
                            id = got;
                        }
                        _snapshots.Add(id);
                    }
                    else
                    {
                        int i = _r.Next(_snapshots.Count);
                        int id = _snapshots[i];
                        _snapshots.RemoveAt(i);
                        Each(e => e.ReleaseSnapshot(id));
                    }
                    break;
            }
        }

        public void Step()
        {
            int op = _r.Next(100);
            if (op < 25) NodeWrite();
            else if (op < 30) NodeDelete();
            else if (op < 40) BlobStore();
            else if (op < 43) BlobDelete();
            else if (op < 53) Queue();
            else if (op < 65) FileQueueOp();
            else if (op < 66) ClearFileQueue();
            else if (op < 67) ClearDeprecatedChangeLog();
            else if (op < 75) Vectors();
            else if (op < 87) TrackChange();
            else if (op < 92) AckChanges();
            else Maintenance();
        }
    }

    // ---- comparisons ------------------------------------------------------------------------------------------------

    private static List<List<KeyValuePair<byte[], byte[]>>> DumpAll(IEngine e, int snapshot = -1)
    {
        var all = new List<List<KeyValuePair<byte[], byte[]>>>();
        for (int cf = 0; cf < s_families.Length; cf++) all.Add(e.Dump(cf, snapshot));
        return all;
    }

    /// <summary>Same entries in every family, and each one found by a point lookup (memtable / hash index / blooms).</summary>
    private static void AssertContents(List<List<KeyValuePair<byte[], byte[]>>> expected, IEngine actual, string what, int snapshot = -1,
        int skipFamily = -1)
    {
        for (int cf = 0; cf < s_families.Length; cf++)
        {
            if (cf == skipFamily) continue;
            List<KeyValuePair<byte[], byte[]>> a = expected[cf];
            List<KeyValuePair<byte[], byte[]>> b = actual.Dump(cf, snapshot);
            string at = what + " / " + s_families[cf].Name + (snapshot < 0 ? "" : " @snapshot " + snapshot);
            Assert.AreEqual(a.Count, b.Count, at + ": entry count");
            for (int i = 0; i < a.Count; i++)
            {
                CollectionAssert.AreEqual(a[i].Key, b[i].Key, at + ": key #" + i);
                CollectionAssert.AreEqual(a[i].Value, b[i].Value, at + ": value #" + i);
                CollectionAssert.AreEqual(a[i].Value, actual.Get(cf, a[i].Key, snapshot), at + ": Get #" + i);
            }
        }
    }

    private static int TotalEntries(List<List<KeyValuePair<byte[], byte[]>>> all)
    {
        int n = 0;
        foreach (List<KeyValuePair<byte[], byte[]>> l in all) n += l.Count;
        return n;
    }

    /// <summary>Owns the per-engine shared cache and write buffer manager (one each, as Mosaik shares them process-wide).</summary>
    private sealed class Env : IDisposable
    {
        public readonly TempDir Root = new("mosaik-wp");
        public readonly BlockCache ManagedCache = BlockCache.CreateLru(8L << 20);
        public readonly RocksDbSharp.Cache NativeCache = RocksDbSharp.Cache.CreateLru(8UL << 20);
        // A small shared buffer so the manager, not the per-family write_buffer_size, triggers flushes (allow_stall=false)
        public readonly WriteBufferManager ManagedWbm = new(512 * 1024, null, allowStall: false);
        public readonly IntPtr NativeWbm = RocksDbSharp.Native.Instance.rocksdb_write_buffer_manager_create(512 * 1024, RocksDbSharp.Native.MarshalBool(false));

        public string Dir(string name) => Path.Combine(Root.Path, name);
        public string Wal(string name) => Path.Combine(Root.Path, name, "journal");
        public IEngine Managed(string name) => new ManagedEngine(Dir(name), Wal(name), ManagedWbm, ManagedCache);
        public IEngine Native(string name) => new NativeEngine(Dir(name), Wal(name), NativeWbm, NativeCache);

        public void Dispose()
        {
            RocksDbSharp.Native.Instance.rocksdb_write_buffer_manager_destroy(NativeWbm);
            Root.Dispose();
        }
    }

    /// <summary>
    /// Mosaik's write patterns with its options on both engines, reopening (WAL recovery) between rounds: identical
    /// contents in every family and at every live snapshot, identical latest sequence numbers; a replica of the other
    /// engine fed the primary's WAL batches ends identical at the same sequence number; each engine reopens the other's
    /// directory (SSTs, MANIFEST, OPTIONS, WAL in the separate journal dir) and reads the same data.
    /// </summary>
    [TestMethod]
    [DataRow(1)]
    [DataRow(2)]
    [DataRow(3)]
    public void MosaikWorkloadMatchesNativeAndCrossOpens(int seedBase)
    {
        int seed = int.TryParse(Environment.GetEnvironmentVariable("MOSAIK_WP_SEED"), out int fixedSeed)
            ? fixedSeed
            : seedBase * 7919 + (Environment.TickCount & 0x3FF);
        Console.WriteLine("seed=" + seed + " (replay with MOSAIK_WP_SEED)");
        using var env = new Env();
        IEngine managed = env.Managed("managed");
        IEngine native = env.Native("native");
        var w = new Workload(seed, [native, managed]);
        List<List<KeyValuePair<byte[], byte[]>>> expected;
        try
        {
            for (int round = 0; round < 3; round++)
            {
                for (int i = 0; i < 700; i++) w.Step();
                Assert.AreEqual(native.LatestSequence, managed.LatestSequence, "latest sequence, round " + round);
                expected = DumpAll(native);
                AssertContents(expected, managed, "round " + round);
                foreach (int s in w.Snapshots)
                {
                    AssertContents(DumpAll(native, s), managed, "round " + round, s, w.ChangeLogFilesDroppedUnder(s) ? ChangeLog : -1);
                }

                // Reopen both: the memtables come back from the WAL (default WriteOptions: WAL on, no sync)
                managed.Dispose();
                native.Dispose();
                w.ForgetSnapshots();
                managed = env.Managed("managed");
                native = env.Native("native");
                w.Engines = [native, managed];
                Assert.AreEqual(native.LatestSequence, managed.LatestSequence, "latest sequence after reopen, round " + round);
                AssertContents(expected, managed, "reopen " + round);
            }
            for (int i = 0; i < 300; i++) w.Step(); // leave some of it in the memtables / WAL only
            expected = DumpAll(native);
            Assert.IsTrue(TotalEntries(expected) > 500, "the workload leaves data behind");
            AssertContents(expected, managed, "final");

            // Replicas: each engine replays the other's WAL batches from sequence 1 (archived WALs included)
            ulong last = native.LatestSequence;
            using (IEngine replicaOfManaged = env.Native("replica-of-managed"))
            {
                foreach (byte[] b in managed.BatchesSince(1)) replicaOfManaged.WriteRaw(b);
                Assert.AreEqual(last, replicaOfManaged.LatestSequence, "native replica of the managed primary");
                AssertContents(expected, replicaOfManaged, "native replica of managed");
            }
            using (IEngine replicaOfNative = env.Managed("replica-of-native"))
            {
                foreach (byte[] b in native.BatchesSince(1)) replicaOfNative.WriteRaw(b);
                Assert.AreEqual(last, replicaOfNative.LatestSequence, "managed replica of the native primary");
                AssertContents(expected, replicaOfNative, "managed replica of native");
            }
        }
        finally
        {
            managed.Dispose();
            native.Dispose();
        }

        // Each engine opens the other's files
        using (IEngine crossManaged = env.Managed("native"))
        {
            AssertContents(expected, crossManaged, "managed opening native's DB");
            crossManaged.Flush(Nodes);
        }
        using (IEngine crossNative = env.Native("managed"))
        {
            AssertContents(expected, crossNative, "native opening managed's DB");
        }
        using (IEngine backNative = env.Native("native"))
        {
            AssertContents(expected, backNative, "native reopening its DB after the managed port wrote to it");
        }
    }

    // ---- DeleteRange / Merge edge cases (write_batch.cc MemTableInserter::DeleteRangeCF / MergeCF, DBImpl::Merge) ----

    private static RocksDbSharp.RocksDb OpenPlainNative(string path) =>
        RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions().SetCreateIfMissing(true), path, new RocksDbSharp.ColumnFamilies());

    private static void NativeDeleteRange(RocksDbSharp.RocksDb db, string begin, string end)
    {
        using var b = new RocksDbSharp.WriteBatch();
        b.DeleteRange(System.Text.Encoding.UTF8.GetBytes(begin), (ulong)begin.Length, System.Text.Encoding.UTF8.GetBytes(end), (ulong)end.Length);
        db.Write(b);
    }

    private static string NativeError(Action a)
    {
        try
        {
            a();
        }
        catch (RocksDbSharp.RocksDbException e)
        {
            return e.Message;
        }
        Assert.Fail("native call did not fail");
        return "";
    }

    /// <summary>
    /// DeleteRange(k, k) covers nothing: upstream does not insert it (no memtable entry, no range tombstone in the
    /// flushed file) and the entries after it in the batch take its sequence number; the batch still consumes its count.
    /// </summary>
    [TestMethod]
    public void EmptyDeleteRangeIsNotApplied()
    {
        using var nd = new TempDir("edr-n");
        using var md = new TempDir("edr-m");
        using RocksDbSharp.RocksDb n = OpenPlainNative(nd.Path);
        using RocksDatabase m = RocksDatabase.Open(md.Path, new DbOptions { CreateIfMissing = true });
        n.Put("b"u8.ToArray(), "1"u8.ToArray());
        m.Put("b"u8, "1"u8);

        NativeDeleteRange(n, "b", "b");
        m.DeleteRange("b"u8, "b"u8);
        using (var nb = new RocksDbSharp.WriteBatch())
        {
            nb.DeleteRange("c"u8.ToArray(), 1, "c"u8.ToArray(), 1).Put("c"u8.ToArray(), "2"u8.ToArray());
            n.Write(nb);
        }
        using (WriteBatch mb = m.CreateWriteBatch())
        {
            mb.DeleteRange("c"u8, "c"u8).Put("c"u8, "2"u8);
            m.Write(mb);
        }

        Assert.AreEqual(n.GetLatestSequenceNumber(), m.GetLatestSequenceNumber());
        Assert.AreEqual(4UL, m.GetLatestSequenceNumber());
        Assert.AreEqual("2", n.GetProperty("rocksdb.num-entries-active-mem-table"));
        Assert.AreEqual("2", m.GetProperty("rocksdb.num-entries-active-mem-table"));
        CollectionAssert.AreEqual(n.Get("b"u8.ToArray()), m.Get("b"u8));
        CollectionAssert.AreEqual(n.Get("c"u8.ToArray()), m.Get("c"u8));
        // The Put after the empty range took its sequence number (3), as MaybeAdvanceSeq is skipped upstream
        using (KeyVersionCollection versions = m.GetAllKeyVersions("c"u8.ToArray(), "c"u8.ToArray(), 10))
        {
            Assert.AreEqual(1, versions.Count);
            Assert.AreEqual(3UL, versions[0].Sequence);
        }
        m.Flush();
        Assert.AreEqual("0", m.GetProperty("rocksdb.num-entries-active-mem-table"));
        CollectionAssert.AreEqual("1"u8.ToArray(), m.Get("b"u8));
        CollectionAssert.AreEqual("2"u8.ToArray(), m.Get("c"u8));
    }

    /// <summary>
    /// A DeleteRange whose end comes before its start: upstream fails the write with InvalidArgument from the memtable
    /// insert, after the WAL write, so nothing of the batch becomes visible and the DB stops accepting writes (background
    /// error, reason kMemTable). Reopening (point-in-time recovery) drops the batch and the DB is writable again.
    /// </summary>
    [TestMethod]
    public void InvertedDeleteRangeFailsTheWriteLikeNative()
    {
        using var nd = new TempDir("idr-n");
        using var md = new TempDir("idr-m");
        using (RocksDbSharp.RocksDb n = OpenPlainNative(nd.Path))
        {
            n.Put("a"u8.ToArray(), "1"u8.ToArray());
            string err = NativeError(() =>
            {
                using var b = new RocksDbSharp.WriteBatch();
                b.Put("x"u8.ToArray(), "1"u8.ToArray()).DeleteRange("c"u8.ToArray(), 1, "a"u8.ToArray(), 1).Put("y"u8.ToArray(), "1"u8.ToArray());
                n.Write(b);
            });
            StringAssert.StartsWith(err, "Invalid argument: end key comes before start key");
            Assert.AreEqual(1UL, n.GetLatestSequenceNumber());
            Assert.IsNull(n.Get("x"u8.ToArray()));
            StringAssert.StartsWith(NativeError(() => n.Put("z"u8.ToArray(), "1"u8.ToArray())), "Invalid argument: end key comes before start key");
        }
        using (RocksDatabase m = RocksDatabase.Open(md.Path, new DbOptions { CreateIfMissing = true }))
        {
            m.Put("a"u8, "1"u8);
            var ex = Assert.ThrowsExactly<RocksDbException>(() =>
            {
                using WriteBatch b = m.CreateWriteBatch();
                b.Put("x"u8, "1"u8).DeleteRange("c"u8, "a"u8).Put("y"u8, "1"u8);
                m.Write(b);
            });
            Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
            StringAssert.StartsWith(ex.Message, "Invalid argument: end key comes before start key");
            Assert.AreEqual(1UL, m.GetLatestSequenceNumber());
            Assert.IsNull(m.Get("x"u8));
            var stopped = Assert.ThrowsExactly<RocksDbException>(() => m.Put("z"u8, "1"u8));
            Assert.AreEqual(StatusCode.InvalidArgument, stopped.Code);
        }
        using (RocksDbSharp.RocksDb n = OpenPlainNative(nd.Path))
        {
            Assert.AreEqual(1UL, n.GetLatestSequenceNumber());
            Assert.IsNull(n.Get("x"u8.ToArray()));
            n.Put("z"u8.ToArray(), "1"u8.ToArray());
        }
        using (RocksDatabase m = RocksDatabase.Open(md.Path, new DbOptions { CreateIfMissing = true }))
        {
            Assert.AreEqual(1UL, m.GetLatestSequenceNumber());
            Assert.IsNull(m.Get("x"u8));
            Assert.IsNull(m.Get("y"u8));
            CollectionAssert.AreEqual("1"u8.ToArray(), m.Get("a"u8));
            m.Put("z"u8, "1"u8);
        }
        // The managed port reads native's WAL holding the refused batch the same way
        using (RocksDatabase m = RocksDatabase.Open(nd.Path, new DbOptions()))
        {
            Assert.IsNull(m.Get("x"u8));
            CollectionAssert.AreEqual("1"u8.ToArray(), m.Get("z"u8));
        }
    }

    /// <summary>
    /// Merge into a family without a merge operator: DB::Merge is NotSupported before anything is written; a Merge in a
    /// WriteBatch fails the write with InvalidArgument (memtable insert). Mosaik configures no merge operator.
    /// </summary>
    [TestMethod]
    public void MergeWithoutMergeOperatorFailsLikeNative()
    {
        using var nd = new TempDir("mrg-n");
        using var md = new TempDir("mrg-m");
        using (RocksDbSharp.RocksDb n = OpenPlainNative(nd.Path))
        {
            StringAssert.StartsWith(NativeError(() => n.Merge("k"u8.ToArray(), "1"u8.ToArray())), "Not implemented: Provide a merge_operator when opening DB");
            Assert.AreEqual(0UL, n.GetLatestSequenceNumber());
            n.Put("k"u8.ToArray(), "1"u8.ToArray());
            string err = NativeError(() =>
            {
                using var b = new RocksDbSharp.WriteBatch();
                b.Merge("k"u8.ToArray(), 1, "2"u8.ToArray(), 1);
                n.Write(b);
            });
            StringAssert.StartsWith(err, "Invalid argument: Merge requires");
            Assert.AreEqual(1UL, n.GetLatestSequenceNumber());
        }
        using (RocksDatabase m = RocksDatabase.Open(md.Path, new DbOptions { CreateIfMissing = true }))
        {
            var ex = Assert.ThrowsExactly<RocksDbException>(() => m.Merge("k"u8, "1"u8));
            Assert.AreEqual(StatusCode.NotSupported, ex.Code);
            StringAssert.StartsWith(ex.Message, "Not implemented: Provide a merge_operator when opening DB");
            var exAsync = Assert.ThrowsExactly<RocksDbException>(() => m.MergeAsync("k"u8.ToArray(), "1"u8.ToArray()).AsTask().GetAwaiter().GetResult());
            Assert.AreEqual(StatusCode.NotSupported, exAsync.Code);
            Assert.AreEqual(0UL, m.GetLatestSequenceNumber());
            m.Put("k"u8, "1"u8);
            var exBatch = Assert.ThrowsExactly<RocksDbException>(() =>
            {
                using WriteBatch b = m.CreateWriteBatch();
                b.Merge("k"u8, "2"u8);
                m.Write(b);
            });
            Assert.AreEqual(StatusCode.InvalidArgument, exBatch.Code);
            StringAssert.StartsWith(exBatch.Message, "Invalid argument: Merge requires `ColumnFamilyOptions::merge_operator != nullptr`");
            Assert.AreEqual(1UL, m.GetLatestSequenceNumber());
        }
        using (RocksDatabase m = RocksDatabase.Open(md.Path, new DbOptions()))
        {
            CollectionAssert.AreEqual("1"u8.ToArray(), m.Get("k"u8));
        }
    }
}
