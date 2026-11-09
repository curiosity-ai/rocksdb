using System.Buffers.Binary;
using System.Globalization;
using System.Text;
using RocksDb.Managed.Cache;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Table.Filters;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>
/// The read path as Mosaik (the main downstream user, through rocksdb-sharp) drives it, replayed on native RocksDB and
/// on the managed port with Mosaik's column family options, and compared:
/// <list type="bullet">
/// <item>table options of every family (RocksDbBase.CreateTableOptions): shared LRU cache, index and filter blocks in
/// the cache with high priority, L0 ones pinned, two-level (partitioned) index, partitioned filters, top level
/// pinned;</item>
/// <item>fixed prefix extractors of 8 and 10 bytes with a 10 bits/key Bloom filter (whole key filtering on) and
/// optimize_filters_for_hits (simple-text-index-*, to-index-queue, command-score-index-entries,
/// file-extraction-queue-stats), and without a filter (data-vectors*);</item>
/// <item>point lookup families (CreatePointLookupColumnFamilyOptions): Bloom filter, data block hash index, a whole
/// key memtable Bloom filter (memtable_prefix_bloom_size_ratio 0.02 + memtable_whole_key_filtering, no prefix
/// extractor);</item>
/// <item>the replica change tracker family (SafeRocksDb.ConfigureForChangeTracker): a comparator ordering by the
/// little-endian ulong in the first 8 bytes, a 16 byte fixed prefix extractor (every 8 byte key is out of its
/// domain), Bloom with whole_key_filtering=false and optimize_filters_for_hits;</item>
/// <item>max_open_files 8 (Mosaik's low memory mode), so table readers are evicted while iterators hold them.</item>
/// </list>
/// Each read pattern Mosaik uses is replayed at several points (memtable only, L0 files, after a full compaction,
/// with newer writes on top): Get / HasKey / GetPooled, a prefix scan between iterate bounds (SeekToFirst + Next),
/// the same bounds in reverse (SeekToLast + Prev), prefix_same_as_start Seek + Next, full forward and reverse scans
/// without bounds, a lower-bound-only scan on the change tracker family, and deleting every key of a range while
/// iterating it. Finally each engine opens the other's directory and the reads are compared again.
/// MOSAIK_READ_SEED=n replays a run.
/// </summary>
[TestClass]
public sealed class MosaikReadPathCompatTests
{
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    public enum Family
    {
        /// <summary>simple-text-index-keys/entries, to-index-queue, command-score-index-entries.</summary>
        Prefix8Bloom,
        /// <summary>file-extraction-queue-stats (prefix of UID64 + ushort).</summary>
        Prefix10Bloom,
        /// <summary>data-vectors, data-vectors-hashes: prefix extractor, no filter.</summary>
        Prefix8NoFilter,
        /// <summary>content, nodeversion, docmetadata, data-*.</summary>
        PointLookup,
        /// <summary>The replica change tracker family with the ulong comparator.</summary>
        ULongChangeTracker,
    }

    private const int UidKeyLength = 8 + 4 + 16; // simple-text-index-entries: UID64 + uint + UID128

    // ---- Mosaik's comparator ---------------------------------------------------------------------------------

    /// <summary>Mosaik.GraphDB.ulong_Comparator: orders by the little-endian ulong in the first 8 bytes only.</summary>
    private sealed class ManagedULongComparator : Comparator
    {
        public static readonly ManagedULongComparator Instance = new();
        public override string Name => "Mosaik.GraphDB.ulong_Comparator";
        public override int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) =>
            BinaryPrimitives.ReadUInt64LittleEndian(a).CompareTo(BinaryPrimitives.ReadUInt64LittleEndian(b));
    }

    private sealed unsafe class NativeULongComparator : RocksDbSharp.Comparator
    {
        public string Name => "Mosaik.GraphDB.ulong_Comparator";
        public int Compare(IntPtr a, UIntPtr alen, IntPtr b, UIntPtr blen) =>
            BinaryPrimitives.ReadUInt64LittleEndian(new ReadOnlySpan<byte>((void*)a, (int)alen))
                .CompareTo(BinaryPrimitives.ReadUInt64LittleEndian(new ReadOnlySpan<byte>((void*)b, (int)blen)));
    }

    // ---- options, as Mosaik sets them ------------------------------------------------------------------------

    private static RocksDbSharp.DbOptions NativeOptions(Family family)
    {
        RocksDbSharp.Native n = RocksDbSharp.Native.Instance;
        RocksDbSharp.Cache cache = RocksDbSharp.Cache.CreateLru(1UL << 20);
        var t = new RocksDbSharp.BlockBasedTableOptions().SetBlockCache(cache);
        n.rocksdb_block_based_options_set_cache_index_and_filter_blocks(t.Handle, RocksDbSharp.Native.MarshalBool(true));
        n.rocksdb_block_based_options_set_cache_index_and_filter_blocks_with_high_priority(t.Handle, RocksDbSharp.Native.MarshalBool(true));
        n.rocksdb_block_based_options_set_pin_l0_filter_and_index_blocks_in_cache(t.Handle, RocksDbSharp.Native.MarshalBool(true));
        n.rocksdb_block_based_options_set_index_type(t.Handle, RocksDbSharp.BlockBasedTableIndexType.TwoLevelIndex);
        n.rocksdb_block_based_options_set_partition_filters(t.Handle, RocksDbSharp.Native.MarshalBool(true));
        n.rocksdb_block_based_options_set_pin_top_level_index_and_filter(t.Handle, RocksDbSharp.Native.MarshalBool(true));
        if (family != Family.Prefix8NoFilter) t.SetFilterPolicy(RocksDbSharp.BloomFilterPolicy.Create(10, false));
        if (family == Family.ULongChangeTracker) t.SetWholeKeyFiltering(false);
        if (family == Family.PointLookup)
        {
            n.rocksdb_block_based_options_set_data_block_index_type(t.Handle, (int)RocksDbSharp.BlockBasedTableDataBlockIndexType.BinarySearchAndHash);
            n.rocksdb_block_based_options_set_data_block_hash_table_util_ratio(t.Handle, 0.75);
        }
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetMaxOpenFiles(8).SetDisableAutoCompactions(1);
        o.SetBlockBasedTableFactory(t);
        switch (family)
        {
            case Family.Prefix8Bloom:
                o.SetPrefixExtractor(RocksDbSharp.SliceTransform.CreateFixedPrefix(8)).SetOptimizeFiltersForHits(1);
                break;
            case Family.Prefix10Bloom:
                o.SetPrefixExtractor(RocksDbSharp.SliceTransform.CreateFixedPrefix(10)).SetOptimizeFiltersForHits(1);
                break;
            case Family.Prefix8NoFilter:
                o.SetPrefixExtractor(RocksDbSharp.SliceTransform.CreateFixedPrefix(8));
                break;
            case Family.PointLookup:
                o.SetMemtablePrefixBloomSizeRatio(0.02);
                n.rocksdb_options_set_memtable_whole_key_filtering(o.Handle, RocksDbSharp.Native.MarshalBool(true));
                break;
            case Family.ULongChangeTracker:
                var cmp = new NativeULongComparator();
                s_keepAlive.Add(cmp);
                o.SetPrefixExtractor(RocksDbSharp.SliceTransform.CreateFixedPrefix(16)).SetOptimizeFiltersForHits(1).SetComparator(cmp);
                break;
        }
        s_keepAlive.Add(cache);
        s_keepAlive.Add(t);
        s_keepAlive.Add(o);
        return o;
    }

    private static ColumnFamilyOptions ManagedOptions(Family family)
    {
        var cfo = new ColumnFamilyOptions();
        BlockBasedTableOptions t = cfo.TableOptions;
        t.BlockCache = new LruCache(new LruCacheOptions { Capacity = 1L << 20 });
        t.CacheIndexAndFilterBlocks = true;
        t.CacheIndexAndFilterBlocksWithHighPriority = true;
        t.PinL0FilterAndIndexBlocksInCache = true;
        t.IndexType = IndexType.TwoLevelIndexSearch;
        t.PartitionFilters = true;
        t.PinTopLevelIndexAndFilter = true;
        if (family != Family.Prefix8NoFilter) t.FilterPolicy = new BloomFilterPolicy(10);
        if (family == Family.ULongChangeTracker) t.WholeKeyFiltering = false;
        if (family == Family.PointLookup)
        {
            t.DataBlockIndexType = DataBlockIndexType.BinaryAndHash;
            t.DataBlockHashTableUtilRatio = 0.75;
        }
        cfo.DisableAutoCompactions = true;
        switch (family)
        {
            case Family.Prefix8Bloom:
                cfo.PrefixExtractor = SliceTransform.CreateFixedPrefix(8);
                cfo.OptimizeFiltersForHits = true;
                break;
            case Family.Prefix10Bloom:
                cfo.PrefixExtractor = SliceTransform.CreateFixedPrefix(10);
                cfo.OptimizeFiltersForHits = true;
                break;
            case Family.Prefix8NoFilter:
                cfo.PrefixExtractor = SliceTransform.CreateFixedPrefix(8);
                break;
            case Family.PointLookup:
                cfo.MemtablePrefixBloomSizeRatio = 0.02;
                cfo.MemtableWholeKeyFiltering = true;
                break;
            case Family.ULongChangeTracker:
                cfo.PrefixExtractor = SliceTransform.CreateFixedPrefix(16);
                cfo.OptimizeFiltersForHits = true;
                cfo.Comparator = ManagedULongComparator.Instance;
                break;
        }
        return cfo;
    }

    private static DbOptions ManagedDbOptions() => new() { CreateIfMissing = true, MaxOpenFiles = 8 };

    // ---- engine abstraction ----------------------------------------------------------------------------------

    /// <summary>What a read pattern sets on its ReadOptions.</summary>
    private sealed record ReadSpec(byte[]? Lower, byte[]? Upper, bool PrefixSameAsStart = false, bool TotalOrderSeek = false, bool NoFillCache = false, ulong Readahead = 0);

    private interface IIter : IDisposable
    {
        bool Valid { get; }
        byte[] Key { get; }
        byte[] Value { get; }
        void SeekToFirst();
        void SeekToLast();
        void Seek(byte[] k);
        void Next();
        void Prev();
    }

    private interface IEngine : IDisposable
    {
        void Put(byte[] k, byte[] v);
        void Delete(byte[] k);
        void SingleDelete(byte[] k);
        void DeleteRange(byte[] begin, byte[] end);
        void Flush();
        void CompactAll();
        byte[]? Get(byte[] k);
        bool HasKey(byte[] k);
        byte[]? GetPooled(byte[] k);
        IIter NewIterator(ReadSpec spec);
    }

    private sealed class NativeEngine : IEngine
    {
        private static readonly RocksDbSharp.ReadOptions s_defaultRead = new();
        private readonly RocksDbSharp.RocksDb _db;

        public NativeEngine(string path, Family family) => _db = RocksDbSharp.RocksDb.Open(NativeOptions(family), path);

        public void Put(byte[] k, byte[] v) => _db.Put(k, v);
        public void Delete(byte[] k) => _db.Remove(k);

        public unsafe void SingleDelete(byte[] k)
        {
            using var batch = new RocksDbSharp.WriteBatch();
            fixed (byte* p = k) RocksDbSharp.Native.Instance.rocksdb_writebatch_singledelete(batch.Handle, p, (UIntPtr)k.Length);
            _db.Write(batch);
        }

        public void DeleteRange(byte[] begin, byte[] end)
        {
            using var batch = new RocksDbSharp.WriteBatch();
            batch.DeleteRange(begin, (ulong)begin.Length, end, (ulong)end.Length);
            _db.Write(batch);
        }

        public void Flush() => _db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));

        // Null begin/end (an empty begin key would reach the ulong comparator, which needs 8 bytes).
        public unsafe void CompactAll() => RocksDbSharp.Native.Instance.rocksdb_compact_range(_db.Handle, (byte*)null, UIntPtr.Zero, (byte*)null, UIntPtr.Zero);

        public byte[]? Get(byte[] k) => _db.Get(k);
        public bool HasKey(byte[] k) => _db.HasKey(k);

        public byte[]? GetPooled(byte[] k)
        {
            // RocksDBExt.GetPooled: rocksdb_get + copy + rocksdb_free.
            IntPtr p = RocksDbSharp.Native.Instance.rocksdb_get(_db.Handle, s_defaultRead.Handle, k, (UIntPtr)k.Length, out UIntPtr len, out IntPtr err);
            Assert.AreEqual(IntPtr.Zero, err);
            if (p == IntPtr.Zero) return null;
            byte[] r = new byte[(int)len];
            System.Runtime.InteropServices.Marshal.Copy(p, r, 0, r.Length);
            RocksDbSharp.Native.Instance.rocksdb_free(p);
            return r;
        }

        public IIter NewIterator(ReadSpec spec)
        {
            var ro = new RocksDbSharp.ReadOptions();
            if (spec.Lower is not null) ro.SetIterateLowerBound(spec.Lower);
            if (spec.Upper is not null) ro.SetIterateUpperBound(spec.Upper);
            if (spec.PrefixSameAsStart) ro.SetPrefixSameAsStart(true);
            if (spec.TotalOrderSeek) ro.SetTotalOrderSeek(true);
            if (spec.NoFillCache) ro.SetFillCache(false);
            if (spec.Readahead != 0) ro.SetReadaheadSize(spec.Readahead);
            return new Iter(_db.NewIterator(null, ro), ro);
        }

        public void Dispose() => _db.Dispose();

        private sealed class Iter(RocksDbSharp.Iterator it, RocksDbSharp.ReadOptions ro) : IIter
        {
            public bool Valid => it.Valid();
            public byte[] Key => it.GetKeySpan().ToArray();
            public byte[] Value => it.GetValueSpan().ToArray();
            public void SeekToFirst() => it.SeekToFirst();
            public void SeekToLast() => it.SeekToLast();
            public void Seek(byte[] k) => it.Seek(k);
            public void Next() => it.Next();
            public void Prev() => it.Prev();

            public void Dispose()
            {
                it.Dispose();
                GC.KeepAlive(ro);
            }
        }
    }

    private sealed class ManagedEngine(string path, Family family) : IEngine
    {
        private readonly RocksDatabase _db = RocksDatabase.Open(path, ManagedDbOptions(), ManagedOptions(family));

        public void Put(byte[] k, byte[] v) => _db.Put(k, v);
        public void Delete(byte[] k) => _db.Delete(k);
        public void SingleDelete(byte[] k) => _db.SingleDelete(k);
        public void DeleteRange(byte[] begin, byte[] end) => _db.DeleteRange(begin, end);
        public void Flush() => _db.Flush();
        public void CompactAll() => _db.CompactRange(null, null);
        public byte[]? Get(byte[] k) => _db.Get(k);
        public bool HasKey(byte[] k) => _db.DefaultColumnFamily.ContainsKey(k);

        public byte[]? GetPooled(byte[] k)
        {
            using PooledValue? v = _db.GetPooled(k);
            return v?.Memory.ToArray();
        }

        public IIter NewIterator(ReadSpec spec)
        {
            var ro = new ReadOptions
            {
                // Not "= spec.Lower" (nor "spec.Lower is null ? null : ..."): a null byte[] converts to an empty,
                // non-null ReadOnlyMemory bound.
                IterateLowerBound = spec.Lower is null ? (ReadOnlyMemory<byte>?)null : spec.Lower,
                IterateUpperBound = spec.Upper is null ? (ReadOnlyMemory<byte>?)null : spec.Upper,
                PrefixSameAsStart = spec.PrefixSameAsStart,
                TotalOrderSeek = spec.TotalOrderSeek,
                FillCache = !spec.NoFillCache,
                ReadaheadSize = spec.Readahead,
            };
            return new Iter(_db.NewIterator(ro));
        }

        public void Dispose() => _db.Dispose();

        private sealed class Iter(Iterator it) : IIter
        {
            public bool Valid => it.Valid;
            public byte[] Key => it.Key.ToArray();
            public byte[] Value => it.Value.ToArray();
            public void SeekToFirst() => it.SeekToFirst();
            public void SeekToLast() => it.SeekToLast();
            public void Seek(byte[] k) => it.Seek(k);
            public void Next() => it.Next();
            public void Prev() => it.Prev();
            public void Dispose() => it.Dispose();
        }
    }

    // ---- workload ----------------------------------------------------------------------------------------------

    private static byte[] U64(ulong v)
    {
        byte[] b = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(b, v);
        return b;
    }

    private static byte[] Concat(byte[] a, ReadOnlySpan<byte> b)
    {
        byte[] r = new byte[a.Length + b.Length];
        a.CopyTo(r, 0);
        b.CopyTo(r.AsSpan(a.Length));
        return r;
    }

    private static byte[] Filled(byte[] prefix, int length, byte fill)
    {
        byte[] r = new byte[Math.Max(length, prefix.Length)];
        r.AsSpan().Fill(fill);
        prefix.CopyTo(r, 0);
        return r;
    }

    /// <summary>The deterministic mutations, read probes and Mosaik read patterns of one family.</summary>
    private sealed class Workload
    {
        private readonly Family _family;
        private readonly Random _rnd;
        public readonly List<byte[]> Prefixes = new();
        public readonly List<byte[]> AbsentPrefixes = new();
        public readonly List<byte[]> Written = new();
        /// <summary>Keys written once and possibly single-deleted (never Delete'd: SD and DEL must not mix).</summary>
        public readonly List<byte[]> SingleDeleteKeys = new();
        public int RangeDeletions;
        private int _singleDeleteNext;
        private readonly List<byte[]> _singleDeleteCandidates = new();
        private ulong _version = 1;

        public Workload(Family family, int seed)
        {
            _family = family;
            _rnd = new Random(seed);
            int prefixLength = family == Family.Prefix10Bloom ? 10 : 8;
            for (int i = 0; i < 24; i++)
            {
                byte[] p = new byte[prefixLength];
                _rnd.NextBytes(p);
                (i < 18 ? Prefixes : AbsentPrefixes).Add(p);
            }
        }

        private byte[] RandomKey(byte[] prefix)
        {
            int suffix = _family switch
            {
                Family.Prefix10Bloom => 1 + _rnd.Next(3),
                Family.PointLookup => 8,
                _ => UidKeyLength - prefix.Length,
            };
            // A few keys exactly as long as the prefix, and (prefix families) a few shorter than it: out of the
            // prefix extractor's domain.
            int r = _rnd.Next(100);
            if (r < 3) return (byte[])prefix.Clone();
            if (r < 5 && _family != Family.PointLookup) return prefix.AsSpan(0, 1 + _rnd.Next(prefix.Length - 1)).ToArray();
            byte[] s = new byte[suffix];
            _rnd.NextBytes(s);
            return Concat(prefix, s);
        }

        private byte[] Value() => Encoding.ASCII.GetBytes("v" + _rnd.Next().ToString(CultureInfo.InvariantCulture));

        /// <summary>One round of writes, concentrated on a subset of the prefixes so files differ in their prefixes.</summary>
        public void Round(IEngine[] engines, int ops, bool rangeAndSingleDeletes)
        {
            if (_family == Family.ULongChangeTracker)
            {
                // ReplicaChangeHandler: Put(version as 8 bytes LE, change); the primary clears the family on open.
                for (int i = 0; i < ops; i++)
                {
                    if (_rnd.Next(100) < 80 || Written.Count == 0)
                    {
                        _version += (ulong)(1 + _rnd.Next(3)) * (_rnd.Next(10) == 0 ? 257UL : 1UL);
                        byte[] k = U64(_version);
                        Written.Add(k);
                        byte[] v = Value();
                        foreach (IEngine e in engines) e.Put(k, v);
                    }
                    else
                    {
                        byte[] k = Written[_rnd.Next(Written.Count)];
                        foreach (IEngine e in engines) e.Delete(k);
                    }
                }
                return;
            }
            var active = new List<byte[]>();
            foreach (byte[] p in Prefixes) if (_rnd.Next(3) == 0) active.Add(p);
            if (active.Count == 0) active.Add(Prefixes[0]);
            for (int i = 0; i < ops; i++)
            {
                byte[] p = active[_rnd.Next(active.Count)];
                int r = _rnd.Next(100);
                if (r < 70 || Written.Count == 0)
                {
                    byte[] k = RandomKey(p), v = Value();
                    Written.Add(k);
                    foreach (IEngine e in engines) e.Put(k, v);
                }
                else if (r < 90)
                {
                    byte[] k = Written[_rnd.Next(Written.Count)];
                    foreach (IEngine e in engines) e.Delete(k);
                }
                else if (rangeAndSingleDeletes && r < 95)
                {
                    // SingleDelete is only defined on a key written once: a dedicated key space under the prefix.
                    if (_singleDeleteCandidates.Count > 0 && _rnd.Next(2) == 0)
                    {
                        int j = _rnd.Next(_singleDeleteCandidates.Count);
                        byte[] k = _singleDeleteCandidates[j];
                        _singleDeleteCandidates.RemoveAt(j);
                        foreach (IEngine e in engines) e.SingleDelete(k);
                    }
                    else
                    {
                        byte[] k = Concat(p, Encoding.ASCII.GetBytes("sd" + (_singleDeleteNext++).ToString("D6", CultureInfo.InvariantCulture)));
                        _singleDeleteCandidates.Add(k);
                        SingleDeleteKeys.Add(k);
                        byte[] v = Value();
                        foreach (IEngine e in engines) e.Put(k, v);
                    }
                }
                else if (rangeAndSingleDeletes)
                {
                    // A range tombstone inside one prefix (often in a file that holds no point key of the prefix).
                    byte[] a = RandomKey(p), b = RandomKey(p);
                    if (a.AsSpan().SequenceCompareTo(b) > 0) (a, b) = (b, a);
                    if (a.AsSpan().SequenceCompareTo(b) < 0)
                    {
                        RangeDeletions++;
                        foreach (IEngine e in engines) e.DeleteRange(a, b);
                    }
                }
            }
        }

        /// <summary>Keys for point lookups: written keys, absent keys under present and absent prefixes, short keys.</summary>
        public List<byte[]> Probes()
        {
            var r = new List<byte[]>();
            for (int i = 0; i < 1500 && Written.Count > 0; i++) r.Add(Written[_rnd.Next(Written.Count)]);
            for (int i = 0; i < 200 && SingleDeleteKeys.Count > 0; i++) r.Add(SingleDeleteKeys[_rnd.Next(SingleDeleteKeys.Count)]);
            if (_family == Family.ULongChangeTracker)
            {
                for (int i = 0; i < 300; i++) r.Add(U64((ulong)_rnd.NextInt64(0, (long)_version + 10)));
                return r;
            }
            for (int i = 0; i < 300; i++) r.Add(RandomKey(Prefixes[_rnd.Next(Prefixes.Count)]));
            for (int i = 0; i < 300; i++) r.Add(RandomKey(AbsentPrefixes[_rnd.Next(AbsentPrefixes.Count)]));
            return r;
        }
    }

    // ---- read patterns ---------------------------------------------------------------------------------------

    private static string Hex(byte[]? b) => b is null ? "<null>" : Convert.ToHexString(b);

    private static void Collect(IIter it, bool forward, List<string> o, byte[]? requiredPrefix = null)
    {
        int n = 0;
        while (it.Valid)
        {
            byte[] k = it.Key;
            if (requiredPrefix is not null && !k.AsSpan().StartsWith(requiredPrefix)) break;
            o.Add(Hex(k) + "=" + Hex(it.Value));
            n++;
            if (forward) it.Next();
            else it.Prev();
        }
        o.Add("count=" + n.ToString(CultureInfo.InvariantCulture));
    }

    private static void FullScans(IEngine e, List<string> o, string name, ReadSpec spec)
    {
        foreach (bool forward in new[] { true, false })
        {
            o.Add(name + " " + forward);
            using IIter it = e.NewIterator(spec);
            if (forward) it.SeekToFirst();
            else it.SeekToLast();
            Collect(it, forward, o);
        }
    }

    /// <summary>Every Mosaik read pattern over the current state of <paramref name="e"/>, as one list of lines.</summary>
    private static List<string> ReadAll(IEngine e, Workload w, List<byte[]> probes, Family family)
    {
        bool managed = e is ManagedEngine;
        var o = new List<string>();
        foreach (byte[] k in probes)
        {
            byte[]? v = e.Get(k);
            o.Add("get " + Hex(k) + "=" + Hex(v) + " has=" + e.HasKey(k) + " pooled=" + Hex(e.GetPooled(k)));
        }

        // Full scans in total order, then without bounds in the default (prefix) mode: FileQueueStats load
        // (SeekToFirst + Next), file queue (SeekToLast + Prev), schema loads.
        FullScans(e, o, "full-total-order", new ReadSpec(null, null, TotalOrderSeek: true));
        if (w.RangeDeletions == 0 || family is Family.Prefix8NoFilter or Family.PointLookup)
        {
            FullScans(e, o, "full", new ReadSpec(null, null));
        }
        else if (managed)
        {
            // With range tombstones, native's prefix-mode scan across prefixes is undefined and in practice drops keys:
            // MergingIterator::SkipNextDeleted reseeks the children to a tombstone's end key (SeekImpl), and in prefix
            // mode each table iterator consults its prefix filter with that key's prefix, so files without that prefix
            // are skipped. The managed merging iterator does not reseek (DbIter skips covered keys only in total order),
            // so its scan must equal the total order one.
            var total = new List<string>();
            var prefixMode = new List<string>();
            FullScans(e, total, "full", new ReadSpec(null, null, TotalOrderSeek: true));
            FullScans(e, prefixMode, "full", new ReadSpec(null, null));
            AssertSame(total, prefixMode, "managed prefix-mode full scan vs total order");
        }

        if (family == Family.ULongChangeTracker)
        {
            // EnumerateChangesFromPrimary: lower bound = last read version, SeekToFirst, Next.
            for (int i = 0; i < 12; i++)
            {
                byte[] lower = i == 0 ? U64(0) : w.Written[(int)((long)w.Written.Count * i / 12)];
                o.Add("lower " + Hex(lower));
                using IIter it = e.NewIterator(new ReadSpec(lower, null));
                it.SeekToFirst();
                Collect(it, true, o);
            }
            return o;
        }

        var prefixes = new List<byte[]>(w.Prefixes);
        prefixes.AddRange(w.AbsentPrefixes);
        foreach (byte[] p in prefixes)
        {
            // ReadKeyEntries / EnumerateUIDsForKeyEntry / ReadVectors: [prefix + 0x00.., prefix + 0xFF..), SeekToFirst.
            byte[] lower = Filled(p, UidKeyLength, 0), upper = Filled(p, UidKeyLength, 0xFF);
            o.Add("bounds " + Hex(p));
            using (IIter it = e.NewIterator(new ReadSpec(lower, upper, Readahead: 16UL << 20)))
            {
                it.SeekToFirst();
                Collect(it, true, o);
            }
            // The same bounds in reverse.
            o.Add("bounds-rev " + Hex(p));
            using (IIter it = e.NewIterator(new ReadSpec(lower, upper, NoFillCache: true)))
            {
                it.SeekToLast();
                Collect(it, false, o);
            }
            // A lower bound equal to the bare prefix (data-vectors-hashes clear).
            o.Add("bounds-short " + Hex(p));
            using (IIter it = e.NewIterator(new ReadSpec((byte[])p.Clone(), upper)))
            {
                it.SeekToFirst();
                Collect(it, true, o);
            }
            // GetCommandScoreIndexEntries: prefix_same_as_start, Seek(prefix), Next.
            o.Add("same-as-start " + Hex(p));
            using (IIter it = e.NewIterator(new ReadSpec(null, null, PrefixSameAsStart: true)))
            {
                it.Seek(p);
                Collect(it, true, o);
            }
            // Prefix mode without bounds or prefix_same_as_start: only keys of the seek prefix are defined.
            o.Add("prefix-mode " + Hex(p));
            using (IIter it = e.NewIterator(new ReadSpec(null, null)))
            {
                it.Seek(p);
                Collect(it, true, o, p);
            }
            // Total order seek over the same range.
            o.Add("total-order " + Hex(p));
            using (IIter it = e.NewIterator(new ReadSpec(null, upper, TotalOrderSeek: true)))
            {
                it.Seek(p);
                Collect(it, true, o);
            }
        }
        return o;
    }

    /// <summary>ClearSimpleTextIndex / ClearVectors: delete every key while iterating the bounded range.</summary>
    private static void DeleteWhileIterating(IEngine e, byte[] p)
    {
        using IIter it = e.NewIterator(new ReadSpec(Filled(p, UidKeyLength, 0), Filled(p, UidKeyLength, 0xFF)));
        for (it.SeekToFirst(); it.Valid; it.Next())
        {
            byte[] k = it.Key;
            // SingleDelete'd keys must not also see a Delete.
            if (k.Length < 2 || k[^8] != (byte)'s' || k[^7] != (byte)'d') e.Delete(k);
        }
    }

    private static void AssertSame(List<string> expected, List<string> actual, string where)
    {
        int n = Math.Min(expected.Count, actual.Count);
        for (int i = 0; i < n; i++)
        {
            if (!string.Equals(expected[i], actual[i], StringComparison.Ordinal))
            {
                // The nearest section header names the read pattern.
                string section = "";
                for (int j = i; j >= 0; j--)
                {
                    if (!expected[j].Contains('=', StringComparison.Ordinal))
                    {
                        section = expected[j];
                        break;
                    }
                }
                Assert.Fail(where + ": first difference at line " + i + " (" + section + "): native " + expected[i] + " managed " + actual[i]);
            }
        }
        Assert.AreEqual(expected.Count, actual.Count, where + ": line count");
    }

    private static void Compare(IEngine native, IEngine managed, Workload w, Family family, string where)
    {
        List<byte[]> probes = w.Probes();
        AssertSame(ReadAll(native, w, probes, family), ReadAll(managed, w, probes, family), where);
    }

    [TestMethod]
    [DataRow(Family.Prefix8Bloom)]
    [DataRow(Family.Prefix10Bloom)]
    [DataRow(Family.Prefix8NoFilter)]
    [DataRow(Family.PointLookup)]
    [DataRow(Family.ULongChangeTracker)]
    public void MosaikReadPatternsMatchNative(Family family)
    {
        int seed = int.TryParse(Environment.GetEnvironmentVariable("MOSAIK_READ_SEED"), NumberStyles.Integer, CultureInfo.InvariantCulture, out int s)
            ? s
            : Environment.TickCount ^ (int)family;
        string ctx = "family=" + family + " seed=" + seed.ToString(CultureInfo.InvariantCulture);
        using var nativeDir = new TempDir("mosaik-read-n");
        using var managedDir = new TempDir("mosaik-read-m");
        var w = new Workload(family, seed);
        using (var native = new NativeEngine(nativeDir.Path, family))
        using (var managed = new ManagedEngine(managedDir.Path, family))
        {
            IEngine[] both = [native, managed];
            w.Round(both, 4000, rangeAndSingleDeletes: false);
            Compare(native, managed, w, family, ctx + " memtable");
            foreach (IEngine e in both) e.Flush();
            Compare(native, managed, w, family, ctx + " one L0 file");
            w.Round(both, 6000, rangeAndSingleDeletes: true);
            foreach (IEngine e in both) e.Flush();
            w.Round(both, 3000, rangeAndSingleDeletes: true);
            foreach (IEngine e in both) e.Flush();
            Compare(native, managed, w, family, ctx + " three L0 files");
            foreach (IEngine e in both) e.CompactAll();
            Compare(native, managed, w, family, ctx + " compacted (bottommost, no filter with optimize_filters_for_hits)");
            w.Round(both, 3000, rangeAndSingleDeletes: true);
            foreach (IEngine e in both) e.Flush();
            w.Round(both, 1500, rangeAndSingleDeletes: true);
            Compare(native, managed, w, family, ctx + " memtable + L0 over the compacted level");
            if (family != Family.ULongChangeTracker)
            {
                for (int i = 0; i < 4; i++)
                {
                    foreach (IEngine e in both) DeleteWhileIterating(e, w.Prefixes[i]);
                }
                Compare(native, managed, w, family, ctx + " after deleting while iterating");
            }
        }

        // Each engine reads the other's files (and WAL tail).
        using (var nativeOnManaged = new NativeEngine(managedDir.Path, family))
        using (var managedOnNative = new ManagedEngine(nativeDir.Path, family))
        {
            Compare(nativeOnManaged, managedOnNative, w, family, ctx + " swapped directories");
            foreach (IEngine e in new IEngine[] { nativeOnManaged, managedOnNative })
            {
                e.Flush();
                e.CompactAll();
            }
            Compare(nativeOnManaged, managedOnNative, w, family, ctx + " swapped directories, compacted");
        }
    }
}
