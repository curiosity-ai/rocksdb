using System.Globalization;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Table;
using RocksDb.Managed.Table.Filters;

namespace RocksDb.Managed.CompatTests.Audit10;

/// <summary>
/// Audit #10, user customization points that shape keys, ordering and reads, differentially against native RocksDB:
/// the same custom prefix extractor (rocksdb_slicetransform_create), custom comparator (rocksdb_comparator_create),
/// built-in extractors changed with SetOptions, and the same ReadOptions::table_filter
/// (rocksdb_readoptions_set_table_filter) are plugged into both engines; a seeded workload (puts, deletes, range
/// deletes, flushes, compactions, snapshots) runs on both and every read (Get, MultiGet-like batches of Gets, iterators
/// forward / backward with bounds and prefix modes) must agree. The DBs are then swapped: each engine reads the other's
/// files with the same hook.
/// </summary>
[TestClass]
public sealed unsafe class Audit10ReadCustomizationCompatTests
{
    private static readonly RocksDbSharp.Native N = RocksDbSharp.Native.Instance;
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);

    private static string Str(byte[]? b) => b is null ? "null" : Encoding.UTF8.GetString(b);

    private static KeyValuePair<string, string> P(string k, string v) => new(k, v);

    // ---- hooks --------------------------------------------------------------------------------------------------

    /// <summary>
    /// The prefix is everything up to and including the first '|'; keys without one are out of the domain. A
    /// variable-length extractor no built-in matches (so the table extractor can only be the configured one).
    /// </summary>
    private static int PrefixLength(ReadOnlySpan<byte> key)
    {
        int i = key.IndexOf((byte)'|');
        return i < 0 ? -1 : i + 1;
    }

    private const string PrefixName = "audit10.UpToBar";

    private sealed class ManagedBarPrefix : SliceTransform
    {
        public override string Name => PrefixName;
        public override ReadOnlySpan<byte> Transform(ReadOnlySpan<byte> key) => key[..PrefixLength(key)];
        public override bool InDomain(ReadOnlySpan<byte> key) => PrefixLength(key) > 0;
    }

    private static readonly IntPtr s_prefixName = Marshal.StringToHGlobalAnsi(PrefixName);

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void NativeDestructor(IntPtr state) { }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static IntPtr NativePrefixName(IntPtr state) => s_prefixName;

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static IntPtr NativeTransform(IntPtr state, IntPtr key, UIntPtr length, UIntPtr* dstLength)
    {
        *dstLength = (UIntPtr)Math.Max(0, PrefixLength(new ReadOnlySpan<byte>((void*)key, checked((int)length))));
        return key;
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static byte NativeInDomain(IntPtr state, IntPtr key, UIntPtr length) =>
        PrefixLength(new ReadOnlySpan<byte>((void*)key, checked((int)length))) > 0 ? (byte)1 : (byte)0;

    private static IntPtr NewNativeBarPrefix() => N.rocksdb_slicetransform_create(IntPtr.Zero,
        (IntPtr)(delegate* unmanaged[Cdecl]<IntPtr, void>)&NativeDestructor,
        (IntPtr)(delegate* unmanaged[Cdecl]<IntPtr, IntPtr, UIntPtr, UIntPtr*, IntPtr>)&NativeTransform,
        (IntPtr)(delegate* unmanaged[Cdecl]<IntPtr, IntPtr, UIntPtr, byte>)&NativeInDomain,
        (IntPtr)(delegate* unmanaged[Cdecl]<IntPtr, IntPtr>)&NativePrefixName);

    /// <summary>Shorter keys first, then bytewise (decimal numbers in numeric order).</summary>
    private static int LengthFirst(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        int c = a.Length.CompareTo(b.Length);
        return c != 0 ? c : a.SequenceCompareTo(b);
    }

    private const string ComparatorName = "audit10.LengthFirst";

    private sealed class ManagedLengthFirst : Comparator
    {
        public override string Name => ComparatorName;
        public override int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => LengthFirst(a, b);
    }

    private sealed class NativeLengthFirst : RocksDbSharp.Comparator
    {
        public string Name => ComparatorName;
        public int Compare(IntPtr a, UIntPtr alen, IntPtr b, UIntPtr blen) =>
            LengthFirst(new ReadOnlySpan<byte>((void*)a, (int)alen), new ReadOnlySpan<byte>((void*)b, (int)blen));
    }

    private const string ReverseName = "audit10.ReverseNoShortening";

    /// <summary>
    /// Reverse bytewise order (the empty key sorts last) under a custom name, with FindShortestSeparator /
    /// FindShortSuccessor overrides that do nothing: comparator.h allows that ("an implementation of this method that
    /// does nothing is correct"), meaning the key stays unchanged.
    /// </summary>
    private sealed class ManagedReverseNoShortening : Comparator
    {
        public override string Name => ReverseName;
        public override int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => b.SequenceCompareTo(a);
        public override void FindShortestSeparator(ReadOnlySpan<byte> start, ReadOnlySpan<byte> limit, System.Buffers.IBufferWriter<byte> result) { }
        public override void FindShortSuccessor(ReadOnlySpan<byte> key, System.Buffers.IBufferWriter<byte> result) { }
    }

    /// <summary>The same order through rocksdb_comparator_create (whose separator / successor do nothing).</summary>
    private sealed class NativeReverse : RocksDbSharp.Comparator
    {
        public string Name => ReverseName;
        public int Compare(IntPtr a, UIntPtr alen, IntPtr b, UIntPtr blen) =>
            new ReadOnlySpan<byte>((void*)b, (int)blen).SequenceCompareTo(new ReadOnlySpan<byte>((void*)a, (int)alen));
    }

    /// <summary>The user key order of <paramref name="hook"/>'s column family.</summary>
    private static int Cmp(Hook hook, ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => hook switch
    {
        Hook.LengthFirstComparator => LengthFirst(a, b),
        Hook.ReverseNoShortening => b.SequenceCompareTo(a),
        _ => a.SequenceCompareTo(b),
    };

    /// <summary>table_filter: tables whose entry count is a multiple of three are skipped.</summary>
    private static bool KeepTable(ulong numEntries) => numEntries % 3 != 0;

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static byte NativeTableFilter(IntPtr state, IntPtr props)
    {
        Interlocked.Increment(ref s_nativeTableFilterCalls);
        return KeepTable(N.rocksdb_table_properties_num_entries(props)) ? (byte)1 : (byte)0;
    }

    private static int s_nativeTableFilterCalls;

    // ---- the workload -------------------------------------------------------------------------------------------

    private enum OpKind { Put, Delete, DeleteRange, Flush, Compact, Snapshot, ReleaseSnapshot, Read, SetPrefix }

    private readonly record struct Op(OpKind Kind, byte[]? A = null, byte[]? B = null, string? Text = null);

    /// <summary>What the scenario plugs into both engines.</summary>
    private enum Hook { BarPrefix, BarPrefixPartitioned, LengthFirstComparator, BuiltInPrefixChanges, TableFilter, ReverseNoShortening }

    /// <summary>Small data and metadata blocks, so that tables have many index entries (separators) and filter partitions.</summary>
    private const ulong SmallBlock = 256;

    private static string[] KeyUniverse(Hook hook, Random r)
    {
        var keys = new List<string>();
        if (hook == Hook.LengthFirstComparator)
        {
            for (int i = 0; i < 400; i++) keys.Add(r.Next(0, 100000).ToString(CultureInfo.InvariantCulture));
        }
        else
        {
            string[] groups = ["a", "ab", "abc", "b", "ba", "bb", "c", "cab", "x", ""];
            foreach (string g in groups)
            {
                for (int i = 0; i < 30; i++) keys.Add(g + "|" + r.Next(0, 1000).ToString("D3", CultureInfo.InvariantCulture));
                keys.Add(g); // out of the bar extractor's domain
            }
            keys.Add("ab|");
            keys.Add("|");
        }
        keys.Sort(StringComparer.Ordinal);
        return keys.Distinct().ToArray();
    }

    private static List<Op> Workload(Hook hook, int seed, int count)
    {
        var r = new Random(seed);
        string[] keys = KeyUniverse(hook, r);
        byte[] K() => U(keys[r.Next(keys.Length)]);
        var ops = new List<Op>(count);
        string[] builtIns = ["rocksdb.CappedPrefix.2", "rocksdb.FixedPrefix.1", "rocksdb.FixedPrefix.3", "rocksdb.CappedPrefix.5"];
        for (int i = 0; i < count; i++)
        {
            int p = r.Next(1000);
            if (p < 600) ops.Add(new Op(OpKind.Put, K(), U("v" + i.ToString(CultureInfo.InvariantCulture))));
            else if (p < 750) ops.Add(new Op(OpKind.Delete, K()));
            else if (p < 770)
            {
                byte[] a = K(), b = K();
                int c = Cmp(hook, a, b);
                if (c != 0) ops.Add(c < 0 ? new Op(OpKind.DeleteRange, a, b) : new Op(OpKind.DeleteRange, b, a));
            }
            else if (p < 800) ops.Add(new Op(OpKind.Flush));
            else if (p < 805) ops.Add(new Op(OpKind.Compact));
            else if (p < 815) ops.Add(new Op(OpKind.Snapshot));
            else if (p < 820) ops.Add(new Op(OpKind.ReleaseSnapshot));
            else if (p < 840) ops.Add(new Op(OpKind.Read, K(), K()));
            else if (p < 843 && hook == Hook.BuiltInPrefixChanges) ops.Add(new Op(OpKind.SetPrefix, Text: builtIns[r.Next(builtIns.Length)]));
            else ops.Add(new Op(OpKind.Put, K(), U("w" + i.ToString(CultureInfo.InvariantCulture))));
        }
        ops.Add(new Op(OpKind.Flush));
        ops.Add(new Op(OpKind.Read, U(keys[0]), U(keys[^1])));
        return ops;
    }

    /// <summary>The operations both engines implement.</summary>
    private interface IEngine : IDisposable
    {
        void Put(byte[] k, byte[] v);
        void Delete(byte[] k);
        void DeleteRange(byte[] b, byte[] e);
        void Flush();
        void Compact();
        void TakeSnapshot();
        void ReleaseSnapshot();
        void SetPrefix(string id);
        string? Get(byte[] k, bool atSnapshot);
        /// <summary>Up to <paramref name="max"/> keys of an iterator positioned by <paramref name="seek"/> and moved with Next / Prev.</summary>
        List<string> Iterate(ReadSpec spec, int max);
    }

    private sealed record ReadSpec(bool AtSnapshot, bool PrefixSameAsStart, bool TotalOrderSeek, bool AutoPrefixMode, byte[]? Lower, byte[]? Upper,
        byte[]? Target, bool Reverse, bool TableFilter);

    private sealed class NativeEngine : IEngine
    {
        private readonly RocksDbSharp.RocksDb _db;
        private RocksDbSharp.Snapshot? _snapshot;

        public NativeEngine(string path, Hook hook)
        {
            var cf = new RocksDbSharp.ColumnFamilyOptions();
            cf.SetDisableAutoCompactions(1);
            var bbto = new RocksDbSharp.BlockBasedTableOptions().SetFilterPolicy(RocksDbSharp.BloomFilterPolicy.Create(10, false)).SetBlockSize(SmallBlock);
            switch (hook)
            {
                case Hook.BarPrefix:
                    bbto.SetWholeKeyFiltering(false);
                    cf.SetPrefixExtractor(NewNativeBarPrefix()).SetMemtablePrefixBloomSizeRatio(0.1);
                    break;
                case Hook.BarPrefixPartitioned:
                    bbto.SetIndexType(RocksDbSharp.BlockBasedTableIndexType.TwoLevelIndex);
                    N.rocksdb_block_based_options_set_partition_filters(bbto.Handle, 1);
                    N.rocksdb_block_based_options_set_metadata_block_size(bbto.Handle, SmallBlock);
                    cf.SetPrefixExtractor(NewNativeBarPrefix()).SetOptimizeFiltersForHits(1);
                    break;
                case Hook.BuiltInPrefixChanges:
                    bbto.SetWholeKeyFiltering(false);
                    cf.SetPrefixExtractor(RocksDbSharp.SliceTransform.CreateFixedPrefix(2)).SetMemtablePrefixBloomSizeRatio(0.1);
                    break;
                case Hook.LengthFirstComparator:
                    cf.SetComparator(new NativeLengthFirst());
                    break;
                case Hook.ReverseNoShortening:
                    cf.SetComparator(new NativeReverse());
                    break;
            }
            cf.SetBlockBasedTableFactory(bbto);
            var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
            s_keepAlive.Add(cf);
            s_keepAlive.Add(o);
            s_keepAlive.Add(bbto);
            _db = RocksDbSharp.RocksDb.Open(o, path, new RocksDbSharp.ColumnFamilies(cf));
        }

        public void Put(byte[] k, byte[] v) => _db.Put(k, v);
        public void Delete(byte[] k) => _db.Remove(k);

        public void DeleteRange(byte[] b, byte[] e)
        {
            using var wb = new RocksDbSharp.WriteBatch();
            wb.DeleteRange(b, (ulong)b.Length, e, (ulong)e.Length);
            _db.Write(wb);
        }

        public void Flush() => _db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
        public void Compact() => _db.CompactRange((byte[]?)null, null);

        public void TakeSnapshot()
        {
            _snapshot?.Dispose();
            _snapshot = _db.CreateSnapshot();
        }

        public void ReleaseSnapshot()
        {
            _snapshot?.Dispose();
            _snapshot = null;
        }

        public void SetPrefix(string id) => _db.SetOptions([P("prefix_extractor", id)]);

        private RocksDbSharp.ReadOptions Ro(bool atSnapshot)
        {
            var ro = new RocksDbSharp.ReadOptions();
            if (atSnapshot && _snapshot is not null) ro.SetSnapshot(_snapshot);
            return ro;
        }

        public string? Get(byte[] k, bool atSnapshot) => _db.Get(k, null, Ro(atSnapshot)) is { } v ? Encoding.UTF8.GetString(v) : null;

        public List<string> Iterate(ReadSpec s, int max)
        {
            RocksDbSharp.ReadOptions ro = Ro(s.AtSnapshot);
            if (s.PrefixSameAsStart) ro.SetPrefixSameAsStart(true);
            if (s.TotalOrderSeek) ro.SetTotalOrderSeek(true);
            if (s.AutoPrefixMode) N.rocksdb_readoptions_set_auto_prefix_mode(ro.Handle, 1);
            if (s.Lower is not null) ro.SetIterateLowerBound(s.Lower);
            if (s.Upper is not null) ro.SetIterateUpperBound(s.Upper);
            if (s.TableFilter)
            {
                N.rocksdb_readoptions_set_table_filter(ro.Handle, IntPtr.Zero, (IntPtr)(delegate* unmanaged[Cdecl]<IntPtr, void>)&NativeDestructor,
                    (IntPtr)(delegate* unmanaged[Cdecl]<IntPtr, IntPtr, byte>)&NativeTableFilter);
            }
            var keys = new List<string>();
            using RocksDbSharp.Iterator it = _db.NewIterator(null, ro);
            if (s.Target is null)
            {
                if (s.Reverse) it.SeekToLast();
                else it.SeekToFirst();
            }
            else if (s.Reverse) it.SeekForPrev(s.Target);
            else it.Seek(s.Target);
            while (it.Valid() && keys.Count < max)
            {
                keys.Add(Encoding.UTF8.GetString(it.Key()) + "=" + Encoding.UTF8.GetString(it.Value()));
                if (s.Reverse) it.Prev();
                else it.Next();
            }
            GC.KeepAlive(ro);
            return keys;
        }

        public void Dispose()
        {
            _snapshot?.Dispose();
            _db.Dispose();
        }
    }

    private sealed class ManagedEngine : IEngine
    {
        private readonly RocksDatabase _db;
        private Snapshot? _snapshot;

        public static ColumnFamilyOptions Options(Hook hook)
        {
            var cf = new ColumnFamilyOptions { DisableAutoCompactions = true };
            cf.TableOptions.FilterPolicy = new BloomFilterPolicy(10);
            cf.TableOptions.BlockSize = SmallBlock;
            switch (hook)
            {
                case Hook.BarPrefix:
                    cf.TableOptions.WholeKeyFiltering = false;
                    cf.PrefixExtractor = new ManagedBarPrefix();
                    cf.MemtablePrefixBloomSizeRatio = 0.1;
                    break;
                case Hook.BarPrefixPartitioned:
                    cf.TableOptions.IndexType = IndexType.TwoLevelIndexSearch;
                    cf.TableOptions.PartitionFilters = true;
                    cf.TableOptions.MetadataBlockSize = SmallBlock;
                    cf.PrefixExtractor = new ManagedBarPrefix();
                    cf.OptimizeFiltersForHits = true;
                    break;
                case Hook.BuiltInPrefixChanges:
                    cf.TableOptions.WholeKeyFiltering = false;
                    cf.PrefixExtractor = SliceTransform.CreateFixedPrefix(2);
                    cf.MemtablePrefixBloomSizeRatio = 0.1;
                    break;
                case Hook.LengthFirstComparator:
                    cf.Comparator = new ManagedLengthFirst();
                    break;
                case Hook.ReverseNoShortening:
                    cf.Comparator = new ManagedReverseNoShortening();
                    break;
            }
            return cf;
        }

        public ManagedEngine(string path, Hook hook) => _db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true }, Options(hook));

        public RocksDatabase Db => _db;
        public void Put(byte[] k, byte[] v) => _db.Put(k, v);
        public void Delete(byte[] k) => _db.Delete(k);
        public void DeleteRange(byte[] b, byte[] e) => _db.DeleteRange(b, e);
        public void Flush() => _db.Flush();
        public void Compact() => _db.CompactRange();

        public void TakeSnapshot()
        {
            _snapshot?.Dispose();
            _snapshot = _db.GetSnapshot();
        }

        public void ReleaseSnapshot()
        {
            _snapshot?.Dispose();
            _snapshot = null;
        }

        public void SetPrefix(string id) => _db.SetOptions([P("prefix_extractor", id)]);

        private ReadOptions Ro(bool atSnapshot) => new() { Snapshot = atSnapshot ? _snapshot : null };

        public string? Get(byte[] k, bool atSnapshot) => _db.Get(Ro(atSnapshot), k) is { } v ? Encoding.UTF8.GetString(v) : null;

        public List<string> Iterate(ReadSpec s, int max)
        {
            ReadOptions ro = Ro(s.AtSnapshot);
            ro.PrefixSameAsStart = s.PrefixSameAsStart;
            ro.TotalOrderSeek = s.TotalOrderSeek;
            ro.AutoPrefixMode = s.AutoPrefixMode;
            if (s.Lower is not null) ro.IterateLowerBound = s.Lower;
            if (s.Upper is not null) ro.IterateUpperBound = s.Upper;
            if (s.TableFilter) ro.TableFilter = static p => KeepTable(p.NumEntries);
            var keys = new List<string>();
            using Iterator it = _db.NewIterator(ro);
            if (s.Target is null)
            {
                if (s.Reverse) it.SeekToLast();
                else it.SeekToFirst();
            }
            else if (s.Reverse) it.SeekForPrev(s.Target);
            else it.Seek(s.Target);
            while (it.Valid && keys.Count < max)
            {
                keys.Add(Encoding.UTF8.GetString(it.Key) + "=" + Encoding.UTF8.GetString(it.Value));
                if (s.Reverse) it.Prev();
                else it.Next();
            }
            return keys;
        }

        public void Dispose()
        {
            _snapshot?.Dispose();
            _db.Dispose();
        }
    }

    /// <summary>The reads of one Read op: Gets around the two keys and iterators in every mode the hook supports.</summary>
    private static List<ReadSpec> ReadSpecs(Hook hook, byte[] a, byte[] b, SliceTransform? extractor)
    {
        (byte[] lo, byte[] hi) = Cmp(hook, a, b) <= 0 ? (a, b) : (b, a);
        bool tf = hook == Hook.TableFilter;
        var specs = new List<ReadSpec>();
        foreach (bool snap in new[] { false, true })
        {
            specs.Add(new ReadSpec(snap, false, true, false, null, null, null, false, tf));
            specs.Add(new ReadSpec(snap, false, true, false, null, null, null, true, tf));
            specs.Add(new ReadSpec(snap, false, true, false, lo, hi, a, false, tf));
            specs.Add(new ReadSpec(snap, false, true, false, lo, hi, b, true, tf));
            if (extractor is not null)
            {
                // prefix_same_as_start only with a target in the extractor's domain: otherwise DBIter sets no prefix
                // (ShouldSetPrefix) and the iterator is a legacy prefix seek whose results past the target's prefix
                // are undefined (they differ between native and the port). auto_prefix_mode gives total order results.
                if (extractor.InDomain(a)) specs.Add(new ReadSpec(snap, true, false, false, null, null, a, false, false));
                if (extractor.InDomain(b)) specs.Add(new ReadSpec(snap, true, false, false, null, null, b, true, false));
                specs.Add(new ReadSpec(snap, false, false, true, null, hi, lo, false, false));
                specs.Add(new ReadSpec(snap, false, false, true, lo, hi, hi, true, false));
            }
        }
        return specs;
    }

    private static SliceTransform? InitialExtractor(Hook hook) => hook switch
    {
        Hook.BarPrefix or Hook.BarPrefixPartitioned => new ManagedBarPrefix(),
        Hook.BuiltInPrefixChanges => SliceTransform.CreateFixedPrefix(2),
        _ => null,
    };

    private static List<string> Run(IEngine e, Hook hook, List<Op> ops, bool readsOnly)
    {
        var log = new List<string>();
        SliceTransform? extractor = InitialExtractor(hook);
        foreach (Op op in ops)
        {
            switch (op.Kind)
            {
                case OpKind.Put when !readsOnly: e.Put(op.A!, op.B!); break;
                case OpKind.Delete when !readsOnly: e.Delete(op.A!); break;
                case OpKind.DeleteRange when !readsOnly: e.DeleteRange(op.A!, op.B!); break;
                case OpKind.Flush when !readsOnly: e.Flush(); break;
                case OpKind.Compact when !readsOnly: e.Compact(); break;
                case OpKind.Snapshot when !readsOnly: e.TakeSnapshot(); break;
                case OpKind.ReleaseSnapshot when !readsOnly: e.ReleaseSnapshot(); break;
                case OpKind.SetPrefix when !readsOnly:
                    e.SetPrefix(op.Text!);
                    extractor = SliceTransform.CreateFromString(op.Text!);
                    log.Add("prefix " + op.Text);
                    break;
                case OpKind.Read:
                    log.Add("get " + Str(op.A) + "=" + (e.Get(op.A!, false) ?? "null") + " / " + (e.Get(op.A!, true) ?? "null"));
                    log.Add("get " + Str(op.B) + "=" + (e.Get(op.B!, false) ?? "null"));
                    foreach (ReadSpec s in ReadSpecs(hook, op.A!, op.B!, extractor)) log.Add(Describe(s) + ": " + string.Join(",", e.Iterate(s, 25)));
                    break;
            }
        }
        return log;
    }

    private static string Describe(ReadSpec s) =>
        (s.AtSnapshot ? "snap " : "") + (s.PrefixSameAsStart ? "psas " : "") + (s.TotalOrderSeek ? "tos " : "") + (s.AutoPrefixMode ? "apm " : "")
        + (s.Reverse ? "rev " : "") + "[" + Str(s.Lower) + "," + Str(s.Upper) + ") @" + Str(s.Target);

    private static void AssertSameLogs(List<string> native, List<string> managed, string what)
    {
        Assert.AreEqual(native.Count, managed.Count, what + ": number of reads");
        for (int i = 0; i < native.Count; i++) Assert.AreEqual(native[i], managed[i], what + ": read " + i.ToString(CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// The differential run of <paramref name="hook"/>: the workload on both engines with every read compared, then the
    /// final reads of each engine on the other engine's DB (cross-engine: files, MANIFEST, OPTIONS and WAL written with
    /// the hook by one engine are read with it by the other).
    /// </summary>
    private static void Differential(Hook hook, int seed, int count)
    {
        Console.WriteLine("seed=" + seed.ToString(CultureInfo.InvariantCulture));
        List<Op> ops = Workload(hook, seed, count);
        using var nativeDir = new TempDir("a10-native");
        using var managedDir = new TempDir("a10-managed");
        List<string> nativeLog, managedLog;
        using (var n = new NativeEngine(nativeDir.Path, hook)) nativeLog = Run(n, hook, ops, readsOnly: false);
        using (var m = new ManagedEngine(managedDir.Path, hook)) managedLog = Run(m, hook, ops, readsOnly: false);
        AssertSameLogs(nativeLog, managedLog, "same workload");
        if (hook == Hook.BuiltInPrefixChanges) return; // the last configured extractor is in each OPTIONS file only
        // Cross-engine: the final reads (the last op) on the other engine's DB.
        List<Op> lastRead = [ops[^1]];
        List<string> expected;
        using (var n = new NativeEngine(nativeDir.Path, hook)) expected = Run(n, hook, lastRead, readsOnly: true);
        using (var m = new ManagedEngine(nativeDir.Path, hook))
        {
            AssertSameLogs(expected, Run(m, hook, lastRead, readsOnly: true), "managed reading the native DB");
            if (hook is Hook.BarPrefix or Hook.BarPrefixPartitioned)
            {
                foreach (TableProperties p in m.Db.DefaultColumnFamily.GetPropertiesOfAllTables().Values) Assert.AreEqual(PrefixName, p.PrefixExtractorName);
            }
            if (hook == Hook.LengthFirstComparator)
            {
                foreach (TableProperties p in m.Db.DefaultColumnFamily.GetPropertiesOfAllTables().Values) Assert.AreEqual(ComparatorName, p.ComparatorName);
            }
            if (hook == Hook.ReverseNoShortening)
            {
                foreach (TableProperties p in m.Db.DefaultColumnFamily.GetPropertiesOfAllTables().Values) Assert.AreEqual(ReverseName, p.ComparatorName);
            }
        }
        using (var m = new ManagedEngine(managedDir.Path, hook)) expected = Run(m, hook, lastRead, readsOnly: true);
        using (var n = new NativeEngine(managedDir.Path, hook)) AssertSameLogs(expected, Run(n, hook, lastRead, readsOnly: true), "native reading the managed DB");
    }

    private static int Seed() => Environment.GetEnvironmentVariable("AUDIT10_SEED") is { } s
        ? int.Parse(s, CultureInfo.InvariantCulture)
        : (int)(DateTime.UtcNow.Ticks % 1_000_000);

    /// <summary>
    /// A custom prefix extractor (variable length, keys out of its domain) with the memtable prefix bloom
    /// (MemTable::Get / MemTableIterator::Seek), prefix-only SST filters (whole_key_filtering = false:
    /// BlockBasedTable::FullFilterKeyMayMatch, PrefixRangeMayMatch), prefix_same_as_start (DBIter), auto_prefix_mode
    /// with bounds and total order seeks; rocksdb.prefix.extractor.name is the extractor's name in both engines' files.
    /// </summary>
    [TestMethod]
    public void CustomPrefixExtractorReadsMatchNative() => Differential(Hook.BarPrefix, Seed(), 2500);

    /// <summary>
    /// The same extractor with partitioned filters and a two-level index (PartitionedFilterBlockBuilder: whole keys and
    /// prefixes, boundary prefixes at partition cuts; PartitionedFilterBlockReader), 256-byte blocks, and
    /// optimize_filters_for_hits (no filter in bottommost files).
    /// </summary>
    [TestMethod]
    public void CustomPrefixExtractorWithPartitionedFiltersMatchesNative() => Differential(Hook.BarPrefixPartitioned, Seed(), 2500);

    /// <summary>
    /// A custom comparator (rocksdb_comparator_create; FindShortestSeparator / FindShortSuccessor unchanged, as for
    /// C API comparators) for memtables, SST index keys, range tombstone fragmentation, merging iterators and
    /// compaction; rocksdb.comparator is its name.
    /// </summary>
    [TestMethod]
    public void CustomComparatorReadsMatchNative() => Differential(Hook.LengthFirstComparator, Seed(), 2500);

    /// <summary>
    /// include/rocksdb/comparator.h: "Simple comparator implementations may return with *start unchanged, i.e., an
    /// implementation of this method that does nothing is correct." A reverse-order comparator (the empty key sorts
    /// last) whose FindShortestSeparator / FindShortSuccessor overrides write nothing must keep the index keys
    /// unchanged, like native's C API comparator (whose separator / successor do nothing): 256-byte blocks give many
    /// index entries; reads, seeks in both directions and the cross-engine reopen match native.
    /// </summary>
    [TestMethod]
    public void ComparatorWithEmptySeparatorOverridesMatchesNative() => Differential(Hook.ReverseNoShortening, Seed(), 2500);

    /// <summary>
    /// SetOptions(prefix_extractor) between built-in extractors (cf_mutable_options prefix_extractor, applied to new
    /// memtables and to the table readers' PrefixExtractorChanged checks; tables keep the extractor they were built
    /// with) while memtables and tables of earlier extractors are live.
    /// </summary>
    [TestMethod]
    public void DynamicBuiltInPrefixExtractorChangesMatchNative() => Differential(Hook.BuiltInPrefixChanges, Seed(), 2500);

    /// <summary>
    /// ReadOptions::table_filter (TableCache::NewIterator) skipping the tables whose num_entries is a multiple of three,
    /// on level 0 and on compacted levels, with range tombstones of skipped tables still applied.
    /// </summary>
    [TestMethod]
    public void TableFilterIteratorsMatchNative()
    {
        int before = Volatile.Read(ref s_nativeTableFilterCalls);
        Differential(Hook.TableFilter, Seed(), 2500);
        Assert.IsTrue(Volatile.Read(ref s_nativeTableFilterCalls) > before, "native table_filter not called");
    }

    /// <summary>
    /// DBImpl::FailIfTableFilterWithRangeConversion: both engines refuse a table_filter iterator with InvalidArgument
    /// while min_tombstones_for_range_conversion is non-zero.
    /// </summary>
    [TestMethod]
    public void TableFilterWithRangeConversionIsRefusedOnBothEngines()
    {
        using var dir = new TempDir("a10-tfrc");
        var cf = new RocksDbSharp.ColumnFamilyOptions();
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        s_keepAlive.Add(cf);
        s_keepAlive.Add(o);
        string nativeMessage;
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(o, dir.Path, new RocksDbSharp.ColumnFamilies(cf)))
        {
            db.Put(U("k"), U("v"));
            db.SetOptions([P("min_tombstones_for_range_conversion", "4")]);
            var ro = new RocksDbSharp.ReadOptions();
            N.rocksdb_readoptions_set_table_filter(ro.Handle, IntPtr.Zero, (IntPtr)(delegate* unmanaged[Cdecl]<IntPtr, void>)&NativeDestructor,
                (IntPtr)(delegate* unmanaged[Cdecl]<IntPtr, IntPtr, byte>)&NativeTableFilter);
            using RocksDbSharp.Iterator it = db.NewIterator(null, ro);
            it.SeekToFirst();
            var ex = Assert.ThrowsExactly<RocksDbSharp.RocksDbException>(() => N.rocksdb_iter_get_error(it.Handle));
            nativeMessage = ex.Message;
        }
        Console.WriteLine("native: " + nativeMessage);
        StringAssert.Contains(nativeMessage, "table_filter is not supported when min_tombstones_for_range_conversion > 0");
        using var mdir = new TempDir("a10-tfrc-m");
        using RocksDatabase mdb = RocksDatabase.Open(mdir.Path, new DbOptions { CreateIfMissing = true });
        mdb.SetOptions([P("min_tombstones_for_range_conversion", "4")]);
        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => mdb.NewIterator(new ReadOptions { TableFilter = static _ => true }).Dispose());
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        StringAssert.Contains(nativeMessage, e.Message.Replace("Invalid argument: ", "", StringComparison.Ordinal));
    }

    /// <summary>Bytewise, or reverse bytewise once <see cref="Reverse"/> is set.</summary>
    private static volatile bool s_flip;

    private sealed class NativeFlipping : RocksDbSharp.Comparator
    {
        public string Name => "audit10.Flipping";
        public int Compare(IntPtr a, UIntPtr alen, IntPtr b, UIntPtr blen)
        {
            var x = new ReadOnlySpan<byte>((void*)a, (int)alen);
            var y = new ReadOnlySpan<byte>((void*)b, (int)blen);
            return s_flip ? y.SequenceCompareTo(x) : x.SequenceCompareTo(y);
        }
    }

    private sealed class ManagedFlipping : Comparator
    {
        public volatile bool Reverse;
        public override string Name => "audit10.Flipping";
        public override int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => Reverse ? b.SequenceCompareTo(a) : a.SequenceCompareTo(b);
    }

    /// <summary>
    /// A comparator whose order changes after the memtable was written (inconsistent comparator): the flush's
    /// OutputValidator (db/builder.cc BuildTable -> OutputValidator::Add / CanAdd) fails it with Corruption "Compaction
    /// sees out-of-order keys." on both engines, and no table file is written.
    /// </summary>
    [TestMethod]
    public void InconsistentComparatorFailsTheFlushOnBothEngines()
    {
        using var dir = new TempDir("a10-flip");
        var cf = new RocksDbSharp.ColumnFamilyOptions();
        cf.SetComparator(new NativeFlipping());
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        s_keepAlive.Add(cf);
        s_keepAlive.Add(o);
        string nativeMessage;
        s_flip = false;
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(o, dir.Path, new RocksDbSharp.ColumnFamilies(cf)))
        {
            for (int i = 0; i < 20; i++) db.Put(U("k" + i.ToString("D2", CultureInfo.InvariantCulture)), U("v"));
            s_flip = true;
            nativeMessage = Assert.ThrowsExactly<RocksDbSharp.RocksDbException>(() => db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true))).Message;
            s_flip = false;
        }
        Console.WriteLine("native: " + nativeMessage);
        Assert.IsFalse(Directory.GetFiles(dir.Path, "*.sst").Length > 0, "native left a table file");

        using var mdir = new TempDir("a10-flip-m");
        var cmp = new ManagedFlipping();
        using RocksDatabase mdb = RocksDatabase.Open(mdir.Path, new DbOptions { CreateIfMissing = true }, new ColumnFamilyOptions { Comparator = cmp });
        for (int i = 0; i < 20; i++) mdb.Put(U("k" + i.ToString("D2", CultureInfo.InvariantCulture)), U("v"));
        cmp.Reverse = true;
        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => mdb.Flush());
        Console.WriteLine("managed: " + e.Message);
        Assert.AreEqual(nativeMessage, e.Message);
        Assert.AreEqual(StatusCode.Corruption, e.Code);
        Assert.AreEqual(0, Directory.GetFiles(mdir.Path, "*.sst").Length);
    }
}
