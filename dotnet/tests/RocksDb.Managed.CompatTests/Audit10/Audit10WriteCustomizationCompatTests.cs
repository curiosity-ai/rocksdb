using System.Buffers;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Env;

namespace RocksDb.Managed.CompatTests.Audit10;

/// <summary>
/// Audit #10, user callbacks on the write / flush / compaction paths against native RocksDB 11.12 (rocksdb-sharp C API):
/// the same custom merge operator (rocksdb_mergeoperator_create) and compaction filter / filter factory
/// (rocksdb_compactionfilter_create, rocksdb_compactionfilterfactory_create) are implemented for both engines and a
/// seeded random workload (puts, merges, deletes, range deletions, flushes, manual compactions, snapshots, reopens) runs
/// on both; every read (Get and MultiGet at the tip and at each snapshot, iterators in both directions with bounds) must
/// agree. At each reopen the engines swap directories, so each one continues on files the other wrote (cross-engine).
/// The partial merge marks its result with parentheses, so the reads also show where flushes and compactions combined
/// operands (MergeHelper::MergeUntil, PartialMergeMulti) and must agree on that too.
/// </summary>
[TestClass]
public sealed unsafe class Audit10WriteCustomizationCompatTests
{
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    private static byte[] B(string s) => Encoding.UTF8.GetBytes(s);

    private static string? S(byte[]? b) => b is null ? null : Encoding.UTF8.GetString(b);

    private const string MergeName = "Audit10Merge";

    // ---- the merge operator, once per engine ------------------------------------------------------------------------

    /// <summary>
    /// existing|op1|op2... (no leading '|' without a base value); a partial merge of a run of operands is "(op1|op2)".
    /// An operand "FAIL" makes the full merge fail (returns false).
    /// </summary>
    private static byte[]? FullMergeImpl(bool hasExisting, ReadOnlySpan<byte> existing, int count, Func<int, byte[]> operand)
    {
        var sb = new StringBuilder();
        if (hasExisting) sb.Append(Encoding.UTF8.GetString(existing));
        for (int i = 0; i < count; i++)
        {
            string op = Encoding.UTF8.GetString(operand(i));
            if (op == "FAIL") return null;
            if (sb.Length > 0 || hasExisting) sb.Append('|');
            sb.Append(op);
        }
        return B(sb.ToString());
    }

    private static byte[] PartialMergeImpl(int count, Func<int, byte[]> operand)
    {
        var sb = new StringBuilder("(");
        for (int i = 0; i < count; i++)
        {
            if (i > 0) sb.Append('|');
            sb.Append(Encoding.UTF8.GetString(operand(i)));
        }
        return B(sb.Append(')').ToString());
    }

    private sealed class ManagedMerge : MergeOperator
    {
        public override string Name => MergeName;

        public override bool FullMerge(ReadOnlySpan<byte> key, bool hasExistingValue, ReadOnlySpan<byte> existingValue, in MergeOperands operands,
            IBufferWriter<byte> result)
        {
            var ops = new byte[operands.Count][];
            for (int i = 0; i < ops.Length; i++) ops[i] = operands[i].ToArray();
            byte[]? r = FullMergeImpl(hasExistingValue, existingValue, ops.Length, i => ops[i]);
            if (r is null) return false;
            result.Write(r);
            return true;
        }

        public override bool PartialMerge(ReadOnlySpan<byte> key, in MergeOperands operands, IBufferWriter<byte> result)
        {
            var ops = new byte[operands.Count][];
            for (int i = 0; i < ops.Length; i++)
            {
                ops[i] = operands[i].ToArray();
                if (S(ops[i]) == "FAIL") return false;
            }
            result.Write(PartialMergeImpl(ops.Length, i => ops[i]));
            return true;
        }
    }

    private static RocksDbSharp.MergeOperator NativeMerge() => RocksDbSharp.MergeOperators.Create(
        MergeName,
        static (ReadOnlySpan<byte> key, RocksDbSharp.MergeOperators.OperandsEnumerator operands, out bool success) =>
        {
            var ops = new byte[operands.Count][];
            for (int i = 0; i < ops.Length; i++)
            {
                ops[i] = operands.Get(i).ToArray();
                if (S(ops[i]) == "FAIL")
                {
                    success = false;
                    return [];
                }
            }
            success = true;
            return PartialMergeImpl(ops.Length, i => ops[i]);
        },
        static (ReadOnlySpan<byte> key, bool hasExisting, ReadOnlySpan<byte> existing, RocksDbSharp.MergeOperators.OperandsEnumerator operands,
            out bool success) =>
        {
            var ops = new byte[operands.Count][];
            for (int i = 0; i < ops.Length; i++) ops[i] = operands.Get(i).ToArray();
            byte[]? r = FullMergeImpl(hasExisting, existing, ops.Length, i => ops[i]);
            success = r is not null;
            return r ?? [];
        });

    // ---- the compaction filter, once per engine ---------------------------------------------------------------------

    /// <summary>
    /// Values ending in '7' are removed, values ending in '3' get "!" appended, keys starting with "e" are emptied (an
    /// empty new value), everything else is kept. Merge operands are kept (the C API filter only implements Filter, so
    /// native's FilterMergeOperand keeps them).
    /// </summary>
    private static (bool Remove, byte[]? NewValue) FilterImpl(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        if (value.Length > 0 && value[^1] == (byte)'7') return (true, null);
        if (key.Length > 0 && key[0] == (byte)'e') return (false, []);
        if (value.Length > 0 && value[^1] == (byte)'3')
        {
            byte[] nv = new byte[value.Length + 1];
            value.CopyTo(nv);
            nv[^1] = (byte)'!';
            return (false, nv);
        }
        return (false, null);
    }

    private sealed class ManagedFilter : CompactionFilter
    {
        public override string Name => "Audit10Filter";

        public override CompactionFilterDecision Filter(int level, ReadOnlySpan<byte> key, CompactionFilterValueType valueType, ReadOnlySpan<byte> existingValue,
            IBufferWriter<byte> newValue, IBufferWriter<byte> skipUntil)
        {
            if (valueType != CompactionFilterValueType.Value) return CompactionFilterDecision.Keep;
            (bool remove, byte[]? nv) = FilterImpl(key, existingValue);
            if (remove) return CompactionFilterDecision.Remove;
            if (nv is null) return CompactionFilterDecision.Keep;
            newValue.Write(nv);
            return CompactionFilterDecision.ChangeValue;
        }
    }

    /// <summary>The contexts each engine's factory saw: "full/manual" per created filter.</summary>
    private sealed class ManagedFilterFactory(List<string> contexts) : CompactionFilterFactory
    {
        public override string Name => "Audit10FilterFactory";

        public override CompactionFilter? CreateCompactionFilter(in CompactionFilterContext context)
        {
            lock (contexts) contexts.Add((context.IsFullCompaction ? "full" : "part") + "/" + (context.IsManualCompaction ? "manual" : "auto"));
            return new ManagedFilter();
        }
    }

    // Native filter callbacks: the new value must stay valid until the callback returns to native code, which copies it
    // (rocksdb_compactionfilter_t::Filter assigns it to new_value); a per-thread unmanaged buffer is kept for that.
    [ThreadStatic] private static IntPtr t_newValue;
    [ThreadStatic] private static int t_newValueCapacity;
    private static readonly IntPtr s_filterName = Marshal.StringToHGlobalAnsi("Audit10Filter");
    private static readonly IntPtr s_factoryName = Marshal.StringToHGlobalAnsi("Audit10FilterFactory");
    private static List<string>? s_nativeContexts;

    [UnmanagedCallersOnly]
    private static byte NativeFilter(IntPtr state, int level, byte* key, nuint keyLength, byte* value, nuint valueLength, byte** newValue,
        nuint* newValueLength, byte* valueChanged)
    {
        (bool remove, byte[]? nv) = FilterImpl(new ReadOnlySpan<byte>(key, (int)keyLength), new ReadOnlySpan<byte>(value, (int)valueLength));
        if (remove) return 1;
        if (nv is not null)
        {
            if (t_newValueCapacity < nv.Length + 1)
            {
                if (t_newValue != IntPtr.Zero) Marshal.FreeHGlobal(t_newValue);
                t_newValueCapacity = Math.Max(64, nv.Length + 1);
                t_newValue = Marshal.AllocHGlobal(t_newValueCapacity);
            }
            Marshal.Copy(nv, 0, t_newValue, nv.Length);
            *newValue = (byte*)t_newValue;
            *newValueLength = (nuint)nv.Length;
            *valueChanged = 1;
        }
        return 0;
    }

    [UnmanagedCallersOnly]
    private static void NativeDestroy(IntPtr state)
    {
    }

    [UnmanagedCallersOnly]
    private static IntPtr NativeFilterName(IntPtr state) => s_filterName;

    [UnmanagedCallersOnly]
    private static IntPtr NativeFactoryName(IntPtr state) => s_factoryName;

    [UnmanagedCallersOnly]
    private static IntPtr NativeCreateFilter(IntPtr state, IntPtr context)
    {
        bool full = RocksDbSharp.Native.Instance.rocksdb_compactionfiltercontext_is_full_compaction(context) != 0;
        bool manual = RocksDbSharp.Native.Instance.rocksdb_compactionfiltercontext_is_manual_compaction(context) != 0;
        List<string> contexts = s_nativeContexts!;
        lock (contexts) contexts.Add((full ? "full" : "part") + "/" + (manual ? "manual" : "auto"));
        return NewNativeFilter();
    }

    private static IntPtr NewNativeFilter() => RocksDbSharp.Native.Instance.rocksdb_compactionfilter_create(IntPtr.Zero,
        (IntPtr)(delegate* unmanaged<IntPtr, void>)&NativeDestroy,
        (IntPtr)(delegate* unmanaged<IntPtr, int, byte*, nuint, byte*, nuint, byte**, nuint*, byte*, byte>)&NativeFilter,
        (IntPtr)(delegate* unmanaged<IntPtr, IntPtr>)&NativeFilterName);

    private static IntPtr NewNativeFilterFactory() => RocksDbSharp.Native.Instance.rocksdb_compactionfilterfactory_create(IntPtr.Zero,
        (IntPtr)(delegate* unmanaged<IntPtr, void>)&NativeDestroy,
        (IntPtr)(delegate* unmanaged<IntPtr, IntPtr, IntPtr>)&NativeCreateFilter,
        (IntPtr)(delegate* unmanaged<IntPtr, IntPtr>)&NativeFactoryName);

    // ---- the two engines --------------------------------------------------------------------------------------------

    public enum FilterMode
    {
        None,
        Filter,
        Factory,
    }

    private interface IEngine : IDisposable
    {
        void Put(byte[] k, byte[] v);
        void Merge(byte[] k, byte[] v);
        void Delete(byte[] k);
        void DeleteRange(byte[] a, byte[] b);
        void Flush();
        void Compact(byte[]? a, byte[]? b);
        int Snapshot();
        void ReleaseSnapshots();
        string Get(byte[] k, int snapshot);
        string MultiGet(byte[][] keys, int snapshot);
        string Scan(byte[]? lower, byte[]? upper, bool reverse, int snapshot);
        string FilesPerLevel();
    }

    private static string Outcome(Func<byte[]?> read)
    {
        try
        {
            return S(read()) ?? "<none>";
        }
        catch (RocksDbException e)
        {
            return "error " + e.Code;
        }
        catch (RocksDbSharp.RocksDbException e)
        {
            // "Corruption: ..." / "Invalid argument: ..." -> the code word.
            string m = e.Message;
            return "error " + (m.StartsWith("Corruption", StringComparison.Ordinal) ? "Corruption"
                : m.StartsWith("Invalid argument", StringComparison.Ordinal) ? "InvalidArgument" : m);
        }
    }

    private sealed class NativeEngine : IEngine
    {
        private readonly RocksDbSharp.RocksDb _db;
        private readonly List<RocksDbSharp.Snapshot> _snaps = new();

        public NativeEngine(string path, FilterMode filter, List<string> contexts)
        {
            RocksDbSharp.DbOptions o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetDisableAutoCompactions(1)
                .SetWriteBufferSize(4 << 20);
            RocksDbSharp.MergeOperator mo = NativeMerge();
            s_keepAlive.Add(mo);
            o.SetMergeOperator(mo);
            if (filter == FilterMode.Filter) o.SetCompactionFilter(NewNativeFilter());
            if (filter == FilterMode.Factory)
            {
                s_nativeContexts = contexts;
                o.SetCompactionFilterFactory(NewNativeFilterFactory());
            }
            s_keepAlive.Add(o);
            _db = RocksDbSharp.RocksDb.Open(o, path);
        }

        public void Put(byte[] k, byte[] v) => _db.Put(k, v);
        public void Merge(byte[] k, byte[] v) => _db.Merge(k, v);
        public void Delete(byte[] k) => _db.Remove(k);

        public void DeleteRange(byte[] a, byte[] b)
        {
            using var batch = new RocksDbSharp.WriteBatch();
            batch.DeleteRange(a, (ulong)a.Length, b, (ulong)b.Length);
            _db.Write(batch);
        }

        public void Flush() => _db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
        public string FilesPerLevel() => string.Join(",", Enumerable.Range(0, 7).Select(l => _db.GetProperty("rocksdb.num-files-at-level" + l)));
        public void Compact(byte[]? a, byte[]? b) => _db.CompactRange(a!, b!);

        public int Snapshot()
        {
            _snaps.Add(_db.CreateSnapshot());
            return _snaps.Count;
        }

        public void ReleaseSnapshots()
        {
            foreach (RocksDbSharp.Snapshot s in _snaps) s.Dispose();
            _snaps.Clear();
        }

        private RocksDbSharp.ReadOptions Ro(int snapshot)
        {
            var ro = new RocksDbSharp.ReadOptions();
            if (snapshot > 0) ro.SetSnapshot(_snaps[snapshot - 1]);
            return ro;
        }

        public string Get(byte[] k, int snapshot) => Outcome(() => _db.Get(k, null, Ro(snapshot)));

        public string MultiGet(byte[][] keys, int snapshot)
        {
            try
            {
                KeyValuePair<byte[], byte[]>[] r = _db.MultiGet(keys, null, Ro(snapshot));
                return string.Join(";", r.Select(kv => S(kv.Value) ?? "<none>"));
            }
            catch (RocksDbSharp.RocksDbException e)
            {
                return "error " + e.Message.Split(':')[0];
            }
        }

        public string Scan(byte[]? lower, byte[]? upper, bool reverse, int snapshot)
        {
            RocksDbSharp.ReadOptions ro = Ro(snapshot);
            if (lower is not null) ro.SetIterateLowerBound(lower);
            if (upper is not null) ro.SetIterateUpperBound(upper);
            var sb = new StringBuilder();
            try
            {
                using RocksDbSharp.Iterator it = _db.NewIterator(null, ro);
                if (reverse) it.SeekToLast();
                else it.SeekToFirst();
                for (; it.Valid(); _ = reverse ? it.Prev() : it.Next()) sb.Append(S(it.Key())).Append('=').Append(S(it.Value())).Append(' ');
                // A merge failure ends the iteration with a non-OK status (Valid() false).
                IntPtr err = IntPtr.Zero;
                RocksDbSharp.Native.Instance.rocksdb_iter_get_error(it.Handle, out err);
                if (err != IntPtr.Zero)
                {
                    string m = Marshal.PtrToStringAnsi(err)!;
                    RocksDbSharp.Native.Instance.rocksdb_free(err);
                    return sb.Append("error ").Append(m.Split(':')[0]).ToString();
                }
            }
            catch (RocksDbSharp.RocksDbException e)
            {
                sb.Append("error ").Append(e.Message.Split(':')[0]);
            }
            return sb.ToString();
        }

        public void Dispose()
        {
            ReleaseSnapshots();
            _db.Dispose();
        }
    }

    private sealed class ManagedEngine : IEngine
    {
        private readonly RocksDatabase _db;
        private readonly List<Snapshot> _snaps = new();

        public ManagedEngine(string path, FilterMode filter, List<string> contexts)
        {
            var cfo = new ColumnFamilyOptions { DisableAutoCompactions = true, MergeOperator = new ManagedMerge(), WriteBufferSize = 4 << 20 };
            if (filter == FilterMode.Filter) cfo.CompactionFilter = new ManagedFilter();
            if (filter == FilterMode.Factory) cfo.CompactionFilterFactory = new ManagedFilterFactory(contexts);
            _db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true }, cfo);
        }

        public RocksDatabase Db => _db;

        public void Put(byte[] k, byte[] v) => _db.Put(k, v);
        public void Merge(byte[] k, byte[] v) => _db.Merge(k, v);
        public void Delete(byte[] k) => _db.Delete(k);
        public void DeleteRange(byte[] a, byte[] b) => _db.DeleteRange(a, b);
        public void Flush() => _db.Flush();
        public string FilesPerLevel() => string.Join(",", Enumerable.Range(0, 7).Select(l => _db.GetProperty("rocksdb.num-files-at-level" + l)));
        public void Compact(byte[]? a, byte[]? b) => _db.CompactRange(a, b);

        public int Snapshot()
        {
            _snaps.Add(_db.GetSnapshot());
            return _snaps.Count;
        }

        public void ReleaseSnapshots()
        {
            foreach (Snapshot s in _snaps) s.Dispose();
            _snaps.Clear();
        }

        private ReadOptions Ro(int snapshot) => new() { Snapshot = snapshot > 0 ? _snaps[snapshot - 1] : null };

        public string Get(byte[] k, int snapshot) => Outcome(() => _db.Get(Ro(snapshot), k));

        public string MultiGet(byte[][] keys, int snapshot)
        {
            try
            {
                return string.Join(";", _db.MultiGet(Ro(snapshot), keys).Select(v => S(v) ?? "<none>"));
            }
            catch (RocksDbException e)
            {
                return "error " + e.Code;
            }
        }

        public string Scan(byte[]? lower, byte[]? upper, bool reverse, int snapshot)
        {
            ReadOptions ro = Ro(snapshot);
            if (lower is not null) ro.IterateLowerBound = lower;
            if (upper is not null) ro.IterateUpperBound = upper;
            var sb = new StringBuilder();
            try
            {
                using Iterator it = _db.NewIterator(ro);
                if (reverse) it.SeekToLast();
                else it.SeekToFirst();
                for (; it.Valid; _ = reverse ? Prev(it) : Next(it)) sb.Append(S(it.Key.ToArray())).Append('=').Append(S(it.Value.ToArray())).Append(' ');
            }
            catch (RocksDbException e)
            {
                sb.Append("error ").Append(e.Code);
            }
            return sb.ToString();
        }

        private static bool Next(Iterator it)
        {
            it.Next();
            return true;
        }

        private static bool Prev(Iterator it)
        {
            it.Prev();
            return true;
        }

        public void Dispose()
        {
            ReleaseSnapshots();
            _db.Dispose();
        }
    }

    // ---- the differential -------------------------------------------------------------------------------------------

    private static string Key(int i) => (i % 5 == 0 ? "e" : "k") + i.ToString("D3", CultureInfo.InvariantCulture);

    private static void Compare(IEngine n, IEngine m, int keySpace, int snapshots, Random rnd, string ctx)
    {
        for (int snap = 0; snap <= snapshots; snap++)
        {
            for (int i = 0; i < keySpace; i++)
            {
                byte[] k = B(Key(i));
                Assert.AreEqual(n.Get(k, snap), m.Get(k, snap), ctx + " Get " + Key(i) + " snapshot " + snap);
            }
            byte[][] keys = Enumerable.Range(0, 12).Select(_ => B(Key(rnd.Next(keySpace)))).ToArray();
            Assert.AreEqual(n.MultiGet(keys, snap), m.MultiGet(keys, snap), ctx + " MultiGet snapshot " + snap);
            for (int r = 0; r < 2; r++)
            {
                byte[]? lo = rnd.Next(3) == 0 ? null : B(Key(rnd.Next(keySpace)));
                byte[]? hi = rnd.Next(3) == 0 ? null : B(Key(rnd.Next(keySpace)));
                bool reverse = r == 1;
                Assert.AreEqual(n.Scan(lo, hi, reverse, snap), m.Scan(lo, hi, reverse, snap),
                    ctx + " Scan [" + S(lo) + "," + S(hi) + ") reverse=" + reverse + " snapshot " + snap);
            }
        }
    }

    /// <summary>
    /// Writes a probe key on both engines and compares the outcome; false when both refused it (the DB is stopped by a
    /// background error).
    /// </summary>
    private static bool SameWriteOutcome(IEngine n, IEngine m, string ctx)
    {
        byte[] k = B("zprobe"), v = B("1");
        string nw = Outcome(() =>
        {
            n.Put(k, v);
            return null;
        });
        string mw = Outcome(() =>
        {
            m.Put(k, v);
            return null;
        });
        Assert.AreEqual(nw, mw, ctx);
        return nw == "<none>";
    }

    private static void RunDifferential(int seed, FilterMode filter, bool failingMerges)
    {
        string ctx = "seed=" + seed.ToString(CultureInfo.InvariantCulture) + " filter=" + filter + " failingMerges=" + failingMerges;
        var rnd = new Random(seed);
        using var dirA = new TempDir("a10wc-a");
        using var dirB = new TempDir("a10wc-b");
        var nContexts = new List<string>();
        var mContexts = new List<string>();
        string nPath = dirA.Path, mPath = dirB.Path;
        IEngine n = new NativeEngine(nPath, filter, nContexts);
        IEngine m = new ManagedEngine(mPath, filter, mContexts);
        const int keySpace = 40;
        bool stopped = false;
        try
        {
            int snapshots = 0;
            for (int phase = 0; phase < 6; phase++)
            {
                for (int i = 0; i < 300; i++)
                {
                    int r = rnd.Next(100);
                    int ki = rnd.Next(4) == 0 ? rnd.Next(5) * 7 : rnd.Next(keySpace);
                    byte[] k = B(Key(ki));
                    byte[] v = B("v" + phase + "." + i.ToString(CultureInfo.InvariantCulture) + (rnd.Next(10) == 0 ? "" : rnd.Next(10).ToString(CultureInfo.InvariantCulture)));
                    if (r < 30)
                    {
                        n.Put(k, v);
                        m.Put(k, v);
                    }
                    else if (r < 75)
                    {
                        byte[] op = failingMerges && rnd.Next(60) == 0 ? B("FAIL") : B("o" + i.ToString(CultureInfo.InvariantCulture));
                        n.Merge(k, op);
                        m.Merge(k, op);
                    }
                    else if (r < 90)
                    {
                        n.Delete(k);
                        m.Delete(k);
                    }
                    else if (r < 93)
                    {
                        int s = rnd.Next(keySpace - 1);
                        byte[] a = B(Key(s)), b = B(Key(Math.Min(keySpace - 1, s + 1 + rnd.Next(4))));
                        if (string.CompareOrdinal(S(a), S(b)) >= 0) continue;
                        n.DeleteRange(a, b);
                        m.DeleteRange(a, b);
                    }
                    else if (r < 95 && snapshots < 4)
                    {
                        snapshots = n.Snapshot();
                        Assert.AreEqual(snapshots, m.Snapshot());
                    }
                    else if (r < 97)
                    {
                        string nf = Outcome(() =>
                        {
                            n.Flush();
                            return null;
                        });
                        string mf = Outcome(() =>
                        {
                            m.Flush();
                            return null;
                        });
                        Assert.AreEqual(nf, mf, ctx + " flush outcome");
                        if (!SameWriteOutcome(n, m, ctx + " write after flush"))
                        {
                            stopped = true;
                            break;
                        }
                    }
                    else
                    {
                        byte[]? a = rnd.Next(3) == 0 ? null : B(Key(rnd.Next(keySpace)));
                        byte[]? b = a is null || rnd.Next(3) == 0 ? null : B(Key(rnd.Next(keySpace)));
                        if (a is not null && b is not null && string.CompareOrdinal(S(a), S(b)) > 0) (a, b) = (b, a);
                        // rocksdb_compact_range drops CompactRange's status: a failed compaction is observed through the
                        // write that follows (a Corruption background error in a compaction is unrecoverable).
                        _ = Outcome(() =>
                        {
                            n.Compact(a, b);
                            return null;
                        });
                        _ = Outcome(() =>
                        {
                            m.Compact(a, b);
                            return null;
                        });
                        if (!SameWriteOutcome(n, m, ctx + " write after compaction"))
                        {
                            stopped = true;
                            break;
                        }
                    }
                }
                Compare(n, m, keySpace, snapshots, rnd, ctx + " phase " + phase);
                if (stopped) break;
                if (phase % 2 == 1)
                {
                    // Reopen, swapping the directories: each engine continues on the other's files.
                    n.Dispose();
                    m.Dispose();
                    snapshots = 0;
                    (nPath, mPath) = (mPath, nPath);
                    n = new NativeEngine(nPath, filter, nContexts);
                    m = new ManagedEngine(mPath, filter, mContexts);
                    Compare(n, m, keySpace, 0, rnd, ctx + " reopened (swapped) phase " + phase);
                }
            }
            if (filter == FilterMode.Factory)
            {
                Assert.AreEqual(string.Join(" ", nContexts), string.Join(" ", mContexts), ctx + " filter contexts");
            }
        }
        finally
        {
            n.Dispose();
            m.Dispose();
        }
        if (stopped)
        {
            // The DB stopped on a merge failure: both reopen the same way (the WAL replay merges the chain again).
            Assert.AreEqual(Outcome(() =>
            {
                new NativeEngine(nPath, filter, []).Dispose();
                return null;
            }), Outcome(() =>
            {
                new ManagedEngine(mPath, filter, []).Dispose();
                return null;
            }), ctx + " reopen after the stop");
            return;
        }
        // Every table file records the merge operator by name (rocksdb.merge.operator), the native ones too.
        var props = new List<string>();
        foreach (string path in new[] { nPath, mPath })
        {
            using var e = new ManagedEngine(path, filter, []);
            foreach (RocksDb.Managed.Table.TableProperties p in e.Db.GetPropertiesOfAllTables().Values) props.Add(p.MergeOperatorName);
        }
        Assert.IsTrue(props.All(p => p == MergeName), ctx + " merge operator names: " + string.Join(",", props));
    }

    /// <summary>
    /// A custom merge operator with partial merges (MergeHelper::MergeUntil in flush and compaction,
    /// PartialMergeMulti, snapshots between operands, range tombstones over merge chains), plain reads and reads at
    /// snapshots, cross-engine reopens.
    /// </summary>
    [TestMethod]
    [DataRow(1)]
    [DataRow(2)]
    [DataRow(3)]
    public void CustomMergeOperatorMatchesNative(int seed) => RunDifferential(seed, FilterMode.None, failingMerges: false);

    /// <summary>
    /// A time-seeded run of the differential with a random filter mode and failing merges or not (the seed is in the
    /// failure message).
    /// </summary>
    [TestMethod]
    public void RandomSeedMatchesNative()
    {
        int seed = Environment.TickCount & int.MaxValue;
        RunDifferential(seed, (FilterMode)(seed % 3), failingMerges: seed / 3 % 2 == 0);
    }

    /// <summary>
    /// The same workload with a compaction filter (CompactionIterator::InvokeFilterIfNeeded: remove, change value,
    /// change to an empty value; merge operands and merge results are not filtered; snapshots), plain and from a
    /// factory, whose CompactionFilter::Context (is_full_compaction, is_manual_compaction) must match native's for every
    /// compaction.
    /// </summary>
    [TestMethod]
    [DataRow(4, FilterMode.Filter)]
    [DataRow(5, FilterMode.Filter)]
    [DataRow(6, FilterMode.Factory)]
    [DataRow(7, FilterMode.Factory)]
    public void CompactionFilterMatchesNative(int seed, FilterMode filter) => RunDifferential(seed, filter, failingMerges: false);

    /// <summary>
    /// A merge operator that fails (returns false) for some chains: reads of those keys fail with Corruption on both
    /// engines (GetContext / DBIter, MergeHelper::TimedFullMerge: Status::Corruption(kMergeOperatorFailed)); the reads of
    /// the other keys agree; a compaction that has to merge such a chain fails on both.
    /// </summary>
    [TestMethod]
    [DataRow(8)]
    [DataRow(9)]
    public void FailingMergeOperatorMatchesNative(int seed) => RunDifferential(seed, FilterMode.None, failingMerges: true);

    /// <summary>
    /// A merge chain whose full merge fails in a flush (BuildTable's CompactionIterator, MergeHelper::MergeUntil over a
    /// Delete): what Flush / CompactRange / later writes / reads return on both engines.
    /// </summary>
    [TestMethod]
    [DataRow("put")]
    [DataRow("delete")]
    [DataRow("none")]
    public void MergeFailureInFlushMatchesNative(string baseKind)
    {
        using var dirN = new TempDir("a10mf-n");
        using var dirM = new TempDir("a10mf-m");
        var outcomes = new List<string>[2];
        for (int e = 0; e < 2; e++)
        {
            var o = new List<string>();
            outcomes[e] = o;
            IEngine db = e == 0 ? new NativeEngine(dirN.Path, FilterMode.None, []) : new ManagedEngine(dirM.Path, FilterMode.None, []);
            try
            {
                db.Put(B("a"), B("1"));
                if (baseKind == "put") db.Put(B("k"), B("base"));
                db.Merge(B("k"), B("o1"));
                if (baseKind == "delete") db.Delete(B("k"));
                db.Merge(B("k"), B("FAIL"));
                o.Add("get " + db.Get(B("k"), 0));
                o.Add("flush " + Outcome(() =>
                {
                    db.Flush();
                    return null;
                }));
                o.Add("put " + Outcome(() =>
                {
                    db.Put(B("b"), B("2"));
                    return null;
                }));
                // rocksdb_compact_range drops the status: the write that follows shows whether the DB stopped.
                _ = Outcome(() =>
                {
                    db.Compact(null, null);
                    return null;
                });
                o.Add("put after compact " + Outcome(() =>
                {
                    db.Put(B("c"), B("3"));
                    return null;
                }));
                o.Add("files " + db.FilesPerLevel());
                o.Add("get a " + db.Get(B("a"), 0));
                o.Add("get k " + db.Get(B("k"), 0));
                o.Add("scan " + db.Scan(null, null, false, 0));
            }
            finally
            {
                db.Dispose();
            }
            // Reopen: the WAL still holds the chain.
            try
            {
                db = e == 0 ? new NativeEngine(dirN.Path, FilterMode.None, []) : new ManagedEngine(dirM.Path, FilterMode.None, []);
            }
            catch (Exception ex) when (ex is RocksDbException or RocksDbSharp.RocksDbException)
            {
                o.Add("reopen " + Outcome(() => throw ex));
                continue;
            }
            try
            {
                o.Add("reopened get a " + db.Get(B("a"), 0));
                o.Add("reopened get b " + db.Get(B("b"), 0));
            }
            finally
            {
                db.Dispose();
            }
        }
        Assert.AreEqual(string.Join("\n", outcomes[0]), string.Join("\n", outcomes[1]), baseKind);
    }
}
