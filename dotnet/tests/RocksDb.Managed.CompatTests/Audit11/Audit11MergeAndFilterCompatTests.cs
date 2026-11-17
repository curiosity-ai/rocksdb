using System.Buffers;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Monitoring;
using RocksDb.Managed.Table;
using RocksDb.Managed.Table.Filters;

namespace RocksDb.Managed.CompatTests.Audit11;

/// <summary>
/// Audit #11 against native RocksDB 11.12 (rocksdb-sharp C API). max_successive_merges / strict_max_successive_merges
/// (rocksdb_options_set_max_successive_merges / rocksdb_options_set_strict_max_successive_merges) with the same merge
/// operator on both engines (rocksdb_mergeoperator_create): its result shows which operands were merged together and on
/// which base, so where each engine folded merges into a value at write time (MemTableInserter::MergeCF) is visible in
/// every read. Filter policies: a custom policy with the built-in compatibility name reads native's built-in filters and
/// native reads its filters. MergeOperator::ShouldMerge and CompactionFilter::Context::input_table_properties have no C
/// API (rocksdb_mergeoperator_create has no should_merge callback; rocksdb_compactionfiltercontext_t only exposes
/// is_full_compaction / is_manual_compaction), so they are pinned by the unit tests against the C++ source.
/// </summary>
[TestClass]
[DoNotParallelize] // the native merge operator counts its calls in a static
public sealed class Audit11MergeAndFilterCompatTests
{
    private static readonly RocksDbSharp.Native N = RocksDbSharp.Native.Instance;
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    private static byte[] B(string s) => Encoding.UTF8.GetBytes(s);

    private static string? S(byte[]? b) => b is null ? null : Encoding.UTF8.GetString(b);

    private const string MergeName = "Audit11Bracket";

    // ---- the merge operator, once per engine ------------------------------------------------------------------------

    /// <summary>"[" + (existing or "_") + ":" + operands joined by "," + "]"; fails for an operand "corrupted".</summary>
    private static byte[]? BracketImpl(bool hasExisting, ReadOnlySpan<byte> existing, int count, Func<int, byte[]> operand)
    {
        var sb = new StringBuilder("[").Append(hasExisting ? Encoding.UTF8.GetString(existing) : "_").Append(':');
        for (int i = 0; i < count; i++)
        {
            string op = Encoding.UTF8.GetString(operand(i));
            if (op == "corrupted") return null;
            if (i > 0) sb.Append(',');
            sb.Append(op);
        }
        return B(sb.Append(']').ToString());
    }

    private sealed class ManagedBracket : MergeOperator
    {
        public int Calls;

        public override string Name => MergeName;

        public override bool FullMerge(ReadOnlySpan<byte> key, bool hasExistingValue, ReadOnlySpan<byte> existingValue, in MergeOperands operands,
            IBufferWriter<byte> result)
        {
            Interlocked.Increment(ref Calls);
            var ops = new byte[operands.Count][];
            for (int i = 0; i < ops.Length; i++) ops[i] = operands[i].ToArray();
            byte[]? r = BracketImpl(hasExistingValue, existingValue, ops.Length, i => ops[i]);
            if (r is null) return false;
            result.Write(r);
            return true;
        }
    }

    private static int s_nativeCalls;

    /// <summary>The same operator for native; no partial merge (success = false), like the managed default.</summary>
    private static RocksDbSharp.MergeOperator NativeBracket() => RocksDbSharp.MergeOperators.Create(
        MergeName,
        static (ReadOnlySpan<byte> key, RocksDbSharp.MergeOperators.OperandsEnumerator operands, out bool success) =>
        {
            success = false;
            return [];
        },
        static (ReadOnlySpan<byte> key, bool hasExisting, ReadOnlySpan<byte> existing, RocksDbSharp.MergeOperators.OperandsEnumerator operands,
            out bool success) =>
        {
            Interlocked.Increment(ref s_nativeCalls);
            var ops = new byte[operands.Count][];
            for (int i = 0; i < ops.Length; i++) ops[i] = operands.Get(i).ToArray();
            byte[]? r = BracketImpl(hasExisting, existing, ops.Length, i => ops[i]);
            success = r is not null;
            return r ?? [];
        });

    // ---- the two engines --------------------------------------------------------------------------------------------

    private interface IEngine : IDisposable
    {
        void Put(byte[] k, byte[] v);
        void Merge(byte[] k, byte[] v);
        void MergeBatch(byte[] k, byte[][] values);
        void Delete(byte[] k);
        void Flush();
        void Compact();
        string Get(byte[] k);
        int MergeCalls { get; }
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
            return "error " + (e.Message.StartsWith("Corruption", StringComparison.Ordinal) ? "Corruption" : e.Message);
        }
    }

    private sealed class NativeEngine : IEngine
    {
        private readonly RocksDbSharp.RocksDb _db;

        public NativeEngine(string path, ulong limit, bool strict)
        {
            RocksDbSharp.DbOptions o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetDisableAutoCompactions(1);
            N.rocksdb_options_set_max_successive_merges(o.Handle, (UIntPtr)limit);
            N.rocksdb_options_set_strict_max_successive_merges(o.Handle, strict ? (byte)1 : (byte)0);
            RocksDbSharp.MergeOperator mo = NativeBracket();
            s_keepAlive.Add(mo);
            o.SetMergeOperator(mo);
            s_keepAlive.Add(o);
            _db = RocksDbSharp.RocksDb.Open(o, path);
        }

        public int MergeCalls => Volatile.Read(ref s_nativeCalls);
        public void Put(byte[] k, byte[] v) => _db.Put(k, v);
        public void Merge(byte[] k, byte[] v) => _db.Merge(k, v);

        public void MergeBatch(byte[] k, byte[][] values)
        {
            using var b = new RocksDbSharp.WriteBatch();
            foreach (byte[] v in values) b.Merge(k, v);
            _db.Write(b);
        }

        public void Delete(byte[] k) => _db.Remove(k);
        public void Flush() => _db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
        public void Compact() => _db.CompactRange(Array.Empty<byte>(), null);
        public string Get(byte[] k) => Outcome(() => _db.Get(k));
        public void Dispose() => _db.Dispose();
    }

    private sealed class ManagedEngine : IEngine
    {
        private readonly RocksDatabase _db;
        private readonly ManagedBracket _op = new();

        public ManagedEngine(string path, ulong limit, bool strict) =>
            _db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true }, new ColumnFamilyOptions
            {
                MergeOperator = _op, DisableAutoCompactions = true, MaxSuccessiveMerges = limit, StrictMaxSuccessiveMerges = strict,
            });

        public int MergeCalls => Volatile.Read(ref _op.Calls);
        public void Put(byte[] k, byte[] v) => _db.Put(k, v);
        public void Merge(byte[] k, byte[] v) => _db.Merge(k, v);

        public void MergeBatch(byte[] k, byte[][] values)
        {
            using var b = new Db.WriteBatch();
            foreach (byte[] v in values) b.Merge(k, v);
            _db.Write(b);
        }

        public void Delete(byte[] k) => _db.Delete(k);
        public void Flush() => _db.Flush();
        public void Compact() => _db.CompactRange();
        public string Get(byte[] k) => Outcome(() => _db.Get(k));
        public void Dispose() => _db.Dispose();
    }

    private static int Seed(int fallback) =>
        int.TryParse(Environment.GetEnvironmentVariable("AUDIT11_SEED"), NumberStyles.Integer, CultureInfo.InvariantCulture, out int s) ? s : fallback;

    /// <summary>
    /// A seeded random workload (puts, merges, merge batches, deletes, flushes, manual compactions, point reads that load
    /// data blocks into the block cache, reopens that swap the engines' directories) runs on native and on the port with
    /// the same max_successive_merges / strict_max_successive_merges; every read must agree, including where merges were
    /// folded at write time (read at the batch's sequence, kBlockCacheTier unless strict) and the number of merge
    /// operator calls (each fold is a read merge plus the fold merge).
    /// </summary>
    [TestMethod]
    [DataRow(1UL, false)]
    [DataRow(2UL, false)]
    [DataRow(3UL, false)]
    [DataRow(2UL, true)]
    [DataRow(3UL, true)]
    public void MaxSuccessiveMerges_RandomWorkloadMatchesNative(ulong limit, bool strict)
    {
        int seed = Seed(1100 + (int)limit * 10 + (strict ? 1 : 0));
        Console.WriteLine("AUDIT11_SEED=" + seed.ToString(CultureInfo.InvariantCulture));
        var rnd = new Random(seed);
        using var dirA = new TempDir("a11-n");
        using var dirB = new TempDir("a11-m");
        string nativeDir = dirA.Path, managedDir = dirB.Path;
        IEngine native = new NativeEngine(nativeDir, limit, strict);
        IEngine managed = new ManagedEngine(managedDir, limit, strict);
        try
        {
            string[] keys = ["k0", "k1", "k2", "k3", "k4"];
            int calls0Native = native.MergeCalls, calls0Managed = managed.MergeCalls;
            for (int step = 0; step < 400; step++)
            {
                byte[] k = B(keys[rnd.Next(keys.Length)]);
                int r = rnd.Next(100);
                string what;
                if (r < 50)
                {
                    byte[] v = B("m" + step.ToString(CultureInfo.InvariantCulture));
                    native.Merge(k, v);
                    managed.Merge(k, v);
                    what = "merge";
                }
                else if (r < 58)
                {
                    byte[][] vs = new byte[1 + rnd.Next(4)][];
                    for (int i = 0; i < vs.Length; i++) vs[i] = B("b" + step.ToString(CultureInfo.InvariantCulture) + "." + i.ToString(CultureInfo.InvariantCulture));
                    native.MergeBatch(k, vs);
                    managed.MergeBatch(k, vs);
                    what = "batch";
                }
                else if (r < 66)
                {
                    byte[] v = B("p" + step.ToString(CultureInfo.InvariantCulture));
                    native.Put(k, v);
                    managed.Put(k, v);
                    what = "put";
                }
                else if (r < 70)
                {
                    native.Delete(k);
                    managed.Delete(k);
                    what = "delete";
                }
                else if (r < 85)
                {
                    Assert.AreEqual(native.Get(k), managed.Get(k), "step " + step + " get " + S(k));
                    what = "get";
                }
                else if (r < 92)
                {
                    native.Flush();
                    managed.Flush();
                    what = "flush";
                }
                else if (r < 95)
                {
                    native.Compact();
                    managed.Compact();
                    what = "compact";
                }
                else
                {
                    foreach (string key in keys) Assert.AreEqual(native.Get(B(key)), managed.Get(B(key)), "before reopen at step " + step + " " + key);
                    native.Dispose();
                    managed.Dispose();
                    native = null!;
                    managed = null!;
                    (nativeDir, managedDir) = (managedDir, nativeDir); // each engine continues on the other's files
                    native = new NativeEngine(nativeDir, limit, strict);
                    managed = new ManagedEngine(managedDir, limit, strict);
                    calls0Native = native.MergeCalls;
                    calls0Managed = managed.MergeCalls;
                    what = "reopen";
                }
                Assert.AreEqual(native.MergeCalls - calls0Native, managed.MergeCalls - calls0Managed, "merge operator calls after step " + step + " (" + what + ")");
            }
            foreach (string key in keys) Assert.AreEqual(native.Get(B(key)), managed.Get(B(key)), "final " + key);
        }
        finally
        {
            native?.Dispose();
            managed?.Dispose();
        }
    }

    /// <summary>
    /// The scripted cases of MemTableInserter::MergeCF on both engines: folding inside one write batch (read at the
    /// batch's sequence), a base value in a table file whose block is not cached (Incomplete without strict: the operand
    /// is stored) and then cached by a read (folded), and a merge that fails at the limit (DBMergeOperatorTest
    /// MergeErrorOnWrite: the delta is stored, the read reports Corruption).
    /// </summary>
    [TestMethod]
    public void MaxSuccessiveMerges_ScriptedCasesMatchNative()
    {
        foreach (bool strict in new[] { false, true })
        {
            using var dirA = new TempDir("a11s-n");
            using var dirB = new TempDir("a11s-m");
            var results = new List<string>[2];
            for (int e = 0; e < 2; e++)
            {
                string dir = e == 0 ? dirA.Path : dirB.Path;
                var log = results[e] = [];
                IEngine db = e == 0 ? new NativeEngine(dir, 2, strict) : new ManagedEngine(dir, 2, strict);
                try
                {
                    db.Put(B("t"), B("base"));
                    db.Flush();
                }
                finally
                {
                    db.Dispose();
                }
                db = e == 0 ? new NativeEngine(dir, 2, strict) : new ManagedEngine(dir, 2, strict);
                try
                {
                    int c0 = db.MergeCalls;
                    db.Merge(B("t"), B("1"));
                    db.Merge(B("t"), B("2"));
                    db.Merge(B("t"), B("3")); // base not cached: folded only with strict
                    log.Add("calls " + (db.MergeCalls - c0));
                    log.Add("t " + db.Get(B("t")));
                    db.Merge(B("t"), B("4")); // the read above cached the block
                    log.Add("t " + db.Get(B("t")));
                    // Merges of one batch are read at the batch's sequence (no base value: the lookup also reaches the
                    // table file, whose block is now cached).
                    db.MergeBatch(B("batch"), [B("1"), B("2"), B("3"), B("4"), B("5")]);
                    log.Add("batch " + db.Get(B("batch")));
                    db.Merge(B("bad"), B("v1"));
                    db.Merge(B("bad"), B("v2"));
                    db.Merge(B("bad"), B("corrupted"));
                    log.Add("bad " + db.Get(B("bad")));
                    db.Put(B("bad"), B("ok"));
                    log.Add("bad " + db.Get(B("bad")));
                    log.Add("calls " + (db.MergeCalls - c0));
                }
                finally
                {
                    db.Dispose();
                }
            }
            CollectionAssert.AreEqual(results[0], results[1], "strict=" + strict + "\nnative: " + string.Join(" | ", results[0]) + "\nmanaged: " +
                string.Join(" | ", results[1]));
            Console.WriteLine("strict=" + strict + ": " + string.Join(" | ", results[1]));
        }
    }

    /// <summary>
    /// The OPTIONS file the port writes carries max_successive_merges / strict_max_successive_merges as native reads them
    /// (rocksdb_load_latest_options).
    /// </summary>
    [TestMethod]
    public void MaxSuccessiveMerges_OptionsFileReadByNative()
    {
        using var dir = new TempDir("a11o");
        using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions { CreateIfMissing = true }, new ColumnFamilyOptions
        {
            MergeOperator = MergeOperator.Put, MaxSuccessiveMerges = 7, StrictMaxSuccessiveMerges = true,
        }))
        {
            db.Merge(B("k"), B("v"));
        }
        IntPtr path = Marshal.StringToHGlobalAnsi(dir.Path);
        IntPtr env = N.rocksdb_create_default_env();
        IntPtr cache = N.rocksdb_cache_create_lru((UIntPtr)(1 << 20));
        IntPtr outs = Marshal.AllocHGlobal(IntPtr.Size * 4);
        try
        {
            for (int i = 0; i < 4; i++) Marshal.WriteIntPtr(outs, i * IntPtr.Size, IntPtr.Zero);
            IntPtr dbOptsOut = outs, numOut = outs + IntPtr.Size, namesOut = outs + 2 * IntPtr.Size, cfOptsOut = outs + 3 * IntPtr.Size;
            N.rocksdb_load_latest_options(path, env, 1, cache, dbOptsOut, numOut, namesOut, cfOptsOut);
            int num = (int)Marshal.ReadIntPtr(numOut);
            Assert.AreEqual(1, num);
            IntPtr cfOpts = Marshal.ReadIntPtr(Marshal.ReadIntPtr(cfOptsOut));
            Assert.AreEqual(7UL, (ulong)N.rocksdb_options_get_max_successive_merges(cfOpts));
            Assert.AreEqual(1, N.rocksdb_options_get_strict_max_successive_merges(cfOpts));
            N.rocksdb_load_latest_options_destroy(Marshal.ReadIntPtr(dbOptsOut), Marshal.ReadIntPtr(namesOut), Marshal.ReadIntPtr(cfOptsOut), (UIntPtr)num);
        }
        finally
        {
            Marshal.FreeHGlobal(outs);
            Marshal.FreeHGlobal(path);
            N.rocksdb_cache_destroy(cache);
            N.rocksdb_env_destroy(env);
        }
    }

    // ---- filter policies ------------------------------------------------------------------------------------------

    /// <summary>Upstream's LevelAndStyleCustomFilterPolicy shape: the built-in name, built-in builders and readers.</summary>
    private sealed class DelegatingPolicy : FilterPolicy
    {
        private readonly BloomFilterPolicy _l0 = new(6);
        private readonly BloomFilterPolicy _otherwise = new(10);
        public int Readers;

        public override string Name => "LevelAndStyleCustomFilterPolicy";
        public override string CompatibilityName => _l0.CompatibilityName;
        public override IFilterBitsBuilder CreateBuilder(int formatVersion) => _otherwise.CreateBuilder(formatVersion);

        public override IFilterBitsBuilder CreateBuilder(in FilterBuildingContext context) =>
            context.LevelAtCreation == 0 ? _l0.CreateBuilder(context) : _otherwise.CreateBuilder(context);

        public override IFilterBitsReader CreateReader(ReadOnlyMemory<byte> contents)
        {
            Interlocked.Increment(ref Readers);
            return _otherwise.CreateReader(contents);
        }
    }

    private static byte[] K(int i) => B("key" + i.ToString("D5", CultureInfo.InvariantCulture));

    private static ulong NativeTicker(string statistics, string name)
    {
        foreach (string line in statistics.Split('\n'))
        {
            if (!line.StartsWith(name + " COUNT : ", StringComparison.Ordinal)) continue;
            return ulong.Parse(line.AsSpan(name.Length + 9).Trim(), CultureInfo.InvariantCulture);
        }
        Assert.Fail(name + " not in the statistics");
        return 0;
    }

    /// <summary>
    /// Native writes built-in Bloom filters (rocksdb_filterpolicy_create_bloom_full); the port reads them through a
    /// custom policy with the built-in compatibility name (its own reader, delegating to a built-in one), with the
    /// filters doing their job; then the port writes with that policy and native reads the files with its built-in Bloom
    /// policy, again with the filters in use (rocksdb.bloom.filter.useful).
    /// </summary>
    [TestMethod]
    public void FilterPolicy_DelegatingWrapperInteroperatesWithNativeBloom()
    {
        using var dir = new TempDir("a11f");
        const int Count = 2000;
        RocksDbSharp.DbOptions NativeOptions()
        {
            var t = new RocksDbSharp.BlockBasedTableOptions().SetFilterPolicy(RocksDbSharp.BloomFilterPolicy.Create(10, false));
            var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetDisableAutoCompactions(1).SetBlockBasedTableFactory(t).EnableStatistics();
            s_keepAlive.Add(t);
            s_keepAlive.Add(o);
            return o;
        }
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(NativeOptions(), dir.Path))
        {
            for (int i = 0; i < Count; i += 2) db.Put(K(i), B("n" + i));
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
        }
        var policy = new DelegatingPolicy();
        var stats = new Statistics();
        ColumnFamilyOptions Cfo() => new()
        {
            DisableAutoCompactions = true,
            TableOptions = new BlockBasedTableOptions { FilterPolicy = policy },
        };
        using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions { Statistics = stats }, Cfo()))
        {
            for (int i = 0; i < Count; i++) Assert.AreEqual(i % 2 == 0 ? "n" + i : null, S(db.Get(K(i))));
            Assert.IsTrue(policy.Readers > 0);
            Assert.IsTrue(stats.GetTickerCount(Ticker.BloomFilterUseful) > Count / 4, "managed useful " + stats.GetTickerCount(Ticker.BloomFilterUseful));
            for (int i = 1; i < Count; i += 4) db.Put(K(i), B("m" + i));
            db.Flush();
            db.CompactRange(new CompactRangeOptions { ChangeLevel = true, TargetLevel = 1 }, null, null);
        }
        RocksDbSharp.DbOptions no = NativeOptions();
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(no, dir.Path))
        {
            for (int i = 0; i < Count; i++)
            {
                string? expected = i % 2 == 0 ? "n" + i : i % 4 == 1 ? "m" + i : null;
                Assert.AreEqual(expected, S(db.Get(K(i))), "key " + i);
            }
            ulong useful = NativeTicker(no.GetStatisticsString(), "rocksdb.bloom.filter.useful");
            Assert.IsTrue(useful > Count / 8, "native useful " + useful);
        }
    }
}
