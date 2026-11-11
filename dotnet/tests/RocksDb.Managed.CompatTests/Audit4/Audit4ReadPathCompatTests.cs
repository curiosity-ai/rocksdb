using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;

namespace RocksDb.Managed.CompatTests.Audit4;

/// <summary>
/// Audit #4, read path: the same random workload (puts, deletes, merges with the order-sensitive string append
/// operator, range deletions, flushes, partial manual compactions with small output files, snapshots) is applied to
/// native RocksDB and to the managed port, each in its own directory with auto compactions off, so data ends up spread
/// over the memtable, several L0 files and several files of the output level, with range tombstones truncated at file
/// boundaries. At checkpoints every key is read at the latest sequence and at every live snapshot through Get,
/// MultiGet (unsorted, with duplicates), full forward and reverse scans, and random Seek / SeekForPrev positions followed
/// by a walk mixing Next and Prev; the two engines must agree on every result (DBImpl::GetImpl, Version::Get /
/// MultiGet, GetContext::SaveValue, MergingIterator + DBIter with range tombstones). AUDIT4_READ_SEED=n replays a run.
/// </summary>
[TestClass]
public sealed unsafe class Audit4ReadPathCompatTests
{
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();
    private static readonly Lazy<IntPtr> s_library = new(LoadLibrary);

    private static IntPtr LoadLibrary()
    {
        _ = RocksDbSharp.Native.Instance; // make sure rocksdb-sharp loaded its library
        foreach (string line in File.ReadLines("/proc/self/maps"))
        {
            int slash = line.IndexOf('/', StringComparison.Ordinal);
            if (slash < 0) continue;
            string path = line[slash..];
            if (Path.GetFileName(path).StartsWith("librocksdb", StringComparison.Ordinal)) return NativeLibrary.Load(path);
        }
        throw new InvalidOperationException("librocksdb is not loaded");
    }

    private static IntPtr Export(string name) => NativeLibrary.GetExport(s_library.Value, name);

    private const int KeyCount = 96;

    private static byte[] B(string s) => Encoding.UTF8.GetBytes(s);

    private static byte[] Key(int i) => B("k" + i.ToString("D4", CultureInfo.InvariantCulture));

    private static string Str(ReadOnlySpan<byte> b) => Encoding.UTF8.GetString(b);

    // ---- the two engines ---------------------------------------------------------------------------------

    private interface IEngine : IDisposable
    {
        void Put(byte[] key, byte[] value);
        void Merge(byte[] key, byte[] operand);
        void Delete(byte[] key);
        void DeleteRange(byte[] begin, byte[] end);
        void Flush();
        void CompactRange(byte[]? begin, byte[]? end);
        int TakeSnapshot();
        void ReleaseSnapshot(int id);
        /// <summary>Appends every read result at <paramref name="snapshot"/> (-1 = latest) to <paramref name="log"/>.</summary>
        void ReadAll(int snapshot, int walkSeed, bool ignoreRangeDeletions, List<string> log);
    }

    /// <summary>The reads of one view, written against a minimal iterator / lookup surface so both engines run the same code.</summary>
    private abstract class EngineBase
    {
        protected abstract string? Get(int snapshot, bool ignoreRangeDeletions, byte[] key);
        protected abstract string?[] MultiGet(int snapshot, bool ignoreRangeDeletions, byte[][] keys);
        protected abstract IIter NewIterator(int snapshot);

        protected interface IIter : IDisposable
        {
            bool Valid { get; }
            string Key { get; }
            string Value { get; }
            void SeekToFirst();
            void SeekToLast();
            void Seek(byte[] key);
            void SeekForPrev(byte[] key);
            void Next();
            void Prev();
        }

        public void ReadAll(int snapshot, int walkSeed, bool ignoreRangeDeletions, List<string> log)
        {
            string view = snapshot < 0 ? "latest" : "snap" + snapshot;
            if (ignoreRangeDeletions) view += "+ignore_range_del";
            var rnd = new Random(walkSeed);
            // Get
            for (int i = 0; i < KeyCount; i++) log.Add(view + " get " + i + " = " + (Get(snapshot, ignoreRangeDeletions, Key(i)) ?? "<null>"));
            // MultiGet: unsorted, with duplicates and a missing key
            var keys = new byte[KeyCount + 24][];
            var which = new int[keys.Length];
            for (int i = 0; i < keys.Length; i++)
            {
                which[i] = i % 17 == 16 ? 5000 : rnd.Next(KeyCount);
                keys[i] = Key(which[i]);
            }
            string?[] values = MultiGet(snapshot, ignoreRangeDeletions, keys);
            for (int i = 0; i < keys.Length; i++) log.Add(view + " mget[" + i + "] " + which[i] + " = " + (values[i] ?? "<null>"));
            if (ignoreRangeDeletions) return; // iterators with ignore_range_deletions are not part of this comparison
            using IIter it = NewIterator(snapshot);
            // full scans
            var sb = new StringBuilder();
            for (it.SeekToFirst(); it.Valid; it.Next()) sb.Append(it.Key).Append('=').Append(it.Value).Append(';');
            log.Add(view + " forward " + sb);
            sb.Clear();
            for (it.SeekToLast(); it.Valid; it.Prev()) sb.Append(it.Key).Append('=').Append(it.Value).Append(';');
            log.Add(view + " reverse " + sb);
            // random positions and walks mixing directions
            for (int round = 0; round < 24; round++)
            {
                int k = rnd.Next(-1, KeyCount + 1);
                byte[] target = k < 0 ? B("a") : k >= KeyCount ? B("z") : Key(k);
                bool forPrev = rnd.Next(2) == 0;
                sb.Clear();
                sb.Append(forPrev ? "seekforprev " : "seek ").Append(Str(target)).Append(':');
                if (forPrev) it.SeekForPrev(target); else it.Seek(target);
                for (int step = 0; step < 10; step++)
                {
                    if (!it.Valid)
                    {
                        sb.Append(" <end>");
                        break;
                    }
                    sb.Append(' ').Append(it.Key).Append('=').Append(it.Value);
                    if (rnd.Next(3) == 0)
                    {
                        sb.Append(" P");
                        it.Prev();
                    }
                    else
                    {
                        sb.Append(" N");
                        it.Next();
                    }
                }
                log.Add(view + " " + sb);
            }
        }
    }

    private sealed class NativeEngine : EngineBase, IEngine
    {
        private readonly RocksDbSharp.RocksDb _db;
        private readonly Dictionary<int, RocksDbSharp.Snapshot> _snapshots = new();
        private int _nextSnapshot;

        public NativeEngine(string path)
        {
            var baseOptions = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
            var options = new RocksDbSharp.DbOptions();
            s_keepAlive.Add(baseOptions);
            s_keepAlive.Add(options);
            IntPtr err = IntPtr.Zero;
            byte[] text = B("merge_operator=stringappend;disable_auto_compactions=true;target_file_size_base=2048;compression=kNoCompression;" +
                            "level_compaction_dynamic_level_bytes=false\0");
            fixed (byte* p = text)
            {
                ((delegate* unmanaged<IntPtr, byte*, IntPtr, IntPtr*, void>)Export("rocksdb_get_options_from_string"))(baseOptions.Handle, p, options.Handle, &err);
            }
            if (err != IntPtr.Zero) Assert.Fail("rocksdb_get_options_from_string: " + Marshal.PtrToStringUTF8(err));
            _db = RocksDbSharp.RocksDb.Open(options, path);
        }

        public void Put(byte[] key, byte[] value) => _db.Put(key, value);
        public void Merge(byte[] key, byte[] operand) => _db.Merge(key, operand);
        public void Delete(byte[] key) => _db.Remove(key);

        public void DeleteRange(byte[] begin, byte[] end)
        {
            using var batch = new RocksDbSharp.WriteBatch();
            batch.DeleteRange(begin, (ulong)begin.Length, end, (ulong)end.Length);
            _db.Write(batch);
        }

        public void Flush() => _db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
        public void CompactRange(byte[]? begin, byte[]? end) => _db.CompactRange(begin, end);

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

        private RocksDbSharp.ReadOptions Options(int snapshot, bool ignoreRangeDeletions)
        {
            var ro = new RocksDbSharp.ReadOptions();
            if (snapshot >= 0) ro.SetSnapshot(_snapshots[snapshot]);
            if (ignoreRangeDeletions)
            {
                ((delegate* unmanaged<IntPtr, byte, void>)Export("rocksdb_readoptions_set_ignore_range_deletions"))(ro.Handle, 1);
            }
            s_keepAlive.Add(ro);
            return ro;
        }

        /// <summary>
        /// Number of lookups where native's Get disagreed with native's own MultiGet of the same key at the same view
        /// (see <see cref="UpstreamGetStopsAtStaleTombstoneWhenUserKeySpansTwoFiles"/>); the MultiGet result is the
        /// reference then.
        /// </summary>
        public int GetAnomalies { get; private set; }

        /// <summary>The raw rocksdb_get result at the latest sequence.</summary>
        public string? RawGet(byte[] key)
        {
            byte[]? v = _db.Get(key, null, Options(-1, false));
            return v is null ? null : Str(v);
        }

        protected override string? Get(int snapshot, bool ignoreRangeDeletions, byte[] key)
        {
            byte[]? raw = _db.Get(key, null, Options(snapshot, ignoreRangeDeletions));
            string? v = raw is null ? null : Str(raw);
            if (ignoreRangeDeletions) return v;
            string? viaMultiGet = MultiGet(snapshot, false, [key])[0];
            if (viaMultiGet != v) GetAnomalies++;
            return viaMultiGet;
        }

        /// <summary>rocksdb_multi_get: per-key value or null; any per-key error fails the test.</summary>
        protected override string?[] MultiGet(int snapshot, bool ignoreRangeDeletions, byte[][] keys)
        {
            int n = keys.Length;
            var pins = new GCHandle[n];
            var kp = new IntPtr[n];
            var ks = new UIntPtr[n];
            var vals = new IntPtr[n];
            var vs = new UIntPtr[n];
            var errs = new IntPtr[n];
            try
            {
                for (int i = 0; i < n; i++)
                {
                    pins[i] = GCHandle.Alloc(keys[i], GCHandleType.Pinned);
                    kp[i] = pins[i].AddrOfPinnedObject();
                    ks[i] = (UIntPtr)keys[i].Length;
                }
                RocksDbSharp.Native.Instance.rocksdb_multi_get(_db.Handle, Options(snapshot, ignoreRangeDeletions).Handle, (UIntPtr)n, kp, ks, vals, vs, errs);
            }
            finally
            {
                foreach (GCHandle h in pins) if (h.IsAllocated) h.Free();
            }
            var result = new string?[n];
            for (int i = 0; i < n; i++)
            {
                if (errs[i] != IntPtr.Zero) Assert.Fail("native multi_get: " + Marshal.PtrToStringUTF8(errs[i]));
                if (vals[i] == IntPtr.Zero) continue;
                var v = new byte[(int)vs[i]];
                Marshal.Copy(vals[i], v, 0, v.Length);
                RocksDbSharp.Native.Instance.rocksdb_free(vals[i]);
                result[i] = Str(v);
            }
            return result;
        }

        protected override IIter NewIterator(int snapshot) => new Iter(_db.NewIterator(null, Options(snapshot, false)));

        private sealed class Iter(RocksDbSharp.Iterator it) : IIter
        {
            public bool Valid => it.Valid();
            public string Key => Str(it.Key());
            public string Value => Str(it.Value());
            public void SeekToFirst() => it.SeekToFirst();
            public void SeekToLast() => it.SeekToLast();
            public void Seek(byte[] key) => it.Seek(key);
            public void SeekForPrev(byte[] key) => it.SeekForPrev(key);
            public void Next() => it.Next();
            public void Prev() => it.Prev();
            public void Dispose() => it.Dispose();
        }

        public void Dispose()
        {
            foreach (RocksDbSharp.Snapshot s in _snapshots.Values) s.Dispose();
            _snapshots.Clear();
            _db.Dispose();
        }
    }

    private sealed class ManagedEngine : EngineBase, IEngine
    {
        private readonly RocksDatabase _db;
        private readonly Dictionary<int, Snapshot> _snapshots = new();
        private int _nextSnapshot;
        private readonly bool _async;

        public ManagedEngine(string path, bool async)
        {
            _async = async;
            _db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true },
                [new ColumnFamilyDescriptor("default", new ColumnFamilyOptions
                {
                    MergeOperator = new StringAppendOperator(),
                    DisableAutoCompactions = true,
                    TargetFileSizeBase = 2048,
                    Compression = RocksDb.Managed.Compression.CompressionType.None,
                    LevelCompactionDynamicLevelBytes = false,
                })]);
        }

        public void Put(byte[] key, byte[] value) => _db.Put(key, value);
        public void Merge(byte[] key, byte[] operand) => _db.Merge(key, operand);
        public void Delete(byte[] key) => _db.Delete(key);
        public void DeleteRange(byte[] begin, byte[] end) => _db.DeleteRange(begin, end);
        public void Flush() => _db.Flush();
        public void CompactRange(byte[]? begin, byte[]? end) => _db.CompactRange(begin, end);

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

        private ReadOptions? Options(int snapshot, bool ignoreRangeDeletions) =>
            snapshot < 0 && !ignoreRangeDeletions
                ? null
                : new ReadOptions { Snapshot = snapshot < 0 ? null : _snapshots[snapshot], IgnoreRangeDeletions = ignoreRangeDeletions };

        protected override string? Get(int snapshot, bool ignoreRangeDeletions, byte[] key)
        {
            ReadOptions? ro = Options(snapshot, ignoreRangeDeletions);
            byte[]? v = _async ? _db.GetAsync(ro, key).AsTask().GetAwaiter().GetResult() : _db.Get(ro, key);
            return v is null ? null : Str(v);
        }

        protected override string?[] MultiGet(int snapshot, bool ignoreRangeDeletions, byte[][] keys)
        {
            using var batch = new MultiGetBatch(keys.Length);
            foreach (byte[] k in keys) batch.Add(k);
            ReadOptions? ro = Options(snapshot, ignoreRangeDeletions);
            if (_async) _db.MultiGetAsync(ro, batch).AsTask().GetAwaiter().GetResult();
            else _db.MultiGet(ro, batch);
            var result = new string?[keys.Length];
            for (int i = 0; i < keys.Length; i++)
            {
                if (batch.GetStatus(i) == MultiGetStatus.Error) throw batch.GetError(i)!;
                if (batch.GetStatus(i) == MultiGetStatus.Found) result[i] = Str(batch.GetValue(i));
            }
            return result;
        }

        protected override IIter NewIterator(int snapshot) => new Iter(_db.NewIterator(Options(snapshot, false)), _async);

        private sealed class Iter(Iterator it, bool async) : IIter
        {
            public bool Valid => it.Valid;
            public string Key => Str(it.Key);
            public string Value => Str(it.Value);

            public void SeekToFirst()
            {
                if (async) it.SeekToFirstAsync().AsTask().GetAwaiter().GetResult(); else it.SeekToFirst();
            }

            public void SeekToLast()
            {
                if (async) it.SeekToLastAsync().AsTask().GetAwaiter().GetResult(); else it.SeekToLast();
            }

            public void Seek(byte[] key)
            {
                if (async) it.SeekAsync(key).AsTask().GetAwaiter().GetResult(); else it.Seek(key);
            }

            public void SeekForPrev(byte[] key)
            {
                if (async) it.SeekForPrevAsync(key).AsTask().GetAwaiter().GetResult(); else it.SeekForPrev(key);
            }

            public void Next()
            {
                if (async) it.NextAsync().AsTask().GetAwaiter().GetResult(); else it.Next();
            }

            public void Prev()
            {
                if (async) it.PrevAsync().AsTask().GetAwaiter().GetResult(); else it.Prev();
            }

            public void Dispose() => it.Dispose();
        }

        public void Dispose()
        {
            foreach (Snapshot s in _snapshots.Values) s.Dispose();
            _snapshots.Clear();
            _db.Dispose();
        }
    }

    // ---- the workload ------------------------------------------------------------------------------------

    /// <summary>Runs the workload of <paramref name="seed"/> on <paramref name="e"/> and returns every read result.</summary>
    private static List<string> Run(IEngine e, int seed, int steps)
    {
        var rnd = new Random(seed);
        var log = new List<string>();
        var live = new List<int>();
        ApplySteps(e, rnd, live, steps, log);
        Checkpoint(e, live, rnd.Next(), log, "end");
        e.Flush();
        Checkpoint(e, live, rnd.Next(), log, "flushed");
        e.CompactRange(null, null);
        Checkpoint(e, live, rnd.Next(), log, "compacted");
        return log;
    }

    /// <summary>Applies <paramref name="steps"/> random operations (checkpoints read into <paramref name="log"/> when given).</summary>
    private static void ApplySteps(IEngine e, Random rnd, List<int> live, int steps, List<string>? log)
    {
        for (int step = 0; step < steps; step++)
        {
            int a = rnd.Next(1000);
            int k = rnd.Next(KeyCount);
            if (a < 430)
            {
                e.Put(Key(k), B("v" + step + new string('p', rnd.Next(0, 60))));
            }
            else if (a < 680)
            {
                e.Merge(Key(k), B("m" + step));
            }
            else if (a < 780)
            {
                e.Delete(Key(k));
            }
            else if (a < 880)
            {
                int len = rnd.Next(1, 14);
                e.DeleteRange(Key(k), Key(Math.Min(KeyCount, k + len)));
            }
            else if (a < 930)
            {
                e.Flush();
            }
            else if (a < 955)
            {
                // partial manual compaction: the overlapping L0 files and the output level's files in the range
                int b = Math.Min(KeyCount - 1, k + rnd.Next(1, 30));
                e.CompactRange(Key(k), Key(b));
            }
            else if (a < 975)
            {
                if (live.Count < 3) live.Add(e.TakeSnapshot());
                else
                {
                    int i = rnd.Next(live.Count);
                    e.ReleaseSnapshot(live[i]);
                    live.RemoveAt(i);
                }
            }
            else
            {
                int walkSeed = rnd.Next();
                if (log is not null) Checkpoint(e, live, walkSeed, log, "step " + step);
            }
        }
    }

    private static void Checkpoint(IEngine e, List<int> live, int walkSeed, List<string> log, string what)
    {
        log.Add("---- " + what);
        e.ReadAll(-1, walkSeed, false, log);
        foreach (int s in live) e.ReadAll(s, walkSeed, false, log);
    }

    private static int Seed()
    {
        string? s = Environment.GetEnvironmentVariable("AUDIT4_READ_SEED");
        return s is not null ? int.Parse(s, CultureInfo.InvariantCulture) : Environment.TickCount;
    }

    private static void AssertSameLogs(List<string> native, List<string> managed, string trace)
    {
        for (int i = 0; i < Math.Min(native.Count, managed.Count); i++)
        {
            if (native[i] != managed[i])
            {
                int section = native.FindLastIndex(i, static l => l.StartsWith("----", StringComparison.Ordinal));
                Assert.Fail(trace + ": first difference at line " + i + " (" + (section < 0 ? "start" : native[section]) + ")\n native:  " + native[i] +
                            "\n managed: " + managed[i]);
            }
        }
        Assert.AreEqual(native.Count, managed.Count, trace);
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void RandomWorkloadReadsMatchNative(bool async)
    {
        int baseSeed = Seed();
        for (int round = 0; round < 3; round++)
        {
            int seed = baseSeed + round;
            string trace = "AUDIT4_READ_SEED=" + seed + " async=" + async;
            using var nativeDir = new TempDir("audit4-read-n");
            using var managedDir = new TempDir("audit4-read-m");
            List<string> native, managed;
            using (var e = new NativeEngine(nativeDir.Path)) native = Run(e, seed, 900);
            using (var e = new ManagedEngine(managedDir.Path, async)) managed = Run(e, seed, 900);
            AssertSameLogs(native, managed, trace);
        }
    }


    [TestMethod]
    public void UpstreamGetStopsAtStaleTombstoneWhenUserKeySpansTwoFiles()
    {
        // Version::Get (db/version_set_sync_and_async.h) leaves its file loop as soon as *max_covering_tombstone_seq > 0
        // ("The remaining files we look at will only contain covered keys, so we stop here"). That does not hold when a
        // compaction (with a snapshot between the versions) cut its output between two versions of one user key: the
        // first file [.. k@531 merge] still carries an old range tombstone over k (TableCache::Get uses it
        // untruncated), the next file of the same level starts with the NEWER-than-the-tombstone base value k@333, and
        // native's Get returns the merge of the operand alone while its MultiGet and iterators return base + operand.
        // The managed port keeps looking (the next file's entry is compared with the tombstone's seqno in
        // GetContext::SaveValue) and returns the full value. This test builds that LSM shape with native (workload of
        // seed 100, which the random test hit), checks the anomaly is present, then opens native's directory with the
        // managed port: every managed point lookup must return what native's MultiGet / iterator return.
        using var dir = new TempDir("audit4-straddle");
        var expected = new string?[KeyCount];
        string? nativeGet;
        const int Anomalous = 70;
        using (var native = new NativeEngine(dir.Path))
        {
            var live = new List<int>();
            ApplySteps(native, new Random(100), live, 598, null);
            var log = new List<string>();
            native.ReadAll(-1, 1, false, log);
            for (int i = 0; i < KeyCount; i++)
            {
                string line = log[i];
                string v = line[(line.IndexOf(" = ", StringComparison.Ordinal) + 3)..];
                expected[i] = v == "<null>" ? null : v;
            }
            nativeGet = native.RawGet(Key(Anomalous));
            string forward = log.Find(static l => l.StartsWith("latest forward ", StringComparison.Ordinal))!;
            StringAssert.Contains(forward, "k0070=" + expected[Anomalous] + ";", "native's iterator agrees with its MultiGet");
        }
        if (nativeGet == expected[Anomalous])
        {
            Assert.Inconclusive("this native library does not produce the LSM shape (or no longer has the Get anomaly)");
        }
        StringAssert.StartsWith(expected[Anomalous], "v376");
        StringAssert.EndsWith(expected[Anomalous], ",m596");
        Assert.AreEqual("m596", nativeGet, "upstream Get: the base value in the next file is lost");

        foreach (bool async in new[] { false, true })
        {
            using var managed = new ManagedEngine(dir.Path, async);
            var log = new List<string>();
            managed.ReadAll(-1, 1, false, log);
            for (int i = 0; i < KeyCount; i++)
            {
                Assert.AreEqual("latest get " + i + " = " + (expected[i] ?? "<null>"), log[i], "async=" + async);
            }
            foreach (string line in log)
            {
                if (line.Contains("] 70 = ", StringComparison.Ordinal)) Assert.IsTrue(line.EndsWith(" = " + expected[Anomalous], StringComparison.Ordinal), line);
            }
        }
    }

    [TestMethod]
    public void ManagedReadsOfNativeBuiltLsmMatchNative()
    {
        // The LSM shapes native's own flushes and compactions produce (user keys cut across two files of a level, range
        // tombstones truncated at file boundaries and at sentinel keys, seqnos zeroed at the bottommost level) read
        // through the managed read path: native runs the workload and reads its latest view, then the managed port
        // opens the same directory (WAL replayed into its memtable) and must return the same Get / MultiGet / iterator
        // results (FilePicker, TruncatedRangeDelIterator bounds, LevelIterator file transitions).
        int baseSeed = Seed();
        for (int round = 0; round < 4; round++)
        {
            int seed = baseSeed + 7919 * (round + 1);
            using var dir = new TempDir("audit4-read-x");
            var native = new List<string>();
            using (var e = new NativeEngine(dir.Path))
            {
                var live = new List<int>();
                ApplySteps(e, new Random(seed), live, 500 + 60 * round, null);
                e.ReadAll(-1, seed, false, native);
            }
            foreach (bool async in new[] { false, true })
            {
                var managed = new List<string>();
                using (var e = new ManagedEngine(dir.Path, async)) e.ReadAll(-1, seed, false, managed);
                AssertSameLogs(native, managed, "AUDIT4_READ_SEED=" + baseSeed + " round=" + round + " async=" + async);
            }
        }
    }

    [TestMethod]
    public void IgnoreRangeDeletionsPointLookupsMatchNative()
    {
        // ReadOptions::ignore_range_deletions in MemTable::Get / MultiGet and TableCache::Get / MultiGet: keys covered by
        // a range tombstone stay visible to point lookups while the tombstone and the key have not been compacted
        // together. Scenario without compaction and with tombstones flushed apart from the keys they cover, so both
        // engines hold the same entries: L0 file {keys}, L0 file {tombstone}, memtable {more keys, another tombstone}.
        using var nativeDir = new TempDir("audit4-ird-n");
        using var managedDir = new TempDir("audit4-ird-m");
        var logs = new List<string>[2];
        for (int side = 0; side < 2; side++)
        {
            using IEngine e = side == 0 ? new NativeEngine(nativeDir.Path) : new ManagedEngine(managedDir.Path, async: false);
            for (int i = 0; i < KeyCount; i++) e.Put(Key(i), B("v" + i));
            for (int i = 0; i < KeyCount; i += 5) e.Merge(Key(i), B("m" + i));
            e.Flush();
            e.DeleteRange(Key(10), Key(30));
            e.Flush();
            for (int i = 20; i < 50; i += 3) e.Merge(Key(i), B("n" + i));
            e.DeleteRange(Key(40), Key(60));
            e.Put(Key(45), B("w45"));
            var log = new List<string>();
            e.ReadAll(-1, 1, ignoreRangeDeletions: false, log);
            e.ReadAll(-1, 1, ignoreRangeDeletions: true, log);
            logs[side] = log;
        }
        AssertSameLogs(logs[0], logs[1], "ignore_range_deletions");
        // The option changes results (otherwise the comparison proves nothing).
        Assert.IsTrue(logs[0].Contains("latest get 12 = <null>"), "covered key hidden by default");
        Assert.IsTrue(logs[0].Contains("latest+ignore_range_del get 12 = v12"), "covered key visible with ignore_range_deletions");
    }
}
