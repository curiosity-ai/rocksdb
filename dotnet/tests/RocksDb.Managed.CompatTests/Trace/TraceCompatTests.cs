using System.Runtime.InteropServices;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Trace;

namespace RocksDb.Managed.CompatTests.Trace;

/// <summary>
/// Trace file compatibility with native RocksDB (trace_replay/trace_replay.cc, block_cache_tracer.cc, io_tracer.cc):
/// native query traces replay on the managed DB and managed traces replay on the native DB (rocksdb_new_default_replayer);
/// native block cache / IO traces parse with the managed readers and managed ones frame correctly for the native reader.
/// </summary>
[TestClass]
public sealed unsafe class TraceCompatTests
{
    private static byte[] B(string s) => Encoding.UTF8.GetBytes(s);

    // ---- C API functions rocksdb-sharp does not bind (resolved from the library it loaded) -------------------------

    private static readonly Lazy<IntPtr> s_lib = new(() =>
    {
        _ = RocksDbSharp.Native.Instance;
        foreach (string line in File.ReadLines("/proc/self/maps"))
        {
            int slash = line.IndexOf('/', StringComparison.Ordinal);
            if (slash >= 0 && Path.GetFileName(line[slash..]).StartsWith("librocksdb", StringComparison.Ordinal)) return NativeLibrary.Load(line[slash..]);
        }
        throw new InvalidOperationException("librocksdb is not loaded");
    });

    private static IntPtr Fn(string name) => NativeLibrary.GetExport(s_lib.Value, name);

    private static void Check(IntPtr err)
    {
        if (err == IntPtr.Zero) return;
        string msg = Marshal.PtrToStringUTF8(err)!;
        ((delegate* unmanaged<IntPtr, void>)Fn("rocksdb_free"))(err);
        Assert.Fail("native error: " + msg);
    }

    private static IntPtr Env() => ((delegate* unmanaged<IntPtr>)Fn("rocksdb_create_default_env"))();
    private static IntPtr EnvOptions() => ((delegate* unmanaged<IntPtr>)Fn("rocksdb_envoptions_create"))();
    private static IntPtr TraceOptionsNative() => ((delegate* unmanaged<IntPtr>)Fn("rocksdb_trace_options_create"))();

    private delegate void StartFn(IntPtr db, IntPtr env, IntPtr envOpts, IntPtr traceOpts, IntPtr path, IntPtr* err);

    private static void NativeStart(string fn, RocksDbSharp.RocksDb db, string path)
    {
        IntPtr env = Env(), eo = EnvOptions(), to = TraceOptionsNative();
        IntPtr p = Marshal.StringToCoTaskMemUTF8(path);
        IntPtr err = IntPtr.Zero;
        ((delegate* unmanaged<IntPtr, IntPtr, IntPtr, IntPtr, IntPtr, IntPtr*, void>)Fn(fn))(db.Handle, env, eo, to, p, &err);
        Marshal.FreeCoTaskMem(p);
        Check(err);
    }

    private static void NativeEnd(string fn, RocksDbSharp.RocksDb db)
    {
        IntPtr err = IntPtr.Zero;
        ((delegate* unmanaged<IntPtr, IntPtr*, void>)Fn(fn))(db.Handle, &err);
        Check(err);
    }

    /// <summary>rocksdb_new_default_replayer + prepare + replay on <paramref name="cfs"/>.</summary>
    private static void NativeReplay(RocksDbSharp.RocksDb db, RocksDbSharp.ColumnFamilyHandle[] cfs, string path, uint threads)
    {
        IntPtr env = Env(), eo = EnvOptions();
        IntPtr p = Marshal.StringToCoTaskMemUTF8(path);
        IntPtr* handles = stackalloc IntPtr[cfs.Length];
        for (int i = 0; i < cfs.Length; i++) handles[i] = cfs[i].Handle;
        IntPtr err = IntPtr.Zero;
        IntPtr replayer = ((delegate* unmanaged<IntPtr, IntPtr*, nuint, IntPtr, IntPtr, IntPtr, IntPtr*, IntPtr>)Fn("rocksdb_new_default_replayer"))(
            db.Handle, handles, (nuint)cfs.Length, env, eo, p, &err);
        Marshal.FreeCoTaskMem(p);
        Check(err);
        ((delegate* unmanaged<IntPtr, IntPtr*, void>)Fn("rocksdb_replayer_prepare"))(replayer, &err);
        Check(err);
        IntPtr ro = ((delegate* unmanaged<IntPtr>)Fn("rocksdb_replay_options_create"))();
        ((delegate* unmanaged<IntPtr, uint, void>)Fn("rocksdb_replay_options_set_num_threads"))(ro, threads);
        ((delegate* unmanaged<IntPtr, double, void>)Fn("rocksdb_replay_options_set_fast_forward"))(ro, 1000.0);
        ((delegate* unmanaged<IntPtr, IntPtr, IntPtr*, void>)Fn("rocksdb_replayer_replay"))(replayer, ro, &err);
        ((delegate* unmanaged<IntPtr, void>)Fn("rocksdb_replay_options_destroy"))(ro);
        ((delegate* unmanaged<IntPtr, void>)Fn("rocksdb_replayer_destroy"))(replayer);
        Check(err);
    }

    /// <summary>All records of a trace file through the native FileTraceReader (rocksdb_trace_reader_read).</summary>
    private static List<byte[]> NativeReadRecords(string path)
    {
        IntPtr p = Marshal.StringToCoTaskMemUTF8(path);
        IntPtr err = IntPtr.Zero;
        IntPtr reader = ((delegate* unmanaged<IntPtr, IntPtr, IntPtr, IntPtr*, IntPtr>)Fn("rocksdb_trace_reader_create"))(Env(), EnvOptions(), p, &err);
        Marshal.FreeCoTaskMem(p);
        Check(err);
        var list = new List<byte[]>();
        while (true)
        {
            nuint size = 0;
            IntPtr rec = ((delegate* unmanaged<IntPtr, nuint*, IntPtr*, IntPtr>)Fn("rocksdb_trace_reader_read"))(reader, &size, &err);
            Check(err);
            if (rec == IntPtr.Zero) break;
            list.Add(new ReadOnlySpan<byte>((void*)rec, (int)size).ToArray());
            ((delegate* unmanaged<IntPtr, void>)Fn("rocksdb_free"))(rec);
        }
        ((delegate* unmanaged<IntPtr, void>)Fn("rocksdb_trace_reader_destroy"))(reader);
        return list;
    }

    private static RocksDbSharp.RocksDb OpenNative(string path)
    {
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetCreateMissingColumnFamilies(true);
        var cfs = new RocksDbSharp.ColumnFamilies { { "one", new RocksDbSharp.ColumnFamilyOptions() } };
        return RocksDbSharp.RocksDb.Open(o, path, cfs);
    }

    private static RocksDatabase OpenManaged(string path) =>
        RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true, CreateMissingColumnFamilies = true },
            [new ColumnFamilyDescriptor("default", new ColumnFamilyOptions()), new ColumnFamilyDescriptor("one", new ColumnFamilyOptions())]);

    [TestMethod]
    public void NativeQueryTraceReplaysOnManagedDb()
    {
        using var dir = new TempDir("trace");
        string trace = Path.Combine(dir.Path, "query.trace");
        using (RocksDbSharp.RocksDb n = OpenNative(Path.Combine(dir.Path, "native")))
        {
            RocksDbSharp.ColumnFamilyHandle one = n.GetColumnFamily("one");
            NativeStart("rocksdb_start_trace", n, trace);
            n.Put(B("a"), B("1"));
            n.Put(B("x"), B("9"), one);
            var wb = new RocksDbSharp.WriteBatch();
            wb.Put(B("b"), B("2")).Delete(B("a"));
            n.Write(wb);
            _ = n.Get(B("b"));
            _ = n.Get(B("x"), one);
            _ = n.MultiGet([B("a"), B("b")]);
            using (RocksDbSharp.Iterator it = n.NewIterator())
            {
                it.Seek(B("b"));
                it.SeekForPrev(B("c"));
            }
            NativeEnd("rocksdb_end_trace", n);
        }

        using RocksDatabase db = OpenManaged(Path.Combine(dir.Path, "managed"));
        using (Replayer r = db.NewDefaultReplayer(TraceReader.OpenFile(trace)))
        {
            r.Prepare();
            Assert.AreEqual(2, r.TraceFileVersion);
            var types = new List<TraceType>();
            while (r.TryNext(out TraceRecord? rec)) types.Add(rec!.TraceType);
            CollectionAssert.IsSubsetOf(new[] { TraceType.Write, TraceType.Get, TraceType.MultiGet, TraceType.IteratorSeek, TraceType.IteratorSeekForPrev }, types);
            Assert.AreEqual(3, types.FindAll(t => t == TraceType.Write).Count);

            r.Prepare();
            int executed = 0;
            r.Replay(new ReplayOptions { FastForward = 1000 }, (e, res) =>
            {
                Assert.IsNull(e);
                Interlocked.Increment(ref executed);
            });
            Assert.AreEqual(types.Count, executed);
        }
        Assert.IsNull(db.Get(B("a")));
        Assert.AreEqual("2", Encoding.UTF8.GetString(db.Get(B("b"))!));
        Assert.AreEqual("9", Encoding.UTF8.GetString(db.GetColumnFamily("one").Get(B("x"))!));
    }

    [TestMethod]
    [DataRow(1u)]
    [DataRow(3u)]
    public void ManagedQueryTraceReplaysOnNativeDb(uint threads)
    {
        using var dir = new TempDir("trace");
        string trace = Path.Combine(dir.Path, "query.trace");
        using (RocksDatabase db = OpenManaged(Path.Combine(dir.Path, "managed")))
        {
            ColumnFamily one = db.GetColumnFamily("one");
            db.StartTrace(new TraceOptions(), trace);
            for (int i = 0; i < 50; i++) db.Put(B("k" + i), B("v" + i));
            one.Put(B("x"), B("9"));
            db.Delete(B("k3"));
            _ = db.Get(B("k1"));
            _ = one.Get(B("x"));
            _ = db.MultiGet([B("k1"), B("k2")]);
            using (Iterator it = db.NewIterator(new ReadOptions { IterateLowerBound = B("k1") }))
            {
                it.Seek(B("k2"));
                it.SeekForPrev(B("k4"));
            }
            db.EndTrace();
        }
        // The native FileTraceReader frames every managed record (header + 56 queries + footer).
        Assert.AreEqual(1 + 52 + 2 + 1 + 2 + 1, NativeReadRecords(trace).Count);

        using RocksDbSharp.RocksDb n = OpenNative(Path.Combine(dir.Path, "native"));
        NativeReplay(n, [n.GetDefaultColumnFamily(), n.GetColumnFamily("one")], trace, threads);
        Assert.AreEqual("v1", n.Get("k1"));
        Assert.IsNull(n.Get("k3"));
        Assert.AreEqual("v49", n.Get("k49"));
        Assert.AreEqual("9", n.Get("x", n.GetColumnFamily("one")));
    }

    [TestMethod]
    public void NativeBlockCacheAndIoTracesParseWithManagedReaders()
    {
        using var dir = new TempDir("trace");
        string bc = Path.Combine(dir.Path, "bc.trace"), io = Path.Combine(dir.Path, "io.trace");
        using (RocksDbSharp.RocksDb n = OpenNative(Path.Combine(dir.Path, "native")))
        {
            for (int i = 0; i < 100; i++) n.Put("k" + i, "v" + i);
            n.Flush(new RocksDbSharp.FlushOptions());
            NativeStart("rocksdb_start_block_cache_trace", n, bc);
            NativeStart("rocksdb_start_io_trace", n, io);
            _ = n.Get("k5");
            _ = n.Get("k5");
            n.Put("late", "1");
            n.Flush(new RocksDbSharp.FlushOptions());
            NativeEnd("rocksdb_end_block_cache_trace", n);
            NativeEnd("rocksdb_end_io_trace", n);
        }
        using (var r = new BlockCacheTraceReader(TraceReader.OpenFile(bc)))
        {
            TraceFileHeader h = r.ReadHeader();
            Assert.AreEqual(11u, h.RocksDbMajorVersion);
            var recs = new List<BlockCacheTraceRecord>();
            while (r.TryReadAccess(out BlockCacheTraceRecord? rec)) recs.Add(rec!);
            List<BlockCacheTraceRecord> gets = recs.FindAll(x => x.Caller == TableReaderCaller.UserGet && x.BlockType == TraceType.BlockDataBlock);
            Assert.AreEqual(2, gets.Count);
            Assert.IsFalse(gets[0].IsCacheHit);
            Assert.IsTrue(gets[1].IsCacheHit);
            Assert.AreEqual(16, gets[0].BlockKey.Length);
            Assert.AreEqual("default", gets[0].ColumnFamilyName);
            // Native traces the internal key looked up (user key + 8 bytes).
            CollectionAssert.AreEqual(B("k5"), gets[0].ReferencedKey.AsSpan(0, 2).ToArray());
        }
        using (var r = new IoTraceReader(TraceReader.OpenFile(io)))
        {
            r.ReadHeader();
            var ops = new List<IoTraceRecord>();
            while (r.TryReadIoOp(out IoTraceRecord? rec)) ops.Add(rec!);
            Assert.IsTrue(ops.Exists(o => o.FileOperation == "Read" && o.FileName.EndsWith(".sst", StringComparison.Ordinal) && o.IoOpData == (IoTraceOp.Length | IoTraceOp.Offset)), string.Join(",", ops.ConvertAll(o => o.FileOperation + ":" + o.FileName + ":" + o.IoOpData)));
            Assert.IsTrue(ops.Exists(o => o.FileOperation == "Append" && o.IoOpData.HasFlag(IoTraceOp.Length) && o.Length > 0));
        }
    }

    [TestMethod]
    public void ManagedBlockCacheAndIoTracesFrameForNativeReader()
    {
        using var dir = new TempDir("trace");
        string bc = Path.Combine(dir.Path, "bc.trace"), io = Path.Combine(dir.Path, "io.trace");
        int bcRecords = 0, ioRecords = 0;
        using (RocksDatabase db = OpenManaged(Path.Combine(dir.Path, "managed")))
        {
            for (int i = 0; i < 100; i++) db.Put(B("k" + i), B("v" + i));
            db.Flush();
            db.StartBlockCacheTrace(new BlockCacheTraceOptions(), bc);
            db.StartIOTrace(new TraceOptions(), io);
            _ = db.Get(B("k5"));
            db.Put(B("late"), B("1"));
            db.Flush();
            db.EndBlockCacheTrace();
            db.EndIOTrace();
        }
        using (var r = new BlockCacheTraceReader(TraceReader.OpenFile(bc)))
        {
            r.ReadHeader();
            while (r.TryReadAccess(out _)) bcRecords++;
        }
        using (var r = new IoTraceReader(TraceReader.OpenFile(io)))
        {
            r.ReadHeader();
            while (r.TryReadIoOp(out _)) ioRecords++;
        }
        Assert.IsTrue(bcRecords > 0 && ioRecords > 0);
        List<byte[]> nb = NativeReadRecords(bc);
        Assert.AreEqual(1 + bcRecords, nb.Count);
        Assert.AreEqual((byte)TraceType.Begin, nb[0][8]);
        Assert.AreEqual(1 + ioRecords, NativeReadRecords(io).Count);
    }
}
