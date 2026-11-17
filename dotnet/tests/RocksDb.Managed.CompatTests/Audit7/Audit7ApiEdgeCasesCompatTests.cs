using System.Runtime.InteropServices;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Env;

namespace RocksDb.Managed.CompatTests.Audit7;

/// <summary>
/// Audit #7, area "ApiEdgeCases": the public API under misuse and boundary inputs, run side by side on native RocksDB
/// (the C API of the RocksDB NuGet package) and on the managed port, comparing the status each one reports.
/// </summary>
[TestClass]
public sealed class Audit7ApiEdgeCasesCompatTests
{
    private static readonly RocksDbSharp.Native N = RocksDbSharp.Native.Instance;

    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);

    /// <summary>The native error message (and frees it); null when the call succeeded.</summary>
    private static string? Err(ref IntPtr err)
    {
        // The binding's exception reads and frees the message. The variable is cleared: the C API's SaveError frees a
        // non-null *errptr before storing a new message, so a reused out variable must not keep the freed pointer.
        if (err == IntPtr.Zero) return null;
        string message = new RocksDbSharp.RocksDbException(err).Message;
        err = IntPtr.Zero;
        return message;
    }

    /// <summary>Status::ToString prefix -> code.</summary>
    private static StatusCode? NativeCode(string? message)
    {
        if (message is null) return null;
        if (message.StartsWith("Invalid argument", StringComparison.Ordinal)) return StatusCode.InvalidArgument;
        if (message.StartsWith("IO error", StringComparison.Ordinal)) return StatusCode.IOError;
        if (message.StartsWith("Not implemented", StringComparison.Ordinal)) return StatusCode.NotSupported;
        if (message.StartsWith("Corruption", StringComparison.Ordinal)) return StatusCode.Corruption;
        if (message.StartsWith("NotFound", StringComparison.Ordinal)) return StatusCode.NotFound;
        if (message.StartsWith("Operation aborted", StringComparison.Ordinal)) return StatusCode.Aborted;
        if (message.StartsWith("Shutdown in progress", StringComparison.Ordinal)) return StatusCode.ShutdownInProgress;
        if (message.StartsWith("Column family dropped", StringComparison.Ordinal)) return StatusCode.ColumnFamilyDropped;
        Assert.Fail("unmapped native status: " + message);
        return null;
    }

    private static StatusCode? ManagedCode(Action a)
    {
        try
        {
            a();
            return null;
        }
        catch (RocksDbException e)
        {
            return e.Code;
        }
    }

    private static RocksDbSharp.RocksDb OpenNative(string path, params string[] cfs)
    {
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetCreateMissingColumnFamilies(true);
        var families = new RocksDbSharp.ColumnFamilies();
        foreach (string cf in cfs) families.Add(cf, new RocksDbSharp.ColumnFamilyOptions());
        return RocksDbSharp.RocksDb.Open(o, path, families);
    }

    private static RocksDatabase OpenManaged(string path, params string[] cfs)
    {
        var list = new List<ColumnFamilyDescriptor> { new("default", new ColumnFamilyOptions()) };
        foreach (string cf in cfs) list.Add(new ColumnFamilyDescriptor(cf, new ColumnFamilyOptions()));
        return RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true, CreateMissingColumnFamilies = true }, list);
    }

    /// <summary>
    /// A dropped column family's handle (db/column_family.cc ColumnFamilyData::SetDropped removes it from the
    /// ColumnFamilySet; db/write_batch.cc MemTableInserter::SeekToColumnFamily -> "Invalid column family specified in write
    /// batch"; DBImpl::FlushMemTable -> ColumnFamilyDropped): writes, flushes and ingestion fail, reads and iterators
    /// still see the data as of the drop, dropping it again is InvalidArgument ("Column family already dropped!").
    /// </summary>
    [TestMethod]
    public void DroppedColumnFamilyHandleMatchesNative()
    {
        using var dir = new TempDir("a7-dropped");
        string sst = Path.Combine(dir.Path, "ext.sst");
        using (var w = new SstFileWriter(new ColumnFamilyOptions()))
        {
            w.Open(sst);
            w.Put(U("z"), U("1"));
            w.Finish();
        }

        var native = new Dictionary<string, object?>();
        string? nativeIngestMessage;
        string np = Path.Combine(dir.Path, "native");
        using (RocksDbSharp.RocksDb db = OpenNative(np, "default", "cf"))
        {
            IntPtr cf = db.GetColumnFamily("cf").Handle;
            var wo = new RocksDbSharp.WriteOptions();
            var ro = new RocksDbSharp.ReadOptions();
            N.rocksdb_put_cf(db.Handle, wo.Handle, cf, U("k"), (UIntPtr)1, U("v"), (UIntPtr)1, out IntPtr err);
            Assert.IsNull(Err(ref err));
            N.rocksdb_drop_column_family(db.Handle, cf, out err);
            Assert.IsNull(Err(ref err));
            N.rocksdb_put_cf(db.Handle, wo.Handle, cf, U("k2"), (UIntPtr)2, U("v"), (UIntPtr)1, out err);
            native["put"] = NativeCode(Err(ref err));
            N.rocksdb_delete_range_cf(db.Handle, wo.Handle, cf, U("a"), (UIntPtr)1, U("b"), (UIntPtr)1, out err);
            native["deleterange"] = NativeCode(Err(ref err));
            IntPtr v = N.rocksdb_get_cf(db.Handle, ro.Handle, cf, U("k"), (UIntPtr)1, out UIntPtr vlen, out err);
            native["get-err"] = NativeCode(Err(ref err));
            native["get"] = v == IntPtr.Zero ? null : Marshal.PtrToStringAnsi(v, (int)vlen);
            if (v != IntPtr.Zero) N.rocksdb_free(v);
            IntPtr it = N.rocksdb_create_iterator_cf(db.Handle, ro.Handle, cf);
            N.rocksdb_iter_seek_to_first(it);
            native["iter"] = N.rocksdb_iter_valid(it) != 0;
            N.rocksdb_iter_destroy(it);
            N.rocksdb_flush_cf(db.Handle, new RocksDbSharp.FlushOptions().Handle, cf, out err);
            native["flush"] = NativeCode(Err(ref err));
            IntPtr io = N.rocksdb_ingestexternalfileoptions_create();
            N.rocksdb_ingest_external_file_cf(db.Handle, cf, [sst], (UIntPtr)1, io, out IntPtr ingestErr);
            nativeIngestMessage = Err(ref ingestErr);
            native["ingest"] = NativeCode(nativeIngestMessage);
            N.rocksdb_ingestexternalfileoptions_destroy(io);
            N.rocksdb_drop_column_family(db.Handle, cf, out err);
            native["drop-again"] = NativeCode(Err(ref err));
        }

        var managed = new Dictionary<string, object?>();
        using (RocksDatabase db = OpenManaged(Path.Combine(dir.Path, "managed"), "cf"))
        {
            ColumnFamily cf = db.GetColumnFamily("cf");
            cf.Put(U("k"), U("v"));
            db.DropColumnFamily(cf);
            managed["put"] = ManagedCode(() => cf.Put(U("k2"), U("v")));
            managed["deleterange"] = ManagedCode(() => cf.DeleteRange(U("a"), U("b")));
            managed["get-err"] = null;
            byte[]? v = null;
            managed["get-err"] = ManagedCode(() => v = cf.Get(U("k")));
            managed["get"] = v is null ? null : Encoding.UTF8.GetString(v);
            using (Iterator it = cf.NewIterator())
            {
                it.SeekToFirst();
                managed["iter"] = it.Valid;
            }
            managed["flush"] = ManagedCode(() => cf.Flush());
            managed["ingest"] = ManagedCode(() => db.IngestExternalFiles(cf, [sst], new IngestExternalFileOptions()));
            managed["drop-again"] = ManagedCode(() => db.DropColumnFamily(cf));
        }
        foreach (KeyValuePair<string, object?> kv in native)
        {
            Console.WriteLine(kv.Key + ": native=" + kv.Value + " managed=" + managed[kv.Key]);
        }
        Console.WriteLine("native ingest message: " + nativeIngestMessage);
        foreach (KeyValuePair<string, object?> kv in native)
        {
            if (kv.Key != "ingest") Assert.AreEqual(kv.Value, managed[kv.Key], kv.Key);
        }
        // Ingestion into a column family dropped before the call: the 11.14 sources fail it in
        // DBImpl::ReserveFileNumbersBeforeIngestion, whose dummy-edit VersionSet::LogAndApply returns
        // Status::ColumnFamilyDropped() when no column family of the edit is undropped (version_set.cc, "0 ==
        // num_undropped_cfds"); the port follows the sources. The 11.12 package reports InvalidArgument ("Invalid column
        // family specified in write batch") instead. Both refuse the file and leave the DB unchanged.
        Assert.AreEqual(StatusCode.InvalidArgument, native["ingest"]);
        Assert.AreEqual(StatusCode.ColumnFamilyDropped, managed["ingest"]);
    }

    /// <summary>
    /// db/write_batch.cc MemTableInserter::DeleteRangeCF: begin &gt; end is InvalidArgument("end key comes before start
    /// key"), begin == end an OK no-op. DBImpl::CompactRangeInternal has no begin/end order check (an empty range).
    /// </summary>
    [TestMethod]
    public void ReversedRangesMatchNative()
    {
        using var dir = new TempDir("a7-ranges");
        StatusCode? nRev, nEq, mRev, mEq;
        string? nAfter, mAfter;
        using (RocksDbSharp.RocksDb db = OpenNative(Path.Combine(dir.Path, "native"), "default"))
        {
            var wo = new RocksDbSharp.WriteOptions();
            IntPtr cf = db.GetDefaultColumnFamily().Handle;
            db.Put(U("b"), U("1"));
            N.rocksdb_delete_range_cf(db.Handle, wo.Handle, cf, U("c"), (UIntPtr)1, U("a"), (UIntPtr)1, out IntPtr err);
            nRev = NativeCode(Err(ref err));
            N.rocksdb_delete_range_cf(db.Handle, wo.Handle, cf, U("b"), (UIntPtr)1, U("b"), (UIntPtr)1, out err);
            nEq = NativeCode(Err(ref err));
            db.CompactRange(U("c"), U("a"));
            nAfter = db.Get("b");
        }
        using (RocksDatabase db = OpenManaged(Path.Combine(dir.Path, "managed")))
        {
            db.Put(U("b"), U("1"));
            mRev = ManagedCode(() => db.DeleteRange(U("c"), U("a")));
            mEq = ManagedCode(() => db.DeleteRange(U("b"), U("b")));
            db.CompactRange(U("c"), U("a"));
            byte[]? v = db.Get(U("b"));
            mAfter = v is null ? null : Encoding.UTF8.GetString(v);
        }
        Assert.AreEqual(StatusCode.InvalidArgument, nRev);
        Assert.AreEqual(nRev, mRev);
        Assert.AreEqual(nEq, mEq);
        Assert.AreEqual("1", nAfter);
        Assert.AreEqual(nAfter, mAfter);
    }

    /// <summary>
    /// DBImpl::Open argument checks (db/db_impl/db_impl_open.cc): a missing DB without create_if_missing and an
    /// existing one with error_if_exists are InvalidArgument; opening without listing an existing column family is
    /// InvalidArgument ("Column families not opened"); a second open while the LOCK is held is IOError.
    /// </summary>
    [TestMethod]
    public void OpenArgumentChecksMatchNative()
    {
        using var dir = new TempDir("a7-open");
        string np = Path.Combine(dir.Path, "native"), mp = Path.Combine(dir.Path, "managed");
        var results = new List<(string What, StatusCode? Native, StatusCode? Managed)>();
        StatusCode? NOpen(string path, RocksDbSharp.DbOptions o)
        {
            try
            {
                RocksDbSharp.RocksDb.Open(o, path).Dispose();
                return null;
            }
            catch (RocksDbSharp.RocksDbException e)
            {
                return NativeCode(e.Message);
            }
        }
        results.Add(("missing", NOpen(np, new RocksDbSharp.DbOptions()), ManagedCode(() => RocksDatabase.Open(mp, new DbOptions()).Dispose())));
        using (OpenNative(np, "default", "cf"))
        using (OpenManaged(mp, "cf"))
        {
            results.Add(("locked", NOpen(np, new RocksDbSharp.DbOptions()), ManagedCode(() => RocksDatabase.Open(mp, new DbOptions()).Dispose())));
        }
        results.Add(("cf-not-opened", NOpen(np, new RocksDbSharp.DbOptions()), ManagedCode(() => RocksDatabase.Open(mp, new DbOptions()).Dispose())));
        results.Add(("error-if-exists", NOpen(np, new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetErrorIfExists(true)),
            ManagedCode(() => RocksDatabase.Open(mp, new DbOptions { CreateIfMissing = true, ErrorIfExists = true }).Dispose())));
        foreach ((string what, StatusCode? n, StatusCode? m) in results) Console.WriteLine(what + ": native=" + n + " managed=" + m);
        foreach ((string what, StatusCode? n, StatusCode? m) in results)
        {
            Assert.IsNotNull(n, what);
            Assert.AreEqual(n, m, what);
        }
    }

    /// <summary>
    /// CheckpointImpl::CreateCheckpoint: an existing target is InvalidArgument("Directory exists");
    /// SstFileWriter::Finish with no entries is InvalidArgument; Rep::AddImpl out of order is InvalidArgument; a Put
    /// after Finish is InvalidArgument("File is not opened").
    /// </summary>
    [TestMethod]
    public void CheckpointAndSstFileWriterMisuseMatchNative()
    {
        using var dir = new TempDir("a7-ckpt");
        string target = Path.Combine(dir.Path, "exists");
        Directory.CreateDirectory(target);
        StatusCode? nCkpt, mCkpt;
        using (RocksDbSharp.RocksDb db = OpenNative(Path.Combine(dir.Path, "native"), "default"))
        {
            IntPtr cp = N.rocksdb_checkpoint_object_create(db.Handle, out IntPtr err);
            Assert.IsNull(Err(ref err));
            N.rocksdb_checkpoint_create(cp, target, 0, out err);
            nCkpt = NativeCode(Err(ref err));
            N.rocksdb_checkpoint_object_destroy(cp);
        }
        using (RocksDatabase db = OpenManaged(Path.Combine(dir.Path, "managed")))
        {
            mCkpt = ManagedCode(() => db.CreateCheckpoint(target));
        }
        Assert.AreEqual(StatusCode.InvalidArgument, nCkpt);
        Assert.AreEqual(nCkpt, mCkpt);

        // SstFileWriter
        IntPtr envOpts = N.rocksdb_envoptions_create();
        IntPtr opts = N.rocksdb_options_create();
        IntPtr nw = N.rocksdb_sstfilewriter_create(envOpts, opts);
        N.rocksdb_sstfilewriter_open(nw, Path.Combine(dir.Path, "n.sst"), out IntPtr e1);
        Assert.IsNull(Err(ref e1));
        N.rocksdb_sstfilewriter_finish(nw, out e1);
        StatusCode? nEmpty = NativeCode(Err(ref e1));
        N.rocksdb_sstfilewriter_put(nw, U("b"), (UIntPtr)1, U("1"), (UIntPtr)1, out e1);
        Assert.IsNull(Err(ref e1));
        N.rocksdb_sstfilewriter_put(nw, U("a"), (UIntPtr)1, U("1"), (UIntPtr)1, out e1);
        StatusCode? nOrder = NativeCode(Err(ref e1));
        N.rocksdb_sstfilewriter_put(nw, U("b"), (UIntPtr)1, U("1"), (UIntPtr)1, out e1);
        StatusCode? nDup = NativeCode(Err(ref e1));
        N.rocksdb_sstfilewriter_finish(nw, out e1);
        Assert.IsNull(Err(ref e1));
        N.rocksdb_sstfilewriter_put(nw, U("c"), (UIntPtr)1, U("1"), (UIntPtr)1, out e1);
        StatusCode? nAfter = NativeCode(Err(ref e1));
        N.rocksdb_sstfilewriter_destroy(nw);
        N.rocksdb_options_destroy(opts);
        N.rocksdb_envoptions_destroy(envOpts);

        using var mw = new SstFileWriter(new ColumnFamilyOptions(), LocalFileSystem.Instance);
        mw.Open(Path.Combine(dir.Path, "m.sst"));
        StatusCode? mEmpty = ManagedCode(() => mw.Finish());
        mw.Put(U("b"), U("1"));
        StatusCode? mOrder = ManagedCode(() => mw.Put(U("a"), U("1")));
        StatusCode? mDup = ManagedCode(() => mw.Put(U("b"), U("1")));
        mw.Finish();
        StatusCode? mAfter = ManagedCode(() => mw.Put(U("c"), U("1")));
        Console.WriteLine($"empty {nEmpty}/{mEmpty} order {nOrder}/{mOrder} dup {nDup}/{mDup} after {nAfter}/{mAfter}");
        Assert.AreEqual(StatusCode.InvalidArgument, nEmpty);
        Assert.AreEqual(nEmpty, mEmpty);
        Assert.AreEqual(nOrder, mOrder);
        Assert.AreEqual(nDup, mDup);
        Assert.AreEqual(nAfter, mAfter);
    }

    /// <summary>
    /// DBImpl::IngestExternalFiles / ExternalSstFileIngestionJob::Prepare: an empty file list, a file that does not
    /// exist and a zero-byte file all fail (and leave the DB as it was).
    /// </summary>
    [TestMethod]
    public void IngestionOfBadInputsMatchesNative()
    {
        using var dir = new TempDir("a7-ingest");
        string missing = Path.Combine(dir.Path, "missing.sst");
        string empty = Path.Combine(dir.Path, "empty.sst");
        File.WriteAllBytes(empty, []);
        string junk = Path.Combine(dir.Path, "junk.sst");
        File.WriteAllBytes(junk, new byte[100]);
        var cases = new (string Name, string[] Files)[] { ("none", []), ("missing", [missing]), ("empty", [empty]), ("junk", [junk]) };
        var nres = new Dictionary<string, StatusCode?>();
        var mres = new Dictionary<string, StatusCode?>();
        using (RocksDbSharp.RocksDb db = OpenNative(Path.Combine(dir.Path, "native"), "default"))
        {
            IntPtr io = N.rocksdb_ingestexternalfileoptions_create();
            foreach ((string name, string[] files) in cases)
            {
                N.rocksdb_ingest_external_file(db.Handle, files, (UIntPtr)files.Length, io, out IntPtr err);
                nres[name] = NativeCode(Err(ref err));
            }
            N.rocksdb_ingestexternalfileoptions_destroy(io);
        }
        using (RocksDatabase db = OpenManaged(Path.Combine(dir.Path, "managed")))
        {
            foreach ((string name, string[] files) in cases)
            {
                mres[name] = ManagedCode(() => db.IngestExternalFiles(files, new IngestExternalFileOptions()));
            }
        }
        foreach ((string name, _) in cases) Console.WriteLine(name + ": native=" + nres[name] + " managed=" + mres[name]);
        foreach ((string name, _) in cases)
        {
            Assert.IsNotNull(nres[name], name);
            Assert.AreEqual(nres[name], mres[name], name);
        }
    }

    /// <summary>
    /// DBImpl::MultiGet with duplicate keys (each slot gets the value) and an unknown property name
    /// (DBImpl::GetProperty -> false, the C API returns NULL).
    /// </summary>
    [TestMethod]
    public void MultiGetDuplicatesAndUnknownPropertyMatchNative()
    {
        using var dir = new TempDir("a7-mget");
        byte[][] keys = [U("a"), U("x"), U("a"), U(""), U("a")];
        string?[] nv, mv;
        string? nProp, mProp;
        using (RocksDbSharp.RocksDb db = OpenNative(Path.Combine(dir.Path, "native"), "default"))
        {
            db.Put(U("a"), U("1"));
            db.Put(U(""), U("e"));
            KeyValuePair<byte[], byte[]>[] r = db.MultiGet(keys);
            nv = new string?[r.Length];
            for (int i = 0; i < r.Length; i++) nv[i] = r[i].Value is null ? null : Encoding.UTF8.GetString(r[i].Value);
            nProp = db.GetProperty("rocksdb.no-such-property");
        }
        using (RocksDatabase db = OpenManaged(Path.Combine(dir.Path, "managed")))
        {
            db.Put(U("a"), U("1"));
            db.Put(U(""), U("e"));
            byte[]?[] r = db.MultiGet(keys);
            mv = new string?[r.Length];
            for (int i = 0; i < r.Length; i++) mv[i] = r[i] is null ? null : Encoding.UTF8.GetString(r[i]!);
            mProp = db.GetProperty("rocksdb.no-such-property");
        }
        CollectionAssert.AreEqual(nv, mv);
        Assert.AreEqual(nProp, mProp);
    }

    /// <summary>
    /// rocksdb-sharp's WriteBatch.Dispose is idempotent (the binding clears its handle), which the managed WriteBatch
    /// now matches; native C++ deleting a WriteBatch twice is undefined behaviour and is not exercised.
    /// </summary>
    [TestMethod]
    public void NativeBindingWriteBatchDisposeIsIdempotent()
    {
        var nb = new RocksDbSharp.WriteBatch();
        nb.Put(U("k"), U("v"));
        nb.Dispose();
        Assert.AreEqual(IntPtr.Zero, nb.Handle, "rocksdb-sharp clears the handle on Dispose");
        nb.Dispose();
        var mb = new Db.WriteBatch();
        mb.Put(U("k"), U("v"));
        mb.Dispose();
        mb.Dispose();
        Assert.ThrowsExactly<ObjectDisposedException>(() => mb.Put(U("k"), U("v")));
    }

    /// <summary>
    /// db/db_impl/db_impl_readonly.cc and db_impl_secondary.cc: a read-only or secondary open of a missing DB fails;
    /// writes, DeleteRange, Flush and CompactRange on a read-only instance are NotSupported ("Not supported operation in
    /// read only mode."); DB::TryCatchUpWithPrimary on a primary is NotSupported.
    /// </summary>
    [TestMethod]
    public void ReadOnlyAndSecondaryMisuseMatchesNative()
    {
        using var dir = new TempDir("a7-ro");
        string np = Path.Combine(dir.Path, "native"), mp = Path.Combine(dir.Path, "managed");
        var results = new List<(string What, StatusCode? Native, StatusCode? Managed)>();
        StatusCode? NTry(Action a)
        {
            try
            {
                a();
                return null;
            }
            catch (RocksDbSharp.RocksDbException e)
            {
                return NativeCode(e.Message);
            }
        }
        results.Add(("ro-missing", NTry(() => RocksDbSharp.RocksDb.OpenReadOnly(new RocksDbSharp.DbOptions(), np, false).Dispose()),
            ManagedCode(() => RocksDatabase.OpenReadOnly(mp, new DbOptions()).Dispose())));
        results.Add(("secondary-missing", NTry(() => RocksDbSharp.RocksDb.OpenAsSecondary(new RocksDbSharp.DbOptions(), np, np + "-sec").Dispose()),
            ManagedCode(() => RocksDatabase.OpenAsSecondary(mp, mp + "-sec", new DbOptions(), [new ColumnFamilyDescriptor("default", new ColumnFamilyOptions())]).Dispose())));
        using (RocksDbSharp.RocksDb db = OpenNative(np, "default"))
        using (RocksDatabase db2 = OpenManaged(mp))
        {
            db.Put(U("a"), U("1"));
            db2.Put(U("a"), U("1"));
            N.rocksdb_try_catch_up_with_primary(db.Handle, out IntPtr err);
            results.Add(("catch-up-on-primary", NativeCode(Err(ref err)), ManagedCode(() => db2.TryCatchUpWithPrimary())));
        }
        using (RocksDbSharp.RocksDb ro = RocksDbSharp.RocksDb.OpenReadOnly(new RocksDbSharp.DbOptions(), np, new RocksDbSharp.ColumnFamilies(), false))
        using (RocksDatabase mro = RocksDatabase.OpenReadOnly(mp, new DbOptions()))
        {
            var wo = new RocksDbSharp.WriteOptions();
            IntPtr cf = ro.GetDefaultColumnFamily().Handle;
            results.Add(("ro-put", NTry(() => ro.Put(U("a"), U("2"))), ManagedCode(() => mro.Put(U("a"), U("2")))));
            N.rocksdb_delete_range_cf(ro.Handle, wo.Handle, cf, U("a"), (UIntPtr)1, U("b"), (UIntPtr)1, out IntPtr err);
            results.Add(("ro-deleterange", NativeCode(Err(ref err)), ManagedCode(() => mro.DeleteRange(U("a"), U("b")))));
            N.rocksdb_flush_cf(ro.Handle, new RocksDbSharp.FlushOptions().Handle, cf, out err);
            results.Add(("ro-flush", NativeCode(Err(ref err)), ManagedCode(() => mro.Flush())));
            Assert.AreEqual("1", ro.Get("a"));
            Assert.AreEqual("1", Encoding.UTF8.GetString(mro.Get(U("a"))!));
        }
        foreach ((string what, StatusCode? n, StatusCode? m) in results) Console.WriteLine(what + ": native=" + n + " managed=" + m);
        foreach ((string what, StatusCode? n, StatusCode? m) in results)
        {
            Assert.IsNotNull(n, what);
            Assert.AreEqual(n, m, what);
        }
    }
}
