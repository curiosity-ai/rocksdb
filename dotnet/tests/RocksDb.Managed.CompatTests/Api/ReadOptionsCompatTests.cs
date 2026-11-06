using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Table.Filters;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>
/// Read options and read-path statistics compared with native RocksDB (rocksdb-sharp, the C API of the NuGet
/// librocksdb): read_tier statuses of Get / MultiGet / iterators per data location, tailing iterators,
/// value_size_soft_limit, per-read verify_checksums, deadline, and the BLOOM_FILTER_* / *_SEEK_FILTER* tickers and
/// filter presence with optimize_filters_for_hits on the same workload.
/// </summary>
[TestClass]
public sealed class ReadOptionsCompatTests
{
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    private static byte[] B(string s) => Encoding.UTF8.GetBytes(s);

    private static RocksDbSharp.RocksDb OpenNative(string path, Action<RocksDbSharp.DbOptions>? configure = null)
    {
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetCompression(RocksDbSharp.Compression.No);
        configure?.Invoke(o);
        s_keepAlive.Add(o);
        return RocksDbSharp.RocksDb.Open(o, path);
    }

    private static RocksDbSharp.ReadOptions NativeRead(int tier = 0)
    {
        var ro = new RocksDbSharp.ReadOptions();
        if (tier != 0) ro.SetReadTier(tier);
        s_keepAlive.Add(ro);
        return ro;
    }

    // ---- outcomes ----------------------------------------------------------------------------------------

    /// <summary>Outcome of a read: "found:value", "notfound", or the status code name.</summary>
    private static string Outcome(StatusCode code) => code.ToString();

    private static string NativeOutcome(string? error, byte[]? value)
    {
        if (error is null) return value is null ? "notfound" : "found:" + Encoding.UTF8.GetString(value);
        if (error.StartsWith("Result incomplete", StringComparison.Ordinal)) return Outcome(StatusCode.Incomplete);
        if (error.StartsWith("Not implemented", StringComparison.Ordinal)) return Outcome(StatusCode.NotSupported);
        if (error.StartsWith("Operation timed out", StringComparison.Ordinal)) return Outcome(StatusCode.TimedOut);
        if (error.StartsWith("Operation aborted", StringComparison.Ordinal)) return Outcome(StatusCode.Aborted);
        if (error.StartsWith("Corruption", StringComparison.Ordinal)) return Outcome(StatusCode.Corruption);
        return "error:" + error;
    }

    private static string NativeGet(RocksDbSharp.RocksDb db, RocksDbSharp.ReadOptions ro, string key)
    {
        try
        {
            return NativeOutcome(null, db.Get(B(key), null, ro));
        }
        catch (RocksDbSharp.RocksDbException e)
        {
            return NativeOutcome(e.Message, null);
        }
    }

    /// <summary>rocksdb_multi_get: the per-key outcomes.</summary>
    private static string[] NativeMultiGet(RocksDbSharp.RocksDb db, RocksDbSharp.ReadOptions ro, string[] keys)
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
                byte[] k = B(keys[i]);
                pins[i] = GCHandle.Alloc(k, GCHandleType.Pinned);
                kp[i] = pins[i].AddrOfPinnedObject();
                ks[i] = (UIntPtr)k.Length;
            }
            RocksDbSharp.Native.Instance.rocksdb_multi_get(db.Handle, ro.Handle, (UIntPtr)n, kp, ks, vals, vs, errs);
        }
        finally
        {
            foreach (GCHandle h in pins) if (h.IsAllocated) h.Free();
        }
        var result = new string[n];
        for (int i = 0; i < n; i++)
        {
            byte[]? v = null;
            if (vals[i] != IntPtr.Zero)
            {
                v = new byte[(int)vs[i]];
                Marshal.Copy(vals[i], v, 0, v.Length);
                RocksDbSharp.Native.Instance.rocksdb_free(vals[i]);
            }
            string? err = null;
            if (errs[i] != IntPtr.Zero)
            {
                err = Marshal.PtrToStringUTF8(errs[i]);
                RocksDbSharp.Native.Instance.rocksdb_free(errs[i]);
            }
            result[i] = NativeOutcome(err, v);
        }
        return result;
    }

    /// <summary>NewIterator + Seek(key): "found:value" when positioned at the key, else notfound / the status.</summary>
    private static string NativeSeek(RocksDbSharp.RocksDb db, RocksDbSharp.ReadOptions ro, string key)
    {
        using RocksDbSharp.Iterator it = db.NewIterator(null, ro);
        it.Seek(B(key));
        RocksDbSharp.Native.Instance.rocksdb_iter_get_error(it.Handle, out IntPtr err);
        if (err != IntPtr.Zero)
        {
            string msg = Marshal.PtrToStringUTF8(err)!;
            RocksDbSharp.Native.Instance.rocksdb_free(err);
            return NativeOutcome(msg, null);
        }
        return it.Valid() && it.Key().AsSpan().SequenceEqual(B(key)) ? NativeOutcome(null, it.Value()) : "notfound";
    }

    private static string ManagedGet(RocksDatabase db, ReadOptions ro, string key)
    {
        try
        {
            byte[]? v = db.Get(ro, B(key));
            return v is null ? "notfound" : "found:" + Encoding.UTF8.GetString(v);
        }
        catch (RocksDbException e)
        {
            return Outcome(e.Code);
        }
    }

    private static string[] ManagedMultiGet(RocksDatabase db, ReadOptions ro, string[] keys)
    {
        using var batch = new MultiGetBatch();
        foreach (string k in keys) batch.Add(B(k));
        db.MultiGet(ro, batch);
        var r = new string[keys.Length];
        for (int i = 0; i < keys.Length; i++)
        {
            r[i] = batch.GetStatus(i) switch
            {
                MultiGetStatus.Found => "found:" + Encoding.UTF8.GetString(batch.GetValue(i)),
                MultiGetStatus.NotFound => "notfound",
                _ => Outcome(batch.GetError(i)!.Code),
            };
        }
        return r;
    }

    private static string ManagedSeek(RocksDatabase db, ReadOptions ro, string key)
    {
        try
        {
            using Iterator it = db.NewIterator(ro);
            it.Seek(B(key));
            return it.Valid && it.Key.SequenceEqual(B(key)) ? "found:" + Encoding.UTF8.GetString(it.Value) : "notfound";
        }
        catch (RocksDbException e)
        {
            return Outcome(e.Code);
        }
    }

    // ---- read tier -------------------------------------------------------------------------------------------

    private static readonly string[] s_tierKeys = ["sst", "sst2", "mem", "nowal", "zzz"];

    /// <summary>
    /// ReadTier x {Get, MultiGet, iterator} x {table file (uncached), memtable, memtable without WAL, absent}: the
    /// statuses of both engines on the same LSM shape (each engine builds it: Put + Flush, reopen so no block is
    /// cached, Put, Put with DisableWal).
    /// </summary>
    [TestMethod]
    public void ReadTierStatusesMatchNative()
    {
        foreach (int tier in new[] { 0, 1, 2, 3 })
        {
            using var nativeDir = new TempDir("tier-n");
            using var managedDir = new TempDir("tier-m");
            var native = new List<string>();
            using (RocksDbSharp.RocksDb db = OpenNative(nativeDir.Path))
            {
                db.Put(B("sst"), B("s"));
                db.Put(B("sst2"), B("s2"));
                db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            }
            using (RocksDbSharp.RocksDb db = OpenNative(nativeDir.Path))
            {
                db.Put(B("mem"), B("m"));
                var noWal = new RocksDbSharp.WriteOptions().DisableWal(1);
                db.Put(B("nowal"), B("n"), null, noWal);
                RocksDbSharp.ReadOptions ro = NativeRead(tier);
                foreach (string k in s_tierKeys)
                {
                    native.Add("get " + k + " " + NativeGet(db, ro, k));
                    native.Add("seek " + k + " " + NativeSeek(db, ro, k));
                }
                native.AddRange(NativeMultiGet(db, ro, s_tierKeys).Select((o, i) => "mget " + s_tierKeys[i] + " " + o));
            }
            var managed = new List<string>();
            var cfo = new ColumnFamilyOptions { Compression = Managed.Compression.CompressionType.None };
            using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true }, cfo))
            {
                db.Put(B("sst"), B("s"));
                db.Put(B("sst2"), B("s2"));
                db.Flush();
            }
            using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions(), cfo))
            {
                db.Put(B("mem"), B("m"));
                db.Put(new WriteOptions { DisableWal = true }, B("nowal"), B("n"));
                var ro = new ReadOptions { ReadTier = (ReadTier)tier };
                foreach (string k in s_tierKeys)
                {
                    managed.Add("get " + k + " " + ManagedGet(db, ro, k));
                    managed.Add("seek " + k + " " + ManagedSeek(db, ro, k));
                }
                managed.AddRange(ManagedMultiGet(db, ro, s_tierKeys).Select((o, i) => "mget " + s_tierKeys[i] + " " + o));
            }
            CollectionAssert.AreEqual(native, managed, "tier " + tier + ":\n native: " + string.Join(", ", native) + "\nmanaged: " + string.Join(", ", managed));
        }
    }

    // ---- tailing, soft limit, verify_checksums, deadline -------------------------------------------------

    [TestMethod]
    public void TailingIteratorMatchesNative()
    {
        using var nativeDir = new TempDir("tail-n");
        using var managedDir = new TempDir("tail-m");
        var native = new List<string>();
        using (RocksDbSharp.RocksDb db = OpenNative(nativeDir.Path))
        {
            db.Put(B("a"), B("1"));
            RocksDbSharp.ReadOptions ro = NativeRead().SetTailing(true);
            using RocksDbSharp.Iterator it = db.NewIterator(null, ro);
            it.SeekToFirst();
            native.Add(it.Valid() ? Encoding.UTF8.GetString(it.Key()) : "-");
            db.Put(B("b"), B("2"));
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            db.Put(B("c"), B("3"));
            for (it.Seek(B("a")); it.Valid(); it.Next()) native.Add(Encoding.UTF8.GetString(it.Key()) + "=" + Encoding.UTF8.GetString(it.Value()));
            it.Seek(B("b"));
            it.Prev();
            RocksDbSharp.Native.Instance.rocksdb_iter_get_error(it.Handle, out IntPtr err);
            native.Add(err == IntPtr.Zero ? "prev ok" : NativeOutcome(Marshal.PtrToStringUTF8(err), null));
        }
        var managed = new List<string>();
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true }))
        {
            db.Put(B("a"), B("1"));
            using Iterator it = db.NewIterator(new ReadOptions { Tailing = true });
            it.SeekToFirst();
            managed.Add(it.Valid ? Encoding.UTF8.GetString(it.Key) : "-");
            db.Put(B("b"), B("2"));
            db.Flush();
            db.Put(B("c"), B("3"));
            for (it.Seek(B("a")); it.Valid; it.Next()) managed.Add(Encoding.UTF8.GetString(it.Key) + "=" + Encoding.UTF8.GetString(it.Value));
            it.Seek(B("b"));
            try
            {
                it.Prev();
                managed.Add("prev ok");
            }
            catch (RocksDbException e)
            {
                managed.Add(Outcome(e.Code));
            }
        }
        CollectionAssert.AreEqual(native, managed, string.Join(", ", native) + " vs " + string.Join(", ", managed));
        CollectionAssert.AreEqual(new[] { "a", "a=1", "b=2", "c=3", "NotSupported" }, managed);
    }

    [TestMethod]
    public void ValueSizeSoftLimitMatchesNative()
    {
        string[] keys = ["k0", "k1", "k2", "k3", "k4", "k5", "missing"];
        foreach (bool flush in new[] { false, true })
        {
            using var nativeDir = new TempDir("soft-n");
            using var managedDir = new TempDir("soft-m");
            string[] native, managed;
            using (RocksDbSharp.RocksDb db = OpenNative(nativeDir.Path))
            {
                for (int i = 0; i < 6; i++) db.Put(B("k" + i), B(new string((char)('a' + i), 10)));
                if (flush) db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
                RocksDbSharp.ReadOptions ro = NativeRead();
                RocksDbSharp.Native.Instance.rocksdb_readoptions_set_value_size_soft_limit(ro.Handle, 25);
                native = NativeMultiGet(db, ro, keys);
            }
            using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true }))
            {
                for (int i = 0; i < 6; i++) db.Put(B("k" + i), B(new string((char)('a' + i), 10)));
                if (flush) db.Flush();
                managed = ManagedMultiGet(db, new ReadOptions { ValueSizeSoftLimit = 25 }, keys);
            }
            CollectionAssert.AreEqual(native, managed, "flush " + flush + ": " + string.Join(", ", native) + " vs " + string.Join(", ", managed));
        }
    }

    [TestMethod]
    public void VerifyChecksumsPerReadMatchesNative()
    {
        using var dir = new TempDir("verify");
        var cfo = new ColumnFamilyOptions { Compression = Managed.Compression.CompressionType.None };
        using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions { CreateIfMissing = true }, cfo))
        {
            db.Put(B("key"), B("VALUE-MARKER-0123456789"));
            db.Flush();
        }
        string sst = Directory.GetFiles(dir.Path, "*.sst").Single();
        byte[] bytes = File.ReadAllBytes(sst);
        int at = bytes.AsSpan().IndexOf("VALUE-MARKER"u8);
        bytes[at] = (byte)'X';
        File.WriteAllBytes(sst, bytes);

        var native = new List<string>();
        using (RocksDbSharp.RocksDb db = OpenNative(dir.Path))
        {
            native.Add(NativeGet(db, NativeRead(), "key"));
            RocksDbSharp.ReadOptions noVerify = NativeRead().SetVerifyChecksums(false).SetFillCache(false);
            native.Add(NativeGet(db, noVerify, "key"));
            native.Add(NativeSeek(db, noVerify, "key"));
            native.Add(NativeMultiGet(db, noVerify, ["key"])[0]);
        }
        var managed = new List<string>();
        using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions(), cfo))
        {
            managed.Add(ManagedGet(db, new ReadOptions(), "key"));
            var noVerify = new ReadOptions { VerifyChecksums = false, FillCache = false };
            managed.Add(ManagedGet(db, noVerify, "key"));
            managed.Add(ManagedSeek(db, noVerify, "key"));
            managed.Add(ManagedMultiGet(db, noVerify, ["key"])[0]);
        }
        CollectionAssert.AreEqual(native, managed, string.Join(", ", native) + " vs " + string.Join(", ", managed));
        Assert.AreEqual("Corruption", managed[0]);
        Assert.AreEqual("found:XALUE-MARKER-0123456789", managed[1]);
    }

    [TestMethod]
    public void DeadlineMatchesNative()
    {
        using var nativeDir = new TempDir("deadline-n");
        using var managedDir = new TempDir("deadline-m");
        string[] keys = ["mem", "sst"];
        var native = new List<string>();
        using (RocksDbSharp.RocksDb db = OpenNative(nativeDir.Path)) { db.Put(B("sst"), B("s")); db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true)); }
        using (RocksDbSharp.RocksDb db = OpenNative(nativeDir.Path))
        {
            db.Put(B("mem"), B("m"));
            RocksDbSharp.ReadOptions ro = NativeRead();
            RocksDbSharp.Native.Instance.rocksdb_readoptions_set_deadline(ro.Handle, 1);
            native.Add(NativeGet(db, ro, "mem"));
            native.Add(NativeGet(db, ro, "sst"));
            native.Add(NativeSeek(db, ro, "sst"));
            native.AddRange(NativeMultiGet(db, ro, keys));
            native.Add(NativeGet(db, NativeRead(), "sst"));
            native.Add(NativeGet(db, ro, "sst")); // cached now
        }
        var managed = new List<string>();
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true })) { db.Put(B("sst"), B("s")); db.Flush(); }
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions()))
        {
            db.Put(B("mem"), B("m"));
            var ro = new ReadOptions { Deadline = TimeSpan.FromMicroseconds(1) };
            managed.Add(ManagedGet(db, ro, "mem"));
            managed.Add(ManagedGet(db, ro, "sst"));
            managed.Add(ManagedSeek(db, ro, "sst"));
            managed.AddRange(ManagedMultiGet(db, ro, keys));
            managed.Add(ManagedGet(db, new ReadOptions(), "sst"));
            managed.Add(ManagedGet(db, ro, "sst"));
        }
        CollectionAssert.AreEqual(native, managed, string.Join(", ", native) + " vs " + string.Join(", ", managed));
    }

    // ---- verify_compression, detect_filter_construct_corruption, read_amp_bytes_per_bit ------------------------

    /// <summary>
    /// The three table options round-trip through each side's OPTIONS file, DBs written with verify_compression and
    /// detect_filter_construct_corruption read back on the other side, and read_amp_bytes_per_bit = 1 reports the same
    /// READ_AMP_TOTAL_READ_BYTES / READ_AMP_ESTIMATE_USEFUL_BYTES as native for the same workload (all useful once every
    /// entry was read).
    /// </summary>
    [TestMethod]
    public void TableSafetyOptionsMatchNative()
    {
        using var nativeDir = new TempDir("tsafe-n");
        using var managedDir = new TempDir("tsafe-m");
        const int n = 3000;
        static byte[] Value(int i) => Encoding.UTF8.GetBytes("value-" + i.ToString(CultureInfo.InvariantCulture) + new string('x', i % 37));
        string nativeText;
        {
            var no = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetCompression(RocksDbSharp.Compression.Snappy).EnableStatistics();
            var bbto = new RocksDbSharp.BlockBasedTableOptions().SetFilterPolicy(RocksDbSharp.BloomFilterPolicy.Create(10, false)).SetFormatVersion(7);
            RocksDbSharp.Native.Instance.rocksdb_block_based_options_set_verify_compression(bbto.Handle, 1);
            RocksDbSharp.Native.Instance.rocksdb_block_based_options_set_detect_filter_construct_corruption(bbto.Handle, 1);
            RocksDbSharp.Native.Instance.rocksdb_block_based_options_set_read_amp_bytes_per_bit(bbto.Handle, 1);
            no.SetBlockBasedTableFactory(bbto);
            s_keepAlive.Add(no);
            s_keepAlive.Add(bbto);
            using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(no, nativeDir.Path))
            {
                for (int i = 0; i < n; i++) db.Put(B(K(i)), Value(i));
                db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            }
            using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(no, nativeDir.Path))
            {
                ulong before = TickerValue(no.GetStatisticsString(), "rocksdb.read.amp.total.read.bytes");
                Assert.AreEqual(0UL, before);
                for (int i = 0; i < n; i++) db.Get(B(K(i)));
                nativeText = no.GetStatisticsString();
            }
        }
        var stats = new Statistics();
        var cfo = new ColumnFamilyOptions { Compression = Managed.Compression.CompressionType.Snappy };
        cfo.TableOptions.FilterPolicy = new BloomFilterPolicy(10);
        cfo.TableOptions.FormatVersion = 7;
        cfo.TableOptions.VerifyCompression = true;
        cfo.TableOptions.DetectFilterConstructCorruption = true;
        cfo.TableOptions.ReadAmpBytesPerBit = 1;
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true, Statistics = stats }, cfo))
        {
            for (int i = 0; i < n; i++) db.Put(B(K(i)), Value(i));
            db.Flush();
        }
        stats.Reset();
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { Statistics = stats }, cfo))
        {
            for (int i = 0; i < n; i++) db.Get(B(K(i)));
        }
        string managedText = stats.ToString();
        ulong nativeTotal = TickerValue(nativeText, "rocksdb.read.amp.total.read.bytes");
        ulong managedTotal = TickerValue(managedText, "rocksdb.read.amp.total.read.bytes");
        Assert.IsTrue(managedTotal > 0);
        Assert.AreEqual(nativeTotal, managedTotal, "READ_AMP_TOTAL_READ_BYTES");
        Assert.AreEqual(TickerValue(nativeText, "rocksdb.read.amp.estimate.useful.bytes"), TickerValue(managedText, "rocksdb.read.amp.estimate.useful.bytes"));
        Assert.AreEqual(managedTotal, TickerValue(managedText, "rocksdb.read.amp.estimate.useful.bytes"));

        // OPTIONS files: each side's values as the other parses them; data readable both ways.
        foreach (string dir in new[] { nativeDir.Path, managedDir.Path })
        {
            OptionsFile f = OptionsFile.Load(Managed.Env.LocalFileSystem.Instance, OptionsFile.FindLatestOptionsFile(Managed.Env.LocalFileSystem.Instance, dir)!, preserveUnknownOptions: true);
            BlockBasedTableOptions t = f.ColumnFamilies[0].Options.TableOptions;
            Assert.IsTrue(t.VerifyCompression, dir);
            Assert.IsTrue(t.DetectFilterConstructCorruption, dir);
            Assert.AreEqual(1U, t.ReadAmpBytesPerBit, dir);
        }
        using (RocksDbSharp.RocksDb db = OpenNative(managedDir.Path))
        {
            Assert.AreEqual(Encoding.UTF8.GetString(Value(77)), Encoding.UTF8.GetString(db.Get(B(K(77)))));
        }
        using (RocksDatabase db = RocksDatabase.Open(nativeDir.Path, new DbOptions(), cfo))
        {
            Assert.AreEqual(Encoding.UTF8.GetString(Value(78)), Encoding.UTF8.GetString(db.Get(B(K(78)))!));
        }
    }

    private static string K(int i) => "key" + i.ToString("D6", CultureInfo.InvariantCulture);

    // ---- row cache --------------------------------------------------------------------------------------------

    private static void RowCacheWorkload(Action<string> put, Action flush, Func<string, string?> get, Func<Snapshot?> snapshot, Func<string, string?> getAtSnapshot)
    {
        put("foo=bar1");
        snapshot();
        put("foo=bar2");
        flush();
        put("foo2=bar");
        for (int pass = 0; pass < 2; pass++)
        {
            Assert.AreEqual("bar2", get("foo"));
            Assert.AreEqual("bar1", getAtSnapshot("foo"));
            Assert.IsNull(get("nope"));
            Assert.AreEqual("bar", get("foo2"));
        }
    }

    /// <summary>
    /// DBOptions::row_cache: the same Get workload (with a snapshot read) gives the same ROW_CACHE_HIT / ROW_CACHE_MISS
    /// counts as native, and a DB used with a row cache round-trips with native (row_cache is not an OPTIONS file
    /// entry on either side, so each side opens the other's OPTIONS file).
    /// </summary>
    [TestMethod]
    public void RowCacheTickersAndOptionsRoundTripMatchNative()
    {
        using var nativeDir = new TempDir("rowcache-n");
        using var managedDir = new TempDir("rowcache-m");
        var no = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).EnableStatistics();
        IntPtr nativeCache = RocksDbSharp.Native.Instance.rocksdb_cache_create_lru((UIntPtr)(1 << 20));
        RocksDbSharp.Native.Instance.rocksdb_options_set_row_cache(no.Handle, nativeCache);
        s_keepAlive.Add(no);
        string nativeText;
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(no, nativeDir.Path))
        {
            RocksDbSharp.Snapshot? snap = null;
            RowCacheWorkload(
                kv => db.Put(B(kv.Split('=')[0]), B(kv.Split('=')[1])),
                () => db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true)),
                k => db.Get(B(k)) is { } v ? Encoding.UTF8.GetString(v) : null,
                () =>
                {
                    snap = db.CreateSnapshot();
                    return null;
                },
                k => db.Get(B(k), null, NativeRead().SetSnapshot(snap!)) is { } v ? Encoding.UTF8.GetString(v) : null);
            nativeText = no.GetStatisticsString();
            snap!.Dispose();
        }
        var stats = new Statistics();
        var rowCache = new Cache.RowCache(1 << 20);
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true, Statistics = stats, RowCache = rowCache }))
        {
            Snapshot? snap = null;
            RowCacheWorkload(
                kv => db.Put(B(kv.Split('=')[0]), B(kv.Split('=')[1])),
                db.Flush,
                k => db.Get(B(k)) is { } v ? Encoding.UTF8.GetString(v) : null,
                () => snap = db.GetSnapshot(),
                k => db.Get(new ReadOptions { Snapshot = snap }, B(k)) is { } v ? Encoding.UTF8.GetString(v) : null);
            snap!.Dispose();
        }
        string managedText = stats.ToString();
        foreach (string name in new[] { "rocksdb.row.cache.hit", "rocksdb.row.cache.miss" })
        {
            Assert.AreEqual(TickerValue(nativeText, name), TickerValue(managedText, name), name);
        }
        Assert.IsTrue(TickerValue(managedText, "rocksdb.row.cache.hit") > 0);

        // Round trip: native reads the managed DB (and writes to it), managed reads it back with its row cache.
        using (RocksDbSharp.RocksDb db = OpenNative(managedDir.Path))
        {
            Assert.AreEqual("bar2", Encoding.UTF8.GetString(db.Get(B("foo"))));
            db.Put(B("foo"), B("bar3"));
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
        }
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { RowCache = rowCache }))
        {
            for (int i = 0; i < 2; i++)
            {
                Assert.AreEqual("bar3", Encoding.UTF8.GetString(db.Get(B("foo"))!));
                Assert.AreEqual("bar", Encoding.UTF8.GetString(db.Get(B("foo2"))!));
            }
        }
        using (RocksDatabase db = RocksDatabase.Open(nativeDir.Path, new DbOptions { RowCache = new Cache.RowCache(1 << 20) }))
        {
            Assert.AreEqual("bar2", Encoding.UTF8.GetString(db.Get(B("foo"))!));
            Assert.AreEqual("bar2", Encoding.UTF8.GetString(db.Get(B("foo"))!));
        }
    }

    // ---- filters and statistics ---------------------------------------------------------------------------

    private static ulong TickerValue(string statistics, string name)
    {
        Match m = Regex.Match(statistics, "^" + Regex.Escape(name) + " COUNT : (\\d+)$", RegexOptions.Multiline);
        Assert.IsTrue(m.Success, name);
        return ulong.Parse(m.Groups[1].Value, CultureInfo.InvariantCulture);
    }

    private static readonly string[] s_filterTickers =
    [
        "rocksdb.bloom.filter.useful", "rocksdb.bloom.filter.full.positive", "rocksdb.bloom.filter.full.true.positive",
        "rocksdb.bloom.filter.prefix.checked", "rocksdb.bloom.filter.prefix.useful", "rocksdb.bloom.filter.prefix.true.positive",
        "rocksdb.non.last.level.seek.filtered", "rocksdb.non.last.level.seek.filter.match",
        "rocksdb.last.level.seek.filtered", "rocksdb.last.level.seek.filter.match",
    ];

    private static IEnumerable<string> FilterWorkloadKeys()
    {
        for (int i = 0; i < 100; i++) yield return "aaa" + i.ToString("D2", CultureInfo.InvariantCulture);
        for (int i = 0; i < 100; i++) yield return "ccc" + i.ToString("D2", CultureInfo.InvariantCulture);
    }

    private static readonly string[] s_gets = ["aaa05", "aaa77", "ccc10", "aaa5x", "bbb00", "bbb50", "ccc9z", "abc00"];
    private static readonly string[] s_seeks = ["aaa10", "bbb00", "bbb99", "ccc00", "abc12", "ddd00"];

    /// <summary>
    /// The filter tickers of Get / MultiGet / prefix seeks on an L0 file, then (after a full compaction) on the last
    /// level, with whole-key filtering on and off, are those of native RocksDB on the same workload.
    /// </summary>
    [TestMethod]
    public void FilterTickersMatchNative()
    {
        foreach (bool wholeKey in new[] { true, false })
        {
            using var nativeDir = new TempDir("ftick-n");
            using var managedDir = new TempDir("ftick-m");
            var no = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetCompression(RocksDbSharp.Compression.No).EnableStatistics();
            var bbto = new RocksDbSharp.BlockBasedTableOptions().SetFilterPolicy(RocksDbSharp.BloomFilterPolicy.Create(10, false)).SetWholeKeyFiltering(wholeKey);
            no.SetBlockBasedTableFactory(bbto).SetPrefixExtractor(RocksDbSharp.SliceTransform.CreateFixedPrefix(3));
            s_keepAlive.Add(no);
            s_keepAlive.Add(bbto);
            string nativeText;
            using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(no, nativeDir.Path))
            {
                foreach (string k in FilterWorkloadKeys()) db.Put(B(k), B("v" + k));
                db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
                RunNativeFilterWorkload(db);
                // A second, overlapping L0 file: the full compaction rewrites both into the last level.
                db.Put(B("aaa00"), B("again"));
                db.Put(B("ccc99"), B("again"));
                db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
                db.CompactRange(Array.Empty<byte>(), null);
                RunNativeFilterWorkload(db);
                nativeText = no.GetStatisticsString();
            }
            var stats = new Statistics();
            var cfo = new ColumnFamilyOptions { Compression = Managed.Compression.CompressionType.None, PrefixExtractor = SliceTransform.CreateFixedPrefix(3) };
            cfo.TableOptions.FilterPolicy = new BloomFilterPolicy(10);
            cfo.TableOptions.WholeKeyFiltering = wholeKey;
            using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true, Statistics = stats }, cfo))
            {
                foreach (string k in FilterWorkloadKeys()) db.Put(B(k), B("v" + k));
                db.Flush();
                RunManagedFilterWorkload(db);
                db.Put(B("aaa00"), B("again"));
                db.Put(B("ccc99"), B("again"));
                db.Flush();
                db.CompactRange(null, null);
                RunManagedFilterWorkload(db);
            }
            string managedText = stats.ToString();
            var diffs = new List<string>();
            foreach (string name in s_filterTickers)
            {
                ulong n = TickerValue(nativeText, name), m = TickerValue(managedText, name);
                if (n != m) diffs.Add(name + " native " + n + " managed " + m);
            }
            Assert.AreEqual(0, diffs.Count, "whole_key_filtering " + wholeKey + ": " + string.Join("; ", diffs));
            Assert.IsTrue(TickerValue(managedText, "rocksdb.non.last.level.seek.filtered") > 0);
            Assert.IsTrue(TickerValue(managedText, "rocksdb.last.level.seek.filtered") > 0);
        }
    }

    private static void RunNativeFilterWorkload(RocksDbSharp.RocksDb db)
    {
        foreach (string k in s_gets) db.Get(B(k));
        NativeMultiGet(db, NativeRead(), s_gets);
        using RocksDbSharp.Iterator it = db.NewIterator();
        foreach (string k in s_seeks) it.Seek(B(k));
    }

    private static void RunManagedFilterWorkload(RocksDatabase db)
    {
        foreach (string k in s_gets) db.Get(B(k));
        ManagedMultiGet(db, new ReadOptions(), s_gets);
        using Iterator it = db.NewIterator();
        foreach (string k in s_seeks) it.Seek(B(k));
    }

    /// <summary>
    /// optimize_filters_for_hits: the bottommost output of a compaction has no filter on both engines, the flushed L0
    /// file has one, and lookups on the last non-empty level skip the filter (the same BLOOM_FILTER_* counts).
    /// </summary>
    [TestMethod]
    public void OptimizeFiltersForHitsMatchesNative()
    {
        using var nativeDir = new TempDir("ofh-n");
        using var managedDir = new TempDir("ofh-m");
        var no = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetCompression(RocksDbSharp.Compression.No).EnableStatistics()
            .SetOptimizeFiltersForHits(1);
        var bbto = new RocksDbSharp.BlockBasedTableOptions().SetFilterPolicy(RocksDbSharp.BloomFilterPolicy.Create(10, false));
        no.SetBlockBasedTableFactory(bbto);
        s_keepAlive.Add(no);
        s_keepAlive.Add(bbto);
        string nativeText;
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(no, nativeDir.Path))
        {
            foreach (string k in FilterWorkloadKeys()) db.Put(B(k), B("v" + k));
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            foreach (string k in s_gets) db.Get(B(k)); // single L0 file = last non-empty level: filter skipped
            db.Put(B("aaa00"), B("again"));
            db.Put(B("ccc99"), B("again"));
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            foreach (string k in s_gets) db.Get(B(k)); // the newer L0 file is not the last of the level: filter checked
            db.CompactRange(Array.Empty<byte>(), null);
            for (int i = 0; i < 10; i++) db.Put(B("aaa" + i.ToString("D2", CultureInfo.InvariantCulture) + "x"), B("new"));
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            foreach (string k in s_gets) db.Get(B(k)); // L0 filter checked, last level not
            nativeText = no.GetStatisticsString();
        }
        var stats = new Statistics();
        var cfo = new ColumnFamilyOptions { Compression = Managed.Compression.CompressionType.None, OptimizeFiltersForHits = true };
        cfo.TableOptions.FilterPolicy = new BloomFilterPolicy(10);
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true, Statistics = stats }, cfo))
        {
            foreach (string k in FilterWorkloadKeys()) db.Put(B(k), B("v" + k));
            db.Flush();
            foreach (string k in s_gets) db.Get(B(k));
            db.Put(B("aaa00"), B("again"));
            db.Put(B("ccc99"), B("again"));
            db.Flush();
            foreach (string k in s_gets) db.Get(B(k));
            db.CompactRange(null, null);
            for (int i = 0; i < 10; i++) db.Put(B("aaa" + i.ToString("D2", CultureInfo.InvariantCulture) + "x"), B("new"));
            db.Flush();
            foreach (string k in s_gets) db.Get(B(k));
        }
        string managedText = stats.ToString();
        foreach (string name in s_filterTickers)
        {
            Assert.AreEqual(TickerValue(nativeText, name), TickerValue(managedText, name), name);
        }
        // Filter presence per file: the L0 flush output has a filter, the bottommost compaction output has none.
        Dictionary<int, ulong> FilterSizes(string dir)
        {
            var r = new Dictionary<int, ulong>();
            using var md = RocksDatabase.Open(dir, new DbOptions(), cfo);
            Dictionary<string, Managed.Table.TableProperties> props = md.GetPropertiesOfAllTables();
            foreach (LiveFileMetadata f in md.GetLiveFilesMetadata())
            {
                Managed.Table.TableProperties p = props.First(kv => kv.Key.EndsWith(f.Name.TrimStart('/'), StringComparison.Ordinal)).Value;
                r[f.Level] = p.FilterSize;
            }
            return r;
        }
        Dictionary<int, ulong> n = FilterSizes(nativeDir.Path), m = FilterSizes(managedDir.Path);
        CollectionAssert.AreEquivalent(n.Keys.ToArray(), m.Keys.ToArray());
        foreach ((int level, ulong size) in n) Assert.AreEqual(size == 0, m[level] == 0, "filter presence at L" + level);
        Assert.IsTrue(m[0] > 0);
        Assert.AreEqual(0UL, m.Where(kv => kv.Key != 0).Single().Value);
    }
}
