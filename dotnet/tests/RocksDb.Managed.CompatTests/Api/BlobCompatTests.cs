using System.Buffers;
using System.Globalization;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Db;
using static RocksDb.Managed.CompatTests.Wal.NativeDbHelper;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>
/// Integrated BlobDB compatibility with native RocksDB (librocksdb 11.12 through rocksdb-sharp; blob options are set
/// with the C API on the native option handles): blob DBs written, compacted and garbage collected by either side are
/// read and extended by the other, and the blob files written by a flush are byte-identical.
/// </summary>
[TestClass]
public sealed class BlobCompatTests
{
    private static readonly string[] s_cfs = ["default", "one"];
    private const ulong MinBlobSize = 64;

    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);

    private static string Key(int i) => "k" + i.ToString("D5", CultureInfo.InvariantCulture);

    /// <summary>Values alternate between inline (shorter than min_blob_size) and blob-sized ones.</summary>
    private static string Val(int i, string tag) =>
        i % 3 == 0 ? tag + "-s" + i.ToString(CultureInfo.InvariantCulture)
                   : tag + "-" + i.ToString(CultureInfo.InvariantCulture) + new string((char)('a' + i % 26), 100 + i % 300);

    /// <summary>Applies the blob settings to a native options handle (ColumnFamilyOptions / DbOptions).</summary>
    private static void SetNativeBlobOptions(IntPtr handle, CompressionType compression, bool gc, double ageCutoff)
    {
        RocksDbSharp.Native n = RocksDbSharp.Native.Instance;
        n.rocksdb_options_set_enable_blob_files(handle, 1);
        n.rocksdb_options_set_min_blob_size(handle, MinBlobSize);
        n.rocksdb_options_set_blob_file_size(handle, 64 * 1024); // several blob files per flush
        n.rocksdb_options_set_blob_compression_type(handle, (int)compression);
        n.rocksdb_options_set_enable_blob_gc(handle, gc ? (byte)1 : (byte)0);
        n.rocksdb_options_set_blob_gc_age_cutoff(handle, ageCutoff);
    }

    private static RocksDbSharp.RocksDb OpenNative(string path, CompressionType compression, bool gc)
    {
        RocksDbSharp.MergeOperator op = CreateConcatMergeOperator();
        var options = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetCreateMissingColumnFamilies(true);
        var cfs = new RocksDbSharp.ColumnFamilies();
        foreach (string name in s_cfs)
        {
            RocksDbSharp.ColumnFamilyOptions o = CfOptions(op);
            SetNativeBlobOptions(o.Handle, compression, gc, 0.5);
            cfs.Add(name, o);
        }
        return Open(path, options, cfs);
    }

    private static RocksDbSharp.RocksDb Open(string path, RocksDbSharp.DbOptions options, RocksDbSharp.ColumnFamilies cfs)
    {
        GC.KeepAlive(options);
        return RocksDbSharp.RocksDb.Open(options, path, cfs);
    }

    private static ColumnFamilyOptions ManagedCf(CompressionType compression, bool gc) => new()
    {
        MergeOperator = new StringAppend(),
        EnableBlobFiles = true,
        MinBlobSize = MinBlobSize,
        BlobFileSize = 64 * 1024,
        BlobCompressionType = compression,
        EnableBlobGarbageCollection = gc,
        BlobGarbageCollectionAgeCutoff = 0.5,
    };

    private static RocksDatabase OpenManaged(string path, CompressionType compression, bool gc)
    {
        ColumnFamilyOptions cfo = ManagedCf(compression, gc);
        return RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true, CreateMissingColumnFamilies = true },
            [new("default", cfo), new("one", cfo.Clone())]);
    }

    private sealed class StringAppend : MergeOperator
    {
        public override string Name => "StringAppendOperator";
        public override bool FullMerge(ReadOnlySpan<byte> key, bool hasExistingValue, ReadOnlySpan<byte> existingValue, in MergeOperands operands,
            System.Buffers.IBufferWriter<byte> result)
        {
            bool first = true;
            if (hasExistingValue)
            {
                result.Write(existingValue);
                first = false;
            }
            for (int i = 0; i < operands.Count; i++)
            {
                if (!first) result.Write(","u8);
                result.Write(operands[i]);
                first = false;
            }
            return true;
        }
    }

    /// <summary>Expected contents per (cf, key); null = deleted.</summary>
    private sealed class Model
    {
        public readonly SortedDictionary<string, string?>[] Cf = [new(StringComparer.Ordinal), new(StringComparer.Ordinal)];
    }

    // ---- workloads -------------------------------------------------------------------------------------

    private static void NativeRound(RocksDbSharp.RocksDb db, Model m, string tag, int n, bool flush)
    {
        for (int c = 0; c < s_cfs.Length; c++)
        {
            RocksDbSharp.ColumnFamilyHandle h = db.GetColumnFamily(s_cfs[c]);
            for (int i = 0; i < n; i++)
            {
                string v = Val(i, tag + c);
                db.Put(U(Key(i)), U(v), h);
                m.Cf[c][Key(i)] = v;
            }
            for (int i = 0; i < n; i += 7)
            {
                db.Remove(U(Key(i)), h);
                m.Cf[c][Key(i)] = null;
            }
            for (int i = 1; i < n; i += 11)
            {
                db.Merge(U(Key(i)), U("m" + tag), h);
                string? old = m.Cf[c][Key(i)];
                m.Cf[c][Key(i)] = old is null ? "m" + tag : old + ",m" + tag;
            }
        }
        if (flush)
        {
            var fo = new RocksDbSharp.FlushOptions().SetWaitForFlush(true);
            foreach (string cf in s_cfs) RocksDbSharp.Native.Instance.rocksdb_flush_cf(db.Handle, fo.Handle, db.GetColumnFamily(cf).Handle);
            GC.KeepAlive(fo);
        }
    }

    private static void ManagedRound(RocksDatabase db, Model m, string tag, int n, bool flush)
    {
        for (int c = 0; c < s_cfs.Length; c++)
        {
            ColumnFamily h = db.GetColumnFamily(s_cfs[c]);
            for (int i = 0; i < n; i++)
            {
                string v = Val(i, tag + c);
                h.Put(U(Key(i)), U(v));
                m.Cf[c][Key(i)] = v;
            }
            for (int i = 3; i < n; i += 13)
            {
                h.Delete(U(Key(i)));
                m.Cf[c][Key(i)] = null;
            }
            for (int i = 2; i < n; i += 9)
            {
                h.Merge(U(Key(i)), U("m" + tag));
                string? old = m.Cf[c][Key(i)];
                m.Cf[c][Key(i)] = old is null ? "m" + tag : old + ",m" + tag;
            }
            if (flush) h.Flush();
        }
    }

    private static void NativeForceGcCompact(RocksDbSharp.RocksDb db)
    {
        RocksDbSharp.Native n = RocksDbSharp.Native.Instance;
        IntPtr opt = n.rocksdb_compactoptions_create();
        try
        {
            n.rocksdb_compactoptions_set_blob_garbage_collection_policy(opt, 0); // kForce
            n.rocksdb_compactoptions_set_blob_garbage_collection_age_cutoff(opt, 1.0);
            foreach (string cf in s_cfs)
            {
                n.rocksdb_compact_range_cf_opt(db.Handle, db.GetColumnFamily(cf).Handle, opt, (byte[]?)null, UIntPtr.Zero, (byte[]?)null, UIntPtr.Zero);
            }
        }
        finally
        {
            n.rocksdb_compactoptions_destroy(opt);
        }
    }

    // ---- verification ----------------------------------------------------------------------------------

    private static void VerifyNative(RocksDbSharp.RocksDb db, Model m)
    {
        for (int c = 0; c < s_cfs.Length; c++)
        {
            RocksDbSharp.ColumnFamilyHandle h = db.GetColumnFamily(s_cfs[c]);
            foreach ((string k, string? v) in m.Cf[c])
            {
                byte[]? got = db.Get(U(k), h);
                Assert.AreEqual(v, got is null ? null : Encoding.UTF8.GetString(got), "native " + s_cfs[c] + "/" + k);
            }
            int live = 0;
            using RocksDbSharp.Iterator it = db.NewIterator(h);
            for (it.SeekToFirst(); it.Valid(); it.Next())
            {
                string k = Encoding.UTF8.GetString(it.Key());
                Assert.AreEqual(m.Cf[c][k], Encoding.UTF8.GetString(it.Value()), "native iter " + k);
                live++;
            }
            Assert.AreEqual(Live(m.Cf[c]), live);
        }
    }

    private static int Live(SortedDictionary<string, string?> d)
    {
        int n = 0;
        foreach (string? v in d.Values) if (v is not null) n++;
        return n;
    }

    private static async Task VerifyManagedAsync(RocksDatabase db, Model m)
    {
        for (int c = 0; c < s_cfs.Length; c++)
        {
            ColumnFamily h = db.GetColumnFamily(s_cfs[c]);
            foreach ((string k, string? v) in m.Cf[c])
            {
                byte[]? got = h.Get(U(k));
                Assert.AreEqual(v, got is null ? null : Encoding.UTF8.GetString(got), "managed " + s_cfs[c] + "/" + k);
                byte[]? asyncGot = await h.GetAsync(U(k));
                Assert.AreEqual(v, asyncGot is null ? null : Encoding.UTF8.GetString(asyncGot), "managed async " + k);
            }
            using (var batch = new MultiGetBatch())
            {
                foreach (string k in m.Cf[c].Keys) batch.Add(U(k));
                h.MultiGet(batch);
                int i = 0;
                foreach (string? v in m.Cf[c].Values)
                {
                    Assert.AreEqual(v, batch.TryGetValue(i, out ReadOnlySpan<byte> got) ? Encoding.UTF8.GetString(got) : null, "multiget " + i);
                    i++;
                }
            }
            int live = 0;
            using (Iterator it = h.NewIterator(null))
            {
                for (it.SeekToFirst(); it.Valid; it.Next())
                {
                    string k = Encoding.UTF8.GetString(it.Key);
                    Assert.AreEqual(m.Cf[c][k], Encoding.UTF8.GetString(it.Value), "managed iter " + k);
                    live++;
                }
            }
            Assert.AreEqual(Live(m.Cf[c]), live);
            int back = 0;
            using (Iterator it = h.NewIterator(null))
            {
                for (it.SeekToLast(); it.Valid; it.Prev()) back++;
            }
            Assert.AreEqual(live, back);
        }
    }

    private static int BlobFileCount(string path)
    {
        int n = 0;
        foreach (string f in Directory.GetFiles(path, "*.blob")) n++;
        return n;
    }

    // ---- tests -----------------------------------------------------------------------------------------

    [TestMethod]
    [DataRow(CompressionType.None)]
    [DataRow(CompressionType.LZ4)]
    [DataRow(CompressionType.Zstd)]
    public async Task NativeBlobDbReadAndExtendedByManagedThenNative(CompressionType compression)
    {
        using var dir = new TempDir("blob-n");
        var m = new Model();
        string nativeBlobFiles;
        // Native: flushes into blob files, compaction with forced GC (relocation + garbage), a WAL-only tail.
        using (RocksDbSharp.RocksDb db = OpenNative(dir.Path, compression, gc: true))
        {
            NativeRound(db, m, "a", 600, flush: true);
            NativeRound(db, m, "b", 400, flush: true);
            db.CompactRange((byte[]?)null, null);
            NativeRound(db, m, "c", 300, flush: true);
            NativeForceGcCompact(db);
            NativeRound(db, m, "d", 200, flush: false);
            VerifyNative(db, m);
            nativeBlobFiles = db.GetProperty("rocksdb.num-blob-files");
            Assert.AreNotEqual("0", nativeBlobFiles);
        }
        Assert.IsTrue(BlobFileCount(dir.Path) > 0);

        // Managed: reads everything (blob files, garbage, WAL replay), keeps writing, compacting and collecting garbage.
        using (RocksDatabase db = OpenManaged(dir.Path, compression, gc: true))
        {
            await VerifyManagedAsync(db, m);
            ManagedRound(db, m, "e", 500, flush: true);
            db.CompactRange(new CompactRangeOptions { BlobGarbageCollectionPolicy = BlobGarbageCollectionPolicy.Force, BlobGarbageCollectionAgeCutoff = 1.0 },
                null, null);
            db.GetColumnFamily("one").CompactRange(new CompactRangeOptions { BlobGarbageCollectionPolicy = BlobGarbageCollectionPolicy.Force }, null, null);
            ManagedRound(db, m, "f", 250, flush: true);
            ManagedRound(db, m, "g", 100, flush: false);
            await VerifyManagedAsync(db, m);
            Assert.AreNotEqual("0", db.GetProperty(DbProperties.NumBlobFiles));
        }

        // Native: reopens the managed-written DB, verifies, compacts (GC) and writes more.
        using (RocksDbSharp.RocksDb db = OpenNative(dir.Path, compression, gc: true))
        {
            VerifyNative(db, m);
            NativeForceGcCompact(db);
            NativeRound(db, m, "h", 150, flush: true);
            VerifyNative(db, m);
        }
        using (RocksDatabase db = OpenManaged(dir.Path, compression, gc: true))
        {
            await VerifyManagedAsync(db, m);
        }
    }

    [TestMethod]
    [DataRow(CompressionType.None)]
    [DataRow(CompressionType.Snappy)]
    [DataRow(CompressionType.Zstd)]
    public async Task ManagedBlobDbReadByNative(CompressionType compression)
    {
        using var dir = new TempDir("blob-m");
        var m = new Model();
        using (RocksDatabase db = OpenManaged(dir.Path, compression, gc: false))
        {
            ManagedRound(db, m, "a", 700, flush: true);
            ManagedRound(db, m, "b", 300, flush: true);
            db.CompactRange(); // no GC: garbage is recorded
            ManagedRound(db, m, "c", 200, flush: true);
            ManagedRound(db, m, "d", 100, flush: false);
            await VerifyManagedAsync(db, m);
            Assert.IsTrue(ulong.Parse(db.GetProperty(DbProperties.LiveBlobFileGarbageSize)!, CultureInfo.InvariantCulture) > 0);
        }
        using (RocksDbSharp.RocksDb db = OpenNative(dir.Path, compression, gc: true))
        {
            VerifyNative(db, m);
            // Native's view of the managed blob metadata (garbage tracking) is consistent: GC compaction succeeds.
            NativeForceGcCompact(db);
            VerifyNative(db, m);
        }
        using (RocksDatabase db = OpenManaged(dir.Path, compression, gc: false))
        {
            await VerifyManagedAsync(db, m);
        }
    }

    /// <summary>Compressible but not trivial value of <paramref name="size"/> bytes.</summary>
    private static byte[] LargeValue(int i, int size)
    {
        var rnd = new Random(i);
        byte[] v = new byte[size];
        for (int p = 0; p < size; p++) v[p] = (byte)('a' + (rnd.Next(8) == 0 ? rnd.Next(26) : (p / 7 + i) % 26));
        return v;
    }

    /// <summary>
    /// zlib blobs and data blocks above 16 KiB (raw deflate read by native with window_bits -14): managed writes,
    /// native reads, compacts with relocation and writes more; managed reads native's large zlib blobs.
    /// </summary>
    [TestMethod]
    public void LargeZlibBlobsAndBlocksReadableBothWays()
    {
        using var dir = new TempDir("blob-zlib");
        int[] sizes = [100, 16 * 1024, 16 * 1024 + 1, 20_000, 40_000, 70_000, 150_000];
        var cfo = ManagedCf(CompressionType.Zlib, gc: false);
        cfo.MergeOperator = null;
        cfo.Compression = CompressionType.Zlib;
        cfo.TableOptions.BlockSize = 64 * 1024;
        using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions { CreateIfMissing = true }, cfo))
        {
            for (int i = 0; i < sizes.Length; i++) db.Put(U("m" + i), LargeValue(i, sizes[i]));
            // Small values land in 64 KiB zlib data blocks.
            for (int i = 0; i < 3000; i++) db.Put(U("s" + i.ToString("D5")), U(Val(i, "small")));
            db.Flush();
        }
        Assert.IsTrue(Directory.GetFiles(dir.Path, "*.blob").Length >= 1);

        var options = new RocksDbSharp.DbOptions();
        SetNativeBlobOptions(options.Handle, CompressionType.Zlib, gc: true, 1.0);
        RocksDbSharp.Native.Instance.rocksdb_options_set_compression(options.Handle, (int)CompressionType.Zlib);
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(options, dir.Path))
        {
            for (int i = 0; i < sizes.Length; i++) CollectionAssert.AreEqual(LargeValue(i, sizes[i]), db.Get(U("m" + i)), "managed zlib blob " + sizes[i]);
            for (int i = 0; i < 3000; i++) Assert.AreEqual(Val(i, "small"), Encoding.UTF8.GetString(db.Get(U("s" + i.ToString("D5")))));
            for (int i = 0; i < sizes.Length; i++) db.Put(U("n" + i), LargeValue(100 + i, sizes[i]));
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            db.CompactRange((byte[]?)null, null); // relocates every blob (age cutoff 1.0)
        }
        GC.KeepAlive(options);

        using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions(), cfo))
        {
            for (int i = 0; i < sizes.Length; i++)
            {
                CollectionAssert.AreEqual(LargeValue(i, sizes[i]), db.Get(U("m" + i)), "native-rewritten zlib blob " + sizes[i]);
                CollectionAssert.AreEqual(LargeValue(100 + i, sizes[i]), db.Get(U("n" + i)), "native zlib blob " + sizes[i]);
            }
            for (int i = 0; i < 3000; i++) Assert.AreEqual(Val(i, "small"), Encoding.UTF8.GetString(db.Get(U("s" + i.ToString("D5")))!));
        }
    }

    [TestMethod]
    [DataRow(CompressionType.None)]
    [DataRow(CompressionType.LZ4)]
    [DataRow(CompressionType.Zstd)]
    [DataRow(CompressionType.Snappy)]
    public void FlushedBlobFilesAreByteIdentical(CompressionType compression)
    {
        // Same keys and values flushed once by each engine: the blob files (header, records, footer) are identical.
        using var nd = new TempDir("blob-bn");
        using var md = new TempDir("blob-bm");
        var options = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        SetNativeBlobOptions(options.Handle, compression, gc: false, 0.25);
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(options, nd.Path))
        {
            for (int i = 0; i < 400; i++) db.Put(U(Key(i)), U(Val(i, "x")));
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
        }
        GC.KeepAlive(options);
        var cfo = ManagedCf(compression, gc: false);
        cfo.MergeOperator = null;
        using (RocksDatabase db = RocksDatabase.Open(md.Path, new DbOptions { CreateIfMissing = true }, cfo))
        {
            for (int i = 0; i < 400; i++) db.Put(U(Key(i)), U(Val(i, "x")));
            db.Flush();
        }
        string[] nf = Directory.GetFiles(nd.Path, "*.blob");
        string[] mf = Directory.GetFiles(md.Path, "*.blob");
        Array.Sort(nf, StringComparer.Ordinal);
        Array.Sort(mf, StringComparer.Ordinal);
        // Uncompressed, the 64 KiB blob_file_size splits the flush into several blob files (rollover is byte-identical too).
        Assert.IsTrue(compression == CompressionType.None ? nf.Length > 1 : nf.Length >= 1, "blob files: " + nf.Length);
        Assert.AreEqual(nf.Length, mf.Length);
        for (int i = 0; i < nf.Length; i++)
        {
            CollectionAssert.AreEqual(File.ReadAllBytes(nf[i]), File.ReadAllBytes(mf[i]), "blob file " + Path.GetFileName(nf[i]) + " vs " + Path.GetFileName(mf[i]));
        }
    }
}
