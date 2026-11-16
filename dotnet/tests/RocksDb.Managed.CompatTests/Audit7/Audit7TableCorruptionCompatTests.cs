using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Table;
using RocksDb.Managed.Table.Filters;
using RocksDb.Managed.Util;
using CompressionType = RocksDb.Managed.Compression.CompressionType;

namespace RocksDb.Managed.CompatTests.Audit7;

/// <summary>
/// Audit #7, area TableCorruption: damaged SST files read by native RocksDB (the NuGet librocksdb) and by the managed
/// port side by side. Each test names the C++ function whose behavior it pins.
/// </summary>
[TestClass]
public sealed unsafe class Audit7TableCorruptionCompatTests
{
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    private static byte[] B(string s) => Encoding.ASCII.GetBytes(s);

    private static string K(int i) => "key" + i.ToString("D6", System.Globalization.CultureInfo.InvariantCulture);

    private static byte[] V(int i) => B("value-" + i.ToString(System.Globalization.CultureInfo.InvariantCulture) + new string((char)('a' + i % 26), 20 + i % 50));

    private static readonly Lazy<IntPtr> s_library = new(() =>
    {
        _ = RocksDbSharp.Native.Instance;
        foreach (string line in File.ReadLines("/proc/self/maps"))
        {
            int slash = line.IndexOf('/', StringComparison.Ordinal);
            if (slash >= 0 && Path.GetFileName(line[slash..]).StartsWith("librocksdb", StringComparison.Ordinal)) return NativeLibrary.Load(line[slash..]);
        }
        throw new InvalidOperationException("librocksdb is not loaded");
    });

    /// <summary>rocksdb_verify_checksum (DB::VerifyChecksum): null when OK, else the status message.</summary>
    private static string? NativeVerifyChecksum(RocksDbSharp.RocksDb db)
    {
        IntPtr err = IntPtr.Zero;
        ((delegate* unmanaged<IntPtr, IntPtr*, void>)NativeLibrary.GetExport(s_library.Value, "rocksdb_verify_checksum"))(db.Handle, &err);
        if (err == IntPtr.Zero) return null;
        string msg = Marshal.PtrToStringUTF8(err)!;
        RocksDbSharp.Native.Instance.rocksdb_free(err);
        return msg;
    }

    /// <summary>The status code name a native status message starts with ("OK" for none).</summary>
    private static string NativeCode(string? message)
    {
        if (message is null) return "OK";
        if (message.StartsWith("Corruption", StringComparison.Ordinal)) return nameof(StatusCode.Corruption);
        if (message.StartsWith("IO error", StringComparison.Ordinal)) return nameof(StatusCode.IOError);
        if (message.StartsWith("Not implemented", StringComparison.Ordinal)) return nameof(StatusCode.NotSupported);
        if (message.StartsWith("Invalid argument", StringComparison.Ordinal)) return nameof(StatusCode.InvalidArgument);
        return "other:" + message;
    }

    private static RocksDbSharp.DbOptions NativeOptions(RocksDbSharp.Compression compression, int formatVersion, char checksum, uint maxDictBytes, bool bloom)
    {
        var bbto = new RocksDbSharp.BlockBasedTableOptions().SetFormatVersion(formatVersion).SetBlockSize(1024);
        RocksDbSharp.Native.Instance.rocksdb_block_based_options_set_checksum(bbto.Handle, checksum);
        if (bloom) bbto.SetFilterPolicy(RocksDbSharp.BloomFilterPolicy.Create(10, false));
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetCompression(compression);
        o.SetBlockBasedTableFactory(bbto);
        // window_bits, level, strategy, max_dict_bytes
        RocksDbSharp.Native.Instance.rocksdb_options_set_compression_options(o.Handle, -14, 32767, 0, (int)maxDictBytes);
        s_keepAlive.Add(bbto);
        s_keepAlive.Add(o);
        return o;
    }

    private static ColumnFamilyOptions ManagedOptions(CompressionType compression, int formatVersion, ChecksumType checksum, uint maxDictBytes, bool bloom)
    {
        var cfo = new ColumnFamilyOptions { Compression = compression };
        cfo.TableOptions.FormatVersion = (uint)formatVersion;
        cfo.TableOptions.BlockSize = 1024;
        cfo.TableOptions.Checksum = checksum;
        cfo.CompressionOpts.MaxDictBytes = maxDictBytes;
        if (bloom) cfo.TableOptions.FilterPolicy = new BloomFilterPolicy(10);
        return cfo;
    }

    private static RocksDbSharp.Compression NativeCompression(CompressionType t) => t switch
    {
        CompressionType.Snappy => RocksDbSharp.Compression.Snappy,
        CompressionType.LZ4 => RocksDbSharp.Compression.Lz4,
        CompressionType.Zstd => RocksDbSharp.Compression.Zstd,
        CompressionType.Zlib => RocksDbSharp.Compression.Zlib,
        _ => RocksDbSharp.Compression.No,
    };

    /// <summary>A native DB with <paramref name="n"/> keys flushed to one SST; returns the SST path.</summary>
    private static string WriteNativeDb(string dir, RocksDbSharp.DbOptions o, int n)
    {
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(o, dir))
        {
            for (int i = 0; i < n; i++) db.Put(B(K(i)), V(i));
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
        }
        return Directory.GetFiles(dir, "*.sst").Single();
    }

    private static string NativeGet(RocksDbSharp.RocksDb db, RocksDbSharp.ReadOptions ro, string key)
    {
        try
        {
            byte[]? v = db.Get(B(key), null, ro);
            return v is null ? "notfound" : "found:" + Encoding.ASCII.GetString(v);
        }
        catch (RocksDbSharp.RocksDbException e)
        {
            return NativeCode(e.Message);
        }
    }

    private static string ManagedGet(RocksDatabase db, ReadOptions ro, string key)
    {
        try
        {
            byte[]? v = db.Get(ro, B(key));
            return v is null ? "notfound" : "found:" + Encoding.ASCII.GetString(v);
        }
        catch (RocksDbException e)
        {
            return e.Code.ToString();
        }
    }

    /// <summary>
    /// BlockBasedTable::VerifyChecksum / VerifyChecksumInBlocks (block_based_table_reader.cc: "BlockFetcher
    /// block_fetcher(..., false /* decompress */, false /*maybe_compressed*/, ...)"): the raw blocks are checksummed,
    /// never decompressed. The managed reader decompressed every data block without the table's dictionary, so
    /// VerifyChecksum failed with Corruption on valid dictionary-compressed tables. Native DB::VerifyChecksum is OK on
    /// the same files.
    /// </summary>
    [TestMethod]
    public void VerifyChecksumOfDictionaryCompressedTableMatchesNative()
    {
        foreach (CompressionType t in new[] { CompressionType.LZ4, CompressionType.Zstd, CompressionType.Zlib })
        {
            using var dir = new TempDir("a7-dictverify");
            string sst = WriteNativeDb(dir.Path, NativeOptions(NativeCompression(t), 7, (char)1, 4096, bloom: false), 3000);
            string? native;
            using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(NativeOptions(NativeCompression(t), 7, (char)1, 4096, bloom: false), dir.Path))
            {
                native = NativeVerifyChecksum(db);
            }
            string managed = "OK";
            using (var reader = new SstFileReader(ManagedOptions(t, 7, ChecksumType.Crc32c, 4096, bloom: false)))
            {
                reader.Open(sst);
                Assert.IsTrue(reader.GetTableProperties().CompressionName.Length > 0, t + ": compression property");
                try
                {
                    reader.VerifyChecksum();
                }
                catch (RocksDbException e)
                {
                    managed = e.Code + ": " + e.Message;
                }
            }
            Assert.AreEqual("OK", NativeCode(native), t + " native: " + native);
            Assert.AreEqual("OK", managed, t.ToString());
        }
    }

    /// <summary>Rewrites the format_version 2..5 footer (checksum byte, metaindex and index handles padded to 40 bytes,
    /// version, magic) with the given metaindex handle.</summary>
    private static void RewriteFooterMetaindex(byte[] file, ulong offset, ulong size)
    {
        const int footerLen = 1 + 40 + 4 + 8;
        Span<byte> footer = file.AsSpan(file.Length - footerLen);
        ReadOnlySpan<byte> handles = footer[1..41];
        int n = Coding.DecodeVarint64(handles, out _);
        n += Coding.DecodeVarint64(handles[n..], out _);
        int m = Coding.DecodeVarint64(handles[n..], out ulong indexOffset);
        Coding.DecodeVarint64(handles[(n + m)..], out ulong indexSize);
        Span<byte> fresh = stackalloc byte[40];
        fresh.Clear();
        int w = Coding.EncodeVarint64(fresh, offset);
        w += Coding.EncodeVarint64(fresh[w..], size);
        w += Coding.EncodeVarint64(fresh[w..], indexOffset);
        Coding.EncodeVarint64(fresh[w..], indexSize);
        fresh.CopyTo(footer[1..41]);
    }

    /// <summary>
    /// ReadFooterFromFile / BlockHandle::DecodeFrom / BlockFetcher::ReadBlockContents (table/format.cc,
    /// table/block_fetcher.cc): a footer whose metaindex handle points past the end of the file (or past 2^63, or
    /// wraps around) fails the table open with native's status, never with an exception from the file layer. Native: ingestion
    /// of the damaged file (ExternalSstFileIngestionJob::GetIngestedFileInfo opens the table); managed: SstFileReader.Open
    /// and the async BlockBasedTableReader.OpenAsync.
    /// </summary>
    [TestMethod]
    public async Task FooterHandlesBeyondTheFileMatchNative()
    {
        using var dir = new TempDir("a7-footer");
        string src = Path.Combine(dir.Path, "src.sst");
        var bbto = new RocksDbSharp.BlockBasedTableOptions().SetFormatVersion(5).SetBlockSize(1024);
        var cfo = new RocksDbSharp.ColumnFamilyOptions();
        cfo.SetBlockBasedTableFactory(bbto);
        cfo.SetCompression(RocksDbSharp.Compression.No);
        s_keepAlive.Add(bbto);
        s_keepAlive.Add(cfo);
        using (var w = new RocksDbSharp.SstFileWriter(new RocksDbSharp.EnvOptions(), cfo))
        {
            w.Open(src);
            for (int i = 0; i < 200; i++) w.Add(B(K(i)), V(i));
            w.Finish();
        }
        byte[] original = File.ReadAllBytes(src);
        // Expected: native's status code, or null where native aborts the process (upstream behavior: a 2^40 block
        // size throws std::bad_alloc out of BlockFetcher; size + kBlockTrailerSize wrapping around segfaults), where
        // the managed port reports Corruption ("block too large").
        (string Name, ulong Offset, ulong Size, string? NativeCode)[] cases =
        [
            ("past EOF", (ulong)original.Length + 100, 64, nameof(StatusCode.Corruption)),
            ("offset 2^63", (1UL << 63) + 5, 64, nameof(StatusCode.IOError)),
            ("offset near 2^64", ulong.MaxValue - 2, 64, nameof(StatusCode.IOError)),
            ("size 2^40", 0, 1UL << 40, null),
            ("size wraps", 10, ulong.MaxValue - 3, null),
        ];
        var results = new List<string>();
        foreach ((string name, ulong offset, ulong size, string? expected) in cases)
        {
            byte[] bytes = (byte[])original.Clone();
            RewriteFooterMetaindex(bytes, offset, size);
            string path = Path.Combine(dir.Path, "bad-" + results.Count + ".sst");
            File.WriteAllBytes(path, bytes);

            string native = "not run (native aborts)";
            if (expected is not null)
            {
                using var ndir = new TempDir("a7-footer-n");
                using RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions().SetCreateIfMissing(true), ndir.Path);
                try
                {
                    db.IngestExternalFiles([path], new RocksDbSharp.IngestExternalFileOptions());
                    native = "OK";
                }
                catch (RocksDbSharp.RocksDbException e)
                {
                    native = NativeCode(e.Message);
                }
            }
            string managed;
            try
            {
                using var reader = new SstFileReader(new ColumnFamilyOptions());
                reader.Open(path);
                managed = "OK";
            }
            catch (RocksDbException e)
            {
                managed = e.Code.ToString();
            }
            string managedAsync;
            try
            {
                using BlockBasedTableReader r = await BlockBasedTableReader.OpenAsync(RocksDb.Managed.Env.LocalFileSystem.Instance.OpenRandomAccess(path), TableReaderOptions.Default);
                managedAsync = "OK";
            }
            catch (RocksDbException e)
            {
                managedAsync = e.Code.ToString();
            }
            results.Add(name + ": native " + native + " | managed " + managed + " | async " + managedAsync);
            if (expected is not null) Assert.AreEqual(expected, native, name + " (native)");
            string want = expected ?? nameof(StatusCode.Corruption);
            Assert.AreEqual(want, managed, name + " (managed)");
            Assert.AreEqual(want, managedAsync, name + " (managed async)");
        }
        Console.WriteLine(string.Join("\n", results));
    }

    /// <summary>
    /// Decompressor::ExtractUncompressedSize / Snappy GetUncompressedLength + DecompressBlockData (util/compression.cc,
    /// table/format.cc): a data block whose stored uncompressed size is ~2 GiB / 4 GiB with a garbage payload. Native
    /// allocates the claimed size and fails decompression with Corruption; the managed reader threw
    /// OutOfMemoryException for a claimed 0x7FFFFFF0 (above Array.MaxLength), ArgumentOutOfRangeException for a snappy
    /// length above int.MaxValue, and allocated (and pooled) ~2 GiB for sizes just below. Read with verify_checksums =
    /// false so the damaged block reaches the decompressor.
    /// </summary>
    [TestMethod]
    public void HugeUncompressedSizeMatchesNative()
    {
        foreach (CompressionType t in new[] { CompressionType.Snappy, CompressionType.LZ4, CompressionType.Zstd, CompressionType.Zlib })
        foreach (ulong claimed in new ulong[] { 0x7FFFFFF0, 0xFFFFFFFF })
        {
            using var dir = new TempDir("a7-hugesize");
            string sst = WriteNativeDb(dir.Path, NativeOptions(NativeCompression(t), 5, (char)1, 0, bloom: false), 500);
            BlockHandle first;
            using (SstFile f = SstFile.Open(sst, new TableReaderOptions()))
            {
                first = f.DataBlockHandles()[0];
                Assert.AreEqual(t, f.BlockCompressionType(first), "first data block is compressed");
            }
            byte[] bytes = File.ReadAllBytes(sst);
            byte[] varint = new byte[10];
            int len = Coding.EncodeVarint64(varint, claimed);
            varint.AsSpan(0, len).CopyTo(bytes.AsSpan((int)first.Offset));
            File.WriteAllBytes(sst, bytes);

            var nativeResults = new List<string>();
            using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(NativeOptions(NativeCompression(t), 5, (char)1, 0, bloom: false), dir.Path))
            {
                var ro = new RocksDbSharp.ReadOptions().SetVerifyChecksums(false).SetFillCache(false);
                nativeResults.Add(NativeGet(db, ro, K(0)));
            }
            var managedResults = new List<string>();
            using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions(), ManagedOptions(t, 5, ChecksumType.Crc32c, 0, bloom: false)))
            {
                managedResults.Add(ManagedGet(db, new ReadOptions { VerifyChecksums = false, FillCache = false }, K(0)));
            }
            string what = t + " claimed=" + claimed.ToString("X", System.Globalization.CultureInfo.InvariantCulture);
            string pair = what + ": " + string.Join(",", nativeResults) + " vs " + string.Join(",", managedResults);
            Console.WriteLine(pair);
            Assert.AreEqual("Corruption", managedResults[0], pair);
            // Upstream behavior: LZ4_DecompressBlock passes static_cast<int>(uncompressed_size) as the output capacity,
            // so a claimed 0xFFFFFFFF becomes -1, LZ4_decompress_safe_continue fails with -1, and "uncompressed_size !=
            // expected_uncompressed_size" is false: native accepts 4 GiB of uninitialized memory as the block (here it
            // parsed as a block without the key). The managed port keeps Corruption.
            if (t == CompressionType.LZ4 && claimed == 0xFFFFFFFF) continue;
            CollectionAssert.AreEqual(nativeResults, managedResults, pair);
        }
    }

    /// <summary>
    /// BuiltinFilterPolicy::GetBuiltinFilterBitsReader / GetBloomBitsReader / GetRibbonBitsReader (filter_policy.cc):
    /// reserved or invalid filter metadata (num_probes 0, unknown sub-implementation, num_probes 31, nonzero reserved
    /// bytes, an unknown marker, a Ribbon filter with fewer than 2 blocks, legacy geometry without a solution) is read
    /// as "always may match", never as a false negative. kNoChecksum keeps the damage visible to the reader; every key
    /// is still found, by native and managed alike.
    /// </summary>
    [TestMethod]
    public void ReservedFilterMetadataMatchesNative()
    {
        // (name, byte offset from the end of the filter payload, new bytes)
        (string Name, int FromEnd, byte[] Bytes)[] cases =
        [
            ("num_probes 0 (legacy byte)", 5, [0]),
            ("reserved marker -3", 5, [0xFD]),
            ("bloom sub-impl 1", 4, [1]),
            ("bloom num_probes 31", 3, [31]),
            ("bloom num_probes 0", 3, [0]),
            ("bloom reserved bytes", 2, [1, 0]),
            ("ribbon with 1 block", 5, [0xFE, 0, 1, 0, 0]),
            ("legacy, lines without solution", 5, [6, 5, 0, 0, 0]),
        ];
        const int n = 400;
        foreach ((string name, int fromEnd, byte[] patch) in cases)
        {
            using var dir = new TempDir("a7-filtermeta");
            string sst = WriteNativeDb(dir.Path, NativeOptions(RocksDbSharp.Compression.No, 5, (char)0, 0, bloom: true), n);
            BlockHandle filter;
            using (SstFile f = SstFile.Open(sst, new TableReaderOptions { FilterPolicy = new BloomFilterPolicy(10) }))
            {
                string metaName = f.Reader.MetaBlocks.Keys.Single(k => k.StartsWith(BlockBasedTableConstants.FullFilterBlockPrefix, StringComparison.Ordinal));
                filter = f.Reader.MetaBlocks[metaName];
            }
            byte[] bytes = File.ReadAllBytes(sst);
            patch.CopyTo(bytes.AsSpan((int)(filter.Offset + filter.Size) - fromEnd));
            File.WriteAllBytes(sst, bytes);

            string[] keys = new string[n + 50];
            for (int i = 0; i < keys.Length; i++) keys[i] = i < n ? K(i) : "absent" + i;
            var native = new List<string>();
            using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(NativeOptions(RocksDbSharp.Compression.No, 5, (char)0, 0, bloom: true), dir.Path))
            {
                var ro = new RocksDbSharp.ReadOptions();
                foreach (string k in keys) native.Add(NativeGet(db, ro, k));
            }
            var managed = new List<string>();
            using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions(), ManagedOptions(CompressionType.None, 5, ChecksumType.NoChecksum, 0, bloom: true)))
            {
                foreach (string k in keys) managed.Add(ManagedGet(db, new ReadOptions(), k));
            }
            CollectionAssert.AreEqual(native, managed, name);
            for (int i = 0; i < n; i++) Assert.AreEqual("found:" + Encoding.ASCII.GetString(V(i)), managed[i], name + " key " + i);
        }
    }
}
