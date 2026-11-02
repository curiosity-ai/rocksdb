using RocksDb.Managed.Compression;
using RocksDb.Managed.Db;
using RocksDb.Managed.Db.Blob;
using RocksDb.Managed.Env;
using RocksDb.Managed.Util;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Blob;

/// <summary>
/// Blob file format, blob index, BlobFileBuilder and BlobFileReader. Ports of key cases of
/// db/blob/blob_file_builder_test.cc, db/blob/blob_file_reader_test.cc and the BlobIndex / blob log format code.
/// </summary>
[TestClass]
public sealed class BlobFormatTests
{
    private const uint CfId = 7;

    private static BlobFileBuilder NewBuilder(MemoryFileSystem fs, ColumnFamilyOptions o, Func<ulong> numbers) =>
        new(fs, DbPath, numbers, o, CfId, BlobFileCreationReason.Flush, null, null, WritableFileOptions.Default);

    private static Func<ulong> Counter(ulong start)
    {
        ulong next = start;
        return () => next++;
    }

    [TestMethod]
    public void HeaderFooterRecordRoundTripAndCorruption()
    {
        byte[] header = new byte[BlobLogFormat.HeaderSize];
        BlobLogFormat.EncodeHeader(header, 5, CompressionType.Zstd, hasTtl: true, 10, 20);
        BlobLogFormat.Header h = BlobLogFormat.DecodeHeader(header);
        Assert.AreEqual(new BlobLogFormat.Header(1, 5, CompressionType.Zstd, true, 10, 20), h);
        // magic number 0x00248f37, version 1
        CollectionAssert.AreEqual(new byte[] { 0x37, 0x8f, 0x24, 0x00, 1, 0, 0, 0 }, header[..8]);
        header[0] ^= 1;
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => BlobLogFormat.DecodeHeader(header)).Code);
        header[0] ^= 1;
        header[4] = 2;
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => BlobLogFormat.DecodeHeader(header)).Code);
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => BlobLogFormat.DecodeHeader(header[..29])).Code);

        byte[] footer = new byte[BlobLogFormat.FooterSize];
        BlobLogFormat.EncodeFooter(footer, 42, 0, 0);
        Assert.AreEqual(42UL, BlobLogFormat.DecodeFooter(footer).BlobCount);
        footer[10] ^= 0x40;
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => BlobLogFormat.DecodeFooter(footer));
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
        StringAssert.Contains(ex.Message, "CRC mismatch");

        byte[] rec = new byte[BlobLogFormat.RecordHeaderSize];
        BlobLogFormat.EncodeRecordHeader(rec, "key"u8, "value"u8, 0);
        BlobLogFormat.RecordHeader r = BlobLogFormat.DecodeRecordHeader(rec);
        Assert.AreEqual(3UL, r.KeySize);
        Assert.AreEqual(5UL, r.ValueSize);
        Assert.IsTrue(BlobLogFormat.CheckBlobCrc(r.BlobCrc, "key"u8, "value"u8));
        Assert.IsFalse(BlobLogFormat.CheckBlobCrc(r.BlobCrc, "key"u8, "valuf"u8));
        rec[3] ^= 1;
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => BlobLogFormat.DecodeRecordHeader(rec)).Code);
    }

    [TestMethod]
    public void IsValidBlobOffset()
    {
        // header 30 + record header 32 + key 3 = first valid value offset 65 for a 3-byte key
        Assert.IsTrue(BlobLogFormat.IsValidBlobOffset(65, 3, 5, 65 + 5 + 32));
        Assert.IsFalse(BlobLogFormat.IsValidBlobOffset(64, 3, 5, 200));
        Assert.IsFalse(BlobLogFormat.IsValidBlobOffset(65, 3, 6, 65 + 5 + 32));
        Assert.IsFalse(BlobLogFormat.IsValidBlobOffset(65, 3, 5, 20));
        Assert.IsFalse(BlobLogFormat.IsValidBlobOffset(ulong.MaxValue, 3, 5, 200));
    }

    [TestMethod]
    public void BlobIndexEncodeDecode()
    {
        var w = new PooledBufferWriter(32);
        BlobIndex.EncodeBlob(w, 1234, 5678, 90, CompressionType.LZ4);
        BlobIndex i = BlobIndex.Decode(w.WrittenSpan);
        Assert.AreEqual(BlobIndexType.Blob, i.Type);
        Assert.AreEqual(1234UL, i.FileNumber);
        Assert.AreEqual(5678UL, i.Offset);
        Assert.AreEqual(90UL, i.Size);
        Assert.AreEqual(CompressionType.LZ4, i.Compression);
        Assert.IsFalse(i.HasTtl);
        Assert.AreEqual("[blob ref] file:1234 offset:5678 size:90 compression: LZ4", i.DebugString());
        // kBlob: type(1) file(varint) offset(varint) size(varint) compression(1)
        CollectionAssert.AreEqual(new byte[] { 1, 0xd2, 0x09, 0xae, 0x2c, 90, 4 }, w.WrittenSpan.ToArray());

        w.Clear();
        BlobIndex.EncodeBlobTtl(w, 99, 3, 4, 5, CompressionType.None);
        i = BlobIndex.Decode(w.WrittenSpan);
        Assert.IsTrue(i.HasTtl);
        Assert.AreEqual(99UL, i.Expiration);
        Assert.AreEqual(3UL, i.FileNumber);

        w.Clear();
        BlobIndex.EncodeInlinedTtl(w, 7, "inline"u8);
        i = BlobIndex.Decode(w.WrittenSpan);
        Assert.IsTrue(i.IsInlined);
        Assert.AreEqual(7UL, i.Expiration);
        Assert.IsTrue(i.InlinedValue.SequenceEqual("inline"u8));

        Assert.IsNotNull(BlobIndex.TryDecode([3], out _));                      // unknown type
        Assert.IsNotNull(BlobIndex.TryDecode([1, 1, 2], out _));                // truncated
        Assert.IsNotNull(BlobIndex.TryDecode([1, 1, 2, 3, 0, 0], out _));       // trailing data
        Assert.IsNotNull(BlobIndex.TryDecode([2, 0x80], out _));                // corrupted expiration
        w.Dispose();
    }

    [TestMethod]
    public void BuildersWritesBlobsAndReaderReadsThem()
    {
        // blob_file_builder_test.cc BuildAndCheckOneFile + blob_file_reader_test.cc CreateReaderAndGetBlob
        MemoryFileSystem fs = NewFs();
        var o = new ColumnFamilyOptions { EnableBlobFiles = true, MinBlobSize = 0 };
        using var builder = NewBuilder(fs, o, Counter(10));
        var indexes = new List<byte[]>();
        var w = new PooledBufferWriter(32);
        for (int i = 0; i < 10; i++)
        {
            w.Clear();
            Assert.IsTrue(builder.Add(B("key" + i), B("blob" + i + new string('v', 1000)), w));
            indexes.Add(w.WrittenSpan.ToArray());
        }
        // An empty value is never extracted.
        w.Clear();
        Assert.IsFalse(builder.Add(B("empty"), default, w));
        Assert.AreEqual(0, w.WrittenCount);
        builder.Finish();
        Assert.AreEqual(1, builder.Additions.Count);
        BlobFileAddition a = builder.Additions[0];
        Assert.AreEqual(10UL, a.BlobFileNumber);
        Assert.AreEqual(10UL, a.TotalBlobCount);
        ulong expectedBytes = 0;
        for (int i = 0; i < 10; i++) expectedBytes += 32 + (ulong)("key" + i).Length + (ulong)("blob" + i).Length + 1000;
        Assert.AreEqual(expectedBytes, a.TotalBlobBytes);
        Assert.AreEqual("", a.ChecksumMethod);
        Assert.AreEqual((long)(BlobLogFormat.HeaderSize + expectedBytes + BlobLogFormat.FooterSize), fs.GetFileSize(FileNames.BlobFileName(DbPath, 10)));

        using BlobFileReader reader = BlobFileReader.Open(fs, FileNames.BlobFileName(DbPath, 10), CfId, null);
        for (int i = 0; i < 10; i++)
        {
            BlobIndex idx = BlobIndex.Decode(indexes[i]);
            Assert.AreEqual(10UL, idx.FileNumber);
            foreach (bool verify in new[] { true, false })
            {
                var output = new PooledBufferWriter(64);
                reader.GetBlob(B("key" + i), idx.Offset, idx.Size, idx.Compression, verify, output);
                Assert.AreEqual("blob" + i + new string('v', 1000), S(output.WrittenSpan.ToArray()));
                output.Dispose();
            }
        }
        // Wrong key with checksum verification; bad offset; compression mismatch.
        BlobIndex firstIndex = BlobIndex.Decode(indexes[0]);
        (ulong fOff, ulong fSize, CompressionType fComp) = (firstIndex.Offset, firstIndex.Size, firstIndex.Compression);
        var o2 = new PooledBufferWriter(64);
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => reader.GetBlob(B("kez0"), fOff, fSize, fComp, true, o2));
        StringAssert.Contains(ex.Message, "Key mismatch");
        ex = Assert.ThrowsExactly<RocksDbException>(() => reader.GetBlob(B("key0"), 10, fSize, fComp, true, o2));
        StringAssert.Contains(ex.Message, "Invalid blob offset");
        ex = Assert.ThrowsExactly<RocksDbException>(() => reader.GetBlob(B("key0"), fOff, fSize, CompressionType.Snappy, true, o2));
        StringAssert.Contains(ex.Message, "Compression type mismatch");
        // Column family mismatch.
        ex = Assert.ThrowsExactly<RocksDbException>(() => BlobFileReader.Open(fs, FileNames.BlobFileName(DbPath, 10), CfId + 1, null));
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
        o2.Dispose();
        w.Dispose();
    }

    [TestMethod]
    public void BuilderRollsOverAtBlobFileSizeAndHonorsMinBlobSize()
    {
        // blob_file_builder_test.cc BuildAndCheckMultipleFiles / InlinedValues
        MemoryFileSystem fs = NewFs();
        var o = new ColumnFamilyOptions { EnableBlobFiles = true, MinBlobSize = 100, BlobFileSize = 1 };
        using var builder = NewBuilder(fs, o, Counter(20));
        var w = new PooledBufferWriter(32);
        Assert.IsFalse(builder.Add(B("small"), new byte[99], w));
        for (int i = 0; i < 3; i++)
        {
            w.Clear();
            Assert.IsTrue(builder.Add(B("k" + i), new byte[100 + i], w));
            Assert.AreEqual((ulong)(20 + i), BlobIndex.Decode(w.WrittenSpan).FileNumber);
        }
        builder.Finish();
        Assert.AreEqual(3, builder.Additions.Count);
        foreach (BlobFileAddition a in builder.Additions) Assert.AreEqual(1UL, a.TotalBlobCount);
        CollectionAssert.AreEqual(new ulong[] { 20, 21, 22 }, builder.CreatedFileNumbers);
        w.Dispose();
    }

    [TestMethod]
    [DataRow(CompressionType.Snappy)]
    [DataRow(CompressionType.LZ4)]
    [DataRow(CompressionType.Zstd)]
    [DataRow(CompressionType.Zlib)]
    public void CompressedBlobs(CompressionType type)
    {
        // blob_file_builder_test.cc Compression / blob_file_reader_test.cc Compression; incompressible data is stored
        // compressed anyway (LegacyForceBuiltinCompression).
        MemoryFileSystem fs = NewFs();
        var o = new ColumnFamilyOptions { EnableBlobFiles = true, BlobCompressionType = type };
        using var builder = NewBuilder(fs, o, Counter(5));
        var w = new PooledBufferWriter(32);
        byte[] compressible = B(new string('a', 5000));
        byte[] random = new byte[3000];
        new Random(1).NextBytes(random);
        Assert.IsTrue(builder.Add(B("c"), compressible, w));
        byte[] i1 = w.WrittenSpan.ToArray();
        w.Clear();
        Assert.IsTrue(builder.Add(B("r"), random, w));
        byte[] i2 = w.WrittenSpan.ToArray();
        builder.Finish();
        Assert.AreEqual(type, BlobIndex.Decode(i1).Compression);
        Assert.IsTrue(BlobIndex.Decode(i1).Size < 5000);
        using BlobFileReader reader = BlobFileReader.Open(fs, FileNames.BlobFileName(DbPath, 5), CfId, null);
        Assert.AreEqual(type, reader.Compression);
        var output = new PooledBufferWriter(64);
        BlobIndex a = BlobIndex.Decode(i1);
        reader.GetBlob(B("c"), a.Offset, a.Size, a.Compression, true, output);
        CollectionAssert.AreEqual(compressible, output.WrittenSpan.ToArray());
        output.Clear();
        BlobIndex b = BlobIndex.Decode(i2);
        reader.GetBlob(B("r"), b.Offset, b.Size, b.Compression, true, output);
        CollectionAssert.AreEqual(random, output.WrittenSpan.ToArray());
        output.Dispose();
        w.Dispose();
    }

    [TestMethod]
    public void ReaderDetectsBlobCrcMismatchAndMalformedFiles()
    {
        // blob_file_reader_test.cc BlobCRCError / Malformed / TTL
        MemoryFileSystem fs = NewFs();
        var o = new ColumnFamilyOptions { EnableBlobFiles = true };
        using (var builder = NewBuilder(fs, o, Counter(3)))
        {
            var w = new PooledBufferWriter(32);
            builder.Add(B("key"), B("blobvalue"), w);
            builder.Finish();
            w.Dispose();
        }
        string path = FileNames.BlobFileName(DbPath, 3);
        byte[] bytes = fs.ReadAllBytes(path);
        // value starts at 30 + 32 + 3
        bytes[65] ^= 0xff;
        string bad = FileNames.BlobFileName(DbPath, 4);
        using (IWritableFile f = fs.CreateWritable(bad)) f.Append(bytes);
        using (BlobFileReader r = BlobFileReader.Open(fs, bad, CfId, null))
        {
            var output = new PooledBufferWriter(16);
            RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => r.GetBlob(B("key"), 65, 9, CompressionType.None, true, output));
            StringAssert.Contains(ex.Message, "Blob CRC mismatch");
            output.Clear();
            r.GetBlob(B("key"), 65, 9, CompressionType.None, false, output); // no verification: corrupt bytes returned
            output.Dispose();
        }
        string tiny = FileNames.BlobFileName(DbPath, 6);
        using (IWritableFile f = fs.CreateWritable(tiny)) f.Append(bytes.AsSpan(0, 40));
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => BlobFileReader.Open(fs, tiny, CfId, null)).Code);
        // TTL header
        byte[] ttl = (byte[])fs.ReadAllBytes(path).Clone();
        ttl[12] = 1;
        string ttlPath = FileNames.BlobFileName(DbPath, 8);
        using (IWritableFile f = fs.CreateWritable(ttlPath)) f.Append(ttl);
        RocksDbException e2 = Assert.ThrowsExactly<RocksDbException>(() => BlobFileReader.Open(fs, ttlPath, CfId, null));
        StringAssert.Contains(e2.Message, "Unexpected TTL blob file");
    }
}
