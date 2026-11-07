using System.Globalization;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.CompatTests.Table;

/// <summary>
/// Embedded-blob SSTs (SstFileWriter::OpenWithEmbeddedBlobs, table/embedded_blob_sst.h). The native C API cannot write
/// one, so the managed writer produces it; the test checks the layout against the C++ definition (SimpleGen2Blob
/// records = payload + kNoCompression byte + fixed32 checksum of payload+type plus the context modifier of the record
/// offset; blob indexes with file number 0; "rocksdb.embedded.blob.stats" = varint64 count + varint64 bytes; full
/// index handles below format_version 8) and that native RocksDB 11.12 (which reads embedded-blob SSTs) and the managed
/// port both ingest and read it, including after compaction.
/// </summary>
[TestClass]
public sealed class EmbeddedBlobSstCompatTests
{
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    private static byte[] K(int i) => Encoding.UTF8.GetBytes("key" + i.ToString("D5", CultureInfo.InvariantCulture));

    /// <summary>Every third value is large (embedded), the others stay inline.</summary>
    private static byte[] V(int i) => Encoding.UTF8.GetBytes((i % 3 == 0 ? new string((char)('a' + i % 26), 3000) : "small") + "-" + i.ToString(CultureInfo.InvariantCulture));

    private const int N = 200;

    private static string WriteEmbedded(string dir, uint formatVersion)
    {
        string path = Path.Combine(dir, "embedded-" + formatVersion + ".sst");
        var cfo = new ColumnFamilyOptions { Compression = Managed.Compression.CompressionType.Snappy };
        cfo.TableOptions.FormatVersion = formatVersion;
        cfo.TableOptions.BlockSize = 1024;
        using var w = new SstFileWriter(cfo);
        w.OpenWithEmbeddedBlobs(path, 2048);
        for (int i = 0; i < N; i++) w.Put(K(i), V(i));
        w.Finish();
        return path;
    }

    [TestMethod]
    public void LayoutMatchesTheCppDefinition()
    {
        using var dir = new TempDir("emb-layout");
        foreach (uint fv in new uint[] { 7, 8 })
        {
            string path = WriteEmbedded(dir.Path, fv);
            byte[] file = File.ReadAllBytes(path);
            using BlockBasedTableReader reader = BlockBasedTableReader.Open(LocalFileSystem.Instance.OpenRandomAccess(path), new TableReaderOptions());
            Assert.IsTrue(reader.HasEmbeddedBlobRecords);
            byte[] stats = reader.Properties.UserCollectedProperties[TablePropertiesNames.EmbeddedBlobSstStats];
            ReadOnlySpan<byte> s = stats;
            Assert.IsTrue(Coding.GetVarint64(ref s, out ulong count));
            Assert.IsTrue(Coding.GetVarint64(ref s, out ulong bytes));
            Assert.IsTrue(s.IsEmpty);
            int expectedCount = 0;
            ulong expectedBytes = 0;
            for (int i = 0; i < N; i++)
            {
                if (V(i).Length < 2048) continue;
                expectedCount++;
                expectedBytes += (ulong)V(i).Length;
            }
            Assert.AreEqual((ulong)expectedCount, count);
            Assert.AreEqual(expectedBytes, bytes);
            Assert.AreEqual(fv >= 8 ? 1UL : 0UL, reader.Properties.IndexValueIsDeltaEncoded, "fv " + fv);

            // The raw table (no resolution): large values are kTypeBlobIndex entries referencing this file.
            int blobIndexes = 0;
            using (BlockBasedTableIterator it = reader.NewIterator())
            {
                int i = 0;
                for (it.SeekToFirst(); it.Valid; it.Next(), i++)
                {
                    DbFormat.UnpackSequenceAndType(DbFormat.ExtractFooter(it.Key), out _, out EntryType t);
                    if (V(i).Length < 2048)
                    {
                        Assert.AreEqual(EntryType.Value, t);
                        continue;
                    }
                    Assert.AreEqual(EntryType.BlobIndex, t);
                    blobIndexes++;
                    // BlobIndex::EncodeBlob: type kBlob (1), varint64 file number (0), varint64 offset, varint64 size, compression.
                    ReadOnlySpan<byte> idx = it.Value;
                    Assert.AreEqual(1, idx[0]);
                    idx = idx[1..];
                    Assert.IsTrue(Coding.GetVarint64(ref idx, out ulong fileNumber));
                    Assert.IsTrue(Coding.GetVarint64(ref idx, out ulong offset));
                    Assert.IsTrue(Coding.GetVarint64(ref idx, out ulong size));
                    Assert.AreEqual(0UL, fileNumber);
                    Assert.AreEqual((ulong)V(i).Length, size);
                    Assert.AreEqual(1, idx.Length);
                    Assert.AreEqual(0, idx[0]); // kNoCompression
                    // The record: payload, compression byte, checksum (ComputeBuiltinChecksumWithLastByte + context modifier).
                    CollectionAssert.AreEqual(V(i), file.AsSpan((int)offset, (int)size).ToArray());
                    Assert.AreEqual(0, file[(int)(offset + size)]);
                    Assert.IsTrue(BlockChecksum.Verify(reader.Footer.ChecksumType, reader.Footer.BaseContextChecksum,
                        file.AsSpan((int)offset, (int)size + BlockBasedTableConstants.BlockTrailerSize), offset));
                }
                Assert.AreEqual(N, i);
            }
            Assert.AreEqual(expectedCount, blobIndexes);

            // The managed SstFileReader resolves them.
            using var sst = new SstFileReader(new ColumnFamilyOptions());
            sst.Open(path);
            sst.VerifyChecksum();
            using (Iterator it = sst.NewIterator(null))
            {
                int i = 0;
                for (it.SeekToFirst(); it.Valid; it.Next(), i++) CollectionAssert.AreEqual(V(i), it.Value.ToArray());
                Assert.AreEqual(N, i);
            }
            CollectionAssert.AreEqual(V(3), sst.Get(null, K(3)));
        }
    }

    [TestMethod]
    public void NativeAndManagedIngestAndReadIt()
    {
        using var dir = new TempDir("emb-ingest");
        string path = WriteEmbedded(dir.Path, 7);
        string forNative = Path.Combine(dir.Path, "n.sst"), forManaged = Path.Combine(dir.Path, "m.sst");
        File.Copy(path, forNative);
        File.Copy(path, forManaged);

        var no = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        s_keepAlive.Add(no);
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(no, Path.Combine(dir.Path, "native")))
        {
            db.IngestExternalFiles([forNative], new RocksDbSharp.IngestExternalFileOptions());
            for (int i = 0; i < N; i++) CollectionAssert.AreEqual(V(i), db.Get(K(i)), "native key " + i);
            db.CompactRange(Array.Empty<byte>(), null);
            for (int i = 0; i < N; i += 7) CollectionAssert.AreEqual(V(i), db.Get(K(i)), "native after compaction " + i);
        }

        string managedDir = Path.Combine(dir.Path, "managed");
        using (RocksDatabase db = RocksDatabase.Open(managedDir, new DbOptions { CreateIfMissing = true }))
        {
            db.IngestExternalFiles([forManaged], new IngestExternalFileOptions());
            for (int i = 0; i < N; i++) CollectionAssert.AreEqual(V(i), db.Get(K(i)), "managed key " + i);
            CollectionAssert.AreEqual(V(9), db.GetAsync(K(9)).AsTask().GetAwaiter().GetResult());
            using (var batch = new MultiGetBatch())
            {
                for (int i = 0; i < N; i += 5) batch.Add(K(i));
                db.MultiGet(batch);
                for (int j = 0; j < batch.Count; j++) CollectionAssert.AreEqual(V(j * 5), batch.GetValue(j).ToArray());
            }
            using (Iterator it = db.NewIterator())
            {
                int i = 0;
                for (it.SeekToFirst(); it.Valid; it.Next(), i++) CollectionAssert.AreEqual(V(i), it.Value.ToArray());
                Assert.AreEqual(N, i);
                for (it.SeekToLast(), i = N - 1; it.Valid; it.Prev(), i--) CollectionAssert.AreEqual(V(i), it.Value.ToArray());
            }
            // Block-cache-only reads cannot read embedded blobs (a direct file read).
            RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => db.Get(new ReadOptions { ReadTier = ReadTier.BlockCacheTier }, K(0)));
            Assert.AreEqual(StatusCode.Incomplete, e.Code);
            // Compaction resolves the blobs: the outputs hold plain values.
            db.CompactRange(new CompactRangeOptions { BottommostLevelCompaction = BottommostLevelCompaction.Force }, null, null);
            foreach (Managed.Table.TableProperties p in db.GetPropertiesOfAllTables().Values)
            {
                Assert.IsFalse(p.UserCollectedProperties.ContainsKey(TablePropertiesNames.EmbeddedBlobSstStats));
            }
            for (int i = 0; i < N; i++) CollectionAssert.AreEqual(V(i), db.Get(K(i)), "managed after compaction " + i);
        }
        // Native reads the managed DB (compacted, plain values).
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(no, managedDir))
        {
            for (int i = 0; i < N; i += 3) CollectionAssert.AreEqual(V(i), db.Get(K(i)));
        }
    }
}
