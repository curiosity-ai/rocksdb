using System.Globalization;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Db;

namespace RocksDb.Managed.CompatTests.Table;

/// <summary>
/// <see cref="SstFileReader"/> (port of rocksdb::SstFileReader) on SST files written by native RocksDB's SstFileWriter:
/// iteration with DBIter semantics, Get / MultiGet, table properties, VerifyChecksum / VerifyNumEntries, compared with
/// what a native DB reads after ingesting the same file (sst_file_reader_test.cc spirit).
/// </summary>
[TestClass]
public sealed class SstFileReaderCompatTests
{
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    private static byte[] B(string s) => Encoding.UTF8.GetBytes(s);
    private static string K(int i) => "key" + i.ToString("D5", CultureInfo.InvariantCulture);

    /// <summary>A native SST: puts of key00000..key00299, every 7th key a deletion, every 11th a merge operand.</summary>
    private static string WriteNativeSst(string dir, bool merges)
    {
        string path = Path.Combine(dir, "native.sst");
        var cfo = new RocksDbSharp.ColumnFamilyOptions().SetCompression(RocksDbSharp.Compression.No);
        s_keepAlive.Add(cfo);
        using var w = new RocksDbSharp.SstFileWriter(new RocksDbSharp.EnvOptions(), cfo);
        w.Open(path);
        for (int i = 0; i < 300; i++)
        {
            if (i % 7 == 3) w.Delete(B(K(i)));
            else if (merges && i % 11 == 5) w.Merge(B(K(i)), B("m" + i));
            else w.Put(B(K(i)), B("v" + i));
        }
        w.Finish();
        return path;
    }

    [TestMethod]
    public void ReadsNativeSstLikeNativeIngestion()
    {
        using var dir = new TempDir("sstreader");
        string path = WriteNativeSst(dir.Path, merges: false);

        // Native view: ingest into a DB and scan.
        var native = new List<string>();
        var no = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        s_keepAlive.Add(no);
        string copy = Path.Combine(dir.Path, "copy.sst");
        File.Copy(path, copy);
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(no, Path.Combine(dir.Path, "db")))
        {
            db.IngestExternalFiles([copy], new RocksDbSharp.IngestExternalFileOptions());
            using RocksDbSharp.Iterator it = db.NewIterator();
            for (it.SeekToFirst(); it.Valid(); it.Next()) native.Add(Encoding.UTF8.GetString(it.Key()) + "=" + Encoding.UTF8.GetString(it.Value()));
        }

        using var reader = new SstFileReader(new ColumnFamilyOptions());
        reader.Open(path);
        var managed = new List<string>();
        using (Iterator it = reader.NewIterator(null))
        {
            for (it.SeekToFirst(); it.Valid; it.Next()) managed.Add(Encoding.UTF8.GetString(it.Key) + "=" + Encoding.UTF8.GetString(it.Value));
            // Backward iteration and seeks.
            it.SeekForPrev(B(K(12)));
            Assert.AreEqual(K(12), Encoding.UTF8.GetString(it.Key));
            it.Prev();
            Assert.AreEqual(K(11), Encoding.UTF8.GetString(it.Key));
            it.Seek(B(K(3)));
            Assert.AreEqual(K(4), Encoding.UTF8.GetString(it.Key)); // key00003 is a deletion
        }
        CollectionAssert.AreEqual(native, managed);

        Assert.AreEqual("v0", Encoding.UTF8.GetString(reader.Get(null, B(K(0)))!));
        Assert.IsNull(reader.Get(null, B(K(3))));
        Assert.IsNull(reader.Get(null, B("zzz")));
        using var batch = new MultiGetBatch();
        batch.Add(B(K(1))).Add(B(K(10))).Add(B(K(299))).Add(B("nope"));
        reader.MultiGet(null, batch);
        Assert.AreEqual(MultiGetStatus.Found, batch.GetStatus(0));
        Assert.AreEqual(MultiGetStatus.NotFound, batch.GetStatus(1)); // deletion
        Assert.AreEqual("v299", Encoding.UTF8.GetString(batch.GetValue(2)));
        Assert.AreEqual(MultiGetStatus.NotFound, batch.GetStatus(3));

        Managed.Table.TableProperties props = reader.GetTableProperties();
        Assert.AreEqual(300UL, props.NumEntries);
        Assert.AreEqual(43UL, props.NumDeletions);
        reader.VerifyChecksum();
        reader.VerifyNumEntries();
    }

    [TestMethod]
    public void MergesAndSnapshots()
    {
        using var dir = new TempDir("sstreader-merge");
        string path = WriteNativeSst(dir.Path, merges: true);
        using var reader = new SstFileReader(new ColumnFamilyOptions { MergeOperator = MergeOperator.StringAppend(",") });
        reader.Open(path);
        // A merge operand without a base: the iterator merges it alone, Get reports MergeInProgress (like upstream).
        using (Iterator it = reader.NewIterator(null))
        {
            it.Seek(B(K(5)));
            Assert.AreEqual("m5", Encoding.UTF8.GetString(it.Value));
        }
        Assert.AreEqual(StatusCode.MergeInProgress, Assert.ThrowsExactly<RocksDbException>(() => reader.Get(null, B(K(5)))).Code);
        using var batch = new MultiGetBatch();
        batch.Add(B(K(5))).Add(B(K(6)));
        reader.MultiGet(null, batch);
        Assert.AreEqual(StatusCode.MergeInProgress, batch.GetError(0)!.Code);
        Assert.AreEqual("v6", Encoding.UTF8.GetString(batch.GetValue(1)));
    }

    [TestMethod]
    public void VerifyChecksumDetectsCorruption()
    {
        using var dir = new TempDir("sstreader-corrupt");
        string path = WriteNativeSst(dir.Path, merges: false);
        byte[] bytes = File.ReadAllBytes(path);
        int at = bytes.AsSpan().IndexOf("v152"u8);
        Assert.IsTrue(at > 0);
        bytes[at + 1] ^= 1;
        File.WriteAllBytes(path, bytes);
        using var reader = new SstFileReader(new ColumnFamilyOptions());
        reader.Open(path); // only meta blocks are read at open
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(reader.VerifyChecksum).Code);
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => reader.Get(null, B(K(152)))).Code);
        // verify_checksums = false reads the damaged block.
        Assert.IsNotNull(reader.Get(new ReadOptions { VerifyChecksums = false }, B(K(152))));
    }
}
