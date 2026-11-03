using System.Runtime.InteropServices;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Env;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>
/// External file ingestion against native RocksDB (db/external_sst_file_ingestion_job.cc): overlapping files in one
/// call, ingest_behind and allow_db_generated_files give the same levels / sequence numbers / contents on both sides
/// and the resulting DBs cross-open; files rewritten by the legacy write_global_seqno are read by native 11.12+.
/// </summary>
[TestClass]
public sealed class IngestionCompatTests
{
    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);
    private static string Key(int i) => "key" + i.ToString("D6");

    private static string WriteSst(string path, int from, int to, string tag)
    {
        using var w = new SstFileWriter(new ColumnFamilyOptions(), LocalFileSystem.Instance);
        w.Open(path);
        for (int i = from; i < to; i++) w.Put(U(Key(i)), U(tag + i));
        w.Finish();
        return path;
    }

    private sealed record FileInfo(int Level, string Smallest, ulong SmallestSeqno, ulong LargestSeqno);

    private static List<FileInfo> NativeFiles(RocksDbSharp.RocksDb db)
    {
        var result = new List<FileInfo>();
        IntPtr lf = RocksDbSharp.Native.Instance.rocksdb_livefiles(db.Handle);
        try
        {
            int n = RocksDbSharp.Native.Instance.rocksdb_livefiles_count(lf);
            for (int i = 0; i < n; i++)
            {
                IntPtr k = RocksDbSharp.Native.Instance.rocksdb_livefiles_smallestkey(lf, i, out UIntPtr len);
                result.Add(new FileInfo(RocksDbSharp.Native.Instance.rocksdb_livefiles_level(lf, i), Marshal.PtrToStringUTF8(k, (int)len)!,
                    RocksDbSharp.Native.Instance.rocksdb_livefiles_smallest_seqno(lf, i), RocksDbSharp.Native.Instance.rocksdb_livefiles_largest_seqno(lf, i)));
            }
        }
        finally
        {
            RocksDbSharp.Native.Instance.rocksdb_livefiles_destroy(lf);
        }
        result.Sort((a, b) => (a.Smallest, a.LargestSeqno).CompareTo((b.Smallest, b.LargestSeqno)));
        return result;
    }

    private static List<FileInfo> ManagedFiles(RocksDatabase db)
    {
        var result = new List<FileInfo>();
        foreach (LiveFileMetadata f in db.GetLiveFilesMetadata())
            result.Add(new FileInfo(f.Level, Encoding.UTF8.GetString(f.SmallestKey), f.SmallestSeqno, f.LargestSeqno));
        result.Sort((a, b) => (a.Smallest, a.LargestSeqno).CompareTo((b.Smallest, b.LargestSeqno)));
        return result;
    }

    private static Dictionary<string, string> NativeScan(RocksDbSharp.RocksDb db)
    {
        var d = new Dictionary<string, string>();
        using RocksDbSharp.Iterator it = db.NewIterator();
        for (it.SeekToFirst(); it.Valid(); it.Next()) d[Encoding.UTF8.GetString(it.Key())] = Encoding.UTF8.GetString(it.Value());
        return d;
    }

    private static Dictionary<string, string> ManagedScan(RocksDatabase db)
    {
        var d = new Dictionary<string, string>();
        using Iterator it = db.NewIterator();
        for (it.SeekToFirst(); it.Valid; it.Next()) d[Encoding.UTF8.GetString(it.Key)] = Encoding.UTF8.GetString(it.Value);
        return d;
    }

    private static void AssertSameContents(Dictionary<string, string> expected, Dictionary<string, string> actual, string what)
    {
        Assert.AreEqual(expected.Count, actual.Count, what + ": count");
        foreach (KeyValuePair<string, string> kv in expected) Assert.AreEqual(kv.Value, actual.GetValueOrDefault(kv.Key), what + ": " + kv.Key);
    }

    /// <summary>Overlapping files in one call: batches, levels and per-file sequence numbers match native.</summary>
    [TestMethod]
    public void OverlappingFilesMatchNative()
    {
        using var dir = new TempDir("ingest-overlap");
        string[] files =
        [
            WriteSst(Path.Combine(dir.Path, "1.sst"), 0, 100, "a"), WriteSst(Path.Combine(dir.Path, "2.sst"), 200, 300, "b"),
            WriteSst(Path.Combine(dir.Path, "3.sst"), 50, 250, "c"), WriteSst(Path.Combine(dir.Path, "4.sst"), 60, 70, "d"),
            WriteSst(Path.Combine(dir.Path, "5.sst"), 400, 450, "e"),
        ];
        string nativePath = Path.Combine(dir.Path, "native"), managedPath = Path.Combine(dir.Path, "managed");
        List<FileInfo> nativeFiles;
        Dictionary<string, string> nativeData;
        // Auto compactions off on both sides: the placement under test is ingestion's, and a background compaction
        // native schedules after the ingestion (5 L0 files) would otherwise race the file listing.
        var no = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetDisableAutoCompactions(1);
        using (var db = RocksDbSharp.RocksDb.Open(no, nativePath))
        {
            db.Put(U(Key(10)), U("base"));
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            db.Put(U("zzz"), U("mem"));
            db.IngestExternalFiles(files, new RocksDbSharp.IngestExternalFileOptions());
            nativeFiles = NativeFiles(db);
            nativeData = NativeScan(db);
        }
        using (RocksDatabase db = RocksDatabase.Open(managedPath, new DbOptions { CreateIfMissing = true },
                   new ColumnFamilyOptions { DisableAutoCompactions = true }))
        {
            db.Put(U(Key(10)), U("base"));
            db.Flush();
            db.Put(U("zzz"), U("mem"));
            db.IngestExternalFiles(files, new IngestExternalFileOptions());
            CollectionAssert.AreEqual(nativeFiles, ManagedFiles(db), "levels and sequence numbers");
            AssertSameContents(nativeData, ManagedScan(db), "managed contents");
            Assert.AreEqual("d65", Encoding.UTF8.GetString(db.Get(U(Key(65)))!));
        }
        // Cross-open both directories.
        using (var db = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions(), managedPath))
        {
            AssertSameContents(nativeData, NativeScan(db), "native reads managed DB");
        }
        using (RocksDatabase db = RocksDatabase.Open(nativePath, new DbOptions()))
        {
            AssertSameContents(nativeData, ManagedScan(db), "managed reads native DB");
        }
    }

    /// <summary>ingest_behind with cf_allow_ingest_behind: same placement and visibility as native; DBs cross-open.</summary>
    [TestMethod]
    public void IngestBehindMatchesNative()
    {
        using var dir = new TempDir("ingest-behind");
        string sst = WriteSst(Path.Combine(dir.Path, "old.sst"), 0, 100, "old");
        string sst2 = WriteSst(Path.Combine(dir.Path, "old2.sst"), 200, 300, "old");
        string nativePath = Path.Combine(dir.Path, "native"), managedPath = Path.Combine(dir.Path, "managed");
        List<FileInfo> nativeFiles;
        Dictionary<string, string> nativeData;
        var no = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetCompactionStyle(RocksDbSharp.Compaction.Universal).SetNumLevels(4);
        RocksDbSharp.Native.Instance.rocksdb_options_set_cf_allow_ingest_behind(no.Handle, 1);
        var behind = new RocksDbSharp.IngestExternalFileOptions();
        RocksDbSharp.Native.Instance.rocksdb_ingestexternalfileoptions_set_ingest_behind(behind.Handle, 1);
        using (var db = RocksDbSharp.RocksDb.Open(no, nativePath))
        {
            for (int i = 0; i < 100; i += 10) db.Put(U(Key(i)), U("new" + i));
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            db.Put(U(Key(5)), U("mem5"));
            db.IngestExternalFiles([sst, sst2], behind);
            nativeFiles = NativeFiles(db);
            nativeData = NativeScan(db);
        }
        using (RocksDatabase db = RocksDatabase.Open(managedPath, new DbOptions { CreateIfMissing = true },
                   new ColumnFamilyOptions { CompactionStyle = CompactionStyle.Universal, NumLevels = 4, CfAllowIngestBehind = true }))
        {
            for (int i = 0; i < 100; i += 10) db.Put(U(Key(i)), U("new" + i));
            db.Flush();
            db.Put(U(Key(5)), U("mem5"));
            db.IngestExternalFiles([sst, sst2], new IngestExternalFileOptions { IngestBehind = true });
            CollectionAssert.AreEqual(nativeFiles, ManagedFiles(db));
            AssertSameContents(nativeData, ManagedScan(db), "managed contents");
            Assert.AreEqual("new10", Encoding.UTF8.GetString(db.Get(U(Key(10)))!));
            Assert.AreEqual("old11", Encoding.UTF8.GetString(db.Get(U(Key(11)))!));
            db.CompactRange();
            AssertSameContents(nativeData, ManagedScan(db), "after compaction");
        }
        var openNative = new RocksDbSharp.DbOptions().SetCompactionStyle(RocksDbSharp.Compaction.Universal).SetNumLevels(4);
        RocksDbSharp.Native.Instance.rocksdb_options_set_cf_allow_ingest_behind(openNative.Handle, 1);
        using (var db = RocksDbSharp.RocksDb.Open(openNative, managedPath))
        {
            AssertSameContents(nativeData, NativeScan(db), "native reads managed DB");
            db.CompactRange(Array.Empty<byte>(), null);
            AssertSameContents(nativeData, NativeScan(db), "native compacts managed DB");
        }
        using (RocksDatabase db = RocksDatabase.Open(nativePath, new DbOptions(),
                   new ColumnFamilyOptions { CompactionStyle = CompactionStyle.Universal, NumLevels = 4, CfAllowIngestBehind = true }))
        {
            AssertSameContents(nativeData, ManagedScan(db), "managed reads native DB");
        }
    }

    /// <summary>
    /// The legacy write_global_seqno (removed from native 11.12) rewrites the properties block in place; native still
    /// opens such files (stale properties checksum accepted through the zero placeholder) and reads the right data.
    /// </summary>
    [TestMethod]
    public void WriteGlobalSeqnoFilesAreReadByNative()
    {
        using var dir = new TempDir("ingest-wgs");
        string dbPath = Path.Combine(dir.Path, "db");
        string sst = WriteSst(Path.Combine(dir.Path, "1.sst"), 0, 100, "ingested");
        using (RocksDatabase db = RocksDatabase.Open(dbPath, new DbOptions { CreateIfMissing = true }))
        {
            for (int i = 0; i < 100; i += 3) db.Put(U(Key(i)), U("old" + i));
            db.Flush();
            db.IngestExternalFiles([sst], new IngestExternalFileOptions { WriteGlobalSeqno = true });
            LiveFileMetadata f = db.GetLiveFilesMetadata().Single(m => m.LargestSeqno > 34);
            byte[] bytes = File.ReadAllBytes(f.Directory + f.Name);
            byte[] name = Encoding.ASCII.GetBytes("global_seqno");
            int at = bytes.AsSpan().IndexOf(name) + name.Length;
            Assert.AreEqual(f.LargestSeqno, BitConverter.ToUInt64(bytes, at));
        }
        using (var db = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions().SetParanoidChecks(true), dbPath))
        {
            for (int i = 0; i < 100; i++) Assert.AreEqual("ingested" + i, db.Get(Key(i)), "key " + i);
            db.CompactRange(Array.Empty<byte>(), null);
            for (int i = 0; i < 100; i++) Assert.AreEqual("ingested" + i, db.Get(Key(i)), "after native compaction, key " + i);
        }
    }

    /// <summary>allow_db_generated_files: a table file written by the other implementation keeps its sequence numbers.</summary>
    [TestMethod]
    public void DbGeneratedFilesCrossIngest()
    {
        using var dir = new TempDir("ingest-dbgen");
        // A managed-generated file ingested by native.
        string managedSrc = Path.Combine(dir.Path, "msrc");
        string managedFile;
        using (RocksDatabase src = RocksDatabase.Open(managedSrc, new DbOptions { CreateIfMissing = true }))
        {
            for (int i = 0; i < 50; i++) src.Put(U(Key(i)), U("m" + i));
            src.Delete(U(Key(7)));
            src.Flush();
            LiveFileMetadata f = src.GetLiveFilesMetadata().Single();
            managedFile = f.Directory + f.Name;
        }
        var nopts = new RocksDbSharp.IngestExternalFileOptions().SetSnapshotConsistency(false).SetAllowBlockingFlush(false);
        RocksDbSharp.Native.Instance.rocksdb_ingestexternalfileoptions_set_allow_db_generated_files(nopts.Handle, 1);
        RocksDbSharp.Native.Instance.rocksdb_ingestexternalfileoptions_set_link_files(nopts.Handle, 1);
        string nativeDst = Path.Combine(dir.Path, "ndst");
        using (var db = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions().SetCreateIfMissing(true), nativeDst))
        {
            db.IngestExternalFiles([managedFile], nopts);
            FileInfo f = NativeFiles(db).Single();
            Assert.AreEqual(1UL, f.SmallestSeqno);
            Assert.AreEqual(51UL, f.LargestSeqno);
            Assert.AreEqual("m3", db.Get(Key(3)));
            Assert.IsNull(db.Get(Key(7)));
        }

        // A native-generated file ingested by managed.
        string nativeSrc = Path.Combine(dir.Path, "nsrc");
        string nativeFile;
        using (var src = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions().SetCreateIfMissing(true), nativeSrc))
        {
            for (int i = 0; i < 50; i++) src.Put(U(Key(i)), U("n" + i));
            src.Remove(U(Key(9)));
            src.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            RocksDbSharp.LiveFileMetadata f = src.GetLiveFilesMetadata().Single();
            nativeFile = nativeSrc + f.FileMetadata.FileName;
        }
        string managedDst = Path.Combine(dir.Path, "mdst");
        var mopts = new IngestExternalFileOptions { AllowDbGeneratedFiles = true, LinkFiles = true, SnapshotConsistency = false, AllowBlockingFlush = false };
        using (RocksDatabase db = RocksDatabase.Open(managedDst, new DbOptions { CreateIfMissing = true }))
        {
            db.IngestExternalFiles([nativeFile], mopts);
            LiveFileMetadata f = db.GetLiveFilesMetadata().Single();
            Assert.AreEqual(1UL, f.SmallestSeqno);
            Assert.AreEqual(51UL, f.LargestSeqno);
            Assert.AreEqual(51UL, db.GetLatestSequenceNumber());
            Assert.AreEqual("n3", Encoding.UTF8.GetString(db.Get(U(Key(3)))!));
            Assert.IsNull(db.Get(U(Key(9))));
        }
        using (var db = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions(), managedDst))
        {
            Assert.AreEqual("n49", db.Get(Key(49)));
            Assert.IsNull(db.Get(Key(9)));
        }
    }

    private static uint Crc32cOfFile(string path) => RocksDb.Managed.Util.Crc32C.Extend(0, File.ReadAllBytes(path));

    private static byte[] BigEndian(uint v)
    {
        byte[] b = new byte[4];
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32BigEndian(b, v);
        return b;
    }

    private static string? OptionLine(string dbPath, string name)
    {
        string latest = Directory.GetFiles(dbPath, "OPTIONS-*").Order(StringComparer.Ordinal).Last();
        foreach (string line in File.ReadLines(latest))
        {
            string t = line.Trim();
            if (t.StartsWith(name + "=", StringComparison.Ordinal)) return t[(name.Length + 1)..];
        }
        return null;
    }

    /// <summary>
    /// file_checksum_gen_factory = FileChecksumGenCrc32cFactory on both sides: checksums of flushed, compacted, blob and
    /// ingested files are identical (crc32c of the whole file, big endian, "FileChecksumCrc32c"), recorded in the
    /// MANIFEST, and each implementation's VerifyFileChecksums accepts the other's DB and detects a modified file.
    /// </summary>
    [TestMethod]
    public void FileChecksumsMatchNative()
    {
        using var dir = new TempDir("ingest-cksum");
        // Native writer.
        string nativeDb = Path.Combine(dir.Path, "native");
        string nativeSst = WriteSst(Path.Combine(dir.Path, "n.sst"), 1000, 1100, "ing");
        var nopts = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        NativeExtras.SetCrc32cFileChecksums(nopts);
        using (var db = RocksDbSharp.RocksDb.Open(nopts, nativeDb))
        {
            for (int i = 0; i < 200; i++) db.Put(Key(i), "v" + i);
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            db.IngestExternalFiles([nativeSst], new RocksDbSharp.IngestExternalFileOptions());
            db.CompactRange(Array.Empty<byte>(), null);
            Assert.IsNull(NativeExtras.VerifyFileChecksums(db));
        }
        string? nativeFactoryOption = OptionLine(nativeDb, "file_checksum_gen_factory");
        Assert.AreEqual("FileChecksumGenCrc32cFactory", nativeFactoryOption);
        using (RocksDatabase db = RocksDatabase.Open(nativeDb, new DbOptions { FileChecksumGenFactory = FileChecksumGenCrc32cFactory.Instance }))
        {
            foreach (LiveFileMetadata f in db.GetLiveFilesMetadata())
            {
                Assert.AreEqual(FileChecksumGenCrc32cFactory.Crc32cFuncName, f.FileChecksumFuncName);
                CollectionAssert.AreEqual(BigEndian(Crc32cOfFile(f.Directory + f.Name)), f.FileChecksum);
            }
            db.VerifyFileChecksums();
        }

        // Managed writer: flush, blob file, ingestion with a provided (verified) checksum, compaction.
        string managedDb = Path.Combine(dir.Path, "managed");
        string managedSst = WriteSst(Path.Combine(dir.Path, "m.sst"), 1000, 1100, "ing");
        var cf = new ColumnFamilyOptions { EnableBlobFiles = true, MinBlobSize = 64 };
        using (RocksDatabase db = RocksDatabase.Open(managedDb, new DbOptions { CreateIfMissing = true, FileChecksumGenFactory = FileChecksumGenCrc32cFactory.Instance },
                   [new ColumnFamilyDescriptor("default", cf)]))
        {
            for (int i = 0; i < 200; i++) db.Put(U(Key(i)), U(i % 10 == 0 ? new string('b', 100) : "v" + i));
            db.Flush();
            db.IngestExternalFiles(db.DefaultColumnFamily, new IngestExternalFileArg
            {
                ExternalFiles = [managedSst],
                FilesChecksums = [BigEndian(Crc32cOfFile(managedSst))],
                FilesChecksumFuncNames = [FileChecksumGenCrc32cFactory.Crc32cFuncName],
            });
            db.CompactRange(null, null);
            db.VerifyFileChecksums();
            foreach (LiveFileMetadata f in db.GetLiveFilesMetadata())
                CollectionAssert.AreEqual(BigEndian(Crc32cOfFile(f.Directory + f.Name)), f.FileChecksum);
        }
        Assert.AreEqual(nativeFactoryOption, OptionLine(managedDb, "file_checksum_gen_factory"));
        var nopts2 = new RocksDbSharp.DbOptions();
        NativeExtras.SetCrc32cFileChecksums(nopts2);
        using (var db = RocksDbSharp.RocksDb.Open(nopts2, managedDb))
        {
            Assert.IsNull(NativeExtras.VerifyFileChecksums(db), "native verifies the managed checksums (tables and blob files)");
            Assert.AreEqual("ing1050", db.Get(Key(1050)));
        }
        // A modified byte (same size) in a table file: both report a checksum mismatch.
        string victim = Directory.GetFiles(managedDb, "*.sst").Order(StringComparer.Ordinal).Last();
        byte[] bytes = File.ReadAllBytes(victim);
        bytes[10] ^= 0x5A; // inside the first data block: the table still opens
        File.WriteAllBytes(victim, bytes);
        using (var db = RocksDbSharp.RocksDb.Open(nopts2, managedDb))
        {
            StringAssert.Contains(NativeExtras.VerifyFileChecksums(db), "file checksum mismatch");
        }
        using (RocksDatabase db = RocksDatabase.Open(managedDb, new DbOptions { FileChecksumGenFactory = FileChecksumGenCrc32cFactory.Instance }))
        {
            var ex = Assert.ThrowsExactly<RocksDbException>(db.VerifyFileChecksums);
            Assert.AreEqual(StatusCode.Corruption, ex.Code);
            StringAssert.Contains(ex.Message, "file checksum mismatch");
        }
    }

    /// <summary>
    /// Blob direct write files of a managed DB with FileChecksumGenCrc32cFactory carry their whole-file checksum in the
    /// MANIFEST (BlobLogWriter::AppendFooter): native VerifyFileChecksums accepts them and the values are the crc32c of
    /// the files.
    /// </summary>
    [TestMethod]
    public void BlobDirectWriteFileChecksumsAreVerifiedByNative()
    {
        using var dir = new TempDir("bdw-cksum");
        string managedDb = Path.Combine(dir.Path, "managed");
        var cf = new ColumnFamilyOptions { EnableBlobFiles = true, EnableBlobDirectWrite = true, MinBlobSize = 64, BlobDirectWritePartitions = 2 };
        using (RocksDatabase db = RocksDatabase.Open(managedDb,
                   new DbOptions { CreateIfMissing = true, FileChecksumGenFactory = FileChecksumGenCrc32cFactory.Instance, AllowConcurrentMemtableWrite = false },
                   [new ColumnFamilyDescriptor("default", cf)]))
        {
            for (int i = 0; i < 300; i++) db.Put(U(Key(i)), U(i % 3 == 0 ? new string('b', 100 + i) : "v" + i));
            db.Flush();
            db.VerifyFileChecksums();
        }
        Assert.IsTrue(Directory.GetFiles(managedDb, "*.blob").Length >= 2);
        var nopts = new RocksDbSharp.DbOptions();
        NativeExtras.SetCrc32cFileChecksums(nopts);
        using (var db = RocksDbSharp.RocksDb.Open(nopts, managedDb))
        {
            Assert.IsNull(NativeExtras.VerifyFileChecksums(db), "native verifies the direct-write blob file checksums");
            Assert.AreEqual(new string('b', 103), db.Get(Key(3)));
        }
        // Every blob file's recorded checksum must be non-empty: flip a byte, native must notice.
        string victim = Directory.GetFiles(managedDb, "*.blob").Order(StringComparer.Ordinal).First();
        byte[] bytes = File.ReadAllBytes(victim);
        bytes[bytes.Length / 2] ^= 0x5A;
        File.WriteAllBytes(victim, bytes);
        using (var db = RocksDbSharp.RocksDb.Open(nopts, managedDb))
        {
            StringAssert.Contains(NativeExtras.VerifyFileChecksums(db), "file checksum mismatch");
        }
    }

    /// <summary>
    /// IngestExternalFileArg::atomic_replace_range (newer than the C API of the native package, which cannot request it):
    /// the DB the managed replacement leaves behind -- replaced files deleted in the MANIFEST, plus, with universal
    /// compaction and a partial overlap, a generated range-tombstone file below the new data -- opens and reads the same
    /// in native RocksDB, also after a native full compaction, and opens again in the managed port.
    /// </summary>
    [TestMethod]
    public void AtomicReplaceRangeResultIsReadByNative()
    {
        using var dir = new TempDir("ingest-replace");
        string dbPath = Path.Combine(dir.Path, "db");
        string old0 = WriteSst(Path.Combine(dir.Path, "old0.sst"), 0, 100, "old");
        string old1 = WriteSst(Path.Combine(dir.Path, "old1.sst"), 100, 200, "old");
        string old2 = WriteSst(Path.Combine(dir.Path, "old2.sst"), 200, 300, "old");
        string replacement = WriteSst(Path.Combine(dir.Path, "new.sst"), 60, 250, "new");
        var cfo = new ColumnFamilyOptions { CompactionStyle = CompactionStyle.Universal, DisableAutoCompactions = true };
        Dictionary<string, string> expected;
        using (RocksDatabase db = RocksDatabase.Open(dbPath, new DbOptions { CreateIfMissing = true }, cfo))
        {
            db.IngestExternalFiles([old0, old1, old2], new IngestExternalFileOptions());
            for (int i = 290; i < 300; i++) db.Put(U(Key(i)), U("mem" + i)); // memtable data outside the range stays unflushed
            // [key000050, key000260): {100..199} is replaced, {0..99} and {200..299} partially overlap it.
            db.IngestExternalFiles(db.DefaultColumnFamily, new IngestExternalFileArg
            {
                ExternalFiles = [replacement],
                Options = new IngestExternalFileOptions { SnapshotConsistency = false },
                AtomicReplaceRange = new IngestReplaceRange(U(Key(50)), U(Key(260))),
            });
            expected = ManagedScan(db);
            Assert.AreEqual("old49", expected[Key(49)]);
            Assert.IsFalse(expected.ContainsKey(Key(55)));
            Assert.AreEqual("new60", expected[Key(60)]);
            Assert.AreEqual("new249", expected[Key(249)]);
            Assert.IsFalse(expected.ContainsKey(Key(255)));
            Assert.AreEqual("old260", expected[Key(260)]);
            Assert.AreEqual("mem295", expected[Key(295)]);
            Assert.AreEqual(50 + 190 + 40, expected.Count);
        }
        var nopts = new RocksDbSharp.DbOptions().SetDisableAutoCompactions(1).SetCompactionStyle(RocksDbSharp.Compaction.Universal);
        using (var db = RocksDbSharp.RocksDb.Open(nopts, dbPath))
        {
            AssertSameContents(expected, NativeScan(db), "native reads the replaced range");
            db.CompactRange(Array.Empty<byte>(), null);
            AssertSameContents(expected, NativeScan(db), "native after a full compaction");
        }
        using (RocksDatabase db = RocksDatabase.Open(dbPath, new DbOptions(), cfo))
        {
            AssertSameContents(expected, ManagedScan(db), "managed after native compaction");
            // Whole column family replacement, read back by native.
            db.IngestExternalFiles(db.DefaultColumnFamily, new IngestExternalFileArg
            {
                ExternalFiles = [WriteSst(Path.Combine(dir.Path, "all.sst"), 500, 510, "all")],
                Options = new IngestExternalFileOptions { SnapshotConsistency = false },
                AtomicReplaceRange = IngestReplaceRange.EntireColumnFamily,
            });
            expected = ManagedScan(db);
            Assert.AreEqual(10, expected.Count);
        }
        using (var db = RocksDbSharp.RocksDb.Open(nopts, dbPath))
        {
            AssertSameContents(expected, NativeScan(db), "native reads the replaced column family");
        }
    }
}
