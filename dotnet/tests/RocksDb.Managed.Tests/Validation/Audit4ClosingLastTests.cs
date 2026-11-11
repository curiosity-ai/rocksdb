using RocksDb.Managed.Compression;
using RocksDb.Managed.Env;
using RocksDb.Managed.Options;
using RocksDb.Managed.Tools;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Regression tests of the audit #4 "closing last" pass: lazy wide-column range reads (db/wide/read_path_blob_resolver.cc)
/// and the ldb tool's option overrides, live file listing and dump dispatch (tools/ldb_cmd.cc).
/// </summary>
[TestClass]
public sealed class Audit4ClosingLastTests
{
    private static WideColumn C(string name, string value) => new(B(name), B(value));

    private static (int Rc, string Out, string Err) Tool(IFileSystem fs, params string[] args)
    {
        var o = new StringWriter { NewLine = "\n" };
        var e = new StringWriter { NewLine = "\n" };
        int rc = ToolMain.Run(args, fs, o, e);
        return (rc, o.ToString(), e.ToString());
    }

    private static (int Rc, string Out, string Err) Ldb(IFileSystem fs, params string[] args) => Tool(fs, ["ldb", "--db=" + DbPath, .. args]);

    private static MemoryFileSystem NewDb(bool blobs)
    {
        MemoryFileSystem fs = NewFs();
        var cfo = new ColumnFamilyOptions { EnableBlobFiles = blobs, MinBlobSize = 8 };
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        db.Put(B("a.1"), B(new string('x', 40)));
        db.Put(B("a.2"), B("v"));
        db.Put(B("b.1"), B("v"));
        db.Flush();
        return fs;
    }

    private static string FileWithSuffix(MemoryFileSystem fs, string suffix)
    {
        foreach (string f in fs.GetChildren(DbPath))
        {
            if (f.EndsWith(suffix, StringComparison.Ordinal)) return DbPath + "/" + f;
        }
        Assert.Fail("no " + suffix + " file");
        return "";
    }

    [TestMethod]
    public void LazyMultiResolve_EmptyRangeOfBlobReference_DoesNoIo()
    {
        // ReadPathBlobResolver::ResolveColumnRange (read_path_blob_resolver.cc:199-205): a strict sub-range of an
        // unresolved uncompressed blob reference whose offset is at or past the end, or whose length is 0, is "Nothing
        // to read; empty (not an error), no I/O". ClassifyColumnRange (:273-276) sends such a read of a batch to the
        // same path. The port fetched (and cached) the whole blob for it.
        MemoryFileSystem fs = NewFs();
        var st = new Statistics();
        DbOptions o = NewOptions(fs);
        o.Statistics = st;
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, new ColumnFamilyOptions { EnableBlobFiles = true, MinBlobSize = 32 });
        db.PutEntity(B("k"), [C("", "default"), C("big", new string('b', 200))]);
        db.Flush();

        using var lazy = new LazyWideColumns();
        Assert.IsTrue(db.DefaultColumnFamily.TryGetEntityLazy(null, B("k"), lazy));
        int big = lazy.IndexOf("big"u8);
        LazyColumnReadRequest[] reads = [new(big, 1000, 10), new(big, 200, LazyColumnReadRequest.WholeColumn), new(big, 0, 0), new(big, 5, 0)];
        lazy.MultiResolve(reads);
        foreach (LazyColumnReadRequest r in reads)
        {
            Assert.IsTrue(r.Succeeded);
            Assert.AreEqual(0, r.Result.Length);
        }
        Assert.AreEqual(0UL, st.GetTickerCount(Ticker.BlobDbLazyReadCount));
        Assert.AreEqual(0UL, st.GetTickerCount(Ticker.BlobDbLazyPartialReadCount));

        using var batch = new LazyWideColumnsBatch();
        using var keys = new MultiGetBatch(1);
        keys.Add(B("k"));
        db.DefaultColumnFamily.MultiGetEntityLazy(null, keys, batch);
        Assert.AreEqual(MultiGetStatus.Found, batch.GetStatus(0));
        LazyColumnReadRequest[] batchReads = [new(0, big, 1000, 10), new(0, big, 0, 0)];
        batch.MultiResolve(batchReads);
        foreach (LazyColumnReadRequest r in batchReads)
        {
            Assert.IsTrue(r.Succeeded);
            Assert.AreEqual(0, r.Result.Length);
        }
        Assert.AreEqual(0UL, st.GetTickerCount(Ticker.BlobDbLazyReadCount));

        // The column is still readable afterwards (one whole fetch).
        Assert.AreEqual(200, lazy.ResolveColumn(big).Length);
        Assert.AreEqual(1UL, st.GetTickerCount(Ticker.BlobDbLazyReadCount));
    }

    [TestMethod]
    public void Ldb_OptionOverrides_AreValidated()
    {
        // LDBCommand::OverrideBaseCFOptions (tools/ldb_cmd.cc:1012-1222) via PrepareOptions (:1273): every command that
        // opens the DB checks the override options; the port accepted and ignored them.
        MemoryFileSystem fs = NewDb(blobs: false);
        (string[] Args, string Message)[] cases =
        [
            (["--bloom_bits=0"], "bloom_bits must be > 0."),
            (["--block_size=-4"], "block_size must be > 0."),
            (["--compression_type=bogus"], "Unknown compression algorithm: bogus"),
            (["--compression_type=bzip2"], "bzip2 compression is not supported in this build."),
            (["--write_buffer_size=0"], "write_buffer_size must be > 0."),
            (["--write_buffer_size=abc"], "write_buffer_size has an invalid value."),
            (["--file_size=0"], "file_size must be > 0."),
            (["--compression_max_dict_bytes=-1"], "compression_max_dict_bytes must be >= 0."),
            // exec_state_ is overwritten by each failing option: the last one is reported.
            (["--bloom_bits=0", "--file_size=0"], "file_size must be > 0."),
        ];
        foreach ((string[] args, string message) in cases)
        {
            (int rc, _, string e) = Ldb(fs, ["get", "a.2", .. args]);
            Assert.AreEqual(1, rc, string.Join(' ', args));
            StringAssert.Contains(e, "Failed: " + message, string.Join(' ', args));
        }
        // Valid values still work.
        (int ok, string o, string err) = Ldb(fs, "get", "a.2", "--bloom_bits=10", "--block_size=1024", "--compression_type=no", "--write_buffer_size=1048576",
            "--file_size=1048576");
        Assert.AreEqual(0, ok, err);
        Assert.AreEqual("v\n", o);
    }

    [TestMethod]
    public void Ldb_OptionOverrides_AreApplied()
    {
        // LDBCommand::OverrideBaseCFOptions: cf_opts->compression / write_buffer_size / target_file_size_base and a
        // block-based table factory with the bloom filter and block size (ldb_cmd.cc:1017-1068, 1168-1210). The DB
        // opened for the write persists them in its OPTIONS file.
        MemoryFileSystem fs = NewDb(blobs: false);
        (int rc, _, string e) = Ldb(fs, "put", "c.1", "v", "--compression_type=zstd", "--write_buffer_size=1048576", "--file_size=2097152", "--bloom_bits=7",
            "--block_size=1024");
        Assert.AreEqual(0, rc, e);
        string latest = OptionsFile.FindLatestOptionsFile(fs, DbPath)!;
        ColumnFamilyOptions cf = OptionsFile.Load(fs, latest, preserveUnknownOptions: true).FindColumnFamily("default")!;
        Assert.AreEqual(CompressionType.Zstd, cf.Compression);
        Assert.AreEqual(1048576UL, cf.WriteBufferSize);
        Assert.AreEqual(2097152UL, cf.TargetFileSizeBase);
        Assert.AreEqual(1024UL, cf.TableOptions.BlockSize);
        Assert.IsNotNull(cf.TableOptions.FilterPolicy);
    }

    [TestMethod]
    public void Ldb_ListLiveFilesMetadata_ListsBlobFiles()
    {
        // DBLiveFilesMetadataDumperCommand::DoCommand (tools/ldb_cmd.cc:5011-5022, 5064-5075): the blob files of each
        // column family follow "Live Blob Files:"; with --sort_by_filename they are listed as "<file>, column family
        // '<cf>'" (level -1). The port never listed them.
        MemoryFileSystem fs = NewDb(blobs: true);
        string blob = FileWithSuffix(fs, ".blob");
        string sst = FileWithSuffix(fs, ".sst");
        (int rc, string o, string e) = Ldb(fs, "list_live_files_metadata");
        Assert.AreEqual(0, rc, e);
        StringAssert.Contains(o, "Live Blob Files:\n" + blob + "\n------------------------------\n");
        StringAssert.Contains(o, "---------- level 0 ----------\n" + sst + "\n");

        (rc, o, e) = Ldb(fs, "list_live_files_metadata", "--sort_by_filename");
        Assert.AreEqual(0, rc, e);
        string blobLine = blob + ", column family 'default'\n", sstLine = sst + " : level 0, column family 'default'\n";
        // std::sort by file name: the two lines in name order.
        string expected = string.CompareOrdinal(blob, sst) < 0 ? blobLine + sstLine : sstLine + blobLine;
        Assert.AreEqual("Live SST and Blob Files:\n" + expected + "------------------------------\n", o);
    }

    [TestMethod]
    public void Ldb_Dump_WithDbAndPath_DumpsTheDb()
    {
        // LDBCommand::Run (tools/ldb_cmd.cc:476-488) opens the DB of --db; DBDumperCommand::DoCommand (:2472-2516) then
        // takes the "db_ != nullptr" branch (DoDumpCommand) and never looks at --path. The port dumped the file.
        MemoryFileSystem fs = NewDb(blobs: false);
        string sst = FileWithSuffix(fs, ".sst");
        (int rc, string dbDump, string e) = Ldb(fs, "dump");
        Assert.AreEqual(0, rc, e);
        StringAssert.Contains(dbDump, "a.2 ==> v\n");
        (rc, string both, e) = Ldb(fs, "dump", "--path=" + sst);
        Assert.AreEqual(0, rc, e);
        Assert.AreEqual(dbDump, both);
        // Without --db the file is dumped (unchanged).
        (rc, string file, e) = Tool(fs, "ldb", "dump", "--path=" + sst);
        Assert.AreEqual(0, rc, e);
        Assert.AreNotEqual(dbDump, file);
    }
}
