using RocksDb.Managed.Env;
using RocksDb.Managed.Tools;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Tools;

/// <summary>ldb / sst_dump equivalents (output formats of tools/ldb_cmd.cc, tools/sst_dump_tool.cc, table/sst_file_dumper.cc).</summary>
[TestClass]
public sealed class ToolsTests
{
    private static (int Rc, string Out, string Err) Run(IFileSystem fs, params string[] args)
    {
        var o = new StringWriter { NewLine = "\n" };
        var e = new StringWriter { NewLine = "\n" };
        int rc = ToolMain.Run(args, fs, o, e);
        return (rc, o.ToString(), e.ToString());
    }

    private static string Ldb(IFileSystem fs, params string[] args)
    {
        (int rc, string o, string e) = Run(fs, ["ldb", "--db=" + DbPath, .. args]);
        Assert.AreEqual(0, rc, e);
        return o;
    }

    private static MemoryFileSystem NewDb()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        db.Put(B("a"), B("1"));
        db.Put(B("b"), B("2"));
        db.Flush();
        db.Put(B("c"), B("3"));
        db.Delete(B("a"));
        return fs;
    }

    [TestMethod]
    public void DataCommandsMatchNativeOutput()
    {
        MemoryFileSystem fs = NewDb();
        Assert.AreEqual("2\n", Ldb(fs, "get", "b"));
        Assert.AreEqual("Key not found\n", Ldb(fs, "get", "zz"));
        Assert.AreEqual("0x32\n", Ldb(fs, "--hex", "get", "0x62"));
        Assert.AreEqual("b ==> 2\nc ==> 3\n", Ldb(fs, "scan"));
        Assert.AreEqual("0x62 ==> 0x32\n", Ldb(fs, "--hex", "scan", "--from=0x62", "--to=0x63"));
        Assert.AreEqual("b\nc\n", Ldb(fs, "scan", "--no_value"));
        Assert.AreEqual("b ==> 2\nc ==> 3\nKeys in range: 2\n", Ldb(fs, "dump"));
        Assert.AreEqual("Keys in range: 1\n", Ldb(fs, "dump", "--count_only", "--max_keys=1").Split("Value size")[0]);
        Assert.AreEqual("b ==> 2\nKey not found: x\n", Ldb(fs, "multi_get", "b", "x"));
        Assert.AreEqual("'a' seq:4, type:0 => \n'a' seq:1, type:1 => 1\n'b' seq:2, type:1 => 2\n'c' seq:3, type:1 => 3\nInternal keys in range: 4\n",
            Ldb(fs, "idump"));
        Assert.AreEqual("OK\n", Ldb(fs, "put", "d", "4"));
        Assert.AreEqual("OK\n", Ldb(fs, "batchput", "e", "5", "f", "6"));
        Assert.AreEqual("OK\n", Ldb(fs, "delete", "e"));
        Assert.AreEqual("OK\n", Ldb(fs, "deleterange", "f", "g"));
        Assert.AreEqual("b ==> 2\nc ==> 3\nd ==> 4\n", Ldb(fs, "scan"));
        Assert.AreEqual("OK\n", Ldb(fs, "checkconsistency"));
        Assert.AreEqual("", Ldb(fs, "compact"));
        Assert.IsTrue(ulong.Parse(Ldb(fs, "approxsize", "--from=a", "--to=z").Trim(), System.Globalization.CultureInfo.InvariantCulture) > 0);
        StringAssert.StartsWith(Ldb(fs, "get_property", "rocksdb.estimate-num-keys"), "rocksdb.estimate-num-keys: ");

        (int rc, _, string err) = Run(fs, "ldb", "--db=" + DbPath, "get");
        Assert.AreEqual(1, rc);
        Assert.AreEqual("Failed: <key> must be specified for the get command\n", err);
        (rc, _, err) = Run(fs, "ldb", "--db=/missing", "get", "a");
        Assert.AreEqual(1, rc);
        StringAssert.StartsWith(err, "Failed: ");
        (rc, _, err) = Run(fs, "ldb", "--db=" + DbPath, "nosuchcommand");
        Assert.AreEqual(1, rc);
        StringAssert.StartsWith(err, "Unknown command\n");
    }

    [TestMethod]
    public void ColumnFamilyCommands()
    {
        MemoryFileSystem fs = NewDb();
        Assert.AreEqual("OK\n", Ldb(fs, "create_column_family", "cf1"));
        Assert.AreEqual("Column families in " + DbPath + ": \n{default, cf1}\n", Ldb(fs, "list_column_families"));
        Assert.AreEqual("OK\n", Ldb(fs, "--column_family=cf1", "put", "x", "y"));
        Assert.AreEqual("x ==> y\n", Ldb(fs, "--column_family=cf1", "scan"));
        Assert.AreEqual("OK\n", Ldb(fs, "drop_column_family", "cf1"));
        Assert.AreEqual("Column families in " + DbPath + ": \n{default}\n", Ldb(fs, "list_column_families"));
        (int rc, _, string err) = Run(fs, "ldb", "--db=" + DbPath, "drop_column_family", "nope");
        Assert.AreEqual(1, rc);
        Assert.AreEqual("Failed: Column family: nope doesn't exist in db.\n", err);
    }

    [TestMethod]
    public void WalAndManifestDumps()
    {
        MemoryFileSystem fs = NewDb();
        string wal = Ldb(fs, "dump_wal", "--walfile=" + DbPath, "--header", "--print_value");
        StringAssert.Contains(wal, "Sequence,Count,ByteSize,Physical Offset,Key(s) : value \n");
        StringAssert.Contains(wal, "3,1,");
        StringAssert.Contains(wal, ",PUT(0) : 0x63 : 0x33 \n");
        StringAssert.Contains(wal, ",DELETE(0) : 0x61 \n");
        Assert.IsFalse(wal.Contains("not opened in DB", StringComparison.Ordinal));
        string walNoValue = Ldb(fs, "dump_wal", "--walfile=" + DbPath);
        StringAssert.Contains(walNoValue, ",PUT(0) : 0x63 \n");

        string m = Ldb(fs, "manifest_dump");
        StringAssert.StartsWith(m, "--------------- Column family \"default\"  (ID 0) --------------\nlog number: ");
        StringAssert.Contains(m, "comparator: leveldb.BytewiseComparator\n--- level 0 --- version# ");
        StringAssert.Contains(m, "['a' seq:1, type:1 .. 'b' seq:2, type:1]\n");
        StringAssert.Contains(m, "next_file_number ");
        string verbose = Ldb(fs, "manifest_dump", "--verbose");
        StringAssert.Contains(verbose, "VersionEdit {\n  Comparator: leveldb.BytewiseComparator");
        StringAssert.Contains(verbose, "\n  AddFile: 0 ");

        string checksums = Ldb(fs, "file_checksum_dump");
        StringAssert.EndsWith(checksums, "Print SST file checksum information finished \n");
        string live = Ldb(fs, "list_live_files_metadata", "--sort_by_filename");
        StringAssert.StartsWith(live, "Live SST and Blob Files:\n");
        StringAssert.Contains(live, ".sst : level 0, column family 'default'\n");
    }

    [TestMethod]
    public void SstDumpCommands()
    {
        MemoryFileSystem fs = NewDb();
        string sst = null!;
        foreach (string f in fs.GetChildren(DbPath)) if (f.EndsWith(".sst", StringComparison.Ordinal)) sst = DbPath + "/" + f;
        (int rc, string o, string e) = Run(fs, "sst_dump", "--file=" + sst, "--command=scan", "--output_hex");
        Assert.AreEqual(0, rc, e);
        StringAssert.Contains(o, "Process " + sst + "\nSst file format: block-based\nfrom [] to []\n'61' seq:1, type:1 => 31\n'62' seq:2, type:1 => 32\n");
        (rc, o, _) = Run(fs, "sst_dump", "--file=" + sst, "--command=scan", "--from=b", "--show_properties");
        Assert.AreEqual(0, rc);
        StringAssert.Contains(o, "from [62] to []\n'b' seq:2, type:1 => 2\nTable Properties:\n------------------------------\n  # data blocks: 1\n");
        StringAssert.Contains(o, "  # entries: 2\n");
        StringAssert.Contains(o, "  column family name: default\n");
        (rc, o, _) = Run(fs, "sst_dump", "--file=" + sst, "--command=verify");
        Assert.AreEqual(0, rc);
        StringAssert.EndsWith(o, "The file is ok\n");
        (rc, o, _) = Run(fs, "sst_dump", "--file=" + DbPath, "--command=identify");
        Assert.AreEqual(0, rc);
        StringAssert.Contains(o, "List of valid SST files found in " + DbPath + ":\n" + sst + "\nNumber of valid SST files: 1\n");
        (rc, o, _) = Run(fs, "sst_dump", "--file=" + sst, "--command=raw");
        Assert.AreEqual(0, rc);
        string dumpName = sst[..^4] + "_dump.txt";
        StringAssert.EndsWith(o, "raw dump written to file " + dumpName + "\n");
        using (ISequentialFile f = fs.OpenSequential(dumpName))
        {
            byte[] buf = new byte[fs.GetFileSize(dumpName)];
            f.Read(buf);
            string raw = System.Text.Encoding.Latin1.GetString(buf);
            StringAssert.StartsWith(raw, "Footer Details:\n");
            StringAssert.Contains(raw, "Data Block # 1 @ ");
            StringAssert.Contains(raw, "  HEX    61: 31\n  ASCII  a : 1 \n  ------\n");
        }
        (rc, _, e) = Run(fs, "sst_dump", "--file=" + DbPath + "/CURRENT");
        Assert.AreEqual(1, rc);
        Assert.AreEqual(DbPath + "/CURRENT is not a valid SST file\n", e);
        (rc, _, e) = Run(fs, "sst_dump", "--bogus");
        Assert.AreEqual(1, rc);
        StringAssert.StartsWith(e, "Unrecognized argument '--bogus'");
    }

    [TestMethod]
    public void RepairAndReduceLevels()
    {
        MemoryFileSystem fs = NewDb();
        foreach (string f in fs.GetChildren(DbPath))
        {
            if (f.StartsWith("MANIFEST-", StringComparison.Ordinal)) fs.DeleteFile(DbPath + "/" + f);
        }
        Assert.AreEqual("OK\n", Ldb(fs, "repair"));
        Assert.AreEqual("b ==> 2\nc ==> 3\n", Ldb(fs, "scan"));
        Assert.AreEqual("", Ldb(fs, "compact"));
        string o = Ldb(fs, "reduce_levels", "--new_levels=3", "--print_old_levels");
        StringAssert.StartsWith(o, "The old number of levels in use is 7\nCompacting the db...\n");
        StringAssert.Contains(Ldb(fs, "list_live_files_metadata", "--sort_by_filename"), ".sst : level 2, column family 'default'\n");
        Assert.AreEqual("b ==> 2\nc ==> 3\n", Ldb(fs, "scan"));
    }
}
