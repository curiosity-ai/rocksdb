using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Env;
using RocksDb.Managed.Tools;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>The managed ldb / sst_dump tools on native DB directories, and native reads of DBs the tools wrote.</summary>
[TestClass]
public sealed class ToolsCompatTests
{
    private static (int Rc, string Out, string Err) Run(params string[] args)
    {
        var o = new StringWriter { NewLine = "\n" };
        var e = new StringWriter { NewLine = "\n" };
        int rc = ToolMain.Run(args, LocalFileSystem.Instance, o, e);
        return (rc, o.ToString(), e.ToString());
    }

    private static string Ok(params string[] args)
    {
        (int rc, string o, string e) = Run(args);
        Assert.AreEqual(0, rc, e);
        return o;
    }

    private static RocksDbSharp.RocksDb OpenNative(string path)
    {
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetCreateMissingColumnFamilies(true);
        return RocksDbSharp.RocksDb.Open(o, path, new RocksDbSharp.ColumnFamilies { { "one", new RocksDbSharp.ColumnFamilyOptions() } });
    }

    [TestMethod]
    public void ToolsReadNativeDb()
    {
        using var dir = new TempDir("tools");
        using (RocksDbSharp.RocksDb n = OpenNative(dir.Path))
        {
            n.Put("a", "1");
            n.Put("b", "2");
            n.Flush(new RocksDbSharp.FlushOptions());
            n.Put("c", "3");
            n.Remove("a");
            n.Put("x", "9", n.GetColumnFamily("one"));
        }
        string db = "--db=" + dir.Path;
        Assert.AreEqual("b ==> 2\nc ==> 3\n", Ok("ldb", db, "scan"));
        Assert.AreEqual("x ==> 9\n", Ok("ldb", db, "--column_family=one", "scan"));
        Assert.AreEqual("Column families in " + dir.Path + ": \n{default, one}\n", Ok("ldb", db, "list_column_families"));
        Assert.AreEqual("'a' seq:4, type:0 => \n'a' seq:1, type:1 => 1\n'b' seq:2, type:1 => 2\n'c' seq:3, type:1 => 3\nInternal keys in range: 4\n", Ok("ldb", db, "idump"));
        string m = Ok("ldb", db, "manifest_dump");
        StringAssert.Contains(m, "--------------- Column family \"one\"  (ID 1) --------------\n");
        StringAssert.Contains(m, "['a' seq:1, type:1 .. 'b' seq:2, type:1]\n");
        string wal = Ok("ldb", db, "dump_wal", "--walfile=" + dir.Path, "--header", "--print_value");
        StringAssert.Contains(wal, ",PUT(0) : 0x63 : 0x33 \n");
        StringAssert.Contains(wal, ",DELETE(0) : 0x61 \n");
        StringAssert.Contains(wal, ",PUT(1) : 0x78 : 0x39 \n");
        string sst = Directory.GetFiles(dir.Path, "*.sst")[0];
        string scan = Ok("sst_dump", "--file=" + sst, "--command=scan", "--show_properties");
        StringAssert.Contains(scan, "'a' seq:1, type:1 => 1\n'b' seq:2, type:1 => 2\n");
        StringAssert.Contains(scan, "  # entries: 2\n");
        StringAssert.Contains(scan, "  comparator name: leveldb.BytewiseComparator\n");
        StringAssert.EndsWith(Ok("sst_dump", "--file=" + sst, "--command=verify"), "The file is ok\n");
        Assert.AreEqual("OK\n", Ok("ldb", db, "checkconsistency"));
        string dump = Ok("ldb", "dump", "--path=" + sst);
        StringAssert.Contains(dump, "'b' seq:2, type:1 => 2\n");
        StringAssert.Contains(dump, "\nTable Properties:\n# data blocks: 1\n");
    }

    [TestMethod]
    public void NativeReadsDbWrittenByTools()
    {
        using var dir = new TempDir("tools");
        string db = "--db=" + dir.Path;
        Assert.AreEqual("OK\n", Ok("ldb", db, "--create_if_missing", "put", "k1", "v1"));
        Assert.AreEqual("OK\n", Ok("ldb", db, "--hex", "batchput", "0x6B32", "0x7632", "0x6B33", "0x7633"));
        Assert.AreEqual("OK\n", Ok("ldb", db, "create_column_family", "one"));
        Assert.AreEqual("OK\n", Ok("ldb", db, "--column_family=one", "put", "x", "9"));
        Assert.AreEqual("", Ok("ldb", db, "compact"));
        using RocksDbSharp.RocksDb n = OpenNative(dir.Path);
        Assert.AreEqual("v1", n.Get("k1"));
        Assert.AreEqual("v3", n.Get("k3"));
        Assert.AreEqual("9", n.Get("x", n.GetColumnFamily("one")));
    }
}
