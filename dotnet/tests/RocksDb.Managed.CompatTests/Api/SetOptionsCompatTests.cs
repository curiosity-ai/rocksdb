using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Env;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>
/// SetOptions / SetDBOptions against native: the same changes applied by native RocksDB and by the managed port produce
/// the same values in the persisted OPTIONS files, the same accept / reject decisions, and the DB stays readable by
/// the other side.
/// </summary>
[TestClass]
public sealed class SetOptionsCompatTests
{
    private static readonly KeyValuePair<string, string>[] s_cfChanges =
    [
        new("write_buffer_size", "1048576"), new("level0_file_num_compaction_trigger", "7"), new("disable_auto_compactions", "true"),
        new("target_file_size_base", "2097152"), new("max_bytes_for_level_base", "8388608"), new("compression", "kZSTD"),
        new("ttl", "7200"), new("max_write_buffer_number", "4"), new("level0_slowdown_writes_trigger", "30"),
    ];

    private static readonly KeyValuePair<string, string>[] s_dbChanges =
    [
        new("max_background_jobs", "5"), new("delayed_write_rate", "2097152"), new("max_total_wal_size", "1073741824"),
        new("bytes_per_sync", "0"), new("compaction_readahead_size", "1048576"),
    ];

    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    private static OptionsFile Latest(string dir) =>
        OptionsFile.Load(LocalFileSystem.Instance, OptionsFile.FindLatestOptionsFile(LocalFileSystem.Instance, dir)!, preserveUnknownOptions: true);

    [TestMethod]
    public void SameOptionsFileValuesAsNative()
    {
        using var nativeDir = new TempDir("setopt-n");
        using var managedDir = new TempDir("setopt-m");
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        s_keepAlive.Add(o);
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(o, nativeDir.Path))
        {
            db.SetOptions(s_cfChanges);
            RocksDbSharp.Native.Instance.rocksdb_set_db_options(db.Handle, s_dbChanges.Length, s_dbChanges.Select(static p => p.Key).ToArray(),
                s_dbChanges.Select(static p => p.Value).ToArray());
            // Native rejects immutable options too.
            Assert.ThrowsExactly<RocksDbSharp.RocksDbException>(() => db.SetOptions([new("num_levels", "3")]));
            db.Put("k"u8.ToArray(), "v"u8.ToArray());
        }
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true }))
        {
            db.SetOptions(s_cfChanges);
            db.SetDBOptions(s_dbChanges);
            Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => db.SetOptions([new("num_levels", "3")])).Code);
            db.Put("k"u8, "v"u8);
        }
        OptionsFile n = Latest(nativeDir.Path), m = Latest(managedDir.Path);
        ColumnFamilyOptions ncf = n.FindColumnFamily("default")!, mcf = m.FindColumnFamily("default")!;
        foreach (KeyValuePair<string, string> kv in s_cfChanges)
        {
            Assert.AreEqual(ncf.GetOption(kv.Key), mcf.GetOption(kv.Key), kv.Key);
        }
        foreach (KeyValuePair<string, string> kv in s_dbChanges)
        {
            Assert.AreEqual(n.DbOptions.GetOption(kv.Key), m.DbOptions.GetOption(kv.Key), kv.Key);
        }
        Assert.AreEqual("1048576", m.DbOptions.GetOption("bytes_per_sync"));

        // Each side opens the other's DB (with its persisted OPTIONS file present).
        using (RocksDatabase db = RocksDatabase.Open(nativeDir.Path, new DbOptions())) Assert.AreEqual("v", System.Text.Encoding.UTF8.GetString(db.Get("k"u8)!));
        var o2 = new RocksDbSharp.DbOptions();
        s_keepAlive.Add(o2);
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(o2, managedDir.Path))
        {
            Assert.AreEqual("v", db.Get("k"));
        }
    }
}
