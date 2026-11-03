using RocksDb.Managed.Env;

namespace RocksDb.Managed.Tests.Db;

/// <summary>Open-time error mapping on the real local file system (DBImpl::Recover / DB::Open semantics).</summary>
[TestClass]
public sealed class DbOpenErrorTests
{
    [TestMethod]
    public void OpenMissingDirectoryWithoutCreateIfMissingIsInvalidArgument()
    {
        string path = Path.Combine(Path.GetTempPath(), "rocksdb-managed-missing-" + Guid.NewGuid().ToString("N"));
        var ex = Assert.ThrowsExactly<RocksDbException>(() =>
            RocksDatabase.Open(path, new DbOptions { CreateIfMissing = false, FileSystem = LocalFileSystem.Instance }).Dispose());
        Assert.AreEqual(StatusCode.InvalidArgument, ex.Code, ex.Message);
        Assert.IsFalse(Directory.Exists(path), "open must not create the directory");
    }

    [TestMethod]
    public void OpenEmptyDirectoryWithoutCreateIfMissingIsInvalidArgument()
    {
        string path = Path.Combine(Path.GetTempPath(), "rocksdb-managed-empty-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        try
        {
            var ex = Assert.ThrowsExactly<RocksDbException>(() =>
                RocksDatabase.Open(path, new DbOptions { CreateIfMissing = false, FileSystem = LocalFileSystem.Instance }).Dispose());
            Assert.AreEqual(StatusCode.InvalidArgument, ex.Code, ex.Message);
        }
        finally
        {
            Directory.Delete(path, recursive: true);
        }
    }

    // ---- LoadTableHandlers (VersionEditHandler::LoadTables at DB::Open) ------------------------------------------------

    /// <summary>A DB with <paramref name="files"/> L0 tables (keys "f{file}-{i}"), closed; returns the table paths, oldest first.</summary>
    private static List<string> BuildTables(MemoryFileSystem fs, int files)
    {
        using (RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, DbTestUtil.NewOptions(fs), new ColumnFamilyOptions { DisableAutoCompactions = true }))
        {
            for (int f = 0; f < files; f++)
            {
                for (int i = 0; i < 20; i++) db.Put(DbTestUtil.B("f" + f + "-" + i), DbTestUtil.B("v" + f));
                db.Flush();
            }
        }
        var tables = new List<(ulong, string)>();
        foreach (string child in fs.GetChildren(DbTestUtil.DbPath))
        {
            if (RocksDb.Managed.Db.FileNames.TryParseFileName(child, out ulong n, out RocksDb.Managed.Db.FileType t) && t == RocksDb.Managed.Db.FileType.TableFile)
                tables.Add((n, DbTestUtil.DbPath + "/" + child));
        }
        tables.Sort();
        return tables.ConvertAll(static x => x.Item2);
    }

    private static void Truncate(MemoryFileSystem fs, string path, int bytes)
    {
        byte[] data = fs.ReadAllBytes(path);
        fs.DeleteFile(path);
        using IWritableFile w = fs.CreateWritable(path);
        w.Append(data.AsSpan(0, data.Length - bytes));
        w.Sync();
    }

    private static string[] Listing(MemoryFileSystem fs) =>
        fs.GetChildren(DbTestUtil.DbPath).Where(static n => !n.StartsWith("LOG", StringComparison.Ordinal)).Order(StringComparer.Ordinal).ToArray();

    /// <summary>
    /// With max_open_files = -1 every table is opened at DB::Open: a truncated table (size differs from the MANIFEST) or a
    /// missing one fails the open with Corruption naming the MANIFEST, before anything (MANIFEST, WAL, OPTIONS) is written.
    /// </summary>
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void DamagedTableFailsOpen(bool delete)
    {
        var fs = DbTestUtil.NewFs();
        List<string> tables = BuildTables(fs, 3);
        if (delete) fs.DeleteFile(tables[1]);
        else Truncate(fs, tables[1], 3);
        string[] before = Listing(fs);
        var ex = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbTestUtil.DbPath, DbTestUtil.NewOptions(fs)).Dispose());
        Assert.AreEqual(StatusCode.Corruption, ex.Code, ex.Message);
        StringAssert.Contains(ex.Message, delete ? "Corruption: " : "Sst file size mismatch");
        StringAssert.Contains(ex.Message, "MANIFEST-");
        StringAssert.Contains(ex.Message, "may be corrupted.");
        CollectionAssert.AreEqual(before, Listing(fs),
            "the failed open wrote nothing");
    }

    /// <summary>paranoid_checks = false: LoadTables errors are ignored; the damaged table fails when it is read.</summary>
    [TestMethod]
    public void WithoutParanoidChecksTheDamagedTableFailsOnRead()
    {
        var fs = DbTestUtil.NewFs();
        List<string> tables = BuildTables(fs, 3);
        Truncate(fs, tables[0], 3);
        DbOptions o = DbTestUtil.NewOptions(fs);
        o.ParanoidChecks = false;
        o.SkipStatsUpdateOnDbOpen = true;
        using RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, o);
        Assert.AreEqual("v2", DbTestUtil.GetString(db, "f2-3"));
        var ex = Assert.ThrowsExactly<RocksDbException>(() => db.Get(DbTestUtil.B("f0-3")));
        Assert.AreEqual(StatusCode.Corruption, ex.Code, ex.Message);
        StringAssert.Contains(ex.Message, "Sst file size mismatch");
    }

    /// <summary>
    /// With a bounded table cache only min(16, capacity / 4) tables are opened at DB::Open (kInitialLoadLimit); a damaged
    /// table outside that set opens fine and fails with the same size check when it is first read.
    /// </summary>
    [TestMethod]
    public void BoundedTableCacheLoadsOnlyTheInitialLimit()
    {
        var fs = DbTestUtil.NewFs();
        List<string> tables = BuildTables(fs, 8);
        // L0 is loaded newest first: the oldest table is past the limit of min(16, 20 / 4) = 5.
        Truncate(fs, tables[0], 3);
        DbOptions o = DbTestUtil.NewOptions(fs);
        o.MaxOpenFiles = 20;
        o.SkipStatsUpdateOnDbOpen = true;
        using (RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, o))
        {
            Assert.AreEqual(5, db.DefaultColumnFamily.Data.TableCache.Count);
            Assert.AreEqual("v7", DbTestUtil.GetString(db, "f7-1"));
            var ex = Assert.ThrowsExactly<RocksDbException>(() => db.Get(DbTestUtil.B("f0-1")));
            Assert.AreEqual(StatusCode.Corruption, ex.Code, ex.Message);
            StringAssert.Contains(ex.Message, "Sst file size mismatch");
        }
        // Unbounded: the same DB fails at open.
        DbOptions all = DbTestUtil.NewOptions(fs);
        Assert.AreEqual(StatusCode.Corruption,
            Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbTestUtil.DbPath, all).Dispose()).Code);
    }

    /// <summary>verify_sst_unique_id_in_manifest: the MANIFEST's unique id must match the one derived from the table properties.</summary>
    [TestMethod]
    public void UniqueIdMismatchIsCorruption()
    {
        var fs = DbTestUtil.NewFs();
        BuildTables(fs, 1);
        RocksDb.Managed.Db.FileMetaData meta;
        using (RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, DbTestUtil.NewOptions(fs)))
        {
            meta = db.DefaultColumnFamily.Data.Current.Storage.LevelFiles(0)[0];
            Assert.AreNotEqual(RocksDb.Managed.Db.UniqueId64x2.Null, meta.UniqueId);
        }
        foreach (bool verify in new[] { true, false })
        {
            using var cache = new RocksDb.Managed.Db.TableCache(fs, [DbTestUtil.DbPath], RocksDb.Managed.Table.TableReaderOptions.Default, -1)
            {
                VerifyUniqueIdInManifest = verify,
            };
            var wrong = new RocksDb.Managed.Db.FileMetaData
            {
                FileNumber = meta.FileNumber,
                FileSize = meta.FileSize,
                LargestSeqno = meta.LargestSeqno,
                UniqueId = new RocksDb.Managed.Db.UniqueId64x2(meta.UniqueId.Lo, meta.UniqueId.Hi ^ 1),
            };
            if (!verify)
            {
                using RocksDb.Managed.Db.TableReaderLease lease = cache.GetReader(wrong);
                continue;
            }
            var ex = Assert.ThrowsExactly<RocksDbException>(() => cache.GetReader(wrong).Dispose());
            Assert.AreEqual(StatusCode.Corruption, ex.Code, ex.Message);
            StringAssert.Contains(ex.Message, "Mismatch in unique ID on table file");
            using RocksDb.Managed.Db.TableReaderLease ok = cache.GetReader(meta);
        }
    }
}
