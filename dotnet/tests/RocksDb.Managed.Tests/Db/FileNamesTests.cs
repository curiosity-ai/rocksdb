using RocksDb.Managed.Db;
using RocksDb.Managed.Env;

namespace RocksDb.Managed.Tests.Db;

/// <summary>Port of db/filename_test.cc plus CURRENT/IDENTITY round trips.</summary>
[TestClass]
public sealed class FileNamesTests
{
    private const int DefaultInfoLogDir = 1, DifferentInfoLogDir = 2, NoCheckLogDir = 4, AllMode = 7;

    private static readonly (string Name, ulong Number, FileType Type, int Mode)[] s_cases =
    [
        ("100.log", 100, FileType.WalFile, AllMode),
        ("0.log", 0, FileType.WalFile, AllMode),
        ("0.sst", 0, FileType.TableFile, AllMode),
        ("CURRENT", 0, FileType.CurrentFile, AllMode),
        ("LOCK", 0, FileType.DbLockFile, AllMode),
        ("MANIFEST-2", 2, FileType.DescriptorFile, AllMode),
        ("MANIFEST-7", 7, FileType.DescriptorFile, AllMode),
        ("METADB-2", 2, FileType.MetaDatabase, AllMode),
        ("METADB-7", 7, FileType.MetaDatabase, AllMode),
        ("LOG", 0, FileType.InfoLogFile, DefaultInfoLogDir),
        ("LOG.old", 0, FileType.InfoLogFile, DefaultInfoLogDir),
        ("LOG.old.6688", 6688, FileType.InfoLogFile, DefaultInfoLogDir),
        ("rocksdb_dir_LOG", 0, FileType.InfoLogFile, DifferentInfoLogDir),
        ("rocksdb_dir_LOG.old", 0, FileType.InfoLogFile, DifferentInfoLogDir),
        ("rocksdb_dir_LOG.old.6688", 6688, FileType.InfoLogFile, DifferentInfoLogDir),
        ("18446744073709551615.log", ulong.MaxValue, FileType.WalFile, AllMode),
        ("IDENTITY", 0, FileType.IdentityFile, AllMode),
        ("OPTIONS-000012", 12, FileType.OptionsFile, AllMode),
        ("OPTIONS-000012.dbtmp", 12, FileType.TempFile, AllMode),
        ("000005.blob", 5, FileType.BlobFile, AllMode),
        ("000007.ldb", 7, FileType.TableFile, AllMode),
        ("000008.dbtmp", 8, FileType.TempFile, AllMode),
        ("COMPACTION_PROGRESS-99", 99, FileType.CompactionProgressFile, AllMode),
    ];

    private static readonly string[] s_errors =
    [
        "", "foo", "foo-dx-100.log", ".log", "", "manifest", "CURREN", "CURRENTX", "MANIFES", "MANIFEST", "MANIFEST-",
        "XMANIFEST-3", "MANIFEST-3x", "META", "METADB", "METADB-", "XMETADB-3", "METADB-3x", "LOC", "LOCKx", "LO", "LOGx",
        "18446744073709551616.log", "184467440737095516150.log", "100", "100.", "100.lop", "archive/000001.sst",
    ];

    [TestMethod]
    public void Parse()
    {
        foreach (int mode in new[] { DifferentInfoLogDir, DefaultInfoLogDir, NoCheckLogDir })
        {
            string prefix = FileNames.InfoLogPrefix(mode != DefaultInfoLogDir, "/rocksdb/dir");
            foreach ((string name, ulong number, FileType type, int caseMode) in s_cases)
            {
                if ((caseMode & mode) == 0) continue;
                ulong n;
                FileType t;
                bool ok = mode == NoCheckLogDir
                    ? FileNames.TryParseFileName(name, out n, out t)
                    : FileNames.TryParseFileName(name, prefix, out n, out t, out _);
                Assert.IsTrue(ok, name);
                Assert.AreEqual(type, t, name);
                Assert.AreEqual(number, n, name);
            }
        }
        foreach (string e in s_errors)
        {
            Assert.IsFalse(FileNames.TryParseFileName(e, out _, out _), e);
        }
    }

    [TestMethod]
    public void ArchivedWal()
    {
        Assert.IsTrue(FileNames.TryParseFileName("archive/000123.log", [], out ulong n, out FileType t, out WalFileType w));
        Assert.AreEqual(123UL, n);
        Assert.AreEqual(FileType.WalFile, t);
        Assert.AreEqual(WalFileType.ArchivedLogFile, w);
        Assert.IsTrue(FileNames.TryParseFileName("000123.log", [], out _, out _, out w));
        Assert.AreEqual(WalFileType.AliveLogFile, w);
        Assert.AreEqual("db/archive/000123.log", FileNames.ArchivedLogFileName("db", 123));
    }

    [TestMethod]
    public void InfoLogFileName()
    {
        Assert.AreEqual("/data/rocksdb/LOG", FileNames.InfoLogFileName("/data/rocksdb", "/data/rocksdb", ""));
        Assert.AreEqual("/data/rocksdb/LOG.old.666", FileNames.OldInfoLogFileName("/data/rocksdb", 666, "/data/rocksdb", ""));
        Assert.AreEqual("/data/rocksdb_log/data_rocksdb_LOG", FileNames.InfoLogFileName("/data/rocksdb", "/data/rocksdb", "/data/rocksdb_log"));
        Assert.AreEqual("/data/rocksdb_log/data_rocksdb_LOG.old.666",
            FileNames.OldInfoLogFileName("/data/rocksdb", 666, "/data/rocksdb", "/data/rocksdb_log"));
    }

    [TestMethod]
    public void Construction()
    {
        static void Check(string fname, string dir, ulong number, FileType type)
        {
            Assert.IsTrue(fname.StartsWith(dir + "/", StringComparison.Ordinal), fname);
            Assert.IsTrue(FileNames.TryParseFileName(fname.AsSpan(dir.Length + 1), out ulong n, out FileType t), fname);
            Assert.AreEqual(number, n);
            Assert.AreEqual(type, t);
        }
        Check(FileNames.CurrentFilePath("foo"), "foo", 0, FileType.CurrentFile);
        Check(FileNames.LockFileName("foo"), "foo", 0, FileType.DbLockFile);
        Check(FileNames.LogFileName("foo", 192), "foo", 192, FileType.WalFile);
        Check(FileNames.TableFileName(["bar"], 200, 0), "bar", 200, FileType.TableFile);
        Assert.AreEqual(FileNames.TableFileName(["bar"], 200, 0), FileNames.TableFileName(["foo", "bar"], 200, 1));
        Check(FileNames.DescriptorFileName("bar", 100), "bar", 100, FileType.DescriptorFile);
        Check(FileNames.TempFileName("tmp", 999), "tmp", 999, FileType.TempFile);
        Check(FileNames.MetaDatabaseName("met", 100), "met", 100, FileType.MetaDatabase);
        Check(FileNames.OptionsFileName("db", 7), "db", 7, FileType.OptionsFile);
        Check(FileNames.TempOptionsFileName("db", 7), "db", 7, FileType.TempFile);
        Check(FileNames.IdentityFileName("db"), "db", 0, FileType.IdentityFile);
        Check(FileNames.BlobFileName("db", 3), "db", 3, FileType.BlobFile);

        Assert.AreEqual("MANIFEST-000005", FileNames.DescriptorFileName(5));
        Assert.AreEqual("000010.log", FileNames.LogFileName(10));
        Assert.AreEqual("1234567.sst", FileNames.MakeTableFileName(1234567));
        Assert.AreEqual("OPTIONS-000007", FileNames.OptionsFileName(7));
        Assert.AreEqual(1234567UL, FileNames.TableFileNameToNumber("/x/1234567.sst"));
        Assert.AreEqual("x/000001.ldb", FileNames.Rocks2LevelTableFileName("x/000001.sst"));
        Assert.AreEqual("12(path 3)", FileNames.FormatFileNumber(12, 3));
    }

    [TestMethod]
    public void NormalizePath()
    {
        string sep = Path.DirectorySeparatorChar.ToString();
        (string Given, string Expected)[] cases =
        [
            ("FOLDER" + sep + "filename.ext", "FOLDER" + sep + "filename.ext"),
            (sep + "a", sep + "a"),
            ("a" + sep, "a" + sep),
            (sep + sep + "a", sep + sep + "a"),
            ("a" + sep + sep, "a" + sep),
            (sep, sep),
            (sep + sep, sep),
            (sep + sep + sep, sep + sep),
            (sep + sep + sep + "a" + sep, sep + sep + "a" + sep),
            ("a" + sep + sep + "b", "a" + sep + "b"),
            (sep + sep + "SERVER" + sep + "a" + sep + sep + "b" + sep + "c", sep + sep + "SERVER" + sep + "a" + sep + "b" + sep + "c"),
        ];
        foreach ((string given, string expected) in cases) Assert.AreEqual(expected, FileNames.NormalizePath(given), given);
    }

    [TestMethod]
    public async Task CurrentAndIdentityFiles()
    {
        string dir = Path.Combine(Path.GetTempPath(), "rdbm-fn-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            IFileSystem fs = LocalFileSystem.Instance;
            await FileNames.SetCurrentFileAsync(fs, dir, 42, syncDirectory: true, CancellationToken.None);
            Assert.AreEqual("MANIFEST-000042\n", File.ReadAllText(Path.Combine(dir, "CURRENT")));
            Assert.IsFalse(File.Exists(Path.Combine(dir, "000042.dbtmp")));
            Assert.AreEqual(dir + "/MANIFEST-000042", FileNames.GetCurrentManifestPath(fs, dir, out ulong number));
            Assert.AreEqual(42UL, number);

            string id = await FileNames.SetIdentityFileAsync(fs, dir, null, CancellationToken.None);
            Assert.AreEqual(36, id.Length);
            Assert.AreEqual(id, FileNames.ReadIdentityFile(fs, dir));
            Assert.AreEqual("my-id", await FileNames.SetIdentityFileAsync(fs, dir, "my-id", CancellationToken.None));

            File.WriteAllText(Path.Combine(dir, "CURRENT"), "MANIFEST-000042");
            Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => FileNames.GetCurrentManifestPath(fs, dir, out _)).Code);
            File.WriteAllText(Path.Combine(dir, "CURRENT"), "000042.log\n");
            Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => FileNames.GetCurrentManifestPath(fs, dir, out _)).Code);
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }
}
