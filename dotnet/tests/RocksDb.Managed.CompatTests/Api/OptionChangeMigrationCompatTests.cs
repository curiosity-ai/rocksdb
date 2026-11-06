using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>
/// OptionChangeMigration (utilities/option_change_migration) against native RocksDB 11.12: a database written by
/// native (or managed) RocksDB with one compaction configuration is migrated by the managed port, then opened by
/// native RocksDB with the new configuration (which fails when files sit above the new num_levels) and read back.
/// The C API has no OptionChangeMigration, so the native side only writes and verifies.
/// </summary>
[TestClass]
public sealed class OptionChangeMigrationCompatTests
{
    private const int NumKeys = 3000;

    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);
    private static string K(int i) => "key" + i.ToString("D6", System.Globalization.CultureInfo.InvariantCulture);
    private static byte[] V(int i) => U("value-" + i + new string('v', 200));

    private static RocksDbSharp.DbOptions NativeOptions(RocksDbSharp.Compaction style, int numLevels, bool dynamic) =>
        new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetCompression(RocksDbSharp.Compression.No)
            .SetCompactionStyle(style).SetNumLevels(numLevels).SetLevelCompactionDynamicLevelBytes(dynamic)
            .SetWriteBufferSize(64 * 1024).SetTargetFileSizeBase(64 * 1024);

    private static ColumnFamilyOptions ManagedOptions(CompactionStyle style, int numLevels, bool dynamic) => new()
    {
        Compression = RocksDb.Managed.Compression.CompressionType.None, CompactionStyle = style, NumLevels = numLevels, LevelCompactionDynamicLevelBytes = dynamic,
        WriteBufferSize = 64 * 1024, TargetFileSizeBase = 64 * 1024,
    };

    private static void VerifyNative(RocksDbSharp.DbOptions options, string path)
    {
        using RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(options, path);
        using RocksDbSharp.Iterator it = db.NewIterator();
        int n = 0;
        for (it.SeekToFirst(); it.Valid(); it.Next(), n++)
        {
            Assert.AreEqual(K(n), Encoding.UTF8.GetString(it.Key()));
            CollectionAssert.AreEqual(V(n), it.Value());
        }
        Assert.AreEqual(NumKeys, n);
    }

    private static void VerifyManaged(ColumnFamilyOptions options, string path)
    {
        using RocksDatabase db = RocksDatabase.Open(path, new DbOptions(), options);
        int n = 0;
        using Iterator it = db.NewIterator();
        for (it.SeekToFirst(); it.Valid; it.Next(), n++) Assert.AreEqual(K(n), Encoding.UTF8.GetString(it.Key));
        Assert.AreEqual(NumKeys, n);
    }

    [TestMethod]
    public void NativeLeveledDb_MigratedToSingleLevelUniversal_OpensInNative()
    {
        using var dir = new TempDir("ocm-native");
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(NativeOptions(RocksDbSharp.Compaction.Level, 4, false), dir.Path))
        {
            for (int i = 0; i < NumKeys; i++) db.Put(U(K(i)), V(i));
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            db.CompactRange((byte[]?)null, null);
        }
        ColumnFamilyOptions oldOptions = ManagedOptions(CompactionStyle.Level, 4, false);
        ColumnFamilyOptions newOptions = ManagedOptions(CompactionStyle.Universal, 1, false);
        OptionChangeMigration.Migrate(dir.Path, new DbOptions(), oldOptions, new DbOptions(), newOptions);

        VerifyNative(NativeOptions(RocksDbSharp.Compaction.Universal, 1, false), dir.Path);
        VerifyManaged(newOptions, dir.Path);
    }

    [TestMethod]
    public void ManagedUniversalDb_MigratedToFifoAndToDynamicLevel_OpensInNative()
    {
        using var dir = new TempDir("ocm-managed");
        ColumnFamilyOptions universal = ManagedOptions(CompactionStyle.Universal, 4, false);
        using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions { CreateIfMissing = true }, universal))
        {
            for (int i = 0; i < NumKeys; i++)
            {
                db.Put(U(K(i)), V(i));
                if (i % 500 == 499) db.Flush();
            }
            db.WaitForCompact();
        }

        // universal (4 levels) -> FIFO: one L0 file.
        ColumnFamilyOptions fifo = ManagedOptions(CompactionStyle.Fifo, 1, false);
        fifo.CompactionOptionsFifo.MaxTableFilesSize = ulong.MaxValue;
        OptionChangeMigration.Migrate(dir.Path, new DbOptions(), universal, new DbOptions(), fifo);
        VerifyNative(NativeOptions(RocksDbSharp.Compaction.Fifo, 1, false), dir.Path);
        using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions(), fifo))
        {
            Assert.AreEqual("1", db.GetProperty(DbProperties.NumFilesAtLevel(0)));
        }

        // FIFO -> dynamic leveling with 5 levels: nothing to do (FIFO data is all in L0); native opens it as is.
        ColumnFamilyOptions leveled = ManagedOptions(CompactionStyle.Level, 5, true);
        OptionChangeMigration.Migrate(dir.Path, new DbOptions(), fifo, new DbOptions(), leveled);
        VerifyNative(NativeOptions(RocksDbSharp.Compaction.Level, 5, true), dir.Path);
        VerifyManaged(leveled, dir.Path);
    }
}
