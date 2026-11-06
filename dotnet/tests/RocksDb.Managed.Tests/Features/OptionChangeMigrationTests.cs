using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Features;

/// <summary>
/// Port of utilities/option_change_migration/option_change_migration_test.cc: Migrate1-4 over the same 19 parameter
/// tuples (old num_levels / style / dynamic, new num_levels / style / dynamic, FIFO max_table_files_size),
/// CompactedSrcToUniversal and the multi-column-family tests.
/// </summary>
[TestClass]
public sealed class OptionChangeMigrationTests
{
    // (level1, style1, dynamic1, level2, style2, dynamic2, fifoMaxTableFilesSize); style 0 = level, 1 = universal, 2 = FIFO.
    public static IEnumerable<object[]> Params =>
    [
        [3, 0, false, 4, 0, false, 0UL],
        [3, 0, true, 4, 0, true, 0UL],
        [3, 0, true, 4, 0, false, 0UL],
        [3, 0, false, 4, 0, true, 0UL],
        [3, 1, false, 4, 1, false, 0UL],
        [1, 1, false, 4, 1, false, 0UL],
        [3, 0, false, 4, 1, false, 0UL],
        [3, 0, false, 1, 1, false, 0UL],
        [3, 0, true, 4, 1, false, 0UL],
        [3, 0, true, 1, 1, false, 0UL],
        [1, 1, false, 4, 0, false, 0UL],
        [4, 0, false, 1, 2, false, 0UL],
        [3, 0, true, 2, 2, false, 0UL],
        [3, 1, false, 3, 2, false, 0UL],
        [1, 1, false, 4, 2, false, 0UL],
        [4, 0, false, 1, 2, false, ulong.MaxValue],
        [3, 0, true, 2, 2, false, ulong.MaxValue],
        [3, 1, false, 3, 2, false, ulong.MaxValue],
        [1, 1, false, 4, 2, false, ulong.MaxValue],
    ];

    private static readonly CompactionStyle[] s_styles = [CompactionStyle.Level, CompactionStyle.Universal, CompactionStyle.Fifo];

    /// <summary>The "old_options" of the parameterized tests (compaction style, dynamic leveling and num_levels from the parameters).</summary>
    private static ColumnFamilyOptions OldOptions(int numLevels, int style, bool dynamic)
    {
        var o = new ColumnFamilyOptions { CompactionStyle = s_styles[style] };
        if (o.CompactionStyle == CompactionStyle.Level) o.LevelCompactionDynamicLevelBytes = dynamic;
        o.Level0FileNumCompactionTrigger = 3;
        o.WriteBufferSize = 64 * 1024;
        o.TargetFileSizeBase = 128 * 1024;
        // Make level target of L1, L2 to be 200KB and 600KB
        o.NumLevels = numLevels;
        o.MaxBytesForLevelMultiplier = 3;
        o.MaxBytesForLevelBase = 200 * 1024;
        return o;
    }

    /// <summary>new_options = old_options with the second style, num_levels and level sizes.</summary>
    private static ColumnFamilyOptions MigratedOptions(ColumnFamilyOptions old, int numLevels, int style, bool dynamic, ulong fifoMaxTableFilesSize)
    {
        ColumnFamilyOptions o = old.Clone();
        o.CompactionStyle = s_styles[style];
        if (o.CompactionStyle == CompactionStyle.Level) o.LevelCompactionDynamicLevelBytes = dynamic;
        // This is set to be extremely large if not zero to avoid dropping any data right after migration.
        if (fifoMaxTableFilesSize != 0) o.CompactionOptionsFifo.MaxTableFilesSize = fifoMaxTableFilesSize;
        o.TargetFileSizeBase = 256 * 1024;
        o.NumLevels = numLevels;
        o.MaxBytesForLevelBase = 150 * 1024;
        o.MaxBytesForLevelMultiplier = 4;
        return o;
    }

    private static byte[] RandomValue(Random rnd, int len)
    {
        byte[] b = new byte[len];
        for (int i = 0; i < len; i++) b[i] = (byte)(' ' + rnd.Next(95));
        return b;
    }

    /// <summary>DBTestBase::GenerateNewFile: 100 increasing keys (990-byte values, the last 1 byte), then wait for flush + compaction.</summary>
    private static void GenerateNewFile(RocksDatabase db, Random rnd, ref int keyIdx)
    {
        for (int i = 0; i < 100; i++) db.Put(Key(keyIdx++), RandomValue(rnd, i == 99 ? 1 : 990));
        db.Flush();
        db.WaitForCompact();
    }

    private static List<string> AllKeys(ColumnFamily cf)
    {
        var keys = new List<string>();
        using Iterator it = cf.NewIterator();
        for (it.SeekToFirst(); it.Valid; it.Next()) keys.Add(System.Text.Encoding.UTF8.GetString(it.Key));
        return keys;
    }

    /// <summary>Migrates, reopens with the new options (twice, waiting for compactions) and checks the keys are exactly the same.</summary>
    private static void MigrateAndVerify(IFileSystem fs, ColumnFamilyOptions oldOptions, ColumnFamilyOptions newOptions, List<string> keys)
    {
        OptionChangeMigration.Migrate(DbPath, NewOptions(fs), oldOptions, NewOptions(fs), newOptions);
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), newOptions))
        {
            // Wait for compaction to finish and make sure it can reopen
            db.WaitForCompact();
        }
        using RocksDatabase db2 = RocksDatabase.Open(DbPath, NewOptions(fs), newOptions);
        CollectionAssert.AreEqual(keys, AllKeys(db2.DefaultColumnFamily));
    }

    [TestMethod]
    [DynamicData(nameof(Params))]
    public void Migrate1(int level1, int style1, bool dynamic1, int level2, int style2, bool dynamic2, ulong fifoSize) =>
        RunGenerateNewFile(OldOptions(level1, style1, dynamic1), level2, style2, dynamic2, fifoSize);

    /// <summary>Migrate2: the reverse direction of Migrate1.</summary>
    [TestMethod]
    [DynamicData(nameof(Params))]
    public void Migrate2(int level1, int style1, bool dynamic1, int level2, int style2, bool dynamic2, ulong fifoSize) =>
        RunGenerateNewFile(OldOptions(level2, style2, dynamic2), level1, style1, dynamic1, fifoSize);

    [TestMethod]
    [DynamicData(nameof(Params))]
    public void Migrate3(int level1, int style1, bool dynamic1, int level2, int style2, bool dynamic2, ulong fifoSize) =>
        RunWithZeroedOutFiles(OldOptions(level1, style1, dynamic1), level2, style2, dynamic2, fifoSize);

    /// <summary>Migrate4: the reverse direction of Migrate3.</summary>
    [TestMethod]
    [DynamicData(nameof(Params))]
    public void Migrate4(int level1, int style1, bool dynamic1, int level2, int style2, bool dynamic2, ulong fifoSize) =>
        RunWithZeroedOutFiles(OldOptions(level2, style2, dynamic2), level1, style1, dynamic1, fifoSize);

    private static void RunGenerateNewFile(ColumnFamilyOptions oldOptions, int level2, int style2, bool dynamic2, ulong fifoSize)
    {
        MemoryFileSystem fs = NewFs();
        List<string> keys;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), oldOptions))
        {
            var rnd = new Random(301);
            int keyIdx = 0;
            // Generate at least 2MB of data
            for (int num = 0; num < 20; num++) GenerateNewFile(db, rnd, ref keyIdx);
            db.WaitForCompact();
            // Will make sure exactly those keys are in the DB after migration.
            keys = AllKeys(db.DefaultColumnFamily);
        }
        MigrateAndVerify(fs, oldOptions, MigratedOptions(oldOptions, level2, style2, dynamic2, fifoSize), keys);
    }

    private static void RunWithZeroedOutFiles(ColumnFamilyOptions oldOptions, int level2, int style2, bool dynamic2, ulong fifoSize)
    {
        MemoryFileSystem fs = NewFs();
        List<string> keys;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), oldOptions))
        {
            var rnd = new Random(301);
            for (int num = 0; num < 20; num++)
            {
                for (int i = 0; i < 50; i++) db.Put(Key(num * 100 + i), RandomValue(rnd, 900));
                db.Flush();
                db.WaitForCompact();
                if (num == 9)
                {
                    // Issue a full compaction to generate some zero-out files
                    db.CompactRange(new CompactRangeOptions { BottommostLevelCompaction = BottommostLevelCompaction.Force }, null, null);
                }
            }
            db.WaitForCompact();
            keys = AllKeys(db.DefaultColumnFamily);
        }
        MigrateAndVerify(fs, oldOptions, MigratedOptions(oldOptions, level2, style2, dynamic2, fifoSize), keys);
    }

    [TestMethod]
    public void CompactedSrcToUniversal()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions oldOptions = OldOptions(4, 0, false);
        oldOptions.MaxCompactionBytes = 200 * 1024;
        List<string> keys;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), oldOptions))
        {
            var rnd = new Random(301);
            for (int num = 0; num < 20; num++)
            {
                for (int i = 0; i < 50; i++) db.Put(Key(num * 100 + i), RandomValue(rnd, 900));
            }
            db.Flush();
            db.CompactRange(new CompactRangeOptions { BottommostLevelCompaction = BottommostLevelCompaction.Force }, null, null);
            keys = AllKeys(db.DefaultColumnFamily);
        }
        ColumnFamilyOptions newOptions = oldOptions.Clone();
        newOptions.CompactionStyle = CompactionStyle.Universal;
        newOptions.TargetFileSizeBase = 256 * 1024;
        newOptions.NumLevels = 1;
        newOptions.MaxBytesForLevelBase = 150 * 1024;
        newOptions.MaxBytesForLevelMultiplier = 4;
        MigrateAndVerify(fs, oldOptions, newOptions, keys);
        using RocksDatabase reopened = RocksDatabase.Open(DbPath, NewOptions(fs), newOptions);
        Assert.AreEqual("0", reopened.GetProperty(DbProperties.NumFilesAtLevel(1)) ?? "0");
    }

    // ---- multiple column families ---------------------------------------------------------------------------

    /// <summary>Writes <paramref name="count"/> keys to the default CF and "cf1", flushes both and returns their keys.</summary>
    private static (List<string> Default, List<string> Cf1) WriteTwoColumnFamilies(MemoryFileSystem fs, ColumnFamilyOptions options, int count)
    {
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), options);
        ColumnFamily cf1 = db.CreateColumnFamily("cf1", options.Clone());
        var rnd = new Random(301);
        for (int i = 0; i < count; i++)
        {
            db.Put(B("key" + i), RandomValue(rnd, 900));
            cf1.Put(B("key" + i), RandomValue(rnd, 900));
        }
        db.Flush();
        cf1.Flush();
        db.WaitForCompact();
        return (AllKeys(db.DefaultColumnFamily), AllKeys(cf1));
    }

    private static ColumnFamilyDescriptor[] Descriptors(ColumnFamilyOptions defaultCf, ColumnFamilyOptions cf1) =>
        [new(ColumnFamilyDescriptor.DefaultColumnFamilyName, defaultCf), new("cf1", cf1)];

    private static void VerifyTwoColumnFamilies(MemoryFileSystem fs, ColumnFamilyDescriptor[] newCfs, (List<string> Default, List<string> Cf1) keys)
    {
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), newCfs);
        CollectionAssert.AreEqual(keys.Default, AllKeys(db.DefaultColumnFamily));
        CollectionAssert.AreEqual(keys.Cf1, AllKeys(db.GetColumnFamily("cf1")));
    }

    [TestMethod]
    public async Task BasicMultiCF()
    {
        MemoryFileSystem fs = NewFs();
        var options = new ColumnFamilyOptions
        {
            CompactionStyle = CompactionStyle.Level, LevelCompactionDynamicLevelBytes = false, NumLevels = 4,
            WriteBufferSize = 64 * 1024, TargetFileSizeBase = 128 * 1024,
        };
        (List<string>, List<string>) keys = WriteTwoColumnFamilies(fs, options, 100);

        // New options: migrate to Universal compaction
        ColumnFamilyOptions newOptions = options.Clone();
        newOptions.CompactionStyle = CompactionStyle.Universal;
        newOptions.NumLevels = 5;
        newOptions.TargetFileSizeBase = 256 * 1024;
        ColumnFamilyDescriptor[] newCfs = Descriptors(newOptions, newOptions);
        await OptionChangeMigration.MigrateAsync(DbPath, NewOptions(fs), Descriptors(options, options), NewOptions(fs), newCfs);
        VerifyTwoColumnFamilies(fs, newCfs, keys);
    }

    [TestMethod]
    public void DifferentStylesPerCF()
    {
        MemoryFileSystem fs = NewFs();
        var options = new ColumnFamilyOptions { CompactionStyle = CompactionStyle.Level, NumLevels = 4, WriteBufferSize = 64 * 1024 };
        (List<string>, List<string>) keys = WriteTwoColumnFamilies(fs, options, 50);

        // default CF to Universal, cf1 to Level with dynamic
        ColumnFamilyOptions newDefault = options.Clone();
        newDefault.CompactionStyle = CompactionStyle.Universal;
        newDefault.NumLevels = 5;
        ColumnFamilyOptions newCf1 = options.Clone();
        newCf1.CompactionStyle = CompactionStyle.Level;
        newCf1.LevelCompactionDynamicLevelBytes = true;
        newCf1.NumLevels = 5;
        ColumnFamilyDescriptor[] newCfs = Descriptors(newDefault, newCf1);
        OptionChangeMigration.Migrate(DbPath, NewOptions(fs), Descriptors(options, options), NewOptions(fs), newCfs);
        VerifyTwoColumnFamilies(fs, newCfs, keys);
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), newCfs);
        // cf1 (dynamic level bytes, 5 levels): everything was compacted into the last level.
        Assert.AreEqual("1", db.GetColumnFamily("cf1").GetProperty(DbProperties.NumFilesAtLevel(4)));
    }

    [TestMethod]
    public void ValidationMismatched()
    {
        MemoryFileSystem fs = NewFs();
        var cfOpts = new ColumnFamilyOptions();
        ColumnFamilyDescriptor d = new(ColumnFamilyDescriptor.DefaultColumnFamilyName, cfOpts);
        ColumnFamilyDescriptor c1 = new("cf1", cfOpts);

        // Mismatched CF count (missing cf1)
        var e = Assert.ThrowsExactly<RocksDbException>(() => OptionChangeMigration.Migrate(DbPath, NewOptions(fs), [d, c1], NewOptions(fs), [d]));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        StringAssert.Contains(e.Message, "same number");
        // Mismatched CF names (cf2 instead of cf1)
        e = Assert.ThrowsExactly<RocksDbException>(() =>
            OptionChangeMigration.Migrate(DbPath, NewOptions(fs), [d, c1], NewOptions(fs), [d, new ColumnFamilyDescriptor("cf2", cfOpts)]));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        StringAssert.Contains(e.Message, "mismatch");
        // Mismatched CF order (swapped)
        e = Assert.ThrowsExactly<RocksDbException>(() => OptionChangeMigration.Migrate(DbPath, NewOptions(fs), [d, c1], NewOptions(fs), [c1, d]));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        StringAssert.Contains(e.Message, "mismatch");
        // Nothing was opened or created.
        Assert.IsFalse(fs.FileExists(DbPath + "/CURRENT"));
    }

    [TestMethod]
    public void FromFIFOMultiCF()
    {
        MemoryFileSystem fs = NewFs();
        var options = new ColumnFamilyOptions { CompactionStyle = CompactionStyle.Fifo, NumLevels = 1 };
        (List<string>, List<string>) keys = WriteTwoColumnFamilies(fs, options, 50);

        ColumnFamilyOptions newOptions = options.Clone();
        newOptions.CompactionStyle = CompactionStyle.Level;
        newOptions.NumLevels = 4;
        ColumnFamilyDescriptor[] newCfs = Descriptors(newOptions, newOptions);
        // Migration should succeed (FIFO is special case)
        OptionChangeMigration.Migrate(DbPath, NewOptions(fs), Descriptors(options, options), NewOptions(fs), newCfs);
        VerifyTwoColumnFamilies(fs, newCfs, keys);
    }
}
