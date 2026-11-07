using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>
/// <see cref="WalFilter"/> (DBOptions::wal_filter): ports of DBTest2.WalFilterTest, WalFilterTestWithChangeBatch,
/// WalFilterTestWithChangeBatchExtraKeys and WalFilterTestWithColumnFamilies (db/db_etc2_test.cc), plus the interaction
/// with wal_recovery_mode and read-only opens.
/// </summary>
[TestClass]
public sealed class WalFilterTests
{
    private static readonly string[][] s_batchKeys = [["key1", "key2"], ["key3", "key4"], ["key5", "key6"]];
    private static readonly string[] s_cfNames = ["default", "pikachu"];

    private static ColumnFamilyDescriptor[] Cfs() => [new("default", new ColumnFamilyOptions()), new("pikachu", new ColumnFamilyOptions())];

    /// <summary>DBTestBase options: kTolerateCorruptedTailRecords; the recovered memtables are flushed at open.</summary>
    private static DbOptions Options(MemoryFileSystem fs, WalFilter? filter) => new()
    {
        CreateIfMissing = true,
        CreateMissingColumnFamilies = true,
        FileSystem = fs,
        WalRecoveryMode = WalRecoveryMode.TolerateCorruptedTailRecords,
        AvoidFlushDuringShutdown = true,
        WalFilter = filter,
    };

    private static RocksDatabase Open(MemoryFileSystem fs, WalFilter? filter) => RocksDatabase.Open(DbPath, Options(fs, filter), Cfs());

    /// <summary>Creates the DB with the "pikachu" column family and writes the batches (to <paramref name="cfIds"/>), unflushed.</summary>
    private static MemoryFileSystem CreateDb(string[][] batches, params uint[] cfIds)
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = Open(fs, null);
        WriteBatches(db, batches, cfIds);
        return fs;
    }

    private static void WriteBatches(RocksDatabase db, string[][] batches, uint[] cfIds)
    {
        string dummy = new('x', 1024);
        foreach (string[] keys in batches)
        {
            using WriteBatch batch = db.CreateWriteBatch();
            foreach (string k in keys)
            {
                foreach (uint cf in cfIds) batch.Put(cf, B(k), B(dummy));
            }
            db.Write(batch);
        }
    }

    /// <summary>ValidateKeyExistence.</summary>
    private static void ValidateKeyExistence(RocksDatabase db, List<string> mustExist, List<string> mustNotExist, string ctx)
    {
        foreach (string k in mustExist) Assert.IsNotNull(db.Get(B(k)), ctx + ": " + k + " must exist");
        foreach (string k in mustNotExist) Assert.IsNull(db.Get(B(k)), ctx + ": " + k + " must not exist");
    }

    /// <summary>Applies an option at one record index (TestWalFilter, through the legacy LogRecord entry point).</summary>
    private sealed class TestWalFilter(WalProcessingOption option, int applyAtIndex) : WalFilter
    {
        private int _currentIndex;
        public override string Name => "TestWalFilter";

        public override WalProcessingOption LogRecord(WriteBatch batch, WriteBatch newBatch, out bool batchChanged)
        {
            batchChanged = false;
            return _currentIndex++ == applyAtIndex ? option : WalProcessingOption.ContinueProcessing;
        }
    }

    [TestMethod]
    public void WalFilterTest()
    {
        const int applyAt = 1;
        foreach (WalProcessingOption option in Enum.GetValues<WalProcessingOption>())
        {
            string ctx = "option=" + option;
            MemoryFileSystem fs = CreateDb(s_batchKeys, 0);
            RocksDatabase db;
            if (option == WalProcessingOption.CorruptedRecord)
            {
                RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => Open(fs, new TestWalFilter(option, applyAt)).Dispose());
                Assert.AreEqual(StatusCode.Corruption, e.Code, ctx);
                StringAssert.Contains(e.Message, "Corruption reported by Wal Filter : TestWalFilter", ctx);
                // In case of corruption we can turn off paranoid_checks to reopen the database.
                DbOptions o = Options(fs, new TestWalFilter(option, applyAt));
                o.ParanoidChecks = false;
                db = RocksDatabase.Open(DbPath, o, Cfs());
            }
            else
            {
                db = Open(fs, new TestWalFilter(option, applyAt));
            }
            var mustExist = new List<string>();
            var mustNotExist = new List<string>();
            for (int i = 0; i < s_batchKeys.Length; i++)
            {
                bool gone = option switch
                {
                    WalProcessingOption.IgnoreCurrentRecord => i == applyAt,
                    WalProcessingOption.StopReplay => i >= applyAt,
                    _ => false, // ContinueProcessing, CorruptedRecord (ignored without paranoid_checks)
                };
                (gone ? mustNotExist : mustExist).AddRange(s_batchKeys[i]);
            }
            using (db) ValidateKeyExistence(db, mustExist, mustNotExist, ctx);
            // Reopen without the filter: the previous logs are not used again (even if they were skipped).
            using RocksDatabase again = Open(fs, null);
            ValidateKeyExistence(again, mustExist, mustNotExist, ctx + " after reopen");
        }
    }

    /// <summary>Keeps the first <paramref name="keysToKeep"/> puts of each record from index <paramref name="fromIndex"/> on.</summary>
    private sealed class ChangeBatchFilter(int fromIndex, int keysToKeep) : WalFilter
    {
        private int _currentIndex;
        public override string Name => "TestWalFilterWithChangeBatch";

        public override WalProcessingOption LogRecord(WriteBatch batch, WriteBatch newBatch, out bool batchChanged)
        {
            batchChanged = false;
            if (_currentIndex++ >= fromIndex)
            {
                int added = 0;
                foreach (WriteBatchEntry r in batch)
                {
                    if (r.Type == EntryType.Value && added < keysToKeep)
                    {
                        newBatch.Put(r.ColumnFamilyId, r.Key, r.Value);
                        added++;
                    }
                }
                batchChanged = true;
            }
            return WalProcessingOption.ContinueProcessing;
        }
    }

    [TestMethod]
    public void WalFilterTestWithChangeBatch()
    {
        const int changeFrom = 1, keysToKeep = 1;
        MemoryFileSystem fs = CreateDb(s_batchKeys, 0);
        var mustExist = new List<string>();
        var mustNotExist = new List<string>();
        for (int i = 0; i < s_batchKeys.Length; i++)
        {
            for (int j = 0; j < s_batchKeys[i].Length; j++)
            {
                (i >= changeFrom && j >= keysToKeep ? mustNotExist : mustExist).Add(s_batchKeys[i][j]);
            }
        }
        using (RocksDatabase db = Open(fs, new ChangeBatchFilter(changeFrom, keysToKeep)))
        {
            ValidateKeyExistence(db, mustExist, mustNotExist, "filtered");
        }
        using RocksDatabase again = Open(fs, null);
        ValidateKeyExistence(again, mustExist, mustNotExist, "after reopen");
    }

    private sealed class AddExtraKeysFilter : WalFilter
    {
        public override string Name => "WalFilterTestWithChangeBatchExtraKeys";

        public override WalProcessingOption LogRecord(WriteBatch batch, WriteBatch newBatch, out bool batchChanged)
        {
            newBatch.SetContents(batch.Data);
            newBatch.Put(B("key_extra"), B("value_extra"));
            batchChanged = true;
            return WalProcessingOption.ContinueProcessing;
        }
    }

    [TestMethod]
    public void WalFilterTestWithChangeBatchExtraKeys()
    {
        foreach (WalRecoveryMode mode in Enum.GetValues<WalRecoveryMode>())
        {
            MemoryFileSystem fs = CreateDb(s_batchKeys, 0);
            DbOptions o = Options(fs, new AddExtraKeysFilter());
            o.WalRecoveryMode = mode;
            // NotSupported regardless of wal_recovery_mode (not a corruption).
            RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, o, Cfs()).Dispose());
            Assert.AreEqual(StatusCode.NotSupported, e.Code, "mode=" + mode);
            StringAssert.Contains(e.Message, "More than original # of records returned by Wal Filter : WalFilterTestWithChangeBatchExtraKeys");
            // Reopen without the filter: the previous attempt must not have altered the DB.
            using RocksDatabase db = Open(fs, null);
            var all = new List<string>();
            foreach (string[] keys in s_batchKeys) all.AddRange(keys);
            ValidateKeyExistence(db, all, [], "mode=" + mode);
            Assert.IsNull(db.Get(B("key_extra")));
        }
    }

    /// <summary>Records, per column family, the keys of the records that still apply to it (TestWalFilterWithColumnFamilies).</summary>
    private sealed class ColumnFamiliesFilter : WalFilter
    {
        public Dictionary<uint, ulong> CfLogNumbers = new();
        public Dictionary<string, uint> CfNameIds = new();
        public readonly Dictionary<uint, List<string>> CfWalKeys = new();
        public override string Name => "WalFilterTestWithColumnFamilies";

        public override void ColumnFamilyLogNumberMap(IReadOnlyDictionary<uint, ulong> columnFamilyLogNumbers, IReadOnlyDictionary<string, uint> columnFamilyNameIds)
        {
            CfLogNumbers = new Dictionary<uint, ulong>(columnFamilyLogNumbers);
            CfNameIds = new Dictionary<string, uint>(columnFamilyNameIds);
        }

        public override WalProcessingOption LogRecordFound(ulong logNumber, string logFileName, WriteBatch batch, WriteBatch newBatch, out bool batchChanged)
        {
            batchChanged = false;
            StringAssert.EndsWith(logFileName, FileNames.LogFileName("", logNumber));
            foreach (WriteBatchEntry r in batch)
            {
                if (r.Type != EntryType.Value) continue;
                // If the current record is applicable for the column family (not yet flushed to table files for it),
                // add it to the keys found for verification.
                if (logNumber >= CfLogNumbers[r.ColumnFamilyId])
                {
                    if (!CfWalKeys.TryGetValue(r.ColumnFamilyId, out List<string>? keys)) CfWalKeys[r.ColumnFamilyId] = keys = [];
                    keys.Add(S(r.Key.ToArray())!);
                }
            }
            return WalProcessingOption.ContinueProcessing;
        }
    }

    [TestMethod]
    public void WalFilterTestWithColumnFamilies()
    {
        string[][] preFlush = s_batchKeys;
        string[][] postFlush = [["key7", "key8"], ["key9", "key10"], ["key11", "key12"]];
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = Open(fs, null))
        {
            uint pikachu = db.GetColumnFamily("pikachu").Id;
            WriteBatches(db, preFlush, [0, pikachu]);
            db.Flush(); // the default column family only
            WriteBatches(db, postFlush, [0, pikachu]);
        }
        var filter = new ColumnFamiliesFilter();
        using (RocksDatabase db = Open(fs, filter))
        {
            Assert.IsNotNull(db.Get(B("key12")));
        }
        var expectedDefault = new List<string>();
        foreach (string[] keys in postFlush) expectedDefault.AddRange(keys);
        var expectedPikachu = new List<string>();
        foreach (string[] keys in preFlush) expectedPikachu.AddRange(keys);
        expectedPikachu.AddRange(expectedDefault);
        CollectionAssert.AreEqual(expectedDefault, filter.CfWalKeys[filter.CfNameIds["default"]], "default: only post-flush keys");
        CollectionAssert.AreEqual(expectedPikachu, filter.CfWalKeys[filter.CfNameIds["pikachu"]], "pikachu: pre and post flush keys");
        CollectionAssert.AreEquivalent(s_cfNames, new List<string>(filter.CfNameIds.Keys));
    }

    private sealed class CountingFilter(WalProcessingOption option) : WalFilter
    {
        public int Records;
        public override string Name => "CountingFilter";

        public override WalProcessingOption LogRecordFound(ulong logNumber, string logFileName, WriteBatch batch, WriteBatch newBatch, out bool batchChanged)
        {
            batchChanged = false;
            Records++;
            return Records == 2 ? option : WalProcessingOption.ContinueProcessing;
        }
    }

    /// <summary>
    /// CorruptedRecord follows wal_recovery_mode like a corruption found by the reader: point-in-time recovery stops the
    /// replay at the record (open succeeds), kSkipAnyCorruptedRecords skips the rest of that WAL, the other modes fail.
    /// </summary>
    [TestMethod]
    public void CorruptedRecordFollowsRecoveryMode()
    {
        foreach (WalRecoveryMode mode in Enum.GetValues<WalRecoveryMode>())
        {
            string ctx = "mode=" + mode;
            MemoryFileSystem fs = CreateDb(s_batchKeys, 0);
            DbOptions o = Options(fs, new CountingFilter(WalProcessingOption.CorruptedRecord));
            o.WalRecoveryMode = mode;
            if (mode is WalRecoveryMode.TolerateCorruptedTailRecords or WalRecoveryMode.AbsoluteConsistency)
            {
                RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, o, Cfs()).Dispose());
                Assert.AreEqual(StatusCode.Corruption, e.Code, ctx);
                continue;
            }
            using RocksDatabase db = RocksDatabase.Open(DbPath, o, Cfs());
            ValidateKeyExistence(db, ["key1", "key2"], ["key3", "key4", "key5", "key6"], ctx);
        }
    }

    /// <summary>The filter also runs for read-only opens (DBImpl::Recover with read_only), not for secondaries.</summary>
    [TestMethod]
    public void ReadOnlyOpenRunsTheFilter()
    {
        MemoryFileSystem fs = CreateDb(s_batchKeys, 0);
        var filter = new CountingFilter(WalProcessingOption.StopReplay);
        DbOptions o = Options(fs, filter);
        using (RocksDatabase ro = RocksDatabase.OpenReadOnly(DbPath, o, Cfs(), errorIfWalFileExists: false))
        {
            Assert.AreEqual(2, filter.Records);
            ValidateKeyExistence(ro, ["key1", "key2"], ["key3", "key4", "key5", "key6"], "read-only");
        }
        // Read-only replay changes nothing: the primary still recovers everything.
        using RocksDatabase db = Open(fs, null);
        ValidateKeyExistence(db, ["key1", "key3", "key6"], [], "primary");
    }
}
