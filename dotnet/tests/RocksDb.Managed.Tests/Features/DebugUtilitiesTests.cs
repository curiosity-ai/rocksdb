using System.Buffers.Binary;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Util;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Features;

/// <summary>
/// GetAllKeyVersions (utilities/debug.cc). Ports of DBBasicTest.GetAllKeyVersions / ValueTypeString
/// (db/db_basic_test.cc) and DBMergeOperatorTest.MaxSuccessiveMergesBaseValues (db/db_merge_operator_test.cc), plus
/// bounds, limits, flushed data, range tombstones and user-defined timestamps.
/// </summary>
[TestClass]
public sealed class DebugUtilitiesTests
{
    [TestMethod]
    public void GetAllKeyVersions_DefaultAndNonDefaultColumnFamily()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { DisableAutoCompactions = true });
        ColumnFamily cf = db.CreateColumnFamily("pikachu", new ColumnFamilyOptions { DisableAutoCompactions = true });
        const int kNumInserts = 4, kNumDeletes = 4, kNumUpdates = 4;

        for (int i = 0; i != kNumInserts; ++i) db.Put(B(i.ToString()), B("value"));
        for (int i = 0; i != kNumUpdates; ++i) db.Put(B(i.ToString()), B("value1"));
        for (int i = 0; i != kNumDeletes; ++i) db.Delete(B(i.ToString()));

        using (KeyVersionCollection kv = db.GetAllKeyVersions(null, null, int.MaxValue))
        {
            Assert.AreEqual(kNumInserts + kNumDeletes + kNumUpdates, kv.Count);
            for (int i = 0; i < kv.Count; i++)
            {
                Assert.AreEqual(i % 3 == 0 ? "TypeDeletion" : "TypeValue", kv[i].TypeName);
            }
            // Newest first within a key: delete (seq 9..12), value1, value.
            Assert.AreEqual("0", S(kv[0].UserKey.ToArray()));
            Assert.AreEqual(9UL, kv[0].Sequence);
            Assert.AreEqual("value1", S(kv[1].Value.ToArray()));
            Assert.AreEqual("value", S(kv[2].Value.ToArray()));
        }
        using (KeyVersionCollection kv = db.DefaultColumnFamily.GetAllKeyVersions(null, null, int.MaxValue))
        {
            Assert.AreEqual(kNumInserts + kNumDeletes + kNumUpdates, kv.Count);
        }

        for (int i = 0; i + 1 != kNumInserts; ++i) cf.Put(B(i.ToString()), B("value"));
        for (int i = 0; i + 1 != kNumUpdates; ++i) cf.Put(B(i.ToString()), B("value1"));
        for (int i = 0; i + 1 != kNumDeletes; ++i) cf.Delete(B(i.ToString()));
        using var reused = new KeyVersionCollection();
        cf.GetAllKeyVersions(null, null, int.MaxValue, reused);
        Assert.AreEqual(kNumInserts + kNumDeletes + kNumUpdates - 3, reused.Count);

        // An empty key is a legal key (not "no bound"): ["", ""] holds nothing.
        cf.GetAllKeyVersions([], [], int.MaxValue, reused);
        Assert.AreEqual(0, reused.Count);
    }

    [TestMethod]
    public void ValueTypeString_EveryValidTypeHasAName()
    {
        // When adding a new type, also update the name table (value_type_string_map).
        for (byte i = (byte)EntryType.Deletion; i < (byte)EntryType.MaxValid; i++)
        {
            Assert.AreNotEqual("Invalid", KeyVersion.GetTypeName((EntryType)i), "type " + i);
        }
        Assert.AreEqual("Invalid", KeyVersion.GetTypeName(EntryType.MaxValid));
        Assert.AreEqual("TypeColumnFamilyValuePreferredSeqno", KeyVersion.GetTypeName(EntryType.ColumnFamilyValuePreferredSeqno));
        Assert.AreEqual("TypeWideColumnEntity", KeyVersion.GetTypeName(EntryType.WideColumnEntity));
    }

    /// <summary>
    /// Adapted from DBMergeOperatorTest.MaxSuccessiveMergesBaseValues: the port does not implement
    /// max_successive_merges (merges are never converted to puts at write time), so every operand is reported as a
    /// merge above its base value.
    /// </summary>
    [TestMethod]
    public void GetAllKeyVersions_MergeOperandsAndBaseValues()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { MergeOperator = MergeOperator.Put });
        using var kv = new KeyVersionCollection();

        // No base value
        db.Merge(B("key1"), B("foo"));
        db.Merge(B("key1"), B("bar"));
        Assert.AreEqual("bar", GetString(db, "key1"));
        db.GetAllKeyVersions(B("key1"), B("key1"), 8, kv);
        CollectionAssert.AreEqual(new[] { EntryType.Merge, EntryType.Merge }, Types(kv));
        Assert.AreEqual("bar", S(kv[0].Value.ToArray()));

        // Plain base value
        db.Put(B("key2"), B("foo"));
        db.Merge(B("key2"), B("bar"));
        db.Merge(B("key2"), B("baz"));
        Assert.AreEqual("baz", GetString(db, "key2"));
        db.GetAllKeyVersions(B("key2"), B("key2"), 8, kv);
        CollectionAssert.AreEqual(new[] { EntryType.Merge, EntryType.Merge, EntryType.Value }, Types(kv));

        // Wide-column base value
        db.PutEntity(B("key3"), [new WideColumn(ReadOnlyMemory<byte>.Empty, B("foo")), new WideColumn(B("bar"), B("baz"))]);
        db.Merge(B("key3"), B("qux"));
        db.Merge(B("key3"), B("corge"));
        db.GetAllKeyVersions(B("key3"), B("key3"), 8, kv);
        CollectionAssert.AreEqual(new[] { EntryType.Merge, EntryType.Merge, EntryType.WideColumnEntity }, Types(kv));
    }

    [TestMethod]
    public void GetAllKeyVersions_BoundsLimitFlushedFilesAndRangeTombstones()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { DisableAutoCompactions = true });
        for (int i = 0; i < 10; i++) db.Put(Key(i), B("v1"));
        db.Flush();
        for (int i = 0; i < 10; i += 2) db.Put(Key(i), B("v2"));
        db.SingleDelete(Key(9));

        using var kv = new KeyVersionCollection();
        db.GetAllKeyVersions(Key(2), Key(4), int.MaxValue, kv);
        // key2 (v2, v1), key3 (v1), key4 (v2, v1): memtable and SST versions merged, end inclusive.
        Assert.AreEqual(5, kv.Count);
        Assert.AreEqual("v2", S(kv[0].Value.ToArray()));
        Assert.AreEqual("v1", S(kv[1].Value.ToArray()));
        Assert.IsTrue(kv[0].Sequence > kv[1].Sequence);
        Assert.AreEqual(S(Key(4)), S(kv[4].UserKey.ToArray()));

        db.GetAllKeyVersions(Key(9), null, int.MaxValue, kv);
        CollectionAssert.AreEqual(new[] { EntryType.SingleDeletion, EntryType.Value }, Types(kv));

        // max_num_ikeys is checked after adding an entry (0 still yields one, like upstream).
        db.GetAllKeyVersions(null, null, 3, kv);
        Assert.AreEqual(3, kv.Count);
        db.GetAllKeyVersions(null, null, 0, kv);
        Assert.AreEqual(1, kv.Count);

        // Keys covered by a range tombstone are skipped (MergingIterator with range tombstones).
        db.DeleteRange(Key(3), Key(6));
        db.GetAllKeyVersions(null, null, int.MaxValue, kv);
        foreach (KeyVersion v in kv)
        {
            string k = S(v.UserKey.ToArray())!;
            Assert.IsFalse(string.CompareOrdinal(k, S(Key(3))) >= 0 && string.CompareOrdinal(k, S(Key(6))) < 0, k);
        }
        Assert.AreEqual(16 - 4, kv.Count); // 16 entries before, minus key3/key5 (1 each) and key4 (2)

        // A version written after the tombstone is visible again.
        db.Put(Key(4), B("v3"));
        db.Flush();
        db.GetAllKeyVersions(Key(4), Key(4), int.MaxValue, kv);
        Assert.AreEqual(1, kv.Count);
        Assert.AreEqual("v3", S(kv[0].Value.ToArray()));
    }

    [TestMethod]
    public void GetAllKeyVersions_UserDefinedTimestamps()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs),
            new ColumnFamilyOptions { Comparator = Comparator.BytewiseWithU64Ts, DisableAutoCompactions = true });
        db.Put(B("a"), Ts(10), B("a10"));
        db.Put(B("b"), Ts(10), B("b10"));
        db.Put(B("b"), Ts(20), B("b20"));
        db.Put(B("c"), Ts(5), B("c5"));

        using var kv = new KeyVersionCollection();
        db.GetAllKeyVersions(null, null, int.MaxValue, kv);
        Assert.AreEqual(4, kv.Count);
        // User keys carry their timestamp; newer timestamps first.
        Assert.AreEqual(9, kv[1].UserKey.Length);
        Assert.AreEqual(20UL, BinaryPrimitives.ReadUInt64LittleEndian(kv[1].UserKey.Span[1..]));
        Assert.AreEqual(10UL, BinaryPrimitives.ReadUInt64LittleEndian(kv[2].UserKey.Span[1..]));

        // begin gets the max timestamp (every version of "b"); so does end, which makes the end bound exclusive
        // for its own key (MaybeAddTimestampsToRange with exclusive_end).
        db.GetAllKeyVersions(B("b"), B("c"), int.MaxValue, kv);
        Assert.AreEqual(2, kv.Count);
        Assert.AreEqual("b20", S(kv[0].Value.ToArray()));
        Assert.AreEqual("b10", S(kv[1].Value.ToArray()));
    }

    private static byte[] Ts(ulong t)
    {
        byte[] b = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(b, t);
        return b;
    }

    private static EntryType[] Types(KeyVersionCollection kv)
    {
        var t = new EntryType[kv.Count];
        for (int i = 0; i < t.Length; i++) t[i] = kv[i].Type;
        return t;
    }
}

/// <summary>
/// InfoLogFinder (utilities/convenience/info_log_finder.cc, GetInfoLogFiles in file/filename.cc) over the port's info
/// LOG naming: LOG / LOG.old / LOG.old.&lt;micros&gt; in the DB directory, &lt;escaped db path&gt;_LOG... in db_log_dir.
/// </summary>
[TestClass]
public sealed class InfoLogFinderTests
{
    [TestMethod]
    public void ListsLogAndRolledLogsInDbDirectory()
    {
        MemoryFileSystem fs = NewFs();
        // Every open rolls the previous LOG to LOG.old.<micros> (CreateLoggerFromOptions).
        for (int i = 0; i < 3; i++)
        {
            using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
            db.Put(B("k"), B("v" + i));
        }
        // Names that only look like info logs are ignored; "LOG.old" is one (ParseFileName).
        foreach (string decoy in new[] { "LOGX", "LOG.old.abc", "LOG.oldx", "xLOG" }) fs.CreateWritable(DbPath + "/" + decoy).Dispose();
        fs.CreateWritable(DbPath + "/LOG.old").Dispose();

        using RocksDatabase opened = RocksDatabase.Open(DbPath, NewOptions(fs));
        List<string> logs = InfoLogFinder.GetInfoLogList(opened);
        logs.Sort(StringComparer.Ordinal);
        Assert.AreEqual(5, logs.Count, string.Join(",", logs)); // LOG, LOG.old, 3 x LOG.old.<micros>
        Assert.AreEqual("LOG", logs[0]);
        Assert.AreEqual("LOG.old", logs[1]);
        for (int i = 2; i < logs.Count; i++) StringAssert.StartsWith(logs[i], "LOG.old.");
        Assert.AreEqual(DbPath, InfoLogFinder.GetInfoLogDirectory(opened));
        foreach (string l in logs) Assert.IsTrue(fs.FileExists(DbPath + "/" + l), l);
    }

    [TestMethod]
    public void ListsOnlyThisDatabasesLogsInDbLogDir()
    {
        MemoryFileSystem fs = NewFs();
        const string logDir = "/logs";
        static DbOptions WithLogDir(IFileSystem f)
        {
            DbOptions o = NewOptions(f);
            o.DbLogDir = logDir;
            return o;
        }
        RocksDatabase.Open(DbPath, WithLogDir(fs)).Dispose();
        fs.CreateDirectoryIfMissing("/other");
        RocksDatabase.Open("/other", WithLogDir(fs)).Dispose(); // another DB's logs in the same directory
        using RocksDatabase db = RocksDatabase.Open(DbPath, WithLogDir(fs));

        List<string> logs = InfoLogFinder.GetInfoLogList(db);
        logs.Sort(StringComparer.Ordinal);
        string prefix = FileNames.InfoLogPrefix(true, Path.GetFullPath(DbPath));
        Assert.AreEqual(2, logs.Count, string.Join(",", logs));
        Assert.AreEqual(prefix, logs[0]);
        StringAssert.StartsWith(logs[1], prefix + ".old.");
        Assert.AreEqual(logDir, InfoLogFinder.GetInfoLogDirectory(db));
        Assert.IsFalse(fs.FileExists(DbPath + "/LOG"));

        // The free function, with the other DB's name.
        List<string> other = InfoLogFinder.GetInfoLogFiles(fs, logDir, Path.GetFullPath("/other"), out string parent);
        Assert.AreEqual(logDir, parent);
        Assert.AreEqual(1, other.Count);
        Assert.AreEqual(FileNames.InfoLogPrefix(true, Path.GetFullPath("/other")), other[0]);
    }
}
