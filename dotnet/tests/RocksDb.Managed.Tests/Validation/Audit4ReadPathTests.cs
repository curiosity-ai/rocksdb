using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4, read path (DBImpl::GetImpl / MultiGet / KeyMayExist / GetMergeOperands, Version::Get, GetContext::SaveValue,
/// MemTable::Get, TableCache::Get): regression tests for the divergences found and coverage of verified branches.
/// </summary>
[TestClass]
public sealed class Audit4ReadPathTests
{
    private const string DbPath = "/audit4-read";

    private static RocksDatabase Open(MemoryFileSystem fs, MergeOperator? mergeOperator = null) =>
        RocksDatabase.Open(DbPath, new DbOptions { CreateIfMissing = true, FileSystem = fs },
            [new ColumnFamilyDescriptor("default", new ColumnFamilyOptions { DisableAutoCompactions = true, MergeOperator = mergeOperator })]);

    private static string? MultiGetOne(RocksDatabase db, ReadOptions? ro, string key)
    {
        using var batch = new MultiGetBatch();
        batch.Add(B(key));
        db.MultiGet(ro, batch);
        if (batch.GetStatus(0) == MultiGetStatus.Error) throw batch.GetError(0)!;
        return batch.GetStatus(0) == MultiGetStatus.Found ? S(batch.GetValue(0).ToArray()) : null;
    }

    private static string? MultiGetOneAsync(RocksDatabase db, ReadOptions? ro, string key)
    {
        using var batch = new MultiGetBatch();
        batch.Add(B(key));
        db.MultiGetAsync(ro, batch).AsTask().GetAwaiter().GetResult();
        if (batch.GetStatus(0) == MultiGetStatus.Error) throw batch.GetError(0)!;
        return batch.GetStatus(0) == MultiGetStatus.Found ? S(batch.GetValue(0).ToArray()) : null;
    }

    /// <summary>Every point-read entry point of the default column family for <paramref name="key"/>.</summary>
    private static string?[] ReadAllWays(RocksDatabase db, ReadOptions? ro, string key) =>
    [
        S(db.Get(ro, B(key))),
        S(db.GetAsync(ro, B(key)).AsTask().GetAwaiter().GetResult()),
        MultiGetOne(db, ro, key),
        MultiGetOneAsync(db, ro, key),
    ];

    private static void AssertAll(string? expected, string?[] actual, string what)
    {
        string[] names = ["Get", "GetAsync", "MultiGet", "MultiGetAsync"];
        for (int i = 0; i < actual.Length; i++) Assert.AreEqual(expected, actual[i], what + " / " + names[i]);
    }

    [TestMethod]
    public void IgnoreRangeDeletionsMakesCoveredKeysVisibleToPointLookups()
    {
        // ReadOptions::ignore_range_deletions: MemTable::Get (db/memtable.cc, "if (read_options.ignore_range_deletions ||
        // ...) return nullptr" in NewRangeTombstoneIteratorInternal), MemTable::MultiGet ("no_range_del"),
        // TableCache::Get ("if (s.ok() && !options.ignore_range_deletions)") and TableCache::MultiGet skip the range
        // tombstones, so max_covering_tombstone_seq stays 0 and a covered key is returned. The port ignored the option
        // in Get / MultiGet (iterators honoured it).
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = Open(fs);
        var ignore = new ReadOptions { IgnoreRangeDeletions = true };

        // 1. key and tombstone in the mutable memtable
        db.Put(B("b"), B("vb"));
        db.DeleteRange(B("a"), B("z"));
        AssertAll(null, ReadAllWays(db, null, "b"), "memtable, default");
        AssertAll("vb", ReadAllWays(db, ignore, "b"), "memtable, ignore_range_deletions");

        // 2. key in an older L0 file, tombstone alone in a newer L0 file (nothing dropped by the flush)
        db.Flush();
        using (RocksDatabase db2 = RocksDatabase.Open("/audit4-read2", new DbOptions { CreateIfMissing = true, FileSystem = fs },
                   [new ColumnFamilyDescriptor("default", new ColumnFamilyOptions { DisableAutoCompactions = true })]))
        {
            db2.Put(B("b"), B("vb"));
            db2.Put(B("y"), B("vy"));
            db2.Flush();
            db2.DeleteRange(B("a"), B("c"));
            db2.Flush();
            AssertAll(null, ReadAllWays(db2, null, "b"), "L0, default");
            AssertAll("vy", ReadAllWays(db2, null, "y"), "L0, uncovered");
            AssertAll("vb", ReadAllWays(db2, ignore, "b"), "L0, ignore_range_deletions");
            // 3. key in L0, tombstone in the memtable above it
            db2.DeleteRange(B("x"), B("z"));
            AssertAll(null, ReadAllWays(db2, null, "y"), "memtable tombstone over L0, default");
            AssertAll("vy", ReadAllWays(db2, ignore, "y"), "memtable tombstone over L0, ignore_range_deletions");
        }
    }

    [TestMethod]
    public void ReadTimestampOnColumnFamilyWithoutTimestampsIsInvalidArgumentOnEveryPointRead()
    {
        // DBImpl::GetImpl and DBImpl::MultiGetCommon: "if (read_options.timestamp) FailIfTsMismatchCf(...)", which is
        // InvalidArgument("cannot call this method on column family ... that does not enable timestamp") when the
        // comparator has no timestamp. The synchronous Get did that; GetAsync and MultiGet returned the value.
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = Open(fs);
        db.Put(B("a"), B("1"));
        var ro = new ReadOptions { Timestamp = new byte[8] };

        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => db.Get(ro, B("a")));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code, "Get");
        e = Assert.ThrowsExactly<RocksDbException>(() => db.GetAsync(ro, B("a")).AsTask().GetAwaiter().GetResult());
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code, "GetAsync");
        e = Assert.ThrowsExactly<RocksDbException>(() => MultiGetOne(db, ro, "a"));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code, "MultiGet");
        e = Assert.ThrowsExactly<RocksDbException>(() => MultiGetOneAsync(db, ro, "a"));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code, "MultiGetAsync");

        // The DB keeps working (no super version reference leaked by the failed calls).
        AssertAll("1", ReadAllWays(db, null, "a"), "after the failures");
    }

    [TestMethod]
    public void KeyMayExistIsFalseForKeyCoveredByMemtableRangeTombstone()
    {
        // DBImpl::KeyMayExist is GetImpl with kBlockCacheTier. The memtable lookup leaves max_covering_tombstone_seq > 0
        // and Version::Get stops before the first file ("if (*max_covering_tombstone_seq > 0) { // The remaining files
        // we look at will only contain covered keys, so we stop here. break; }"): NotFound, so KeyMayExist is false.
        // The port went on to the table filters and answered "may exist".
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = Open(fs, new StringAppendOperator());
        db.Put(B("b"), B("vb"));
        db.Put(B("m"), B("vm"));
        db.Flush();
        Assert.IsNull(db.Get(B("zz")));
        Assert.IsTrue(db.KeyMayExist(B("b")), "in a table file");
        db.DeleteRange(B("a"), B("c"));
        Assert.IsNull(db.Get(B("b")));
        Assert.IsFalse(db.KeyMayExist(B("b")), "covered by a memtable range tombstone");
        Assert.IsTrue(db.KeyMayExist(B("m")), "not covered");
        // ignore_range_deletions is part of the read options GetImpl receives: the tombstone is not consulted.
        Assert.IsTrue(db.KeyMayExist(new ReadOptions { IgnoreRangeDeletions = true }, B("b"), null, out _), "ignore_range_deletions");
        // A merge operand above the tombstone: Version::Get ends in kMerge and merges without a base value -> OK.
        db.Merge(B("b"), B("op"));
        Assert.AreEqual("op", GetString(db, "b"));
        Assert.IsTrue(db.KeyMayExist(B("b")), "merge operand newer than the tombstone");
    }

    [TestMethod]
    public void MaxCoveringTombstoneSeqPropagatesFromNewerSourcesToOlderOnes()
    {
        // GetContext::SaveValue: an entry with a seqno below *max_covering_tombstone_seq_ is treated as
        // kTypeRangeDeletion whatever level the tombstone came from (memtable, immutable memtable, newer L0 file, upper
        // level); merge operands newer than the tombstone merge without a base value; a snapshot older than the
        // tombstone does not see it (FragmentedRangeTombstoneIterator upper bound).
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = Open(fs, new StringAppendOperator());
        db.Put(B("k1"), B("base1"));
        db.Put(B("k2"), B("base2"));
        db.Put(B("k3"), B("base3"));
        db.Flush();
        db.CompactRange();                       // base values in the bottommost level
        db.Merge(B("k1"), B("a"));
        db.Merge(B("k2"), B("a"));
        db.Flush();                              // L0: operands below the tombstone
        using Snapshot before = db.GetSnapshot();
        db.DeleteRange(B("k1"), B("k3"));        // covers k1, k2 (not k3)
        db.Flush();                              // L0: the tombstone alone
        using Snapshot atTombstone = db.GetSnapshot();
        db.Merge(B("k2"), B("b"));
        db.Flush();                              // L0: operand above the tombstone
        db.Merge(B("k2"), B("c"));               // memtable: operand above the tombstone
        db.Merge(B("k3"), B("x"));

        AssertAll(null, ReadAllWays(db, null, "k1"), "k1 latest");
        AssertAll("b,c", ReadAllWays(db, null, "k2"), "k2 latest");
        AssertAll("base3,x", ReadAllWays(db, null, "k3"), "k3 latest");
        var roBefore = new ReadOptions { Snapshot = before };
        AssertAll("base1,a", ReadAllWays(db, roBefore, "k1"), "k1 before the tombstone");
        AssertAll("base2,a", ReadAllWays(db, roBefore, "k2"), "k2 before the tombstone");
        var roAt = new ReadOptions { Snapshot = atTombstone };
        AssertAll(null, ReadAllWays(db, roAt, "k1"), "k1 at the tombstone");
        AssertAll(null, ReadAllWays(db, roAt, "k2"), "k2 at the tombstone");
        AssertAll("base3", ReadAllWays(db, roAt, "k3"), "k3 at the tombstone");

        // GetMergeOperands (GetContext::SaveValue with do_merge_ = false): the covered base value and operand are not
        // part of the list; only the operands newer than the tombstone are.
        using var operands = new MergeOperandsResult();
        Assert.IsTrue(db.TryGetMergeOperands(B("k2"), new GetMergeOperandsOptions { ExpectedMaxNumberOfOperands = 10 }, operands));
        Assert.AreEqual(2, operands.Count);
        Assert.AreEqual("b", S(operands[0].ToArray()));
        Assert.AreEqual("c", S(operands[1].ToArray()));
        Assert.IsFalse(db.TryGetMergeOperands(B("k1"), new GetMergeOperandsOptions { ExpectedMaxNumberOfOperands = 10 }, operands));
        // FinalizeGetMergeOperands: more operands than expected_max_number_of_operands -> Incomplete, only the count.
        Assert.IsTrue(db.TryGetMergeOperands(roBefore, B("k2"), new GetMergeOperandsOptions { ExpectedMaxNumberOfOperands = 1 }, operands));
        Assert.IsTrue(operands.ExceededExpectedMax);
        Assert.AreEqual(2, operands.NumberOfOperands);
        Assert.AreEqual(0, operands.Count);
    }

    [TestMethod]
    public void MultiGetWithDuplicateAndUnsortedKeysMatchesGetPerKey()
    {
        // DBImpl::MultiGetCommon / PrepareMultiGetKeys sort the key contexts; results are reported per input position,
        // duplicates each get their own result, and every key agrees with a Get at the same snapshot (memtable,
        // immutable-free L0 files and the bottommost level, deletions, range deletions and merges).
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = Open(fs, new StringAppendOperator());
        for (int i = 0; i < 40; i++) db.Put(Key(i), B("v" + i));
        db.Flush();
        db.CompactRange();
        for (int i = 0; i < 40; i += 3) db.Merge(Key(i), B("m" + i));
        for (int i = 1; i < 40; i += 7) db.Delete(Key(i));
        db.Flush();
        db.DeleteRange(Key(10), Key(15));
        db.Flush();
        using Snapshot snap = db.GetSnapshot();
        for (int i = 12; i < 40; i += 5) db.Merge(Key(i), B("n" + i));
        db.DeleteRange(Key(30), Key(33));

        int seed = Environment.TickCount;
        var rnd = new Random(seed);
        foreach (ReadOptions? ro in new ReadOptions?[] { null, new ReadOptions { Snapshot = snap } })
        {
            var keys = new List<byte[]>();
            for (int i = 0; i < 120; i++) keys.Add(Key(rnd.Next(-2, 44) < 0 ? 999 : rnd.Next(0, 44))); // duplicates, missing keys
            using var batch = new MultiGetBatch();
            foreach (byte[] k in keys) batch.Add(k);
            db.MultiGet(ro, batch);
            byte[]?[] viaArray = db.MultiGet(ro, keys);
            for (int i = 0; i < keys.Count; i++)
            {
                string? expected = S(db.Get(ro, keys[i]));
                string what = "seed=" + seed + " key=" + S(keys[i]) + " snapshot=" + (ro is not null);
                Assert.AreEqual(expected is null ? MultiGetStatus.NotFound : MultiGetStatus.Found, batch.GetStatus(i), what);
                Assert.AreEqual(expected, batch.GetStatus(i) == MultiGetStatus.Found ? S(batch.GetValue(i).ToArray()) : null, what);
                Assert.AreEqual(expected, S(viaArray[i]), what);
            }
        }
    }
}
