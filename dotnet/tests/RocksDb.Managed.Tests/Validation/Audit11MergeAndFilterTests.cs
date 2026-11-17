using System.Buffers;
using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Monitoring;
using RocksDb.Managed.Table;
using RocksDb.Managed.Table.Filters;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #11, upstream merge / filter features the port was missing: MergeOperator::ShouldMerge
/// (include/rocksdb/merge_operator.h, called by MemTable's HandleTypeMerge and GetContext::SaveValue),
/// max_successive_merges / strict_max_successive_merges (MemTableInserter::MergeCF, db/write_batch.cc),
/// CompactionFilter::Context::input_table_properties (Compaction::CreateCompactionFilter) and a custom filter policy
/// that reuses the built-in compatibility name (ParsedFullFilterBlock calls the policy's own GetFilterBitsReader).
/// </summary>
[TestClass]
public sealed class Audit11MergeAndFilterTests
{
    // ---- shared helpers ---------------------------------------------------------------------------------------------

    /// <summary>
    /// StringAppendTESTOperator(',') with upstream's LimitedStringAppendMergeOp::ShouldMerge (db_merge_operator_test.cc):
    /// true once <c>operands.size() &gt;= limit</c>. Records the operands of every ShouldMerge call (as passed: newest
    /// first). Throws from ShouldMerge for keys starting with "boom".
    /// </summary>
    private sealed class LimitedAppend(int limit) : MergeOperator
    {
        public readonly List<string[]> ShouldMergeCalls = [];
        public int FullMergeCalls;

        public override string Name => "DBMergeOperatorTest::LimitedStringAppendMergeOp";

        public override bool FullMerge(ReadOnlySpan<byte> key, bool hasExistingValue, ReadOnlySpan<byte> existingValue, in MergeOperands operands,
            IBufferWriter<byte> result)
        {
            Interlocked.Increment(ref FullMergeCalls);
            bool first = true;
            if (hasExistingValue)
            {
                result.Write(existingValue);
                first = false;
            }
            for (int i = 0; i < operands.Count; i++)
            {
                if (!first) result.Write(","u8);
                result.Write(operands[i]);
                first = false;
            }
            return true;
        }

        public override bool ShouldMerge(in MergeOperands operands)
        {
            var seen = new string[operands.Count];
            for (int i = 0; i < seen.Length; i++) seen[i] = Encoding.UTF8.GetString(operands[i]);
            lock (ShouldMergeCalls) ShouldMergeCalls.Add(seen);
            if (seen.Length > 0 && seen[0].StartsWith("boom", StringComparison.Ordinal)) throw new InvalidOperationException("ShouldMerge threw");
            return operands.Count > 0 && limit > 0 && operands.Count >= limit;
        }
    }

    /// <summary>
    /// "[" + (existing or "_") + ":" + operands joined by "," + "]": the result shows which operands were merged together
    /// and on which base, so a merge folded at write time is visible in the value. FullMerge fails for an operand
    /// "corrupted" (like upstream's TestPutOperator) and throws for "throw".
    /// </summary>
    private sealed class Bracket : MergeOperator
    {
        public int FullMergeCalls;

        public override string Name => "Audit11Bracket";

        public override bool FullMerge(ReadOnlySpan<byte> key, bool hasExistingValue, ReadOnlySpan<byte> existingValue, in MergeOperands operands,
            IBufferWriter<byte> result)
        {
            Interlocked.Increment(ref FullMergeCalls);
            var sb = new StringBuilder("[").Append(hasExistingValue ? Encoding.UTF8.GetString(existingValue) : "_").Append(':');
            for (int i = 0; i < operands.Count; i++)
            {
                string op = Encoding.UTF8.GetString(operands[i]);
                if (op == "corrupted") return false;
                if (op == "throw") throw new InvalidOperationException("FullMerge threw");
                if (i > 0) sb.Append(',');
                sb.Append(op);
            }
            result.Write(Encoding.UTF8.GetBytes(sb.Append(']').ToString()));
            return true;
        }
    }

    private static EntryType[] Types(KeyVersionCollection kv)
    {
        var t = new EntryType[kv.Count];
        for (int i = 0; i < t.Length; i++) t[i] = kv[i].Type;
        return t;
    }

    private static EntryType[] VersionTypes(RocksDatabase db, string key)
    {
        using var kv = new KeyVersionCollection();
        db.GetAllKeyVersions(B(key), B(key), 64, kv);
        return Types(kv);
    }

    private static string[] Strings(List<string[]> calls, int i) => calls[i];

    /// <summary>The live L0 files (their full names), oldest first by file number.</summary>
    private static List<string> LevelFiles(RocksDatabase db, int level)
    {
        var l = new List<(ulong, string)>();
        foreach (LiveFileMetadata m in db.GetLiveFilesMetadata())
        {
            if (m.Level == level) l.Add((m.FileNumber, m.Directory + m.Name));
        }
        l.Sort();
        return l.ConvertAll(static x => x.Item2);
    }

    /// <summary>Flushes and moves the new L0 file to <paramref name="level"/> (DBTestBase::MoveFilesToLevel for one file).</summary>
    private static void FlushToLevel(RocksDatabase db, int level)
    {
        db.Flush();
        List<string> l0 = LevelFiles(db, 0);
        db.CompactFiles([l0[^1]], level);
    }

    // =================================================================================================================
    // 1. MergeOperator::ShouldMerge
    // =================================================================================================================

    /// <summary>
    /// Port of DBMergeOperatorTest.LimitMergeOperands (db/db_merge_operator_test.cc): with ShouldMerge true at two
    /// operands only the latest two are merged, whether they are in the memtable (MemTable's HandleTypeMerge), one table
    /// file, several L0 files, the memtable plus a file, or files at different levels (GetContext::SaveValue).
    /// </summary>
    [TestMethod]
    public void ShouldMerge_LimitMergeOperands()
    {
        MemoryFileSystem fs = NewFs();
        var op = new LimitedAppend(2);
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { MergeOperator = op, DisableAutoCompactions = true });
        // All K1 values are in memtable.
        foreach (string v in new[] { "a", "b", "c", "d" }) db.Merge(B("k1"), B(v));
        Assert.AreEqual("c,d", GetString(db, "k1"));
        // ShouldMerge gets the operands newest first (GetOperandsDirectionBackward): first ["d"], then ["d", "c"].
        Assert.AreEqual(2, op.ShouldMergeCalls.Count);
        CollectionAssert.AreEqual(new[] { "d" }, Strings(op.ShouldMergeCalls, 0));
        CollectionAssert.AreEqual(new[] { "d", "c" }, Strings(op.ShouldMergeCalls, 1));

        // All K2 values are flushed to L0 into a single file.
        foreach (string v in new[] { "a", "b", "c", "d" }) db.Merge(B("k2"), B(v));
        db.Flush();
        Assert.AreEqual("c,d", GetString(db, "k2"));

        // All K3 values are flushed and are in different files.
        foreach (string v in new[] { "ab", "bc", "cd" })
        {
            db.Merge(B("k3"), B(v));
            db.Flush();
        }
        db.Merge(B("k3"), B("de"));
        Assert.AreEqual("cd,de", GetString(db, "k3"));
        // Tests that merge operands reach exact limit at memtable.
        db.Merge(B("k3"), B("fg"));
        Assert.AreEqual("de,fg", GetString(db, "k3"));

        // All K4 values are in different levels.
        db.Merge(B("k4"), B("ab"));
        FlushToLevel(db, 4);
        db.Merge(B("k4"), B("bc"));
        FlushToLevel(db, 3);
        db.Merge(B("k4"), B("cd"));
        FlushToLevel(db, 1);
        db.Merge(B("k4"), B("de"));
        Assert.AreEqual("cd,de", GetString(db, "k4"));
    }

    /// <summary>
    /// ShouldMerge ends the lookup before the base value: the operands are merged with no base value (MergeWithNoBaseValue,
    /// kFound), on every point-lookup path: Get, GetAsync, MultiGet, MultiGetAsync, GetEntity, KeyMayExist with the value
    /// (memtable), and with a row cache (TableCache::Get's replay log re-runs SaveValue). A key whose operands stay under
    /// the limit still merges with its base. Without the feature every read returned "x,a,b".
    /// </summary>
    [TestMethod]
    public async Task ShouldMerge_StopsBeforeBaseValueOnEveryLookupPath()
    {
        MemoryFileSystem fs = NewFs();
        var op = new LimitedAppend(2);
        DbOptions dbo = NewOptions(fs);
        dbo.RowCache = new Managed.Cache.RowCache(1 << 20);
        using RocksDatabase db = RocksDatabase.Open(DbPath, dbo, new ColumnFamilyOptions { MergeOperator = op, DisableAutoCompactions = true });
        // A flush merges the operands with a base value in the same memtable, so the base values go to an older file.
        db.Put(B("sst"), B("x"));
        db.Put(B("split"), B("x"));
        db.Flush();
        db.Merge(B("sst"), B("a"));
        db.Merge(B("sst"), B("b"));
        db.Merge(B("split"), B("a"));
        db.Flush();
        db.Merge(B("split"), B("b"));
        db.Put(B("mem"), B("x"));
        db.Merge(B("mem"), B("a"));
        db.Merge(B("mem"), B("b"));
        db.Put(B("under"), B("x"));
        db.Merge(B("under"), B("a"));

        for (int pass = 0; pass < 2; pass++) // the second pass reads the table file through the row cache
        {
            foreach (string k in new[] { "mem", "sst", "split" })
            {
                Assert.AreEqual("a,b", GetString(db, k), k);
                Assert.AreEqual("a,b", S(await db.GetAsync(B(k))), k);
                WideColumns? e = db.GetEntity(B(k));
                Assert.IsNotNull(e);
                Assert.AreEqual("a,b", S(e.DefaultColumnValue.ToArray()), k);
            }
            Assert.AreEqual("x,a", GetString(db, "under"));
            CollectionAssert.AreEqual(new[] { "a,b", "a,b", "a,b", "x,a" }, Array.ConvertAll(db.MultiGet([B("mem"), B("sst"), B("split"), B("under")]), S));
            using var batch = new MultiGetBatch();
            batch.Add(B("sst"));
            batch.Add(B("split"));
            await db.MultiGetAsync(batch);
            Assert.AreEqual("a,b", S(batch.GetValue(0).ToArray()));
            Assert.AreEqual("a,b", S(batch.GetValue(1).ToArray()));
        }
        // DBImpl::KeyMayExist runs GetImpl on the memtables: the merged value is returned.
        var w = new ArrayBufferWriter<byte>();
        Assert.IsTrue(db.KeyMayExist(null, B("mem"), w, out bool found));
        Assert.IsTrue(found);
        Assert.AreEqual("a,b", S(w.WrittenSpan.ToArray()));
    }

    /// <summary>
    /// ShouldMerge is a point-lookup hook only (merge_operator.h: "Doesn't help with iterators"): iterators, flush and
    /// compaction merge every operand with the base value, and GetMergeOperands (do_merge_ = false) returns every operand
    /// (DBMergeOperandTest.GetMergeOperandsBasic with LimitedStringAppendMergeOp).
    /// </summary>
    [TestMethod]
    public void ShouldMerge_NotUsedByIteratorsCompactionOrGetMergeOperands()
    {
        MemoryFileSystem fs = NewFs();
        var op = new LimitedAppend(2);
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { MergeOperator = op, DisableAutoCompactions = true });
        db.Merge(B("k1"), B("a"));
        db.Put(B("k1"), B("x"));
        db.Merge(B("k1"), B("b"));
        db.Merge(B("k1"), B("c"));
        db.Merge(B("k1"), B("d"));
        int calls = op.ShouldMergeCalls.Count;

        var opts = new GetMergeOperandsOptions { ExpectedMaxNumberOfOperands = 4 };
        using var r = new MergeOperandsResult();
        Assert.IsTrue(db.TryGetMergeOperands(B("k1"), opts, r));
        Assert.AreEqual(4, r.NumberOfOperands);
        var got = new List<string>();
        for (int i = 0; i < r.Count; i++) got.Add(S(r[i].ToArray())!);
        CollectionAssert.AreEqual(new[] { "x", "b", "c", "d" }, got);
        Assert.AreEqual(calls, op.ShouldMergeCalls.Count, "GetMergeOperands must not call ShouldMerge");

        using (Iterator it = db.NewIterator())
        {
            it.SeekToFirst();
            Assert.IsTrue(it.Valid);
            Assert.AreEqual("x,b,c,d", S(it.Value.ToArray()));
            it.SeekForPrev(B("k1"));
            Assert.AreEqual("x,b,c,d", S(it.Value.ToArray()));
        }
        Assert.AreEqual(calls, op.ShouldMergeCalls.Count, "iterators must not call ShouldMerge");

        db.Flush();
        db.CompactRange();
        Assert.AreEqual(calls, op.ShouldMergeCalls.Count, "flush and compaction must not call ShouldMerge");
        // The compaction output holds the fully merged value: a Get now sees it as a plain value.
        Assert.AreEqual("x,b,c,d", GetString(db, "k1"));
        CollectionAssert.AreEqual(new[] { EntryType.Value }, VersionTypes(db, "k1"));
    }

    /// <summary>
    /// A ShouldMerge that throws (upstream: undefined behaviour) fails only that read, like a throwing FullMerge on a
    /// foreground call (audit #10 rule): the exception reaches the caller of Get / MultiGet, the DB stays usable and
    /// other keys read normally.
    /// </summary>
    [TestMethod]
    public void ShouldMerge_ThrowingFailsOnlyTheRead()
    {
        MemoryFileSystem fs = NewFs();
        var op = new LimitedAppend(3);
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { MergeOperator = op, DisableAutoCompactions = true });
        db.Merge(B("k"), B("boom1"));
        db.Merge(B("ok"), B("a"));
        Assert.ThrowsExactly<InvalidOperationException>(() => db.Get(B("k")));
        Assert.ThrowsExactly<InvalidOperationException>(() => db.MultiGet([B("ok"), B("k")]));
        db.Flush();
        Assert.ThrowsExactly<InvalidOperationException>(() => db.Get(B("k")));
        Assert.AreEqual("a", GetString(db, "ok"));
        // Iterators do not call it.
        using (Iterator it = db.NewIterator())
        {
            it.Seek(B("k"));
            Assert.AreEqual("boom1", S(it.Value.ToArray()));
        }
        db.Put(B("k"), B("fixed"));
        Assert.AreEqual("fixed", GetString(db, "k"));
    }

    /// <summary>The default ShouldMerge returns false (merge_operator.h), so built-in operators keep merging with the base value.</summary>
    [TestMethod]
    public void ShouldMerge_DefaultIsFalse()
    {
        Assert.IsFalse(MergeOperator.Put.ShouldMerge(default));
        Assert.IsFalse(MergeOperator.StringAppend(",").ShouldMerge(new MergeOperands("ab"u8, [0, 1, 2])));
        Assert.IsFalse(MergeOperator.Put.OverridesShouldMerge);
        Assert.IsTrue(new LimitedAppend(1).OverridesShouldMerge);
    }

    // =================================================================================================================
    // 2. max_successive_merges / strict_max_successive_merges
    // =================================================================================================================

    /// <summary>
    /// Port of DBMergeOperatorTest.MaxSuccessiveMergesBaseValues (db/db_merge_operator_test.cc): with
    /// max_successive_merges = 1 the second merge is converted to a Put (no base value, plain base value) or a PutEntity
    /// that keeps the other columns (wide-column base value).
    /// </summary>
    [TestMethod]
    public void MaxSuccessiveMerges_BaseValues()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { MergeOperator = MergeOperator.Put, MaxSuccessiveMerges = 1 });

        // No base value
        db.Merge(B("key1"), B("foo"));
        db.Merge(B("key1"), B("bar"));
        Assert.AreEqual("bar", GetString(db, "key1"));
        // We expect the second Merge to be converted to a Put because of max_successive_merges.
        CollectionAssert.AreEqual(new[] { EntryType.Value, EntryType.Merge }, VersionTypes(db, "key1"));

        // Plain base value
        db.Put(B("key2"), B("foo"));
        db.Merge(B("key2"), B("bar"));
        db.Merge(B("key2"), B("baz"));
        Assert.AreEqual("baz", GetString(db, "key2"));
        CollectionAssert.AreEqual(new[] { EntryType.Value, EntryType.Merge, EntryType.Value }, VersionTypes(db, "key2"));

        // Wide-column base value
        db.PutEntity(B("key3"), [new WideColumn(ReadOnlyMemory<byte>.Empty, B("foo")), new WideColumn(B("bar"), B("baz"))]);
        db.Merge(B("key3"), B("qux"));
        db.Merge(B("key3"), B("corge"));
        WideColumns? e = db.GetEntity(B("key3"));
        Assert.IsNotNull(e);
        Assert.AreEqual(2, e.Count);
        Assert.AreEqual("corge", S(e.DefaultColumnValue.ToArray()));
        Assert.IsTrue(e.TryGetValue(B("bar"), out ReadOnlySpan<byte> barValue));
        Assert.AreEqual("baz", S(barValue.ToArray()));
        CollectionAssert.AreEqual(new[] { EntryType.WideColumnEntity, EntryType.Merge, EntryType.WideColumnEntity }, VersionTypes(db, "key3"));
    }

    /// <summary>
    /// The count is of successive merges at the head of the key in the memtable (MemTable::CountSuccessiveMergeEntries,
    /// capped at the limit): with 3, every fourth merge reads the value (including the earlier merges of the same batch,
    /// read at the batch's sequence) and stores the merged result instead of the operand, and the count restarts. A
    /// write batch with more merges than the limit (TransactionTest's "Merge more than max_successive_merges times")
    /// folds inside the batch.
    /// </summary>
    [TestMethod]
    public void MaxSuccessiveMerges_FoldsEveryLimitPlusOneMerges()
    {
        MemoryFileSystem fs = NewFs();
        var op = new Bracket();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { MergeOperator = op, MaxSuccessiveMerges = 3 });
        db.Put(B("k"), B("b"));
        for (int i = 1; i <= 3; i++) db.Merge(B("k"), B(i.ToString(System.Globalization.CultureInfo.InvariantCulture)));
        Assert.AreEqual(0, op.FullMergeCalls);
        db.Merge(B("k"), B("4"));
        // Read [b:1,2,3] then merged with 4 at write time: one FullMerge for the read, one for the fold.
        Assert.AreEqual(2, op.FullMergeCalls);
        Assert.AreEqual("[[b:1,2,3]:4]", GetString(db, "k"));
        for (int i = 5; i <= 8; i++) db.Merge(B("k"), B(i.ToString(System.Globalization.CultureInfo.InvariantCulture)));
        Assert.AreEqual("[[[[b:1,2,3]:4]:5,6,7]:8]", GetString(db, "k"));
        CollectionAssert.AreEqual(new[]
        {
            EntryType.Value, EntryType.Merge, EntryType.Merge, EntryType.Merge, EntryType.Value, EntryType.Merge, EntryType.Merge, EntryType.Merge,
            EntryType.Value,
        }, VersionTypes(db, "k"));

        // One batch: Put then four merges with a limit of 2 (TransactionTest: "value,1,2,3,4" with StringAppend).
        using RocksDatabase db2 = RocksDatabase.Open("/db2", NewOptions(fs), new ColumnFamilyOptions
        {
            MergeOperator = MergeOperator.StringAppend(","), MaxSuccessiveMerges = 2,
        });
        db2.Put(B("key"), B("value"));
        using (var batch = new WriteBatch())
        {
            batch.Merge(B("key"), B("1")).Merge(B("key"), B("2")).Merge(B("key"), B("3")).Merge(B("key"), B("4"));
            db2.Write(batch);
        }
        Assert.AreEqual("value,1,2,3,4", GetString(db2, "key"));
        CollectionAssert.AreEqual(new[] { EntryType.Merge, EntryType.Value, EntryType.Merge, EntryType.Merge, EntryType.Value }, VersionTypes(db2, "key"));
    }

    /// <summary>
    /// Port of DBMergeOperatorTest.MergeErrorOnWrite: the merge at the limit fails, the delta is inserted nevertheless
    /// and the data stays unmerged. A merge operator that throws during the write-time merge is treated the same way
    /// (the write succeeds, the operand is stored, and the read surfaces the exception).
    /// </summary>
    [TestMethod]
    public void MaxSuccessiveMerges_FailedOrThrowingMergeStoresTheOperand()
    {
        MemoryFileSystem fs = NewFs();
        var op = new Bracket();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { MergeOperator = op, MaxSuccessiveMerges = 3 });
        db.Merge(B("k1"), B("v1"));
        db.Merge(B("k1"), B("v2"));
        db.Merge(B("k1"), B("v3"));
        // Will trigger a merge when hitting max_successive_merges and the merge will fail. The delta will be inserted
        // nevertheless.
        db.Merge(B("k1"), B("corrupted"));
        CollectionAssert.AreEqual(new[] { EntryType.Merge, EntryType.Merge, EntryType.Merge, EntryType.Merge }, VersionTypes(db, "k1"));
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.Get(B("k1")));
        Assert.AreEqual(StatusCode.Corruption, ex.Code);

        db.Merge(B("k2"), B("v1"));
        db.Merge(B("k2"), B("v2"));
        db.Merge(B("k2"), B("v3"));
        db.Merge(B("k2"), B("throw"));
        CollectionAssert.AreEqual(new[] { EntryType.Merge, EntryType.Merge, EntryType.Merge, EntryType.Merge }, VersionTypes(db, "k2"));
        Assert.ThrowsExactly<InvalidOperationException>(() => db.Get(B("k2")));
        // The DB keeps working; a later value replaces the bad operands.
        db.Put(B("k2"), B("p"));
        db.Merge(B("k2"), B("x"));
        Assert.AreEqual("[p:x]", GetString(db, "k2"));
    }

    /// <summary>
    /// Without strict_max_successive_merges the write-time read uses read_tier = kBlockCacheTier: a base value whose data
    /// block is not cached makes the read Incomplete and the operand is stored; once the block is in the block cache (a
    /// Get loaded it) the next merge at the limit folds. strict_max_successive_merges reads the table file and folds
    /// right away.
    /// </summary>
    [TestMethod]
    public void MaxSuccessiveMerges_StrictControlsTableFileReads()
    {
        foreach (bool strict in new[] { false, true })
        {
            MemoryFileSystem fs = NewFs();
            var op = new Bracket();
            var cfo = new ColumnFamilyOptions { MergeOperator = op, MaxSuccessiveMerges = 2, StrictMaxSuccessiveMerges = strict, DisableAutoCompactions = true };
            using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo))
            {
                db.Put(B("k"), B("b"));
                db.Flush();
            }
            using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo))
            {
                db.Merge(B("k"), B("1"));
                db.Merge(B("k"), B("2"));
                db.Merge(B("k"), B("3"));
                if (strict)
                {
                    CollectionAssert.AreEqual(new[] { EntryType.Value, EntryType.Merge, EntryType.Merge, EntryType.Value }, VersionTypes(db, "k"));
                    Assert.AreEqual("[[b:1,2]:3]", GetString(db, "k"));
                    continue;
                }
                CollectionAssert.AreEqual(new[] { EntryType.Merge, EntryType.Merge, EntryType.Merge, EntryType.Value }, VersionTypes(db, "k"), "not folded");
                Assert.AreEqual("[b:1,2,3]", GetString(db, "k")); // loads the data block into the block cache
                db.Merge(B("k"), B("4"));
                CollectionAssert.AreEqual(new[] { EntryType.Value, EntryType.Merge, EntryType.Merge, EntryType.Merge, EntryType.Value }, VersionTypes(db, "k"));
                Assert.AreEqual("[[b:1,2,3]:4]", GetString(db, "k"));
            }
        }
    }

    /// <summary>
    /// max_successive_merges is a mutable option read when a memtable is created (ImmutableMemTableOptions): SetOptions
    /// applies to the next memtable. WAL recovery never folds (recovering_log_number_ != 0), and reopening with a
    /// smaller limit is fine (DBTest2.MaxSuccessiveMergesChangeWithDBRecovery). The value round-trips through the OPTIONS
    /// file.
    /// </summary>
    [TestMethod]
    public void MaxSuccessiveMerges_SetOptionsRecoveryAndOptionsFile()
    {
        MemoryFileSystem fs = NewFs();
        var op = new Bracket();
        var cfo = new ColumnFamilyOptions { MergeOperator = op, DisableAutoCompactions = true };
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo))
        {
            db.SetOptions([new("max_successive_merges", "2"), new("strict_max_successive_merges", "true")]);
            for (int i = 0; i < 3; i++) db.Merge(B("a"), B("x"));
            CollectionAssert.AreEqual(new[] { EntryType.Merge, EntryType.Merge, EntryType.Merge }, VersionTypes(db, "a"), "current memtable keeps 0");
            db.Flush();
            for (int i = 0; i < 3; i++) db.Merge(B("b"), B("y"));
            CollectionAssert.AreEqual(new[] { EntryType.Value, EntryType.Merge, EntryType.Merge }, VersionTypes(db, "b"), "new memtable uses 2");
            Assert.AreEqual("[[_:y,y]:y]", GetString(db, "b"));
            Assert.Contains("max_successive_merges=2", OptionsText(fs));
            Assert.Contains("strict_max_successive_merges=true", OptionsText(fs));
            db.SetOptions([new("max_successive_merges", "0")]);
            db.Flush();
            for (int i = 0; i < 4; i++) db.Merge(B("w"), B("r"));
        }
        // Reopen with a limit of 3 (smaller than the 4 operands of the WAL): the WAL is replayed without folding.
        cfo.MaxSuccessiveMerges = 3;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo))
        {
            CollectionAssert.AreEqual(new[] { EntryType.Merge, EntryType.Merge, EntryType.Merge, EntryType.Merge }, VersionTypes(db, "w"));
            Assert.AreEqual("[_:r,r,r,r]", GetString(db, "w"));
            Assert.Contains("max_successive_merges=3", OptionsText(fs));
            // The replayed memtable was flushed at the end of recovery (avoid_flush_during_recovery = false): the next
            // merge starts a new count in the new memtable.
            db.Merge(B("w"), B("s"));
            Assert.AreEqual("[_:r,r,r,r,s]", GetString(db, "w"));
        }
    }

    /// <summary>The text of the latest OPTIONS file.</summary>
    private static string OptionsText(MemoryFileSystem fs) => Encoding.UTF8.GetString(fs.ReadAllBytes(OptionsFile.FindLatestOptionsFile(fs, DbPath)!));

    /// <summary>
    /// The merged value inherits the operand's write batch protection info, with the value and type replaced
    /// (merged_kv_prot_info.UpdateV / UpdateO): with per key-value protection in the batch and in the memtable the
    /// folded Put is verified like any entry, no false "ProtectionInfo mismatch".
    /// </summary>
    [TestMethod]
    public void MaxSuccessiveMerges_WithKeyValueProtection()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions
        {
            MergeOperator = new Bracket(), MaxSuccessiveMerges = 1, MemtableProtectionBytesPerKey = 8,
        });
        var wo = new WriteOptions { ProtectionBytesPerKey = 8 };
        for (int i = 0; i < 5; i++)
        {
            using var batch = new WriteBatch();
            batch.Merge(B("k"), B(i.ToString(System.Globalization.CultureInfo.InvariantCulture)));
            db.Write(wo, batch);
        }
        // m0 stored; m1 folded into "[[_:0]:1]"; m2 stored; m3 folded; m4 stored.
        const string Expected = "[[[[[_:0]:1]:2]:3]:4]";
        Assert.AreEqual(Expected, GetString(db, "k"));
        CollectionAssert.AreEqual(new[] { EntryType.Merge, EntryType.Value, EntryType.Merge, EntryType.Value, EntryType.Merge }, VersionTypes(db, "k"));
        db.Flush();
        Assert.AreEqual(Expected, GetString(db, "k"));
    }

    // =================================================================================================================
    // 3. CompactionFilter::Context::input_table_properties
    // =================================================================================================================

    /// <summary>Adds a user property "audit11.first" = the first user key of the file.</summary>
    private sealed class FirstKeyCollectorFactory : TablePropertiesCollectorFactory
    {
        public override string Name => "Audit11FirstKey";

        public override TablePropertiesCollector CreateTablePropertiesCollector(TablePropertiesCollectorContext context) => new Collector();

        private sealed class Collector : TablePropertiesCollector
        {
            private byte[]? _first;

            public override string Name => "Audit11FirstKey";

            public override void AddUserKey(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, TableEntryType type, ulong sequence, ulong fileSize) =>
                _first ??= key.ToArray();

            public override void Finish(IDictionary<string, byte[]> properties) => properties["audit11.first"] = _first ?? [];
        }
    }

    /// <summary>Records the context of every filter it creates; the filter keeps everything.</summary>
    private sealed class RecordingFactory : CompactionFilterFactory
    {
        public readonly List<CompactionFilterContext> Contexts = [];

        public override string Name => "Audit11RecordingFactory";

        public override bool ShouldFilterTableFileCreation(TableFileCreationReason reason) => true;

        public override CompactionFilter? CreateCompactionFilter(in CompactionFilterContext context)
        {
            lock (Contexts) Contexts.Add(context);
            return new KeepAll();
        }

        private sealed class KeepAll : CompactionFilter
        {
            public override string Name => "KeepAll";

            public override CompactionFilterDecision Filter(int level, ReadOnlySpan<byte> key, CompactionFilterValueType valueType,
                ReadOnlySpan<byte> existingValue, IBufferWriter<byte> newValue, IBufferWriter<byte> skipUntil) => CompactionFilterDecision.Keep;
        }
    }

    /// <summary>
    /// Compaction::CreateCompactionFilter fills Context::input_table_properties with GetInputTableProperties(): one entry
    /// per input file keyed by its path (TableFileName), with the user properties of table properties collectors.
    /// Flushes have no input files (BuildTable leaves it empty). Every subcompaction's filter gets the same map. Before
    /// the feature the context had no such member.
    /// </summary>
    [TestMethod]
    public void InputTableProperties_FilledForCompactionsEmptyForFlushes()
    {
        MemoryFileSystem fs = NewFs();
        var factory = new RecordingFactory();
        var cfo = new ColumnFamilyOptions
        {
            CompactionFilterFactory = factory, DisableAutoCompactions = true,
            TablePropertiesCollectorFactories = [new FirstKeyCollectorFactory()],
        };
        DbOptions dbo = NewOptions(fs);
        dbo.MaxSubcompactions = 4;
        using RocksDatabase db = RocksDatabase.Open(DbPath, dbo, cfo);
        var expected = new Dictionary<string, string>(StringComparer.Ordinal);
        for (int f = 0; f < 3; f++)
        {
            for (int i = 0; i < 200; i++) db.Put(B("f" + f + "-" + i.ToString("D4", System.Globalization.CultureInfo.InvariantCulture)), new byte[64]);
            db.Flush();
        }
        foreach (TableProperties tp in db.GetPropertiesOfAllTables().Values) Assert.IsNotNull(tp.GetUserPropertyString("audit11.first"));
        foreach (KeyValuePair<string, TableProperties> kv in db.GetPropertiesOfAllTables())
        {
            expected[kv.Key] = kv.Value.GetUserPropertyString("audit11.first")!;
        }
        Assert.AreEqual(3, expected.Count);
        lock (factory.Contexts)
        {
            Assert.AreEqual(3, factory.Contexts.Count);
            foreach (CompactionFilterContext c in factory.Contexts)
            {
                Assert.IsTrue(c.IsFlush);
                Assert.AreEqual(0, c.InputTableProperties.Count, "flush has no input files");
            }
            factory.Contexts.Clear();
        }

        db.CompactRange(new CompactRangeOptions { ChangeLevel = true, TargetLevel = 1 }, null, null);
        lock (factory.Contexts)
        {
            Assert.IsTrue(factory.Contexts.Count >= 1);
            IReadOnlyDictionary<string, TableProperties> props = factory.Contexts[0].InputTableProperties;
            CollectionAssert.AreEquivalent(expected.Keys.ToArray(), props.Keys.ToArray());
            foreach (KeyValuePair<string, TableProperties> kv in props)
            {
                Assert.AreEqual(expected[kv.Key], kv.Value.GetUserPropertyString("audit11.first"));
                Assert.AreEqual(200UL, kv.Value.NumEntries);
            }
            foreach (CompactionFilterContext c in factory.Contexts)
            {
                Assert.IsFalse(c.IsFlush);
                Assert.AreSame(props, c.InputTableProperties, "one map for every subcompaction");
            }
            factory.Contexts.Clear();
        }

        // CompactFiles: only the chosen input files.
        for (int i = 0; i < 10; i++) db.Put(B("g-" + i), B("v"));
        db.Flush();
        List<string> l0 = LevelFiles(db, 0);
        db.CompactFiles(l0, 1);
        lock (factory.Contexts)
        {
            Assert.AreEqual(2, factory.Contexts.Count); // the flush and the compaction
            CompactionFilterContext c = factory.Contexts[1];
            Assert.AreEqual(1, c.InputTableProperties.Count);
            Assert.AreEqual(l0[0], c.InputTableProperties.Keys.Single());
            Assert.AreEqual("g-0", c.InputTableProperties[l0[0]].GetUserPropertyString("audit11.first"));
        }
        // default(CompactionFilterContext) has an empty map, never null.
        Assert.AreEqual(0, default(CompactionFilterContext).InputTableProperties.Count);
    }

    // =================================================================================================================
    // 4. A custom filter policy with the built-in compatibility name
    // =================================================================================================================

    /// <summary>
    /// Upstream's LevelAndStyleCustomFilterPolicy (db/db_bloom_filter_test.cc): returns the built-in compatibility name
    /// and defers to built-in Bloom policies, choosing the builder by level; the reader of any of them reads every
    /// built-in format. Counts its reader creations.
    /// </summary>
    private sealed class LevelAndStyleCustomFilterPolicy(int bpkL0, int bpkOtherwise) : FilterPolicy
    {
        private readonly BloomFilterPolicy _l0 = new(bpkL0);
        private readonly BloomFilterPolicy _otherwise = new(bpkOtherwise);
        public int Readers;

        public override string Name => "LevelAndStyleCustomFilterPolicy";

        // OK to use built-in policy name because we are deferring to a built-in builder. We aren't changing the
        // serialized format.
        public override string CompatibilityName => _l0.CompatibilityName;

        public override IFilterBitsBuilder CreateBuilder(int formatVersion) => _otherwise.CreateBuilder(formatVersion);

        public override IFilterBitsBuilder CreateBuilder(in FilterBuildingContext context) =>
            context.LevelAtCreation == 0 ? _l0.CreateBuilder(context) : _otherwise.CreateBuilder(context);

        public override IFilterBitsReader CreateReader(ReadOnlyMemory<byte> contents)
        {
            Interlocked.Increment(ref Readers);
            return _l0.CreateReader(contents);
        }
    }

    /// <summary>
    /// A custom policy with the built-in name and its own reader: the reader wraps a built-in one and counts the
    /// queries, showing whose reader the table uses.
    /// </summary>
    private sealed class OwnReaderPolicy : FilterPolicy
    {
        private readonly BloomFilterPolicy _inner = new(10);
        public int Readers;
        public int Queries;

        public override string Name => "Audit11OwnReader";

        public override string CompatibilityName => BloomLikeFilterPolicy.BuiltinFilterMetadataName;

        public override IFilterBitsBuilder CreateBuilder(int formatVersion) => _inner.CreateBuilder(formatVersion);

        public override IFilterBitsReader CreateReader(ReadOnlyMemory<byte> contents)
        {
            Interlocked.Increment(ref Readers);
            return new CountingReader(this, _inner.CreateReader(contents));
        }

        private sealed class CountingReader(OwnReaderPolicy owner, IFilterBitsReader inner) : IFilterBitsReader
        {
            public bool MayMatch(ReadOnlySpan<byte> key)
            {
                Interlocked.Increment(ref owner.Queries);
                return inner.MayMatch(key);
            }
        }
    }

    private static ColumnFamilyOptions FilterCfo(FilterPolicy policy, bool partitioned) => new()
    {
        DisableAutoCompactions = true,
        TableOptions = new BlockBasedTableOptions
        {
            FilterPolicy = policy,
            PartitionFilters = partitioned,
            IndexType = partitioned ? IndexType.TwoLevelIndexSearch : IndexType.BinarySearch,
            MetadataBlockSize = 256,
            BlockCache = new Managed.Cache.LruCache(new Managed.Cache.LruCacheOptions { Capacity = 8 << 20 }),
        },
    };

    private static void WriteKeys(RocksDatabase db, int count)
    {
        for (int i = 0; i < count; i += 2) db.Put(Key(i), B("v" + i));
        db.Flush();
    }

    /// <summary>Gets of every key (even ones exist): correct values, and the filter rejects most missing keys.</summary>
    private static void AssertReadsUseFilter(RocksDatabase db, Statistics stats, int count)
    {
        ulong usefulBefore = stats.GetTickerCount(Ticker.BloomFilterUseful);
        for (int i = 0; i < count; i++)
        {
            string? v = GetString(db, S(Key(i))!);
            Assert.AreEqual(i % 2 == 0 ? "v" + i : null, v);
        }
        ulong useful = stats.GetTickerCount(Ticker.BloomFilterUseful) - usefulBefore;
        Assert.IsTrue(useful > (ulong)count / 4, "filter useful " + useful);
    }

    /// <summary>
    /// A delegating wrapper with the built-in name (upstream's LevelAndStyleCustomFilterPolicy) writes built-in filters
    /// and reads them through its own reader (delegated to a built-in policy), with full and partitioned filters; the
    /// files are read by the plain built-in Bloom policy too and vice versa (same compatibility name, same formats).
    /// </summary>
    [TestMethod]
    public void FilterPolicy_DelegatingWrapperWithBuiltinName()
    {
        foreach (bool partitioned in new[] { false, true })
        {
            MemoryFileSystem fs = NewFs();
            var wrapper = new LevelAndStyleCustomFilterPolicy(4, 10);
            var stats = new Statistics();
            DbOptions dbo = NewOptions(fs);
            dbo.Statistics = stats;
            using (RocksDatabase db = RocksDatabase.Open(DbPath, dbo, FilterCfo(wrapper, partitioned)))
            {
                WriteKeys(db, 400);
                AssertReadsUseFilter(db, stats, 400);
                Assert.IsTrue(wrapper.Readers > 0, "the wrapper's reader is used");
            }
            // The plain built-in policy reads the wrapper's filters...
            using (RocksDatabase db = RocksDatabase.Open(DbPath, dbo, FilterCfo(new BloomFilterPolicy(10), partitioned)))
            {
                AssertReadsUseFilter(db, stats, 400);
                db.Put(B("zz"), B("1"));
                db.Flush();
            }
            // ...and the wrapper reads the built-in policy's.
            var again = new LevelAndStyleCustomFilterPolicy(4, 10);
            using (RocksDatabase db = RocksDatabase.Open(DbPath, dbo, FilterCfo(again, partitioned)))
            {
                AssertReadsUseFilter(db, stats, 400);
                Assert.AreEqual("1", GetString(db, "zz"));
                Assert.IsTrue(again.Readers > 0);
            }
        }
    }

    /// <summary>
    /// A custom policy that reuses the built-in compatibility name but supplies its own reader: the table uses that
    /// reader (ParsedFullFilterBlock: filter_policy->GetFilterBitsReader), for full and partitioned filters. Before the
    /// fix the port substituted the built-in reader whenever the name was the built-in one, so the policy's reader was
    /// never created or queried.
    /// </summary>
    [TestMethod]
    public void FilterPolicy_CustomReaderWithBuiltinNameIsUsed()
    {
        foreach (bool partitioned in new[] { false, true })
        {
            MemoryFileSystem fs = NewFs();
            var policy = new OwnReaderPolicy();
            var stats = new Statistics();
            DbOptions dbo = NewOptions(fs);
            dbo.Statistics = stats;
            using RocksDatabase db = RocksDatabase.Open(DbPath, dbo, FilterCfo(policy, partitioned));
            WriteKeys(db, 400);
            AssertReadsUseFilter(db, stats, 400);
            Assert.IsTrue(policy.Readers > 0, "CreateReader called");
            Assert.IsTrue(policy.Queries >= 400, "queries " + policy.Queries);
        }
    }
}
