using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;
using RocksDb.Managed.Tests.Table;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4 follow-up, MultiGet slice: DBImpl::MultiGetImpl / Version::MultiGet / BlockBasedTable::MultiGet read side
/// by side with Db/DbImpl.MultiGet.cs and Table/BlockBasedTableReader.MultiGet.cs.
/// </summary>
[TestClass]
public sealed class Audit4FollowUpMultiGetTests
{
    private const string DbPath = "/audit4-multiget";

    private static string K(int i) => "k" + i.ToString("D2", System.Globalization.CultureInfo.InvariantCulture);

    [TestMethod]
    public void ValueSizeSoftLimitIsAppliedPerBatchOf32SortedKeys()
    {
        // DBImpl::MultiGetImpl (db/db_impl/db_impl_sync_and_async.h:560-615): the sorted keys are looked up in batches
        // of MultiGetContext::MAX_BATCH_SIZE (32) -- memtables, then Version::MultiGet, per batch -- and the loop ends
        // with Aborted once curr_value_size > value_size_soft_limit; the keys of the batches not started get that
        // status (lines 695-701). Here the first batch (k00..k31) finds k00 in a table file: 10 bytes > 5, so k00 is
        // returned (Version::MultiGetFromSST marks it done before the check, version_set_sync_and_async.h:321-357),
        // the rest of the batch is Aborted (Version::MultiGet's final loop, lines 666-669) and k32, although it sits
        // in the memtable, belongs to the second batch and is never looked up. The port searched the memtables for
        // all keys first and returned k32 while aborting k00.
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        db.Put(B(K(0)), B("0123456789"));
        db.Flush();
        db.Put(B(K(32)), B("abcdefghij"));
        using var batch = new MultiGetBatch();
        // Added in reverse: the batches are cut from the keys sorted by user key, not from the caller's order.
        for (int i = 32; i >= 0; i--) batch.Add(B(K(i)));
        db.MultiGet(new ReadOptions { ValueSizeSoftLimit = 5 }, batch);
        Assert.AreEqual(MultiGetStatus.Found, batch.GetStatus(32), K(0));
        Assert.AreEqual("0123456789", S(batch.GetValue(32).ToArray()));
        for (int i = 0; i < 32; i++)
        {
            Assert.AreEqual(MultiGetStatus.Error, batch.GetStatus(i), K(32 - i));
            Assert.AreEqual(StatusCode.Aborted, batch.GetError(i)!.Code, K(32 - i));
        }

        // Within one batch the memtables come first (MemTable::MultiGet, db/memtable.cc:2042-2048): the memtable value
        // crosses the limit and every other key of the batch is Aborted, including the one in the table file.
        using var small = new MultiGetBatch();
        small.Add(B(K(0))).Add(B(K(32))).Add(B(K(5)));
        db.MultiGet(new ReadOptions { ValueSizeSoftLimit = 5 }, small);
        Assert.AreEqual(StatusCode.Aborted, small.GetError(0)!.Code);
        Assert.AreEqual(MultiGetStatus.Found, small.GetStatus(1));
        Assert.AreEqual(StatusCode.Aborted, small.GetError(2)!.Code);

        // A limit that is not crossed changes nothing, across batches too.
        using var all = new MultiGetBatch();
        for (int i = 0; i <= 40; i++) all.Add(B(K(i)));
        db.MultiGet(new ReadOptions { ValueSizeSoftLimit = 20 }, all);
        Assert.AreEqual(2, all.FoundCount);
        for (int i = 0; i <= 40; i++) Assert.AreEqual(i is 0 or 32 ? MultiGetStatus.Found : MultiGetStatus.NotFound, all.GetStatus(i), K(i));
    }

    [TestMethod]
    public void ValueSizeSoftLimitAbortsLaterBatchesAfterTableFileHit()
    {
        // MultiGetImpl: a limit crossed by the last key of a batch still ends the call (curr_value_size check after the
        // batch, db_impl_sync_and_async.h:611-615), so every key of the next batch is Aborted even when it is absent.
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        db.Put(B(K(31)), B("0123456789"));
        db.Put(B(K(40)), B("abcdefghij"));
        db.Flush();
        using var batch = new MultiGetBatch();
        for (int i = 0; i <= 40; i++) batch.Add(B(K(i)));
        db.MultiGet(new ReadOptions { ValueSizeSoftLimit = 5 }, batch);
        // First batch k00..k31: its last key crosses the limit and is returned; the keys of the batch that were not
        // found yet are Aborted, not NotFound (Version::MultiGet skips the left-over loop when s is not OK and sets s
        // on every key still in the range, version_set_sync_and_async.h:623 and 666-669).
        for (int i = 0; i < 31; i++) Assert.AreEqual(StatusCode.Aborted, batch.GetError(i)?.Code, K(i));
        Assert.AreEqual(MultiGetStatus.Found, batch.GetStatus(31));
        for (int i = 32; i <= 40; i++) Assert.AreEqual(StatusCode.Aborted, batch.GetError(i)?.Code, K(i));
    }

    [TestMethod]
    public void MultiGetMatchesGetForDuplicatesMergesAndRangeTombstonesAcrossLevels()
    {
        // Version::MultiGet / FilePickerMultiGet (db/version_set.cc:537-1028) and TableCache::MultiGet: duplicate keys
        // in one batch, merge operands collected from the memtable, an L0 file and the bottom level, a range tombstone
        // of an L0 file covering an older value in a lower level, and the same at a snapshot. Every key must agree
        // with Get.
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs),
            [new ColumnFamilyDescriptor("default", new ColumnFamilyOptions { DisableAutoCompactions = true, MergeOperator = new StringAppendOperator() })]);
        db.Put(B("a"), B("1"));
        db.Merge(B("m"), B("x"));
        db.Put(B("d"), B("old"));
        db.Flush();
        db.CompactRange();
        db.Merge(B("m"), B("y"));
        db.DeleteRange(B("c"), B("e"));
        db.Put(B("e"), B("E"));
        db.Flush();
        using Snapshot snap = db.GetSnapshot();
        db.Merge(B("m"), B("z"));
        db.Put(B("a"), B("2"));
        db.Put(B("d"), B("new"));

        string[] keys = ["a", "m", "d", "m", "a", "zz", "e", "d"];
        string?[] latest = ["2", "x,y,z", "new", "x,y,z", "2", null, "E", "new"];
        string?[] atSnap = ["1", "x,y", null, "x,y", "1", null, "E", null];
        foreach (bool useSnapshot in new[] { false, true })
        {
            var ro = new ReadOptions { Snapshot = useSnapshot ? snap : null };
            string?[] expected = useSnapshot ? atSnap : latest;
            using var batch = new MultiGetBatch();
            foreach (string k in keys) batch.Add(B(k));
            db.MultiGet(ro, batch);
            for (int i = 0; i < keys.Length; i++)
            {
                string what = keys[i] + " #" + i + (useSnapshot ? " at snapshot" : "");
                string? got = batch.GetStatus(i) == MultiGetStatus.Found ? S(batch.GetValue(i).ToArray()) : null;
                Assert.AreEqual(expected[i] is null ? MultiGetStatus.NotFound : MultiGetStatus.Found, batch.GetStatus(i), what);
                Assert.AreEqual(expected[i], got, what);
                Assert.AreEqual(S(db.Get(ro, B(keys[i]))), got, what);
            }
        }
    }

    [TestMethod]
    public void HashIndexPrefixesBlockReadFailureFailsTheOpen()
    {
        // HashIndexReader::Create (table/block_based/hash_index_reader.cc:81-95): a failed read of the
        // "rocksdb.hashindex.prefixes" block is returned ("return s", line 83) and fails the table open, while a failed
        // read of "rocksdb.hashindex.metadata" is ignored ("return Status::OK()", line 94: binary search is used).
        // The port ignored both.
        var entries = new List<(byte[] Key, byte[] Value)>();
        for (int i = 0; i < 200; i++) entries.Add((TableTestUtil.IKey("p" + (i / 10).ToString("D3") + "/" + i.ToString("D5"), 5), B("v" + i)));
        SliceTransform pe = SliceTransform.CreateFixedPrefix(4);
        var format = new TableFormatOptions { BlockSize = 256, IndexType = IndexType.HashSearch };
        byte[] file = TableTestUtil.BuildTable(entries, format, new TableBuilderOptions { PrefixExtractor = pe });
        BlockHandle prefixes, meta;
        using (BlockBasedTableReader r = TableTestUtil.Open(file, new TableReaderOptions { PrefixExtractor = pe }))
        {
            Assert.IsNotNull(r.PrefixIndex);
            prefixes = r.MetaBlocks[BlockBasedTableConstants.HashIndexPrefixesBlockName];
            meta = r.MetaBlocks[BlockBasedTableConstants.HashIndexPrefixesMetadataBlockName];
        }

        byte[] badMeta = (byte[])file.Clone();
        badMeta[(int)meta.Offset] ^= 0x40;
        using (BlockBasedTableReader r = TableTestUtil.Open(badMeta, new TableReaderOptions { PrefixExtractor = pe }))
        {
            Assert.IsNull(r.PrefixIndex);
        }

        byte[] badPrefixes = (byte[])file.Clone();
        badPrefixes[(int)prefixes.Offset] ^= 0x40;
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => TableTestUtil.Open(badPrefixes, new TableReaderOptions { PrefixExtractor = pe }).Dispose());
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
    }
}
