using System.Globalization;
using RocksDb.Managed.Db;
using RocksDb.Managed.Db.Iterators;
using RocksDb.Managed.Table;
using RocksDb.Managed.Tests.Table;
using static RocksDb.Managed.Tests.Iterators.IterTestHelpers;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.Tests.Iterators;

/// <summary>
/// A randomized LSM shape (2 memtables, L0 files, two partitioned levels of SSTs, all with range tombstones) read
/// through <see cref="DbIteratorFactory"/> and compared with <see cref="ReferenceModel"/>.
/// </summary>
internal sealed class RandomLsm : IDisposable
{
    public readonly List<TestOp> Ops = new();
    public readonly List<MemTable> MemTables = new();
    public readonly List<List<FileMetaData>> Levels = new() { new(), new(), new() };
    public readonly TestTableCache TableCache = new();
    public readonly MergeOperator Merge = new StringAppendOperator();
    public readonly int KeySpace;
    private ulong _nextFile = 10;

    public static string Key(int i) => "k" + i.ToString("D3", CultureInfo.InvariantCulture);

    public RandomLsm(int seed, int numOps, int keySpace, bool withRangeDeletes)
    {
        KeySpace = keySpace;
        var rnd = new Random(seed);
        for (int i = 0; i < numOps; i++)
        {
            ulong seq = (ulong)i + 1;
            int r = rnd.Next(100);
            string k = Key(rnd.Next(keySpace));
            if (r < 40) Ops.Add(new TestOp(k, seq, EntryType.Value, "v" + seq));
            else if (r < 68) Ops.Add(new TestOp(k, seq, EntryType.Merge, "m" + seq));
            else if (r < 85) Ops.Add(new TestOp(k, seq, EntryType.Deletion, ""));
            else if (r < 92) Ops.Add(new TestOp(k, seq, EntryType.SingleDeletion, ""));
            else if (withRangeDeletes)
            {
                int s = rnd.Next(keySpace);
                string start = rnd.Next(4) == 0 ? Key(s) + "x" : Key(s);
                Ops.Add(new TestOp(start, seq, EntryType.RangeDeletion, Key(Math.Min(keySpace, s + 1 + rnd.Next(8)))));
            }
            else Ops.Add(new TestOp(k, seq, EntryType.Value, "v" + seq));
        }

        // Oldest 35% -> L2, next 25% -> L1, next 20% -> 2 L0 files, rest -> 2 memtables.
        int n = Ops.Count;
        int c2 = n * 35 / 100, c1 = c2 + n * 25 / 100, c0 = c1 + n * 20 / 100, cm = c0 + (n - c0) / 2;
        BuildLevel(2, Ops.GetRange(0, c2), rnd);
        BuildLevel(1, Ops.GetRange(c2, c1 - c2), rnd);
        int mid0 = (c0 - c1) / 2;
        AddL0File(Ops.GetRange(c1, mid0));
        AddL0File(Ops.GetRange(c1 + mid0, c0 - c1 - mid0));
        MemTables.Add(BuildMem(Ops.GetRange(cm, n - cm))); // newest first
        MemTables.Add(BuildMem(Ops.GetRange(c0, cm - c0)));
    }

    private static MemTable BuildMem(List<TestOp> ops)
    {
        var mem = new MemTable(InternalKeyComparator.Bytewise, new MemTableOptions(), 0);
        foreach (TestOp op in ops) Assert.IsTrue(mem.Add(op.Seq, op.Type, B(op.Key), B(op.Value)));
        return mem;
    }

    private void AddL0File(List<TestOp> ops)
    {
        FileMetaData? f = BuildFile(ops, null, null);
        if (f is not null) Levels[0].Insert(0, f);
    }

    /// <summary>Partitions the key space into 3 files; range tombstones are split at partition boundaries like compaction output.</summary>
    private void BuildLevel(int level, List<TestOp> ops, Random rnd)
    {
        int a = 1 + rnd.Next(KeySpace / 2), b = a + 1 + rnd.Next(KeySpace / 2);
        string?[] cuts = [null, Key(a), Key(b), null];
        for (int p = 0; p < 3; p++)
        {
            FileMetaData? f = BuildFile(ops, cuts[p], cuts[p + 1]);
            if (f is not null) Levels[level].Add(f);
        }
    }

    private FileMetaData? BuildFile(List<TestOp> ops, string? lo, string? hi)
    {
        InternalKeyComparator icmp = InternalKeyComparator.Bytewise;
        var points = new List<(byte[] Key, byte[] Value)>();
        var dels = new List<(byte[] Key, byte[] Value)>();
        byte[]? smallest = null, largest = null;
        void Extend(byte[] small, byte[] large)
        {
            if (smallest is null || icmp.Compare(small, smallest) < 0) smallest = small;
            if (largest is null || icmp.Compare(large, largest) > 0) largest = large;
        }
        foreach (TestOp op in ops)
        {
            if (op.Type == EntryType.RangeDeletion)
            {
                string s = lo is not null && string.CompareOrdinal(op.Key, lo) < 0 ? lo : op.Key;
                string e = hi is not null && string.CompareOrdinal(op.Value, hi) > 0 ? hi : op.Value;
                if (string.CompareOrdinal(s, e) >= 0) continue;
                byte[] sk = TableTestUtil.IKey(s, op.Seq, EntryType.RangeDeletion);
                dels.Add((sk, B(e)));
                Extend(sk, TableTestUtil.IKey(e, DbFormat.MaxSequenceNumber, EntryType.RangeDeletion));
            }
            else
            {
                if (lo is not null && string.CompareOrdinal(op.Key, lo) < 0) continue;
                if (hi is not null && string.CompareOrdinal(op.Key, hi) >= 0) continue;
                byte[] ik = TableTestUtil.IKey(op.Key, op.Seq, op.Type);
                points.Add((ik, B(op.Value)));
                Extend(ik, ik);
            }
        }
        if (smallest is null) return null;
        points.Sort((x, y) => icmp.Compare(x.Key, y.Key));
        dels.Sort((x, y) => icmp.Compare(x.Key, y.Key));
        points.AddRange(dels); // range deletions go to their own meta block
        byte[] data = TableTestUtil.BuildTable(points, new TableFormatOptions { BlockSize = 256 });
        ulong number = _nextFile++;
        TableCache.AddFile(number, data);
        return new FileMetaData { FileNumber = number, FileSize = (ulong)data.Length, Smallest = smallest, Largest = largest! };
    }

    public DbIteratorSources Sources(ulong sequence, ReadOptions? ro, ulong maxSkip) => new()
    {
        Comparator = InternalKeyComparator.Bytewise,
        ReadOptions = ro,
        Sequence = sequence,
        MergeOperator = Merge,
        MemTables = MemTables,
        Levels = Levels.ConvertAll(static l => (IReadOnlyList<FileMetaData>)l),
        TableCache = TableCache,
        MaxSequentialSkipInIterations = maxSkip,
    };

    public void Dispose()
    {
        foreach (MemTable m in MemTables) m.Dispose();
        TableCache.Dispose();
    }
}

[TestClass]
public sealed class DbIterRandomTests
{
    private static int LowerIndex(List<(string Key, string Value)> e, string target)
    {
        int i = 0;
        while (i < e.Count && string.CompareOrdinal(e[i].Key, target) < 0) i++;
        return i;
    }

    private static int SeekForPrevIndex(List<(string Key, string Value)> e, string target)
    {
        int i = e.Count - 1;
        while (i >= 0 && string.CompareOrdinal(e[i].Key, target) > 0) i--;
        return i;
    }

    private static void AssertPos(List<(string Key, string Value)> e, int pos, Iterator it, string context)
    {
        if (pos < 0 || pos >= e.Count)
        {
            Assert.IsFalse(it.Valid, context + ": expected invalid, got " + (it.Valid ? S(it.Key) : ""));
            return;
        }
        Assert.IsTrue(it.Valid, context + ": expected " + e[pos].Key);
        Assert.AreEqual(e[pos].Key, S(it.Key), context);
        Assert.AreEqual(e[pos].Value, S(it.Value), context + " value of " + e[pos].Key);
    }

    [TestMethod]
    [DataRow(1, true)]
    [DataRow(2, true)]
    [DataRow(3, false)]
    [DataRow(4, true)]
    [DataRow(5, true)]
    public void DifferentialAgainstModel(int seedOffset, bool rangeDeletes)
    {
        int seed = Environment.TickCount ^ (seedOffset * 7919);
        string trace = "seed=" + seed.ToString(CultureInfo.InvariantCulture);
        using var lsm = new RandomLsm(seed, 700, 60, rangeDeletes);
        var rnd = new Random(seed + 1);
        for (int round = 0; round < 12; round++)
        {
            ulong snapshot = round == 0 ? DbFormat.MaxSequenceNumber : (ulong)rnd.Next(1, lsm.Ops.Count + 1);
            string? lower = rnd.Next(3) == 0 ? RandomLsm.Key(rnd.Next(lsm.KeySpace)) : null;
            string? upper = rnd.Next(3) == 0 ? RandomLsm.Key(rnd.Next(lsm.KeySpace)) : null;
            var ro = new ReadOptions();
            if (lower is not null) ro.IterateLowerBound = B(lower);
            if (upper is not null) ro.IterateUpperBound = B(upper);
            ulong maxSkip = rnd.Next(2) == 0 ? 8UL : (ulong)rnd.Next(0, 3);
            string ctx = trace + " round=" + round + " snap=" + snapshot + " lower=" + lower + " upper=" + upper + " maxSkip=" + maxSkip;

            List<(string Key, string Value)> e = ReferenceModel.Expected(lsm.Ops, snapshot, lsm.Merge, lower, upper);
            using Iterator it = DbIteratorFactory.Create(lsm.Sources(snapshot, ro, maxSkip));

            // Forward and reverse full scans.
            var fwd = new List<(string, string)>();
            for (it.SeekToFirst(); it.Valid; it.Next()) fwd.Add((S(it.Key), S(it.Value)));
            Assert.AreEqual(Format(e), Format(fwd), ctx + " forward");
            var rev = new List<(string, string)>();
            for (it.SeekToLast(); it.Valid; it.Prev()) rev.Add((S(it.Key), S(it.Value)));
            rev.Reverse();
            Assert.AreEqual(Format(e), Format(rev), ctx + " reverse");

            // Random walks with direction changes.
            for (int walk = 0; walk < 20; walk++)
            {
                string target = RandomLsm.Key(rnd.Next(lsm.KeySpace + 1)) + (rnd.Next(3) == 0 ? "a" : "");
                int pos;
                if (rnd.Next(2) == 0)
                {
                    it.Seek(B(target));
                    pos = LowerIndex(e, target);
                    AssertPos(e, pos, it, ctx + " Seek(" + target + ")");
                }
                else
                {
                    it.SeekForPrev(B(target));
                    pos = SeekForPrevIndex(e, target);
                    AssertPos(e, pos, it, ctx + " SeekForPrev(" + target + ")");
                }
                for (int step = 0; step < 15 && it.Valid; step++)
                {
                    if (rnd.Next(2) == 0)
                    {
                        it.Next();
                        pos++;
                        AssertPos(e, pos, it, ctx + " Next step " + step);
                    }
                    else
                    {
                        it.Prev();
                        pos--;
                        AssertPos(e, pos, it, ctx + " Prev step " + step);
                    }
                }
            }

            // foreach over a range (ref struct enumerator).
            string rs = RandomLsm.Key(rnd.Next(lsm.KeySpace)), re = RandomLsm.Key(rnd.Next(lsm.KeySpace));
            var ranged = new List<(string, string)>();
            foreach (KeyValueRef kv in it.Range(B(rs), B(re))) ranged.Add((S(kv.Key), S(kv.Value)));
            List<(string Key, string Value)> eRange = e.FindAll(x => string.CompareOrdinal(x.Key, rs) >= 0 && string.CompareOrdinal(x.Key, re) < 0);
            Assert.AreEqual(Format(eRange), Format(ranged), ctx + " Range(" + rs + "," + re + ")");
        }
    }

    [TestMethod]
    public async Task AsyncScanMatchesModel()
    {
        int seed = Environment.TickCount;
        using var lsm = new RandomLsm(seed, 500, 50, true);
        List<(string Key, string Value)> e = ReferenceModel.Expected(lsm.Ops, DbFormat.MaxSequenceNumber, lsm.Merge, null, null);
        using Iterator it = await DbIteratorFactory.CreateAsync(lsm.Sources(DbFormat.MaxSequenceNumber, null, 8));
        var got = new List<(string, string)>();
        await foreach (KeyValue kv in it.ScanAsync()) got.Add((S(kv.KeySpan), S(kv.ValueSpan)));
        Assert.AreEqual(Format(e), Format(got), "seed=" + seed);

        string start = RandomLsm.Key(10), end = RandomLsm.Key(30);
        got.Clear();
        await foreach (KeyValue kv in it.ScanAsync(B(start), B(end))) got.Add((S(kv.KeySpan), S(kv.ValueSpan)));
        Assert.AreEqual(Format(e.FindAll(x => string.CompareOrdinal(x.Key, start) >= 0 && string.CompareOrdinal(x.Key, end) < 0)), Format(got), "seed=" + seed);

        got.Clear();
        await it.SeekAsync(B(start));
        while (it.Valid)
        {
            got.Add((S(it.Key), S(it.Value)));
            await it.NextAsync();
        }
        Assert.AreEqual(Format(e.FindAll(x => string.CompareOrdinal(x.Key, start) >= 0)), Format(got), "seed=" + seed);
    }

    [TestMethod]
    public void LevelIteratorOpensFilesLazilyAndReleasesThem()
    {
        using var lsm = new RandomLsm(42, 400, 40, false);
        var sources = new DbIteratorSources
        {
            Comparator = InternalKeyComparator.Bytewise,
            MergeOperator = lsm.Merge,
            Levels = [[], lsm.Levels[1]],
            TableCache = lsm.TableCache,
        };
        int before = lsm.TableCache.IteratorsCreated;
        using (Iterator it = DbIteratorFactory.Create(sources))
        {
            Assert.AreEqual(before, lsm.TableCache.IteratorsCreated, "no file opened before positioning");
            it.Seek(B(RandomLsm.Key(0)));
            Assert.AreEqual(1, lsm.TableCache.OpenIterators);
            int n = 0;
            for (; it.Valid; it.Next()) n++;
            Assert.IsTrue(n > 0);
            Assert.AreEqual(0, lsm.TableCache.OpenIterators, "exhausted level iterator releases its file");
            it.SeekToLast();
            Assert.IsTrue(lsm.TableCache.OpenIterators <= 1);
        }
        Assert.AreEqual(0, lsm.TableCache.OpenIterators);
    }

    [TestMethod]
    public void FindFileBinarySearch()
    {
        InternalKeyComparator icmp = InternalKeyComparator.Bytewise;
        var files = new List<FileMetaData>();
        for (int i = 0; i < 5; i++)
        {
            files.Add(new FileMetaData
            {
                FileNumber = (ulong)i + 1,
                Smallest = TableTestUtil.IKey(RandomLsm.Key(i * 10), 100, EntryType.Value),
                Largest = TableTestUtil.IKey(RandomLsm.Key(i * 10 + 5), 100, EntryType.Value),
            });
        }
        Assert.AreEqual(0, LevelIterator.FindFile(icmp, files, TableTestUtil.LookupKey("a")));
        Assert.AreEqual(0, LevelIterator.FindFile(icmp, files, TableTestUtil.LookupKey(RandomLsm.Key(5))));
        Assert.AreEqual(1, LevelIterator.FindFile(icmp, files, TableTestUtil.IKey(RandomLsm.Key(5), 99, EntryType.Value)));
        Assert.AreEqual(1, LevelIterator.FindFile(icmp, files, TableTestUtil.LookupKey(RandomLsm.Key(6))));
        Assert.AreEqual(4, LevelIterator.FindFile(icmp, files, TableTestUtil.LookupKey(RandomLsm.Key(45))));
        Assert.AreEqual(5, LevelIterator.FindFile(icmp, files, TableTestUtil.LookupKey(RandomLsm.Key(46))));
        Assert.AreEqual(0, LevelIterator.FindFile(icmp, [], TableTestUtil.LookupKey("x")));
    }
}
