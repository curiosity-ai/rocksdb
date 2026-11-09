using RocksDb.Managed.Memory;
using RocksDb.Managed.Memtable;

namespace RocksDb.Managed.Tests.Memtable;

/// <summary>Skiplist keys are 8-byte native-endian ulongs (as in memtable/inlineskiplist_test.cc's TestComparator).</summary>
internal readonly unsafe struct U64Comparer : ISkipListComparer
{
    public SkipListKey Decode(byte* encodedKey) => new(encodedKey, 8);

    public int Compare(byte* encodedKey, SkipListKey decoded)
    {
        ulong a = *(ulong*)encodedKey, b = *(ulong*)decoded.Ptr;
        return a.CompareTo(b);
    }
}

/// <summary>Ports of memtable/inlineskiplist_test.cc.</summary>
[TestClass]
public sealed unsafe class InlineSkipListTests
{
    private static bool Insert(InlineSkipList<U64Comparer> list, ulong key, bool concurrent)
    {
        byte* buf = list.AllocateKey(8);
        *(ulong*)buf = key;
        return concurrent ? list.InsertConcurrently(buf) : list.Insert(buf);
    }

    private static SkipListKey K(ulong* k) => new((byte*)k, 8);

    private static ulong Get(byte* key) => *(ulong*)key;

    [TestMethod]
    public void Empty()
    {
        using var arena = new Arena();
        var list = new InlineSkipList<U64Comparer>(default, arena);
        ulong k = 10;
        Assert.IsFalse(list.Contains(K(&k)));
        var it = list.GetIterator();
        Assert.IsFalse(it.Valid);
        it.SeekToFirst();
        Assert.IsFalse(it.Valid);
        it.Seek(K(&k));
        Assert.IsFalse(it.Valid);
        it.SeekForPrev(K(&k));
        Assert.IsFalse(it.Valid);
        it.SeekToLast();
        Assert.IsFalse(it.Valid);
    }

    [TestMethod]
    public void InsertAndLookup()
    {
        foreach (bool concurrent in new[] { false, true })
        {
            const int N = 2000;
            const int R = 5000;
            var rnd = new Random(1000);
            var keys = new SortedSet<ulong>();
            using var arena = new ConcurrentArena();
            var list = new InlineSkipList<U64Comparer>(default, arena);
            for (int i = 0; i < N; i++)
            {
                ulong key = (ulong)rnd.Next(R);
                bool added = keys.Add(key);
                Assert.AreEqual(added, Insert(list, key, concurrent), "duplicate detection");
            }
            list.Validate();
            for (ulong i = 0; i < R; i++)
            {
                ulong k = i;
                Assert.AreEqual(keys.Contains(i), list.Contains(K(&k)));
            }

            // Simple iterator tests
            var it = list.GetIterator();
            ulong t = 0;
            it.Seek(K(&t));
            Assert.IsTrue(it.Valid);
            Assert.AreEqual(keys.Min, Get(it.Key));
            it.SeekForPrev(K(&t));
            Assert.AreEqual(keys.Contains(0), it.Valid);
            it.SeekToFirst();
            Assert.AreEqual(keys.Min, Get(it.Key));
            it.SeekToLast();
            Assert.AreEqual(keys.Max, Get(it.Key));

            // Forward iteration test
            for (ulong i = 0; i < R; i++)
            {
                ulong k = i;
                it.Seek(K(&k));
                using IEnumerator<ulong> model = keys.GetViewBetween(i, ulong.MaxValue).GetEnumerator();
                for (int j = 0; j < 3; j++)
                {
                    if (!model.MoveNext())
                    {
                        Assert.IsFalse(it.Valid);
                        break;
                    }
                    Assert.IsTrue(it.Valid);
                    Assert.AreEqual(model.Current, Get(it.Key));
                    it.Next();
                }
            }

            // Backward iteration test
            for (ulong i = 0; i < R; i++)
            {
                ulong k = i;
                it.SeekForPrev(K(&k));
                SortedSet<ulong> view = keys.GetViewBetween(0, i);
                using IEnumerator<ulong> model = view.Reverse().GetEnumerator();
                for (int j = 0; j < 3; j++)
                {
                    if (!model.MoveNext())
                    {
                        Assert.IsFalse(it.Valid);
                        break;
                    }
                    Assert.IsTrue(it.Valid);
                    Assert.AreEqual(model.Current, Get(it.Key));
                    it.Prev();
                }
            }

            // Full scans
            it.SeekToFirst();
            foreach (ulong k in keys)
            {
                Assert.IsTrue(it.Valid);
                Assert.AreEqual(k, Get(it.Key));
                it.Next();
            }
            Assert.IsFalse(it.Valid);
            it.SeekToLast();
            foreach (ulong k in keys.Reverse())
            {
                Assert.IsTrue(it.Valid);
                Assert.AreEqual(k, Get(it.Key));
                it.Prev();
            }
            Assert.IsFalse(it.Valid);

            // ApproximateNumEntries is a rough estimate; for the full range it must be in the right ballpark.
            ulong lo = 0, hi = ulong.MaxValue;
            ulong approx = list.ApproximateNumEntries(K(&lo), K(&hi));
            Assert.IsTrue(approx > (ulong)keys.Count / 4 && approx < (ulong)keys.Count * 4, "approx=" + approx);
        }
    }

    [TestMethod]
    public void SequentialAndReverseInsertsUseSplice()
    {
        using var arena = new Arena(1 << 16);
        var list = new InlineSkipList<U64Comparer>(default, arena);
        for (ulong i = 0; i < 10000; i++) Assert.IsTrue(Insert(list, i * 2, false));
        for (ulong i = 10000; i > 0; i--) Assert.IsTrue(Insert(list, i * 2 - 1, false));
        list.Validate();
        var it = list.GetIterator();
        ulong expect = 0;
        for (it.SeekToFirst(); it.Valid; it.Next()) Assert.AreEqual(expect++, Get(it.Key));
        Assert.AreEqual(20000UL, expect);
        Assert.IsTrue(list.Height > 1 && list.Height <= 12);
    }

    /// <summary>
    /// Concurrent writers (InsertConcurrently) with concurrent readers checking that every scan is strictly ordered and
    /// that keys known to be published before a scan started are visible. Port of the intent of ConcurrentInsert/
    /// ConcurrentTest in inlineskiplist_test.cc, bounded to ~1.5 s.
    /// </summary>
    [TestMethod]
    public void ConcurrentInsertAndRead()
    {
        const int Writers = 4;
        using var arena = new ConcurrentArena(64 * 1024);
        var list = new InlineSkipList<U64Comparer>(default, arena);
        // Each writer inserts keys k with k % Writers == writerId, ascending-random; publishes its progress.
        long[] published = new long[Writers]; // count of keys fully inserted by each writer
        ulong[][] writerKeys = new ulong[Writers][];
        const int PerWriter = 50_000;
        for (int w = 0; w < Writers; w++)
        {
            var rnd = new Random(w + 17);
            var set = new HashSet<ulong>();
            var arr = new ulong[PerWriter];
            for (int i = 0; i < PerWriter; i++)
            {
                ulong k;
                do k = ((ulong)rnd.NextInt64(0, long.MaxValue / Writers) * Writers) + (ulong)w;
                while (!set.Add(k));
                arr[i] = k;
            }
            writerKeys[w] = arr;
        }

        var deadline = DateTime.UtcNow.AddSeconds(1.5);
        int writersDone = 0;
        Exception? failure = null;
        var threads = new List<Thread>();
        for (int w = 0; w < Writers; w++)
        {
            int id = w;
            threads.Add(new Thread(() =>
            {
                try
                {
                    ulong[] arr = writerKeys[id];
                    for (int i = 0; i < arr.Length; i++)
                    {
                        if (!Insert(list, arr[i], concurrent: true)) throw new InvalidOperationException("unexpected duplicate");
                        Volatile.Write(ref published[id], i + 1);
                    }
                }
                catch (Exception e) { failure = e; }
                finally { Interlocked.Increment(ref writersDone); }
            }));
        }
        for (int r = 0; r < 2; r++)
        {
            int rid = r;
            threads.Add(new Thread(() =>
            {
                try
                {
                    var rnd = new Random(rid);
                    while (Volatile.Read(ref writersDone) < Writers && DateTime.UtcNow < deadline)
                    {
                        // Pick a writer and a key it already published; it must be found.
                        int w = rnd.Next(Writers);
                        long n = Volatile.Read(ref published[w]);
                        if (n > 0)
                        {
                            ulong k = writerKeys[w][rnd.NextInt64(n)];
                            if (!list.Contains(K(&k))) throw new InvalidOperationException("published key not visible");
                        }
                        // A bounded scan must be strictly increasing.
                        var it = list.GetIterator();
                        ulong start = (ulong)rnd.NextInt64();
                        it.Seek(K(&start));
                        ulong prev = 0;
                        bool first = true;
                        for (int j = 0; j < 200 && it.Valid; j++, it.Next())
                        {
                            ulong cur = Get(it.Key);
                            if (cur < start || (!first && cur <= prev)) throw new InvalidOperationException("order violated");
                            prev = cur;
                            first = false;
                        }
                    }
                }
                catch (Exception e) { failure = e; }
            }));
        }
        foreach (Thread t in threads) t.Start();
        foreach (Thread t in threads) t.Join();
        Assert.IsNull(failure, failure?.ToString());

        list.Validate();
        int count = 0;
        var iter = list.GetIterator();
        for (iter.SeekToFirst(); iter.Valid; iter.Next()) count++;
        Assert.AreEqual(Writers * PerWriter, count);
        for (int w = 0; w < Writers; w++)
        {
            foreach (ulong k in writerKeys[w])
            {
                ulong kk = k;
                Assert.IsTrue(list.Contains(K(&kk)));
            }
        }
    }

    [TestMethod]
    public void ConcurrentDuplicateInsertsOnlyOneWins()
    {
        using var arena = new ConcurrentArena();
        var list = new InlineSkipList<U64Comparer>(default, arena);
        int wins = 0;
        Parallel.For(0, 8, _ =>
        {
            for (ulong k = 0; k < 2000; k++)
            {
                if (Insert(list, k, concurrent: true)) Interlocked.Increment(ref wins);
            }
        });
        Assert.AreEqual(2000, wins);
        list.Validate();
    }
}

/// <summary>Ports of memory/arena_test.cc essentials.</summary>
[TestClass]
public sealed unsafe class ArenaTests
{
    [TestMethod]
    public void AllocateAndVerify()
    {
        using var arena = new Arena(Arena.MinBlockSize);
        var rnd = new Random(301);
        var allocated = new List<(nint ptr, int size)>();
        long bytes = 0;
        for (int i = 0; i < 20000; i++)
        {
            int s = i % (i / 1000 + 1) == 0 ? (rnd.Next(6000) + 1) : (rnd.Next(20) + 1);
            if (s == 0) s = 1;
            byte* r = rnd.Next(10) == 0 ? arena.AllocateAligned(s) : arena.Allocate(s);
            if ((i & 1) == 0) Assert.AreEqual(0, (int)((nuint)arena.AllocateAligned(8) & 7));
            for (int b = 0; b < s; b++) r[b] = (byte)(i % 256);
            bytes += s;
            allocated.Add(((nint)r, s));
            Assert.IsTrue(arena.MemoryAllocatedBytes >= bytes);
        }
        for (int i = 0; i < allocated.Count; i++)
        {
            byte* p = (byte*)allocated[i].ptr;
            for (int b = 0; b < allocated[i].size; b++) Assert.AreEqual((byte)(i % 256), p[b]);
        }
        Assert.IsTrue(arena.IrregularBlockNum > 0);
    }

    [TestMethod]
    public void MemoryAccounting()
    {
        const int block = 8192;
        using var arena = new Arena(block);
        // The arena starts with its inline block (arena_test.cc MemoryAllocatedBytes: kInlineSize).
        Assert.AreEqual((long)Arena.InlineSize, arena.MemoryAllocatedBytes);
        Assert.IsTrue(arena.IsEmpty);
        arena.Allocate(100);
        Assert.AreEqual((long)Arena.InlineSize, arena.MemoryAllocatedBytes);
        Assert.IsTrue(arena.ApproximateMemoryUsage >= 100);
        // Past the inline block, a regular block.
        arena.Allocate(Arena.InlineSize);
        Assert.IsFalse(arena.IsEmpty);
        Assert.AreEqual((long)Arena.InlineSize + block, arena.MemoryAllocatedBytes);
        long usage = arena.ApproximateMemoryUsage;
        Assert.IsTrue(usage >= Arena.InlineSize + 100 && usage < Arena.InlineSize + block, "usage=" + usage);
        // Large allocation goes to an irregular block and does not disturb the current block.
        int unused = arena.AllocatedAndUnused;
        arena.Allocate(block);
        Assert.AreEqual(1, arena.IrregularBlockNum);
        Assert.AreEqual(unused, arena.AllocatedAndUnused);
        Assert.AreEqual(Arena.MinBlockSize, Arena.OptimizeBlockSize(1));
        Assert.AreEqual(4112, Arena.OptimizeBlockSize(4097));
    }

    [TestMethod]
    public void ConcurrentArenaThreadsDoNotOverlap()
    {
        using var arena = new ConcurrentArena(64 * 1024);
        const int Threads = 4, PerThread = 20000;
        var ptrs = new (nint p, int size)[Threads][];
        Parallel.For(0, Threads, t =>
        {
            var rnd = new Random(t);
            var mine = new (nint, int)[PerThread];
            for (int i = 0; i < PerThread; i++)
            {
                int s = rnd.Next(1, 64);
                byte* p = (i & 1) == 0 ? arena.AllocateAligned(s) : arena.Allocate(s);
                new Span<byte>(p, s).Fill((byte)(t + 1));
                mine[i] = ((nint)p, s);
            }
            ptrs[t] = mine;
        });
        for (int t = 0; t < Threads; t++)
        {
            foreach ((nint p, int s) in ptrs[t])
            {
                Assert.IsTrue(new ReadOnlySpan<byte>((byte*)p, s).IndexOfAnyExcept((byte)(t + 1)) < 0, "allocations overlap");
            }
        }
        Assert.IsTrue(arena.ApproximateMemoryUsage > 0);
        Assert.IsTrue(arena.MemoryAllocatedBytes >= arena.ApproximateMemoryUsage);
    }
}
