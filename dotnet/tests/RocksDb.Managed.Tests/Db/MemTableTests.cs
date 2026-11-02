using RocksDb.Managed.Db;
using ValueType = RocksDb.Managed.Db.ValueType;

namespace RocksDb.Managed.Tests.Db;

/// <summary>Memtable get/iterate/overwrite/deletion/merge/range-deletion semantics (db/memtable.cc, db/db_memtable_test.cc).</summary>
[TestClass]
public sealed class MemTableTests
{
    private static byte[] B(string s) => System.Text.Encoding.UTF8.GetBytes(s);
    private static string S(ReadOnlySpan<byte> s) => System.Text.Encoding.UTF8.GetString(s);

    private static MemTable NewMem(InternalKeyComparator? icmp = null, MemTableOptions? options = null) =>
        new(icmp ?? InternalKeyComparator.Bytewise, options ?? new MemTableOptions(), 0);

    private readonly record struct GetOutcome(MemTableGetResult Result, string? Value, ValueType Type, ulong Seq, ulong MaxCovering);

    private static GetOutcome Get(MemTable mem, string key, ulong seq, MergeContext? mc = null, ulong maxCovering = 0)
    {
        using var lk = new LookupKey(B(key), seq, stackalloc byte[LookupKey.StackBufferSize]);
        MemTableGetResult r = mem.Get(in lk, out ReadOnlySpan<byte> v, out ValueType t, out ulong s, mc, ref maxCovering);
        return new GetOutcome(r, r == MemTableGetResult.Found ? S(v) : null, t, s, maxCovering);
    }

    private static void Put(MemTable mem, ulong seq, string k, string v) => Assert.IsTrue(mem.Add(seq, ValueType.Value, B(k), B(v)));

    [TestMethod]
    public void EmptyMemTable()
    {
        using MemTable mem = NewMem();
        Assert.IsTrue(mem.IsEmpty);
        Assert.AreEqual(MemTableGetResult.NotFound, Get(mem, "a", 100).Result);
        MemTableIterator it = mem.NewIterator();
        it.SeekToFirst();
        Assert.IsFalse(it.Valid);
        Assert.AreEqual(0UL, mem.FirstSequenceNumber);
    }

    [TestMethod]
    public void GetWithMultipleSequenceNumbers()
    {
        using MemTable mem = NewMem();
        Put(mem, 1, "k", "v1");
        Put(mem, 5, "k", "v5");
        Put(mem, 3, "j", "j3");
        Assert.IsTrue(mem.Add(9, ValueType.Deletion, B("k"), default));
        Put(mem, 12, "k", "v12");

        Assert.AreEqual(MemTableGetResult.NotFound, Get(mem, "k", 0).Result);
        Assert.AreEqual(new GetOutcome(MemTableGetResult.Found, "v1", ValueType.Value, 1, 0), Get(mem, "k", 1));
        Assert.AreEqual("v1", Get(mem, "k", 4).Value);
        Assert.AreEqual("v5", Get(mem, "k", 5).Value);
        Assert.AreEqual("v5", Get(mem, "k", 8).Value);
        GetOutcome del = Get(mem, "k", 9);
        Assert.AreEqual(MemTableGetResult.Deleted, del.Result);
        Assert.AreEqual(ValueType.Deletion, del.Type);
        Assert.AreEqual(9UL, del.Seq);
        Assert.AreEqual(MemTableGetResult.Deleted, Get(mem, "k", 11).Result);
        Assert.AreEqual("v12", Get(mem, "k", DbFormat.MaxSequenceNumber).Value);
        Assert.AreEqual("j3", Get(mem, "j", 100).Value);
        Assert.AreEqual(MemTableGetResult.NotFound, Get(mem, "i", 100).Result);
        Assert.AreEqual(MemTableGetResult.NotFound, Get(mem, "kk", 100).Result);
        Assert.AreEqual(MemTableGetResult.NotFound, Get(mem, "", 100).Result);

        Assert.AreEqual(5L, mem.NumEntries);
        Assert.AreEqual(1L, mem.NumDeletes);
        Assert.AreEqual(1UL, mem.FirstSequenceNumber);
        Assert.AreEqual(0UL, mem.EarliestSequenceNumber, "earliest starts at the creation seqno (0) and only decreases");
        Assert.IsFalse(mem.IsEmpty);
        Assert.IsTrue(mem.DataSize > 0);
    }

    [TestMethod]
    public void DuplicateKeyAndSequenceRejected()
    {
        using MemTable mem = NewMem();
        Put(mem, 7, "a", "x");
        Assert.IsFalse(mem.Add(7, ValueType.Value, B("a"), B("y")));
        // Same seq, different type is also rejected (memtable ordering ignores the type, as upstream).
        Assert.IsFalse(mem.Add(7, ValueType.Deletion, B("a"), default));
        Assert.IsTrue(mem.Add(8, ValueType.Value, B("a"), B("y")));
        Assert.AreEqual(2L, mem.NumEntries);
    }

    [TestMethod]
    public void SingleDeleteAndValuePreferredSeqno()
    {
        using MemTable mem = NewMem();
        Put(mem, 1, "a", "1");
        Assert.IsTrue(mem.Add(2, ValueType.SingleDeletion, B("a"), default));
        Assert.AreEqual(ValueType.SingleDeletion, Get(mem, "a", 5).Type);
        Assert.AreEqual(MemTableGetResult.Deleted, Get(mem, "a", 5).Result);

        byte[] packed = new byte[3 + 8];
        B("val").CopyTo(packed, 0);
        BitConverter.TryWriteBytes(packed.AsSpan(3), 123UL);
        Assert.IsTrue(mem.Add(10, ValueType.ValuePreferredSeqno, B("b"), packed));
        GetOutcome g = Get(mem, "b", 10);
        Assert.AreEqual(MemTableGetResult.Found, g.Result);
        Assert.AreEqual("val", g.Value);
        Assert.AreEqual(ValueType.Value, g.Type);
    }

    [TestMethod]
    public void MergeOperandsCollected()
    {
        using MemTable mem = NewMem();
        using var mc = new MergeContext();
        Assert.IsTrue(mem.Add(1, ValueType.Value, B("m"), B("base")));
        Assert.IsTrue(mem.Add(2, ValueType.Merge, B("m"), B("+a")));
        Assert.IsTrue(mem.Add(3, ValueType.Merge, B("m"), B("+b")));
        Assert.IsTrue(mem.Add(4, ValueType.Merge, B("n"), B("+x")));
        Assert.IsTrue(mem.Add(5, ValueType.Deletion, B("o"), default));
        Assert.IsTrue(mem.Add(6, ValueType.Merge, B("o"), B("+y")));

        GetOutcome g = Get(mem, "m", 10, mc);
        Assert.AreEqual(MemTableGetResult.Found, g.Result);
        Assert.AreEqual("base", g.Value);
        Assert.AreEqual(2, mc.Count);
        Assert.AreEqual("+b", S(mc.GetOperandNewestFirst(0)));
        Assert.AreEqual("+a", S(mc.GetOperand(0)));
        Assert.AreEqual(3UL, g.Seq);

        mc.Clear();
        Assert.AreEqual(MemTableGetResult.Found, Get(mem, "m", 2, mc).Result);
        Assert.AreEqual(1, mc.Count);

        mc.Clear();
        Assert.AreEqual(MemTableGetResult.MergeInProgress, Get(mem, "n", 10, mc).Result);
        Assert.AreEqual(1, mc.Count);
        Assert.AreEqual(MemTableGetResult.MergeInProgress, Get(mem, "zzz", 10, mc).Result, "carries merge state through a memtable without the key");

        mc.Clear();
        Assert.AreEqual(MemTableGetResult.Deleted, Get(mem, "o", 10, mc).Result);
        Assert.AreEqual(1, mc.Count);

        Assert.AreEqual(MemTableGetResult.MergeInProgress, Get(mem, "m", 10, null).Result);

        // Many operands grow the context.
        mc.Clear();
        for (int i = 0; i < 50; i++) mc.PushOperand(B("op" + i));
        Assert.AreEqual("op49", S(mc.GetOperand(0)));
    }

    [TestMethod]
    public void RangeDeletionCoversOlderEntries()
    {
        using MemTable mem = NewMem();
        Put(mem, 1, "a", "a1");
        Put(mem, 2, "c", "c2");
        Put(mem, 3, "e", "e3");
        Assert.IsTrue(mem.Add(5, ValueType.RangeDeletion, B("b"), B("e")));
        Put(mem, 6, "d", "d6");
        Assert.AreEqual(1L, mem.NumRangeDeletes);

        Assert.AreEqual("a1", Get(mem, "a", 10).Value);
        GetOutcome c = Get(mem, "c", 10);
        Assert.AreEqual(MemTableGetResult.Deleted, c.Result);
        Assert.AreEqual(ValueType.RangeDeletion, c.Type);
        Assert.AreEqual(5UL, c.MaxCovering);
        Assert.AreEqual(5UL, c.Seq);
        Assert.AreEqual("c2", Get(mem, "c", 4).Value, "snapshot before the range deletion");
        Assert.AreEqual("d6", Get(mem, "d", 10).Value, "newer than the tombstone");
        Assert.AreEqual("e3", Get(mem, "e", 10).Value, "end key is exclusive");
        GetOutcome bb = Get(mem, "bb", 10);
        Assert.AreEqual(MemTableGetResult.NotFound, bb.Result);
        Assert.AreEqual(5UL, bb.MaxCovering, "coverage reported for older sources");

        Assert.AreEqual(5UL, mem.MaxCoveringTombstoneSeqnum(B("b"), 100));
        Assert.AreEqual(0UL, mem.MaxCoveringTombstoneSeqnum(B("b"), 4));

        // Newer, overlapping tombstone invalidates the cache.
        Assert.IsTrue(mem.Add(8, ValueType.RangeDeletion, B("d"), B("z")));
        Assert.AreEqual(MemTableGetResult.Deleted, Get(mem, "d", 10).Result);
        Assert.AreEqual(MemTableGetResult.Deleted, Get(mem, "e", 10).Result);
        Assert.AreEqual("d6", Get(mem, "d", 7).Value);
        Assert.AreEqual(8UL, mem.MaxCoveringTombstoneSeqnum(B("d"), 100));
        Assert.AreEqual(5UL, mem.MaxCoveringTombstoneSeqnum(B("d"), 7));

        // Point iterator does not see range tombstones; the range-del iterator does.
        int n = 0;
        MemTableIterator it = mem.NewIterator();
        for (it.SeekToFirst(); it.Valid; it.Next()) n++;
        Assert.AreEqual(4, n);
        MemTableIterator rit = mem.NewRangeTombstoneEntryIterator();
        rit.SeekToFirst();
        Assert.AreEqual("b", S(rit.UserKey));
        Assert.AreEqual("e", S(rit.Value));
        Assert.AreEqual(ValueType.RangeDeletion, rit.Type);
    }

    [TestMethod]
    public void FragmenterOverlaps()
    {
        var list = new List<FragmentedRangeTombstoneList.Tombstone>
        {
            new(B("a"), B("e"), 10),
            new(B("c"), B("g"), 20),
            new(B("c"), B("d"), 5),
            new(B("x"), B("x"), 99), // empty: dropped
        };
        FragmentedRangeTombstoneList f = FragmentedRangeTombstoneList.Build(Comparator.Bytewise, list);
        // [a,c){10} [c,d){20,10,5} [d,e){20,10} [e,g){20}
        Assert.AreEqual(4, f.Count);
        Assert.AreEqual("c", S(f.GetStart(1)));
        CollectionAssert.AreEqual(new ulong[] { 20, 10, 5 }, f.GetSequences(1).ToArray());
        Assert.AreEqual(10UL, f.MaxCoveringTombstoneSeqnum(B("b"), 100));
        Assert.AreEqual(20UL, f.MaxCoveringTombstoneSeqnum(B("cc"), 100));
        Assert.AreEqual(10UL, f.MaxCoveringTombstoneSeqnum(B("cc"), 19));
        Assert.AreEqual(5UL, f.MaxCoveringTombstoneSeqnum(B("cc"), 9));
        Assert.AreEqual(0UL, f.MaxCoveringTombstoneSeqnum(B("cc"), 4));
        Assert.AreEqual(0UL, f.MaxCoveringTombstoneSeqnum(B("g"), 100));
        Assert.AreEqual(0UL, f.MaxCoveringTombstoneSeqnum(B("0"), 100));
    }

    [TestMethod]
    public void IteratorOrderingAndSeek()
    {
        using MemTable mem = NewMem();
        var rnd = new Random(42);
        var model = new SortedDictionary<(string key, ulong negSeq), string>();
        ulong seq = 1;
        for (int i = 0; i < 3000; i++)
        {
            string k = "key" + rnd.Next(500).ToString("D4");
            string v = "val" + i;
            Put(mem, seq, k, v);
            model[(k, ulong.MaxValue - seq)] = v;
            seq++;
        }
        var expected = new List<(string k, ulong seq, string v)>();
        foreach (var kv in model) expected.Add((kv.Key.key, ulong.MaxValue - kv.Key.negSeq, kv.Value));

        MemTableIterator it = mem.NewIterator();
        int idx = 0;
        for (it.SeekToFirst(); it.Valid; it.Next(), idx++)
        {
            Assert.AreEqual(expected[idx].k, S(it.UserKey));
            Assert.AreEqual(expected[idx].seq, it.Sequence);
            Assert.AreEqual(expected[idx].v, S(it.Value));
            Assert.AreEqual(ValueType.Value, it.Type);
        }
        Assert.AreEqual(expected.Count, idx);
        idx = expected.Count - 1;
        for (it.SeekToLast(); it.Valid; it.Prev(), idx--)
        {
            Assert.AreEqual(expected[idx].k, S(it.UserKey));
            Assert.AreEqual(expected[idx].seq, it.Sequence);
        }
        Assert.AreEqual(-1, idx);

        // Seek to (user key, snapshot) lands on the newest entry visible at that snapshot.
        (string k, ulong seq, string v) target = expected[expected.Count / 2];
        it.Seek(new InternalKey(B(target.k), target.seq, DbFormat.ValueTypeForSeek).Encoded);
        Assert.IsTrue(it.Valid);
        Assert.AreEqual(target.k, S(it.UserKey));
        Assert.AreEqual(target.seq, it.Sequence);
        it.SeekForPrev(new InternalKey(B(target.k), target.seq, ValueType.Deletion).Encoded);
        Assert.AreEqual(target.seq, it.Sequence);
        it.Seek(new InternalKey(B("zzz"), 1, ValueType.Value).Encoded);
        Assert.IsFalse(it.Valid);
        it.SeekForPrev(new InternalKey(B("a"), 1, ValueType.Value).Encoded);
        Assert.IsFalse(it.Valid);

        ulong approx = mem.ApproximateNumEntries(InternalKey.MinPossibleForUserKey(B("key0100")).Encoded, InternalKey.MinPossibleForUserKey(B("key0200")).Encoded);
        Assert.IsTrue(approx > 100 && approx < 3000, "approx=" + approx);
    }

    [TestMethod]
    public void LargeKeysAndValues()
    {
        using MemTable mem = NewMem();
        byte[] key = new byte[300];
        byte[] val = new byte[100_000]; // bigger than a quarter arena block -> irregular block
        new Random(3).NextBytes(key);
        new Random(4).NextBytes(val);
        Assert.IsTrue(mem.Add(1, ValueType.Value, key, val));
        Assert.IsTrue(mem.Add(2, ValueType.Value, key, default));
        using var lk = new LookupKey(key, 1, stackalloc byte[LookupKey.StackBufferSize]);
        ulong mc = 0;
        Assert.AreEqual(MemTableGetResult.Found, mem.Get(in lk, out ReadOnlySpan<byte> v, out _, out _, null, ref mc));
        Assert.IsTrue(v.SequenceEqual(val));
        MemTableIterator it = mem.NewIterator();
        it.SeekToLast();
        Assert.IsTrue(it.UserKey.SequenceEqual(key));
        Assert.AreEqual(1UL, it.Sequence);
        Assert.AreEqual(val.Length, it.Value.Length);
    }

    [TestMethod]
    public void ReverseComparatorMemTable()
    {
        using MemTable mem = NewMem(new InternalKeyComparator(Comparator.ReverseBytewise));
        Put(mem, 1, "a", "1");
        Put(mem, 2, "b", "2");
        Put(mem, 3, "c", "3");
        var keys = new List<string>();
        MemTableIterator it = mem.NewIterator();
        for (it.SeekToFirst(); it.Valid; it.Next()) keys.Add(S(it.UserKey));
        CollectionAssert.AreEqual(new[] { "c", "b", "a" }, keys);
        Assert.AreEqual("2", Get(mem, "b", 10).Value);
    }

    [TestMethod]
    public void ShouldFlushWhenWriteBufferFull()
    {
        var opts = new MemTableOptions { WriteBufferSize = 256 * 1024 };
        Assert.AreEqual(32 * 1024, opts.GetEffectiveArenaBlockSize());
        using MemTable mem = NewMem(options: opts);
        Assert.IsFalse(mem.ShouldScheduleFlush);
        byte[] val = new byte[100];
        ulong seq = 1;
        while (!mem.ShouldScheduleFlush)
        {
            Assert.IsTrue(mem.Add(seq, ValueType.Value, BitConverter.GetBytes(seq), val));
            seq++;
            Assert.IsTrue(seq < 10_000, "never became full");
        }
        long usage = mem.ApproximateMemoryUsage;
        Assert.IsTrue(usage > 200 * 1024 && usage < 300 * 1024, "usage=" + usage);
        Assert.IsTrue(mem.MarkFlushScheduled());
        Assert.IsFalse(mem.MarkFlushScheduled());
        Assert.IsFalse(mem.ShouldScheduleFlush);
    }

    [TestMethod]
    public void MaxRangeDeletionsTriggersFlush()
    {
        using MemTable mem = NewMem(options: new MemTableOptions { MaxRangeDeletions = 2 });
        Assert.IsTrue(mem.Add(1, ValueType.RangeDeletion, B("a"), B("b")));
        Assert.IsFalse(mem.ShouldScheduleFlush);
        Assert.IsTrue(mem.Add(2, ValueType.RangeDeletion, B("c"), B("d")));
        Assert.IsTrue(mem.ShouldScheduleFlush);
    }

    [TestMethod]
    public void ConcurrentAddsWithReaders()
    {
        using MemTable mem = NewMem();
        const int Writers = 4, PerWriter = 20_000;
        Exception? failure = null;
        int done = 0;
        var threads = new List<Thread>();
        for (int w = 0; w < Writers; w++)
        {
            int id = w;
            threads.Add(new Thread(() =>
            {
                var info = new MemTablePostProcessInfo();
                try
                {
                    for (int i = 0; i < PerWriter; i++)
                    {
                        ulong seq = (ulong)(i * Writers + id + 1);
                        byte[] k = B("k" + (i % 1000).ToString("D4"));
                        ValueType t = i % 7 == 0 ? ValueType.Deletion : ValueType.Value;
                        if (!mem.AddConcurrently(seq, t, k, BitConverter.GetBytes(seq), ref info)) throw new InvalidOperationException("dup");
                    }
                }
                catch (Exception e) { failure = e; }
                finally
                {
                    mem.BatchPostProcess(in info);
                    Interlocked.Increment(ref done);
                }
            }));
        }
        threads.Add(new Thread(() =>
        {
            try
            {
                while (Volatile.Read(ref done) < Writers)
                {
                    MemTableIterator it = mem.NewIterator();
                    byte[]? prev = null;
                    for (it.SeekToFirst(); it.Valid; it.Next())
                    {
                        byte[] cur = it.Key.ToArray();
                        if (prev is not null && InternalKeyComparator.Bytewise.Compare(prev, cur) >= 0) throw new InvalidOperationException("order");
                        prev = cur;
                    }
                }
            }
            catch (Exception e) { failure = e; }
        }));
        foreach (Thread t in threads) t.Start();
        foreach (Thread t in threads) t.Join();
        Assert.IsNull(failure, failure?.ToString());
        Assert.AreEqual((long)Writers * PerWriter, mem.NumEntries);
        Assert.AreEqual(1UL, mem.FirstSequenceNumber);
        // The newest entry for key k0000 is the largest seq written for it.
        ulong maxSeq = 0;
        for (int w = 0; w < Writers; w++)
        {
            for (int i = 0; i < PerWriter; i += 1000) maxSeq = Math.Max(maxSeq, (ulong)(i * Writers + w + 1));
        }
        GetOutcome g = Get(mem, "k0000", DbFormat.MaxSequenceNumber);
        Assert.AreEqual(maxSeq, g.Seq);
    }

    [TestMethod]
    public void RefCounting()
    {
        MemTable mem = NewMem();
        mem.Ref();
        mem.Ref();
        Assert.IsFalse(mem.Unref());
        Assert.IsTrue(mem.Unref());
        mem.Dispose();
        mem.Dispose();
    }
}
