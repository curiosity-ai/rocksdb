using System.Buffers.Binary;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;
using RocksDb.Managed.Tests.Db;
using RocksDb.Managed.Tests.Table;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.Tests.RangeDel;

/// <summary>
/// <see cref="RangeTombstoneFragmenter"/> (FragmentedRangeTombstoneList::FragmentTombstones, db/range_tombstone_fragmenter.cc):
/// differential tests against the previous list-based implementation, reuse of the pooled fragmenter, and the allocation
/// bounds of fragmenting, flushing, compacting and writing range tombstones.
/// </summary>
[TestClass]
public sealed class RangeTombstoneFragmenterTests
{
    private const int TsSize = 8;

    // ------------------------------------------------------------------------------------------------------------
    // Reference: the previous implementation (List + closures), kept verbatim in structure to diff the new one against.

    private sealed record RefFragment(byte[] Start, byte[] End, ulong[] Seqs, byte[][] Timestamps);

    private static List<RefFragment> ReferenceBuild(Comparator ucmp, List<FragmentedRangeTombstoneList.Tombstone> input, bool endIncludesTimestamp)
    {
        var tombstones = new List<FragmentedRangeTombstoneList.Tombstone>(input);
        int tsSize = ucmp.TimestampSize;
        var result = new List<RefFragment>();
        if (tsSize == 0)
        {
            tombstones.RemoveAll(t => ucmp.Compare(t.Start, t.End) >= 0);
            if (tombstones.Count == 0) return result;
            tombstones.Sort((a, b) => ucmp.Compare(a.Start, b.Start));
            var bounds = new List<byte[]>();
            foreach (FragmentedRangeTombstoneList.Tombstone t in tombstones)
            {
                bounds.Add(t.Start);
                bounds.Add(t.End);
            }
            bounds.Sort((a, b) => ucmp.Compare(a, b));
            int w = 1;
            for (int i = 1; i < bounds.Count; i++)
            {
                if (ucmp.Compare(bounds[i], bounds[w - 1]) != 0) bounds[w++] = bounds[i];
            }
            bounds.RemoveRange(w, bounds.Count - w);
            var active = new List<FragmentedRangeTombstoneList.Tombstone>();
            int nextT = 0;
            for (int k = 0; k + 1 < bounds.Count; k++)
            {
                byte[] lo = bounds[k];
                while (nextT < tombstones.Count && ucmp.Compare(tombstones[nextT].Start, lo) <= 0) active.Add(tombstones[nextT++]);
                active.RemoveAll(t => ucmp.Compare(t.End, lo) <= 0);
                if (active.Count == 0) continue;
                var scratch = new List<ulong>();
                foreach (FragmentedRangeTombstoneList.Tombstone t in active) scratch.Add(t.Sequence);
                scratch.Sort();
                var seqs = new List<ulong>();
                ulong prev = ulong.MaxValue;
                for (int i = scratch.Count - 1; i >= 0; i--)
                {
                    if (scratch[i] != prev) seqs.Add(scratch[i]);
                    prev = scratch[i];
                }
                result.Add(new RefFragment(lo, bounds[k + 1], seqs.ToArray(), []));
            }
            return result;
        }

        var items = new List<(byte[] S, byte[] E, ulong Seq, byte[] Ts)>();
        foreach (FragmentedRangeTombstoneList.Tombstone t in tombstones)
        {
            byte[] ts = t.Timestamp ?? t.Start.AsSpan(t.Start.Length - tsSize).ToArray();
            byte[] s = t.Start.AsSpan(0, t.Start.Length - tsSize).ToArray();
            byte[] e = endIncludesTimestamp ? t.End.AsSpan(0, t.End.Length - tsSize).ToArray() : t.End;
            if (ucmp.CompareWithoutTimestamp(s, false, e, false) >= 0) continue;
            items.Add((s, e, t.Sequence, ts));
        }
        if (items.Count == 0) return result;
        items.Sort((a, b) => ucmp.CompareWithoutTimestamp(a.S, false, b.S, false));
        var tb = new List<byte[]>();
        foreach (var t in items)
        {
            tb.Add(t.S);
            tb.Add(t.E);
        }
        tb.Sort((a, b) => ucmp.CompareWithoutTimestamp(a, false, b, false));
        int tw = 1;
        for (int i = 1; i < tb.Count; i++)
        {
            if (ucmp.CompareWithoutTimestamp(tb[i], false, tb[tw - 1], false) != 0) tb[tw++] = tb[i];
        }
        tb.RemoveRange(tw, tb.Count - tw);
        byte[] maxTs = new byte[tsSize];
        maxTs.AsSpan().Fill(0xff);
        var tActive = new List<(byte[] S, byte[] E, ulong Seq, byte[] Ts)>();
        int nt = 0;
        for (int k = 0; k + 1 < tb.Count; k++)
        {
            byte[] lo = tb[k];
            while (nt < items.Count && ucmp.CompareWithoutTimestamp(items[nt].S, false, lo, false) <= 0) tActive.Add(items[nt++]);
            tActive.RemoveAll(t => ucmp.CompareWithoutTimestamp(t.E, false, lo, false) <= 0);
            if (tActive.Count == 0) continue;
            var seqs = new List<ulong>();
            var tss = new List<byte[]>();
            foreach (var t in tActive)
            {
                seqs.Add(t.Seq);
                tss.Add(t.Ts);
            }
            seqs.Sort((a, b) => b.CompareTo(a));
            tss.Sort((a, b) => ucmp.CompareTimestamp(b, a));
            result.Add(new RefFragment([.. lo, .. maxTs], [.. tb[k + 1], .. maxTs], seqs.ToArray(), tss.ToArray()));
        }
        return result;
    }

    private static void AssertSame(List<RefFragment> expected, FragmentedRangeTombstoneList actual)
    {
        Assert.AreEqual(expected.Count, actual.Count, "fragment count");
        int total = 0;
        for (int i = 0; i < expected.Count; i++)
        {
            RefFragment e = expected[i];
            CollectionAssert.AreEqual(e.Start, actual.GetStart(i).ToArray(), "start of fragment " + i);
            CollectionAssert.AreEqual(e.End, actual.GetEnd(i).ToArray(), "end of fragment " + i);
            CollectionAssert.AreEqual(e.Seqs, actual.GetSequences(i).ToArray(), "seqs of fragment " + i);
            for (int k = 0; k < e.Timestamps.Length; k++)
            {
                CollectionAssert.AreEqual(e.Timestamps[k], actual.GetTimestamp(i, k).ToArray(), "ts " + k + " of fragment " + i);
            }
            total += e.Seqs.Length;
        }
        Assert.AreEqual(total, actual.SequenceCount);
    }

    private static byte[] RandomKey(Random rnd, int alphabet, int maxLen)
    {
        byte[] k = new byte[rnd.Next(1, maxLen + 1)];
        for (int i = 0; i < k.Length; i++) k[i] = (byte)('a' + rnd.Next(alphabet));
        return k;
    }

    private static byte[] WithTs(byte[] key, ulong ts)
    {
        byte[] r = new byte[key.Length + TsSize];
        key.CopyTo(r, 0);
        BinaryPrimitives.WriteUInt64LittleEndian(r.AsSpan(key.Length), ts);
        return r;
    }

    private static List<FragmentedRangeTombstoneList.Tombstone> RandomTombstones(Random rnd, int count, bool timestamps, bool endIncludesTs,
        bool explicitTs)
    {
        var list = new List<FragmentedRangeTombstoneList.Tombstone>(count);
        // Small alphabets and short keys give many overlaps, shared boundaries, duplicates and empty ranges.
        int alphabet = rnd.Next(2, 8);
        for (int i = 0; i < count; i++)
        {
            byte[] s = RandomKey(rnd, alphabet, 3), e = RandomKey(rnd, alphabet, 3);
            if (rnd.Next(4) != 0 && s.AsSpan().SequenceCompareTo(e) > 0) (s, e) = (e, s);
            ulong seq = (ulong)rnd.Next(1, Math.Max(2, count / 2)); // duplicates on purpose
            if (!timestamps)
            {
                list.Add(new(s, e, seq));
                continue;
            }
            ulong ts = (ulong)rnd.Next(0, 50);
            byte[] st = WithTs(s, explicitTs ? (ulong)rnd.Next(0, 50) : ts);
            byte[] et = endIncludesTs ? WithTs(e, (ulong)rnd.Next(0, 50)) : e;
            byte[]? tsBytes = null;
            if (explicitTs)
            {
                tsBytes = new byte[TsSize];
                BinaryPrimitives.WriteUInt64LittleEndian(tsBytes, ts);
            }
            list.Add(new(st, et, seq, tsBytes));
        }
        return list;
    }

    [TestMethod]
    public void RandomTombstonesMatchReferenceWithoutTimestamps()
    {
        int seed = Environment.TickCount;
        var rnd = new Random(seed);
        for (int round = 0; round < 300; round++)
        {
            List<FragmentedRangeTombstoneList.Tombstone> input = RandomTombstones(rnd, rnd.Next(0, 60), timestamps: false, endIncludesTs: true, explicitTs: false);
            var copy = new List<FragmentedRangeTombstoneList.Tombstone>(input);
            FragmentedRangeTombstoneList actual = FragmentedRangeTombstoneList.Build(Comparator.Bytewise, input);
            try
            {
                AssertSame(ReferenceBuild(Comparator.Bytewise, copy, true), actual);
                CollectionAssert.AreEqual(copy, input, "the input list is not modified");
            }
            catch (AssertFailedException e)
            {
                throw new AssertFailedException("seed=" + seed + " round=" + round + ": " + e.Message, e);
            }
        }
    }

    [TestMethod]
    [DataRow(true, false)]
    [DataRow(true, true)]
    [DataRow(false, false)]
    public void RandomTombstonesMatchReferenceWithTimestamps(bool endIncludesTs, bool explicitTs)
    {
        int seed = Environment.TickCount;
        var rnd = new Random(seed);
        Comparator ucmp = Comparator.BytewiseWithU64Ts;
        for (int round = 0; round < 300; round++)
        {
            List<FragmentedRangeTombstoneList.Tombstone> input = RandomTombstones(rnd, rnd.Next(0, 60), timestamps: true, endIncludesTs, explicitTs);
            FragmentedRangeTombstoneList actual = FragmentedRangeTombstoneList.Build(ucmp, input, endIncludesTs);
            try
            {
                AssertSame(ReferenceBuild(ucmp, input, endIncludesTs), actual);
            }
            catch (AssertFailedException e)
            {
                throw new AssertFailedException("seed=" + seed + " round=" + round + ": " + e.Message, e);
            }
        }
    }

    [TestMethod]
    public void LookupsMatchBruteForce()
    {
        int seed = Environment.TickCount;
        var rnd = new Random(seed);
        for (int round = 0; round < 100; round++)
        {
            List<FragmentedRangeTombstoneList.Tombstone> input = RandomTombstones(rnd, rnd.Next(1, 40), false, true, false);
            FragmentedRangeTombstoneList f = FragmentedRangeTombstoneList.Build(Comparator.Bytewise, input);
            for (int q = 0; q < 50; q++)
            {
                byte[] key = RandomKey(rnd, 8, 3);
                ulong read = (ulong)rnd.Next(0, 40);
                ulong expected = 0;
                foreach (FragmentedRangeTombstoneList.Tombstone t in input)
                {
                    if (t.Start.AsSpan().SequenceCompareTo(key) <= 0 && key.AsSpan().SequenceCompareTo(t.End) < 0 && t.Sequence <= read)
                        expected = Math.Max(expected, t.Sequence);
                }
                Assert.AreEqual(expected, f.MaxCoveringTombstoneSeqnum(key, read), "seed=" + seed);
            }
        }
    }

    [TestMethod]
    public void FragmenterIsReusableAfterReturnAndInterleaved()
    {
        RangeTombstoneFragmenter a = RangeTombstoneFragmenter.Rent(Comparator.Bytewise, true);
        a.Add("a"u8, "e"u8, 5);
        // A nested build on the same thread (e.g. a table opened while a compaction collects tombstones) gets its own.
        RangeTombstoneFragmenter b = RangeTombstoneFragmenter.Rent(Comparator.Bytewise, true);
        Assert.AreNotSame(a, b);
        b.Add("c"u8, "g"u8, 7);
        FragmentedRangeTombstoneList lb = b.Finish();
        RangeTombstoneFragmenter.Return(b);
        a.Add("c"u8, "d"u8, 9);
        FragmentedRangeTombstoneList la = a.Finish();
        RangeTombstoneFragmenter.Return(a);
        Assert.AreEqual(1, lb.Count);
        Assert.AreEqual(3, la.Count); // a-c:5 c-d:9,5 d-e:5
        CollectionAssert.AreEqual(new ulong[] { 9, 5 }, la.GetSequences(1).ToArray());

        // The cached instance comes back cleared.
        RangeTombstoneFragmenter c = RangeTombstoneFragmenter.Rent(Comparator.Bytewise, true);
        Assert.AreEqual(0, c.Count);
        Assert.IsTrue(c.Finish().IsEmpty);
        RangeTombstoneFragmenter.Return(c);
        Assert.ThrowsExactly<ObjectDisposedException>(() => c.Add("a"u8, "b"u8, 1));
        Assert.ThrowsExactly<InvalidOperationException>(() => RangeTombstoneFragmenter.Return(c));
        // Built lists stay valid after their fragmenter was reused.
        Assert.AreEqual(9UL, la.MaxCoveringTombstoneSeqnum("c"u8, 100));
        Assert.AreEqual(7UL, lb.MaxCoveringTombstoneSeqnum("f"u8, 100));
    }

    [TestMethod]
    public void ConcurrentBuildsAreIndependent()
    {
        var lists = new FragmentedRangeTombstoneList[8];
        Parallel.For(0, lists.Length, i =>
        {
            for (int r = 0; r < 200; r++)
            {
                RangeTombstoneFragmenter f = RangeTombstoneFragmenter.Rent(Comparator.Bytewise, true);
                try
                {
                    for (int k = 0; k < 50; k++)
                    {
                        byte[] s = [(byte)i, (byte)k], e = [(byte)i, (byte)(k + 2)];
                        f.Add(s, e, (ulong)(k + 1));
                    }
                    lists[i] = f.Finish();
                }
                finally
                {
                    RangeTombstoneFragmenter.Return(f);
                }
            }
        });
        for (int i = 0; i < lists.Length; i++)
        {
            Assert.AreEqual(51, lists[i].Count);
            Assert.AreEqual(50UL, lists[i].MaxCoveringTombstoneSeqnum([(byte)i, 50], 1000));
            Assert.AreEqual(0UL, lists[i].MaxCoveringTombstoneSeqnum([(byte)(i + 1), 0], 1000));
        }
    }

    [TestMethod]
    public void TimestampSizeIsValidated()
    {
        RangeTombstoneFragmenter f = RangeTombstoneFragmenter.Rent(Comparator.BytewiseWithU64Ts, true);
        try
        {
            var ex = Assert.ThrowsExactly<RocksDbException>(() => f.Add("abc"u8, "abcdefghij"u8, 1));
            Assert.AreEqual(StatusCode.Corruption, ex.Code);
            ex = Assert.ThrowsExactly<RocksDbException>(() => f.Add(WithTs("a"u8.ToArray(), 1), WithTs("b"u8.ToArray(), 1), 1, "123"u8));
            Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
        }
        finally
        {
            RangeTombstoneFragmenter.Return(f);
        }
    }

    // ------------------------------------------------------------------------------------------------------------
    // Allocation bounds

    /// <summary>Bytes of the arrays and the object of a built list (an upper bound: boundaries shared by fragments count twice).</summary>
    private static long OutputBytes(FragmentedRangeTombstoneList l)
    {
        long keyBytes = 0;
        for (int i = 0; i < l.Count; i++) keyBytes += l.GetStart(i).Length + l.GetEnd(i).Length;
        const int arrayOverhead = 32;
        return 7 * arrayOverhead + 96 + keyBytes + 4L * (2 * l.Count + 1) + 4L * l.Count + 4L * (l.Count + 1)
            + 8L * l.SequenceCount + (long)l.SequenceCount * l.TimestampSize;
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void SteadyStateFragmentingAllocatesOnlyTheResult(bool timestamps)
    {
        Comparator ucmp = timestamps ? Comparator.BytewiseWithU64Ts : Comparator.Bytewise;
        const int n = 5000;
        var starts = new byte[n][];
        var ends = new byte[n][];
        var rnd = new Random(3);
        for (int i = 0; i < n; i++)
        {
            byte[] s = new byte[12], e = new byte[12];
            BinaryPrimitives.WriteUInt64BigEndian(s, (ulong)rnd.Next(1_000_000));
            BinaryPrimitives.WriteUInt64BigEndian(e, BinaryPrimitives.ReadUInt64BigEndian(s) + (ulong)rnd.Next(1, 500));
            starts[i] = timestamps ? WithTs(s, (ulong)i) : s;
            ends[i] = timestamps ? WithTs(e, (ulong)i) : e;
        }
        FragmentedRangeTombstoneList Build()
        {
            RangeTombstoneFragmenter f = RangeTombstoneFragmenter.Rent(ucmp, true);
            try
            {
                for (int i = 0; i < n; i++) f.Add(starts[i], ends[i], (ulong)(i + 1));
                return f.Finish();
            }
            finally
            {
                RangeTombstoneFragmenter.Return(f);
            }
        }
        for (int i = 0; i < 3; i++) Build(); // prime the pools
        long before = GC.GetAllocatedBytesForCurrentThread();
        FragmentedRangeTombstoneList list = Build();
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.IsTrue(list.Count > n);
        long bound = OutputBytes(list);
        Assert.IsTrue(allocated <= bound, "allocated " + allocated + " B for a result of at most " + bound + " B");
    }

    /// <summary>Flush + table open + compaction of many range tombstones: allocations per tombstone stay small.</summary>
    [TestMethod]
    public async Task FlushAndCompactionOfRangeTombstonesAllocateLittlePerTombstone()
    {
        string dir = Path.Combine(Path.GetTempPath(), "rocksdb-rangedel-alloc-" + Guid.NewGuid().ToString("N"));
        try
        {
            var cfo = new ColumnFamilyOptions { DisableAutoCompactions = true };
            await using RocksDatabase db = await RocksDatabase.OpenAsync(dir, new DbOptions { CreateIfMissing = true }, cfo);
            const int perRound = 20_000;
            byte[] begin = new byte[16], end = new byte[16], value = new byte[16];
            long Round(int round, bool rangeDeletes)
            {
                long before = GC.GetTotalAllocatedBytes(precise: true);
                for (int i = 0; i < perRound; i++)
                {
                    ulong k = (ulong)(round * perRound + i) * 20;
                    BinaryPrimitives.WriteUInt64BigEndian(begin, k);
                    BinaryPrimitives.WriteUInt64BigEndian(end, k + 10);
                    db.Put(begin, value);
                    if (rangeDeletes) db.DeleteRange(begin, end);
                    else db.Delete(end); // same number of writes and WAL records
                }
                db.Flush();
                db.CompactRange();
                return GC.GetTotalAllocatedBytes(precise: true) - before;
            }
            int r = 0;
            for (int i = 0; i < 2; i++)
            {
                Round(r++, true);
                Round(r++, false);
            }
            long withRangeDels = Round(r++, true);
            long without = Round(r++, false);
            // What range deletions add per tombstone: the memtable's fragmented list, flush (fragmenting, the range-del
            // block, the reader's cached list) and the compaction (collecting, re-fragmenting, writing, the output's reader).
            // That is the fragmented lists themselves (~50 B per tombstone each) and block reads: measured 150-450 B.
            double perTombstone = (double)(withRangeDels - without) / perRound;
            Assert.IsTrue(perTombstone < 1000, perTombstone.ToString("F1", System.Globalization.CultureInfo.InvariantCulture) + " B per tombstone");
            // The range-deleted keys stay deleted, the others are there.
            BinaryPrimitives.WriteUInt64BigEndian(begin, (ulong)(4 * perRound) * 20);
            Assert.IsNull(db.Get(begin));
            BinaryPrimitives.WriteUInt64BigEndian(begin, (ulong)(5 * perRound) * 20);
            Assert.IsNotNull(db.Get(begin));
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, true);
        }
    }

    /// <summary>
    /// Flush writes only the newest tombstone of each snapshot stripe of a fragment (BuildTable writes
    /// CompactionRangeDelAggregator::NewIterator(), fragmented for_compaction with the snapshots); reads at every
    /// snapshot keep their result.
    /// </summary>
    [TestMethod]
    public void FlushKeepsNewestTombstonePerSnapshotStripe()
    {
        MemoryFileSystem fs = DbTestUtil.NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, DbTestUtil.NewOptions(fs), new ColumnFamilyOptions { DisableAutoCompactions = true });
        db.Put(DbTestUtil.B("b"), DbTestUtil.B("v1"));
        db.DeleteRange(DbTestUtil.B("a"), DbTestUtil.B("z")); // seq 2
        db.DeleteRange(DbTestUtil.B("a"), DbTestUtil.B("z")); // seq 3: same stripe as 2
        using Snapshot snap = db.GetSnapshot();                // sees up to 3
        db.Put(DbTestUtil.B("b"), DbTestUtil.B("v2"));         // seq 4
        db.DeleteRange(DbTestUtil.B("a"), DbTestUtil.B("z")); // seq 5
        db.DeleteRange(DbTestUtil.B("a"), DbTestUtil.B("z")); // seq 6: same stripe as 5
        db.Flush();
        TableProperties p = db.GetPropertiesOfAllTables().Values.Single();
        // One fragment [a, z) covered by 6, 5, 3, 2: kept 6 (latest stripe) and 3 (the snapshot's stripe).
        Assert.AreEqual(2UL, p.NumRangeDeletions);
        Assert.IsNull(db.Get(DbTestUtil.B("b")));
        Assert.IsNull(db.Get(new ReadOptions { Snapshot = snap }, DbTestUtil.B("b")));
    }

    /// <summary>A range-del block larger than the builder's staging threshold is written directly; sync and async agree.</summary>
    [TestMethod]
    public async Task LargeRangeDelBlockIsWrittenDirectlyAndReadsBack()
    {
        const int n = 30_000; // ~ 30 bytes per entry: well over the 256 KiB staging threshold
        var tombstones = new List<(byte[] Key, byte[] Value)>(n + 1);
        tombstones.Add((TableTestUtil.IKey("a", 1), [1, 2, 3]));
        for (int i = 0; i < n; i++)
        {
            byte[] s = new byte[10], e = new byte[10];
            BinaryPrimitives.WriteUInt64BigEndian(s.AsSpan(2), (ulong)i * 4);
            BinaryPrimitives.WriteUInt64BigEndian(e.AsSpan(2), (ulong)i * 4 + 2);
            tombstones.Add((TableTestUtil.IKey(s, (ulong)(i + 10), EntryType.RangeDeletion), e));
        }
        foreach (ChecksumType checksum in new[] { ChecksumType.Crc32c, ChecksumType.Xxh3 })
        {
            var options = new TableFormatOptions { Checksum = checksum };
            // (The two files differ only in their random base context checksum.)
            foreach (byte[] table in new[] { Build(tombstones, options), await BuildAsync(tombstones, options) })
            {
                VerifyLargeRangeDelTable(table, n);
            }
        }
        // Steady state: building such a table no longer grows the staging buffer past the pool (several MB per table).
        var opts = new TableFormatOptions();
        for (int i = 0; i < 3; i++) Build(tombstones, opts);
        long before = GC.GetAllocatedBytesForCurrentThread();
        byte[] data = Build(tombstones, opts);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        // The MemWritableFile's MemoryStream and ToArray copy account for ~2-3x the file size; the builder must not add
        // another multiple of the block size.
        Assert.IsTrue(allocated < 4L * data.Length + (256 << 10), "allocated " + allocated + " B for a " + data.Length + " B table");
    }

    private static void VerifyLargeRangeDelTable(byte[] data, int n)
    {
        // Block checksums (incl. the context checksum of the directly written block) are verified on read.
        using BlockBasedTableReader reader = TableTestUtil.Open(data);
        Assert.AreEqual((ulong)n, reader.Properties.NumRangeDeletions);
        FragmentedRangeTombstoneList? f = RangeDelTableTombstones.Get(reader);
        Assert.IsNotNull(f);
        Assert.AreEqual(n, f.Count);
        byte[] probe = new byte[10];
        BinaryPrimitives.WriteUInt64BigEndian(probe.AsSpan(2), 4 * 777 + 1);
        Assert.AreEqual(787UL, f.MaxCoveringTombstoneSeqnum(probe, 10_000));
        using BlockBasedTableIterator it = reader.NewIterator();
        it.SeekToFirst();
        Assert.IsTrue(it.Valid);
        Assert.AreEqual("a", TableTestUtil.UserKeyOf(it.Key));
        it.Next();
        Assert.IsFalse(it.Valid);
    }

    private static byte[] Build(List<(byte[] Key, byte[] Value)> entries, TableFormatOptions options)
    {
        var file = new MemWritableFile();
        using (var b = new BlockBasedTableBuilder(file, options, new TableBuilderOptions()))
        {
            foreach ((byte[] k, byte[] v) in entries) b.Add(k, v);
            b.Finish();
            Assert.AreEqual((ulong)file.Size, b.FileSize);
        }
        return file.ToArray();
    }

    private static async Task<byte[]> BuildAsync(List<(byte[] Key, byte[] Value)> entries, TableFormatOptions options)
    {
        var file = new MemWritableFile();
        using (var b = new BlockBasedTableBuilder(file, options, new TableBuilderOptions()))
        {
            foreach ((byte[] k, byte[] v) in entries) b.Add(k, v);
            await b.FinishAsync();
            Assert.AreEqual((ulong)file.Size, b.FileSize);
        }
        return file.ToArray();
    }
}
