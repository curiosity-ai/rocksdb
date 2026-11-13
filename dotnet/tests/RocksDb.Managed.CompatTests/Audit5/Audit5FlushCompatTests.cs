using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.CompatTests.Wal;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.CompatTests.Audit5;

/// <summary>
/// Audit #5: what a memtable flush writes, against native RocksDB (FlushJob::WriteLevel0Table and BuildTable, whose
/// CompactionIterator runs the merge helper, the single-delete and range-tombstone rules over the memtables). The same
/// operations run on both engines (one sequence number per operation, so the sequence numbers agree), auto compaction is
/// off, and every level-0 file is then read entry by entry with the managed table reader: user key, sequence number,
/// type and value of each point entry, the range tombstones and the entry counters of the table properties.
/// </summary>
[TestClass]
public sealed class Audit5FlushCompatTests
{
    private enum Op { Put, Delete, SingleDelete, Merge, DeleteRange, Snapshot, Flush, Reopen }

    private readonly record struct Step(Op Op, string Key = "", string Value = "");

    private static Step Put(string k, string v) => new(Op.Put, k, v);
    private static Step Del(string k) => new(Op.Delete, k);
    private static Step SingleDel(string k) => new(Op.SingleDelete, k);
    private static Step Merge(string k, string v) => new(Op.Merge, k, v);
    private static Step DelRange(string a, string b) => new(Op.DeleteRange, a, b);
    private static readonly Step Snap = new(Op.Snapshot);
    private static readonly Step Flush = new(Op.Flush);
    /// <summary>Closes and reopens the DB: the WAL is replayed and its memtable flushed (WriteLevel0TableForRecovery).</summary>
    private static readonly Step Reopen = new(Op.Reopen);

    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);

    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    /// <summary>What one engine wrote: the entries of each level-0 file (oldest file first) and the flush tickers.</summary>
    private sealed class Outcome
    {
        public List<string> Files { get; } = new();
        public ulong PayloadBytes { get; set; }
        public ulong GarbageBytes { get; set; }

        public string Describe() => Files.Count + " file(s)\n" + string.Join("\n", Files) + "\npayload=" + PayloadBytes + " garbage=" + GarbageBytes;
    }

    /// <summary>Every entry of an SST file as text, plus the entry counters of its table properties.</summary>
    private static string Dump(string path)
    {
        var sb = new StringBuilder();
        using var reader = BlockBasedTableReader.Open(LocalFileSystem.Instance.OpenRandomAccess(path), TableReaderOptions.Default);
        using (BlockBasedTableIterator it = reader.NewIterator())
        {
            for (it.SeekToFirst(); it.Valid; it.Next())
            {
                DbFormat.UnpackSequenceAndType(DbFormat.ExtractFooter(it.Key), out ulong seq, out EntryType t);
                sb.Append(Encoding.UTF8.GetString(DbFormat.ExtractUserKey(it.Key))).Append('@').Append(seq).Append(':').Append(t)
                    .Append('=').Append(Encoding.UTF8.GetString(it.Value)).Append("; ");
            }
        }
        using (BlockIter? rd = reader.NewRangeTombstoneIterator())
        {
            if (rd is not null)
            {
                for (rd.SeekToFirst(); rd.Valid; rd.Next())
                {
                    DbFormat.UnpackSequenceAndType(DbFormat.ExtractFooter(rd.Key), out ulong seq, out _);
                    sb.Append('[').Append(Encoding.UTF8.GetString(DbFormat.ExtractUserKey(rd.Key))).Append(',')
                        .Append(Encoding.UTF8.GetString(rd.Value)).Append(")@").Append(seq).Append("; ");
                }
            }
        }
        TableProperties p = reader.Properties;
        sb.Append("| entries=").Append(p.NumEntries).Append(" deletions=").Append(p.NumDeletions).Append(" merges=").Append(p.NumMergeOperands)
            .Append(" rangedels=").Append(p.NumRangeDeletions).Append(" rawkey=").Append(p.RawKeySize).Append(" rawvalue=").Append(p.RawValueSize);
        return sb.ToString();
    }

    private static ulong NumberOf(string name) => ulong.Parse(Path.GetFileNameWithoutExtension(name.TrimStart('/')), CultureInfo.InvariantCulture);

    private static ulong NativeTicker(string statistics, string name)
    {
        Match m = Regex.Match(statistics, "^" + Regex.Escape(name) + " COUNT : (\\d+)$", RegexOptions.Multiline);
        return m.Success ? ulong.Parse(m.Groups[1].Value, CultureInfo.InvariantCulture) : 0;
    }

    private static unsafe Outcome RunNative(IReadOnlyList<Step> steps, bool mergeOperator)
    {
        using var dir = new TempDir("a5fl-n");
        RocksDbSharp.DbOptions o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetDisableAutoCompactions(1).EnableStatistics();
        o.SetCompression(RocksDbSharp.Compression.No);
        if (mergeOperator)
        {
            RocksDbSharp.MergeOperator mo = NativeDbHelper.CreateConcatMergeOperator();
            s_keepAlive.Add(mo);
            o.SetMergeOperator(mo);
        }
        s_keepAlive.Add(o);
        var outcome = new Outcome();
        var snapshots = new List<RocksDbSharp.Snapshot>();
        RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(o, dir.Path);
        {
            try
            {
                foreach (Step s in steps)
                {
                    byte[] k = U(s.Key), v = U(s.Value);
                    using var batch = new RocksDbSharp.WriteBatch();
                    switch (s.Op)
                    {
                        case Op.Put: batch.Put(k, v); break;
                        case Op.Delete: batch.Delete(k); break;
                        case Op.SingleDelete:
                            fixed (byte* p = k) RocksDbSharp.Native.Instance.rocksdb_writebatch_singledelete(batch.Handle, p, (UIntPtr)k.Length);
                            break;
                        case Op.Merge: batch.Merge(k, (ulong)k.Length, v, (ulong)v.Length); break;
                        case Op.DeleteRange: batch.DeleteRange(k, (ulong)k.Length, v, (ulong)v.Length); break;
                        case Op.Snapshot: snapshots.Add(db.CreateSnapshot()); continue;
                        case Op.Flush: db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true)); continue;
                        case Op.Reopen:
                            db.Dispose();
                            db = RocksDbSharp.RocksDb.Open(o, dir.Path);
                            continue;
                    }
                    db.Write(batch);
                }
                var files = new List<RocksDbSharp.LiveFileMetadata>(db.GetLiveFilesMetadata());
                files.Sort(static (a, b) => NumberOf(a.FileMetadata.FileName).CompareTo(NumberOf(b.FileMetadata.FileName)));
                foreach (RocksDbSharp.LiveFileMetadata f in files) outcome.Files.Add(Dump(Path.Combine(dir.Path, f.FileMetadata.FileName.TrimStart('/'))));
                string statistics = o.GetStatisticsString();
                outcome.PayloadBytes = NativeTicker(statistics, "rocksdb.memtable.payload.bytes.at.flush");
                outcome.GarbageBytes = NativeTicker(statistics, "rocksdb.memtable.garbage.bytes.at.flush");
            }
            finally
            {
                foreach (RocksDbSharp.Snapshot s in snapshots) s.Dispose();
                db.Dispose();
            }
        }
        return outcome;
    }

    private static Outcome RunManaged(IReadOnlyList<Step> steps, bool mergeOperator)
    {
        using var dir = new TempDir("a5fl-m");
        var stats = new Statistics();
        var cfo = new ColumnFamilyOptions { DisableAutoCompactions = true, Compression = RocksDb.Managed.Compression.CompressionType.None };
        if (mergeOperator) cfo.MergeOperator = new StringAppendOperator();
        var outcome = new Outcome();
        var snapshots = new List<Snapshot>();
        var dbo = new DbOptions { CreateIfMissing = true, Statistics = stats };
        RocksDatabase db = RocksDatabase.Open(dir.Path, dbo, cfo);
        {
            try
            {
                foreach (Step s in steps)
                {
                    byte[] k = U(s.Key), v = U(s.Value);
                    switch (s.Op)
                    {
                        case Op.Put: db.Put(k, v); break;
                        case Op.Delete: db.Delete(k); break;
                        case Op.SingleDelete: db.SingleDelete(k); break;
                        case Op.Merge: db.Merge(k, v); break;
                        case Op.DeleteRange: db.DeleteRange(k, v); break;
                        case Op.Snapshot: snapshots.Add(db.GetSnapshot()); break;
                        case Op.Flush: db.Flush(); break;
                        case Op.Reopen:
                            db.Dispose();
                            db = RocksDatabase.Open(dir.Path, dbo, cfo);
                            break;
                    }
                }
                List<LiveFileMetadata> files = db.GetLiveFilesMetadata();
                files.Sort(static (a, b) => NumberOf(a.Name).CompareTo(NumberOf(b.Name)));
                foreach (LiveFileMetadata f in files) outcome.Files.Add(Dump(Path.Combine(dir.Path, f.Name.TrimStart('/'))));
                outcome.PayloadBytes = stats.GetTickerCount(Ticker.MemtablePayloadBytesAtFlush);
                outcome.GarbageBytes = stats.GetTickerCount(Ticker.MemtableGarbageBytesAtFlush);
            }
            finally
            {
                foreach (Snapshot s in snapshots) s.Dispose();
                db.Dispose();
            }
        }
        return outcome;
    }

    /// <summary>Runs <paramref name="steps"/> on both engines and requires identical level-0 files and flush tickers.</summary>
    private static void AssertSameFlushOutput(bool mergeOperator, params Step[] steps)
    {
        Outcome native = RunNative(steps, mergeOperator);
        Outcome managed = RunManaged(steps, mergeOperator);
        Assert.AreEqual(native.Describe(), managed.Describe());
    }

    /// <summary>Default configuration: overwrites, deletes and snapshot stripes (upstream rule (A) only).</summary>
    [TestMethod]
    public void PutsAndDeletesMatchNative() => AssertSameFlushOutput(false,
        Put("a", "1"), Put("a", "2"), Put("b", "1"), Del("b"), Put("c", "1"), Snap, Put("c", "2"), Del("d"), Put("d", "3"), Snap,
        Put("d", "4"), Del("a"), Put("e", "5"), Flush,
        Put("a", "6"), Del("e"), Flush);

    /// <summary>Merge operands of one memtable: MergeHelper::MergeUntil collapses them at flush.</summary>
    [TestMethod]
    public void MergeOperandsAreCollapsedLikeNative() => AssertSameFlushOutput(true,
        // operands only (no base: partial merge into one operand)
        Merge("a", "1"), Merge("a", "2"), Merge("a", "3"),
        // Put base
        Put("b", "0"), Merge("b", "1"), Merge("b", "2"),
        // Delete base
        Put("c", "0"), Del("c"), Merge("c", "1"), Merge("c", "2"),
        // a single operand
        Merge("d", "1"),
        // a Put newer than the operands hides them
        Merge("e", "1"), Merge("e", "2"), Put("e", "3"),
        Flush);

    /// <summary>A snapshot between merge operands stops the merge (the operands of each stripe are kept apart).</summary>
    [TestMethod]
    public void MergeStopsAtSnapshotsLikeNative() => AssertSameFlushOutput(true,
        Merge("a", "1"), Merge("a", "2"), Snap, Merge("a", "3"), Merge("a", "4"),
        Put("b", "0"), Merge("b", "1"), Snap, Merge("b", "2"), Merge("b", "3"),
        Put("c", "0"), Snap, Merge("c", "1"), Snap, Merge("c", "2"),
        Del("d"), Snap, Merge("d", "1"), Merge("d", "2"),
        Flush);

    /// <summary>Merge operands and range tombstones of the same memtable.</summary>
    [TestMethod]
    public void MergeWithRangeTombstonesMatchesNative() => AssertSameFlushOutput(true,
        Put("a", "0"), Merge("a", "1"), DelRange("a", "b"), Merge("a", "2"), Merge("a", "3"),
        Merge("c", "1"), Merge("c", "2"), DelRange("c", "d"),
        Merge("e", "1"), DelRange("e", "f"), Snap, Merge("e", "2"),
        Flush);

    /// <summary>Point entries covered by a range tombstone of the same flush (CompactionRangeDelAggregator::ShouldDelete).</summary>
    [TestMethod]
    public void RangeTombstoneCoveredEntriesMatchNative() => AssertSameFlushOutput(false,
        Put("a", "1"), Put("b", "1"), Put("c", "1"), DelRange("a", "c"), Put("b", "2"),
        Put("d", "1"), Snap, DelRange("d", "e"), Del("f"), DelRange("f", "g"),
        Flush);

    /// <summary>SingleDelete next to the Put it removes (the SingleDelete branch of CompactionIterator::NextFromInput).</summary>
    [TestMethod]
    public void SingleDeletePairsMatchNative() => AssertSameFlushOutput(false,
        Put("a", "1"), SingleDel("a"),
        SingleDel("b"),
        Put("c", "1"), Snap, SingleDel("c"),
        Put("d", "1"), SingleDel("d"), Put("d", "2"),
        Put("e", "1"), Put("e", "2"), SingleDel("e"),
        Flush);

    /// <summary>Several flushes: every level-0 file is what native writes.</summary>
    [TestMethod]
    public void MergeAcrossFlushesMatchesNative() => AssertSameFlushOutput(true,
        Put("a", "0"), Merge("a", "1"), Flush,
        Merge("a", "2"), Merge("a", "3"), Flush,
        Del("a"), Merge("a", "4"), Merge("b", "1"), Flush);

    /// <summary>The recovery flush (WriteLevel0TableForRecovery) goes through the same BuildTable.</summary>
    [TestMethod]
    public void RecoveryFlushMatchesNative() => AssertSameFlushOutput(true,
        Put("a", "0"), Merge("a", "1"), Merge("a", "2"), Merge("b", "1"), Merge("b", "2"), Put("c", "1"), SingleDel("c"),
        Put("d", "1"), DelRange("d", "e"), Put("e", "1"), Put("e", "2"), Reopen,
        Merge("a", "3"), Merge("a", "4"), Reopen);

    /// <summary>
    /// min_write_buffer_number_to_merge = 3: full memtables are sealed (automatic switches) but not flushed while fewer
    /// than three wait; a manual flush then writes all of them, with overwritten versions deduplicated, into one file.
    /// Each engine stops writing once two memtables are sealed, so the stop points (and sequence numbers) may differ;
    /// the shape is compared.
    /// </summary>
    [TestMethod]
    public void SealedMemtablesWaitForMinWriteBufferNumberToMergeLikeNative()
    {
        const int Keys = 8;
        var value = new byte[4096];
        static byte[] KeyOf(int i) => U("key" + (i % Keys).ToString(CultureInfo.InvariantCulture));

        string NativeShape()
        {
            using var dir = new TempDir("a5mm-n");
            RocksDbSharp.DbOptions o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetDisableAutoCompactions(1)
                .SetWriteBufferSize(64 * 1024).SetMaxWriteBufferNumber(5);
            RocksDbSharp.Native.Instance.rocksdb_options_set_min_write_buffer_number_to_merge(o.Handle, 3);
            s_keepAlive.Add(o);
            using RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(o, dir.Path);
            int i = 0;
            while (db.GetProperty("rocksdb.num-immutable-mem-table") != "2" && i < 10000) db.Put(KeyOf(i++), value);
            string before = db.GetProperty("rocksdb.num-immutable-mem-table") + "/" + db.GetProperty("rocksdb.num-files-at-level0");
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            var files = new List<RocksDbSharp.LiveFileMetadata>(db.GetLiveFilesMetadata());
            string entries = files.Count == 1 ? EntriesOf(Path.Combine(dir.Path, files[0].FileMetadata.FileName.TrimStart('/'))) : "-";
            return before + " -> " + db.GetProperty("rocksdb.num-immutable-mem-table") + "/" + files.Count + " files, " + entries + " entries";
        }

        string ManagedShape()
        {
            using var dir = new TempDir("a5mm-m");
            var cfo = new ColumnFamilyOptions { DisableAutoCompactions = true, WriteBufferSize = 64 * 1024, MaxWriteBufferNumber = 5, MinWriteBufferNumberToMerge = 3 };
            using RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions { CreateIfMissing = true }, cfo);
            int i = 0;
            while (db.GetProperty("rocksdb.num-immutable-mem-table") != "2" && i < 10000) db.Put(KeyOf(i++), value);
            string before = db.GetProperty("rocksdb.num-immutable-mem-table") + "/" + db.GetProperty("rocksdb.num-files-at-level0");
            db.Flush();
            List<LiveFileMetadata> files = db.GetLiveFilesMetadata();
            string entries = files.Count == 1 ? EntriesOf(Path.Combine(dir.Path, files[0].Name.TrimStart('/'))) : "-";
            return before + " -> " + db.GetProperty("rocksdb.num-immutable-mem-table") + "/" + files.Count + " files, " + entries + " entries";
        }

        static string EntriesOf(string path)
        {
            using var reader = BlockBasedTableReader.Open(LocalFileSystem.Instance.OpenRandomAccess(path), TableReaderOptions.Default);
            return reader.Properties.NumEntries.ToString(CultureInfo.InvariantCulture);
        }

        string native = NativeShape();
        Assert.AreEqual("2/0 -> 0/1 files, 8 entries", native);
        Assert.AreEqual(native, ManagedShape());
    }

    /// <summary>
    /// Random operations over a few keys (long version chains), with snapshots and flushes at random points. Keys that
    /// get a SingleDelete get no Merge and no Delete (mixing them is outside the SingleDelete contract).
    /// </summary>
    [TestMethod]
    [DataRow(1)]
    [DataRow(2)]
    [DataRow(3)]
    [DataRow(4)]
    public void RandomOperationsMatchNative(int seed)
    {
        var rnd = new Random(seed);
        var steps = new List<Step>();
        for (int i = 0; i < 600; i++)
        {
            int k = rnd.Next(12);
            string key = "k" + k.ToString("D2", CultureInfo.InvariantCulture);
            string value = "v" + i.ToString(CultureInfo.InvariantCulture);
            bool singleDeleteKey = k >= 9;
            int r = rnd.Next(100);
            if (r < 30) steps.Add(Put(key, value));
            else if (r < 60) steps.Add(singleDeleteKey ? Put(key, value) : Merge(key, value));
            else if (r < 72) steps.Add(singleDeleteKey ? SingleDel(key) : Del(key));
            else if (r < 78)
            {
                int a = rnd.Next(9), b = rnd.Next(9);
                if (a == b) b++;
                steps.Add(DelRange("k" + Math.Min(a, b).ToString("D2", CultureInfo.InvariantCulture), "k" + Math.Max(a, b).ToString("D2", CultureInfo.InvariantCulture)));
            }
            else if (r < 86) steps.Add(Snap);
            else if (r < 90) steps.Add(Flush);
            else steps.Add(Put(key, value));
        }
        steps.Add(Flush);
        AssertSameFlushOutput(true, steps.ToArray());
    }
}
