using System.Globalization;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Db;
using RocksDb.Managed.Db.Iterators;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.CompatTests.Iterators;

/// <summary><see cref="ITableCache"/> over the SST files of a DB directory (readers kept open).</summary>
internal sealed class DirectoryTableCache(string dir) : ITableCache, IDisposable
{
    private readonly Dictionary<ulong, BlockBasedTableReader> _readers = new();

    private BlockBasedTableReader Open(FileMetaData file)
    {
        lock (_readers)
        {
            if (!_readers.TryGetValue(file.FileNumber, out BlockBasedTableReader? r))
            {
                string path = FileNames.MakeTableFileName(dir, file.FileNumber);
                r = BlockBasedTableReader.Open(LocalFileSystem.Instance.OpenRandomAccess(path), new TableReaderOptions { LargestSeqno = file.LargestSeqno });
                _readers[file.FileNumber] = r;
            }
            return r;
        }
    }

    public ValueTask<TableReaderLease> GetReaderAsync(FileMetaData file, CancellationToken cancellationToken = default) => new(GetReader(file));
    public TableReaderLease GetReader(FileMetaData file) => new(Open(file), null, null);
    public InternalIterator NewIterator(FileMetaData file) => new TableInternalIterator(GetReader(file));
    public void Evict(ulong fileNumber) { }

    public void Dispose()
    {
        foreach (BlockBasedTableReader r in _readers.Values) r.Dispose();
        _readers.Clear();
    }
}

/// <summary>
/// Native RocksDB writes a DB with data spread over several levels (CompactRange/CompactFiles), with deletes, single
/// deletes, range deletes, merges (UInt64Add / StringAppend) and snapshots; the managed iterator stack
/// (<see cref="DbIteratorFactory"/> over the MANIFEST's files) must return exactly what native iterators return,
/// forward, reverse, with bounds and at every snapshot.
/// </summary>
[TestClass]
public sealed class IteratorCompatTests
{
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    private static RocksDbSharp.MergeOperator NativeStringAppend() => RocksDbSharp.MergeOperators.Create(
        "StringAppendOperator",
        static (ReadOnlySpan<byte> key, RocksDbSharp.MergeOperators.OperandsEnumerator operands, out bool success) =>
        {
            success = true;
            return Join(default, false, operands);
        },
        static (ReadOnlySpan<byte> key, bool hasExisting, ReadOnlySpan<byte> existing, RocksDbSharp.MergeOperators.OperandsEnumerator operands, out bool success) =>
        {
            success = true;
            return Join(existing, hasExisting, operands);
        });

    /// <summary>Exactly upstream StringAppendOperator(',') folded over the operands.</summary>
    private static byte[] Join(ReadOnlySpan<byte> existing, bool hasExisting, RocksDbSharp.MergeOperators.OperandsEnumerator operands)
    {
        var result = new List<byte>();
        bool any = hasExisting;
        if (hasExisting) result.AddRange(existing.ToArray());
        for (int i = 0; i < operands.Count; i++)
        {
            if (any) result.Add((byte)',');
            result.AddRange(operands.Get(i).ToArray());
            any = true;
        }
        return result.ToArray();
    }

    private static string Key(int i) => "key" + i.ToString("D5", CultureInfo.InvariantCulture);
    private static byte[] B(string s) => Encoding.UTF8.GetBytes(s);

    private static byte[] U64(ulong v)
    {
        byte[] b = new byte[8];
        Coding.EncodeFixed64(b, v);
        return b;
    }

    public enum Kind { UInt64Add, StringAppend }

    /// <summary>One iteration step of the scripted comparison.</summary>
    private readonly record struct Step(int Op, byte[] Target);

    private interface IIter
    {
        bool Valid { get; }
        string Current { get; }
        void SeekToFirst();
        void SeekToLast();
        void Seek(byte[] k);
        void SeekForPrev(byte[] k);
        void Next();
        void Prev();
    }

    private sealed class NativeIter(RocksDbSharp.Iterator it) : IIter
    {
        public bool Valid => it.Valid();
        public string Current => Convert.ToHexString(it.GetKeySpan()) + "=" + Convert.ToHexString(it.GetValueSpan());
        public void SeekToFirst() => it.SeekToFirst();
        public void SeekToLast() => it.SeekToLast();
        public void Seek(byte[] k) => it.Seek(k);
        public void SeekForPrev(byte[] k) => it.SeekForPrev(k);
        public void Next() => it.Next();
        public void Prev() => it.Prev();
    }

    private sealed class ManagedIter(Iterator it) : IIter
    {
        public bool Valid => it.Valid;
        public string Current => Convert.ToHexString(it.Key) + "=" + Convert.ToHexString(it.Value);
        public void SeekToFirst() => it.SeekToFirst();
        public void SeekToLast() => it.SeekToLast();
        public void Seek(byte[] k) => it.Seek(k);
        public void SeekForPrev(byte[] k) => it.SeekForPrev(k);
        public void Next() => it.Next();
        public void Prev() => it.Prev();
    }

    /// <summary>Runs a full forward scan, a full reverse scan and the random steps; returns the observed sequence.</summary>
    private static List<string> Run(IIter it, List<Step> steps)
    {
        var o = new List<string>();
        for (it.SeekToFirst(); it.Valid; it.Next()) o.Add(it.Current);
        o.Add("|");
        for (it.SeekToLast(); it.Valid; it.Prev()) o.Add(it.Current);
        o.Add("|");
        foreach (Step s in steps)
        {
            switch (s.Op)
            {
                case 0: it.Seek(s.Target); break;
                case 1: it.SeekForPrev(s.Target); break;
                case 2: if (it.Valid) it.Next(); break;
                case 3: if (it.Valid) it.Prev(); break;
            }
            o.Add(it.Valid ? it.Current : "<invalid>");
        }
        return o;
    }

    private sealed record ReadConfig(int SnapshotIndex, string? Lower, string? Upper);

    [TestMethod]
    [DataRow(Kind.UInt64Add)]
    [DataRow(Kind.StringAppend)]
    public void ManagedIteratorMatchesNativeAcrossLevels(Kind kind)
    {
        int seed = Environment.TickCount ^ (int)kind;
        string ctx = "seed=" + seed.ToString(CultureInfo.InvariantCulture) + " kind=" + kind;
        var rnd = new Random(seed);
        using var dir = new TempDir("iter");
        const int keySpace = 400;

        // ---- native: write phases, pushing data to different levels, take snapshots in between.
        var configs = new List<ReadConfig>();
        var expected = new List<List<string>>();
        var scripts = new List<List<Step>>();
        var snapshotSeqs = new List<ulong>();

        var options = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetDisableAutoCompactions(1).SetNumLevels(7);
        RocksDbSharp.Native.Instance.rocksdb_options_set_level_compaction_dynamic_level_bytes(options.Handle, 0);
        if (kind == Kind.UInt64Add) options.SetUint64addMergeOperator();
        else
        {
            RocksDbSharp.MergeOperator mo = NativeStringAppend();
            s_keepAlive.Add(mo);
            options.SetMergeOperator(mo);
        }
        s_keepAlive.Add(options);
        MergeOperator managedOp = kind == Kind.UInt64Add ? UInt64AddOperator.Instance : new StringAppendOperator();

        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(options, dir.Path))
        {
            var snapshots = new List<RocksDbSharp.Snapshot>();
            int sdNext = 0;
            for (int phase = 0; phase < 5; phase++)
            {
                for (int i = 0; i < 600; i++)
                {
                    using var batch = new RocksDbSharp.WriteBatch();
                    int r = rnd.Next(100);
                    byte[] k = B(Key(rnd.Next(keySpace)));
                    byte[] v = kind == Kind.UInt64Add
                        ? (rnd.Next(20) == 0 ? B("bad") : U64((ulong)rnd.Next(1000)))
                        : B("v" + phase + "_" + i);
                    if (r < 45) batch.Put(k, v);
                    else if (r < 72) batch.Merge(k, (ulong)k.Length, v, (ulong)v.Length);
                    else if (r < 88) batch.Delete(k);
                    else if (r < 94)
                    {
                        // SingleDelete is only well defined on a key written exactly once.
                        byte[] sk = B("sd" + (sdNext++).ToString("D5", CultureInfo.InvariantCulture));
                        db.Put(sk, v);
                        if (rnd.Next(2) == 0)
                        {
                            unsafe
                            {
                                fixed (byte* p = sk) RocksDbSharp.Native.Instance.rocksdb_writebatch_singledelete(batch.Handle, p, (UIntPtr)sk.Length);
                            }
                        }
                    }
                    else
                    {
                        int s = rnd.Next(keySpace);
                        byte[] start = B(Key(s)), end = B(Key(Math.Min(keySpace, s + 1 + rnd.Next(12))));
                        batch.DeleteRange(start, (ulong)start.Length, end, (ulong)end.Length);
                    }
                    db.Write(batch);
                }
                db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
                // Phase 0 -> L5 (CompactRange + change_level), phases 1/2 -> L3/L1 (CompactFiles of the new L0 file),
                // phases 3/4 stay in L0.
                if (phase == 0) CompactRangeToLevel(db, 5);
                else if (phase is 1 or 2) CompactL0ToLevel(db, phase == 1 ? 3 : 1);
                if (phase < 4)
                {
                    RocksDbSharp.Snapshot snap = db.CreateSnapshot();
                    snapshots.Add(snap);
                    snapshotSeqs.Add(RocksDbSharp.Native.Instance.rocksdb_snapshot_get_sequence_number(snap.Handle));
                }
            }
            snapshotSeqs.Add(RocksDbSharp.Native.Instance.rocksdb_get_latest_sequence_number(db.Handle));

            var levels = new HashSet<int>();
            foreach (RocksDbSharp.LiveFileMetadata f in db.GetLiveFilesMetadata(true)) levels.Add(f.FileMetadata.FileLevel);
            Assert.IsTrue(levels.Count >= 3, ctx + ": expected files in several levels, got " + string.Join(",", levels));

            for (int si = 0; si < snapshotSeqs.Count; si++)
            {
                foreach ((string? lo, string? hi) in new[] { ((string?)null, (string?)null), (Key(rnd.Next(keySpace / 2)), (string?)null), (null, Key(keySpace / 2 + rnd.Next(keySpace / 2))), (Key(100), Key(300)) })
                {
                    var steps = new List<Step>();
                    for (int i = 0; i < 60; i++)
                    {
                        int op = rnd.Next(4);
                        string t = rnd.Next(5) == 0 ? "sd" + rnd.Next(sdNext + 1).ToString("D5", CultureInfo.InvariantCulture) : Key(rnd.Next(keySpace + 2));
                        steps.Add(new Step(op, B(t + (rnd.Next(4) == 0 ? "!" : ""))));
                    }
                    var ro = new RocksDbSharp.ReadOptions();
                    if (si < snapshots.Count) ro.SetSnapshot(snapshots[si]);
                    byte[]? lb = lo is null ? null : B(lo), ub = hi is null ? null : B(hi);
                    if (lb is not null) ro.SetIterateLowerBound(lb);
                    if (ub is not null) ro.SetIterateUpperBound(ub);
                    using (RocksDbSharp.Iterator nit = db.NewIterator(null, ro))
                    {
                        expected.Add(Run(new NativeIter(nit), steps));
                    }
                    GC.KeepAlive(lb);
                    GC.KeepAlive(ub);
                    GC.KeepAlive(ro);
                    configs.Add(new ReadConfig(si, lo, hi));
                    scripts.Add(steps);
                }
            }
            foreach (RocksDbSharp.Snapshot s in snapshots) s.Dispose();
        }

        // ---- managed: open the files listed in the MANIFEST and iterate with the same scripts.
        RecoveredVersionState state = ManifestReader.RecoverFromDirectory(LocalFileSystem.Instance, dir.Path, null, out _);
        ColumnFamilyVersion cf = state.ColumnFamilies[0];
        int nonEmptyLevels = 0;
        foreach (IReadOnlyList<FileMetaData> l in cf.Levels) if (l.Count > 0) nonEmptyLevels++;
        Assert.IsTrue(nonEmptyLevels >= 3, ctx);
        using var tableCache = new DirectoryTableCache(dir.Path);
        for (int c = 0; c < configs.Count; c++)
        {
            ReadConfig rc = configs[c];
            var ro = new ReadOptions();
            if (rc.Lower is not null) ro.IterateLowerBound = B(rc.Lower);
            if (rc.Upper is not null) ro.IterateUpperBound = B(rc.Upper);
            using Iterator it = DbIteratorFactory.Create(new DbIteratorSources
            {
                Comparator = InternalKeyComparator.Bytewise,
                ReadOptions = ro,
                Sequence = snapshotSeqs[rc.SnapshotIndex],
                MergeOperator = managedOp,
                Levels = cf.Levels,
                TableCache = tableCache,
            });
            List<string> got = Run(new ManagedIter(it), scripts[c]);
            string where = ctx + " snapshot#" + rc.SnapshotIndex + " lower=" + rc.Lower + " upper=" + rc.Upper;
            Assert.AreEqual(expected[c].Count, got.Count, where);
            for (int i = 0; i < got.Count; i++) Assert.AreEqual(expected[c][i], got[i], where + " at output " + i);
        }
    }

    private static unsafe void CompactRangeToLevel(RocksDbSharp.RocksDb db, int level)
    {
        RocksDbSharp.Native native = RocksDbSharp.Native.Instance;
        IntPtr opts = native.rocksdb_compactoptions_create();
        try
        {
            native.rocksdb_compactoptions_set_change_level(opts, 1);
            native.rocksdb_compactoptions_set_target_level(opts, level);
            native.rocksdb_compact_range_opt(db.Handle, opts, (byte*)null, UIntPtr.Zero, (byte*)null, UIntPtr.Zero);
        }
        finally
        {
            native.rocksdb_compactoptions_destroy(opts);
        }
    }

    private static void CompactL0ToLevel(RocksDbSharp.RocksDb db, int level)
    {
        var l0 = new List<string>();
        foreach (RocksDbSharp.LiveFileMetadata f in db.GetLiveFilesMetadata(true))
        {
            if (f.FileMetadata.FileLevel == 0) l0.Add(f.FileMetadata.FileName);
        }
        if (l0.Count == 0) return;
        RocksDbSharp.Native.Instance.rocksdb_compact_files(db.Handle, IntPtr.Zero, l0.ToArray(), (UIntPtr)l0.Count, level, 0, IntPtr.Zero, IntPtr.Zero, IntPtr.Zero);
    }
}
