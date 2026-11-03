using System.Buffers;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>DB-level compaction tests on <see cref="MemoryFileSystem"/> (in the spirit of db/db_compaction_test.cc).</summary>
[TestClass]
public sealed class DbCompactionTests
{
    private static ColumnFamilyOptions SmallCf() => new()
    {
        WriteBufferSize = 64 * 1024,
        ArenaBlockSize = 16 * 1024,
        TargetFileSizeBase = 64 * 1024,
        MaxBytesForLevelBase = 256 * 1024,
        Level0FileNumCompactionTrigger = 4,
        Compression = RocksDb.Managed.Compression.CompressionType.None,
        MaxWriteBufferNumber = 3,
    };

    private static int[] FilesPerLevel(RocksDatabase db, ColumnFamily? cf = null)
    {
        cf ??= db.DefaultColumnFamily;
        var r = new int[7];
        for (int l = 0; l < 7; l++) r[l] = int.Parse(cf.GetProperty("rocksdb.num-files-at-level" + l)!, System.Globalization.CultureInfo.InvariantCulture);
        return r;
    }

    private static byte[] Val(int i, int round) => B("value-" + i + "-" + round + new string('x', 40 + i % 50));

    /// <summary>Asserts that levels 1+ are sorted and non-overlapping (via the live file metadata).</summary>
    private static void AssertLsmShapeValid(RocksDatabase db)
    {
        var byLevel = new Dictionary<(string, int), List<LiveFileMetadata>>();
        foreach (LiveFileMetadata f in db.GetLiveFilesMetadata())
        {
            if (f.Level == 0) continue;
            if (!byLevel.TryGetValue((f.ColumnFamilyName, f.Level), out List<LiveFileMetadata>? l)) byLevel[(f.ColumnFamilyName, f.Level)] = l = new();
            l.Add(f);
        }
        foreach (List<LiveFileMetadata> files in byLevel.Values)
        {
            files.Sort((a, b) => a.SmallestKey.AsSpan().SequenceCompareTo(b.SmallestKey));
            for (int i = 1; i < files.Count; i++)
            {
                Assert.IsTrue(files[i - 1].LargestKey.AsSpan().SequenceCompareTo(files[i].SmallestKey) <= 0,
                    "overlapping files at level " + files[i].Level);
            }
        }
    }

    [TestMethod]
    public void AutoCompactionKeepsL0BoundedAndDataCorrect()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), SmallCf());
        var rnd = new Random(301);
        var model = new Dictionary<int, int>();
        for (int i = 0; i < 20000; i++)
        {
            int k = rnd.Next(5000);
            db.Put(Key(k), Val(k, i));
            model[k] = i;
        }
        db.WaitForCompact();
        Console.WriteLine(db.GetProperty("rocksdb.stats"));
        int[] files = FilesPerLevel(db);
        Assert.IsTrue(files[0] < 4 + 2, "L0 files: " + files[0]);
        int below = 0;
        for (int l = 1; l < 7; l++) below += files[l];
        Assert.IsTrue(below > 0, "expected compacted files below L0: " + string.Join(",", files));
        foreach ((int k, int round) in model) CollectionAssert.AreEqual(Val(k, round), db.Get(Key(k)), "key " + k);
        AssertLsmShapeValid(db);
    }

    // ---------------------------------------------------------------------------------------------------------
    // randomized model check

    /// <summary>Expected state: key -> value (null = deleted/absent).</summary>
    private sealed class Model
    {
        public SortedDictionary<string, string?> Data = new(StringComparer.Ordinal);

        public Model Clone() => new() { Data = new SortedDictionary<string, string?>(Data, StringComparer.Ordinal) };

        public void Merge(string k, string v) => Data[k] = Data.TryGetValue(k, out string? old) && old is not null ? old + "," + v : v;

        public void DeleteRange(string b, string e)
        {
            foreach (string k in new List<string>(Data.Keys))
            {
                if (string.CompareOrdinal(k, b) >= 0 && string.CompareOrdinal(k, e) < 0) Data[k] = null;
            }
        }
    }

    private static void Verify(RocksDatabase db, Model m, Snapshot? snap, string context)
    {
        ReadOptions? ro = snap is null ? null : new ReadOptions { Snapshot = snap };
        foreach ((string k, string? v) in m.Data)
        {
            Assert.AreEqual(v, S(db.Get(ro, B(k))), context + " get " + k);
        }
        // Full scan must equal the live part of the model.
        var expected = new List<string>();
        foreach ((string k, string? v) in m.Data)
        {
            if (v is not null) expected.Add(k + "=" + v);
        }
        var actual = new List<string>();
        using (Iterator it = db.NewIterator(ro))
        {
            for (it.SeekToFirst(); it.Valid; it.Next()) actual.Add(S(it.Key.ToArray()) + "=" + S(it.Value.ToArray()));
        }
        CollectionAssert.AreEqual(expected, actual, context + " scan");
    }

    private const ulong DefaultReadahead = 2 * 1024 * 1024;

    /// <summary>
    /// Compaction inputs read through a tiny readahead buffer (smaller than two data blocks: blocks straddle the
    /// prefetched chunks constantly) and without readahead give the same DB as the model.
    /// </summary>
    [TestMethod]
    public void RandomizedModelCompactionReadahead()
    {
        RunRandomizedModel(Environment.TickCount & 0x7fffffff, withMerge: true, CompactionStyle.Level, 5000);
        RunRandomizedModel(4242, withMerge: true, CompactionStyle.Level, 0);
    }

    private static void RunRandomizedModel(int seed, bool withMerge, CompactionStyle style, ulong compactionReadahead)
    {
        SCOPED(seed);
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cfo = SmallCf();
        cfo.WriteBufferSize = 32 * 1024;
        cfo.CompactionStyle = style;
        if (withMerge) cfo.MergeOperator = new StringAppendOperator();
        var dbo = NewOptions(fs);
        dbo.MaxBackgroundJobs = 4;
        dbo.CompactionReadaheadSize = compactionReadahead;
        RocksDatabase db = RocksDatabase.Open(DbPath, dbo, cfo);
        try
        {
            var rnd = new Random(seed);
            var model = new Model();
            var snaps = new List<(Snapshot Snap, Model State)>();
            var sdKeys = new Dictionary<string, bool>(); // single-delete key space: true = currently put once
            const int keySpace = 600;
            for (int op = 0; op < 12000; op++)
            {
                int r = rnd.Next(100);
                string k = "k" + rnd.Next(keySpace).ToString("D4", System.Globalization.CultureInfo.InvariantCulture);
                string v = "v" + op + new string((char)('a' + op % 26), rnd.Next(60));
                if (r < 45)
                {
                    db.Put(B(k), B(v));
                    model.Data[k] = v;
                }
                else if (r < 55)
                {
                    db.Delete(B(k));
                    model.Data[k] = null;
                }
                else if (r < 68 && withMerge)
                {
                    db.Merge(B(k), B("m" + op));
                    model.Merge(k, "m" + op);
                }
                else if (r < 70)
                {
                    int a = rnd.Next(keySpace), len = rnd.Next(1, 30);
                    string b = "k" + a.ToString("D4", System.Globalization.CultureInfo.InvariantCulture);
                    string e = "k" + (a + len).ToString("D4", System.Globalization.CultureInfo.InvariantCulture);
                    db.DeleteRange(B(b), B(e));
                    model.DeleteRange(b, e);
                }
                else if (r < 82)
                {
                    // Single delete key space ("s..."): put once, then single delete, never overwrite.
                    string sk = "s" + rnd.Next(200).ToString("D4", System.Globalization.CultureInfo.InvariantCulture);
                    if (sdKeys.TryGetValue(sk, out bool present) && present)
                    {
                        db.SingleDelete(B(sk));
                        model.Data[sk] = null;
                        sdKeys[sk] = false;
                    }
                    else
                    {
                        db.Put(B(sk), B(v));
                        model.Data[sk] = v;
                        sdKeys[sk] = true;
                    }
                }
                else if (r < 83)
                {
                    if (snaps.Count < 4) snaps.Add((db.GetSnapshot(), model.Clone()));
                    else
                    {
                        int i = rnd.Next(snaps.Count);
                        Verify(db, snaps[i].State, snaps[i].Snap, "seed " + seed + " op " + op + " snapshot");
                        snaps[i].Snap.Dispose();
                        snaps.RemoveAt(i);
                    }
                }
                else if (r < 84 && rnd.Next(10) == 0)
                {
                    if (rnd.Next(2) == 0) db.CompactRange();
                    else
                    {
                        int a = rnd.Next(keySpace);
                        db.CompactRange(B("k" + a.ToString("D4", System.Globalization.CultureInfo.InvariantCulture)),
                            B("k" + (a + 100).ToString("D4", System.Globalization.CultureInfo.InvariantCulture)));
                    }
                }
                else
                {
                    db.Put(B(k), B(v));
                    model.Data[k] = v;
                }
                if (op % 3000 == 2999)
                {
                    Verify(db, model, null, "seed " + seed + " op " + op);
                    foreach ((Snapshot s, Model state) in snaps) Verify(db, state, s, "seed " + seed + " op " + op + " snapshot");
                }
            }
            db.WaitForCompact();
            Verify(db, model, null, "seed " + seed + " end");
            foreach ((Snapshot s, Model state) in snaps) Verify(db, state, s, "seed " + seed + " end snapshot");
            AssertLsmShapeValid(db);
            db.CompactRange(new CompactRangeOptions { BottommostLevelCompaction = BottommostLevelCompaction.Force }, null, null);
            Verify(db, model, null, "seed " + seed + " after full compaction");
            foreach ((Snapshot s, Model state) in snaps) Verify(db, state, s, "seed " + seed + " after full compaction snapshot");
            foreach ((Snapshot s, _) in snaps) s.Dispose();
            db.Dispose();
            db = RocksDatabase.Open(DbPath, dbo, cfo);
            Verify(db, model, null, "seed " + seed + " reopen");
            AssertLsmShapeValid(db);
        }
        finally
        {
            db.Dispose();
        }
    }

    private static void SCOPED(int seed) => Console.WriteLine("seed=" + seed);

    [TestMethod]
    public void RandomizedModelLeveled()
    {
        int seed = Environment.TickCount & 0x7fffffff;
        RunRandomizedModel(seed, withMerge: true, CompactionStyle.Level, DefaultReadahead);
        RunRandomizedModel(12345, withMerge: true, CompactionStyle.Level, DefaultReadahead);
    }

    [TestMethod]
    public void RandomizedModelLeveledNoMergeOperator() => RunRandomizedModel(777, withMerge: false, CompactionStyle.Level, DefaultReadahead);

    [TestMethod]
    public void RandomizedModelUniversal()
    {
        RunRandomizedModel(Environment.TickCount & 0x7fffffff, withMerge: true, CompactionStyle.Universal, DefaultReadahead);
        RunRandomizedModel(99, withMerge: true, CompactionStyle.Universal, DefaultReadahead);
    }

    [TestMethod]
    public void UniversalCompactionBoundsSortedRuns()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cfo = SmallCf();
        cfo.CompactionStyle = CompactionStyle.Universal;
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        var rnd = new Random(5);
        var model = new Dictionary<int, int>();
        for (int i = 0; i < 20000; i++)
        {
            int k = rnd.Next(4000);
            db.Put(Key(k), Val(k, i));
            model[k] = i;
        }
        db.WaitForCompact();
        int[] files = FilesPerLevel(db);
        int runs = files[0];
        for (int l = 1; l < 7; l++) runs += files[l] > 0 ? 1 : 0;
        Assert.IsTrue(runs <= cfo.Level0FileNumCompactionTrigger, "sorted runs: " + string.Join(",", files));
        foreach ((int k, int round) in model) CollectionAssert.AreEqual(Val(k, round), db.Get(Key(k)));
        db.CompactRange();
        files = FilesPerLevel(db);
        for (int l = 0; l < 6; l++) Assert.AreEqual(0, files[l], string.Join(",", files));
        foreach ((int k, int round) in model) CollectionAssert.AreEqual(Val(k, round), db.Get(Key(k)));
    }

    [TestMethod]
    public void FifoCompactionDeletesOldestFiles()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cfo = SmallCf();
        cfo.CompactionStyle = CompactionStyle.Fifo;
        cfo.CompactionOptionsFifo = new CompactionOptionsFifo { MaxTableFilesSize = 200 * 1024 };
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        for (int round = 0; round < 20; round++)
        {
            for (int i = 0; i < 300; i++) db.Put(Key(round * 1000 + i), Val(i, round));
            db.Flush();
            db.WaitForCompact();
        }
        ulong total = ulong.Parse(db.GetProperty("rocksdb.total-sst-files-size")!, System.Globalization.CultureInfo.InvariantCulture);
        Assert.IsTrue(total <= 200 * 1024, "total " + total);
        // The newest data survives, the oldest was dropped.
        for (int i = 0; i < 300; i++) CollectionAssert.AreEqual(Val(i, 19), db.Get(Key(19 * 1000 + i)));
        Assert.IsNull(db.Get(Key(0)));
        foreach (LiveFileMetadata f in db.GetLiveFilesMetadata()) Assert.AreEqual(0, f.Level);
    }

    [TestMethod]
    public void FifoIntraL0CompactionReducesFileCount()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cfo = SmallCf();
        cfo.CompactionStyle = CompactionStyle.Fifo;
        cfo.CompactionOptionsFifo = new CompactionOptionsFifo { MaxTableFilesSize = 64UL << 20, AllowCompaction = true };
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        for (int round = 0; round < 12; round++)
        {
            for (int i = 0; i < 20; i++) db.Put(Key(round * 1000 + i), Val(i, round));
            db.Flush();
            db.WaitForCompact();
        }
        Assert.IsTrue(FilesPerLevel(db)[0] < 12, "L0 files " + FilesPerLevel(db)[0]);
        for (int round = 0; round < 12; round++)
        {
            for (int i = 0; i < 20; i++) CollectionAssert.AreEqual(Val(i, round), db.Get(Key(round * 1000 + i)));
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // deterministic cases

    [TestMethod]
    public void CompactRangeMovesEverythingToBottommostAndZeroesSeqnos()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cfo = SmallCf();
        cfo.DisableAutoCompactions = true;
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        for (int round = 0; round < 3; round++)
        {
            for (int i = 0; i < 300; i++) db.Put(Key(i), Val(i, round));
            db.Delete(Key(5));
            db.Flush();
        }
        Assert.AreEqual(3, FilesPerLevel(db)[0]);
        db.CompactRange();
        int[] files = FilesPerLevel(db);
        Assert.AreEqual(0, files[0]);
        Assert.IsTrue(files[6] > 0, string.Join(",", files));
        foreach (LiveFileMetadata f in db.GetLiveFilesMetadata())
        {
            Assert.AreEqual(6, f.Level);
            Assert.AreEqual(0UL, f.LargestSeqno, "bottommost seqnos are zeroed without snapshots");
        }
        Assert.IsNull(db.Get(Key(5)));
        for (int i = 0; i < 300; i++)
        {
            if (i != 5) CollectionAssert.AreEqual(Val(i, 2), db.Get(Key(i)));
        }
        // The deleted key's tombstone is gone at the bottommost level: the number of entries is exact.
        Assert.AreEqual("299", db.GetProperty("rocksdb.estimate-num-keys"));
    }

    [TestMethod]
    public void SnapshotPreservesOldVersionsAcrossCompaction()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), SmallCf());
        for (int i = 0; i < 200; i++) db.Put(Key(i), Val(i, 0));
        using Snapshot snap = db.GetSnapshot();
        for (int i = 0; i < 200; i++)
        {
            if (i % 2 == 0) db.Put(Key(i), Val(i, 1));
            else db.Delete(Key(i));
        }
        db.DeleteRange(Key(50), Key(60));
        db.CompactRange();
        for (int i = 0; i < 200; i++)
        {
            CollectionAssert.AreEqual(Val(i, 0), db.Get(new ReadOptions { Snapshot = snap }, Key(i)), "snapshot key " + i);
            byte[]? expected = i % 2 == 0 && (i < 50 || i >= 60) ? Val(i, 1) : null;
            CollectionAssert.AreEqual(expected, db.Get(Key(i)), "key " + i);
        }
    }

    private sealed class DropOddFilter : CompactionFilter
    {
        public int Calls;
        public override string Name => "DropOddFilter";

        public override CompactionFilterDecision Filter(int level, ReadOnlySpan<byte> key, CompactionFilterValueType valueType, ReadOnlySpan<byte> existingValue,
            IBufferWriter<byte> newValue, IBufferWriter<byte> skipUntil)
        {
            Interlocked.Increment(ref Calls);
            int n = int.Parse(S(key.ToArray())!.AsSpan(3), System.Globalization.CultureInfo.InvariantCulture);
            if (n % 2 == 1) return CompactionFilterDecision.Remove;
            if (n % 10 == 0)
            {
                newValue.Write("changed"u8);
                return CompactionFilterDecision.ChangeValue;
            }
            if (n == 100)
            {
                return CompactionFilterDecision.Keep;
            }
            return CompactionFilterDecision.Keep;
        }
    }

    [TestMethod]
    public void CompactionFilterRemovesAndChangesValues()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cfo = SmallCf();
        var filter = new DropOddFilter();
        cfo.CompactionFilter = filter;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo))
        {
            for (int i = 0; i < 500; i++) db.Put(Key(i), Val(i, 0));
            db.CompactRange();
            Assert.IsTrue(filter.Calls >= 500);
            for (int i = 0; i < 500; i++)
            {
                byte[]? expected = i % 2 == 1 ? null : i % 10 == 0 ? B("changed") : Val(i, 0);
                CollectionAssert.AreEqual(expected, db.Get(Key(i)), "key " + i);
            }
        }
        // The filter name is recorded in the OPTIONS file.
        bool found = false;
        foreach (string child in fs.GetChildren(DbPath))
        {
            if (child.StartsWith("OPTIONS-", StringComparison.Ordinal))
            {
                using var r = new StreamReader(new MemoryStream(ReadAll(fs, DbPath + "/" + child)));
                if (r.ReadToEnd().Contains("compaction_filter=DropOddFilter", StringComparison.Ordinal)) found = true;
            }
        }
        Assert.IsTrue(found);
    }

    private static byte[] ReadAll(MemoryFileSystem fs, string path)
    {
        using IRandomAccessFile f = fs.OpenRandomAccess(path);
        var buf = new byte[f.Length];
        f.Read(0, buf);
        return buf;
    }

    private sealed class SkipFilterFactory : CompactionFilterFactory
    {
        public int Created;
        public override string Name => "SkipFilterFactory";

        public override CompactionFilter? CreateCompactionFilter(in CompactionFilterContext context)
        {
            Interlocked.Increment(ref Created);
            Assert.IsTrue(context.IsManualCompaction);
            return new SkipFilter();
        }

        private sealed class SkipFilter : CompactionFilter
        {
            public override string Name => "SkipFilter";

            public override CompactionFilterDecision Filter(int level, ReadOnlySpan<byte> key, CompactionFilterValueType valueType,
                ReadOnlySpan<byte> existingValue, IBufferWriter<byte> newValue, IBufferWriter<byte> skipUntil)
            {
                if (key.SequenceEqual(Key(100)))
                {
                    skipUntil.Write(Key(200));
                    return CompactionFilterDecision.RemoveAndSkipUntil;
                }
                return CompactionFilterDecision.Keep;
            }
        }
    }

    [TestMethod]
    public void CompactionFilterFactoryRemoveAndSkipUntil()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cfo = SmallCf();
        cfo.DisableAutoCompactions = true;
        var factory = new SkipFilterFactory();
        cfo.CompactionFilterFactory = factory;
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        for (int i = 0; i < 300; i++) db.Put(Key(i), Val(i, 0));
        db.CompactRange();
        Assert.IsTrue(factory.Created > 0);
        for (int i = 0; i < 300; i++)
        {
            CollectionAssert.AreEqual(i is >= 100 and < 200 ? null : Val(i, 0), db.Get(Key(i)), "key " + i);
        }
    }

    [TestMethod]
    public async Task CompactRangeAsyncChangeLevelAndColumnFamilies()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cfo = SmallCf();
        cfo.DisableAutoCompactions = true;
        await using RocksDatabase db = await RocksDatabase.OpenAsync(DbPath, NewOptions(fs), cfo);
        ColumnFamily cf = await db.CreateColumnFamilyAsync("other", cfo);
        for (int i = 0; i < 400; i++)
        {
            await cf.PutAsync(Key(i), Val(i, 1));
            await db.PutAsync(Key(i), Val(i, 2));
        }
        await cf.CompactRangeAsync();
        Assert.IsTrue(FilesPerLevel(db, cf)[6] > 0);
        // Move the bottommost level up to L1.
        await cf.CompactRangeAsync(new CompactRangeOptions { ChangeLevel = true, TargetLevel = 1 }, null, null);
        int[] files = FilesPerLevel(db, cf);
        Assert.AreEqual(0, files[6], string.Join(",", files));
        Assert.IsTrue(files[1] > 0, string.Join(",", files));
        // Default CF untouched by the other CF's compactions.
        Assert.IsTrue(FilesPerLevel(db)[0] <= 1);
        for (int i = 0; i < 400; i++)
        {
            CollectionAssert.AreEqual(Val(i, 1), await cf.GetAsync(Key(i)));
            CollectionAssert.AreEqual(Val(i, 2), await db.GetAsync(Key(i)));
        }
    }

    [TestMethod]
    public void TrivialMoveOfNonOverlappingFiles()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cfo = SmallCf();
        cfo.Level0FileNumCompactionTrigger = 2;
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        // Sequential, disjoint L0 files: L0 -> Lbase moves are trivial.
        for (int round = 0; round < 6; round++)
        {
            for (int i = 0; i < 100; i++) db.Put(Key(round * 1000 + i), Val(i, round));
            db.Flush();
            db.WaitForCompact();
        }
        string stats = db.GetProperty("rocksdb.stats")!;
        StringAssert.Contains(stats, "trivial moves");
        Assert.IsFalse(stats.Contains("(trivial moves 0)", StringComparison.Ordinal), stats);
        for (int round = 0; round < 6; round++)
        {
            for (int i = 0; i < 100; i++) CollectionAssert.AreEqual(Val(i, round), db.Get(Key(round * 1000 + i)));
        }
        AssertLsmShapeValid(db);
    }

    [TestMethod]
    public void WriteStallStopTriggersAndRecovers()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cfo = SmallCf();
        cfo.Level0FileNumCompactionTrigger = 2;
        cfo.Level0SlowdownWritesTrigger = 3;
        cfo.Level0StopWritesTrigger = 4;
        cfo.DisableAutoCompactions = true;
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        for (int round = 0; round < 4; round++)
        {
            db.Put(Key(round), Val(round, round));
            db.Flush();
        }
        // disable_auto_compactions: L0 triggers don't stall.
        Assert.AreEqual("0", db.GetProperty("rocksdb.is-write-stopped"));
        db.Put(Key(10), Val(10, 0));
        db.Dispose();

        cfo.DisableAutoCompactions = false;
        // Keep compactions from finishing until released (upstream tests use a sleeping background task). The L0
        // triggers must stay ordered: out-of-order values are sanitized at open like upstream's SanitizeCfOptions.
        using var gate = new ManualResetEventSlim(false);
        TestHooks.Set("DBImpl::RunCompaction:Start", () => gate.Wait());
        try
        {
            using RocksDatabase db2 = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
            // 5 L0 files >= stop trigger (4): writes stop.
            Assert.AreEqual("1", db2.GetProperty("rocksdb.is-write-stopped"));
            var ex = Assert.ThrowsExactly<RocksDbException>(() => db2.Put(new WriteOptions { NoSlowdown = true }, Key(11), Val(11, 0)));
            Assert.AreEqual(StatusCode.Incomplete, ex.Code);
            // A blocked writer resumes once a compaction reduces L0.
            Task writer = Task.Run(() => db2.Put(Key(12), Val(12, 0)));
            Assert.IsFalse(writer.Wait(200));
            gate.Set();
            db2.CompactRange();
            Assert.IsTrue(writer.Wait(10000));
            Assert.AreEqual("0", db2.GetProperty("rocksdb.is-write-stopped"));
            CollectionAssert.AreEqual(Val(12, 0), db2.Get(Key(12)));
            for (int round = 0; round < 4; round++) CollectionAssert.AreEqual(Val(round, round), db2.Get(Key(round)));
        }
        finally
        {
            gate.Set();
            TestHooks.ClearAll();
        }
    }

    [TestMethod]
    public void OutOfOrderL0TriggersAreSanitizedAtOpen()
    {
        // SanitizeCfOptions: level0_stop >= level0_slowdown >= level0_file_num_compaction_trigger, and the OPTIONS
        // file records the sanitized values.
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cfo = SmallCf();
        cfo.Level0FileNumCompactionTrigger = 100;
        cfo.Level0SlowdownWritesTrigger = 3;
        cfo.Level0StopWritesTrigger = 4;
        cfo.MaxWriteBufferNumber = 1;
        cfo.WriteBufferSize = 1024;
        cfo.NumLevels = 1;
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        Assert.AreEqual(3, cfo.Level0SlowdownWritesTrigger, "caller's options are not modified");
        ColumnFamilyOptions effective = db.GetColumnFamily("default").Data.Options;
        Assert.AreEqual(100, effective.Level0SlowdownWritesTrigger);
        Assert.AreEqual(100, effective.Level0StopWritesTrigger);
        Assert.AreEqual(2, effective.MaxWriteBufferNumber);
        Assert.AreEqual(64UL << 10, effective.WriteBufferSize);
        Assert.AreEqual(2, effective.NumLevels);
    }

    [TestMethod]
    public void WriteSlowdownDelaysWrites()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cfo = SmallCf();
        WriteL0Files(fs, cfo, 3);
        cfo.DisableAutoCompactions = false;
        cfo.Level0FileNumCompactionTrigger = 3;
        cfo.Level0SlowdownWritesTrigger = 3;
        cfo.Level0StopWritesTrigger = 100;
        DbOptions dbo = NewOptions(fs);
        dbo.DelayedWriteRate = 100 * 1024; // 100 KB/s
        // Keep the triggered compaction from finishing until released.
        using var gate = new ManualResetEventSlim(false);
        TestHooks.Set("DBImpl::RunCompaction:Start", () => gate.Wait());
        try
        {
            using RocksDatabase db2 = RocksDatabase.Open(DbPath, dbo, cfo);
            Assert.AreEqual("0", db2.GetProperty("rocksdb.is-write-stopped"));
            // The first delay starts at delayed_write_rate: installing the same version again (open, enabling
            // compactions) does not recalculate the stall, which would count as a further slowdown (x 0.8 each).
            Assert.AreEqual("102400", db2.GetProperty("rocksdb.actual-delayed-write-rate"));
            // The clock starts before the first delayed write: the WriteController refills credit for the time that
            // passes between writes (a descheduled test thread gets that time back as credit), so only the span from
            // its first refill has a scheduling-independent lower bound: (bytes requested - 1 ms of credit) / rate.
            var sw = System.Diagnostics.Stopwatch.StartNew();
            var ex = Assert.ThrowsExactly<RocksDbException>(() =>
            {
                for (int i = 0; i < 100; i++) db2.Put(new WriteOptions { NoSlowdown = true }, Key(100 + i), new byte[4096]);
            });
            Assert.AreEqual(StatusCode.Incomplete, ex.Code);
            for (int i = 0; i < 10; i++) db2.Put(Key(200 + i), new byte[4096]);
            // 11 delayed writes of 4 KiB (the rejected one is charged too) at <= 100 KB/s: at least ~440 ms.
            Assert.IsGreaterThanOrEqualTo(400, sw.ElapsedMilliseconds, "elapsed " + sw.ElapsedMilliseconds);
            gate.Set();
            db2.CompactRange();
            // CompactRange returns once L0 (including the file of its own flush) was compacted, waiting for a running
            // L0 compaction if there is one. The compaction that emptied L0 installed its super version, and with it
            // the released stall (RecalculateWriteStallConditions: no delay token without L0 pressure), in the
            // critical section that installed its version, so the stall is already released here.
            Assert.AreEqual("0", db2.GetProperty("rocksdb.num-files-at-level0"), "L0 after CompactRange");
            Assert.AreEqual("0", db2.GetProperty("rocksdb.is-write-stopped"));
            Assert.AreEqual("0", db2.GetProperty("rocksdb.actual-delayed-write-rate"), "delay token still held with an empty L0");
        }
        finally
        {
            gate.Set();
            TestHooks.ClearAll();
        }
    }

    [TestMethod]
    public void CompactionInstallsVersionAndWriteStallTogether()
    {
        // Upstream installs a compaction's version and recalculates the write stall (InstallSuperVersionAndScheduleWork)
        // without releasing the DB mutex in between. Another thread (here: the test, at the hook right after the
        // MANIFEST write) must never see the new version (L0 below the slowdown trigger) with the old stall state
        // (writes still delayed): a manual compaction waiting for that L0 compaction could return in between, and the
        // caller would then read a delayed write rate with an empty L0.
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cfo = SmallCf();
        WriteL0Files(fs, cfo, 3);
        cfo.DisableAutoCompactions = false;
        cfo.Level0FileNumCompactionTrigger = 3;
        cfo.Level0SlowdownWritesTrigger = 3;
        cfo.Level0StopWritesTrigger = 100;
        DbOptions dbo = NewOptions(fs);
        dbo.DelayedWriteRate = 100 * 1024;
        using var gate = new ManualResetEventSlim(false);
        RocksDatabase? db = null;
        int observedL0 = -1;
        string? observedRate = null;
        TestHooks.Set("DBImpl::RunCompaction:Start", () => gate.Wait());
        TestHooks.Set("DBImpl::RunCompaction:AfterLogAndApply", () =>
        {
            if (observedRate is not null) return;
            DbImpl impl = db!.DefaultColumnFamily.Db;
            lock (impl.MutexForTests)
            {
                observedL0 = db.DefaultColumnFamily.Data.Current.Storage.NumLevelFiles(0);
                observedRate = db.GetProperty("rocksdb.actual-delayed-write-rate");
            }
        });
        try
        {
            db = RocksDatabase.Open(DbPath, dbo, cfo);
            Assert.AreNotEqual("0", db.GetProperty("rocksdb.actual-delayed-write-rate"));
            gate.Set();
            db.WaitForCompact();
            Assert.IsNotNull(observedRate, "no compaction was installed");
            Assert.IsLessThan(3, observedL0, "L0 after the compaction");
            Assert.AreEqual("0", observedRate, "write stall of the previous version seen with the new version installed");
            Assert.AreEqual("0", db.GetProperty("rocksdb.actual-delayed-write-rate"));
        }
        finally
        {
            gate.Set();
            TestHooks.ClearAll();
            db?.Dispose();
        }
    }

    /// <summary>Creates a DB with <paramref name="count"/> L0 files (one key each, auto compactions disabled).</summary>
    private static void WriteL0Files(MemoryFileSystem fs, ColumnFamilyOptions cfo, int count)
    {
        cfo.DisableAutoCompactions = true;
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        for (int round = 0; round < count; round++)
        {
            db.Put(Key(round), Val(round, round));
            db.Flush();
        }
    }

    [TestMethod]
    public void CompactionProperties()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cfo = SmallCf();
        cfo.DisableAutoCompactions = true;
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        Assert.AreEqual("0", db.GetProperty("rocksdb.num-running-compactions"));
        Assert.AreEqual("0", db.GetProperty("rocksdb.compaction-pending"));
        for (int round = 0; round < 5; round++)
        {
            for (int i = 0; i < 100; i++) db.Put(Key(i), Val(i, round));
            db.Flush();
        }
        // HandleCompactionPending reports the picker's NeedsCompaction also when auto compactions are disabled.
        Assert.AreEqual("1", db.GetProperty("rocksdb.compaction-pending"));
        Assert.AreNotEqual("0", db.GetProperty("rocksdb.estimate-pending-compaction-bytes"));
        StringAssert.Contains(db.GetProperty("rocksdb.levelstats"), "Level Files Size(MB)");
        db.CompactRange();
        Assert.AreEqual("0", db.GetProperty("rocksdb.estimate-pending-compaction-bytes"));
    }

    [TestMethod]
    public void ManyCompactionsUnderConcurrentWritersAndClose()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cfo = SmallCf();
        DbOptions dbo = NewOptions(fs);
        dbo.MaxBackgroundJobs = 4;
        var expected = new byte[4][];
        using (RocksDatabase db = RocksDatabase.Open(DbPath, dbo, cfo))
        {
            Parallel.For(0, 4, t =>
            {
                for (int i = 0; i < 4000; i++) db.Put(B("t" + t + "-" + (i % 1000).ToString("D4", System.Globalization.CultureInfo.InvariantCulture)), Val(i, t));
            });
            // Close while compactions may still be running.
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, dbo, cfo))
        {
            for (int t = 0; t < 4; t++)
            {
                for (int i = 3000; i < 4000; i++)
                {
                    CollectionAssert.AreEqual(Val(i, t), db.Get(B("t" + t + "-" + (i % 1000).ToString("D4", System.Globalization.CultureInfo.InvariantCulture))));
                }
            }
            db.WaitForCompact();
            AssertLsmShapeValid(db);
        }
        _ = expected;
    }

    [TestMethod]
    public void TtlDatabaseCompactionDropsExpiredValues()
    {
        MemoryFileSystem fs = NewFs();
        long now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        long clock = now;
        using (TtlDatabase db = TtlDatabase.Open(DbPath, NewOptions(fs), SmallCf(), 100, () => Interlocked.Read(ref clock)))
        {
            for (int i = 0; i < 200; i++) db.Put(Key(i), Val(i, 0));
            db.Flush();
            // Not expired yet: compaction keeps everything.
            db.CompactRange();
            for (int i = 0; i < 200; i++) CollectionAssert.AreEqual(Val(i, 0), db.Get(Key(i)));
            // Advance the filter's clock past the TTL: the next compaction drops the values.
            Interlocked.Exchange(ref clock, now + 1000);
            db.CompactRange();
            for (int i = 0; i < 200; i++) Assert.IsNull(db.Get(Key(i)), "key " + i);
            Assert.AreEqual("0", db.Database.GetProperty("rocksdb.estimate-num-keys"));
        }
        bool found = false;
        foreach (string child in fs.GetChildren(DbPath))
        {
            if (!child.StartsWith("OPTIONS-", StringComparison.Ordinal)) continue;
            using var r = new StreamReader(new MemoryStream(ReadAll(fs, DbPath + "/" + child)));
            if (r.ReadToEnd().Contains("compaction_filter_factory=TtlCompactionFilterFactory", StringComparison.Ordinal)) found = true;
        }
        Assert.IsTrue(found);
    }
}

/// <summary>DB::CompactFiles on <see cref="MemoryFileSystem"/> (compact_files_test.cc in spirit).</summary>
[TestClass]
public sealed class DbCompactFilesTests
{
    private static ColumnFamilyOptions ManualCf() => new()
    {
        DisableAutoCompactions = true,
        LevelCompactionDynamicLevelBytes = false,
        Compression = RocksDb.Managed.Compression.CompressionType.None,
    };

    private static List<LiveFileMetadata> Files(RocksDatabase db, string cf, int level) =>
        db.GetLiveFilesMetadata().FindAll(f => f.ColumnFamilyName == cf && f.Level == level);

    private static void AssertValues(ColumnFamily cf)
    {
        for (int i = 0; i < 300; i++) Assert.AreEqual(S(Value(i, i == 0 ? 0 : i % 2 == 0 ? 2 : 1)), S(cf.Get(Key(i))), "key " + i);
    }

    [TestMethod]
    public async Task CompactFilesOnColumnFamilyKeepsDataAndValidatesInputs()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), ManualCf());
        ColumnFamily cf = db.CreateColumnFamily("other", ManualCf());
        for (int round = 0; round < 3; round++)
        {
            for (int i = round; i < 300; i += 2) cf.Put(Key(i), Value(i, round));
            cf.Flush();
        }
        List<LiveFileMetadata> l0 = Files(db, "other", 0);
        Assert.AreEqual(3, l0.Count);
        // By number, async; compacting the newest L0 file to L1 takes all older L0 files too.
        ulong newest = l0.MaxBy(f => f.FileNumber)!.FileNumber;
        IReadOnlyList<string> outputs = await cf.CompactFilesAsync(new CompactFilesOptions { OutputFileSizeLimit = 8 * 1024 }, [newest], 1);
        Assert.IsTrue(outputs.Count > 1, "output_file_size_limit cuts the outputs");
        Assert.AreEqual(0, Files(db, "other", 0).Count);
        Assert.AreEqual(outputs.Count, Files(db, "other", 1).Count);
        foreach (string o in outputs) Assert.IsTrue(fs.FileExists(o), o);
        AssertValues(cf);
        // The default column family has no such file.
        var ex = Assert.ThrowsExactly<RocksDbException>(() => db.CompactFiles([Files(db, "other", 1)[0].Name], 2));
        Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
        ex = Assert.ThrowsExactly<RocksDbException>(() => cf.CompactFiles([Files(db, "other", 1)[0].Name], -1));
        Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
        ex = Assert.ThrowsExactly<RocksDbException>(() => cf.CompactFiles([], 1));
        Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
        // The first and the last L1 file to L2: the files between them on the sorted level come along.
        List<LiveFileMetadata> l1 = Files(db, "other", 1);
        l1.Sort((a, b) => a.SmallestKey.AsSpan().SequenceCompareTo(b.SmallestKey));
        cf.CompactFiles(new CompactFilesOptions(), [l1[0].FileNumber, l1[^1].FileNumber], 2);
        Assert.AreEqual(0, Files(db, "other", 1).Count);
        AssertValues(cf);
    }

    [TestMethod]
    public void CompactFilesFifoOnlyOutputsToL0()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions o = ManualCf();
        o.CompactionStyle = CompactionStyle.Fifo;
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), o);
        db.Put(Key(1), Value(1, 0));
        db.Flush();
        db.Put(Key(2), Value(2, 0));
        db.Flush();
        List<LiveFileMetadata> l0 = Files(db, "default", 0);
        var ex = Assert.ThrowsExactly<RocksDbException>(() => db.CompactFiles([l0[0].Name], 1));
        Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
        StringAssert.Contains(ex.Message, "maximum output level");
        IReadOnlyList<string> outputs = db.CompactFiles([l0[0].Name, l0[1].Name], 0);
        Assert.AreEqual(1, outputs.Count);
        Assert.AreEqual(1, Files(db, "default", 0).Count);
        Assert.AreEqual(S(Value(1, 0)), S(db.Get(Key(1))));
    }
    /// <summary>
    /// CompactFilesImpl: EnoughRoomForCompaction with an SstFileManager (CompactionTooLarge, COMPACTION_CANCELLED, the
    /// reservation released) and the compaction_job_info output (BuildCompactionJobInfo).
    /// </summary>
    [TestMethod]
    public async Task CompactFilesChecksRoomAndReturnsJobInfo()
    {
        MemoryFileSystem fs = NewFs();
        using var sfm = new SstFileManager(fs, 0, 0.25, 64UL << 20);
        var stats = new Statistics();
        DbOptions o = NewOptions(fs);
        o.SstFileManager = sfm;
        o.Statistics = stats;
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, ManualCf());
        for (int round = 0; round < 3; round++)
        {
            for (int i = round; i < 300; i += 2) db.Put(Key(i), Value(i, round));
            db.Flush();
        }
        List<LiveFileMetadata> l0 = Files(db, "default", 0);
        ulong[] inputs = l0.ConvertAll(f => f.FileNumber).ToArray();
        sfm.SetMaxAllowedSpaceUsage(1);
        var ex = Assert.ThrowsExactly<RocksDbException>(() => db.DefaultColumnFamily.CompactFiles(new CompactFilesOptions(), inputs, 1, out _));
        Assert.AreEqual(StatusCode.CompactionTooLarge, ex.Code);
        Assert.AreEqual(1UL, stats.GetTickerCount(Ticker.CompactionCancelled));
        Assert.AreEqual(3, Files(db, "default", 0).Count, "nothing compacted");
        Assert.AreEqual(0UL, sfm.CompactionsReservedSize);

        sfm.SetMaxAllowedSpaceUsage(0);
        IReadOnlyList<string> outputs = db.DefaultColumnFamily.CompactFiles(new CompactFilesOptions(), inputs, 1, out CompactionJobInfo? info);
        Assert.IsNotNull(info);
        Assert.IsNull(info.Status);
        Assert.AreEqual("default", info.ColumnFamilyName);
        Assert.AreEqual(0, info.BaseInputLevel);
        Assert.AreEqual(1, info.OutputLevel);
        Assert.AreEqual(3, info.InputFiles.Count);
        CollectionAssert.AreEquivalent(outputs.ToList(), info.OutputFiles.ToList());
        Assert.AreEqual(CompactionReason.ManualCompaction, info.CompactionReason);
        Assert.IsTrue(info.NumInputRecords >= 300 && info.NumOutputRecords == 300);
        Assert.AreEqual(0UL, sfm.CompactionsReservedSize, "reservation released");

        // Async, with a trivial move.
        List<LiveFileMetadata> l1 = Files(db, "default", 1);
        (IReadOnlyList<string> moved, CompactionJobInfo? moveInfo) = await db.DefaultColumnFamily.CompactFilesWithJobInfoAsync(
            new CompactFilesOptions { AllowTrivialMove = true }, l1.ConvertAll(f => f.FileNumber), 3);
        Assert.IsNotNull(moveInfo);
        Assert.IsTrue(moveInfo.IsTrivialMove);
        Assert.AreEqual(3, moveInfo.OutputLevel);
        Assert.AreEqual(l1.Count, moved.Count);
    }
}

/// <summary>Subcompactions (max_subcompactions; CompactionJob::GenSubcompactionBoundaries) on <see cref="MemoryFileSystem"/>.</summary>
[TestClass]
public sealed class DbSubcompactionTests
{
    private static ColumnFamilyOptions Cf() => new()
    {
        DisableAutoCompactions = true,
        WriteBufferSize = 4 << 20,
        TargetFileSizeBase = 64 * 1024,
        Compression = RocksDb.Managed.Compression.CompressionType.None,
    };

    /// <summary>
    /// Overlapping L0 files compacted with max_subcompactions = 4: the input is split at anchor keys into 4 ranges
    /// compacted in parallel (NUM_SUBCOMPACTIONS_SCHEDULED records 4); the result is a valid sorted level with the same
    /// data, including deletions and range tombstones that span subcompaction boundaries.
    /// </summary>
    [TestMethod]
    [DataRow(1u, DisplayName = "max_subcompactions 1")]
    [DataRow(4u, DisplayName = "max_subcompactions 4")]
    public void SubcompactionsProduceSameData(uint maxSubcompactions)
    {
        MemoryFileSystem fs = NewFs();
        var stats = new Statistics();
        DbOptions o = NewOptions(fs);
        o.MaxSubcompactions = maxSubcompactions;
        o.Statistics = stats;
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, Cf());
        const int N = 6000;
        var model = new string?[N];
        var rnd = new Random(77);
        for (int round = 0; round < 6; round++)
        {
            for (int i = 0; i < 2500; i++)
            {
                int k = rnd.Next(N);
                if (rnd.Next(10) == 0)
                {
                    db.Delete(Key(k));
                    model[k] = null;
                }
                else
                {
                    db.Put(Key(k), Value(k, round));
                    model[k] = S(Value(k, round));
                }
            }
            if (round == 3)
            {
                // A range tombstone across most of the key space (spans the subcompaction boundaries).
                db.DeleteRange(Key(1000), Key(5000));
                for (int k = 1000; k < 5000; k++) model[k] = null;
            }
            db.Flush();
        }
        db.CompactRange();
        HistogramData h = stats.GetHistogramData(Histogram.NumSubcompactionsScheduled);
        if (maxSubcompactions == 1) Assert.AreEqual(0UL, h.Count);
        else
        {
            Assert.AreEqual(1UL, h.Count, "one compaction split into subcompactions");
            Assert.AreEqual(4.0, h.Max);
        }
        List<LiveFileMetadata> files = db.GetLiveFilesMetadata();
        files.Sort((a, b) => a.SmallestKey.AsSpan().SequenceCompareTo(b.SmallestKey));
        for (int i = 0; i < files.Count; i++)
        {
            Assert.AreNotEqual(0, files[i].Level);
            if (i > 0) Assert.IsTrue(files[i - 1].LargestKey.AsSpan().SequenceCompareTo(files[i].SmallestKey) < 0, "sorted, non-overlapping outputs");
        }
        for (int k = 0; k < N; k++) Assert.AreEqual(model[k], S(db.Get(Key(k))), "key " + k);
        int count = 0;
        using (Iterator it = db.NewIterator())
        {
            for (it.SeekToFirst(); it.Valid; it.Next()) count++;
        }
        int expected = 0;
        foreach (string? v in model) if (v is not null) expected++;
        Assert.AreEqual(expected, count);
    }

    /// <summary>
    /// Subcompactions with user-defined timestamps: boundaries carry the maximum timestamp (InitializeReadOptionsAndBoundaries),
    /// so every version of a user key is compacted by one subcompaction; outputs do not overlap by user key and every
    /// version stays readable at its timestamp.
    /// </summary>
    [TestMethod]
    public void SubcompactionsWithUserDefinedTimestamps()
    {
        MemoryFileSystem fs = NewFs();
        var stats = new Statistics();
        DbOptions o = NewOptions(fs);
        o.MaxSubcompactions = 4;
        o.Statistics = stats;
        ColumnFamilyOptions cf = Cf();
        cf.Comparator = Comparator.BytewiseWithU64Ts;
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, cf);
        const int N = 3000;
        const int Rounds = 5;
        for (int round = 0; round < Rounds; round++)
        {
            byte[] ts = DbTimestampTests.Ts((ulong)(round + 1) * 10);
            for (int k = 0; k < N; k++)
            {
                if (round == 3 && k % 7 == 0) db.Delete(Key(k), ts);
                else db.Put(Key(k), ts, Value(k, round));
            }
            db.Flush();
        }
        db.CompactRange();
        HistogramData h = stats.GetHistogramData(Histogram.NumSubcompactionsScheduled);
        Assert.AreEqual(1UL, h.Count, "one compaction split into subcompactions");
        Assert.IsTrue(h.Max > 1);
        List<LiveFileMetadata> files = db.GetLiveFilesMetadata();
        Assert.IsTrue(files.Count > 1);
        // Sorted, non-overlapping outputs (user keys with timestamps, newest timestamp first).
        Comparator ucmp = Comparator.BytewiseWithU64Ts;
        files.Sort((a, b) => ucmp.Compare(a.SmallestKey, b.SmallestKey));
        for (int i = 1; i < files.Count; i++) Assert.IsTrue(ucmp.Compare(files[i - 1].LargestKey, files[i].SmallestKey) < 0, "overlapping outputs");
        for (int k = 0; k < N; k += 3)
        {
            for (int round = 0; round < Rounds; round++)
            {
                string? expected = round == 3 && k % 7 == 0 ? null : S(Value(k, round));
                Assert.AreEqual(expected, S(db.Get(new ReadOptions { Timestamp = DbTimestampTests.Ts((ulong)(round + 1) * 10) }, Key(k))), "key " + k + " round " + round);
            }
        }
    }

    [TestMethod]
    public void ClippingIteratorRestrictsToRange()
    {
        var mem = new List<(byte[], byte[])>();
        for (int i = 0; i < 10; i++)
        {
            var k = new byte[Key(i).Length + 8];
            DbFormat.AppendInternalKey(k, Key(i), 100, EntryType.Value);
            mem.Add((k, Value(i, 0)));
        }
        static byte[] Seek(byte[] user)
        {
            var k = new byte[user.Length + 8];
            DbFormat.AppendInternalKey(k, user, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
            return k;
        }
        using var it = new ClippingIterator(new ListIterator(mem), Seek(Key(3)), Seek(Key(7)), InternalKeyComparator.Bytewise);
        var seen = new List<string>();
        for (it.SeekToFirst(); it.Valid; it.Next()) seen.Add(S(DbFormat.ExtractUserKey(it.Key).ToArray())!);
        CollectionAssert.AreEqual(new[] { S(Key(3)), S(Key(4)), S(Key(5)), S(Key(6)) }, seen);
        it.SeekToLast();
        Assert.AreEqual(S(Key(6)), S(DbFormat.ExtractUserKey(it.Key).ToArray()));
        it.Seek(Seek(Key(8)));
        Assert.IsFalse(it.Valid);
        it.Seek(Seek(Key(1)));
        Assert.AreEqual(S(Key(3)), S(DbFormat.ExtractUserKey(it.Key).ToArray()));
    }

    /// <summary>A sorted in-memory internal iterator.</summary>
    private sealed class ListIterator(List<(byte[] Key, byte[] Value)> items) : RocksDb.Managed.Db.Iterators.InternalIterator
    {
        private int _pos = -1;
        public override bool Valid => _pos >= 0 && _pos < items.Count;
        public override ReadOnlySpan<byte> Key => items[_pos].Key;
        public override ReadOnlySpan<byte> Value => items[_pos].Value;
        public override void SeekToFirst() => _pos = 0;
        public override void SeekToLast() => _pos = items.Count - 1;
        public override void Seek(ReadOnlySpan<byte> target)
        {
            _pos = 0;
            while (_pos < items.Count && InternalKeyComparator.Bytewise.Compare(items[_pos].Key, target) < 0) _pos++;
        }
        public override void SeekForPrev(ReadOnlySpan<byte> target)
        {
            _pos = items.Count - 1;
            while (_pos >= 0 && InternalKeyComparator.Bytewise.Compare(items[_pos].Key, target) > 0) _pos--;
        }
        public override void Next() => _pos++;
        public override void Prev() => _pos--;
    }
}
