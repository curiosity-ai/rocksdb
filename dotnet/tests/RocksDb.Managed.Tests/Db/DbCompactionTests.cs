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

    private static void RunRandomizedModel(int seed, bool withMerge, CompactionStyle style)
    {
        SCOPED(seed);
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cfo = SmallCf();
        cfo.WriteBufferSize = 32 * 1024;
        cfo.CompactionStyle = style;
        if (withMerge) cfo.MergeOperator = new StringAppendOperator();
        var dbo = NewOptions(fs);
        dbo.MaxBackgroundJobs = 4;
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
        RunRandomizedModel(seed, withMerge: true, CompactionStyle.Level);
        RunRandomizedModel(12345, withMerge: true, CompactionStyle.Level);
    }

    [TestMethod]
    public void RandomizedModelLeveledNoMergeOperator() => RunRandomizedModel(777, withMerge: false, CompactionStyle.Level);

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
        cfo.Level0FileNumCompactionTrigger = 100; // keep compaction from running on its own
        using RocksDatabase db2 = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        // 5 L0 files >= stop trigger (4): writes stop.
        Assert.AreEqual("1", db2.GetProperty("rocksdb.is-write-stopped"));
        var ex = Assert.ThrowsExactly<RocksDbException>(() => db2.Put(new WriteOptions { NoSlowdown = true }, Key(11), Val(11, 0)));
        Assert.AreEqual(StatusCode.Incomplete, ex.Code);
        // A blocked writer resumes once a (manual) compaction reduces L0.
        Task writer = Task.Run(() => db2.Put(Key(12), Val(12, 0)));
        Assert.IsFalse(writer.Wait(200));
        db2.CompactRange();
        Assert.IsTrue(writer.Wait(10000));
        Assert.AreEqual("0", db2.GetProperty("rocksdb.is-write-stopped"));
        CollectionAssert.AreEqual(Val(12, 0), db2.Get(Key(12)));
        for (int round = 0; round < 4; round++) CollectionAssert.AreEqual(Val(round, round), db2.Get(Key(round)));
    }

    [TestMethod]
    public void WriteSlowdownDelaysWrites()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cfo = SmallCf();
        cfo.DisableAutoCompactions = true;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo))
        {
            for (int round = 0; round < 3; round++)
            {
                db.Put(Key(round), Val(round, round));
                db.Flush();
            }
        }
        cfo.DisableAutoCompactions = false;
        cfo.Level0FileNumCompactionTrigger = 100;
        cfo.Level0SlowdownWritesTrigger = 3;
        cfo.Level0StopWritesTrigger = 100;
        DbOptions dbo = NewOptions(fs);
        dbo.DelayedWriteRate = 100 * 1024; // 100 KB/s
        using RocksDatabase db2 = RocksDatabase.Open(DbPath, dbo, cfo);
        Assert.AreEqual("0", db2.GetProperty("rocksdb.is-write-stopped"));
        Assert.AreNotEqual("0", db2.GetProperty("rocksdb.actual-delayed-write-rate"));
        var ex = Assert.ThrowsExactly<RocksDbException>(() =>
        {
            for (int i = 0; i < 100; i++) db2.Put(new WriteOptions { NoSlowdown = true }, Key(100 + i), new byte[4096]);
        });
        Assert.AreEqual(StatusCode.Incomplete, ex.Code);
        var sw = System.Diagnostics.Stopwatch.StartNew();
        for (int i = 0; i < 10; i++) db2.Put(Key(200 + i), new byte[4096]); // 40KB at 100KB/s
        Assert.IsTrue(sw.ElapsedMilliseconds >= 150, "elapsed " + sw.ElapsedMilliseconds);
        db2.CompactRange();
        Assert.AreEqual("0", db2.GetProperty("rocksdb.actual-delayed-write-rate"));
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
        // Auto compactions are disabled: nothing pending.
        Assert.AreEqual("0", db.GetProperty("rocksdb.compaction-pending"));
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
