using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>
/// Background (flush / compaction / file) allocations: the long-lived background thread pools, the write-buffer pool of
/// local writable files, table-properties interning, the compression-options string cache, dictionary-compression block
/// buffering, EVENT_LOG formatting and version installation. The steady-state bounds measure all threads
/// (<see cref="GC.GetTotalAllocatedBytes(bool)"/>), since the work being measured runs on background threads.
/// </summary>
[TestClass]
[DoNotParallelize]
public sealed class BackgroundAllocationTests
{
    private const int EntriesPerFlush = 5_000;

    private static string NewTempDir(string name) => Path.Combine(Path.GetTempPath(), "rdbm-bgalloc-" + name + "-" + Guid.NewGuid().ToString("N"));

    private static void DeleteDir(string dir)
    {
        try
        {
            Directory.Delete(dir, recursive: true);
        }
        catch (IOException)
        {
        }
    }

    /// <summary>
    /// One cycle: 4 flushes of the same keys (new values) and a full compaction, so the LSM shape is the same after every
    /// cycle. Returns the bytes all threads allocated during the flushes and the compaction (the Puts are not counted:
    /// memtable growth is not background work).
    /// </summary>
    private static long FlushAndCompactCycle(RocksDatabase db, byte[][] keys, byte[] value, WriteOptions wo)
    {
        long background = 0;
        for (int f = 0; f < 4; f++)
        {
            for (int i = f; i < keys.Length; i += 4) db.Put(wo, keys[i], value);
            long before = GC.GetTotalAllocatedBytes(precise: true);
            db.Flush();
            background += GC.GetTotalAllocatedBytes(precise: true) - before;
        }
        long c = GC.GetTotalAllocatedBytes(precise: true);
        db.CompactRange(null, null);
        return background + GC.GetTotalAllocatedBytes(precise: true) - c;
    }

    private static byte[][] Keys(int n)
    {
        var keys = new byte[n][];
        for (int i = 0; i < n; i++) keys[i] = Key(i);
        return keys;
    }

    /// <summary>Bytes allocated by all threads per cycle after <paramref name="warmup"/> cycles, averaged over <paramref name="measured"/> cycles.</summary>
    private static long MeasureCycles(RocksDatabase db, int warmup, int measured, CompressionType compression)
    {
        byte[][] keys = Keys(EntriesPerFlush * 4);
        byte[] value = new byte[100];
        new Random(5).NextBytes(value);
        var wo = new WriteOptions { DisableWal = true };
        for (int i = 0; i < warmup; i++) FlushAndCompactCycle(db, keys, value, wo);
        long total = 0;
        for (int i = 0; i < measured; i++) total += FlushAndCompactCycle(db, keys, value, wo);
        long perCycle = total / measured;
        Console.WriteLine($"{compression}: {perCycle} B per cycle (4 flushes of {EntriesPerFlush} entries + compaction)");
        _ = compression;
        return perCycle;
    }

    private static (DbOptions Db, ColumnFamilyOptions Cf) LocalOptions(CompressionType compression, uint dictBytes, InfoLogLevel level)
    {
        var o = new DbOptions { CreateIfMissing = true, InfoLogLevel = level };
        var cf = new ColumnFamilyOptions
        {
            WriteBufferSize = 64UL << 20,
            DisableAutoCompactions = true,
            Compression = compression,
            BottommostCompression = compression,
        };
        if (dictBytes > 0)
        {
            cf.CompressionOpts.MaxDictBytes = dictBytes;
            cf.CompressionOpts.ZstdMaxTrainBytes = dictBytes * 8;
            cf.BottommostCompressionOpts.MaxDictBytes = dictBytes;
            cf.BottommostCompressionOpts.ZstdMaxTrainBytes = dictBytes * 8;
            cf.BottommostCompressionOpts.Enabled = true;
        }
        return (o, cf);
    }

    // ---- steady-state bounds -----------------------------------------------------------------------------

    /// <summary>
    /// Flushes and compactions with the info LOG at INFO (the default). About 155 KB per cycle of 4 flushes + 1
    /// compaction remain in a Debug build, most of it the index and filter blocks the readers of the new tables pin
    /// (retained, not garbage) and one string per LOG line; the EVENT_LOG builders, table-properties strings, per-file
    /// write buffers and per-job thread scratch no longer allocate.
    /// </summary>
    [TestMethod]
    public void SteadyStateFlushAndCompactionAllocateLittle()
    {
        string dir = NewTempDir("steady");
        try
        {
            (DbOptions o, ColumnFamilyOptions cf) = LocalOptions(CompressionType.None, 0, InfoLogLevel.Info);
            using RocksDatabase db = RocksDatabase.Open(dir, o, cf);
            long perCycle = MeasureCycles(db, warmup: 4, measured: 6, CompressionType.None);
            Assert.IsTrue(perCycle < 224 * 1024, $"{perCycle} B per cycle");
        }
        finally
        {
            DeleteDir(dir);
        }
    }

    /// <summary>
    /// With the info LOG above INFO, no flush / compaction event or INFO line is formatted at all (about 30 KB per cycle
    /// less than at INFO).
    /// </summary>
    [TestMethod]
    public void SteadyStateFlushAndCompactionWithLogAtWarnAllocateLess()
    {
        long warn = MeasureInDir(InfoLogLevel.Warn);
        long info = MeasureInDir(InfoLogLevel.Info);
        Assert.IsTrue(warn < 180 * 1024, $"{warn} B per cycle");
        Assert.IsTrue(warn + 8 * 1024 < info, $"warn {warn} B vs info {info} B per cycle");
    }

    private static long MeasureInDir(InfoLogLevel level)
    {
        string dir = NewTempDir("level");
        try
        {
            (DbOptions o, ColumnFamilyOptions cf) = LocalOptions(CompressionType.None, 0, level);
            using RocksDatabase db = RocksDatabase.Open(dir, o, cf);
            return MeasureCycles(db, warmup: 4, measured: 6, CompressionType.None);
        }
        finally
        {
            DeleteDir(dir);
        }
    }

    /// <summary>
    /// Zstd with a dictionary: the buffered data blocks are copied into pooled chunks, not into a new array per block
    /// (before: ~4.5 MB per cycle here, a copy of every data block of every output file).
    /// </summary>
    [TestMethod]
    public void SteadyStateDictionaryCompressionDoesNotCopyBlocksToNewArrays()
    {
        if (!BlockCompression.IsSupported(CompressionType.Zstd)) Assert.Inconclusive("zstd not available");
        string dir = NewTempDir("dict");
        try
        {
            (DbOptions o, ColumnFamilyOptions cf) = LocalOptions(CompressionType.Zstd, 16 * 1024, InfoLogLevel.Info);
            using RocksDatabase db = RocksDatabase.Open(dir, o, cf);
            long perCycle = MeasureCycles(db, warmup: 3, measured: 4, CompressionType.Zstd);
            Assert.IsTrue(perCycle < 512 * 1024, $"{perCycle} B per cycle");
            // The data reads back (the replayed blocks were compressed with the dictionary).
            byte[] buffer = new byte[256];
            for (int i = 0; i < EntriesPerFlush * 4; i += 97) Assert.IsTrue(db.TryGet(Key(i), buffer, out int n) && n == 100);
        }
        finally
        {
            DeleteDir(dir);
        }
    }

    // ---- EVENT_LOG / info LOG ----------------------------------------------------------------------------

    private sealed class CollectingLogger(InfoLogLevel level) : InfoLogger(level)
    {
        public readonly List<string> Lines = new();

        protected override void LogCore(InfoLogLevel level, string message)
        {
            lock (Lines) Lines.Add(message);
        }

        public List<string> Snapshot()
        {
            lock (Lines) return new List<string>(Lines);
        }
    }

    private static List<JsonElement> Events(IEnumerable<string> lines, string name)
    {
        var result = new List<JsonElement>();
        foreach (string line in lines)
        {
            int i = line.IndexOf("EVENT_LOG_v1 ", StringComparison.Ordinal);
            if (i < 0) continue;
            JsonElement e = JsonDocument.Parse(line[(i + "EVENT_LOG_v1 ".Length)..]).RootElement;
            if (e.TryGetProperty("event", out JsonElement ev) && ev.GetString() == name) result.Add(e);
        }
        return result;
    }

    /// <summary>The events formatted into the reused builder are complete, valid JSON with the expected fields.</summary>
    [TestMethod]
    public void FlushAndCompactionEventsAreWellFormed()
    {
        MemoryFileSystem fs = NewFs();
        var log = new CollectingLogger(InfoLogLevel.Info);
        DbOptions o = NewOptions(fs);
        o.InfoLog = log;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o, new ColumnFamilyOptions { DisableAutoCompactions = true }))
        {
            for (int f = 0; f < 3; f++)
            {
                for (int i = 0; i < 200; i++) db.Put(Key(i), Value(i, f));
                db.Flush();
            }
            db.CompactRange(null, null);
        }
        List<string> lines = log.Snapshot();
        Assert.AreEqual(3, Events(lines, "flush_started").Count);
        List<JsonElement> created = Events(lines, "table_file_creation");
        Assert.AreEqual(4, created.Count);
        foreach (JsonElement e in created)
        {
            Assert.AreEqual("default", e.GetProperty("cf_name").GetString());
            JsonElement tp = e.GetProperty("table_properties");
            Assert.AreEqual("leveldb.BytewiseComparator", tp.GetProperty("comparator").GetString());
            Assert.IsTrue(tp.GetProperty("compression_options").GetString()!.StartsWith("window_bits=", StringComparison.Ordinal));
            Assert.AreEqual("OK", e.GetProperty("status").GetString());
        }
        List<JsonElement> finished = Events(lines, "flush_finished");
        Assert.AreEqual(3, finished.Count);
        Assert.AreEqual(7, finished[0].GetProperty("lsm_state").GetArrayLength());
        JsonElement started = Events(lines, "compaction_started").Single();
        Assert.AreEqual(3, started.GetProperty("files_L0").GetArrayLength());
        Assert.AreEqual("ManualCompaction", started.GetProperty("compaction_reason").GetString());
        Assert.AreEqual(-1, started.GetProperty("oldest_snapshot_seqno").GetInt64());
        Assert.AreEqual(1, Events(lines, "compaction_finished").Single().GetProperty("num_output_files").GetInt32());
        Assert.IsTrue(lines.Exists(l => l.StartsWith("[default] [JOB ", StringComparison.Ordinal) && l.Contains("] Level-0 flush table #", StringComparison.Ordinal)));
        Assert.IsTrue(lines.Exists(l => l.Contains("] Compacting 3@0 files to L", StringComparison.Ordinal)));
        Assert.IsTrue(Events(lines, "table_file_deletion").Count >= 3);
    }

    /// <summary>With a logger above INFO, no event reaches it (and none is formatted, see the bound above).</summary>
    [TestMethod]
    public void EventsAreNotLoggedAboveInfoLevel()
    {
        MemoryFileSystem fs = NewFs();
        var log = new CollectingLogger(InfoLogLevel.Warn);
        DbOptions o = NewOptions(fs);
        o.InfoLog = log;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o))
        {
            for (int i = 0; i < 100; i++) db.Put(Key(i), Value(i, 0));
            db.Flush();
            db.CompactRange(null, null);
        }
        Assert.IsFalse(log.Snapshot().Exists(l => l.Contains("EVENT_LOG_v1", StringComparison.Ordinal)));
    }

    /// <summary>The LOG file lines keep the EnvLogger format: timestamp with microseconds, thread id in hex, message.</summary>
    [TestMethod]
    public void LogFileLinesKeepTheirFormat()
    {
        string dir = NewTempDir("logfmt");
        try
        {
            using (RocksDatabase db = RocksDatabase.Open(dir, new DbOptions { CreateIfMissing = true }))
            {
                db.Put(Key(1), Value(1, 0));
                db.Flush();
            }
            string[] lines = File.ReadAllLines(Path.Combine(dir, "LOG"));
            var re = new System.Text.RegularExpressions.Regex(@"^\d{4}/\d{2}/\d{2}-\d{2}:\d{2}:\d{2}\.\d{6} [0-9a-f]+ ");
            int events = 0;
            foreach (string l in lines)
            {
                if (!re.IsMatch(l)) continue;
                if (l.Contains("EVENT_LOG_v1 {\"time_micros\": ", StringComparison.Ordinal) && l.EndsWith('}')) events++;
            }
            Assert.IsTrue(events >= 3, string.Join('\n', lines));
            Assert.IsTrue(Array.Exists(lines, l => re.IsMatch(l) && l.Contains("[default] [JOB ", StringComparison.Ordinal)));
        }
        finally
        {
            DeleteDir(dir);
        }
    }

    // ---- table properties --------------------------------------------------------------------------------

    private static byte[] BuildPropertiesBlock(TableProperties p)
    {
        using var b = new PropertyBlockBuilder();
        p.AddTo(b);
        b.Add("my.user.prop", Encoding.UTF8.GetBytes("user-value"));
        b.Add(TablePropertiesNames.BlockBasedIndexType, new byte[] { 0, 0, 0, 0 });
        return b.Finish().ToArray();
    }

    private static TableProperties SampleProperties(string sessionId) => new()
    {
        DataSize = 1234,
        NumEntries = 99,
        NumDeletions = 3,
        NumMergeOperands = 2,
        ColumnFamilyId = 0,
        ColumnFamilyName = "default",
        ComparatorName = "leveldb.BytewiseComparator",
        MergeOperatorName = "nullptr",
        PrefixExtractorName = "nullptr",
        PropertyCollectorsNames = "[]",
        CompressionName = "NoCompression",
        CompressionOptions = "window_bits=-14; level=32767; ",
        DbId = "db-id",
        DbSessionId = sessionId,
        FilterPolicyName = "bloomfilter",
        SequenceNumberTimeMapping = [1, 2, 3],
        KeyLargestSeqno = 77,
        KeySmallestSeqno = 7,
    };

    private static TableProperties Parse(byte[] blockBytes)
    {
        byte[] buf = new byte[blockBytes.Length];
        blockBytes.CopyTo(buf, 0);
        Block block = Block.Create(buf, buf.Length, rentedFromSharedPool: false);
        try
        {
            return TableProperties.Parse(block, 0);
        }
        finally
        {
            block.Release();
        }
    }

    [TestMethod]
    public void TablePropertiesParseRoundTripsAndInternsStrings()
    {
        byte[] bytes = BuildPropertiesBlock(SampleProperties("session-1"));
        TableProperties a = Parse(bytes);
        TableProperties b = Parse(bytes);
        foreach (TableProperties p in new[] { a, b })
        {
            Assert.AreEqual(1234UL, p.DataSize);
            Assert.AreEqual(99UL, p.NumEntries);
            Assert.AreEqual(3UL, p.NumDeletions);
            Assert.AreEqual(77UL, p.KeyLargestSeqno);
            Assert.AreEqual(7UL, p.KeySmallestSeqno);
            Assert.AreEqual("default", p.ColumnFamilyName);
            Assert.AreEqual("leveldb.BytewiseComparator", p.ComparatorName);
            Assert.AreEqual("session-1", p.DbSessionId);
            Assert.AreEqual("bloomfilter", p.FilterPolicyName);
            CollectionAssert.AreEqual(new byte[] { 1, 2, 3 }, p.SequenceNumberTimeMapping);
            Assert.AreEqual("user-value", p.GetUserPropertyString("my.user.prop"));
            Assert.IsTrue(p.UserCollectedProperties.ContainsKey(TablePropertiesNames.BlockBasedIndexType));
            // API backwards compatibility: deletions and merge operands are also user-collected properties.
            Assert.IsTrue(p.UserCollectedProperties.ContainsKey(TablePropertiesNames.DeletedKeys));
            Assert.IsTrue(p.UserCollectedProperties.ContainsKey(TablePropertiesNames.MergeOperands));
        }
        // The strings of the second parse are the first parse's instances; byte values are copies.
        Assert.AreSame(a.ComparatorName, b.ComparatorName);
        Assert.AreSame(a.DbSessionId, b.DbSessionId);
        Assert.AreSame(a.CompressionOptions, b.CompressionOptions);
        Assert.AreNotSame(a.SequenceNumberTimeMapping, b.SequenceNumberTimeMapping);
        Assert.AreSame(TablePropertiesNames.BlockBasedIndexType, FindKey(b, TablePropertiesNames.BlockBasedIndexType));
        Assert.AreSame(FindKey(a, "my.user.prop"), FindKey(b, "my.user.prop"));
        // A different value gets its own string.
        TableProperties c = Parse(BuildPropertiesBlock(SampleProperties("session-2")));
        Assert.AreEqual("session-2", c.DbSessionId);
        Assert.AreEqual("session-1", Parse(bytes).DbSessionId);
    }

    private static string FindKey(TableProperties p, string key)
    {
        foreach (string k in p.UserCollectedProperties.Keys)
        {
            if (k == key) return k;
        }
        throw new KeyNotFoundException(key);
    }

    [TestMethod]
    public void TablePropertiesParseSkipsMalformedNumbersAndRejectsUnsortedBlocks()
    {
        using (var b = new PropertyBlockBuilder())
        {
            b.Add(TablePropertiesNames.DataSize, new byte[] { 0x80 }); // truncated varint: skipped
            b.Add(TablePropertiesNames.NumEntries, 5UL);
            byte[] block = b.Finish().ToArray();
            TableProperties p = Parse(block);
            Assert.AreEqual(0UL, p.DataSize);
            Assert.AreEqual(5UL, p.NumEntries);
            Assert.IsFalse(p.UserCollectedProperties.ContainsKey(TablePropertiesNames.DataSize));
        }
        // Keys out of order: built with a meta block builder directly (PropertyBlockBuilder sorts).
        using var raw = BlockBuilder.ForMetaBlock(int.MaxValue);
        raw.Add("rocksdb.z"u8, "1"u8);
        raw.Add("rocksdb.a"u8, "1"u8);
        byte[] unsorted = raw.Finish().ToArray();
        var ex = Assert.ThrowsExactly<RocksDbException>(() => Parse(unsorted));
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
    }

    [TestMethod]
    public void TablePropertiesParseSteadyStateAllocatesOnlyTheResult()
    {
        byte[] bytes = BuildPropertiesBlock(SampleProperties("session-alloc"));
        byte[] buf = new byte[bytes.Length];
        bytes.CopyTo(buf, 0);
        Block block = Block.Create(buf, buf.Length, rentedFromSharedPool: false);
        try
        {
            for (int i = 0; i < 100; i++) _ = TableProperties.Parse(block, 0);
            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < 100; i++) _ = TableProperties.Parse(block, 0);
            long perParse = (GC.GetAllocatedBytesForCurrentThread() - before) / 100;
            // The TableProperties object, its user-property dictionary (4 entries) and the copied byte values; before:
            // ~2.5 KB with ~40 key strings, a key copy per property and the string values.
            Assert.IsTrue(perParse < 1400, $"{perParse} B per parse");
        }
        finally
        {
            block.Release();
        }
    }

    // ---- compression options string ----------------------------------------------------------------------

    [TestMethod]
    public void CompressionOptionsStringIsCachedByValue()
    {
        var o = new CompressionOptions { Level = 3, MaxDictBytes = 1024 };
        string a = BlockBasedTableBuilder.CompressionOptionsStringCache.Get(o, CompressionType.Zstd);
        Assert.AreEqual("window_bits=-14; level=3; strategy=0; max_dict_bytes=1024; zstd_max_train_bytes=0; enabled=0; max_dict_buffer_bytes=0; " +
                        "use_zstd_dict_trainer=1; max_compressed_bytes_per_kb=896; checksum=0; _type=7; ", a);
        Assert.AreSame(a, BlockBasedTableBuilder.CompressionOptionsStringCache.Get(new CompressionOptions { Level = 3, MaxDictBytes = 1024 }, CompressionType.Zstd));
        // The options are mutable: a changed value (or type) gives the new string.
        o.Level = 5;
        StringAssert.Contains(BlockBasedTableBuilder.CompressionOptionsStringCache.Get(o, CompressionType.Zstd), "; level=5; ");
        StringAssert.EndsWith(BlockBasedTableBuilder.CompressionOptionsStringCache.Get(o, CompressionType.LZ4), "_type=4; ");
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 100; i++) _ = BlockBasedTableBuilder.CompressionOptionsStringCache.Get(o, CompressionType.LZ4);
        Assert.AreEqual(0, GC.GetAllocatedBytesForCurrentThread() - before);
    }

    // ---- local writable file buffers ---------------------------------------------------------------------

    [TestMethod]
    public void LocalWritableFilesReuseTheirBuffersAcrossThreads()
    {
        string dir = NewTempDir("wbuf");
        Directory.CreateDirectory(dir);
        try
        {
            var fs = new LocalFileSystem();
            byte[] data = new byte[100_000];
            new Random(3).NextBytes(data);
            void WriteFile(int i)
            {
                string path = Path.Combine(dir, "f" + (i % 4));
                using IWritableFile f = fs.CreateWritable(path, truncate: true);
                f.Append(data.AsSpan(0, 1000));
                f.Append(data);  // grows the buffer from 64 KiB
                f.Flush();
            }
            // Warm up on several threads (files closed on another thread than the next one is created on).
            for (int i = 0; i < 8; i++) Task.Run(() => WriteFile(i)).Wait();
            long before = GC.GetTotalAllocatedBytes(precise: true);
            for (int i = 0; i < 20; i++)
            {
                int n = i;
                if (i % 2 == 0) WriteFile(n);
                else Task.Run(() => WriteFile(n)).Wait();
            }
            long perFile = (GC.GetTotalAllocatedBytes(precise: true) - before) / 20;
            // Without the pool each file allocated its 64 KiB buffer (and the 128 KiB one it grew to).
            Assert.IsTrue(perFile < 8 * 1024, $"{perFile} B per file");
            // Content is intact with reused buffers.
            byte[] back = File.ReadAllBytes(Path.Combine(dir, "f3"));
            Assert.AreEqual(101_000, back.Length);
            CollectionAssert.AreEqual(data, back.AsSpan(1000).ToArray());
        }
        finally
        {
            DeleteDir(dir);
        }
    }

    [TestMethod]
    public void LocalWritableFileRejectsUseAfterDispose()
    {
        string dir = NewTempDir("wdisp");
        Directory.CreateDirectory(dir);
        try
        {
            var fs = new LocalFileSystem();
            IWritableFile f = fs.CreateWritable(Path.Combine(dir, "x"), truncate: true);
            f.Append("abc"u8);
            f.Dispose();
            // The buffer went back to the pool: further writes must fail instead of writing into a buffer another file uses.
            var ex = Assert.ThrowsExactly<RocksDbException>(() => f.Append("def"u8));
            Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
            using IWritableFile g = fs.CreateWritable(Path.Combine(dir, "y"), truncate: true);
            g.Append("ghi"u8);
            g.Flush();
            Assert.AreEqual("abc", File.ReadAllText(Path.Combine(dir, "x")));
            Assert.AreEqual("ghi", File.ReadAllText(Path.Combine(dir, "y")));
        }
        finally
        {
            DeleteDir(dir);
        }
    }

    // ---- version installation ----------------------------------------------------------------------------

    [TestMethod]
    public void ApplyEditKeepsUnchangedLevelsAndRebuildsChangedOnes()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { DisableAutoCompactions = true });
        for (int f = 0; f < 2; f++)
        {
            for (int i = 0; i < 50; i++) db.Put(Key(i * 2 + f), Value(i, f));
            db.Flush();
        }
        db.CompactRange(null, null); // everything in L6
        for (int i = 0; i < 10; i++) db.Put(Key(1000 + i), Value(i, 9));
        db.Flush(); // one L0 file
        ColumnFamilyData cfd = db.DefaultColumnFamily.Data;
        VersionStorageInfo cur = cfd.Current.Storage;
        Assert.AreEqual(1, cur.NumLevelFiles(0));
        Assert.AreEqual(1, cur.NumLevelFiles(cur.NumLevels - 1));
        FileMetaData l0 = cur.LevelFiles(0)[0];
        FileMetaData bottom = cur.LevelFiles(cur.NumLevels - 1)[0];

        // Move the L0 file to L1: L0 and L1 change, the bottom level keeps its array.
        var edit = new VersionEdit();
        edit.DeleteFile(0, l0.FileNumber);
        edit.AddFile(1, l0);
        FileMetaData[][] next = cur.ApplyEdit(edit, cur.NumLevels);
        Assert.AreEqual(0, next[0].Length);
        Assert.AreSame(l0, next[1].Single());
        Assert.AreSame(cur.LevelFilesArray(cur.NumLevels - 1), next[cur.NumLevels - 1]);
        Assert.AreSame(bottom, next[cur.NumLevels - 1][0]);

        // Deleting a file that is not there is a corruption.
        var bad = new VersionEdit();
        bad.DeleteFile(2, l0.FileNumber);
        var ex = Assert.ThrowsExactly<RocksDbException>(() => cur.ApplyEdit(bad, cur.NumLevels));
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
    }

    // ---- background thread pools -------------------------------------------------------------------------

    private sealed class RecordingItem(ManualResetEventSlim? gate, CountdownEvent done) : IThreadPoolWorkItem
    {
        public int ThreadId;
        public string? ThreadName;

        public void Execute()
        {
            ThreadId = Environment.CurrentManagedThreadId;
            ThreadName = Thread.CurrentThread.Name;
            gate?.Wait();
            done.Signal();
        }
    }

    [TestMethod]
    public void BackgroundThreadPoolReusesThreadsAndNeverQueuesBehindBusyOnes()
    {
        BackgroundThreadPool pool = BackgroundThreadPool.Bottom;
        // Sequential jobs reuse the same (idle) thread.
        var ids = new HashSet<int>();
        for (int i = 0; i < 10; i++)
        {
            using var done = new CountdownEvent(1);
            var item = new RecordingItem(null, done);
            pool.Schedule(item);
            Assert.IsTrue(done.Wait(TimeSpan.FromSeconds(10)));
            ids.Add(item.ThreadId);
            StringAssert.StartsWith(item.ThreadName, "rocksdb:bottom:");
        }
        Assert.IsTrue(ids.Count <= 2, $"{ids.Count} threads for 10 sequential jobs");

        // Jobs that block each run on their own thread (the pool grows instead of queueing them).
        using var gate = new ManualResetEventSlim(false);
        using var all = new CountdownEvent(6);
        var items = new RecordingItem[6];
        for (int i = 0; i < items.Length; i++) pool.Schedule(items[i] = new RecordingItem(gate, all));
        // All six must have started before any is released.
        Assert.IsTrue(SpinWait.SpinUntil(() =>
        {
            foreach (RecordingItem it in items)
            {
                if (Volatile.Read(ref it.ThreadId) == 0) return false;
            }
            return true;
        }, TimeSpan.FromSeconds(10)));
        gate.Set();
        Assert.IsTrue(all.Wait(TimeSpan.FromSeconds(10)));
        var distinct = new HashSet<int>();
        foreach (RecordingItem it in items) distinct.Add(it.ThreadId);
        Assert.AreEqual(6, distinct.Count);
    }

    [TestMethod]
    public async Task BackgroundThreadPoolRunPropagatesTheJobOutcome()
    {
        var box = new StrongBox<int>(0);
        await BackgroundThreadPool.Low.Run(static s => ((StrongBox<int>)s!).Value = Thread.CurrentThread.Name!.StartsWith("rocksdb:low:", StringComparison.Ordinal) ? 1 : -1, box);
        Assert.AreEqual(1, box.Value);
        var ex = await Assert.ThrowsExactlyAsync<RocksDbException>(() =>
            BackgroundThreadPool.Low.Run(static _ => throw new RocksDbException(StatusCode.Aborted, "job failed"), null));
        Assert.AreEqual(StatusCode.Aborted, ex.Code);
        // A failing job does not kill its thread: the pool still runs work.
        await BackgroundThreadPool.Low.Run(static s => Interlocked.Increment(ref ((StrongBox<int>)s!).Value), box);
        Assert.AreEqual(2, box.Value);
    }

    private sealed class ThreadRecordingListener : EventListener
    {
        public readonly List<string> FlushThreads = new();
        public readonly List<string> CompactionThreads = new();

        public override void OnFlushCompleted(RocksDatabase? db, FlushJobInfo info)
        {
            lock (FlushThreads) FlushThreads.Add(Thread.CurrentThread.Name ?? "");
        }

        public override void OnCompactionCompleted(RocksDatabase? db, CompactionJobInfo info)
        {
            lock (CompactionThreads) CompactionThreads.Add(Thread.CurrentThread.Name ?? "");
        }
    }

    /// <summary>Flushes run on the HIGH pool and automatic compactions on the LOW pool, across DB instances.</summary>
    [TestMethod]
    public void FlushesAndCompactionsRunOnTheBackgroundPools()
    {
        for (int round = 0; round < 3; round++)
        {
            MemoryFileSystem fs = NewFs();
            var listener = new ThreadRecordingListener();
            DbOptions o = NewOptions(fs);
            o.Listeners.Add(listener);
            using (RocksDatabase db = RocksDatabase.Open(DbPath, o, new ColumnFamilyOptions { Level0FileNumCompactionTrigger = 2 }))
            {
                for (int f = 0; f < 4; f++)
                {
                    for (int i = 0; i < 100; i++) db.Put(Key(i), Value(i, f));
                    db.Flush();
                }
                db.WaitForCompact();
            }
            lock (listener.FlushThreads)
            {
                Assert.AreEqual(4, listener.FlushThreads.Count);
                foreach (string n in listener.FlushThreads) StringAssert.StartsWith(n, "rocksdb:high:");
            }
            lock (listener.CompactionThreads)
            {
                Assert.IsTrue(listener.CompactionThreads.Count >= 1);
                foreach (string n in listener.CompactionThreads) StringAssert.StartsWith(n, "rocksdb:low:");
            }
        }
        // Threads are kept (not one per job): opening, flushing and closing DBs did not grow the pools without bound.
        Assert.IsTrue(BackgroundThreadPool.High.ThreadCount <= 16, BackgroundThreadPool.High.ThreadCount.ToString());
        Assert.IsTrue(BackgroundThreadPool.High.KeptThreadCount >= 1);
    }

    /// <summary>Close while flushes and compactions are scheduled: the DB waits for them, and the pools stay usable.</summary>
    [TestMethod]
    public void CloseAndCancelAllBackgroundWorkWithPooledThreads()
    {
        for (int round = 0; round < 5; round++)
        {
            MemoryFileSystem fs = NewFs();
            using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { Level0FileNumCompactionTrigger = 2, WriteBufferSize = 64 * 1024 }))
            {
                for (int i = 0; i < 5000; i++) db.Put(Key(i), Value(i, round));
                if (round % 2 == 1) db.CancelAllBackgroundWork(wait: true);
            }
            using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
            {
                for (int i = 0; i < 5000; i += 251) Assert.AreEqual(S(Value(i, round)), GetString(db, S(Key(i))!));
            }
        }
    }
}
