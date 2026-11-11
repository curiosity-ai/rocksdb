using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Db;
using RocksDb.Managed.Db.Blob;
using RocksDb.Managed.Db.Wide;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;
using RocksDb.Managed.Tools;
using RocksDb.Managed.Util;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Regression tests of the audit #4 "final events" pass: blob read accounting (db/blob/blob_source.cc), value reads of
/// entities with blob columns (table/get_context.cc, db/wide/wide_column_serialization.cc), lazy wide-column reads
/// (db/wide/read_path_blob_resolver.cc), the EVENT_LOG of table file creations and compactions (db/event_helpers.cc,
/// db/compaction/compaction_job.cc), attribute groups and the row cache.
/// </summary>
[TestClass]
public sealed partial class Audit4FinalEventsTests
{
    private static WideColumn C(string name, string value) => new(B(name), B(value));

    /// <summary>The raw (unresolved) value stored for <paramref name="userKey"/> in the table files.</summary>
    private static byte[] RawTableValue(IFileSystem fs, string userKey)
    {
        foreach (string c in fs.GetChildren(DbPath))
        {
            if (!c.EndsWith(".sst", StringComparison.Ordinal)) continue;
            using BlockBasedTableReader reader = BlockBasedTableReader.Open(fs.OpenRandomAccess(DbPath + "/" + c), new TableReaderOptions());
            using BlockBasedTableIterator it = reader.NewIterator();
            for (it.SeekToFirst(); it.Valid; it.Next())
            {
                if (Encoding.UTF8.GetString(DbFormat.ExtractUserKey(it.Key)) == userKey) return it.Value.ToArray();
            }
        }
        Assert.Fail("key not found in any table: " + userKey);
        return [];
    }

    [TestMethod]
    public void BlobSourceGetBlob_CacheHitReportsRecordSize()
    {
        // BlobSource::GetBlob (blob_source.cc:300-312): on a blob cache hit, "for consistency, the size of on-disk
        // (possibly compressed) blob record is assigned to bytes_read": value_size plus
        // BlobLogRecord::CalculateAdjustmentForRecordHeader(user_key.size()) under verify_checksums. The port returned
        // 0, so a blob GC compaction under-reported its blob bytes read for cached blobs.
        MemoryFileSystem fs = NewFs();
        var cfo = new ColumnFamilyOptions { EnableBlobFiles = true, MinBlobSize = 16, BlobCache = Managed.Cache.BlockCache.CreateLru(1 << 20) };
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        byte[] value = B(new string('v', 300));
        db.Put(B("key"), value);
        db.Flush();
        BlobIndex index = BlobIndex.Decode(RawTableValue(fs, "key"));
        Assert.AreEqual(300UL, index.Size);
        BlobSource source = db.DefaultColumnFamily.Data.BlobSource;
        ulong record = 300 + BlobLogFormat.RecordHeaderSize + 3; // value + header (32) + key

        foreach (bool verify in new[] { true, false })
        {
            var ro = new ReadOptions { VerifyChecksums = verify };
            ulong expected = verify ? record : 300UL;
            using var w = new PooledBufferWriter(512);
            if (verify)
            {
                Assert.IsFalse(source.IsInCache(index.FileNumber, index.Offset));
                Assert.AreEqual(expected, source.GetBlob(ro, B("key"), index.FileNumber, index.Offset, index.Size, index.Compression, w, out bool hit0));
                Assert.IsFalse(hit0);
                w.Clear();
            }
            Assert.IsTrue(source.IsInCache(index.FileNumber, index.Offset));
            Assert.AreEqual(expected, source.GetBlob(ro, B("key"), index.FileNumber, index.Offset, index.Size, index.Compression, w, out bool hit));
            Assert.IsTrue(hit);
            CollectionAssert.AreEqual(value, w.WrittenSpan.ToArray());
        }
    }

    [TestMethod]
    public void GetValueOfDefaultColumn_V2WithBlobColumns()
    {
        // WideColumnSerialization::GetValueOfDefaultColumn (wide_column_serialization.cc:700-802): the version 2 fast
        // path reads column 0 through the skip info and reports is_blob_reference; the other columns are not decoded,
        // so an entity with blob columns is not an error.
        static byte[] V2(params (string Name, byte[] Value, bool Blob)[] cols)
        {
            using var list = new EntityColumnList();
            foreach ((string n, byte[] v, bool b) in cols) list.Add(B(n), v, b);
            var w = new PooledBufferWriter(64);
            WideColumnSerialization.SerializeV2(list, w);
            return w.WrittenSpan.ToArray();
        }
        var bw = new PooledBufferWriter(32);
        BlobIndex.EncodeBlob(bw, 7, 100, 50, CompressionType.None);
        byte[] blobIndex = bw.WrittenSpan.ToArray();

        byte[] inlineDefault = V2(("", B("dflt"), false), ("big", blobIndex, true));
        Assert.AreEqual("dflt", Encoding.UTF8.GetString(WideColumnSerialization.GetValueOfDefaultColumn(inlineDefault, out bool isBlob)));
        Assert.IsFalse(isBlob);
        // The fully-resolved-entity overload still rejects the blob reference (Deserialize with blob_columns == nullptr).
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => WideColumnSerialization.GetValueOfDefaultColumn(inlineDefault).ToArray());
        Assert.AreEqual(StatusCode.Corruption, ex.Code);

        byte[] blobDefault = V2(("", blobIndex, true), ("small", B("s"), false));
        CollectionAssert.AreEqual(blobIndex, WideColumnSerialization.GetValueOfDefaultColumn(blobDefault, out isBlob).ToArray());
        Assert.IsTrue(isBlob);

        byte[] noDefault = V2(("a", blobIndex, true), ("b", B("x"), false));
        Assert.AreEqual(0, WideColumnSerialization.GetValueOfDefaultColumn(noDefault, out isBlob).Length);
        Assert.IsFalse(isBlob);

        // Version 1 goes through the full deserialization.
        var v1 = new PooledBufferWriter(32);
        WideColumnSerialization.Serialize([C("", "d1"), C("x", "y")], v1);
        Assert.AreEqual("d1", Encoding.UTF8.GetString(WideColumnSerialization.GetValueOfDefaultColumn(v1.WrittenSpan, out isBlob)));
        Assert.IsFalse(isBlob);

        // A truncated values section is Corruption ("Error decoding wide column value payload").
        ex = Assert.ThrowsExactly<RocksDbException>(() => WideColumnSerialization.GetValueOfDefaultColumn(inlineDefault.AsSpan(0, inlineDefault.Length - blobIndex.Length - 2), out _).ToArray());
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
    }

    [TestMethod]
    public async Task GetOfEntityWithBlobColumns_ReadsOnlyTheDefaultColumn()
    {
        // GetContext::SaveWideColumnEntityToPinnable (get_context.cc:125-153): DB::Get of an entity takes the default
        // column through GetValueOfDefaultColumn; "only a blob-referenced default needs a fetch". The port resolved
        // every blob column of the entity first (extra blob reads; a read that fails for another column failed the Get).
        MemoryFileSystem fs = NewFs();
        var st = new Statistics();
        DbOptions o = NewOptions(fs);
        o.Statistics = st;
        var cfo = new ColumnFamilyOptions
        {
            EnableBlobFiles = true, MinBlobSize = 32, DisableAutoCompactions = true,
            MergeOperator = new RocksDb.Managed.Tests.Db.StringAppendMergeOperator(),
        };
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, cfo);
        string big = new('b', 200), bigDefault = new('d', 100);
        db.PutEntity(B("inl"), [C("", "small-default"), C("big", big)]);
        db.PutEntity(B("blb"), [C("", bigDefault), C("big", big)]);
        db.PutEntity(B("mrg"), [C("", "base"), C("big", big)]);
        db.Flush();
        db.Merge(B("mrg"), B("op"));

        // Default column inline: no blob file read at all, also with async reads and with no I/O allowed for blobs.
        Assert.AreEqual("small-default", GetString(db, "inl"));
        Assert.AreEqual("small-default", S(await db.GetAsync(B("inl"))));
        Assert.AreEqual(0UL, st.GetTickerCount(Ticker.BlobDbBlobFileBytesRead));
        Assert.AreEqual("small-default", S(db.Get(new ReadOptions { ReadTier = ReadTier.BlockCacheTier }, B("inl"))));

        // Default column in a blob file: exactly that record is read (value + 32-byte header + key).
        // (Opening the blob file reader reads its header and footer, BlobFileReader::ReadHeader / ReadFooter.)
        Assert.AreEqual(bigDefault, GetString(db, "blb"));
        Assert.AreEqual(100UL + 32 + 3 + BlobLogFormat.HeaderSize + BlobLogFormat.FooterSize, st.GetTickerCount(Ticker.BlobDbBlobFileBytesRead));

        // GetEntity still resolves every column.
        WideColumns? e = db.DefaultColumnFamily.GetEntity(B("inl"));
        Assert.IsNotNull(e);
        Assert.AreEqual(2, e.Count);
        Assert.AreEqual(big, Encoding.UTF8.GetString(e.GetValue(1)));

        // A merge on top of the entity resolves it whole (MergeWithWideColumnBaseValue -> ResolveEntityForMerge) and
        // applies the operand to the default column.
        Assert.AreEqual("base,op", GetString(db, "mrg"));
        WideColumns? m = db.DefaultColumnFamily.GetEntity(B("mrg"));
        Assert.IsNotNull(m);
        Assert.AreEqual("base,op", Encoding.UTF8.GetString(m.GetValue(0)));
        Assert.AreEqual(big, Encoding.UTF8.GetString(m.GetValue(1)));
    }

    [TestMethod]
    public void IteratorMergeOnBlobEntity_DoesNotReuseThePreviousEntity()
    {
        // DBIter::MergeWithWideColumnBaseValue (db_iter.cc:1650 ff.) resolves the blob columns of the entity the merge
        // operands apply to (ResolveEntityForMerge). The port's per-position cache of the resolved entity was still
        // marked valid from the previous position, so the operands were merged into the previous key's entity.
        MemoryFileSystem fs = NewFs();
        var cfo = new ColumnFamilyOptions
        {
            EnableBlobFiles = true, MinBlobSize = 32, DisableAutoCompactions = true,
            MergeOperator = new RocksDb.Managed.Tests.Db.StringAppendMergeOperator(),
        };
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        string big = new('b', 200);
        db.PutEntity(B("a"), [C("", "first"), C("big", big)]);
        db.PutEntity(B("b"), [C("", "base"), C("big", new string('c', 100))]);
        db.Flush();
        db.Merge(B("b"), B("op"));

        using Iterator it = db.NewIterator();
        it.SeekToFirst();
        Assert.IsTrue(it.Valid);
        Assert.AreEqual("first", Encoding.UTF8.GetString(it.Value)); // resolves (and caches) the entity of "a"
        it.Next();
        Assert.IsTrue(it.Valid);
        Assert.AreEqual("b", Encoding.UTF8.GetString(it.Key));
        Assert.AreEqual("base,op", Encoding.UTF8.GetString(it.Value));
        // Backward: the same merge reached through Prev after reading "a" again.
        it.SeekToFirst();
        Assert.AreEqual("first", Encoding.UTF8.GetString(it.Value));
        it.SeekToLast();
        Assert.AreEqual("base,op", Encoding.UTF8.GetString(it.Value));
        it.Prev();
        Assert.AreEqual("first", Encoding.UTF8.GetString(it.Value));
    }

    [TestMethod]
    public async Task LazyWideColumns_AsyncResolveRecordsRecordBytes()
    {
        // BlobSource::GetBlob records the lazy read with the bytes BlobFileReader::GetBlob read (blob_source.cc:351-359):
        // the whole record under verify_checksums. The port's ResolveColumnAsync recorded the value size.
        MemoryFileSystem fs = NewFs();
        var st = new Statistics();
        DbOptions o = NewOptions(fs);
        o.Statistics = st;
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, new ColumnFamilyOptions { EnableBlobFiles = true, MinBlobSize = 0 });
        db.PutEntity(B("k"), [C("", "default"), C("big", new string('b', 200)), C("empty", "")]);
        db.Flush();
        const ulong Record = 200 + 32 + 1;

        using (var lazy = new LazyWideColumns())
        {
            Assert.IsTrue(db.DefaultColumnFamily.TryGetEntityLazy(null, B("k"), lazy));
            Assert.AreEqual(200, lazy.ResolveColumn(lazy.IndexOf("big"u8)).Length);
            Assert.AreEqual(Record, st.GetTickerCount(Ticker.BlobDbLazyReadBytes));
        }
        using (var lazy = new LazyWideColumns())
        {
            Assert.IsTrue(db.DefaultColumnFamily.TryGetEntityLazy(null, B("k"), lazy));
            Assert.AreEqual(200, (await lazy.ResolveColumnAsync(lazy.IndexOf("big"u8))).Length);
            Assert.AreEqual(2 * Record, st.GetTickerCount(Ticker.BlobDbLazyReadBytes));
            Assert.AreEqual(2UL, st.GetTickerCount(Ticker.BlobDbLazyReadCount));

            // BlobFileBuilder::Add (blob_file_builder.cc:122) never writes an empty value to a blob file, so the
            // zero-size reference of ReadPathBlobResolver::ResolveColumnRange's strict_subrange test
            // (read_path_blob_resolver.cc:196-199) cannot be produced by a write; the empty column stays inline.
            int empty = lazy.IndexOf("empty"u8);
            Assert.IsFalse(lazy.IsReference(empty));
            Assert.AreEqual(0, lazy.ReadColumnRange(empty, 0, new byte[8]));
            Assert.AreEqual(2UL, st.GetTickerCount(Ticker.BlobDbLazyReadCount));
        }
    }

    // ---- EVENT_LOG (db/event_helpers.cc, db/builder.cc, db/compaction/compaction_job.cc) -------------------------

    private sealed class CollectingLogger() : InfoLogger(InfoLogLevel.Info)
    {
        private readonly List<string> _lines = new();

        protected override void LogCore(InfoLogLevel level, string message)
        {
            lock (_lines) _lines.Add(message);
        }

        public List<JsonElement> Events(string name)
        {
            var result = new List<JsonElement>();
            lock (_lines)
            {
                foreach (string line in _lines)
                {
                    int i = line.IndexOf("EVENT_LOG_v1 ", StringComparison.Ordinal);
                    if (i < 0) continue;
                    JsonElement e = JsonDocument.Parse(line[(i + "EVENT_LOG_v1 ".Length)..]).RootElement;
                    if (e.TryGetProperty("event", out JsonElement ev) && ev.GetString() == name) result.Add(e);
                }
            }
            return result;
        }
    }

    private sealed class CreationRecorder : EventListener
    {
        public readonly ConcurrentQueue<TableFileCreationInfo> Created = new();
        public Action? FlushBegin;
        public Action? CompactionBegin;

        public override void OnFlushBegin(RocksDatabase? db, FlushJobInfo info) => FlushBegin?.Invoke();

        public override void OnCompactionBegin(RocksDatabase? db, CompactionJobInfo info) => CompactionBegin?.Invoke();

        public override void OnTableFileCreated(TableFileCreationInfo info) => Created.Enqueue(info);
    }

    private static (MemoryFileSystem Fs, DbOptions Options, CollectingLogger Log, CreationRecorder Listener) EventSetup()
    {
        MemoryFileSystem fs = NewFs();
        var log = new CollectingLogger();
        var l = new CreationRecorder();
        DbOptions o = NewOptions(fs);
        o.InfoLog = log;
        o.AvoidFlushDuringShutdown = true;
        o.Listeners.Add(l);
        return (fs, o, log, l);
    }

    private static List<JsonElement> NotKept(CollectingLogger log)
    {
        var result = new List<JsonElement>();
        foreach (JsonElement e in log.Events("table_file_creation"))
        {
            if (e.GetProperty("status").GetString() != "OK") result.Add(e);
        }
        return result;
    }

    [TestMethod]
    public void EventLog_FailedFlushOutputIsLoggedWithItsStatus()
    {
        // BuildTable (builder.cc:192-198, 597-602): EventHelpers::LogAndNotifyTableFileCreationFinished writes the
        // table_file_creation event with `"status" << s.ToString()` for a table that failed too (event_helpers.cc:174).
        // The port wrote the event only for installed outputs, always with "status": "OK".
        (MemoryFileSystem fs, DbOptions options, CollectingLogger log, CreationRecorder l) = EventSetup();
        using RocksDatabase db = RocksDatabase.Open(DbPath, options);
        l.FlushBegin = () => fs.SetFilesystemActive(false);
        db.Put(B("a"), B("1"));
        try
        {
            db.Flush();
            Assert.Fail("the flush must fail");
        }
        catch (RocksDbException)
        {
        }
        fs.SetFilesystemActive(true);
        List<JsonElement> failed = NotKept(log);
        Assert.AreEqual(1, failed.Count);
        JsonElement e = failed[0];
        Assert.AreEqual("default", e.GetProperty("cf_name").GetString());
        Assert.IsTrue(e.GetProperty("file_number").GetUInt64() > 0);
        Assert.AreEqual(0UL, e.GetProperty("file_size").GetUInt64());
        Assert.AreEqual("Unknown", e.GetProperty("file_checksum_func_name").GetString());
        Assert.AreEqual(0UL, e.GetProperty("table_properties").GetProperty("num_entries").GetUInt64());
        Assert.AreEqual(1, l.Created.Count);
        Assert.IsTrue(l.Created.TryPeek(out TableFileCreationInfo? info));
        Assert.IsNotNull(info.Status);
        Assert.AreEqual(info.Status.Message, e.GetProperty("status").GetString());
        Assert.AreEqual(0, log.Events("flush_finished").Count);
    }

    [TestMethod]
    public void EventLog_EmptyCompactionOutputIsReportedAsNotKept()
    {
        // CompactionJob::FinishCompactionOutputFile (compaction_job.cc:2392-2447): an output opened only for range
        // tombstones that are all dropped has "nothing to output"; its creation is logged and notified with the file
        // name "(nil)", an empty FileDescriptor and Status::Aborted("Empty SST file not kept").
        (_, DbOptions options, CollectingLogger log, CreationRecorder l) = EventSetup();
        using RocksDatabase db = RocksDatabase.Open(DbPath, options, new ColumnFamilyOptions { DisableAutoCompactions = true });
        db.Put(B("a"), B("1"));
        db.Put(B("b"), B("1"));
        db.Flush();
        db.DeleteRange(B("a"), B("c"));
        db.Flush();
        db.CompactRange();
        Assert.AreEqual(0, db.GetLiveFilesMetadata().Count);

        List<JsonElement> notKept = NotKept(log);
        Assert.AreEqual(1, notKept.Count);
        JsonElement e = notKept[0];
        Assert.AreEqual("Operation aborted: Empty SST file not kept", e.GetProperty("status").GetString());
        Assert.AreEqual(0UL, e.GetProperty("file_number").GetUInt64());
        Assert.AreEqual(0UL, e.GetProperty("file_size").GetUInt64());
        Assert.AreEqual(DbFormat.MaxSequenceNumber, e.GetProperty("smallest_seqno").GetUInt64()); // FileDescriptor()
        Assert.AreEqual(0UL, e.GetProperty("largest_seqno").GetUInt64());
        Assert.IsTrue(e.GetProperty("job").GetInt32() > 0);

        TableFileCreationInfo? empty = null;
        foreach (TableFileCreationInfo i in l.Created)
        {
            if (i.Reason == TableFileCreationReason.Compaction) empty = i;
        }
        Assert.IsNotNull(empty);
        Assert.AreEqual("(nil)", empty.FilePath);
        Assert.IsNotNull(empty.Status);
        Assert.AreEqual(StatusCode.Aborted, empty.Status.Code);
        Assert.AreEqual(e.GetProperty("job").GetInt32(), empty.JobId);
    }

    [TestMethod]
    public void EventLog_CompactionOutputThatCannotBeCreatedIsLogged()
    {
        // CompactionJob::OpenCompactionOutputFile (compaction_job.cc:2830-2843): when NewWritableFile fails,
        // LogAndNotifyTableFileCreationFinished is called with FileDescriptor(), TableProperties() and the status.
        (MemoryFileSystem fs, DbOptions options, CollectingLogger log, CreationRecorder l) = EventSetup();
        using RocksDatabase db = RocksDatabase.Open(DbPath, options, new ColumnFamilyOptions { DisableAutoCompactions = true });
        for (int f = 0; f < 2; f++)
        {
            db.Put(B("a"), B("v" + f));
            db.Put(B("z"), B("v" + f));
            db.Flush();
        }
        l.CompactionBegin = () => fs.SetFilesystemActive(false);
        try
        {
            db.CompactRange();
            Assert.Fail("the compaction must fail");
        }
        catch (RocksDbException)
        {
        }
        fs.SetFilesystemActive(true);
        List<JsonElement> failed = NotKept(log);
        Assert.AreEqual(1, failed.Count);
        Assert.AreEqual(0UL, failed[0].GetProperty("file_number").GetUInt64());
        TableFileCreationInfo? info = null;
        foreach (TableFileCreationInfo i in l.Created)
        {
            if (i.Reason == TableFileCreationReason.Compaction) info = i;
        }
        Assert.IsNotNull(info);
        Assert.IsNotNull(info.Status);
        Assert.AreEqual(info.Status.Message, failed[0].GetProperty("status").GetString());
        Assert.IsTrue(info.FilePath.EndsWith(".sst", StringComparison.Ordinal));
        Assert.AreEqual(0UL, info.FileSize);
    }

    [TestMethod]
    public void EventLog_CompactionStartedCarriesPlacementCutoffs()
    {
        // CompactionJob::LogCompaction (compaction_job.cc:3204-3211): with per-key placement the compaction_started
        // event continues, after oldest_snapshot_seqno, with proximal_after_seqno, preserve_seqno_after,
        // proximal_output_level and proximal_output_range (GetCompactionProximalOutputRangeTypeString). The port wrote
        // the event before the job had computed the cutoffs and left the four keys out.
        (_, DbOptions options, CollectingLogger log, _) = EventSetup();
        var cfo = new ColumnFamilyOptions
        {
            NumLevels = 7, PrecludeLastLevelDataSeconds = 10000, DisableAutoCompactions = true, Compression = CompressionType.None,
        };
        using RocksDatabase db = RocksDatabase.Open(DbPath, options, cfo);
        for (int i = 0; i < 100; i++) db.Put(Key(i), Value(i, 0));
        db.Flush();
        const ulong Cutoff = 150; // the DB starts at seqno 100 (PrePopulateInDB): keys 0..49 are "old"
        CompactionJob.TestPrecludeLastLevelMinSeqno = _ => Cutoff;
        try
        {
            db.CompactRange();
        }
        finally
        {
            CompactionJob.TestPrecludeLastLevelMinSeqno = null;
        }
        List<JsonElement> started = log.Events("compaction_started");
        Assert.IsTrue(started.Count > 0);
        var jobs = new HashSet<int>();
        int withPlacement = 0;
        foreach (JsonElement e in started)
        {
            Assert.IsTrue(jobs.Add(e.GetProperty("job").GetInt32()), "one compaction_started event per job");
            if (!e.TryGetProperty("proximal_after_seqno", out JsonElement after)) continue;
            withPlacement++;
            var names = new List<string>();
            foreach (JsonProperty prop in e.EnumerateObject()) names.Add(prop.Name);
            int at = names.IndexOf("oldest_snapshot_seqno");
            CollectionAssert.AreEqual(new[] { "proximal_after_seqno", "preserve_seqno_after", "proximal_output_level", "proximal_output_range" },
                names.GetRange(at + 1, names.Count - at - 1));
            Assert.AreEqual(5, e.GetProperty("proximal_output_level").GetInt32());
            // PopulateProximalLevelOutputRange: leveled compaction never has the full range.
            Assert.AreEqual("NonLastRange", e.GetProperty("proximal_output_range").GetString());
            // proximal_after_seqno_ = max(preclude_last_level_min_seqno, keep_in_last_level_through_seqno).
            Assert.IsTrue(after.GetUInt64() >= Cutoff);
            // preserve_seqno_after_ = min(max(preserve_time_min_seqno, 1) - 1, earliest_snapshot) < the cutoff.
            Assert.IsTrue(e.GetProperty("preserve_seqno_after").GetUInt64() < Cutoff);
        }
        Assert.IsTrue(withPlacement > 0, "a compaction to the last level supports per-key placement");
        // The cutoff was applied as logged: the new half of the keys stayed on the proximal level.
        var levels = new HashSet<int>();
        foreach (LiveFileMetadata f in db.GetLiveFilesMetadata()) levels.Add(f.Level);
        Assert.IsTrue(levels.SetEquals([5, 6]));
    }

    // ---- wide columns / attribute groups ---------------------------------------------------------------------

    [TestMethod]
    public void InternalDumpOfEntityWithBlobColumnsReportsTheError()
    {
        // WideColumnsHelper::DumpSliceAsWideColumns (wide_columns_helper.cc:30-41) uses DeserializeSimple, which is
        // Corruption for an entity with a blob reference; ldb idump (ldb_cmd.cc:2320) and sst_dump
        // (sst_file_dumper.cc:945-950) then print "error deserializing wide columns" to stderr and skip the entry. The
        // port printed the column with its raw blob index bytes.
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { EnableBlobFiles = true, MinBlobSize = 32 }))
        {
            db.PutEntity(B("blob"), [C("", "d"), C("big", new string('b', 100))]);
            db.PutEntity(B("inline"), [C("", "d"), C("x", "y")]);
            db.Flush();
        }
        var o = new StringWriter { NewLine = "\n" };
        var e = new StringWriter { NewLine = "\n" };
        ToolMain.Run(["ldb", "--db=" + DbPath, "idump"], fs, o, e);
        StringAssert.Contains(o.ToString(), ":d x:y");
        Assert.IsFalse(o.ToString().Contains("big:", StringComparison.Ordinal), o.ToString());
        StringAssert.Contains(e.ToString(), "error deserializing wide columns");
    }

    [TestMethod]
    public void AttributeGroupIterator_GroupOrderAndDirectionChanges()
    {
        // MultiCfIteratorImpl (multi_cf_iterator_impl.h): the heap orders equal keys by the column family's position in
        // the argument list in both directions (MultiCfHeapItemComparator: "c == 0 ? a.order - b.order > 0"), so the
        // attribute groups of a key are in argument order forward and backward; Next after Prev re-seeks every child to
        // the current key (GetHeap's init function) and then advances all children positioned at it.
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        ColumnFamily hot = db.CreateColumnFamily("hot", new ColumnFamilyOptions());
        ColumnFamily cold = db.CreateColumnFamily("cold", new ColumnFamilyOptions());
        db.PutAttributeGroups(B("a"), [new AttributeGroup(hot, new[] { C("h", "a-hot") }), new AttributeGroup(cold, new[] { C("c", "a-cold") })]);
        db.PutAttributeGroups(B("b"), [new AttributeGroup(cold, new[] { C("c", "b-cold") })]);
        db.PutAttributeGroups(B("c"), [new AttributeGroup(hot, new[] { C("h", "c-hot") }), new AttributeGroup(cold, new[] { C("c", "c-cold") })]);
        hot.Put(B("d"), B("plain"));

        static string Describe(AttributeGroupIterator it)
        {
            var sb = new StringBuilder(Encoding.UTF8.GetString(it.Key)).Append('=');
            foreach (IteratorAttributeGroup g in it.AttributeGroups)
            {
                sb.Append(g.ColumnFamily.Name).Append('[');
                for (int i = 0; i < g.Columns.Count; i++) sb.Append(Encoding.UTF8.GetString(g.Columns.GetName(i))).Append(':').Append(Encoding.UTF8.GetString(g.Columns.GetValue(i)));
                sb.Append(']');
            }
            return sb.ToString();
        }

        // The groups follow the argument order (cold before hot here), not the column family ids.
        using AttributeGroupIterator it = db.NewAttributeGroupIterator(null, [cold, hot]);
        const string A = "a=cold[c:a-cold]hot[h:a-hot]", Bk = "b=cold[c:b-cold]", Ck = "c=cold[c:c-cold]hot[h:c-hot]", D = "d=hot[:plain]";
        it.SeekToFirst();
        Assert.AreEqual(A, Describe(it));
        it.Next();
        Assert.AreEqual(Bk, Describe(it));
        it.Next();
        Assert.AreEqual(Ck, Describe(it));
        // Direction change at a key both column families have.
        it.Prev();
        Assert.AreEqual(Bk, Describe(it));
        // ... and at a key only one has: the other child must not be left behind the current key.
        it.Next();
        Assert.AreEqual(Ck, Describe(it));
        it.Next();
        Assert.AreEqual(D, Describe(it));
        it.Prev();
        Assert.AreEqual(Ck, Describe(it));
        it.Prev();
        Assert.AreEqual(Bk, Describe(it));
        it.Prev();
        Assert.AreEqual(A, Describe(it));
        it.Prev();
        Assert.IsFalse(it.Valid);
        it.SeekForPrev(B("bb"));
        Assert.AreEqual(Bk, Describe(it));
        it.Next();
        Assert.AreEqual(Ck, Describe(it));
        it.SeekToLast();
        Assert.AreEqual(D, Describe(it));
        it.Next();
        Assert.IsFalse(it.Valid);

        // DBImpl::NewMultiCfIterator: "No Column Family was provided".
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.NewAttributeGroupIterator(null, []));
        Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
    }

    // ---- row cache ----------------------------------------------------------------------------------------------

    [TestMethod]
    public void RowCache_ShardCountFollowsCapacity()
    {
        // NewLRUCache(capacity) picks GetDefaultCacheShardBits(capacity) shards (cache/sharded_cache.cc:129-139: each at
        // least 512 KB, at most 2^6) of (capacity + n - 1) / n bytes. The port always split the capacity into 16
        // shards, so a 256 KB row cache could not hold an entry above 16 KB that upstream caches.
        Assert.AreEqual(0, Managed.Cache.RowCache.DefaultShardBits(0));
        Assert.AreEqual(0, Managed.Cache.RowCache.DefaultShardBits((1 << 20) - 1));
        Assert.AreEqual(1, Managed.Cache.RowCache.DefaultShardBits(1 << 20));
        Assert.AreEqual(4, Managed.Cache.RowCache.DefaultShardBits(8 << 20));
        Assert.AreEqual(6, Managed.Cache.RowCache.DefaultShardBits(32 << 20));
        Assert.AreEqual(6, Managed.Cache.RowCache.DefaultShardBits(1L << 40));

        var cache = new Managed.Cache.RowCache(256 * 1024);
        Assert.AreEqual(1, cache.NumShards);
        byte[] big = new byte[100 * 1024];
        cache.Insert(B("k1"), big);
        Assert.IsTrue(cache.TryGet(B("k1"), out byte[]? v));
        Assert.AreEqual(big.Length, v.Length);
        // LRU eviction within the one shard: the third 100 KB entry evicts the least recently used one (k2: k1 was
        // just read).
        cache.Insert(B("k2"), big);
        Assert.IsTrue(cache.TryGet(B("k1"), out _));
        cache.Insert(B("k3"), big);
        Assert.IsFalse(cache.TryGet(B("k2"), out _));
        Assert.IsTrue(cache.TryGet(B("k1"), out _));
        Assert.IsTrue(cache.TryGet(B("k3"), out _));
        Assert.AreEqual(2, cache.Count);
        Assert.IsTrue(cache.Usage <= cache.Capacity);
        // An entry larger than the cache is not kept (LRUCacheShard::InsertItem without a handle).
        cache.Insert(B("huge"), new byte[300 * 1024]);
        Assert.IsFalse(cache.TryGet(B("huge"), out _));
        // Replacing a key keeps one entry and its new charge.
        cache.Insert(B("k3"), new byte[10]);
        Assert.AreEqual(2, cache.Count);
        cache.Clear();
        Assert.AreEqual(0L, cache.Usage);
        Assert.AreEqual(0, cache.Count);
    }

    // ---- atomic flush request tickers ---------------------------------------------------------------------------

    [TestMethod]
    [Ignore("audit4: the recording site is SwitchMemtablesForFlushAsync in Db/DbImpl.Flush.cs (owned by another change); DbImpl.RecordAtomicFlushRequestReason is ready to be called there")]
    public void AtomicFlushRequestReasonTickers()
    {
        // DBImpl::EnqueuePendingFlush (db_impl_compaction_flush.cc:4548): every queued atomic FlushRequest records
        // RecordAtomicFlushRequestReason(stats_, flush_req.flush_reason): write_buffer_full / write_buffer_manager /
        // memtable_max_range_deletions, anything else (a manual flush here) as "other".
        MemoryFileSystem fs = NewFs();
        var st = new Statistics();
        DbOptions o = NewOptions(fs);
        o.Statistics = st;
        o.AtomicFlush = true;
        using RocksDatabase db = RocksDatabase.Open(DbPath, o);
        ColumnFamily cf = db.CreateColumnFamily("cf", new ColumnFamilyOptions());
        db.Put(B("a"), B("1"));
        cf.Put(B("b"), B("2"));
        db.Flush();
        Assert.AreEqual(1UL, st.GetTickerCount(Ticker.AtomicFlushRequestReasonOther));
        Assert.AreEqual(0UL, st.GetTickerCount(Ticker.AtomicFlushRequestReasonWriteBufferFull));
        Assert.AreEqual(0UL, st.GetTickerCount(Ticker.AtomicFlushRequestReasonWriteBufferManager));
    }
}
