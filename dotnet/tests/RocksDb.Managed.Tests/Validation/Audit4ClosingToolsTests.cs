using System.Buffers;
using RocksDb.Managed.Db;
using RocksDb.Managed.Monitoring;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4, slice ClosingTools (LOUDS trie, ldb / sst_dump tools, EVENT_LOG, ticker call sites, small option files):
/// regression tests for the divergences found against the C++ sources. The differential tests against native
/// librocksdb are in Audit4ClosingToolsCompatTests.
/// </summary>
[TestClass]
public sealed class Audit4ClosingToolsTests
{
    /// <summary>
    /// EventHelpers::LogAndNotifyTableFileCreationFinished (db/event_helpers.cc:151): "seqno_to_time_mapping" is "N/A"
    /// without a mapping, SeqnoToTimeMapping::ToHumanString ("seqno->time," per pair) of a decodable one and "Invalid"
    /// otherwise. The port wrote the placeholder "(mapping)".
    /// </summary>
    [TestMethod]
    public void TableFileCreationEventPrintsTheSeqnoToTimeMapping()
    {
        Assert.AreEqual("N/A", DbImpl.SeqnoToTimeMappingText([]));
        Assert.AreEqual("N/A", DbImpl.SeqnoToTimeMappingText(null));
        var mapping = new SeqnoToTimeMapping();
        mapping.AddUnenforced(10, 1000);
        mapping.AddUnenforced(25, 1700);
        var encoded = new ArrayBufferWriter<byte>();
        mapping.EncodeTo(encoded);
        Assert.AreEqual("10->1000,25->1700,", DbImpl.SeqnoToTimeMappingText(encoded.WrittenSpan.ToArray()));
        Assert.AreEqual("Invalid", DbImpl.SeqnoToTimeMappingText([0xff]));
    }

    /// <summary>
    /// CompactionJob::LogCompaction writes "score" with JSONWriter's operator&lt;&lt; (logging/event_logger.h), an
    /// std::ostream insertion of a double: printf "%g", six significant digits. The port printed six decimals, so a
    /// score of 8/3 came out as 2.666667 where native logs 2.66667.
    /// </summary>
    [TestMethod]
    public void CompactionStartedScoreIsFormattedLikeAnOstreamDouble()
    {
        (double Value, string Text)[] cases =
        [
            (1, "1"), (0.5, "0.5"), (8.0 / 3, "2.66667"), (123.456789, "123.457"), (0.0001, "0.0001"), (0.00001234, "1.234e-05"),
            (1234567.0, "1.23457e+06"), (0, "0"), (100000, "100000"),
        ];
        foreach ((double value, string text) in cases) Assert.AreEqual(text, EventLogJson.FormatDouble(value));
    }

    /// <summary>
    /// MemTable::MultiGet (db/memtable.cc:1959) ticks MEMTABLE_HIT per key the memtable finished (a value or a
    /// tombstone) and DBImpl::MultiGetImpl (db_impl_sync_and_async.h:601) ticks MEMTABLE_MISS with the keys left for
    /// the table files. The port's MultiGet counted neither.
    /// </summary>
    [TestMethod]
    public void MultiGetCountsMemtableHitsAndMisses()
    {
        var stats = new Statistics();
        DbOptions o = NewOptions(NewFs());
        o.Statistics = stats;
        using RocksDatabase db = RocksDatabase.Open(DbPath, o);
        db.Put(B("a"), B("1"));
        db.Put(B("b"), B("2"));
        db.Flush();
        db.Put(B("c"), B("3"));
        db.Delete(B("a")); // a tombstone in the memtable finishes the key too
        db.MultiGet([B("a"), B("b"), B("c"), B("zz")]);
        Assert.AreEqual(2UL, stats.GetTickerCount(Ticker.MemtableHit), "c and the tombstone of a");
        Assert.AreEqual(2UL, stats.GetTickerCount(Ticker.MemtableMiss), "b and zz go to the files");
    }

    /// <summary>
    /// TableCache::GetTableReader (db/table_cache.cc:133) ticks NO_FILE_OPENS for every table file it opens; DBIter's
    /// LocalStatistics (db/db_iter.h:142, ResetInternalKeysSkippedCounter) reports the internal keys an iterator
    /// passed over as NUMBER_ITER_SKIP when it is destroyed. The port never incremented either.
    /// </summary>
    [TestMethod]
    public void FileOpensAndIteratorSkipsAreCounted()
    {
        var stats = new Statistics();
        DbOptions o = NewOptions(NewFs());
        o.Statistics = stats;
        using RocksDatabase db = RocksDatabase.Open(DbPath, o);
        db.Put(B("a"), B("1"));
        db.Put(B("b"), B("1"));
        db.Flush();
        db.Put(B("c"), B("1"));
        db.Flush();
        Assert.AreEqual(1, db.Get(B("a"))!.Length);
        Assert.AreEqual(2UL, stats.GetTickerCount(Ticker.NoFileOpens), "one open per flushed file");
        // b is overwritten three times and deleted while a snapshot keeps every version: Seek(b) passes the tombstone
        // and the 4 hidden entries of b and stops on c.
        using (Snapshot keep = db.GetSnapshot())
        {
            for (int i = 0; i < 3; i++) db.Put(B("b"), B("v" + i));
            db.Delete(B("b"));
            using (Iterator it = db.NewIterator())
            {
                it.Seek(B("b"));
                Assert.IsTrue(it.Valid);
                CollectionAssert.AreEqual(B("c"), it.Key.ToArray());
            }
        }
        // num_internal_keys_skipped_ counts every internal entry FindNextUserEntryInternal looked at (5 of b and c);
        // ResetInternalKeysSkippedCounter takes one off for the entry a valid iterator stands on.
        Assert.AreEqual(5UL, stats.GetTickerCount(Ticker.NumberIterSkip));
    }

    // ---- sst_dump (tools/sst_dump_tool.cc, table/sst_file_dumper.cc) ------------------------------------------

    private static (int Rc, string Out, string Err) Tool(RocksDb.Managed.Env.IFileSystem fs, params string[] args)
    {
        var o = new StringWriter { NewLine = "\n" };
        var e = new StringWriter { NewLine = "\n" };
        int rc = RocksDb.Managed.Tools.ToolMain.Run(args, fs, o, e);
        return (rc, o.ToString(), e.ToString());
    }

    private static string OnlySst(RocksDb.Managed.Env.MemoryFileSystem fs)
    {
        string? sst = null;
        foreach (string child in fs.GetChildren(DbPath))
        {
            if (!child.EndsWith(".sst", StringComparison.Ordinal)) continue;
            Assert.IsNull(sst, "one table file expected");
            sst = DbPath + "/" + child;
        }
        return sst ?? throw new AssertFailedException("no table file");
    }

    /// <summary>
    /// SstFileDumper::ReadSequential (table/sst_file_dumper.cc:955): a kTypeValuePreferredSeqno entry (TimedPut) is
    /// printed as "key => value, preferred_seqno" (ParsePackedValueWithSeqno), not with the packed 8-byte seqno as
    /// part of the value. remaining.md R40(c).
    /// </summary>
    [TestMethod]
    public void SstDumpScanPrintsTimedPutValueAndPreferredSeqno()
    {
        RocksDb.Managed.Env.MemoryFileSystem fs = NewFs();
        var cf = new ColumnFamilyOptions { PreserveInternalTimeSeconds = 1_000_000 };
        ulong now = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cf))
        {
            db.Put(B("plain"), B("pv"));
            using (WriteBatch batch = db.CreateWriteBatch())
            {
                // Written "half a window ago": the flush keeps the entry as a TimedPut with a smaller preferred seqno.
                batch.TimedPut(db.DefaultColumnFamily, B("timed"), B("tv"), now - 500_000);
                db.Write(batch);
            }
            db.Flush();
        }
        string sst = OnlySst(fs);
        (int rc, string o, string e) = Tool(fs, "sst_dump", "--file=" + sst, "--command=scan");
        Assert.AreEqual(0, rc, e);
        string[] lines = o.Split('\n');
        string timed = Array.Find(lines, l => l.StartsWith("'timed' ", StringComparison.Ordinal)) ?? throw new AssertFailedException(o);
        // "'timed' seq:S, type:24 => tv, P" with 0 < P < S (type 0x18 = kTypeValuePreferredSeqno).
        System.Text.RegularExpressions.Match m = System.Text.RegularExpressions.Regex.Match(timed, "^'timed' seq:(\\d+), type:24 => tv, (\\d+)$");
        Assert.IsTrue(m.Success, timed);
        ulong seq = ulong.Parse(m.Groups[1].Value, System.Globalization.CultureInfo.InvariantCulture);
        ulong preferred = ulong.Parse(m.Groups[2].Value, System.Globalization.CultureInfo.InvariantCulture);
        Assert.IsTrue(preferred < seq, timed);
        string plain = Array.Find(lines, l => l.StartsWith("'plain' ", StringComparison.Ordinal))!;
        StringAssert.EndsWith(plain, ", type:1 => pv");
        // --output_hex prints the unpacked value in hex and the seqno in decimal.
        (rc, o, e) = Tool(fs, "sst_dump", "--file=" + sst, "--command=scan", "--output_hex");
        Assert.AreEqual(0, rc, e);
        StringAssert.Contains(o, "'74696D6564' seq:" + seq + ", type:24 => 7476, " + preferred + "\n");
    }

    /// <summary>
    /// SstFileDumper::ReadSequential with --decode_blob_index (table/sst_file_dumper.cc:942, 968): a kTypeBlobIndex
    /// entry is printed as BlobIndex::DebugString ("[blob ref] file:N offset:O size:S compression: NoCompression");
    /// without the flag the raw index bytes are printed. The port accepted the flag and ignored it.
    /// </summary>
    [TestMethod]
    public void SstDumpDecodeBlobIndexPrintsTheBlobReference()
    {
        RocksDb.Managed.Env.MemoryFileSystem fs = NewFs();
        var cf = new ColumnFamilyOptions { EnableBlobFiles = true, MinBlobSize = 10, Compression = RocksDb.Managed.Compression.CompressionType.None };
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cf))
        {
            db.Put(B("big"), B(new string('x', 100)));
            db.Put(B("small"), B("s"));
            db.Flush();
        }
        string sst = OnlySst(fs);
        ulong blobFile = 0;
        foreach (string child in fs.GetChildren(DbPath))
        {
            if (child.EndsWith(".blob", StringComparison.Ordinal)) blobFile = ulong.Parse(child[..^5], System.Globalization.CultureInfo.InvariantCulture);
        }
        Assert.AreNotEqual(0UL, blobFile);
        (int rc, string o, string e) = Tool(fs, "sst_dump", "--file=" + sst, "--command=scan", "--decode_blob_index");
        Assert.AreEqual(0, rc, e);
        // BlobLogHeader::kSize (30) + the record header (32) + the key: the blob value's offset in the first record.
        StringAssert.Contains(o, "'big' seq:1, type:17 => [blob ref] file:" + blobFile + " offset:" + (30 + 32 + 3) + " size:100 compression: NoCompression\n");
        StringAssert.Contains(o, "'small' seq:2, type:1 => s\n");
        (rc, o, e) = Tool(fs, "sst_dump", "--file=" + sst, "--command=scan");
        Assert.AreEqual(0, rc, e);
        Assert.IsFalse(o.Contains("[blob ref]", StringComparison.Ordinal), o);
    }

    // ---- tracing (remaining.md R37) ----------------------------------------------------------------------------

    private static List<RocksDb.Managed.Tracing.TraceType> TraceTypes(RocksDb.Managed.Env.MemoryFileSystem fs, string path)
    {
        var types = new List<RocksDb.Managed.Tracing.TraceType>();
        using RocksDb.Managed.Tracing.TraceReader reader = RocksDb.Managed.Tracing.TraceReader.OpenFile(fs, path);
        var buffer = new ArrayBufferWriter<byte>();
        while (true)
        {
            buffer.Clear();
            if (!reader.TryRead(buffer)) break;
            Assert.IsTrue(RocksDb.Managed.Tracing.TraceFormat.TryDecodeTrace(buffer.WrittenMemory, out RocksDb.Managed.Tracing.TraceEntry trace));
            types.Add(trace.Type);
        }
        return types;
    }

    /// <summary>
    /// DBImpl::StartTrace (db/db_impl/db_impl.cc:8909) replaces a running tracer: ~Tracer only releases the writer, so
    /// the replaced trace file ends without a kTraceEnd record; only DBImpl::EndTrace (Tracer::Close -> WriteFooter)
    /// writes one. The port ended the replaced trace with a footer.
    /// </summary>
    [TestMethod]
    public void StartTraceOverARunningTraceWritesNoFooterToTheOldFile()
    {
        RocksDb.Managed.Env.MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        db.StartTrace(new RocksDb.Managed.Tracing.TraceOptions(), "/trace1");
        db.Put(B("a"), B("1"));
        db.StartTrace(new RocksDb.Managed.Tracing.TraceOptions(), "/trace2");
        db.Put(B("b"), B("2"));
        db.EndTrace();
        List<RocksDb.Managed.Tracing.TraceType> first = TraceTypes(fs, "/trace1"), second = TraceTypes(fs, "/trace2");
        CollectionAssert.AreEqual(new[] { RocksDb.Managed.Tracing.TraceType.Begin, RocksDb.Managed.Tracing.TraceType.Write }, first);
        CollectionAssert.AreEqual(
            new[] { RocksDb.Managed.Tracing.TraceType.Begin, RocksDb.Managed.Tracing.TraceType.Write, RocksDb.Managed.Tracing.TraceType.End }, second);
    }

    /// <summary>
    /// TryParseTimeRangeString (util/string_util.cc:456) splits with StringSplit, whose std::getline loop yields no
    /// final empty piece: "01:00-02:00-" is two pieces and a valid daily_offpeak_time_utc. The port's string.Split
    /// saw three and rejected it.
    /// </summary>
    [TestMethod]
    public void OffpeakTimeRangeWithATrailingDashParsesLikeStringSplit()
    {
        Assert.IsTrue(RocksDb.Managed.Options.OffpeakTimeOption.TryParseTimeRange("01:00-02:00-", out int start, out int end));
        Assert.AreEqual(3600, start);
        Assert.AreEqual(7200, end);
        Assert.IsTrue(RocksDb.Managed.Options.OffpeakTimeOption.TryParseTimeRange("23:30-04:30", out start, out end));
        Assert.AreEqual(23 * 3600 + 1800, start);
        Assert.AreEqual(4 * 3600 + 1800, end);
        foreach (string bad in new[] { "01:00-", "-", "01:00-02:00-03:00", "-01:00-02:00", "01:00-02:00--", "24:00-01:00", "01:60-02:00", "01:00 -02:00", "1-2" })
        {
            Assert.IsFalse(RocksDb.Managed.Options.OffpeakTimeOption.TryParseTimeRange(bad, out _, out _), bad);
        }
        Assert.IsTrue(RocksDb.Managed.Options.OffpeakTimeOption.TryParseTimeRange("", out start, out end));
        Assert.AreEqual(0, start + end);
    }
}
