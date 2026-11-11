using RocksDb.Managed.Db;
using System.Buffers;
using RocksDb.Managed.Db.Blob;
using RocksDb.Managed.Db.Wide;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4, slice Residuals: the last residual items of the audit (job I/O tickers, the compaction CPU clock, the
/// zero-size blob range, the compaction service serialization, the automatic HyperClockCache table).
/// </summary>
[TestClass]
public sealed class Audit4ResidualsTests
{
    private const string DbPath = "/audit4residuals";

    private static ColumnFamilyOptions NoAutoCompaction() =>
        new() { DisableAutoCompactions = true, Compression = RocksDb.Managed.Compression.CompressionType.None };

    private static void FillAndFlush(RocksDatabase db, int from, int count, int round)
    {
        for (int i = from; i < from + count; i++) db.Put(Key(i), Value(i, round));
        db.Flush();
    }

    private static long ManifestSize(MemoryFileSystem fs)
    {
        long size = 0;
        foreach (string name in fs.GetChildren(DbPath))
        {
            string file = name[(name.LastIndexOf('/') + 1)..];
            if (file.StartsWith("MANIFEST-", StringComparison.Ordinal)) size += fs.GetFileSize(DbPath + "/" + file);
        }
        return size;
    }

    /// <summary>
    /// CompactionJob::VerifyOutputFiles / verify_table (db/compaction/compaction_job.cc ~1063): "Verify that the table
    /// is usable" -- every output is opened through the table cache on the compaction thread whatever
    /// verify_output_flags says, so the open (NO_FILE_OPENS) and its reads (IOSTATS(bytes_read), COMPACT_READ_BYTES in
    /// RecordCompactionIOStats) belong to the compaction. The port only opened the output when a verification flag was
    /// set: the file was opened by the first read after the compaction and COMPACT_READ_BYTES missed the open.
    /// </summary>
    [TestMethod]
    public void CompactionOpensItsOutputThroughTheTableCache()
    {
        MemoryFileSystem fs = NewFs();
        var stats = new Statistics();
        DbOptions o = NewOptions(fs);
        o.Statistics = stats;
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, NoAutoCompaction());
        FillAndFlush(db, 0, 200, 0);
        FillAndFlush(db, 100, 200, 1);
        // Both inputs are in the table cache and every data block is in the block cache: the compaction reads nothing
        // from its inputs.
        for (int i = 0; i < 300; i++) Assert.IsNotNull(db.Get(Key(i)));
        ulong opensBefore = stats.GetTickerCount(Ticker.NoFileOpens);
        Assert.AreEqual(0UL, stats.GetTickerCount(Ticker.CompactReadBytes));
        db.CompactRange(null, null);
        Assert.AreEqual(opensBefore + 1, stats.GetTickerCount(Ticker.NoFileOpens), "the output is opened by the compaction");
        ulong read = stats.GetTickerCount(Ticker.CompactReadBytes);
        // The open reads at least the footer (53 bytes with format_version >= 6) and the properties block.
        Assert.IsTrue(read > 53, read.ToString());
        Assert.IsNotNull(db.Get(Key(5)));
        Assert.AreEqual(opensBefore + 1, stats.GetTickerCount(Ticker.NoFileOpens), "a read finds the output in the table cache");
    }

    /// <summary>
    /// FlushJob::Run (db/flush_job.cc): RecordFlushIOStats() runs after WriteLevel0Table (the table file) and again after
    /// TryInstallMemtableFlushResults, where IOSTATS(bytes_written) of the flush thread holds the MANIFEST record its
    /// LogAndApply appended (WritableFileWriter counts every file). FLUSH_WRITE_BYTES is the table file size plus the
    /// growth of the MANIFEST; the port only counted the table file.
    /// </summary>
    [TestMethod]
    public void FlushWriteBytesIncludesTheManifestRecordOfTheInstall()
    {
        MemoryFileSystem fs = NewFs();
        var stats = new Statistics();
        DbOptions o = NewOptions(fs);
        o.Statistics = stats;
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, NoAutoCompaction());
        ulong expected = 0;
        for (int round = 0; round < 3; round++)
        {
            long manifestBefore = ManifestSize(fs);
            FillAndFlush(db, round * 50, 100, round);
            long manifestRecord = ManifestSize(fs) - manifestBefore;
            Assert.IsTrue(manifestRecord > 7, manifestRecord.ToString()); // at least a log record header
            expected += (ulong)manifestRecord;
        }
        foreach (LiveFileMetadata f in db.GetLiveFilesMetadata()) expected += f.Size;
        Assert.AreEqual(expected, stats.GetTickerCount(Ticker.FlushWriteBytes));
    }

    /// <summary>
    /// CompactionJob::ProcessKeyValueCompaction / UpdateSubcompactionJobStatsIncrementally / FinalizeSubcompactionJobStats
    /// (db/compaction/compaction_job.cc ~1944-2006): COMPACTION_CPU_TOTAL_TIME and CompactionJobStats::cpu_micros are
    /// db_options_.clock->CPUMicros() deltas of the compaction thread (clock_gettime(CLOCK_THREAD_CPUTIME_ID)). The port
    /// had no thread CPU clock and never recorded the ticker.
    /// </summary>
    [TestMethod]
    public void CompactionRecordsTheCpuTimeOfItsThread()
    {
        if (!OperatingSystem.IsLinux() && !OperatingSystem.IsMacOS() && !OperatingSystem.IsWindows()) Assert.Inconclusive("no thread CPU clock");
        Assert.IsTrue(ThreadCpuClock.IsSupported);
        // The clock itself: advances while the thread burns CPU.
        ulong t0 = ThreadCpuClock.CpuMicros();
        ulong spin = 0;
        ulong t1 = t0;
        for (int i = 0; i < 2000 && t1 == t0; i++)
        {
            for (int j = 0; j < 200_000; j++) spin = spin * 31 + (ulong)j;
            t1 = ThreadCpuClock.CpuMicros();
        }
        Assert.IsTrue(t1 > t0, spin.ToString());

        MemoryFileSystem fs = NewFs();
        var stats = new Statistics();
        DbOptions o = NewOptions(fs);
        o.Statistics = stats;
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, NoAutoCompaction());
        for (int round = 0; round < 4; round++) FillAndFlush(db, round * 1000, 20_000, round);
        Assert.AreEqual(0UL, stats.GetTickerCount(Ticker.CompactionCpuTotalTime));
        long wall = System.Diagnostics.Stopwatch.GetTimestamp();
        db.CompactRange(null, null);
        ulong wallMicros = (ulong)System.Diagnostics.Stopwatch.GetElapsedTime(wall).TotalMicroseconds;
        ulong cpu = stats.GetTickerCount(Ticker.CompactionCpuTotalTime);
        Assert.IsTrue(cpu > 0, "COMPACTION_CPU_TOTAL_TIME is not recorded");
        // One compaction thread: its CPU time cannot exceed the elapsed time of the whole call (plus clock granularity).
        Assert.IsTrue(cpu <= wallMicros + 20_000, cpu + " > " + wallMicros);
    }

    /// <summary>
    /// ReadPathBlobResolver::ResolveColumnRange / ClassifyColumnRange (db/wide/read_path_blob_resolver.cc ~197, ~271):
    /// "strict_subrange = range_offset > 0 || range_length &lt; value_size". For a blob reference of size 0 a read at
    /// offset 0 is never a strict sub-range, so it takes the whole (verifying) read and reports what that read finds;
    /// only an offset past the start is the empty range that needs no I/O. BlobFileBuilder::Add never writes an empty
    /// blob (db/blob/blob_file_builder.cc:122, same guard in the port's BlobFileBuilder.Add), so the reference is
    /// hand-built: a v2 entity through WriteBatchInternal::PutEntitySerialized pointing into the blob file header.
    /// The port answered "empty" without reading for (offset 0, length 0) and for a zero-length destination.
    /// </summary>
    [TestMethod]
    public void ZeroSizeBlobReferenceAtOffsetZeroTakesTheWholeRead()
    {
        MemoryFileSystem fs = NewFs();
        var cfo = new ColumnFamilyOptions { EnableBlobFiles = true, MinBlobSize = 32 };
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        db.PutEntity(B("a"), [new WideColumn(B(""), B("d")), new WideColumn(B("big"), B(new string('b', 200)))]);
        db.Flush();
        ulong blobFile = 0;
        foreach (string name in fs.GetChildren(DbPath))
        {
            string file = name[(name.LastIndexOf('/') + 1)..];
            if (file.EndsWith(".blob", StringComparison.Ordinal)) blobFile = ulong.Parse(file[..^5], System.Globalization.CultureInfo.InvariantCulture);
        }
        Assert.AreNotEqual(0UL, blobFile);

        // Offset 5 is inside the blob file header: any read of the record is "Invalid blob offset".
        var index = new ArrayBufferWriter<byte>();
        BlobIndex.EncodeBlob(index, blobFile, 5, 0, RocksDb.Managed.Compression.CompressionType.None);
        using var columns = new EntityColumnList();
        columns.Add(B(""), B("d"), isBlobIndex: false);
        columns.Add(B("z"), index.WrittenSpan, isBlobIndex: true);
        var entity = new ArrayBufferWriter<byte>();
        WideColumnSerialization.SerializeV2(columns, entity);
        using (WriteBatch wb = db.CreateWriteBatch())
        {
            wb.PutEntitySerialized(0, B("k"), entity.WrittenSpan);
            db.Write(wb);
        }
        db.Flush();

        using var lazy = new LazyWideColumns();
        Assert.IsTrue(db.DefaultColumnFamily.TryGetEntityLazy(null, B("k"), lazy));
        int z = lazy.IndexOf("z"u8);
        Assert.IsTrue(z >= 0);
        LazyColumnReadRequest[] reads =
        [
            new(z, 1, 5),                                  // offset past the (empty) value: empty, no I/O
            new(z, 0, 0),                                  // offset 0: not a strict sub-range -> whole read
            new(z, 0, LazyColumnReadRequest.WholeColumn),  // the whole column
        ];
        lazy.MultiResolve(reads);
        Assert.IsTrue(reads[0].Succeeded);
        Assert.AreEqual(0, reads[0].Result.Length);
        for (int i = 1; i < 3; i++)
        {
            Assert.IsFalse(reads[i].Succeeded, i.ToString());
            Assert.AreEqual(StatusCode.Corruption, reads[i].Error!.Code, i.ToString());
        }
        // LazyWideColumns::ResolveColumnRange (the single-read form).
        Assert.AreEqual(0, lazy.ReadColumnRange(z, 1, new byte[4]));
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => lazy.ReadColumnRange(z, 0, Span<byte>.Empty));
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
        ex = Assert.ThrowsExactly<RocksDbException>(() => lazy.ReadColumnRange(z, 0, new byte[4]));
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
    }

    private static byte[] Wire(string optionsString) => [1, 0, 0, 0, .. System.Text.Encoding.UTF8.GetBytes(optionsString)];

    /// <summary>
    /// CompactionServiceResult::Read / CompactionServiceInput::Read (db/compaction/compaction_service_job.cc ~1930-2040)
    /// parse through OptionTypeInfo::Parse (options/options_helper.cc ~1214), which turns the std::out_of_range of
    /// ParseInt (kInt: output_level, proximal_level, counts, ...; util/string_util.cc:375 std::stoi) and of ParseUint8
    /// (the kUInt8T code / subcode / severity of the status adapter, string_util.cc:295) into InvalidArgument. The
    /// port let a System.OverflowException escape for an int field and silently truncated the status bytes (code 256
    /// read as 0 = OK).
    /// </summary>
    [TestMethod]
    public void CompactionServiceFormatRejectsOutOfRangeIntegers()
    {
        foreach (string body in new[]
                 {
                     "output_level=99999999999;",
                     "output_level=-2147483649;",
                     "status={code=256;subcode=0;severity=0;message=;};",
                     "status={code=0;subcode=300;severity=0;message=;};",
                     "status={code=0;subcode=0;severity=256;message=;};",
                     "internal_stats={output_level_stats={counts=1:4294967296;};};",
                 })
        {
            RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => CompactionServiceFormat.ReadResult(Wire(body)), body);
            Assert.AreEqual(StatusCode.InvalidArgument, ex.Code, body);
        }
        RocksDbException input = Assert.ThrowsExactly<RocksDbException>(() => CompactionServiceFormat.ReadInput(Wire("proximal_level=2147483648;")));
        Assert.AreEqual(StatusCode.InvalidArgument, input.Code);

        // The limits themselves parse (and unknown fields are ignored: ignore_unknown_options).
        CompactionServiceResult r = CompactionServiceFormat.ReadResult(Wire(
            "output_level=2147483647;status={code=255;subcode=14;severity=4;message=4142;};some_future_field={a=1;b={c=2}};"
            + "internal_stats={output_level_stats={counts=-2147483648:7;};has_proximal_level_output=true;};"));
        Assert.AreEqual(int.MaxValue, r.OutputLevel);
        Assert.AreEqual(255, (int)r.StatusCode);
        Assert.AreEqual(14, (int)r.StatusSubCode);
        Assert.AreEqual((byte)4, r.StatusSeverity);
        Assert.AreEqual("AB", r.StatusMessage);
        Assert.AreEqual(int.MinValue, r.OutputLevelStats.Counts[0]);
        Assert.AreEqual(7, r.OutputLevelStats.Counts[1]);
        Assert.AreEqual(0, r.OutputLevelStats.Counts[2]); // SizeTolerantDeserializeArray: missing elements are zero
        Assert.IsTrue(r.HasProximalLevelOutput);
    }

    /// <summary>
    /// AutoHyperClockTable::Lookup / Grow / SplitForGrow (cache/clock_cache.cc ~2700-3200): while the table grows a
    /// lookup follows the chains under construction and re-reads the head when it sees a changed shift, so an entry
    /// that is in the cache is always found. The port's grow rewrote the displacement counts lookups use to stop
    /// probing (and moved the slots to a new array) under running lookups: a lookup of a present, never evicted entry
    /// could miss during a grow.
    /// </summary>
    [TestMethod]
    public void AutoTableLookupDuringGrowthNeverMissesAPresentEntry()
    {
        var c = new RocksDb.Managed.Cache.HyperClockCache(new RocksDb.Managed.Cache.HyperClockCacheOptions
        {
            Capacity = 256L << 20, EstimatedEntryCharge = 0, NumShardBits = 0,
            MetadataChargePolicy = RocksDb.Managed.Cache.CacheMetadataChargePolicy.DontCharge,
        });
        Assert.IsTrue(c.IsAutoResizing);
        static RocksDb.Managed.Cache.CacheKey K(int k) => new(0xA4E51D, (ulong)k);
        const int stable = 4000, more = 300_000;
        // Far below the capacity: nothing is ever evicted, the table only grows.
        for (int i = 0; i < stable; i++) Assert.IsTrue(c.Insert(K(i), RocksDb.Managed.Cache.CacheValue.FromObject("s"), 100, RocksDb.Managed.Cache.CachePriority.Low));
        int tableBefore = c.TableSize;
        int done = 0;
        long misses = 0, lookups = 0;
        Task reader = Task.Run(() =>
        {
            int i = 0;
            while (Volatile.Read(ref done) == 0)
            {
                if (c.TryLookup(K(i), out RocksDb.Managed.Cache.CachedBlock h)) h.Dispose();
                else misses++;
                lookups++;
                if (++i == stable) i = 0;
            }
        });
        try
        {
            for (int i = 0; i < more; i++) c.Insert(K(stable + i), RocksDb.Managed.Cache.CacheValue.FromObject("m"), 100, RocksDb.Managed.Cache.CachePriority.Low);
        }
        finally
        {
            Volatile.Write(ref done, 1);
            reader.Wait();
        }
        Assert.IsTrue(c.TableSize >= 16 * tableBefore, c.TableSize.ToString()); // several grows happened under the reader
        Assert.AreEqual(stable + more, c.Count);
        Assert.AreEqual(0L, misses, "misses in " + lookups + " lookups");
    }
}
