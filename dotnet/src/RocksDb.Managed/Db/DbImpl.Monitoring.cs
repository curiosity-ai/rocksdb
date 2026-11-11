using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using RocksDb.Managed.Logging;

namespace RocksDb.Managed.Db;

/// <summary>
/// Info LOG, statistics and internal-stats hooks of the DB. Ports the logging parts of DBImpl::Open
/// (DumpRocksDBBuildVersion, DumpDBFileSummary, DBOptions::Dump / ColumnFamilyOptions::Dump), the EventLogger records of
/// FlushJob / CompactionJob (db/event_helpers.cc), DBImpl::DumpStats (periodic "DUMPING STATS"), and the ticker /
/// histogram recording of the read, write, flush and compaction paths (RecordTick / StopWatch call sites).
/// All hooks are cheap when statistics and logging are off (a null check).
/// </summary>
internal sealed partial class DbImpl
{
    private InfoLogger? _infoLog;
    private bool _ownsInfoLog;
    private Timer? _statsDumpTimer;
    private int _nextJobId;
    private readonly DbWriteStats _writeStats = new();

    /// <summary>The info logger (user-supplied or the DB's LOG file), or null.</summary>
    internal InfoLogger? InfoLog => _infoLog;

    /// <summary>Statistics of this DB, or null.</summary>
    internal Statistics? Stats => Options.Statistics;

    internal DbWriteStats WriteStats => _writeStats;

    internal int NewJobId() => Interlocked.Increment(ref _nextJobId);

    /// <summary>
    /// SanitizeOptions / CreateLoggerFromOptions: uses <see cref="DbOptions.InfoLog"/> or creates the DB's LOG file. Logging
    /// failures never fail the open.
    /// </summary>
    private void InitInfoLog()
    {
        // Upstream writes the open header (version, DB summary, options) in the DBImpl constructor, before Recover; the
        // managed header needs the recovered state, so events of the recovery are held back until it was written.
        _deferredEvents = [];
        if (Options.InfoLog is { } user)
        {
            _infoLog = user;
            return;
        }
        try
        {
            _infoLog = FileInfoLogger.Create(FileSystem, DbName, Options);
            _ownsInfoLog = true;
        }
        catch (Exception e) when (e is IOException or RocksDbException or UnauthorizedAccessException)
        {
            _infoLog = null;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void LogInfo(string message) => _infoLog?.Log(InfoLogLevel.Info, message);

    internal void LogWarn(string message) => _infoLog?.Log(InfoLogLevel.Warn, message);

    internal void LogError(string message) => _infoLog?.Log(InfoLogLevel.Error, message);

    private void LogHeader(string message) => _infoLog?.Log(InfoLogLevel.Header, message);

    /// <summary>"EVENT_LOG_v1 {...}" (EventLogger::Log with a JSONWriter): the time and the given fields.</summary>
    private void LogEvent(string fields)
    {
        if (BeginEvent() is not { } sb) return;
        sb.Append(fields);
        EndEvent(sb);
    }

    /// <summary>Whether INFO messages (and EVENT_LOG records) are logged: nothing is formatted when they are not.</summary>
    private bool InfoLogEnabled => _infoLog is { } log && log.IsEnabled(InfoLogLevel.Info);

    // The builder INFO lines and events are formatted into (one per thread: flushes, compactions and file deletions log
    // from their own threads). Taken while in use, so a nested line (none today) would just use a new builder.
    [ThreadStatic] private static StringBuilder? t_logLine;

    /// <summary>A cleared builder for one INFO line, or null when INFO messages are not logged. Pass it to <see cref="EndInfoLine"/>.</summary>
    private StringBuilder? BeginInfoLine()
    {
        if (!InfoLogEnabled) return null;
        StringBuilder sb = t_logLine ?? new StringBuilder(1024);
        t_logLine = null;
        sb.Clear();
        return sb;
    }

    /// <summary>Logs the line built in <paramref name="sb"/> (from <see cref="BeginInfoLine"/>) and keeps the builder for the next line.</summary>
    private void EndInfoLine(StringBuilder sb)
    {
        _infoLog?.LogInfo(sb);
        ReleaseLogLine(sb);
    }

    private static void ReleaseLogLine(StringBuilder sb)
    {
        // Don't keep a builder that grew for an unusually large line (e.g. a compaction of thousands of files).
        if (sb.Capacity <= 16 * 1024) t_logLine = sb;
    }

    /// <summary>
    /// <see cref="BeginInfoLine"/> with the event prefix (<c>EVENT_LOG_v1 {"time_micros": N, </c>) written; append the
    /// fields and pass it to <see cref="EndEvent"/>.
    /// </summary>
    private StringBuilder? BeginEvent()
    {
        if (BeginInfoLine() is not { } sb) return null;
        ulong micros = (ulong)(DateTime.UtcNow - DateTime.UnixEpoch).Ticks / 10;
        sb.Append("EVENT_LOG_v1 {\"time_micros\": ").Append(micros).Append(", ");
        return sb;
    }

    /// <summary>Closes and logs an event from <see cref="BeginEvent"/> (held back while the DB is still opening).</summary>
    private void EndEvent(StringBuilder sb)
    {
        sb.Append('}');
        List<string>? deferred = _deferredEvents;
        if (deferred is not null)
        {
            lock (deferred)
            {
                if (_deferredEvents is not null)
                {
                    deferred.Add(sb.ToString());
                    ReleaseLogLine(sb);
                    return;
                }
            }
        }
        EndInfoLine(sb);
    }

    // Events logged during open before the header (see InitInfoLog); null once written.
    private List<string>? _deferredEvents;

    /// <summary>Writes the events held back during open (after the open header, or when the open failed).</summary>
    private void FlushDeferredEvents()
    {
        List<string>? deferred = _deferredEvents;
        if (deferred is null) return;
        lock (deferred)
        {
            _deferredEvents = null;
            foreach (string line in deferred) _infoLog?.Log(InfoLogLevel.Info, line);
        }
    }

    /// <summary>Status::ToString of a file operation's outcome ("OK", or the error).</summary>
    private static string StatusString(Exception? failure) =>
        failure is null ? "OK" : failure is RocksDbException r ? r.Message : "IO error: " + failure.Message;

    /// <summary>
    /// EventHelpers::LogAndNotifyTableFileDeletion / LogAndNotifyBlobFileDeletion (db/event_helpers.cc), logging part:
    /// <c>{"time_micros", "job", "event": "table_file_deletion" | "blob_file_deletion", "file_number", "db_session_id",
    /// "status"}</c>.
    /// </summary>
    private void LogFileDeletionEvent(string eventName, int jobId, ulong fileNumber, Exception? failure)
    {
        if (BeginEvent() is not { } sb) return;
        sb.Append("\"job\": ").Append(jobId).Append(", \"event\": \"").Append(eventName).Append("\", \"file_number\": ").Append(fileNumber)
            .Append(", \"db_session_id\": ");
        sb.AppendJsonString(Identity.SessionId).Append(", \"status\": ");
        sb.AppendJsonString(StatusString(failure));
        EndEvent(sb);
    }

    /// <summary>
    /// EventHelpers::LogAndNotifyBlobFileCreationFinished, logging part: <c>{"time_micros", "cf_name", "job", "event":
    /// "blob_file_creation", "file_number", "total_blob_count", "total_blob_bytes", "file_checksum",
    /// "file_checksum_func_name", "status"}</c>.
    /// </summary>
    internal void LogBlobFileCreationEvent(string cfName, int jobId, ulong fileNumber, ulong totalBlobCount, ulong totalBlobBytes,
        string fileChecksum, string fileChecksumFuncName, Exception? failure)
    {
        if (BeginEvent() is not { } sb) return;
        sb.Append("\"cf_name\": ").AppendJsonString(cfName).Append(", \"job\": ").Append(jobId)
            .Append(", \"event\": \"blob_file_creation\", \"file_number\": ").Append(fileNumber)
            .Append(", \"total_blob_count\": ").Append(totalBlobCount)
            .Append(", \"total_blob_bytes\": ").Append(totalBlobBytes)
            .Append(", \"file_checksum\": ");
        sb.AppendJsonString(fileChecksum).Append(", \"file_checksum_func_name\": ");
        sb.AppendJsonString(fileChecksumFuncName).Append(", \"status\": ");
        sb.AppendJsonString(StatusString(failure));
        EndEvent(sb);
    }

    /// <summary>
    /// The blob_file_creation events of a flush or compaction (BlobFileCompletionCallback::OnBlobFileCompleted, once per
    /// finished blob file; logged here when the job installs its result).
    /// </summary>
    internal void LogBlobFileCreationEvents(ColumnFamilyData cfd, int jobId, List<BlobFileAddition> additions)
    {
        if (!InfoLogEnabled) return;
        foreach (BlobFileAddition a in additions)
        {
            LogBlobFileCreationEvent(cfd.Name, jobId, a.BlobFileNumber, a.TotalBlobCount, a.TotalBlobBytes,
                a.ChecksumMethod.Length == 0 ? "" : Convert.ToHexString(a.ChecksumValue), a.ChecksumMethod.Length == 0 ? "Unknown" : a.ChecksumMethod, null);
        }
    }

    /// <summary>DBImpl::Recover (db_impl_open.cc): <c>{"time_micros", "job", "event": "recovery_started", "wal_files": [...]}</c>.</summary>
    private void LogRecoveryStartedEvent(int jobId, IReadOnlyList<ulong> walNumbers)
    {
        if (!InfoLogEnabled) return;
        var sb = new StringBuilder();
        for (int i = 0; i < walNumbers.Count; i++)
        {
            if (i > 0) sb.Append(", ");
            sb.Append(walNumbers[i].ToString(CultureInfo.InvariantCulture));
        }
        LogEvent("\"job\": " + jobId.ToString(CultureInfo.InvariantCulture) + ", \"event\": \"recovery_started\", \"wal_files\": [" + sb + "]");
    }

    /// <summary>
    /// DBImpl::FinishLogFilesRecovery: <c>{"time_micros", "job", "event": "recovery_finished" | "recovery_failed",
    /// "status"}</c>.
    /// </summary>
    private void LogRecoveryFinishedEvent(int jobId, Exception? failure)
    {
        if (!InfoLogEnabled) return;
        LogEvent("\"job\": " + jobId.ToString(CultureInfo.InvariantCulture) + ", \"event\": \"" + (failure is null ? "recovery_finished" : "recovery_failed") +
                 "\", \"status\": " + Json(StatusString(failure)));
    }

    /// <summary>
    /// ExternalSstFileIngestionJob::UpdateStats: <c>{"time_micros", "event": "ingest_finished", "files_ingested": ["file",
    /// path, "level", N, ...], "lsm_state": [...]}</c> (upstream writes the file / level pairs as plain array elements).
    /// </summary>
    internal void LogIngestFinishedEvent(ColumnFamilyData cfd, IReadOnlyList<(string Path, int Level)> files)
    {
        if (BeginEvent() is not { } sb) return;
        sb.Append("\"event\": \"ingest_finished\", \"files_ingested\": [");
        for (int i = 0; i < files.Count; i++)
        {
            if (i > 0) sb.Append(", ");
            sb.Append("\"file\", ").AppendJsonString(files[i].Path).Append(", \"level\", ").Append(files[i].Level);
        }
        AppendLsmState(sb.Append("], \"lsm_state\": ["), cfd).Append(']');
        EndEvent(sb);
    }

    private static string Json(string s) => "\"" + s.Replace("\\", "\\\\", StringComparison.Ordinal).Replace("\"", "\\\"", StringComparison.Ordinal) + "\"";

    /// <summary>
    /// The open-time header: version, DB summary, session id and the options of the DB and of every column family
    /// (DumpRocksDBBuildVersion, DumpDBFileSummary, DumpSupportInfo, DBOptions::Dump, ColumnFamilyOptions::Dump).
    /// </summary>
    /// <param name="dbDirChildren">The DB directory's entries if just listed, else null (listed here).</param>
    private void LogOpenSummary(List<string>? dbDirChildren)
    {
        InfoLogger? log = _infoLog;
        if (log is null)
        {
            _deferredEvents = null;
            return;
        }
        try
        {
            LogHeader("RocksDB version: 11.14.0 (RocksDb.Managed " + typeof(DbImpl).Assembly.GetName().Version + ")");
            LogHeader("Compile date (managed port) .NET " + System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription);
            LogHeader("DB SUMMARY\n");
            LogHeader("Host name (Env):  " + Identity.HostId);
            LogHeader("DB ID: " + Identity.DbId + "\n");
            var files = new StringBuilder();
            int sst = 0;
            try
            {
                foreach (string child in dbDirChildren ?? FileSystem.GetChildren(DbName))
                {
                    if (!FileNames.TryParseFileName(child, out ulong _, out FileType type)) continue;
                    switch (type)
                    {
                        case FileType.CurrentFile: LogHeader("CURRENT file:  " + child); break;
                        case FileType.IdentityFile: LogHeader("IDENTITY file:  " + child); break;
                        case FileType.DescriptorFile: LogHeader("MANIFEST file:  " + child + " size: " + FileSystem.GetFileSize(DbName + "/" + child) + " Bytes"); break;
                        case FileType.TableFile: sst++; files.Append(child).Append(' '); break;
                    }
                }
            }
            catch (Exception e) when (e is IOException or RocksDbException) { }
            LogHeader("SST files in " + DbName + " dir, Total Num: " + sst + ", files: " + files);
            LogHeader("Write Ahead Log file in " + WalDir + ": ");
            LogHeader("DB Session ID:  " + Identity.SessionId + "\n");
            foreach (KeyValuePair<string, string> kv in Options.ToOptionsList()) LogHeader(FormatOption(kv.Key, kv.Value));
            LogHeader(FormatOption("statistics", Options.Statistics is null ? "(nil)" : "enabled"));
            LogHeader(FormatOption("info_log", Options.InfoLog is null ? "LOG file" : Options.InfoLog.GetType().Name));
            foreach (ColumnFamilyData cfd in GetColumnFamilies()) LogColumnFamilyOptions(cfd);
        }
        catch (Exception e) when (e is IOException or RocksDbException or InvalidOperationException)
        {
            // Never fail the open because of logging.
        }
        finally
        {
            FlushDeferredEvents();
        }
    }

    private void LogColumnFamilyOptions(ColumnFamilyData cfd)
    {
        if (_infoLog is null) return;
        LogHeader("--------------- Options for column family [" + cfd.Name + "]:");
        foreach (KeyValuePair<string, string> kv in cfd.Options.ToOptionsList()) LogHeader(FormatOption(kv.Key, kv.Value));
        foreach (KeyValuePair<string, string> kv in cfd.Options.TableOptions.ToOptionsList()) LogHeader(FormatOption("table_factory." + kv.Key, kv.Value));
    }

    /// <summary>ROCKS_LOG_HEADER(log, "%*s: %s") alignment of the options dump.</summary>
    private static string FormatOption(string name, string value) => ("Options." + name).PadLeft(45) + ": " + value;

    // ---- flush / compaction events and stats -------------------------------------------------------------

    /// <summary>FlushJob::Run logging and stats: "flush_started" event.</summary>
    internal void OnFlushStarted(ColumnFamilyData cfd, int jobId, IReadOnlyList<MemTable> mems, FlushReason reason)
    {
        if (!InfoLogEnabled) return;
        long entries = 0, deletes = 0, dataSize = 0, memory = 0, rangeDeletes = 0;
        foreach (MemTable m in mems)
        {
            entries += m.NumEntries;
            deletes += m.NumDeletes;
            dataSize += m.DataSize;
            memory += m.ApproximateMemoryUsageFast;
            rangeDeletes += m.NumRangeDeletes;
        }
        if (BeginInfoLine() is { } line)
        {
            line.Append('[').Append(cfd.Name).Append("] [JOB ").Append(jobId).Append("] Flushing memtable with next log file: ").Append(_logNumber);
            EndInfoLine(line);
        }
        // FlushJob::WriteLevel0Table.
        if (BeginEvent() is not { } sb) return;
        sb.Append("\"job\": ").Append(jobId).Append(", \"event\": \"flush_started\", \"num_memtables\": ").Append(mems.Count)
            .Append(", \"total_num_input_entries\": ").Append(entries).Append(", \"num_deletes\": ").Append(deletes)
            .Append(", \"total_data_size\": ").Append(dataSize).Append(", \"memory_usage\": ").Append(memory)
            .Append(", \"num_range_deletes\": ").Append(rangeDeletes).Append(", \"flush_reason\": ");
        sb.AppendJsonString(FlushReasonString(reason));
        EndEvent(sb);
    }

    /// <summary>FlushJob::WriteLevel0Table: "[cf] [JOB n] Level-0 flush table #N: S bytes OK".</summary>
    internal void LogLevel0FlushTable(ColumnFamilyData cfd, int jobId, FileMetaData meta)
    {
        if (BeginInfoLine() is not { } sb) return;
        sb.Append('[').Append(cfd.Name).Append("] [JOB ").Append(jobId).Append("] Level-0 flush table #").Append(meta.FileNumber).Append(": ")
            .Append(meta.FileSize).Append(" bytes OK");
        EndInfoLine(sb);
    }

    /// <summary>GetFlushReasonString (db/flush_job.cc).</summary>
    internal static string FlushReasonString(FlushReason reason) => reason switch
    {
        FlushReason.Others => "Other Reasons",
        FlushReason.GetLiveFiles => "Get Live Files",
        FlushReason.ShutDown => "Shut down",
        FlushReason.ExternalFileIngestion => "External File Ingestion",
        FlushReason.ManualCompaction => "Manual Compaction",
        FlushReason.WriteBufferManager => "Write Buffer Manager",
        FlushReason.WriteBufferFull => "Write Buffer Full",
        FlushReason.Test => "Test",
        FlushReason.DeleteFiles => "Delete Files",
        FlushReason.AutoCompaction => "Auto Compaction",
        FlushReason.ManualFlush => "Manual Flush",
        FlushReason.ErrorRecovery => "Error Recovery",
        FlushReason.ErrorRecoveryRetryFlush => "Error Recovery Retry Flush",
        FlushReason.WalFull => "WAL Full",
        FlushReason.CatchUpAfterErrorRecovery => "Catch Up After Error Recovery",
        _ => "Invalid",
    };

    /// <summary>CompressionTypeToString (util/compression.h): the option-string name ("NoCompression", "LZ4", ...).</summary>
    private static string CompressionTypeName(Compression.CompressionType t) => t switch
    {
        Compression.CompressionType.None => "NoCompression",
        Compression.CompressionType.Snappy => "Snappy",
        Compression.CompressionType.Zlib => "Zlib",
        Compression.CompressionType.BZip2 => "BZip2",
        Compression.CompressionType.LZ4 => "LZ4",
        Compression.CompressionType.LZ4HC => "LZ4HC",
        Compression.CompressionType.Xpress => "Xpress",
        Compression.CompressionType.Zstd => "ZSTD",
        _ => t.ToString(),
    };

    /// <summary>
    /// EventHelpers::LogAndNotifyTableFileCreationFinished, logging part: file, checksum and sequence range, the
    /// table_properties object (TableProperties fields; user-collected properties are not listed), the oldest blob file
    /// when there is one, and the status.
    /// </summary>
    private void LogTableFileCreationEvent(ColumnFamilyData cfd, int jobId, FileMetaData meta)
    {
        if (!InfoLogEnabled) return;
        Table.TableProperties? tp = null;
        try
        {
            using TableReaderLease lease = cfd.TableCache.GetReader(meta);
            tp = lease.Reader.Properties;
        }
        catch (Exception e) when (e is IOException or RocksDbException)
        {
        }
        if (BeginEvent() is not { } sb) return;
        sb.Append("\"cf_name\": ").AppendJsonString(cfd.Name).Append(", \"job\": ").Append(jobId).Append(", \"event\": \"table_file_creation\", \"file_number\": ")
            .Append(meta.FileNumber).Append(", \"file_size\": ").Append(meta.FileSize)
            .Append(", \"file_checksum\": ").AppendJsonString(meta.FileChecksum is { Length: > 0 } c ? Convert.ToHexString(c) : "")
            .Append(", \"file_checksum_func_name\": ").AppendJsonString(string.IsNullOrEmpty(meta.FileChecksumFuncName) ? "Unknown" : meta.FileChecksumFuncName)
            .Append(", \"smallest_seqno\": ").Append(meta.SmallestSeqno).Append(", \"largest_seqno\": ").Append(meta.LargestSeqno)
            .Append(", \"table_properties\": {");
        if (tp is not null)
        {
            static ulong SafeDivide(ulong a, ulong b) => b == 0 ? 0 : a / b;
            sb.Append("\"data_size\": ").Append(tp.DataSize).Append(", \"index_size\": ").Append(tp.IndexSize)
                .Append(", \"index_partitions\": ").Append(tp.IndexPartitions).Append(", \"top_level_index_size\": ").Append(tp.TopLevelIndexSize)
                .Append(", \"index_key_is_user_key\": ").Append(tp.IndexKeyIsUserKey).Append(", \"index_value_is_delta_encoded\": ").Append(tp.IndexValueIsDeltaEncoded)
                .Append(", \"filter_size\": ").Append(tp.FilterSize).Append(", \"raw_key_size\": ").Append(tp.RawKeySize)
                .Append(", \"raw_average_key_size\": ").Append(SafeDivide(tp.RawKeySize, tp.NumEntries))
                .Append(", \"raw_value_size\": ").Append(tp.RawValueSize).Append(", \"raw_average_value_size\": ").Append(SafeDivide(tp.RawValueSize, tp.NumEntries))
                .Append(", \"num_data_blocks\": ").Append(tp.NumDataBlocks).Append(", \"num_entries\": ").Append(tp.NumEntries)
                .Append(", \"num_filter_entries\": ").Append(tp.NumFilterEntries).Append(", \"num_deletions\": ").Append(tp.NumDeletions)
                .Append(", \"num_merge_operands\": ").Append(tp.NumMergeOperands).Append(", \"num_range_deletions\": ").Append(tp.NumRangeDeletions)
                .Append(", \"format_version\": ").Append(tp.FormatVersion).Append(", \"fixed_key_len\": ").Append(tp.FixedKeyLen)
                .Append(", \"filter_policy\": ").AppendJsonString(tp.FilterPolicyName).Append(", \"column_family_name\": ").AppendJsonString(tp.ColumnFamilyName)
                .Append(", \"column_family_id\": ").Append(tp.ColumnFamilyId).Append(", \"comparator\": ").AppendJsonString(tp.ComparatorName)
                .Append(", \"user_defined_timestamps_persisted\": ").Append(tp.UserDefinedTimestampsPersisted)
                .Append(", \"key_largest_seqno\": ").Append(tp.KeyLargestSeqno).Append(", \"key_smallest_seqno\": ").Append(tp.KeySmallestSeqno)
                .Append(", \"merge_operator\": ").AppendJsonString(tp.MergeOperatorName).Append(", \"prefix_extractor_name\": ").AppendJsonString(tp.PrefixExtractorName)
                .Append(", \"property_collectors\": ").AppendJsonString(tp.PropertyCollectorsNames).Append(", \"compression\": ").AppendJsonString(tp.CompressionName)
                .Append(", \"compression_options\": ").AppendJsonString(tp.CompressionOptions).Append(", \"creation_time\": ").Append(tp.CreationTime)
                .Append(", \"oldest_key_time\": ").Append(tp.OldestKeyTime).Append(", \"newest_key_time\": ").Append(tp.NewestKeyTime)
                .Append(", \"file_creation_time\": ").Append(tp.FileCreationTime)
                .Append(", \"slow_compression_estimated_data_size\": ").Append(tp.SlowCompressionEstimatedDataSize)
                .Append(", \"fast_compression_estimated_data_size\": ").Append(tp.FastCompressionEstimatedDataSize)
                .Append(", \"db_id\": ").AppendJsonString(tp.DbId).Append(", \"db_session_id\": ").AppendJsonString(tp.DbSessionId)
                .Append(", \"orig_file_number\": ").Append(tp.OriginalFileNumber)
                .Append(", \"seqno_to_time_mapping\": ").AppendJsonString(SeqnoToTimeMappingText(tp.SequenceNumberTimeMapping));
        }
        sb.Append('}');
        if (meta.OldestBlobFileNumber != 0) sb.Append(", \"oldest_blob_file_number\": ").Append(meta.OldestBlobFileNumber);
        sb.Append(", \"status\": \"OK\"");
        EndEvent(sb);
    }

    /// <summary>
    /// The "seqno_to_time_mapping" value of the table_file_creation event: "N/A" without a mapping, else its
    /// SeqnoToTimeMapping::ToHumanString, or "Invalid" when it does not decode.
    /// </summary>
    internal static string SeqnoToTimeMappingText(byte[]? encoded)
    {
        if (encoded is not { Length: > 0 }) return "N/A";
        var mapping = new SeqnoToTimeMapping();
        return mapping.DecodeFrom(encoded) is null ? mapping.ToHumanString() : "Invalid";
    }

    /// <summary>
    /// After a flush installed its result: InternalStats (BYTES_FLUSHED, L0 compaction stats), FLUSH_WRITE_BYTES /
    /// FLUSH_TIME statistics and the "table_file_creation" / "flush_finished" events.
    /// </summary>
    internal void OnFlushCompleted(ColumnFamilyData cfd, int jobId, FileMetaData? meta, ulong blobBytes, long startTimestamp)
    {
        TimeSpan elapsed = Stopwatch.GetElapsedTime(startTimestamp);
        // Table file plus the blob files the flush wrote (bytes_written + bytes_written_blob).
        ulong bytes = (meta?.FileSize ?? 0) + blobBytes;
        cfd.Stats.AddFlush(bytes, (long)elapsed.TotalMicroseconds);
        if (Stats is { } st)
        {
            st.RecordTick(Ticker.FlushWriteBytes, bytes);
            st.RecordInHistogram(Histogram.FlushTime, (ulong)elapsed.TotalMicroseconds);
        }
        if (!InfoLogEnabled) return;
        if (meta is not null) LogTableFileCreationEvent(cfd, jobId, meta);
        // FlushJob::Run: output_compression, lsm_state, blob_file_head / blob_file_tail, immutable_memtables.
        int immutable;
        lock (_mutex) immutable = cfd.NumNotFlushed;
        if (BeginEvent() is not { } sb) return;
        sb.Append("\"job\": ").Append(jobId).Append(", \"event\": \"flush_finished\", \"output_compression\": ");
        sb.AppendJsonString(CompressionTypeName(cfd.Options.Compression)).Append(", \"lsm_state\": [");
        AppendBlobFileHeadTail(AppendLsmState(sb, cfd).Append(']'), cfd).Append(", \"immutable_memtables\": ").Append(immutable);
        EndEvent(sb);
    }

    /// <summary>Appends ", "blob_file_head": N, "blob_file_tail": M" of the current version's blob files, or nothing.</summary>
    private static StringBuilder AppendBlobFileHeadTail(StringBuilder sb, ColumnFamilyData cfd)
    {
        VersionStorageInfo? s = cfd.Current?.Storage;
        if (s is null || s.BlobFiles.Length == 0) return sb;
        return sb.Append(", \"blob_file_head\": ").Append(s.BlobFiles[0].BlobFileNumber).Append(", \"blob_file_tail\": ")
            .Append(s.BlobFiles[^1].BlobFileNumber);
    }

    /// <summary>
    /// CompactionJob::LogCompaction: the "Compacting N@L + M@L' files to Lx, score s" line and the "compaction_started"
    /// event (same fields as upstream, so native and managed LOGs can be compared).
    /// </summary>
    internal void OnCompactionStarted(ColumnFamilyData cfd, Compaction c, int jobId)
    {
        if (!InfoLogEnabled) return;
        // Trivial moves and deletion compactions don't run a CompactionJob upstream (they log "Moved #..." instead).
        if (c.IsDeletionCompaction || c.IsTrivialMove()) return;
        if (BeginInfoLine() is not { } line) return;
        line.Append('[').Append(cfd.Name).Append("] [JOB ").Append(jobId).Append("] Compacting ");
        bool first = true;
        ulong inputBytes = 0;
        for (int i = 0; i < c.NumInputLevels; i++)
        {
            List<FileMetaData> inputs = c.Inputs(i).Files;
            foreach (FileMetaData f in inputs) inputBytes += f.FileSize;
            // The summary line lists only the non-empty input levels.
            if (inputs.Count == 0) continue;
            if (!first) line.Append(" + ");
            first = false;
            line.Append(inputs.Count).Append('@').Append(c.Level(i));
        }
        line.Append(" files to L").Append(c.OutputLevel).Append(", score ").Append(c.Score.ToString("F2", CultureInfo.InvariantCulture));
        EndInfoLine(line);
        if (BeginEvent() is not { } sb) return;
        sb.Append("\"job\": ").Append(jobId).Append(", \"event\": \"compaction_started\", \"cf_name\": ");
        sb.AppendJsonString(cfd.Name).Append(", \"compaction_reason\": ");
        sb.AppendJsonString(CompactionReasonName(c.Reason));
        // The event lists every input level (an empty one as []).
        for (int i = 0; i < c.NumInputLevels; i++)
        {
            List<FileMetaData> inputs = c.Inputs(i).Files;
            sb.Append(", \"files_L").Append(c.Level(i)).Append("\": [");
            for (int j = 0; j < inputs.Count; j++)
            {
                if (j > 0) sb.Append(", ");
                sb.Append(inputs[j].FileNumber);
            }
            sb.Append(']');
        }
        sb.Append(", \"score\": ").Append(EventLogJson.FormatDouble(c.Score)).Append(", \"input_data_size\": ").Append(inputBytes)
            .Append(", \"oldest_snapshot_seqno\": ");
        lock (_mutex)
        {
            if (_snapshots.IsEmpty) sb.Append("-1");
            else sb.Append(_snapshots.Oldest.SequenceNumber);
        }
        EndEvent(sb);
    }

    /// <summary>CompactionReason names (enum names, as the event logged them), cached so the event does not format them.</summary>
    private static readonly string[] s_compactionReasonNames = BuildCompactionReasonNames();

    private static string[] BuildCompactionReasonNames()
    {
        CompactionReason[] values = Enum.GetValues<CompactionReason>();
        int max = 0;
        foreach (CompactionReason r in values) max = Math.Max(max, (int)r);
        var names = new string[max + 1];
        foreach (CompactionReason r in values) names[(int)r] = r.ToString();
        return names;
    }

    private static string CompactionReasonName(CompactionReason reason) =>
        (uint)reason < (uint)s_compactionReasonNames.Length && s_compactionReasonNames[(int)reason] is { } n ? n : reason.ToString();

    /// <summary>Appends the file count of every level ("4, 0, 0, ...") of the current version.</summary>
    private static StringBuilder AppendLsmState(StringBuilder sb, ColumnFamilyData cfd)
    {
        VersionStorageInfo? s = cfd.Current?.Storage;
        if (s is null) return sb;
        for (int l = 0; l < s.NumLevels; l++)
        {
            if (l > 0) sb.Append(", ");
            sb.Append(s.NumLevelFiles(l));
        }
        return sb;
    }

    /// <summary>
    /// After a compaction installed its result: InternalStats per output level, COMPACT_READ_BYTES / COMPACT_WRITE_BYTES
    /// / COMPACTION_KEY_DROP_* tickers, COMPACTION_TIME histogram and the "compaction_finished" event.
    /// </summary>
    internal void OnCompactionCompleted(ColumnFamilyData cfd, Compaction c, CompactionJob? job, int jobId, long startTimestamp)
    {
        long micros = (long)Stopwatch.GetElapsedTime(startTimestamp).TotalMicroseconds;
        var ls = new CompactionLevelStats { Micros = micros, CpuMicros = micros, Count = 1 };
        for (int i = 0; i < c.NumInputLevels; i++)
        {
            ulong bytes = 0;
            foreach (FileMetaData f in c.Inputs(i).Files) bytes += f.FileSize;
            if (c.Level(i) == c.OutputLevel)
            {
                ls.BytesReadOutputLevel += bytes;
                ls.NumInputFilesInOutputLevel += c.Inputs(i).Files.Count;
            }
            else
            {
                ls.BytesReadNonOutputLevels += bytes;
                ls.NumInputFilesInNonOutputLevels += c.Inputs(i).Files.Count;
            }
        }
        if (job is null)
        {
            // Trivial move or deletion compaction: nothing read or written.
            if (!c.IsDeletionCompaction) ls.BytesMoved = ls.BytesReadNonOutputLevels;
            ls.BytesReadNonOutputLevels = 0;
            ls.BytesReadOutputLevel = 0;
        }
        else
        {
            ls.BytesWritten = job.Stats.BytesWritten;
            ls.NumOutputFiles = job.Outputs.Count + job.ProximalOutputs.Count;
            ls.NumInputRecords = (ulong)job.Stats.NumInputRecords;
            ls.NumDroppedRecords = (ulong)Math.Max(0, job.Stats.NumInputRecords - job.Stats.NumOutputRecords);
        }
        cfd.Stats.AddCompaction(c.OutputLevel, ls);
        if (Stats is { } st && job is not null)
        {
            st.RecordTick(Ticker.CompactReadBytes, job.Stats.BytesRead);
            st.RecordTick(Ticker.CompactWriteBytes, job.Stats.BytesWritten);
            CompactionIterationStats it = job.Stats.IterStats;
            st.RecordTick(Ticker.CompactionKeyDropNewerEntry, (ulong)it.NumRecordDropHidden);
            st.RecordTick(Ticker.CompactionKeyDropObsolete, (ulong)it.NumRecordDropObsolete);
            st.RecordTick(Ticker.CompactionKeyDropRangeDel, (ulong)it.NumRecordDropRangeDel);
            st.RecordTick(Ticker.CompactionKeyDropUser, (ulong)it.NumRecordDropUser);
            st.RecordTick(Ticker.CompactionRangeDelDropObsolete, (ulong)it.NumRangeDelDropObsolete);
            st.RecordTick(Ticker.CompactionOptimizedDelDropObsolete, (ulong)it.NumOptimizedDelDropObsolete);
            st.RecordInHistogram(Histogram.CompactionTime, (ulong)micros);
            st.RecordInHistogram(Histogram.CompactionCpuTime, (ulong)micros);
            st.RecordInHistogram(Histogram.NumFilesInSingleCompaction, (ulong)(ls.NumInputFilesInNonOutputLevels + ls.NumInputFilesInOutputLevel));
        }
        if (!InfoLogEnabled) return;
        if (job is not null)
        {
            LogBlobFileCreationEvents(cfd, jobId, job.BlobFileAdditions);
            foreach (FileMetaData meta in job.AllOutputs)
            {
                if (BeginInfoLine() is { } generated)
                {
                    generated.Append('[').Append(cfd.Name).Append("] [JOB ").Append(jobId).Append("] Generated table #").Append(meta.FileNumber)
                        .Append(": ").Append(meta.FileSize).Append(" bytes");
                    EndInfoLine(generated);
                }
                LogTableFileCreationEvent(cfd, jobId, meta);
            }
        }
        else if (!c.IsDeletionCompaction)
        {
            // DBImpl::BackgroundCompaction, trivial move branch: the "trivial_move" event, then "Moved #N files".
            int moved = ls.NumInputFilesInNonOutputLevels + ls.NumInputFilesInOutputLevel;
            LogEvent("\"job\": " + jobId + ", \"event\": \"trivial_move\", \"destination_level\": " + c.OutputLevel + ", \"files\": " + moved +
                     ", \"total_files_size\": " + ls.BytesMoved);
            LogInfo("[" + cfd.Name + "] Moved #" + moved + " files to level-" + c.OutputLevel + " " + ls.BytesMoved + " bytes OK");
        }
        string kind = job is null ? (c.IsDeletionCompaction ? "deletion" : "trivial move") : "compaction";
        if (BeginInfoLine() is { } compacted)
        {
            compacted.Append('[').Append(cfd.Name).Append("] [JOB ").Append(jobId).Append("] Compacted (").Append(kind).Append(") ")
                .Append(ls.NumInputFilesInNonOutputLevels + ls.NumInputFilesInOutputLevel).Append(" files to L").Append(c.OutputLevel).Append(" => ")
                .Append(ls.BytesWritten).Append(" bytes");
            EndInfoLine(compacted);
        }
        // Trivial moves and deletion compactions run no CompactionJob upstream, so they log no "compaction_finished".
        if (job is null) return;
        // CompactionJob::Install: the table-file outputs, then the blob outputs when there are any (num_subcompactions is
        // 1: subcompactions are not ported).
        ulong tableBytes = 0;
        foreach (FileMetaData meta in job.AllOutputs) tableBytes += meta.FileSize;
        if (BeginEvent() is not { } ev) return;
        int numOutputFiles = 0;
        foreach (FileMetaData _ in job.AllOutputs) numOutputFiles++;
        ev.Append("\"job\": ").Append(jobId).Append(", \"event\": \"compaction_finished\", \"compaction_time_micros\": ").Append(micros)
            .Append(", \"compaction_time_cpu_micros\": ").Append(micros).Append(", \"output_level\": ").Append(c.OutputLevel)
            .Append(", \"num_output_files\": ").Append(numOutputFiles).Append(", \"total_output_size\": ").Append(tableBytes);
        if (job.BlobFileAdditions.Count > 0)
        {
            ulong blobBytes = 0;
            foreach (BlobFileAddition a in job.BlobFileAdditions) blobBytes += a.TotalBlobBytes + Blob.BlobLogFormat.HeaderSize + Blob.BlobLogFormat.FooterSize;
            ev.Append(", \"num_blob_output_files\": ").Append(job.BlobFileAdditions.Count).Append(", \"total_blob_output_size\": ").Append(blobBytes);
        }
        ev.Append(", \"num_input_records\": ").Append(ls.NumInputRecords).Append(", \"num_output_records\": ").Append(job.Stats.NumOutputRecords)
            .Append(", \"num_subcompactions\": 1, \"output_compression\": ").AppendJsonString(CompressionTypeName(c.OutputCompression))
            .Append(", \"num_single_delete_mismatches\": ").Append(job.Stats.IterStats.NumSingleDelMismatch)
            .Append(", \"num_single_delete_fallthrough\": ").Append(job.Stats.IterStats.NumSingleDelFallthru)
            .Append(", \"lsm_state\": [");
        AppendBlobFileHeadTail(AppendLsmState(ev, cfd).Append(']'), cfd);
        EndEvent(ev);
    }

    // ---- write / read path -------------------------------------------------------------------------------

    /// <summary>
    /// Per write group (leader only): DB stats (dbstats) and the NUMBER_KEYS_WRITTEN / BYTES_WRITTEN /
    /// WRITE_DONE_BY_SELF / WRITE_DONE_BY_OTHER / WRITE_WITH_WAL / WAL_FILE_BYTES / WAL_FILE_SYNCED tickers
    /// (DBImpl::WriteImpl).
    /// </summary>
    internal void RecordWriteGroup(List<WriteThread.Writer> group, ulong keys, int walBytes, bool wal, bool synced)
    {
        long bytes = 0;
        for (int i = 0; i < group.Count; i++) bytes += group[i].Batch!.DataSize;
        DbWriteStats w = _writeStats;
        Interlocked.Add(ref w.NumKeysWritten, (long)keys);
        Interlocked.Add(ref w.BytesWritten, bytes);
        Interlocked.Increment(ref w.WriteDoneBySelf);
        if (group.Count > 1) Interlocked.Add(ref w.WriteDoneByOther, group.Count - 1);
        if (wal)
        {
            Interlocked.Add(ref w.WriteWithWal, group.Count);
            Interlocked.Add(ref w.WalFileBytes, walBytes);
            if (synced) Interlocked.Increment(ref w.WalFileSynced);
        }
        if (Stats is not { } st) return;
        st.RecordTick(Ticker.NumberKeysWritten, keys);
        st.RecordTick(Ticker.BytesWritten, (ulong)bytes);
        st.RecordTick(Ticker.WriteDoneBySelf);
        if (group.Count > 1) st.RecordTick(Ticker.WriteDoneByOther, (ulong)(group.Count - 1));
        if (wal)
        {
            st.RecordTick(Ticker.WriteWithWal, (ulong)group.Count);
            st.RecordTick(Ticker.WalFileBytes, (ulong)walBytes);
            if (synced) st.RecordTick(Ticker.WalFileSynced);
        }
        st.RecordInHistogram(Histogram.BytesPerWrite, (ulong)bytes);
    }

    /// <summary>Write stall time (STALL_MICROS ticker, WRITE_STALL histogram, "Cumulative stall" in dbstats).</summary>
    internal void RecordWriteStall(long startTimestamp)
    {
        long micros = (long)Stopwatch.GetElapsedTime(startTimestamp).TotalMicroseconds;
        Interlocked.Add(ref _writeStats.WriteStallMicros, micros);
        if (Stats is { } st)
        {
            st.RecordTick(Ticker.StallMicros, (ulong)micros);
            st.RecordInHistogram(Histogram.WriteStall, (ulong)micros);
        }
    }

    /// <summary>
    /// NUMBER_KEYS_READ, BYTES_READ, MEMTABLE_HIT / MISS, GET_HIT_L0 / L1 / L2_AND_UP (Version::Get, kFound in a table
    /// file), BYTES_PER_READ and DB_GET for one point lookup.
    /// </summary>
    internal static void RecordGet(Statistics st, GetContext ctx, long startTimestamp)
    {
        st.RecordTick(Ticker.NumberKeysRead);
        st.RecordTick(ctx.FromMemTable ? Ticker.MemtableHit : Ticker.MemtableMiss);
        if (ctx.HitLevel >= 0 && ctx.State == GetContext.GetState.Found)
        {
            st.RecordTick(ctx.HitLevel switch { 0 => Ticker.GetHitL0, 1 => Ticker.GetHitL1, _ => Ticker.GetHitL2AndUp });
        }
        if (ctx.State == GetContext.GetState.Found || ctx.MergeContext.Count > 0)
        {
            int n = ctx.Value.WrittenCount;
            st.RecordTick(Ticker.BytesRead, (ulong)n);
            st.RecordInHistogram(Histogram.BytesPerRead, (ulong)n);
        }
        st.RecordElapsedMicros(Histogram.DbGet, startTimestamp);
    }

    // ---- periodic stats dump -----------------------------------------------------------------------------

    /// <summary>Starts DBImpl::DumpStats every stats_dump_period_sec when there is an info log.</summary>
    private void StartStatsDump()
    {
        uint period = Options.StatsDumpPeriodSec;
        if (period == 0 || _infoLog is null) return;
        _statsDumpTimer = new Timer(static s => ((DbImpl)s!).DumpStats(), this, TimeSpan.FromSeconds(period), TimeSpan.FromSeconds(period));
    }

    /// <summary>
    /// Port of DBImpl::DumpStats: "------- DUMPING STATS -------", the dbstats and cfstats of every column family, and
    /// the statistics string when statistics are enabled.
    /// </summary>
    internal void DumpStats()
    {
        InfoLogger? log = _infoLog;
        if (log is null || IsDisposed) return;
        try
        {
            var sb = new StringBuilder();
            sb.Append(_writeStats.Dump());
            foreach (ColumnFamilyData cfd in GetColumnFamilies())
            {
                SuperVersion sv = GetReferencedSuperVersion(cfd);
                try
                {
                    sb.Append(cfd.Stats.Dump(cfd.Name, sv.Current.Storage, isPeriodic: true)).Append(cfd.Stats.DumpFileHistogram(cfd.Name));
                }
                finally
                {
                    ReturnSuperVersion(sv);
                }
            }
            log.Log(InfoLogLevel.Info, "------- DUMPING STATS -------");
            log.Log(InfoLogLevel.Info, sb.ToString());
            if (Stats is { } st) log.Log(InfoLogLevel.Info, "STATISTICS:\n " + st.ToString());
            log.Flush();
        }
        catch (Exception e) when (e is RocksDbException or ObjectDisposedException or InvalidOperationException)
        {
            // The DB is closing.
        }
    }

    /// <summary>Stops the stats dump timer and closes the DB's own LOG file (CloseHelper).</summary>
    private void ShutdownMonitoring()
    {
        _statsDumpTimer?.Dispose();
        _statsDumpTimer = null;
        StopStatsPersist();
        StopCompactionTrigger();
        StopRecordSeqnoTimeWorker();
        InfoLogger? log = _infoLog;
        if (log is null) return;
        LogInfo("Shutdown complete");
        if (_ownsInfoLog) log.Dispose();
        else log.Flush();
    }
}

/// <summary>JSON string values of the EVENT_LOG records (JSONWriter::AddValue of a string).</summary>
internal static class EventLogJson
{
    /// <summary>
    /// JSONWriter's operator&lt;&lt; for a double (logging/event_logger.h: the std::ostream default, i.e. printf "%g":
    /// six significant digits, trailing zeros dropped, "e-07" style exponents).
    /// </summary>
    public static string FormatDouble(double value) =>
        value.ToString("G6", System.Globalization.CultureInfo.InvariantCulture).Replace('E', 'e');

    /// <summary>Appends <paramref name="s"/> as a JSON string: quotes and backslashes escaped (as DbImpl.Json does).</summary>
    public static StringBuilder AppendJsonString(this StringBuilder sb, string s)
    {
        sb.Append('"');
        foreach (char ch in s)
        {
            if (ch is '\\' or '"') sb.Append('\\');
            sb.Append(ch);
        }
        return sb.Append('"');
    }
}
