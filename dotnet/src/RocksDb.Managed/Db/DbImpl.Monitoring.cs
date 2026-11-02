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
        if (Environment.GetEnvironmentVariable("RDB_NOLOG") == "1") return; // TEMP
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
        if (_infoLog is null || !_infoLog.IsEnabled(InfoLogLevel.Info)) return;
        ulong micros = (ulong)(DateTime.UtcNow - DateTime.UnixEpoch).Ticks / 10;
        _infoLog.Log(InfoLogLevel.Info, "EVENT_LOG_v1 {\"time_micros\": " + micros.ToString(CultureInfo.InvariantCulture) + ", " + fields + "}");
    }

    private static string Json(string s) => "\"" + s.Replace("\\", "\\\\", StringComparison.Ordinal).Replace("\"", "\\\"", StringComparison.Ordinal) + "\"";

    /// <summary>
    /// The open-time header: version, DB summary, session id and the options of the DB and of every column family
    /// (DumpRocksDBBuildVersion, DumpDBFileSummary, DumpSupportInfo, DBOptions::Dump, ColumnFamilyOptions::Dump).
    /// </summary>
    private void LogOpenSummary()
    {
        InfoLogger? log = _infoLog;
        if (log is null) return;
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
                foreach (string child in FileSystem.GetChildren(DbName))
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
    internal void OnFlushStarted(ColumnFamilyData cfd, int jobId, IReadOnlyList<MemTable> mems)
    {
        if (_infoLog is null) return;
        long entries = 0, deletes = 0, memory = 0;
        foreach (MemTable m in mems)
        {
            entries += m.NumEntries;
            deletes += m.NumDeletes;
            memory += m.ApproximateMemoryUsageFast;
        }
        LogInfo("[" + cfd.Name + "] [JOB " + jobId + "] Flushing memtable with next log file: " + _logNumber);
        LogEvent("\"job\": " + jobId + ", \"event\": \"flush_started\", \"num_memtables\": " + mems.Count + ", \"num_entries\": " + entries +
                 ", \"num_deletes\": " + deletes + ", \"total_data_size\": " + memory + ", \"memory_usage\": " + memory +
                 ", \"flush_reason\": \"Manual Flush\"");
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
        if (_infoLog is null) return;
        if (meta is not null)
        {
            LogEvent("\"cf_name\": " + Json(cfd.Name) + ", \"job\": " + jobId + ", \"event\": \"table_file_creation\", \"file_number\": " +
                     meta.FileNumber + ", \"file_size\": " + meta.FileSize);
        }
        LogEvent("\"job\": " + jobId + ", \"event\": \"flush_finished\", \"output_compression\": " +
                 Json(cfd.Options.Compression.ToString()) + ", \"lsm_state\": [" + LsmState(cfd) + "]");
    }

    /// <summary>
    /// CompactionJob::LogCompaction: the "Compacting N@L + M@L' files to Lx, score s" line and the "compaction_started"
    /// event (same fields as upstream, so native and managed LOGs can be compared).
    /// </summary>
    internal void OnCompactionStarted(ColumnFamilyData cfd, Compaction c, int jobId)
    {
        if (_infoLog is null || !_infoLog.IsEnabled(InfoLogLevel.Info)) return;
        // Trivial moves and deletion compactions don't run a CompactionJob upstream (they log "Moved #..." instead).
        if (c.IsDeletionCompaction || c.IsTrivialMove()) return;
        var summary = new StringBuilder();
        var files = new StringBuilder();
        ulong inputBytes = 0;
        for (int i = 0; i < c.NumInputLevels; i++)
        {
            List<FileMetaData> inputs = c.Inputs(i).Files;
            if (inputs.Count == 0) continue;
            if (summary.Length > 0) summary.Append(" + ");
            summary.Append(inputs.Count).Append('@').Append(c.Level(i));
            files.Append(", \"files_L").Append(c.Level(i)).Append("\": [");
            for (int j = 0; j < inputs.Count; j++)
            {
                if (j > 0) files.Append(", ");
                files.Append(inputs[j].FileNumber);
                inputBytes += inputs[j].FileSize;
            }
            files.Append(']');
        }
        string score = c.Score.ToString("0.######", CultureInfo.InvariantCulture);
        LogInfo("[" + cfd.Name + "] [JOB " + jobId + "] Compacting " + summary + " files to L" + c.OutputLevel + ", score " +
                c.Score.ToString("F2", CultureInfo.InvariantCulture));
        LogEvent("\"job\": " + jobId + ", \"event\": \"compaction_started\", \"cf_name\": " + Json(cfd.Name) + ", \"compaction_reason\": " +
                 Json(c.Reason.ToString()) + files + ", \"score\": " + score + ", \"input_data_size\": " + inputBytes);
    }

    private static string LsmState(ColumnFamilyData cfd)
    {
        VersionStorageInfo? s = cfd.Current?.Storage;
        if (s is null) return "";
        var sb = new StringBuilder();
        for (int l = 0; l < s.NumLevels; l++)
        {
            if (l > 0) sb.Append(", ");
            sb.Append(s.NumLevelFiles(l));
        }
        return sb.ToString();
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
            ls.NumOutputFiles = job.Outputs.Count;
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
        if (_infoLog is null) return;
        if (job is not null)
        {
            foreach (FileMetaData meta in job.Outputs)
            {
                LogInfo("[" + cfd.Name + "] [JOB " + jobId + "] Generated table #" + meta.FileNumber + ": " + meta.FileSize + " bytes");
                LogEvent("\"cf_name\": " + Json(cfd.Name) + ", \"job\": " + jobId + ", \"event\": \"table_file_creation\", \"file_number\": " +
                         meta.FileNumber + ", \"file_size\": " + meta.FileSize);
            }
        }
        else if (!c.IsDeletionCompaction)
        {
            // DBImpl::BackgroundCompaction, trivial move branch.
            LogInfo("[" + cfd.Name + "] Moved #" + (ls.NumInputFilesInNonOutputLevels + ls.NumInputFilesInOutputLevel) + " files to level-" + c.OutputLevel +
                    " " + ls.BytesMoved + " bytes OK");
        }
        string kind = job is null ? (c.IsDeletionCompaction ? "deletion" : "trivial move") : "compaction";
        LogInfo("[" + cfd.Name + "] [JOB " + jobId + "] Compacted (" + kind + ") " + (ls.NumInputFilesInNonOutputLevels + ls.NumInputFilesInOutputLevel) +
                " files to L" + c.OutputLevel + " => " + ls.BytesWritten + " bytes");
        LogEvent("\"job\": " + jobId + ", \"event\": \"compaction_finished\", \"compaction_time_micros\": " + micros +
                 ", \"output_level\": " + c.OutputLevel + ", \"num_output_files\": " + ls.NumOutputFiles +
                 ", \"total_output_size\": " + ls.BytesWritten + ", \"num_input_records\": " + ls.NumInputRecords +
                 ", \"num_output_records\": " + (job?.Stats.NumOutputRecords ?? 0) + ", \"lsm_state\": [" + LsmState(cfd) + "]");
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

    /// <summary>NUMBER_KEYS_READ, BYTES_READ, MEMTABLE_HIT / MISS, BYTES_PER_READ and DB_GET for one point lookup.</summary>
    internal static void RecordGet(Statistics st, GetContext ctx, long startTimestamp)
    {
        st.RecordTick(Ticker.NumberKeysRead);
        st.RecordTick(ctx.FromMemTable ? Ticker.MemtableHit : Ticker.MemtableMiss);
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
                    sb.Append(cfd.Stats.Dump(cfd.Name, sv.Current.Storage));
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
        StopCompactionTrigger();
        InfoLogger? log = _infoLog;
        if (log is null) return;
        LogInfo("Shutdown complete");
        if (_ownsInfoLog) log.Dispose();
        else log.Flush();
    }
}
