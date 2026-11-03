using RocksDb.Managed.Env;
using RocksDb.Managed.Monitoring;
using RocksDb.Managed.Table;

namespace RocksDb.Managed.Db;

/// <summary>
/// Remote compaction. The primary side is a port of CompactionJob::ProcessKeyValueCompactionWithCompactionService
/// (db/compaction/compaction_service_job.cc): the job is serialized, handed to <see cref="CompactionService"/>, and the
/// worker's output files are renamed into the DB under new file numbers and installed like a local compaction's. The
/// worker side ports DB::OpenAndCompact and DBImplSecondary::CompactWithoutInstallation (db/db_impl/db_impl_secondary.cc):
/// the DB is opened as a secondary (no WAL replay) in the job's output directory, the compaction is rebuilt from the
/// input file numbers and run, and its result serialized.
/// <para>
/// Managed deviations: one remote job per compaction (upstream schedules each subcompaction separately); compactions
/// that read or write blob files, use per-key placement, have a snapshot checker (WritePrepared / WriteUnprepared) or a
/// trim timestamp run locally (upstream: blob files are supported by 11.14 workers, the others have the same TODOs or no
/// remote support); the managed worker declines (Incomplete) jobs involving blob files.
/// </para>
/// </summary>
internal sealed partial class DbImpl
{
    /// <summary>Outputs of a remote compaction moved into the DB (the equivalent of the local CompactionJob outputs).</summary>
    internal sealed class RemoteCompactionOutcome
    {
        public readonly List<FileMetaData> Outputs = new();
        public readonly List<FileMetaData> ProximalOutputs = new();
        public readonly List<ulong> CreatedFileNumbers = new();
        public ulong BytesRead;
        public ulong BytesWritten;
        public string ScheduledJobId = "";
    }

    /// <summary>
    /// ProcessKeyValueCompactionWithCompactionService for the whole compaction. Returns null when the compaction must
    /// run locally (kUseLocal or unsupported); throws for kFailure / kAborted.
    /// </summary>
    private RemoteCompactionOutcome? TryRunRemoteCompaction(CompactionService service, ColumnFamilyData cfd, Compaction c, ulong[] snapshots, int jobId,
        bool bottom, CancellationToken ct)
    {
        if (c.DoesInputReferenceBlobFiles() || cfd.Options.EnableBlobFiles || c.SupportsPerKeyPlacement || c.TrimTs is not null) return null;
        var input = new CompactionServiceInput
        {
            CfName = cfd.Name,
            OutputLevel = c.OutputLevel,
            DbId = Identity.DbId,
            OptionsFileNumber = LatestOptionsFileNumber(),
            PreserveInternalTimeSeconds = cfd.Options.PreserveInternalTimeSeconds,
            PrecludeLastLevelDataSeconds = cfd.Options.PrecludeLastLevelDataSeconds,
        };
        (input.MinManifestFileNumber, input.MinManifestFileSize) = Versions.InstalledManifest;
        for (int l = 0; l < c.NumInputLevels; l++)
        {
            foreach (FileMetaData f in c.Inputs(l).Files) input.InputFiles.Add(FileNames.MakeTableFileName(f.FileNumber));
        }
        var sorted = new List<ulong>(snapshots);
        sorted.Sort();
        for (int i = 0; i < sorted.Count; i++)
        {
            if (i == 0 || sorted[i] != sorted[i - 1]) input.Snapshots.Add(sorted[i]);
        }
        byte[] binary = CompactionServiceFormat.Write(input);
        var info = new CompactionServiceJobInfo(DbName, Identity.DbId, Identity.SessionId, cfd.Id, cfd.Name, (ulong)jobId,
            bottom ? CompactionThreadPriority.Bottom : CompactionThreadPriority.Low, c.Reason, c.IsFullCompaction, c.IsManual, c.BottommostLevel,
            c.StartLevel, c.OutputLevel);
        LogInfo("[" + cfd.Name + "] [JOB " + jobId + "] Starting remote compaction (output level: " + c.OutputLevel + "): "
            + string.Join(", ", input.InputFiles));
        CompactionServiceScheduleResponse response = Await(service.ScheduleAsync(info, binary, ct));
        switch (response.Status)
        {
            case CompactionServiceJobStatus.Success:
                break;
            case CompactionServiceJobStatus.Aborted:
                throw new RocksDbException(StatusCode.Aborted, "Operation aborted: Scheduling a remote compaction job was aborted");
            case CompactionServiceJobStatus.UseLocal:
                LogInfo("[" + cfd.Name + "] [JOB " + jobId + "] Remote compaction fallback to local by API (Schedule)");
                return null;
            default:
                throw new RocksDbException(StatusCode.Incomplete, "Incomplete: CompactionService failed to schedule a remote compaction job.");
        }
        CompactionServiceWaitResult waited;
        Interlocked.Increment(ref _numRunningRemoteCompactions);
        try
        {
            waited = Await(service.WaitAsync(response.ScheduledJobId, ct));
        }
        finally
        {
            Interlocked.Decrement(ref _numRunningRemoteCompactions);
        }
        string jobTag = "[" + cfd.Name + "] [JOB " + jobId + "] ";
        if (waited.Status == CompactionServiceJobStatus.UseLocal)
        {
            LogInfo(jobTag + "Remote compaction fallback to local by API (Wait)");
            return null;
        }
        if (waited.Status == CompactionServiceJobStatus.Aborted)
        {
            throw new RocksDbException(StatusCode.Aborted, "Operation aborted: Waiting a remote compaction job was aborted");
        }
        CompactionServiceResult? result = null;
        RocksDbException? parseError = null;
        try
        {
            if (waited.Result is not null) result = CompactionServiceFormat.ReadResult(waited.Result);
        }
        catch (RocksDbException e)
        {
            parseError = e;
        }
        if (waited.Status != CompactionServiceJobStatus.Success)
        {
            LogWarn(jobTag + "Remote compaction failed.");
            if (result is not null && !result.Ok) throw result.ToException();
            throw new RocksDbException(StatusCode.Incomplete, result is not null
                ? "Incomplete: CompactionService failed to run the compaction job (even though the internal status is okay)."
                : "Incomplete: CompactionService failed to run the compaction job (and no valid result is returned).");
        }
        if (result is null)
        {
            // Wait() returned kSuccess but the result can't be read: nothing was imported, decline so the service may retry
            // or compact locally.
            LogWarn(jobTag + "Declining malformed remote compaction result (" + (parseError?.Message ?? "no result") + ")");
            service.OnInstallation(response.ScheduledJobId, CompactionServiceJobStatus.UseLocal);
            return null;
        }
        foreach (CompactionServiceOutputFile f in result.OutputFiles)
        {
            if (f.IsProximalLevelOutput || f.OldestBlobFileNumber != FileMetaData.InvalidBlobFileNumber)
            {
                LogWarn(jobTag + "Remote compaction returned proximal-level or blob output; falling back to local compaction");
                service.OnInstallation(response.ScheduledJobId, CompactionServiceJobStatus.UseLocal);
                return null;
            }
        }
        if (result.BlobFileAdditions.Count > 0 || result.BlobFileGarbages.Count > 0)
        {
            service.OnInstallation(response.ScheduledJobId, CompactionServiceJobStatus.UseLocal);
            return null;
        }
        if (!result.Ok)
        {
            LogWarn(jobTag + "Remote compaction returned a non-OK result: " + result.StatusMessage);
            service.OnInstallation(response.ScheduledJobId, CompactionServiceJobStatus.Failure);
            throw result.ToException();
        }
        LogInfo(jobTag + "Received remote compaction result, output path: " + result.OutputPath);
        var outcome = new RemoteCompactionOutcome { BytesRead = result.BytesRead, BytesWritten = result.BytesWritten, ScheduledJobId = response.ScheduledJobId };
        try
        {
            foreach (CompactionServiceOutputFile f in result.OutputFiles)
            {
                ulong fileNum = NewPendingOutputFileNumber();
                outcome.CreatedFileNumbers.Add(fileNum);
                string src = result.OutputPath + "/" + f.FileName;
                string tgt = FileNames.TableFileName(DbPaths, fileNum, c.OutputPathId);
                FileSystem.RenameFile(src, tgt);
                ulong fileSize = f.FileSize != 0 ? f.FileSize : (ulong)FileSystem.GetFileSize(tgt);
                TableProperties props = f.TableProperties;
                var meta = new FileMetaData
                {
                    FileNumber = fileNum,
                    PathId = c.OutputPathId,
                    FileSize = fileSize,
                    SmallestSeqno = f.SmallestSeqno,
                    LargestSeqno = f.LargestSeqno,
                    Smallest = f.SmallestInternalKey,
                    Largest = f.LargestInternalKey,
                    OldestBlobFileNumber = f.OldestBlobFileNumber,
                    OldestAncestorTime = f.OldestAncestorTime,
                    FileCreationTime = f.FileCreationTime,
                    EpochNumber = f.EpochNumber,
                    FileChecksum = f.FileChecksum,
                    FileChecksumFuncName = f.FileChecksumFuncName,
                    MarkedForCompaction = f.MarkedForCompaction,
                    UniqueId = f.UniqueId,
                    Temperature = f.FileTemperature,
                    // FileMetaData::CalculateTailSize.
                    TailSize = props.TailStartOffset > 0 && props.TailStartOffset <= fileSize ? fileSize - props.TailStartOffset : 0,
                    UserDefinedTimestampsPersisted = props.UserDefinedTimestampsPersisted != 0,
                };
                if (props.NumEntries > 0) meta.SetEntryStats(props);
                outcome.Outputs.Add(meta);
            }
        }
        catch (Exception e) when (e is IOException or RocksDbException)
        {
            foreach (ulong n in outcome.CreatedFileNumbers) TryDeleteFile(FileNames.TableFileName(DbPaths, n, c.OutputPathId));
            lock (_mutex)
            {
                foreach (ulong n in outcome.CreatedFileNumbers) _pendingOutputs.Remove(n);
            }
            service.OnInstallation(response.ScheduledJobId, CompactionServiceJobStatus.Failure);
            throw;
        }
        Stats?.RecordTick(Ticker.RemoteCompactReadBytes, result.BytesRead);
        Stats?.RecordTick(Ticker.RemoteCompactWriteBytes, result.BytesWritten);
        service.OnInstallation(response.ScheduledJobId, CompactionServiceJobStatus.Success);
        return outcome;
    }

    private int _numRunningRemoteCompactions; // rocksdb.num-running-remote-compactions (waiting in CompactionService::Wait)

    /// <summary>Compactions currently waiting for their remote worker.</summary>
    internal int NumRunningRemoteCompactions => Volatile.Read(ref _numRunningRemoteCompactions);

    private static T Await<T>(ValueTask<T> t) => t.IsCompletedSuccessfully ? t.Result : t.AsTask().GetAwaiter().GetResult();

    // ---- worker ----------------------------------------------------------------------------------------

    /// <summary>
    /// RemoteCompactionJobDir (file/filename.cc): the job's output directory is relative to the DB directory unless it
    /// already starts with it.
    /// </summary>
    internal static string RemoteCompactionJobDir(string dbname, string jobDirName)
    {
        string prefix = dbname.EndsWith('/') ? dbname : dbname + "/";
        return jobDirName.StartsWith(prefix, StringComparison.Ordinal) ? jobDirName : prefix + jobDirName;
    }

    /// <summary>DB::OpenAndCompact: runs the serialized job against the DB at <paramref name="name"/> and returns the serialized result.</summary>
    public static async ValueTask<byte[]> OpenAndCompactAsync(OpenAndCompactOptions options, string name, string outputDirectory, ReadOnlyMemory<byte> input,
        CompactionServiceOptionsOverride overrideOptions, bool synchronous, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentException.ThrowIfNullOrEmpty(outputDirectory);
        ArgumentNullException.ThrowIfNull(overrideOptions);
        if (options.Canceled.IsCancellationRequested) throw ManualCompactionPausedException(null);
        CompactionServiceInput ci = CompactionServiceFormat.ReadInput(input.Span);
        string dbname = name.Length > 1 ? name.TrimEnd('/') : name;
        IFileSystem fs = overrideOptions.FileSystem ?? LocalFileSystem.Instance;
        OptionsFile file = OptionsFile.Load(fs, FileNames.OptionsFileName(dbname, ci.OptionsFileNumber), preserveUnknownOptions: true);
        DbOptions dbOptions = file.DbOptions.Clone();
        dbOptions.FileSystem = fs;
        dbOptions.FileChecksumGenFactory = overrideOptions.FileChecksumGenFactory;
        dbOptions.Statistics = overrideOptions.Statistics;
        dbOptions.InfoLog = overrideOptions.InfoLog;
        dbOptions.Listeners.Clear();
        if (overrideOptions.Listeners is { } listeners) dbOptions.Listeners.AddRange(listeners);
        dbOptions.CompactionService = null;
        ApplyOptionsMap(dbOptions, overrideOptions.OptionsMap);
        string outputPath = RemoteCompactionJobDir(dbname, outputDirectory);

        var columnFamilies = new List<ColumnFamilyDescriptor>();
        bool found = false;
        foreach (ColumnFamilyDescriptor cf in file.ColumnFamilies)
        {
            if (cf.Name == ci.CfName)
            {
                ColumnFamilyOptions o = cf.Options.Clone();
                ApplyOptionsMap(o, overrideOptions.OptionsMap);
                o.Comparator = overrideOptions.Comparator;
                o.MergeOperator = overrideOptions.MergeOperator;
                o.CompactionFilter = overrideOptions.CompactionFilter;
                o.CompactionFilterFactory = overrideOptions.CompactionFilterFactory;
                o.PrefixExtractor = overrideOptions.PrefixExtractor;
                o.SstPartitionerFactory = overrideOptions.SstPartitionerFactory;
                o.TablePropertiesCollectorFactories = overrideOptions.TablePropertiesCollectorFactories is { } f ? new List<TablePropertiesCollectorFactory>(f) : new();
                columnFamilies.Add(new ColumnFamilyDescriptor(cf.Name, o));
                found = true;
            }
            else if (cf.Name == ColumnFamilyDescriptor.DefaultColumnFamilyName)
            {
                columnFamilies.Add(cf);
            }
        }
        if (!found) ThrowHelper.InvalidArgument("Cannot find column family " + ci.CfName + " in the OPTIONS file");

        DbImpl? db = null;
        for (uint retry = 0; ; retry++)
        {
            try
            {
                db = await OpenReadOnlyAsync(dbname, dbOptions, columnFamilies, errorIfWalFileExists: false, outputPath, recoverWal: false, synchronous, ct)
                    .ConfigureAwait(false);
                break;
            }
            catch (RocksDbException e) when (e.Code is StatusCode.TryAgain or StatusCode.NotFound
                                             || e.SubCode == StatusSubCode.PathNotFound)
            {
                // The primary may have replaced CURRENT / the MANIFEST concurrently.
                if (retry >= options.MaxSecondaryOpenRetries) throw;
            }
        }
        try
        {
            if (ci.MinManifestFileNumber != 0 && db._manifestTailer is { } tailer && tailer.ManifestNumber < ci.MinManifestFileNumber)
            {
                throw new RocksDbException(StatusCode.Incomplete,
                    "Incomplete: Remote compaction worker recovered an older MANIFEST view than the primary scheduled from");
            }
            CompactionServiceResult result = await db.CompactWithoutInstallationAsync(options, db.GetColumnFamilyData(ci.CfName), ci, outputPath, synchronous)
                .ConfigureAwait(false);
            byte[] serialized = CompactionServiceFormat.Write(result);
            if (!result.Ok) throw new OpenAndCompactException(result.ToException(), serialized);
            return serialized;
        }
        finally
        {
            await db.CloseAsync(synchronous).ConfigureAwait(false);
        }
    }

    private static void ApplyOptionsMap<T>(PersistedOptions<T> target, IReadOnlyDictionary<string, string>? map) where T : PersistedOptions<T>
    {
        if (map is null) return;
        foreach (KeyValuePair<string, string> kv in map)
        {
            // GetDBOptionsFromMap / GetColumnFamilyOptionsFromMap with ignore_unknown_options: names of the other kind are skipped.
            if (target.OptionTable.TryGet(kv.Key, out _)) target.SetOption(kv.Key, kv.Value, preserveUnknown: false);
        }
    }

    /// <summary>DBImplSecondary::CompactWithoutInstallation: rebuilds the compaction from the input file numbers and runs it into <paramref name="outputPath"/>.</summary>
    private async ValueTask<CompactionServiceResult> CompactWithoutInstallationAsync(OpenAndCompactOptions options, ColumnFamilyData cfd,
        CompactionServiceInput ci, string outputPath, bool synchronous)
    {
        var result = new CompactionServiceResult { OutputLevel = ci.OutputLevel, OutputPath = outputPath };
        if (options.Canceled.IsCancellationRequested) return Failed(result, ManualCompactionPausedException(null));
        if (ci.InputBlobFiles.Count > 0 || ci.BlobFileNumberStart != 0 || cfd.Options.EnableBlobFiles)
        {
            return Failed(result, new RocksDbException(StatusCode.Incomplete, "Incomplete: the managed remote compaction worker does not support BlobDB"));
        }
        var inputSet = new HashSet<ulong>();
        foreach (string name in ci.InputFiles)
        {
            if (!FileNames.TryParseFileName(name.AsSpan().TrimStart('/'), out ulong number, out FileType type) || type != FileType.TableFile)
            {
                return Failed(result, RocksDbException.InvalidArgument("Invalid input file name " + name));
            }
            inputSet.Add(number);
        }
        Compaction? c = null;
        CompactionJob? job = null;
        bool ok = false;
        long start = System.Diagnostics.Stopwatch.GetTimestamp();
        try
        {
            lock (_mutex)
            {
                VersionStorageInfo vstorage = cfd.Current.Storage;
                List<CompactionInputFiles> inputs = GetCompactionInputsFromFileNumbers(inputSet, vstorage);
                ulong outputFileSizeLimit = CompactionPicker.MaxFileSizeForLevel(cfd.Options, ci.OutputLevel, vstorage.BaseLevel);
                c = cfd.CompactionState.Picker.PickCompactionForCompactFiles(vstorage, inputs, ci.OutputLevel, 0, outputFileSizeLimit, null,
                    Temperature.Unknown, 1, allowTrivialMove: false);
                c.InputVersion = cfd.Current;
                cfd.Current.Ref();
                // The worker re-derives per-key placement; the primary only schedules compactions without it (or an
                // older primary sends no decision: proximal_level 0).
                if (ci.ProximalLevel != 0 && c.ProximalLevel != ci.ProximalLevel)
                {
                    throw new RocksDbException(StatusCode.Incomplete,
                        "Incomplete: Remote compaction worker's per-key placement differs from the primary's");
                }
            }
            // Use the primary's db_id for the outputs' unique ids; the session id stays the worker's own.
            if (ci.DbId.Length > 0) Identity = Identity with { DbId = ci.DbId };
            CancellationToken ct = options.Canceled;
            job = CompactionJob.ForRange(this, cfd, c, ci.Snapshots.ToArray(), Versions.NewFileNumber, ct,
                ci.HasBegin ? ci.Begin : null, ci.HasEnd ? ci.End : null, outputPath);
            await job.RunAsync(synchronous).ConfigureAwait(false);
            FileSystem.SyncDirectory(outputPath);
            FillResult(result, cfd, c, job, System.Diagnostics.Stopwatch.GetElapsedTime(start));
            ok = true;
            return result;
        }
        catch (OperationCanceledException e)
        {
            return Failed(result, ManualCompactionPausedException(e));
        }
        catch (RocksDbException e)
        {
            return Failed(result, e);
        }
        finally
        {
            if (c is not null)
            {
                lock (_mutex)
                {
                    cfd.CompactionState.Picker.ReleaseCompactionFiles(c, ok);
                    if (c.InputVersion is not null) Versions.UnrefVersion(c.InputVersion);
                }
            }
        }
    }

    private static CompactionServiceResult Failed(CompactionServiceResult r, RocksDbException e)
    {
        r.StatusCode = e.Code;
        r.StatusSubCode = e.SubCode;
        r.StatusSeverity = (byte)e.Severity;
        r.StatusMessage = e.Message;
        return r;
    }

    /// <summary>CompactionPicker::GetCompactionInputsFromFileNumbers: the given files per level, first to last non-empty level, without expansion.</summary>
    private static List<CompactionInputFiles> GetCompactionInputsFromFileNumbers(HashSet<ulong> inputSet, VersionStorageInfo vstorage)
    {
        if (inputSet.Count == 0) ThrowHelper.InvalidArgument("Compaction must include at least one file.");
        var remaining = new HashSet<ulong>(inputSet);
        var matched = new List<FileMetaData>[vstorage.NumLevels];
        int first = -1, last = -1;
        for (int level = 0; level < vstorage.NumLevels; ++level)
        {
            matched[level] = new List<FileMetaData>();
            foreach (FileMetaData f in vstorage.LevelFiles(level))
            {
                if (!remaining.Remove(f.FileNumber)) continue;
                matched[level].Add(f);
                last = level;
                if (first == -1) first = level;
            }
        }
        if (remaining.Count > 0)
        {
            ThrowHelper.InvalidArgument("Cannot find matched SST files for the following file numbers: " + string.Join(" ", remaining));
        }
        var inputs = new List<CompactionInputFiles>();
        for (int level = first; level <= last; ++level) inputs.Add(new CompactionInputFiles(level, matched[level]));
        return inputs;
    }

    /// <summary>The result of a finished worker compaction (CompactionServiceCompactionJob::Run filling compaction_result_).</summary>
    private void FillResult(CompactionServiceResult r, ColumnFamilyData cfd, Compaction c, CompactionJob job, TimeSpan elapsed)
    {
        CompactionJobStats s = job.Stats;
        ulong micros = (ulong)elapsed.TotalMicroseconds;
        foreach (FileMetaData f in job.Outputs)
        {
            string path = FileNames.MakeTableFileName(r.OutputPath, f.FileNumber);
            TableProperties props;
            using (BlockBasedTableReader reader = BlockBasedTableReader.Open(FileSystem.OpenRandomAccess(path), cfd.ReaderOptions with { BlockCache = NoBlockCache.Instance }))
            {
                props = reader.Properties;
            }
            r.OutputFiles.Add(new CompactionServiceOutputFile
            {
                FileName = FileNames.MakeTableFileName(f.FileNumber),
                FileSize = f.FileSize,
                SmallestSeqno = f.SmallestSeqno,
                LargestSeqno = f.LargestSeqno,
                SmallestInternalKey = f.Smallest,
                LargestInternalKey = f.Largest,
                OldestBlobFileNumber = f.OldestBlobFileNumber,
                OldestAncestorTime = f.OldestAncestorTime,
                FileCreationTime = f.FileCreationTime,
                EpochNumber = f.EpochNumber,
                FileChecksum = f.FileChecksum,
                FileChecksumFuncName = f.FileChecksumFuncName,
                MarkedForCompaction = f.MarkedForCompaction,
                UniqueId = f.UniqueId,
                TableProperties = props,
                FileTemperature = f.Temperature,
            });
        }
        r.BytesRead = s.BytesRead;
        r.BytesWritten = s.BytesWritten;
        // Like CompactionServiceCompactionJob::Run, the input-side statistics are left to the primary, which derives them
        // from the input files' table properties (VerifyInputRecordCount adds the worker's output-level stats to them).
        r.Stats = new RemoteCompactionJobStats
        {
            ElapsedMicros = micros,
            CpuMicros = micros,
            HasAccurateNumInputRecords = true,
            NumInputRecords = (ulong)s.NumInputRecords,
            NumOutputRecords = (ulong)s.NumOutputRecords,
            NumOutputFiles = (ulong)s.NumOutputFiles,
            IsFullCompaction = c.IsFullCompaction,
            IsManualCompaction = c.IsManual,
            IsRemoteCompaction = true,
            TotalOutputBytes = s.BytesWritten,
            NumRecordsReplaced = (ulong)Math.Max(0, s.NumInputRecords - s.NumOutputRecords),
        };
        var levelStats = new RemoteCompactionLevelStats
        {
            Micros = micros,
            CpuMicros = micros,
            BytesWritten = s.BytesWritten,
            NumOutputFiles = s.NumOutputFiles,
            NumOutputRecords = (ulong)s.NumOutputRecords,
            Count = 1,
        };
        int reason = (int)c.Reason;
        if (reason >= 0 && reason < levelStats.Counts.Length) levelStats.Counts[reason] = 1;
        r.OutputLevelStats = levelStats;
        r.ProximalLevelStats = new RemoteCompactionLevelStats { Micros = micros, CpuMicros = micros, Count = 1 };
        r.ProximalLevelStats.Counts[Math.Clamp(reason, 0, r.ProximalLevelStats.Counts.Length - 1)] = 1;
    }
}

/// <summary>
/// A failed <see cref="RocksDatabase.OpenAndCompact(OpenAndCompactOptions, string, string, ReadOnlySpan{byte}, CompactionServiceOptionsOverride)"/>:
/// <see cref="RocksDbException.Code"/> is the compaction's status and <see cref="Result"/> the serialized result carrying
/// it (upstream fills the output even when OpenAndCompact returns a non-OK status), which the service may return to the
/// primary from <see cref="CompactionService.WaitAsync"/> with <see cref="CompactionServiceJobStatus.Failure"/>.
/// </summary>
public sealed class OpenAndCompactException : RocksDbException
{
    internal OpenAndCompactException(RocksDbException status, byte[] result)
        : base(status.Code, status.Message, status.SubCode, status)
    {
        Result = result;
    }

    /// <summary>The serialized CompactionServiceResult.</summary>
    public byte[] Result { get; }
}
